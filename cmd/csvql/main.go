package main

import (
	"encoding/xml"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"

	"csvql"

	"github.com/google/uuid"
)

func main() {
	var (
		dir       = flag.String("dir", ".", "Directory to scan for CSV/TSV files")
		dbPath    = flag.String("db", "", "SQLite database path (default: .csvql.db in target dir)")
		query     = flag.String("q", "", "Execute a single query and exit")
		jetbrains = flag.Bool("jetbrains", false, "Create JetBrains IDE datasource configuration")
	)
	flag.Parse()

	opts := csvql.Options{
		RootDir: *dir,
		DBPath:  *dbPath,
		Watch:   true,
		OnChange: func(event, path string) {
			fmt.Printf("[%s] %s\n", event, path)
		},
	}

	c, err := csvql.New(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	// List loaded tables
	tables, err := c.ListTables()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error listing tables: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("CSVQL - CSV/TSV to SQLite\n")
	fmt.Printf("Database: %s\n", c.DBPath)
	fmt.Printf("Loaded %d table(s):\n", len(tables))
	for _, t := range tables {
		cols, _ := c.GetTableInfo(t)
		fmt.Printf("  - %s (%d columns)\n", t, len(cols))
	}
	fmt.Println()

	// Create IDE datasource if requested
	if *jetbrains {
		if err := createJetBrainsDatasource(c.RootDir, c.DBPath); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: could not create JetBrains datasource: %v\n", err)
		}
	}

	// Single query mode
	if *query != "" {
		executeQuery(c, *query)
		return
	}

	// Watch mode - wait for changes
	fmt.Println("Watching for changes... (Ctrl+C to stop)")
	fmt.Println()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nStopping...")
}

func executeQuery(c *csvql.CSVQL, query string) {
	columns, rows, err := c.Query(query)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(rows) == 0 {
		fmt.Println("(no results)")
		return
	}

	// Print results as table
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	// Header
	fmt.Fprintln(w, strings.Join(columns, "\t"))

	// Separator
	sep := make([]string, len(columns))
	for i := range sep {
		sep[i] = strings.Repeat("-", len(columns[i]))
	}
	fmt.Fprintln(w, strings.Join(sep, "\t"))

	// Rows
	for _, row := range rows {
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}

	w.Flush()
	fmt.Printf("\n(%d rows)\n", len(rows))
}

// JetBrains dataSources.xml structures
type dataSourcesProject struct {
	XMLName   xml.Name       `xml:"project"`
	Version   string         `xml:"version,attr"`
	Component dsComponent    `xml:"component"`
}

type dsComponent struct {
	Name           string       `xml:"name,attr"`
	Format         string       `xml:"format,attr"`
	MultifileModel string       `xml:"multifile-model,attr"`
	DataSources    []dataSource `xml:"data-source"`
}

type dataSource struct {
	Source      string       `xml:"source,attr"`
	Name        string       `xml:"name,attr"`
	UUID        string       `xml:"uuid,attr"`
	DriverRef   string       `xml:"driver-ref"`
	Synchronize bool         `xml:"synchronize"`
	JDBCDriver  string       `xml:"jdbc-driver"`
	JDBCURL     string       `xml:"jdbc-url"`
	WorkingDir  string       `xml:"working-dir"`
}

func createJetBrainsDatasource(rootDir, dbPath string) error {
	// Find .idea folder by walking up from rootDir
	ideaDir := findIdeaDir(rootDir)
	if ideaDir == "" {
		return fmt.Errorf(".idea folder not found")
	}

	dsFile := filepath.Join(ideaDir, "dataSources.xml")
	dsName := filepath.Base(rootDir)

	// Calculate relative path from project root to db
	projectRoot := filepath.Dir(ideaDir)
	relDBPath, err := filepath.Rel(projectRoot, dbPath)
	if err != nil {
		relDBPath = dbPath
	}
	jdbcURL := fmt.Sprintf("jdbc:sqlite:$PROJECT_DIR$/%s", relDBPath)

	// Load existing or create new
	var project dataSourcesProject
	if data, err := os.ReadFile(dsFile); err == nil {
		xml.Unmarshal(data, &project)
	} else {
		project = dataSourcesProject{
			Version: "4",
			Component: dsComponent{
				Name:           "DataSourceManagerImpl",
				Format:         "xml",
				MultifileModel: "true",
			},
		}
	}

	// Check if datasource already exists
	for _, ds := range project.Component.DataSources {
		if ds.Name == dsName {
			fmt.Printf("IDE datasource '%s' already exists\n", dsName)
			return nil
		}
	}

	// Add new datasource
	newDS := dataSource{
		Source:      "LOCAL",
		Name:        dsName,
		UUID:        uuid.New().String(),
		DriverRef:   "sqlite.xerial",
		Synchronize: true,
		JDBCDriver:  "org.sqlite.JDBC",
		JDBCURL:     jdbcURL,
		WorkingDir:  "$ProjectFileDir$",
	}
	project.Component.DataSources = append(project.Component.DataSources, newDS)

	// Write file
	output, err := xml.MarshalIndent(project, "", "  ")
	if err != nil {
		return err
	}

	header := []byte(xml.Header)
	if err := os.WriteFile(dsFile, append(header, output...), 0644); err != nil {
		return err
	}

	fmt.Printf("IDE datasource '%s' created in %s\n", dsName, dsFile)
	return nil
}

func findIdeaDir(startDir string) string {
	gitRoot := findGitRoot(startDir)
	dir := startDir
	for {
		ideaPath := filepath.Join(dir, ".idea")
		if info, err := os.Stat(ideaPath); err == nil && info.IsDir() {
			return ideaPath
		}

		// Stop at git root
		if gitRoot != "" && dir == gitRoot {
			break
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

func findGitRoot(startDir string) string {
	dir := startDir
	for {
		gitPath := filepath.Join(dir, ".git")
		if info, err := os.Stat(gitPath); err == nil && info.IsDir() {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

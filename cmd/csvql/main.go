package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"

	"csvql"
)

func main() {
	var (
		dir   = flag.String("dir", ".", "Directory to scan for CSV/TSV files")
		dbPath = flag.String("db", "", "SQLite database path (default: .csvql.db in target dir)")
		query = flag.String("q", "", "Execute a single query and exit")
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

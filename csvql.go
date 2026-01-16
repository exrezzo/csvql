// Package csvql provides CSV/TSV to SQLite functionality
package csvql

import (
	"fmt"
	"path/filepath"

	"csvql/db"
	"csvql/loader"
	"csvql/watcher"
)

// CSVQL is the main interface for CSV/TSV to SQLite operations
type CSVQL struct {
	RootDir   string
	DBPath    string
	DB        *db.Manager
	Watcher   *watcher.Watcher
	OnChange  func(event string, path string)
}

// Options for creating a new CSVQL instance
type Options struct {
	RootDir  string
	DBPath   string
	Watch    bool
	OnChange func(event string, path string)
}

// New creates a new CSVQL instance
func New(opts Options) (*CSVQL, error) {
	if opts.RootDir == "" {
		opts.RootDir = "."
	}

	absRoot, err := filepath.Abs(opts.RootDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	if opts.DBPath == "" {
		opts.DBPath = filepath.Join(absRoot, ".csvql.db")
	}

	dbManager, err := db.New(opts.DBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	c := &CSVQL{
		RootDir:  absRoot,
		DBPath:   opts.DBPath,
		DB:       dbManager,
		OnChange: opts.OnChange,
	}

	// Initial scan and load
	if err := c.Scan(); err != nil {
		dbManager.Close()
		return nil, err
	}

	// Start watcher if requested
	if opts.Watch {
		w, err := watcher.New(absRoot, dbManager)
		if err != nil {
			dbManager.Close()
			return nil, fmt.Errorf("failed to create watcher: %w", err)
		}
		if opts.OnChange != nil {
			w.SetOnChange(opts.OnChange)
		}
		w.Start()
		c.Watcher = w
	}

	return c, nil
}

// Scan finds and loads all CSV/TSV files
func (c *CSVQL) Scan() error {
	files, err := loader.ScanDirectory(c.RootDir)
	if err != nil {
		return fmt.Errorf("failed to scan directory: %w", err)
	}

	for _, file := range files {
		parsed, err := loader.ParseFile(file, c.RootDir)
		if err != nil {
			fmt.Printf("Warning: failed to parse %s: %v\n", file, err)
			continue
		}

		if !c.DB.NeedsUpdate(parsed.Info.TableName, parsed.Info.ModTime) {
			continue
		}

		if err := c.DB.LoadFile(parsed); err != nil {
			fmt.Printf("Warning: failed to load %s: %v\n", file, err)
			continue
		}
	}

	return nil
}

// Query executes a SQL query
func (c *CSVQL) Query(sql string) ([]string, [][]string, error) {
	return c.DB.Query(sql)
}

// ListTables returns all loaded tables
func (c *CSVQL) ListTables() ([]string, error) {
	return c.DB.ListTables()
}

// GetTableInfo returns column names for a table
func (c *CSVQL) GetTableInfo(tableName string) ([]string, error) {
	return c.DB.GetTableInfo(tableName)
}

// Close cleans up resources
func (c *CSVQL) Close() error {
	if c.Watcher != nil {
		c.Watcher.Stop()
	}
	return c.DB.Close()
}

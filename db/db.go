// Package db handles SQLite database operations
package db

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"csvql/loader"

	_ "github.com/mattn/go-sqlite3"
)

// Manager handles SQLite database operations
type Manager struct {
	db       *sql.DB
	mu       sync.RWMutex
	metadata map[string]int64 // tableName -> modTime
}

// New creates a new database manager
func New(dbPath string) (*Manager, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Create metadata table
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS _csvql_metadata (
			table_name TEXT PRIMARY KEY,
			file_path TEXT NOT NULL,
			mod_time INTEGER NOT NULL
		)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create metadata table: %w", err)
	}

	m := &Manager{
		db:       db,
		metadata: make(map[string]int64),
	}

	// Load existing metadata
	if err := m.loadMetadata(); err != nil {
		db.Close()
		return nil, err
	}

	return m, nil
}

// loadMetadata loads existing table metadata from database
func (m *Manager) loadMetadata() error {
	rows, err := m.db.Query("SELECT table_name, mod_time FROM _csvql_metadata")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		var modTime int64
		if err := rows.Scan(&tableName, &modTime); err != nil {
			return err
		}
		m.metadata[tableName] = modTime
	}
	return rows.Err()
}

// NeedsUpdate checks if a file needs to be reloaded
func (m *Manager) NeedsUpdate(tableName string, modTime int64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	existingModTime, exists := m.metadata[tableName]
	return !exists || existingModTime != modTime
}

// LoadFile loads a parsed CSV/TSV file into SQLite
func (m *Manager) LoadFile(parsed *loader.ParsedFile) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableName := parsed.Info.TableName

	// Start transaction
	tx, err := m.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Drop existing table
	_, err = tx.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		return fmt.Errorf("failed to drop table %s: %w", tableName, err)
	}

	// Build column definitions
	columns := make([]string, len(parsed.Info.Headers))
	columnNames := make([]string, len(parsed.Info.Headers))
	for i, header := range parsed.Info.Headers {
		colName := loader.SanitizeColumnName(header)
		// Handle duplicate column names
		baseName := colName
		counter := 1
		for j := 0; j < i; j++ {
			if columnNames[j] == colName {
				colName = fmt.Sprintf("%s_%d", baseName, counter)
				counter++
			}
		}
		columnNames[i] = colName
		columns[i] = fmt.Sprintf("%s TEXT", colName)
	}

	// Create table
	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(columns, ", "))
	_, err = tx.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	// Insert data
	if len(parsed.Records) > 0 {
		placeholders := make([]string, len(columnNames))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			tableName,
			strings.Join(columnNames, ", "),
			strings.Join(placeholders, ", "))

		stmt, err := tx.Prepare(insertSQL)
		if err != nil {
			return fmt.Errorf("failed to prepare insert: %w", err)
		}
		defer stmt.Close()

		for _, record := range parsed.Records {
			// Pad or trim record to match column count
			values := make([]interface{}, len(columnNames))
			for i := range values {
				if i < len(record) {
					values[i] = record[i]
				} else {
					values[i] = ""
				}
			}
			_, err = stmt.Exec(values...)
			if err != nil {
				return fmt.Errorf("failed to insert record: %w", err)
			}
		}
	}

	// Update metadata
	_, err = tx.Exec(`
		INSERT OR REPLACE INTO _csvql_metadata (table_name, file_path, mod_time)
		VALUES (?, ?, ?)
	`, tableName, parsed.Info.Path, parsed.Info.ModTime)
	if err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	m.metadata[tableName] = parsed.Info.ModTime
	return nil
}

// RemoveTable removes a table from the database
func (m *Manager) RemoveTable(tableName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, err := m.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		return err
	}

	_, err = m.db.Exec("DELETE FROM _csvql_metadata WHERE table_name = ?", tableName)
	if err != nil {
		return err
	}

	delete(m.metadata, tableName)
	return nil
}

// RemoveTableByPath removes a table associated with the given file path
func (m *Manager) RemoveTableByPath(filePath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find table name from file path
	var tableName string
	err := m.db.QueryRow("SELECT table_name FROM _csvql_metadata WHERE file_path = ?", filePath).Scan(&tableName)
	if err != nil {
		return fmt.Errorf("no table found for path %s: %w", filePath, err)
	}

	_, err = m.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	if err != nil {
		return err
	}

	_, err = m.db.Exec("DELETE FROM _csvql_metadata WHERE file_path = ?", filePath)
	if err != nil {
		return err
	}

	delete(m.metadata, tableName)
	return nil
}

// RenameTable renames a table and updates metadata
func (m *Manager) RenameTable(oldName, newName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if oldName == newName {
		return nil
	}

	_, err := m.db.Exec(fmt.Sprintf("ALTER TABLE %s RENAME TO %s", oldName, newName))
	if err != nil {
		return err
	}

	_, err = m.db.Exec("UPDATE _csvql_metadata SET table_name = ? WHERE table_name = ?", newName, oldName)
	if err != nil {
		return err
	}

	if modTime, exists := m.metadata[oldName]; exists {
		delete(m.metadata, oldName)
		m.metadata[newName] = modTime
	}

	return nil
}

// GetAllTableMappings returns a map of file_path -> table_name for all tables
func (m *Manager) GetAllTableMappings() (map[string]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rows, err := m.db.Query("SELECT file_path, table_name FROM _csvql_metadata")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var filePath, tableName string
		if err := rows.Scan(&filePath, &tableName); err != nil {
			return nil, err
		}
		result[filePath] = tableName
	}
	return result, rows.Err()
}

// Query executes a SQL query and returns results
func (m *Manager) Query(query string) ([]string, [][]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	var results [][]string
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, nil, err
		}

		row := make([]string, len(columns))
		for i, v := range values {
			if v == nil {
				row[i] = "NULL"
			} else {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		results = append(results, row)
	}

	return columns, results, rows.Err()
}

// ListTables returns all loaded CSV/TSV tables
func (m *Manager) ListTables() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rows, err := m.db.Query("SELECT table_name FROM _csvql_metadata ORDER BY table_name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// GetTableInfo returns column info for a table
func (m *Manager) GetTableInfo(tableName string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	rows, err := m.db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dfltValue interface{}
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, rows.Err()
}

// Close closes the database connection
func (m *Manager) Close() error {
	return m.db.Close()
}

// DB returns the underlying database connection (for testing)
func (m *Manager) DB() *sql.DB {
	return m.db
}

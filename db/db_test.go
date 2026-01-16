package db

import (
	"csvql/loader"
	"os"
	"path/filepath"
	"testing"
)

func TestNew(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	// Verify metadata table exists
	var tableName string
	err = m.db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='_csvql_metadata'").Scan(&tableName)
	if err != nil {
		t.Errorf("Metadata table not created: %v", err)
	}
}

func TestLoadFile(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	parsed := &loader.ParsedFile{
		Info: loader.FileInfo{
			Path:      "/test/users.csv",
			TableName: "users",
			Delimiter: ',',
			Headers:   []string{"id", "name", "email"},
			ModTime:   12345,
		},
		Records: [][]string{
			{"1", "Alice", "alice@example.com"},
			{"2", "Bob", "bob@example.com"},
		},
	}

	if err := m.LoadFile(parsed); err != nil {
		t.Fatalf("LoadFile failed: %v", err)
	}

	// Verify table was created
	columns, rows, err := m.Query("SELECT * FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(columns))
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}

	if rows[0][1] != "Alice" {
		t.Errorf("Expected 'Alice', got %q", rows[0][1])
	}
}

func TestLoadFile_DuplicateColumns(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	parsed := &loader.ParsedFile{
		Info: loader.FileInfo{
			Path:      "/test/dup.csv",
			TableName: "dup_cols",
			Delimiter: ',',
			Headers:   []string{"name", "name", "name"},
			ModTime:   12345,
		},
		Records: [][]string{
			{"a", "b", "c"},
		},
	}

	if err := m.LoadFile(parsed); err != nil {
		t.Fatalf("LoadFile failed: %v", err)
	}

	columns, _, err := m.Query("SELECT * FROM dup_cols")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	// Should have deduplicated column names
	if len(columns) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(columns))
	}
}

func TestNeedsUpdate(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	// New table should need update
	if !m.NeedsUpdate("newtable", 12345) {
		t.Error("Expected NeedsUpdate to return true for new table")
	}

	// Load a table
	parsed := &loader.ParsedFile{
		Info: loader.FileInfo{
			Path:      "/test/test.csv",
			TableName: "test",
			Headers:   []string{"col1"},
			ModTime:   12345,
		},
		Records: [][]string{{"val1"}},
	}
	m.LoadFile(parsed)

	// Same modtime should not need update
	if m.NeedsUpdate("test", 12345) {
		t.Error("Expected NeedsUpdate to return false for same modtime")
	}

	// Different modtime should need update
	if !m.NeedsUpdate("test", 99999) {
		t.Error("Expected NeedsUpdate to return true for different modtime")
	}
}

func TestRemoveTable(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	// Load a table
	parsed := &loader.ParsedFile{
		Info: loader.FileInfo{
			Path:      "/test/test.csv",
			TableName: "to_remove",
			Headers:   []string{"col1"},
			ModTime:   12345,
		},
		Records: [][]string{{"val1"}},
	}
	m.LoadFile(parsed)

	// Remove it
	if err := m.RemoveTable("to_remove"); err != nil {
		t.Fatalf("RemoveTable failed: %v", err)
	}

	// Verify table is gone
	_, _, err = m.Query("SELECT * FROM to_remove")
	if err == nil {
		t.Error("Expected error querying removed table")
	}

	// Verify metadata is gone
	tables, _ := m.ListTables()
	for _, t := range tables {
		if t == "to_remove" {
			t = "Table still in metadata after removal"
		}
	}
}

func TestListTables(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	// Initially empty
	tables, err := m.ListTables()
	if err != nil {
		t.Fatalf("ListTables failed: %v", err)
	}
	if len(tables) != 0 {
		t.Errorf("Expected 0 tables, got %d", len(tables))
	}

	// Add some tables
	for _, name := range []string{"alpha", "beta", "gamma"} {
		parsed := &loader.ParsedFile{
			Info: loader.FileInfo{
				Path:      "/test/" + name + ".csv",
				TableName: name,
				Headers:   []string{"col1"},
				ModTime:   12345,
			},
			Records: [][]string{{"val1"}},
		}
		m.LoadFile(parsed)
	}

	tables, err = m.ListTables()
	if err != nil {
		t.Fatalf("ListTables failed: %v", err)
	}
	if len(tables) != 3 {
		t.Errorf("Expected 3 tables, got %d", len(tables))
	}

	// Should be sorted
	if tables[0] != "alpha" || tables[1] != "beta" || tables[2] != "gamma" {
		t.Errorf("Tables not sorted correctly: %v", tables)
	}
}

func TestGetTableInfo(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	parsed := &loader.ParsedFile{
		Info: loader.FileInfo{
			Path:      "/test/info.csv",
			TableName: "info_test",
			Headers:   []string{"id", "name", "value"},
			ModTime:   12345,
		},
		Records: [][]string{{"1", "test", "100"}},
	}
	m.LoadFile(parsed)

	cols, err := m.GetTableInfo("info_test")
	if err != nil {
		t.Fatalf("GetTableInfo failed: %v", err)
	}

	if len(cols) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(cols))
	}

	expected := []string{"id", "name", "value"}
	for i, col := range cols {
		if col != expected[i] {
			t.Errorf("Expected column %q, got %q", expected[i], col)
		}
	}
}

func TestQuery_Aggregation(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	parsed := &loader.ParsedFile{
		Info: loader.FileInfo{
			Path:      "/test/sales.csv",
			TableName: "sales",
			Headers:   []string{"product", "amount"},
			ModTime:   12345,
		},
		Records: [][]string{
			{"A", "100"},
			{"B", "200"},
			{"A", "150"},
			{"B", "50"},
		},
	}
	m.LoadFile(parsed)

	// Test COUNT
	_, rows, err := m.Query("SELECT COUNT(*) as cnt FROM sales")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if rows[0][0] != "4" {
		t.Errorf("Expected count 4, got %s", rows[0][0])
	}

	// Test GROUP BY
	cols, rows, err := m.Query("SELECT product, COUNT(*) as cnt FROM sales GROUP BY product ORDER BY product")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(rows))
	}
	if cols[0] != "product" || cols[1] != "cnt" {
		t.Errorf("Unexpected columns: %v", cols)
	}
}

func TestQuery_Join(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	// Create users table
	users := &loader.ParsedFile{
		Info: loader.FileInfo{
			Path:      "/test/users.csv",
			TableName: "users",
			Headers:   []string{"id", "name"},
			ModTime:   12345,
		},
		Records: [][]string{
			{"1", "Alice"},
			{"2", "Bob"},
		},
	}
	m.LoadFile(users)

	// Create orders table
	orders := &loader.ParsedFile{
		Info: loader.FileInfo{
			Path:      "/test/orders.csv",
			TableName: "orders",
			Headers:   []string{"user_id", "amount"},
			ModTime:   12345,
		},
		Records: [][]string{
			{"1", "100"},
			{"1", "200"},
			{"2", "50"},
		},
	}
	m.LoadFile(orders)

	// Test JOIN
	_, rows, err := m.Query(`
		SELECT u.name, o.amount 
		FROM users u 
		JOIN orders o ON u.id = o.user_id 
		ORDER BY u.name, o.amount
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("Expected 3 rows from join, got %d", len(rows))
	}
}

func TestPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "persist.db")

	// Create and populate
	m1, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	parsed := &loader.ParsedFile{
		Info: loader.FileInfo{
			Path:      "/test/persist.csv",
			TableName: "persistent",
			Headers:   []string{"data"},
			ModTime:   12345,
		},
		Records: [][]string{{"value1"}, {"value2"}},
	}
	m1.LoadFile(parsed)
	m1.Close()

	// Reopen and verify
	m2, err := New(dbPath)
	if err != nil {
		t.Fatalf("New (reopen) failed: %v", err)
	}
	defer m2.Close()

	tables, _ := m2.ListTables()
	if len(tables) != 1 || tables[0] != "persistent" {
		t.Errorf("Expected persistent table, got %v", tables)
	}

	_, rows, _ := m2.Query("SELECT * FROM persistent")
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows after reopen, got %d", len(rows))
	}

	// ModTime should be remembered
	if m2.NeedsUpdate("persistent", 12345) {
		t.Error("Should not need update for same modtime after reopen")
	}
}

func TestLoadFile_Update(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	// Initial load
	parsed := &loader.ParsedFile{
		Info: loader.FileInfo{
			Path:      "/test/update.csv",
			TableName: "updateable",
			Headers:   []string{"col1"},
			ModTime:   100,
		},
		Records: [][]string{{"old"}},
	}
	m.LoadFile(parsed)

	// Update with new data
	parsed.Info.ModTime = 200
	parsed.Records = [][]string{{"new1"}, {"new2"}}
	m.LoadFile(parsed)

	_, rows, _ := m.Query("SELECT * FROM updateable")
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows after update, got %d", len(rows))
	}
	if rows[0][0] != "new1" {
		t.Errorf("Expected 'new1', got %q", rows[0][0])
	}
}

func TestConcurrentQueries(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	parsed := &loader.ParsedFile{
		Info: loader.FileInfo{
			Path:      "/test/concurrent.csv",
			TableName: "concurrent",
			Headers:   []string{"id"},
			ModTime:   12345,
		},
		Records: [][]string{{"1"}, {"2"}, {"3"}},
	}
	m.LoadFile(parsed)

	// Run concurrent queries
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_, _, err := m.Query("SELECT * FROM concurrent")
			if err != nil {
				t.Errorf("Concurrent query failed: %v", err)
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestLoadFile_EmptyRecords(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	parsed := &loader.ParsedFile{
		Info: loader.FileInfo{
			Path:      "/test/empty.csv",
			TableName: "empty_table",
			Headers:   []string{"col1", "col2"},
			ModTime:   12345,
		},
		Records: [][]string{}, // No data rows
	}

	if err := m.LoadFile(parsed); err != nil {
		t.Fatalf("LoadFile failed: %v", err)
	}

	// Table should exist with correct schema
	cols, err := m.GetTableInfo("empty_table")
	if err != nil {
		t.Fatalf("GetTableInfo failed: %v", err)
	}
	if len(cols) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(cols))
	}

	// Should return empty results
	_, rows, _ := m.Query("SELECT * FROM empty_table")
	if len(rows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(rows))
	}
}

func TestLoadFile_UnevenRows(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer m.Close()

	// Row with fewer columns than headers
	parsed := &loader.ParsedFile{
		Info: loader.FileInfo{
			Path:      "/test/uneven.csv",
			TableName: "uneven",
			Headers:   []string{"a", "b", "c"},
			ModTime:   12345,
		},
		Records: [][]string{
			{"1", "2", "3"},
			{"4", "5"}, // Missing column
			{"6"},      // Missing two columns
		},
	}

	if err := m.LoadFile(parsed); err != nil {
		t.Fatalf("LoadFile failed: %v", err)
	}

	_, rows, _ := m.Query("SELECT * FROM uneven")
	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}

	// Missing values should be empty strings
	if rows[1][2] != "" {
		t.Errorf("Expected empty string for missing column, got %q", rows[1][2])
	}
}

func TestDatabaseFile_Created(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "newdb.db")

	// File should not exist yet
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatal("Database file should not exist before creation")
	}

	m, err := New(dbPath)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	m.Close()

	// File should exist now
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Database file should exist after creation")
	}
}

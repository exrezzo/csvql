package csvql

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNew_BasicUsage(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test CSV
	csvPath := filepath.Join(tmpDir, "users.csv")
	os.WriteFile(csvPath, []byte("id,name,email\n1,Alice,alice@test.com\n2,Bob,bob@test.com"), 0644)

	c, err := New(Options{RootDir: tmpDir})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	tables, _ := c.ListTables()
	if len(tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(tables))
	}

	if tables[0] != "users" {
		t.Errorf("Expected table 'users', got %q", tables[0])
	}
}

func TestNew_MultipleFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple files
	os.WriteFile(filepath.Join(tmpDir, "a.csv"), []byte("col\nval"), 0644)
	os.WriteFile(filepath.Join(tmpDir, "b.csv"), []byte("col\nval"), 0644)
	os.WriteFile(filepath.Join(tmpDir, "c.tsv"), []byte("col\tval\nval\tval"), 0644)

	c, err := New(Options{RootDir: tmpDir})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	tables, _ := c.ListTables()
	if len(tables) != 3 {
		t.Errorf("Expected 3 tables, got %d: %v", len(tables), tables)
	}
}

func TestNew_NestedDirectories(t *testing.T) {
	tmpDir := t.TempDir()

	// Create nested structure
	os.MkdirAll(filepath.Join(tmpDir, "data/sales"), 0755)
	os.MkdirAll(filepath.Join(tmpDir, "data/users"), 0755)

	os.WriteFile(filepath.Join(tmpDir, "data/sales/2024.csv"), []byte("month,amount\nJan,100"), 0644)
	os.WriteFile(filepath.Join(tmpDir, "data/users/active.csv"), []byte("id,name\n1,test"), 0644)

	c, err := New(Options{RootDir: tmpDir})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	tables, _ := c.ListTables()
	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d: %v", len(tables), tables)
	}

	// Check table names reflect path
	foundSales := false
	foundUsers := false
	for _, tbl := range tables {
		if tbl == "data_sales_2024" {
			foundSales = true
		}
		if tbl == "data_users_active" {
			foundUsers = true
		}
	}
	if !foundSales || !foundUsers {
		t.Errorf("Expected nested table names, got %v", tables)
	}
}

func TestQuery_Select(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "data.csv"), []byte("id,name,score\n1,Alice,100\n2,Bob,85\n3,Charlie,92"), 0644)

	c, err := New(Options{RootDir: tmpDir})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	// Basic select
	cols, rows, err := c.Query("SELECT * FROM data")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(cols) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(cols))
	}
	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}
}

func TestQuery_Where(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "products.csv"), []byte("id,name,price\n1,Apple,1.50\n2,Banana,0.75\n3,Cherry,2.00"), 0644)

	c, err := New(Options{RootDir: tmpDir})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	cols, rows, err := c.Query("SELECT name FROM products WHERE price > '1.00'")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(cols) != 1 || cols[0] != "name" {
		t.Errorf("Expected single 'name' column, got %v", cols)
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}
}

func TestQuery_OrderBy(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "nums.csv"), []byte("val\n3\n1\n2"), 0644)

	c, err := New(Options{RootDir: tmpDir})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	_, rows, err := c.Query("SELECT val FROM nums ORDER BY val ASC")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if rows[0][0] != "1" || rows[1][0] != "2" || rows[2][0] != "3" {
		t.Errorf("Expected ordered results, got %v", rows)
	}
}

func TestQuery_Join(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "employees.csv"), []byte("id,name,dept_id\n1,Alice,10\n2,Bob,20"), 0644)
	os.WriteFile(filepath.Join(tmpDir, "departments.csv"), []byte("id,name\n10,Engineering\n20,Sales"), 0644)

	c, err := New(Options{RootDir: tmpDir})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	_, rows, err := c.Query(`
		SELECT e.name, d.name as dept 
		FROM employees e 
		JOIN departments d ON e.dept_id = d.id
		ORDER BY e.name
	`)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}

	if rows[0][0] != "Alice" || rows[0][1] != "Engineering" {
		t.Errorf("Unexpected join result: %v", rows[0])
	}
}

func TestQuery_Aggregate(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "sales.csv"), []byte("region,amount\nNorth,100\nSouth,200\nNorth,150\nSouth,50"), 0644)

	c, err := New(Options{RootDir: tmpDir})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	_, rows, err := c.Query("SELECT region, SUM(amount) as total FROM sales GROUP BY region ORDER BY region")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(rows))
	}
}

func TestGetTableInfo(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "info.csv"), []byte("first_name,last_name,age\n"), 0644)

	c, err := New(Options{RootDir: tmpDir})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	cols, err := c.GetTableInfo("info")
	if err != nil {
		t.Fatalf("GetTableInfo failed: %v", err)
	}

	expected := []string{"first_name", "last_name", "age"}
	if len(cols) != len(expected) {
		t.Errorf("Expected %d columns, got %d", len(expected), len(cols))
	}

	for i, col := range cols {
		if col != expected[i] {
			t.Errorf("Expected column %q, got %q", expected[i], col)
		}
	}
}

func TestScan_Rescan(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "data.csv"), []byte("col\nold"), 0644)

	c, err := New(Options{RootDir: tmpDir})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	// Modify file
	time.Sleep(10 * time.Millisecond) // Ensure different mtime
	os.WriteFile(filepath.Join(tmpDir, "data.csv"), []byte("col\nnew"), 0644)

	// Rescan
	if err := c.Scan(); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	_, rows, _ := c.Query("SELECT * FROM data")
	if len(rows) != 1 || rows[0][0] != "new" {
		t.Errorf("Expected updated data, got %v", rows)
	}
}

func TestNew_WithWatch(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "initial.csv"), []byte("col\nval"), 0644)

	eventChan := make(chan string, 10)
	c, err := New(Options{
		RootDir: tmpDir,
		Watch:   true,
		OnChange: func(event, path string) {
			eventChan <- event
		},
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	// Create new file
	os.WriteFile(filepath.Join(tmpDir, "new.csv"), []byte("col\nnew"), 0644)

	// Wait for event
	select {
	case event := <-eventChan:
		if event != "UPDATE" {
			t.Errorf("Expected UPDATE event, got %s", event)
		}
	case <-time.After(3 * time.Second):
		t.Error("Timeout waiting for file change event")
	}

	// Verify new table exists
	tables, _ := c.ListTables()
	found := false
	for _, tbl := range tables {
		if tbl == "new" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected 'new' table, got %v", tables)
	}
}

func TestNew_EmptyDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := New(Options{RootDir: tmpDir})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	tables, _ := c.ListTables()
	if len(tables) != 0 {
		t.Errorf("Expected 0 tables for empty dir, got %d", len(tables))
	}
}

func TestNew_CustomDBPath(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "custom.db")
	dataDir := filepath.Join(tmpDir, "data")
	os.MkdirAll(dataDir, 0755)
	os.WriteFile(filepath.Join(dataDir, "test.csv"), []byte("col\nval"), 0644)

	c, err := New(Options{
		RootDir: dataDir,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	if c.DBPath != dbPath {
		t.Errorf("Expected DBPath %q, got %q", dbPath, c.DBPath)
	}

	// Verify file was created
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Error("Database file not created at custom path")
	}
}

func TestClose_Idempotent(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "test.csv"), []byte("col\nval"), 0644)

	c, err := New(Options{RootDir: tmpDir, Watch: true})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Multiple closes should not panic
	c.Close()
	// Note: second close may error but should not panic
}

func TestQuery_InvalidSQL(t *testing.T) {
	tmpDir := t.TempDir()
	os.WriteFile(filepath.Join(tmpDir, "test.csv"), []byte("col\nval"), 0644)

	c, err := New(Options{RootDir: tmpDir})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	_, _, err = c.Query("INVALID SQL SYNTAX")
	if err == nil {
		t.Error("Expected error for invalid SQL")
	}
}

func TestQuery_NonExistentTable(t *testing.T) {
	tmpDir := t.TempDir()

	c, err := New(Options{RootDir: tmpDir})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	defer c.Close()

	_, _, err = c.Query("SELECT * FROM nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

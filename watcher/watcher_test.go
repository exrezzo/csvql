package watcher

import (
	"csvql/db"
	"csvql/loader"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestWatcher_NewFile(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := db.New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create db: %v", err)
	}
	defer m.Close()

	w, err := New(tmpDir, m)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}

	var mu sync.Mutex
	events := []string{}
	w.SetOnChange(func(event, path string) {
		mu.Lock()
		events = append(events, event+":"+filepath.Base(path))
		mu.Unlock()
	})

	w.Start()
	defer w.Stop()

	// Create a new CSV file
	csvPath := filepath.Join(tmpDir, "new.csv")
	err = os.WriteFile(csvPath, []byte("id,name\n1,test"), 0644)
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	// Wait for watcher to process
	time.Sleep(1500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(events) == 0 {
		t.Error("Expected at least one event")
	}

	// Verify table was created
	tables, _ := m.ListTables()
	found := false
	for _, tbl := range tables {
		if tbl == "new" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected 'new' table to be created, got tables: %v", tables)
	}
}

func TestWatcher_ModifyFile(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create initial file
	csvPath := filepath.Join(tmpDir, "modify.csv")
	err := os.WriteFile(csvPath, []byte("id,value\n1,original"), 0644)
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	m, err := db.New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create db: %v", err)
	}
	defer m.Close()

	// Load initial data
	parsed, _ := loader.ParseFile(csvPath, tmpDir)
	m.LoadFile(parsed)

	w, err := New(tmpDir, m)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}

	w.Start()
	defer w.Stop()

	// Modify the file
	time.Sleep(100 * time.Millisecond)
	err = os.WriteFile(csvPath, []byte("id,value\n1,modified\n2,new"), 0644)
	if err != nil {
		t.Fatalf("Failed to modify file: %v", err)
	}

	// Wait for watcher to process
	time.Sleep(1500 * time.Millisecond)

	// Verify data was updated
	_, rows, err := m.Query("SELECT * FROM modify ORDER BY id")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 rows after update, got %d", len(rows))
	}

	if len(rows) > 0 && rows[0][1] != "modified" {
		t.Errorf("Expected 'modified', got %q", rows[0][1])
	}
}

func TestWatcher_DeleteFile(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create initial file
	csvPath := filepath.Join(tmpDir, "todelete.csv")
	err := os.WriteFile(csvPath, []byte("id\n1"), 0644)
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	m, err := db.New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create db: %v", err)
	}
	defer m.Close()

	// Load initial data
	parsed, _ := loader.ParseFile(csvPath, tmpDir)
	m.LoadFile(parsed)

	var mu sync.Mutex
	events := []string{}

	w, err := New(tmpDir, m)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	w.SetOnChange(func(event, path string) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	})

	w.Start()
	defer w.Stop()

	// Delete the file
	time.Sleep(100 * time.Millisecond)
	os.Remove(csvPath)

	// Wait for watcher to process
	time.Sleep(1500 * time.Millisecond)

	mu.Lock()
	hasDelete := false
	for _, e := range events {
		if e == "DELETE" {
			hasDelete = true
			break
		}
	}
	mu.Unlock()

	if !hasDelete {
		t.Error("Expected DELETE event")
	}

	// Verify table was removed
	tables, _ := m.ListTables()
	for _, tbl := range tables {
		if tbl == "todelete" {
			t.Error("Table should have been removed")
		}
	}
}

func TestWatcher_IgnoreNonCSV(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := db.New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create db: %v", err)
	}
	defer m.Close()

	eventCount := 0
	var mu sync.Mutex

	w, err := New(tmpDir, m)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}
	w.SetOnChange(func(event, path string) {
		mu.Lock()
		eventCount++
		mu.Unlock()
	})

	w.Start()
	defer w.Stop()

	// Create non-CSV files
	os.WriteFile(filepath.Join(tmpDir, "test.txt"), []byte("text"), 0644)
	os.WriteFile(filepath.Join(tmpDir, "test.json"), []byte("{}"), 0644)
	os.WriteFile(filepath.Join(tmpDir, "test.md"), []byte("# md"), 0644)

	time.Sleep(1500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if eventCount > 0 {
		t.Errorf("Expected no events for non-CSV files, got %d", eventCount)
	}
}

func TestWatcher_Subdirectory(t *testing.T) {
	tmpDir := t.TempDir()
	subDir := filepath.Join(tmpDir, "subdir")
	os.MkdirAll(subDir, 0755)

	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := db.New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create db: %v", err)
	}
	defer m.Close()

	w, err := New(tmpDir, m)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}

	var mu sync.Mutex
	events := []string{}
	w.SetOnChange(func(event, path string) {
		mu.Lock()
		events = append(events, filepath.Base(path))
		mu.Unlock()
	})

	w.Start()
	defer w.Stop()

	// Create CSV in subdirectory
	csvPath := filepath.Join(subDir, "nested.csv")
	err = os.WriteFile(csvPath, []byte("col\nval"), 0644)
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	time.Sleep(1500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(events) == 0 {
		t.Error("Expected event for subdirectory file")
	}

	// Verify table exists - name is "nested" (base name) since no conflicts
	tables, _ := m.ListTables()
	found := false
	for _, tbl := range tables {
		if tbl == "nested" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected 'nested' table, got %v", tables)
	}
}

func TestWatcher_TSVFile(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := db.New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create db: %v", err)
	}
	defer m.Close()

	w, err := New(tmpDir, m)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}

	w.Start()
	defer w.Stop()

	// Create TSV file
	tsvPath := filepath.Join(tmpDir, "data.tsv")
	err = os.WriteFile(tsvPath, []byte("id\tname\n1\ttest"), 0644)
	if err != nil {
		t.Fatalf("Failed to write file: %v", err)
	}

	time.Sleep(1500 * time.Millisecond)

	// Verify table was created
	tables, _ := m.ListTables()
	found := false
	for _, tbl := range tables {
		if tbl == "data" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected 'data' table for TSV, got %v", tables)
	}
}

func TestWatcher_Stop(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	m, err := db.New(dbPath)
	if err != nil {
		t.Fatalf("Failed to create db: %v", err)
	}
	defer m.Close()

	w, err := New(tmpDir, m)
	if err != nil {
		t.Fatalf("Failed to create watcher: %v", err)
	}

	w.Start()

	// Stop should not block or panic
	done := make(chan bool)
	go func() {
		w.Stop()
		done <- true
	}()

	select {
	case <-done:
		// Good
	case <-time.After(2 * time.Second):
		t.Error("Stop blocked for too long")
	}
}

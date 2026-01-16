package loader

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScanDirectory(t *testing.T) {
	// Create temp directory with test files
	tmpDir := t.TempDir()

	// Create test files
	testFiles := []string{
		"data.csv",
		"info.tsv",
		"subdir/nested.csv",
		"subdir/deep/more.tsv",
		"ignore.txt",
		"readme.md",
	}

	for _, f := range testFiles {
		path := filepath.Join(tmpDir, f)
		os.MkdirAll(filepath.Dir(path), 0755)
		os.WriteFile(path, []byte("header\nvalue"), 0644)
	}

	files, err := ScanDirectory(tmpDir)
	if err != nil {
		t.Fatalf("ScanDirectory failed: %v", err)
	}

	// Should find 4 CSV/TSV files
	if len(files) != 4 {
		t.Errorf("Expected 4 files, got %d", len(files))
	}

	// Verify only CSV/TSV files are included
	for _, f := range files {
		ext := filepath.Ext(f)
		if ext != ".csv" && ext != ".tsv" {
			t.Errorf("Unexpected file extension: %s", f)
		}
	}
}

func TestGetTableName(t *testing.T) {
	tests := []struct {
		filePath string
		rootDir  string
		expected string
	}{
		{"/root/data.csv", "/root", "data"},
		{"/root/sub/file.tsv", "/root", "sub_file"},
		{"/root/my-file.csv", "/root", "my_file"},
		{"/root/123data.csv", "/root", "_123data"},
		{"/root/a.b.c.csv", "/root", "a_b_c"},
		{"/root/dir/sub dir/file.csv", "/root", "dir_sub_dir_file"},
	}

	for _, tt := range tests {
		result := GetTableName(tt.filePath, tt.rootDir)
		if result != tt.expected {
			t.Errorf("GetTableName(%q, %q) = %q, want %q",
				tt.filePath, tt.rootDir, result, tt.expected)
		}
	}
}

func TestDetectDelimiter(t *testing.T) {
	tests := []struct {
		path     string
		expected rune
	}{
		{"file.csv", ','},
		{"file.CSV", ','},
		{"file.tsv", '\t'},
		{"file.TSV", '\t'},
		{"path/to/file.csv", ','},
		{"path/to/file.tsv", '\t'},
	}

	for _, tt := range tests {
		result := DetectDelimiter(tt.path)
		if result != tt.expected {
			t.Errorf("DetectDelimiter(%q) = %q, want %q",
				tt.path, string(result), string(tt.expected))
		}
	}
}

func TestSanitizeColumnName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"Name", "name"},
		{"First Name", "first_name"},
		{"user-id", "user_id"},
		{"price.usd", "price_usd"},
		{"count()", "count"},
		{"123col", "_123col"},
		{"", "column"},
		{"UPPER_CASE", "upper_case"},
	}

	for _, tt := range tests {
		result := SanitizeColumnName(tt.input)
		if result != tt.expected {
			t.Errorf("SanitizeColumnName(%q) = %q, want %q",
				tt.input, result, tt.expected)
		}
	}
}

func TestParseFile_CSV(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test.csv")

	content := `id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
3,Charlie,charlie@example.com`

	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	parsed, err := ParseFile(csvPath, tmpDir)
	if err != nil {
		t.Fatalf("ParseFile failed: %v", err)
	}

	if parsed.Info.TableName != "test" {
		t.Errorf("Expected table name 'test', got %q", parsed.Info.TableName)
	}

	if len(parsed.Info.Headers) != 3 {
		t.Errorf("Expected 3 headers, got %d", len(parsed.Info.Headers))
	}

	if len(parsed.Records) != 3 {
		t.Errorf("Expected 3 records, got %d", len(parsed.Records))
	}

	if parsed.Info.Delimiter != ',' {
		t.Errorf("Expected comma delimiter, got %q", string(parsed.Info.Delimiter))
	}
}

func TestParseFile_TSV(t *testing.T) {
	tmpDir := t.TempDir()
	tsvPath := filepath.Join(tmpDir, "test.tsv")

	content := "id\tname\tvalue\n1\tFirst\t100\n2\tSecond\t200"

	if err := os.WriteFile(tsvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	parsed, err := ParseFile(tsvPath, tmpDir)
	if err != nil {
		t.Fatalf("ParseFile failed: %v", err)
	}

	if parsed.Info.Delimiter != '\t' {
		t.Errorf("Expected tab delimiter, got %q", string(parsed.Info.Delimiter))
	}

	if len(parsed.Records) != 2 {
		t.Errorf("Expected 2 records, got %d", len(parsed.Records))
	}
}

func TestParseFile_Empty(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "empty.csv")

	if err := os.WriteFile(csvPath, []byte(""), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	_, err := ParseFile(csvPath, tmpDir)
	if err == nil {
		t.Error("Expected error for empty file, got nil")
	}
}

func TestParseFile_HeaderOnly(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "header_only.csv")

	if err := os.WriteFile(csvPath, []byte("id,name,value\n"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	parsed, err := ParseFile(csvPath, tmpDir)
	if err != nil {
		t.Fatalf("ParseFile failed: %v", err)
	}

	if len(parsed.Records) != 0 {
		t.Errorf("Expected 0 records for header-only file, got %d", len(parsed.Records))
	}
}

func TestParseFile_QuotedFields(t *testing.T) {
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "quoted.csv")

	content := `name,description,value
"John Doe","A ""quoted"" description",100
"Jane, Smith","Contains, commas",200`

	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	parsed, err := ParseFile(csvPath, tmpDir)
	if err != nil {
		t.Fatalf("ParseFile failed: %v", err)
	}

	if len(parsed.Records) != 2 {
		t.Errorf("Expected 2 records, got %d", len(parsed.Records))
	}

	// Check that commas in quoted fields are handled correctly
	if parsed.Records[1][0] != "Jane, Smith" {
		t.Errorf("Expected 'Jane, Smith', got %q", parsed.Records[1][0])
	}
}

func TestParseFile_NonExistent(t *testing.T) {
	_, err := ParseFile("/nonexistent/file.csv", "/nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}

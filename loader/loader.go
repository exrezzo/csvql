// Package loader handles CSV/TSV file parsing and loading into SQLite
package loader

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FileInfo represents metadata about a CSV/TSV file
type FileInfo struct {
	Path      string
	TableName string
	Delimiter rune
	Headers   []string
	ModTime   int64
}

// ParsedFile contains all data from a parsed CSV/TSV file
type ParsedFile struct {
	Info    FileInfo
	Records [][]string
}

// ScanDirectory finds all CSV and TSV files in directory and subdirectories
func ScanDirectory(rootDir string) ([]string, error) {
	var files []string

	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(path))
		if ext == ".csv" || ext == ".tsv" {
			files = append(files, path)
		}
		return nil
	})

	return files, err
}

// sanitizeTableName applies SQLite naming rules to a name
func sanitizeTableName(name string) string {
	// Replace path separators and invalid chars with underscores
	name = strings.ReplaceAll(name, string(filepath.Separator), "_")
	name = strings.ReplaceAll(name, "-", "_")
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ReplaceAll(name, ".", "_")

	// Ensure it starts with a letter or underscore
	if len(name) > 0 && name[0] >= '0' && name[0] <= '9' {
		name = "_" + name
	}

	return strings.ToLower(name)
}

// GetBaseTableName generates a table name using only the file name (without path)
func GetBaseTableName(filePath string) string {
	base := filepath.Base(filePath)
	ext := filepath.Ext(base)
	name := strings.TrimSuffix(base, ext)
	return sanitizeTableName(name)
}

// GetFullTableName generates a table name including the relative path
func GetFullTableName(filePath, rootDir string) string {
	// Get relative path from root
	relPath, err := filepath.Rel(rootDir, filePath)
	if err != nil {
		relPath = filepath.Base(filePath)
	}

	// Remove extension
	ext := filepath.Ext(relPath)
	name := strings.TrimSuffix(relPath, ext)

	return sanitizeTableName(name)
}

// GetTableName generates a valid SQLite table name from file path
// Deprecated: Use GetBaseTableName or GetFullTableName with conflict detection
func GetTableName(filePath, rootDir string) string {
	return GetFullTableName(filePath, rootDir)
}

// ResolveTableNames takes a list of file paths and returns a map of path -> table name
// Uses base name only when unique, full path when there are conflicts
func ResolveTableNames(filePaths []string, rootDir string) map[string]string {
	// First pass: count base names
	baseNameCount := make(map[string][]string)
	for _, path := range filePaths {
		baseName := GetBaseTableName(path)
		baseNameCount[baseName] = append(baseNameCount[baseName], path)
	}

	// Second pass: assign names based on conflicts
	result := make(map[string]string)
	for _, path := range filePaths {
		baseName := GetBaseTableName(path)
		if len(baseNameCount[baseName]) > 1 {
			// Conflict: use full path name
			result[path] = GetFullTableName(path, rootDir)
		} else {
			// No conflict: use base name only
			result[path] = baseName
		}
	}

	return result
}

// DetectDelimiter determines if file is CSV or TSV based on extension
func DetectDelimiter(filePath string) rune {
	ext := strings.ToLower(filepath.Ext(filePath))
	if ext == ".tsv" {
		return '\t'
	}
	return ','
}

// ParseFile reads and parses a CSV/TSV file
// tableName is optional - if empty, uses GetFullTableName for backwards compatibility
func ParseFile(filePath, rootDir string, tableName ...string) (*ParsedFile, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file %s: %w", filePath, err)
	}

	delimiter := DetectDelimiter(filePath)

	reader := csv.NewReader(file)
	reader.Comma = delimiter
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse file %s: %w", filePath, err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("file %s is empty", filePath)
	}

	// Use provided table name or fall back to full path name
	resolvedTableName := GetFullTableName(filePath, rootDir)
	if len(tableName) > 0 && tableName[0] != "" {
		resolvedTableName = tableName[0]
	}

	return &ParsedFile{
		Info: FileInfo{
			Path:      filePath,
			TableName: resolvedTableName,
			Delimiter: delimiter,
			Headers:   records[0],
			ModTime:   stat.ModTime().UnixNano(),
		},
		Records: records[1:], // Exclude headers
	}, nil
}

// SanitizeColumnName creates a valid SQLite column name
func SanitizeColumnName(name string) string {
	// Replace invalid characters
	sanitized := strings.ReplaceAll(name, " ", "_")
	sanitized = strings.ReplaceAll(sanitized, "-", "_")
	sanitized = strings.ReplaceAll(sanitized, ".", "_")
	sanitized = strings.ReplaceAll(sanitized, "(", "")
	sanitized = strings.ReplaceAll(sanitized, ")", "")

	// Ensure it starts with a letter or underscore
	if len(sanitized) > 0 && sanitized[0] >= '0' && sanitized[0] <= '9' {
		sanitized = "_" + sanitized
	}

	if sanitized == "" {
		sanitized = "column"
	}

	return strings.ToLower(sanitized)
}

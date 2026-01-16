// Package watcher provides file system watching for CSV/TSV changes
package watcher

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"csvql/db"
	"csvql/loader"

	"github.com/fsnotify/fsnotify"
)

// Watcher monitors directory for CSV/TSV file changes
type Watcher struct {
	rootDir   string
	dbManager *db.Manager
	fsWatcher *fsnotify.Watcher
	done      chan struct{}
	wg        sync.WaitGroup
	onChange  func(event string, path string)
}

// New creates a new file watcher
func New(rootDir string, dbManager *db.Manager) (*Watcher, error) {
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	w := &Watcher{
		rootDir:   rootDir,
		dbManager: dbManager,
		fsWatcher: fsWatcher,
		done:      make(chan struct{}),
	}

	// Add all directories to watcher
	err = filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return fsWatcher.Add(path)
		}
		return nil
	})
	if err != nil {
		fsWatcher.Close()
		return nil, err
	}

	return w, nil
}

// SetOnChange sets callback for file changes
func (w *Watcher) SetOnChange(fn func(event string, path string)) {
	w.onChange = fn
}

// Start begins watching for file changes
func (w *Watcher) Start() {
	w.wg.Add(1)
	go w.watch()
}

// Stop stops the watcher
func (w *Watcher) Stop() {
	close(w.done)
	w.wg.Wait()
	w.fsWatcher.Close()
}

func (w *Watcher) watch() {
	defer w.wg.Done()

	// Debounce map to avoid processing same file multiple times
	pending := make(map[string]time.Time)
	var mu sync.Mutex
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-w.done:
			return

		case event, ok := <-w.fsWatcher.Events:
			if !ok {
				return
			}

			// Check if it's a CSV/TSV file
			ext := strings.ToLower(filepath.Ext(event.Name))
			if ext != ".csv" && ext != ".tsv" {
				// Check if new directory was created
				if event.Has(fsnotify.Create) {
					if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
						w.fsWatcher.Add(event.Name)
					}
				}
				continue
			}

			mu.Lock()
			pending[event.Name] = time.Now()
			mu.Unlock()

		case err, ok := <-w.fsWatcher.Errors:
			if !ok {
				return
			}
			log.Printf("Watcher error: %v", err)

		case <-ticker.C:
			mu.Lock()
			now := time.Now()
			for path, t := range pending {
				// Process files that haven't been modified for 300ms
				if now.Sub(t) > 300*time.Millisecond {
					delete(pending, path)
					go w.processFile(path)
				}
			}
			mu.Unlock()
		}
	}
}

func (w *Watcher) processFile(path string) {
	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// File was deleted
		tableName := loader.GetTableName(path, w.rootDir)
		if err := w.dbManager.RemoveTable(tableName); err != nil {
			log.Printf("Error removing table %s: %v", tableName, err)
		} else {
			if w.onChange != nil {
				w.onChange("DELETE", path)
			}
			log.Printf("Removed table: %s", tableName)
		}
		return
	}

	// File was created or modified
	parsed, err := loader.ParseFile(path, w.rootDir)
	if err != nil {
		log.Printf("Error parsing file %s: %v", path, err)
		return
	}

	if err := w.dbManager.LoadFile(parsed); err != nil {
		log.Printf("Error loading file %s: %v", path, err)
		return
	}

	if w.onChange != nil {
		w.onChange("UPDATE", path)
	}
	log.Printf("Updated table: %s", parsed.Info.TableName)
}

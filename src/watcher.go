package main

import (
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
)

type Watcher struct {
	fw       *fsnotify.Watcher
	watchDir string
	handler  func(op, absPath string)
}

func NewWatcher(watchDir string, handler func(op, absPath string)) (*Watcher, error) {
	fw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	w := &Watcher{fw: fw, watchDir: watchDir, handler: handler}

	// Add the root and every subdirectory recursively.
	err = filepath.WalkDir(watchDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if d.Name() == MetadataDir {
				return filepath.SkipDir
			}
			return fw.Add(p)
		}
		return nil
	})
	if err != nil {
		fw.Close()
		return nil, err
	}
	return w, nil
}

// Add registers a path (file or directory) with the underlying watcher.
func (w *Watcher) Add(p string) {
	if err := w.fw.Add(p); err != nil {
		log.Printf("watcher add %q: %v", p, err)
	}
}

// Run processes fsnotify events until stopCh is closed.
func (w *Watcher) Run(stopCh <-chan struct{}) {
	defer w.fw.Close()
	metaPrefix := filepath.Join(w.watchDir, MetadataDir) + string(os.PathSeparator)

	for {
		select {
		case <-stopCh:
			return

		case event, ok := <-w.fw.Events:
			if !ok {
				return
			}
			// Skip anything inside .synak/
			if strings.HasPrefix(event.Name, metaPrefix) {
				continue
			}
			op := ""
			switch {
			case event.Has(fsnotify.Create):
				op = "create"
			case event.Has(fsnotify.Write):
				op = "write"
			case event.Has(fsnotify.Remove):
				op = "remove"
			case event.Has(fsnotify.Rename):
				op = "rename"
			default:
				continue
			}
			w.handler(op, event.Name)

		case err, ok := <-w.fw.Errors:
			if !ok {
				return
			}
			log.Printf("watcher error: %v", err)
		}
	}
}

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

const (
	MetadataDir = ".synak"
	IndexFile   = "index.json"
)

var defaultExcludes = []string{
	"*.swp", "*-swp", "*.swpx", "*.swn",
	".DS_Store",
	"Thumbs.db",
	"*.tmp", "*.temp",
	"*~",
}

// FileEntry mirrors Python's FileEntry.to_dict() JSON shape exactly.
type FileEntry struct {
	Path            string         `json:"path"`
	Checksum        string         `json:"checksum"`
	ModifiedTime    float64        `json:"modified_time"`
	VectorClockData map[string]any `json:"vector_clock_data"`
	Deleted         bool           `json:"deleted"`
}

func (e FileEntry) GetClock(nodeID string) VectorClock {
	return ClockFromDict(e.VectorClockData, nodeID)
}

type FileIndex struct {
	watchDir  string
	nodeID    string
	excludes  []string
	metaDir   string
	indexPath string

	mu        sync.RWMutex
	entries   map[string]FileEntry
	corrupted map[string]struct{}
}

func NewFileIndex(watchDir, nodeID string, extraExcludes []string) *FileIndex {
	watchDir = expandHome(watchDir)
	metaDir := filepath.Join(watchDir, MetadataDir)
	excl := make([]string, len(defaultExcludes)+len(extraExcludes))
	copy(excl, defaultExcludes)
	copy(excl[len(defaultExcludes):], extraExcludes)
	return &FileIndex{
		watchDir:  watchDir,
		nodeID:    nodeID,
		excludes:  excl,
		metaDir:   metaDir,
		indexPath: filepath.Join(metaDir, IndexFile),
		entries:   make(map[string]FileEntry),
		corrupted: make(map[string]struct{}),
	}
}

func (fi *FileIndex) Load() error {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	if err := os.MkdirAll(fi.metaDir, 0o755); err != nil {
		return err
	}
	hidePath(fi.metaDir)
	data, err := os.ReadFile(fi.indexPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &fi.entries)
}

func (fi *FileIndex) Save() error {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	data, err := json.MarshalIndent(fi.entries, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(fi.indexPath, data, 0o644)
}

// Scan re-indexes the whole watch directory. Files are hashed in a goroutine pool.
func (fi *FileIndex) Scan() error {
	type scanItem struct {
		absPath string
		relPath string
		mtime   float64
	}

	var items []scanItem
	seen := make(map[string]struct{})

	err := filepath.WalkDir(fi.watchDir, func(absPath string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if d.Name() == MetadataDir {
				return filepath.SkipDir
			}
			return nil
		}
		rel := filepath.ToSlash(mustRel(fi.watchDir, absPath))
		if fi.isExcluded(path.Base(rel), rel) {
			return nil
		}
		seen[rel] = struct{}{}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		mtime := float64(info.ModTime().UnixNano()) / 1e9
		items = append(items, scanItem{absPath, rel, mtime})
		return nil
	})
	if err != nil {
		return err
	}

	// Snapshot current entries for mtime comparison inside workers (read-only).
	fi.mu.RLock()
	snap := make(map[string]FileEntry, len(fi.entries))
	for k, v := range fi.entries {
		snap[k] = v
	}
	fi.mu.RUnlock()

	type result struct {
		rel      string
		checksum string
		mtime    float64
		changed  bool
	}

	workCh := make(chan scanItem, len(items))
	for _, it := range items {
		workCh <- it
	}
	close(workCh)

	resultCh := make(chan result, len(items))
	numWorkers := runtime.NumCPU()
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range workCh {
				existing, ok := snap[item.relPath]
				if ok && !existing.Deleted && math.Abs(item.mtime-existing.ModifiedTime) <= 0.001 {
					resultCh <- result{item.relPath, existing.Checksum, item.mtime, false}
					continue
				}
				checksum, err := sha256File(item.absPath)
				if err != nil {
					continue
				}
				changed := !ok || existing.Deleted || checksum != existing.Checksum
				resultCh <- result{item.relPath, checksum, item.mtime, changed}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	fi.mu.Lock()
	defer fi.mu.Unlock()

	for r := range resultCh {
		existing, ok := fi.entries[r.rel]
		if ok && !existing.Deleted && !r.changed {
			continue
		}
		var clk VectorClock
		if ok {
			clk = existing.GetClock(fi.nodeID)
		} else {
			clk = NewClock(fi.nodeID)
		}
		clk.Increment()
		fi.entries[r.rel] = FileEntry{
			Path:            r.rel,
			Checksum:        r.checksum,
			ModifiedTime:    r.mtime,
			VectorClockData: clk.ToDict(),
		}
	}

	// Tombstone files no longer on disk.
	for rel, entry := range fi.entries {
		if _, ok := seen[rel]; !ok && !entry.Deleted {
			clk := entry.GetClock(fi.nodeID)
			clk.Increment()
			fi.entries[rel] = FileEntry{
				Path:            rel,
				VectorClockData: clk.ToDict(),
				Deleted:         true,
			}
		}
	}

	return nil
}

// ScanOne re-indexes a single file. Returns true if the entry changed.
func (fi *FileIndex) ScanOne(relPath string) bool {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	return fi.scanOneNoLock(relPath)
}

// ScanDir walks absDir and calls scanOneNoLock on each file. Must be called under write lock.
func (fi *FileIndex) ScanDir(absDir string) {
	filepath.WalkDir(absDir, func(absPath string, d fs.DirEntry, err error) error { //nolint:errcheck
		if err != nil {
			return nil
		}
		if d.IsDir() {
			if d.Name() == MetadataDir {
				return filepath.SkipDir
			}
			return nil
		}
		rel := filepath.ToSlash(mustRel(fi.watchDir, absPath))
		fi.scanOneNoLock(rel)
		return nil
	})
}

func (fi *FileIndex) scanOneNoLock(relPath string) bool {
	relPath = filepath.ToSlash(relPath)
	fname := path.Base(relPath)
	if fi.isExcluded(fname, relPath) {
		return false
	}
	absPath := filepath.Join(fi.watchDir, filepath.FromSlash(relPath))
	info, err := os.Stat(absPath)
	if err != nil || info.IsDir() {
		return false
	}
	mtime := float64(info.ModTime().UnixNano()) / 1e9
	existing, ok := fi.entries[relPath]
	if ok && !existing.Deleted && math.Abs(mtime-existing.ModifiedTime) <= 0.001 {
		return false
	}
	checksum, err := sha256File(absPath)
	if err != nil {
		return false
	}
	if ok && !existing.Deleted && checksum == existing.Checksum {
		return false
	}
	var clk VectorClock
	if ok {
		clk = existing.GetClock(fi.nodeID)
	} else {
		clk = NewClock(fi.nodeID)
	}
	clk.Increment()
	fi.entries[relPath] = FileEntry{
		Path:            relPath,
		Checksum:        checksum,
		ModifiedTime:    mtime,
		VectorClockData: clk.ToDict(),
	}
	return true
}

// MarkDeleted tombstones a single file. Returns false if path isn't in the index
// (caller should try MarkDirDeleted instead).
func (fi *FileIndex) MarkDeleted(relPath string) bool {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	relPath = filepath.ToSlash(relPath)
	absPath := filepath.Join(fi.watchDir, filepath.FromSlash(relPath))
	if _, err := os.Stat(absPath); err == nil {
		return false // file still exists (atomic-write rename)
	}
	existing, ok := fi.entries[relPath]
	if !ok || existing.Deleted {
		return false
	}
	fi.entries[relPath] = fi.makeTombstone(relPath, existing)
	return true
}

// MarkDirDeleted tombstones all live entries whose path starts with relDir/.
func (fi *FileIndex) MarkDirDeleted(relDir string) bool {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	relDir = filepath.ToSlash(relDir)
	prefix := relDir + "/"
	dirty := false
	for rel, entry := range fi.entries {
		if strings.HasPrefix(rel, prefix) && !entry.Deleted {
			fi.entries[rel] = fi.makeTombstone(rel, entry)
			dirty = true
		}
	}
	return dirty
}

func (fi *FileIndex) Get(relPath string) *FileEntry {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	e, ok := fi.entries[relPath]
	if !ok {
		return nil
	}
	copy := e
	return &copy
}

func (fi *FileIndex) AllEntries() map[string]FileEntry {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	snap := make(map[string]FileEntry, len(fi.entries))
	for k, v := range fi.entries {
		snap[k] = v
	}
	return snap
}

// ApplyRemote writes a remote file to disk (or removes it for tombstones) and updates the index.
func (fi *FileIndex) ApplyRemote(entry FileEntry, content []byte) error {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	return fi.applyRemoteNoLock(entry, content)
}

func (fi *FileIndex) applyRemoteNoLock(entry FileEntry, content []byte) error {
	delete(fi.corrupted, entry.Path)
	absPath := filepath.Join(fi.watchDir, filepath.FromSlash(entry.Path))

	if entry.Deleted {
		if _, err := os.Stat(absPath); err == nil {
			if err := os.Remove(absPath); err != nil {
				return err
			}
			// Prune empty parent directories up to watch root.
			parent := filepath.Dir(absPath)
			for parent != fi.watchDir {
				entries, err := os.ReadDir(parent)
				if err != nil || len(entries) > 0 {
					break
				}
				os.Remove(parent) //nolint:errcheck
				parent = filepath.Dir(parent)
			}
		}
		fi.entries[entry.Path] = entry
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		return err
	}
	if content != nil {
		if err := os.WriteFile(absPath, content, 0o644); err != nil {
			return err
		}
	}
	mtime := entry.ModifiedTime
	if info, err := os.Stat(absPath); err == nil {
		mtime = float64(info.ModTime().UnixNano()) / 1e9
	}
	fi.entries[entry.Path] = FileEntry{
		Path:            entry.Path,
		Checksum:        entry.Checksum,
		ModifiedTime:    mtime,
		VectorClockData: entry.VectorClockData,
	}
	return nil
}

// VerifyOne returns true if the file's on-disk checksum differs from the index (corruption detected).
func (fi *FileIndex) VerifyOne(relPath string) bool {
	fi.mu.RLock()
	entry, ok := fi.entries[relPath]
	fi.mu.RUnlock()
	if !ok || entry.Deleted {
		return false
	}
	absPath := filepath.Join(fi.watchDir, filepath.FromSlash(relPath))
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		return false
	}
	checksum, err := sha256File(absPath)
	if err != nil {
		return false
	}
	return checksum != entry.Checksum
}

func (fi *FileIndex) MarkCorrupted(relPath string) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.corrupted[relPath] = struct{}{}
}

func (fi *FileIndex) IsCorrupted(relPath string) bool {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	_, ok := fi.corrupted[relPath]
	return ok
}

func (fi *FileIndex) isExcluded(fname, relPath string) bool {
	for _, pat := range fi.excludes {
		if m, _ := path.Match(pat, fname); m {
			return true
		}
		if m, _ := path.Match(pat, relPath); m {
			return true
		}
	}
	return false
}

func (fi *FileIndex) makeTombstone(relPath string, existing FileEntry) FileEntry {
	clk := existing.GetClock(fi.nodeID)
	clk.Increment()
	return FileEntry{
		Path:            relPath,
		VectorClockData: clk.ToDict(),
		Deleted:         true,
	}
}

func sha256File(absPath string) (string, error) {
	f, err := os.Open(absPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func mustRel(base, target string) string {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		log.Printf("filepath.Rel(%q, %q): %v", base, target, err)
		return target
	}
	return rel
}

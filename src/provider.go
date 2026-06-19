package main

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io/fs"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Provider struct {
	ctx      SyncContext
	nodeID   string
	watchDir string

	conflictStrategy string
	syncDeletes      bool
	peers            []string
	port             int

	index   *FileIndex
	watcher *Watcher

	listener net.Listener
	stopCh   chan struct{}
	paused   atomic.Bool

	// Paths seen as remotely deleted once but not yet confirmed — deferred one round
	// to avoid deleting files mid-move when the watcher hasn't indexed the new path yet.
	pendingDeletes sync.Map // map[string]struct{}

	// status fields
	statusMu sync.RWMutex
	state    string
	lastSync float64
	lastErr  string
}

func NewProvider(ctx SyncContext) (*Provider, error) {
	cfg := ctx.ProviderConfig

	peers := cfgStrings(cfg, "peers")
	if len(peers) == 0 {
		return nil, fmt.Errorf("'peers' is required and must be a non-empty list")
	}

	if cs, ok := cfg["conflict_strategy"].(string); ok {
		if cs != "last-write-wins" && cs != "keep-both" {
			return nil, fmt.Errorf("'conflict_strategy' must be \"last-write-wins\" or \"keep-both\", got %q", cs)
		}
	}

	if vi, ok := cfg["verify_interval"]; ok {
		if _, ok := vi.(float64); !ok {
			return nil, fmt.Errorf("'verify_interval' must be an integer")
		}
	}

	nodeID := cfgString(cfg, "node_id", "")
	if nodeID == "" {
		nodeID = randomID()
	}

	return &Provider{
		ctx:              ctx,
		nodeID:           nodeID,
		watchDir:         expandHome(ctx.Local),
		conflictStrategy: cfgString(cfg, "conflict_strategy", "last-write-wins"),
		syncDeletes:      cfgBool(cfg, "sync_deletes", true),
		peers:            peers,
		stopCh:           make(chan struct{}),
	}, nil
}

func (p *Provider) Start() error {
	p.index = NewFileIndex(p.watchDir, p.nodeID, p.ctx.Exclude)
	if err := p.index.Load(); err != nil {
		return fmt.Errorf("load index: %w", err)
	}
	if err := p.index.Scan(); err != nil {
		return fmt.Errorf("scan: %w", err)
	}
	if err := p.index.Save(); err != nil {
		return fmt.Errorf("save index: %w", err)
	}

	// Determine listen port.
	p.port = int(cfgFloat(p.ctx.ProviderConfig, "port", 0))
	if p.port == 0 {
		p.port = portForPair(p.ctx.PairID)
	}

	ln, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", p.port))
	if err != nil {
		return fmt.Errorf("listen on :%d: %w", p.port, err)
	}
	p.listener = ln

	var watchErr error
	p.watcher, watchErr = NewWatcher(p.watchDir, p.onWatchEvent)
	if watchErr != nil {
		ln.Close()
		return fmt.Errorf("watcher: %w", watchErr)
	}

	p.setState("idle")

	go p.acceptLoop()
	go p.syncLoop()
	go p.watcher.Run(p.stopCh)

	if vi := cfgFloat(p.ctx.ProviderConfig, "verify_interval", 0); vi > 0 {
		go p.verifyLoop(int(vi))
	}

	log.Printf("go-p2p: node %q listening on :%d", p.nodeID, p.port)
	return nil
}

func (p *Provider) Stop() {
	close(p.stopCh)
	if p.listener != nil {
		p.listener.Close()
	}
	p.index.Save() //nolint:errcheck
	p.setState("stopped")
}

func (p *Provider) Pause() {
	p.paused.Store(true)
	p.setState("paused")
}

func (p *Provider) Resume() {
	p.paused.Store(false)
	p.setState("idle")
}

func (p *Provider) Status() ProviderStatus {
	p.statusMu.RLock()
	defer p.statusMu.RUnlock()
	return ProviderStatus{
		PairID:   p.ctx.PairID,
		State:    p.state,
		LastSync: p.lastSync,
		Error:    p.lastErr,
		Extra:    map[string]any{"node_id": p.nodeID, "port": p.port},
	}
}

// --- accept loop (listener side) ---

func (p *Provider) acceptLoop() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-p.stopCh:
				return
			default:
				log.Printf("go-p2p: accept error: %v", err)
				return
			}
		}
		go p.handleConnection(conn)
	}
}

func (p *Provider) handleConnection(conn net.Conn) {
	defer conn.Close()
	s := NewSession(conn)
	peer := conn.RemoteAddr().String()

	msg, err := s.Read()
	if err != nil || msg.Type != "HELLO" {
		return
	}
	peerIndex := msg.Index

	// Send own HELLO before applying deletions so peer gets an accurate picture.
	ownIndex := p.index.AllEntries()
	if err := s.Send(HelloMsg(p.nodeID, ownIndex)); err != nil {
		return
	}
	p.applyRemoteDeletions(peerIndex, peer)

	// Phase 1: serve GET_FILE requests until SYNC_DONE.
	for {
		req, err := s.Read()
		if err != nil || req.Type == "SYNC_DONE" {
			break
		}
		if req.Type == "GET_FILE" {
			p.serveFile(s, req.Path)
		}
	}

	// Phase 2: accept FILE_DATA / FILE_DATA_STREAM pushes until ACK.
	for {
		req, err := s.Read()
		if err != nil || req.Type == "ACK" {
			break
		}
		if req.Entry == nil {
			continue
		}
		switch req.Type {
		case "FILE_DATA":
			content, _ := DecodeContent(req.Content)
			p.applyIncomingFile(*req.Entry, content, peer)
		case "FILE_DATA_STREAM":
			absPath := filepath.Join(p.watchDir, filepath.FromSlash(req.Entry.Path))
			// Decide before writing — if rejected, drain bytes to keep stream valid.
			local := p.index.Get(req.Entry.Path)
			accept := local == nil
			if !accept {
				switch Reconcile(*local, *req.Entry, p.nodeID) {
				case AcceptRemote:
					accept = true
				case Conflict:
					accept = p.resolveConflict(*local, *req.Entry, req.Entry.Path) == AcceptRemote
				}
			}
			if accept {
				if err := RecvStreamToDisk(s, req.Size, absPath); err != nil {
					log.Printf("go-p2p: recv stream %q failed: %v", req.Entry.Path, err)
					return // connection state unknown after partial read
				}
				p.index.ApplyRemote(*req.Entry, nil) //nolint:errcheck
				log.Printf("go-p2p: accepted %q from %s", req.Entry.Path, peer)
			} else {
				DrainStream(s, req.Size)
			}
		case "RENAME_FILE":
			if req.From != "" && req.To != "" && req.Entry != nil {
				p.applyRename(req.From, req.To, *req.Entry, peer)
			}
		}
	}

	p.index.Save() //nolint:errcheck
}

// --- sync loop (initiator side) ---

func (p *Provider) syncLoop() {
	interval := time.Duration(p.ctx.Interval) * time.Second
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			if !p.paused.Load() {
				p.RunSyncRound()
			}
		}
	}
}

func (p *Provider) RunSyncRound() {
	p.setState("syncing")
	p.setError("")

	var wg sync.WaitGroup
	for _, addr := range p.peers {
		wg.Add(1)
		go func(peerAddr string) {
			defer wg.Done()
			if err := p.syncWithPeer(peerAddr); err != nil {
				if !isConnectionRefused(err) {
					log.Printf("go-p2p: sync with %s failed: %v", peerAddr, err)
					p.setError(err.Error())
				}
			}
		}(addr)
	}
	wg.Wait()

	p.statusMu.Lock()
	p.lastSync = float64(time.Now().UnixNano()) / 1e9
	p.statusMu.Unlock()
	p.setState("idle")
}

func (p *Provider) syncWithPeer(addr string) error {
	host, port := parsePeer(addr, p.port)
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), 10*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	s := NewSession(conn)

	ownIndex := p.index.AllEntries()
	if err := s.Send(HelloMsg(p.nodeID, ownIndex)); err != nil {
		return err
	}

	msg, err := s.Read()
	if err != nil || msg.Type != "HELLO" {
		return fmt.Errorf("expected HELLO, got %v", err)
	}
	peerIndex := msg.Index

	p.applyRemoteDeletions(peerIndex, addr)
	p.applyLocalRenames(peerIndex)

	// Phase 1: pull files we need.
	for _, path := range p.computeNeeded(peerIndex) {
		if err := s.Send(GetFileMsg(path)); err != nil {
			return err
		}
		resp, err := s.Read()
		if err != nil {
			return err
		}
		if resp.Entry != nil {
			switch resp.Type {
			case "FILE_DATA":
				content, _ := DecodeContent(resp.Content)
				p.applyIncomingFile(*resp.Entry, content, addr)
				log.Printf("go-p2p: pulled %q from %s", path, addr)
			case "FILE_DATA_STREAM":
				absPath := filepath.Join(p.watchDir, filepath.FromSlash(resp.Entry.Path))
				if err := RecvStreamToDisk(s, resp.Size, absPath); err != nil {
					log.Printf("go-p2p: recv stream %q from %s failed: %v", path, addr, err)
				} else {
					p.applyIncomingFile(*resp.Entry, nil, addr)
					log.Printf("go-p2p: pulled %q from %s", path, addr)
				}
			}
		}
	}

	if err := s.Send(SyncDoneMsg()); err != nil {
		return err
	}

	// Phase 2: push files peer is missing or behind on.
	peerLiveByChecksum := make(map[string]string)
	for path, entry := range peerIndex {
		if !entry.Deleted && entry.Checksum != "" {
			peerLiveByChecksum[entry.Checksum] = path
		}
	}
	renames := make(map[string]string) // new path → old peer path
	localIndex := p.index.AllEntries()
	for path, entry := range localIndex {
		if entry.Deleted || entry.Checksum == "" {
			continue
		}
		old, ok := peerLiveByChecksum[entry.Checksum]
		if ok && old != path {
			if ourOld := p.index.Get(old); ourOld != nil && ourOld.Deleted {
				renames[path] = old
			}
		}
	}

	for path, entry := range localIndex {
		if entry.Deleted {
			continue
		}
		remote, ok := peerIndex[path]
		shouldPush := !ok || remote.Deleted || Reconcile(remote, entry, p.nodeID) == AcceptRemote
		if !shouldPush {
			continue
		}
		if oldPath, isRename := renames[path]; isRename {
			if err := s.Send(RenameFileMsg(oldPath, path, entry)); err != nil {
				log.Printf("go-p2p: send rename %q → %q to %s failed: %v", oldPath, path, addr, err)
			} else {
				log.Printf("go-p2p: sent rename %q → %q to %s", oldPath, path, addr)
			}
			continue
		}
		absPath := filepath.Join(p.watchDir, filepath.FromSlash(path))
		if err := SendFileData(s, path, absPath, entry); err != nil {
			log.Printf("go-p2p: send %q to %s failed: %v", path, addr, err)
			continue
		}
		log.Printf("go-p2p: pushed %q to %s", path, addr)
	}

	if err := s.Send(AckMsg(p.nodeID)); err != nil {
		return err
	}
	return p.index.Save()
}

// --- shared helpers ---

func (p *Provider) computeNeeded(remoteIndex map[string]FileEntry) []string {
	localIndex := p.index.AllEntries()
	localLiveByChecksum := make(map[string]string)
	for path, entry := range localIndex {
		if !entry.Deleted && entry.Checksum != "" {
			localLiveByChecksum[entry.Checksum] = path
		}
	}
	var needed []string
	for path, remote := range remoteIndex {
		if remote.Deleted {
			continue
		}
		local, ok := localIndex[path]
		if !ok || local.Deleted {
			if existing, has := localLiveByChecksum[remote.Checksum]; has && existing != path {
				continue // we have this content under a different name — it's a rename
			}
			if !ok {
				needed = append(needed, path)
			} else if Reconcile(local, remote, p.nodeID) == AcceptRemote {
				needed = append(needed, path)
			}
			continue
		}
		switch Reconcile(local, remote, p.nodeID) {
		case AcceptRemote:
			needed = append(needed, path)
		case Conflict:
			if p.resolveConflict(local, remote, path) == AcceptRemote {
				needed = append(needed, path)
			}
		case KeepLocal:
			if p.index.IsCorrupted(path) {
				needed = append(needed, path)
			}
		}
	}
	return needed
}

func (p *Provider) resolveConflict(local, remote FileEntry, path string) Action {
	if local.Checksum == remote.Checksum {
		return KeepLocal
	}
	log.Printf("go-p2p: conflict on %q for pair %q — strategy: %q", path, p.ctx.PairID, p.conflictStrategy)
	if p.conflictStrategy == "keep-both" {
		return ResolveKeepBoth(local, remote, p.nodeID, p.watchDir)
	}
	return ResolveLastWriteWins(local, remote)
}

func (p *Provider) applyRemoteDeletions(remoteIndex map[string]FileEntry, label string) {
	if !p.syncDeletes {
		return
	}
	remoteLiveByChecksum := make(map[string]string)
	for path, entry := range remoteIndex {
		if !entry.Deleted && entry.Checksum != "" {
			remoteLiveByChecksum[entry.Checksum] = path
		}
	}
	for path, remote := range remoteIndex {
		if !remote.Deleted {
			continue
		}
		local := p.index.Get(path)
		if local == nil || local.Deleted {
			p.pendingDeletes.Delete(path)
			continue
		}
		if newPath, ok := remoteLiveByChecksum[local.Checksum]; ok && newPath != path {
			p.pendingDeletes.Delete(path)
			continue // rename incoming — skip deletion
		}
		if Reconcile(*local, remote, p.nodeID) == AcceptRemote {
			if _, pending := p.pendingDeletes.Load(path); !pending {
				// Defer by one round: a move whose create event hasn't landed yet
				// would look identical to a genuine delete at this moment.
				p.pendingDeletes.Store(path, struct{}{})
				log.Printf("go-p2p: deferring deletion of %q — confirming next round", path)
				continue
			}
			p.pendingDeletes.Delete(path)
			if err := p.index.ApplyRemote(remote, nil); err != nil {
				log.Printf("go-p2p: apply deletion of %q failed: %v", path, err)
				continue
			}
			log.Printf("go-p2p: applied remote deletion of %q from %s", path, label)
		}
	}
}

func (p *Provider) applyLocalRenames(remoteIndex map[string]FileEntry) {
	remoteLiveByChecksum := make(map[string]string)
	for path, entry := range remoteIndex {
		if !entry.Deleted && entry.Checksum != "" {
			remoteLiveByChecksum[entry.Checksum] = path
		}
	}
	remoteTombstoned := make(map[string]bool)
	for path, entry := range remoteIndex {
		if entry.Deleted {
			remoteTombstoned[path] = true
		}
	}
	for path, local := range p.index.AllEntries() {
		if local.Deleted || local.Checksum == "" {
			continue
		}
		if !remoteTombstoned[path] {
			continue
		}
		newPath, ok := remoteLiveByChecksum[local.Checksum]
		if !ok || newPath == path {
			continue
		}
		p.applyRename(path, newPath, remoteIndex[newPath], "remote")
	}
}

func (p *Provider) applyRename(fromPath, toPath string, newEntry FileEntry, label string) {
	absFrom := filepath.Join(p.watchDir, filepath.FromSlash(fromPath))
	absTo   := filepath.Join(p.watchDir, filepath.FromSlash(toPath))

	fromEntry := p.index.Get(fromPath)
	if fromEntry == nil || fromEntry.Checksum != newEntry.Checksum {
		log.Printf("go-p2p: rename %q → %q: source missing or checksum mismatch, will re-sync next round", fromPath, toPath)
		return
	}
	if _, err := os.Stat(absFrom); err != nil {
		log.Printf("go-p2p: rename %q → %q: source not on disk, will re-sync next round", fromPath, toPath)
		return
	}
	if err := os.MkdirAll(filepath.Dir(absTo), 0o755); err != nil {
		log.Printf("go-p2p: rename %q → %q: mkdir failed: %v", fromPath, toPath, err)
		return
	}
	if err := os.Rename(absFrom, absTo); err != nil {
		log.Printf("go-p2p: rename %q → %q: os.Rename failed: %v", fromPath, toPath, err)
		return
	}
	clk := fromEntry.GetClock(p.nodeID)
	clk.Increment()
	tombstone := FileEntry{Path: fromPath, VectorClockData: clk.ToDict(), Deleted: true}
	p.index.ApplyRemote(tombstone, nil) //nolint:errcheck
	p.index.ApplyRemote(newEntry, nil)  //nolint:errcheck

	// Prune empty parent directories left behind by the rename.
	parent := filepath.Dir(absFrom)
	for parent != p.watchDir {
		entries, err := os.ReadDir(parent)
		if err != nil || len(entries) > 0 {
			break
		}
		os.Remove(parent) //nolint:errcheck
		parent = filepath.Dir(parent)
	}
	log.Printf("go-p2p: renamed %q → %q from %s", fromPath, toPath, label)
}

func (p *Provider) applyIncomingFile(entry FileEntry, content []byte, label string) {
	local := p.index.Get(entry.Path)
	var action Action
	if local == nil {
		action = AcceptRemote
	} else {
		action = Reconcile(*local, entry, p.nodeID)
	}
	if action == AcceptRemote {
		if err := p.index.ApplyRemote(entry, content); err != nil {
			log.Printf("go-p2p: apply %q from %s failed: %v", entry.Path, label, err)
		} else {
			log.Printf("go-p2p: accepted %q from %s", entry.Path, label)
		}
		return
	}
	if action == Conflict && local != nil {
		if p.resolveConflict(*local, entry, entry.Path) == AcceptRemote {
			p.index.ApplyRemote(entry, content) //nolint:errcheck
		}
	}
}

func (p *Provider) serveFile(s *Session, path string) {
	entry := p.index.Get(path)
	if entry == nil || entry.Deleted {
		return
	}
	absPath := filepath.Join(p.watchDir, filepath.FromSlash(path))
	if err := SendFileData(s, path, absPath, *entry); err != nil {
		log.Printf("go-p2p: serve %q failed: %v", path, err)
	}
}

// --- file watcher handler ---

func (p *Provider) onWatchEvent(op, absPath string) {
	rel := toRelPosix(p.watchDir, absPath)
	if rel == "" {
		return
	}

	switch op {
	case "remove", "rename":
		changed := p.index.MarkDeleted(rel)
		if !changed {
			changed = p.index.MarkDirDeleted(rel)
		}
		if changed {
			p.index.Save() //nolint:errcheck
		}

	case "create", "write":
		info, err := os.Stat(absPath)
		if err != nil {
			return
		}
		if info.IsDir() {
			// Watch all subdirs and scan files in the new directory.
			filepath.WalkDir(absPath, func(sub string, d fs.DirEntry, err error) error { //nolint:errcheck
				if err != nil {
					return nil
				}
				if d.IsDir() && d.Name() != MetadataDir {
					p.watcher.Add(sub)
				}
				return nil
			})
			p.index.ScanDir(absPath)
		} else {
			p.index.ScanOne(rel)
		}
		p.index.Save() //nolint:errcheck
	}
}

// --- verify loop ---

func (p *Provider) verifyLoop(intervalSecs int) {
	interval := time.Duration(intervalSecs) * time.Second
	vs := cfgFloat(p.ctx.ProviderConfig, "verify_sleep", 0.1)
	verifySleep := time.Duration(vs * float64(time.Second))

	for {
		select {
		case <-p.stopCh:
			return
		case <-time.After(interval):
		}
		if p.paused.Load() {
			continue
		}

		snapshot := p.index.AllEntries()
		for path, entry := range snapshot {
			select {
			case <-p.stopCh:
				return
			default:
			}
			if entry.Deleted {
				continue
			}
			if p.index.VerifyOne(path) {
				p.index.MarkCorrupted(path)
				log.Printf("go-p2p: corruption detected in %q for pair %q", path, p.ctx.PairID)
			}
			time.Sleep(verifySleep)
		}
	}
}

// --- status helpers ---

func (p *Provider) setState(s string) {
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	p.state = s
}

func (p *Provider) setError(e string) {
	p.statusMu.Lock()
	defer p.statusMu.Unlock()
	p.lastErr = e
}

// --- utility ---

func portForPair(pairID string) int {
	h := sha256.Sum256([]byte(pairID))
	n := new(big.Int).SetBytes(h[:])
	mod := new(big.Int).SetInt64(35536)
	return 30000 + int(new(big.Int).Mod(n, mod).Int64())
}

func parsePeer(addr string, defaultPort int) (string, int) {
	idx := strings.LastIndex(addr, ":")
	if idx >= 0 {
		host := addr[:idx]
		port := 0
		fmt.Sscanf(addr[idx+1:], "%d", &port)
		if port > 0 {
			return host, port
		}
	}
	return addr, defaultPort
}

func isConnectionRefused(err error) bool {
	return err != nil && strings.Contains(err.Error(), "connection refused")
}

func toRelPosix(base, absPath string) string {
	rel, err := filepath.Rel(base, absPath)
	if err != nil {
		return ""
	}
	return filepath.ToSlash(rel)
}

func randomID() string {
	b := make([]byte, 4)
	rand.Read(b) //nolint:errcheck
	return fmt.Sprintf("%x", b)
}

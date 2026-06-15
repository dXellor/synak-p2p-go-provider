package main

import (
	"log"
	"os"
	"path/filepath"
)

type Action int

const (
	KeepLocal    Action = iota
	AcceptRemote Action = iota
	Conflict     Action = iota
)

// Reconcile matches Python's reconcile() function exactly.
func Reconcile(local, remote FileEntry, nodeID string) Action {
	lc := local.GetClock(nodeID)
	rc := remote.GetClock(nodeID)
	if lc.Equals(rc) {
		return KeepLocal
	}
	if rc.HappensBefore(lc) {
		return KeepLocal
	}
	if lc.HappensBefore(rc) {
		return AcceptRemote
	}
	return Conflict
}

func ResolveLastWriteWins(local, remote FileEntry) Action {
	if remote.ModifiedTime > local.ModifiedTime {
		return AcceptRemote
	}
	return KeepLocal
}

// ResolveKeepBoth renames the local copy to <path>.syncd-conflict.<nodeID> and
// returns AcceptRemote so the caller writes the remote version at the original path.
func ResolveKeepBoth(local, remote FileEntry, nodeID, watchDir string) Action {
	src := filepath.Join(watchDir, filepath.FromSlash(local.Path))
	dst := src + ".syncd-conflict." + nodeID
	if _, err := os.Stat(src); err == nil {
		if _, err := os.Stat(dst); os.IsNotExist(err) {
			if err := os.Rename(src, dst); err != nil {
				log.Printf("keep-both rename failed for %q: %v", local.Path, err)
			}
		}
	}
	return AcceptRemote
}

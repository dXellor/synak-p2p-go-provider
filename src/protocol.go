package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
)

const StreamThreshold int64 = 64 * 1024 * 1024 // bytes; files at or above this use binary streaming

// Message covers all wire protocol message types. Zero-value fields with omitempty are omitted.
// Content is never omitempty — empty string must round-trip correctly for zero-byte files.
type Message struct {
	Type    string               `json:"type"`
	NodeID  string               `json:"node_id,omitempty"`
	Index   map[string]FileEntry `json:"index,omitempty"`
	Path    string               `json:"path,omitempty"`
	Content string               `json:"content"`          // base64-encoded bytes (FILE_DATA)
	Size    int64                `json:"size,omitempty"`   // raw byte count (FILE_DATA_STREAM)
	Entry   *FileEntry           `json:"entry,omitempty"`
}

// Session wraps a TCP connection with buffered read/write.
type Session struct {
	r    *bufio.Reader
	w    *bufio.Writer
	conn net.Conn
}

func NewSession(conn net.Conn) *Session {
	return &Session{
		r:    bufio.NewReaderSize(conn, 1<<20),
		w:    bufio.NewWriterSize(conn, 1<<20),
		conn: conn,
	}
}

func (s *Session) Send(msg Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := s.w.Write(data); err != nil {
		return err
	}
	return s.w.Flush()
}

func (s *Session) Read() (*Message, error) {
	line, err := s.r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	var msg Message
	if err := json.Unmarshal(line, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

func (s *Session) Close() {
	s.conn.Close()
}

// --- message constructors ---

func HelloMsg(nodeID string, index map[string]FileEntry) Message {
	return Message{Type: "HELLO", NodeID: nodeID, Index: index}
}

func GetFileMsg(path string) Message {
	return Message{Type: "GET_FILE", Path: path}
}

func FileDataMsg(path string, content []byte, entry FileEntry) Message {
	return Message{
		Type:    "FILE_DATA",
		Path:    path,
		Content: base64.StdEncoding.EncodeToString(content),
		Entry:   &entry,
	}
}

func FileDataStreamMsg(path string, size int64, entry FileEntry) Message {
	return Message{Type: "FILE_DATA_STREAM", Path: path, Size: size, Entry: &entry}
}

func SyncDoneMsg() Message          { return Message{Type: "SYNC_DONE"} }
func AckMsg(nodeID string) Message  { return Message{Type: "ACK", NodeID: nodeID} }

func DecodeContent(encoded string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(encoded)
}

// SendFileData sends FILE_DATA (base64) for small files, FILE_DATA_STREAM (raw bytes) for large ones.
func SendFileData(s *Session, path, absPath string, entry FileEntry) error {
	info, err := os.Stat(absPath)
	if err != nil {
		return err
	}
	size := info.Size()
	if size < StreamThreshold {
		content, err := os.ReadFile(absPath)
		if err != nil {
			return err
		}
		return s.Send(FileDataMsg(path, content, entry))
	}
	// Stream: write header JSON line then raw file bytes.
	hdr, err := json.Marshal(FileDataStreamMsg(path, size, entry))
	if err != nil {
		return err
	}
	if _, err := s.w.Write(append(hdr, '\n')); err != nil {
		return err
	}
	f, err := os.Open(absPath)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 1<<20)
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if _, werr := s.w.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	return s.w.Flush()
}

// RecvStreamToDisk reads exactly size raw bytes from the session and writes them to absPath.
// Writes to a .synak.tmp temp file first (matched by the built-in *.tmp watcher exclusion),
// then atomically renames so no partial-write events reach the file watcher.
func RecvStreamToDisk(s *Session, size int64, absPath string) error {
	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		return err
	}
	tmp := absPath + ".synak.tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	buf := make([]byte, 1<<20)
	remaining := size
	for remaining > 0 {
		n := int64(len(buf))
		if n > remaining {
			n = remaining
		}
		if _, err := io.ReadFull(s.r, buf[:n]); err != nil {
			f.Close()
			os.Remove(tmp) //nolint:errcheck
			return err
		}
		if _, err := f.Write(buf[:n]); err != nil {
			f.Close()
			os.Remove(tmp) //nolint:errcheck
			return err
		}
		remaining -= n
	}
	f.Close()
	return os.Rename(tmp, absPath)
}

// DrainStream discards size bytes from the session. Used when a FILE_DATA_STREAM push is rejected
// by reconciliation — bytes must still be consumed to keep the protocol stream in a valid state.
func DrainStream(s *Session, size int64) {
	buf := make([]byte, 1<<20)
	remaining := size
	for remaining > 0 {
		n := int64(len(buf))
		if n > remaining {
			n = remaining
		}
		io.ReadFull(s.r, buf[:n]) //nolint:errcheck
		remaining -= n
	}
}

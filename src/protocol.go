package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"net"
)

// Message covers all wire protocol message types. Zero-value fields with omitempty are omitted.
type Message struct {
	Type    string             `json:"type"`
	NodeID  string             `json:"node_id,omitempty"`
	Index   map[string]FileEntry `json:"index,omitempty"`
	Path    string             `json:"path,omitempty"`
	Content string             `json:"content,omitempty"` // base64-encoded file bytes
	Entry   *FileEntry         `json:"entry,omitempty"`
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

func SyncDoneMsg() Message { return Message{Type: "SYNC_DONE"} }
func AckMsg(nodeID string) Message { return Message{Type: "ACK", NodeID: nodeID} }

func DecodeContent(encoded string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(encoded)
}

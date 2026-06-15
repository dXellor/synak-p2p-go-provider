package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

type ipcCmd struct {
	Cmd     string          `json:"cmd"`
	Context json.RawMessage `json:"context,omitempty"`
}

type ipcResp struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	Data  any    `json:"data,omitempty"`
}

func ok(enc *json.Encoder) {
	enc.Encode(ipcResp{OK: true}) //nolint:errcheck
}

func fail(enc *json.Encoder, msg string) {
	enc.Encode(ipcResp{OK: false, Error: msg}) //nolint:errcheck
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	// Increase scanner buffer for large index payloads in the start command.
	scanner.Buffer(make([]byte, 4<<20), 4<<20)
	enc := json.NewEncoder(os.Stdout)

	var p *Provider

	for scanner.Scan() {
		var cmd ipcCmd
		if err := json.Unmarshal(scanner.Bytes(), &cmd); err != nil {
			fail(enc, fmt.Sprintf("parse command: %v", err))
			continue
		}

		switch cmd.Cmd {
		case "start":
			var ctx SyncContext
			if err := json.Unmarshal(cmd.Context, &ctx); err != nil {
				fail(enc, fmt.Sprintf("parse context: %v", err))
				continue
			}
			var err error
			p, err = NewProvider(ctx)
			if err != nil {
				fail(enc, err.Error())
				continue
			}
			if err := p.Start(); err != nil {
				fail(enc, err.Error())
				continue
			}
			ok(enc)

		case "stop":
			if p != nil {
				p.Stop()
			}
			ok(enc)
			return // process exits after stop

		case "pause":
			if p != nil {
				p.Pause()
			}
			ok(enc)

		case "resume":
			if p != nil {
				p.Resume()
			}
			ok(enc)

		case "trigger":
			if p != nil {
				go p.RunSyncRound()
			}
			ok(enc)

		case "status":
			if p == nil {
				enc.Encode(ipcResp{OK: true, Data: ProviderStatus{State: "stopped"}}) //nolint:errcheck
				continue
			}
			enc.Encode(ipcResp{OK: true, Data: p.Status()}) //nolint:errcheck

		default:
			fail(enc, fmt.Sprintf("unknown command: %q", cmd.Cmd))
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "stdin read error: %v\n", err)
	}
}

// Package terminal exposes a cc-connect platform that accepts user input from
// a local terminal (via a per-project unix socket) and delivers assistant
// output back the same way. It mirrors the design of other platforms (line,
// telegram, ...) so that a terminal session is a first-class chat alongside
// Feishu / WeChat / etc.
//
// Wire protocol over the unix socket — one JSON object per line:
//
//   client → server
//     {"type":"attach"}                       // optional handshake, kept for future fields
//     {"type":"input","content":"..."}        // user message; passed to MessageHandler
//     {"type":"detach"}                       // graceful disconnect
//
//   server → client
//     {"type":"attached","session_key":"..."} // ack after first input, includes session key
//     {"type":"assistant","content":"..."}    // assistant text reply
//     {"type":"error","message":"..."}        // server-side problem
//
// Tool-call / thinking events are intentionally omitted in the MVP; only
// final assistant text reaches the client. A follower-mode extension can
// stream richer events later.
package terminal

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"

	"github.com/chenhg5/cc-connect/core"
)

func init() {
	core.RegisterPlatform("terminal", New)
}

// replyContext identifies which attached client should receive an outbound
// reply. Stored on every inbound core.Message and handed back to Reply/Send.
type replyContext struct {
	connID string
}

type clientConn struct {
	id      string
	conn    net.Conn
	encoder *json.Encoder
}

// Platform is the cc-connect Platform impl for terminal sessions. One
// instance per project — the socket path includes the project name so
// multiple bots can coexist without clashing.
type Platform struct {
	project    string
	socketPath string

	listener net.Listener
	handler  core.MessageHandler

	mu    sync.Mutex
	conns map[string]*clientConn
}

// New is the factory registered with core.RegisterPlatform. Reads the
// per-project options map; `socket_path` may override the default
// `~/.cc-connect/terminal-<project>.sock`.
func New(opts map[string]any) (core.Platform, error) {
	project, _ := opts["cc_project"].(string)
	if project == "" {
		project = "default"
	}
	socketPath, _ := opts["socket_path"].(string)
	if socketPath == "" {
		dataDir, _ := opts["cc_data_dir"].(string)
		if dataDir == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return nil, fmt.Errorf("terminal: locate home: %w", err)
			}
			dataDir = filepath.Join(home, ".cc-connect")
		}
		socketPath = filepath.Join(dataDir, fmt.Sprintf("terminal-%s.sock", project))
	}
	return &Platform{
		project:    project,
		socketPath: socketPath,
		conns:      make(map[string]*clientConn),
	}, nil
}

func (p *Platform) Name() string { return "terminal" }

func (p *Platform) Start(handler core.MessageHandler) error {
	p.handler = handler

	// Stale socket file from a previous crash would prevent re-bind.
	_ = os.Remove(p.socketPath)
	if err := os.MkdirAll(filepath.Dir(p.socketPath), 0o755); err != nil {
		return fmt.Errorf("terminal: mkdir socket dir: %w", err)
	}
	l, err := net.Listen("unix", p.socketPath)
	if err != nil {
		return fmt.Errorf("terminal: listen %s: %w", p.socketPath, err)
	}
	// Only the owning user should be able to connect.
	if err := os.Chmod(p.socketPath, 0o600); err != nil {
		slog.Warn("terminal: chmod socket", "path", p.socketPath, "error", err)
	}
	p.listener = l
	slog.Info("terminal: listening", "project", p.project, "socket", p.socketPath)
	go p.acceptLoop()
	return nil
}

func (p *Platform) acceptLoop() {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			// Listener closed by Stop() — normal shutdown path.
			return
		}
		go p.handleConn(conn)
	}
}

func (p *Platform) handleConn(c net.Conn) {
	id := generateConnID()
	cc := &clientConn{id: id, conn: c, encoder: json.NewEncoder(c)}

	p.mu.Lock()
	p.conns[id] = cc
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		delete(p.conns, id)
		p.mu.Unlock()
		_ = c.Close()
	}()

	decoder := json.NewDecoder(c)
	for {
		var msg map[string]any
		if err := decoder.Decode(&msg); err != nil {
			return
		}
		switch msg["type"] {
		case "attach":
			// Handshake — currently nothing to negotiate. Reserved for future
			// fields (e.g. requesting tool-event streaming).
			_ = cc.encoder.Encode(map[string]string{
				"type":        "attached",
				"project":     p.project,
				"session_key": p.sessionKey(id),
			})

		case "input":
			content, _ := msg["content"].(string)
			if content == "" {
				continue
			}
			rctx := replyContext{connID: id}
			p.handler(p, &core.Message{
				SessionKey: p.sessionKey(id),
				Platform:   "terminal",
				MessageID:  generateConnID(),
				UserID:     "terminal-user",
				UserName:   "terminal",
				ChatName:   fmt.Sprintf("terminal://%s", p.project),
				Content:    content,
				ReplyCtx:   rctx,
			})

		case "detach":
			return

		default:
			_ = cc.encoder.Encode(map[string]string{
				"type":    "error",
				"message": fmt.Sprintf("unknown message type %q", msg["type"]),
			})
		}
	}
}

func (p *Platform) sessionKey(connID string) string {
	return fmt.Sprintf("terminal:%s:%s", p.project, connID)
}

func (p *Platform) Reply(ctx context.Context, rctxAny any, content string) error {
	if content == "" {
		return nil
	}
	rctx, ok := rctxAny.(replyContext)
	if !ok {
		return fmt.Errorf("terminal: invalid reply context type %T", rctxAny)
	}
	p.mu.Lock()
	cc := p.conns[rctx.connID]
	p.mu.Unlock()
	if cc == nil {
		// Client disconnected before assistant finished — drop silently; the
		// jsonl transcript still persists for later inspection.
		return nil
	}
	return cc.encoder.Encode(map[string]string{
		"type":    "assistant",
		"content": content,
	})
}

func (p *Platform) Send(ctx context.Context, rctx any, content string) error {
	return p.Reply(ctx, rctx, content)
}

func (p *Platform) Stop() error {
	if p.listener != nil {
		_ = p.listener.Close()
	}
	p.mu.Lock()
	for _, cc := range p.conns {
		_ = cc.conn.Close()
	}
	p.conns = map[string]*clientConn{}
	p.mu.Unlock()
	_ = os.Remove(p.socketPath)
	return nil
}

func generateConnID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

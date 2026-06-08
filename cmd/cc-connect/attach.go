package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
)

// runAttach implements `cc-connect attach <project>` — a thin client that
// connects to the project's terminal-platform unix socket, forwards stdin
// lines as user input, and prints assistant replies to stdout.
//
// The peer end is platform/terminal/terminal.go. Wire protocol is one JSON
// object per line in both directions.
func runAttach(args []string) {
	fs := flag.NewFlagSet("attach", flag.ContinueOnError)
	var (
		socketPath  string
		dataDir     string
		showPrompt  bool
		joinMode    bool
	)
	fs.StringVar(&socketPath, "socket", "", "explicit unix socket path (overrides default)")
	fs.StringVar(&dataDir, "data-dir", "", "cc-connect data dir (default: ~/.cc-connect)")
	fs.BoolVar(&showPrompt, "prompt", true, "show '> ' input prompt (disable for piped stdin)")
	fs.BoolVar(&joinMode, "join", false, "join existing session as mirror instead of creating new")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `Usage: cc-connect attach <project> [flags]

Connect a local terminal to a cc-connect bot project that has the 'terminal'
platform enabled. Stdin lines are sent as user messages; assistant replies
are printed to stdout. Ctrl+D / Ctrl+C to detach.

Flags:
`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	if fs.NArg() < 1 && socketPath == "" {
		fs.Usage()
		os.Exit(2)
	}

	if socketPath == "" {
		project := fs.Arg(0)
		if dataDir == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				fmt.Fprintf(os.Stderr, "cannot locate home: %v\n", err)
				os.Exit(1)
			}
			dataDir = filepath.Join(home, ".cc-connect")
		}
		socketPath = filepath.Join(dataDir, fmt.Sprintf("terminal-%s.sock", project))
	}

	if _, err := os.Stat(socketPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: socket %s does not exist.\n", socketPath)
		fmt.Fprintf(os.Stderr, "Is cc-connect running with the terminal platform enabled for this project?\n")
		os.Exit(1)
	}

	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to connect %s: %v\n", socketPath, err)
		os.Exit(1)
	}
	defer conn.Close()

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	// Handshake: "attach" creates a new session, "join" mirrors an existing one.
	handshakeType := "attach"
	if joinMode {
		handshakeType = "join"
	}
	if err := encoder.Encode(map[string]string{"type": handshakeType}); err != nil {
		fmt.Fprintf(os.Stderr, "Error: handshake: %v\n", err)
		os.Exit(1)
	}

	var (
		wg   sync.WaitGroup
		done = make(chan struct{})
		once sync.Once
	)
	shutdown := func() { once.Do(func() { close(done) }) }

	// Reader goroutine: socket → stdout
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			var msg map[string]any
			if err := decoder.Decode(&msg); err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
					fmt.Fprintf(os.Stderr, "\n[server disconnected: %v]\n", err)
				} else {
					fmt.Fprintln(os.Stderr, "\n[server disconnected]")
				}
				shutdown()
				return
			}
			switch msg["type"] {
			case "attached":
				if sk, ok := msg["session_key"].(string); ok {
					fmt.Fprintf(os.Stderr, "\033[2m[attached, session=%s]\033[0m\n", sk)
				}
			case "assistant":
				if content, ok := msg["content"].(string); ok {
					// Clear the prompt line, print, restore prompt
					if showPrompt && isTerminal(os.Stdin) {
						fmt.Print("\r\033[K")
					}
					fmt.Println(content)
					if showPrompt && isTerminal(os.Stdin) {
						fmt.Print("> ")
					}
				}
			case "error":
				if errMsg, ok := msg["message"].(string); ok {
					fmt.Fprintf(os.Stderr, "\033[31m[server error: %s]\033[0m\n", errMsg)
				}
			}
		}
	}()

	// Signal goroutine: Ctrl+C → graceful detach
	wg.Add(1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		defer wg.Done()
		select {
		case <-sigCh:
			fmt.Fprintln(os.Stderr, "\n[detaching]")
			_ = encoder.Encode(map[string]string{"type": "detach"})
			shutdown()
		case <-done:
		}
		_ = conn.Close()
	}()

	// Main loop: stdin → socket
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	if showPrompt && isTerminal(os.Stdin) {
		fmt.Print("> ")
	}
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if line == "" {
			if showPrompt && isTerminal(os.Stdin) {
				fmt.Print("> ")
			}
			continue
		}
		if err := encoder.Encode(map[string]string{"type": "input", "content": line}); err != nil {
			fmt.Fprintf(os.Stderr, "[send failed: %v]\n", err)
			break
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "[stdin read error: %v]\n", err)
	}

	// stdin closed (Ctrl+D) — tell server we're leaving, then drain.
	_ = encoder.Encode(map[string]string{"type": "detach"})
	shutdown()
	_ = conn.Close()
	wg.Wait()
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

package core

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// platformTarget represents a platform endpoint connected to a shared session.
type platformTarget struct {
	platform   Platform
	replyCtx   any
	sessionKey string
	joinedAt   time.Time
}

// mirrorState holds the mirror targets for an interactiveState.
// Embedded in interactiveState; all methods are concurrency-safe.
type mirrorState struct {
	mu      sync.RWMutex
	targets []platformTarget
}

func (ms *mirrorState) add(t platformTarget) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for _, existing := range ms.targets {
		if existing.sessionKey == t.sessionKey {
			return
		}
	}
	ms.targets = append(ms.targets, t)
}

func (ms *mirrorState) remove(sessionKey string) bool {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	for i, t := range ms.targets {
		if t.sessionKey == sessionKey {
			ms.targets = append(ms.targets[:i], ms.targets[i+1:]...)
			return true
		}
	}
	return false
}

func (ms *mirrorState) snapshot() []platformTarget {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if len(ms.targets) == 0 {
		return nil
	}
	out := make([]platformTarget, len(ms.targets))
	copy(out, ms.targets)
	return out
}

func (ms *mirrorState) count() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.targets)
}

func (ms *mirrorState) has(sessionKey string) bool {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for _, t := range ms.targets {
		if t.sessionKey == sessionKey {
			return true
		}
	}
	return false
}

// rotatePrimary swaps the current primary with a mirror target.
// Called when a message arrives from a mirror platform: the sender becomes
// primary (gets full rendering), and the old primary becomes a mirror
// (gets plain text fan-out).
func (state *interactiveState) rotatePrimary(newPlatform Platform, newReplyCtx any, newSessionKey string) {
	state.mirrors.mu.Lock()
	defer state.mirrors.mu.Unlock()

	idx := -1
	for i, t := range state.mirrors.targets {
		if t.sessionKey == newSessionKey {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}

	if state.primarySessionKey != "" {
		state.mirrors.targets = append(state.mirrors.targets, platformTarget{
			platform:   state.platform,
			replyCtx:   state.replyCtx,
			sessionKey: state.primarySessionKey,
			joinedAt:   time.Now(),
		})
	}
	state.mirrors.targets = append(state.mirrors.targets[:idx], state.mirrors.targets[idx+1:]...)

	state.platform = newPlatform
	state.replyCtx = newReplyCtx
	state.primarySessionKey = newSessionKey
}

// mirrorSend sends plain text to all mirror targets. Errors on individual
// targets are logged but do not affect others.
func (e *Engine) mirrorSend(state *interactiveState, content string) {
	mirrors := state.mirrors.snapshot()
	if len(mirrors) == 0 {
		return
	}
	for _, m := range mirrors {
		if err := m.platform.Send(e.ctx, m.replyCtx, content); err != nil {
			slog.Debug("mirror send failed", "platform", m.platform.Name(), "session_key", m.sessionKey, "error", err)
		}
	}
}

// mirrorReply sends a reply to all mirror targets.
func (e *Engine) mirrorReply(state *interactiveState, content string) {
	mirrors := state.mirrors.snapshot()
	if len(mirrors) == 0 {
		return
	}
	for _, m := range mirrors {
		if err := m.platform.Reply(e.ctx, m.replyCtx, content); err != nil {
			slog.Debug("mirror reply failed", "platform", m.platform.Name(), "session_key", m.sessionKey, "error", err)
		}
	}
}

// findActiveStateForProject finds the active interactiveState for a project.
// Returns the state and its session key, or nil/"" if none found.
func (e *Engine) findActiveStateForProject() (string, *interactiveState) {
	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()

	for key, state := range e.interactiveStates {
		if state != nil && state.agentSession != nil && state.agentSession.Alive() {
			return key, state
		}
	}
	return "", nil
}

// JoinSession adds a platform as a mirror to the active session.
func (e *Engine) JoinSession(joinerPlatform Platform, joinerReplyCtx any, joinerSessionKey string) error {
	primaryKey, state := e.findActiveStateForProject()
	if state == nil {
		return fmt.Errorf("no active session to join")
	}
	if primaryKey == joinerSessionKey {
		return fmt.Errorf("cannot join own session")
	}
	if state.mirrors.has(joinerSessionKey) {
		return fmt.Errorf("already joined")
	}

	state.mirrors.add(platformTarget{
		platform:   joinerPlatform,
		replyCtx:   joinerReplyCtx,
		sessionKey: joinerSessionKey,
		joinedAt:   time.Now(),
	})

	e.interactiveMu.Lock()
	e.interactiveStates[joinerSessionKey] = state
	e.interactiveMu.Unlock()

	slog.Info("session joined",
		"primary_key", primaryKey,
		"joiner_key", joinerSessionKey,
		"joiner_platform", joinerPlatform.Name(),
		"total_mirrors", state.mirrors.count(),
	)
	return nil
}

// DetachFromSession removes a platform mirror from its session.
func (e *Engine) DetachFromSession(sessionKey string) error {
	e.interactiveMu.Lock()
	state, ok := e.interactiveStates[sessionKey]
	if !ok || state == nil {
		e.interactiveMu.Unlock()
		return fmt.Errorf("no session found for key %s", sessionKey)
	}

	removed := state.mirrors.remove(sessionKey)
	if removed {
		delete(e.interactiveStates, sessionKey)
	}
	e.interactiveMu.Unlock()

	if !removed {
		if state.primarySessionKey == sessionKey {
			return fmt.Errorf("cannot detach primary session; use /stop instead")
		}
		return fmt.Errorf("session key %s is not a mirror", sessionKey)
	}

	slog.Info("session detached", "session_key", sessionKey, "remaining_mirrors", state.mirrors.count())

	mirrors := state.mirrors.snapshot()
	state.mu.Lock()
	p := state.platform
	replyCtx := state.replyCtx
	state.mu.Unlock()

	notice := fmt.Sprintf("🔗 Mirror detached (%d connected)", 1+len(mirrors))
	if p != nil {
		e.send(p, replyCtx, notice)
	}
	for _, m := range mirrors {
		e.send(m.platform, m.replyCtx, notice)
	}
	return nil
}

// ListMirrors returns a description of all connected platforms for a session.
func (e *Engine) ListMirrors(sessionKey string) string {
	e.interactiveMu.Lock()
	state, ok := e.interactiveStates[sessionKey]
	e.interactiveMu.Unlock()
	if !ok || state == nil {
		return "No active session"
	}

	state.mu.Lock()
	primaryPlatform := state.platform
	primaryKey := state.primarySessionKey
	state.mu.Unlock()

	mirrors := state.mirrors.snapshot()
	if len(mirrors) == 0 && primaryPlatform != nil {
		return fmt.Sprintf("Only %s connected (no mirrors)", primaryPlatform.Name())
	}

	var b strings.Builder
	b.WriteString("Connected platforms:\n")
	if primaryPlatform != nil {
		fmt.Fprintf(&b, "  • %s (primary) — %s\n", primaryPlatform.Name(), primaryKey)
	}
	for _, m := range mirrors {
		fmt.Fprintf(&b, "  • %s (mirror, joined %s) — %s\n",
			m.platform.Name(), m.joinedAt.Format("15:04"), m.sessionKey)
	}
	return b.String()
}

// cmdJoin handles the /join command.
func (e *Engine) cmdJoin(p Platform, msg *Message) {
	sessionKey := msg.SessionKey
	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)

	if err := e.JoinSession(p, msg.ReplyCtx, interactiveKey); err != nil {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf("Join failed: %s", err))
		return
	}

	e.reply(p, msg.ReplyCtx, "Joined session. Output will be mirrored to this platform.")

	// Notify the primary
	e.interactiveMu.Lock()
	state := e.interactiveStates[interactiveKey]
	e.interactiveMu.Unlock()
	if state != nil {
		state.mu.Lock()
		pp := state.platform
		prc := state.replyCtx
		state.mu.Unlock()
		if pp != nil {
			e.send(pp, prc, fmt.Sprintf("🔗 %s joined as mirror (%d connected)", p.Name(), 1+state.mirrors.count()))
		}
	}
}

// cmdDetach handles the /detach command.
func (e *Engine) cmdDetach(p Platform, msg *Message) {
	sessionKey := msg.SessionKey
	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)

	if err := e.DetachFromSession(interactiveKey); err != nil {
		e.reply(p, msg.ReplyCtx, fmt.Sprintf("Detach failed: %s", err))
		return
	}
	e.reply(p, msg.ReplyCtx, "Detached from session.")
}

// cmdMirrors handles the /mirrors command.
func (e *Engine) cmdMirrors(p Platform, msg *Message) {
	sessionKey := msg.SessionKey
	interactiveKey := e.interactiveKeyForSessionKey(sessionKey)
	e.reply(p, msg.ReplyCtx, e.ListMirrors(interactiveKey))
}

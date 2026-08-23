package ads

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

// reconnect_lifecycle_test.go — how a reconnect ends, and the rule that it can
// never end in a state nobody can get out of.
//
// The consumer contract this protects: benthos-umh never calls Reconnect and
// never inspects the FSM. It polls IsClosed(), and on true it drops the session
// and builds a new one (read.go:109/154). So a session that is neither connected
// nor closed is invisible to it — no data flows, nothing rebuilds, forever.
// Every exit from Reconnect must therefore leave the session Connected or Closed.

// fastBackoff keeps a reconnect loop quick without making the delays the thing
// under test. Slow enough that an ignored cancellation is visibly slower than an
// honoured one.
func fastBackoff() BackoffConfig {
	return BackoffConfig{
		InitialInterval: 50 * time.Millisecond,
		InitialAttempts: 100,
		MidInterval:     50 * time.Millisecond,
		MidAttempts:     100,
		SlowInterval:    50 * time.Millisecond,
		SlowAttempts:    100,
		MaxInterval:     50 * time.Millisecond,
	}
}

func newTestNotificationManager() *notificationManager {
	return &notificationManager{
		activeNotifications: make(map[uint32]activeNotification),
		configsByKey:        make(map[string]struct{}),
		orphanSeen:          make(map[uint32]time.Time),
		orphanSem:           make(chan struct{}, orphanDeleteMaxConcurrency),
	}
}

// newDialableTestSession builds a Session the reconnect loop can drive against a
// real address.
//
// Deliberately NOT newWiredTestSession: that helper's Client comes from Dial,
// which gives it a Background context of its own, so tearDownAndReset's wait for
// the worker goroutines never returns. Production Clients take the session's
// context (see publishWiredClient), which is what lets a teardown stop them.
func newDialableTestSession(t *testing.T, host string, port int, maxAttempts int) *Session {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sess := &Session{
		ip:   host,
		port: port,
		tx: &transport{
			sendChannel:    make(chan []byte),
			systemResponse: make(chan []byte, 1),
			recvQueue:      make(chan []byte, recvQueueSize),
			activeRequests: map[uint32]chan []byte{},
		},
		notifications: newTestNotificationManager(),
		cache:         &symbolCache{symbols: map[string]*symbol{}, onDemandSymbols: map[string]bool{}},
		// Production always has one; ensureRoute dereferences it unconditionally.
		route:          &routeManager{},
		logger:         getDefaultLogger(),
		requestTimeout: 500 * time.Millisecond,
		lifecycle: &sessionLifecycle{
			closedCh:             make(chan struct{}),
			parentCtx:            context.Background(),
			maxReconnectAttempts: maxAttempts,
			backoffConfig:        fastBackoff(),
			ctx:                  ctx,
			shutdown:             cancel,
		},
	}
	sess.lifecycle.state.transitionTo(SessionStateConstructed)
	sess.lifecycle.state.transitionTo(SessionStateConnecting)
	sess.lifecycle.state.transitionTo(SessionStateConnected)
	return sess
}

// unreachableSession is newReconnectTestSession pointed at port 1 on loopback,
// where every dial is refused instantly.
func unreachableSession(t *testing.T, maxAttempts int) *Session {
	t.Helper()
	sess := newDialableTestSession(t, "127.0.0.1", 1, maxAttempts)
	sess.lifecycle.state.transitionTo(SessionStateDisconnected)
	return sess
}

// TestReconnect_CancelledCtxClosesSession: cancelling the context handed to
// Reconnect must end the attempt promptly AND leave the session closed.
//
// Closed rather than merely stopped, because Reconnecting has no exit to
// Disconnected (FSM table rows 21-24) — returning early any other way parks the
// session in Reconnecting, where the single-flight gate turns every later
// Reconnect into a no-op and the consumer never learns anything is wrong.
func TestReconnect_CancelledCtxClosesSession(t *testing.T) {
	// 60 attempts x 50ms: ignoring the ctx takes seconds, honouring it is
	// immediate, so the two outcomes are unambiguous.
	sess := unreachableSession(t, 60)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sess.Reconnect(ctx) }()

	time.Sleep(120 * time.Millisecond) // let a dial fail and a backoff start
	cancelledAt := time.Now()
	cancel()

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Reconnect never returned")
	}
	if elapsed := time.Since(cancelledAt); elapsed > 400*time.Millisecond {
		t.Errorf("Reconnect took %v to return after cancellation — it ran on through the attempt budget",
			elapsed.Round(time.Millisecond))
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Reconnect returned %v, want an error wrapping context.Canceled", err)
	}
	if !sess.IsClosed() {
		t.Error("session not closed after a cancelled reconnect — IsClosed is the consumer's only signal, so it would never rebuild")
	}
	if got := sess.lifecycle.state.load(); got != SessionStateClosed {
		t.Errorf("state = %v, want Closed", got)
	}
}

// TestReconnect_EveryExitLeavesConnectedOrClosed is the invariant itself, driven
// over each way a reconnect can end.
func TestReconnect_EveryExitLeavesConnectedOrClosed(t *testing.T) {
	cases := []struct {
		name string
		run  func(t *testing.T) *Session
	}{
		{
			name: "attempts exhausted",
			run: func(t *testing.T) *Session {
				sess := unreachableSession(t, 2)
				_ = sess.Reconnect(context.Background())
				return sess
			},
		},
		{
			name: "context cancelled",
			run: func(t *testing.T) *Session {
				sess := unreachableSession(t, 60)
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() { _ = sess.Reconnect(ctx); close(done) }()
				time.Sleep(80 * time.Millisecond)
				cancel()
				waitFor(t, done, 5*time.Second, "Reconnect after cancellation")
				return sess
			},
		},
		{
			name: "session closed mid-attempt",
			run: func(t *testing.T) *Session {
				sess := unreachableSession(t, 60)
				done := make(chan struct{})
				go func() { _ = sess.Reconnect(context.Background()); close(done) }()
				time.Sleep(80 * time.Millisecond)
				// The real API, not markClosed: Close is what a consumer calls,
				// and it also drives the FSM transition.
				_ = sess.Close()
				waitFor(t, done, 5*time.Second, "Reconnect after close")
				return sess
			},
		},
		{
			name: "abandoned Reconnecting state",
			run: func(t *testing.T) *Session {
				sess := unreachableSession(t, 2)
				// Nobody owns this state: an earlier attempt ended without
				// resolving it. Reconnect must take it over, not decline it.
				sess.lifecycle.state.value.Store(uint32(SessionStateReconnecting))
				_ = sess.Reconnect(context.Background())
				return sess
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sess := tc.run(t)
			switch got := sess.lifecycle.state.load(); got {
			case SessionStateClosed, SessionStateConnected:
			default:
				t.Errorf("state after %q = %v; a session left in %v is invisible to a consumer polling IsClosed()",
					tc.name, got, got)
			}
		})
	}
}

func waitFor(t *testing.T, done <-chan struct{}, d time.Duration, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %v", what, d)
	}
}

// TestReconnect_PreReconnectHandlesReleasedWhenTransportIsUp: Reconnect empties
// activeNotifications up front and keeps the handles only in a local snapshot,
// releasing them at a point reached only after BOTH the route probe and the
// symbol reload have succeeded. A reconnect whose dial and route come up but
// whose reload keeps failing therefore has a live, routed transport on every
// attempt and still never issues those deletes — the handles stay in the PLC's
// notification table, and a session flapping this way accumulates them, which is
// the table exhaustion (Beckhoff #268) the cleanup exists to prevent.
//
// What this does NOT claim: once the loop gives up the transport is gone, so
// nothing can be deleted then. The fix is to release while the transport is
// usable, not to keep trying afterwards.
func TestReconnect_PreReconnectHandlesReleasedWhenTransportIsUp(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess := newDialableTestSession(t, srv.host, srv.port, 2)

	var deletedMu sync.Mutex
	var deleted []uint32
	// The release goes out as a sum-delete; record the handles it carries.
	srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(req []byte) []byte {
		deletedMu.Lock()
		for i := 0; i+4 <= len(req); i += 4 {
			deleted = append(deleted, binary.LittleEndian.Uint32(req[i:i+4]))
		}
		deletedMu.Unlock()
		codes := make([]ReturnCode, len(req)/4)
		return buildSumDeleteNotifPayload(codes)
	})
	// Per-handle fallback, in case the sum path is unavailable.
	srv.onDeleteDeviceNotification(func(h uint32) ReturnCode {
		deletedMu.Lock()
		deleted = append(deleted, h)
		deletedMu.Unlock()
		return ReturnCodeNoErrors
	})
	// Route probe succeeds every attempt, so the transport is routed and usable;
	// the symbol reload is what fails, so the loop exhausts.
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{9}
	})
	srv.onRead(GroupSymbolUploadInfo, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeDeviceError, nil
	})

	// A subscription for Reconnect to snapshot.
	sess.notifications.lock.Lock()
	sym := &symbol{FullName: "MAIN.flap", Name: "MAIN.flap", DataType: "INT", Length: 2, Handle: 0xE200}
	sess.notifications.activeNotifications[0xBEEF] = activeNotification{Sym: sym, Ch: make(chan *Update, 1)}
	sess.notifications.lock.Unlock()

	sess.lifecycle.state.transitionTo(SessionStateDisconnected)
	_ = sess.Reconnect(context.Background())

	deletedMu.Lock()
	got := append([]uint32(nil), deleted...)
	deletedMu.Unlock()
	if !slices.Contains(got, 0xBEEF) {
		t.Errorf("pre-reconnect handle 0xBEEF was never released (deletes seen: %v) — it stays in the PLC notification table while the session keeps reconnecting", got)
	}
}

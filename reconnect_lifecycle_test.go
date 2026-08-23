package ads

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
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

// TestReconnect_CleanupKeepsTheUserChannel is the bug a power-cycle found on
// hardware and no stub test had caught.
//
// Reconnect wipes activeNotifications up front, then issues a best-effort delete
// for the handles it snapshotted. Session.SumDeleteDeviceNotification clears
// notificationChannel whenever activeNotifications is empty — a rule that exists
// so a user who deletes their last subscription can subscribe again with a
// different channel. During a reconnect that map is empty by construction, so the
// internal cleanup nils the channel, and resubscribeNotifications then returns
// early on savedChannel == nil without a single log line.
//
// Observed consequence on a power-cycled TC2: reconnect reported success, the FSM
// said Connected, IsClosed() stayed false, and not one notification ever arrived
// again. A consumer polling IsClosed() has no way to notice.
func TestReconnect_CleanupKeepsTheUserChannel(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	// One Add per symbol, which is also what a TC2 does — the shape this bug was
	// found on.
	if !c.capabilities.SumAddNotifStateCAS(0, 2) {
		t.Fatal("could not force SumAddNotif into the unsupported state")
	}
	srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(req []byte) []byte {
		return buildSumDeleteNotifPayload(make([]ReturnCode, len(req)/4))
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode { return ReturnCodeNoErrors })

	// State as it stands when a reconnect begins: one subscription, and the
	// config + channel that a resubscribe will need.
	const handle = 0xAB01
	sym := preSeedTypedSymbol(sess, "MAIN.keepchan", 0xE300)
	ch := make(chan *Update, 4)
	sess.notifications.lock.Lock()
	sess.notifications.activeNotifications[handle] = activeNotification{Sym: sym, Ch: ch}
	sess.notifications.notificationChannel = ch
	sess.notifications.addConfig(NotificationConfig{SymbolName: "MAIN.keepchan", TransmissionMode: TransModeServerOnChange})
	sess.notifications.lock.Unlock()

	// Exactly what Reconnect does: snapshot the handles, wipe the map, then
	// release the old registrations.
	sess.notifications.lock.Lock()
	saved := []uint32{handle}
	sess.notifications.activeNotifications = make(map[uint32]activeNotification)
	sess.notifications.lock.Unlock()
	sess.bestEffortDeleteNotifications(context.Background(), saved)

	sess.notifications.lock.Lock()
	gotCh := sess.notifications.notificationChannel
	pending := len(sess.notifications.pending)
	sess.notifications.lock.Unlock()

	if gotCh == nil {
		t.Error("cleanup cleared notificationChannel; resubscribeNotifications will return early and no notification will ever be restored")
	}
	if pending == 0 {
		t.Error("cleanup dropped the saved configs; there is nothing left to resubscribe")
	}

	// And the end-to-end consequence: a resubscribe must actually reach the PLC.
	var adds atomic.Int32
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		adds.Add(1)
		return addNotifResponse{Handle: 0xAB02}
	})
	if err := sess.resubscribeNotifications(); err != nil {
		t.Fatalf("resubscribeNotifications: %v", err)
	}
	if adds.Load() == 0 {
		t.Error("resubscribe issued no Add — the subscription is gone for the life of the session")
	}
}

// TestReconnect_FailedHandleReleaseIsRetried: the pre-reconnect snapshot is the
// only record of handles that still exist on the PLC, so it must not be thrown
// away on a release that did not land.
//
// Found by flapping the link against a real TC2 through the proxy: with route
// registration skipped (a pre-registered route, which is common) the release
// attempt runs before anything has proved the transport works, reports
// "requested=3 deleted=0", and the snapshot is cleared anyway. Every flap then
// leaves another three registrations in the PLC's notification table — the
// accumulation this cleanup exists to prevent, reintroduced by moving the release
// earlier.
func TestReconnect_FailedHandleReleaseIsRetried(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	const attempts = 3
	sess := newDialableTestSession(t, srv.host, srv.port, attempts)

	var releaseAttempts atomic.Int32
	// Refuse every delete with a code that is NOT success-equivalent, so no
	// attempt ever lands and the snapshot must survive for the next one.
	srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(req []byte) []byte {
		releaseAttempts.Add(1)
		codes := make([]ReturnCode, len(req)/4)
		for i := range codes {
			codes[i] = ReturnCodeDeviceError
		}
		return buildSumDeleteNotifPayload(codes)
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode {
		releaseAttempts.Add(1)
		return ReturnCodeDeviceError
	})
	// Route probe fine, reload always fails: the loop runs its full budget.
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{9}
	})
	srv.onRead(GroupSymbolUploadInfo, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeDeviceError, nil
	})

	sess.notifications.lock.Lock()
	sym := &symbol{FullName: "MAIN.retryrelease", Name: "MAIN.retryrelease", DataType: "INT", Length: 2, Handle: 0xE400}
	sess.notifications.activeNotifications[0xBE01] = activeNotification{Sym: sym, Ch: make(chan *Update, 1)}
	sess.notifications.lock.Unlock()

	// Full discovery had been done, so the reload path really runs — and fails
	// against the stub above, which is what makes the loop take every attempt.
	sess.cache.lock.Lock()
	sess.cache.symbolsFullyLoaded = true
	sess.cache.lock.Unlock()

	sess.lifecycle.state.transitionTo(SessionStateDisconnected)
	_ = sess.Reconnect(context.Background())

	if got := releaseAttempts.Load(); got < 2 {
		t.Errorf("release attempted %d time(s) across %d reconnect attempts; a release that did not land must be retried, not discarded",
			got, attempts)
	}
}

// TestReconnect_HandleReleaseRetryIsBounded: retrying is right, retrying forever
// is not. Production runs with unbounded reconnect attempts by default, so a PLC
// that keeps rejecting the delete would be asked once per attempt for the life of
// the session. After a few rounds the handles are better left to the orphan
// reaper, which deletes them if they ever stream again.
func TestReconnect_HandleReleaseRetryIsBounded(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	// More reconnect attempts than release attempts, so the cap is what limits it.
	sess := newDialableTestSession(t, srv.host, srv.port, preReconnectReleaseAttempts+4)

	var releaseAttempts atomic.Int32
	srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(req []byte) []byte {
		releaseAttempts.Add(1)
		codes := make([]ReturnCode, len(req)/4)
		for i := range codes {
			codes[i] = ReturnCodeDeviceError
		}
		return buildSumDeleteNotifPayload(codes)
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode {
		releaseAttempts.Add(1)
		return ReturnCodeDeviceError
	})
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{9}
	})
	srv.onRead(GroupSymbolUploadInfo, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeDeviceError, nil
	})

	sess.notifications.lock.Lock()
	sym := &symbol{FullName: "MAIN.bounded", Name: "MAIN.bounded", DataType: "INT", Length: 2, Handle: 0xE500}
	sess.notifications.activeNotifications[0xBE02] = activeNotification{Sym: sym, Ch: make(chan *Update, 1)}
	sess.notifications.lock.Unlock()
	sess.cache.lock.Lock()
	sess.cache.symbolsFullyLoaded = true
	sess.cache.lock.Unlock()

	sess.lifecycle.state.transitionTo(SessionStateDisconnected)
	_ = sess.Reconnect(context.Background())

	if got := int(releaseAttempts.Load()); got > preReconnectReleaseAttempts {
		t.Errorf("release attempted %d times, want at most %d — an unreleasable handle must not be retried on every reconnect attempt forever",
			got, preReconnectReleaseAttempts)
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

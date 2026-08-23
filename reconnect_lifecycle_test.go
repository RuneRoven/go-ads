package ads

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
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

// TestReconnect_UnservedPLCTriggersCooldown: a PLC that accepts the TCP
// connection and then answers nothing must not be hammered.
//
// This is the .224 shape, seen in this lab for months: connect succeeds, the
// route registers "successfully", and every request times out. Beckhoff's own
// maintainer describes the mechanism — the TwinCAT router expects exactly one TCP
// connection per remote IP and drops the older one whenever a new connection is
// accepted (Beckhoff/ADS#85) — so a client that redials every backoff sustains
// the fight rather than resolving it. Field observation matches: the device often
// serves again once something stops trying entirely for a while.
//
// So after a few consecutive unserved attempts the loop must go quiet: no
// sockets, no route registration, for a cooldown period. Distinguishing feature
// of "unserved" is that the dial SUCCEEDS and the request then times out —
// unreachable hosts (refused dials) are a different failure and keep their fast
// retries.
func TestReconnect_UnservedPLCTriggersCooldown(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	// Accept the connection, then answer nothing at all — the .224 shape. The
	// silence has to land on a step AFTER the dial, so route registration is
	// skipped (it is UDP and there is no responder here, which would be a
	// different failure) and the symbol reload is what goes unanswered.
	srv.delayBefore(CommandIDRead, uint32(GroupSymbolUploadInfo), time.Hour)
	srv.delayBefore(CommandIDReadWrite, uint32(GroupSymbolUploadInfo), time.Hour)

	sess := newDialableTestSession(t, srv.host, srv.port, 0) // unbounded attempts
	sess.route = &routeManager{skipRegistration: true}
	sess.requestTimeout = 200 * time.Millisecond
	sess.cache.symbolsFullyLoaded = true
	sess.lifecycle.unservedCooldown = 2 * time.Second
	sess.lifecycle.state.transitionTo(SessionStateDisconnected)

	done := make(chan struct{})
	go func() { _ = sess.Reconnect(context.Background()); close(done) }()
	t.Cleanup(func() { _ = sess.Close(); <-done })

	// Let it burn through the unserved-attempt allowance, then watch it hold off.
	time.Sleep(3 * time.Second)
	duringCooldown := srv.accepts()
	time.Sleep(1500 * time.Millisecond)
	afterQuiet := srv.accepts()

	t.Logf("dials: %d by the time the cooldown started, %d after a further 1.5s", duringCooldown, afterQuiet)
	if afterQuiet-duringCooldown > 2 {
		t.Errorf("%d further dials during a %v cooldown — an unserved PLC is being hammered, which is what keeps the router losing",
			afterQuiet-duringCooldown, sess.lifecycle.unservedCooldown)
	}
	if duringCooldown == 0 {
		t.Error("never dialed at all; the test did not exercise the path")
	}
}

// TestReconnect_RefusedDialKeepsFastRetries: the cooldown must not slow down the
// ordinary case. A refused dial means the PLC is down or unreachable, and the
// router is not in a bad state, so retries stay on the configured backoff.
func TestReconnect_RefusedDialKeepsFastRetries(t *testing.T) {
	sess := unreachableSession(t, 0)            // port 1: instantly refused, unbounded attempts
	sess.lifecycle.unservedCooldown = time.Hour // would be catastrophic if applied here

	done := make(chan struct{})
	go func() { _ = sess.Reconnect(context.Background()); close(done) }()
	t.Cleanup(func() { _ = sess.Close(); <-done })

	// fastBackoff is 50ms, so a second is worth many attempts.
	time.Sleep(time.Second)
	if got := sess.lifecycle.reconnectAttemptsForTest(); got < 5 {
		t.Errorf("only %d attempts in 1s against a refused port; a refused dial must not enter the unserved cooldown", got)
	}
}

// TestConnect_VerifiesTheLinkAnswersEvenWithoutRouteRegistration: Connect must
// never report success against a PLC that accepts the socket and answers nothing.
//
// The liveness check used to live inside the route-registration branch, so a
// caller with a pre-registered route (no WithRoute, or WithSkipRouteRegistration)
// got no check at all. Measured against a TC/RTOS device in this state: Connect
// returned nil in 5.01s, IsClosed() false, FSM Connected — and every subsequent
// request timed out. That is the invisible-stuck state a consumer polling
// IsClosed() cannot detect.
func TestConnect_VerifiesTheLinkAnswersEvenWithoutRouteRegistration(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	// Accepts the connection; answers nothing.
	srv.delayBefore(CommandIDRead, uint32(GroupSymbolVersion), time.Hour)

	sess, err := NewSession(context.Background(),
		AMSEndpoint{IP: srv.host, Port: srv.port, AMS: AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851}},
		WithRequestTimeout(300*time.Millisecond),
		WithTargetCheck(TargetCheckOff),
		WithAutoReconnect(false),
		// No WithRoute: the route is assumed to exist already, so registration —
		// and with it the old liveness check — is skipped.
	)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	err = sess.Connect(context.Background())
	if err == nil {
		t.Error("Connect reported success against a PLC that answered nothing; the session is deaf but looks healthy")
	}
	if err == nil && !sess.IsClosed() {
		t.Error("...and IsClosed() is false, so a consumer has no way to notice")
	}
}

// TestReconnect_DoesNotReRegisterRouteEveryAttempt: a session registers its route
// at most once, however many times it reconnects.
//
// ensureRoute registers whenever its probe fails, and the probe fails on every
// attempt against a PLC that answers nothing — so an unbounded reconnect loop
// registered the same route dozens of times. On a TC/RTOS device in this lab that
// left the runtime route table holding TWO entries for our NetID where the
// persisted config has one, after which the router began answering on a
// connection it opened to us instead of ours, and the session stopped working
// entirely. Registering once is enough; if the route is registered and the PLC
// still will not talk, more registrations cannot help.
func TestReconnect_DoesNotReRegisterRouteEveryAttempt(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	router := startRouteResponder(t)

	// The PLC accepts TCP and answers no ADS request, so every probe fails.
	srv.delayBefore(CommandIDRead, uint32(GroupSymbolVersion), time.Hour)

	sess := newDialableTestSession(t, srv.host, srv.port, 6)
	sess.routerPort = router.port
	sess.route = &routeManager{
		name:              "go-ads-test",
		username:          "Administrator",
		password:          secret("1"),
		activationTimeout: 300 * time.Millisecond,
	}
	sess.requestTimeout = 200 * time.Millisecond
	sess.lifecycle.unservedCooldown = 300 * time.Millisecond
	sess.lifecycle.state.transitionTo(SessionStateDisconnected)

	_ = sess.Reconnect(context.Background())

	// One per unserved episode, not one per attempt. Re-registering the correct
	// route is the measured recovery for a mute router, so a session that has
	// concluded the PLC is silent is allowed one more — but a plain retry loop
	// must not register every time round.
	got := router.registrations()
	if got == 0 {
		t.Error("no registration at all; the route was never established")
	}
	if got >= 6 {
		t.Errorf("route registered %d times across 6 reconnect attempts — that is once per attempt, which is the storm this guards against", got)
	}
	t.Logf("route registered %d time(s) across 6 attempts", got)
}

// TestReconnect_ReRegistersRouteToHealAMuteDevice: re-registering our own route is
// the measured way back from a router that has stopped answering, so a reconnect
// loop that has concluded the PLC is silent must be allowed to try it.
//
// The state, measured on TC3.1.4024 and TC3.1.4026: a foreign NetID claimed the
// address the PLC routes to us, and TC3 keys its route table by ADDRESS, so the
// device answered nothing at all — not on our connection, not on one it opened.
// One registration of our own NetID for our own address rebound it and both
// devices returned to normal service immediately.
//
// So the rule is neither "register every attempt" (which is how the address gets
// contested in the first place) nor "register once per session, ever" (which locks
// out the recovery). It is once per unserved episode.
func TestReconnect_ReRegistersRouteToHealAMuteDevice(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	router := startRouteResponder(t)

	// The device answers only once it has seen a SECOND registration: the first is
	// the session establishing its route, the second is the healing one.
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		if router.registrations() < 2 {
			// Outlast the client's request timeout without answering: silence, not
			// a malformed reply, is what a router in this state produces — and only
			// a plain deadline counts as "unserved".
			time.Sleep(400 * time.Millisecond)
		}
		return ReturnCodeNoErrors, []byte{12}
	})

	sess := newDialableTestSession(t, srv.host, srv.port, 40)
	sess.routerPort = router.port
	sess.route = &routeManager{
		name:              "go-ads-test",
		username:          "Administrator",
		password:          secret("1"),
		activationTimeout: 500 * time.Millisecond,
	}
	sess.callbackIP = "192.168.3.52"
	sess.requestTimeout = 200 * time.Millisecond
	sess.lifecycle.unservedCooldown = 300 * time.Millisecond
	sess.lifecycle.state.transitionTo(SessionStateDisconnected)

	done := make(chan error, 1)
	go func() { done <- sess.Reconnect(context.Background()) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("reconnect never recovered: %v (registrations=%d)", err, router.registrations())
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("reconnect did not finish; registrations=%d", router.registrations())
	}

	if got := router.registrations(); got < 2 {
		t.Errorf("registrations = %d, want at least 2: the session must be allowed one healing re-registration after concluding the PLC is silent", got)
	} else {
		t.Logf("recovered after %d registrations", got)
	}
	if state := sess.lifecycle.state.load(); state != SessionStateConnected {
		t.Errorf("state = %v, want Connected after recovery", state)
	}
}

// gateOnLog pins the goroutine that logs a chosen message until the test releases
// it, which is how these tests reach a window that is otherwise nanoseconds wide.
//
// Reconnect's tail — mark the transport live, transition to Connected, log
// "reconnect successful", return, run the defers — has no seam a test can drive:
// the whole stretch is CPU-only, so a drop injected from outside lands after the
// defers essentially every time. The log call inside that stretch is the one place
// the session hands control to something the test owns.
//
// Only the FIRST match gates. Later ones pass straight through, so the recovery
// this pins the way open for is free to log the same line.
// watch is the shared state, so handlers cloned by WithAttrs/WithGroup gate on the
// same channels as the original.
type gateWatch struct {
	match     string
	reached   chan struct{}
	release   chan struct{}
	gateOnce  sync.Once
	signal    string
	signalled chan struct{}
	sigOnce   sync.Once
}

type gateOnLog struct {
	inner slog.Handler
	w     *gateWatch
}

// newGateOnLog gates the goroutine that logs `match`, and separately signals —
// without blocking — when some other goroutine logs `signal`.
func newGateOnLog(match, signal string) *gateOnLog {
	return &gateOnLog{
		inner: slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}),
		w: &gateWatch{
			match:     match,
			reached:   make(chan struct{}),
			release:   make(chan struct{}),
			signal:    signal,
			signalled: make(chan struct{}),
		},
	}
}

func (g *gateOnLog) Enabled(context.Context, slog.Level) bool { return true }

func (g *gateOnLog) Handle(ctx context.Context, r slog.Record) error {
	if g.w.signal != "" && strings.Contains(r.Message, g.w.signal) {
		g.w.sigOnce.Do(func() { close(g.w.signalled) })
	}
	if strings.Contains(r.Message, g.w.match) {
		first := false
		g.w.gateOnce.Do(func() { first = true })
		if first {
			close(g.w.reached)
			<-g.w.release
		}
	}
	return g.inner.Handle(ctx, r)
}

func (g *gateOnLog) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &gateOnLog{inner: g.inner.WithAttrs(attrs), w: g.w}
}

func (g *gateOnLog) WithGroup(name string) slog.Handler {
	return &gateOnLog{inner: g.inner.WithGroup(name), w: g.w}
}

// TestReconnect_DropWhileFinishingIsNotLost: a drop that lands while a reconnect
// is finishing must still be recovered.
//
// Reconnect holds reconnectOwner until its deferred release runs, which is AFTER
// it has marked the transport live and gone Connected. A drop in that gap takes
// the session to Disconnected and spawns a Reconnect that loses the ownership CAS
// and returns immediately — so the drop is acknowledged by the FSM and then
// dropped on the floor. Nothing retries: the owner is on its way out, and
// heartbeatWatch skips while disconnected.
//
// The session then sits Disconnected with IsClosed() false and no retry loop,
// which is precisely the state a consumer polling IsClosed() cannot see (see this
// file's header). The same gap orphans a reconnectDone that Close() waits on
// unconditionally, so Close() hangs too — asserted here as well, since it is the
// same root cause.
func TestReconnect_DropWhileFinishingIsNotLost(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{7}
	})

	sess := newDialableTestSession(t, srv.host, srv.port, 40)
	gate := newGateOnLog("reconnect successful", "reconnect already in progress")
	sess.logger = slog.New(gate)
	// The drop we inject is delivered the way the read loop delivers one, so the
	// auto path has to be the thing that recovers it.
	sess.lifecycle.autoReconnect = true
	sess.lifecycle.state.transitionTo(SessionStateDisconnected)

	first := make(chan error, 1)
	go func() { first <- sess.Reconnect(context.Background()) }()

	select {
	case <-gate.w.reached:
	case err := <-first:
		t.Fatalf("reconnect finished without reaching the success log: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("reconnect never reached the success log")
	}

	// Pinned inside the window: transport live, state Connected, owner still held.
	if got := sess.lifecycle.state.load(); got != SessionStateConnected {
		t.Fatalf("state = %v at the success log, want Connected — the gate is in the wrong place", got)
	}
	if !sess.lifecycle.reconnectOwner.Load() {
		t.Fatal("reconnectOwner is already released at the success log — the gate is in the wrong place")
	}

	// The drop lands here, exactly as callOnDrop would deliver it.
	sess.triggerReconnect()

	// Wait for the reconnect it spawned to actually LOSE the ownership CAS before
	// releasing the gate. Without this the test is a race it usually wins: the
	// owner is released microseconds after triggerReconnect returns, so the new
	// goroutine's CAS normally succeeds and the window is never exercised.
	select {
	case <-gate.w.signalled:
	case <-time.After(10 * time.Second):
		t.Fatal("the drop's reconnect never reported losing the ownership CAS; the window was not exercised")
	}
	close(gate.w.release)

	if err := <-first; err != nil {
		t.Fatalf("first reconnect returned %v, want nil", err)
	}

	// The session must end up Connected (recovered) or Closed (gave up loudly).
	// Disconnected forever is the failure this test exists for.
	deadline := time.Now().Add(15 * time.Second)
	for {
		state := sess.lifecycle.state.load()
		if state == SessionStateConnected || state == SessionStateClosed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("session parked in %v with IsClosed()=%v and no reconnect owner (%v): "+
				"the drop that landed while the previous reconnect was finishing was acknowledged and then abandoned",
				state, sess.IsClosed(), sess.lifecycle.reconnectOwner.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Same root cause, second symptom: triggerReconnect may have installed a fresh
	// reconnectDone that no owner will ever close, and Close() waits on it with no
	// timeout.
	closed := make(chan struct{})
	go func() { defer close(closed); sess.Close() }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close() hung: reconnectDone was orphaned by the abandoned drop")
	}
}

// TestConnect_FailedRouteActivationLeavesNothingRunning: a Connect that reports
// failure must not leave a live transport behind.
//
// Both route-stage error returns in Connect skipped tearDownAndReset, unlike every
// sibling path in the same function. The socket stayed open, the Client's workers
// (listen, transmit, recvWorkers) kept running, and the deferred re-arm restored
// onDrop — so when the PLC eventually closed that abandoned socket, the session
// silently began reconnecting a Connect the caller had been told failed. If it
// then succeeded, the caller holds a Session it believes is dead and never reads
// from, while a second Client publishes a second transmitWorker onto the same
// shared tx.sendChannel and frames go to whichever socket wins.
func TestConnect_FailedRouteActivationLeavesNothingRunning(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	router := startRouteResponder(t)

	// The router ACKs the registration, but the device never serves the route:
	// silence, which is what awaitRouteActive is there to catch.
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		time.Sleep(400 * time.Millisecond)
		return ReturnCodeNoErrors, []byte{3}
	})

	sess, err := NewSession(context.Background(),
		AMSEndpoint{IP: srv.host, Port: srv.port, AMS: AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851}},
		WithRequestTimeout(150*time.Millisecond),
		WithTargetCheck(TargetCheckOff),
		WithRoute("go-ads-test", "Administrator", "1"),
		WithHostIP("127.0.0.1"),
		WithRouteActivationTimeout(600*time.Millisecond),
		WithoutAmsPeerFallback(),
		WithBackoff(fastBackoff()),
	)
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	sess.routerPort = router.port
	t.Cleanup(func() { sess.Close() })

	if err := sess.Connect(context.Background()); err == nil {
		t.Fatal("Connect reported success although the route never activated")
	}

	// The Client that was published during the attempt must be finished with.
	if c := sess.client.Load(); c != nil && c.ctx != nil && c.ctx.Err() == nil {
		t.Error("the Client published during the failed Connect is still live: its workers are running on an open socket")
	}

	// A retry is explicitly allowed from here (the rollback leaves Disconnected,
	// and Disconnected -> Connecting is a legal edge). It must not end up with two
	// Clients on the shared transport: the first one's transmitWorker would still
	// be competing for tx.sendChannel and writing to a socket that is gone.
	before := srv.accepts()
	_ = sess.Connect(context.Background())
	if got := srv.accepts() - before; got != 1 {
		t.Errorf("the retry produced %d new connections, want 1: the abandoned transport was never closed, "+
			"so the route stage redialed on top of it", got)
	}
}

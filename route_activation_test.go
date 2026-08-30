package ads

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// route_activation_test.go — awaitRouteActive's context handling.
//
// The bug these pin: awaitRouteActive redials via tearDownAndReset, which
// cancels lifecycle.ctx and installs a fresh one. A caller that passed
// lifecycle.ctx *by value* therefore had the loop cancel its own context on the
// first redial — every later probe born already dead, and the failure reported
// as caller cancellation. That is why the parameter is a func() context.Context
// re-read per attempt rather than a captured ctx.

// TestCurrentLifecycleCtx_TracksReplacement: the helper must see the context
// tearDownAndReset installs, not the one it replaced.
func TestCurrentLifecycleCtx_TracksReplacement(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	sessCtx, cancel := context.WithCancel(parent)
	sess := &Session{
		lifecycle: &sessionLifecycle{
			closedCh:  make(chan struct{}),
			parentCtx: parent,
			ctx:       sessCtx,
			shutdown:  cancel,
		},
		logger: getDefaultLogger(),
	}

	before := sess.currentLifecycleCtx()
	if before.Err() != nil {
		t.Fatalf("lifecycle ctx already done: %v", before.Err())
	}

	// Replace it the way a redial does.
	sess.lifecycle.ctxMu.Lock()
	sess.lifecycle.shutdown()
	sess.lifecycle.ctx, sess.lifecycle.shutdown = context.WithCancel(parent)
	sess.lifecycle.ctxMu.Unlock()
	defer sess.lifecycle.shutdown()

	if before.Err() == nil {
		t.Error("the replaced context should be cancelled — a captured one is exactly the bug")
	}
	after := sess.currentLifecycleCtx()
	if after.Err() != nil {
		t.Errorf("currentLifecycleCtx returned a dead context: %v", after.Err())
	}
	if after == before {
		t.Error("currentLifecycleCtx returned the stale context")
	}
}

// TestAwaitRouteActive_SurvivesCtxReplacement is the regression proper: it runs
// the real awaitRouteActive against a stub PLC whose first probe fails, and
// replaces lifecycle.ctx mid-loop exactly as the redial's tearDownAndReset
// does. With the ctx supplied per attempt the second probe succeeds; with a
// captured ctx (the bug) it is born cancelled and the call fails.
func TestAwaitRouteActive_SurvivesCtxReplacement(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, _ := newWiredTestSession(t, srv)
	sess.route = &routeManager{name: "go-ads-test", activationTimeout: 4 * time.Second}

	var probes atomic.Int32
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		n := probes.Add(1)
		if n == 1 {
			// Fail the first probe, then replace lifecycle.ctx the way a redial
			// would. A captured ctx is dead from here on.
			sess.lifecycle.ctxMu.Lock()
			sess.lifecycle.shutdown()
			sess.lifecycle.ctx, sess.lifecycle.shutdown = context.WithCancel(sess.lifecycle.parentCtx)
			sess.lifecycle.ctxMu.Unlock()
			return ReturnCodeDeviceError, nil
		}
		return ReturnCodeNoErrors, []byte{7}
	})

	version, err := sess.awaitRouteActive(sess.currentLifecycleCtx)
	if err != nil {
		t.Fatalf("awaitRouteActive after a mid-loop ctx replacement: %v", err)
	}
	if version != 7 {
		t.Errorf("symbol version = %d, want 7 (the winning probe's value must be returned so Connect need not re-read it)", version)
	}
	if got := probes.Load(); got < 2 {
		t.Errorf("probe attempts = %d, want >= 2 (the loop must retry after the first failure)", got)
	}
}

// TestAwaitRouteActive_RestoresClientState: the ondrop handler and the
// handshaking flag must both be put back however the call ends, or a later
// transport fault is either ignored or logged at the wrong level for the rest
// of the session.
func TestAwaitRouteActive_RestoresClientState(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, c := newWiredTestSession(t, srv)
	sess.route = &routeManager{name: "go-ads-test", activationTimeout: time.Second}
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{3}
	})

	if _, err := sess.awaitRouteActive(sess.currentLifecycleCtx); err != nil {
		t.Fatalf("awaitRouteActive: %v", err)
	}
	if c.handshaking.Load() != 0 {
		t.Error("handshaking count non-zero after return — later real faults would log at Debug")
	}
	c.ondropMu.RLock()
	restored := c.ondrop != nil
	c.ondropMu.RUnlock()
	if !restored {
		t.Error("ondrop not restored after return — a later drop would not trigger reconnect")
	}
}

// TestRouteActivationBudget covers the derived per-probe timeout, including the
// clamps that keep a shortened total coherent.
func TestRouteActivationBudget(t *testing.T) {
	tests := []struct {
		name       string
		configured time.Duration
		wantTotal  time.Duration
		wantProbe  time.Duration
	}{
		{name: "default", configured: 0, wantTotal: defaultRouteActivationTimeout, wantProbe: 2 * time.Second},
		{name: "short total clamps probe to floor", configured: time.Second, wantTotal: time.Second, wantProbe: minRouteActivationProbe},
		{name: "long total clamps probe to ceiling", configured: time.Minute, wantTotal: time.Minute, wantProbe: maxRouteActivationProbe},
		{name: "mid total divides by four", configured: 4 * time.Second, wantTotal: 4 * time.Second, wantProbe: time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sess := &Session{route: &routeManager{activationTimeout: tt.configured}}
			total, probe := sess.routeActivationBudget()
			if total != tt.wantTotal {
				t.Errorf("total = %v, want %v", total, tt.wantTotal)
			}
			if probe != tt.wantProbe {
				t.Errorf("probe = %v, want %v", probe, tt.wantProbe)
			}
			if probe > total {
				t.Errorf("probe %v exceeds total %v — one attempt would overrun the budget", probe, total)
			}
		})
	}
}

// --- redial storm bound (P1a) ---

// TestRedialBackoff pins the arithmetic. A loop that waits the wrong amount is
// invisible from outside — it just retries at the wrong rate — which is why this
// is a pure function with its own test rather than arithmetic inline in the loop.
func TestRedialBackoff(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want time.Duration
	}{
		{name: "first redial waits the base", n: 0, want: 250 * time.Millisecond},
		{name: "second doubles", n: 1, want: 500 * time.Millisecond},
		{name: "third doubles again", n: 2, want: time.Second},
		{name: "fourth would be 2s, which is also the cap", n: 3, want: 2 * time.Second},
		{name: "beyond the cap stays at the cap", n: 9, want: 2 * time.Second},
		{name: "negative is treated as the first redial", n: -1, want: 250 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := redialBackoff(tc.n); got != tc.want {
				t.Errorf("redialBackoff(%d) = %v, want %v", tc.n, got, tc.want)
			}
		})
	}
}

// activationTestSession builds a session that has really dialed srv through
// dialAndStart, so its Client came from publishWiredClient and shares
// lifecycle.ctx.
//
// newWiredTestSession cannot be used for anything that redials: its Client comes
// from Dial with a context of its own, so tearDownAndReset's wait for that
// Client's workers never returns (the helper says as much). That is also why no
// test reached awaitRouteActive's redial branch before this file.
func activationTestSession(t *testing.T, srv *scriptableServer, budget time.Duration) *Session {
	t.Helper()
	sess := newDialableTestSession(t, srv.host, srv.port, 0)
	sess.route = &routeManager{name: "go-ads-test", activationTimeout: budget}
	if err := sess.dialAndStart(); err != nil {
		t.Fatalf("dialAndStart: %v", err)
	}
	t.Cleanup(func() { sess.markClosed() })
	return sess
}

// TestAwaitRouteActive_CapsTheRedialStorm is the regression this branch exists
// for.
//
// Before the cap, awaitRouteActive redialled on EVERY 250ms poll for the whole
// activation budget: measured in the field as 76 ephemeral ports in 11s, and ~40
// sockets per 10s window. Under the Beckhoff one-TCP-per-host rule each redial
// evicted its own predecessor, so the storm was self-defeating as well as
// expensive — it is what turned one drop into an unrecoverable deadloop.
//
// The stub drops the connection on every symbol-version probe, which is what a
// device does for a route it is not serving yet, so the loop takes the retryable
// path every time. What is asserted is the number of TCP connections the window
// costs.
func TestAwaitRouteActive_CapsTheRedialStorm(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	// 3s: long enough that the old behaviour would redial ~12 times (3s / 250ms).
	sess := activationTestSession(t, srv, 3*time.Second)

	// Sticky, unlike dropConnAfter, which disarms itself after one firing: the
	// probe has to keep failing at transport level or the loop never takes the
	// branch under test.
	srv.dropConnAlways(CommandIDRead)

	if _, err := sess.awaitRouteActive(sess.currentLifecycleCtx); err == nil {
		t.Fatal("awaitRouteActive returned nil for a route the PLC never served")
	}

	// Total accepts, not a delta around the call: the stub's counter is incremented
	// by its accept goroutine, so it can lag the client-side dial that the fixture
	// already made, and a baseline read can miss it. The budget is therefore stated
	// as "the one connection this session started with, plus the cap".
	const want = maxRouteActivationRedials + 1
	if dials := srv.accepts(); dials > want {
		t.Errorf("the session used %d TCP connections in total, want <= %d (1 initial + %d redials) — this is the redial storm",
			dials, want, maxRouteActivationRedials)
	}
}

// TestAwaitRouteActive_DoesNotSpinAfterTheBudget: with the redial budget spent
// and the transport gone, every further probe fails instantly. Continuing to poll
// would be a tight spin for the rest of the budget — so capping the redials
// WITHOUT this early exit makes the loop worse, not better.
//
// The budget is deliberately long and the assertion is on elapsed time: the loop
// must give up when there is nothing left to probe on, rather than sitting out the
// full window.
func TestAwaitRouteActive_DoesNotSpinAfterTheBudget(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	const budget = 10 * time.Second
	sess := activationTestSession(t, srv, budget)
	srv.dropConnAlways(CommandIDRead)

	start := time.Now()
	if _, err := sess.awaitRouteActive(sess.currentLifecycleCtx); err == nil {
		t.Fatal("awaitRouteActive returned nil for a route the PLC never served")
	}
	elapsed := time.Since(start)

	// The bounded path costs the three backoffs (250+500+1000ms) plus a few probe
	// round-trips against a stub that answers by hanging up. Anything near the
	// budget means it kept polling a dead transport.
	if elapsed > budget/2 {
		t.Errorf("awaitRouteActive took %v of a %v budget; it is still polling after the redial budget was spent",
			elapsed, budget)
	}
}

// TestAwaitRouteActive_CloseDuringTheWaitOpensNoSocket.
//
// The wait before a redial has to watch three things, and Close is the one a
// two-arm select misses: on the Connect path the context passed in is Connect's
// OWN caller context, which Close does not cancel. Without closedCh in that
// select, Close returns, this loop then wakes, and dialAndStart opens a real TCP
// connection to the PLC — it dials before it re-checks isClosed — leaving a stray
// socket and a stray ephemeral port in the one code path whose whole purpose is to
// stop burning ephemeral ports.
func TestAwaitRouteActive_CloseDuringTheWaitOpensNoSocket(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess := activationTestSession(t, srv, 5*time.Second)
	srv.dropConnAlways(CommandIDRead)

	// A context of its own that is never cancelled: this is Connect's caller ctx,
	// the case where closedCh is the only signal that ever arrives.
	callerCtx, cancelCaller := context.WithCancel(context.Background())
	defer cancelCaller()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = sess.awaitRouteActive(func() context.Context { return callerCtx })
	}()

	// Let it fail a probe and enter the backoff wait, then close underneath it.
	time.Sleep(150 * time.Millisecond)
	sess.markClosed()
	afterClose := srv.accepts()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("awaitRouteActive did not return after the session was closed — the wait is not watching closedCh")
	}

	if extra := srv.accepts() - afterClose; extra > 0 {
		t.Errorf("%d TCP connection(s) opened after the session was closed", extra)
	}
}

// TestAwaitRouteActive_ProbesAgainAfterARedial: the reordered loop is
// close -> wait -> dial -> probe, and the probe at the end of that sequence is the
// point of the whole exercise. A reorder that left the window able to expire
// between the dial and the probe would turn every recoverable activation into a
// timeout, and would do it silently — the socket count would look right.
//
// The stub refuses at transport level until the route "comes live", then answers.
func TestAwaitRouteActive_ProbesAgainAfterARedial(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess := activationTestSession(t, srv, 5*time.Second)
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{9}
	})
	srv.dropConnAlways(CommandIDRead)

	// Let the loop take the retryable path at least once, then start serving.
	go func() {
		time.Sleep(300 * time.Millisecond)
		srv.stopDroppingConn(CommandIDRead)
	}()

	version, err := sess.awaitRouteActive(sess.currentLifecycleCtx)
	if err != nil {
		t.Fatalf("awaitRouteActive did not recover once the route was served: %v", err)
	}
	if version != 9 {
		t.Errorf("symbol version = %d, want 9 — the winning probe's value must be returned", version)
	}
	if srv.droppingAlways(CommandIDRead) {
		t.Fatal("the stub was still refusing; this test proved nothing")
	}
}

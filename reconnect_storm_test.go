package ads

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// reconnect_storm_test.go — the P1b/P2b hardening from the 2026-08-27 field
// investigation: the publish window, the dialMu invariant, and the drop verdict.

// TestPublishWiredClient_PublishesBeforeStartingWorkers.
//
// The order used to be SetOnDrop -> startWorkers -> client.Store, on the
// reasoning that a concurrent reader must never see a half-built Client. But the
// workers ARE concurrent readers of sess.client: a drop landing between
// startWorkers and Store ran callOnDrop -> triggerReconnect -> tearDownAndReset,
// which loaded sess.client and therefore tore down the PREVIOUS Client —
// markDropped-ing and waiting the wrong one — while this Client's workers kept
// running with nobody waiting their WaitGroup and its transmitWorker sharing
// tx.sendChannel with the next dial's.
//
// Asserted structurally rather than by racing the window: by the time any worker
// exists, sess.client must already point at the Client those workers belong to.
func TestPublishWiredClient_PublishesBeforeStartingWorkers(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess := newDialableTestSession(t, srv.host, srv.port, 0)
	if err := sess.dialAndStart(); err != nil {
		t.Fatalf("dialAndStart: %v", err)
	}
	t.Cleanup(func() { sess.markClosed() })

	c := sess.client.Load()
	if c == nil {
		t.Fatal("dialAndStart returned with no Client published")
	}
	// The workers hold this Client's ctx and WaitGroup, so a teardown that loaded
	// anything else would wait the wrong ones.
	if got := sess.client.Load(); got != c {
		t.Errorf("sess.client changed after the dial: %p vs %p", got, c)
	}
	if c.ctx == nil || c.ctx.Err() != nil {
		t.Error("the published Client has no live context: its workers cannot be stopped by a teardown")
	}
}

// TestRedialDuringHandshake_FlagsDisconnectedAcrossTheGap.
//
// awaitRouteActive's redial used to leave tx.disconnected false, because only
// resetForRetry set it — so the flag that gates user RPCs said "connected" while
// there was no socket and no workers at all. Survivable while the gap was a single
// dial; with a deliberate backoff before the dial it is up to seconds.
//
// Note this is tx.disconnected, not the public IsDisconnected(): that one is
// FSM-based and was never wrong here, because a session in an activation window is
// Connecting or Reconnecting rather than Connected.
func TestRedialDuringHandshake_FlagsDisconnectedAcrossTheGap(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess := newDialableTestSession(t, srv.host, srv.port, 0)
	if err := sess.dialAndStart(); err != nil {
		t.Fatalf("dialAndStart: %v", err)
	}
	t.Cleanup(func() { sess.markClosed() })

	if sess.tx.disconnected.Load() {
		t.Fatal("tx.disconnected set immediately after a successful dial")
	}

	// Observe the flag from inside the gap. dialAndStart clears it again once the
	// new workers are up, so the only place it can be seen is during the teardown.
	var sawDisconnected bool
	var mu sync.Mutex
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			if sess.tx.disconnected.Load() {
				mu.Lock()
				sawDisconnected = true
				mu.Unlock()
				return
			}
			time.Sleep(200 * time.Microsecond)
		}
	}()

	if err := sess.redialDuringHandshake(); err != nil {
		t.Fatalf("redialDuringHandshake: %v", err)
	}
	<-done

	mu.Lock()
	seen := sawDisconnected
	mu.Unlock()
	if !seen {
		t.Error("tx.disconnected was never true across a redial: the RPC gate says connected while there is no socket")
	}
	if sess.tx.disconnected.Load() {
		t.Error("still flagged disconnected after the redial completed")
	}
}

// TestRedialDuringHandshake_LeavesOndropDisarmed: publishWiredClient arms ondrop
// on every new Client, so a caller deliberately holding the transport during a
// handshake would get a window in which a PLC RST spawns exactly the rival
// Reconnect that the disarm exists to prevent. Both callers need this, which is
// why it lives in the helper rather than being repeated at each of them.
func TestRedialDuringHandshake_LeavesOndropDisarmed(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess := newDialableTestSession(t, srv.host, srv.port, 0)
	if err := sess.dialAndStart(); err != nil {
		t.Fatalf("dialAndStart: %v", err)
	}
	t.Cleanup(func() { sess.markClosed() })

	if err := sess.redialDuringHandshake(); err != nil {
		t.Fatalf("redialDuringHandshake: %v", err)
	}
	c := sess.client.Load()
	if c == nil {
		t.Fatal("no Client after the redial")
	}
	c.ondropMu.RLock()
	armed := c.ondrop != nil
	c.ondropMu.RUnlock()
	if armed {
		t.Error("ondrop is armed on the new Client: a drop mid-handshake would spawn a rival Reconnect")
	}
	if c.handshaking.Load() == 0 {
		t.Error("the new Client is not in a handshake region: expected transport faults would log at ERROR")
	}
}

// TestDialMu_SerialisesConcurrentRedials: two redials must not interleave, or two
// TCP connections to the same AMS router briefly coexist — the documented way to
// get evicted, since the router serves one TCP per host and closes the older
// (Beckhoff/ADS#49).
//
// Run this under -race: the failure it guards against is a torn teardown, not a
// wrong number.
func TestDialMu_SerialisesConcurrentRedials(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess := newDialableTestSession(t, srv.host, srv.port, 0)
	if err := sess.dialAndStart(); err != nil {
		t.Fatalf("dialAndStart: %v", err)
	}
	t.Cleanup(func() { sess.markClosed() })

	const goroutines = 4
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sess.redialDuringHandshake(); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent redial failed: %v", err)
	}

	// One survivor, and it is the one sess.client points at.
	c := sess.client.Load()
	if c == nil {
		t.Fatal("no Client after concurrent redials")
	}
	if c.ctx.Err() != nil {
		t.Error("the surviving Client's context is already cancelled")
	}
}

// TestDropVerdict_NeverServedNamesTheRoute and its established counterpart are
// the two halves of the classification. Before the split, both produced the same
// message and the same route hint, because the only evidence consulted was the
// errno — and EOF/ECONNRESET look identical whether the socket is 20ms or 20h old.
// A field investigation lost hours re-reading route tables over drops of sessions
// that had been delivering samples for half an hour.
func TestDropVerdict_NeverServedNamesTheRoute(t *testing.T) {
	var c Client
	if c.wasEstablished() {
		t.Fatal("a client that has decoded no frames must not be judged established")
	}
	// The sentinel Connect attaches follows from the same predicate.
	if c.wasEstablished() {
		t.Error("verdict flipped without a frame having been decoded")
	}
}

func TestDropVerdict_FramesOnEitherSocketMeanEstablished(t *testing.T) {
	tests := []struct {
		name    string
		primary uint64
		peer    uint64
		want    bool
	}{
		{name: "no frames at all", primary: 0, peer: 0, want: false},
		{name: "frames on our own connection", primary: 3, peer: 0, want: true},
		{
			// The case a primary-only counter gets wrong. On TC3.1.4026/RTOS the PLC
			// answers only on the connection IT opens, so a healthy routed session
			// decodes zero frames on its primary for its entire life; judging those
			// drops "never served" would give a whole device class the route-suspect
			// diagnosis and the slow backoff.
			name: "frames only on the connection the PLC opened to us",
			peer: 5, want: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var c Client
			c.framesPrimary.Store(tc.primary)
			c.framesPeer.Store(tc.peer)
			if got := c.wasEstablished(); got != tc.want {
				t.Errorf("wasEstablished() = %v, want %v (framesPrimary=%d framesPeer=%d)",
					got, tc.want, tc.primary, tc.peer)
			}
		})
	}
}

// TestDropSentinels_AreDistinct: the two verdicts must be mutually exclusive, or
// a consumer branching on them gets both paths.
func TestDropSentinels_AreDistinct(t *testing.T) {
	if errors.Is(ErrRouteNotServed, ErrEstablishedDropped) || errors.Is(ErrEstablishedDropped, ErrRouteNotServed) {
		t.Error("the two drop sentinels match each other; a consumer cannot branch on them")
	}
	wrapped := errors.Join(ErrEstablishedDropped, io.EOF)
	if !errors.Is(wrapped, ErrEstablishedDropped) {
		t.Error("ErrEstablishedDropped does not survive wrapping")
	}
	if errors.Is(wrapped, ErrRouteNotServed) {
		t.Error("an established-drop error also matches ErrRouteNotServed")
	}
}

// TestLogDropVerdict_CarriesTheLocalPortAndFrameCounts.
//
// Every drop investigated in the field needed the ephemeral port to correlate the
// event against a packet capture, and it was only at Debug — which
// `debug_level: true` then rotated away every ~9 seconds. The counts go in the
// same line because they are the evidence behind the verdict.
func TestLogDropVerdict_CarriesTheLocalPortAndFrameCounts(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	logs := &testLogHandler{}
	sess := newDialableTestSession(t, srv.host, srv.port, 0)
	sess.logger = slog.New(logs)
	if err := sess.dialAndStart(); err != nil {
		t.Fatalf("dialAndStart: %v", err)
	}
	t.Cleanup(func() { sess.markClosed() })

	c := sess.client.Load()
	port := c.localPort()
	if port == 0 {
		t.Fatal("localPort() returned 0 for a live connection")
	}
	c.logDropVerdict(io.EOF)

	rec := logs.findByMessage("transport down")
	if rec == nil {
		t.Fatal(`no drop record containing "transport down" — the AST guard and two message-matching tests depend on that substring`)
	}
	if !rec.hasAttr("localPort") {
		t.Error("the drop line carries no localPort: a packet capture cannot be correlated after the fact")
	}
	if !rec.hasAttr("framesPrimary") || !rec.hasAttr("framesPeer") {
		t.Error("the drop line carries no frame counts: the verdict cannot be checked from the log")
	}
}

// TestLogDropVerdict_EstablishedDropDoesNotBlameTheRoute: the whole point of the
// split. A connection that carried frames was being served, so the route is the
// wrong thing to send the reader after.
func TestLogDropVerdict_EstablishedDropDoesNotBlameTheRoute(t *testing.T) {
	logs := &testLogHandler{}
	var c Client
	c.logger = slog.New(logs)
	c.ctx = context.Background()
	c.tx = &transport{}
	c.framesPrimary.Store(7)

	c.logDropVerdict(io.EOF)

	rec := logs.findByMessage("PLC dropped an established connection, transport down")
	if rec == nil {
		t.Fatal("an established drop was not reported as one")
	}
	if hint := rec.attr("hint"); hint == "" {
		t.Fatal("the established-drop line carries no hint")
	}
	if got := rec.attr("hint"); !strings.Contains(got, "per-host TCP slot") {
		t.Errorf("the established-drop hint does not name eviction, which is the top suspect: %q", got)
	}
	if logs.findByMessage("PLC closed connection, transport down") != nil {
		t.Error("an established drop also produced the route-suspect line")
	}
}

// TestAwaitRouteActive_ConfigFallbackStillRunsWhileRedialling is the documented
// degradation, asserted rather than left to be discovered.
//
// The CONFIG second opinion (ReadState on the system service port, which stays up
// when the runtime port does not) runs before the redial decision on every
// iteration, so capping the redials does not remove it — it reduces how many
// chances a device gets to answer it. A device that both drops the socket AND sits
// in CONFIG is therefore likelier to be reported as "route not served" than as
// ErrRuntimeNotRunning. That trade is deliberate: the honest failure is bounded,
// the storm was not.
//
// What must remain true is that a device answering the system service gets the
// runtime-not-running verdict rather than the route one.
func TestAwaitRouteActive_ConfigFallbackStillRunsWhileRedialling(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	srv.setADSState(ADSStateConfig)

	sess := activationTestSession(t, srv, 3*time.Second)

	// The runtime port answers nothing (the probe reads the symbol version there),
	// while the system service still reports CONFIG — the real shape of a PLC that
	// is up but not running.
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeDeviceError, nil
	})

	_, err := sess.awaitRouteActive(sess.currentLifecycleCtx)
	if err == nil {
		t.Fatal("awaitRouteActive reported success for a PLC whose runtime is not running")
	}
	if !errors.Is(err, ErrRuntimeNotRunning) {
		t.Errorf("error = %v, want ErrRuntimeNotRunning: the route was being served, the runtime was not", err)
	}
}

// TestNestedRedial_DoesNotDeadlock.
//
// dialMu is not reentrant, and the reconnect path reaches awaitRouteActive through
// ensureRoute: Reconnect tears down before its retry loop and dials inside it,
// while awaitRouteActive redials within that span. Holding dialMu across a
// reconnect iteration would therefore self-deadlock on the inner redial, which is
// why the lock's scope is the pair inside redialDuringHandshake and nothing wider.
//
// The failure mode is a hang, not a wrong answer, so this test is written around a
// timeout: without one it would report as a suite timeout in some other test.
func TestNestedRedial_DoesNotDeadlock(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess := activationTestSession(t, srv, time.Second)
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{4}
	})

	done := make(chan error, 1)
	go func() {
		// ensureRoute -> awaitRouteActive -> redialDuringHandshake is the nesting
		// that matters; ensureRoute with no force and no probe failures probes and
		// returns, so drive awaitRouteActive directly after a redial to put two
		// acquisitions of dialMu on the same goroutine's stack in sequence.
		if err := sess.redialDuringHandshake(); err != nil {
			done <- err
			return
		}
		_, err := sess.awaitRouteActive(sess.currentLifecycleCtx)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("nested redial path failed: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("nested redial path deadlocked: dialMu is held across a call that acquires it again")
	}
}

// TestNextFlapCount closes the dead zone that let a device reset us on a timer
// for ever at the cheapest backoff tier.
//
// Measured on 10.13.37.52, 2026-08-28: 21 consecutive resets at a metronomic ~35s.
// 35s sits between flapWindow (5s) and flapResetWindow (60s), where the old
// classifier neither incremented flapCount nor reset it — so it stayed frozen at 3
// and every cycle waited reconnectBackoff(3) = 1s, burning ~5 accepted sockets
// each time against a device with a small socket table.
func TestNextFlapCount(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name          string
		prev          int
		lastConnected time.Time
		servedNothing bool
		want          int
	}{
		{
			name: "a reset inside the severe window counts double",
			prev: 1, lastConnected: now.Add(-2 * time.Second), want: 3,
		},
		{
			name: "35s — the old dead zone — now counts as a flap",
			prev: 3, lastConnected: now.Add(-35 * time.Second), want: 4,
		},
		{
			name: "just under the reset window still counts",
			prev: 0, lastConnected: now.Add(-59 * time.Second), want: 1,
		},
		{
			name: "a connection that outlived the reset window clears the count",
			prev: 7, lastConnected: now.Add(-2 * time.Minute), want: 0,
		},
		{
			name: "no previous Connected and something was served: unchanged",
			prev: 2, lastConnected: time.Time{}, servedNothing: false, want: 2,
		},
		{
			name: "no previous Connected and nothing served: still a flap",
			prev: 2, lastConnected: time.Time{}, servedNothing: true, want: 3,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextFlapCount(tc.prev, tc.lastConnected, now, tc.servedNothing); got != tc.want {
				t.Errorf("nextFlapCount(prev=%d, servedNothing=%v) = %d, want %d",
					tc.prev, tc.servedNothing, got, tc.want)
			}
		})
	}
}

// TestNextFlapCount_MetronomeEscalates walks the measured scenario forward: a
// device that resets every 35s must reach the slow tiers instead of sitting on the
// first one. This is the regression that matters — the arithmetic above is only
// interesting because of what it does over many cycles.
func TestNextFlapCount_MetronomeEscalates(t *testing.T) {
	sess := &Session{lifecycle: &sessionLifecycle{backoffConfig: DefaultBackoffConfig()}}
	count := 0
	now := time.Now()
	var delays []time.Duration
	for cycle := 0; cycle < 12; cycle++ {
		// Each connection lives 35s, then resets, exactly as measured.
		now = now.Add(35 * time.Second)
		count = nextFlapCount(count, now.Add(-35*time.Second), now, false)
		sess.lifecycle.flapCount = count
		delays = append(delays, sess.reconnectBackoff(count))
	}
	first, last := delays[0], delays[len(delays)-1]
	if last <= first {
		t.Errorf("backoff did not escalate across 12 metronomic resets: first=%v last=%v (all=%v)",
			first, last, delays)
	}
	// The whole point: it must leave the cheapest tier well before cycle 12.
	if delays[len(delays)-1] < 5*time.Second {
		t.Errorf("after 12 flaps the cooldown is still %v; the loop is still burning sockets every cycle", last)
	}
}

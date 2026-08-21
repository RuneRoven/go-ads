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

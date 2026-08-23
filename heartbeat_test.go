package ads

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// heartbeat_test.go — noticing that subscriptions have died, without asking the
// PLC anything.
//
// Measured on TC3.1.4024 across CONFIG -> RUN with no program change, three runs
// including a fully passive listener that sent the PLC nothing at all: the TCP
// connection survives (no drop, no reconnect), the symbol version is unchanged
// because nothing was recompiled, ADS state reads back identical, no error and no
// terminal sample arrives — and the caller's subscriptions never deliver again.
// 210 samples, then silence.
//
// So there is no inbound event to react to, and silence alone proves nothing
// either, because an on-change subscription on a constant symbol is legitimately
// silent forever. One CYCLIC subscription resolves it: TwinCAT pushes those on a
// timer regardless of change, and on that same transition the beat and the
// caller's samples stopped in the same second (heartbeat +1 then +0, symbol +2
// then +0). Its absence is therefore conclusive, and the PLC does the sending.

// TestHeartbeat_ResubscribesWhenBeatsStop is the requirement: data must come back
// once the PLC serves again, with nobody rebuilding the session.
func TestHeartbeat_ResubscribesWhenBeatsStop(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var adds atomic.Int32
	var nextHandle atomic.Uint32
	nextHandle.Store(0x700)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		adds.Add(1)
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode { return ReturnCodeNoErrors })

	sess, c := newWiredTestSession(t, srv, WithNotificationHeartbeat(150*time.Millisecond, 3))
	c.SetNotificationHandler(sess.handleNotification)
	// The stub speaks individual Add/Delete, not the sum groups.
	if !c.capabilities.SumAddNotifStateCAS(0, 2) || !c.capabilities.SumDeleteNotifStateCAS(0, 2) {
		t.Fatal("could not force the sum commands into the unsupported state")
	}
	preSeedTypedSymbol(sess, "MAIN.beat", 0xF300)

	ch := make(chan *Update, 16)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.beat", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	afterSetup := adds.Load()
	if afterSetup < 2 {
		t.Fatalf("Add calls = %d; expected the caller's subscription plus a cyclic heartbeat", afterSetup)
	}
	hb := sess.notifications.heartbeatHandle.Load()
	if hb == 0 {
		t.Fatal("no heartbeat handle established")
	}

	// While the PLC beats, nothing may be re-subscribed: churning subscriptions
	// costs handle-table slots and loses samples across the gap.
	for i := 0; i < 6; i++ {
		if err := sess.drivePacket(sess.currentLifecycleCtx(), buildNotificationPacket(hb, 0, []byte{1})); err != nil {
			t.Fatalf("drivePacket: %v", err)
		}
		time.Sleep(60 * time.Millisecond)
	}
	if got := adds.Load(); got != afterSetup {
		t.Fatalf("re-subscribed while beats were arriving (%d -> %d)", afterSetup, got)
	}

	// Now the beats stop, as they did on hardware after CONFIG -> RUN.
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if adds.Load() > afterSetup {
			t.Logf("re-subscribed after the beats stopped (%d -> %d Add calls)", afterSetup, adds.Load())
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("beats stopped and nothing re-subscribed within 4s; a notification-only session would stay silent forever")
}

// TestHeartbeat_NotDeliveredToTheCaller: the heartbeat is the library's own
// business. A consumer must not see samples for something it never subscribed to.
func TestHeartbeat_NotDeliveredToTheCaller(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	var nextHandle atomic.Uint32
	nextHandle.Store(0x800)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	var deletedMu sync.Mutex
	var deleted []uint32
	srv.onDeleteDeviceNotification(func(h uint32) ReturnCode {
		deletedMu.Lock()
		deleted = append(deleted, h)
		deletedMu.Unlock()
		return ReturnCodeNoErrors
	})

	// A cycle long enough that the watcher cannot fire during the test.
	sess, c := newWiredTestSession(t, srv, WithNotificationHeartbeat(5*time.Second, 3))
	c.SetNotificationHandler(sess.handleNotification)
	preSeedTypedSymbol(sess, "MAIN.quiet", 0xF400)
	ch := make(chan *Update, 8)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.quiet", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	hb := sess.notifications.heartbeatHandle.Load()
	if hb == 0 {
		t.Fatal("no heartbeat handle")
	}

	// What matters is not just that the caller is spared the sample, but that the
	// beat is RECORDED (otherwise the watchdog fires spuriously and churns every
	// subscription) and that the reaper does not mistake our own handle for a
	// leaked one and delete it.
	before := sess.notifications.heartbeatLastNs.Load()
	time.Sleep(5 * time.Millisecond)
	if err := sess.drivePacket(sess.currentLifecycleCtx(), buildNotificationPacket(hb, 0, []byte{1})); err != nil {
		t.Fatalf("drivePacket: %v", err)
	}
	select {
	case u := <-ch:
		t.Errorf("heartbeat sample delivered to the caller: %+v", u)
	case <-time.After(300 * time.Millisecond):
	}
	if got := sess.notifications.heartbeatLastNs.Load(); got <= before {
		t.Errorf("beat not recorded: lastNs %d -> %d; the watchdog would conclude the beats had stopped", before, got)
	}
	deletedMu.Lock()
	killedOwnHeartbeat := slices.Contains(deleted, hb)
	deletedMu.Unlock()
	if killedOwnHeartbeat {
		t.Error("the orphan reaper deleted our own heartbeat handle")
	}
}

// TestHeartbeat_OptOut: the heartbeat costs a handle in the PLC's table and a
// cyclic sample per interval, so it has to be refusable.
func TestHeartbeat_OptOut(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	var adds atomic.Int32
	var nextHandle atomic.Uint32
	nextHandle.Store(0x900)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		adds.Add(1)
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})

	sess, c := newWiredTestSession(t, srv, WithoutNotificationHeartbeat())
	c.SetNotificationHandler(sess.handleNotification)
	preSeedTypedSymbol(sess, "MAIN.nohb", 0xF500)
	ch := make(chan *Update, 4)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.nohb", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	if got := adds.Load(); got != 1 {
		t.Errorf("Add calls = %d with the heartbeat disabled, want 1 (the caller's subscription only)", got)
	}
	if hb := sess.notifications.heartbeatHandle.Load(); hb != 0 {
		t.Errorf("heartbeat handle %d established although disabled", hb)
	}
}

// TestHeartbeat_CarriesSymbolVersionChange: the beat's payload IS the symbol
// version, so an online change shows up without any extra request.
func TestHeartbeat_CarriesSymbolVersionChange(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	var nextHandle atomic.Uint32
	nextHandle.Store(0xA00)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode { return ReturnCodeNoErrors })

	// The reload cap defaults to 0 in this helper, which degrades AutoReload to
	// Ignore — set it so the strategy can actually run.
	sess, c := newWiredTestSession(t, srv,
		WithNotificationHeartbeat(5*time.Second, 3),
		WithMaxSymbolVersionReloadAttempts(5),
		WithSymbolVersionReloadWindow(time.Minute))
	c.SetNotificationHandler(sess.handleNotification)
	preSeedTypedSymbol(sess, "MAIN.ver", 0xF600)
	ch := make(chan *Update, 4)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.ver", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	sess.cache.lock.Lock()
	sess.cache.symbolVersion = 7
	sess.cache.lock.Unlock()

	before := sess.epoch()
	hb := sess.notifications.heartbeatHandle.Load()
	if err := sess.drivePacket(sess.currentLifecycleCtx(), buildNotificationPacket(hb, 0, []byte{8})); err != nil {
		t.Fatalf("drivePacket: %v", err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if sess.epoch() != before {
			return // stale detection fired, which is the existing reload path
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Error("a heartbeat carrying a different symbol version did not trigger stale-cache handling")
}

// TestHeartbeat_RecoverySurvivesAnUnavailablePLC: the recovery must be able to
// fail and try again.
//
// Found on hardware: the heartbeat correctly detected dead subscriptions 10s after
// a CONFIG toggle, but every re-subscribe attempted while the PLC was still in
// CONFIG failed and consumed the resubscribe retry budget (three strikes, then the
// configs are dropped). Worse, once the active-notification map was empty the
// watchdog stopped caring, because it keyed on active handles rather than on the
// caller's intent — so nothing ever retried and the session was silent for good.
//
// No data while the PLC is in CONFIG is expected. Losing the subscriptions
// permanently is not.
func TestHeartbeat_RecoverySurvivesAnUnavailablePLC(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var serving atomic.Bool // false = "PLC in CONFIG": Adds are refused
	var adds, refusals atomic.Int32
	var nextHandle atomic.Uint32
	nextHandle.Store(0xB00)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		if !serving.Load() {
			refusals.Add(1)
			return addNotifResponse{Error: ReturnCodeDeviceServiceNotSupported}
		}
		adds.Add(1)
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode { return ReturnCodeNoErrors })

	serving.Store(true)
	sess, c := newWiredTestSession(t, srv, WithNotificationHeartbeat(100*time.Millisecond, 2))
	c.SetNotificationHandler(sess.handleNotification)
	if !c.capabilities.SumAddNotifStateCAS(0, 2) || !c.capabilities.SumDeleteNotifStateCAS(0, 2) {
		t.Fatal("could not force the sum commands into the unsupported state")
	}
	preSeedTypedSymbol(sess, "MAIN.survive", 0xF700)

	ch := make(chan *Update, 8)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.survive", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}

	// The PLC goes to CONFIG: beats stop AND re-subscribes are refused. Several
	// recovery windows go by — more than the resubscribe retry budget.
	serving.Store(false)
	time.Sleep(2 * time.Second)
	if refusals.Load() == 0 {
		t.Fatal("no re-subscribe was attempted while the PLC was refusing; the test did not exercise the path")
	}
	t.Logf("%d re-subscribe attempts refused while unavailable", refusals.Load())

	// Back to RUN: the session must recover by itself, which it cannot do if the
	// configs were dropped in the meantime.
	before := adds.Load()
	serving.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if adds.Load() > before {
			sess.notifications.lock.Lock()
			active := len(sess.notifications.activeNotifications)
			sess.notifications.lock.Unlock()
			if active > 0 {
				t.Logf("recovered once the PLC served again (%d successful Add calls, %d active)", adds.Load(), active)
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	sess.notifications.lock.Lock()
	pending := len(sess.notifications.pending)
	sess.notifications.lock.Unlock()
	t.Errorf("never recovered after the PLC served again: successful Adds=%d, pending configs=%d — a session that loses its configs can never come back",
		adds.Load(), pending)
}

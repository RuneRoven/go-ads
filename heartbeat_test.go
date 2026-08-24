package ads

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
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
	// The reaper must delete a handle that is genuinely nobody's, and must not
	// delete the heartbeat. The first half is what makes the second half mean
	// something: asserting only "the heartbeat was not deleted" passed even with the
	// reaper call removed entirely, because dispatchSample consumes the heartbeat
	// before the reaper is ever reached — unreachable by construction, not by timing.
	const strayHandle = uint32(0xBADBEEF)
	sess.notifications.lastSubscribeNs.Store(0) // no subscribe race to hide behind
	if err := sess.drivePacket(sess.currentLifecycleCtx(), buildNotificationPacket(strayHandle, 0, []byte{2})); err != nil {
		t.Fatalf("drivePacket: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		deletedMu.Lock()
		sawStray := slices.Contains(deleted, strayHandle)
		killedOwnHeartbeat := slices.Contains(deleted, hb)
		deletedMu.Unlock()
		if killedOwnHeartbeat {
			t.Fatal("the orphan reaper deleted our own heartbeat handle")
		}
		if sawStray {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the reaper never deleted a handle belonging to nobody (%d), so this test cannot tell whether it runs at all", strayHandle)
		}
		time.Sleep(50 * time.Millisecond)
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
	// Throttled, not once per window. 2s with a 200ms window is 10 attempts
	// unthrottled; backing off doubles the wait after each failure. Each attempt
	// re-queues every config, so an unthrottled retry also burns the resubscribe
	// attempt counters at the heartbeat interval.
	if got := refusals.Load(); got > 5 {
		t.Errorf("%d re-subscribe attempts in 2s against a PLC that is refusing: recovery is not backing off, so a runtime left in "+
			"CONFIG is retried at the heartbeat interval indefinitely", got)
	}

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

// TestHeartbeat_ReEstablishedAfterReconnect: a reconnect must leave the session
// with a live heartbeat.
//
// The heartbeat lives outside activeNotifications, so the reconnect sweep — which
// snapshots that map, wipes it and deletes those handles PLC-side — does not touch
// heartbeatHandle. A stale non-zero handle makes establishHeartbeat a no-op, so
// from the first drop onward the session has no beat and cannot notice its
// subscriptions dying quietly. That is the entire feature, off, in the situation
// most likely to need it.
//
// Two more consequences the assertions below cover: the pre-drop registration is
// never deleted (one leaked PLC handle per reconnect — the Beckhoff #268
// accumulation this code fights everywhere else), and the stale handle NUMBER
// stays armed in consumeHeartbeat, so a caller subscription that the PLC later
// assigns that same number has every sample swallowed as a beat.
func TestHeartbeat_ReEstablishedAfterReconnect(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var nextHandle atomic.Uint32
	nextHandle.Store(0x900)
	var mu sync.Mutex
	var heartbeatAdds []uint32 // handles issued for a cyclic add on the version group
	var deleted []uint32

	srv.onAddDeviceNotification(func(req addNotifRequest) addNotifResponse {
		h := nextHandle.Add(1)
		if Group(req.Group) == GroupSymbolVersion && req.TransMode == uint32(TransModeServerCycle) {
			mu.Lock()
			heartbeatAdds = append(heartbeatAdds, h)
			mu.Unlock()
		}
		return addNotifResponse{Handle: h}
	})
	srv.onDeleteDeviceNotification(func(h uint32) ReturnCode {
		mu.Lock()
		deleted = append(deleted, h)
		mu.Unlock()
		return ReturnCodeNoErrors
	})
	// The resubscribe goes through the batch path, which tries the sum command first.
	srv.onWriteRead(GroupSumupAddDeviceNotification, func(_ []byte) []byte {
		return buildSumAddNotifPayload([]sumNotifResponse{{Error: ReturnCodeNoErrors, Handle: nextHandle.Add(1)}})
	})
	// Releases go through the sum group too, so record them there or the leak
	// assertion below can never be satisfied by any implementation.
	srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(req []byte) []byte {
		codes := make([]ReturnCode, len(req)/4)
		mu.Lock()
		for i := range codes {
			deleted = append(deleted, binary.LittleEndian.Uint32(req[i*4:]))
			codes[i] = ReturnCodeNoErrors
		}
		mu.Unlock()
		return buildSumDeleteNotifPayload(codes)
	})
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{9}
	})

	sess := newDialableTestSession(t, srv.host, srv.port, 5)
	// Long enough that the watchdog never fires during the test: this is about the
	// reconnect path re-establishing the beat, not about detection.
	sess.heartbeatInterval = 30 * time.Second
	sess.lifecycle.state.transitionTo(SessionStateDisconnected)

	// First reconnect just gives us a live Client, so everything below is built by
	// product code rather than by hand.
	if err := sess.Reconnect(context.Background()); err != nil {
		t.Fatalf("initial reconnect: %v", err)
	}
	t.Cleanup(func() { sess.Close() })

	preSeedTypedSymbol(sess, "MAIN.beat", 0xF300)
	ch := make(chan *Update, 16)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.beat", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	oldHB := sess.notifications.heartbeatHandle.Load()
	if oldHB == 0 {
		t.Fatal("no heartbeat established by the first subscribe")
	}

	// Now the drop and the reconnect that has to restore everything.
	sess.lifecycle.state.transitionTo(SessionStateDisconnected)
	if err := sess.Reconnect(context.Background()); err != nil {
		t.Fatalf("reconnect: %v", err)
	}

	newHB := sess.notifications.heartbeatHandle.Load()
	if newHB == 0 {
		t.Fatal("no heartbeat handle after the reconnect: the session cannot notice its subscriptions dying")
	}
	if newHB == oldHB {
		t.Errorf("heartbeat handle is still the pre-drop %d after a reconnect: establishHeartbeat short-circuits on the stale "+
			"handle, so no beat was registered on the new connection and the stale number stays armed in consumeHeartbeat", oldHB)
	}
	mu.Lock()
	hbAdds := append([]uint32(nil), heartbeatAdds...)
	dels := append([]uint32(nil), deleted...)
	mu.Unlock()
	if len(hbAdds) < 2 {
		t.Errorf("cyclic adds on the version group = %d, want 2 (one per connection): the PLC was never asked to beat again", len(hbAdds))
	}
	if !slices.Contains(dels, oldHB) {
		t.Errorf("pre-drop heartbeat handle %d was never released (deleted=%v): it is not in activeNotifications, so the "+
			"reconnect sweep does not snapshot it and one handle leaks per reconnect", oldHB, dels)
	}
}

// TestDeleteNotification_AlreadyGoneStillCleansUpBookkeeping: when the PLC says
// the registration is already gone, the local bookkeeping must go with it.
//
// 0x714 NotifyHandleInvalid and 0x715 DeviceClientUnknown mean the PLC has no such
// registration — after a runtime restart or a dropped client identity, that is the
// normal answer. Returning early on it left the entry in activeNotifications
// forever (every retry gets the same code, so the caller can never delete it) and
// left the config on file, so the next reconnect re-subscribed a symbol the caller
// had explicitly deleted, creating a duplicate PLC registration. The batch sibling
// has always treated these codes as success-equivalent.
func TestDeleteNotification_AlreadyGoneStillCleansUpBookkeeping(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	var nextHandle atomic.Uint32
	nextHandle.Store(0xB00)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	// The PLC no longer knows this registration.
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode {
		return ReturnCodeDeviceNotifyHandleInvalid
	})

	sess, c := newWiredTestSession(t, srv, WithoutNotificationHeartbeat())
	c.SetNotificationHandler(sess.handleNotification)
	preSeedTypedSymbol(sess, "MAIN.gone", 0xF600)
	ch := make(chan *Update, 4)
	handle, err := sess.AddSymbolNotification(context.Background(), "MAIN.gone", 0, 0,
		TransModeServerOnChange, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}

	// The error is still reported — the caller asked for a delete and it did not
	// happen the way they asked — but the state must not be stranded.
	_ = sess.DeleteDeviceNotification(context.Background(), handle)

	sess.notifications.lock.Lock()
	_, stillActive := sess.notifications.activeNotifications[handle]
	stillConfigured := sess.notifications.hasConfig("MAIN.gone")
	sess.notifications.lock.Unlock()

	if stillActive {
		t.Errorf("handle %d still in activeNotifications after the PLC reported it already gone: "+
			"every retry gets the same code, so the caller can never remove it", handle)
	}
	if stillConfigured {
		t.Error("config for MAIN.gone still on file after a delete the PLC confirmed as already gone: " +
			"the next reconnect re-subscribes a symbol the caller deleted")
	}
}

// TestDeleteNotification_ForeignHandleKeepsTheSubscriptionChannel: deleting a
// handle this session does not own must not disturb the ones it does.
//
// The clear of notificationChannel was gated on the map being empty rather than on
// the handle having actually been removed, so a delete for an unknown handle wiped
// the channel whenever the map happened to be empty — exactly the state a sweep
// leaves behind. resubscribeNotifications then returns early on a nil channel while
// the reconnect logs success, and every subscription is silently dropped. This is
// the single-symbol twin of the sum-path bug that hardware caught: a power cycle
// left notifications never resuming.
func TestDeleteNotification_ForeignHandleKeepsTheSubscriptionChannel(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode { return ReturnCodeNoErrors })

	sess, c := newWiredTestSession(t, srv, WithoutNotificationHeartbeat())
	c.SetNotificationHandler(sess.handleNotification)

	// The state a sweep leaves: nothing bound yet, but the caller's intent and
	// channel are on file for the re-subscribe that follows.
	ch := make(chan *Update, 4)
	sess.notifications.lock.Lock()
	sess.notifications.notificationChannel = ch
	sess.notifications.addConfig(NotificationConfig{SymbolName: "MAIN.keepme"})
	sess.notifications.lock.Unlock()

	if err := sess.DeleteDeviceNotification(context.Background(), 0xDEAD); err != nil {
		t.Fatalf("delete of a foreign handle: %v", err)
	}

	sess.notifications.lock.Lock()
	channel := sess.notifications.notificationChannel
	configs := len(sess.notifications.pending)
	sess.notifications.lock.Unlock()
	if channel == nil {
		t.Errorf("notificationChannel was cleared by deleting a handle this session never owned, with %d config(s) still on file: "+
			"resubscribeNotifications returns early on a nil channel while the reconnect reports success", configs)
	}
}

// TestHeartbeat_SymbolVersionChangeDetectedOnce: the beat carries the symbol
// version, so a change shows up for free — but it must be detected once, not on
// every beat forever.
//
// consumeHeartbeat compared the beat's payload against cache.symbolVersion and
// never wrote the new value back, so under SymbolVersionIgnore the same change
// re-fired handleStaleDetection every interval (2s by default): markAllHandlesStale
// plus a fresh versionCallback goroutine each time, against the documented
// once-per-detection contract. Only AutoReload was accidentally safe, because
// LoadSymbols rewrites the field on its way through.
func TestHeartbeat_SymbolVersionChangeDetectedOnce(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	var nextHandle atomic.Uint32
	nextHandle.Store(0xC00)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode { return ReturnCodeNoErrors })

	var detections atomic.Int32
	// A long cycle so the watchdog cannot interfere; the beats here are driven by
	// hand.
	sess, c := newWiredTestSession(t, srv,
		WithNotificationHeartbeat(5*time.Second, 3),
		WithSymbolVersionStrategy(SymbolVersionIgnore),
		WithOnSymbolVersionChanged(func(Reason) { detections.Add(1) }),
	)
	c.SetNotificationHandler(sess.handleNotification)
	sess.cache.lock.Lock()
	sess.cache.symbolVersion = 4
	sess.cache.lock.Unlock()

	preSeedTypedSymbol(sess, "MAIN.ver", 0xF700)
	ch := make(chan *Update, 16)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.ver", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	hb := sess.notifications.heartbeatHandle.Load()
	if hb == 0 {
		t.Fatal("no heartbeat handle")
	}

	// Five beats all reporting the same NEW version: one change, one detection.
	for i := 0; i < 5; i++ {
		if err := sess.drivePacket(sess.currentLifecycleCtx(), buildNotificationPacket(hb, 0, []byte{9})); err != nil {
			t.Fatalf("drivePacket: %v", err)
		}
	}
	time.Sleep(300 * time.Millisecond) // the callback is dispatched on its own goroutine

	if got := detections.Load(); got != 1 {
		t.Errorf("symbol-version change detected %d times across 5 beats reporting the same version, want 1: "+
			"the cached version is never advanced, so every subsequent beat re-detects the same change", got)
	}
	sess.cache.lock.Lock()
	cached := sess.cache.symbolVersion
	sess.cache.lock.Unlock()
	if cached != 9 {
		t.Errorf("cache.symbolVersion = %d after the beat reported 9: the detection never records what it saw", cached)
	}
}

// TestHeartbeat_RecoveryDoesNothingAfterClose: recovery must not touch the PLC
// once the session is closed.
//
// heartbeatWatch checks isClosed() before calling recovery, but that is a TOCTOU:
// Close can land right after it. Recovery would then delete and re-subscribe
// AFTER releasePLCResources had already run, so registrations created by a closed
// session survive in the PLC's table with nothing left to ever delete them, and
// samples stream into a channel the caller considers finished. The watcher was also
// untracked — heartbeatWG was Add/Done'd but never waited — so Close returned while
// all of that was still in flight.
func TestHeartbeat_RecoveryDoesNothingAfterClose(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	var adds, deletes atomic.Int32
	var nextHandle atomic.Uint32
	nextHandle.Store(0xD00)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		adds.Add(1)
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode {
		deletes.Add(1)
		return ReturnCodeNoErrors
	})
	// The resubscribe uses the batch path, so this group has to answer or the test
	// proves nothing: it would fail on a parse error long before reaching the
	// behaviour under test.
	srv.onWriteRead(GroupSumupAddDeviceNotification, func(_ []byte) []byte {
		adds.Add(1)
		return buildSumAddNotifPayload([]sumNotifResponse{{Error: ReturnCodeNoErrors, Handle: nextHandle.Add(1)}})
	})
	srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(req []byte) []byte {
		codes := make([]ReturnCode, len(req)/4)
		for i := range codes {
			deletes.Add(1)
			codes[i] = ReturnCodeNoErrors
		}
		return buildSumDeleteNotifPayload(codes)
	})

	// newDialableTestSession, not newWiredTestSession: the latter's Client comes
	// from Dial with a context of its own, so Session.Close() can never finish
	// waiting for its workers (see that helper's comment).
	srv.onRead(GroupSymbolVersion, func(_, _, _ uint32) (ReturnCode, []byte) {
		return ReturnCodeNoErrors, []byte{5}
	})
	sess := newDialableTestSession(t, srv.host, srv.port, 5)
	sess.heartbeatInterval = 5 * time.Second
	sess.lifecycle.state.transitionTo(SessionStateDisconnected)
	if err := sess.Reconnect(context.Background()); err != nil {
		t.Fatalf("initial reconnect: %v", err)
	}
	preSeedTypedSymbol(sess, "MAIN.afterclose", 0xF800)
	ch := make(chan *Update, 4)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.afterclose", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}

	// The window that matters is INSIDE Close: it marks the session closed and
	// releases the PLC resources, and only then cancels the context. A recovery
	// entering during that stretch still has a live context, so its RPCs land -
	// after the release that was supposed to be the last word. Pin Close there.
	gate := newGateOnLog("releasePLCResources", "")
	sess.logger = slog.New(gate)

	closeDone := make(chan error, 1)
	go func() { closeDone <- sess.Close() }()
	select {
	case <-gate.w.reached:
	case err := <-closeDone:
		t.Fatalf("Close finished without reaching the release log: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Close never reached the release log")
	}

	addsAtClose, deletesAtClose := adds.Load(), deletes.Load()

	// Exactly what the watcher does when Close lands after its isClosed() check.
	done := make(chan struct{})
	go func() { defer close(done); sess.recoverDeadSubscriptions() }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery on a closing session never returned")
	}
	recoveryAdds, recoveryDeletes := adds.Load(), deletes.Load()
	recoveredHB := sess.notifications.heartbeatHandle.Load()

	// establishHeartbeat has its own reachable path onto a closing session — a
	// caller subscribing concurrently with Close — so it carries its own guard.
	// Asserted here, still inside the window: after Close returns the context is
	// cancelled and the RPC would fail regardless, which would make the guard
	// untestable.
	sess.establishHeartbeat(sess.currentLifecycleCtx())
	beatAdds := adds.Load()

	close(gate.w.release)
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}

	if recoveryAdds != addsAtClose {
		t.Errorf("recovery registered %d PLC notification(s) after the session had released its resources: nothing will ever delete them",
			recoveryAdds-addsAtClose)
	}
	if recoveryDeletes != deletesAtClose {
		t.Errorf("recovery issued %d PLC delete(s) on a closing session", recoveryDeletes-deletesAtClose)
	}
	if recoveredHB != 0 {
		t.Errorf("recovery established heartbeat handle %d on a closing session", recoveredHB)
	}
	if beatAdds != recoveryAdds {
		t.Errorf("establishHeartbeat registered a beat on a closing session (%d new add(s)): nothing will delete it", beatAdds-recoveryAdds)
	}
}

// TestHeartbeat_DoesNotSpinWhenTheTransportIsGone: a dead transport is the
// reconnect path's problem, not the heartbeat's.
//
// Found on hardware, in our own integration run against .224: 1468 copies of the
// silence warning, 28% of the whole log, each followed by "batch add notification
// failed: ads: client transport closed". The watcher treated a closed transport
// exactly like a PLC sitting in CONFIG — worth retrying on the very next tick,
// forever — so a session whose transport died spun at the heartbeat interval for
// the rest of the process, re-queueing configs and burning resubscribe attempts
// every time.
//
// Two properties are asserted: silence on a transport that cannot carry a
// re-subscribe, and an exit once the session is closed. The latter is why the
// flood in that run outlived its own test by 666 tests.
func TestHeartbeat_DoesNotSpinWhenTheTransportIsGone(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	var nextHandle atomic.Uint32
	nextHandle.Store(0xE00)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode { return ReturnCodeNoErrors })

	logs := &testLogHandler{}
	sess, c := newWiredTestSession(t, srv,
		WithNotificationHeartbeat(50*time.Millisecond, 2),
		WithLogger(slog.New(logs)))
	c.SetNotificationHandler(sess.handleNotification)
	preSeedTypedSymbol(sess, "MAIN.spin", 0xF900)
	ch := make(chan *Update, 4)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.spin", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}

	// The transport dies without the session being closed — the state the reconnect
	// path exists to resolve.
	_ = c.Close()

	// ~30 ticks at 50ms. One complaint is fine; one per tick is the bug.
	time.Sleep(1500 * time.Millisecond)
	// Zero, not "a few": with the transport gone there is nothing to attempt, so
	// the watcher should not even reach its complaint. A bounded count here would
	// be satisfied by the backoff alone and would leave the transport check
	// untested.
	if got := logs.countByMessage("no notification heartbeat within the allowed window"); got != 0 {
		t.Errorf("watcher complained %d time(s) in ~30 ticks against a closed transport: it is retrying a resubscribe that cannot "+
			"work, and would keep doing so for the life of the process", got)
	}

	// And it must stop for good once the session is closed.
	sess.markClosed()
	time.Sleep(300 * time.Millisecond)
	before := logs.countByMessage("no notification heartbeat within the allowed window")
	time.Sleep(500 * time.Millisecond)
	if after := logs.countByMessage("no notification heartbeat within the allowed window"); after != before {
		t.Errorf("watcher logged %d more time(s) after the session was closed: the goroutine outlives its session", after-before)
	}
}

// TestRuntimeState_RefusesSymbolWorkOutsideRun: when the system service says the
// runtime is not in RUN, symbol and subscription calls must refuse and say why.
//
// Measured on TC3.1.4024 in CONFIG: every request to the runtime port 851 came back
// with AMS ErrorCode 6 (target port not found), while port 10000 answered
// ADSState=15. The library discarded the AMS error and parsed the response body
// anyway, so a subscribe failed with "0xF008: unknown error code" — an index group
// formatted as a return code. Asking the system service turns that into a fact.
func TestRuntimeState_RefusesSymbolWorkOutsideRun(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		return addNotifResponse{Handle: 0x1234}
	})

	sess, c := newWiredTestSession(t, srv, WithoutNotificationHeartbeat())
	c.SetNotificationHandler(sess.handleNotification)
	preSeedTypedSymbol(sess, "MAIN.cfg", 0xFC00)
	preSeedTypedSymbol(sess, "MAIN.cfg2", 0xFC01)
	preSeedTypedSymbol(sess, "MAIN.cfg3", 0xFC02)
	ch := make(chan *Update, 4)

	// No reading yet: the gate must permit, or every device that does not serve the
	// system service port would stop working.
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.cfg", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Fatalf("subscribe refused with no runtime-state reading: %v", err)
	}

	// Now the system service reports CONFIG.
	sess.recordRuntimeState(ADSStateConfig)
	_, err := sess.AddSymbolNotification(context.Background(), "MAIN.cfg2", 0, 0,
		TransModeServerOnChange, ch)
	if err == nil {
		t.Error("subscribe succeeded although the runtime is in CONFIG: the runtime port does not exist in that state, so this " +
			"can only fail later and obscurely")
	}
	if !errors.Is(err, ErrRuntimeNotRunning) {
		t.Errorf("error = %v, want one wrapping ErrRuntimeNotRunning so callers can branch on it", err)
	}
	if err != nil && !strings.Contains(err.Error(), "15") {
		t.Errorf("error %q does not name the state; the operator needs to know it is CONFIG, not just that something failed", err)
	}
	if lerr := sess.LoadSymbols(context.Background()); !errors.Is(lerr, ErrRuntimeNotRunning) {
		t.Errorf("LoadSymbols error = %v, want ErrRuntimeNotRunning", lerr)
	}

	// Back to RUN: work is allowed again without rebuilding anything.
	sess.recordRuntimeState(ADSStateRun)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.cfg3", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Errorf("subscribe still refused after the runtime returned to RUN: %v", err)
	}
}

// TestRuntimeState_PollReportsTheState: the state has to be discovered by the
// session, not only by a caller who asks.
//
// It is a poll on purpose. There is nothing to subscribe to that survives the
// transition being watched: in CONFIG the runtime port that would carry a
// notification does not exist. One small request per heartbeat interval to a port
// that is up whenever the device is.
func TestRuntimeState_PollReportsTheState(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	srv.setADSState(ADSStateConfig)

	sess, _ := newWiredTestSession(t, srv, WithNotificationHeartbeat(100*time.Millisecond, 3))
	if state, known := sess.knownRuntimeState(); known {
		t.Fatalf("state already known before polling: %v", state)
	}
	sess.startRuntimeStateWatch()

	deadline := time.Now().Add(3 * time.Second)
	for {
		if state, known := sess.knownRuntimeState(); known {
			if state != ADSStateConfig {
				t.Errorf("polled state = %v, want CONFIG", state)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the runtime state was never polled: the session cannot tell CONFIG from a broken device")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// And it must notice the way back.
	srv.setADSState(ADSStateRun)
	deadline = time.Now().Add(3 * time.Second)
	for {
		if state, _ := sess.knownRuntimeState(); state == ADSStateRun {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the poll never saw the return to RUN, so the session would refuse subscriptions forever")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestHeartbeat_DetectionSurvivesABackwardClockStep: silence is measured in ticks,
// not in wall-clock time.
//
// The detector used to compare time.Now().UnixNano() against a stored timestamp,
// and time.Unix carries no monotonic reading, so a wall-clock STEP was read as
// elapsed time. Both directions were wrong: a forward step declared every live
// subscription dead and re-registered all of them for nothing, and a BACKWARD step
// left time.Since negative, so the watchdog went blind for the length of the step
// while subscriptions may genuinely have been gone. Not exotic on this hardware —
// an IPC without a battery-backed RTC steps years forward on its first NTP sync,
// and a suspended VM resumes with a jump.
//
// A future timestamp is exactly what a backward step leaves behind, so setting one
// reproduces it deterministically.
func TestHeartbeat_DetectionSurvivesABackwardClockStep(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	var adds atomic.Int32
	var nextHandle atomic.Uint32
	nextHandle.Store(0xFD00)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		adds.Add(1)
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode { return ReturnCodeNoErrors })

	sess, c := newWiredTestSession(t, srv, WithNotificationHeartbeat(100*time.Millisecond, 2))
	c.SetNotificationHandler(sess.handleNotification)
	if !c.capabilities.SumAddNotifStateCAS(0, 2) || !c.capabilities.SumDeleteNotifStateCAS(0, 2) {
		t.Fatal("could not force the sum commands into the unsupported state")
	}
	preSeedTypedSymbol(sess, "MAIN.clock", 0xFD10)
	ch := make(chan *Update, 8)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.clock", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	afterSetup := adds.Load()

	// The clock jumps backwards by an hour: the stored "last beat" is now an hour in
	// the future. Under the old timestamp comparison this made silence unmeasurable.
	sess.notifications.heartbeatLastNs.Store(time.Now().Add(time.Hour).UnixNano())

	// Beats stop. The detector must still fire, because it counts ticks.
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if adds.Load() > afterSetup {
			return // re-subscribed despite the clock being wrong
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("beats stopped but nothing was re-subscribed within 4s while the stored timestamp sat in the future: "+
		"detection is still keyed on the wall clock, so a backward step blinds it (Adds stayed at %d)", afterSetup)
}

// TestHeartbeat_ConcurrentSubscribesEstablishOneBeat: two subscribes racing on a
// fresh session must leave exactly one cyclic registration on the PLC.
//
// establishHeartbeat checked the handle and then stored it, so both callers could
// see zero, both register, and the second Store orphan the first — a cyclic
// registration the PLC keeps pushing that belongs to nothing, reclaimed only by the
// orphan reaper.
func TestHeartbeat_ConcurrentSubscribesEstablishOneBeat(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var cyclicAdds atomic.Int32
	var deletes atomic.Int32
	var nextHandle atomic.Uint32
	nextHandle.Store(0xFE00)
	srv.onAddDeviceNotification(func(req addNotifRequest) addNotifResponse {
		if Group(req.Group) == GroupSymbolVersion && req.TransMode == uint32(TransModeServerCycle) {
			cyclicAdds.Add(1)
			// Wide enough that both callers are inside the round-trip together.
			time.Sleep(150 * time.Millisecond)
		}
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode {
		deletes.Add(1)
		return ReturnCodeNoErrors
	})

	sess, c := newWiredTestSession(t, srv, WithNotificationHeartbeat(10*time.Second, 3))
	c.SetNotificationHandler(sess.handleNotification)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); sess.establishHeartbeat(context.Background()) }()
	}
	wg.Wait()

	hb := sess.notifications.heartbeatHandle.Load()
	if hb == 0 {
		t.Fatal("no heartbeat established by either caller")
	}
	if got := cyclicAdds.Load(); got == 2 && deletes.Load() == 0 {
		t.Errorf("both callers registered a cyclic notification (%d adds) and neither released the loser: the PLC is left "+
			"pushing a beat that belongs to nothing", got)
	}
	if cyclicAdds.Load() > 1 && deletes.Load() < cyclicAdds.Load()-1 {
		t.Errorf("%d cyclic registrations, only %d released", cyclicAdds.Load(), deletes.Load())
	}
}

// TestHeartbeat_RetriesAfterAFailedEstablish: a first attempt that the PLC refuses
// must not leave the session without a watchdog for its entire life.
//
// startHeartbeatWatch ran only after a SUCCESSFUL establish. So if the very first
// cyclic subscribe was refused, no watcher existed, nothing ever retried, and a
// later silent subscription death went unnoticed with one Warn as the only trace.
// None of the three firmwares on the bench does that — TC2 2.10, TC3.1.4024 and
// TC3.1.4026 all accept a cyclic subscribe on 0xF008 — so this is a hazard on
// unobserved firmware, guarded because the failure is silent and the fix is small.
func TestHeartbeat_RetriesAfterAFailedEstablish(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var refuse atomic.Bool
	refuse.Store(true)
	var cyclicAttempts atomic.Int32
	var nextHandle atomic.Uint32
	nextHandle.Store(0xFF00)
	srv.onAddDeviceNotification(func(req addNotifRequest) addNotifResponse {
		if Group(req.Group) == GroupSymbolVersion && req.TransMode == uint32(TransModeServerCycle) {
			cyclicAttempts.Add(1)
			if refuse.Load() {
				return addNotifResponse{Error: ReturnCodeDeviceServiceNotSupported}
			}
		}
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode { return ReturnCodeNoErrors })

	sess, c := newWiredTestSession(t, srv, WithNotificationHeartbeat(100*time.Millisecond, 2))
	c.SetNotificationHandler(sess.handleNotification)
	preSeedTypedSymbol(sess, "MAIN.nobeat", 0xFF10)
	ch := make(chan *Update, 4)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.nobeat", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	if sess.notifications.heartbeatHandle.Load() != 0 {
		t.Fatal("the stub was supposed to refuse the cyclic subscribe")
	}
	if cyclicAttempts.Load() == 0 {
		t.Fatal("no cyclic subscribe was attempted; the test proves nothing")
	}

	// The PLC starts accepting it. Nobody subscribes again, so only a watcher can
	// pick this up.
	refuse.Store(false)
	deadline := time.Now().Add(4 * time.Second)
	for {
		if hb := sess.notifications.heartbeatHandle.Load(); hb != 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the heartbeat was never re-attempted after the first refusal (%d cyclic attempts): the session has no "+
				"watchdog and will not notice its subscriptions dying", cyclicAttempts.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestHeartbeat_RecoveryKeepsALargeConfigSet: recovery must not shed subscriptions
// when there are many of them.
//
// Every failed recovery re-queues every config and increments its per-config
// resubscribe counter, and a config is dropped once that counter hits
// resubscribeMaxAttempts. With one subscription that behaviour is invisible; the
// defect that motivated this only appeared at 40 symbols on hardware, where a
// power cycle produced "bound notifications = 24, want 40". So exercise it at a
// size where partial loss is visible.
func TestHeartbeat_RecoveryKeepsALargeConfigSet(t *testing.T) {
	const symbols = 40

	srv := startScriptableServer(t)
	defer srv.stop()

	var serving atomic.Bool
	var nextHandle atomic.Uint32
	nextHandle.Store(0x2000)
	srv.onWriteRead(GroupSumupAddDeviceNotification, func(req []byte) []byte {
		items := make([]sumNotifResponse, len(req)/40)
		for i := range items {
			if !serving.Load() {
				items[i] = sumNotifResponse{Error: ReturnCodeDeviceServiceNotSupported}
				continue
			}
			items[i] = sumNotifResponse{Error: ReturnCodeNoErrors, Handle: nextHandle.Add(1)}
		}
		return buildSumAddNotifPayload(items)
	})
	srv.onWriteRead(GroupSumupDeleteDeviceNotification, func(req []byte) []byte {
		codes := make([]ReturnCode, len(req)/4)
		for i := range codes {
			codes[i] = ReturnCodeNoErrors
		}
		return buildSumDeleteNotifPayload(codes)
	})
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		if !serving.Load() {
			return addNotifResponse{Error: ReturnCodeDeviceServiceNotSupported}
		}
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode { return ReturnCodeNoErrors })

	serving.Store(true)
	sess, c := newWiredTestSession(t, srv, WithNotificationHeartbeat(80*time.Millisecond, 2))
	c.SetNotificationHandler(sess.handleNotification)

	configs := make([]NotificationConfig, 0, symbols)
	for i := 0; i < symbols; i++ {
		name := fmt.Sprintf("MAIN.bulk%02d", i)
		preSeedTypedSymbol(sess, name, uint32(0x3000+i))
		configs = append(configs, NotificationConfig{SymbolName: name, TransmissionMode: TransModeServerOnChange})
	}
	ch := make(chan *Update, 256)
	results, err := sess.AddSymbolNotifications(context.Background(), configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications: %v", err)
	}
	bound := 0
	for _, r := range results {
		if r.Skipped == nil && r.Handle != 0 {
			bound++
		}
	}
	if bound != symbols {
		t.Fatalf("bound %d/%d symbols up front", bound, symbols)
	}

	// The PLC stops serving: beats stop AND every re-subscribe is refused, for long
	// enough that the per-config retry budget would be spent several times over.
	serving.Store(false)
	time.Sleep(2 * time.Second)

	// Back to serving. Nothing may have been dropped in the meantime.
	serving.Store(true)
	deadline := time.Now().Add(10 * time.Second)
	for {
		sess.notifications.lock.Lock()
		active := len(sess.notifications.activeNotifications)
		pending := len(sess.notifications.pending)
		sess.notifications.lock.Unlock()
		if active == symbols {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after recovery %d/%d subscriptions are bound (%d configs still on file): a recovery that fails while the "+
				"PLC is unavailable must not shed the caller's subscriptions", active, symbols, pending)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestRuntimeState_RefusesOnlyMeasuredStates: the gate must refuse only where a
// runtime port provably does not serve.
//
// An earlier version refused on anything that was not RUN, then on a list that
// included STOP and SHUTDOWN by inference. Only CONFIG and RECONFIG are measured
// (TC3.1.4024 in CONFIG reports 15 and answers AMS ErrorCode 6 for every request to
// a runtime port). STOP was seen only as a ~4s way-point during a CONFIG -> RUN
// switch, so whether a device can idle there while serving is unknown — and
// refusing every subscribe on such a device, with no PLC error to explain it, is
// worse than attempting the call.
func TestRuntimeState_RefusesOnlyMeasuredStates(t *testing.T) {
	refuse := []ADSState{ADSStateConfig, ADSStateReconfig}
	permit := []ADSState{ADSStateRun, ADSStateStop, ADSStateShutdown, ADSStateIdle, ADSStateStart, ADSStateInvalid, ADSState(99)}

	for _, state := range refuse {
		if !runtimeDefinitelyNotServing(state) {
			t.Errorf("state %d should be refused: it is measured to have no serving runtime port", uint16(state))
		}
	}
	for _, state := range permit {
		if runtimeDefinitelyNotServing(state) {
			t.Errorf("state %d is refused on inference rather than evidence; attempting the call and letting the PLC answer is "+
				"the safer default", uint16(state))
		}
	}
}

// TestRuntimeState_ReadingExpires: a state reading must not outlive its usefulness.
//
// The watch gives up after a run of failed polls, and before this a session that had
// seen CONFIG then kept that verdict forever — refusing every symbol and subscribe
// call for the rest of its life with nothing left to notice the runtime returning.
// Failing OPEN is deliberate: the worst case is the behaviour that predates the
// gate.
func TestRuntimeState_ReadingExpires(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	sess, _ := newWiredTestSession(t, srv, WithoutNotificationHeartbeat())

	sess.recordRuntimeState(ADSStateConfig)
	if _, known := sess.knownRuntimeState(); !known {
		t.Fatal("a fresh reading is not known")
	}
	if err := sess.requireRunningRuntime("probe"); err == nil {
		t.Fatal("a fresh CONFIG reading must refuse")
	}

	// Age it past the TTL, as an abandoned poll would.
	sess.runtimeStateNs.Store(time.Now().Add(-2 * runtimeStateTTL).UnixNano())
	if _, known := sess.knownRuntimeState(); known {
		t.Error("a stale reading is still reported as known: nothing refreshes it once the watch has given up, so the gates " +
			"would refuse for the life of the session")
	}
	if err := sess.requireRunningRuntime("probe"); err != nil {
		t.Errorf("a stale reading still refuses: %v", err)
	}
}

// TestHeartbeat_DeferralsKeepAConstantRate: waiting for a runtime that is not
// serving must not make the next check later.
//
// Found on hardware, .118 with 40 symbols: the CONFIG toggle was detected and the
// re-subscribe correctly deferred, but every deferral counted as a recovery FAILURE,
// so the backoff doubled each window. By the time the runtime returned to RUN the
// next attempt was minutes out and the session missed a 2 minute grace entirely.
//
// Measured as a RATE, not as "did it recover": a recovery-after-RUN assertion is
// also satisfied by the return-to-RUN nudge, so it cannot tell the two fixes apart.
// A deferral attempts nothing, so its interval must stay at the base window.
func TestHeartbeat_DeferralsKeepAConstantRate(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()
	var nextHandle atomic.Uint32
	nextHandle.Store(0x5000)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode { return ReturnCodeNoErrors })

	logs := &testLogHandler{}
	sess, c := newWiredTestSession(t, srv,
		WithNotificationHeartbeat(100*time.Millisecond, 2),
		WithLogger(slog.New(logs)))
	c.SetNotificationHandler(sess.handleNotification)
	preSeedTypedSymbol(sess, "MAIN.deferred", 0x5100)
	ch := make(chan *Update, 8)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.deferred", 0, 0,
		TransModeServerOnChange, ch); err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}

	// The runtime is in CONFIG for 1.5s. At a 100ms cycle and 2 missed ticks the
	// base window is 200ms, so a constant rate is ~7 deferrals. Doubling gives
	// 200ms, 400ms, 800ms, 1600ms — three at most inside the same window.
	sess.recordRuntimeState(ADSStateConfig)
	time.Sleep(1500 * time.Millisecond)

	got := logs.countByMessage("re-subscribe deferred")
	if got < 5 {
		t.Errorf("only %d deferrals in 1.5s (base window 200ms, so ~7 expected): the interval is growing, which means a "+
			"deferral is being counted as a failure — it attempts nothing, so it says nothing about how hard recovery is", got)
	}
}

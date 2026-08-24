package ads

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// notification_race_test.go — subscribe-race regression tests.
//
// Regression guarded here (shipped in v2.2.0, bisected on TC2 hardware
// 2026-08-20): the PLC-side notification handle exists the moment
// AddDeviceNotification returns, so the PLC can emit the first sample before
// activeNotifications carries the handle. Two failures followed from that:
//
//  1. The sample was dropped. Harmless for a cyclic tag (later samples land
//     on the normal path) but total loss for a static symbol, which emits
//     exactly once at subscribe.
//  2. The orphan reaper treated the handle as leaked and deleted the
//     subscription off the PLC, so the tag never streamed again. On TC2 —
//     which answers 0x0701 to the sum command, forcing one Add per symbol —
//     a 40-symbol batch takes far longer than the 100 ms race window, and
//     30 of 40 tags were reaped.

// preSeedTypedSymbol primes the cache with a symbol whose handle is non-zero
// so getSymbol resolves without a GetHandleByName roundtrip. INT/2 parses
// against a nil datatypes map.
func preSeedTypedSymbol(sess *Session, name string, handle uint32) *symbol {
	sym := &symbol{
		FullName: name,
		Name:     name,
		DataType: "INT",
		Length:   2,
		Handle:   handle,
	}
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey(name)] = sym
	sess.cache.lock.Unlock()
	return sym
}

func intSample(v uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, v)
	return b
}

// ageOpenSubscribe backdates an open subscribe's start time, standing in for an
// RPC that has been in flight for a long time without waiting for one.
func ageOpenSubscribe(sess *Session, tok subscribeToken, start time.Time) {
	sess.notifications.openMu.Lock()
	defer sess.notifications.openMu.Unlock()
	sess.notifications.openSubscribes[tok] = start.UnixNano()
}

// earlySampleCount reports how many handles currently hold a buffered sample.
func earlySampleCount(sess *Session) int {
	sess.notifications.earlyMu.Lock()
	defer sess.notifications.earlyMu.Unlock()
	return len(sess.notifications.earlySamples)
}

// TestSubscribeRace_EarlySampleReplayedAfterCommit drives the exact ordering
// that loses a static symbol: the PLC emits the first (and, for a constant,
// only) sample while AddDeviceNotification is still in flight. The sample
// must be buffered and replayed once the handle is committed, not dropped.
func TestSubscribeRace_EarlySampleReplayedAfterCommit(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var deleted atomic.Int32
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode {
		deleted.Add(1)
		return ReturnCodeNoErrors
	})

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	preSeedTypedSymbol(sess, "MAIN.sMachineName", 0xC0DE)

	const plcHandle = 0x1234
	var addSeen atomic.Int32
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		// Only the caller's subscription (the first Add) gets plcHandle and the
		// early sample; anything else — the session's own cyclic heartbeat — must
		// get a DIFFERENT handle, as a real PLC would. A stub handing the same
		// handle to two subscriptions makes the heartbeat swallow the caller's
		// samples, which no device can actually do.
		if addSeen.Add(1) > 1 {
			return addNotifResponse{Handle: plcHandle + uint32(addSeen.Load())}
		}
		// Fire the sample BEFORE the Add response goes back on the wire, so
		// the commit into activeNotifications cannot have happened yet.
		if err := sess.drivePacket(sess.lifecycle.ctx, buildNotificationPacket(plcHandle, 0, intSample(4242))); err != nil {
			t.Errorf("drivePacket from Add handler: %v", err)
		}
		return addNotifResponse{Handle: plcHandle}
	})

	ch := make(chan *Update, 4)
	handle, err := sess.AddSymbolNotification(context.Background(), "MAIN.sMachineName", 0, 0, TransModeServerOnChange, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotification: %v", err)
	}
	if handle != plcHandle {
		t.Fatalf("handle = 0x%X, want 0x%X", handle, plcHandle)
	}

	select {
	case u := <-ch:
		if u.Value != "4242" {
			t.Errorf("Update.Value = %q, want %q", u.Value, "4242")
		}
		if u.Variable != "MAIN.sMachineName" {
			t.Errorf("Update.Variable = %q, want %q", u.Variable, "MAIN.sMachineName")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("early sample never delivered: buffered sample was not replayed after commit")
	}

	if got := deleted.Load(); got != 0 {
		t.Errorf("Delete RPC calls = %d, want 0 (our own in-flight handle must never be reaped)", got)
	}
	if got := earlySampleCount(sess); got != 0 {
		t.Errorf("buffered samples after commit = %d, want 0", got)
	}
}

// TestSubscribeRace_BatchOnSumUnsupportedPLC reproduces the TC2 shape: the sum
// command is unsupported, so AddSymbolNotifications degrades to one Add per
// symbol with enough latency that the batch outlasts subscribeRaceWindow. The
// first symbol streams while the last is still registering. Every early sample
// must survive and no handle may be reaped.
func TestSubscribeRace_BatchOnSumUnsupportedPLC(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var deleted atomic.Int32
	var deletedHandles []uint32
	var delMu sync.Mutex
	srv.onDeleteDeviceNotification(func(h uint32) ReturnCode {
		delMu.Lock()
		deletedHandles = append(deletedHandles, h)
		delMu.Unlock()
		deleted.Add(1)
		return ReturnCodeNoErrors
	})
	// 40ms per Add over 5 symbols puts the batch well past the 100ms window.
	srv.delayBefore(CommandIDAddDeviceNotification, 0, 40*time.Millisecond)

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	// Mark the sum command unsupported, as TC2 does by answering 0x0701 to
	// group 0xF085. SumAddDeviceNotification then degrades to one Add per
	// symbol — the condition under which the batch outlasts the race window.
	if !c.capabilities.SumAddNotifStateCAS(0, 2) {
		t.Fatal("could not force SumAddNotif into the unsupported state")
	}

	const symbolCount = 5
	names := make([]string, symbolCount)
	configs := make([]NotificationConfig, symbolCount)
	for i := range names {
		names[i] = "MAIN.tag" + string(rune('A'+i))
		preSeedTypedSymbol(sess, names[i], uint32(0xC000+i))
		configs[i] = NotificationConfig{SymbolName: names[i], TransmissionMode: TransModeServerOnChange}
	}

	var nextHandle atomic.Uint32
	nextHandle.Store(0x100)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		h := nextHandle.Add(1)
		// Every symbol emits its one sample immediately, before its own Add
		// response is even sent — the worst case of the race.
		if err := sess.drivePacket(sess.lifecycle.ctx, buildNotificationPacket(h, 0, intSample(uint16(h)))); err != nil {
			t.Errorf("drivePacket from Add handler: %v", err)
		}
		return addNotifResponse{Handle: h}
	})

	ch := make(chan *Update, 2*symbolCount)
	results, err := sess.AddSymbolNotifications(context.Background(), configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications: %v", err)
	}
	for i, r := range results {
		if r.Skipped != nil {
			t.Fatalf("results[%d] (%s) Skipped = %v", i, names[i], r.Skipped)
		}
		if r.Error != ReturnCodeNoErrors {
			t.Fatalf("results[%d] (%s) Error = 0x%X", i, names[i], uint32(r.Error))
		}
	}

	got := make(map[string]string, symbolCount)
	deadline := time.After(3 * time.Second)
	for len(got) < symbolCount {
		select {
		case u := <-ch:
			got[u.Variable] = u.Value
		case <-deadline:
			t.Fatalf("delivered %d/%d symbols, want all; missing early samples were dropped: got=%v", len(got), symbolCount, got)
		}
	}
	for _, name := range names {
		if _, ok := got[name]; !ok {
			t.Errorf("no Update for %s", name)
		}
	}

	// Give any scheduled reaper goroutine time to fire before asserting.
	time.Sleep(200 * time.Millisecond)
	if n := deleted.Load(); n != 0 {
		delMu.Lock()
		handles := append([]uint32(nil), deletedHandles...)
		delMu.Unlock()
		t.Errorf("Delete RPC calls = %d, want 0 (reaper deleted live subscriptions): handles=%v", n, handles)
	}
}

// TestSubscribeRace_BatchBindsEachHandleBeforeNextAdd pins the per-item bind:
// on a PLC that rejects the sum command, each handle must be in
// activeNotifications before the next AddDeviceNotification is issued. Binding
// the whole batch at the end instead leaves the early handles unrecognisable
// for the rest of the batch, which is what the PLC is streaming into.
func TestSubscribeRace_BatchBindsEachHandleBeforeNextAdd(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	if !c.capabilities.SumAddNotifStateCAS(0, 2) {
		t.Fatal("could not force SumAddNotif into the unsupported state")
	}

	const symbolCount = 4
	names := make([]string, symbolCount)
	configs := make([]NotificationConfig, symbolCount)
	for i := range names {
		names[i] = "MAIN.bind" + string(rune('A'+i))
		preSeedTypedSymbol(sess, names[i], uint32(0xD000+i))
		configs[i] = NotificationConfig{SymbolName: names[i], TransmissionMode: TransModeServerOnChange}
	}

	var mu sync.Mutex
	var issued []uint32 // handles already returned by a previous Add
	var unboundAtNextAdd []uint32
	var nextHandle atomic.Uint32
	nextHandle.Store(0x200)

	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		// Every handle handed out earlier in this batch must be bound by now.
		mu.Lock()
		prior := append([]uint32(nil), issued...)
		mu.Unlock()
		sess.notifications.lock.Lock()
		for _, h := range prior {
			if _, ok := sess.notifications.activeNotifications[h]; !ok {
				unboundAtNextAdd = append(unboundAtNextAdd, h)
			}
		}
		sess.notifications.lock.Unlock()

		h := nextHandle.Add(1)
		mu.Lock()
		issued = append(issued, h)
		mu.Unlock()
		return addNotifResponse{Handle: h}
	})

	ch := make(chan *Update, 4*symbolCount)
	results, err := sess.AddSymbolNotifications(context.Background(), configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications: %v", err)
	}
	for i, r := range results {
		if r.Skipped != nil || r.Error != ReturnCodeNoErrors {
			t.Fatalf("results[%d] (%s): Skipped=%v Error=0x%X", i, names[i], r.Skipped, uint32(r.Error))
		}
	}
	if len(unboundAtNextAdd) != 0 {
		t.Errorf("handles still unbound when the next Add was issued: %v", unboundAtNextAdd)
	}

	sess.notifications.lock.Lock()
	active := len(sess.notifications.activeNotifications)
	sess.notifications.lock.Unlock()
	if active != symbolCount {
		t.Errorf("activeNotifications = %d, want %d", active, symbolCount)
	}
}

// TestSubscribeRace_ReloadMidBatchStrandsWholeBatch: a symbol-cache reload
// landing partway through a batch invalidates the entries committed before it
// (the reload snapshots activeNotifications and deletes those handles PLC-side)
// as well as everything after it. Both halves must come back Skipped with
// ErrNotificationStrandedByReload — reporting the first half as success hands
// the caller subscriptions that exist on neither side.
//
// The epoch is bumped from inside the Add handler, which is exactly where a
// real reload lands: between two individual Adds of a sum-unsupported batch.
func TestSubscribeRace_ReloadMidBatchStrandsWholeBatch(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	if !c.capabilities.SumAddNotifStateCAS(0, 2) {
		t.Fatal("could not force SumAddNotif into the unsupported state")
	}

	const symbolCount = 4
	names := make([]string, symbolCount)
	configs := make([]NotificationConfig, symbolCount)
	for i := range names {
		names[i] = "MAIN.reload" + string(rune('A'+i))
		preSeedTypedSymbol(sess, names[i], uint32(0xE000+i))
		configs[i] = NotificationConfig{SymbolName: names[i], TransmissionMode: TransModeServerOnChange}
	}

	var adds atomic.Int32
	var nextHandle atomic.Uint32
	nextHandle.Store(0x300)
	// Written on the stub's goroutine, read on the test's: guarded, because a
	// data race here would be reported as a failure of the code under test.
	var reapedMu sync.Mutex
	var reapedByReload []uint32
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		// After the second Add, do what autoReloadOnStaleDetection actually does
		// to shared state, in its real order: bump the epoch FIRST, then
		// snapshot activeNotifications and swap the map. The amendment's
		// correctness depends on exactly that ordering, so the test has to
		// reproduce it rather than just move the counter.
		if adds.Add(1) == 2 {
			sess.bumpEpoch()
			sess.notifications.lock.Lock()
			for h := range sess.notifications.activeNotifications {
				reapedMu.Lock()
				reapedByReload = append(reapedByReload, h)
				reapedMu.Unlock()
			}
			sess.notifications.activeNotifications = make(map[uint32]activeNotification)
			sess.notifications.lock.Unlock()
		}
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})

	ch := make(chan *Update, symbolCount)
	results, err := sess.AddSymbolNotifications(context.Background(), configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications: %v", err)
	}

	for i, r := range results {
		if r.Skipped == nil {
			t.Errorf("results[%d] (%s) reported success, but the reload invalidated it", i, names[i])
			continue
		}
		if !errors.Is(r.Skipped, ErrNotificationStrandedByReload) {
			t.Errorf("results[%d] (%s) Skipped = %v, want ErrNotificationStrandedByReload", i, names[i], r.Skipped)
		}
		// The PLC created the registration before the library refused it, so the
		// handle has to reach the caller for cleanup.
		if r.Handle == 0 {
			t.Errorf("results[%d] (%s) Handle = 0; the caller cannot release what it is not told about", i, names[i])
		}
	}
	// The entries the reload swept are precisely the ones reported as stranded:
	// that equality is the invariant the amendment relies on.
	reapedMu.Lock()
	sweptCount := len(reapedByReload)
	reapedMu.Unlock()
	if sweptCount == 0 {
		t.Error("the simulated reload swept nothing — the test did not reproduce the interleaving it claims to")
	}
	sess.notifications.lock.Lock()
	surviving := len(sess.notifications.activeNotifications)
	sess.notifications.lock.Unlock()
	if surviving != 0 {
		t.Errorf("activeNotifications = %d, want 0 (nothing may be committed after the reload)", surviving)
	}
}

// TestSubscribeRace_PlainSymbolReloadDoesNotStrandBatch: an epoch bump is not by
// itself evidence that anything was stranded. A caller running LoadSymbols /
// RefreshSymbols in parallel advances the epoch but never touches
// activeNotifications, so entries this batch bound are alive on both sides. The
// amendment used to key on the epoch and tore them down anyway: reported
// Skipped, then deleted PLC-side — a working subscription destroyed by an
// unrelated cache refresh. It must key on an actual notification sweep.
func TestSubscribeRace_PlainSymbolReloadDoesNotStrandBatch(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	if !c.capabilities.SumAddNotifStateCAS(0, 2) {
		t.Fatal("could not force SumAddNotif into the unsupported state")
	}

	const symbolCount = 3
	names := make([]string, symbolCount)
	configs := make([]NotificationConfig, symbolCount)
	for i := range names {
		names[i] = "MAIN.plain" + string(rune('A'+i))
		preSeedTypedSymbol(sess, names[i], uint32(0xC100+i))
		configs[i] = NotificationConfig{SymbolName: names[i], TransmissionMode: TransModeServerOnChange}
	}

	var deletedMu sync.Mutex
	var deleted []uint32
	srv.onDeleteDeviceNotification(func(h uint32) ReturnCode {
		deletedMu.Lock()
		deleted = append(deleted, h)
		deletedMu.Unlock()
		return ReturnCodeNoErrors
	})

	var nextHandle atomic.Uint32
	nextHandle.Store(0x800)
	var adds atomic.Int32
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		// A concurrent LoadSymbols lands after the first commit: epoch moves,
		// activeNotifications is untouched.
		if adds.Add(1) == 2 {
			sess.bumpEpoch()
		}
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})

	ch := make(chan *Update, symbolCount)
	results, err := sess.AddSymbolNotifications(context.Background(), configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications: %v", err)
	}

	// Item 0 was committed before the bump and nothing swept it: it stays a
	// success. (Items after the bump are legitimately refused — their symbol
	// handles came from the pre-reload cache.)
	if results[0].Skipped != nil {
		t.Errorf("results[0] (%s) Skipped = %v; a plain symbol reload strands nothing", names[0], results[0].Skipped)
	}
	if results[0].Handle == 0 {
		t.Errorf("results[0] (%s) has no handle despite committing", names[0])
	}

	sess.notifications.lock.Lock()
	_, stillBound := sess.notifications.activeNotifications[results[0].Handle]
	sess.notifications.lock.Unlock()
	if !stillBound {
		t.Errorf("handle %d is no longer bound; the batch unwound a live subscription", results[0].Handle)
	}
	deletedMu.Lock()
	releasedFirst := slices.Contains(deleted, results[0].Handle)
	deletedMu.Unlock()
	if releasedFirst {
		t.Errorf("handle %d was released PLC-side; the caller still holds it", results[0].Handle)
	}
}

// TestSubscribeRace_PostSweepCommitIsNotStranded: "a sweep happened" is not the
// same question as "was MY entry swept". A batch item that commits AFTER the
// sweep goes into the fresh activeNotifications map — nothing swept it, and its
// handle is in no snapshot anyone will delete. Reporting it stranded is a double
// failure: the caller is told a working subscription is gone, and the library
// then deletes the handle out from under itself.
//
// The reconnect path is where this bites. Auto-reload bumps the epoch before it
// sweeps, so commitNotification refuses late commits; Reconnect does not, so the
// late commit succeeds and only the sweep-vs-batch comparison can catch it.
func TestSubscribeRace_PostSweepCommitIsNotStranded(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	if !c.capabilities.SumAddNotifStateCAS(0, 2) {
		t.Fatal("could not force SumAddNotif into the unsupported state")
	}
	if !c.capabilities.SumDeleteNotifStateCAS(0, 2) {
		t.Fatal("could not force SumDeleteNotif into the unsupported state")
	}

	const symbolCount = 2
	names := make([]string, symbolCount)
	configs := make([]NotificationConfig, symbolCount)
	for i := range names {
		names[i] = "MAIN.post" + string(rune('A'+i))
		preSeedTypedSymbol(sess, names[i], uint32(0xD200+i))
		configs[i] = NotificationConfig{SymbolName: names[i], TransmissionMode: TransModeServerOnChange}
	}

	var deletedMu sync.Mutex
	var deleted []uint32
	srv.onDeleteDeviceNotification(func(h uint32) ReturnCode {
		deletedMu.Lock()
		deleted = append(deleted, h)
		deletedMu.Unlock()
		return ReturnCodeNoErrors
	})

	var adds atomic.Int32
	var nextHandle atomic.Uint32
	nextHandle.Store(0x900)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		// Between item 0's commit and item 1's, wipe the map the way Reconnect
		// does — and, like Reconnect, WITHOUT bumping the epoch, so item 1's
		// commit legitimately lands in the new map.
		if adds.Add(1) == 2 {
			sess.notifications.lock.Lock()
			sess.notifications.activeNotifications = make(map[uint32]activeNotification)
			sess.notifications.lock.Unlock()
		}
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})

	ch := make(chan *Update, symbolCount)
	results, err := sess.AddSymbolNotifications(context.Background(), configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications: %v", err)
	}

	// Item 0 was in the snapshot the sweep took: correctly stranded.
	if results[0].Skipped == nil {
		t.Errorf("results[0] (%s) reported success, but the sweep removed it", names[0])
	}

	// Item 1 committed after the sweep. It is bound, live, and owned by us.
	if results[1].Skipped != nil {
		t.Errorf("results[1] (%s) Skipped = %v; it committed after the sweep, so nothing stranded it",
			names[1], results[1].Skipped)
	}
	if results[1].Handle == 0 {
		t.Fatalf("results[1] (%s) has no handle despite committing", names[1])
	}
	sess.notifications.lock.Lock()
	_, stillBound := sess.notifications.activeNotifications[results[1].Handle]
	sess.notifications.lock.Unlock()
	if !stillBound {
		t.Errorf("handle %d is no longer bound; the batch tore down a live subscription", results[1].Handle)
	}
	deletedMu.Lock()
	releasedLive := slices.Contains(deleted, results[1].Handle)
	deletedMu.Unlock()
	if releasedLive {
		t.Errorf("handle %d was deleted PLC-side while the caller still owns it", results[1].Handle)
	}
}

// TestSubscribeRace_StrandedSymbolCanBeResubscribed: ErrNotificationStrandedByReload
// is documented as retryable, so a retry has to actually work. commitNotification
// registers the symbol in configsByKey before the sweep takes it away, and the
// cleanup delete cannot undo that — Session.SumDeleteDeviceNotification only
// drops a config for a handle still present in activeNotifications, and the sweep
// emptied that map. So the entry the caller was told to retry was rejected
// ErrNotificationDuplicate forever, rescued only if an unrelated reconnect
// happened to reset the config table.
func TestSubscribeRace_StrandedSymbolCanBeResubscribed(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	if !c.capabilities.SumAddNotifStateCAS(0, 2) {
		t.Fatal("could not force SumAddNotif into the unsupported state")
	}
	if !c.capabilities.SumDeleteNotifStateCAS(0, 2) {
		t.Fatal("could not force SumDeleteNotif into the unsupported state")
	}
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode { return ReturnCodeNoErrors })

	const symbolCount = 2
	names := make([]string, symbolCount)
	configs := make([]NotificationConfig, symbolCount)
	for i := range names {
		names[i] = "MAIN.retry" + string(rune('A'+i))
		preSeedTypedSymbol(sess, names[i], uint32(0xD400+i))
		configs[i] = NotificationConfig{SymbolName: names[i], TransmissionMode: TransModeServerOnChange}
	}

	var adds atomic.Int32
	var nextHandle atomic.Uint32
	nextHandle.Store(0xA00)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		// Sweep after item 0 is bound, so item 0 comes back stranded.
		if adds.Add(1) == 2 {
			sess.notifications.lock.Lock()
			sess.notifications.activeNotifications = make(map[uint32]activeNotification)
			sess.notifications.lock.Unlock()
		}
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})

	ch := make(chan *Update, symbolCount)
	results, err := sess.AddSymbolNotifications(context.Background(), configs, ch)
	if err != nil {
		t.Fatalf("AddSymbolNotifications: %v", err)
	}
	if !errors.Is(results[0].Skipped, ErrNotificationStrandedByReload) {
		t.Fatalf("results[0] Skipped = %v, want ErrNotificationStrandedByReload — the test did not reproduce the stranding it needs", results[0].Skipped)
	}

	// The documented response to a stranded entry: subscribe it again.
	if _, err := sess.AddSymbolNotification(context.Background(), names[0], 0, 0, TransModeServerOnChange, ch); err != nil {
		if errors.Is(err, ErrNotificationDuplicate) || strings.Contains(err.Error(), "already") {
			t.Errorf("retry of a stranded symbol rejected as a duplicate: %v", err)
		} else {
			t.Errorf("retry of a stranded symbol failed: %v", err)
		}
	}
}

// TestSubscribeRace_AbortedBatchStillAmendsAndReleases: a batch that gives up
// part-way is exactly the case where the PLC has created handles nobody owns and
// where a reload may have landed on the ones already bound. The abort path used
// to return straight out, skipping both the reload amendment and the release —
// so the caller was told a stranded entry had succeeded, and the handle stayed
// registered, streaming into a channel nothing reads.
//
// The batch is aborted with an expiring context, which keeps the connection
// alive so the cleanup delete is observable. That is also why the release runs on
// a fresh context: on the caller's expired one it would fail before being sent.
func TestSubscribeRace_AbortedBatchStillAmendsAndReleases(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	if !c.capabilities.SumAddNotifStateCAS(0, 2) {
		t.Fatal("could not force SumAddNotif into the unsupported state")
	}
	// Sum-delete goes down the same fallback road, one Delete per handle, so the
	// stub sees them individually.
	if !c.capabilities.SumDeleteNotifStateCAS(0, 2) {
		t.Fatal("could not force SumDeleteNotif into the unsupported state")
	}

	const symbolCount = 3
	names := make([]string, symbolCount)
	configs := make([]NotificationConfig, symbolCount)
	for i := range names {
		names[i] = "MAIN.abort" + string(rune('A'+i))
		preSeedTypedSymbol(sess, names[i], uint32(0xB000+i))
		configs[i] = NotificationConfig{SymbolName: names[i], TransmissionMode: TransModeServerOnChange}
	}

	var deletedMu sync.Mutex
	var deleted []uint32
	srv.onDeleteDeviceNotification(func(h uint32) ReturnCode {
		deletedMu.Lock()
		deleted = append(deleted, h)
		deletedMu.Unlock()
		return ReturnCodeNoErrors
	})

	var adds atomic.Int32
	var nextHandle atomic.Uint32
	nextHandle.Store(0x700)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		if adds.Add(1) == 2 {
			// Item 0 is bound by now, so the reload lands on a committed entry and
			// the amendment — not commitNotification's own epoch check — is what has
			// to strand it. Then outlast the caller's deadline so the batch aborts
			// here, on a live connection.
			sess.bumpEpoch()
			sess.notifications.lock.Lock()
			sess.notifications.activeNotifications = make(map[uint32]activeNotification)
			sess.notifications.lock.Unlock()
			time.Sleep(600 * time.Millisecond)
		}
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	ch := make(chan *Update, symbolCount)
	results, err := sess.AddSymbolNotifications(ctx, configs, ch)
	if err == nil {
		t.Fatal("batch reported success despite the context expiring mid-batch")
	}
	if len(results) != symbolCount {
		t.Fatalf("results len = %d, want %d", len(results), symbolCount)
	}

	// The amendment must have run on the abort path: item 0 was bound before the
	// reload, so it cannot be reported as a success.
	if results[0].Skipped == nil {
		t.Errorf("results[0] (%s) reported success, but a reload invalidated it before the abort", names[0])
	} else if !errors.Is(results[0].Skipped, ErrNotificationStrandedByReload) {
		t.Errorf("results[0] Skipped = %v, want ErrNotificationStrandedByReload", results[0].Skipped)
	}
	if results[0].Handle == 0 {
		t.Error("results[0] Handle = 0; the caller cannot release what it is not told about")
	}

	// The release must have run too, on a context the caller's expiry did not kill.
	deadline := time.Now().Add(2 * time.Second)
	for {
		deletedMu.Lock()
		n := len(deleted)
		deletedMu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	deletedMu.Lock()
	got := append([]uint32(nil), deleted...)
	deletedMu.Unlock()
	if len(got) == 0 {
		t.Error("no handle was released after the abort — the PLC keeps streaming a subscription nothing owns")
	}
	if results[0].Handle != 0 && !slices.Contains(got, results[0].Handle) {
		t.Errorf("stranded handle %d was not released; deletes seen: %v", results[0].Handle, got)
	}

	sess.notifications.lock.Lock()
	bound := len(sess.notifications.activeNotifications)
	sess.notifications.lock.Unlock()
	if bound != 0 {
		t.Errorf("activeNotifications = %d, want 0 after an aborted batch whose commits were stranded", bound)
	}
}

// TestOrphanReaperArmedAfterAbortedBatch: an Add whose reply is lost to a
// transport failure may have created a registration on the PLC whose handle
// number this side never learned. Nothing can delete a number nobody has, so the
// only cleanup is the orphan reaper firing when that handle next streams — which
// makes "the reaper is armed once an aborted batch returns" a load-bearing claim
// rather than a detail. It would not hold if the batch left its subscribe window
// open: while the window is open an unknown handle is presumed to be ours and the
// sample is buffered instead.
func TestOrphanReaperArmedAfterAbortedBatch(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	if !c.capabilities.SumAddNotifStateCAS(0, 2) {
		t.Fatal("could not force SumAddNotif into the unsupported state")
	}

	var deletedMu sync.Mutex
	var deleted []uint32
	srv.onDeleteDeviceNotification(func(h uint32) ReturnCode {
		deletedMu.Lock()
		deleted = append(deleted, h)
		deletedMu.Unlock()
		return ReturnCodeNoErrors
	})

	const symbolCount = 3
	configs := make([]NotificationConfig, symbolCount)
	for i := range configs {
		name := fmt.Sprintf("MAIN.reaped%d", i)
		preSeedTypedSymbol(sess, name, uint32(0xD600+i))
		configs[i] = NotificationConfig{SymbolName: name, TransmissionMode: TransModeServerOnChange}
	}

	var adds atomic.Int32
	var nextHandle atomic.Uint32
	nextHandle.Store(0xC00)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		if adds.Add(1) == 2 {
			// Answer late enough that the caller's deadline expires first: the PLC
			// created a registration whose reply this side never used, which is the
			// shape that leaves an unknown handle behind.
			time.Sleep(600 * time.Millisecond)
		}
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	ch := make(chan *Update, symbolCount)
	if _, err := sess.AddSymbolNotifications(ctx, configs, ch); err == nil {
		t.Fatal("batch unexpectedly succeeded; the abort this test needs did not happen")
	}

	// The window must be shut, or the sample below is buffered as "probably ours".
	if n := sess.notifications.subscribeInFlight.Load(); n != 0 {
		t.Fatalf("subscribeInFlight = %d after the batch returned; the reaper is still disabled", n)
	}
	// Past the trailing race window too.
	time.Sleep(subscribeRaceWindow + 50*time.Millisecond)

	// The PLC streams the handle nobody recorded.
	const unknown = 0xC0FF
	if err := sess.drivePacket(sess.currentLifecycleCtx(), buildNotificationPacket(unknown, 0, intSample(1))); err != nil {
		t.Fatalf("drivePacket: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		deletedMu.Lock()
		seen := slices.Contains(deleted, unknown)
		deletedMu.Unlock()
		if seen || time.Now().After(deadline) {
			if !seen {
				t.Errorf("handle 0x%X was never reaped; an Add whose reply was lost would stay registered on the PLC", unknown)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSubscribeRace_ConnectionDropsMidBatch is the realistic trigger: not an
// engineer doing an online change, but the link failing while a sum-unsupported
// PLC is being subscribed one Add at a time. The stub answers two Adds and then
// drops the TCP connection outright.
//
// What must hold afterwards: every config has a verdict, nothing claims success
// without a handle, and the reported successes match what is actually bound —
// a result set that lies about the transport being alive is the failure mode
// this whole branch exists to eliminate.
func TestSubscribeRace_ConnectionDropsMidBatch(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	if !c.capabilities.SumAddNotifStateCAS(0, 2) {
		t.Fatal("could not force SumAddNotif into the unsupported state")
	}
	// Don't let the drop spawn a reconnect that races the assertions.
	c.SetOnDrop(nil)

	const symbolCount = 5
	names := make([]string, symbolCount)
	configs := make([]NotificationConfig, symbolCount)
	for i := range names {
		names[i] = "MAIN.drop" + string(rune('A'+i))
		preSeedTypedSymbol(sess, names[i], uint32(0xF000+i))
		configs[i] = NotificationConfig{SymbolName: names[i], TransmissionMode: TransModeServerOnChange}
	}

	var nextHandle atomic.Uint32
	nextHandle.Store(0x400)
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	// Answer two Adds, then vanish mid-request on the third.
	srv.dropConnAfter(CommandIDAddDeviceNotification, 3)

	ch := make(chan *Update, symbolCount)
	results, err := sess.AddSymbolNotifications(context.Background(), configs, ch)
	// An error is acceptable here and so is nil — what matters is the results.
	t.Logf("AddSymbolNotifications returned err=%v", err)

	if len(results) != symbolCount {
		t.Fatalf("results len = %d, want %d", len(results), symbolCount)
	}
	successes := 0
	for i, r := range results {
		switch {
		case r.Skipped != nil:
			// fine: refused, with the handle surfaced if the PLC made one
		case r.Error != ReturnCodeNoErrors:
			// fine: PLC-side rejection
		case r.Handle != 0:
			successes++
		default:
			t.Errorf("results[%d] (%s) is a zero value: no verdict was recorded for it", i, names[i])
		}
	}

	sess.notifications.lock.Lock()
	bound := len(sess.notifications.activeNotifications)
	sess.notifications.lock.Unlock()
	if successes != bound {
		t.Errorf("reported %d successes but %d handles are bound — the result set does not match reality", successes, bound)
	}
	t.Logf("after mid-batch link loss: %d reported success, %d bound", successes, bound)

	// The subscribe window must have closed despite the failure, or the orphan
	// reaper stays disabled for the life of the session.
	if n := sess.notifications.subscribeInFlight.Load(); n != 0 {
		t.Errorf("subscribeInFlight = %d, want 0 after the batch returned", n)
	}
}

// TestSubscribeRace_ConnectionDropsMidBatchAtScale is the same failure at field
// size. It pins the cost, because the cost IS the bug: a dead transport is not
// marked dead (only Client.Close sets tx.disconnected — listen's drop path just
// fires ondrop), so every remaining Add waits out its own request timeout on a
// corpse. 40 symbols dropping early means the batch holds subscribeInFlight for
// minutes, and the orphan reaper is disabled for every second of it.
func TestSubscribeRace_ConnectionDropsMidBatchAtScale(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, c := newWiredTestSession(t, srv, WithRequestTimeout(300*time.Millisecond))
	c.SetNotificationHandler(sess.handleNotification)
	if !c.capabilities.SumAddNotifStateCAS(0, 2) {
		t.Fatal("could not force SumAddNotif into the unsupported state")
	}
	c.SetOnDrop(nil)

	const symbolCount = 40
	configs := make([]NotificationConfig, symbolCount)
	for i := range configs {
		name := fmt.Sprintf("MAIN.scale%02d", i)
		preSeedTypedSymbol(sess, name, uint32(0xA000+i))
		configs[i] = NotificationConfig{SymbolName: name, TransmissionMode: TransModeServerOnChange}
	}

	var nextHandle atomic.Uint32
	nextHandle.Store(0x500)
	var adds atomic.Int32
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		adds.Add(1)
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	// Die after the third Add: 37 requests still to go.
	srv.dropConnAfter(CommandIDAddDeviceNotification, 3)

	ch := make(chan *Update, symbolCount)
	start := time.Now()
	results, err := sess.AddSymbolNotifications(context.Background(), configs, ch)
	elapsed := time.Since(start)
	t.Logf("40-symbol batch with the link dying at Add 3: took %v, err=%v (answered Adds=%d)",
		elapsed.Round(time.Millisecond), err, adds.Load())

	if len(results) != symbolCount {
		t.Fatalf("results len = %d, want %d", len(results), symbolCount)
	}
	successes, transportFailures, other := 0, 0, 0
	for _, r := range results {
		switch {
		case r.Skipped != nil && errors.Is(r.Skipped, ErrNotificationTransportFailure):
			transportFailures++
		case r.Skipped != nil:
			other++
		case r.Handle != 0 && r.Error == ReturnCodeNoErrors:
			successes++
		default:
			other++
		}
	}
	t.Logf("verdicts: %d success, %d transport-failure, %d other", successes, transportFailures, other)

	// The batch must give up once the transport is confirmed dead rather than
	// waiting out a timeout per remaining symbol. With a 300ms timeout, honest
	// abort finishes in well under a second; the pre-fix behaviour is ~37 x 300ms.
	if elapsed > 5*time.Second {
		t.Errorf("batch took %v after the link died — it is timing out per symbol instead of aborting", elapsed)
	}
	// A link failure must not be reported as a PLC-side rejection.
	if transportFailures == 0 {
		t.Errorf("no entry reported ErrNotificationTransportFailure; %d 'other' verdicts hide a link failure as something else", other)
	}
	if n := sess.notifications.subscribeInFlight.Load(); n != 0 {
		t.Errorf("subscribeInFlight = %d, want 0", n)
	}
}

// TestSubscribeFallback_AMSRouterErrorAbortsBatch is the deliberate sibling of
// the test above, one failure mode over: instead of the socket dying, the AMS
// router keeps answering and refuses every request. That is what a TwinCAT
// system dropping into CONFIG does — port 851 stops existing, the connection
// stays up, and each reply carries AMS ErrorCode 0x06.
//
// The router's refusal must not be recorded as a per-item PLC verdict. Skipped
// == nil means "Error carries the PLC-side return code" (cmd_sum.go's
// SumNotificationResult contract), so mislabelling here tells the consumer the
// runtime individually rejected 37 named symbols it never saw — destroying the
// ErrNotificationTransportFailure retry signal that is the documented way to
// know the batch is worth retrying.
func TestSubscribeFallback_AMSRouterErrorAbortsBatch(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, c := newWiredTestSession(t, srv, WithRequestTimeout(300*time.Millisecond))
	c.SetNotificationHandler(sess.handleNotification)
	if !c.capabilities.SumAddNotifStateCAS(0, 2) {
		t.Fatal("could not force SumAddNotif into the unsupported state")
	}
	c.SetOnDrop(nil)

	const symbolCount = 40
	configs := make([]NotificationConfig, symbolCount)
	for i := range configs {
		name := fmt.Sprintf("MAIN.router%02d", i)
		preSeedTypedSymbol(sess, name, uint32(0xB000+i))
		configs[i] = NotificationConfig{SymbolName: name, TransmissionMode: TransModeServerOnChange}
	}

	var nextHandle atomic.Uint32
	nextHandle.Store(0x500)
	var adds atomic.Int32
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		adds.Add(1)
		return addNotifResponse{Handle: nextHandle.Add(1)}
	})
	// Items 0-2 get handles; from item 3 on, the router refuses. Sticky, as
	// CONFIG mode is.
	srv.amsErrorAfter(CommandIDAddDeviceNotification, 4, ReturnCodeGlobalTargetPortNotFound)

	ch := make(chan *Update, symbolCount)
	results, err := sess.AddSymbolNotifications(context.Background(), configs, ch)
	t.Logf("40-symbol batch with the router refusing from Add 4: err=%v (answered Adds=%d)", err, adds.Load())

	if err == nil {
		t.Error("AddSymbolNotifications returned nil error after the router refused 37 of 40 items")
	}
	if len(results) != symbolCount {
		t.Fatalf("results len = %d, want %d", len(results), symbolCount)
	}
	successes, transportFailures, routerCodeAsVerdict, other := 0, 0, 0, 0
	for _, r := range results {
		switch {
		case r.Skipped != nil && errors.Is(r.Skipped, ErrNotificationTransportFailure):
			transportFailures++
		case r.Skipped == nil && r.Error == ReturnCodeGlobalTargetPortNotFound:
			routerCodeAsVerdict++
		case r.Skipped == nil && r.Handle != 0 && r.Error == ReturnCodeNoErrors:
			successes++
		default:
			other++
		}
	}
	t.Logf("verdicts: %d success, %d transport-failure, %d router-code-as-device-verdict, %d other",
		successes, transportFailures, routerCodeAsVerdict, other)

	// The load-bearing assertion: a router code must never be presented as a
	// per-item PLC verdict.
	if routerCodeAsVerdict != 0 {
		t.Errorf("%d items report the router's 0x06 as a PLC verdict (Skipped == nil), want 0", routerCodeAsVerdict)
	}
	if successes != 3 {
		t.Errorf("successes = %d, want 3", successes)
	}
	if transportFailures != symbolCount-3 {
		t.Errorf("transport failures = %d, want %d", transportFailures, symbolCount-3)
	}
	if other != 0 {
		t.Errorf("%d items have a verdict that is none of the three expected shapes", other)
	}
	// The batch must stop issuing requests at the first refusal instead of
	// sending all 40 against a router that has already said no.
	if got := adds.Load(); got != 4 {
		t.Errorf("stub answered %d Adds, want 4 — the batch carried on past the first router refusal", got)
	}
	if n := sess.notifications.subscribeInFlight.Load(); n != 0 {
		t.Errorf("subscribeInFlight = %d, want 0", n)
	}
}

// TestOrphanDeleteAbortReason covers the reaper's last-moment guards directly.
// The previous pair of tests for this did not: one set subscribeInFlight and
// asserted no Delete RPC, but with the counter set dispatch takes the buffer
// branch and never reaches the reaper at all, so the assertion held for the
// wrong reason; the other raced an unsynchronised goroutine against a Store and
// would flake on a loaded machine.
func TestOrphanDeleteAbortReason(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(sess *Session)
		wantAbort  bool
		wantReason string
	}{
		{
			name:      "unknown handle, nothing in flight",
			setup:     func(*Session) {},
			wantAbort: false,
		},
		{
			name: "handle reappeared in activeNotifications",
			setup: func(sess *Session) {
				sess.notifications.lock.Lock()
				sess.notifications.activeNotifications[0x4242] = activeNotification{Sym: &symbol{FullName: "MAIN.x"}}
				sess.notifications.lock.Unlock()
			},
			wantAbort:  true,
			wantReason: "reappeared",
		},
		{
			name: "a subscribe is in flight",
			setup: func(sess *Session) {
				sess.beginSubscribe()
			},
			wantAbort:  true,
			wantReason: "in flight",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sess := newNotifTestSession()
			tt.setup(sess)
			reason, abort := sess.orphanDeleteAbortReason(0x4242)
			if abort != tt.wantAbort {
				t.Fatalf("abort = %v, want %v (reason %q)", abort, tt.wantAbort, reason)
			}
			if tt.wantReason != "" && !strings.Contains(reason, tt.wantReason) {
				t.Errorf("reason = %q, want it to mention %q", reason, tt.wantReason)
			}
		})
	}
}

// TestOrphanDelete_BuffersRatherThanReapsWhileSubscribing keeps the behavioural
// half of what the old test was really checking: with a subscribe in flight an
// unknown sample is parked, so the reaper is never even consulted.
func TestOrphanDelete_BuffersRatherThanReapsWhileSubscribing(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var deleted atomic.Int32
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode {
		deleted.Add(1)
		return ReturnCodeNoErrors
	})

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	// Stale timestamp: the old 100ms-window guard alone would let the reaper through.
	sess.notifications.lastSubscribeNs.Store(time.Now().Add(-time.Second).UnixNano())
	sess.beginSubscribe()

	if err := sess.drivePacket(sess.lifecycle.ctx, buildNotificationPacket(0x7777, 0, intSample(1))); err != nil {
		t.Fatalf("drivePacket: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	if got := deleted.Load(); got != 0 {
		t.Errorf("Delete RPC calls = %d, want 0 while a subscribe is in flight", got)
	}
	if got := earlySampleCount(sess); got != 1 {
		t.Errorf("buffered samples = %d, want 1 (the sample must be parked, not dropped)", got)
	}
}

// TestSubscribeRace_UncommittedSamplesDiscarded: when the subscribe that
// opened the window commits nothing (PLC rejected the item), the parked
// sample must be discarded rather than retained. A genuinely leaked handle
// keeps firing and its next sample takes the orphan path normally.
func TestSubscribeRace_UncommittedSamplesDiscarded(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	preSeedTypedSymbol(sess, "MAIN.rejected", 0xC0DE)

	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
		if err := sess.drivePacket(sess.lifecycle.ctx, buildNotificationPacket(0x555, 0, intSample(7))); err != nil {
			t.Errorf("drivePacket from Add handler: %v", err)
		}
		return addNotifResponse{Error: ReturnCodeDeviceInvalidParam}
	})

	ch := make(chan *Update, 1)
	if _, err := sess.AddSymbolNotification(context.Background(), "MAIN.rejected", 0, 0, TransModeServerOnChange, ch); err == nil {
		t.Fatal("AddSymbolNotification: err = nil, want PLC rejection")
	}

	if got := earlySampleCount(sess); got != 0 {
		t.Errorf("buffered samples after failed subscribe = %d, want 0", got)
	}
	select {
	case u := <-ch:
		t.Errorf("unexpected Update %+v for a handle that was never committed", u)
	default:
	}
}

// TestReplayEarlySamples_LeavesUnboundHandleParked: a handle still absent from
// activeNotifications must stay in the buffer. Its commit may yet arrive, and
// dispatching it would take the unknown-handle path and schedule an orphan
// delete against a handle we may be about to own. endSubscribe's discard is
// what clears it, not replay.
func TestReplayEarlySamples_LeavesUnboundHandleParked(t *testing.T) {
	sess := newNotifTestSession()
	sess.lifecycle.ctx, sess.lifecycle.shutdown = context.WithCancel(context.Background())
	defer sess.lifecycle.shutdown()
	// Keep the reaper out: it needs a client, which this session has none of.
	sess.beginSubscribe()

	sess.bufferEarlySample(context.Background(), 0x42, 0, intSample(9))
	if got := earlySampleCount(sess); got != 1 {
		t.Fatalf("buffered samples = %d, want 1", got)
	}

	sess.replayEarlySamples(context.Background(), []uint32{0x42})

	if got := earlySampleCount(sess); got != 1 {
		t.Errorf("buffered samples after replaying an unbound handle = %d, want 1 (must stay parked)", got)
	}
}

// TestBufferEarlySample_SelfHealsWhenCommitLandedFirst is the regression for the
// lost static-symbol sample. dispatchSample releases notifications.lock before
// it buffers, so the commit and its replay can both complete in that gap —
// leaving the sample parked after the only replay that would have collected it.
// Buffering therefore re-checks, and delivers the sample itself if the handle is
// bound by then. Constructed deterministically: the handle is already bound when
// bufferEarlySample runs, which is exactly the state that interleaving produces.
func TestBufferEarlySample_SelfHealsWhenCommitLandedFirst(t *testing.T) {
	sess := newNotifTestSession()
	sess.lifecycle.ctx, sess.lifecycle.shutdown = context.WithCancel(context.Background())
	defer sess.lifecycle.shutdown()
	sess.beginSubscribe()

	const handle = 0x99
	sym := preSeedTypedSymbol(sess, "MAIN.sStaticName", 0xC0DE)
	ch := make(chan *Update, 2)
	sess.notifications.lock.Lock()
	sess.notifications.activeNotifications[handle] = activeNotification{Sym: sym, Ch: ch}
	sess.notifications.lock.Unlock()

	sess.bufferEarlySample(context.Background(), handle, 0, intSample(1234))

	select {
	case u := <-ch:
		if u.Value != "1234" {
			t.Errorf("Update.Value = %q, want %q", u.Value, "1234")
		}
		if u.Variable != "MAIN.sStaticName" {
			t.Errorf("Update.Variable = %q, want %q", u.Variable, "MAIN.sStaticName")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sample never delivered: buffering did not notice the handle was already bound")
	}
	if got := earlySampleCount(sess); got != 0 {
		t.Errorf("buffered samples = %d, want 0 (the sample must be consumed, not left parked)", got)
	}
}

// TestEndSubscribe_DiscardIsAtomicWithDecrement: a subscribe that begins while
// another is finishing must keep its buffered sample. The decrement and the
// discard have to be one critical section, or the newcomer's sample is wiped by
// the outgoing call.
func TestEndSubscribe_DiscardIsAtomicWithDecrement(t *testing.T) {
	sess := newNotifTestSession()
	ctx := context.Background()

	first := sess.beginSubscribe()
	second := sess.beginSubscribe()
	sess.bufferEarlySample(ctx, 0x1, 0, intSample(1))

	// First subscribe finishes: still one in flight, so nothing may be dropped.
	sess.endSubscribe(ctx, first, nil)
	if got := earlySampleCount(sess); got != 1 {
		t.Fatalf("buffered samples = %d, want 1 while a subscribe is still in flight", got)
	}

	// Last one finishes with nothing committed: now the parked sample goes.
	sess.endSubscribe(ctx, second, nil)
	if got := earlySampleCount(sess); got != 0 {
		t.Errorf("buffered samples = %d, want 0 after the last subscribe finished", got)
	}
	if n := sess.notifications.subscribeInFlight.Load(); n != 0 {
		t.Errorf("subscribeInFlight = %d, want 0", n)
	}
}

// TestEndSubscribe_DiscardIsAtomicWithDecrement_Concurrent is the atomicity half
// the test above cannot reach: single-threaded, it only shows the count is
// honoured. An outgoing endSubscribe runs on one goroutine while a newcomer opens
// its own window and parks a sample on another; closing the subscribe and
// deciding what may be discarded have to be one critical section, or the outgoing
// call sees nothing in flight after the newcomer has already buffered and wipes
// its sample.
//
// The ordering is forced rather than raced. An earlier version released both
// goroutines together and asserted the same thing — but the outgoing side only
// had to reach a mutex while the newcomer had to open a window AND allocate,
// copy and insert a sample, so the outgoing side won essentially always, and when
// it wins the buffer is still empty and every assertion holds for free. It passed
// with the critical section split three different ways (measured, 15k rounds).
// Sequencing it means one round is enough to kill a split.
func TestEndSubscribe_DiscardIsAtomicWithDecrement_Concurrent(t *testing.T) {
	ctx := context.Background()

	sess := newNotifTestSession()
	outgoing := sess.beginSubscribe()

	buffered := make(chan struct{})
	var newcomer subscribeToken
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		// Only start closing once the newcomer's sample is parked — the state in
		// which discarding it would be a bug.
		<-buffered
		sess.endSubscribe(ctx, outgoing, nil)
	}()
	go func() {
		defer wg.Done()
		newcomer = sess.beginSubscribe()
		sess.bufferEarlySample(ctx, 0x2, 0, intSample(7))
		close(buffered)
	}()
	wg.Wait()

	// The newcomer's window is still open, so its sample must still be there.
	if got := earlySampleCount(sess); got != 1 {
		t.Fatalf("buffered samples = %d, want 1 — the outgoing endSubscribe discarded a sample belonging to a window that was still open", got)
	}
	sess.endSubscribe(ctx, newcomer, nil)
	if got := earlySampleCount(sess); got != 0 {
		t.Errorf("buffered samples = %d, want 0 once nothing is in flight", got)
	}
	if n := sess.notifications.subscribeInFlight.Load(); n != 0 {
		t.Errorf("subscribeInFlight = %d, want 0", n)
	}
}

// TestBufferEarlySample_BoundEnforced: the buffer is capped so a flood of
// unknown handles during a subscribe cannot grow it without limit.
func TestBufferEarlySample_BoundEnforced(t *testing.T) {
	sess := newNotifTestSession()
	sess.beginSubscribe()

	for i := 0; i < earlySampleMaxHandles+16; i++ {
		sess.bufferEarlySample(context.Background(), uint32(i), 0, intSample(uint16(i)))
	}
	if got := earlySampleCount(sess); got != earlySampleMaxHandles {
		t.Errorf("buffered samples = %d, want %d (cap)", got, earlySampleMaxHandles)
	}

	// A handle already in the buffer is still updatable at the cap.
	sess.bufferEarlySample(context.Background(), 0, 0, intSample(0xBEEF))
	sess.notifications.earlyMu.Lock()
	s, ok := sess.notifications.earlySamples[0]
	sess.notifications.earlyMu.Unlock()
	if !ok {
		t.Fatal("handle 0 missing from buffer")
	}
	if v := binary.LittleEndian.Uint16(s.content); v != 0xBEEF {
		t.Errorf("buffered content = 0x%X, want 0xBEEF (latest sample wins)", v)
	}
}

// TestBufferEarlySample_CopiesContent: the buffer outlives the dispatch call,
// so it must not alias the caller's slice.
func TestBufferEarlySample_CopiesContent(t *testing.T) {
	sess := newNotifTestSession()

	content := intSample(1)
	sess.bufferEarlySample(context.Background(), 0x11, 0, content)
	binary.LittleEndian.PutUint16(content, 0xFFFF)

	sess.notifications.earlyMu.Lock()
	s := sess.notifications.earlySamples[0x11]
	sess.notifications.earlyMu.Unlock()
	if v := binary.LittleEndian.Uint16(s.content); v != 1 {
		t.Errorf("buffered content = %d, want 1 (buffer aliases caller slice)", v)
	}
}

// TestSubscribeRaceActive covers the two independent triggers and the negative
// case. Cases go through beginSubscribe rather than poking the counter, so a case
// that claims a subscribe is open has one that really is: an out-of-band counter
// raise used to reach a fail-open branch that returned true unconditionally, and
// the table pinned that as if it were the spec.
func TestSubscribeRaceActive(t *testing.T) {
	tests := []struct {
		name      string
		openAt    *time.Duration // when an open subscribe began, nil = none open
		lastSubAt time.Duration  // relative to now, used only when nothing is open
		want      bool
	}{
		{name: "in flight, recently opened", openAt: durPtr(-time.Second), want: true},
		{name: "in flight, opened just inside the cap", openAt: durPtr(-subscribeRaceMaxOpen + 5*time.Second), want: true},
		{name: "in flight, wedged past the cap", openAt: durPtr(-subscribeRaceMaxOpen - time.Second), want: false},
		{name: "idle, recent subscribe", lastSubAt: 0, want: true},
		{name: "idle, stale subscribe", lastSubAt: -time.Second, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sess := newNotifTestSession()
			if tt.openAt != nil {
				tok := sess.beginSubscribe()
				ageOpenSubscribe(sess, tok, time.Now().Add(*tt.openAt))
			}
			sess.notifications.lastSubscribeNs.Store(time.Now().Add(tt.lastSubAt).UnixNano())
			if got := sess.subscribeRaceActive(); got != tt.want {
				t.Errorf("subscribeRaceActive() = %v, want %v", got, tt.want)
			}
		})
	}
}

func durPtr(d time.Duration) *time.Duration { return &d }

// TestSubscribeRaceActive_WedgedSubscribeExpires pins both directions of the cap,
// which is why per-subscribe start times are tracked instead of one timestamp.
//
// A wedged subscribe alone must NOT hold the window open: measured from
// lastSubscribeNs — refreshed by both begin and end — the deadline was pushed
// forward forever by ordinary traffic and the reaper stayed off for the life of
// the session, which is what fills the PLC handle table.
//
// A young subscribe running alongside a wedged one must hold it OPEN: judged on
// the oldest open subscribe instead, the window closed while a freshly started
// subscribe was still registering, and its first sample took the unknown-handle
// path — not buffered, logged as a leak, an orphan delete scheduled. For a static
// symbol that is the only sample the tag will ever emit.
func TestSubscribeRaceActive_WedgedSubscribeExpires(t *testing.T) {
	sess := newNotifTestSession()

	wedged := sess.beginSubscribe()
	if !sess.subscribeRaceActive() {
		t.Fatal("subscribeRaceActive() = false with a subscribe just opened")
	}

	// Age it past the cap.
	ageOpenSubscribe(sess, wedged, time.Now().Add(-subscribeRaceMaxOpen-time.Second))
	if sess.subscribeRaceActive() {
		t.Error("subscribeRaceActive() = true for a subscribe wedged past subscribeRaceMaxOpen — the reaper is disabled indefinitely")
	}

	// A short subscribe running alongside it keeps the window open on its own
	// merit, then closes — and must not have extended the wedged one's life.
	healthy := sess.beginSubscribe()
	if !sess.subscribeRaceActive() {
		t.Error("window closed while a freshly opened subscribe was in flight")
	}
	sess.endSubscribe(context.Background(), healthy, nil)
	if sess.subscribeRaceActive() {
		t.Error("a later subscribe pushed the wedged one's deadline out again")
	}

	// Once the wedged call finally returns, nothing is tracked and a fresh
	// subscribe starts from its own clock.
	sess.endSubscribe(context.Background(), wedged, nil)
	if _, open := sess.notifications.newestOpenSubscribe(); open {
		t.Error("a subscribe is still tracked after every one closed")
	}
	sess.beginSubscribe()
	if !sess.subscribeRaceActive() {
		t.Error("a fresh subscribe inherited the wedged one's expired clock")
	}
}

// TestEndSubscribe_NestedSubscribesKeepWindowOpen: two concurrent subscribes
// must not have the first one to finish tear down the shared buffer while the
// second is still registering handles.
func TestEndSubscribe_NestedSubscribesKeepWindowOpen(t *testing.T) {
	sess := newNotifTestSession()
	ctx := context.Background()

	first := sess.beginSubscribe()
	second := sess.beginSubscribe()
	sess.bufferEarlySample(context.Background(), 0xABC, 0, intSample(3))

	sess.endSubscribe(ctx, first, nil)
	if got := earlySampleCount(sess); got != 1 {
		t.Errorf("buffered samples with one subscribe still in flight = %d, want 1", got)
	}
	if !sess.subscribeRaceActive() {
		t.Error("subscribeRaceActive() = false while a subscribe is still in flight")
	}

	sess.endSubscribe(ctx, second, nil)
	if got := earlySampleCount(sess); got != 0 {
		t.Errorf("buffered samples after last subscribe finished = %d, want 0", got)
	}
	if n := sess.notifications.subscribeInFlight.Load(); n != 0 {
		t.Errorf("subscribeInFlight = %d, want 0", n)
	}
}

// TestBufferEarlySample_ByteBudgetEnforced: the handle cap alone does not bound
// memory. A sample is network-sized — struct and array symbols run to tens of KB
// — so the buffer also has to stop counting bytes, and replacing an entry must
// release the bytes it held rather than leaking them.
func TestBufferEarlySample_ByteBudgetEnforced(t *testing.T) {
	sess := newNotifTestSession()
	sess.beginSubscribe()

	big := make([]byte, 1<<20) // 1 MiB per sample
	for i := 0; i < (earlySampleMaxBytes/len(big))+4; i++ {
		sess.bufferEarlySample(context.Background(), uint32(i), 0, big)
	}

	sess.notifications.earlyMu.Lock()
	held := sess.notifications.earlyBytes
	entries := len(sess.notifications.earlySamples)
	sess.notifications.earlyMu.Unlock()

	if held > earlySampleMaxBytes {
		t.Errorf("held %d bytes, over the %d budget", held, earlySampleMaxBytes)
	}
	if entries == 0 {
		t.Error("budget rejected everything; it should admit samples up to the cap")
	}

	// Replacing an entry must not double-count: same handle, same size, so the
	// total has to stay put.
	before := held
	sess.bufferEarlySample(context.Background(), 0, 0, big)
	sess.notifications.earlyMu.Lock()
	after := sess.notifications.earlyBytes
	sess.notifications.earlyMu.Unlock()
	if after != before {
		t.Errorf("byte count moved from %d to %d when replacing an entry of equal size", before, after)
	}
}

// TestReplayEarlySamples_ReleasesBytes: taking a sample out of the buffer has to
// give its bytes back, or a long-lived session leaks the budget one replay at a
// time until nothing can be buffered.
func TestReplayEarlySamples_ReleasesBytes(t *testing.T) {
	sess := newNotifTestSession()
	sess.beginSubscribe()

	const handle = 0x555
	sym := preSeedTypedSymbol(sess, "MAIN.x", 0xC0DE)
	ch := make(chan *Update, 1)
	sess.notifications.lock.Lock()
	sess.notifications.activeNotifications[handle] = activeNotification{Sym: sym, Ch: ch}
	sess.notifications.lock.Unlock()

	// bufferEarlySample self-heals when the handle is already bound, so the
	// sample is consumed and its bytes must be released with it.
	sess.bufferEarlySample(context.Background(), handle, 0, intSample(7))

	sess.notifications.earlyMu.Lock()
	held := sess.notifications.earlyBytes
	sess.notifications.earlyMu.Unlock()
	if held != 0 {
		t.Errorf("earlyBytes = %d after the sample was consumed, want 0", held)
	}
}

// TestRestoreConfigs_KeepsWhatArrivedDuringTheAttempt: putting a snapshot back
// must not discard newer configs.
//
// Heartbeat recovery snapshots the caller's intent, tries to re-subscribe, and
// restores the snapshot on failure. With resetConfigs that restore was an
// overwrite, so a subscribe made while the attempt was in flight lost its config
// while its PLC handle stayed registered: never resubscribed after a reconnect, and
// subscribing that symbol again would duplicate the registration. Hardware showed
// the wider version of this race when power-cycling 192.168.3.70 with 40 symbols —
// heartbeat recovery and the reconnect loop both resubscribing, "bound
// notifications = 24, want 40".
func TestRestoreConfigs_KeepsWhatArrivedDuringTheAttempt(t *testing.T) {
	mgr := newTestNotificationManager()

	// What recovery snapshotted before it started.
	snapshot := []pendingNotification{
		{Config: NotificationConfig{SymbolName: "MAIN.a"}},
		{Config: NotificationConfig{SymbolName: "MAIN.b"}, resubscribeAttempts: 2},
	}
	// What the attempt left on file: its own configs cleared, plus one the user
	// subscribed while it was running.
	mgr.resetConfigs(nil)
	mgr.addConfig(NotificationConfig{SymbolName: "MAIN.late"})

	mgr.restoreConfigs(snapshot)

	if !mgr.hasConfig("MAIN.late") {
		t.Error("the config that arrived during the attempt was discarded by the restore: its handle stays registered on the PLC " +
			"with nothing to resubscribe it")
	}
	for _, name := range []string{"MAIN.a", "MAIN.b"} {
		if !mgr.hasConfig(name) {
			t.Errorf("%s was not restored", name)
		}
	}
	if len(mgr.pending) != 3 {
		t.Errorf("pending = %d, want 3 (two restored plus the newer one)", len(mgr.pending))
	}
	// The retry counter has to survive, or a symbol that has already failed twice
	// gets a fresh budget on every recovery and never drops out.
	for _, entry := range mgr.pending {
		if entry.Config.SymbolName == "MAIN.b" && entry.resubscribeAttempts != 2 {
			t.Errorf("MAIN.b restored with resubscribeAttempts = %d, want 2", entry.resubscribeAttempts)
		}
	}
	// And a restore must not duplicate what is already on file.
	mgr.restoreConfigs(snapshot)
	if len(mgr.pending) != 3 {
		t.Errorf("pending = %d after restoring the same snapshot twice, want 3", len(mgr.pending))
	}
}

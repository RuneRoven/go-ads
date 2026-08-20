package ads

import (
	"context"
	"encoding/binary"
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
	srv.onAddDeviceNotification(func(_ addNotifRequest) addNotifResponse {
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

// TestOrphanDelete_SuppressedWhileSubscribeInFlight is the narrow regression
// guard: lastSubscribeNs is stale (a long batch), yet a subscribe is still in
// flight, so an unknown handle must not be reaped. Pre-fix this is precisely
// the state in which the reaper deleted valid subscriptions.
func TestOrphanDelete_SuppressedWhileSubscribeInFlight(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var deleted atomic.Int32
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode {
		deleted.Add(1)
		return ReturnCodeNoErrors
	})

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	// Stale timestamp: the old 100ms-window guard would let the reaper through.
	sess.notifications.lastSubscribeNs.Store(time.Now().Add(-1 * time.Second).UnixNano())
	sess.notifications.subscribeInFlight.Store(1)

	if err := sess.drivePacket(sess.lifecycle.ctx, buildNotificationPacket(0x7777, 0, intSample(1))); err != nil {
		t.Fatalf("drivePacket: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	if got := deleted.Load(); got != 0 {
		t.Errorf("Delete RPC calls = %d, want 0 (in-flight subscribe must suppress the reaper)", got)
	}
	if got := earlySampleCount(sess); got != 1 {
		t.Errorf("buffered samples = %d, want 1 (sample must be parked, not dropped)", got)
	}
}

// TestOrphanDelete_AbortsWhenSubscribeStartsAfterScheduling covers the second
// half of the reaper's race guard: the sample looked like an orphan when it
// arrived, but a subscribe started before the async Delete fired, so the
// handle may be one we are about to commit and must be left alone.
func TestOrphanDelete_AbortsWhenSubscribeStartsAfterScheduling(t *testing.T) {
	srv := startScriptableServer(t)
	defer srv.stop()

	var deleted atomic.Int32
	srv.delayBefore(CommandIDDeleteDeviceNotification, 0, 200*time.Millisecond)
	srv.onDeleteDeviceNotification(func(_ uint32) ReturnCode {
		deleted.Add(1)
		return ReturnCodeNoErrors
	})

	sess, c := newWiredTestSession(t, srv)
	c.SetNotificationHandler(sess.handleNotification)
	sess.notifications.lastSubscribeNs.Store(time.Now().Add(-1 * time.Second).UnixNano())

	if err := sess.drivePacket(sess.lifecycle.ctx, buildNotificationPacket(0xFEED, 0, intSample(1))); err != nil {
		t.Fatalf("drivePacket: %v", err)
	}
	// Open a subscribe window while the Delete goroutine is still queued.
	sess.notifications.subscribeInFlight.Store(1)

	time.Sleep(500 * time.Millisecond)
	if got := deleted.Load(); got != 0 {
		t.Errorf("Delete RPC calls = %d, want 0 (subscribe started before the Delete fired)", got)
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

// TestReplayEarlySamples_DropsUncommittedHandle: replay must not re-park a
// sample whose handle is still absent from activeNotifications — otherwise
// buffer and replay bounce a sample forever.
func TestReplayEarlySamples_DropsUncommittedHandle(t *testing.T) {
	sess := newNotifTestSession()
	sess.lifecycle.ctx, sess.lifecycle.shutdown = context.WithCancel(context.Background())
	defer sess.lifecycle.shutdown()
	// Keep the reaper out: it needs a client, which this session has none of.
	sess.notifications.subscribeInFlight.Store(1)

	sess.bufferEarlySample(0x42, 0, intSample(9))
	if got := earlySampleCount(sess); got != 1 {
		t.Fatalf("buffered samples = %d, want 1", got)
	}

	sess.replayEarlySamples(context.Background(), []uint32{0x42})

	if got := earlySampleCount(sess); got != 0 {
		t.Errorf("buffered samples after replay = %d, want 0 (replay must not re-buffer)", got)
	}
}

// TestBufferEarlySample_BoundEnforced: the buffer is capped so a flood of
// unknown handles during a subscribe cannot grow it without limit.
func TestBufferEarlySample_BoundEnforced(t *testing.T) {
	sess := newNotifTestSession()
	sess.notifications.subscribeInFlight.Store(1)

	for i := 0; i < earlySampleMaxHandles+16; i++ {
		sess.bufferEarlySample(uint32(i), 0, intSample(uint16(i)))
	}
	if got := earlySampleCount(sess); got != earlySampleMaxHandles {
		t.Errorf("buffered samples = %d, want %d (cap)", got, earlySampleMaxHandles)
	}

	// A handle already in the buffer is still updatable at the cap.
	sess.bufferEarlySample(0, 0, intSample(0xBEEF))
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
	sess.bufferEarlySample(0x11, 0, content)
	binary.LittleEndian.PutUint16(content, 0xFFFF)

	sess.notifications.earlyMu.Lock()
	s := sess.notifications.earlySamples[0x11]
	sess.notifications.earlyMu.Unlock()
	if v := binary.LittleEndian.Uint16(s.content); v != 1 {
		t.Errorf("buffered content = %d, want 1 (buffer aliases caller slice)", v)
	}
}

// TestSubscribeRaceActive covers the two independent triggers and the
// negative case.
func TestSubscribeRaceActive(t *testing.T) {
	tests := []struct {
		name      string
		inFlight  int64
		lastSubAt time.Duration // relative to now
		want      bool
	}{
		{name: "in flight, stale timestamp", inFlight: 1, lastSubAt: -1 * time.Second, want: true},
		{name: "idle, recent subscribe", inFlight: 0, lastSubAt: 0, want: true},
		{name: "idle, stale subscribe", inFlight: 0, lastSubAt: -1 * time.Second, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sess := newNotifTestSession()
			sess.notifications.subscribeInFlight.Store(tt.inFlight)
			sess.notifications.lastSubscribeNs.Store(time.Now().Add(tt.lastSubAt).UnixNano())
			if got := sess.subscribeRaceActive(); got != tt.want {
				t.Errorf("subscribeRaceActive() = %v, want %v (inFlight=%d lastSubAt=%v)",
					got, tt.want, tt.inFlight, tt.lastSubAt)
			}
		})
	}
}

// TestEndSubscribe_NestedSubscribesKeepWindowOpen: two concurrent subscribes
// must not have the first one to finish tear down the shared buffer while the
// second is still registering handles.
func TestEndSubscribe_NestedSubscribesKeepWindowOpen(t *testing.T) {
	sess := newNotifTestSession()
	ctx := context.Background()

	sess.beginSubscribe()
	sess.beginSubscribe()
	sess.bufferEarlySample(0xABC, 0, intSample(3))

	sess.endSubscribe(ctx, nil)
	if got := earlySampleCount(sess); got != 1 {
		t.Errorf("buffered samples with one subscribe still in flight = %d, want 1", got)
	}
	if !sess.subscribeRaceActive() {
		t.Error("subscribeRaceActive() = false while a subscribe is still in flight")
	}

	sess.endSubscribe(ctx, nil)
	if got := earlySampleCount(sess); got != 0 {
		t.Errorf("buffered samples after last subscribe finished = %d, want 0", got)
	}
	if n := sess.notifications.subscribeInFlight.Load(); n != 0 {
		t.Errorf("subscribeInFlight = %d, want 0", n)
	}
}

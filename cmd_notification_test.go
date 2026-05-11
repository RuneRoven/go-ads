package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"math"
	"testing"
	"time"
)

func TestDurationToADSTicks(t *testing.T) {
	tests := []struct {
		name    string
		d       time.Duration
		wantErr bool
		want    uint32
	}{
		{"zero", 0, false, 0},
		{"1ms", time.Millisecond, false, 10_000},
		{"max valid", time.Duration(math.MaxUint32) * 100 * time.Nanosecond, false, math.MaxUint32},
		{"negative", -time.Millisecond, true, 0},
		{"overflow", time.Duration(math.MaxUint32)*100*time.Nanosecond + 100*time.Nanosecond, true, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := durationToADSTicks(tt.d, "test")
			if (err != nil) != tt.wantErr {
				t.Fatalf("durationToADSTicks(%v) error = %v, wantErr %v", tt.d, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("durationToADSTicks(%v) = %d, want %d", tt.d, got, tt.want)
			}
		})
	}
}

// Verify that sending to a closed user channel does NOT panic the listen goroutine.
// Go runtime panics on send-to-closed-channel regardless of select default,
// so deliverNotification must guard with defer recover().
// Validates: R-NOT-006.
func TestDeliverNotification_ClosedChannelDoesNotPanic(t *testing.T) {
	ch := make(chan *Update, 1)
	close(ch)

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("expected no panic, got recovered panic: %v", r)
		}
	}()

	conn := &Session{logger: getDefaultLogger()}
	ctx := context.Background()
	update := &Update{Variable: "x", Value: "1", TimeStamp: time.Now()}

	conn.deliverNotification(ctx, ch, update, 42, "x")
}

// Verify normal happy-path delivery on an open buffered channel.
// Validates: R-NOT-006.
func TestDeliverNotification_DeliversOnOpenChannel(t *testing.T) {
	ch := make(chan *Update, 1)
	conn := &Session{logger: getDefaultLogger()}
	ctx := context.Background()
	update := &Update{Variable: "x", Value: "1", TimeStamp: time.Now()}

	conn.deliverNotification(ctx, ch, update, 42, "x")

	select {
	case got := <-ch:
		if got != update {
			t.Errorf("got %v, want %v", got, update)
		}
	default:
		t.Errorf("update was not delivered to channel")
	}
}

// Verify drop on full buffered channel (default branch of select fires).
// Validates: R-NOT-006.
func TestDeliverNotification_DropsWhenChannelFull(t *testing.T) {
	ch := make(chan *Update, 1)
	ch <- &Update{Variable: "filler"} // fill buffer

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("expected no panic, got recovered panic: %v", r)
		}
	}()

	conn := &Session{logger: getDefaultLogger()}
	ctx := context.Background()
	update := &Update{Variable: "x", Value: "1", TimeStamp: time.Now()}

	conn.deliverNotification(ctx, ch, update, 42, "x")
	// Should drop without panic; channel still has only the filler.
	if len(ch) != 1 {
		t.Errorf("expected channel to keep filler, got len=%d", len(ch))
	}
}

// ==========================================================================
// DeviceNotification parsing — binary packet tests
// ==========================================================================

// Validates: R-NOT-005, R-NOT-012.
func TestDeviceNotification_SingleSample(t *testing.T) {
	conn := newTestConnection()
	defer conn.lifecycle.shutdown()

	// Register a notification for handle 42
	ch := make(chan *Update, 10)
	sym := &Symbol{
		FullName:     "MAIN.testVar",
		DataType:     "INT",
		Length:       2,
		Notification: ch,
	}
	conn.notifications.activeNotifications[42] = sym
	conn.cache.symbols[symbolKey(sym.FullName)] = sym

	// Build INT value = 1234
	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, 1234)

	// Windows FILETIME for 2024-01-01 00:00:00 UTC
	// = (Unix epoch offset + unix timestamp) * ticks per second
	unixTS := int64(1704067200) // 2024-01-01 00:00:00 UTC
	filetime := uint64((unixTS + secToUnixEpoch) * windowsTick)

	packet := buildNotificationPacket(42, filetime, data)
	err := conn.drivePacket(conn.lifecycle.ctx, packet)
	if err != nil {
		t.Fatalf("DeviceNotification error: %v", err)
	}

	select {
	case update := <-ch:
		if update.Variable != "MAIN.testVar" {
			t.Errorf("variable = %q, want %q", update.Variable, "MAIN.testVar")
		}
		if update.Value != "1234" {
			t.Errorf("value = %q, want %q", update.Value, "1234")
		}
		// Verify timestamp is approximately correct (within a second)
		expectedTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		if update.TimeStamp.Sub(expectedTime).Abs() > time.Second {
			t.Errorf("timestamp = %v, want ~%v", update.TimeStamp, expectedTime)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for notification")
	}
}

// Validates: R-NOT-007 (partial).
func TestDeviceNotification_UnknownHandle(t *testing.T) {
	conn := newTestConnection()
	defer conn.lifecycle.shutdown()

	// No notifications registered — handle 99 is unknown
	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, 42)
	packet := buildNotificationPacket(99, 0, data)

	// Should not error, just log warning
	err := conn.drivePacket(conn.lifecycle.ctx, packet)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Validates: R-NOT-007.
func TestDeviceNotification_UnknownHandleDuringClose(t *testing.T) {
	handler := &testLogHandler{}
	conn := newTestConnection()
	conn.logger = slog.New(handler)
	conn.lifecycle.closedCh = make(chan struct{})
	defer conn.lifecycle.shutdown()

	// Mark connection as closed via the FSM (flag removed in Phase 3.b).
	conn.lifecycle.state.transitionTo(SessionStateClosed)
	close(conn.lifecycle.closedCh)

	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, 42)
	packet := buildNotificationPacket(99, 0, data)

	err := conn.drivePacket(conn.lifecycle.ctx, packet)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// During Close(), stale notifications should be Debug, not Warn
	rec := handler.findByMessage("received notification for deleted handle")
	if rec == nil {
		t.Fatal("expected debug log for stale notification during close")
	}
	if rec.Level != slog.LevelDebug {
		t.Errorf("expected Debug level during close, got %v", rec.Level)
	}
}

// Validates: R-NOT-007.
func TestDeviceNotification_UnknownHandleNormalCondition(t *testing.T) {
	handler := &testLogHandler{}
	conn := newTestConnection()
	conn.logger = slog.New(handler)
	defer conn.lifecycle.shutdown()

	// No close, no recent reconnect — should be Warn
	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, 42)
	packet := buildNotificationPacket(99, 0, data)

	err := conn.drivePacket(conn.lifecycle.ctx, packet)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rec := handler.findByMessage("received notification for unknown handle")
	if rec == nil {
		t.Fatal("expected warn log for unknown handle in normal conditions")
	}
	if rec.Level != slog.LevelWarn {
		t.Errorf("expected Warn level in normal conditions, got %v", rec.Level)
	}
}

// Validates: R-NOT-005 (partial).
func TestDeviceNotification_MultipleStampsAndSamples(t *testing.T) {
	conn := newTestConnection()
	defer conn.lifecycle.shutdown()

	ch := make(chan *Update, 10)
	sym1 := &Symbol{FullName: "var1", DataType: "BYTE", Length: 1, Notification: ch}
	sym2 := &Symbol{FullName: "var2", DataType: "BYTE", Length: 1, Notification: ch}
	conn.notifications.activeNotifications[1] = sym1
	conn.notifications.activeNotifications[2] = sym2
	conn.cache.symbols[symbolKey(sym1.FullName)] = sym1
	conn.cache.symbols[symbolKey(sym2.FullName)] = sym2

	stamps := []struct {
		timestamp uint64
		samples   []struct {
			handle uint32
			data   []byte
		}
	}{
		{
			timestamp: 0,
			samples: []struct {
				handle uint32
				data   []byte
			}{
				{1, []byte{10}},
				{2, []byte{20}},
			},
		},
		{
			timestamp: 0,
			samples: []struct {
				handle uint32
				data   []byte
			}{
				{1, []byte{30}},
			},
		},
	}

	packet := buildNotificationPacketMultiSample(stamps)
	err := conn.drivePacket(conn.lifecycle.ctx, packet)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	// Should receive 3 updates total
	var updates []*Update
	for i := 0; i < 3; i++ {
		select {
		case u := <-ch:
			updates = append(updates, u)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out after %d updates", len(updates))
		}
	}
	if len(updates) != 3 {
		t.Errorf("expected 3 updates, got %d", len(updates))
	}
}

// Validates: R-CMD-007.
func TestDeviceNotification_EmptyPacket(t *testing.T) {
	conn := newTestConnection()
	defer conn.lifecycle.shutdown()

	// Too short — should return error
	err := conn.drivePacket(conn.lifecycle.ctx, []byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for truncated packet")
	}
}

// Validates: NO-SPEC.
func TestDeviceNotification_ZeroStamps(t *testing.T) {
	conn := newTestConnection()
	defer conn.lifecycle.shutdown()

	// Valid header with 0 stamps
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(8)) // length
	binary.Write(buf, binary.LittleEndian, uint32(0)) // 0 stamps

	err := conn.drivePacket(conn.lifecycle.ctx, buf.Bytes())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// Validates: R-CMD-007 (sum-up).
func TestDeviceNotification_SampleSizeExceedsData(t *testing.T) {
	conn := newTestConnection()
	defer conn.lifecycle.shutdown()

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(100))      // length (fake)
	binary.Write(buf, binary.LittleEndian, uint32(1))        // 1 stamp
	binary.Write(buf, binary.LittleEndian, uint64(0))        // timestamp
	binary.Write(buf, binary.LittleEndian, uint32(1))        // 1 sample
	binary.Write(buf, binary.LittleEndian, uint32(42))       // handle
	binary.Write(buf, binary.LittleEndian, uint32(99999999)) // size > remaining

	err := conn.drivePacket(conn.lifecycle.ctx, buf.Bytes())
	if err == nil {
		t.Error("expected error for sample size exceeding data")
	}
}

// Validates: R-PARSE-007 (BOOL).
func TestDeviceNotification_BoolType(t *testing.T) {
	conn := newTestConnection()
	defer conn.lifecycle.shutdown()

	ch := make(chan *Update, 5)
	sym := &Symbol{FullName: "MAIN.bFlag", DataType: "BOOL", Length: 1, Notification: ch}
	conn.notifications.activeNotifications[10] = sym
	conn.cache.symbols[symbolKey(sym.FullName)] = sym

	packet := buildNotificationPacket(10, 0, []byte{1})
	err := conn.drivePacket(conn.lifecycle.ctx, packet)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	select {
	case u := <-ch:
		if u.Value != "true" {
			t.Errorf("BOOL value = %q, want %q", u.Value, "true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

// Validates: R-PARSE-005 + R-PARSE-007 (STRING).
func TestDeviceNotification_StringType(t *testing.T) {
	conn := newTestConnection()
	defer conn.lifecycle.shutdown()

	ch := make(chan *Update, 5)
	sym := &Symbol{FullName: "MAIN.sName", DataType: "STRING", Length: 20, Notification: ch}
	conn.notifications.activeNotifications[11] = sym
	conn.cache.symbols[symbolKey(sym.FullName)] = sym

	strData := make([]byte, 20)
	copy(strData, "Hello\x00")

	packet := buildNotificationPacket(11, 0, strData)
	err := conn.drivePacket(conn.lifecycle.ctx, packet)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	select {
	case u := <-ch:
		if u.Value != "Hello" {
			t.Errorf("STRING value = %q, want %q", u.Value, "Hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
	}
}

// ==========================================================================
// Notification timestamp conversion
// ==========================================================================

// TestWindowsFiletimeConversion drives hard-coded raw Windows FILETIME
// values through the production deviceNotification → handleNotification
// path and asserts the resulting Update.TimeStamp.
//
// The previous version computed the encoding (filetime = (unixSec +
// secToUnixEpoch) * windowsTick) and then immediately reversed it with
// the same constants — neither side touched the production handler, so
// a constant drift in the production code would shift both sides in
// lockstep and the test would still pass. This rewrite pins literal
// FILETIME values from external authority (Wolfram Alpha / Boost
// reference) so a constant drift surfaces here as an explicit failure.
//
// Validates: R-NOT-012 (Windows-100ns to time.Time conversion).
func TestWindowsFiletimeConversion(t *testing.T) {
	tests := []struct {
		name     string
		filetime uint64    // raw Windows FILETIME, externally calculated.
		want     time.Time // expected UTC instant.
	}{
		// Unix epoch, 1970-01-01 00:00:00 UTC.
		// 11644473600 sec since 1601-01-01, * 10^7 ticks/sec.
		{"unix_epoch", 116444736000000000, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		// Y2K, 2000-01-01 00:00:00 UTC.
		// (11644473600 + 946684800) * 10^7.
		{"y2k", 125911584000000000, time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
		// 2024-01-01 00:00:00 UTC.
		// (11644473600 + 1704067200) * 10^7.
		{"2024", 133485408000000000, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := newTestConnection()
			defer conn.lifecycle.shutdown()

			ch := make(chan *Update, 1)
			sym := &Symbol{
				FullName:     "MAIN.x",
				DataType:     "INT",
				Length:       2,
				Notification: ch,
			}
			conn.notifications.activeNotifications[7] = sym
			conn.cache.symbols[symbolKey(sym.FullName)] = sym

			data := make([]byte, 2)
			binary.LittleEndian.PutUint16(data, 1)
			packet := buildNotificationPacket(7, tt.filetime, data)

			if err := conn.drivePacket(conn.lifecycle.ctx, packet); err != nil {
				t.Fatalf("drivePacket: %v", err)
			}

			select {
			case update := <-ch:
				if !update.TimeStamp.Equal(tt.want) {
					t.Errorf("TimeStamp = %v, want %v", update.TimeStamp.UTC(), tt.want)
				}
			case <-time.After(time.Second):
				t.Fatal("no update received")
			}
		})
	}
}

// TestNotification_TerminalZeroByteSample_TriggersDetection validates that
// the notification listener intercepts a 0-byte terminal sample (TwinCAT
// signal that the symbol is gone post-online-change) BEFORE the
// parse-error log path executes, classifying it as a R-CACHE-009
// supplementary signal and firing the configured online-change callback
// with ReasonSymbolNotFound.
//
// Hardware finding (TC3 sweep): symbol deletion via online change → PLC
// drops the old notification handle silently and emits one terminal
// 0-byte sample on the now-dead handle. Without interception, the parser
// errors with "symbol.Length 4 exceeds data buffer size 0" and the user
// gets noise instead of a structured stale-cache signal.
//
// Validates: R-CACHE-009 supplementary detection + R-NOT-016 (callback
// reason) + no Update delivered for the dead handle.
func TestNotification_TerminalZeroByteSample_TriggersDetection(t *testing.T) {
	conn := newTestConnection()
	defer conn.lifecycle.shutdown()

	cbReason := make(chan string, 1)
	conn.versionStrategy = SymbolVersionIgnore
	conn.versionCallback = func(r string) {
		select {
		case cbReason <- r:
		default:
		}
	}

	ch := make(chan *Update, 4)
	sym := &Symbol{
		FullName:     "MAIN.x",
		DataType:     "DINT",
		Length:       4,
		Notification: ch,
	}
	conn.notifications.activeNotifications[42] = sym
	conn.cache.symbols[symbolKey(sym.FullName)] = sym

	// Inject 0-byte terminal sample. drivePacket may or may not return an
	// error from the now-skipped parse path — what matters is the callback
	// firing with ReasonSymbolNotFound BEFORE the (downgraded) parse log.
	packet := buildNotificationPacket(42, 0, []byte{})
	if err := conn.drivePacket(conn.lifecycle.ctx, packet); err != nil {
		t.Logf("drivePacket err (acceptable, detection still must fire): %v", err)
	}

	select {
	case r := <-cbReason:
		if r != ReasonSymbolNotFound {
			t.Errorf("callback reason = %q, want %q", r, ReasonSymbolNotFound)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("0-byte terminal sample did not trigger online-change callback")
	}

	// No Update delivered for dead handle — terminal sample, not a value.
	select {
	case u := <-ch:
		t.Errorf("unexpected Update delivered for terminal 0-byte sample: %+v", u)
	case <-time.After(50 * time.Millisecond):
		// expected: nothing on the channel
	}
}

// Validates: R-NOT-016 + R-NOT-017 — Ignore strategy flags next sample
// Stale=true, Reason=detected reason; flag is one-shot.
func TestNotification_StaleFlag_OneShotAfterDetection(t *testing.T) {
	sess := newTestConnection()
	defer sess.lifecycle.shutdown()

	ch := make(chan *Update, 4)
	sym := &Symbol{
		FullName: "MAIN.x", DataType: "INT", Length: 2, Notification: ch,
	}
	const handle uint32 = 7
	sess.notifications.lock.Lock()
	sess.notifications.activeNotifications[handle] = sym
	sess.notifications.lock.Unlock()
	sess.cache.lock.Lock()
	sess.cache.symbols[symbolKey(sym.FullName)] = sym
	sess.cache.lock.Unlock()

	// Mark stale (simulates prior R-CACHE-009 detection under Ignore).
	sess.markSymbolStale(handle, ReasonSymbolVersionInvalid)

	// First sample: must carry Stale=true.
	pkt := buildNotificationPacket(handle, 0, []byte{0x01, 0x00})
	if err := sess.drivePacket(sess.lifecycle.ctx, pkt); err != nil {
		t.Fatalf("drivePacket #1: %v", err)
	}
	select {
	case u := <-ch:
		if !u.Stale || u.Reason != ReasonSymbolVersionInvalid {
			t.Errorf("first sample: got Stale=%v Reason=%q, want Stale=true Reason=%q",
				u.Stale, u.Reason, ReasonSymbolVersionInvalid)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no first sample")
	}

	// Second sample: flag consumed, must NOT be Stale.
	pkt2 := buildNotificationPacket(handle, 0, []byte{0x02, 0x00})
	if err := sess.drivePacket(sess.lifecycle.ctx, pkt2); err != nil {
		t.Fatalf("drivePacket #2: %v", err)
	}
	select {
	case u := <-ch:
		if u.Stale || u.Reason != "" {
			t.Errorf("second sample: got Stale=%v Reason=%q, want Stale=false Reason=\"\"",
				u.Stale, u.Reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no second sample")
	}
}

// Validates: R-CACHE-012 + R-NOT-017 — Ignore strategy detection marks ALL
// active handles, not just the one that triggered detection.
func TestNotification_StaleFlag_IgnoreMarksAllHandles(t *testing.T) {
	sess := newTestConnection()
	defer sess.lifecycle.shutdown()
	sess.versionStrategy = SymbolVersionIgnore

	const h1, h2 uint32 = 11, 22
	sess.notifications.lock.Lock()
	sess.notifications.activeNotifications[h1] = &Symbol{FullName: "a"}
	sess.notifications.activeNotifications[h2] = &Symbol{FullName: "b"}
	sess.notifications.lock.Unlock()

	// Trigger detection through the strategy dispatcher.
	stale, reason := sess.handleStaleDetection(ReturnCodeDeviceSymbolVersionInvalid)
	if !stale {
		t.Fatal("expected stale=true")
	}
	if reason != ReasonSymbolVersionInvalid {
		t.Fatalf("reason = %q, want %q", reason, ReasonSymbolVersionInvalid)
	}

	r1, ok1 := sess.consumeStaleFlag(h1)
	r2, ok2 := sess.consumeStaleFlag(h2)
	if !ok1 || r1 != ReasonSymbolVersionInvalid {
		t.Errorf("h1: ok=%v reason=%q", ok1, r1)
	}
	if !ok2 || r2 != ReasonSymbolVersionInvalid {
		t.Errorf("h2: ok=%v reason=%q", ok2, r2)
	}
}

// TestNotification_ListenerPathTriggersIgnoreMarksOtherHandles validates
// the END-TO-END wire from listener-path 0-byte terminal detection through
// the Ignore branch: a 0-byte terminal sample on handle Hdead must mark
// every OTHER active handle stale so its next delivery carries Stale=true.
//
// Hardware regression target: TestSymbolVersionIgnore_RemovedSymbolStops
// (symbol_version_hardware_test.go) couldn't observe Stale via the dead
// handle (terminal sample is suppressed by design — see
// TestNotification_TerminalZeroByteSample_TriggersDetection). This test
// proves the marking still propagates to surviving subscriptions.
//
// Validates: R-CACHE-009 (listener-path detection) + R-CACHE-012 +
// R-NOT-016 (callback) + R-NOT-017 (Ignore branch marks all handles).
func TestNotification_ListenerPathTriggersIgnoreMarksOtherHandles(t *testing.T) {
	conn := newTestConnection()
	defer conn.lifecycle.shutdown()
	conn.versionStrategy = SymbolVersionIgnore

	cbReason := make(chan string, 1)
	conn.versionCallback = func(r string) {
		select {
		case cbReason <- r:
		default:
		}
	}

	const hDead, hLive uint32 = 100, 200
	chDead := make(chan *Update, 4)
	chLive := make(chan *Update, 4)
	symDead := &Symbol{FullName: "MAIN.dead", DataType: "DINT", Length: 4, Notification: chDead}
	symLive := &Symbol{FullName: "MAIN.live", DataType: "INT", Length: 2, Notification: chLive}

	conn.notifications.activeNotifications[hDead] = symDead
	conn.notifications.activeNotifications[hLive] = symLive
	conn.cache.symbols[symbolKey(symDead.FullName)] = symDead
	conn.cache.symbols[symbolKey(symLive.FullName)] = symLive

	// Inject 0-byte terminal sample on hDead — listener path fires
	// handleStaleDetection(0x710), Ignore branch must mark BOTH handles.
	terminal := buildNotificationPacket(hDead, 0, []byte{})
	if err := conn.drivePacket(conn.lifecycle.ctx, terminal); err != nil {
		t.Logf("drivePacket terminal err (acceptable): %v", err)
	}

	// Callback must fire with symbol-not-found.
	select {
	case r := <-cbReason:
		if r != ReasonSymbolNotFound {
			t.Errorf("callback reason = %q, want %q", r, ReasonSymbolNotFound)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not fire from listener-path detection")
	}

	// Dead handle: by design no Update emitted.
	select {
	case u := <-chDead:
		t.Errorf("unexpected Update on dead handle: %+v", u)
	case <-time.After(50 * time.Millisecond):
	}

	// Live handle: next normal sample must carry Stale=true,
	// Reason=ReasonSymbolNotFound (Ignore branch marked it).
	livePkt := buildNotificationPacket(hLive, 0, []byte{0x05, 0x00})
	if err := conn.drivePacket(conn.lifecycle.ctx, livePkt); err != nil {
		t.Fatalf("drivePacket live: %v", err)
	}
	select {
	case u := <-chLive:
		if !u.Stale || u.Reason != ReasonSymbolNotFound {
			t.Errorf("live first post-detection sample: Stale=%v Reason=%q, want Stale=true Reason=%q",
				u.Stale, u.Reason, ReasonSymbolNotFound)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no live sample after listener-path detection")
	}

	// Live handle: second sample must NOT be Stale (one-shot consumed).
	livePkt2 := buildNotificationPacket(hLive, 0, []byte{0x06, 0x00})
	if err := conn.drivePacket(conn.lifecycle.ctx, livePkt2); err != nil {
		t.Fatalf("drivePacket live #2: %v", err)
	}
	select {
	case u := <-chLive:
		if u.Stale || u.Reason != "" {
			t.Errorf("live second sample: Stale=%v Reason=%q, want Stale=false Reason=\"\"",
				u.Stale, u.Reason)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no live sample #2")
	}
}

// Validates: consumeStaleFlag idempotency.
func TestSession_ConsumeStaleFlag_IdempotentSecondCallEmpty(t *testing.T) {
	sess := &Session{}
	sess.markSymbolStale(99, ReasonSymbolNotFound)
	r, ok := sess.consumeStaleFlag(99)
	if !ok || r != ReasonSymbolNotFound {
		t.Errorf("first consume: ok=%v r=%q", ok, r)
	}
	r2, ok2 := sess.consumeStaleFlag(99)
	if ok2 || r2 != "" {
		t.Errorf("second consume: ok=%v r=%q, want (false, \"\")", ok2, r2)
	}
}

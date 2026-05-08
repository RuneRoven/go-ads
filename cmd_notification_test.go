package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"testing"
	"time"
)

// Verify that sending to a closed user channel does NOT panic the listen goroutine.
// Go runtime panics on send-to-closed-channel regardless of select default,
// so deliverNotification must guard with defer recover().
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

func TestDeviceNotification_EmptyPacket(t *testing.T) {
	conn := newTestConnection()
	defer conn.lifecycle.shutdown()

	// Too short — should return error
	err := conn.drivePacket(conn.lifecycle.ctx, []byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for truncated packet")
	}
}

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

func TestWindowsFiletimeConversion(t *testing.T) {
	// Windows FILETIME: 100-nanosecond intervals since 1601-01-01
	// Unix epoch: 1970-01-01 = 11644473600 seconds after 1601-01-01
	tests := []struct {
		name      string
		unixSec   int64
		wantYear  int
		wantMonth time.Month
		wantDay   int
	}{
		{"Unix epoch", 0, 1970, time.January, 1},
		{"Y2K", 946684800, 2000, time.January, 1},
		{"2024", 1704067200, 2024, time.January, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filetime := uint64((tt.unixSec + secToUnixEpoch) * windowsTick)
			// Reverse conversion (same as handleNotification)
			ts := int64(filetime)/windowsTick - secToUnixEpoch
			result := time.Unix(ts, 0).UTC()
			if result.Year() != tt.wantYear || result.Month() != tt.wantMonth || result.Day() != tt.wantDay {
				t.Errorf("got %v, want %d-%02d-%02d", result, tt.wantYear, tt.wantMonth, tt.wantDay)
			}
		})
	}
}

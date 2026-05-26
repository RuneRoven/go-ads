package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"unicode/utf16"
)

// Float comparison tolerances — single source of truth
const (
	toleranceFloat32 = 1e-6  // relative, for REAL (float32 round-trip)
	toleranceFloat64 = 1e-10 // relative, for LREAL (float64 round-trip)
)

// --- Assertion helpers ---

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertEqual(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func assertFloatApprox(t *testing.T, got, want, tol float64) {
	t.Helper()
	if want == 0 {
		if math.Abs(got) > tol {
			t.Errorf("got %v, want ~0 (tolerance %v)", got, tol)
		}
		return
	}
	if math.Abs((got-want)/want) > tol {
		t.Errorf("got %v, want %v (relative tolerance %v)", got, want, tol)
	}
}

// --- Byte-encoding helpers for test data construction ---

func le16(v int16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, uint16(v))
	return b
}

func leu16(v uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, v)
	return b
}

func le32(v int32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(v))
	return b
}

func leu32(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}

func le64(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func leu64(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func leF32(v float32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, math.Float32bits(v))
	return b
}

func leF64(v float64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, math.Float64bits(v))
	return b
}

// --- Notification packet builders ---

// buildNotificationPacket constructs a valid ADS DeviceNotification payload
// with one stamp containing one sample.
func buildNotificationPacket(handle uint32, timestamp uint64, data []byte) []byte {
	buf := new(bytes.Buffer)
	// NotificationStream: Length + Stamps
	streamLen := uint32(8 + 12 + 8 + len(data)) // stream header + stamp header + sample header + data
	binary.Write(buf, binary.LittleEndian, streamLen)
	binary.Write(buf, binary.LittleEndian, uint32(1)) // 1 stamp

	// StampHeader: Timestamp + Samples
	binary.Write(buf, binary.LittleEndian, timestamp)
	binary.Write(buf, binary.LittleEndian, uint32(1)) // 1 sample

	// NotificationSample: Handle + Size
	binary.Write(buf, binary.LittleEndian, handle)
	binary.Write(buf, binary.LittleEndian, uint32(len(data)))

	// Data
	buf.Write(data)
	return buf.Bytes()
}

func buildNotificationPacketMultiSample(stamps []struct {
	timestamp uint64
	samples   []struct {
		handle uint32
		data   []byte
	}
},
) []byte {
	// Calculate total length
	buf := new(bytes.Buffer)
	totalLen := uint32(8) // stream header
	for _, s := range stamps {
		totalLen += 12 // stamp header
		for _, samp := range s.samples {
			totalLen += 8 + uint32(len(samp.data)) // sample header + data
		}
	}
	binary.Write(buf, binary.LittleEndian, totalLen)
	binary.Write(buf, binary.LittleEndian, uint32(len(stamps)))

	for _, s := range stamps {
		binary.Write(buf, binary.LittleEndian, s.timestamp)
		binary.Write(buf, binary.LittleEndian, uint32(len(s.samples)))
		for _, samp := range s.samples {
			binary.Write(buf, binary.LittleEndian, samp.handle)
			binary.Write(buf, binary.LittleEndian, uint32(len(samp.data)))
			buf.Write(samp.data)
		}
	}
	return buf.Bytes()
}

// testEndpoint returns the conventional AMSEndpoint used by unit tests:
// loopback IP, TwinCAT TCP default port, fixed AMS NetID 1.2.3.4.1.1,
// AMS port 851 (PortR0PlcTc3). Tests that need a different target should
// build their own AMSEndpoint inline.
func testEndpoint() AMSEndpoint {
	return AMSEndpoint{
		IP:   "127.0.0.1",
		Port: 48898,
		AMS:  AMSAddress{NetID: [6]byte{1, 2, 3, 4, 1, 1}, Port: 851},
	}
}

// newTestConnection creates a minimal Session for unit testing notification parsing.
// The Session has a synthetic *Client wired with its handleNotification installed
// so packet-level tests can drive `conn.client.Load().deviceNotification(ctx, packet)`
// and exercise the cache-aware handler.
func newTestConnection() *Session {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &Session{
		lifecycle:     &sessionLifecycle{ctx: ctx, shutdown: cancel},
		notifications: &notificationManager{activeNotifications: make(map[uint32]*symbol), configsByKey: make(map[string]struct{})},
		cache:         &symbolCache{symbols: map[string]*symbol{}, onDemandSymbols: map[string]bool{}},
		logger:        getDefaultLogger(),
	}
	conn.client.Store(&Client{
		logger: conn.logger,
		ctx:    ctx,
		cancel: cancel,
	})
	conn.client.Load().SetNotificationHandler(conn.handleNotification)
	return conn
}

// drivePacket feeds a wire-format DeviceNotification packet into the
// Client.deviceNotification decoder, which then dispatches to
// Session.handleNotification via the installed callback.
func (conn *Session) drivePacket(ctx context.Context, packet []byte) error {
	return conn.client.Load().deviceNotification(ctx, packet)
}

// --- testLogHandler — captures slog records for assertions ---

// logRecord captures a single log entry for test assertions.
type logRecord struct {
	Level   slog.Level
	Message string
}

// testLogHandler is a minimal slog.Handler that captures log records for testing.
type testLogHandler struct {
	records []logRecord
	mu      sync.Mutex
}

func (h *testLogHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *testLogHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, logRecord{Level: r.Level, Message: r.Message})
	h.mu.Unlock()
	return nil
}
func (h *testLogHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *testLogHandler) WithGroup(_ string) slog.Handler      { return h }

func (h *testLogHandler) findByMessage(msg string) *logRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if strings.Contains(r.Message, msg) {
			return &r
		}
	}
	return nil
}

// --- WSTRING / UTF-16LE helpers ---

func encodeUTF16LE(s string) []byte {
	encoded := utf16.Encode([]rune(s))
	buf := make([]byte, len(encoded)*2)
	for i, r := range encoded {
		binary.LittleEndian.PutUint16(buf[i*2:], r)
	}
	return buf
}

// --- symbol writeToNode round-trip helper ---

// testWriteRoundTrip writes a value via writeToNode then reads it back via parse and compares.
func testWriteRoundTrip(t *testing.T, dataType string, length uint32, value string) {
	t.Helper()
	sym := &symbol{DataType: dataType, Length: length}
	data, err := sym.writeToNode(value, nil)
	if err != nil {
		t.Fatalf("writeToNode(%q, %q) error: %v", dataType, value, err)
	}

	sym2 := &symbol{DataType: dataType, Length: length}
	parsed, err := sym2.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("parse(%q) error: %v", dataType, err)
	}
	if parsed != value {
		t.Errorf("round-trip %q: wrote %q, got back %q", dataType, value, parsed)
	}
}

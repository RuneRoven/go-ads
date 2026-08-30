package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
		notifications: &notificationManager{activeNotifications: make(map[uint32]activeNotification), configsByKey: make(map[string]struct{}), orphanSeen: make(map[uint32]time.Time), orphanSem: make(chan struct{}, orphanDeleteMaxConcurrency)},
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
//
// Attrs is rendered with %v rather than kept as `any`: every assertion this
// suite makes on an attribute is "is it there, and does it say what I expect",
// and comparing strings keeps those assertions readable. Attributes used to be
// discarded entirely, which made a whole class of log content — the local port
// on a drop, the frame counts behind a drop verdict — untestable, so an empty
// field was indistinguishable from a correct one.
type logRecord struct {
	Level   slog.Level
	Message string
	Attrs   map[string]string
}

// attr returns the value of the named attribute, or "" when the record does not
// carry it. The two cases are distinguished by hasAttr.
func (r *logRecord) attr(key string) string { return r.Attrs[key] }

// hasAttr reports whether the record carries the named attribute at all, so a
// test can tell "absent" from "present but empty" — the difference between a log
// site that was never reached and one that logged a zero value.
func (r *logRecord) hasAttr(key string) bool {
	_, ok := r.Attrs[key]
	return ok
}

// testLogHandler is a minimal slog.Handler that captures log records for testing.
//
// WithAttrs returns a derived handler that records into the same store, so
// attributes attached by the library through logger.With() (which is how the
// session tags records) survive into the assertions instead of being dropped.
type testLogHandler struct {
	records []logRecord
	mu      sync.Mutex

	// parent, with and groups are set only on derived handlers. The zero value is
	// a usable root handler, which is how every test constructs one.
	parent *testLogHandler
	with   []attrBatch
	groups []string
}

// attrBatch is the attributes from one WithAttrs call together with the group
// prefix that was open when it was made.
//
// The prefix has to travel with the batch rather than being read from the
// handler at Handle time: slog's contract is that a group qualifies only the
// attributes added after it, so logger.With("request_id", x).WithGroup("net")
// must still record "request_id", not "net.request_id".
type attrBatch struct {
	prefix []string
	attrs  []slog.Attr
}

// root returns the handler that owns the record store.
func (h *testLogHandler) root() *testLogHandler {
	for h.parent != nil {
		h = h.parent
	}
	return h
}

func (h *testLogHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

// collectAttr records one attribute under its group-qualified key, flattening
// slog.Group values recursively.
//
// Groups are qualified rather than dropped because a handler that silently
// flattens them can make a test pass on a key the real handler would have
// written as "group.key" — the kind of green that means nothing. Nothing in the
// library groups attributes today; this keeps the helper honest if that changes.
func collectAttr(dst map[string]string, prefix []string, a slog.Attr) {
	// Resolve before the kind check. A slog.LogValuer — secret in this package is
	// one — may return a group, and an unresolved value would land in the map as a
	// single opaque scalar instead of its children.
	a.Value = a.Value.Resolve()
	if a.Value.Kind() == slog.KindGroup {
		inner := a.Value.Group()
		if len(inner) == 0 {
			return // slog: a group with no attributes is ignored
		}
		next := prefix
		if a.Key != "" { // an empty group key inlines its attributes
			next = append(append([]string{}, prefix...), a.Key)
		}
		for _, sub := range inner {
			collectAttr(dst, next, sub)
		}
		return
	}
	key := a.Key
	if len(prefix) > 0 {
		key = strings.Join(prefix, ".") + "." + key
	}
	dst[key] = a.Value.String()
}

func (h *testLogHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]string, r.NumAttrs()+len(h.with))
	for _, batch := range h.with {
		for _, a := range batch.attrs {
			collectAttr(attrs, batch.prefix, a)
		}
	}
	r.Attrs(func(a slog.Attr) bool {
		collectAttr(attrs, h.groups, a)
		return true
	})
	root := h.root()
	root.mu.Lock()
	root.records = append(root.records, logRecord{Level: r.Level, Message: r.Message, Attrs: attrs})
	root.mu.Unlock()
	return nil
}

func (h *testLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	// h.groups is never mutated in place — WithGroup builds a fresh slice — so the
	// batch can share it. attrs is the caller's slice and is copied.
	merged := make([]attrBatch, 0, len(h.with)+1)
	merged = append(merged, h.with...)
	merged = append(merged, attrBatch{prefix: h.groups, attrs: slices.Clone(attrs)})
	return &testLogHandler{parent: h.root(), with: merged, groups: h.groups}
}

func (h *testLogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h // slog: an empty group name must return the receiver
	}
	return &testLogHandler{
		parent: h.root(),
		with:   h.with,
		groups: append(append([]string{}, h.groups...), name),
	}
}

func (h *testLogHandler) findByMessage(msg string) *logRecord {
	root := h.root()
	root.mu.Lock()
	defer root.mu.Unlock()
	for _, r := range root.records {
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

// --- helpers for the AMS peer-listener tests ---

// freeLocalPort reserves and releases a loopback port, returning its number. The
// small race (something else could take it) is acceptable in tests and keeps the
// stub and the listener agreeing on one number.
func freeLocalPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		_ = ln.Close()
		t.Fatalf("unexpected addr type %T", ln.Addr())
	}
	_ = ln.Close()
	return addr.Port
}

func localAddr(port int) string {
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
}

// --- stub AMS router: counts AddRoute requests and answers them ---

// routeResponder is a UDP stub that speaks the AddRoute half of the AMS router
// protocol, so tests can count how many times a session registers a route.
type routeResponder struct {
	// dropFirst silently swallows this many registration datagrams before
	// answering, so a test can tell a retransmit from a single-shot send.
	dropFirst atomic.Int32
	// noiseFirst sends this many junk datagrams BEFORE the real reply. Injected
	// here rather than from a third party because the client dials a connected UDP
	// socket, which only ever receives from this responder's address — noise sent
	// from anywhere else cannot reach the code that is supposed to skip it.
	noiseFirst atomic.Int32

	pc    *net.UDPConn
	port  int
	adds  atomic.Int64
	done  chan struct{}
	wg    sync.WaitGroup
	reply atomic.Int64 // result code to answer with (0 = success)

	// netIDMu guards netIDs, the source NetID carried by each registration. Kept
	// so a test can assert on the identity that went on the wire and not only on
	// how many datagrams did — a torn read of Session.source shows up here as a
	// NetID that was never written.
	netIDMu sync.Mutex
	netIDs  [][6]byte
}

func startRouteResponder(t *testing.T) *routeResponder {
	t.Helper()
	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("route responder listen: %v", err)
	}
	addr, ok := pc.LocalAddr().(*net.UDPAddr)
	if !ok {
		_ = pc.Close()
		t.Fatalf("unexpected addr type %T", pc.LocalAddr())
	}
	r := &routeResponder{pc: pc, port: addr.Port, done: make(chan struct{})}
	r.wg.Add(1)
	go r.serve()
	t.Cleanup(func() { close(r.done); _ = pc.Close(); r.wg.Wait() })
	return r
}

func (r *routeResponder) serve() {
	defer r.wg.Done()
	buf := make([]byte, 2048)
	for {
		select {
		case <-r.done:
			return
		default:
		}
		_ = r.pc.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, from, err := r.pc.ReadFromUDP(buf)
		if err != nil || n < 24 {
			continue
		}
		invokeID := binary.LittleEndian.Uint32(buf[4:8])
		service := binary.LittleEndian.Uint32(buf[8:12])
		if service != routeServiceAdd {
			continue
		}
		r.adds.Add(1)
		var netID [6]byte
		copy(netID[:], buf[12:18])
		r.netIDMu.Lock()
		r.netIDs = append(r.netIDs, netID)
		r.netIDMu.Unlock()
		for r.noiseFirst.Load() > 0 {
			r.noiseFirst.Add(-1)
			_, _ = r.pc.WriteToUDP([]byte("junk on the shared AMS router port"), from)
		}
		if drop := r.dropFirst.Load(); drop > 0 {
			// Model a lost datagram on a plant network: count it, answer nothing.
			r.dropFirst.Add(-1)
			continue
		}
		// cookie + invokeID + (RESPONSE|service) + AmsAddr + tagCount, then tag 1
		// carrying the 4-byte result.
		resp := make([]byte, 0, 40)
		hdr := make([]byte, 24)
		binary.LittleEndian.PutUint32(hdr[0:], routeCookie)
		binary.LittleEndian.PutUint32(hdr[4:], invokeID)
		binary.LittleEndian.PutUint32(hdr[8:], 0x80000000|routeServiceAdd)
		binary.LittleEndian.PutUint32(hdr[20:], 1)
		resp = append(resp, hdr...)
		tag := make([]byte, 8)
		binary.LittleEndian.PutUint16(tag[0:], tagResponseError)
		binary.LittleEndian.PutUint16(tag[2:], 4)
		binary.LittleEndian.PutUint32(tag[4:], uint32(r.reply.Load()))
		resp = append(resp, tag...)
		_, _ = r.pc.WriteToUDP(resp, from)
	}
}

func (r *routeResponder) registrations() int64 { return r.adds.Load() }

// registeredNetIDs returns the source NetID from every registration received.
func (r *routeResponder) registeredNetIDs() [][6]byte {
	r.netIDMu.Lock()
	defer r.netIDMu.Unlock()
	out := make([][6]byte, len(r.netIDs))
	copy(out, r.netIDs)
	return out
}

// countByMessage reports how many records contain msg. Separate from
// findByMessage because "did this happen at all" and "did this happen once
// rather than every tick" are different questions.
func (h *testLogHandler) countByMessage(msg string) int {
	root := h.root()
	root.mu.Lock()
	defer root.mu.Unlock()
	n := 0
	for _, r := range root.records {
		if strings.Contains(r.Message, msg) {
			n++
		}
	}
	return n
}

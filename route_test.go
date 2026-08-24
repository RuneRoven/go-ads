package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// F-24: parseRouteResponse must reject a response whose invokeID does not
// match the expected value. This is the spoof defense.
//
// Validates: R-CMD-008.
func TestParseRouteResponse_RejectsInvokeIdMismatch(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	resp := make([]byte, 24)
	binary.LittleEndian.PutUint32(resp[0:], routeCookie)
	binary.LittleEndian.PutUint32(resp[4:], 0xCAFEBABE)                 // response invokeID
	binary.LittleEndian.PutUint32(resp[8:], 0x80000000|routeServiceAdd) // valid serviceId

	err := parseRouteResponse(logger, resp, 0xDEADBEEF) // expecting different invokeID
	if err == nil {
		t.Fatalf("expected invokeID mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "invokeID") {
		t.Errorf("expected error to mention invokeID, got: %v", err)
	}
}

// parseRouteResponse with a matching invokeID but NO error tag must return an
// error. Previously the absence of tagResponseError was treated as success,
// which masked malformed/truncated PLC responses — caller would see Connect
// succeed and every subsequent ADS command fail with TargetNotFound. Updated
// per v2.2 to require the error tag explicitly.
//
// Validates: R-CMD-008.
func TestParseRouteResponse_NoErrorTagRejected(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	resp := make([]byte, 24)
	binary.LittleEndian.PutUint32(resp[0:], routeCookie)
	binary.LittleEndian.PutUint32(resp[4:], 0x12345678)
	binary.LittleEndian.PutUint32(resp[8:], 0x80000000|routeServiceAdd)
	// tagCount=0; no error tag → must surface as error now.

	err := parseRouteResponse(logger, resp, 0x12345678)
	if err == nil {
		t.Errorf("expected error for missing error tag, got nil")
	}
}

// F-24: buildRoutePacket must encode the provided invokeID at offset 4.
//
// Validates: R-CMD-008.
func TestBuildRoutePacket_EncodesInvokeId(t *testing.T) {
	netId := [6]byte{1, 2, 3, 4, 5, 6}
	pkt := buildRoutePacket(netId, "route", "host", "admin", "pwd", 0xABCDEF12)
	if len(pkt) < 24 {
		t.Fatalf("packet too short: %d", len(pkt))
	}
	got := binary.LittleEndian.Uint32(pkt[4:])
	if got != 0xABCDEF12 {
		t.Errorf("invokeID in packet = 0x%08X, want 0xABCDEF12", got)
	}
}

// --- buildRoutePacket ---

// Validates: R-ROUTE-001.
func TestBuildRoutePacket(t *testing.T) {
	netID := [6]byte{192, 168, 1, 100, 1, 1}
	packet := buildRoutePacket(netID, "TestRoute", "192.168.1.100", "Admin", "secret", 0)

	// Verify header
	if len(packet) < 24 {
		t.Fatalf("packet too short: %d bytes", len(packet))
	}

	// Cookie
	cookie := binary.LittleEndian.Uint32(packet[0:])
	if cookie != routeCookie {
		t.Errorf("cookie = 0x%08X, want 0x%08X", cookie, routeCookie)
	}

	// InvokeID
	invokeID := binary.LittleEndian.Uint32(packet[4:])
	if invokeID != 0 {
		t.Errorf("invokeID = %d, want 0", invokeID)
	}

	// ServiceID
	serviceID := binary.LittleEndian.Uint32(packet[8:])
	if serviceID != routeServiceAdd {
		t.Errorf("serviceID = %d, want %d", serviceID, routeServiceAdd)
	}

	// AmsAddr: NetID at offset 12, Port at offset 18
	var parsedNetID [6]byte
	copy(parsedNetID[:], packet[12:18])
	if parsedNetID != netID {
		t.Errorf("NetID = %v, want %v", parsedNetID, netID)
	}

	port := binary.LittleEndian.Uint16(packet[18:])
	if port != 0 {
		t.Errorf("port = %d, want 0", port)
	}

	// Tag count
	tagCount := binary.LittleEndian.Uint32(packet[20:])
	if tagCount != 5 {
		t.Errorf("tagCount = %d, want 5", tagCount)
	}

	// Verify tags are present by scanning for tag IDs
	tagIDs := make(map[uint16]bool)
	offset := 24
	for offset+4 <= len(packet) {
		tid := binary.LittleEndian.Uint16(packet[offset:])
		tlen := binary.LittleEndian.Uint16(packet[offset+2:])
		tagIDs[tid] = true
		offset += 4 + int(tlen)
	}
	expectedTags := []uint16{tagNetID, tagPassword, tagComputerName, tagRouteName, tagUsername}
	for _, expected := range expectedTags {
		if !tagIDs[expected] {
			t.Errorf("missing tag ID %d in packet", expected)
		}
	}
}

// --- parseRouteResponse ---

// Validates: R-ROUTE-001.
func TestParseRouteResponse_Success(t *testing.T) {
	// Build a successful route response:
	// cookie(4) + invokeID(4) + serviceId(4) + AmsAddr(8) + tagCount(4) + error tag
	resp := make([]byte, 24+8) // header + one tag with 4-byte data
	binary.LittleEndian.PutUint32(resp[0:], routeCookie)
	binary.LittleEndian.PutUint32(resp[4:], 0) // invokeID
	binary.LittleEndian.PutUint32(resp[8:], 0x80000000|routeServiceAdd)
	// AmsAddr (8 bytes) at offset 12 - zero is fine
	binary.LittleEndian.PutUint32(resp[20:], 1) // tagCount = 1
	// Error tag: id=1, len=4, data=0 (success)
	binary.LittleEndian.PutUint16(resp[24:], tagResponseError)
	binary.LittleEndian.PutUint16(resp[26:], 4)
	binary.LittleEndian.PutUint32(resp[28:], 0) // success

	err := parseRouteResponse(getDefaultLogger(), resp, 0)
	if err != nil {
		t.Errorf("expected success, got error: %v", err)
	}
}

// Validates: R-ROUTE-001.
func TestParseRouteResponse_ErrorCode(t *testing.T) {
	resp := make([]byte, 32)
	binary.LittleEndian.PutUint32(resp[0:], routeCookie)
	binary.LittleEndian.PutUint32(resp[8:], 0x80000000|routeServiceAdd)
	binary.LittleEndian.PutUint32(resp[20:], 1)
	binary.LittleEndian.PutUint16(resp[24:], tagResponseError)
	binary.LittleEndian.PutUint16(resp[26:], 4)
	binary.LittleEndian.PutUint32(resp[28:], 7) // error code 7

	err := parseRouteResponse(getDefaultLogger(), resp, 0)
	if err == nil {
		t.Error("expected error for non-zero error code")
	}
}

// Validates: R-ROUTE-001.
func TestParseRouteResponse_TooShort(t *testing.T) {
	err := parseRouteResponse(getDefaultLogger(), []byte{1, 2, 3}, 0)
	if err == nil {
		t.Error("expected error for short response")
	}
}

// Validates: R-ROUTE-001.
func TestParseRouteResponse_WrongCookie(t *testing.T) {
	resp := make([]byte, 24)
	binary.LittleEndian.PutUint32(resp[0:], 0xDEADBEEF) // wrong cookie
	binary.LittleEndian.PutUint32(resp[8:], 0x80000000|routeServiceAdd)

	err := parseRouteResponse(getDefaultLogger(), resp, 0)
	if err == nil {
		t.Error("expected error for wrong cookie")
	}
}

// Validates: R-ROUTE-001.
func TestParseRouteResponse_WrongServiceID(t *testing.T) {
	resp := make([]byte, 24)
	binary.LittleEndian.PutUint32(resp[0:], routeCookie)
	binary.LittleEndian.PutUint32(resp[8:], 0x12345678) // wrong serviceId

	err := parseRouteResponse(getDefaultLogger(), resp, 0)
	if err == nil {
		t.Error("expected error for wrong serviceId")
	}
}

// F-25: AddRemoteRouteWithLogger must not panic when logger is nil.
// A nil logger must be replaced by getDefaultLogger() before first use.
//
// Aimed at a local responder on an ephemeral port, never at 48899: passing a
// bare "127.0.0.1" made this test write a real routeServiceAdd datagram (source
// NetID 0.0.0.0.0.0, credentials in cleartext) to the host's own AMS router
// port, and burn the full 6s retransmit budget whenever anything held that port
// or ICMP port-unreachable was suppressed — invisibly, because the error was
// discarded. Asserting err and the registration count also covers the host:port
// branch of splitHostRouterPort.
func TestAddRemoteRouteWithLoggerNilLogger(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked with nil logger: %v", r)
		}
	}()
	router := startRouteResponder(t) // 127.0.0.1, ephemeral UDP port
	err := AddRemoteRouteWithLogger(nil,
		net.JoinHostPort("127.0.0.1", strconv.Itoa(router.port)),
		[6]byte{192, 168, 3, 52, 1, 1}, "go-ads-nil-logger", "127.0.0.1", "Administrator", "1")
	if err != nil {
		t.Fatalf("registration against the local responder failed: %v", err)
	}
	if got := router.registrations(); got != 1 {
		t.Errorf("registration datagrams = %d, want 1", got)
	}
}

// TestAddRoute_RetransmitsOnALostDatagram: one dropped UDP datagram must not fail
// a registration.
//
// identify already retransmits three times, with a comment recording why: a single
// dropped datagram was observed failing NewSession outright. Registration ran on
// the same plant networks over the same shared port 48899 with a single Write and a
// single Read, and its failure is the costlier one — Connect aborts on it.
func TestAddRoute_RetransmitsOnALostDatagram(t *testing.T) {
	router := startRouteResponder(t)
	router.dropFirst.Store(1) // the first attempt is lost

	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	netID := [6]byte{192, 168, 3, 52, 1, 1}
	err := addRemoteRouteFrom(logger, nil, "127.0.0.1", router.port, netID, "go-ads-retransmit",
		"127.0.0.1", "Administrator", "1")
	if err != nil {
		t.Fatalf("AddRoute gave up after one lost datagram: %v", err)
	}
	if got := router.adds.Load(); got < 2 {
		t.Errorf("registration datagrams sent = %d, want at least 2: the first was dropped, so a single-shot send "+
			"cannot have succeeded", got)
	}
}

// TestAddRoute_ReadsTheSourceNetIDUnderItsLock: Session.AddRoute is exported and
// callable from any goroutine, and it read sess.source with no lock while
// localHandshake writes that field under tx.connMu from the reconnect goroutine.
//
// The read that matters is the one INSIDE the spawned goroutine: the code's own
// comment says that goroutine outlives the AddRoute call when the caller's context
// is cancelled, so the racing read is on a detached goroutine with no join. A torn
// NetID there registers a route for a spliced identity — the PLC answers nothing,
// the session burns its whole unserved/cooldown budget, and a junk entry is left in
// the device's route table, which is the failure shape that took two TC3 devices
// mute (see route.go on duplicate entries for one NetID).
//
// The assertion is the race detector's; this test earns its place in the -race job.
// It is deterministic in the accesses it makes, not in whether the detector sees
// them, so the value assertion below stands in when it does not: every registration
// must carry one of the two whole NetIDs, never a mixture of both.
func TestAddRoute_ReadsTheSourceNetIDUnderItsLock(t *testing.T) {
	router := startRouteResponder(t)

	sess := newDialableTestSession(t, "127.0.0.1", 1, 1)
	sess.routerPort = router.port
	sess.route = &routeManager{name: "go-ads-test", username: "Administrator", password: secret("1")}
	// No callbackIP: that is what makes AddRoute derive the host IP from the NetID,
	// which is the second bare read.
	sess.callbackIP = ""

	first := [6]byte{10, 0, 0, 1, 1, 1}
	second := [6]byte{192, 168, 3, 52, 1, 1}
	sess.tx.connMu.Lock()
	sess.source = AMSAddress{NetID: first, Port: 10500}
	sess.tx.connMu.Unlock()

	// The writer, doing exactly what localHandshake does: replace the whole address
	// under tx.connMu. Every byte differs between the two NetIDs, so a torn read is
	// visible in the value and not only to the detector.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; i < 500; i++ {
			next := first
			if i%2 == 0 {
				next = second
			}
			sess.tx.connMu.Lock()
			sess.source = AMSAddress{NetID: next, Port: 10500}
			sess.tx.connMu.Unlock()
		}
	}()

	for i := 0; i < 40; i++ {
		if err := sess.AddRoute(context.Background(), sess.route.name, sess.route.username, string(sess.route.password)); err != nil {
			t.Fatalf("AddRoute against the stub router: %v", err)
		}
	}
	<-writerDone

	if got := router.adds.Load(); got < 40 {
		t.Errorf("registrations = %d, want at least 40: the test did not exercise the read it is about", got)
	}
	for _, netID := range router.registeredNetIDs() {
		if netID != first && netID != second {
			t.Errorf("a registration carried NetID %v, which is neither of the two written: the read was torn", netID)
		}
	}
}

// TestAddRoute_IgnoresAnUnrelatedDatagram: a datagram that is not our answer must
// be skipped, not treated as a failure.
//
// The noise has to come from the responder's own address. An earlier version of
// this test opened a second UDP socket and wrote to the router, which proved
// nothing: the client dials a CONNECTED socket (net.DialUDP), so it only ever
// receives from the address it dialled and the junk never reached the code under
// test. Verified by mutation — making the skip branch fail immediately left that
// version green.
func TestAddRoute_IgnoresAnUnrelatedDatagram(t *testing.T) {
	router := startRouteResponder(t)
	router.noiseFirst.Store(1)

	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	netID := [6]byte{192, 168, 3, 52, 1, 1}
	if err := addRemoteRouteFrom(logger, nil, "127.0.0.1", router.port, netID, "go-ads-noise",
		"127.0.0.1", "Administrator", "1"); err != nil {
		t.Errorf("registration failed because of a datagram that was not its answer: %v", err)
	}
	// One send: the junk must be skipped within the same window, not answered by a
	// retransmit.
	if got := router.adds.Load(); got != 1 {
		t.Errorf("registration datagrams sent = %d, want 1: the junk should be skipped inside the read window, "+
			"not waited out until a retransmit", got)
	}
}

// TestAddRoute_RefusalIsNotRetransmitted: an answer that says no must be reported
// at once.
//
// parseRouteResponse's error covered both "not ours" and "ours, and refused", and
// both landed in the skip-and-keep-reading branch. So a wrong password — answered
// immediately by the router — was waited out for the whole 6s budget and the packet,
// password included, went on the wire three times.
func TestAddRoute_RefusalIsNotRetransmitted(t *testing.T) {
	router := startRouteResponder(t)
	router.reply.Store(int64(0x706)) // whatever the router says no with

	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	netID := [6]byte{192, 168, 3, 52, 1, 1}
	start := time.Now()
	err := addRemoteRouteFrom(logger, nil, "127.0.0.1", router.port, netID, "go-ads-refused",
		"127.0.0.1", "Administrator", "wrong")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("registration reported success although the router refused it")
	}
	if got := router.adds.Load(); got != 1 {
		t.Errorf("the refusal was retransmitted %d times; the answer was already in hand, and each retransmission puts the "+
			"route password on the wire again", got)
	}
	if elapsed > 2*time.Second {
		t.Errorf("took %v to report a refusal answered immediately; the caller waited out the retransmit budget", elapsed)
	}
}

package ads

import (
	"bytes"
	"encoding/binary"
	"log/slog"
	"net"
	"strings"
	"testing"
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
func TestAddRemoteRouteWithLoggerNilLogger(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked with nil logger: %v", r)
		}
	}()
	// unreachable host → network error, not panic
	err := AddRemoteRouteWithLogger(nil, "127.0.0.1", [6]byte{}, "route", "host", "user", "pass")
	_ = err
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

// TestAddRoute_IgnoresAnUnrelatedDatagram: port 48899 is shared with identify, so a
// datagram meant for somebody else must not fail a registration.
func TestAddRoute_IgnoresAnUnrelatedDatagram(t *testing.T) {
	router := startRouteResponder(t)

	// Something else on the port answers first, with a payload that is not a route
	// reply. The registration must keep reading rather than fail on it.
	go func() {
		conn, err := net.Dial("udp4", localAddr(router.port))
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = conn.Write([]byte("not an ads route reply, just noise on the shared port"))
	}()

	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	netID := [6]byte{192, 168, 3, 52, 1, 1}
	if err := addRemoteRouteFrom(logger, nil, "127.0.0.1", router.port, netID, "go-ads-noise",
		"127.0.0.1", "Administrator", "1"); err != nil {
		t.Errorf("registration failed because of an unrelated datagram on the shared port: %v", err)
	}
}

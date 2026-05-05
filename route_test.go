package ads

import (
	"bytes"
	"encoding/binary"
	"log/slog"
	"strings"
	"testing"
)

// F-24: parseRouteResponse must reject a response whose invokeId does not
// match the expected value. This is the spoof defense.
func TestParseRouteResponse_RejectsInvokeIdMismatch(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	resp := make([]byte, 24)
	binary.LittleEndian.PutUint32(resp[0:], routeCookie)
	binary.LittleEndian.PutUint32(resp[4:], 0xCAFEBABE)                 // response invokeId
	binary.LittleEndian.PutUint32(resp[8:], 0x80000000|routeServiceAdd) // valid serviceId

	err := parseRouteResponse(logger, resp, 0xDEADBEEF) // expecting different invokeId
	if err == nil {
		t.Fatalf("expected invokeId mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "invokeId") {
		t.Errorf("expected error to mention invokeId, got: %v", err)
	}
}

// F-24: parseRouteResponse must accept a response whose invokeId matches.
func TestParseRouteResponse_AcceptsMatchingInvokeId(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	resp := make([]byte, 24)
	binary.LittleEndian.PutUint32(resp[0:], routeCookie)
	binary.LittleEndian.PutUint32(resp[4:], 0x12345678)
	binary.LittleEndian.PutUint32(resp[8:], 0x80000000|routeServiceAdd)
	// tagCount=0; no error tag → "no error tag found, assuming success"

	err := parseRouteResponse(logger, resp, 0x12345678)
	if err != nil {
		t.Errorf("expected nil error for matching invokeId, got: %v", err)
	}
}

// F-24: buildRoutePacket must encode the provided invokeId at offset 4.
func TestBuildRoutePacket_EncodesInvokeId(t *testing.T) {
	netId := [6]byte{1, 2, 3, 4, 5, 6}
	pkt := buildRoutePacket(netId, "route", "host", "admin", "pwd", 0xABCDEF12)
	if len(pkt) < 24 {
		t.Fatalf("packet too short: %d", len(pkt))
	}
	got := binary.LittleEndian.Uint32(pkt[4:])
	if got != 0xABCDEF12 {
		t.Errorf("invokeId in packet = 0x%08X, want 0xABCDEF12", got)
	}
}

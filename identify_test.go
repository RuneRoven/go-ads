package ads

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"
)

// buildIdentifyResponse assembles a response of the shape a TwinCAT router
// sends, so the parser is tested against the documented layout rather than
// against itself.
func buildIdentifyResponse(invokeID uint32, netID [6]byte, port uint16, tags [][2]any) []byte {
	var tagBytes []byte
	for _, t := range tags {
		id, _ := t[0].(int)
		data, _ := t[1].([]byte)
		tag := make([]byte, 4+len(data))
		binary.LittleEndian.PutUint16(tag[0:], uint16(id))
		binary.LittleEndian.PutUint16(tag[2:], uint16(len(data)))
		copy(tag[4:], data)
		tagBytes = append(tagBytes, tag...)
	}
	out := make([]byte, 24, 24+len(tagBytes))
	binary.LittleEndian.PutUint32(out[0:], routeCookie)
	binary.LittleEndian.PutUint32(out[4:], invokeID)
	binary.LittleEndian.PutUint32(out[8:], 0x80000000|routeServiceIdentify)
	copy(out[12:18], netID[:])
	binary.LittleEndian.PutUint16(out[18:], port)
	binary.LittleEndian.PutUint32(out[20:], uint32(len(tags)))
	return append(out, tagBytes...)
}

// TestParseIdentifyResponse_RealShape uses the tag set and values observed on
// hardware: a CX reporting TwinCAT 3.1.4024 as netID 5.66.133.203.1.1 on the
// router's own port 10000.
func TestParseIdentifyResponse_RealShape(t *testing.T) {
	const invokeID = 0xAABBCCDD
	version := []byte{3, 1, 0xB8, 0x0F} // 3.1.4024
	resp := buildIdentifyResponse(invokeID, [6]byte{5, 66, 133, 203, 1, 1}, 10000, [][2]any{
		{int(tagComputerName), append([]byte("CX-4285CB"), 0)},
		{int(tagSystemVersion), version},
		{4, []byte{0x14, 0x01, 0, 0, 7, 0, 0, 0}}, // uninterpreted system-info blob
	})

	id, err := parseIdentifyResponse(resp, invokeID)
	if err != nil {
		t.Fatalf("parseIdentifyResponse: %v", err)
	}
	if got, want := id.AMS.NetIDString(), "5.66.133.203.1.1"; got != want {
		t.Errorf("NetID = %q, want %q", got, want)
	}
	if id.AMS.Port != 10000 {
		t.Errorf("Port = %d, want 10000 (the router's own port)", id.AMS.Port)
	}
	if got, want := id.HostName, "CX-4285CB"; got != want {
		t.Errorf("HostName = %q, want %q (trailing NUL must be trimmed)", got, want)
	}
	if got, want := id.Version(), "3.1.4024"; got != want {
		t.Errorf("Version() = %q, want %q", got, want)
	}
	if got, want := len(id.Tags), 3; got != want {
		t.Errorf("Tags len = %d, want %d (uninterpreted tags must be preserved)", got, want)
	}
	if _, ok := id.Tags[4]; !ok {
		t.Error("tag 4 missing from Tags; callers rely on raw access for platform details")
	}
}

// TestParseIdentifyResponse_Rejects covers every malformed or hostile response
// the parser must refuse rather than return a half-filled identity for.
func TestParseIdentifyResponse_Rejects(t *testing.T) {
	const invokeID = 0x11223344
	good := func() []byte {
		return buildIdentifyResponse(invokeID, [6]byte{5, 1, 2, 3, 1, 1}, 10000, [][2]any{
			{int(tagComputerName), append([]byte("PLC"), 0)},
		})
	}

	tests := []struct {
		name string
		data []byte
	}{
		{name: "too short", data: good()[:23]},
		{
			name: "wrong cookie",
			data: func() []byte { b := good(); binary.LittleEndian.PutUint32(b[0:], 0xDEADBEEF); return b }(),
		},
		{
			name: "invokeID mismatch",
			data: func() []byte { b := good(); binary.LittleEndian.PutUint32(b[4:], invokeID+1); return b }(),
		},
		{
			name: "serviceId without response flag",
			data: func() []byte { b := good(); binary.LittleEndian.PutUint32(b[8:], routeServiceIdentify); return b }(),
		},
		{
			name: "zero NetID",
			data: buildIdentifyResponse(invokeID, [6]byte{}, 10000, nil),
		},
		{
			name: "tag header truncated",
			data: func() []byte { b := good(); return b[:len(b)-4] }(),
		},
		{
			name: "tag data exceeds response",
			data: func() []byte {
				b := good()
				binary.LittleEndian.PutUint16(b[26:], 0xFFFF) // tag length past the buffer
				return b
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseIdentifyResponse(tt.data, invokeID); err == nil {
				t.Error("err = nil, want rejection")
			}
		})
	}
}

// TestRemoteIdentity_RuntimePort: the identify service never reports a runtime
// port, so this is a per-major-version convention.
func TestRemoteIdentity_RuntimePort(t *testing.T) {
	tests := []struct {
		name  string
		major uint8
		want  uint16
	}{
		{name: "TwinCAT 2 uses 801", major: 2, want: 801},
		{name: "TwinCAT 3 uses 851", major: 3, want: 851},
		{name: "unknown major defaults to 851", major: 0, want: 851},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := RemoteIdentity{Major: tt.major}
			if got := id.RuntimePort(); got != tt.want {
				t.Errorf("RuntimePort() = %d, want %d (major=%d)", got, tt.want, tt.major)
			}
		})
	}
}

// TestBuildIdentifyPacket pins the request: tag-less, zero source AmsAddr, and
// the identify service id. Every tested TwinCAT answers this, which is what
// makes discovery need no local configuration.
func TestBuildIdentifyPacket(t *testing.T) {
	const invokeID = 0x01020304
	pkt := buildIdentifyPacket(invokeID)
	if len(pkt) != 24 {
		t.Fatalf("len = %d, want 24 (header only, no tags)", len(pkt))
	}
	if got := binary.LittleEndian.Uint32(pkt[0:]); got != routeCookie {
		t.Errorf("cookie = 0x%08X, want 0x%08X", got, routeCookie)
	}
	if got := binary.LittleEndian.Uint32(pkt[4:]); got != invokeID {
		t.Errorf("invokeID = 0x%08X, want 0x%08X", got, invokeID)
	}
	if got := binary.LittleEndian.Uint32(pkt[8:]); got != routeServiceIdentify {
		t.Errorf("serviceId = %d, want %d", got, routeServiceIdentify)
	}
	for i, b := range pkt[12:20] {
		if b != 0 {
			t.Errorf("source AmsAddr byte %d = 0x%02X, want 0 (discovery must not need a local NetID)", i, b)
		}
	}
	if got := binary.LittleEndian.Uint32(pkt[20:]); got != 0 {
		t.Errorf("tagCount = %d, want 0", got)
	}
}

// startFlakyIdentifyResponder stands in for an AMS router on a lossy network.
// It ignores the first `dropFirst` requests, then answers — optionally sending
// an unrelated datagram first, which port 48899 really does carry (route
// registration replies share it).
func startFlakyIdentifyResponder(t *testing.T, dropFirst int, sendStray bool) (host string, port int, stop func()) {
	t.Helper()
	pc, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	addr, ok := pc.LocalAddr().(*net.UDPAddr)
	if !ok {
		_ = pc.Close()
		t.Fatalf("unexpected addr type: %T", pc.LocalAddr())
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 2048)
		seen := 0
		for {
			select {
			case <-done:
				return
			default:
			}
			_ = pc.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			n, from, err := pc.ReadFromUDP(buf)
			if err != nil {
				continue
			}
			seen++
			if seen <= dropFirst {
				continue // swallow it, as a lossy link would
			}
			invokeID := binary.LittleEndian.Uint32(buf[4:8])
			if sendStray {
				// Wrong invokeID: another exchange's late reply. Must be skipped,
				// not mistaken for ours and not fatal.
				stray := buildIdentifyResponse(invokeID^0xFFFF, [6]byte{9, 9, 9, 9, 1, 1}, 10000, nil)
				_, _ = pc.WriteToUDP(stray, from)
			}
			resp := buildIdentifyResponse(invokeID, [6]byte{5, 1, 2, 3, 1, 1}, 10000, [][2]any{
				{int(tagComputerName), append([]byte("FLAKY-CX"), 0)},
				{int(tagSystemVersion), []byte{3, 1, 0xB8, 0x0F}},
			})
			_, _ = pc.WriteToUDP(resp, from)
			_ = n
		}
	}()

	return addr.IP.String(), addr.Port, func() {
		close(done)
		_ = pc.Close()
		wg.Wait()
	}
}

// TestIdentifyRemote_RetransmitsOnPacketLoss: one lost request must not fail the
// call. This was observed for real — a dropped datagram failed NewSession
// outright, which is a bad trade for a single packet on a plant network.
func TestIdentifyRemote_RetransmitsOnPacketLoss(t *testing.T) {
	host, port, stop := startFlakyIdentifyResponder(t, 1, false)
	defer stop()
	id, err := identifyRemoteFrom(t.Context(), nil, nil, host, port)
	if err != nil {
		t.Fatalf("identify with one dropped request: %v", err)
	}
	if got := id.AMS.NetIDString(); got != "5.1.2.3.1.1" {
		t.Errorf("NetID = %s, want 5.1.2.3.1.1", got)
	}
}

// TestIdentifyRemote_SkipsUnrelatedDatagram: a stray reply on the shared port
// must be discarded, not consume the read and not be reported as malformed.
func TestIdentifyRemote_SkipsUnrelatedDatagram(t *testing.T) {
	host, port, stop := startFlakyIdentifyResponder(t, 0, true)
	defer stop()
	id, err := identifyRemoteFrom(t.Context(), nil, nil, host, port)
	if err != nil {
		t.Fatalf("identify with a stray datagram present: %v", err)
	}
	if got := id.HostName; got != "FLAKY-CX" {
		t.Errorf("HostName = %q, want FLAKY-CX", got)
	}
}

// TestIdentifyResponseIsOurs covers the discriminator used to tell our answer
// from someone else's traffic.
func TestIdentifyResponseIsOurs(t *testing.T) {
	const invokeID = 0x11223344
	good := buildIdentifyResponse(invokeID, [6]byte{5, 1, 2, 3, 1, 1}, 10000, nil)

	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{name: "our answer", data: good, want: true},
		{name: "too short", data: good[:20], want: false},
		{
			name: "another exchange's invokeID",
			data: buildIdentifyResponse(invokeID+1, [6]byte{5, 1, 2, 3, 1, 1}, 10000, nil),
			want: false,
		},
		{
			name: "wrong cookie",
			data: func() []byte { b := append([]byte(nil), good...); binary.LittleEndian.PutUint32(b[0:], 1); return b }(),
			want: false,
		},
		{
			name: "different service",
			data: func() []byte {
				b := append([]byte(nil), good...)
				binary.LittleEndian.PutUint32(b[8:], 0x80000000|routeServiceAdd)
				return b
			}(),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := identifyResponseIsOurs(tt.data, invokeID); got != tt.want {
				t.Errorf("identifyResponseIsOurs() = %v, want %v", got, tt.want)
			}
		})
	}
}

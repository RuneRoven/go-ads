package ads

import (
	"encoding/binary"
	"testing"
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

package ads

import (
	"encoding/binary"
	"math"
	"testing"
)

// --- stringToNetID / StringToNetID ---

func TestStringToNetID(t *testing.T) {
	tests := []struct {
		input    string
		expected [6]byte
	}{
		{"192.168.1.1.1.1", [6]byte{192, 168, 1, 1, 1, 1}},
		{"5.154.236.19.1.1", [6]byte{5, 154, 236, 19, 1, 1}},
		{"127.0.0.1.1.1", [6]byte{127, 0, 0, 1, 1, 1}},
		{"0.0.0.0.0.0", [6]byte{0, 0, 0, 0, 0, 0}},
		{"255.255.255.255.255.255", [6]byte{255, 255, 255, 255, 255, 255}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := StringToNetID(tt.input)
			if result != tt.expected {
				t.Errorf("StringToNetID(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// --- buildRoutePacket ---

func TestBuildRoutePacket(t *testing.T) {
	netID := [6]byte{192, 168, 1, 100, 1, 1}
	packet := buildRoutePacket(netID, "TestRoute", "192.168.1.100", "Admin", "secret")

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

func TestParseRouteResponse_Success(t *testing.T) {
	// Build a successful route response:
	// cookie(4) + invokeId(4) + serviceId(4) + AmsAddr(8) + tagCount(4) + error tag
	resp := make([]byte, 24+8) // header + one tag with 4-byte data
	binary.LittleEndian.PutUint32(resp[0:], routeCookie)
	binary.LittleEndian.PutUint32(resp[4:], 0) // invokeId
	binary.LittleEndian.PutUint32(resp[8:], 0x80000000|routeServiceAdd)
	// AmsAddr (8 bytes) at offset 12 - zero is fine
	binary.LittleEndian.PutUint32(resp[20:], 1) // tagCount = 1
	// Error tag: id=1, len=4, data=0 (success)
	binary.LittleEndian.PutUint16(resp[24:], tagResponseError)
	binary.LittleEndian.PutUint16(resp[26:], 4)
	binary.LittleEndian.PutUint32(resp[28:], 0) // success

	err := parseRouteResponse(resp)
	if err != nil {
		t.Errorf("expected success, got error: %v", err)
	}
}

func TestParseRouteResponse_ErrorCode(t *testing.T) {
	resp := make([]byte, 32)
	binary.LittleEndian.PutUint32(resp[0:], routeCookie)
	binary.LittleEndian.PutUint32(resp[8:], 0x80000000|routeServiceAdd)
	binary.LittleEndian.PutUint32(resp[20:], 1)
	binary.LittleEndian.PutUint16(resp[24:], tagResponseError)
	binary.LittleEndian.PutUint16(resp[26:], 4)
	binary.LittleEndian.PutUint32(resp[28:], 7) // error code 7

	err := parseRouteResponse(resp)
	if err == nil {
		t.Error("expected error for non-zero error code")
	}
}

func TestParseRouteResponse_TooShort(t *testing.T) {
	err := parseRouteResponse([]byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for short response")
	}
}

func TestParseRouteResponse_WrongCookie(t *testing.T) {
	resp := make([]byte, 24)
	binary.LittleEndian.PutUint32(resp[0:], 0xDEADBEEF) // wrong cookie
	binary.LittleEndian.PutUint32(resp[8:], 0x80000000|routeServiceAdd)

	err := parseRouteResponse(resp)
	if err == nil {
		t.Error("expected error for wrong cookie")
	}
}

func TestParseRouteResponse_WrongServiceID(t *testing.T) {
	resp := make([]byte, 24)
	binary.LittleEndian.PutUint32(resp[0:], routeCookie)
	binary.LittleEndian.PutUint32(resp[8:], 0x12345678) // wrong serviceId

	err := parseRouteResponse(resp)
	if err == nil {
		t.Error("expected error for wrong serviceId")
	}
}

// --- downgradeTransMode ---

func TestDowngradeTransMode(t *testing.T) {
	tests := []struct {
		input    TransMode
		expected TransMode
	}{
		{TransModeServerOnChange2, TransModeServerOnChange},
		{TransModeServerCycle2, TransModeServerCycle},
		{TransModeServerOnChange, TransModeServerOnChange},
		{TransModeServerCycle, TransModeServerCycle},
		{TransModeClientCycle, TransModeClientCycle},
		{TransModeNoTransmission, TransModeNoTransmission},
	}

	for _, tt := range tests {
		t.Run(tt.input.String(), func(t *testing.T) {
			result := downgradeTransMode(tt.input)
			if result != tt.expected {
				t.Errorf("downgradeTransMode(%v) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

// --- TransMode.String ---

func TestTransModeString(t *testing.T) {
	if s := TransModeServerOnChange.String(); s != "ServerOnChange" {
		t.Errorf("got %q, want %q", s, "ServerOnChange")
	}
	if s := TransModeServerCycle2.String(); s != "ServerCycle2" {
		t.Errorf("got %q, want %q", s, "ServerCycle2")
	}
	if s := TransMode(99).String(); s != "Unknown(99)" {
		t.Errorf("got %q, want %q", s, "Unknown(99)")
	}
}

// --- ReturnCode.String ---

func TestReturnCodeString(t *testing.T) {
	tests := []struct {
		code     ReturnCode
		contains string
	}{
		{ReturnCodeNoErrors, "no error"},
		{ReturnCodeGlobalTargetNotFound, "target machine not found"},
		{ReturnCodeDeviceSymbolNoFound, "symbol not found"},
		{ReturnCodeClientSyncTimeout, "timeout elapsed"},
		{ReturnCode(0xFFFF), "unknown error code"},
	}

	for _, tt := range tests {
		t.Run(tt.contains, func(t *testing.T) {
			s := tt.code.String()
			if !containsSubstring(s, tt.contains) {
				t.Errorf("ReturnCode(0x%04X).String() = %q, want it to contain %q", uint32(tt.code), s, tt.contains)
			}
		})
	}
}

func TestReturnCodeError(t *testing.T) {
	rc := ReturnCodeDeviceBusy
	var err error = rc
	if err.Error() == "" {
		t.Error("expected non-empty error string")
	}
}

// --- buildTag ---

func TestBuildTag(t *testing.T) {
	tag := buildTag(7, []byte{192, 168, 1, 1, 1, 1})
	if len(tag) != 10 {
		t.Fatalf("tag length = %d, want 10", len(tag))
	}
	tid := binary.LittleEndian.Uint16(tag[0:])
	if tid != 7 {
		t.Errorf("tag ID = %d, want 7", tid)
	}
	tlen := binary.LittleEndian.Uint16(tag[2:])
	if tlen != 6 {
		t.Errorf("tag length field = %d, want 6", tlen)
	}
}

// --- appendNull ---

func TestAppendNull(t *testing.T) {
	result := appendNull([]byte("hello"))
	if len(result) != 6 {
		t.Fatalf("length = %d, want 6", len(result))
	}
	if result[5] != 0 {
		t.Error("last byte should be null terminator")
	}
}

// --- ParseUploadSymbolInfoDataTypes ---

func TestParseUploadSymbolInfoDataTypes_Empty(t *testing.T) {
	datatypes, err := ParseUploadSymbolInfoDataTypes([]byte{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(datatypes) != 0 {
		t.Errorf("expected empty map, got %d entries", len(datatypes))
	}
}

// --- ParseUploadSymbolInfoSymbols ---

func TestParseUploadSymbolInfoSymbols_Empty(t *testing.T) {
	symbols, err := ParseUploadSymbolInfoSymbols([]byte{}, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(symbols) != 0 {
		t.Errorf("expected empty map, got %d entries", len(symbols))
	}
}

// --- Symbol.parse (readWriter) ---

func TestSymbolParseBOOL(t *testing.T) {
	sym := &Symbol{DataType: "BOOL", Length: 1}
	val, err := sym.parse([]byte{1}, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "true" {
		t.Errorf("got %q, want %q", val, "true")
	}

	sym2 := &Symbol{DataType: "BOOL", Length: 1}
	val, err = sym2.parse([]byte{0}, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "false" {
		t.Errorf("got %q, want %q", val, "false")
	}
}

func TestSymbolParseINT16(t *testing.T) {
	sym := &Symbol{DataType: "INT", Length: 2}
	data := make([]byte, 2)
	data[0] = byte(0xD6) // -42 as little-endian int16 = 0xFFD6
	data[1] = byte(0xFF)
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "-42" {
		t.Errorf("got %q, want %q", val, "-42")
	}
}

func TestSymbolParseUINT(t *testing.T) {
	sym := &Symbol{DataType: "UINT", Length: 2}
	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, 1234)
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "1234" {
		t.Errorf("got %q, want %q", val, "1234")
	}
}

func TestSymbolParseREAL(t *testing.T) {
	sym := &Symbol{DataType: "REAL", Length: 4}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 0x41200000) // 10.0 as float32
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "10" {
		t.Errorf("got %q, want %q", val, "10")
	}
}

func TestSymbolParseSTRING(t *testing.T) {
	sym := &Symbol{DataType: "STRING", Length: 20}
	data := make([]byte, 20)
	copy(data, "Hello\x00")
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "Hello" {
		t.Errorf("got %q, want %q", val, "Hello")
	}
}

func TestSymbolParseUnknownType(t *testing.T) {
	sym := &Symbol{DataType: "UNKNOWN_TYPE", Length: 4}
	_, err := sym.parse([]byte{0, 0, 0, 0}, 0, nil)
	if err == nil {
		t.Error("expected error for unknown data type")
	}
}

// --- strcmp ---

func TestStrcmp(t *testing.T) {
	if strcmp("abc", "abc") != 0 {
		t.Error("equal strings should return 0")
	}
	if strcmp("abc", "abd") >= 0 {
		t.Error("abc < abd")
	}
	if strcmp("abd", "abc") <= 0 {
		t.Error("abd > abc")
	}
	if strcmp("ab", "abc") >= 0 {
		t.Error("shorter string should be less")
	}
}

// helper
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// --- writeToNode round-trip tests ---

// testWriteRoundTrip writes a value via writeToNode then reads it back via parse and compares.
func testWriteRoundTrip(t *testing.T, dataType string, length uint32, value string) {
	t.Helper()
	sym := &Symbol{DataType: dataType, Length: length}
	data, err := sym.writeToNode(value, 0, nil)
	if err != nil {
		t.Fatalf("writeToNode(%q, %q) error: %v", dataType, value, err)
	}

	sym2 := &Symbol{DataType: dataType, Length: length}
	parsed, err := sym2.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("parse(%q) error: %v", dataType, err)
	}
	if parsed != value {
		t.Errorf("round-trip %q: wrote %q, got back %q", dataType, value, parsed)
	}
}

func TestWriteToNodeRoundTrip_BOOL(t *testing.T) {
	testWriteRoundTrip(t, "BOOL", 1, "true")
	testWriteRoundTrip(t, "BOOL", 1, "false")
}

func TestWriteToNodeRoundTrip_BYTE(t *testing.T) {
	testWriteRoundTrip(t, "BYTE", 1, "0")
	testWriteRoundTrip(t, "BYTE", 1, "255")
	testWriteRoundTrip(t, "USINT", 1, "42")
}

func TestWriteToNodeRoundTrip_SINT(t *testing.T) {
	testWriteRoundTrip(t, "SINT", 1, "-128")
	testWriteRoundTrip(t, "SINT", 1, "127")
}

func TestWriteToNodeRoundTrip_UINT(t *testing.T) {
	testWriteRoundTrip(t, "UINT", 2, "0")
	testWriteRoundTrip(t, "UINT", 2, "65535")
	testWriteRoundTrip(t, "WORD", 2, "1234")
	testWriteRoundTrip(t, "UINT16", 2, "5678")
}

func TestWriteToNodeRoundTrip_INT(t *testing.T) {
	testWriteRoundTrip(t, "INT", 2, "-32768")
	testWriteRoundTrip(t, "INT", 2, "32767")
	testWriteRoundTrip(t, "INT16", 2, "-42")
}

func TestWriteToNodeRoundTrip_UDINT(t *testing.T) {
	testWriteRoundTrip(t, "UDINT", 4, "0")
	testWriteRoundTrip(t, "UDINT", 4, "4294967295")
	testWriteRoundTrip(t, "DWORD", 4, "12345678")
}

func TestWriteToNodeRoundTrip_DINT(t *testing.T) {
	testWriteRoundTrip(t, "DINT", 4, "-2147483648")
	testWriteRoundTrip(t, "DINT", 4, "2147483647")
}

func TestWriteToNodeRoundTrip_REAL(t *testing.T) {
	sym := &Symbol{DataType: "REAL", Length: 4}
	data, err := sym.writeToNode("3.14", 0, nil)
	if err != nil {
		t.Fatalf("writeToNode error: %v", err)
	}
	// Verify bytes encode float32(3.14)
	bits := binary.LittleEndian.Uint32(data)
	f := math.Float32frombits(bits)
	if math.Abs(float64(f)-3.14) > 0.001 {
		t.Errorf("REAL: got %v, want ~3.14", f)
	}
}

func TestWriteToNodeRoundTrip_LREAL(t *testing.T) {
	sym := &Symbol{DataType: "LREAL", Length: 8}
	data, err := sym.writeToNode("3.141592653589793", 0, nil)
	if err != nil {
		t.Fatalf("writeToNode error: %v", err)
	}
	bits := binary.LittleEndian.Uint64(data)
	f := math.Float64frombits(bits)
	if math.Abs(f-3.141592653589793) > 1e-10 {
		t.Errorf("LREAL: got %v, want 3.141592653589793", f)
	}
}

func TestWriteToNodeRoundTrip_STRING(t *testing.T) {
	testWriteRoundTrip(t, "STRING", 20, "Hello")
}

func TestWriteToNodeRoundTrip_TIME(t *testing.T) {
	testWriteRoundTrip(t, "TIME", 4, "12:34:56")
	testWriteRoundTrip(t, "TIME", 4, "00:00:00")
}

func TestWriteToNodeRoundTrip_TIME_WithMs(t *testing.T) {
	testWriteRoundTrip(t, "TIME", 4, "12:34:56.789")
}

func TestWriteToNodeRoundTrip_TOD(t *testing.T) {
	testWriteRoundTrip(t, "TOD", 4, "15:30")
	testWriteRoundTrip(t, "TOD", 4, "00:00")
}

func TestWriteToNodeRoundTrip_DATE(t *testing.T) {
	testWriteRoundTrip(t, "DATE", 4, "2024-01-15")
	testWriteRoundTrip(t, "DATE", 4, "2000-06-15")
}

func TestWriteToNodeRoundTrip_DT(t *testing.T) {
	testWriteRoundTrip(t, "DT", 4, "2024-01-15 12:30:00")
	testWriteRoundTrip(t, "DT", 4, "1970-01-01 00:00:00")
}

func TestWriteToNodeRoundTrip_LINT(t *testing.T) {
	testWriteRoundTrip(t, "LINT", 8, "0")
	testWriteRoundTrip(t, "LINT", 8, "-9223372036854775808")
	testWriteRoundTrip(t, "LINT", 8, "9223372036854775807")
}

func TestWriteToNodeRoundTrip_ULINT(t *testing.T) {
	testWriteRoundTrip(t, "ULINT", 8, "0")
	testWriteRoundTrip(t, "ULINT", 8, "18446744073709551615")
	testWriteRoundTrip(t, "LWORD", 8, "42")
}

func TestWriteToNodeStruct(t *testing.T) {
	child1 := &Symbol{
		Name:     "field1",
		FullName: "test.field1",
		DataType: "INT",
		Length:   2,
		Offset:  0,
	}
	child2 := &Symbol{
		Name:     "field2",
		FullName: "test.field2",
		DataType: "DINT",
		Length:   4,
		Offset:  4, // 2 bytes padding after field1
	}
	parent := &Symbol{
		Name:     "test",
		FullName: "test",
		DataType: "ST_Test",
		Length:   8,
		Childs: map[string]*Symbol{
			"field1": child1,
			"field2": child2,
		},
	}

	data, err := parent.writeToNode(`{"field1":"42","field2":"100"}`, 0, nil)
	if err != nil {
		t.Fatalf("struct writeToNode error: %v", err)
	}

	if len(data) != 8 {
		t.Fatalf("expected 8 bytes, got %d", len(data))
	}

	// field1 at offset 0: INT 42
	v1 := int16(binary.LittleEndian.Uint16(data[0:2]))
	if v1 != 42 {
		t.Errorf("field1: got %d, want 42", v1)
	}

	// bytes 2-3 should be zero (padding)
	if data[2] != 0 || data[3] != 0 {
		t.Errorf("padding bytes should be zero: got %v", data[2:4])
	}

	// field2 at offset 4: DINT 100
	v2 := int32(binary.LittleEndian.Uint32(data[4:8]))
	if v2 != 100 {
		t.Errorf("field2: got %d, want 100", v2)
	}
}

func TestWriteToNodeStructPartialFields(t *testing.T) {
	child1 := &Symbol{Name: "x", FullName: "s.x", DataType: "BYTE", Length: 1, Offset: 0}
	child2 := &Symbol{Name: "y", FullName: "s.y", DataType: "BYTE", Length: 1, Offset: 1}
	parent := &Symbol{
		Name: "s", FullName: "s", DataType: "ST_S", Length: 2,
		Childs: map[string]*Symbol{"x": child1, "y": child2},
	}

	// Only write "x", "y" should remain zero
	data, err := parent.writeToNode(`{"x":"7"}`, 0, nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if data[0] != 7 {
		t.Errorf("x: got %d, want 7", data[0])
	}
	if data[1] != 0 {
		t.Errorf("y: got %d, want 0 (unset)", data[1])
	}
}

func TestWriteToNodeAliasResolution(t *testing.T) {
	datatypes := map[string]SymbolUploadDataType{
		"MyInt": {DataType: "INT"},
	}
	sym := &Symbol{DataType: "MyInt", Length: 2}
	data, err := sym.writeToNode("123", 0, datatypes)
	if err != nil {
		t.Fatalf("alias writeToNode error: %v", err)
	}
	v := int16(binary.LittleEndian.Uint16(data))
	if v != 123 {
		t.Errorf("got %d, want 123", v)
	}
}

func TestWriteToNodeAliasWithoutDatatypes(t *testing.T) {
	sym := &Symbol{DataType: "MyCustomType", Length: 4}
	_, err := sym.writeToNode("42", 0, nil)
	if err == nil {
		t.Error("expected error for alias without datatypes")
	}
}

func TestWriteToNodeUnknownType(t *testing.T) {
	sym := &Symbol{DataType: "UNKNOWN_XYZ", Length: 4}
	_, err := sym.writeToNode("42", 0, map[string]SymbolUploadDataType{})
	if err == nil {
		t.Error("expected error for unknown type")
	}
}

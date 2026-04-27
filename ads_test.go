package ads

import (
	"bytes"
	"encoding/binary"
	"math"
	"strconv"
	"strings"
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
			result, err := StringToNetID(tt.input)
			if err != nil {
				t.Fatalf("StringToNetID(%q) unexpected error: %v", tt.input, err)
			}
			if result != tt.expected {
				t.Errorf("StringToNetID(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestStringToNetIDErrors(t *testing.T) {
	tests := []struct {
		input string
		desc  string
	}{
		{"192.168.1.1", "too few parts"},
		{"192.168.1.1.1.1.1", "too many parts"},
		{"abc.168.1.1.1.1", "non-numeric part"},
		{"256.168.1.1.1.1", "value out of range"},
		{"", "empty string"},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			_, err := StringToNetID(tt.input)
			if err == nil {
				t.Errorf("StringToNetID(%q) expected error for %s, got nil", tt.input, tt.desc)
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

	err := parseRouteResponse(getDefaultLogger(), resp)
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

	err := parseRouteResponse(getDefaultLogger(), resp)
	if err == nil {
		t.Error("expected error for non-zero error code")
	}
}

func TestParseRouteResponse_TooShort(t *testing.T) {
	err := parseRouteResponse(getDefaultLogger(), []byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for short response")
	}
}

func TestParseRouteResponse_WrongCookie(t *testing.T) {
	resp := make([]byte, 24)
	binary.LittleEndian.PutUint32(resp[0:], 0xDEADBEEF) // wrong cookie
	binary.LittleEndian.PutUint32(resp[8:], 0x80000000|routeServiceAdd)

	err := parseRouteResponse(getDefaultLogger(), resp)
	if err == nil {
		t.Error("expected error for wrong cookie")
	}
}

func TestParseRouteResponse_WrongServiceID(t *testing.T) {
	resp := make([]byte, 24)
	binary.LittleEndian.PutUint32(resp[0:], routeCookie)
	binary.LittleEndian.PutUint32(resp[8:], 0x12345678) // wrong serviceId

	err := parseRouteResponse(getDefaultLogger(), resp)
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
			if !strings.Contains(s, tt.contains) {
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
		Offset:   0,
	}
	child2 := &Symbol{
		Name:     "field2",
		FullName: "test.field2",
		DataType: "DINT",
		Length:   4,
		Offset:   4, // 2 bytes padding after field1
	}
	parent := &Symbol{
		Name:     "test",
		FullName: "test",
		DataType: "ST_Test",
		Length:   8,
		Children: map[string]*Symbol{
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
		Children: map[string]*Symbol{"x": child1, "y": child2},
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

// --- parse error paths ---

func TestSymbolParseDataTooShort(t *testing.T) {
	tests := []struct {
		name     string
		dataType string
		length   uint32
		data     []byte
	}{
		{"BOOL_empty", "BOOL", 1, []byte{}},
		{"INT_1byte", "INT", 2, []byte{0}},
		{"UINT_1byte", "UINT", 2, []byte{0}},
		{"DINT_2bytes", "DINT", 4, []byte{0, 0}},
		{"UDINT_2bytes", "UDINT", 4, []byte{0, 0}},
		{"REAL_2bytes", "REAL", 4, []byte{0, 0}},
		{"LREAL_4bytes", "LREAL", 8, []byte{0, 0, 0, 0}},
		{"LINT_4bytes", "LINT", 8, []byte{0, 0, 0, 0}},
		{"ULINT_4bytes", "ULINT", 8, []byte{0, 0, 0, 0}},
		{"TIME_2bytes", "TIME", 4, []byte{0, 0}},
		{"TOD_2bytes", "TOD", 4, []byte{0, 0}},
		{"DATE_2bytes", "DATE", 4, []byte{0, 0}},
		{"DT_2bytes", "DT", 4, []byte{0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sym := &Symbol{DataType: tt.dataType, Length: tt.length}
			_, err := sym.parse(tt.data, 0, nil)
			if err == nil {
				t.Errorf("expected error for %s with %d bytes", tt.dataType, len(tt.data))
			}
		})
	}
}

func TestSymbolParseSizeWrong(t *testing.T) {
	// BOOL with 2 bytes should fail (size mismatch)
	sym := &Symbol{DataType: "BOOL", Length: 2}
	_, err := sym.parse([]byte{1, 0}, 0, nil)
	if err == nil {
		t.Error("expected error for BOOL with wrong size")
	}
}

func TestSymbolParseAliasResolution(t *testing.T) {
	datatypes := map[string]SymbolUploadDataType{
		"MyAlias": {DataType: "INT"},
	}
	sym := &Symbol{DataType: "MyAlias", Length: 2}
	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, uint16(42))
	val, err := sym.parse(data, 0, datatypes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "42" {
		t.Errorf("got %q, want %q", val, "42")
	}
}

func TestSymbolParseDINT(t *testing.T) {
	sym := &Symbol{DataType: "DINT", Length: 4}
	data := make([]byte, 4)
	v := int32(-12345)
	binary.LittleEndian.PutUint32(data, uint32(v))
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "-12345" {
		t.Errorf("got %q, want %q", val, "-12345")
	}
}

func TestSymbolParseLINT(t *testing.T) {
	sym := &Symbol{DataType: "LINT", Length: 8}
	data := make([]byte, 8)
	v := int64(-9999999999)
	binary.LittleEndian.PutUint64(data, uint64(v))
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "-9999999999" {
		t.Errorf("got %q, want %q", val, "-9999999999")
	}
}

func TestSymbolParseULINT(t *testing.T) {
	sym := &Symbol{DataType: "ULINT", Length: 8}
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, 18446744073709551615)
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "18446744073709551615" {
		t.Errorf("got %q, want %q", val, "18446744073709551615")
	}
}

func TestSymbolParseLREAL(t *testing.T) {
	sym := &Symbol{DataType: "LREAL", Length: 8}
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, math.Float64bits(3.141592653589793))
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	f, _ := strconv.ParseFloat(val, 64)
	if math.Abs(f-3.141592653589793) > 1e-10 {
		t.Errorf("got %q, want ~3.141592653589793", val)
	}
}

func TestSymbolParseBYTE(t *testing.T) {
	sym := &Symbol{DataType: "BYTE", Length: 1}
	val, err := sym.parse([]byte{255}, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "255" {
		t.Errorf("got %q, want %q", val, "255")
	}
}

func TestSymbolParseSINT(t *testing.T) {
	sym := &Symbol{DataType: "SINT", Length: 1}
	v := int8(-100)
	val, err := sym.parse([]byte{byte(v)}, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "-100" {
		t.Errorf("got %q, want %q", val, "-100")
	}
}

// --- writeToNode error paths ---

func TestWriteToNodeInvalidValues(t *testing.T) {
	tests := []struct {
		name     string
		dataType string
		length   uint32
		value    string
	}{
		{"BOOL_invalid", "BOOL", 1, "notabool"},
		{"BYTE_negative", "BYTE", 1, "-1"},
		{"BYTE_overflow", "BYTE", 1, "256"},
		{"INT_overflow", "INT", 2, "40000"},
		{"UINT_negative", "UINT", 2, "-1"},
		{"DINT_overflow", "DINT", 4, "3000000000"},
		{"REAL_notfloat", "REAL", 4, "notafloat"},
		{"LREAL_notfloat", "LREAL", 8, "xyz"},
		{"LINT_notint", "LINT", 8, "abc"},
		{"ULINT_negative", "ULINT", 8, "-1"},
		{"TIME_bad_format", "TIME", 4, "not-a-time"},
		{"TOD_bad_format", "TOD", 4, "not-a-tod"},
		{"DATE_bad_format", "DATE", 4, "not-a-date"},
		{"DT_bad_format", "DT", 4, "not-a-datetime"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sym := &Symbol{DataType: tt.dataType, Length: tt.length}
			_, err := sym.writeToNode(tt.value, 0, nil)
			if err == nil {
				t.Errorf("expected error for %s with value %q", tt.dataType, tt.value)
			}
		})
	}
}

func TestWriteToNodeStructInvalidJSON(t *testing.T) {
	parent := &Symbol{
		Name: "s", FullName: "s", DataType: "ST_S", Length: 2,
		Children: map[string]*Symbol{
			"x": {Name: "x", FullName: "s.x", DataType: "BYTE", Length: 1, Offset: 0},
		},
	}
	_, err := parent.writeToNode("not json", 0, nil)
	if err == nil {
		t.Error("expected error for invalid JSON in struct write")
	}
}

// --- addOffset / addChildren ---

func TestAddOffsetEmptySegmentName(t *testing.T) {
	parent := &Symbol{Name: "parent", FullName: "MAIN.parent"}
	dt := &SymbolUploadDataType{
		Children: map[string]*SymbolUploadDataType{
			"": {Name: "", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2}},
		},
	}
	children := dt.addOffset(parent, nil, 0, 0)
	// Empty segment name should be skipped (F-06 fix)
	if len(children) != 0 {
		t.Errorf("expected 0 children for empty segment name, got %d", len(children))
	}
}

func TestAddOffsetFullNameWithDot(t *testing.T) {
	parent := &Symbol{Name: "motor", FullName: "MAIN.motor"}
	dt := &SymbolUploadDataType{
		Children: map[string]*SymbolUploadDataType{
			"speed": {Name: "speed", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 0}},
		},
	}
	children := dt.addOffset(parent, nil, 0, 0)
	child, ok := children["speed"]
	if !ok {
		t.Fatal("expected child 'speed'")
	}
	if child.FullName != "MAIN.motor.speed" {
		t.Errorf("FullName = %q, want %q", child.FullName, "MAIN.motor.speed")
	}
}

func TestAddOffsetArrayFullName(t *testing.T) {
	// Array children have names like "[0]", "[1]" — should use parent.FullName (F-05 fix)
	parent := &Symbol{Name: "arr", FullName: "MAIN.arr"}
	dt := &SymbolUploadDataType{
		Children: map[string]*SymbolUploadDataType{
			"[0]": {Name: "[0]", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 0}},
			"[1]": {Name: "[1]", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 2}},
		},
	}
	children := dt.addOffset(parent, nil, 0, 0)
	child0, ok := children["[0]"]
	if !ok {
		t.Fatal("expected child '[0]'")
	}
	// Should be "MAIN.arr[0]", NOT "arr[0]"
	if child0.FullName != "MAIN.arr[0]" {
		t.Errorf("FullName = %q, want %q", child0.FullName, "MAIN.arr[0]")
	}
}

func TestParseEnumNestedInStruct(t *testing.T) {
	// Non-strict enum: TwinCAT includes enum constants as sub-items.
	// Without the isEnumDataType guard, addOffset would expand these
	// constants as struct children, breaking parse.
	t.Run("non-strict enum with children", func(t *testing.T) {
		datatypes := map[string]SymbolUploadDataType{
			"E_MotorState": {
				Name:     "E_MotorState",
				DataType: "DINT",
				DatatypeEntry: datatypeEntry{
					Size:     4,
					SubItems: 3, // enum has 3 constants
				},
				Children: map[string]*SymbolUploadDataType{
					"Idle":    {Name: "Idle", DataType: "DINT", DatatypeEntry: datatypeEntry{Size: 4}},
					"Running": {Name: "Running", DataType: "DINT", DatatypeEntry: datatypeEntry{Size: 4}},
					"Error":   {Name: "Error", DataType: "DINT", DatatypeEntry: datatypeEntry{Size: 4}},
				},
			},
			"ST_Motor": {
				Name:     "ST_Motor",
				DataType: "",
				DatatypeEntry: datatypeEntry{
					Size:     8,
					SubItems: 2,
				},
				Children: map[string]*SymbolUploadDataType{
					"state": {Name: "state", DataType: "E_MotorState", DatatypeEntry: datatypeEntry{Size: 4, Offs: 0}},
					"speed": {Name: "speed", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 4}},
				},
			},
		}

		motorSym := &Symbol{
			Name: "motor", FullName: "MAIN.motor",
			DataType: "ST_Motor", Length: 8, Group: 0x4040,
		}
		dt := datatypes["ST_Motor"]
		motorSym.Children = dt.addOffset(motorSym, datatypes, motorSym.Group, motorSym.Offset)

		// Wire data: state=2 (Running, DINT), speed=1500 (INT)
		data := make([]byte, 8)
		binary.LittleEndian.PutUint32(data[0:4], 2)
		binary.LittleEndian.PutUint16(data[4:6], 1500)

		value, err := motorSym.parse(data, 0, datatypes)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		t.Logf("parsed value: %s", value)

		stateChild := motorSym.Children["state"]
		if stateChild == nil {
			t.Fatal("expected child 'state'")
		}
		if stateChild.Value != "2" {
			t.Errorf("state value = %q, want %q", stateChild.Value, "2")
		}
		// Enum child must NOT have children (enum constants must not be expanded)
		if len(stateChild.Children) != 0 {
			t.Errorf("enum child should have 0 children, got %d", len(stateChild.Children))
		}

		speedChild := motorSym.Children["speed"]
		if speedChild == nil {
			t.Fatal("expected child 'speed'")
		}
		if speedChild.Value != "1500" {
			t.Errorf("speed value = %q, want %q", speedChild.Value, "1500")
		}
	})

	// Strict enum (TC3 with {attribute 'strict'}): no sub-items in datatype,
	// just a base type. This is what TC3 actually reports for qualified enums.
	t.Run("strict enum no children", func(t *testing.T) {
		datatypes := map[string]SymbolUploadDataType{
			"E_MachineState": {
				Name:     "E_MachineState",
				DataType: "DINT",
				DatatypeEntry: datatypeEntry{
					Size:     4,
					SubItems: 0, // strict: no enum constants exposed
				},
			},
			"ST_Motor": {
				Name:     "ST_Motor",
				DataType: "",
				DatatypeEntry: datatypeEntry{
					Size:     8,
					SubItems: 2,
				},
				Children: map[string]*SymbolUploadDataType{
					"state": {Name: "state", DataType: "E_MachineState", DatatypeEntry: datatypeEntry{Size: 4, Offs: 0}},
					"speed": {Name: "speed", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 4}},
				},
			},
		}

		motorSym := &Symbol{
			Name: "motor", FullName: "MAIN.motor",
			DataType: "ST_Motor", Length: 8, Group: 0x4040,
		}
		dt := datatypes["ST_Motor"]
		motorSym.Children = dt.addOffset(motorSym, datatypes, motorSym.Group, motorSym.Offset)

		data := make([]byte, 8)
		binary.LittleEndian.PutUint32(data[0:4], 4) // ERROR=4
		binary.LittleEndian.PutUint16(data[4:6], 750)

		value, err := motorSym.parse(data, 0, datatypes)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}
		t.Logf("parsed value: %s", value)

		stateChild := motorSym.Children["state"]
		if stateChild == nil {
			t.Fatal("expected child 'state'")
		}
		if stateChild.Value != "4" {
			t.Errorf("state value = %q, want %q", stateChild.Value, "4")
		}
	})
}

func TestArrayTypedefNotMistakenForEnum(t *testing.T) {
	// A typedef array like "TYPE MyInts : ARRAY[0..2] OF INT;" has
	// Children (array elements) and DataType="INT" — same as an enum.
	// ArrayDim distinguishes them: arrays have ArrayDim > 0.
	datatypes := map[string]SymbolUploadDataType{
		"MyInts": {
			Name:     "MyInts",
			DataType: "INT",
			DatatypeEntry: datatypeEntry{
				Size:     6, // 3 x INT(2)
				ArrayDim: 1,
			},
			Children: map[string]*SymbolUploadDataType{
				"[0]": {Name: "[0]", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 0}},
				"[1]": {Name: "[1]", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 2}},
				"[2]": {Name: "[2]", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 4}},
			},
		},
		"ST_WithArray": {
			Name:     "ST_WithArray",
			DataType: "",
			DatatypeEntry: datatypeEntry{
				Size:     8,
				SubItems: 2,
			},
			Children: map[string]*SymbolUploadDataType{
				"values": {Name: "values", DataType: "MyInts", DatatypeEntry: datatypeEntry{Size: 6, Offs: 0}},
				"count":  {Name: "count", DataType: "INT", DatatypeEntry: datatypeEntry{Size: 2, Offs: 6}},
			},
		},
	}

	parent := &Symbol{
		Name:     "s",
		FullName: "MAIN.s",
		DataType: "ST_WithArray",
		Length:   8,
		Group:    0x4040,
		Offset:   0,
	}
	dt := datatypes["ST_WithArray"]
	parent.Children = dt.addOffset(parent, datatypes, parent.Group, parent.Offset)

	// "values" child must have array element children expanded
	valuesChild, ok := parent.Children["values"]
	if !ok {
		t.Fatal("expected child 'values'")
	}
	if len(valuesChild.Children) != 3 {
		t.Fatalf("expected 3 array children for 'values', got %d", len(valuesChild.Children))
	}

	// Parse: values=[10, 20, 30], count=3
	data := make([]byte, 8)
	binary.LittleEndian.PutUint16(data[0:2], 10)
	binary.LittleEndian.PutUint16(data[2:4], 20)
	binary.LittleEndian.PutUint16(data[4:6], 30)
	binary.LittleEndian.PutUint16(data[6:8], 3)

	value, err := parent.parse(data, 0, datatypes)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	t.Logf("parsed: %s", value)

	countChild, ok := parent.Children["count"]
	if !ok {
		t.Fatal("expected child 'count'")
	}
	if countChild.Value != "3" {
		t.Errorf("count = %q, want %q", countChild.Value, "3")
	}
}

func TestAddChildren(t *testing.T) {
	child := &Symbol{Name: "x", FullName: "s.x", DataType: "INT", Length: 2}
	parent := &Symbol{
		Name: "s", FullName: "s", DataType: "ST_S", Length: 2,
		Children: map[string]*Symbol{"x": child},
	}
	symbols := map[string]*Symbol{"s": parent}
	addChildren(parent, symbols)
	if _, ok := symbols["s.x"]; !ok {
		t.Error("expected child 's.x' to be added to symbols map")
	}
}

func TestAddChildrenNoDuplicates(t *testing.T) {
	child := &Symbol{Name: "x", FullName: "s.x", DataType: "INT", Length: 2}
	parent := &Symbol{
		Name: "s", FullName: "s", DataType: "ST_S", Length: 2,
		Children: map[string]*Symbol{"x": child},
	}
	// Pre-populate to ensure it doesn't overwrite
	existing := &Symbol{Name: "x", FullName: "s.x", DataType: "DINT", Length: 4}
	symbols := map[string]*Symbol{"s": parent, "s.x": existing}
	addChildren(parent, symbols)
	if symbols["s.x"].DataType != "DINT" {
		t.Error("addChildren should not overwrite existing symbols")
	}
}

// --- makeArrayChildren ---

func TestMakeArrayChildren(t *testing.T) {
	levels := []datatypeArrayInfo{{LBound: 0, Elements: 3}}
	children := makeArrayChildren(levels, "INT", 6)
	if len(children) != 3 {
		t.Fatalf("expected 3 children, got %d", len(children))
	}
	for i := 0; i < 3; i++ {
		name := "[" + strconv.Itoa(i) + "]"
		child, ok := children[name]
		if !ok {
			t.Errorf("missing child %s", name)
			continue
		}
		if child.DataType != "INT" {
			t.Errorf("child %s datatype = %q, want %q", name, child.DataType, "INT")
		}
		if child.DatatypeEntry.Size != 2 {
			t.Errorf("child %s size = %d, want 2", name, child.DatatypeEntry.Size)
		}
	}
}

func TestMakeArrayChildrenEmpty(t *testing.T) {
	children := makeArrayChildren(nil, "INT", 6)
	if len(children) != 0 {
		t.Errorf("expected 0 children for empty levels, got %d", len(children))
	}
}

func TestMakeArrayChildrenNonZeroLBound(t *testing.T) {
	levels := []datatypeArrayInfo{{LBound: 5, Elements: 2}}
	children := makeArrayChildren(levels, "BYTE", 2)
	if _, ok := children["[5]"]; !ok {
		t.Error("expected child '[5]'")
	}
	if _, ok := children["[6]"]; !ok {
		t.Error("expected child '[6]'")
	}
}

// --- GetJSON ---

func TestGetJSON(t *testing.T) {
	sym := &Symbol{DataType: "INT", Length: 2, Value: "42"}
	json := sym.GetJSON(false)
	if json != "42" {
		t.Errorf("got %q, want %q", json, "42")
	}
}

func TestGetJSONBool(t *testing.T) {
	sym := &Symbol{DataType: "BOOL", Length: 1, Value: "true"}
	json := sym.GetJSON(false)
	if json != "true" {
		t.Errorf("got %q, want %q", json, "true")
	}
}

func TestGetJSONString(t *testing.T) {
	sym := &Symbol{DataType: "STRING", Length: 20, Value: "hello"}
	json := sym.GetJSON(false)
	if json != `"hello"` {
		t.Errorf("got %q, want %q", json, `"hello"`)
	}
}

func TestGetJSONStruct(t *testing.T) {
	child := &Symbol{Name: "x", FullName: "s.x", DataType: "INT", Length: 2, Value: "10"}
	parent := &Symbol{
		Name: "s", FullName: "s", DataType: "ST_S", Length: 2,
		Children: map[string]*Symbol{"x": child},
	}
	json := parent.GetJSON(false)
	if !strings.Contains(json, "10") {
		t.Errorf("expected JSON to contain value 10, got %q", json)
	}
}

func TestGetJSONOnlyChanged(t *testing.T) {
	child1 := &Symbol{Name: "x", FullName: "s.x", DataType: "INT", Length: 2, Value: "10", Changed: true}
	child2 := &Symbol{Name: "y", FullName: "s.y", DataType: "INT", Length: 2, Value: "20", Changed: false}
	parent := &Symbol{
		Name: "s", FullName: "s", DataType: "ST_S", Length: 4,
		Children: map[string]*Symbol{"x": child1, "y": child2},
	}
	json := parent.GetJSON(true)
	if !strings.Contains(json, "10") {
		t.Errorf("expected JSON to contain changed value 10, got %q", json)
	}
	if strings.Contains(json, "20") {
		t.Errorf("expected JSON to NOT contain unchanged value 20, got %q", json)
	}
}

// --- ParseUploadSymbolInfoSymbols with real data ---

func TestParseUploadSymbolInfoSymbols_SingleSymbol(t *testing.T) {
	// Build a minimal symbol entry: header + name + null + type + null + comment + null
	name := []byte("MAIN.test")
	dt := []byte("INT")
	comment := []byte("")

	entry := symbolEntry{
		IGroup:        0x4020,
		IOffs:         0,
		Size:          2,
		DataType:      0,
		Flags:         0,
		NameLength:    uint16(len(name)),
		TypeLength:    uint16(len(dt)),
		CommentLength: uint16(len(comment)),
	}
	// EntryLength = sizeof(symbolEntry) + name + 1 + dt + 1 + comment + 1
	entryLen := 26 + len(name) + 1 + len(dt) + 1 + len(comment) + 1
	entry.EntryLength = uint32(entryLen)

	buf := make([]byte, 0, entryLen)
	b := make([]byte, 26)
	binary.LittleEndian.PutUint32(b[0:], entry.EntryLength)
	binary.LittleEndian.PutUint32(b[4:], entry.IGroup)
	binary.LittleEndian.PutUint32(b[8:], entry.IOffs)
	binary.LittleEndian.PutUint32(b[12:], entry.Size)
	binary.LittleEndian.PutUint32(b[16:], entry.DataType)
	binary.LittleEndian.PutUint32(b[20:], entry.Flags)
	// The last 6 bytes need special packing for uint16 fields
	// Actually symbolEntry is a packed struct; let's use binary.Write
	var entryBuf bytes.Buffer
	if err := binary.Write(&entryBuf, binary.LittleEndian, entry); err != nil {
		t.Fatalf("binary.Write failed: %v", err)
	}
	buf = append(buf, entryBuf.Bytes()...)
	buf = append(buf, name...)
	buf = append(buf, 0) // null terminator
	buf = append(buf, dt...)
	buf = append(buf, 0) // null terminator
	buf = append(buf, comment...)
	buf = append(buf, 0) // null terminator

	symbols, err := ParseUploadSymbolInfoSymbols(buf, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(symbols) != 1 {
		t.Fatalf("expected 1 symbol, got %d", len(symbols))
	}
	sym, ok := symbols["MAIN.test"]
	if !ok {
		t.Fatal("expected symbol 'MAIN.test'")
	}
	if sym.DataType != "INT" {
		t.Errorf("DataType = %q, want %q", sym.DataType, "INT")
	}
	if sym.Length != 2 {
		t.Errorf("Length = %d, want 2", sym.Length)
	}
	if sym.Group != 0x4020 {
		t.Errorf("Group = 0x%x, want 0x4020", sym.Group)
	}
}

func TestParseUploadSymbolInfoSymbols_TruncatedEntry(t *testing.T) {
	// Only 10 bytes — not enough for a symbolEntry header
	_, err := ParseUploadSymbolInfoSymbols([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, nil)
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

// --- parentChanged ---

func TestParentChanged(t *testing.T) {
	grandparent := &Symbol{Name: "root", FullName: "root"}
	parent := &Symbol{Name: "mid", FullName: "root.mid", Parent: grandparent}
	child := &Symbol{Name: "leaf", FullName: "root.mid.leaf", Parent: parent}

	child.parentChanged()
	if !child.Changed {
		t.Error("child should be changed")
	}
	if !parent.Changed {
		t.Error("parent should be changed")
	}
	if !grandparent.Changed {
		t.Error("grandparent should be changed")
	}
}

// --- symbolSumAddress ---

func TestSymbolSumAddress_PrefersHandleOverDirect(t *testing.T) {
	// Handle-based addressing preferred for sum commands because direct
	// process image addressing (0x4040) fails inside sum reads on some PLCs.
	sym := &Symbol{
		Group:  0x4020,
		Offset: 0x1234,
		Handle: 0xABCD,
		Length: 4,
	}
	group, offset := symbolSumAddress(sym)
	if group != uint32(GroupSymbolValueByHandle) {
		t.Errorf("group = 0x%X, want 0x%X (GroupSymbolValueByHandle)", group, uint32(GroupSymbolValueByHandle))
	}
	if offset != 0xABCD {
		t.Errorf("offset = 0x%X, want 0xABCD (handle)", offset)
	}
}

func TestSymbolSumAddress_HandleOnlyNoGroup(t *testing.T) {
	// Handle-based when Group is 0
	sym := &Symbol{
		Group:  0,
		Offset: 0,
		Handle: 0xABCD,
		Length: 4,
	}
	group, offset := symbolSumAddress(sym)
	if group != uint32(GroupSymbolValueByHandle) {
		t.Errorf("group = 0x%X, want 0x%X (GroupSymbolValueByHandle)", group, uint32(GroupSymbolValueByHandle))
	}
	if offset != 0xABCD {
		t.Errorf("offset = 0x%X, want 0xABCD (handle)", offset)
	}
}

func TestSymbolSumAddress_DirectFallbackNoHandle(t *testing.T) {
	// Falls back to direct group/offset when no handle is available
	sym := &Symbol{
		Group:  0x4020,
		Offset: 0x0100,
		Handle: 0,
		Length: 2,
	}
	group, offset := symbolSumAddress(sym)
	if group != 0x4020 {
		t.Errorf("group = 0x%X, want 0x4020", group)
	}
	if offset != 0x0100 {
		t.Errorf("offset = 0x%X, want 0x0100", offset)
	}
}

func TestSymbolSumAddress_DirectFallbackChildAccumulatesOffset(t *testing.T) {
	// Without handles, child symbols accumulate offsets from parent chain.
	parent := &Symbol{
		Group:  0x4040,
		Offset: 0x1000, // absolute offset in PLC memory
		Handle: 0,
		Length: 100,
	}
	child := &Symbol{
		Group:  0x4040,
		Offset: 0x0010, // relative offset within parent struct
		Handle: 0,
		Length: 4,
		Parent: parent,
	}
	group, offset := symbolSumAddress(child)
	if group != 0x4040 {
		t.Errorf("group = 0x%X, want 0x4040", group)
	}
	if offset != 0x1010 { // 0x1000 + 0x0010
		t.Errorf("offset = 0x%X, want 0x1010 (parent 0x1000 + child 0x0010)", offset)
	}
}

func TestSymbolSumAddress_DirectFallbackNestedChild(t *testing.T) {
	// Deeply nested symbol without handles: grandparent → parent → child
	grandparent := &Symbol{
		Group:  0x4040,
		Offset: 0x2000, // absolute
		Handle: 0,
		Length: 200,
	}
	parent := &Symbol{
		Group:  0x4040,
		Offset: 0x0080, // relative within grandparent
		Handle: 0,
		Length: 50,
		Parent: grandparent,
	}
	child := &Symbol{
		Group:  0x4040,
		Offset: 0x0004, // relative within parent
		Handle: 0,
		Length: 2,
		Parent: parent,
	}
	group, offset := symbolSumAddress(child)
	if group != 0x4040 {
		t.Errorf("group = 0x%X, want 0x4040", group)
	}
	// 0x2000 + 0x0080 + 0x0004 = 0x2084
	if offset != 0x2084 {
		t.Errorf("offset = 0x%X, want 0x2084 (0x2000 + 0x0080 + 0x0004)", offset)
	}
}

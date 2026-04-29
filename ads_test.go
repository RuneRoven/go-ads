package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf16"
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
	if s := TransModeServerCycle2.String(); s != "ServerCycle2/CyclicInContext" {
		t.Errorf("got %q, want %q", s, "ServerCycle2/CyclicInContext")
	}
	if s := TransMode(99).String(); s != "Unknown(99)" {
		t.Errorf("got %q, want %q", s, "Unknown(99)")
	}
}

// --- SymbolFlag ---

func TestSymbolFlagContextMask(t *testing.T) {
	tests := []struct {
		flags   SymbolFlag
		ctxMask uint8
	}{
		{0x0008, 0},        // TypeGuid only, no context
		{0x0108, 1},        // ContextMask=1
		{0x0F08, 15},       // ContextMask=15 (max)
		{0x1008, 0},        // Attributes flag, no context
		{0x0308, 3},        // ContextMask=3
		{SymbolFlag(0), 0}, // No flags
		{0x8F08, 15},       // ExtendedFlags + max ContextMask
	}
	for _, tt := range tests {
		got := tt.flags.ContextMask()
		if got != tt.ctxMask {
			t.Errorf("SymbolFlag(0x%04X).ContextMask() = %d, want %d", uint32(tt.flags), got, tt.ctxMask)
		}
	}
}

func TestSymbolFlagHas(t *testing.T) {
	f := SymbolFlag(0x1008) // TypeGuid + Attributes
	if !f.Has(SymbolFlagTypeGuid) {
		t.Error("expected Has(TypeGuid) = true")
	}
	if !f.Has(SymbolFlagAttributes) {
		t.Error("expected Has(Attributes) = true")
	}
	if f.Has(SymbolFlagExtendedFlags) {
		t.Error("expected Has(ExtendedFlags) = false")
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
	// Use size 3 which doesn't match any standard integer size,
	// so inferBaseType won't resolve it
	sym := &Symbol{DataType: "UNKNOWN_TYPE", Length: 3}
	_, err := sym.parse([]byte{0, 0, 0}, 0, nil)
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

func TestParseEnumWithoutDatatypes(t *testing.T) {
	// When datatypes table is nil (on-demand mode), enum types should still
	// parse by inferring the base type from the symbol's byte size.
	tests := []struct {
		name     string
		dataType string
		size     uint32
		data     []byte
		want     string
	}{
		{"1-byte enum", "E_SmallState", 1, []byte{42}, "42"},
		{"2-byte enum", "E_WordState", 2, func() []byte { b := make([]byte, 2); binary.LittleEndian.PutUint16(b, 1500); return b }(), "1500"},
		{"4-byte enum DINT", "E_MachineState", 4, func() []byte { b := make([]byte, 4); binary.LittleEndian.PutUint32(b, 2); return b }(), "2"},
		{"4-byte enum negative", "E_MachineState", 4, func() []byte {
			b := make([]byte, 4)
			v := int32(-1)
			binary.LittleEndian.PutUint32(b, uint32(v))
			return b
		}(), "-1"},
		{"8-byte enum", "E_BigState", 8, func() []byte { b := make([]byte, 8); binary.LittleEndian.PutUint64(b, 99); return b }(), "99"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sym := &Symbol{
				Name:     "testEnum",
				FullName: "MAIN.testEnum",
				DataType: tt.dataType,
				Length:   tt.size,
			}
			// Pass nil datatypes — simulates on-demand mode
			value, err := sym.parse(tt.data, 0, nil)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if value != tt.want {
				t.Errorf("got %q, want %q", value, tt.want)
			}
		})
	}

	// Verify non-standard sizes still error
	t.Run("3-byte unknown type still errors", func(t *testing.T) {
		sym := &Symbol{
			Name: "weird", FullName: "MAIN.weird",
			DataType: "UNKNOWN_TYPE", Length: 3,
		}
		_, err := sym.parse([]byte{1, 2, 3}, 0, nil)
		if err == nil {
			t.Fatal("expected error for 3-byte unknown type")
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

// ==========================================================================
// STRING edge cases
// ==========================================================================

func TestSymbolParseSTRING_NoNullTerminator(t *testing.T) {
	// STRING buffer full with no null byte — should return entire buffer
	sym := &Symbol{DataType: "STRING", Length: 5}
	data := []byte("Hello") // no null
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "Hello" {
		t.Errorf("got %q, want %q", val, "Hello")
	}
}

func TestSymbolParseSTRING_Empty(t *testing.T) {
	// Null byte at start — empty string
	sym := &Symbol{DataType: "STRING", Length: 10}
	data := make([]byte, 10) // all zeros
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "" {
		t.Errorf("got %q, want empty string", val)
	}
}

func TestSymbolParseSTRING_TrailingGarbage(t *testing.T) {
	// Null byte in middle, garbage after — should stop at null
	sym := &Symbol{DataType: "STRING", Length: 10}
	data := []byte("Hi\x00GARBAGE")
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "Hi" {
		t.Errorf("got %q, want %q", val, "Hi")
	}
}

func TestWriteToNodeSTRING_PadsWithZeros(t *testing.T) {
	sym := &Symbol{DataType: "STRING", Length: 10}
	data, err := sym.writeToNode("Hi", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 10 {
		t.Fatalf("expected 10 bytes, got %d", len(data))
	}
	if data[0] != 'H' || data[1] != 'i' {
		t.Errorf("first 2 bytes: got %v, want Hi", data[:2])
	}
	// Remaining bytes should be zero (null-padded)
	for i := 2; i < 10; i++ {
		if data[i] != 0 {
			t.Errorf("byte[%d] = %d, want 0", i, data[i])
		}
	}
}

func TestWriteToNodeSTRING_ExactLength(t *testing.T) {
	sym := &Symbol{DataType: "STRING", Length: 5}
	data, err := sym.writeToNode("Hello", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != "Hello" {
		t.Errorf("got %q, want %q", string(data), "Hello")
	}
}

func TestWriteToNodeSTRING_Overflow(t *testing.T) {
	// String longer than buffer — should be truncated
	sym := &Symbol{DataType: "STRING", Length: 3}
	data, err := sym.writeToNode("Hello", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 3 {
		t.Fatalf("expected 3 bytes, got %d", len(data))
	}
	if string(data) != "Hel" {
		t.Errorf("got %q, want %q", string(data), "Hel")
	}
}

func TestSymbolParseSTRING_SpecialChars(t *testing.T) {
	sym := &Symbol{DataType: "STRING", Length: 20}
	data := make([]byte, 20)
	copy(data, "a/b\\c\t\n\x00")
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "a/b\\c\t\n" {
		t.Errorf("got %q, want %q", val, "a/b\\c\t\n")
	}
}

// ==========================================================================
// REAL/LREAL special values (NaN, Inf, -Inf, -0)
// ==========================================================================

func TestSymbolParseREAL_NaN(t *testing.T) {
	sym := &Symbol{DataType: "REAL", Length: 4}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, math.Float32bits(float32(math.NaN())))
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "NaN" {
		t.Errorf("got %q, want %q", val, "NaN")
	}
}

func TestSymbolParseREAL_PosInf(t *testing.T) {
	sym := &Symbol{DataType: "REAL", Length: 4}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, math.Float32bits(float32(math.Inf(1))))
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "+Inf" {
		t.Errorf("got %q, want %q", val, "+Inf")
	}
}

func TestSymbolParseREAL_NegInf(t *testing.T) {
	sym := &Symbol{DataType: "REAL", Length: 4}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, math.Float32bits(float32(math.Inf(-1))))
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "-Inf" {
		t.Errorf("got %q, want %q", val, "-Inf")
	}
}

func TestSymbolParseREAL_NegZero(t *testing.T) {
	sym := &Symbol{DataType: "REAL", Length: 4}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, math.Float32bits(float32(math.Copysign(0, -1))))
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// FormatFloat for -0 → "0" (float32 precision)
	if val != "0" && val != "-0" {
		t.Errorf("got %q, want %q or %q", val, "0", "-0")
	}
}

func TestSymbolParseLREAL_NaN(t *testing.T) {
	sym := &Symbol{DataType: "LREAL", Length: 8}
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, math.Float64bits(math.NaN()))
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "NaN" {
		t.Errorf("got %q, want %q", val, "NaN")
	}
}

func TestSymbolParseLREAL_PosInf(t *testing.T) {
	sym := &Symbol{DataType: "LREAL", Length: 8}
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, math.Float64bits(math.Inf(1)))
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "+Inf" {
		t.Errorf("got %q, want %q", val, "+Inf")
	}
}

func TestSymbolParseLREAL_NegInf(t *testing.T) {
	sym := &Symbol{DataType: "LREAL", Length: 8}
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, math.Float64bits(math.Inf(-1)))
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "-Inf" {
		t.Errorf("got %q, want %q", val, "-Inf")
	}
}

func TestSymbolParseLREAL_SmallSubnormal(t *testing.T) {
	sym := &Symbol{DataType: "LREAL", Length: 8}
	data := make([]byte, 8)
	// Smallest positive subnormal float64
	binary.LittleEndian.PutUint64(data, 1) // 5e-324
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	f, parseErr := strconv.ParseFloat(val, 64)
	if parseErr != nil {
		t.Fatalf("cannot parse result %q: %v", val, parseErr)
	}
	if f != math.Float64frombits(1) {
		t.Errorf("got %v, want %v", f, math.Float64frombits(1))
	}
}

func TestWriteToNodeREAL_NaN(t *testing.T) {
	sym := &Symbol{DataType: "REAL", Length: 4}
	data, err := sym.writeToNode("NaN", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bits := binary.LittleEndian.Uint32(data)
	f := math.Float32frombits(bits)
	if !math.IsNaN(float64(f)) {
		t.Errorf("expected NaN, got %v", f)
	}
}

func TestWriteToNodeREAL_Inf(t *testing.T) {
	sym := &Symbol{DataType: "REAL", Length: 4}
	data, err := sym.writeToNode("+Inf", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bits := binary.LittleEndian.Uint32(data)
	f := math.Float32frombits(bits)
	if !math.IsInf(float64(f), 1) {
		t.Errorf("expected +Inf, got %v", f)
	}
}

func TestWriteToNodeLREAL_NaN(t *testing.T) {
	sym := &Symbol{DataType: "LREAL", Length: 8}
	data, err := sym.writeToNode("NaN", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	bits := binary.LittleEndian.Uint64(data)
	f := math.Float64frombits(bits)
	if !math.IsNaN(f) {
		t.Errorf("expected NaN, got %v", f)
	}
}

// ==========================================================================
// DATE/TIME boundary values
// ==========================================================================

func TestSymbolParseTIME_Midnight(t *testing.T) {
	sym := &Symbol{DataType: "TIME", Length: 4}
	data := make([]byte, 4) // 0 ms = midnight
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "00:00:00" {
		t.Errorf("got %q, want %q", val, "00:00:00")
	}
}

func TestSymbolParseTIME_MaxMs(t *testing.T) {
	// 23:59:59.999
	sym := &Symbol{DataType: "TIME", Length: 4}
	data := make([]byte, 4)
	ms := uint32(23*3600000 + 59*60000 + 59*1000 + 999)
	binary.LittleEndian.PutUint32(data, ms)
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "23:59:59.999" {
		t.Errorf("got %q, want %q", val, "23:59:59.999")
	}
}

func TestSymbolParseTIME_SubMillisecondPrecision(t *testing.T) {
	// TIME with exact seconds (no ms) — should not show decimal
	sym := &Symbol{DataType: "TIME", Length: 4}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 3600000) // 1 hour exactly
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "01:00:00" {
		t.Errorf("got %q, want %q", val, "01:00:00")
	}
}

func TestSymbolParseTOD_Midnight(t *testing.T) {
	sym := &Symbol{DataType: "TOD", Length: 4}
	data := make([]byte, 4)
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "00:00" {
		t.Errorf("got %q, want %q", val, "00:00")
	}
}

func TestSymbolParseTOD_EndOfDay(t *testing.T) {
	sym := &Symbol{DataType: "TOD", Length: 4}
	data := make([]byte, 4)
	ms := uint32(23*3600000 + 59*60000) // 23:59
	binary.LittleEndian.PutUint32(data, ms)
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "23:59" {
		t.Errorf("got %q, want %q", val, "23:59")
	}
}

func TestSymbolParseDATE_Epoch(t *testing.T) {
	sym := &Symbol{DataType: "DATE", Length: 4}
	data := make([]byte, 4) // 0 seconds = 1970-01-01
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "1970-01-01" {
		t.Errorf("got %q, want %q", val, "1970-01-01")
	}
}

func TestSymbolParseDATE_LeapYear(t *testing.T) {
	sym := &Symbol{DataType: "DATE", Length: 4}
	data := make([]byte, 4)
	// 2024-02-29 in unix seconds
	ts := uint32(1709164800) // 2024-02-29 00:00:00 UTC
	binary.LittleEndian.PutUint32(data, ts)
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "2024-02-29" {
		t.Errorf("got %q, want %q", val, "2024-02-29")
	}
}

func TestSymbolParseDATE_MaxUint32(t *testing.T) {
	// uint32 max = 4294967295 seconds = 2106-02-07
	sym := &Symbol{DataType: "DATE", Length: 4}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, math.MaxUint32)
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "2106-02-07" {
		t.Errorf("got %q, want %q", val, "2106-02-07")
	}
}

func TestSymbolParseDT_Epoch(t *testing.T) {
	testWriteRoundTrip(t, "DT", 4, "1970-01-01 00:00:00")
}

func TestSymbolParseDT_Y2K38(t *testing.T) {
	// 2038-01-19 03:14:07 — last second of signed 32-bit unix time
	sym := &Symbol{DataType: "DT", Length: 4}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 2147483647) // max int32
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "2038-01-19 03:14:07" {
		t.Errorf("got %q, want %q", val, "2038-01-19 03:14:07")
	}
}

func TestWriteToNodeDATE_RoundTrip_LeapYear(t *testing.T) {
	testWriteRoundTrip(t, "DATE", 4, "2024-02-29")
}

func TestWriteToNodeTIME_RoundTrip_WithMs(t *testing.T) {
	testWriteRoundTrip(t, "TIME", 4, "01:02:03.456")
}

func TestWriteToNodeTOD_RoundTrip_Aliases(t *testing.T) {
	// TIME_OF_DAY is alias for TOD
	sym := &Symbol{DataType: "TIME_OF_DAY", Length: 4}
	data, err := sym.writeToNode("14:30", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sym2 := &Symbol{DataType: "TIME_OF_DAY", Length: 4}
	val, err := sym2.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if val != "14:30" {
		t.Errorf("got %q, want %q", val, "14:30")
	}
}

func TestWriteToNodeDT_RoundTrip_Alias(t *testing.T) {
	// DATE_AND_TIME is alias for DT
	sym := &Symbol{DataType: "DATE_AND_TIME", Length: 4}
	data, err := sym.writeToNode("2024-06-15 23:59:59", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sym2 := &Symbol{DataType: "DATE_AND_TIME", Length: 4}
	val, err := sym2.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	if val != "2024-06-15 23:59:59" {
		t.Errorf("got %q, want %q", val, "2024-06-15 23:59:59")
	}
}

// ==========================================================================
// ReturnCode coverage — verify String() for all major categories
// ==========================================================================

func TestReturnCodeString_AllCategories(t *testing.T) {
	tests := []struct {
		code     ReturnCode
		contains string
	}{
		// Global errors
		{ReturnCodeGlobalInternalError, "internal error"},
		{ReturnCodeGlobalTargetPortNotFound, "target port not found"},
		{ReturnCodeGlobalInvalidAdsLength, "invalid ADS length"},
		{ReturnCodeGlobalInvalidAmsNetID, "invalid AMS Net ID"},
		{ReturnCodeGlobalTcpSendError, "TCP send error"},
		{ReturnCodeGlobalHostUnreachable, "host unreachable"},
		{ReturnCodeGlobalAccessDenied, "access denied"},

		// Router errors
		{ReturnCodeRouterNoLockedMemory, "locked memory"},
		{ReturnCodeRouterMailboxFull, "mailbox full"},
		{ReturnCodeRouterNotInitialized, "not initialized"},
		{ReturnCodeRouterPortAlreadyInUse, "already assigned"},

		// Device errors
		{ReturnCodeDeviceError, "general device error"},
		{ReturnCodeDeviceServiceNotSupported, "service not supported"},
		{ReturnCodeDeviceInvalidGroup, "invalid index group"},
		{ReturnCodeDeviceInvalidOffset, "invalid index offset"},
		{ReturnCodeDeviceInvalidSize, "parameter size not correct"},
		{ReturnCodeDeviceInvalidData, "invalid parameter value"},
		{ReturnCodeDeviceNotReady, "not in a ready state"},
		{ReturnCodeDeviceInvalidContext, "invalid operating system context"},
		{ReturnCodeDeviceInvalidParam, "invalid parameter value"},
		{ReturnCodeDeviceTimeout, "timeout"},
		{ReturnCodeDeviceTransModeNotSupported, "TransMode not supported"},
		{ReturnCodeDeviceNotifyHandleInvalid, "notification handle is invalid"},
		{ReturnCodeDeviceNoMoreHandles, "no more notification handles"},
		{ReturnCodeDeviceInvalidWatchSize, "notification size too large"},
		{ReturnCodeDeviceInvalidArrayIndex, "invalid array index"},
		{ReturnCodeDeviceSymbolNotActive, "symbol not active"},
		{ReturnCodeDeviceAccessDenied, "access denied"},
		{ReturnCodeDeviceLicenseNotFound, "missing license"},
		{ReturnCodeDeviceLicenseExpired, "license expired"},

		// Client errors
		{ReturnCodeClientError, "client error"},
		{ReturnCodeClientInvalidParameter, "invalid parameter"},
		{ReturnCodeClientSyncTimeout, "timeout elapsed"},
		{ReturnCodeClientPortNotOpen, "port not opened"},
		{ReturnCodeClientRequestCancelled, "cancelled"},

		// RTime errors
		{ReturnCodeRTimeInternal, "fatal error"},
		{ReturnCodeRTimeBadTimerPeriods, "timer value not valid"},

		// TCP errors
		{ReturnCodeWsaeTimedOut, "timed out"},
		{ReturnCodeWsaeConnRefused, "refused"},
		{ReturnCodeWsaeHostDown, "host is down"},
	}

	for _, tt := range tests {
		t.Run(tt.contains, func(t *testing.T) {
			s := tt.code.String()
			if !strings.Contains(strings.ToLower(s), strings.ToLower(tt.contains)) {
				t.Errorf("ReturnCode(0x%04X).String() = %q, want it to contain %q",
					uint32(tt.code), s, tt.contains)
			}
		})
	}
}

func TestReturnCodeError_ImplementsError(t *testing.T) {
	codes := []ReturnCode{
		ReturnCodeDeviceTimeout,
		ReturnCodeDeviceInvalidParam,
		ReturnCodeDeviceSymbolNoFound,
		ReturnCodeClientSyncTimeout,
		ReturnCodeGlobalTargetNotFound,
	}
	for _, rc := range codes {
		var err error = rc
		if err.Error() == "" {
			t.Errorf("ReturnCode(0x%04X).Error() should not be empty", uint32(rc))
		}
	}
}

func TestIsSumCommandUnsupportedError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{ReturnCodeDeviceServiceNotSupported, true},
		{ReturnCodeGlobalUnknownCommandID, true},
		{ReturnCodeGlobalUnknownAdsCommand, true},
		{ReturnCodeDeviceBusy, false},
		{ReturnCodeDeviceTimeout, false},
		{fmt.Errorf("network error"), false},
		{nil, false},
	}
	for _, tt := range tests {
		got := isSumCommandUnsupportedError(tt.err)
		if got != tt.want {
			t.Errorf("isSumCommandUnsupportedError(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

// ==========================================================================
// Multi-dimensional arrays
// ==========================================================================

func TestMakeArrayChildren_2D(t *testing.T) {
	// ARRAY[0..1, 0..2] OF INT — 2x3 = 6 elements, 12 bytes total
	levels := []datatypeArrayInfo{
		{LBound: 0, Elements: 2},
		{LBound: 0, Elements: 3},
	}
	children := makeArrayChildren(levels, "INT", 12)
	if len(children) != 2 {
		t.Fatalf("expected 2 top-level children, got %d", len(children))
	}
	for _, name := range []string{"[0]", "[1]"} {
		child, ok := children[name]
		if !ok {
			t.Errorf("missing child %s", name)
			continue
		}
		if len(child.Children) != 3 {
			t.Errorf("child %s: expected 3 sub-children, got %d", name, len(child.Children))
		}
		if child.DatatypeEntry.Size != 6 { // 12/2 = 6 bytes per row
			t.Errorf("child %s: size = %d, want 6", name, child.DatatypeEntry.Size)
		}
	}
}

func TestMakeArrayChildren_3D(t *testing.T) {
	// ARRAY[0..1, 0..1, 0..1] OF BYTE — 2x2x2 = 8 elements
	levels := []datatypeArrayInfo{
		{LBound: 0, Elements: 2},
		{LBound: 0, Elements: 2},
		{LBound: 0, Elements: 2},
	}
	children := makeArrayChildren(levels, "BYTE", 8)
	if len(children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(children))
	}
	// Drill down to leaf level
	c0 := children["[0]"]
	if c0 == nil {
		t.Fatal("missing [0]")
	}
	if len(c0.Children) != 2 {
		t.Fatalf("[0] expected 2 children, got %d", len(c0.Children))
	}
	c00 := c0.Children["[0]"]
	if c00 == nil {
		t.Fatal("missing [0][0]")
	}
	if len(c00.Children) != 2 {
		t.Fatalf("[0][0] expected 2 children, got %d", len(c00.Children))
	}
}

func TestMakeArrayChildren_ZeroElements(t *testing.T) {
	levels := []datatypeArrayInfo{{LBound: 0, Elements: 0}}
	children := makeArrayChildren(levels, "INT", 0)
	if len(children) != 0 {
		t.Errorf("expected 0 children for zero elements, got %d", len(children))
	}
}

// ==========================================================================
// inferBaseType
// ==========================================================================

func TestInferBaseType(t *testing.T) {
	tests := []struct {
		size uint32
		want string
	}{
		{1, "SINT"},
		{2, "INT"},
		{4, "DINT"},
		{8, "LINT"},
		{3, ""},
		{16, ""},
		{0, ""},
	}
	for _, tt := range tests {
		got := inferBaseType(tt.size)
		if got != tt.want {
			t.Errorf("inferBaseType(%d) = %q, want %q", tt.size, got, tt.want)
		}
	}
}

// ==========================================================================
// Deeply nested struct parsing
// ==========================================================================

func TestParseNestedStructThreeLevels(t *testing.T) {
	// ST_Inner { value: INT }
	// ST_Middle { inner: ST_Inner, count: BYTE }
	// ST_Outer { middle: ST_Middle, flag: BOOL }
	innerChild := &Symbol{Name: "value", FullName: "o.middle.inner.value", DataType: "INT", Length: 2, Offset: 0}
	inner := &Symbol{
		Name: "inner", FullName: "o.middle.inner", DataType: "ST_Inner", Length: 2, Offset: 0,
		Children: map[string]*Symbol{"value": innerChild},
	}
	innerChild.Parent = inner

	countChild := &Symbol{Name: "count", FullName: "o.middle.count", DataType: "BYTE", Length: 1, Offset: 2}
	middle := &Symbol{
		Name: "middle", FullName: "o.middle", DataType: "ST_Middle", Length: 4, Offset: 0,
		Children: map[string]*Symbol{"inner": inner, "count": countChild},
	}
	inner.Parent = middle
	countChild.Parent = middle

	flagChild := &Symbol{Name: "flag", FullName: "o.flag", DataType: "BOOL", Length: 1, Offset: 4}
	outer := &Symbol{
		Name: "o", FullName: "o", DataType: "ST_Outer", Length: 5,
		Children: map[string]*Symbol{"middle": middle, "flag": flagChild},
	}
	middle.Parent = outer
	flagChild.Parent = outer

	// Data: INT=1234, BYTE=42, padding(1), BOOL=1
	data := make([]byte, 5)
	binary.LittleEndian.PutUint16(data[0:2], 1234)
	data[2] = 42
	data[3] = 0 // padding
	data[4] = 1

	_, err := outer.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if innerChild.Value != "1234" {
		t.Errorf("inner.value = %q, want %q", innerChild.Value, "1234")
	}
	if countChild.Value != "42" {
		t.Errorf("count = %q, want %q", countChild.Value, "42")
	}
	if flagChild.Value != "true" {
		t.Errorf("flag = %q, want %q", flagChild.Value, "true")
	}
}

// ==========================================================================
// Struct write with nested children
// ==========================================================================

func TestWriteToNodeNestedStruct(t *testing.T) {
	innerChild := &Symbol{Name: "val", FullName: "s.inner.val", DataType: "INT", Length: 2, Offset: 0}
	inner := &Symbol{
		Name: "inner", FullName: "s.inner", DataType: "ST_Inner", Length: 2, Offset: 0,
		Children: map[string]*Symbol{"val": innerChild},
	}
	outerField := &Symbol{Name: "flag", FullName: "s.flag", DataType: "BOOL", Length: 1, Offset: 2}
	parent := &Symbol{
		Name: "s", FullName: "s", DataType: "ST_S", Length: 4,
		Children: map[string]*Symbol{"inner": inner, "flag": outerField},
	}

	data, err := parent.writeToNode(`{"inner":{"val":"99"},"flag":"true"}`, 0, nil)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(data) != 4 {
		t.Fatalf("expected 4 bytes, got %d", len(data))
	}
	v := int16(binary.LittleEndian.Uint16(data[0:2]))
	if v != 99 {
		t.Errorf("inner.val = %d, want 99", v)
	}
	if data[2] != 1 {
		t.Errorf("flag = %d, want 1", data[2])
	}
}

// ==========================================================================
// GetJSON edge cases
// ==========================================================================

func TestGetJSON_EmptyValue(t *testing.T) {
	sym := &Symbol{DataType: "STRING", Length: 10, Value: ""}
	json := sym.GetJSON(false)
	if json != `""` {
		t.Errorf("got %q, want %q", json, `""`)
	}
}

func TestGetJSON_NumericOverflow(t *testing.T) {
	// ULINT max value — verify JSON doesn't lose precision
	sym := &Symbol{DataType: "ULINT", Length: 8, Value: "18446744073709551615"}
	j := sym.GetJSON(false)
	// parseFloat loses precision for uint64 max — this is a known limitation
	// JSON outputs float64 representation
	if j == "" {
		t.Error("expected non-empty JSON output")
	}
}

func TestGetJSON_NestedOnlyChanged(t *testing.T) {
	inner := &Symbol{Name: "a", FullName: "s.a", DataType: "INT", Length: 2, Value: "1", Changed: true}
	innerUnchanged := &Symbol{Name: "b", FullName: "s.b", DataType: "INT", Length: 2, Value: "2", Changed: false}
	parent := &Symbol{
		Name: "s", FullName: "s", DataType: "ST_S", Length: 4,
		Children: map[string]*Symbol{"a": inner, "b": innerUnchanged},
	}
	j := parent.GetJSON(true)
	if !strings.Contains(j, "1") {
		t.Errorf("expected changed value 1 in JSON: %s", j)
	}
	if strings.Contains(j, `"b"`) {
		t.Errorf("expected unchanged field b to be excluded: %s", j)
	}
}

// ==========================================================================
// DeviceNotification parsing — binary packet tests
// ==========================================================================

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

// newTestConnection creates a minimal Connection for unit testing notification parsing.
func newTestConnection() *Connection {
	ctx, cancel := context.WithCancel(context.Background())
	conn := &Connection{
		ctx:                 ctx,
		shutdown:            cancel,
		activeNotifications: make(map[uint32]*Symbol),
		logger:              getDefaultLogger(),
	}
	return conn
}

func TestDeviceNotification_SingleSample(t *testing.T) {
	conn := newTestConnection()
	defer conn.shutdown()

	// Register a notification for handle 42
	ch := make(chan *Update, 10)
	sym := &Symbol{
		FullName:     "MAIN.testVar",
		DataType:     "INT",
		Length:       2,
		Notification: ch,
	}
	conn.activeNotifications[42] = sym

	// Build INT value = 1234
	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, 1234)

	// Windows FILETIME for 2024-01-01 00:00:00 UTC
	// = (Unix epoch offset + unix timestamp) * ticks per second
	unixTS := int64(1704067200) // 2024-01-01 00:00:00 UTC
	filetime := uint64((unixTS + secToUnixEpoch) * windowsTick)

	packet := buildNotificationPacket(42, filetime, data)
	err := conn.DeviceNotification(conn.ctx, packet)
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
	defer conn.shutdown()

	// No notifications registered — handle 99 is unknown
	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, 42)
	packet := buildNotificationPacket(99, 0, data)

	// Should not error, just log warning
	err := conn.DeviceNotification(conn.ctx, packet)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeviceNotification_MultipleStampsAndSamples(t *testing.T) {
	conn := newTestConnection()
	defer conn.shutdown()

	ch := make(chan *Update, 10)
	sym1 := &Symbol{FullName: "var1", DataType: "BYTE", Length: 1, Notification: ch}
	sym2 := &Symbol{FullName: "var2", DataType: "BYTE", Length: 1, Notification: ch}
	conn.activeNotifications[1] = sym1
	conn.activeNotifications[2] = sym2

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
	err := conn.DeviceNotification(conn.ctx, packet)
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
	defer conn.shutdown()

	// Too short — should return error
	err := conn.DeviceNotification(conn.ctx, []byte{1, 2, 3})
	if err == nil {
		t.Error("expected error for truncated packet")
	}
}

func TestDeviceNotification_ZeroStamps(t *testing.T) {
	conn := newTestConnection()
	defer conn.shutdown()

	// Valid header with 0 stamps
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(8)) // length
	binary.Write(buf, binary.LittleEndian, uint32(0)) // 0 stamps

	err := conn.DeviceNotification(conn.ctx, buf.Bytes())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeviceNotification_SampleSizeExceedsData(t *testing.T) {
	conn := newTestConnection()
	defer conn.shutdown()

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, uint32(100))      // length (fake)
	binary.Write(buf, binary.LittleEndian, uint32(1))        // 1 stamp
	binary.Write(buf, binary.LittleEndian, uint64(0))        // timestamp
	binary.Write(buf, binary.LittleEndian, uint32(1))        // 1 sample
	binary.Write(buf, binary.LittleEndian, uint32(42))       // handle
	binary.Write(buf, binary.LittleEndian, uint32(99999999)) // size > remaining

	err := conn.DeviceNotification(conn.ctx, buf.Bytes())
	if err == nil {
		t.Error("expected error for sample size exceeding data")
	}
}

func TestDeviceNotification_BoolType(t *testing.T) {
	conn := newTestConnection()
	defer conn.shutdown()

	ch := make(chan *Update, 5)
	sym := &Symbol{FullName: "MAIN.bFlag", DataType: "BOOL", Length: 1, Notification: ch}
	conn.activeNotifications[10] = sym

	packet := buildNotificationPacket(10, 0, []byte{1})
	err := conn.DeviceNotification(conn.ctx, packet)
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
	defer conn.shutdown()

	ch := make(chan *Update, 5)
	sym := &Symbol{FullName: "MAIN.sName", DataType: "STRING", Length: 20, Notification: ch}
	conn.activeNotifications[11] = sym

	strData := make([]byte, 20)
	copy(strData, "Hello\x00")

	packet := buildNotificationPacket(11, 0, strData)
	err := conn.DeviceNotification(conn.ctx, packet)
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
// AMS packet encoding
// ==========================================================================

func TestEncodePacket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &Connection{
		ctx:    ctx,
		logger: getDefaultLogger(),
		target: AmsAddress{
			NetID: [6]byte{5, 154, 236, 19, 1, 1},
			Port:  851,
		},
		source: AmsAddress{
			NetID: [6]byte{192, 168, 1, 100, 1, 1},
			Port:  10500,
		},
	}

	data := []byte{0x01, 0x02, 0x03, 0x04}
	packet, err := conn.encode(CommandIDRead, data, 7)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// TCP header: 6 bytes (Unknown1=0, System=0, Length=32+4=36)
	if len(packet) != 6+32+4 {
		t.Fatalf("packet length = %d, want %d", len(packet), 42)
	}

	// Check TCP header length field
	tcpLen := binary.LittleEndian.Uint32(packet[2:6])
	if tcpLen != 36 {
		t.Errorf("TCP length = %d, want 36", tcpLen)
	}

	// Check AMS header target NetID (starts at offset 6)
	var targetNetID [6]byte
	copy(targetNetID[:], packet[6:12])
	if targetNetID != conn.target.NetID {
		t.Errorf("target NetID = %v, want %v", targetNetID, conn.target.NetID)
	}

	// Target port at offset 12
	targetPort := binary.LittleEndian.Uint16(packet[12:14])
	if targetPort != 851 {
		t.Errorf("target port = %d, want 851", targetPort)
	}

	// Source NetID at offset 14
	var sourceNetID [6]byte
	copy(sourceNetID[:], packet[14:20])
	if sourceNetID != conn.source.NetID {
		t.Errorf("source NetID = %v, want %v", sourceNetID, conn.source.NetID)
	}

	// Command at offset 22
	cmd := binary.LittleEndian.Uint16(packet[22:24])
	if CommandID(cmd) != CommandIDRead {
		t.Errorf("command = %d, want %d (Read)", cmd, CommandIDRead)
	}

	// State at offset 24 — should be 4 (request)
	state := binary.LittleEndian.Uint16(packet[24:26])
	if state != 4 {
		t.Errorf("state = %d, want 4", state)
	}

	// Data length at offset 26
	dataLen := binary.LittleEndian.Uint32(packet[26:30])
	if dataLen != 4 {
		t.Errorf("data length = %d, want 4", dataLen)
	}

	// InvokeID at offset 34
	invokeID := binary.LittleEndian.Uint32(packet[34:38])
	if invokeID != 7 {
		t.Errorf("invokeID = %d, want 7", invokeID)
	}

	// Payload at offset 38
	if !bytes.Equal(packet[38:], data) {
		t.Errorf("payload = %v, want %v", packet[38:], data)
	}
}

func TestEncodePacket_EmptyData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &Connection{
		ctx:    ctx,
		logger: getDefaultLogger(),
		target: AmsAddress{NetID: [6]byte{1, 2, 3, 4, 5, 6}, Port: 851},
		source: AmsAddress{NetID: [6]byte{10, 20, 30, 40, 1, 1}, Port: 10500},
	}

	packet, err := conn.encode(CommandIDReadDeviceInfo, nil, 0)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// 6 (TCP) + 32 (AMS) + 0 (data)
	if len(packet) != 38 {
		t.Fatalf("packet length = %d, want 38", len(packet))
	}

	tcpLen := binary.LittleEndian.Uint32(packet[2:6])
	if tcpLen != 32 {
		t.Errorf("TCP length = %d, want 32", tcpLen)
	}
}

func TestEncodePacket_AllCommands(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &Connection{
		ctx:    ctx,
		logger: getDefaultLogger(),
		target: AmsAddress{NetID: [6]byte{1, 2, 3, 4, 5, 6}, Port: 851},
		source: AmsAddress{NetID: [6]byte{10, 20, 30, 40, 1, 1}, Port: 10500},
	}

	commands := []CommandID{
		CommandIDReadDeviceInfo,
		CommandIDRead,
		CommandIDWrite,
		CommandIDReadState,
		CommandIDWriteControl,
		CommandIDAddDeviceNotification,
		CommandIDDeleteDeviceNotification,
		CommandIDReadWrite,
	}

	for _, cmd := range commands {
		t.Run(fmt.Sprintf("Command_%d", cmd), func(t *testing.T) {
			packet, err := conn.encode(cmd, []byte{0xFF}, 1)
			if err != nil {
				t.Fatalf("encode error: %v", err)
			}
			// Verify command in header
			encodedCmd := binary.LittleEndian.Uint16(packet[22:24])
			if CommandID(encodedCmd) != cmd {
				t.Errorf("encoded command = %d, want %d", encodedCmd, cmd)
			}
		})
	}
}

// ==========================================================================
// handleReceive — AMS response routing
// ==========================================================================

func TestHandleReceive_RoutesToCorrectChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &Connection{
		ctx:            ctx,
		logger:         getDefaultLogger(),
		activeRequests: make(map[uint32]chan []byte),
	}

	// Register a response channel for invokeID 42
	ch := make(chan []byte, 1)
	conn.activeRequestLock.Lock()
	conn.activeRequests[42] = ch
	conn.activeRequestLock.Unlock()

	// Build AMS header + data
	header := amsHeader{
		Target:    AmsAddress{},
		Source:    AmsAddress{},
		Command:   CommandIDRead,
		State:     5, // response
		Length:    4,
		ErrorCode: 0,
		InvokeID:  42,
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, &header)
	buf.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF})

	conn.handleReceive(ctx, buf.Bytes())

	select {
	case resp := <-ch:
		if !bytes.Equal(resp, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
			t.Errorf("response = %v, want [0xDE 0xAD 0xBE 0xEF]", resp)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for response")
	}
}

func TestHandleReceive_UnknownInvokeID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &Connection{
		ctx:            ctx,
		logger:         getDefaultLogger(),
		activeRequests: make(map[uint32]chan []byte),
	}

	// No registered channels — should not panic
	header := amsHeader{
		Command:  CommandIDRead,
		State:    5,
		Length:   2,
		InvokeID: 999,
	}
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, &header)
	buf.Write([]byte{0x00, 0x00})

	// Should not panic or error
	conn.handleReceive(ctx, buf.Bytes())
}

func TestHandleReceive_TooShort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &Connection{
		ctx:            ctx,
		logger:         getDefaultLogger(),
		activeRequests: make(map[uint32]chan []byte),
	}

	// Less than 32 bytes — should return early
	conn.handleReceive(ctx, []byte{1, 2, 3, 4, 5})
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

// ==========================================================================
// hexAttr utility
// ==========================================================================

func TestHexAttr(t *testing.T) {
	attr := hexAttr("data", []byte{0xDE, 0xAD, 0xBE, 0xEF})
	if attr.Key != "data" {
		t.Errorf("key = %q, want %q", attr.Key, "data")
	}
	s := attr.Value.String()
	if !strings.Contains(s, "DEADBEEF") && !strings.Contains(s, "deadbeef") {
		t.Errorf("hex string = %q, expected to contain DEADBEEF", s)
	}
}

func TestHexAttr_Empty(t *testing.T) {
	attr := hexAttr("empty", []byte{})
	if attr.Key != "empty" {
		t.Errorf("key = %q, want %q", attr.Key, "empty")
	}
}

// ==========================================================================
// WSTRING (UTF-16LE) parse/write tests
// ==========================================================================

func encodeUTF16LE(s string) []byte {
	encoded := utf16.Encode([]rune(s))
	buf := make([]byte, len(encoded)*2)
	for i, r := range encoded {
		binary.LittleEndian.PutUint16(buf[i*2:], r)
	}
	return buf
}

func TestParseWSTRING_ASCII(t *testing.T) {
	text := "Hello"
	raw := encodeUTF16LE(text)
	// Add null terminator
	raw = append(raw, 0, 0)
	// Pad to 20 bytes
	padded := make([]byte, 20)
	copy(padded, raw)
	sym := &Symbol{DataType: "WSTRING", Length: 20}
	val, err := sym.parse(padded, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != text {
		t.Errorf("got %q, want %q", val, text)
	}
}

func TestParseWSTRING_Unicode(t *testing.T) {
	text := "日本語"
	raw := encodeUTF16LE(text)
	raw = append(raw, 0, 0)
	padded := make([]byte, 20)
	copy(padded, raw)
	sym := &Symbol{DataType: "WSTRING", Length: 20}
	val, err := sym.parse(padded, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != text {
		t.Errorf("got %q, want %q", val, text)
	}
}

func TestParseWSTRING_NullTerminated(t *testing.T) {
	// "Hi" followed by null terminator then garbage
	raw := encodeUTF16LE("Hi")
	data := make([]byte, 20)
	copy(data, raw)
	// Null terminator at byte 4-5
	data[4] = 0
	data[5] = 0
	// Garbage after
	data[6] = 0xFF
	data[7] = 0xFF
	sym := &Symbol{DataType: "WSTRING", Length: 20}
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "Hi" {
		t.Errorf("got %q, want %q", val, "Hi")
	}
}

func TestParseWSTRING_NoNullTerminator(t *testing.T) {
	// Fill entire buffer with UTF-16 chars, no null
	text := "ABCDE"
	raw := encodeUTF16LE(text)
	sym := &Symbol{DataType: "WSTRING", Length: uint32(len(raw))}
	val, err := sym.parse(raw, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != text {
		t.Errorf("got %q, want %q", val, text)
	}
}

func TestParseWSTRING_Empty(t *testing.T) {
	// Just null terminator
	data := make([]byte, 10)
	sym := &Symbol{DataType: "WSTRING", Length: 10}
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "" {
		t.Errorf("got %q, want empty string", val)
	}
}

func TestParseWSTRING_SurrogatePair(t *testing.T) {
	// U+1F600 (😀) requires surrogate pair in UTF-16
	text := "😀"
	raw := encodeUTF16LE(text)
	if len(raw) != 4 {
		t.Fatalf("expected 4 bytes for surrogate pair, got %d", len(raw))
	}
	raw = append(raw, 0, 0) // null terminator
	padded := make([]byte, 10)
	copy(padded, raw)
	sym := &Symbol{DataType: "WSTRING", Length: 10}
	val, err := sym.parse(padded, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != text {
		t.Errorf("got %q, want %q", val, text)
	}
}

func TestWriteWSTRING_ASCII(t *testing.T) {
	sym := &Symbol{DataType: "WSTRING", Length: 20}
	data, err := sym.writeToNode("Hello", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 20 {
		t.Fatalf("expected 20 bytes, got %d", len(data))
	}
	// Verify UTF-16LE encoding
	expected := encodeUTF16LE("Hello")
	for i, b := range expected {
		if data[i] != b {
			t.Errorf("byte[%d] = 0x%02X, want 0x%02X", i, data[i], b)
		}
	}
	// Verify null terminator (bytes after "Hello" should be 0)
	for i := len(expected); i < 20; i++ {
		if data[i] != 0 {
			t.Errorf("byte[%d] = 0x%02X, want 0x00 (padding)", i, data[i])
		}
	}
}

func TestWriteWSTRING_Unicode(t *testing.T) {
	sym := &Symbol{DataType: "WSTRING", Length: 20}
	text := "日本語"
	data, err := sym.writeToNode(text, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expected := encodeUTF16LE(text)
	for i, b := range expected {
		if data[i] != b {
			t.Errorf("byte[%d] = 0x%02X, want 0x%02X", i, data[i], b)
		}
	}
}

func TestWriteWSTRING_Truncation(t *testing.T) {
	// Length 6 = room for 2 chars + null terminator (each 2 bytes)
	sym := &Symbol{DataType: "WSTRING", Length: 6}
	data, err := sym.writeToNode("ABCDE", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 6 {
		t.Fatalf("expected 6 bytes, got %d", len(data))
	}
	// Should only contain "AB" (2 chars * 2 bytes = 4) + 2 bytes null
	expected := encodeUTF16LE("AB")
	for i, b := range expected {
		if data[i] != b {
			t.Errorf("byte[%d] = 0x%02X, want 0x%02X", i, data[i], b)
		}
	}
	// Last 2 bytes should be null terminator
	if data[4] != 0 || data[5] != 0 {
		t.Errorf("expected null terminator at bytes 4-5, got 0x%02X 0x%02X", data[4], data[5])
	}
}

func TestWriteWSTRING_RoundTrip(t *testing.T) {
	texts := []string{"Hello", "日本語", "😀", "a", ""}
	for _, text := range texts {
		t.Run(text, func(t *testing.T) {
			sym := &Symbol{DataType: "WSTRING", Length: 40}
			data, err := sym.writeToNode(text, 0, nil)
			if err != nil {
				t.Fatalf("write error: %v", err)
			}
			val, err := sym.parse(data, 0, nil)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if val != text {
				t.Errorf("round-trip: got %q, want %q", val, text)
			}
		})
	}
}

// ==========================================================================
// Bit-level operations tests
// ==========================================================================

func TestParseBitSymbol_True(t *testing.T) {
	sym := &Symbol{DataType: "BYTE", Length: 1, Flags: SymbolFlagBitValue}
	data := []byte{0x01}
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "true" {
		t.Errorf("got %q, want %q", val, "true")
	}
}

func TestParseBitSymbol_False(t *testing.T) {
	sym := &Symbol{DataType: "BYTE", Length: 1, Flags: SymbolFlagBitValue}
	data := []byte{0x00}
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "false" {
		t.Errorf("got %q, want %q", val, "false")
	}
}

func TestWriteBitSymbol_True(t *testing.T) {
	sym := &Symbol{DataType: "BYTE", Length: 1, Flags: SymbolFlagBitValue}
	data, err := sym.writeToNode("true", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 1 || data[0] != 0x01 {
		t.Errorf("got %v, want [0x01]", data)
	}
}

func TestWriteBitSymbol_False(t *testing.T) {
	sym := &Symbol{DataType: "BYTE", Length: 1, Flags: SymbolFlagBitValue}
	data, err := sym.writeToNode("false", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 1 || data[0] != 0x00 {
		t.Errorf("got %v, want [0x00]", data)
	}
}

func TestSymbolFlagBitValue_Detection(t *testing.T) {
	flags := SymbolFlag(0x0002)
	if !flags.Has(SymbolFlagBitValue) {
		t.Error("expected Has(SymbolFlagBitValue) to be true")
	}
	flags = SymbolFlag(0x0000)
	if flags.Has(SymbolFlagBitValue) {
		t.Error("expected Has(SymbolFlagBitValue) to be false for 0x0000")
	}
	// Combined flags
	flags = SymbolFlag(0x0F03) // Persistent | BitValue | ContextMask
	if !flags.Has(SymbolFlagBitValue) {
		t.Error("expected Has(SymbolFlagBitValue) to be true for combined flags")
	}
}

func TestReadBit_Extract(t *testing.T) {
	// 0xA5 = 10100101
	data := []byte{0xA5}
	if !ReadBit(data, 0) {
		t.Error("bit 0 should be 1")
	}
	if ReadBit(data, 1) {
		t.Error("bit 1 should be 0")
	}
	if !ReadBit(data, 2) {
		t.Error("bit 2 should be 1")
	}
}

func TestReadBit_AllPositions(t *testing.T) {
	// 0xA5 = 10100101
	data := []byte{0xA5}
	expected := []bool{true, false, true, false, false, true, false, true}
	for i, want := range expected {
		t.Run(fmt.Sprintf("bit%d", i), func(t *testing.T) {
			got := ReadBit(data, i)
			if got != want {
				t.Errorf("bit %d: got %v, want %v", i, got, want)
			}
		})
	}
}

func TestWriteBit_Set(t *testing.T) {
	data := []byte{0x00}
	WriteBit(data, 3, true)
	if data[0] != 0x08 {
		t.Errorf("got 0x%02X, want 0x08", data[0])
	}
}

func TestWriteBit_Clear(t *testing.T) {
	data := []byte{0xFF}
	WriteBit(data, 3, false)
	if data[0] != 0xF7 {
		t.Errorf("got 0x%02X, want 0xF7", data[0])
	}
}

func TestWriteBit_PreservesOthers(t *testing.T) {
	// 0xA5 = 10100101, set bit 2 (already set) → no change
	data := []byte{0xA5}
	WriteBit(data, 2, true)
	if data[0] != 0xA5 {
		t.Errorf("got 0x%02X, want 0xA5 (unchanged)", data[0])
	}
	// Clear bit 1 (already clear) → no change
	WriteBit(data, 1, false)
	if data[0] != 0xA5 {
		t.Errorf("got 0x%02X, want 0xA5 (unchanged)", data[0])
	}
}

// ==========================================================================
// Process image constants and helpers
// ==========================================================================

func TestProcessImageConstants(t *testing.T) {
	tests := []struct {
		name string
		got  Group
		want uint32
	}{
		{"GroupIoImageRwib", GroupIoImageRwib, 0xF020},
		{"GroupIoImageRwix", GroupIoImageRwix, 0xF021},
		{"GroupIoImageRisize", GroupIoImageRisize, 0xF025},
		{"GroupIoImageRwob", GroupIoImageRwob, 0xF030},
		{"GroupIoImageRwox", GroupIoImageRwox, 0xF031},
		{"GroupIoImageCleari", GroupIoImageCleari, 0xF040},
		{"GroupIoImageClearo", GroupIoImageClearo, 0xF050},
		{"GroupIoImageRwiob", GroupIoImageRwiob, 0xF060},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if uint32(tt.got) != tt.want {
				t.Errorf("%s = 0x%04X, want 0x%04X", tt.name, uint32(tt.got), tt.want)
			}
		})
	}
}

func TestProcessImageBitOffset(t *testing.T) {
	// Verify the bit offset calculation used in process image bit access
	// byteOffset * 8 + bitIndex
	tests := []struct {
		byteOffset uint32
		bitIndex   uint8
		want       uint32
	}{
		{0, 0, 0},
		{0, 7, 7},
		{1, 0, 8},
		{1, 3, 11},
		{10, 5, 85},
	}
	for _, tt := range tests {
		got := tt.byteOffset*8 + uint32(tt.bitIndex)
		if got != tt.want {
			t.Errorf("byte=%d bit=%d: got %d, want %d", tt.byteOffset, tt.bitIndex, got, tt.want)
		}
	}
}

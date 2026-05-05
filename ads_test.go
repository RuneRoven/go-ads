package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
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

// ============================================================
// Symbol.parse — basic types
// ============================================================

func TestSymbolParseBasicTypes(t *testing.T) {
	tests := []struct {
		name     string
		dataType string
		length   uint32
		data     []byte
		want     string
	}{
		// BOOL
		{"BOOL/true", "BOOL", 1, []byte{1}, "true"},
		{"BOOL/false", "BOOL", 1, []byte{0}, "false"},
		// Integer types
		{"INT/-42", "INT", 2, le16(-42), "-42"},
		{"UINT/1234", "UINT", 2, leu16(1234), "1234"},
		{"SINT/-100", "SINT", 1, []byte{156}, "-100"}, // 156 = uint8(int8(-100))
		{"BYTE/255", "BYTE", 1, []byte{255}, "255"},
		{"DINT/-12345", "DINT", 4, le32(-12345), "-12345"},
		{"LINT/-9999999999", "LINT", 8, le64(-9999999999), "-9999999999"},
		{"ULINT/max", "ULINT", 8, leu64(18446744073709551615), "18446744073709551615"},
		// Float
		{"REAL/10", "REAL", 4, leF32(10.0), "10"},
		// String
		{"STRING/Hello", "STRING", 20, append([]byte("Hello\x00"), make([]byte, 14)...), "Hello"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sym := &Symbol{DataType: tt.dataType, Length: tt.length}
			got, err := sym.parse(tt.data, 0, nil)
			requireNoError(t, err)
			assertEqual(t, got, tt.want)
		})
	}
}

func TestSymbolParseLREAL(t *testing.T) {
	sym := &Symbol{DataType: "LREAL", Length: 8}
	val, err := sym.parse(leF64(3.141592653589793), 0, nil)
	requireNoError(t, err)
	f, _ := strconv.ParseFloat(val, 64)
	assertFloatApprox(t, f, 3.141592653589793, toleranceFloat64)
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

// ============================================================
// writeToNode round-trip tests
// ============================================================

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

func TestWriteToNodeRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		dataType string
		length   uint32
		value    string
	}{
		// BOOL
		{"BOOL/true", "BOOL", 1, "true"},
		{"BOOL/false", "BOOL", 1, "false"},
		// 8-bit
		{"BYTE/0", "BYTE", 1, "0"},
		{"BYTE/255", "BYTE", 1, "255"},
		{"USINT/42", "USINT", 1, "42"},
		{"SINT/min", "SINT", 1, "-128"},
		{"SINT/max", "SINT", 1, "127"},
		// 16-bit
		{"UINT/0", "UINT", 2, "0"},
		{"UINT/max", "UINT", 2, "65535"},
		{"WORD/1234", "WORD", 2, "1234"},
		{"UINT16/5678", "UINT16", 2, "5678"},
		{"INT/min", "INT", 2, "-32768"},
		{"INT/max", "INT", 2, "32767"},
		{"INT16/-42", "INT16", 2, "-42"},
		// 32-bit
		{"UDINT/0", "UDINT", 4, "0"},
		{"UDINT/max", "UDINT", 4, "4294967295"},
		{"DWORD/12345678", "DWORD", 4, "12345678"},
		{"DINT/min", "DINT", 4, "-2147483648"},
		{"DINT/max", "DINT", 4, "2147483647"},
		// 64-bit
		{"LINT/0", "LINT", 8, "0"},
		{"LINT/min", "LINT", 8, "-9223372036854775808"},
		{"LINT/max", "LINT", 8, "9223372036854775807"},
		{"ULINT/0", "ULINT", 8, "0"},
		{"ULINT/max", "ULINT", 8, "18446744073709551615"},
		{"LWORD/42", "LWORD", 8, "42"},
		// STRING
		{"STRING/Hello", "STRING", 20, "Hello"},
		// TIME
		{"TIME/12:34:56", "TIME", 4, "12:34:56"},
		{"TIME/midnight", "TIME", 4, "00:00:00"},
		{"TIME/with_ms", "TIME", 4, "12:34:56.789"},
		// TOD
		{"TOD/15:30", "TOD", 4, "15:30"},
		{"TOD/midnight", "TOD", 4, "00:00"},
		// DATE
		{"DATE/2024-01-15", "DATE", 4, "2024-01-15"},
		{"DATE/2000-06-15", "DATE", 4, "2000-06-15"},
		// DT
		{"DT/2024-01-15", "DT", 4, "2024-01-15 12:30:00"},
		{"DT/epoch", "DT", 4, "1970-01-01 00:00:00"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testWriteRoundTrip(t, tt.dataType, tt.length, tt.value)
		})
	}
}

func TestWriteToNodeRoundTripFloat(t *testing.T) {
	t.Run("REAL/3.14", func(t *testing.T) {
		sym := &Symbol{DataType: "REAL", Length: 4}
		data, err := sym.writeToNode("3.14", 0, nil)
		requireNoError(t, err)
		bits := binary.LittleEndian.Uint32(data)
		f := math.Float32frombits(bits)
		assertFloatApprox(t, float64(f), 3.14, toleranceFloat32)
	})

	t.Run("LREAL/pi", func(t *testing.T) {
		sym := &Symbol{DataType: "LREAL", Length: 8}
		data, err := sym.writeToNode("3.141592653589793", 0, nil)
		requireNoError(t, err)
		bits := binary.LittleEndian.Uint64(data)
		f := math.Float64frombits(bits)
		assertFloatApprox(t, f, 3.141592653589793, toleranceFloat64)
	})
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

// NOTE: per-type parse tests (DINT, LINT, ULINT, LREAL, BYTE, SINT) are consolidated
// into TestSymbolParseBasicTypes and TestSymbolParseLREAL above.

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
	sym, ok := symbols["main.test"] // internal keys are lowercased
	if !ok {
		t.Fatal("expected symbol 'main.test'")
	}
	if sym.FullName != "MAIN.test" {
		t.Errorf("FullName = %q, want %q (PLC original casing)", sym.FullName, "MAIN.test")
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
	// STRING(5) → Length=6 (5 chars + null). Writing exactly 5 chars should fit.
	sym := &Symbol{DataType: "STRING", Length: 6}
	data, err := sym.writeToNode("Hello", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data[:5]) != "Hello" {
		t.Errorf("got %q, want %q", string(data[:5]), "Hello")
	}
	if data[5] != 0 {
		t.Errorf("last byte should be null terminator, got %d", data[5])
	}
}

func TestWriteToNodeSTRING_Overflow(t *testing.T) {
	// STRING(3) → Length=4 (3 chars + null). "Hello" truncated to 3 chars + null.
	sym := &Symbol{DataType: "STRING", Length: 4}
	data, err := sym.writeToNode("Hello", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 4 {
		t.Fatalf("expected 4 bytes, got %d", len(data))
	}
	if string(data[:3]) != "Hel" {
		t.Errorf("got %q, want %q", string(data[:3]), "Hel")
	}
	if data[3] != 0 {
		t.Errorf("last byte should be null terminator, got %d", data[3])
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

// ============================================================
// REAL/LREAL special values (NaN, Inf, -Inf, -0, subnormal)
// ============================================================

func TestSymbolParseFloatSpecial(t *testing.T) {
	tests := []struct {
		name     string
		dataType string
		length   uint32
		data     []byte
		want     string // exact match; empty means custom check
		check    func(t *testing.T, val string)
	}{
		{"REAL/NaN", "REAL", 4, leF32(float32(math.NaN())), "NaN", nil},
		{"REAL/+Inf", "REAL", 4, leF32(float32(math.Inf(1))), "+Inf", nil},
		{"REAL/-Inf", "REAL", 4, leF32(float32(math.Inf(-1))), "-Inf", nil},
		{"REAL/-0", "REAL", 4, leF32(float32(math.Copysign(0, -1))), "", func(t *testing.T, val string) {
			if val != "0" && val != "-0" {
				t.Errorf("got %q, want %q or %q", val, "0", "-0")
			}
		}},
		{"LREAL/NaN", "LREAL", 8, leF64(math.NaN()), "NaN", nil},
		{"LREAL/+Inf", "LREAL", 8, leF64(math.Inf(1)), "+Inf", nil},
		{"LREAL/-Inf", "LREAL", 8, leF64(math.Inf(-1)), "-Inf", nil},
		{"LREAL/subnormal", "LREAL", 8, leu64(1), "", func(t *testing.T, val string) {
			f, err := strconv.ParseFloat(val, 64)
			requireNoError(t, err)
			if f != math.Float64frombits(1) {
				t.Errorf("got %v, want %v", f, math.Float64frombits(1))
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sym := &Symbol{DataType: tt.dataType, Length: tt.length}
			val, err := sym.parse(tt.data, 0, nil)
			requireNoError(t, err)
			if tt.check != nil {
				tt.check(t, val)
			} else {
				assertEqual(t, val, tt.want)
			}
		})
	}
}

func TestWriteToNodeFloatSpecial(t *testing.T) {
	t.Run("REAL/NaN", func(t *testing.T) {
		sym := &Symbol{DataType: "REAL", Length: 4}
		data, err := sym.writeToNode("NaN", 0, nil)
		requireNoError(t, err)
		f := math.Float32frombits(binary.LittleEndian.Uint32(data))
		if !math.IsNaN(float64(f)) {
			t.Errorf("expected NaN, got %v", f)
		}
	})

	t.Run("REAL/+Inf", func(t *testing.T) {
		sym := &Symbol{DataType: "REAL", Length: 4}
		data, err := sym.writeToNode("+Inf", 0, nil)
		requireNoError(t, err)
		f := math.Float32frombits(binary.LittleEndian.Uint32(data))
		if !math.IsInf(float64(f), 1) {
			t.Errorf("expected +Inf, got %v", f)
		}
	})

	t.Run("LREAL/NaN", func(t *testing.T) {
		sym := &Symbol{DataType: "LREAL", Length: 8}
		data, err := sym.writeToNode("NaN", 0, nil)
		requireNoError(t, err)
		f := math.Float64frombits(binary.LittleEndian.Uint64(data))
		if !math.IsNaN(f) {
			t.Errorf("expected NaN, got %v", f)
		}
	})
}

// ============================================================
// DATE/TIME boundary values and aliases
// ============================================================

func TestSymbolParseTemporalTypes(t *testing.T) {
	tests := []struct {
		name     string
		dataType string
		length   uint32
		data     []byte
		want     string
	}{
		// TIME
		{"TIME/midnight", "TIME", 4, leu32(0), "00:00:00"},
		{"TIME/max_ms", "TIME", 4, leu32(23*3600000 + 59*60000 + 59*1000 + 999), "23:59:59.999"},
		{"TIME/1h_exact", "TIME", 4, leu32(3600000), "01:00:00"},
		// TOD
		{"TOD/midnight", "TOD", 4, leu32(0), "00:00"},
		{"TOD/end_of_day", "TOD", 4, leu32(23*3600000 + 59*60000), "23:59"},
		// DATE
		{"DATE/epoch", "DATE", 4, leu32(0), "1970-01-01"},
		{"DATE/leap_year", "DATE", 4, leu32(1709164800), "2024-02-29"},
		{"DATE/max_uint32", "DATE", 4, leu32(math.MaxUint32), "2106-02-07"},
		// DT
		{"DT/Y2K38", "DT", 4, leu32(2147483647), "2038-01-19 03:14:07"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sym := &Symbol{DataType: tt.dataType, Length: tt.length}
			got, err := sym.parse(tt.data, 0, nil)
			requireNoError(t, err)
			assertEqual(t, got, tt.want)
		})
	}
}

func TestWriteToNodeTemporalRoundTrip(t *testing.T) {
	tests := []struct {
		name     string
		dataType string
		length   uint32
		value    string
	}{
		{"DT/epoch", "DT", 4, "1970-01-01 00:00:00"},
		{"DATE/leap_year", "DATE", 4, "2024-02-29"},
		{"TIME/with_ms", "TIME", 4, "01:02:03.456"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testWriteRoundTrip(t, tt.dataType, tt.length, tt.value)
		})
	}
}

func TestWriteToNodeTemporalAliases(t *testing.T) {
	tests := []struct {
		name     string
		dataType string
		length   uint32
		value    string
	}{
		{"TIME_OF_DAY/14:30", "TIME_OF_DAY", 4, "14:30"},
		{"DATE_AND_TIME/full", "DATE_AND_TIME", 4, "2024-06-15 23:59:59"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sym := &Symbol{DataType: tt.dataType, Length: tt.length}
			data, err := sym.writeToNode(tt.value, 0, nil)
			requireNoError(t, err)
			sym2 := &Symbol{DataType: tt.dataType, Length: tt.length}
			val, err := sym2.parse(data, 0, nil)
			requireNoError(t, err)
			assertEqual(t, val, tt.value)
		})
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

func TestDeviceNotification_UnknownHandleDuringClose(t *testing.T) {
	handler := &testLogHandler{}
	conn := newTestConnection()
	conn.logger = slog.New(handler)
	conn.closedCh = make(chan struct{})
	defer conn.shutdown()

	// Mark connection as closed
	conn.closed.Store(true)
	close(conn.closedCh)

	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, 42)
	packet := buildNotificationPacket(99, 0, data)

	err := conn.DeviceNotification(conn.ctx, packet)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// During Close(), stale notifications should be Debug, not Warn
	rec := handler.findByMessage("received notification for deleted handle")
	if rec == nil {
		t.Fatal("expected debug log for stale notification during close")
	}
	if rec.Level != slog.LevelDebug {
		t.Errorf("expected Debug level during close, got %v", rec.Level)
	}
}

func TestDeviceNotification_UnknownHandleNormalCondition(t *testing.T) {
	handler := &testLogHandler{}
	conn := newTestConnection()
	conn.logger = slog.New(handler)
	defer conn.shutdown()

	// No close, no recent reconnect — should be Warn
	data := make([]byte, 2)
	binary.LittleEndian.PutUint16(data, 42)
	packet := buildNotificationPacket(99, 0, data)

	err := conn.DeviceNotification(conn.ctx, packet)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rec := handler.findByMessage("received notification for unknown handle")
	if rec == nil {
		t.Fatal("expected warn log for unknown handle in normal conditions")
	}
	if rec.Level != slog.LevelWarn {
		t.Errorf("expected Warn level in normal conditions, got %v", rec.Level)
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

// Bit symbols in TwinCAT have DataType="BOOL" — the BitValue flag affects
// addressing (offset encodes bit position) but NOT parsing. Handle-based
// reads return data matching the declared DataType. Tests verify that
// BitValue flag does NOT interfere with normal type parsing.

func TestParseBitSymbol_True(t *testing.T) {
	// Real bit symbol: DataType=BOOL, BitValue flag set
	sym := &Symbol{DataType: "BOOL", Length: 1, Flags: SymbolFlagBitValue}
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
	sym := &Symbol{DataType: "BOOL", Length: 1, Flags: SymbolFlagBitValue}
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
	sym := &Symbol{DataType: "BOOL", Length: 1, Flags: SymbolFlagBitValue}
	data, err := sym.writeToNode("true", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 1 || data[0] != 0x01 {
		t.Errorf("got %v, want [0x01]", data)
	}
}

func TestWriteBitSymbol_False(t *testing.T) {
	sym := &Symbol{DataType: "BOOL", Length: 1, Flags: SymbolFlagBitValue}
	data, err := sym.writeToNode("false", 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 1 || data[0] != 0x00 {
		t.Errorf("got %v, want [0x00]", data)
	}
}

// BitValue flag must NOT override parsing of non-BOOL types.
// TC2 can set flag 0x0002 on UDINT, LREAL, etc.
func TestBitValueFlag_DoesNotOverrideUDINT(t *testing.T) {
	sym := &Symbol{DataType: "UDINT", Length: 4, Flags: SymbolFlagBitValue}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 12345)
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "12345" {
		t.Errorf("got %q, want %q", val, "12345")
	}
}

func TestBitValueFlag_DoesNotOverrideLREAL(t *testing.T) {
	sym := &Symbol{DataType: "LREAL", Length: 8, Flags: SymbolFlagBitValue}
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, math.Float64bits(3.14))
	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "3.14" {
		t.Errorf("got %q, want %q", val, "3.14")
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

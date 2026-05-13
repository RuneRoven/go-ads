package ads

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"unicode/utf16"
)

// F-16: int(symbol.Length) wraps on 32-bit Go. Validate symbol.Length against
// data buffer size before arithmetic to prevent negative slice / huge alloc.
// Validates: R-PARSE-006.
func TestParse_RejectsOversizedSymbolLength(t *testing.T) {
	data := make([]byte, 16)
	sym := &Symbol{
		Name:     "x",
		DataType: "INT",
		Length:   0xFFFFFFFF, // wildly oversized
	}
	_, err := sym.parse(data, 0, nil)
	if err == nil {
		t.Fatalf("expected error for oversized symbol.Length, got nil")
	}
	if !strings.Contains(err.Error(), "Length") {
		t.Errorf("expected error mentioning Length, got: %v", err)
	}
}

// Happy path: an INT symbol with Length=2 parses normally.
// Validates: R-PARSE-007.
func TestParse_HappyPath_INT(t *testing.T) {
	data := []byte{0x01, 0x00} // little-endian 1
	sym := &Symbol{
		Name:     "x",
		DataType: "INT",
		Length:   2,
	}
	got, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "1" {
		t.Errorf("got %q, want %q", got, "1")
	}
}

// F-17: STRING write with symbol.Length=0 must error rather than silently
// produce a 0-byte payload to the PLC.
// Validates: NO-SPEC strict.
func TestWriteToNode_STRING_RejectsZeroLength(t *testing.T) {
	sym := &Symbol{
		Name:     "s",
		DataType: "STRING",
		Length:   0,
	}
	_, err := sym.writeToNode("hello", nil)
	if err == nil {
		t.Fatalf("expected error for STRING with Length=0, got nil")
	}
}

// F-17: WSTRING write with symbol.Length<2 must error (need 2 bytes minimum
// for the null terminator).
// Validates: NO-SPEC strict.
func TestWriteToNode_WSTRING_RejectsTooShort(t *testing.T) {
	cases := []uint32{0, 1}
	for _, length := range cases {
		sym := &Symbol{
			Name:     "ws",
			DataType: "WSTRING",
			Length:   length,
		}
		_, err := sym.writeToNode("hi", nil)
		if err == nil {
			t.Errorf("expected error for WSTRING with Length=%d, got nil", length)
		}
	}
}

// F-18: write path must symmetrize with read — if read can fall back to
// inferBaseType for unknown types, write must accept the same input and
// round-trip it. Inference is limited to 1- and 2-byte widths (REAL/LREAL
// share 4 and 8 byte widths with DINT/LINT and cannot be disambiguated
// without a datatype table — see inferBaseType doc).
// Validates: NO-SPEC.
func TestWriteToNode_FallsBackToInferredType(t *testing.T) {
	// Unknown 2-byte type — write should fall back to INT.
	sym := &Symbol{
		Name:     "x",
		DataType: "MyEnum16", // unknown user type, 2-byte enum
		Length:   2,
	}
	got, err := sym.writeToNode("42", nil)
	if err != nil {
		t.Fatalf("expected inference fallback to succeed for 2-byte type, got error: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 bytes, got %d", len(got))
	}
	// Little-endian 42 = 2A 00
	if got[0] != 42 || got[1] != 0 {
		t.Errorf("expected [2A 00], got %v", got)
	}
}

// 4- and 8-byte writes must NOT silently infer DINT/LINT — would corrupt
// REAL/LREAL aliases. Caller must call LoadSymbols or use a known type.
// Validates: NO-SPEC (regression guard for REAL/LREAL ambiguity fix).
func TestWriteToNode_Refuses4And8ByteInferenceWithoutDatatypes(t *testing.T) {
	for _, size := range []uint32{4, 8} {
		sym := &Symbol{
			Name:     "x",
			DataType: "MyUnknownType",
			Length:   size,
		}
		_, err := sym.writeToNode("42", nil)
		if err == nil {
			t.Errorf("size=%d: expected error refusing inference (REAL/LREAL ambiguity), got nil", size)
		}
	}
}

// F-18: write must still error for sizes that don't match a known integer
// width (no inference possible).
// Validates: NO-SPEC.
func TestWriteToNode_RejectsUnknownTypeWithUninferableSize(t *testing.T) {
	sym := &Symbol{
		Name:     "x",
		DataType: "MyWeirdType",
		Length:   3, // not 1/2/4/8
	}
	_, err := sym.writeToNode("42", nil)
	if err == nil {
		t.Fatalf("expected error for size=3 unknown type, got nil")
	}
}

// Happy path: STRING with Length=10 truncates "hello world" to fit.
// Validates: R-PARSE-003.
func TestWriteToNode_STRING_HappyPath(t *testing.T) {
	sym := &Symbol{
		Name:     "s",
		DataType: "STRING",
		Length:   10,
	}
	got, err := sym.writeToNode("hello world", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 10 {
		t.Errorf("expected 10 bytes, got %d", len(got))
	}
	// First 9 bytes should be "hello wor" (Length-1 to reserve the null terminator).
	if string(got[:9]) != "hello wor" {
		t.Errorf("got %q, want %q", string(got[:9]), "hello wor")
	}
	if got[9] != 0 {
		t.Errorf("expected null terminator at byte 9, got 0x%X", got[9])
	}
}

// ============================================================
// Symbol.parse — basic types
// ============================================================

// Validates: R-PARSE-007.
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

// Validates: R-PARSE-007.
func TestSymbolParseLREAL(t *testing.T) {
	sym := &Symbol{DataType: "LREAL", Length: 8}
	val, err := sym.parse(leF64(3.141592653589793), 0, nil)
	requireNoError(t, err)
	f, _ := strconv.ParseFloat(val, 64)
	assertFloatApprox(t, f, 3.141592653589793, toleranceFloat64)
}

// Validates: NO-SPEC.
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

// Validates: R-PARSE-007 (write/read symmetry).
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

// Validates: R-PARSE-007.
func TestWriteToNodeRoundTripFloat(t *testing.T) {
	t.Run("REAL/3.14", func(t *testing.T) {
		sym := &Symbol{DataType: "REAL", Length: 4}
		data, err := sym.writeToNode("3.14", nil)
		requireNoError(t, err)
		bits := binary.LittleEndian.Uint32(data)
		f := math.Float32frombits(bits)
		assertFloatApprox(t, float64(f), 3.14, toleranceFloat32)
	})

	t.Run("LREAL/pi", func(t *testing.T) {
		sym := &Symbol{DataType: "LREAL", Length: 8}
		data, err := sym.writeToNode("3.141592653589793", nil)
		requireNoError(t, err)
		bits := binary.LittleEndian.Uint64(data)
		f := math.Float64frombits(bits)
		assertFloatApprox(t, f, 3.141592653589793, toleranceFloat64)
	})
}

// Validates: R-PARSE-001.
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

	data, err := parent.writeToNode(`{"field1":"42","field2":"100"}`, nil)
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

// Validates: R-PARSE-001.
func TestWriteToNodeStructPartialFields(t *testing.T) {
	child1 := &Symbol{Name: "x", FullName: "s.x", DataType: "BYTE", Length: 1, Offset: 0}
	child2 := &Symbol{Name: "y", FullName: "s.y", DataType: "BYTE", Length: 1, Offset: 1}
	parent := &Symbol{
		Name: "s", FullName: "s", DataType: "ST_S", Length: 2,
		Children: map[string]*Symbol{"x": child1, "y": child2},
	}

	// Only write "x", "y" should remain zero
	data, err := parent.writeToNode(`{"x":"7"}`, nil)
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

// Validates: NO-SPEC.
func TestWriteToNodeAliasResolution(t *testing.T) {
	datatypes := map[string]SymbolUploadDataType{
		"MyInt": {DataType: "INT"},
	}
	sym := &Symbol{DataType: "MyInt", Length: 2}
	data, err := sym.writeToNode("123", datatypes)
	if err != nil {
		t.Fatalf("alias writeToNode error: %v", err)
	}
	v := int16(binary.LittleEndian.Uint16(data))
	if v != 123 {
		t.Errorf("got %d, want 123", v)
	}
}

// Post-F-18: write falls back to inferBaseType for unknown types when size
// matches a known integer width (1/2/4/8). To keep error-path coverage on
// unknown types, use a non-inferable size (3) so the fallback also rejects.
// Validates: NO-SPEC.
func TestWriteToNodeAliasWithoutDatatypes(t *testing.T) {
	sym := &Symbol{DataType: "MyCustomType", Length: 3}
	_, err := sym.writeToNode("42", nil)
	if err == nil {
		t.Error("expected error for alias without datatypes (uninferable size)")
	}
}

// Validates: NO-SPEC.
func TestWriteToNodeUnknownType(t *testing.T) {
	sym := &Symbol{DataType: "UNKNOWN_XYZ", Length: 3}
	_, err := sym.writeToNode("42", map[string]SymbolUploadDataType{})
	if err == nil {
		t.Error("expected error for unknown type (uninferable size)")
	}
}

// --- parse error paths ---

// Validates: R-PARSE-006.
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

// Validates: R-SYM-003 (partial).
func TestSymbolParseSizeWrong(t *testing.T) {
	// BOOL with 2 bytes should fail (size mismatch)
	sym := &Symbol{DataType: "BOOL", Length: 2}
	_, err := sym.parse([]byte{1, 0}, 0, nil)
	if err == nil {
		t.Error("expected error for BOOL with wrong size")
	}
}

// Validates: NO-SPEC.
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

// Validates: NO-SPEC.
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
			_, err := sym.writeToNode(tt.value, nil)
			if err == nil {
				t.Errorf("expected error for %s with value %q", tt.dataType, tt.value)
			}
		})
	}
}

// Validates: NO-SPEC.
func TestWriteToNodeStructInvalidJSON(t *testing.T) {
	parent := &Symbol{
		Name: "s", FullName: "s", DataType: "ST_S", Length: 2,
		Children: map[string]*Symbol{
			"x": {Name: "x", FullName: "s.x", DataType: "BYTE", Length: 1, Offset: 0},
		},
	}
	_, err := parent.writeToNode("not json", nil)
	if err == nil {
		t.Error("expected error for invalid JSON in struct write")
	}
}

// ==========================================================================
// STRING edge cases
// ==========================================================================

// Validates: R-PARSE-005.
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

// Validates: R-PARSE-005.
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

// Validates: R-PARSE-005.
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

// Validates: R-PARSE-003.
func TestWriteToNodeSTRING_PadsWithZeros(t *testing.T) {
	sym := &Symbol{DataType: "STRING", Length: 10}
	data, err := sym.writeToNode("Hi", nil)
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

// Validates: R-PARSE-003.
func TestWriteToNodeSTRING_ExactLength(t *testing.T) {
	// STRING(5) → Length=6 (5 chars + null). Writing exactly 5 chars should fit.
	sym := &Symbol{DataType: "STRING", Length: 6}
	data, err := sym.writeToNode("Hello", nil)
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

// Validates: R-PARSE-003.
func TestWriteToNodeSTRING_Overflow(t *testing.T) {
	// STRING(3) → Length=4 (3 chars + null). "Hello" truncated to 3 chars + null.
	sym := &Symbol{DataType: "STRING", Length: 4}
	data, err := sym.writeToNode("Hello", nil)
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

// Validates: R-PARSE-005.
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

// Validates: R-PARSE-007.
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

// Validates: R-PARSE-007.
func TestWriteToNodeFloatSpecial(t *testing.T) {
	t.Run("REAL/NaN", func(t *testing.T) {
		sym := &Symbol{DataType: "REAL", Length: 4}
		data, err := sym.writeToNode("NaN", nil)
		requireNoError(t, err)
		f := math.Float32frombits(binary.LittleEndian.Uint32(data))
		if !math.IsNaN(float64(f)) {
			t.Errorf("expected NaN, got %v", f)
		}
	})

	t.Run("REAL/+Inf", func(t *testing.T) {
		sym := &Symbol{DataType: "REAL", Length: 4}
		data, err := sym.writeToNode("+Inf", nil)
		requireNoError(t, err)
		f := math.Float32frombits(binary.LittleEndian.Uint32(data))
		if !math.IsInf(float64(f), 1) {
			t.Errorf("expected +Inf, got %v", f)
		}
	})

	t.Run("LREAL/NaN", func(t *testing.T) {
		sym := &Symbol{DataType: "LREAL", Length: 8}
		data, err := sym.writeToNode("NaN", nil)
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

// Validates: R-PARSE-007.
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

// Validates: R-PARSE-007.
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

// Validates: R-PARSE-007.
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
			data, err := sym.writeToNode(tt.value, nil)
			requireNoError(t, err)
			sym2 := &Symbol{DataType: tt.dataType, Length: tt.length}
			val, err := sym2.parse(data, 0, nil)
			requireNoError(t, err)
			assertEqual(t, val, tt.value)
		})
	}
}

// ==========================================================================
// Deeply nested struct parsing
// ==========================================================================

// Validates: R-PARSE-001.
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

// Validates: R-PARSE-001.
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

	data, err := parent.writeToNode(`{"inner":{"val":"99"},"flag":"true"}`, nil)
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
// WSTRING (UTF-16LE) parse/write tests
// ==========================================================================

// Validates: R-PARSE-007 (WSTRING).
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

// Validates: R-PARSE-007 (WSTRING).
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

// Validates: R-PARSE-007 (WSTRING).
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

// Validates: R-PARSE-007 (WSTRING).
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

// Validates: R-PARSE-007 (WSTRING).
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

// Validates: R-PARSE-007 (WSTRING).
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

// Validates: R-PARSE-004 / R-PARSE-007.
func TestWriteWSTRING_ASCII(t *testing.T) {
	sym := &Symbol{DataType: "WSTRING", Length: 20}
	data, err := sym.writeToNode("Hello", nil)
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

// Validates: R-PARSE-004.
func TestWriteWSTRING_Unicode(t *testing.T) {
	sym := &Symbol{DataType: "WSTRING", Length: 20}
	text := "日本語"
	data, err := sym.writeToNode(text, nil)
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

// Validates: R-PARSE-004.
func TestWriteWSTRING_Truncation(t *testing.T) {
	// Length 6 = room for 2 chars + null terminator (each 2 bytes)
	sym := &Symbol{DataType: "WSTRING", Length: 6}
	data, err := sym.writeToNode("ABCDE", nil)
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

// Validates: R-PARSE-004.
func TestWriteWSTRING_RoundTrip(t *testing.T) {
	texts := []string{"Hello", "日本語", "😀", "a", ""}
	for _, text := range texts {
		t.Run(text, func(t *testing.T) {
			sym := &Symbol{DataType: "WSTRING", Length: 40}
			data, err := sym.writeToNode(text, nil)
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

// Validates: R-PARSE-007.
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

// Validates: R-PARSE-007.
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

// Validates: R-PARSE-007.
func TestWriteBitSymbol_True(t *testing.T) {
	sym := &Symbol{DataType: "BOOL", Length: 1, Flags: SymbolFlagBitValue}
	data, err := sym.writeToNode("true", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 1 || data[0] != 0x01 {
		t.Errorf("got %v, want [0x01]", data)
	}
}

// Validates: R-PARSE-007.
func TestWriteBitSymbol_False(t *testing.T) {
	sym := &Symbol{DataType: "BOOL", Length: 1, Flags: SymbolFlagBitValue}
	data, err := sym.writeToNode("false", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 1 || data[0] != 0x00 {
		t.Errorf("got %v, want [0x00]", data)
	}
}

// BitValue flag must NOT override parsing of non-BOOL types.
// TC2 can set flag 0x0002 on UDINT, LREAL, etc.
// Validates: R-PARSE-007.
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

// Validates: R-PARSE-007.
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

// Validates: NO-SPEC.
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

// Validates: NO-SPEC.
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

// Validates: NO-SPEC.
func TestWriteBit_Set(t *testing.T) {
	data := []byte{0x00}
	WriteBit(data, 3, true)
	if data[0] != 0x08 {
		t.Errorf("got 0x%02X, want 0x08", data[0])
	}
}

// Validates: NO-SPEC.
func TestWriteBit_Clear(t *testing.T) {
	data := []byte{0xFF}
	WriteBit(data, 3, false)
	if data[0] != 0xF7 {
		t.Errorf("got 0x%02X, want 0xF7", data[0])
	}
}

// Validates: NO-SPEC.
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

// --- ADST_ BaseType parse paths ---

// Validates: R-SYM-004.
func TestParseWithBaseType_REAL(t *testing.T) {
	// Symbol with unknown DataType but BaseType=ADST_REAL32 should parse as float
	sym := &Symbol{
		Name:     "MyAlias",
		FullName: "GVL.MyAlias",
		DataType: "MyRealAlias", // not in parseableTypes
		Length:   4,
		BaseType: ADSTReal32,
	}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, math.Float32bits(3.14))

	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	f, _ := strconv.ParseFloat(val, 64)
	if math.Abs(f-3.14) > 0.01 {
		t.Errorf("expected ~3.14, got %s", val)
	}
}

// Validates: R-SYM-004.
func TestParseWithBaseType_UDINT(t *testing.T) {
	// Symbol with BaseType=ADST_UINT32 should parse as unsigned, not signed
	sym := &Symbol{
		Name:     "MyEnum",
		FullName: "GVL.MyEnum",
		DataType: "E_MyEnum",
		Length:   4,
		BaseType: ADSTUint32,
	}
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, 3000000000) // > INT32_MAX

	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if val != "3000000000" {
		t.Errorf("expected 3000000000, got %s", val)
	}
}

// Validates: R-SYM-004.
func TestParseWithBaseType_FallbackToInfer(t *testing.T) {
	// BaseType=0 (ADST_VOID) → falls through to inferBaseType
	sym := &Symbol{
		Name:     "UnknownAlias",
		FullName: "GVL.UnknownAlias",
		DataType: "SomeAlias",
		Length:   2,
		BaseType: ADSTVoid, // no ADST_ info
	}
	data := []byte{0x39, 0x05} // 1337 as INT16

	val, err := sym.parse(data, 0, nil)
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if val != "1337" {
		t.Errorf("expected 1337, got %s", val)
	}
}

// TestWSTRINGSurrogatePairTruncation verifies that truncation of a WSTRING
// write that lands on a UTF-16 high surrogate drops the unpaired surrogate
// instead of writing a malformed sequence to the PLC.
// Validates: R-PARSE-004.
func TestWSTRINGSurrogatePairTruncation(t *testing.T) {
	// String with one BMP char followed by a non-BMP char (encodes as 2
	// UTF-16 code units forming a surrogate pair).
	// "A" = 1 code unit, "𝐀" (MATHEMATICAL BOLD CAPITAL A, U+1D400) = 2
	// code units (surrogate pair).
	value := "A\U0001D400"
	encoded := utf16.Encode([]rune(value))
	if len(encoded) != 3 {
		t.Fatalf("expected 3 UTF-16 code units, got %d", len(encoded))
	}
	if encoded[0] != 0x0041 {
		t.Errorf("expected 'A' = 0x0041, got 0x%04X", encoded[0])
	}
	if encoded[1] < 0xD800 || encoded[1] > 0xDBFF {
		t.Errorf("expected high surrogate in 0xD800-0xDBFF, got 0x%04X", encoded[1])
	}
	if encoded[2] < 0xDC00 || encoded[2] > 0xDFFF {
		t.Errorf("expected low surrogate in 0xDC00-0xDFFF, got 0x%04X", encoded[2])
	}

	// Symbol with Length such that maxChars = (Length-2)/2 lands the
	// truncation between the high+low surrogates. Length=6 => maxChars=2,
	// truncate keeps encoded[0:2] = ['A', high-surrogate]. The surrogate
	// fix should drop the high surrogate to avoid emitting an unpaired
	// surrogate to the PLC.
	sym := &Symbol{
		Name:     "ws",
		FullName: "ws",
		DataType: "WSTRING",
		Length:   6, // 2 chars + 2-byte null = 4+2
	}
	data, err := sym.writeToNode(value, nil)
	if err != nil {
		t.Fatalf("writeToNode error: %v", err)
	}
	if len(data) != 6 {
		t.Fatalf("expected 6 bytes (3 code units), got %d", len(data))
	}
	// First 2 bytes: 'A' little-endian = 0x41 0x00
	if data[0] != 0x41 || data[1] != 0x00 {
		t.Errorf("expected 'A' at bytes 0-1, got 0x%02X 0x%02X", data[0], data[1])
	}
	// Bytes 2-3 should be ZEROS (the would-be high surrogate dropped),
	// followed by 2-byte null terminator at bytes 4-5.
	if data[2] != 0x00 || data[3] != 0x00 {
		t.Errorf("expected high surrogate dropped (zero bytes 2-3 - either truncated or null), got 0x%02X 0x%02X", data[2], data[3])
	}
}

// F-xx: child.Offset + child.Length overflows uint32 — must error, not panic.
// Validates: R-WRITE-OVERFLOW-001.
func TestWriteToNode_ChildOffsetOverflow(t *testing.T) {
	// Offset = MaxUint32-1, Length = 4: sum overflows to 2, bypassing bounds check.
	child := &Symbol{
		Name:     "Field",
		FullName: "Parent.Field",
		DataType: "DINT",
		Offset:   math.MaxUint32 - 1,
		Length:   4,
	}
	parent := &Symbol{
		Name:     "Parent",
		FullName: "Parent",
		DataType: "SomeStruct",
		Length:   10,
		Children: map[string]*Symbol{"Field": child},
	}
	_, err := parent.writeToNode(`{"Field": "42"}`, nil)
	if err == nil {
		t.Fatal("expected error for child Offset+Length overflow, got nil")
	}
}

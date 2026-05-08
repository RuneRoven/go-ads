package ads

import (
	"strings"
	"testing"
)

// F-16: int(symbol.Length) wraps on 32-bit Go. Validate symbol.Length against
// data buffer size before arithmetic to prevent negative slice / huge alloc.
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
// round-trip it. Currently write errors on unknown types even when size
// matches a known integer width.
func TestWriteToNode_FallsBackToInferredType(t *testing.T) {
	// Unknown type with size=4 — read would fall back to DINT.
	sym := &Symbol{
		Name:     "x",
		DataType: "MyEnum32", // unknown user type
		Length:   4,
	}
	got, err := sym.writeToNode("42", nil)
	if err != nil {
		t.Fatalf("expected inference fallback to succeed, got error: %v", err)
	}
	if len(got) != 4 {
		t.Errorf("expected 4 bytes, got %d", len(got))
	}
	// Little-endian 42 = 2A 00 00 00
	if got[0] != 42 || got[1] != 0 || got[2] != 0 || got[3] != 0 {
		t.Errorf("expected [2A 00 00 00], got %v", got)
	}
}

// F-18: write must still error for sizes that don't match a known integer
// width (no inference possible).
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

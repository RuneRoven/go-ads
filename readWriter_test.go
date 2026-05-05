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
	_, err := sym.writeToNode("hello", 0, nil)
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
		_, err := sym.writeToNode("hi", 0, nil)
		if err == nil {
			t.Errorf("expected error for WSTRING with Length=%d, got nil", length)
		}
	}
}

// Happy path: STRING with Length=10 truncates "hello world" to fit.
func TestWriteToNode_STRING_HappyPath(t *testing.T) {
	sym := &Symbol{
		Name:     "s",
		DataType: "STRING",
		Length:   10,
	}
	got, err := sym.writeToNode("hello world", 0, nil)
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

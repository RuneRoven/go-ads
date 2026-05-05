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

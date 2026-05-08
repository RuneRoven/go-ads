package ads

import (
	"testing"
	"unicode/utf16"
)

// TestAddOffsetDepthCap verifies the recursion cap defends against malformed
// PLC datatype tables that form a self-cycle (forbidden by IEC 61131-3 but
// not enforced over the wire).
func TestAddOffsetDepthCap(t *testing.T) {
	// Build a self-cycle: type "MyStruct" has a child of type "MyStruct".
	// Real PLCs reject this at compile time; the wire response could in
	// theory contain it through a buggy or malicious target.
	cyclic := SymbolUploadDataType{
		DataType: "MyStruct",
		Children: map[string]*SymbolUploadDataType{
			"member": {
				Name:     "member",
				DataType: "MyStruct",
				DatatypeEntry: datatypeEntry{
					Size: 4,
					Offs: 0,
				},
			},
		},
	}
	datatypes := map[string]SymbolUploadDataType{
		"MyStruct": cyclic,
	}

	parent := &Symbol{Name: "root", FullName: "root", Length: 4}
	// Should NOT stack-overflow even though the cycle would otherwise recurse
	// indefinitely.
	children := cyclic.addOffset(parent, datatypes, 0x4020)
	if len(children) == 0 {
		t.Fatal("expected at least one child before depth cap fired")
	}
	// The cap fires partway down; depth 256 produces ~256 nested children
	// before the warn-and-return. Sanity check we didn't blow the stack.
}

// TestCollectSubtreeDepthCap verifies the recursion cap on Symbol.Children
// walks defends against tree corruption (cycle in Children map).
func TestCollectSubtreeDepthCap(t *testing.T) {
	// Build a Symbol cycle: A -> B -> A. addOffset cannot produce this in
	// real cache data, but defensively we should not stack-overflow.
	a := &Symbol{Name: "A", FullName: "A"}
	b := &Symbol{Name: "B", FullName: "A.B"}
	a.Children = map[string]*Symbol{"B": b}
	b.Children = map[string]*Symbol{"A": a}

	conn := newTestConnection()
	defer conn.lifecycle.shutdown()

	// Walk via collectSubtree (lock not actually needed for this test
	// because we are not touching cache.symbols).
	var collected []SymbolView
	collectSubtree(a, conn, &collected)
	// Should NOT stack-overflow; cap=256 limits the walk.
	if len(collected) == 0 {
		t.Fatal("expected at least one collected view before cap fires")
	}
}

// TestWSTRINGSurrogatePairTruncation verifies that truncation of a WSTRING
// write that lands on a UTF-16 high surrogate drops the unpaired surrogate
// instead of writing a malformed sequence to the PLC.
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

// TestSumNotificationResultTriState exercises the three states of the
// Skipped+Error+Handle field on SumNotificationResult to ensure callers can
// distinguish library-skipped from PLC-errored from successful entries.
func TestSumNotificationResultTriState(t *testing.T) {
	cases := []struct {
		name        string
		result      SumNotificationResult
		wantSkipped bool
		wantPLCErr  bool
		wantSuccess bool
	}{
		{
			name:        "success",
			result:      SumNotificationResult{Handle: 42, Error: ReturnCodeNoErrors, Skipped: nil},
			wantSuccess: true,
		},
		{
			name:       "PLC error",
			result:     SumNotificationResult{Handle: 0, Error: ReturnCodeDeviceError, Skipped: nil},
			wantPLCErr: true,
		},
		{
			name:        "library skipped (duplicate)",
			result:      SumNotificationResult{Handle: 0, Skipped: errSentinel("dup")},
			wantSkipped: true,
		},
		{
			name:        "library skipped + PLC handle (TOCTOU loss)",
			result:      SumNotificationResult{Handle: 99, Skipped: errSentinel("race")},
			wantSkipped: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isSkipped := tc.result.Skipped != nil
			isPLCErr := tc.result.Skipped == nil && tc.result.Error != ReturnCodeNoErrors
			isSuccess := tc.result.Skipped == nil && tc.result.Error == ReturnCodeNoErrors
			if isSkipped != tc.wantSkipped {
				t.Errorf("skipped: got %v, want %v", isSkipped, tc.wantSkipped)
			}
			if isPLCErr != tc.wantPLCErr {
				t.Errorf("PLC error: got %v, want %v", isPLCErr, tc.wantPLCErr)
			}
			if isSuccess != tc.wantSuccess {
				t.Errorf("success: got %v, want %v", isSuccess, tc.wantSuccess)
			}
			// TOCTOU-loss case: Skipped non-nil AND Handle non-zero means
			// caller must release the PLC-side registration.
			if isSkipped && tc.result.Handle != 0 {
				t.Logf("caller would call DeleteDeviceNotification(%d) for cleanup", tc.result.Handle)
			}
		})
	}
}

// errSentinel is a tiny helper to build named errors for table-driven tests.
type errSentinel string

func (e errSentinel) Error() string { return string(e) }

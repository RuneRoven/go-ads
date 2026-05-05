package ads

import (
	"fmt"
	"testing"
)

// F-15: PLC-controlled Elements with no sanity cap allows DoS via huge map allocation.
// Cap at 1M entries per level. Reject and return empty map.
func TestMakeArrayChildren_CapsExcessiveElements(t *testing.T) {
	levels := []datatypeArrayInfo{{LBound: 0, Elements: 10_000_000}} // 10M
	got := makeArrayChildren(levels, "INT", 20_000_000)
	if got == nil {
		t.Fatalf("expected non-nil empty map on cap-exceeded, got nil")
	}
	if len(got) != 0 {
		t.Errorf("expected zero children when Elements exceeds cap, got %d", len(got))
	}
}

// F-15: LBound + Elements that overflows uint32 must be rejected.
func TestMakeArrayChildren_RejectsOverflowBound(t *testing.T) {
	levels := []datatypeArrayInfo{{LBound: 0xFFFFFFF0, Elements: 0x20}} // overflows
	got := makeArrayChildren(levels, "INT", 64)
	if got == nil {
		t.Fatalf("expected non-nil empty map on overflow, got nil")
	}
	if len(got) != 0 {
		t.Errorf("expected zero children on uint32 overflow, got %d", len(got))
	}
}

// Happy path: small array of 4 elements still works after the cap is added.
func TestMakeArrayChildren_HappyPath(t *testing.T) {
	levels := []datatypeArrayInfo{{LBound: 0, Elements: 4}}
	got := makeArrayChildren(levels, "INT", 8) // 8 bytes / 4 = 2 bytes each
	if len(got) != 4 {
		t.Errorf("expected 4 children, got %d", len(got))
	}
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("[%d]", i)
		if _, ok := got[key]; !ok {
			t.Errorf("missing child %q", key)
		}
	}
}

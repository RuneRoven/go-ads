package ads

import (
	"testing"
	"time"
)

// TestZeroOldSymbolHandles validates R-CACHE-004 in full: loadSymbols
// replaces the cache.symbols map, but callers (e.g. readMultipleSymbolsRetry)
// may hold *Symbol pointers into the OLD map. zeroOldSymbolHandles MUST
// clear Handle, Value, Valid, ValueParsed, and LastUpdateTime so stale
// data cannot leak post-reconnect.
//
// Validates: R-CACHE-004.
func TestZeroOldSymbolHandles(t *testing.T) {
	t0 := time.Now()
	oldMap := map[string]*Symbol{
		"a": {
			Name:           "a",
			Handle:         0x1234,
			Value:          "42",
			Valid:          true,
			ValueParsed:    true,
			LastUpdateTime: t0,
		},
		"b": {
			Name:           "b",
			Handle:         0x5678,
			Value:          "hello",
			Valid:          true,
			ValueParsed:    true,
			LastUpdateTime: t0,
		},
		"c": {Name: "c"}, // already zero across all fields
	}
	pa := oldMap["a"]
	pb := oldMap["b"]
	pc := oldMap["c"]

	zeroOldSymbolHandles(oldMap)

	for _, p := range []*Symbol{pa, pb, pc} {
		if p.Handle != 0 {
			t.Errorf("%s.Handle = 0x%X, want 0", p.Name, p.Handle)
		}
		if p.Value != "" {
			t.Errorf("%s.Value = %q, want empty string", p.Name, p.Value)
		}
		if p.Valid {
			t.Errorf("%s.Valid = true, want false", p.Name)
		}
		if p.ValueParsed {
			t.Errorf("%s.ValueParsed = true, want false", p.Name)
		}
		if !p.LastUpdateTime.IsZero() {
			t.Errorf("%s.LastUpdateTime = %v, want zero", p.Name, p.LastUpdateTime)
		}
	}
}

// Nil and empty input must not panic. Validates: R-CACHE-004 (defensive).
func TestZeroOldSymbolHandles_NilSafe(t *testing.T) {
	zeroOldSymbolHandles(nil)
	zeroOldSymbolHandles(map[string]*Symbol{})
}

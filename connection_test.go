package ads

import (
	"testing"
)

// F-20: loadSymbols replaces conn.symbols map with fresh entries (Handle=0).
// Callers may hold *Symbol pointers from the OLD map (e.g. infos[i].symbol
// in readMultipleSymbolsRetry). After the swap, those old pointers retain
// stale Handle values. Defensive fix: zero Handle on every old *Symbol
// before replacing the map.
func TestZeroOldSymbolHandles(t *testing.T) {
	oldMap := map[string]*Symbol{
		"a": {Name: "a", Handle: 0x1234},
		"b": {Name: "b", Handle: 0x5678},
		"c": {Name: "c", Handle: 0}, // already zero
	}
	pa := oldMap["a"]
	pb := oldMap["b"]
	pc := oldMap["c"]

	zeroOldSymbolHandles(oldMap)

	if pa.Handle != 0 {
		t.Errorf("pa.Handle = 0x%X, want 0", pa.Handle)
	}
	if pb.Handle != 0 {
		t.Errorf("pb.Handle = 0x%X, want 0", pb.Handle)
	}
	if pc.Handle != 0 {
		t.Errorf("pc.Handle = 0x%X, want 0", pc.Handle)
	}
}

// Nil and empty input must not panic.
func TestZeroOldSymbolHandles_NilSafe(t *testing.T) {
	zeroOldSymbolHandles(nil)
	zeroOldSymbolHandles(map[string]*Symbol{})
}

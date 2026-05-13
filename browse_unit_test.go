package ads

import (
	"testing"
)

// TestBrowseSymbols_VirtualRootGroupingPreservesCase validates that virtual
// root-level groupings (where no Symbol exists for the prefix itself, only
// for child paths) display in the original PLC casing, not the lowercased
// cache-key form. Regression test for browse.go::browseRoot.
func TestBrowseSymbols_VirtualRootGroupingPreservesCase(t *testing.T) {
	sess := newTestConnection()
	defer sess.lifecycle.shutdown()

	// Seed cache: only "MAIN_DP1.nCounter" exists, no symbol named "MAIN_DP1" itself.
	sess.cache.symbols[symbolKey("MAIN_DP1.nCounter")] = &Symbol{
		FullName: "MAIN_DP1.nCounter",
		Name:     "nCounter",
		DataType: "DINT",
		Length:   4,
	}
	sess.cache.symbolListLoaded = true

	entries, err := sess.BrowseSymbols("")
	if err != nil {
		t.Fatalf("BrowseSymbols: %v", err)
	}

	var found *SymbolBrowseEntry
	for i := range entries {
		if entries[i].FullName == "MAIN_DP1" {
			found = &entries[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("expected MAIN_DP1 (original case) virtual grouping, got entries: %+v", entries)
	}
	if found.Name != "MAIN_DP1" {
		t.Errorf("Name = %q, want MAIN_DP1", found.Name)
	}
	if !found.HasChildren {
		t.Errorf("HasChildren = false, want true for virtual grouping")
	}
}

// TestBrowseSymbols_VirtualChildGroupingPreservesCase validates the same
// case-preservation behavior for nested (middle-segment) virtual groupings
// in browseChildren's fallback prefix-scan path.
func TestBrowseSymbols_VirtualChildGroupingPreservesCase(t *testing.T) {
	sess := newTestConnection()
	defer sess.lifecycle.shutdown()

	// Seed: "MAIN_DP1.stStruct.nField" — no symbol for "MAIN_DP1" or
	// "MAIN_DP1.stStruct" themselves. browseChildren("MAIN_DP1") falls
	// through to the prefix-scan branch (no exact-match symbol with
	// Children present), exercising the virtual-middle-grouping case.
	sess.cache.symbols[symbolKey("MAIN_DP1.stStruct.nField")] = &Symbol{
		FullName: "MAIN_DP1.stStruct.nField",
		Name:     "nField",
		DataType: "INT",
		Length:   2,
	}
	sess.cache.symbolListLoaded = true

	entries, err := sess.BrowseSymbols("MAIN_DP1")
	if err != nil {
		t.Fatalf("BrowseSymbols: %v", err)
	}

	var found *SymbolBrowseEntry
	for i := range entries {
		if entries[i].FullName == "MAIN_DP1.stStruct" {
			found = &entries[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("expected MAIN_DP1.stStruct (original case) virtual grouping, got: %+v", entries)
	}
	if found.Name != "stStruct" {
		t.Errorf("Name = %q, want stStruct", found.Name)
	}
	if !found.HasChildren {
		t.Errorf("HasChildren = false, want true for virtual grouping")
	}
}

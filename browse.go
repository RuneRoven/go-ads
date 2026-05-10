package ads

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
)

// SymbolBrowseEntry represents a browsable symbol or child.
type SymbolBrowseEntry struct {
	Name        string // short name (e.g., "motor")
	FullName    string // full path (e.g., "MAIN.motor")
	DataType    string // type name (e.g., "ST_Motor", "INT")
	Size        uint32
	HasChildren bool // true if struct/array (requires LoadDataTypes to expand)
	Comment     string
}

// BrowseSymbols returns browsable entries at the given path in the symbol hierarchy.
// If path is empty, returns root-level groupings (first path segments).
// If path is specified, returns children of that symbol or prefix.
// Requires LoadSymbolList() or LoadSymbols() to have been called first.
func (sess *Session) BrowseSymbols(path string) ([]SymbolBrowseEntry, error) {
	sess.cache.lock.Lock()
	defer sess.cache.lock.Unlock()

	if !sess.cache.symbolListLoaded && !sess.cache.symbolsFullyLoaded {
		return nil, fmt.Errorf("symbol list not loaded; call LoadSymbolList() or LoadSymbols() first")
	}

	if path == "" {
		return sess.browseRoot(), nil
	}

	return sess.browseChildren(path), nil
}

// browseRoot returns unique root-level entries (first segment of each symbol name).
// Must be called with cache.lock held.
func (sess *Session) browseRoot() []SymbolBrowseEntry {
	// Two-pass to preserve original PLC casing for virtual groupings.
	// symbolKey() lowercases for cache lookup, but Symbol.FullName retains
	// original case — derive display from any child symbol's FullName.
	// Map key = lowercased prefix, value = original-case prefix.
	roots := make(map[string]string)
	var entries []SymbolBrowseEntry

	for name, sym := range sess.cache.symbols {
		// Skip children (only top-level symbols from the upload have no Parent)
		if sym.Parent != nil {
			continue
		}

		// Get first segment (e.g., "MAIN" from "MAIN.myVar")
		dot := strings.IndexByte(name, '.')
		if dot < 0 {
			// No dot — this is a root symbol itself
			if _, seen := roots[name]; !seen {
				roots[name] = sym.FullName
				entries = append(entries, SymbolBrowseEntry{
					Name:        sym.Name,
					FullName:    sym.FullName,
					DataType:    sym.DataType,
					Size:        sym.Length,
					HasChildren: sess.symbolHasChildren(sym),
					Comment:     sym.Comment,
				})
			}
			continue
		}

		root := name[:dot]
		if _, seen := roots[root]; seen {
			continue
		}
		// Capture original-case prefix from this symbol's FullName.
		// sym.FullName length >= dot since key is its lowercased form.
		origRoot := root
		if len(sym.FullName) >= dot {
			origRoot = sym.FullName[:dot]
		}
		roots[root] = origRoot
		// Check if the root itself is a symbol
		if rootSym, ok := sess.cache.symbols[symbolKey(root)]; ok {
			entries = append(entries, SymbolBrowseEntry{
				Name:        rootSym.Name,
				FullName:    rootSym.FullName,
				DataType:    rootSym.DataType,
				Size:        rootSym.Length,
				HasChildren: true, // has children since we found dotted names
				Comment:     rootSym.Comment,
			})
		} else {
			// Virtual grouping (e.g., "MAIN" prefix with no symbol for "MAIN" itself).
			// Use original case derived from child symbol's FullName.
			entries = append(entries, SymbolBrowseEntry{
				Name:        origRoot,
				FullName:    origRoot,
				HasChildren: true,
			})
		}
	}

	slices.SortFunc(entries, func(a, b SymbolBrowseEntry) int {
		return cmp.Compare(a.FullName, b.FullName)
	})
	return entries
}

// browseChildren returns children of a given path.
// Must be called with cache.lock held.
func (sess *Session) browseChildren(path string) []SymbolBrowseEntry {
	// First: check if the exact symbol exists and has Children
	if sym, ok := sess.cache.symbols[symbolKey(path)]; ok && len(sym.Children) > 0 {
		entries := make([]SymbolBrowseEntry, 0, len(sym.Children))
		for _, child := range sym.Children {
			entries = append(entries, SymbolBrowseEntry{
				Name:        child.Name,
				FullName:    child.FullName,
				DataType:    child.DataType,
				Size:        child.Length,
				HasChildren: sess.symbolHasChildren(child),
				Comment:     child.Comment,
			})
		}
		slices.SortFunc(entries, func(a, b SymbolBrowseEntry) int {
			return cmp.Compare(a.FullName, b.FullName)
		})
		return entries
	}

	// Fallback: scan for symbols with the prefix "path."
	prefix := symbolKey(path) + "."
	seen := make(map[string]bool)
	var entries []SymbolBrowseEntry

	for name, sym := range sess.cache.symbols {
		if !strings.HasPrefix(name, prefix) {
			continue
		}

		// Get next segment after prefix
		rest := name[len(prefix):]
		dot := strings.IndexByte(rest, '.')
		var segment string
		if dot < 0 {
			segment = rest
		} else {
			segment = rest[:dot]
		}

		childFullName := prefix + segment
		if seen[childFullName] {
			continue
		}
		seen[childFullName] = true

		if childSym, ok := sess.cache.symbols[symbolKey(childFullName)]; ok {
			entries = append(entries, SymbolBrowseEntry{
				Name:        childSym.Name,
				FullName:    childSym.FullName,
				DataType:    childSym.DataType,
				Size:        childSym.Length,
				HasChildren: sess.symbolHasChildren(childSym),
				Comment:     childSym.Comment,
			})
		} else {
			// Virtual middle-grouping. Recover original case for the segment
			// from the child symbol's FullName (cache key is lowercased, but
			// Symbol.FullName retains PLC case). sym.FullName aligns with the
			// lowercased key, so we slice at the same offsets.
			origSegment := segment
			origFullName := childFullName
			if len(sym.FullName) >= len(prefix)+len(segment) {
				origSegment = sym.FullName[len(prefix) : len(prefix)+len(segment)]
				origFullName = sym.FullName[:len(prefix)+len(segment)]
			}
			entries = append(entries, SymbolBrowseEntry{
				Name:        origSegment,
				FullName:    origFullName,
				HasChildren: true,
			})
		}
	}

	// Also check for the exact symbol with no deeper children
	if sym, ok := sess.cache.symbols[symbolKey(path)]; ok && len(entries) == 0 {
		entries = append(entries, SymbolBrowseEntry{
			Name:        sym.Name,
			FullName:    sym.FullName,
			DataType:    sym.DataType,
			Size:        sym.Length,
			HasChildren: false,
			Comment:     sym.Comment,
		})
	}

	slices.SortFunc(entries, func(a, b SymbolBrowseEntry) int {
		return cmp.Compare(a.FullName, b.FullName)
	})
	return entries
}

// symbolHasChildren determines if a symbol likely has children.
// Must be called with cache.lock held.
func (sess *Session) symbolHasChildren(sym *Symbol) bool {
	// If we already have expanded children, yes
	if len(sym.Children) > 0 {
		return true
	}

	// If datatypes are loaded, check the datatype table
	if sess.cache.datatypesLoaded && sess.cache.datatypes != nil {
		if dt, ok := sess.cache.datatypes[sym.DataType]; ok {
			return len(dt.Children) > 0
		}
	}

	// Heuristic: if the datatype is not a primitive parseable type, it's likely a struct
	if sym.DataType != "" && !slices.Contains(parseableTypes, sym.DataType) {
		return true
	}

	return false
}

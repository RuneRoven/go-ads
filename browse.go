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
func (conn *Session) BrowseSymbols(path string) ([]SymbolBrowseEntry, error) {
	conn.cache.lock.Lock()
	defer conn.cache.lock.Unlock()

	if !conn.cache.symbolListLoaded && !conn.cache.symbolsFullyLoaded {
		return nil, fmt.Errorf("symbol list not loaded; call LoadSymbolList() or LoadSymbols() first")
	}

	if path == "" {
		return conn.browseRoot(), nil
	}

	return conn.browseChildren(path), nil
}

// browseRoot returns unique root-level entries (first segment of each symbol name).
// Must be called with cache.lock held.
func (conn *Session) browseRoot() []SymbolBrowseEntry {
	roots := make(map[string]bool)
	var entries []SymbolBrowseEntry

	for name, sym := range conn.cache.symbols {
		// Skip children (only top-level symbols from the upload have no Parent)
		if sym.Parent != nil {
			continue
		}

		// Get first segment (e.g., "MAIN" from "MAIN.myVar")
		dot := strings.IndexByte(name, '.')
		if dot < 0 {
			// No dot — this is a root symbol itself
			if !roots[name] {
				roots[name] = true
				entries = append(entries, SymbolBrowseEntry{
					Name:        sym.Name,
					FullName:    sym.FullName,
					DataType:    sym.DataType,
					Size:        sym.Length,
					HasChildren: conn.symbolHasChildren(sym),
					Comment:     sym.Comment,
				})
			}
			continue
		}

		root := name[:dot]
		if !roots[root] {
			roots[root] = true
			// Check if the root itself is a symbol
			if rootSym, ok := conn.cache.symbols[symbolKey(root)]; ok {
				entries = append(entries, SymbolBrowseEntry{
					Name:        rootSym.Name,
					FullName:    rootSym.FullName,
					DataType:    rootSym.DataType,
					Size:        rootSym.Length,
					HasChildren: true, // has children since we found dotted names
					Comment:     rootSym.Comment,
				})
			} else {
				// Virtual grouping (e.g., "MAIN" prefix with no symbol for "MAIN" itself)
				entries = append(entries, SymbolBrowseEntry{
					Name:        root,
					FullName:    root,
					HasChildren: true,
				})
			}
		}
	}

	slices.SortFunc(entries, func(a, b SymbolBrowseEntry) int {
		return cmp.Compare(a.FullName, b.FullName)
	})
	return entries
}

// browseChildren returns children of a given path.
// Must be called with cache.lock held.
func (conn *Session) browseChildren(path string) []SymbolBrowseEntry {
	// First: check if the exact symbol exists and has Children
	if sym, ok := conn.cache.symbols[symbolKey(path)]; ok && len(sym.Children) > 0 {
		entries := make([]SymbolBrowseEntry, 0, len(sym.Children))
		for _, child := range sym.Children {
			entries = append(entries, SymbolBrowseEntry{
				Name:        child.Name,
				FullName:    child.FullName,
				DataType:    child.DataType,
				Size:        child.Length,
				HasChildren: conn.symbolHasChildren(child),
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

	for name := range conn.cache.symbols {
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

		if childSym, ok := conn.cache.symbols[symbolKey(childFullName)]; ok {
			entries = append(entries, SymbolBrowseEntry{
				Name:        childSym.Name,
				FullName:    childSym.FullName,
				DataType:    childSym.DataType,
				Size:        childSym.Length,
				HasChildren: conn.symbolHasChildren(childSym),
				Comment:     childSym.Comment,
			})
		} else {
			// We know there are deeper symbols, so this is a grouping
			entries = append(entries, SymbolBrowseEntry{
				Name:        segment,
				FullName:    childFullName,
				HasChildren: true,
			})
		}
	}

	// Also check for the exact symbol with no deeper children
	if sym, ok := conn.cache.symbols[symbolKey(path)]; ok && len(entries) == 0 {
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
func (conn *Session) symbolHasChildren(sym *Symbol) bool {
	// If we already have expanded children, yes
	if len(sym.Children) > 0 {
		return true
	}

	// If datatypes are loaded, check the datatype table
	if conn.cache.datatypesLoaded && conn.cache.datatypes != nil {
		if dt, ok := conn.cache.datatypes[sym.DataType]; ok {
			return len(dt.Children) > 0
		}
	}

	// Heuristic: if the datatype is not a primitive parseable type, it's likely a struct
	if sym.DataType != "" && !slices.Contains(parseableTypes, sym.DataType) {
		return true
	}

	return false
}

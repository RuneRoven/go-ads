package ads

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// symbolCache owns the connection-level symbol metadata: the symbol map,
// the data-type table, the PLC's reported symbol version (for change
// detection), and discovery-mode flags that track which load function was
// used (LoadSymbols, LoadSymbolsSlow, LoadSymbolList, LoadDataTypes).
//
// Lock also covers Symbol mutation during parse() — Symbol objects live
// in the cache.symbols map and parse() rewrites their Value/Valid
// fields. Lock ordering: NEVER hold both cache.lock and notifications.lock at
// the same time. Paths that need both must release one before acquiring
// the other.
//
// Generation tracking lives on sessionFSM.epoch. Cache.symbols swaps
// (loadSymbols, LoadSymbolList, LoadDataTypes, on-demand reset in
// reloadSymbols) call Session.bumpEpoch() under this lock. Simple insert
// (on-demand getSymbol) does NOT bump — existing pointers stay valid
// across an insert.
//
// Callers that need to publish a *Symbol pointer they obtained pre-roundtrip
// into another data structure (e.g. notifications.activeNotifications) MUST capture
// the epoch before resolve and recheck before commit; if the value changed,
// the pointer is stranded and must be discarded. Closes the residual race
// window between cache.lock release and notifications.lock acquire that the
// simple re-fetch pattern leaves open.
type symbolCache struct {
	lock               sync.Mutex
	symbols            map[string]*Symbol
	datatypes          map[string]SymbolUploadDataType
	symbolVersion      uint8
	onDemandSymbols    map[string]bool
	symbolListLoaded   bool
	symbolsFullyLoaded bool
	datatypesLoaded    bool
}

// symbolKey normalizes a symbol name for use as an internal map key.
// TwinCAT treats symbol names case-insensitively (IEC 61131-3).
// TC2 returns uppercase, TC3 preserves original casing — lowercasing
// ensures consistent lookups regardless of caller or PLC casing.
func symbolKey(name string) string { return strings.ToLower(name) }

// normalizeStringDataType strips trailing array/length suffixes from STRING
// and WSTRING type names. The PLC reports types like "STRING(80)" or
// "WSTRING(255)"; the library treats all string variants of the same kind
// uniformly, so we collapse to the bare "STRING" or "WSTRING" prefix.
//
// Returns the input unchanged if it is neither a STRING nor a WSTRING.
func normalizeStringDataType(dt string) string {
	switch {
	case len(dt) >= 7 && dt[:7] == "WSTRING":
		return "WSTRING"
	case len(dt) >= 6 && dt[:6] == "STRING":
		return "STRING"
	default:
		return dt
	}
}

type datatypeEntry struct {
	EntryLength   uint32
	Version       uint32
	HashValue     uint32
	TypeHashValue uint32
	Size          uint32
	Offs          uint32
	DataType      uint32
	Flags         uint32
	NameLength    uint16
	TypeLength    uint16
	CommentLength uint16
	ArrayDim      uint16
	SubItems      uint16
}

type datatypeArrayInfo struct {
	LBound   uint32
	Elements uint32
}

type SymbolUploadDataType struct {
	DatatypeEntry datatypeEntry
	Name          string
	DataType      string
	Comment       string
	Children      map[string]*SymbolUploadDataType
}

type symbolEntry struct {
	EntryLength   uint32
	IGroup        uint32
	IOffs         uint32
	Size          uint32
	DataType      uint32
	Flags         uint32
	NameLength    uint16
	TypeLength    uint16
	CommentLength uint16
}

type symbolUploadSymbol struct {
	SymbolEntry symbolEntry
	Name        string
	DataType    string
	Comment     string
	Children    map[string]*symbolUploadSymbol
}

type SymbolUploadInfo struct {
	SymbolCount    uint32
	SymbolLength   uint32
	DataTypeCount  uint32
	DataTypeLength uint32
	ExtraCount     uint32
	ExtraLength    uint32
}

// Symbol is the internal cache record for a PLC symbol. External callers
// should use SymbolView (returned by GetSymbol/ListSymbols) - direct access
// to *Symbol is reserved for in-package code paths that need to mutate
// state under the appropriate lock.
//
// Field guards:
//
//   - Immutable after construction (safe to read without a lock):
//     FullName, Name, DataType, Comment, Group, Offset, Length, BaseType,
//     Flags, ContextMask, MinUpdateInterval, Parent, Children.
//
//   - Guarded by Session.cache.lock (mutated by parse/Read/Write/loadSymbols):
//     Value, Valid, ValueParsed, LastUpdateTime.
//
//   - Handle: write-once during getSymbol resolve under cache.lock, then
//     stable until loadSymbols zeroes the old map (also under cache.lock).
//     Concurrent reads after resolve are safe lock-free — observed value is
//     either the resolved handle or 0 (post-reload), and a 0 handle naturally
//     fails the next PLC operation (ReturnCodeDeviceNotifyHandleInvalid),
//     prompting re-resolve via GetSymbol.
//
//   - Guarded by Session.notifications.lock (mutated by AddSymbolNotification(s)
//     and DeleteDeviceNotification):
//     Notification.
//
// The Children map and Parent pointer form a tree, set up during discovery
// and never mutated after - callers may walk freely without a lock as long
// as no concurrent reload (LoadSymbols/LoadSymbolsSlow) is in progress.
type Symbol struct {
	FullName          string
	LastUpdateTime    time.Time
	MinUpdateInterval time.Duration
	Name              string
	DataType          string
	Comment           string
	Handle            uint32
	Group             uint32
	Offset            uint32
	Length            uint32
	BaseType          uint32 // ADST_ numeric type code from protocol (e.g., ADSTReal32=4 for REAL)
	Flags             SymbolFlag
	ContextMask       uint8 // PLC task context (bits 8-11 of Flags); 0 = no task binding

	Value       string
	Valid       bool
	ValueParsed bool // true after first successful parse

	Notification chan<- *Update

	Parent   *Symbol
	Children map[string]*Symbol
}

// SymbolView is a read-only snapshot of a symbol's metadata and current
// cached value, captured atomically under cache.lock at view creation.
// All fields - including Value and Parsed - reflect the cache state at the
// instant GetSymbol/ListSymbols returned. The view does NOT track later
// updates; for fresh data, call GetSymbol again or subscribe via
// AddSymbolNotification.
//
// Trade-offs of the snapshot model:
//   - Read-only ergonomics: every field access is a cheap struct read, no
//     locks, no allocation, no deadlock risk.
//   - Internally consistent: Parsed and Value were captured together, so
//     callers cannot observe a "Parsed=true, Value=empty" tear.
//   - Stale after concurrent loadSymbols / online-change: if the cache is
//     swapped after the view is built, the view shows the prior state.
//
// IsValid() returns false for the zero-value SymbolView. Children() and
// ChildrenWalk() collect snapshots under cache.lock and release before
// invoking the caller's iterator - safe to call any Session method
// from within a walk.
type SymbolView struct {
	Name        string
	FullName    string
	DataType    string
	Comment     string
	Handle      uint32
	Group       uint32
	Offset      uint32
	Length      uint32
	BaseType    uint32 // ADST_ numeric type code from protocol
	Flags       SymbolFlag
	ContextMask uint8 // PLC task context (bits 8-11 of Flags); 0 = no task binding
	Parsed      bool  // true if Value has been parsed at least once at snapshot time
	IsRoot      bool  // true if this symbol has no parent (top-level program/global var)
	Value       string

	conn *Session
}

// IsValid reports whether the view is backed by a live connection. Zero-value
// SymbolView returns false; views obtained from GetSymbol/ListSymbols return
// true (until the connection is closed).
func (v SymbolView) IsValid() bool { return v.conn != nil && v.FullName != "" }

// Children returns SymbolViews for struct/array members captured at view
// creation time. Returns nil for scalars and for symbols whose subtree has
// not been populated by full discovery. The map is freshly allocated per
// call; callers can mutate it without affecting library state.
//
// Each child is a freshly-snapshotted SymbolView (lookups walk the cache
// under cache.lock once per call, then release). Caller code in a walk loop
// is free to call any Session method - no lock held.
func (v SymbolView) Children() map[string]SymbolView {
	if v.conn == nil {
		return nil
	}
	v.conn.cache.lock.Lock()
	s := v.conn.cache.symbols[symbolKey(v.FullName)]
	if s == nil || len(s.Children) == 0 {
		v.conn.cache.lock.Unlock()
		return nil
	}
	out := make(map[string]SymbolView, len(s.Children))
	for k, c := range s.Children {
		if c == nil {
			continue
		}
		out[k] = c.view(v.conn)
	}
	v.conn.cache.lock.Unlock()
	return out
}

// ChildrenWalk visits every symbol in the subtree rooted at this view in
// depth-first order. Snapshots the entire subtree under cache.lock once,
// releases the lock, then invokes fn for each entry. fn is free to call
// any Session method (Value field reads are lock-free, GetSymbol etc.
// take their own locks - no deadlock risk).
//
// Walk terminates early if fn returns false.
func (v SymbolView) ChildrenWalk(fn func(SymbolView) bool) {
	if v.conn == nil || fn == nil {
		return
	}
	v.conn.cache.lock.Lock()
	root := v.conn.cache.symbols[symbolKey(v.FullName)]
	if root == nil {
		v.conn.cache.lock.Unlock()
		return
	}
	// Phase 1: collect snapshots under lock.
	var snapshots []SymbolView
	collectSubtree(root, v.conn, &snapshots)
	v.conn.cache.lock.Unlock()
	// Phase 2: invoke fn outside lock.
	for _, view := range snapshots {
		if !fn(view) {
			return
		}
	}
}

// collectSubtreeMaxDepth caps recursion depth as defense against malformed
// data forming a Children cycle. Real PLC struct nesting is at most a few
// dozen levels.
const collectSubtreeMaxDepth = 256

func collectSubtree(s *Symbol, conn *Session, out *[]SymbolView) {
	collectSubtreeDepth(s, conn, out, 0)
}

func collectSubtreeDepth(s *Symbol, conn *Session, out *[]SymbolView, depth int) {
	if depth >= collectSubtreeMaxDepth {
		getDefaultLogger().Warn("collectSubtree hit depth cap; possible Children cycle or malformed Symbol tree",
			"symbol", s.FullName,
			"max_depth", collectSubtreeMaxDepth)
		return
	}
	for _, c := range s.Children {
		if c == nil {
			continue
		}
		*out = append(*out, c.view(conn))
		collectSubtreeDepth(c, conn, out, depth+1)
	}
}

// view builds a SymbolView for s. Caller must hold cache.lock so the
// snapshot of metadata + value is internally consistent. O(1).
func (s *Symbol) view(conn *Session) SymbolView {
	return SymbolView{
		Name:        s.Name,
		FullName:    s.FullName,
		DataType:    s.DataType,
		Comment:     s.Comment,
		Handle:      s.Handle,
		Group:       s.Group,
		Offset:      s.Offset,
		Length:      s.Length,
		BaseType:    s.BaseType,
		Flags:       s.Flags,
		ContextMask: s.ContextMask,
		Parsed:      s.Valid,
		IsRoot:      s.Parent == nil,
		Value:       s.Value,
		conn:        conn,
	}
}

func parseUploadSymbolInfoSymbols(data []byte, datatypes map[string]SymbolUploadDataType) (symbols map[string]*Symbol, err error) {
	symbols = map[string]*Symbol{}
	buff := bytes.NewBuffer(data)

	for buff.Len() > 0 {
		begBuff := buff.Len()
		result := symbolEntry{}
		if err := binary.Read(buff, binary.LittleEndian, &result); err != nil {
			return nil, fmt.Errorf("reading symbol entry: %w", err)
		}

		name := make([]byte, result.NameLength)
		dt := make([]byte, result.TypeLength)
		comment := make([]byte, result.CommentLength)
		if err := binary.Read(buff, binary.LittleEndian, name); err != nil {
			return nil, fmt.Errorf("reading symbol name: %w", err)
		}
		buff.Next(1)
		if err := binary.Read(buff, binary.LittleEndian, dt); err != nil {
			return nil, fmt.Errorf("reading symbol type: %w", err)
		}
		buff.Next(1)
		if err := binary.Read(buff, binary.LittleEndian, comment); err != nil {
			return nil, fmt.Errorf("reading symbol comment: %w", err)
		}
		buff.Next(1)
		item := symbolUploadSymbol{}
		item.Name = string(name)
		item.DataType = string(dt)
		item.DataType = normalizeStringDataType(item.DataType)
		item.Comment = string(comment)
		item.SymbolEntry = result
		endBuff := buff.Len()
		symbol := addSymbol(item, datatypes)

		symbols[symbolKey(item.Name)] = symbol
		addChildren(symbol, symbols)

		skip := int(item.SymbolEntry.EntryLength) - (begBuff - endBuff)
		if skip < 0 {
			return nil, fmt.Errorf("symbol %q: EntryLength %d is smaller than bytes consumed %d",
				item.Name, item.SymbolEntry.EntryLength, begBuff-endBuff)
		}
		if skip > 0 {
			buff.Next(skip)
		}
	}
	return
}

func addChildren(symbol *Symbol, symbols map[string]*Symbol) {
	for _, child := range symbol.Children {
		if _, ok := symbols[symbolKey(child.FullName)]; !ok {
			symbols[symbolKey(child.FullName)] = child
			addChildren(child, symbols)
		}
	}
}

func addSymbol(symbol symbolUploadSymbol, datatypes map[string]SymbolUploadDataType) *Symbol {
	flags := SymbolFlag(symbol.SymbolEntry.Flags)
	sym := &Symbol{
		Name:              symbol.Name,
		LastUpdateTime:    time.Now(),
		MinUpdateInterval: 50 * time.Millisecond,
		FullName:          symbol.Name,
		DataType:          symbol.DataType,
		Comment:           symbol.Comment,
		Length:            symbol.SymbolEntry.Size,
		BaseType:          symbol.SymbolEntry.DataType,
		Group:             symbol.SymbolEntry.IGroup,
		Offset:            symbol.SymbolEntry.IOffs,
		Flags:             flags,
		ContextMask:       flags.ContextMask(),
	}

	dt, ok := datatypes[symbol.DataType]
	if ok {
		sym.Children = dt.addOffset(sym, datatypes, sym.Group)
	}

	return sym
}

// addOffsetMaxDepth caps recursion depth in datatype tree expansion. Real
// PLC nesting is at most a few dozen levels; the cap defends against a
// malformed datatype table forming a self-cycle (forbidden by IEC 61131-3
// but not enforced over the wire).
const addOffsetMaxDepth = 256

func (data *SymbolUploadDataType) addOffset(parent *Symbol, datatypes map[string]SymbolUploadDataType, group uint32) (children map[string]*Symbol) {
	return data.addOffsetDepth(parent, datatypes, group, 0)
}

func (data *SymbolUploadDataType) addOffsetDepth(parent *Symbol, datatypes map[string]SymbolUploadDataType, group uint32, depth int) (children map[string]*Symbol) {
	children = map[string]*Symbol{}
	if depth >= addOffsetMaxDepth {
		getDefaultLogger().Warn("addOffset hit depth cap; possible datatype self-cycle in PLC response",
			"parent", parent.FullName,
			"datatype", data.DataType,
			"max_depth", addOffsetMaxDepth)
		return
	}

	for key, segment := range data.Children {
		var path string
		if len(segment.Name) == 0 {
			continue
		}
		if segment.Name[0:1] != "[" {
			path = fmt.Sprint(parent.FullName, ".", segment.Name)
		} else {
			path = fmt.Sprint(parent.FullName, segment.Name)
		}

		child := Symbol{
			Name:              segment.Name,
			LastUpdateTime:    time.Now(),
			MinUpdateInterval: 50 * time.Millisecond,
			FullName:          path,
			DataType:          segment.DataType,
			Comment:           segment.Comment,
			Length:            segment.DatatypeEntry.Size,
			// Update with area and offset
			Group:  group,
			Offset: segment.DatatypeEntry.Offs,
			Parent: parent,
		}

		// Check if subitems exist — but skip enum types.
		// Enums have children (enum constants) but should be parsed as
		// their base type (e.g. INT), not expanded as struct fields.
		if dt, ok := datatypes[segment.DataType]; ok && !isEnumDataType(&dt) {
			child.Children = dt.addOffsetDepth(&child, datatypes, child.Group, depth+1)
		}

		children[key] = &child
	}

	return
}

// isEnumDataType returns true if a datatype represents an enum.
// Enums have a parseable base type (e.g. INT, UINT), children
// (enum constants), and ArrayDim == 0. Arrays of parseable types
// also have children and a parseable base type but have ArrayDim > 0.
func isEnumDataType(dt *SymbolUploadDataType) bool {
	return len(dt.Children) > 0 &&
		dt.DatatypeEntry.ArrayDim == 0 &&
		slices.Contains(parseableTypes, dt.DataType)
}

func parseUploadSymbolInfoDataTypes(data []byte) (datatypes map[string]SymbolUploadDataType, err error) {
	buff := bytes.NewBuffer(data)
	datatypes = make(map[string]SymbolUploadDataType)
	for buff.Len() > 0 {
		header, err := decodeSymbolUploadDataType(buff, "")
		if err != nil {
			return nil, fmt.Errorf("parsing datatype entry: %w", err)
		}
		datatypes[header.Name] = header
	}
	return
}

func decodeSymbolUploadDataType(data *bytes.Buffer, parent string) (header SymbolUploadDataType, err error) {
	result := datatypeEntry{}
	header = SymbolUploadDataType{}

	totalSize := data.Len()

	if totalSize < 48 {
		err = fmt.Errorf("%s - wrong size < 48 bytes", parent)
		getDefaultLogger().Error("error during binary read", "error", err, hexAttr("data", data.Bytes()))
		return
	}

	err = binary.Read(data, binary.LittleEndian, &result)
	if err != nil {
		getDefaultLogger().Error("error during binary read", "error", err)
		return
	}
	name := make([]byte, result.NameLength)
	dt := make([]byte, result.TypeLength)
	comment := make([]byte, result.CommentLength)

	err = binary.Read(data, binary.LittleEndian, name)
	if err != nil {
		getDefaultLogger().Error("error during binary read", "error", err)
		return
	}
	data.Next(1)
	err = binary.Read(data, binary.LittleEndian, dt)
	if err != nil {
		getDefaultLogger().Error("error during binary read", "error", err)
		return
	}
	data.Next(1)
	err = binary.Read(data, binary.LittleEndian, comment)
	if err != nil {
		getDefaultLogger().Error("error during binary read", "error", err)
		return
	}
	data.Next(1)

	header.Name = string(name)
	header.DataType = string(dt)
	header.Comment = string(comment)

	header.DatatypeEntry = result

	header.DataType = normalizeStringDataType(header.DataType)

	childLen := int(result.EntryLength) - (totalSize - data.Len())
	if childLen <= 0 {
		return
	}
	if childLen > data.Len() {
		return header, fmt.Errorf("childLen %d exceeds remaining data %d for %s", childLen, data.Len(), header.Name)
	}

	childData := make([]byte, childLen)
	n, err := data.Read(childData)
	if err != nil {
		return header, fmt.Errorf("reading children of %s (got %d of %d bytes): %w", header.Name, n, childLen, err)
	}

	buff := bytes.NewBuffer(childData)
	if header.Children == nil {
		header.Children = map[string]*SymbolUploadDataType{}
	}
	if header.DatatypeEntry.ArrayDim > 0 {
		// Children is an array
		var arrayInfo datatypeArrayInfo
		arrayLevels := []datatypeArrayInfo{}

		for i := 0; i < int(header.DatatypeEntry.ArrayDim); i++ {
			err = binary.Read(buff, binary.LittleEndian, &arrayInfo)
			if err != nil {
				return header, fmt.Errorf("reading array info for %s: %w", header.Name, err)
			}
			arrayLevels = append(arrayLevels, arrayInfo)
		}
		header.Children = makeArrayChildren(arrayLevels, header.DataType, header.DatatypeEntry.Size)
	} else {
		// Children is standard variables
		for j := 0; j < int(result.SubItems); j++ {
			child, err := decodeSymbolUploadDataType(buff, header.Name)
			if err != nil {
				return header, fmt.Errorf("reading subitem %d of %s: %w", j, header.Name, err)
			}
			header.Children[child.Name] = &child
		}
	}

	return
}

// maxArrayElementsPerLevel caps PLC-declared array Elements per dimension to
// prevent malformed/buggy datatype responses from triggering huge map
// allocations. Real PLC arrays rarely exceed a few thousand elements;
// 1M is a safety ceiling well above any legitimate use.
const maxArrayElementsPerLevel = 1_000_000

func makeArrayChildren(levels []datatypeArrayInfo, dt string, size uint32) (children map[string]*SymbolUploadDataType) {
	children = map[string]*SymbolUploadDataType{}

	if len(levels) < 1 {
		return
	}

	level := levels[0]
	if level.Elements == 0 {
		return
	}
	// defend against malformed/buggy PLC datatype responses.
	// (1) Cap Elements at a sanity limit to prevent DoS via huge map allocation.
	// (2) Reject when LBound + Elements overflows uint32 — loop counter would
	//     wrap and either skip the body or iterate ~4 billion times.
	if level.Elements > maxArrayElementsPerLevel {
		getDefaultLogger().Error("makeArrayChildren: array Elements exceeds sanity cap, refusing to allocate",
			"declared_elements", level.Elements,
			"cap", maxArrayElementsPerLevel,
			"datatype", dt)
		return
	}
	if uint64(level.LBound)+uint64(level.Elements) > uint64(^uint32(0)) {
		getDefaultLogger().Error("makeArrayChildren: LBound + Elements overflows uint32, refusing",
			"lbound", level.LBound,
			"elements", level.Elements,
			"datatype", dt)
		return
	}
	// subChildren is shared across all array elements at this level.
	// This is intentional: children are read-only after symbol table construction,
	// and deep-copying would be expensive for large arrays (e.g., ARRAY[0..999]).
	subChildren := makeArrayChildren(levels[1:], dt, size)

	var offset uint32

	for i := level.LBound; i < level.LBound+level.Elements; i++ {
		name := fmt.Sprintf("[%d]", i)

		child := SymbolUploadDataType{}
		child.Name = name
		child.DataType = dt
		child.DatatypeEntry.Offs = offset
		child.DatatypeEntry.Size = size / level.Elements
		child.Children = subChildren

		children[name] = &child
		offset += size / level.Elements
	}

	return
}

// GetJSON returns symbol value as JSON string.
func (symbol *Symbol) GetJSON() string {
	data := symbol.parseSymbol()
	jsonData, err := json.Marshal(data)
	if err != nil {
		getDefaultLogger().Warn("GetJSON marshal error", "symbol", symbol.Name, "error", err)
		return ""
	}
	return string(jsonData)
}

var stringsList = map[string]struct{}{"STRING": {}, "WSTRING": {}, "TIME": {}, "TOD": {}, "TIME_OF_DAY": {}, "DATE": {}, "DT": {}, "DATE_AND_TIME": {}}

// parseSymbol returns JSON interface for symbol
func (symbol *Symbol) parseSymbol() (rData interface{}) {
	if len(symbol.Children) == 0 {
		if symbol.DataType == "BOOL" {
			v, err := strconv.ParseBool(symbol.Value)
			if err != nil {
				getDefaultLogger().Warn("parseSymbol: invalid BOOL value, defaulting to false",
					"symbol", symbol.Name, "value", symbol.Value, "error", err)
			}
			rData = v
		} else if _, ok := stringsList[symbol.DataType]; ok {
			rData = symbol.Value
		} else {
			v, err := strconv.ParseFloat(symbol.Value, 64)
			if err != nil {
				getDefaultLogger().Warn("parseSymbol: invalid numeric value, defaulting to 0",
					"symbol", symbol.Name, "dataType", symbol.DataType, "value", symbol.Value, "error", err)
			}
			rData = v
		}
	} else {
		localMap := make(map[string]interface{})
		for _, child := range symbol.Children {
			s := strings.ReplaceAll(child.Name, "[", `"[`)
			s = strings.ReplaceAll(s, "]", `]"`)
			localMap[s] = child.parseSymbol()
		}
		rData = localMap
	}
	return
}

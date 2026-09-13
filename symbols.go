package ads

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// symbolCache owns the connection-level symbol metadata: the symbol map, the
// datatype table, the reported symbol version, and which load function was used.
// The lock also covers parse(), which rewrites symbols in place.
//
// Lock ordering: NEVER hold cache.lock and notifications.lock at once.
//
// Generation tracking lives on sessionFSM.epoch: a cache.symbols swap bumps it, a
// simple on-demand insert does not. A caller publishing a *symbol obtained
// pre-roundtrip into another structure MUST capture the epoch before resolve and
// recheck before commit, or the pointer may be stranded.
type symbolCache struct {
	lock               sync.Mutex
	symbols            map[string]*symbol
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

// symbol is the internal cache record; external callers use SymbolView.
//
// Field guards: the metadata (FullName, DataType, Group, Offset, Length,
// BaseType, Flags, Parent, Children) is immutable after construction and needs no
// lock. Value, Valid, ValueParsed and LastUpdateTime are guarded by cache.lock, as
// is Handle -- zeroed on reload, and an observed zero simply fails the next PLC
// call and prompts a re-resolve. Parent/Children form a tree fixed at discovery.
type symbol struct {
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
	BaseType          ADSDataType // protocol ADST_ code (e.g., ADSTReal32=4 for REAL)
	Flags             SymbolFlag
	ContextMask       uint8 // PLC task context (bits 8-11 of Flags); 0 = no task binding

	Value       string
	Valid       bool
	ValueParsed bool // true after first successful parse

	Parent   *symbol
	Children map[string]*symbol

	// The session's logger, stamped on as the symbol enters the cache; nil falls
	// back to the package default. Carried here rather than threaded through
	// parse/writeToNode, whose ~70 call sites would otherwise bypass WithLogger --
	// unmutable, unroutable, and stripped of the host's structured fields.
	logger *slog.Logger

	// inferenceWarned latches the "inferring base type from size" warning, which
	// reports a STATIC property of the symbol — its width and the absence of a
	// datatype table — and so says nothing new on the second poll. It fired once
	// per symbol per poll: measured on TC2 with loadSymbols false, 44 of the 61
	// lines in a 25 s run. Latched per symbol, so the first one still tells the
	// operator to call LoadSymbols.
	inferenceWarned bool

	// baseTypeWarned latches the "base type unresolvable" warning from
	// SymbolView.BaseTypeName. Same reasoning as inferenceWarned: the condition is
	// a static property of the symbol, so repeating it per call says nothing — and
	// a consumer may call BaseTypeName once per sample.
	baseTypeWarned bool
}

// warnInferenceOnce logs the base-type inference warning the first time it
// applies to this symbol and stays quiet afterwards.
//
// Not atomic: symbols are parsed under cache.lock (see the F-39 note in
// improvements.md), so this field has the same protection every other field on
// symbol has. If that ever changes, this becomes a race — and a benign one, since
// the worst outcome is a duplicate warning.
func (s *symbol) warnInferenceOnce(msg string, args ...any) {
	if s.inferenceWarned {
		return
	}
	s.inferenceWarned = true
	s.log().Warn(msg, args...)
}

// logOr returns lg, or the package default when a caller has none to give (a
// test-built fixture, or a parse that runs before a session owns the result).
// Lets the discovery-path free functions take a logger without every call site
// having to invent one.
func logOr(lg *slog.Logger) *slog.Logger {
	if lg == nil {
		return getDefaultLogger()
	}
	return lg
}

// connLogger returns a session's logger, or the package default for a nil or
// detached session. Free functions that receive a *Session use this so their
// records reach the caller's handler like everything else.
func connLogger(conn *Session) *slog.Logger {
	if conn == nil || conn.logger == nil {
		return getDefaultLogger()
	}
	return conn.logger
}

// log returns the logger records about this view belong on.
func (v SymbolView) log() *slog.Logger { return connLogger(v.conn) }

// log returns the logger records about this symbol belong on: the session's if
// the symbol came from one, the package default otherwise.
func (s *symbol) log() *slog.Logger {
	if s == nil || s.logger == nil {
		return getDefaultLogger()
	}
	return s.logger
}

// stampLogger attaches lg to a symbol and everything below it, so records
// produced while parsing or serialising a symbol reach the caller's logger.
//
// Called at the points where a session takes ownership of symbols, which is the
// only place that knows both the tree and the logger. Depth-bounded for the same
// reason collectSubtreeDepth is: a malformed PLC response can present a cycle,
// and a stack overflow is a worse outcome than an unstamped subtree.
func stampLogger(s *symbol, lg *slog.Logger, depth int) {
	if s == nil || lg == nil || depth >= collectSubtreeMaxDepth {
		return
	}
	s.logger = lg
	for _, child := range s.Children {
		stampLogger(child, lg, depth+1)
	}
}

// stampLoggerOnAll stamps a whole symbol map, for the bulk cache swaps.
func stampLoggerOnAll(symbols map[string]*symbol, lg *slog.Logger) {
	for _, s := range symbols {
		stampLogger(s, lg, 0)
	}
}

// SymbolView is a read-only snapshot of a symbol's metadata and cached value,
// captured under cache.lock at creation. It does not track later updates -- call
// GetSymbol again or subscribe for fresh data.
//
// The snapshot model buys lock-free field access and internal consistency (Parsed
// and Value captured together), at the cost of going stale after a concurrent
// reload. IsValid() is false for the zero value. Children() and ChildrenWalk()
// release the lock before calling the iterator, so any Session method is safe
// inside a walk.
type SymbolView struct {
	Name        string
	FullName    string
	DataType    string
	Comment     string
	Handle      uint32
	Group       uint32
	Offset      uint32
	Length      uint32
	BaseType    ADSDataType // protocol ADST_ code; see adsTypeToString
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

// BaseTypeName returns the IEC 61131-3 primitive underlying this symbol, for
// consumers that need the storage layout without tracking user-defined names.
// Resolved from the protocol ADST_ code, else the datatype table, else inferred
// from size -- inference is limited to 1- and 2-byte widths, since 4 and 8 are
// REAL/LREAL ambiguous.
//
// Returns "" when none resolve, in practice a 4- or 8-byte user type with no
// datatype table loaded. No symbol has "" legitimately, so callers branch on it
// directly. Warns once per symbol naming the remedy. Never fetches the table
// itself: a getter with no ctx and no error has no business doing I/O.
func (v SymbolView) BaseTypeName() string {
	// parse() switches on the declared name first, so it wins: TC3 stamps DT
	// with the ADST_ code for its storage type and still parses a timestamp.
	if slices.Contains(parseableTypes, v.DataType) {
		return v.DataType
	}
	if name := adsTypeToString(v.BaseType); name != "" {
		return name
	}
	if v.conn != nil {
		v.conn.cache.lock.Lock()
		dts := v.conn.cache.datatypes
		v.conn.cache.lock.Unlock()
		if dts != nil {
			if dt, ok := dts[v.DataType]; ok {
				if name := dt.DataType; name != "" {
					return name
				}
			}
		}
	}
	if inferred := inferBaseType(v.Length, v.BaseType); inferred != "" {
		return inferred
	}
	// Unresolvable, and until now silently so: a 4- or 8-byte user-defined type
	// (typically a DINT-backed enum) with no datatype table loaded returned "" with
	// nothing logged and nothing in the return to distinguish it from a symbol that
	// genuinely has no base type. Downstream that shipped as a quoted string and was
	// never repaired.
	//
	// Deliberately does NOT fetch the datatype table: a caller running with symbol
	// loading off has declined that upload, and this getter has no context to do I/O
	// with anyway. Say what is missing and what fixes it; the caller decides.
	v.warnUnresolvedBaseType()
	return ""
}

// warnUnresolvedBaseType reports an unresolvable base type once per symbol.
func (v SymbolView) warnUnresolvedBaseType() {
	if v.conn == nil {
		return
	}
	v.conn.warnUnresolvedBaseType(v.FullName)
}

// warnUnresolvedBaseType reports once per symbol that the base type needs the
// datatype table. Called from BaseTypeName and from the subscribe paths, where it
// is actionable before samples arrive; one latch serves both. The latch lives on
// the cached *symbol, since a SymbolView is a per-call copy and would latch
// nothing. Logged after the lock is released -- the handler is user-supplied.
func (sess *Session) warnUnresolvedBaseType(symbolName string) {
	sess.cache.lock.Lock()
	sym, ok := sess.cache.symbols[symbolKey(symbolName)]
	if !ok || sym.baseTypeWarned {
		sess.cache.lock.Unlock()
		return
	}
	// Re-derive resolvability under the lock: the datatype table may have been
	// loaded since whatever prompted this call.
	dataType, baseType, length := sym.DataType, sym.BaseType, sym.Length
	_, inTable := sess.cache.datatypes[dataType]
	sess.cache.lock.Unlock()

	if adsTypeToString(baseType) != "" || inTable || inferBaseType(length, baseType) != "" ||
		slices.Contains(parseableTypes, dataType) {
		return
	}

	sess.cache.lock.Lock()
	if sym.baseTypeWarned {
		sess.cache.lock.Unlock()
		return
	}
	sym.baseTypeWarned = true
	sess.cache.lock.Unlock()

	sess.logger.Warn("cannot resolve the base type of a user-defined type; no datatype table is loaded",
		"symbol", symbolName,
		"dataType", dataType,
		"size", length,
		"detail", "4- and 8-byte widths are ambiguous (DINT/REAL, LINT/LREAL share them), so they cannot be inferred from size",
		"hint", "call LoadSymbols (or LoadDataTypes) to load the datatype table; without it this symbol's value is delivered as an unconverted string")
}

// GetJSON serializes the cached value: nested JSON walked from the children
// subtree for composites, a single literal for primitives. Returns "" for a
// detached view or a symbol no longer in the cache. Takes cache.lock briefly;
// safe from any goroutine.
func (v SymbolView) GetJSON() string {
	if v.conn == nil {
		return ""
	}
	v.conn.cache.lock.Lock()
	sym := v.conn.cache.symbols[symbolKey(v.FullName)]
	if sym == nil {
		v.conn.cache.lock.Unlock()
		return ""
	}
	// parseTree walks Children + reads Value; both require cache.lock.
	// json.Marshal is the expensive part and operates on the produced
	// interface tree, which has no further cache dependencies — run it
	// outside the lock so notification handling + cache-backed APIs are
	// not blocked on large struct/array marshaling.
	data := sym.parseTree()
	name := sym.Name
	v.conn.cache.lock.Unlock()

	jsonData, err := json.Marshal(data)
	if err != nil {
		v.log().Warn("GetJSON marshal error", "symbol", name, "error", err)
		return ""
	}
	return string(jsonData)
}

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
	// Snapshot the subtree under cache.lock so the slice is internally
	// consistent, then release the lock BEFORE invoking the caller's fn —
	// fn may call other Session methods that take their own locks, and the
	// lock-ordering rule forbids holding cache.lock across user callbacks.
	var snapshots []SymbolView
	collectSubtree(root, v.conn, &snapshots)
	v.conn.cache.lock.Unlock()
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

func collectSubtree(s *symbol, conn *Session, out *[]SymbolView) {
	collectSubtreeDepth(s, conn, out, 0)
}

func collectSubtreeDepth(s *symbol, conn *Session, out *[]SymbolView, depth int) {
	if depth >= collectSubtreeMaxDepth {
		connLogger(conn).Warn("collectSubtree hit depth cap; possible Children cycle or malformed symbol tree",
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
func (s *symbol) view(conn *Session) SymbolView {
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

func parseUploadSymbolInfoSymbols(data []byte, datatypes map[string]SymbolUploadDataType, lg *slog.Logger) (symbols map[string]*symbol, err error) {
	symbols = map[string]*symbol{}
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
		symbol := addSymbol(item, datatypes, lg)

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

func addChildren(s *symbol, symbols map[string]*symbol) {
	for _, child := range s.Children {
		if _, ok := symbols[symbolKey(child.FullName)]; !ok {
			symbols[symbolKey(child.FullName)] = child
			addChildren(child, symbols)
		}
	}
}

func addSymbol(uploadSym symbolUploadSymbol, datatypes map[string]SymbolUploadDataType, lg *slog.Logger) *symbol {
	flags := SymbolFlag(uploadSym.SymbolEntry.Flags)
	sym := &symbol{
		Name:              uploadSym.Name,
		LastUpdateTime:    time.Now(),
		MinUpdateInterval: 50 * time.Millisecond,
		FullName:          uploadSym.Name,
		DataType:          uploadSym.DataType,
		Comment:           uploadSym.Comment,
		Length:            uploadSym.SymbolEntry.Size,
		BaseType:          ADSDataType(uploadSym.SymbolEntry.DataType),
		Group:             uploadSym.SymbolEntry.IGroup,
		Offset:            uploadSym.SymbolEntry.IOffs,
		Flags:             flags,
		ContextMask:       flags.ContextMask(),
	}

	dt, ok := datatypes[uploadSym.DataType]
	if ok {
		sym.Children = dt.addOffset(sym, datatypes, sym.Group, lg)
	}

	return sym
}

// addOffsetMaxDepth caps recursion depth in datatype tree expansion. Real
// PLC nesting is at most a few dozen levels; the cap defends against a
// malformed datatype table forming a self-cycle (forbidden by IEC 61131-3
// but not enforced over the wire).
const addOffsetMaxDepth = 256

func (data *SymbolUploadDataType) addOffset(parent *symbol, datatypes map[string]SymbolUploadDataType, group uint32, lg *slog.Logger) (children map[string]*symbol) {
	return data.addOffsetDepth(parent, datatypes, group, 0, lg)
}

func (data *SymbolUploadDataType) addOffsetDepth(parent *symbol, datatypes map[string]SymbolUploadDataType, group uint32, depth int, lg *slog.Logger) (children map[string]*symbol) {
	children = map[string]*symbol{}
	if depth >= addOffsetMaxDepth {
		logOr(lg).Warn("addOffset hit depth cap; possible datatype self-cycle in PLC response",
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

		child := symbol{
			// Children built here reach the cache through their parent, so the
			// stamping at ingest never sees them — and rebuildSymbolChildren runs
			// this path again when the datatype table arrives after the symbol list.
			// Without this their parse and serialise warnings fall back to the
			// package default logger, which is the bypass this change set removes.
			logger:            lg,
			Name:              segment.Name,
			LastUpdateTime:    time.Now(),
			MinUpdateInterval: 50 * time.Millisecond,
			FullName:          path,
			DataType:          segment.DataType,
			Comment:           segment.Comment,
			Length:            segment.DatatypeEntry.Size,
			// Left at ADSTVoid, a member resolved by guess instead: BOOL became
			// BYTE on TC3, SINT on TC2. Composites still report ADSTBigType.
			BaseType: ADSDataType(segment.DatatypeEntry.DataType),
			// Update with area and offset
			Group:  group,
			Offset: segment.DatatypeEntry.Offs,
			Parent: parent,
		}

		// Check if subitems exist — but skip enum types.
		// Enums have children (enum constants) but should be parsed as
		// their base type (e.g. INT), not expanded as struct fields.
		if dt, ok := datatypes[segment.DataType]; ok && !isEnumDataType(&dt) {
			child.Children = dt.addOffsetDepth(&child, datatypes, child.Group, depth+1, lg)
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

func parseUploadSymbolInfoDataTypes(data []byte, lg *slog.Logger) (datatypes map[string]SymbolUploadDataType, err error) {
	buff := bytes.NewBuffer(data)
	datatypes = make(map[string]SymbolUploadDataType)
	for buff.Len() > 0 {
		header, err := decodeSymbolUploadDataType(buff, "", lg)
		if err != nil {
			return nil, fmt.Errorf("parsing datatype entry: %w", err)
		}
		datatypes[header.Name] = header
	}
	return
}

func decodeSymbolUploadDataType(data *bytes.Buffer, parent string, lg *slog.Logger) (header SymbolUploadDataType, err error) {
	result := datatypeEntry{}
	header = SymbolUploadDataType{}

	totalSize := data.Len()

	if totalSize < 48 {
		err = fmt.Errorf("%s - wrong size < 48 bytes", parent)
		logOr(lg).Error("error during binary read", "error", err, hexAttr("data", data.Bytes()))
		return
	}

	err = binary.Read(data, binary.LittleEndian, &result)
	if err != nil {
		logOr(lg).Error("error during binary read", "error", err)
		return
	}
	name := make([]byte, result.NameLength)
	dt := make([]byte, result.TypeLength)
	comment := make([]byte, result.CommentLength)

	err = binary.Read(data, binary.LittleEndian, name)
	if err != nil {
		logOr(lg).Error("error during binary read", "error", err)
		return
	}
	data.Next(1)
	err = binary.Read(data, binary.LittleEndian, dt)
	if err != nil {
		logOr(lg).Error("error during binary read", "error", err)
		return
	}
	data.Next(1)
	err = binary.Read(data, binary.LittleEndian, comment)
	if err != nil {
		logOr(lg).Error("error during binary read", "error", err)
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
		header.Children = makeArrayChildren(arrayLevels, header.DataType, header.DatatypeEntry.Size, lg)
	} else {
		// Children is standard variables
		for j := 0; j < int(result.SubItems); j++ {
			child, err := decodeSymbolUploadDataType(buff, header.Name, lg)
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

func makeArrayChildren(levels []datatypeArrayInfo, dt string, size uint32, lg *slog.Logger) (children map[string]*SymbolUploadDataType) {
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
		logOr(lg).Error("makeArrayChildren: array Elements exceeds sanity cap, refusing to allocate",
			"declared_elements", level.Elements,
			"cap", maxArrayElementsPerLevel,
			"datatype", dt)
		return
	}
	if uint64(level.LBound)+uint64(level.Elements) > uint64(^uint32(0)) {
		logOr(lg).Error("makeArrayChildren: LBound + Elements overflows uint32, refusing",
			"lbound", level.LBound,
			"elements", level.Elements,
			"datatype", dt)
		return
	}
	// subChildren is shared across all array elements at this level.
	// This is intentional: children are read-only after symbol table construction,
	// and deep-copying would be expensive for large arrays (e.g., ARRAY[0..999]).
	// Recursion size shrinks by this level's element count so inner-dim
	// elements compute correct per-element byte size, not the outer total.
	subChildren := makeArrayChildren(levels[1:], dt, size/level.Elements, lg)

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

// getJSON returns the symbol's current value serialized as JSON.
// Internal API — public access via SymbolView.GetJSON.
func (s *symbol) getJSON() string {
	data := s.parseTree()
	jsonData, err := json.Marshal(data)
	if err != nil {
		s.log().Warn("getJSON marshal error", "symbol", s.Name, "error", err)
		return ""
	}
	return string(jsonData)
}

var stringsList = map[string]struct{}{"STRING": {}, "WSTRING": {}, "TIME": {}, "TOD": {}, "TIME_OF_DAY": {}, "DATE": {}, "DT": {}, "DATE_AND_TIME": {}}

// signedIntTypes / unsignedIntTypes / floatTypes are the IEC 61131-3 base
// type names that symbol_codec.parse writes into s.Value as decimal
// strings. parseTree dispatches on these so 64-bit integers (LINT/ULINT)
// retain full precision in the resulting JSON instead of being rounded
// through float64's 53-bit mantissa.
var (
	signedIntTypes = map[string]struct{}{
		"SINT": {}, "INT": {}, "DINT": {}, "LINT": {},
	}
	unsignedIntTypes = map[string]struct{}{
		"USINT": {}, "UINT": {}, "UDINT": {}, "ULINT": {},
		"BYTE": {}, "WORD": {}, "DWORD": {}, "LWORD": {},
	}
	floatTypes = map[string]struct{}{
		"REAL": {}, "LREAL": {},
	}
)

// parseTree returns a JSON-marshallable interface for the symbol subtree.
// Internal API — public access via SymbolView.GetJSON.
func (s *symbol) parseTree() (rData interface{}) {
	if len(s.Children) == 0 {
		switch {
		case s.DataType == "BOOL":
			v, err := strconv.ParseBool(s.Value)
			if err != nil {
				s.log().Warn("parseTree: invalid BOOL value, defaulting to false",
					"symbol", s.Name, "value", s.Value, "error", err)
			}
			rData = v
		case isInSet(s.DataType, stringsList):
			rData = s.Value
		case isInSet(s.DataType, signedIntTypes):
			v, err := strconv.ParseInt(s.Value, 10, 64)
			if err != nil {
				s.log().Warn("parseTree: invalid signed integer value, defaulting to 0",
					"symbol", s.Name, "dataType", s.DataType, "value", s.Value, "error", err)
			}
			rData = v
		case isInSet(s.DataType, unsignedIntTypes):
			v, err := strconv.ParseUint(s.Value, 10, 64)
			if err != nil {
				s.log().Warn("parseTree: invalid unsigned integer value, defaulting to 0",
					"symbol", s.Name, "dataType", s.DataType, "value", s.Value, "error", err)
			}
			rData = v
		case isInSet(s.DataType, floatTypes):
			v, err := strconv.ParseFloat(s.Value, 64)
			if err != nil {
				s.log().Warn("parseTree: invalid float value, defaulting to 0",
					"symbol", s.Name, "dataType", s.DataType, "value", s.Value, "error", err)
			}
			rData = v
		default:
			// Unknown / user-defined scalar (enum alias, TIME-derived,
			// pointer, REFERENCE TO, etc.). Fall back to float64 to
			// preserve prior behaviour for values that historically
			// parsed cleanly under ParseFloat.
			v, err := strconv.ParseFloat(s.Value, 64)
			if err != nil {
				s.log().Warn("parseTree: invalid numeric value, defaulting to 0",
					"symbol", s.Name, "dataType", s.DataType, "value", s.Value, "error", err)
			}
			rData = v
		}
	} else {
		localMap := make(map[string]interface{})
		for _, child := range s.Children {
			key := strings.ReplaceAll(child.Name, "[", `"[`)
			key = strings.ReplaceAll(key, "]", `]"`)
			localMap[key] = child.parseTree()
		}
		rData = localMap
	}
	return
}

func isInSet(s string, set map[string]struct{}) bool {
	_, ok := set[s]
	return ok
}

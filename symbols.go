package ads

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// symbolKey normalizes a symbol name for use as an internal map key.
// TwinCAT treats symbol names case-insensitively (IEC 61131-3).
// TC2 returns uppercase, TC3 preserves original casing — lowercasing
// ensures consistent lookups regardless of caller or PLC casing.
func symbolKey(name string) string { return strings.ToLower(name) }

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
	BaseType          uint32     // ADST_ numeric type code from protocol (e.g., ADSTReal32=4 for REAL)
	Flags             SymbolFlag
	ContextMask       uint8 // PLC task context (bits 8-11 of Flags); 0 = no task binding
	Changed           bool

	Value       string
	Valid       bool
	ValueParsed bool // true after first successful parse

	Notification chan<- *Update

	Parent   *Symbol
	Children map[string]*Symbol
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
		if len(item.DataType) >= 7 && item.DataType[:7] == "WSTRING" {
			item.DataType = "WSTRING"
		} else if len(item.DataType) >= 6 && item.DataType[:6] == "STRING" {
			item.DataType = "STRING"
		}
		item.Comment = string(comment)
		item.SymbolEntry = result
		endBuff := buff.Len()
		symbol := addSymbol(item, datatypes)

		symbols[symbolKey(item.Name)] = symbol
		addChildren(symbol, symbols)

		skip := int(item.SymbolEntry.EntryLength) - (begBuff - endBuff)
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
		sym.Children = dt.addOffset(sym, datatypes, sym.Group, sym.Offset)
	}

	return sym
}

func (data *SymbolUploadDataType) addOffset(parent *Symbol, datatypes map[string]SymbolUploadDataType, group uint32, offset uint32) (children map[string]*Symbol) {
	children = map[string]*Symbol{}

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
			child.Children = dt.addOffset(&child, datatypes, child.Group, child.Offset)
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

	if len(header.DataType) >= 7 && header.DataType[:7] == "WSTRING" {
		header.DataType = "WSTRING"
	} else if len(header.DataType) >= 6 && header.DataType[:6] == "STRING" {
		header.DataType = "STRING"
	}

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
// allocations (F-15). Real PLC arrays rarely exceed a few thousand elements;
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
	// F-15: defend against malformed/buggy PLC datatype responses.
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

// GetJSON (onlyChanged bool) string
func (symbol *Symbol) GetJSON(onlyChanged bool) string {
	data := symbol.parseSymbol(onlyChanged)
	jsonData, err := json.Marshal(data)
	if err != nil {
		getDefaultLogger().Warn("GetJSON marshal error", "symbol", symbol.Name, "error", err)
		return ""
	}
	return string(jsonData)
}

var stringsList = map[string]struct{}{"STRING": {}, "WSTRING": {}, "TIME": {}, "TOD": {}, "TIME_OF_DAY": {}, "DATE": {}, "DT": {}, "DATE_AND_TIME": {}}

// parseSymbol returns JSON interface for symbol
func (symbol *Symbol) parseSymbol(onlyChanged bool) (rData interface{}) {
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
			if !onlyChanged || child.Changed {
				localMap[s] = child.parseSymbol(onlyChanged)
			}
		}
		rData = localMap
	}
	return
}

package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"
)

// ListSymbols returns the full symbol table.
// Requires LoadSymbols() or LoadSymbolsSlow() to have been called first.
// Returns an error if full discovery has not been performed.
//
// ListSymbols returns read-only SymbolViews for every symbol discovered
// via LoadSymbols/LoadSymbolsSlow. Keys are PLC-cased FullNames.
func (conn *Connection) ListSymbols() (map[string]SymbolView, error) {
	conn.cache.lock.Lock()
	defer conn.cache.lock.Unlock()
	if !conn.cache.symbolsFullyLoaded {
		return nil, fmt.Errorf("full symbol discovery has not been run; call LoadSymbols() or LoadSymbolsSlow() first")
	}
	out := make(map[string]SymbolView, len(conn.cache.symbols))
	for _, v := range conn.cache.symbols {
		out[v.FullName] = v.view(conn)
	}
	return out, nil
}

// LoadSymbols performs full symbol and datatype discovery from the PLC.
// After calling this, ListSymbols() returns all symbols, and struct/array
// children are available. Write operations with type aliases also work.
// This downloads the entire symbol and datatype tables in single requests,
// which may cause real-time jitter on the PLC. For large programs, consider
// LoadSymbolsSlow() instead.
func (conn *Connection) LoadSymbols() error {
	err := conn.loadSymbols()
	if err != nil {
		return err
	}
	conn.cache.lock.Lock()
	conn.cache.symbolsFullyLoaded = true
	conn.cache.onDemandSymbols = map[string]bool{}
	conn.cache.lock.Unlock()
	return nil
}

// SlowDiscoveryConfig configures chunked symbol table download.
type SlowDiscoveryConfig struct {
	// ChunkSize is the number of bytes to download per request.
	// Default: 4096 bytes.
	ChunkSize uint32

	// ChunkDelay is the delay between chunk requests, giving the PLC
	// time to handle its real-time tasks. Default: 100ms.
	ChunkDelay time.Duration
}

// applyDefaults fills in zero-valued fields with sensible defaults.
func (cfg *SlowDiscoveryConfig) applyDefaults() {
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 4096
	}
	if cfg.ChunkDelay == 0 {
		cfg.ChunkDelay = 100 * time.Millisecond
	}
}

// LoadSymbolsSlow downloads the full symbol table in chunks with delays
// between each chunk, to minimize disruption to the PLC's real-time task.
// If the PLC does not support offset-based chunked reads, it falls back
// to downloading each table in a single request with a delay between them.
func (conn *Connection) LoadSymbolsSlow(cfg SlowDiscoveryConfig) error {
	cfg.applyDefaults()

	// Step 1: Read symbol version
	version, err := conn.GetSymbolVersion()
	if err != nil {
		conn.logger.Warn("failed to read symbol version during slow discovery", "error", err)
	} else {
		conn.cache.lock.Lock()
		conn.cache.symbolVersion = version
		conn.cache.lock.Unlock()
	}

	// Step 2: Get upload info (small request)
	uploadInfo, err := conn.GetSymbolUploadInfo()
	if err != nil {
		return fmt.Errorf("failed to get symbol upload info: %w", err)
	}

	time.Sleep(cfg.ChunkDelay)

	// Step 3: Download datatypes in chunks
	datatypesData, err := conn.downloadInChunks(
		uint32(GroupSymbolDataTypeUpload),
		uploadInfo.DataTypeLength,
		cfg.ChunkSize,
		cfg.ChunkDelay,
	)
	if err != nil {
		// Fallback: download datatypes in one request
		conn.logger.Info("chunked datatype download failed, falling back to single request", "error", err)
		datatypesData, err = conn.getUploadSymbolInfoDataTypes(uploadInfo.DataTypeLength)
		if err != nil {
			return fmt.Errorf("failed to download datatypes: %w", err)
		}
	}
	datatypes, err := parseUploadSymbolInfoDataTypes(datatypesData)
	if err != nil {
		return fmt.Errorf("failed to parse datatypes: %w", err)
	}

	time.Sleep(cfg.ChunkDelay)

	// Step 4: Download symbols in chunks
	symbolsData, err := conn.downloadInChunks(
		uint32(GroupSymbolUpload),
		uploadInfo.SymbolLength,
		cfg.ChunkSize,
		cfg.ChunkDelay,
	)
	if err != nil {
		// Fallback: download symbols in one request
		conn.logger.Info("chunked symbol download failed, falling back to single request", "error", err)
		symbolsData, err = conn.getUploadSymbolInfoSymbols(uploadInfo.SymbolLength)
		if err != nil {
			return fmt.Errorf("failed to download symbols: %w", err)
		}
	}
	symbols, err := parseUploadSymbolInfoSymbols(symbolsData, datatypes)
	if err != nil {
		return fmt.Errorf("failed to parse symbols: %w", err)
	}

	// Step 5: Store results
	conn.cache.lock.Lock()
	conn.cache.datatypes = datatypes
	conn.cache.symbols = symbols
	conn.cache.symbolsFullyLoaded = true
	conn.cache.onDemandSymbols = map[string]bool{}
	conn.cache.generation.Inc()
	conn.cache.lock.Unlock()

	conn.logger.Info("slow symbol discovery complete",
		"symbolCount", uploadInfo.SymbolCount,
		"datatypeCount", uploadInfo.DataTypeCount)

	return nil
}

// downloadInChunks reads a large ADS data blob in smaller pieces using
// the offset parameter of the ADS Read command.
// If chunked downloads are already known to be unsupported (e.g. TwinCAT 2),
// this returns an error immediately so the caller can use the single-request fallback.
func (conn *Connection) downloadInChunks(group uint32, totalLength uint32, chunkSize uint32, delay time.Duration) ([]byte, error) {
	if totalLength == 0 {
		return []byte{}, nil
	}

	// Skip if already known unsupported
	if conn.capabilities.ChunkedDownloadCheckedLoad() && !conn.capabilities.ChunkedDownloadSupportedLoad() {
		return nil, fmt.Errorf("chunked download not supported by this PLC")
	}

	const maxDownloadSize = 64 * 1024 * 1024 // 64 MB sanity limit
	if totalLength > maxDownloadSize {
		return nil, fmt.Errorf("download size %d exceeds sanity limit of %d bytes", totalLength, maxDownloadSize)
	}

	result := make([]byte, 0, totalLength)
	var offset uint32

	for offset < totalLength {
		remaining := totalLength - offset
		readLen := chunkSize
		if remaining < readLen {
			readLen = remaining
		}

		chunk, err := conn.Read(group, offset, readLen)
		if err != nil {
			if !conn.capabilities.ChunkedDownloadCheckedLoad() {
				conn.capabilities.ChunkedDownloadSupportedStore(false)
				conn.capabilities.ChunkedDownloadCheckedStore(true)
			}
			return nil, fmt.Errorf("chunk read at offset %d failed: %w", offset, err)
		}
		if len(chunk) == 0 {
			return nil, fmt.Errorf("chunk read at offset %d returned empty response", offset)
		}

		result = append(result, chunk...)
		offset += uint32(len(chunk))

		if offset < totalLength && delay > 0 {
			time.Sleep(delay)
		}
	}

	if !conn.capabilities.ChunkedDownloadCheckedLoad() {
		conn.capabilities.ChunkedDownloadSupportedStore(true)
		conn.capabilities.ChunkedDownloadCheckedStore(true)
	}

	if uint32(len(result)) != totalLength {
		return nil, fmt.Errorf("downloaded %d bytes but expected %d", len(result), totalLength)
	}

	return result, nil
}

// GetSymbol returns a read-only SymbolView for the named symbol.
// Resolves on-demand if the symbol is not in the cache (single-symbol
// lookup against the PLC).
func (conn *Connection) GetSymbol(symbolName string) (SymbolView, error) {
	sym, err := conn.getSymbol(symbolName)
	if err != nil {
		return SymbolView{}, err
	}
	conn.cache.lock.Lock()
	defer conn.cache.lock.Unlock()
	return sym.view(conn), nil
}

// getSymbol returns the internal *Symbol for the named symbol. Used by
// in-package code paths that need direct access to mutable Symbol state
// (notifications, reads, writes). External callers should use GetSymbol.
func (conn *Connection) getSymbol(symbolName string) (*Symbol, error) {
	conn.cache.lock.Lock()
	localSymbol, ok := conn.cache.symbols[symbolKey(symbolName)]
	needHandle := ok && localSymbol.Handle == 0
	conn.cache.lock.Unlock()

	if ok {
		if needHandle {
			// Network I/O must happen outside the lock to avoid deadlock
			// with handleNotification which also acquires cache.lock.
			handle, err := conn.GetHandleByName(symbolName)
			if err != nil {
				return nil, err
			}
			conn.cache.lock.Lock()
			if localSymbol.Handle != 0 {
				// Another goroutine set the handle while we were waiting — release ours.
				conn.cache.lock.Unlock()
				handleBytes := make([]byte, 4)
				binary.LittleEndian.PutUint32(handleBytes, handle)
				if err := conn.Write(uint32(GroupSymbolReleaseHandle), 0, handleBytes); err != nil {
					conn.logger.Warn("failed to release duplicate symbol handle",
						"symbol", symbolName, "handle", handle, "error", err)
				}
			} else {
				localSymbol.Handle = handle
				conn.cache.lock.Unlock()
			}
		}
		conn.logger.Log(context.Background(), LevelTrace, "symbol got", "symbol", localSymbol)
		return localSymbol, nil
	}

	// On-demand resolution: query the PLC for this specific symbol
	sym, err := conn.getSymbolInfoByName(symbolName)
	if err != nil {
		return nil, fmt.Errorf("symbol %q not found and on-demand lookup failed: %w", symbolName, err)
	}

	handle, err := conn.GetHandleByName(symbolName)
	if err != nil {
		return nil, fmt.Errorf("failed to get handle for %q: %w", symbolName, err)
	}
	sym.Handle = handle

	conn.cache.lock.Lock()
	// Check if another goroutine resolved this symbol while we were waiting
	if existing, ok := conn.cache.symbols[symbolKey(symbolName)]; ok {
		conn.cache.lock.Unlock()
		// Release the handle we just acquired since another goroutine beat us
		handleBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(handleBytes, handle)
		if err := conn.Write(uint32(GroupSymbolReleaseHandle), 0, handleBytes); err != nil {
			conn.logger.Warn("failed to release duplicate symbol handle",
				"symbol", symbolName, "handle", handle, "error", err)
		}
		return existing, nil
	}
	conn.cache.symbols[symbolKey(symbolName)] = sym
	conn.cache.onDemandSymbols[symbolKey(symbolName)] = true
	conn.cache.lock.Unlock()

	conn.logger.Info("symbol resolved on-demand",
		"symbol", symbolName,
		"dataType", sym.DataType,
		"length", sym.Length)

	return sym, nil
}

// getSymbolInfoByName queries a single symbol's metadata from the PLC
// using GroupSymbolInfoByNameEx (0xF009).
// Returns a populated Symbol with Group, Offset, Length, DataType, etc.
// Does NOT populate Children (struct/array children require full discovery).
func (conn *Connection) getSymbolInfoByName(symbolName string) (*Symbol, error) {
	resp, err := conn.WriteRead(
		uint32(GroupSymbolInfoByNameEx),
		0,
		2048,
		append([]byte(symbolName), 0),
	)
	if err != nil {
		return nil, fmt.Errorf("GetSymbolInfoByName(%s) failed: %w", symbolName, err)
	}

	buff := bytes.NewBuffer(resp)
	entry := symbolEntry{}
	if err := binary.Read(buff, binary.LittleEndian, &entry); err != nil {
		return nil, fmt.Errorf("failed to parse symbol entry for %s: %w", symbolName, err)
	}

	// Read null-terminated strings: name, type, comment
	name := make([]byte, entry.NameLength)
	if err := binary.Read(buff, binary.LittleEndian, name); err != nil {
		return nil, fmt.Errorf("reading symbol name for %s: %w", symbolName, err)
	}
	buff.Next(1) // null terminator

	dt := make([]byte, entry.TypeLength)
	if err := binary.Read(buff, binary.LittleEndian, dt); err != nil {
		return nil, fmt.Errorf("reading symbol type for %s: %w", symbolName, err)
	}
	buff.Next(1) // null terminator

	comment := make([]byte, entry.CommentLength)
	if err := binary.Read(buff, binary.LittleEndian, comment); err != nil {
		return nil, fmt.Errorf("reading symbol comment for %s: %w", symbolName, err)
	}

	dataType := normalizeStringDataType(string(dt))

	flags := SymbolFlag(entry.Flags)
	sym := &Symbol{
		FullName:          string(name), // PLC-returned casing (authoritative)
		Name:              string(name),
		DataType:          dataType,
		Comment:           string(comment),
		Group:             entry.IGroup,
		Offset:            entry.IOffs,
		Length:            entry.Size,
		BaseType:          entry.DataType,
		Flags:             flags,
		ContextMask:       flags.ContextMask(),
		LastUpdateTime:    time.Now(),
		MinUpdateInterval: 50 * time.Millisecond,
	}
	return sym, nil
}

func (conn *Connection) GetHandleByName(symbolName string) (handle uint32, err error) {
	resp, err := conn.WriteRead(uint32(GroupSymbolHandleByName), 0, 4, append([]byte(symbolName), 0))
	if err != nil {
		return 0, fmt.Errorf("getting handle for %q: %w", symbolName, err)
	}
	if len(resp) < 4 {
		return 0, fmt.Errorf("getting handle for %q: response too short (%d bytes)", symbolName, len(resp))
	}
	handle = binary.LittleEndian.Uint32(resp)
	return handle, nil
}

func (conn *Connection) GetSymbolUploadInfo() (uploadInfo SymbolUploadInfo, err error) {
	// Try extended info (0xF00F) first, fall back to basic info (0xF00C)
	res, err := conn.Read(uint32(GroupSymbolUploadInfo2), 0, 24)
	if err != nil {
		conn.logger.Debug("GroupSymbolUploadInfo2 not supported, falling back to GroupSymbolUploadInfo", "error", err)
		res, err = conn.Read(uint32(GroupSymbolUploadInfo), 0, 16)
		if err != nil {
			return uploadInfo, fmt.Errorf("GetSymbolUploadInfo failed: %w", err)
		}
	}
	buff := bytes.NewBuffer(res)
	if err = binary.Read(buff, binary.LittleEndian, &uploadInfo.SymbolCount); err != nil {
		return uploadInfo, fmt.Errorf("reading SymbolCount: %w", err)
	}
	if err = binary.Read(buff, binary.LittleEndian, &uploadInfo.SymbolLength); err != nil {
		return uploadInfo, fmt.Errorf("reading SymbolLength: %w", err)
	}
	if err = binary.Read(buff, binary.LittleEndian, &uploadInfo.DataTypeCount); err != nil {
		return uploadInfo, fmt.Errorf("reading DataTypeCount: %w", err)
	}
	if err = binary.Read(buff, binary.LittleEndian, &uploadInfo.DataTypeLength); err != nil {
		return uploadInfo, fmt.Errorf("reading DataTypeLength: %w", err)
	}
	if buff.Len() >= 8 {
		if err = binary.Read(buff, binary.LittleEndian, &uploadInfo.ExtraCount); err != nil {
			return uploadInfo, fmt.Errorf("reading ExtraCount: %w", err)
		}
		if err = binary.Read(buff, binary.LittleEndian, &uploadInfo.ExtraLength); err != nil {
			return uploadInfo, fmt.Errorf("reading ExtraLength: %w", err)
		}
	}
	return
}

func (conn *Connection) getUploadSymbolInfoSymbols(length uint32) (data []byte, err error) {
	res, err := conn.Read(uint32(GroupSymbolUpload), 0, length)
	if err != nil {
		return nil, fmt.Errorf("getUploadSymbolInfoSymbols failed: %w", err)
	}
	return res, nil
}

func (conn *Connection) getUploadSymbolInfoDataTypes(length uint32) (data []byte, err error) {
	data, err = conn.Read(
		uint32(GroupSymbolDataTypeUpload),
		0x0,
		length)
	if err != nil {
		return nil, fmt.Errorf("error doing DT UPLOAD: %w", err)
	}
	return data, nil
}

// GetSymbolVersion reads the current symbol version from the PLC.
// The symbol version is a single byte (uint8) that increments on online-change or download.
func (conn *Connection) GetSymbolVersion() (uint8, error) {
	data, err := conn.Read(uint32(GroupSymbolVersion), 0, 1)
	if err != nil {
		return 0, fmt.Errorf("failed to read symbol version: %w", err)
	}
	if len(data) < 1 {
		return 0, fmt.Errorf("symbol version response too short: %d bytes", len(data))
	}
	return data[0], nil
}

// CheckSymbolVersion compares the current PLC symbol version against the stored version.
// Returns true if the version has changed.
func (conn *Connection) CheckSymbolVersion() (changed bool, err error) {
	version, err := conn.GetSymbolVersion()
	if err != nil {
		return false, err
	}
	conn.cache.lock.Lock()
	oldVersion := conn.cache.symbolVersion
	conn.cache.lock.Unlock()
	if version != oldVersion {
		conn.logger.Info("symbol version changed",
			"old", oldVersion,
			"new", version)
		return true, nil
	}
	return false, nil
}

// RefreshSymbols reloads the symbol table if the symbol version has changed.
// It releases old handles, reloads symbol/datatype tables, and re-acquires handles for active symbols.
func (conn *Connection) RefreshSymbols() error {
	changed, err := conn.CheckSymbolVersion()
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}

	// Collect handles under lock, then release without holding the lock
	// to avoid deadlock (conn.Write does network I/O and waits for response).
	conn.cache.lock.Lock()
	var handleList []uint32
	for _, symbol := range conn.cache.symbols {
		if symbol.Handle != 0 {
			handleList = append(handleList, symbol.Handle)
			symbol.Handle = 0
		}
	}
	conn.cache.lock.Unlock()

	for _, h := range handleList {
		handleBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(handleBytes, h)
		if err := conn.Write(uint32(GroupSymbolReleaseHandle), 0, handleBytes); err != nil {
			conn.logger.Warn("failed to release symbol handle", "error", err, "handle", h)
		}
	}

	// Reload symbols
	err = conn.loadSymbols()
	if err != nil {
		return fmt.Errorf("failed to refresh symbols: %w", err)
	}

	conn.cache.lock.Lock()
	v := conn.cache.symbolVersion
	conn.cache.lock.Unlock()
	conn.logger.Info("symbols refreshed", "version", v)
	return nil
}

// LoadSymbolList downloads only the symbol table (0xF00B) from the PLC in chunks.
// This is the smaller of the two tables and enables browsing top-level symbol names.
// After calling this, BrowseSymbols() can list root symbols and navigate by prefix.
// To also expand struct/array children, call LoadDataTypes() afterwards.
func (conn *Connection) LoadSymbolList(cfg SlowDiscoveryConfig) error {
	cfg.applyDefaults()

	// Read symbol version
	version, err := conn.GetSymbolVersion()
	if err != nil {
		conn.logger.Warn("failed to read symbol version during LoadSymbolList", "error", err)
	} else {
		conn.cache.lock.Lock()
		conn.cache.symbolVersion = version
		conn.cache.lock.Unlock()
	}

	// Get upload info
	uploadInfo, err := conn.GetSymbolUploadInfo()
	if err != nil {
		return fmt.Errorf("failed to get symbol upload info: %w", err)
	}

	time.Sleep(cfg.ChunkDelay)

	// Download symbols in chunks
	symbolsData, err := conn.downloadInChunks(
		uint32(GroupSymbolUpload),
		uploadInfo.SymbolLength,
		cfg.ChunkSize,
		cfg.ChunkDelay,
	)
	if err != nil {
		// Fallback: download symbols in one request
		conn.logger.Info("chunked symbol download failed, falling back to single request", "error", err)
		symbolsData, err = conn.getUploadSymbolInfoSymbols(uploadInfo.SymbolLength)
		if err != nil {
			return fmt.Errorf("failed to download symbols: %w", err)
		}
	}

	// Parse without datatypes — no child expansion
	symbols, err := parseUploadSymbolInfoSymbols(symbolsData, nil)
	if err != nil {
		return fmt.Errorf("failed to parse symbols: %w", err)
	}

	conn.cache.lock.Lock()
	conn.cache.symbols = symbols
	conn.cache.symbolListLoaded = true
	conn.cache.onDemandSymbols = map[string]bool{}

	// If datatypes were already loaded, retroactively expand children
	if conn.cache.datatypesLoaded && conn.cache.datatypes != nil {
		conn.rebuildSymbolChildrenLocked()
	}
	conn.cache.generation.Inc()
	conn.cache.lock.Unlock()

	conn.logger.Info("symbol list loaded (browse mode)",
		"symbolCount", uploadInfo.SymbolCount)

	return nil
}

// LoadDataTypes downloads only the datatype table (0xF00E) from the PLC in chunks.
// After calling this along with LoadSymbolList(), struct/array children can be
// browsed and expanded via BrowseSymbols().
func (conn *Connection) LoadDataTypes(cfg SlowDiscoveryConfig) error {
	cfg.applyDefaults()

	// Get upload info
	uploadInfo, err := conn.GetSymbolUploadInfo()
	if err != nil {
		return fmt.Errorf("failed to get symbol upload info: %w", err)
	}

	time.Sleep(cfg.ChunkDelay)

	// Download datatypes in chunks
	datatypesData, err := conn.downloadInChunks(
		uint32(GroupSymbolDataTypeUpload),
		uploadInfo.DataTypeLength,
		cfg.ChunkSize,
		cfg.ChunkDelay,
	)
	if err != nil {
		// Fallback: download datatypes in one request
		conn.logger.Info("chunked datatype download failed, falling back to single request", "error", err)
		datatypesData, err = conn.getUploadSymbolInfoDataTypes(uploadInfo.DataTypeLength)
		if err != nil {
			return fmt.Errorf("failed to download datatypes: %w", err)
		}
	}

	datatypes, err := parseUploadSymbolInfoDataTypes(datatypesData)
	if err != nil {
		return fmt.Errorf("failed to parse datatypes: %w", err)
	}

	conn.cache.lock.Lock()
	conn.cache.datatypes = datatypes
	conn.cache.datatypesLoaded = true

	// If symbols were already loaded, retroactively expand children
	if conn.cache.symbolListLoaded && conn.cache.symbols != nil {
		conn.rebuildSymbolChildrenLocked()
	}
	conn.cache.generation.Inc()
	conn.cache.lock.Unlock()

	conn.logger.Info("datatypes loaded",
		"datatypeCount", uploadInfo.DataTypeCount)

	return nil
}

// rebuildSymbolChildrenLocked rebuilds children for all symbols using the datatype table.
// Must be called with cache.lock held.
func (conn *Connection) rebuildSymbolChildrenLocked() {
	if conn.cache.symbols == nil || conn.cache.datatypes == nil {
		return
	}

	// Collect top-level symbol names (those without a dot, i.e., not children)
	// We rebuild from the original top-level symbols only
	topLevel := make(map[string]*Symbol)
	for name, sym := range conn.cache.symbols {
		topLevel[name] = sym
	}

	for _, sym := range topLevel {
		dt, ok := conn.cache.datatypes[sym.DataType]
		if ok {
			sym.Children = dt.addOffset(sym, conn.cache.datatypes, sym.Group)
			addChildren(sym, conn.cache.symbols)
		}
	}

	conn.logger.Info("symbol children rebuilt from datatypes", "symbols", len(conn.cache.symbols))
}

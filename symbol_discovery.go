package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// sleepCtx sleeps for d or returns early on ctx cancellation.
// Returns ctx.Err() on cancellation, nil otherwise. Non-positive d
// returns nil immediately without checking ctx (matches time.Sleep
// semantics).
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// isChunkedDownloadUnsupportedErr reports whether err from the first
// chunked-download Read indicates the PLC genuinely does not implement
// offset-based reads on the upload groups (TwinCAT 2 behaviour).
// Conservative: only ADS-level rejections that name the service / offset
// as the cause count. Transport, context, marshaling, and arbitrary
// device errors are deliberately excluded so a transient failure cannot
// poison the session-wide ChunkedDownloadSupported flag.
func isChunkedDownloadUnsupportedErr(err error) bool {
	var rc ReturnCode
	if !errors.As(err, &rc) {
		return false
	}
	switch rc {
	case ReturnCodeDeviceServiceNotSupported, // 0x701
		ReturnCodeDeviceInvalidOffset: // 0x703
		return true
	default:
		return false
	}
}

// ListSymbols returns the full symbol table.
// Requires LoadSymbols() or LoadSymbolsSlow() to have been called first.
// Returns an error if full discovery has not been performed.
//
// ListSymbols returns read-only SymbolViews for every symbol discovered
// via LoadSymbols/LoadSymbolsSlow. Keys are PLC-cased FullNames.
func (sess *Session) ListSymbols() (map[string]SymbolView, error) {
	sess.cache.lock.Lock()
	defer sess.cache.lock.Unlock()
	if !sess.cache.symbolsFullyLoaded {
		return nil, fmt.Errorf("full symbol discovery has not been run; call LoadSymbols() or LoadSymbolsSlow() first")
	}
	out := make(map[string]SymbolView, len(sess.cache.symbols))
	for _, v := range sess.cache.symbols {
		out[v.FullName] = v.view(sess)
	}
	return out, nil
}

// LoadSymbols performs full symbol and datatype discovery from the PLC.
// After calling this, ListSymbols() returns all symbols, and struct/array
// children are available. Write operations with type aliases also work.
// This downloads the entire symbol and datatype tables in single requests,
// which may cause real-time jitter on the PLC. For large programs, consider
// LoadSymbolsSlow() instead.
func (sess *Session) LoadSymbols(ctx context.Context) error {
	// Refuse outside RUN rather than produce a misleading failure: in CONFIG the
	// runtime port does not exist, so this cannot succeed, and the PLC's answer is
	// an AMS "port not found" rather than anything about symbols. Permits when no
	// state has been observed — see requireRunningRuntime.
	if err := sess.requireRunningRuntime("LoadSymbols"); err != nil {
		return err
	}
	err := sess.loadSymbols(ctx)
	if err != nil {
		return err
	}
	sess.cache.lock.Lock()
	sess.cache.symbolsFullyLoaded = true
	sess.cache.onDemandSymbols = map[string]bool{}
	sess.cache.lock.Unlock()
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
func (sess *Session) LoadSymbolsSlow(ctx context.Context, cfg SlowDiscoveryConfig) error {
	cfg.applyDefaults()

	// Step 1: Read symbol version
	version, err := sess.client.Load().GetSymbolVersion(ctx)
	if err != nil {
		sess.logger.Warn("failed to read symbol version during slow discovery", "error", err)
	} else {
		sess.cache.lock.Lock()
		sess.cache.symbolVersion = version
		sess.cache.lock.Unlock()
	}

	// Step 2: Get upload info (small request)
	uploadInfo, err := sess.client.Load().GetSymbolUploadInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbol upload info: %w", err)
	}

	if err := sleepCtx(ctx, cfg.ChunkDelay); err != nil {
		return err
	}

	// Step 3: Download datatypes in chunks
	datatypesData, err := sess.client.Load().DownloadInChunks(ctx,
		uint32(GroupSymbolDataTypeUpload),
		uploadInfo.DataTypeLength,
		cfg.ChunkSize,
		cfg.ChunkDelay,
	)
	if err != nil {
		// Fallback: download datatypes in one request
		sess.logger.Info("chunked datatype download failed, falling back to single request", "error", err)
		datatypesData, err = sess.client.Load().DownloadDataTypes(ctx, uploadInfo.DataTypeLength)
		if err != nil {
			return fmt.Errorf("failed to download datatypes: %w", err)
		}
	}
	datatypes, err := parseUploadSymbolInfoDataTypes(datatypesData)
	if err != nil {
		return fmt.Errorf("failed to parse datatypes: %w", err)
	}

	if err := sleepCtx(ctx, cfg.ChunkDelay); err != nil {
		return err
	}

	// Step 4: Download symbols in chunks
	symbolsData, err := sess.client.Load().DownloadInChunks(ctx,
		uint32(GroupSymbolUpload),
		uploadInfo.SymbolLength,
		cfg.ChunkSize,
		cfg.ChunkDelay,
	)
	if err != nil {
		// Fallback: download symbols in one request
		sess.logger.Info("chunked symbol download failed, falling back to single request", "error", err)
		symbolsData, err = sess.client.Load().DownloadSymbolList(ctx, uploadInfo.SymbolLength)
		if err != nil {
			return fmt.Errorf("failed to download symbols: %w", err)
		}
	}
	symbols, err := parseUploadSymbolInfoSymbols(symbolsData, datatypes)
	if err != nil {
		return fmt.Errorf("failed to parse symbols: %w", err)
	}

	// Step 5: Store results
	sess.cache.lock.Lock()
	sess.cache.datatypes = datatypes
	sess.cache.symbols = symbols
	sess.cache.symbolsFullyLoaded = true
	sess.cache.onDemandSymbols = map[string]bool{}
	sess.bumpEpoch()
	sess.cache.lock.Unlock()

	sess.logger.Info("slow symbol discovery complete",
		"symbolCount", uploadInfo.SymbolCount,
		"datatypeCount", uploadInfo.DataTypeCount)

	return nil
}

// downloadInChunks reads a large ADS data blob in smaller pieces using
// the offset parameter of the ADS Read command.
// If chunked downloads are already known to be unsupported (e.g. TwinCAT 2),
// this returns an error immediately so the caller can use the single-request fallback.
// DownloadInChunks reads totalLength bytes from group:0 in chunkSize-byte
// chunks with optional inter-chunk delay. Tracks chunked-download support
// in capabilities so subsequent calls short-circuit when the PLC does not
// support it. Used by the Slow / List / DataTypes loaders to fetch large
// upload tables without overwhelming the PLC's real-time loop.
func (c *Client) DownloadInChunks(ctx context.Context, group uint32, totalLength uint32, chunkSize uint32, delay time.Duration) ([]byte, error) {
	if totalLength == 0 {
		return []byte{}, nil
	}
	if c.capabilities.ChunkedDownloadCheckedLoad() && !c.capabilities.ChunkedDownloadSupportedLoad() {
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
		chunk, err := c.Read(ctx, group, offset, readLen)
		if err != nil {
			// Only flip the "chunked-download unsupported" capability when
			// the PLC's response unambiguously indicates this offset/service
			// combination is not implemented — and only on the very first
			// chunk attempt. Transient errors (transport closed, ctx
			// cancellation, timeouts, intermittent transport hiccups) must
			// NOT poison the session-wide capability flag, since the PLC
			// may support chunking just fine and a subsequent call would
			// then take the single-request fallback unnecessarily.
			if offset == 0 && !c.capabilities.ChunkedDownloadCheckedLoad() && isChunkedDownloadUnsupportedErr(err) {
				c.capabilities.ChunkedDownloadSupportedStore(false)
				c.capabilities.ChunkedDownloadCheckedStore(true)
			}
			return nil, fmt.Errorf("chunk read at offset %d failed: %w", offset, err)
		}
		if len(chunk) == 0 {
			return nil, fmt.Errorf("chunk read at offset %d returned empty response", offset)
		}
		result = append(result, chunk...)
		offset += uint32(len(chunk))
		if offset < totalLength && delay > 0 {
			if err := sleepCtx(ctx, delay); err != nil {
				return nil, err
			}
		}
	}
	if !c.capabilities.ChunkedDownloadCheckedLoad() {
		c.capabilities.ChunkedDownloadSupportedStore(true)
		c.capabilities.ChunkedDownloadCheckedStore(true)
	}
	if uint32(len(result)) != totalLength {
		return nil, fmt.Errorf("downloaded %d bytes but expected %d", len(result), totalLength)
	}
	return result, nil
}

// GetSymbol returns a read-only SymbolView for the named symbol.
// Resolves on-demand if the symbol is not in the cache (single-symbol
// lookup against the PLC).
func (sess *Session) GetSymbol(ctx context.Context, symbolName string) (SymbolView, error) {
	sym, err := sess.getSymbol(ctx, symbolName)
	if err != nil {
		return SymbolView{}, err
	}
	sess.cache.lock.Lock()
	defer sess.cache.lock.Unlock()
	return sym.view(sess), nil
}

// getSymbol returns the internal *symbol for the named symbol. Used by
// in-package code paths that need direct access to mutable symbol state
// (notifications, reads, writes). External callers should use GetSymbol.
func (sess *Session) getSymbol(ctx context.Context, symbolName string) (*symbol, error) {
	sess.cache.lock.Lock()
	localSymbol, ok := sess.cache.symbols[symbolKey(symbolName)]
	needHandle := ok && localSymbol.Handle == 0
	sess.cache.lock.Unlock()

	if ok {
		if needHandle {
			// Network I/O must happen outside the lock to avoid deadlock
			// with handleNotification which also acquires cache.lock.
			handle, err := sess.client.Load().GetHandleByName(ctx, symbolName)
			if err != nil {
				return nil, err
			}
			sess.cache.lock.Lock()
			// Re-check the cache map for our entry. LoadSymbols /
			// reloadSymbolsAndResubscribe can swap sess.cache.symbols
			// wholesale while GetHandleByName is in flight. If the swap
			// happened, localSymbol now points to a stranded *symbol —
			// writing the acquired handle into it would leak the PLC
			// handle (no live cache entry tracks it) and leave the live
			// entry with Handle=0.
			currentEntry, stillInMap := sess.cache.symbols[symbolKey(symbolName)]
			swapped := !stillInMap || currentEntry != localSymbol
			switch {
			case swapped:
				sess.cache.lock.Unlock()
				handleBytes := make([]byte, 4)
				binary.LittleEndian.PutUint32(handleBytes, handle)
				if err := sess.client.Load().Write(ctx, uint32(GroupSymbolReleaseHandle), 0, handleBytes); err != nil {
					sess.logger.Warn("failed to release orphan symbol handle after cache swap",
						"symbol", symbolName, "handle", handle, "error", err)
				}
				return nil, fmt.Errorf("symbol cache reloaded during handle acquisition for %q; retry", symbolName)
			case localSymbol.Handle != 0:
				// Another goroutine set the handle while we were waiting — release ours.
				sess.cache.lock.Unlock()
				handleBytes := make([]byte, 4)
				binary.LittleEndian.PutUint32(handleBytes, handle)
				if err := sess.client.Load().Write(ctx, uint32(GroupSymbolReleaseHandle), 0, handleBytes); err != nil {
					sess.logger.Warn("failed to release duplicate symbol handle",
						"symbol", symbolName, "handle", handle, "error", err)
				}
			default:
				localSymbol.Handle = handle
				sess.cache.lock.Unlock()
			}
		}
		sess.logger.Log(context.Background(), LevelTrace, "symbol got", "symbol", localSymbol)
		return localSymbol, nil
	}

	// On-demand resolution: query the PLC for this specific symbol
	sym, err := sess.client.Load().GetSymbolInfoByName(ctx, symbolName)
	if err != nil {
		return nil, fmt.Errorf("symbol %q not found and on-demand lookup failed: %w", symbolName, err)
	}

	handle, err := sess.client.Load().GetHandleByName(ctx, symbolName)
	if err != nil {
		return nil, fmt.Errorf("failed to get handle for %q: %w", symbolName, err)
	}
	sym.Handle = handle

	sess.cache.lock.Lock()
	// Check if another goroutine resolved this symbol while we were waiting
	if existing, ok := sess.cache.symbols[symbolKey(symbolName)]; ok {
		sess.cache.lock.Unlock()
		// Release the handle we just acquired since another goroutine beat us
		handleBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(handleBytes, handle)
		if err := sess.client.Load().Write(ctx, uint32(GroupSymbolReleaseHandle), 0, handleBytes); err != nil {
			sess.logger.Warn("failed to release duplicate symbol handle",
				"symbol", symbolName, "handle", handle, "error", err)
		}
		return existing, nil
	}
	sess.cache.symbols[symbolKey(symbolName)] = sym
	sess.cache.onDemandSymbols[symbolKey(symbolName)] = true
	sess.cache.lock.Unlock()

	sess.logger.Info("symbol resolved on-demand",
		"symbol", symbolName,
		"dataType", sym.DataType,
		"length", sym.Length)

	return sym, nil
}

// getSymbolInfoByName queries a single symbol's metadata from the PLC
// using GroupSymbolInfoByNameEx (0xF009).
// GetSymbolInfoByName resolves a single symbol on the PLC and returns a
// populated symbol with Group, Offset, Length, DataType, etc. Does NOT
// populate Children (struct / array children require full discovery via
// LoadSymbols / LoadSymbolList + LoadDataTypes). This is a raw RPC and bypasses the symbol cache; the caller is responsible for decoding the response.
func (c *Client) GetSymbolInfoByName(ctx context.Context, symbolName string) (*symbol, error) {
	resp, err := c.WriteRead(
		ctx,
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
	return &symbol{
		FullName:          string(name), // PLC-returned casing (authoritative)
		Name:              string(name),
		DataType:          dataType,
		Comment:           string(comment),
		Group:             entry.IGroup,
		Offset:            entry.IOffs,
		Length:            entry.Size,
		BaseType:          ADSDataType(entry.DataType),
		Flags:             flags,
		ContextMask:       flags.ContextMask(),
		LastUpdateTime:    time.Now(),
		MinUpdateInterval: 50 * time.Millisecond,
	}, nil
}

// GetHandleByName resolves a symbol name to its PLC-side handle. Wire RPC;
// no cache. The handle is valid until the TCP transport drops or until
// the caller releases it via ReleaseHandle.
func (c *Client) GetHandleByName(ctx context.Context, symbolName string) (uint32, error) {
	resp, err := c.WriteRead(ctx, uint32(GroupSymbolHandleByName), 0, 4, append([]byte(symbolName), 0))
	if err != nil {
		return 0, fmt.Errorf("getting handle for %q: %w", symbolName, err)
	}
	if len(resp) < 4 {
		return 0, fmt.Errorf("getting handle for %q: response too short (%d bytes)", symbolName, len(resp))
	}
	return binary.LittleEndian.Uint32(resp), nil
}

// GetSymbolUploadInfo reads the symbol-table size header from the PLC.
// Tries extended info (0xF00F, 24 bytes) first; falls back to basic info
// (0xF00C, 16 bytes) for older PLCs. Used by LoadSymbols / LoadSymbolList /
// LoadDataTypes to size the subsequent download. This is a raw RPC and bypasses the symbol cache; the caller is responsible for decoding the response.
func (c *Client) GetSymbolUploadInfo(ctx context.Context) (SymbolUploadInfo, error) {
	var uploadInfo SymbolUploadInfo
	res, err := c.Read(ctx, uint32(GroupSymbolUploadInfo2), 0, 24)
	if err != nil {
		c.logger.Debug("GroupSymbolUploadInfo2 not supported, falling back to GroupSymbolUploadInfo", "error", err)
		res, err = c.Read(ctx, uint32(GroupSymbolUploadInfo), 0, 16)
		if err != nil {
			return uploadInfo, fmt.Errorf("GetSymbolUploadInfo failed: %w", err)
		}
	}
	buff := bytes.NewBuffer(res)
	if err := binary.Read(buff, binary.LittleEndian, &uploadInfo.SymbolCount); err != nil {
		return uploadInfo, fmt.Errorf("reading SymbolCount: %w", err)
	}
	if err := binary.Read(buff, binary.LittleEndian, &uploadInfo.SymbolLength); err != nil {
		return uploadInfo, fmt.Errorf("reading SymbolLength: %w", err)
	}
	if err := binary.Read(buff, binary.LittleEndian, &uploadInfo.DataTypeCount); err != nil {
		return uploadInfo, fmt.Errorf("reading DataTypeCount: %w", err)
	}
	if err := binary.Read(buff, binary.LittleEndian, &uploadInfo.DataTypeLength); err != nil {
		return uploadInfo, fmt.Errorf("reading DataTypeLength: %w", err)
	}
	if buff.Len() >= 8 {
		if err := binary.Read(buff, binary.LittleEndian, &uploadInfo.ExtraCount); err != nil {
			return uploadInfo, fmt.Errorf("reading ExtraCount: %w", err)
		}
		if err := binary.Read(buff, binary.LittleEndian, &uploadInfo.ExtraLength); err != nil {
			return uploadInfo, fmt.Errorf("reading ExtraLength: %w", err)
		}
	}
	return uploadInfo, nil
}

// DownloadSymbolList downloads the raw symbol-table bytes (group 0xF00B).
// Caller decodes per parseUploadSymbolInfoSymbols. This is a raw RPC and bypasses the symbol cache; the caller is responsible for decoding the response.
func (c *Client) DownloadSymbolList(ctx context.Context, length uint32) ([]byte, error) {
	res, err := c.Read(ctx, uint32(GroupSymbolUpload), 0, length)
	if err != nil {
		return nil, fmt.Errorf("DownloadSymbolList failed: %w", err)
	}
	return res, nil
}

// DownloadDataTypes downloads the raw datatype-table bytes (group 0xF00E).
// Caller decodes via parseUploadSymbolInfoDataTypes. This is a raw RPC and bypasses the symbol cache; the caller is responsible for decoding the response.
func (c *Client) DownloadDataTypes(ctx context.Context, length uint32) ([]byte, error) {
	res, err := c.Read(ctx, uint32(GroupSymbolDataTypeUpload), 0x0, length)
	if err != nil {
		return nil, fmt.Errorf("DownloadDataTypes failed: %w", err)
	}
	return res, nil
}

// GetSymbolVersion reads the current PLC symbol version (single byte).
// Increments on online-change or download. This is a raw RPC and bypasses the symbol cache; the caller is responsible for decoding the response.
func (c *Client) GetSymbolVersion(ctx context.Context) (uint8, error) {
	data, err := c.Read(ctx, uint32(GroupSymbolVersion), 0, 1)
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
func (sess *Session) CheckSymbolVersion(ctx context.Context) (changed bool, err error) {
	version, err := sess.client.Load().GetSymbolVersion(ctx)
	if err != nil {
		return false, err
	}
	sess.cache.lock.Lock()
	oldVersion := sess.cache.symbolVersion
	sess.cache.lock.Unlock()
	if version != oldVersion {
		sess.logger.Info("symbol version changed",
			"old", oldVersion,
			"new", version)
		return true, nil
	}
	return false, nil
}

// RefreshSymbols reloads the symbol table if the symbol version has changed.
// It releases old handles, reloads symbol/datatype tables, and re-acquires handles for active symbols.
func (sess *Session) RefreshSymbols(ctx context.Context) error {
	changed, err := sess.CheckSymbolVersion(ctx)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}

	// Collect handles under lock, then release without holding the lock
	// to avoid deadlock (sess.Write does network I/O and waits for response).
	sess.cache.lock.Lock()
	var handleList []uint32
	for _, symbol := range sess.cache.symbols {
		if symbol.Handle != 0 {
			handleList = append(handleList, symbol.Handle)
			symbol.Handle = 0
		}
	}
	sess.cache.lock.Unlock()

	for _, h := range handleList {
		handleBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(handleBytes, h)
		if err := sess.client.Load().Write(ctx, uint32(GroupSymbolReleaseHandle), 0, handleBytes); err != nil {
			sess.logger.Warn("failed to release symbol handle", "error", err, "handle", h)
		}
	}

	// Reload symbols
	err = sess.loadSymbols(ctx)
	if err != nil {
		return fmt.Errorf("failed to refresh symbols: %w", err)
	}

	sess.cache.lock.Lock()
	v := sess.cache.symbolVersion
	sess.cache.lock.Unlock()
	sess.logger.Info("symbols refreshed", "version", v)
	return nil
}

// LoadSymbolList downloads only the symbol table (0xF00B) from the PLC in chunks.
// This is the smaller of the two tables and enables browsing top-level symbol names.
// After calling this, BrowseSymbols() can list root symbols and navigate by prefix.
// To also expand struct/array children, call LoadDataTypes() afterwards.
func (sess *Session) LoadSymbolList(ctx context.Context, cfg SlowDiscoveryConfig) error {
	cfg.applyDefaults()

	// Read symbol version
	version, err := sess.client.Load().GetSymbolVersion(ctx)
	if err != nil {
		sess.logger.Warn("failed to read symbol version during LoadSymbolList", "error", err)
	} else {
		sess.cache.lock.Lock()
		sess.cache.symbolVersion = version
		sess.cache.lock.Unlock()
	}

	// Get upload info
	uploadInfo, err := sess.client.Load().GetSymbolUploadInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbol upload info: %w", err)
	}

	time.Sleep(cfg.ChunkDelay)

	// Download symbols in chunks
	symbolsData, err := sess.client.Load().DownloadInChunks(ctx,
		uint32(GroupSymbolUpload),
		uploadInfo.SymbolLength,
		cfg.ChunkSize,
		cfg.ChunkDelay,
	)
	if err != nil {
		// Fallback: download symbols in one request
		sess.logger.Info("chunked symbol download failed, falling back to single request", "error", err)
		symbolsData, err = sess.client.Load().DownloadSymbolList(ctx, uploadInfo.SymbolLength)
		if err != nil {
			return fmt.Errorf("failed to download symbols: %w", err)
		}
	}

	// Parse without datatypes — no child expansion
	symbols, err := parseUploadSymbolInfoSymbols(symbolsData, nil)
	if err != nil {
		return fmt.Errorf("failed to parse symbols: %w", err)
	}

	sess.cache.lock.Lock()
	sess.cache.symbols = symbols
	sess.cache.symbolListLoaded = true
	sess.cache.onDemandSymbols = map[string]bool{}

	// If datatypes were already loaded, retroactively expand children
	if sess.cache.datatypesLoaded && sess.cache.datatypes != nil {
		sess.rebuildSymbolChildrenLocked()
	}
	sess.bumpEpoch()
	sess.cache.lock.Unlock()

	sess.logger.Info("symbol list loaded (browse mode)",
		"symbolCount", uploadInfo.SymbolCount)

	return nil
}

// LoadDataTypes downloads only the datatype table (0xF00E) from the PLC in chunks.
// After calling this along with LoadSymbolList(), struct/array children can be
// browsed and expanded via BrowseSymbols().
func (sess *Session) LoadDataTypes(ctx context.Context, cfg SlowDiscoveryConfig) error {
	cfg.applyDefaults()

	// Get upload info
	uploadInfo, err := sess.client.Load().GetSymbolUploadInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbol upload info: %w", err)
	}

	time.Sleep(cfg.ChunkDelay)

	// Download datatypes in chunks
	datatypesData, err := sess.client.Load().DownloadInChunks(ctx,
		uint32(GroupSymbolDataTypeUpload),
		uploadInfo.DataTypeLength,
		cfg.ChunkSize,
		cfg.ChunkDelay,
	)
	if err != nil {
		// Fallback: download datatypes in one request
		sess.logger.Info("chunked datatype download failed, falling back to single request", "error", err)
		datatypesData, err = sess.client.Load().DownloadDataTypes(ctx, uploadInfo.DataTypeLength)
		if err != nil {
			return fmt.Errorf("failed to download datatypes: %w", err)
		}
	}

	datatypes, err := parseUploadSymbolInfoDataTypes(datatypesData)
	if err != nil {
		return fmt.Errorf("failed to parse datatypes: %w", err)
	}

	sess.cache.lock.Lock()
	sess.cache.datatypes = datatypes
	sess.cache.datatypesLoaded = true

	// If symbols were already loaded, retroactively expand children
	if sess.cache.symbolListLoaded && sess.cache.symbols != nil {
		sess.rebuildSymbolChildrenLocked()
	}
	sess.bumpEpoch()
	sess.cache.lock.Unlock()

	sess.logger.Info("datatypes loaded",
		"datatypeCount", uploadInfo.DataTypeCount)

	return nil
}

// rebuildSymbolChildrenLocked rebuilds children for all symbols using the datatype table.
// Must be called with cache.lock held.
func (sess *Session) rebuildSymbolChildrenLocked() {
	if sess.cache.symbols == nil || sess.cache.datatypes == nil {
		return
	}

	// Collect top-level symbol names (those without a dot, i.e., not children)
	// We rebuild from the original top-level symbols only
	topLevel := make(map[string]*symbol)
	for name, sym := range sess.cache.symbols {
		topLevel[name] = sym
	}

	for _, sym := range topLevel {
		dt, ok := sess.cache.datatypes[sym.DataType]
		if ok {
			sym.Children = dt.addOffset(sym, sess.cache.datatypes, sym.Group)
			addChildren(sym, sess.cache.symbols)
		}
	}

	sess.logger.Info("symbol children rebuilt from datatypes", "symbols", len(sess.cache.symbols))
}

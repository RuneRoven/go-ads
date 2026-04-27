package ads

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"time"
)

// ListSymbols returns the full symbol table.
// Requires LoadSymbols() or LoadSymbolsSlow() to have been called first.
// Returns an error if full discovery has not been performed.
func (conn *Connection) ListSymbols() (map[string]*Symbol, error) {
	conn.symbolLock.Lock()
	defer conn.symbolLock.Unlock()
	if !conn.symbolsFullyLoaded {
		return nil, fmt.Errorf("full symbol discovery has not been run; call LoadSymbols() or LoadSymbolsSlow() first")
	}
	// Return a shallow copy to prevent callers from mutating the internal map
	copy := make(map[string]*Symbol, len(conn.symbols))
	for k, v := range conn.symbols {
		copy[k] = v
	}
	return copy, nil
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
	conn.symbolLock.Lock()
	conn.symbolsFullyLoaded = true
	conn.onDemandSymbols = map[string]bool{}
	conn.symbolLock.Unlock()
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

// LoadSymbolsSlow downloads the full symbol table in chunks with delays
// between each chunk, to minimize disruption to the PLC's real-time task.
// If the PLC does not support offset-based chunked reads, it falls back
// to downloading each table in a single request with a delay between them.
func (conn *Connection) LoadSymbolsSlow(cfg SlowDiscoveryConfig) error {
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 4096
	}
	if cfg.ChunkDelay == 0 {
		cfg.ChunkDelay = 100 * time.Millisecond
	}

	// Step 1: Read symbol version
	version, err := conn.GetSymbolVersion()
	if err != nil {
		conn.logger.Warn("failed to read symbol version during slow discovery", "error", err)
	} else {
		conn.symbolLock.Lock()
		conn.symbolVersion = version
		conn.symbolLock.Unlock()
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
		datatypesData, err = conn.GetUploadSymbolInfoDataTypes(uploadInfo.DataTypeLength)
		if err != nil {
			return fmt.Errorf("failed to download datatypes: %w", err)
		}
	}
	datatypes, err := ParseUploadSymbolInfoDataTypes(datatypesData)
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
		symbolsData, err = conn.GetUploadSymbolInfoSymbols(uploadInfo.SymbolLength)
		if err != nil {
			return fmt.Errorf("failed to download symbols: %w", err)
		}
	}
	symbols, err := ParseUploadSymbolInfoSymbols(symbolsData, datatypes)
	if err != nil {
		return fmt.Errorf("failed to parse symbols: %w", err)
	}

	// Step 5: Store results
	conn.symbolLock.Lock()
	conn.datatypes = datatypes
	conn.symbols = symbols
	conn.symbolsFullyLoaded = true
	conn.onDemandSymbols = map[string]bool{}
	conn.symbolLock.Unlock()

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
	if conn.chunkedDownloadChecked.Load() && !conn.chunkedDownloadSupported.Load() {
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
			if !conn.chunkedDownloadChecked.Load() {
				conn.chunkedDownloadSupported.Store(false)
				conn.chunkedDownloadChecked.Store(true)
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

	if !conn.chunkedDownloadChecked.Load() {
		conn.chunkedDownloadSupported.Store(true)
		conn.chunkedDownloadChecked.Store(true)
	}

	if uint32(len(result)) != totalLength {
		return nil, fmt.Errorf("downloaded %d bytes but expected %d", len(result), totalLength)
	}

	return result, nil
}

func (conn *Connection) GetSymbol(symbolName string) (*Symbol, error) {
	conn.symbolLock.Lock()
	localSymbol, ok := conn.symbols[symbolName]
	needHandle := ok && localSymbol.Handle == 0
	conn.symbolLock.Unlock()

	if ok {
		if needHandle {
			// Network I/O must happen outside the lock to avoid deadlock
			// with handleNotification which also acquires symbolLock.
			handle, err := conn.GetHandleByName(symbolName)
			if err != nil {
				return nil, err
			}
			conn.symbolLock.Lock()
			localSymbol.Handle = handle
			conn.symbolLock.Unlock()
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

	conn.symbolLock.Lock()
	// Check if another goroutine resolved this symbol while we were waiting
	if existing, ok := conn.symbols[symbolName]; ok {
		conn.symbolLock.Unlock()
		// Release the handle we just acquired since another goroutine beat us
		handleBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(handleBytes, handle)
		_ = conn.Write(uint32(GroupSymbolReleaseHandle), 0, handleBytes)
		return existing, nil
	}
	conn.symbols[symbolName] = sym
	conn.onDemandSymbols[symbolName] = true
	conn.symbolLock.Unlock()

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

	dataType := string(dt)
	if len(dataType) >= 6 && dataType[:6] == "STRING" {
		dataType = "STRING"
	}

	sym := &Symbol{
		FullName:          symbolName,
		Name:              string(name),
		DataType:          dataType,
		Comment:           string(comment),
		Group:             entry.IGroup,
		Offset:            entry.IOffs,
		Length:            entry.Size,
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

func (conn *Connection) WriteToSymbol(symbolName string, value string) error {
	symbol, err := conn.GetSymbol(symbolName)
	if err != nil {
		return fmt.Errorf("write to %q: %w", symbolName, err)
	}

	// Snapshot datatypes under lock, then serialize without holding the lock
	conn.symbolLock.Lock()
	datatypes := conn.datatypes
	handle := symbol.Handle
	conn.symbolLock.Unlock()

	data, err := symbol.writeToNode(value, 0, datatypes)
	if err != nil {
		return fmt.Errorf("write to %q: serialization failed: %w", symbolName, err)
	}

	// Network I/O without lock
	err = conn.Write(uint32(GroupSymbolValueByHandle), handle, data)
	if err != nil {
		return fmt.Errorf("write to %q: %w", symbolName, err)
	}

	// Invalidate cached value so the next ReadFromSymbol fetches fresh data
	conn.symbolLock.Lock()
	symbol.Value = ""
	symbol.ValueParsed = false
	conn.symbolLock.Unlock()

	conn.logger.Log(context.Background(), LevelTrace, "wrote to symbol",
		"symbol", symbolName,
		"value", value)
	return nil
}

func (conn *Connection) ReadFromSymbol(symbolName string) (string, error) {
	symbol, err := conn.GetSymbol(symbolName)
	if err != nil {
		return "", fmt.Errorf("read %q: %w", symbolName, err)
	}

	// Check cache under lock
	conn.symbolLock.Lock()
	now := time.Now()
	if now.Sub(symbol.LastUpdateTime) < symbol.MinUpdateInterval && symbol.Value != "" {
		cached := symbol.Value
		conn.symbolLock.Unlock()
		return cached, nil
	}
	handle := symbol.Handle
	length := symbol.Length
	datatypes := conn.datatypes
	conn.symbolLock.Unlock()

	// Network I/O without lock
	data, err := conn.Read(uint32(GroupSymbolValueByHandle), handle, length)
	if err != nil {
		return "", fmt.Errorf("read %q: %w", symbolName, err)
	}

	// parse() mutates Symbol fields (Value, Changed, Valid, etc.) so it must
	// run under lock to avoid racing with handleNotification.
	conn.symbolLock.Lock()
	value, err := symbol.parse(data, 0, datatypes)
	if err != nil {
		conn.symbolLock.Unlock()
		return "", fmt.Errorf("read %q: parse failed: %w", symbolName, err)
	}
	symbol.LastUpdateTime = time.Now()
	symbol.Value = value
	conn.symbolLock.Unlock()

	conn.logger.Log(context.Background(), LevelTrace, "Read from symbol",
		"symbol", symbolName,
		"Value", value)

	return value, nil
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

func (conn *Connection) GetUploadSymbolInfoSymbols(length uint32) (data []byte, err error) {
	res, err := conn.Read(uint32(GroupSymbolUpload), 0, length)
	if err != nil {
		return nil, fmt.Errorf("GetUploadSymbolInfoSymbols failed: %w", err)
	}
	return res, nil
}

func (conn *Connection) GetUploadSymbolInfoDataTypes(length uint32) (data []byte, err error) {
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
func (conn *Connection) GetSymbolVersion() (uint32, error) {
	data, err := conn.Read(uint32(GroupSymbolVersion), 0, 4)
	if err != nil {
		return 0, fmt.Errorf("failed to read symbol version: %w", err)
	}
	if len(data) < 4 {
		return 0, fmt.Errorf("symbol version response too short: %d bytes", len(data))
	}
	return binary.LittleEndian.Uint32(data), nil
}

// CheckSymbolVersion compares the current PLC symbol version against the stored version.
// Returns true if the version has changed.
func (conn *Connection) CheckSymbolVersion() (changed bool, err error) {
	version, err := conn.GetSymbolVersion()
	if err != nil {
		return false, err
	}
	conn.symbolLock.Lock()
	oldVersion := conn.symbolVersion
	conn.symbolLock.Unlock()
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
	conn.symbolLock.Lock()
	var handleList []uint32
	for _, symbol := range conn.symbols {
		if symbol.Handle != 0 {
			handleList = append(handleList, symbol.Handle)
			symbol.Handle = 0
		}
	}
	conn.symbolLock.Unlock()

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

	conn.symbolLock.Lock()
	v := conn.symbolVersion
	conn.symbolLock.Unlock()
	conn.logger.Info("symbols refreshed", "version", v)
	return nil
}

// AddSymbolNotification registers a notification for a single symbol.
// Note: all notifications must share the same updateReceiver channel.
// On reconnect, the stored channel is used to re-subscribe all notifications.
// For multiple notifications, prefer AddSymbolNotifications.
func (conn *Connection) AddSymbolNotification(symbolName string, maxDelay int, cycleTime int, transMode TransMode, updateReceiver chan *Update) (uint32, error) {
	conn.symbolLock.Lock()
	if conn.notificationChannel != nil && conn.notificationChannel != updateReceiver {
		conn.symbolLock.Unlock()
		return 0, fmt.Errorf("all symbol notifications on a connection must use the same updateReceiver channel")
	}
	conn.symbolLock.Unlock()

	symbol, err := conn.GetSymbol(symbolName)
	if err != nil {
		return 0, fmt.Errorf("notification for %q: %w", symbolName, err)
	}
	handle, err := conn.AddDeviceNotification(
		uint32(GroupSymbolValueByHandle),
		symbol.Handle,
		symbol.Length,
		transMode,
		time.Duration(maxDelay)*time.Millisecond,
		time.Duration(cycleTime)*time.Millisecond)
	if err != nil {
		return 0, err
	}
	conn.logger.Info("notification created",
		"handle", handle,
		"symbol", symbolName)
	conn.symbolLock.Lock()
	defer conn.symbolLock.Unlock()
	symbol.Notification = updateReceiver
	conn.activeNotifications[handle] = symbol

	// Save config for reconnect re-subscribe
	conn.notificationConfigs = append(conn.notificationConfigs, NotificationConfig{
		SymbolName:       symbolName,
		MaxDelay:         maxDelay,
		CycleTime:        cycleTime,
		TransmissionMode: transMode,
	})
	conn.notificationChannel = updateReceiver

	return handle, nil
}

type Update struct {
	Variable  string
	Value     string
	TimeStamp time.Time
}

// NotificationConfig holds configuration for a symbol notification, used for batch add and reconnect re-subscribe.
type NotificationConfig struct {
	SymbolName       string
	MaxDelay         int
	CycleTime        int
	TransmissionMode TransMode
}

// symbolSumAddress returns the index group and offset to use for a symbol
// inside a sum command (batch read/write).
//
// It prefers handle-based addressing (ADSIGRP_SYM_VALBYHND / 0xF005) because
// direct group/offset addressing with process image groups (e.g. 0x4040) does
// not work inside sum read commands on some TwinCAT versions, even with correct
// absolute offsets.
//
// Falls back to direct group/offset with accumulated absolute offsets when no
// handle is available (e.g. before handle acquisition).
func symbolSumAddress(sym *Symbol) (group, offset uint32) {
	if sym.Handle != 0 {
		return uint32(GroupSymbolValueByHandle), sym.Handle
	}
	if sym.Group != 0 {
		absOffset := sym.Offset
		for p := sym.Parent; p != nil; p = p.Parent {
			absOffset += p.Offset
		}
		return sym.Group, absOffset
	}
	return uint32(GroupSymbolValueByHandle), sym.Handle
}

// ReadMultipleSymbols reads multiple symbols in a single ADS round-trip using SumRead.
// Returns a map of symbol name to parsed string value.
func (conn *Connection) ReadMultipleSymbols(names []string) (map[string]string, error) {
	if len(names) == 0 {
		return nil, nil
	}

	// Resolve symbols and build SumRead requests
	type symbolInfo struct {
		name   string
		symbol *Symbol
	}
	var infos []symbolInfo
	var requests []SumReadRequest

	for _, name := range names {
		symbol, err := conn.GetSymbol(name)
		if err != nil {
			conn.logger.Error("error getting symbol for batch read", "error", err, "symbol", name)
			continue
		}
		infos = append(infos, symbolInfo{name: name, symbol: symbol})
		group, offset := symbolSumAddress(symbol)
		requests = append(requests, SumReadRequest{Group: group, Offset: offset, Length: symbol.Length})
	}

	if len(requests) == 0 {
		return nil, fmt.Errorf("no valid symbols found for batch read")
	}

	results, err := conn.SumRead(requests)
	if err != nil {
		return nil, fmt.Errorf("batch read failed: %w", err)
	}

	values := make(map[string]string, len(results))
	conn.symbolLock.Lock()
	defer conn.symbolLock.Unlock()

	for i, result := range results {
		if result.Error != ReturnCodeNoErrors {
			conn.logger.Warn("symbol read error in batch",
				"symbol", infos[i].name,
				"errorCode", uint32(result.Error))
			continue
		}
		value, err := infos[i].symbol.parse(result.Data, 0, conn.datatypes)
		if err != nil {
			conn.logger.Error("error parsing symbol in batch read", "error", err, "symbol", infos[i].name)
			continue
		}
		now := time.Now()
		infos[i].symbol.LastUpdateTime = now
		infos[i].symbol.Value = value
		values[infos[i].name] = value
	}

	return values, nil
}

// WriteMultipleSymbols writes multiple symbols in a single ADS round-trip using SumWrite.
// Returns a map of symbol name to per-symbol error code.
// Uses direct iGroup/iOffs addressing when available (after LoadSymbols),
// falling back to handle-based addressing for on-demand symbols.
func (conn *Connection) WriteMultipleSymbols(values map[string]string) (map[string]ReturnCode, error) {
	if len(values) == 0 {
		return nil, nil
	}

	// Snapshot datatypes under lock
	conn.symbolLock.Lock()
	datatypes := conn.datatypes
	conn.symbolLock.Unlock()

	type symbolInfo struct {
		name   string
		symbol *Symbol
	}
	var infos []symbolInfo
	var requests []SumWriteRequest

	for name, value := range values {
		symbol, err := conn.GetSymbol(name)
		if err != nil {
			conn.logger.Error("error getting symbol for batch write", "error", err, "symbol", name)
			continue
		}

		data, err := symbol.writeToNode(value, 0, datatypes)
		if err != nil {
			conn.logger.Error("error serializing symbol for batch write", "error", err, "symbol", name)
			continue
		}

		group, offset := symbolSumAddress(symbol)
		req := SumWriteRequest{Group: group, Offset: offset, Data: data}

		infos = append(infos, symbolInfo{name: name, symbol: symbol})
		requests = append(requests, req)
	}

	if len(requests) == 0 {
		return nil, fmt.Errorf("no valid symbols found for batch write")
	}

	results, err := conn.SumWrite(requests)
	if err != nil {
		return nil, fmt.Errorf("batch write failed: %w", err)
	}

	codes := make(map[string]ReturnCode, len(results))
	conn.symbolLock.Lock()
	for i, result := range results {
		codes[infos[i].name] = result.Error
		// Invalidate cached value for successful writes so next read is fresh
		if result.Error == ReturnCodeNoErrors {
			infos[i].symbol.Value = ""
			infos[i].symbol.ValueParsed = false
		}
	}
	conn.symbolLock.Unlock()

	return codes, nil
}

// AddSymbolNotifications adds multiple symbol notifications in a single ADS round-trip using SumAddDeviceNotification.
func (conn *Connection) AddSymbolNotifications(configs []NotificationConfig, ch chan *Update) error {
	if len(configs) == 0 {
		return nil
	}

	// Resolve symbols and build requests
	type symbolInfo struct {
		config NotificationConfig
		symbol *Symbol
	}
	var infos []symbolInfo
	var requests []SumNotificationRequest

	for _, cfg := range configs {
		symbol, err := conn.GetSymbol(cfg.SymbolName)
		if err != nil {
			conn.logger.Error("error getting symbol for batch notification", "error", err, "symbol", cfg.SymbolName)
			continue
		}
		infos = append(infos, symbolInfo{config: cfg, symbol: symbol})
		requests = append(requests, SumNotificationRequest{
			Group:            uint32(GroupSymbolValueByHandle),
			Offset:           symbol.Handle,
			Length:           symbol.Length,
			TransmissionMode: cfg.TransmissionMode,
			MaxDelay:         time.Duration(cfg.MaxDelay) * time.Millisecond,
			CycleTime:        time.Duration(cfg.CycleTime) * time.Millisecond,
		})
	}

	if len(requests) == 0 {
		return fmt.Errorf("no valid symbols for batch notification add")
	}

	handles, errors, err := conn.SumAddDeviceNotification(requests)
	if err != nil {
		return fmt.Errorf("batch add notification failed: %w", err)
	}

	conn.symbolLock.Lock()
	defer conn.symbolLock.Unlock()

	for i, h := range handles {
		if errors[i] != ReturnCodeNoErrors {
			conn.logger.Error("error adding notification in batch",
				"symbol", infos[i].config.SymbolName,
				"errorCode", uint32(errors[i]))
			continue
		}
		infos[i].symbol.Notification = ch
		conn.activeNotifications[h] = infos[i].symbol
		conn.logger.Info("batch notification created",
			"handle", h,
			"symbol", infos[i].config.SymbolName)
	}

	// Store notification configs and channel for reconnect
	conn.notificationConfigs = append(conn.notificationConfigs, configs...)
	conn.notificationChannel = ch

	return nil
}

// SymbolBrowseEntry represents a browsable symbol or child.
type SymbolBrowseEntry struct {
	Name        string // short name (e.g., "motor")
	FullName    string // full path (e.g., "MAIN.motor")
	DataType    string // type name (e.g., "ST_Motor", "INT")
	Size        uint32
	HasChildren bool // true if struct/array (requires LoadDataTypes to expand)
	Comment     string
}

// LoadSymbolList downloads only the symbol table (0xF00B) from the PLC in chunks.
// This is the smaller of the two tables and enables browsing top-level symbol names.
// After calling this, BrowseSymbols() can list root symbols and navigate by prefix.
// To also expand struct/array children, call LoadDataTypes() afterwards.
func (conn *Connection) LoadSymbolList(cfg SlowDiscoveryConfig) error {
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 4096
	}
	if cfg.ChunkDelay == 0 {
		cfg.ChunkDelay = 100 * time.Millisecond
	}

	// Read symbol version
	version, err := conn.GetSymbolVersion()
	if err != nil {
		conn.logger.Warn("failed to read symbol version during LoadSymbolList", "error", err)
	} else {
		conn.symbolLock.Lock()
		conn.symbolVersion = version
		conn.symbolLock.Unlock()
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
		symbolsData, err = conn.GetUploadSymbolInfoSymbols(uploadInfo.SymbolLength)
		if err != nil {
			return fmt.Errorf("failed to download symbols: %w", err)
		}
	}

	// Parse without datatypes — no child expansion
	symbols, err := ParseUploadSymbolInfoSymbols(symbolsData, nil)
	if err != nil {
		return fmt.Errorf("failed to parse symbols: %w", err)
	}

	conn.symbolLock.Lock()
	conn.symbols = symbols
	conn.symbolListLoaded = true
	conn.onDemandSymbols = map[string]bool{}

	// If datatypes were already loaded, retroactively expand children
	if conn.datatypesLoaded && conn.datatypes != nil {
		conn.rebuildSymbolChildrenLocked()
	}
	conn.symbolLock.Unlock()

	conn.logger.Info("symbol list loaded (browse mode)",
		"symbolCount", uploadInfo.SymbolCount)

	return nil
}

// LoadDataTypes downloads only the datatype table (0xF00E) from the PLC in chunks.
// After calling this along with LoadSymbolList(), struct/array children can be
// browsed and expanded via BrowseSymbols().
func (conn *Connection) LoadDataTypes(cfg SlowDiscoveryConfig) error {
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 4096
	}
	if cfg.ChunkDelay == 0 {
		cfg.ChunkDelay = 100 * time.Millisecond
	}

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
		datatypesData, err = conn.GetUploadSymbolInfoDataTypes(uploadInfo.DataTypeLength)
		if err != nil {
			return fmt.Errorf("failed to download datatypes: %w", err)
		}
	}

	datatypes, err := ParseUploadSymbolInfoDataTypes(datatypesData)
	if err != nil {
		return fmt.Errorf("failed to parse datatypes: %w", err)
	}

	conn.symbolLock.Lock()
	conn.datatypes = datatypes
	conn.datatypesLoaded = true

	// If symbols were already loaded, retroactively expand children
	if conn.symbolListLoaded && conn.symbols != nil {
		conn.rebuildSymbolChildrenLocked()
	}
	conn.symbolLock.Unlock()

	conn.logger.Info("datatypes loaded",
		"datatypeCount", uploadInfo.DataTypeCount)

	return nil
}

// rebuildSymbolChildrenLocked rebuilds children for all symbols using the datatype table.
// Must be called with symbolLock held.
func (conn *Connection) rebuildSymbolChildrenLocked() {
	if conn.symbols == nil || conn.datatypes == nil {
		return
	}

	// Collect top-level symbol names (those without a dot, i.e., not children)
	// We rebuild from the original top-level symbols only
	topLevel := make(map[string]*Symbol)
	for name, sym := range conn.symbols {
		topLevel[name] = sym
	}

	for _, sym := range topLevel {
		dt, ok := conn.datatypes[sym.DataType]
		if ok {
			sym.Children = dt.addOffset(sym, conn.datatypes, sym.Group, sym.Offset)
			addChildren(sym, conn.symbols)
		}
	}

	conn.logger.Info("symbol children rebuilt from datatypes", "symbols", len(conn.symbols))
}

// BrowseSymbols returns browsable entries at the given path in the symbol hierarchy.
// If path is empty, returns root-level groupings (first path segments).
// If path is specified, returns children of that symbol or prefix.
// Requires LoadSymbolList() or LoadSymbols() to have been called first.
func (conn *Connection) BrowseSymbols(path string) ([]SymbolBrowseEntry, error) {
	conn.symbolLock.Lock()
	defer conn.symbolLock.Unlock()

	if !conn.symbolListLoaded && !conn.symbolsFullyLoaded {
		return nil, fmt.Errorf("symbol list not loaded; call LoadSymbolList() or LoadSymbols() first")
	}

	if path == "" {
		return conn.browseRoot(), nil
	}

	return conn.browseChildren(path), nil
}

// browseRoot returns unique root-level entries (first segment of each symbol name).
// Must be called with symbolLock held.
func (conn *Connection) browseRoot() []SymbolBrowseEntry {
	roots := make(map[string]bool)
	var entries []SymbolBrowseEntry

	for name, sym := range conn.symbols {
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
			if rootSym, ok := conn.symbols[root]; ok {
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
// Must be called with symbolLock held.
func (conn *Connection) browseChildren(path string) []SymbolBrowseEntry {
	// First: check if the exact symbol exists and has Children
	if sym, ok := conn.symbols[path]; ok && len(sym.Children) > 0 {
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
	prefix := path + "."
	seen := make(map[string]bool)
	var entries []SymbolBrowseEntry

	for name := range conn.symbols {
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

		if childSym, ok := conn.symbols[childFullName]; ok {
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
	if sym, ok := conn.symbols[path]; ok && len(entries) == 0 {
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
// Must be called with symbolLock held.
func (conn *Connection) symbolHasChildren(sym *Symbol) bool {
	// If we already have expanded children, yes
	if len(sym.Children) > 0 {
		return true
	}

	// If datatypes are loaded, check the datatype table
	if conn.datatypesLoaded && conn.datatypes != nil {
		if dt, ok := conn.datatypes[sym.DataType]; ok {
			return len(dt.Children) > 0
		}
	}

	// Heuristic: if the datatype is not a primitive parseable type, it's likely a struct
	if sym.DataType != "" && !slices.Contains(parseableTypes, sym.DataType) {
		// Exclude known non-struct types
		if sym.DataType != "TIME" && sym.DataType != "TOD" && sym.DataType != "DATE" && sym.DataType != "DT" {
			return true
		}
	}

	return false
}

package ads

import (
	"context"
	"fmt"
	"time"
)

func (conn *Session) WriteToSymbol(symbolName string, value string) error {
	return conn.writeToSymbolRetry(symbolName, value, 1)
}

func (conn *Session) writeToSymbolRetry(symbolName string, value string, retriesLeft int) error {
	gen := conn.epoch()

	symbol, err := conn.getSymbol(symbolName)
	if err != nil {
		return fmt.Errorf("write to %q: %w", symbolName, err)
	}

	// Snapshot datatypes and handle under lock, then serialize without holding the lock.
	// writeToNode reads symbol.Length and symbol.DataType internally — these fields are
	// set at construction and never mutated, so no lock is needed for them.
	conn.cache.lock.Lock()
	datatypes := conn.cache.datatypes
	handle := symbol.Handle
	conn.cache.lock.Unlock()

	data, err := symbol.writeToNode(value, datatypes)
	if err != nil {
		return fmt.Errorf("write to %q: serialization failed: %w", symbolName, err)
	}

	// Network I/O without lock
	err = conn.client.Write(uint32(GroupSymbolValueByHandle), handle, data)
	if err != nil {
		// If a reconnect happened during our operation, retry once with fresh handles
		if retriesLeft > 0 && conn.epoch() != gen {
			return conn.writeToSymbolRetry(symbolName, value, retriesLeft-1)
		}
		return fmt.Errorf("write to %q: %w", symbolName, err)
	}

	// Invalidate cached value so the next ReadFromSymbol fetches fresh data
	conn.cache.lock.Lock()
	symbol.Value = ""
	symbol.ValueParsed = false
	conn.cache.lock.Unlock()

	conn.logger.Log(context.Background(), LevelTrace, "wrote to symbol",
		"symbol", symbolName,
		"value", value)
	return nil
}

func (conn *Session) ReadFromSymbol(symbolName string) (string, error) {
	return conn.readFromSymbolRetry(symbolName, 1)
}

func (conn *Session) readFromSymbolRetry(symbolName string, retriesLeft int) (string, error) {
	gen := conn.epoch()

	symbol, err := conn.getSymbol(symbolName)
	if err != nil {
		return "", fmt.Errorf("read %q: %w", symbolName, err)
	}

	// Check cache under lock
	conn.cache.lock.Lock()
	now := time.Now()
	if now.Sub(symbol.LastUpdateTime) < symbol.MinUpdateInterval && symbol.Value != "" {
		cached := symbol.Value
		conn.cache.lock.Unlock()
		return cached, nil
	}
	handle := symbol.Handle
	length := symbol.Length
	datatypes := conn.cache.datatypes
	conn.cache.lock.Unlock()

	// Network I/O without lock
	data, err := conn.client.Read(uint32(GroupSymbolValueByHandle), handle, length)
	if err != nil {
		// If a reconnect happened during our operation, retry once with fresh handles
		if retriesLeft > 0 && conn.epoch() != gen {
			return conn.readFromSymbolRetry(symbolName, retriesLeft-1)
		}
		return "", fmt.Errorf("read %q: %w", symbolName, err)
	}

	// parse() mutates Symbol fields (Value, Valid, etc.) so it must
	// run under lock to avoid racing with handleNotification.
	conn.cache.lock.Lock()
	value, err := symbol.parse(data, 0, datatypes)
	if err != nil {
		conn.cache.lock.Unlock()
		return "", fmt.Errorf("read %q: parse failed: %w", symbolName, err)
	}
	symbol.LastUpdateTime = time.Now()
	symbol.Value = value
	conn.cache.lock.Unlock()

	conn.logger.Log(context.Background(), LevelTrace, "Read from symbol",
		"symbol", symbolName,
		"Value", value)

	return value, nil
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
	// Handle and Group both zero — shouldn't happen in normal operation.
	// Return handle-based addressing; PLC will return an error for handle 0.
	return uint32(GroupSymbolValueByHandle), sym.Handle
}

// ReadMultipleSymbols reads multiple symbols in a single ADS round-trip using SumRead.
// Returns a map of symbol name to parsed string value.
func (conn *Session) ReadMultipleSymbols(names []string) (map[string]string, error) {
	return conn.readMultipleSymbolsRetry(names, 1)
}

func (conn *Session) readMultipleSymbolsRetry(names []string, retriesLeft int) (map[string]string, error) {
	if len(names) == 0 {
		return nil, nil
	}

	gen := conn.epoch()

	// Resolve symbols and build SumRead requests
	type symbolInfo struct {
		name   string
		symbol *Symbol
	}
	var infos []symbolInfo
	var requests []SumReadRequest

	for _, name := range names {
		symbol, err := conn.getSymbol(name)
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

	results, err := conn.client.SumRead(requests)
	if err != nil {
		// If a reconnect happened during our operation, retry once with fresh handles
		if retriesLeft > 0 && conn.epoch() != gen {
			return conn.readMultipleSymbolsRetry(names, retriesLeft-1)
		}
		return nil, fmt.Errorf("batch read failed: %w", err)
	}

	values := make(map[string]string, len(results))
	conn.cache.lock.Lock()
	defer conn.cache.lock.Unlock()

	for i, result := range results {
		if result.Error != ReturnCodeNoErrors {
			conn.logger.Warn("symbol read error in batch",
				"symbol", infos[i].name,
				"errorCode", uint32(result.Error))
			continue
		}
		// Re-resolve via cache.symbols: infos[i].symbol may be stranded if
		// loadSymbols swapped the cache during the SumRead roundtrip. Parse
		// + Value mutation must target the live entry, otherwise
		// ReadFromSymbol returns "" while parse silently writes the orphan.
		live := conn.cache.symbols[symbolKey(infos[i].name)]
		if live == nil {
			conn.logger.Warn("batch read result for symbol no longer in cache; skipping",
				"symbol", infos[i].name)
			continue
		}
		value, err := live.parse(result.Data, 0, conn.cache.datatypes)
		if err != nil {
			conn.logger.Error("error parsing symbol in batch read", "error", err, "symbol", infos[i].name)
			continue
		}
		now := time.Now()
		live.LastUpdateTime = now
		live.Value = value
		values[infos[i].name] = value
	}

	return values, nil
}

// WriteMultipleSymbols writes multiple symbols in a single ADS round-trip using SumWrite.
// Returns a map of symbol name to per-symbol error code.
// Uses direct iGroup/iOffs addressing when available (after LoadSymbols),
// falling back to handle-based addressing for on-demand symbols.
func (conn *Session) WriteMultipleSymbols(values map[string]string) (map[string]ReturnCode, error) {
	return conn.writeMultipleSymbolsRetry(values, 1)
}

func (conn *Session) writeMultipleSymbolsRetry(values map[string]string, retriesLeft int) (map[string]ReturnCode, error) {
	if len(values) == 0 {
		return nil, nil
	}

	gen := conn.epoch()

	// Snapshot datatypes under lock
	conn.cache.lock.Lock()
	datatypes := conn.cache.datatypes
	conn.cache.lock.Unlock()

	type symbolInfo struct {
		name   string
		symbol *Symbol
	}
	var infos []symbolInfo
	var requests []SumWriteRequest

	for name, value := range values {
		symbol, err := conn.getSymbol(name)
		if err != nil {
			conn.logger.Error("error getting symbol for batch write", "error", err, "symbol", name)
			continue
		}

		data, err := symbol.writeToNode(value, datatypes)
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

	results, err := conn.client.SumWrite(requests)
	if err != nil {
		// If a reconnect happened during our operation, retry once with fresh handles
		if retriesLeft > 0 && conn.epoch() != gen {
			return conn.writeMultipleSymbolsRetry(values, retriesLeft-1)
		}
		return nil, fmt.Errorf("batch write failed: %w", err)
	}

	codes := make(map[string]ReturnCode, len(results))
	conn.cache.lock.Lock()
	for i, result := range results {
		codes[infos[i].name] = result.Error
		// Invalidate cached value for successful writes so next read is fresh.
		// Re-resolve via cache.symbols: infos[i].symbol may be stranded if
		// loadSymbols swapped during the SumWrite roundtrip; clearing the
		// orphan would leave the live entry showing stale Value.
		if result.Error == ReturnCodeNoErrors {
			if live := conn.cache.symbols[symbolKey(infos[i].name)]; live != nil {
				live.Value = ""
				live.ValueParsed = false
			}
		}
	}
	conn.cache.lock.Unlock()

	return codes, nil
}

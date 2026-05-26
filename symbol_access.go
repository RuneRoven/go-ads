package ads

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// WriteToSymbol writes a value to a PLC symbol by name (handle resolved
// on-demand and cached).
func (sess *Session) WriteToSymbol(ctx context.Context, symbolName string, value string) error {
	return sess.writeToSymbolRetry(ctx, symbolName, value, 1)
}

func (sess *Session) writeToSymbolRetry(ctx context.Context, symbolName string, value string, retriesLeft int) error {
	gen := sess.epoch()

	symbol, err := sess.getSymbol(ctx, symbolName)
	if err != nil {
		return fmt.Errorf("write to %q: %w", symbolName, err)
	}

	// Snapshot datatypes and handle under lock, then serialize without holding the lock.
	// writeToNode reads symbol.Length and symbol.DataType internally — these fields are
	// set at construction and never mutated, so no lock is needed for them.
	sess.cache.lock.Lock()
	datatypes := sess.cache.datatypes
	handle := symbol.Handle
	sess.cache.lock.Unlock()

	data, err := symbol.writeToNode(value, datatypes)
	if err != nil {
		return fmt.Errorf("write to %q: serialization failed: %w", symbolName, err)
	}

	// Network I/O without lock
	err = sess.client.Load().Write(ctx, uint32(GroupSymbolValueByHandle), handle, data)
	if err != nil {
		// Online-change detection (R-CACHE-009).
		var rc ReturnCode
		if errors.As(err, &rc) {
			sess.handleStaleDetection(rc)
		}
		// If a reconnect happened during our operation, retry once with fresh handles
		sess.waitForReconnect()
		if retriesLeft > 0 && sess.epoch() != gen {
			return sess.writeToSymbolRetry(ctx, symbolName, value, retriesLeft-1)
		}
		return fmt.Errorf("write to %q: %w", symbolName, err)
	}

	// Invalidate cached value so the next ReadFromSymbol fetches fresh data.
	// Re-resolve via cache.symbols: the symbol pointer captured at the top of
	// this function may be stranded if loadSymbols swapped the cache during
	// the Write roundtrip. writeMultipleSymbolsRetry uses the same pattern at
	// the per-item commit site; symmetric here.
	sess.cache.lock.Lock()
	if live := sess.cache.symbols[symbolKey(symbolName)]; live != nil {
		live.Value = ""
		live.ValueParsed = false
	}
	sess.cache.lock.Unlock()

	sess.logger.Log(context.Background(), LevelTrace, "wrote to symbol",
		"symbol", symbolName,
		"value", value)
	return nil
}

// ReadFromSymbol reads a PLC symbol by name and returns its stringified
// value (handle resolved on-demand and cached).
func (sess *Session) ReadFromSymbol(ctx context.Context, symbolName string) (string, error) {
	return sess.readFromSymbolRetry(ctx, symbolName, 1)
}

func (sess *Session) readFromSymbolRetry(ctx context.Context, symbolName string, retriesLeft int) (string, error) {
	gen := sess.epoch()

	symbol, err := sess.getSymbol(ctx, symbolName)
	if err != nil {
		return "", fmt.Errorf("read %q: %w", symbolName, err)
	}

	// Check cache under lock
	sess.cache.lock.Lock()
	now := time.Now()
	if now.Sub(symbol.LastUpdateTime) < symbol.MinUpdateInterval && symbol.Value != "" {
		cached := symbol.Value
		sess.cache.lock.Unlock()
		return cached, nil
	}
	handle := symbol.Handle
	length := symbol.Length
	datatypes := sess.cache.datatypes
	sess.cache.lock.Unlock()

	// Network I/O without lock
	data, err := sess.client.Load().Read(ctx, uint32(GroupSymbolValueByHandle), handle, length)
	if err != nil {
		// Online-change detection (R-CACHE-009).
		var rc ReturnCode
		if errors.As(err, &rc) {
			sess.handleStaleDetection(rc)
		}
		// If a reconnect happened during our operation, retry once with fresh handles
		sess.waitForReconnect()
		if retriesLeft > 0 && sess.epoch() != gen {
			return sess.readFromSymbolRetry(ctx, symbolName, retriesLeft-1)
		}
		return "", fmt.Errorf("read %q: %w", symbolName, err)
	}

	// R-CACHE-009 supplementary detection: the cached symbol.Length disagreeing
	// with the PLC-returned payload length indicates an online change (e.g.
	// operator toggled nProbeA INT↔LREAL). The PLC ships data of the new size,
	// but our cache still has the old size — parse() would fail with
	// "symbol.Length N exceeds data buffer size M" and never surface a
	// ReturnCode, so handleStaleDetection would be bypassed. Detect here
	// (Session-wrapper layer where sess is in scope) and dispatch the
	// configured strategy. Returns a ReturnCode-typed error so chained
	// errors.As on the caller side keeps matching.
	if data != nil && length > 0 && uint32(len(data)) != length {
		sess.handleStaleDetection(ReturnCodeDeviceInvalidSize)
		return "", fmt.Errorf("read %q: %w", symbolName, ReturnCodeDeviceInvalidSize)
	}

	// parse() mutates symbol fields (Value, Valid, etc.) so it must
	// run under lock to avoid racing with handleNotification.
	sess.cache.lock.Lock()
	value, err := symbol.parse(data, 0, datatypes)
	if err != nil {
		sess.cache.lock.Unlock()
		return "", fmt.Errorf("read %q: parse failed: %w", symbolName, err)
	}
	symbol.LastUpdateTime = time.Now()
	symbol.Value = value
	sess.cache.lock.Unlock()

	sess.logger.Log(context.Background(), LevelTrace, "Read from symbol",
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
//
// Caller MUST hold cache.lock: this reads sym.Handle which is written by
// zeroOldSymbolHandles + handle-resolve paths under the same lock.
func symbolSumAddress(sym *symbol) (group, offset uint32) {
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
func (sess *Session) ReadMultipleSymbols(ctx context.Context, names []string) (map[string]string, error) {
	return sess.readMultipleSymbolsRetry(ctx, names, 1)
}

func (sess *Session) readMultipleSymbolsRetry(ctx context.Context, names []string, retriesLeft int) (map[string]string, error) {
	if len(names) == 0 {
		return nil, nil
	}

	gen := sess.epoch()

	// Resolve symbols and build SumRead requests
	type symbolInfo struct {
		name   string
		symbol *symbol
	}
	var infos []symbolInfo
	var requests []SumReadRequest

	for _, name := range names {
		symbol, err := sess.getSymbol(ctx, name)
		if err != nil {
			sess.logger.Error("error getting symbol for batch read", "error", err, "symbol", name)
			continue
		}
		// Snapshot Handle under cache.lock — autoReload's zeroOldSymbolHandles
		// writes symbol.Handle under cache.lock, so racing reads here without
		// the lock would trip -race and could see an in-flight zero. Length is
		// write-once at construction; snapshot it together for symmetry.
		sess.cache.lock.Lock()
		group, offset := symbolSumAddress(symbol)
		length := symbol.Length
		sess.cache.lock.Unlock()
		infos = append(infos, symbolInfo{name: name, symbol: symbol})
		requests = append(requests, SumReadRequest{Group: group, Offset: offset, Length: length})
	}

	if len(requests) == 0 {
		return nil, fmt.Errorf("no valid symbols found for batch read")
	}

	results, err := sess.client.Load().SumRead(ctx, requests)
	if err != nil {
		// If a reconnect happened during our operation, retry once with fresh handles
		sess.waitForReconnect()
		if retriesLeft > 0 && sess.epoch() != gen {
			return sess.readMultipleSymbolsRetry(ctx, names, retriesLeft-1)
		}
		return nil, fmt.Errorf("batch read failed: %w", err)
	}

	// R-CACHE-009: fire online-change detection for first stale per-item code.
	// Once-per-batch semantics avoid callback amplification when N items in
	// the same response carry the same stale code (R-SES-011 "once per
	// detection").
	for _, r := range results {
		if r.Error == ReturnCodeNoErrors {
			continue
		}
		if stale, _ := detectStaleCache(r.Error); stale {
			sess.handleStaleDetection(r.Error)
			break
		}
	}

	values := make(map[string]string, len(results))
	sess.cache.lock.Lock()
	defer sess.cache.lock.Unlock()

	for i, result := range results {
		if result.Error != ReturnCodeNoErrors {
			sess.logger.Warn("symbol read error in batch",
				"symbol", infos[i].name,
				"errorCode", uint32(result.Error))
			continue
		}
		// Re-resolve via cache.symbols: infos[i].symbol may be stranded if
		// loadSymbols swapped the cache during the SumRead roundtrip. Parse
		// + Value mutation must target the live entry, otherwise
		// ReadFromSymbol returns "" while parse silently writes the orphan.
		live := sess.cache.symbols[symbolKey(infos[i].name)]
		if live == nil {
			sess.logger.Warn("batch read result for symbol no longer in cache; skipping",
				"symbol", infos[i].name)
			continue
		}
		value, err := live.parse(result.Data, 0, sess.cache.datatypes)
		if err != nil {
			sess.logger.Error("error parsing symbol in batch read", "error", err, "symbol", infos[i].name)
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
func (sess *Session) WriteMultipleSymbols(ctx context.Context, values map[string]string) (map[string]ReturnCode, error) {
	return sess.writeMultipleSymbolsRetry(ctx, values, 1)
}

func (sess *Session) writeMultipleSymbolsRetry(ctx context.Context, values map[string]string, retriesLeft int) (map[string]ReturnCode, error) {
	if len(values) == 0 {
		return nil, nil
	}

	gen := sess.epoch()

	// Snapshot datatypes under lock
	sess.cache.lock.Lock()
	datatypes := sess.cache.datatypes
	sess.cache.lock.Unlock()

	type symbolInfo struct {
		name   string
		symbol *symbol
	}
	var infos []symbolInfo
	var requests []SumWriteRequest

	for name, value := range values {
		symbol, err := sess.getSymbol(ctx, name)
		if err != nil {
			sess.logger.Error("error getting symbol for batch write", "error", err, "symbol", name)
			continue
		}

		data, err := symbol.writeToNode(value, datatypes)
		if err != nil {
			sess.logger.Error("error serializing symbol for batch write", "error", err, "symbol", name)
			continue
		}

		// Snapshot Handle under cache.lock — see readMultipleSymbolsRetry for rationale.
		sess.cache.lock.Lock()
		group, offset := symbolSumAddress(symbol)
		sess.cache.lock.Unlock()
		req := SumWriteRequest{Group: group, Offset: offset, Data: data}

		infos = append(infos, symbolInfo{name: name, symbol: symbol})
		requests = append(requests, req)
	}

	if len(requests) == 0 {
		return nil, fmt.Errorf("no valid symbols found for batch write")
	}

	results, err := sess.client.Load().SumWrite(ctx, requests)
	if err != nil {
		// If a reconnect happened during our operation, retry once with fresh handles
		sess.waitForReconnect()
		if retriesLeft > 0 && sess.epoch() != gen {
			return sess.writeMultipleSymbolsRetry(ctx, values, retriesLeft-1)
		}
		return nil, fmt.Errorf("batch write failed: %w", err)
	}

	// R-CACHE-009: fire online-change detection for first stale per-item code.
	// Once-per-batch semantics — see readMultipleSymbolsRetry for rationale.
	for _, r := range results {
		if r.Error == ReturnCodeNoErrors {
			continue
		}
		if stale, _ := detectStaleCache(r.Error); stale {
			sess.handleStaleDetection(r.Error)
			break
		}
	}

	codes := make(map[string]ReturnCode, len(results))
	sess.cache.lock.Lock()
	for i, result := range results {
		codes[infos[i].name] = result.Error
		// Invalidate cached value for successful writes so next read is fresh.
		// Re-resolve via cache.symbols: infos[i].symbol may be stranded if
		// loadSymbols swapped during the SumWrite roundtrip; clearing the
		// orphan would leave the live entry showing stale Value.
		if result.Error == ReturnCodeNoErrors {
			if live := sess.cache.symbols[symbolKey(infos[i].name)]; live != nil {
				live.Value = ""
				live.ValueParsed = false
			}
		}
	}
	sess.cache.lock.Unlock()

	return codes, nil
}

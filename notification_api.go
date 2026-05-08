package ads

import (
	"fmt"
	"strings"
	"time"
)

// Update is delivered to the user channel for each PLC notification sample.
type Update struct {
	Variable  string
	Value     string
	TimeStamp time.Time
}

// NotificationConfig holds configuration for a symbol notification, used for batch add and reconnect re-subscribe.
// MaxDelay and CycleTime use time.Duration for consistency with SumNotificationRequest
// and the rest of the standard library.
//
// resubscribeAttempts is incremented each time a reconnect re-subscribe round
// returns this config as Skipped (e.g. TOCTOU loss against a concurrent
// caller, cache stranded mid-batch). Above resubscribeMaxAttempts the
// library drops the config rather than retrying forever.
type NotificationConfig struct {
	SymbolName          string
	MaxDelay            time.Duration
	CycleTime           time.Duration
	TransmissionMode    TransMode
	resubscribeAttempts int // unexported; reset on successful re-subscribe
}

// resubscribeMaxAttempts caps Skipped-config retries across reconnect cycles
// to prevent infinite churn on persistently-flapping symbols. After the cap
// the config is dropped with a Warn log; the user must re-subscribe via
// AddSymbolNotification to re-establish.
const resubscribeMaxAttempts = 3

// AddSymbolNotification registers a notification for a single symbol.
// All notifications on one connection must share the same updateReceiver
// channel; subscribing the same symbol twice is rejected.
// On reconnect, the stored channel is used to re-subscribe all notifications.
// For multiple notifications, prefer AddSymbolNotifications.
//
// Channel ownership: the caller MUST NOT close updateReceiver while any
// notification is active on this connection. The library guards against
// accidental close with a recover (see deliverNotification), but a closed
// channel will silently drop notifications and emit Error logs. To stop
// receiving notifications, call DeleteDeviceNotification or Close() — these
// remove the PLC-side registration before the channel is no longer used.
func (conn *Session) AddSymbolNotification(symbolName string, maxDelay time.Duration, cycleTime time.Duration, transMode TransMode, updateReceiver chan *Update) (uint32, error) {
	// Pre-check: channel match + duplicate-subscribe.
	conn.notifs.lock.Lock()
	if conn.notifs.notificationChannel != nil && conn.notifs.notificationChannel != updateReceiver {
		conn.notifs.lock.Unlock()
		return 0, fmt.Errorf("all symbol notifications on a connection must use the same updateReceiver channel")
	}
	for _, cfg := range conn.notifs.notificationConfigs {
		if strings.EqualFold(cfg.SymbolName, symbolName) {
			conn.notifs.lock.Unlock()
			return 0, fmt.Errorf("symbol %q already has an active notification; delete it before re-subscribing", symbolName)
		}
	}
	conn.notifs.lock.Unlock()

	symbol, err := conn.getSymbol(symbolName)
	if err != nil {
		return 0, fmt.Errorf("notification for %q: %w", symbolName, err)
	}

	// Auto-fallback: InContext modes (5/6) require non-zero ContextMask on the symbol.
	// If ContextMask is 0 (single-task PLC, TC2, or variable not bound to a task),
	// downgrade to the regular mode (3/4) to avoid 0x070B errors or silent failures.
	actualMode := transMode
	if (transMode == TransModeServerCycle2 || transMode == TransModeServerOnChange2) && symbol.ContextMask == 0 {
		actualMode = downgradeTransMode(transMode)
		conn.logger.Warn("InContext mode not available for symbol (ContextMask=0), falling back",
			"symbol", symbolName,
			"requested", transMode.String(),
			"using", actualMode.String(),
			"flags", fmt.Sprintf("0x%04X", uint32(symbol.Flags)))
	}

	handle, err := conn.AddDeviceNotification(
		uint32(GroupSymbolValueByHandle),
		symbol.Handle,
		symbol.Length,
		actualMode,
		maxDelay,
		cycleTime)
	if err != nil {
		return 0, err
	}
	conn.logger.Info("notification created",
		"handle", handle,
		"symbol", symbolName,
		"mode", actualMode.String())
	// Re-fetch *Symbol from cache before commit. The pointer obtained
	// pre-roundtrip may be orphaned if loadSymbols swapped the cache during
	// the network call; using the stranded pointer would write notifications
	// into a Symbol no longer reachable via ReadFromSymbol. Take cache.lock
	// FIRST and release before taking notifs.lock (lock-ordering rule).
	// Capture cache.generation under the lock; re-check it under notifs.lock
	// (atomic Load is lock-free) to close the residual race window where a
	// THIRD loadSymbols could run between cache.lock release and notifs.lock
	// acquire and re-strand `fresh`.
	conn.cache.lock.Lock()
	fresh := conn.cache.symbols[symbolKey(symbolName)]
	cacheGen := conn.cache.generation.Load()
	conn.cache.lock.Unlock()
	if fresh == nil {
		if delErr := conn.DeleteDeviceNotification(handle); delErr != nil {
			conn.logger.Warn("failed to release PLC handle after symbol vanished",
				"handle", handle, "symbol", symbolName, "error", delErr)
		}
		return 0, fmt.Errorf("symbol %q removed from cache during subscribe (likely online change or LoadSymbols)", symbolName)
	}
	// Re-check under notifs.lock to defend the channel/duplicate invariants
	// against concurrent callers that passed the pre-check while we were
	// doing the PLC roundtrip. On mismatch, release the just-acquired PLC handle.
	conn.notifs.lock.Lock()
	if conn.cache.generation.Load() != cacheGen {
		conn.notifs.lock.Unlock()
		if delErr := conn.DeleteDeviceNotification(handle); delErr != nil {
			conn.logger.Warn("failed to release PLC handle after cache reload during subscribe",
				"handle", handle, "symbol", symbolName, "error", delErr)
		}
		return 0, fmt.Errorf("symbol %q stranded by concurrent cache reload during subscribe", symbolName)
	}
	if conn.notifs.notificationChannel != nil && conn.notifs.notificationChannel != updateReceiver {
		conn.notifs.lock.Unlock()
		if delErr := conn.DeleteDeviceNotification(handle); delErr != nil {
			conn.logger.Warn("failed to release PLC handle after channel-mismatch reject",
				"handle", handle, "symbol", symbolName, "error", delErr)
		}
		return 0, fmt.Errorf("all symbol notifications on a connection must use the same updateReceiver channel")
	}
	for _, cfg := range conn.notifs.notificationConfigs {
		if strings.EqualFold(cfg.SymbolName, symbolName) {
			conn.notifs.lock.Unlock()
			if delErr := conn.DeleteDeviceNotification(handle); delErr != nil {
				conn.logger.Warn("failed to release PLC handle after duplicate-subscribe reject",
					"handle", handle, "symbol", symbolName, "error", delErr)
			}
			return 0, fmt.Errorf("symbol %q already has an active notification; delete it before re-subscribing", symbolName)
		}
	}
	defer conn.notifs.lock.Unlock()
	fresh.Notification = updateReceiver
	conn.notifs.activeNotifications[handle] = fresh

	// Save config for reconnect re-subscribe
	conn.notifs.notificationConfigs = append(conn.notifs.notificationConfigs, NotificationConfig{
		SymbolName:       symbolName,
		MaxDelay:         maxDelay,
		CycleTime:        cycleTime,
		TransmissionMode: transMode,
	})
	conn.notifs.notificationChannel = updateReceiver
	// record subscribe time so handleNotification suppresses the
	// "unknown handle" Warn for any notifications arriving in the small
	// race window between PLC firing the first sample and the map insert
	// completing here.
	conn.notifs.lastSubscribeNs.Store(time.Now().UnixNano())

	return handle, nil
}

// AddSymbolNotifications adds multiple symbol notifications in a single ADS round-trip using SumAddDeviceNotification.
// Returns per-config results (parallel to configs) so callers can detect
// partial failure. A non-nil error indicates the batch could not be sent at
// all (transport failure); per-item state is still in the result slice.
//
// Per-result state:
//   - Skipped != nil: library refused to send (duplicate-subscribe, symbol
//     resolution failure, or transport-aborted batch). Error/Handle are not
//     meaningful.
//   - Skipped == nil && Error != ReturnCodeNoErrors: PLC accepted the batch
//     but rejected this item (e.g. invalid handle).
//   - Skipped == nil && Error == ReturnCodeNoErrors: success; Handle is valid.
//
// Channel ownership: the caller MUST NOT close ch while any notification is
// active on this connection. To stop receiving notifications, call
// DeleteDeviceNotification or Close() — these remove the PLC-side registration
// before the channel is no longer used.
func (conn *Session) AddSymbolNotifications(configs []NotificationConfig, ch chan *Update) ([]SumNotificationResult, error) {
	if len(configs) == 0 {
		return nil, nil
	}

	results := make([]SumNotificationResult, len(configs))

	// Snapshot already-subscribed symbol names so we can reject duplicates
	// pre-flight; the same check is repeated under the post-PLC lock to close
	// the TOCTOU window where a concurrent caller subscribed mid-roundtrip.
	conn.notifs.lock.Lock()
	if conn.notifs.notificationChannel != nil && conn.notifs.notificationChannel != ch {
		conn.notifs.lock.Unlock()
		return nil, fmt.Errorf("all symbol notifications on a connection must use the same updateReceiver channel")
	}
	existing := make(map[string]struct{}, len(conn.notifs.notificationConfigs))
	for _, cfg := range conn.notifs.notificationConfigs {
		existing[symbolKey(cfg.SymbolName)] = struct{}{}
	}
	conn.notifs.lock.Unlock()

	// Resolve symbols and build requests; track which result index maps to
	// which request slot so we can splice the sum response back.
	type symbolInfo struct {
		configIndex int // index into configs/results
		config      NotificationConfig
		symbol      *Symbol
	}
	var infos []symbolInfo
	var requests []SumNotificationRequest
	batchSeen := make(map[string]struct{}, len(configs))

	for i, cfg := range configs {
		key := symbolKey(cfg.SymbolName)
		if _, dup := existing[key]; dup {
			results[i].Skipped = fmt.Errorf("symbol %q already subscribed", cfg.SymbolName)
			conn.logger.Warn("duplicate notification rejected (already subscribed)", "symbol", cfg.SymbolName)
			continue
		}
		if _, dup := batchSeen[key]; dup {
			results[i].Skipped = fmt.Errorf("symbol %q duplicated within batch", cfg.SymbolName)
			conn.logger.Warn("duplicate notification rejected (within batch)", "symbol", cfg.SymbolName)
			continue
		}
		batchSeen[key] = struct{}{}

		symbol, err := conn.getSymbol(cfg.SymbolName)
		if err != nil {
			results[i].Skipped = fmt.Errorf("resolve symbol %q: %w", cfg.SymbolName, err)
			conn.logger.Error("error getting symbol for batch notification", "error", err, "symbol", cfg.SymbolName)
			continue
		}
		infos = append(infos, symbolInfo{configIndex: i, config: cfg, symbol: symbol})

		actualMode := cfg.TransmissionMode
		if (actualMode == TransModeServerCycle2 || actualMode == TransModeServerOnChange2) && symbol.ContextMask == 0 {
			actualMode = downgradeTransMode(actualMode)
			conn.logger.Warn("InContext mode not available for symbol (ContextMask=0), falling back",
				"symbol", cfg.SymbolName,
				"requested", cfg.TransmissionMode.String(),
				"using", actualMode.String(),
				"flags", fmt.Sprintf("0x%04X", uint32(symbol.Flags)))
		}

		requests = append(requests, SumNotificationRequest{
			Group:            uint32(GroupSymbolValueByHandle),
			Offset:           symbol.Handle,
			Length:           symbol.Length,
			TransmissionMode: actualMode,
			MaxDelay:         cfg.MaxDelay,
			CycleTime:        cfg.CycleTime,
		})
	}

	if len(requests) == 0 {
		return results, nil
	}

	subResults, err := conn.SumAddDeviceNotification(requests)
	if err != nil {
		// Transport-aborted batch: every entry that was about to be sent must
		// be marked Skipped so callers can distinguish "lib didn't try" from
		// "PLC rejected".
		for _, info := range infos {
			results[info.configIndex].Skipped = fmt.Errorf("batch transport failure: %w", err)
		}
		return results, fmt.Errorf("batch add notification failed: %w", err)
	}

	// Re-fetch *Symbol pointers under cache.lock before commit. Defends
	// against a concurrent loadSymbols / online-change reload that swapped
	// cache.symbols while we did the PLC roundtrip - the originally-resolved
	// pointer would be orphaned (Handle=0, Value=""), and later
	// handleNotification would parse into the stranded Symbol while
	// ReadFromSymbol would see a different fresh entry.
	// Capture cache.generation under the lock; re-check it under notifs.lock
	// to close the residual race where a third loadSymbols runs between
	// cache.lock release and notifs.lock acquire.
	freshSymbols := make([]*Symbol, len(infos))
	conn.cache.lock.Lock()
	for i, info := range infos {
		freshSymbols[i] = conn.cache.symbols[symbolKey(info.config.SymbolName)]
	}
	cacheGen := conn.cache.generation.Load()
	conn.cache.lock.Unlock()

	conn.notifs.lock.Lock()
	defer conn.notifs.lock.Unlock()

	// Generation re-check: if cache swapped between our cache.lock release
	// and now, every freshSymbols[] entry is potentially stranded - mark all
	// PLC-accepted entries as Skipped and surface handles for caller cleanup.
	if conn.cache.generation.Load() != cacheGen {
		for i, r := range subResults {
			info := infos[i]
			if r.Error == ReturnCodeNoErrors {
				results[info.configIndex].Skipped = fmt.Errorf("cache reload during batch subscribe stranded symbol %q", info.config.SymbolName)
				results[info.configIndex].Handle = r.Handle
				conn.logger.Warn("batch entry stranded by cache reload; surfacing PLC handle for caller cleanup",
					"symbol", info.config.SymbolName, "handle", r.Handle)
			} else {
				results[info.configIndex] = r
			}
		}
		return results, nil
	}

	// TOCTOU re-check: another goroutine may have subscribed one of our names
	// while we were doing the PLC roundtrip. Mark such items Skipped and
	// surface handle so the caller can release the PLC-side registration.
	postExisting := make(map[string]struct{}, len(conn.notifs.notificationConfigs))
	for _, cfg := range conn.notifs.notificationConfigs {
		postExisting[symbolKey(cfg.SymbolName)] = struct{}{}
	}

	successes := 0
	for i, r := range subResults {
		info := infos[i]
		key := symbolKey(info.config.SymbolName)
		if r.Error != ReturnCodeNoErrors {
			results[info.configIndex] = r
			conn.logger.Error("error adding notification in batch",
				"symbol", info.config.SymbolName,
				"errorCode", uint32(r.Error))
			continue
		}
		if _, dup := postExisting[key]; dup {
			// Concurrent subscribe won the race.
			results[info.configIndex].Skipped = fmt.Errorf("symbol %q subscribed concurrently during batch", info.config.SymbolName)
			results[info.configIndex].Handle = r.Handle
			conn.logger.Warn("batch entry lost TOCTOU race; surfacing PLC handle for caller cleanup",
				"symbol", info.config.SymbolName, "handle", r.Handle)
			continue
		}
		fresh := freshSymbols[i]
		if fresh == nil {
			// Symbol vanished from cache between resolve and commit.
			results[info.configIndex].Skipped = fmt.Errorf("symbol %q removed from cache during batch (likely online change or LoadSymbols)", info.config.SymbolName)
			results[info.configIndex].Handle = r.Handle
			conn.logger.Warn("batch entry symbol vanished mid-flight; surfacing PLC handle for caller cleanup",
				"symbol", info.config.SymbolName, "handle", r.Handle)
			continue
		}
		results[info.configIndex] = r
		fresh.Notification = ch
		conn.notifs.activeNotifications[r.Handle] = fresh
		// Reset resubscribe attempts on successful subscribe; if this entry
		// originated from a previous reconnect Skipped retry, the counter is
		// no longer relevant.
		commitCfg := info.config
		commitCfg.resubscribeAttempts = 0
		conn.notifs.notificationConfigs = append(conn.notifs.notificationConfigs, commitCfg)
		postExisting[key] = struct{}{}
		successes++
		conn.logger.Info("batch notification created",
			"handle", r.Handle,
			"symbol", info.config.SymbolName)
	}

	if successes > 0 {
		conn.notifs.notificationChannel = ch
		// record subscribe time so handleNotification suppresses the
		// "unknown handle" Warn for any notifications arriving in the small
		// race window between PLC firing the first sample and the map insert
		// completing here.
		conn.notifs.lastSubscribeNs.Store(time.Now().UnixNano())
	}

	return results, nil
}

// removeNotificationConfig removes the first config matching symbolName.
// Must be called with notifs.lock held.
func (conn *Session) removeNotificationConfig(symbolName string) {
	for i, cfg := range conn.notifs.notificationConfigs {
		if strings.EqualFold(cfg.SymbolName, symbolName) {
			conn.notifs.notificationConfigs = append(conn.notifs.notificationConfigs[:i], conn.notifs.notificationConfigs[i+1:]...)
			return
		}
	}
}

# Changelog

All notable changes to this project will be documented in this file.

This project uses [Conventional Commits](https://www.conventionalcommits.org/) and
[go-semantic-release](https://github.com/go-semantic-release/semantic-release) for
automated versioning and changelog generation.

## v2.2.0: context propagation, type safety, ergonomic constructor (breaking)

v2.2 bundles the v2.1 self-review findings into a single break: every RPC
method takes a `context.Context`, the constructor drops 7 positional
arguments for a typed `AMSEndpoint` plus options, several wire-protocol
values are now distinct types, and the public API surface shrinks (`Symbol`
is unexported, internal retry counters split out of `NotificationConfig`).

### Critical fixes (also worth backporting if you stay on v2.1.x)

- `Session.client` is now `atomic.Pointer[Client]`. `Connect` and
  `dialAndStart` publish the new Client after handlers + workers are wired,
  so concurrent RPC paths cannot observe a half-initialized Client.
- `makeArrayChildren` now passes `size / level.Elements` to the recursive
  inner-dimension call. Multi-dimensional fixed-width arrays
  (e.g. `ARRAY[0..3, 0..3] OF REAL`) compute per-element byte size
  correctly; previously the inner-most elements got the outer row size.
- `sumWriteFallback`, `sumAddNotificationFallback`,
  `sumDeleteNotificationFallback` now use `errors.As` to extract the
  underlying `ReturnCode` (matching `sumReadFallback`). On TC2 (always-
  fallback) under online change, `detectStaleCache` now sees the real
  0x711/0x705 codes and `SymbolVersionAutoReload` fires for writes and
  notifications, not just reads.
- `symbolSumAddress` callers (`ReadMultipleSymbols`, `WriteMultipleSymbols`)
  take `cache.lock` around the handle snapshot to close the read-vs-
  `zeroOldSymbolHandles` race surfaced by `-race`.

### Breaking changes

- **`context.Context` is now the first argument** on every RPC method.
  This includes the raw `Client` surface (`Read`, `Write`, `WriteRead`,
  `ReadState`, `ReadDeviceInfo`, `AddDeviceNotification`,
  `DeleteDeviceNotification`, `SumRead`, `SumWrite`,
  `SumAddDeviceNotification`, `SumDeleteDeviceNotification`,
  `GetHandleByName`, `GetSymbolInfoByName`, `GetSymbolVersion`,
  `GetSymbolUploadInfo`, `DownloadInChunks`, `DownloadDataTypes`,
  `DownloadSymbolList`, `ReleaseHandle`, all `ReadProcess*`,
  `WriteProcess*`, `ClearProcess*`) and the cache-aware `Session` surface
  (`ReadFromSymbol`, `WriteToSymbol`, `ReadMultipleSymbols`,
  `WriteMultipleSymbols`, `AddSymbolNotification`, `AddSymbolNotifications`,
  `DeleteDeviceNotification`, `SumDeleteDeviceNotification`, `LoadSymbols`,
  `LoadSymbolsSlow`, `LoadSymbolList`, `LoadDataTypes`, `GetSymbol`,
  `RefreshSymbols`, `CheckSymbolVersion`, `Reconnect`, `AddRoute`).
  The caller-supplied ctx is merged with the per-request timeout via
  `context.WithTimeout`; pass `context.Background()` to preserve the v2.1
  timeout-only semantic.
- **`NewSession` ergonomic refactor.** Old:
  `NewSession(ip, port, netid, amsPort, localNetID, localPort, requestTimeout, opts...)`.
  New: `NewSession(ctx, AMSEndpoint{IP, Port, AMS: AMSAddress{NetID, Port}}, opts...)`.
  Local NetID/port moved to `WithLocalAMS(AMSAddress{...})`. Request
  timeout moved to `WithRequestTimeout(...)`. Local-mode flag moved from
  `Connect(bool)` to `WithLocalMode()` option. `Connect` now takes a
  `context.Context` instead of the local-mode bool.
- **`Session.Close() error`** (was `Close()`). Implements `io.Closer`.
  Returns nil today; reserved for future failure modes (TCP close failure,
  PLC handle release failure).
- **`Symbol` is unexported.** Use `SymbolView` (already the recommended
  read-only handle returned by `GetSymbol` / `ListSymbols`). The internal
  `(*Symbol).GetJSON` and `parseSymbol` methods are unexported; the new
  `(SymbolView).GetJSON()` is the public entry point.
- **`Update.Stale bool` + `Update.Reason string`** collapse into a single
  `Update.Stale *StaleInfo`. Stale samples have `u.Stale != nil` and carry
  `u.Stale.Reason`; normal samples have `u.Stale == nil`. The boolean is
  no longer separately addressable.
- **`NotificationConfig`** no longer carries the internal `resubscribeAttempts`
  counter. Internal resubscribe bookkeeping moved to the unexported
  `pendingNotification` wrapper.
- **`ADSDataType`** is a distinct type (was `uint32`). The `ADST*`
  constants (`ADSTBool`, `ADSTInt16`, ..., `ADSTBigType`) are typed
  `ADSDataType`. `Symbol.BaseType` / `SymbolView.BaseType` / `inferBaseType`
  signatures retyped.
- **`Reason`** is a distinct type (was `string`). The `Reason*` constants
  (`ReasonSymbolVersionInvalid`, ..., `ReasonReloadInProgress`) are typed
  `Reason`. `Update.Reason` / `WithOnSymbolVersionChanged` callback /
  `detectStaleCache` / `consumeStaleFlag` / `markAllHandlesStale` /
  `staleHandles` map signatures retyped.
- **`AMSAddress` constructor + methods.** `NewAMSAddress(netID, port)`,
  `(AMSAddress).String() / NetIDString() / Equal()` are now public.
  `stringToNetID` exported as `ParseNetID`.
- **`BackoffConfig.Validate() error`** added. `WithBackoff` rejects invalid
  configs (zero or negative intervals, negative attempts, non-monotonic
  tiers) with a Warn log and keeps the default.
- **Route-registration error propagation.** `Connect` now returns an error
  if route registration fails, rather than logging Warn and continuing
  with a half-working session.
- **`parseRouteResponse`** returns an error when the PLC response has no
  error tag (was treated as success, masking malformed/truncated
  responses).

### Migration sketch (top use cases)

```go
// v2.1
sess, err := ads.NewSession("192.168.1.10", 48898, "5.154.236.19.1.1", 851,
    "auto", 10500, 5*time.Second)
if err != nil { return err }
defer sess.Close()
if err := sess.Connect(false); err != nil { return err }
v, err := sess.ReadFromSymbol("MAIN.x")

// v2.2
target, _ := ads.NewAMSAddress("5.154.236.19.1.1", 851)
sess, err := ads.NewSession(ctx, ads.AMSEndpoint{IP: "192.168.1.10", Port: 48898, AMS: target},
    ads.WithRequestTimeout(5*time.Second),
    ads.WithLocalAMS(ads.AMSAddress{Port: 10500}),
)
if err != nil { return err }
defer sess.Close()
if err := sess.Connect(ctx); err != nil { return err }
v, err := sess.ReadFromSymbol(ctx, "MAIN.x")
```

```go
// v2.1 Update consumer
for u := range ch {
    if u.Stale { handleStale(u.Reason) }
    process(u.Value)
}

// v2.2
for u := range ch {
    if u.Stale != nil { handleStale(u.Stale.Reason) }
    process(u.Value)
}
```

### Non-breaking improvements (concurrency hardening)

- `DeleteDeviceNotification`: snapshot symbol name BEFORE the PLC RPC so a
  concurrent `Reconnect` clearing `activeNotifications` mid-flight cannot
  strand the entry in `notificationConfigs` (which would cause
  `resubscribeNotifications` to re-subscribe a symbol the user deleted).
- `handleStaleDetection`: `versionCallback` now fires inside the
  `reloadInProgress` CAS-success branch for `AutoReload`. N concurrent
  triggers produce 1 callback (R-SES-011 once-per-detection); previously
  N triggers fired N callbacks.
- `notification_api.go`: `lastSubscribeNs` is stored BEFORE the
  `AddDeviceNotification` / `SumAddDeviceNotification` RPC so a
  first-sample arriving in the race window between PLC return and our map
  insert sees a fresh timestamp in the unknown-handle log-level decision.
- `Connect`: uses `transitionToOnce(Connecting)` to reject concurrent
  callers instead of letting both race on socket + client publish.
- `dialAndStart`: captures `ctx` and `cancel` under a single `RLock` (no
  split-window); `disconnected.Store(false)` happens AFTER `startWorkers`
  so a user RPC observing `disconnected=false` is guaranteed to find
  `transmitWorker` running.
- `Close` + `tearDownAndReset`: capture the shutdown cancel under RLock,
  release, then invoke. Holding RLock across the cancel was blocking the
  ctx-replacement path's `Lock`.
- `closedCh` close is now `sync.Once`-gated. The `Reconnect`-exhaustion
  path runs the full `releasePLCResources` cleanup so a subsequent
  `Close` is not a no-op that strands PLC subscriptions per source NetID.
- FSM gains `Connecting → Disconnected` and `Disconnected → Connecting`
  edges. `Connect` rolls back to `Disconnected` on error so the caller
  can retry via `Connect` (was stranded in `Connecting`, recoverable
  only via `Reconnect` auto-path).
- `notificationManager` gains a `configsByKey map[string]struct{}` mirror.
  Duplicate-subscribe probes are O(1) instead of O(N) per call; bulk
  subscribe is O(N) total instead of O(N²).
- `cmd_sum.go executeSumCommand`: overflow guard on `n*itemReadSize` /
  `n*itemWriteSize` (mirrors `SumRead`).
- On-demand reconnect (`reloadSymbols`) keeps the requested set in
  `cache.onDemandSymbols` across retries; previously partial-success on
  retry N silently dropped failed names from retry N+1.

### Documentation + style cleanup

- Many `defs.go` exported types and constants gained godoc (was
  fragments or missing): `AMSAddress`, `TransMode`, `ADSState`, `Port`,
  `CommandID`, `Group`, `Offset`, `ReturnCode`, `Write`, `DeviceInfo`,
  `NotificationStream`, `StampHeader`, `NotificationSample`.
- `NewConnection` doc fragment fixed to `NewSession`.
- `process_image.go` exported functions gained `EXPERIMENTAL:` per-symbol
  prefix so pkg.go.dev surfaces the marker per method.
- `session_options.go` `WithSymbolVersion*` docs replaced internal R-IDs
  with prose users on pkg.go.dev can read.
- Stale file references purged (`connection.go:575`, `commandRead.go`,
  `four previously-duplicated reset paths`).
- Phase tags (`Phase 1:`, `Phase 5.c relocated...`) stripped from
  comments per the v2.1 cleanup sweep.
- `capabilities.go` self-contradiction removed (`reset() clears all
  fields` vs `Reset is implicit`).
- `0xf010` → `0xF010` for consistency with surrounding hex constants.
- CHANGELOG em-dashes replaced with ASCII punctuation.

### Test coverage additions

- `TestFSM_AllowedTransitions`: table test pinning every legal and
  illegal (from, to) transition in the FSM.
- `TestFSM_TransitionToOnce_FirstWinnerOnly`: N=50 concurrent
  `transitionToOnce(Closed)` produces exactly one winner.
- `TestReconnectExhaustConcurrentClose_NoPanic`: 20-iter race between
  exhaustion path and `markClosed`; no double-close panic.
- `TestReconnect_FlapDetection_AccumulatesAcrossCycles`: flap counter
  increments across reconnect cycles within `flapWindow`.
- `TestReleasePLCResources_NotificationCleanup` +
  `TestReleasePLCResources_SymbolHandleRelease_{Skipped,Fired}`:
  `Close()` cleanup helper.
- `TestResubscribeNotifications_RollbackOnError`: rollback restores
  saved configs after `AddSymbolNotifications` outer error.
- `TestParseSumReadResponse_ErroredItemAdvancesOffset` +
  `TestParseSumReadResponse_ErroredItemOverflowsRemaining`: per-item
  alignment after an errored item.
- `TestBaseTypeName_LayeredResolution`: 7 sub-cases pinning the layered
  resolution priority (ADST_ primitive, datatype table, size inference).
- `TestHandleStaleDetection_Ignore_FiresCallbackPerTrigger`: companion
  to `TestSession_AutoReload_SingleFlight` for the Ignore strategy.

## v2.1.0: Layered architecture (breaking)

The single `Connection` god-type is split into two distinct public types:

- **`Client`** - thin Beckhoff-equivalent RPC layer. One TCP socket, raw AMS
  framing, request multiplexing, no cache, no notification persistence, no
  reconnect. Constructed via `Dial`. Suitable for one-shot consumers (CLI
  tools, web ADS browsers).
- **`Session`** - managed wrapper. Owns a `*Client` and adds the symbol
  cache, name-based read/write, persistent notifications with auto-resubscribe,
  auto-reconnect with backoff, FSM-based lifecycle, and lifecycle callbacks.
  Constructed via `NewSession`.

`Session` does NOT embed `Client` - there is no method promotion. Callers
choose a layer at construction. This keeps the boundary explicit.

### Breaking changes

- `Connection` type renamed to `Session`.
- `NewConnection(...)` renamed to `NewSession(...)`. The leading `ctx
  context.Context` parameter is removed; Session manages its own internal
  context.
- `ConnectionOption` renamed to `SessionOption`.
- The raw RPC methods (`Read`, `Write`, `WriteRead`, `ReadDeviceInfo`,
  `ReadState`, `Sum*`, `AddDeviceNotification`, `DeleteDeviceNotification`,
  `GetSymbolUploadInfo`, `GetSymbolVersion`, `GetHandleByName`,
  `ReadProcess*`, `WriteProcess*`, `ClearProcess*`, etc.) are no longer on
  `Session`. Reach them via `Dial(...) *Client` for raw use, or via the
  cache-aware Session methods (`ReadFromSymbol`, `WriteToSymbol`,
  `ReadMultipleSymbols`, `WriteMultipleSymbols`, `AddSymbolNotification(s)`)
  for managed use.
- `WithLogger` is a `SessionOption`; the new `WithClientLogger` configures
  a raw `Client`.
- `ConnectionOption` legacy alias is intentionally NOT provided; v2.0.x is
  deprecated and consumers migrate explicitly.
- Wire-RPC discovery helpers gained Beckhoff-equivalent exported names on
  `Client`: `DownloadInChunks`, `DownloadSymbolList`, `DownloadDataTypes`,
  `GetSymbolInfoByName`. The unexported equivalents on `Session` are
  removed.

### Internal cleanup (non-breaking from a user perspective)

- Lifecycle FSM promoted to first-class `SessionState` enum
  (`SessionStateConstructed` … `SessionStateClosed`). Replaces the previous
  three-flag soup (`closed` / `disconnected` / `reconnecting`).
- Generation counter unified: the previous `cache.generation` and
  `lifecycle.reconnectGeneration` collapse into a single `epoch
  atomic.Uint64` on the FSM, bumped on every Connected entry plus on
  user-driven cache swaps.
- Transport-down flag (`disconnected`) moved from the lifecycle struct onto
  the transport, where it semantically belongs.
- `ReleaseHandle` added on `Client` as the symmetric counterpart to
  `GetHandleByName`.

### Migration sketch

```go
// Before (v2.0.x):
ctx := context.Background()
conn, _ := ads.NewConnection(ctx, ip, 48898, netid, 851, "auto", 10500, 5*time.Second,
    ads.WithRoute("my-route", "Administrator", "password"))
conn.Connect(false)

// After (v2.1.0):
sess, _ := ads.NewSession(ip, 48898, netid, 851, "auto", 10500, 5*time.Second,
    ads.WithRoute("my-route", "Administrator", "password"))
sess.Connect(false)
```

For raw RPC use cases (no cache, no auto-reconnect):

```go
client, _ := ads.Dial(ip, 48898, target, source, 5*time.Second,
    ads.WithClientLogger(slog.Default()))
defer client.Close()
info, _ := client.ReadDeviceInfo()
```

### Online-change handling (DP-1)

- Detection of PLC online-change via the six R-CACHE-009 return codes
  (`0x711` `DeviceSymbolVersionInvalid`, `0x710` `DeviceSymbolNoFound`,
  `0x703` `DeviceInvalidOffset`, `0x722` `DeviceSymbolNotActive`, `0x714`
  `DeviceNotifyHandleInvalid`, `0x705` `DeviceInvalidSize`).
- Three strategies via `WithSymbolVersionStrategy`:
  - `SymbolVersionAutoReload` (default) - re-discover symbols + resubscribe
    notifications on detection.
  - `SymbolVersionClose` - terminate the session on detection (fires
    `OnDisconnect`, then `Close()`).
  - `SymbolVersionIgnore` - surface the PLC error to the calling op and
    flag surviving notification handles' next sample with `Update.Stale=true`
    (one-shot, consumed on first delivery).
- New `Update` fields `Stale bool` + `Reason string` (R-NOT-016). Reason
  values are stable strings exported as `Reason*` constants.
- Optional callback via `WithOnSymbolVersionChanged(fn func(reason string))`
  fires once per detection. Required signal under `SymbolVersionIgnore` to
  observe symbol-removal events (the dead handle's user channel goes silent
  - no terminal Update is delivered).
- Reload cap: `WithMaxSymbolVersionReloadAttempts` (default 3) within
  `WithSymbolVersionReloadWindow` (default 60s) prevents runaway reload
  loops under recurring online-change conditions (R-CACHE-013). On cap
  exhaustion the strategy degrades to Ignore for that detection and the
  callback fires with `ReasonReloadCapExhausted`.
- Hardware-validated against TwinCAT 3 v3.1.4024.
- Migration: the `Update` struct gained two trailing fields. Field-named
  struct literals are unaffected; positional literals (rare) require an
  update.

# Changelog

All notable changes to this project will be documented in this file.

This project uses [Conventional Commits](https://www.conventionalcommits.org/) and
[go-semantic-release](https://github.com/go-semantic-release/semantic-release) for
automated versioning and changelog generation.

## Unreleased: target NetID discovery, and a pre-release correctness pass

This branch started as target-NetID discovery and became a hardening pass. Two
review rounds plus a spec-and-verify round found fifteen candidate defects; five
were refuted or materially corrected by measurement rather than implemented, and
two of the worst were found while checking something else. Every fix here has a
test that was watched failing first, and every test was mutation-verified.

### Breaking / consumer-visible

- **Batch reads and writes now report per-item failures.** `ReadMultipleSymbols`
  dropped any item it could not produce a value for and returned the short map
  with a nil error; after a TwinCAT 3 runtime restart every cached handle is
  refused, so the caller got an **empty map and no error**. `WriteMultipleSymbols`
  had the same hole from the other side, because `ReturnCodeNoErrors` is zero and a
  never-sent item read as success.
  Signatures are unchanged. The detail rides in a new `*BatchError` that
  `errors.As` unwraps, returned only when at least one item failed, and the map
  still holds everything that succeeded. Per item, in the same three-state shape
  `SumNotificationResult` already used: `Skipped != nil` means the library is why
  there is no value; `Skipped == nil` with a non-zero `Error` is a PLC verdict on
  that entry, where a genuinely absent symbol lands.
  Deliberately not all-or-nothing: reading 40 symbols of which one is misspelled
  still yields 39 values plus an error naming the one that failed, and batch size
  does not change the shape. **Callers that do `if err != nil { return err }` will
  treat a partial batch as fatal until they branch on `*BatchError`.**
- **New `AMSError`, and AMS router codes are no longer ADS device verdicts.** The
  AMS header's ErrorCode was wrapped as a bare `ReturnCode`, so a transport
  failure was indistinguishable from a per-item answer about a symbol. With a PLC
  dropping into CONFIG at item 3 of a 40-symbol subscribe, 37 items came back
  shaped as PLC verdicts and the call returned `nil` — `ErrNotificationTransportFailure`,
  documented as the retry signal, was never set. `AMSError` has `Is` and
  deliberately no `Unwrap`, which is what stops `errors.As(&ReturnCode{})`
  matching while `errors.Is` keeps working.
- **Per-item code for a transport abort** in `sumReadFallback`/`sumWriteFallback`
  is now `ReturnCodeDeviceError` rather than `0x06`.
- **"Duplicate subscription" now means "has a live handle"**, not "is on file". A
  symbol sitting in the resubscribe retry queue is re-subscribable; a symbol with
  a live handle is still refused.
- **`WithForceRouteRegistration` now forces on reconnect too**, which is what its
  godoc always claimed. Cost: one route registration per reconnect for sessions
  that opt in. Sessions that do not set it are unaffected.
- **`Connect` no longer self-heals from a reset during its probe phase.** It
  returns an error, fires `onDisconnect`, and is retryable — and the retry now
  actually works, which it previously did not.
- **`SymbolVersionClose` now closes the session on a failed single subscribe**, and
  **`SymbolVersionIgnore` now fires `OnSymbolVersionChanged`** where it was
  previously silent.
- **`consecutiveFailures` resets per connection**, so a PLC flapping faster than
  the heartbeat recovery window regains the un-backed-off retry rate on each
  reconnect, bounded by `reconnectSleep`.

### Silent-failure fixes

- **A drop during the reconnect tail was erased on the way out.** `Reconnect`
  stored `disconnected = false` just before announcing Connected — a no-op on the
  happy path, since `dialAndStart` had already cleared it, so its only effect was
  to forget a drop that landed in between. `tx.disconnected` is the sole record of
  such a drop (the FSM has no `Reconnecting → Disconnected` edge), so the session
  sat Connected on a dead socket with `IsClosed()` false: no data, and no signal.
- **A reset during `Connect`'s probe spawned a rival reconnect** that ran
  `tearDownAndReset` concurrently with `Connect` on the shared transport, leaving a
  session that reported Connected after `Connect` returned an error, with a live
  client, two accepted connections and an unbounded reconnect loop — unrecoverable
  in place, because a retry was refused with "Connect already in progress".
- **A reconnect inherited the silence that preceded it.** `quietTicks`, `lastBeats`
  and `consecutiveFailures` survived a disconnect, so a session that dropped one
  tick short of its window tore down its subscriptions on the first tick after a
  completely successful reconnect — worst case leaving a Connected session with no
  subscriptions and no data.
- **A stale handle on subscribe left the symbol dead forever.** Single-symbol
  `AddSymbolNotification` returned the PLC's refusal without the stale-cache
  detection the read and write paths do. On TC3 a runtime restart answers 0x710
  and does not bump the symbol version, so nothing else ever triggered a reload.
- **A symbol awaiting resubscribe could not be subscribed again**, and the
  destructive intent reset that masked it also destroyed declared intent ahead of a
  success return.
- **`giveUpReconnecting` closed the FSM without tearing anything down**, and
  `Close()` then found the session already Closed and returned nil having done
  nothing — leaking the socket, the 48898 listener and every worker for the life of
  the process. Reachable from any `WithMaxReconnectAttempts` session whose PLC does
  not come back.
- **A failed `Connect` leaked the inbound listener** it had bound, once per attempt,
  for callers that respond to an error by discarding the session.
- **A repaired device stayed remembered for the life of the process.** The
  peer-route cache was only invalidated from `Connect`'s route-registration branch,
  so a session without `WithRoute` never forgot a host and every later session
  pre-bound wildcard `:48898` for it.
- **Heartbeat recovery backoff ran backwards**, and above a 30s cycle not at all:
  converting an absolute cap to ticks could yield fewer ticks than the base window,
  so the first failure *shrank* the tolerated silence (50s → 30s at a 10s cycle),
  and once the cycle reached the cap the division truncated to zero and the window
  grew to hours.

### Races

- `Client.source`, written under `connMu` and read bare in the transport-fault log
  line on the Client's own listen goroutine.
- `getSymbol` logged a `*symbol` **by reflection after dropping `cache.lock`**, so
  slog read the exact fields `dispatchSample` writes under that lock. `-race` was
  green only because slog skips its args at a disabled level and no test had ever
  enabled trace logging while notifications flowed — it fires in production, not in
  CI.
- `AddRoute`'s detached goroutine read the source NetID without its lock; a torn
  NetID there registers the junk route entry this repo already blames for muting
  two TC3 devices.

A sweep of all fifteen mutexes in the package established these three were the
complete set of bare accesses to a mutated guarded field.

### Tooling and tests

- CI and Makefile test timeouts 120s → 600s (both jobs were failing on time, not on
  a test), and `go vet -tags integration ./...` now runs in CI and `make vet` —
  over 20 integration files were compiled by nothing.
- `make hardware-parallel` runs one test run per PLC concurrently across all three
  devices. Two concurrent runs against a *single* PLC break its AMS router, so a
  repeated argument is refused rather than launched.
- `-race` suite 67s → 54s: the wire stub now closes accepted connections and its
  injected delays are interruptible, so tests stopped paying a 2s teardown cap each.
- Four tests that could not fail were fixed or deleted, each confirmed by mutation;
  one test that sent a real route-registration datagram to `127.0.0.1:48899` (6s in
  any environment where ICMP is suppressed) now uses the in-process responder.
- The integration suite understands the new batch contract, names the failing symbol
  and whether the library or the PLC is responsible, and restore steps that
  previously discarded their error now say which symbols were left holding test
  values.

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

### Notification handle hygiene (PLC handle-table accumulation fix)

Three complementary strategies prevent the PLC notification-handle
table from growing without bound across reconnects, crashes, and
online-change events. Beckhoff/ADS #268 root-cause: handles persist
on the PLC for `routeIdleTimeout` (default 5 minutes) after the
client's TCP slot evicts. Old clients hit the per-AMS-port soft cap
(~550 handles) and `AddDeviceNotification` returns 0x716 `NoMoreHandles`.

- **Orphan-Delete (`tryOrphanDelete`).** When the recvWorker decodes a
  `DeviceNotification` packet whose handle is not in
  `activeNotifications`, schedule a best-effort Delete RPC. Bounded
  by `orphanSem` (max 10 concurrent), throttled per-handle
  (`orphanSeen` map, 60s TTL), GC every 5 min. Re-checks
  `activeNotifications` under lock before firing to guard against
  handle-reuse races. Lifecycle-aware: skip if closed/reconnecting,
  honor `lifecycle.ctx` cancellation.
- **Reconnect pre-delete (Fix 3).** On Reconnect, snapshot
  `activeNotifications` handle list under `notifications.lock`
  before wiping. After the new transport is fully validated (dial +
  handshake + ensureRoute + reloadSymbols) and BEFORE
  resubscribeNotifications, fire `bestEffortDeleteNotifications` on
  the new transport. Old handles freed before new ones replace them.
- **Auto-reload pre-delete.** `reloadSymbolsAndResubscribe` snapshots
  handles → bumps `lastSubscribeNs` → wipes map (single lock) →
  best-effort-Delete on alive transport → LoadSymbols → re-subscribe.
  Eliminates online-change-induced handle flood.

Success-equivalent return codes (treated as cleanup wins by
`isBestEffortDeleteSuccess`): `0x000`, `0x714 NotifyHandleInvalid`,
`0x715 DeviceClientUnknown`. The 0x715 classification deliberately
diverges from official Beckhoff AdsLib — confirmed against
jisotalo/ads-client; documented in godoc.

### Source AMS port randomisation

`WithLocalAMS` default port no longer hard-coded to 10500. Each new
session draws a random port in the IANA dynamic range 32768-49151
(`randomAMSPort`). Prevents stale-slot collisions on the PLC when
the same client reconnects after an ungraceful exit and the PLC has
not yet aged out the old AMS port entry. AMS port is a logical
identifier inside the AMS header — distinct from TCP source port
(OS-assigned ephemeral) and TCP destination port (always 48898).

### Smart route registration (probe-first)

`Connect()` and `Reconnect()` no longer blindly register routes. A
single cheap `GetSymbolVersion` probe on a fresh TCP connection
classifies state:

- Probe succeeds → route exists, skip credential-bearing UDP
  `AddRoute` entirely.
- Probe fails with transport-level error (`ErrTransportClosed`,
  `io.EOF`, `syscall.ECONNRESET`) → redial + retry probe once.
  Catches transient PLC RST due to slot conflict (previous TCP from
  same source IP not yet released) rather than missing route.
- Probe fails with non-transport error → register immediately
  (re-dial would not help an ADS-level rejection).
- After `probeFailures >= 3` cumulative probe failures, fall back
  to always-register.

`ondrop` callback on `*Client` is disarmed via `defer` for the
entire `ensureRouteOnConnect` call, preventing a probe RST from
spawning a competing Reconnect goroutine. Re-armed automatically
in `dialAndStart`; retry block manually re-disarms after.

New options:

- **`WithSkipRouteRegistration()`** — bypass BOTH probe and
  `AddRoute` entirely. Use when route is pre-registered via TC3 UI /
  TC2 properties / AdsTool, or when fronted by a local AMS router
  daemon. Equivalent to omitting `WithRoute` but explicit; both can
  coexist (skip wins).
- **`WithLocalBindIP(ip)`** — pin outbound TCP source IP via
  `net.Dialer{LocalAddr: ...}`. Required for multi-Session
  deployments on a host with IP aliases, since TwinCAT enforces 1
  TCP slot per source IP regardless of source AMS NetID / port /
  route name (Beckhoff/ADS #49, #72). Option-time validation: invalid
  IPs log Warn and clear the binding (caller does not crash).

Route-naming convention update: callers that change source IP
across runs (DHCP, wifi↔ethernet, container vs bare metal) should
use a route name unique per source IP (e.g., `go-ads-{source-ip}`).
The integration test helper does this automatically when
`ADS_HOST_IP` is set.

### Router-subpackage preparation (additive exports)

Internal types/functions promoted to package surface so a future
in-process AMS router subpackage (planned for the next minor) can
front multiple `Session`s without forking the lib:

- `AMSHeader` struct + `AMSHeaderSize` (32) + `MaxAMSPayloadSize`
  (32 MiB) constants.
- `ParseAMSHeader(data []byte) (AMSHeader, error)` — bounds-checks
  `Length` against `MaxAMSPayloadSize`.
- `EncodeAMSHeader(h AMSHeader) []byte` — stdlib `binary.Write`
  on `bytes.Buffer`, error swallow documented as safe.
- `NotificationHandler` callback type (was unexported).

These exports are additive; they do not change any existing
signature.

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

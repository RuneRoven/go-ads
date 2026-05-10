# go-ads Implementation Details

Library-specific implementation details, fallback strategies, and API reference for the go-ads library. For protocol-level documentation, see [PROTOCOL.md](PROTOCOL.md).

This document is the source-of-truth for architectural detail. godoc comments stay short and point here; this file holds the longer explanation. Specs in `specs/` cite section anchors here — section headers are anchor targets and must not be renamed without updating those specs.

## Table of Contents

- [Architecture](#architecture)
- [Concurrency Model](#concurrency-model)
- [Sum Command Fallback Strategy](#sum-command-fallback-strategy)
- [Transmission Mode Downgrade](#transmission-mode-downgrade)
- [Symbol Discovery](#symbol-discovery)
- [Online-Change Handling](#online-change-handling)
- [Reconnection](#reconnection)
- [API Reference](#api-reference)
- [Connection Lifecycle](#connection-lifecycle)
- [Data Types](#data-types)

---

## Architecture

Post-Phase-5 layout: `Session` is a managed façade that composes sub-types; `Client` is the raw RPC layer exposed as an escape hatch for power users.

```text
                  Application
                       │
                       ▼
  ┌─────────────────────────────────────────────┐
  │ Session (managed)                           │
  │   - symbol cache + datatype cache           │
  │   - notification manager + dispatch         │
  │   - lifecycle FSM + reconnect manager       │
  │   - online-change strategy dispatcher       │
  │   - route registration                      │
  └────────────────┬────────────────────────────┘
                   │ composes
                   ▼
  ┌─────────────────────────────────────────────┐
  │ Client (raw)                                │
  │   - sendRequest, listen, transmit workers   │
  │   - capability probe state (3-state atomic) │
  │   - transport (TCP socket)                  │
  └────────────────┬────────────────────────────┘
                   │
                   ▼
              ams.go (AMS/TCP encode/decode)
                   │
                   ▼
              transport (net.Conn)
```

### File layout

| File | Role |
|------|------|
| `session.go` | `Session` façade + reload/stale dispatcher + reconnect orchestration |
| `session_fsm.go` | `SessionState` enum, `sessionFSM` struct (atomic.Uint32 state, atomic.Uint64 epoch) |
| `session_options.go` | `SessionOption` functional options |
| `client.go` | Raw `Client`: Dial, sendRequest, listen, lifecycle, notification dispatch |
| `transport.go` | TCP socket + dial helpers |
| `capabilities.go` | Per-command sum-capability state (3-state atomic) |
| `ams.go` | AMS/TCP packet encode/decode |
| `route.go` | UDP route registration (port 48899) |
| `defs.go` | Types, enums, error codes, `Reason` constants, `SymbolVersionStrategy` |
| `logger.go` | Logger plumbing + helpers |
| `cmd_simple.go` | Single Read/Write/WriteRead/ReadState/ReadDeviceInfo on Client |
| `cmd_sum.go` | SumRead/SumWrite/SumAddDeviceNotification/SumDeleteDeviceNotification |
| `cmd_notification.go` | AddDeviceNotification + listener dispatch |
| `symbol_access.go` | Session.ReadFromSymbol/WriteToSymbol/ReadMultipleSymbols/WriteMultipleSymbols (handles caches + online-change detection) |
| `symbol_discovery.go` | LoadSymbols / LoadSymbolsSlow / LoadSymbolList / LoadDataTypes / RefreshSymbols |
| `symbol_codec.go` | Symbol.parse / serialize (typed value codec) |
| `symbols.go` | `Symbol` struct + `symbolCache` primitives |
| `notification_api.go` | `Update`, AddSymbolNotification(s), DeleteDeviceNotification, `notificationManager` |
| `browse.go` | BrowseSymbols |
| `process_image.go` | Raw process-image read/write helpers |

### Composition

`Session` (session.go) embeds and owns:
- `*Client` — raw RPC layer (sess.client)
- `*symbolCache` — symbols + datatypes + on-demand tracking (sess.cache)
- `*notificationManager` — active notifications + configs (sess.notifications)
- `*sessionLifecycle` — FSM, reconnect state, epoch counter (sess.lifecycle)
- `routeConfig` — route name/user/pass + host IP (sess.route)

Receiver naming: `sess *Session`, `c *Client`. Callers construct a Session via `NewSession(...)`.

---

## Concurrency Model

Read-mostly state under coarse locks; transport hot path uses atomics.

### Locks

| Lock | Type | Guards |
|------|------|--------|
| `sess.cache.lock` | sync.RWMutex | symbols map, datatypes map, symbolVersion, onDemandSymbols set, symbolListLoaded, datatypesLoaded, symbolsFullyLoaded |
| `sess.notifications.lock` | sync.Mutex | activeNotifications, notificationConfigs, notificationChannel |
| `sess.staleHandlesMu` | sync.Mutex | staleHandles map (DP-1 / R-NOT-017) |
| `sess.reloadMu` | sync.Mutex | reloadAttempts sliding window (R-CACHE-013) |
| `sess.lifecycle.reconnectMu` | sync.Mutex | reconnect FSM transitions, reconnectDone channel |
| `sess.lifecycle.state` | atomic.Uint32 (inside `sessionFSM`) | session state (Constructed/Connecting/…/Closed) |
| `sess.lifecycle.epoch` | atomic.Uint64 | reconnect / reload generation counter |
| `c.notifyMu` | sync.RWMutex | notification dispatcher callback (RLock on hot path) |
| `c.tx.disconnected` | atomic.Bool | transport-down flag for sendRequest fast-fail |

### Lock ordering invariant (R-CACHE-008)

**`cache.lock` and `notifications.lock` MUST NEVER be held simultaneously.** Crossing these locks is the only way to deadlock in this codebase; the rule is enforced by code review.

`staleHandlesMu` and `reloadMu` are leaves — acquired and released without holding any other Session-level lock. Documented exception: `markAllHandlesStale` holds `notifications.lock` and acquires `staleHandlesMu` inside `markSymbolStale` (notifications → staleHandles is a permitted top-down order).

### Goroutines per Session

1. **listen** (`Client.listen`) — reads packets from TCP, parses AMS header, routes to response map keyed by invoke ID; notification packets go to `dispatchNotification`.
2. **transmitWorker** (`Client.transmitWorker`) — serializes outgoing packets from send channel to TCP.
3. **reconnect manager** — spawned by `triggerReconnect()` when auto-reconnect is enabled; runs the retry/backoff loop and re-subscribes notifications.
4. **notification dispatcher** — fan-out runs in the caller goroutine of `dispatchNotification`; no dedicated dispatcher worker.

### Request flow

1. Allocate invoke ID (atomic counter inside Client).
2. Register response channel in `activeRequests` map.
3. Push encoded packet to send channel; transmitWorker writes.
4. Block on response channel with `requestTimeout`.
5. Clean up map entry on return (defer).

When `tx.disconnected` is set, `sendRequest` returns `ErrDisconnected` immediately without touching the socket — this is the legacy transport-down flag and is distinct from the FSM state. See [Reconnection](#reconnection) for why both exist.

---

## Sum Command Fallback Strategy

Per Beckhoff: try newest commands first, fall back on "not supported" errors. The library probes once per Connect cycle and caches the result in per-command 3-state atomics defined in `capabilities.go`.

### Capability state (3-state atomic)

Each sum command category has its own `atomic.Uint32`:

```
0 = unchecked
1 = supported (use the modern command)
2 = unsupported (use the fallback path)
```

For SumRead, the "supported" state additionally carries which command id won (`0xF084` vs `0xF083`); see code for the encoding. State transitions use CompareAndSwap so concurrent first-callers don't amplify the probe.

| Field (in `capabilities`) | Commands gated |
|---------------------------|----------------|
| `sumReadState` | 0xF084 (Ex2, TC3) → 0xF083 (Ex, TC2+TC3) → individual Read |
| `sumWriteState` | 0xF081 → individual Write |
| `sumAddNotifState` | 0xF085 → individual AddDeviceNotification (with mode downgrade) |
| `sumDeleteNotifState` | 0xF086 → individual DeleteDeviceNotification |
| `chunkedDownloadState` | offset-based chunked uploads (for `LoadSymbolsSlow` / `LoadSymbolList`) |

Per-command separation is load-bearing: a TC2 PLC supports SumRead via 0xF083 but does NOT support SumAddDeviceNotification (0xF085). A single shared flag would block the fallback for notifications.

All capability state resets on a successful Reconnect (`tearDownAndReset(resetFeatureFlags=true)`). On Reload (online-change AutoReload path) the state is preserved — the PLC's command surface did not change, only its symbol table did.

### SumRead — three-tier fallback

```text
SumReadEx2 (0xF084)  →  SumReadEx (0xF083)  →  Individual Read
     TC3 only               TC2 + TC3            Always works
```

Both 0xF084 and 0xF083 return the same response shape: `[N × (error(4), length(4))][data]`. `parseSumReadResponse` handles both.

### SumWrite — single fallback

```text
SumWrite (0xF081)  →  Individual Write
```

Writes are not idempotent. Fallback fires ONLY on "not supported" return codes — never on transient errors, since the PLC may have partially applied the batch.

### SumAddDeviceNotification — single fallback with mode downgrade

```text
SumAddDeviceNotification (0xF085)  →  Individual AddDeviceNotification
```

The fallback path automatically downgrades v2 transmission modes to v1 (see [Transmission Mode Downgrade](#transmission-mode-downgrade)) because TC2 silently ignores v2 modes.

### SumDeleteDeviceNotification — single fallback

```text
SumDeleteDeviceNotification (0xF086)  →  Individual DeleteDeviceNotification
```

### Unsupported-error detection

Fallback triggers when the PLC returns one of:
- `0x0701` — DeviceServiceNotSupported
- `0x0008` — UnknownCommandID
- `0x000B` — UnknownAdsCommand

All other return codes propagate to the caller.

---

## Transmission Mode Downgrade

When the notification fallback path is active (typically TC2), v2 modes are silently ignored by the PLC. The library detects this case and downgrades:

| Requested mode | Downgraded to |
|----------------|---------------|
| `ServerOnChange2` (6) | `ServerOnChange` (4) |
| `ServerCycle2` (5)    | `ServerCycle` (3)    |
| All others            | Unchanged            |

Downgrade only applies in the fallback path. When 0xF085 succeeds (TC3), the original mode is sent unchanged.

---

## Symbol Discovery

### Modes

| Mode | API | What's downloaded | Capabilities |
|------|-----|-------------------|--------------|
| None (default) | `Connect()` | Nothing | On-demand reads/writes only |
| Symbol list | `LoadSymbolList(cfg)` | Symbol table (0xF00B) | Browse top-level names |
| Datatypes | `LoadDataTypes(cfg)` | Datatype table (0xF00E) | Struct child expansion |
| Full | `LoadSymbols()` | Both tables | Everything |
| Full chunked | `LoadSymbolsSlow(cfg)` | Both tables, in chunks | Everything, PLC-friendly |

### Chunked downloads

`LoadSymbolsSlow` and `LoadSymbolList` download data in configurable chunks with delays between them, to minimize PLC real-time impact:

```go
sess.LoadSymbolsSlow(ads.SlowDiscoveryConfig{
    ChunkSize:  4096,                  // bytes per chunk (default: 4096)
    ChunkDelay: 100 * time.Millisecond, // delay between chunks (default: 100ms)
})
```

Falls back to a single-request download if the PLC doesn't support offset-based chunked reads (gated by `chunkedDownloadState` in `capabilities.go`).

### On-demand resolution

When discovery mode is None, `GetSymbol(name)` and the `ReadFromSymbol`/`WriteToSymbol` path resolve symbols lazily via `GetSymbolInfoByName` + `GetHandleByName` and add them to `cache.onDemandSymbols`. On reconnect, only on-demand symbols are re-resolved (graceful skip on missing — see [Reconnection](#reconnection)).

### Symbol handles

Handles are acquired lazily via `GroupSymbolHandleByName` (0xF003) and cached on `Symbol.Handle`. Released on `Close()` or on online-change AutoReload.

### Symbol version

Read via `GroupSymbolVersion` (0xF008). Returns a `uint8` that changes on every PLC program download. Used both for explicit version polling (`CheckSymbolVersion`, `RefreshSymbols`) and as one of several signals for online-change detection (see next section).

### Internal symbol-name normalisation

All internal cache keys are lowercased. PLC-supplied capitalisation is preserved in returned `Symbol`/`SymbolView` fields for readability, but lookups are case-insensitive. This avoids cache misses from capitalisation mismatch.

---

## Online-Change Handling

Added in v2.1.0 (DP-1). Detects when the PLC's symbol table has changed underneath an established Session (e.g., after a TwinCAT online change) and applies a user-configurable strategy.

### Strategies

Configured via `WithSymbolVersionStrategy(s)`:

| Strategy | Constant | Behaviour |
|----------|----------|-----------|
| AutoReload (default) | `SymbolVersionAutoReload` | bump epoch → zero old symbol handles → `LoadSymbols` → `resubscribeNotifications` → fire `onReconnect`; rate-limited by reload window (R-CACHE-013) |
| Close | `SymbolVersionClose` | Fire `onDisconnect` then `Session.Close()`; Session enters terminal Closed state and cannot be reused |
| Ignore | `SymbolVersionIgnore` | Surface the PLC error verbatim; `markAllHandlesStale` flags affected handles so the next notification sample carries `Stale=true, Reason=<detection-reason>` (one-shot) |

### Detection set (R-CACHE-009)

Six return codes trigger the dispatcher:

| Code | Constant | Why |
|------|----------|-----|
| 0x711 | `ReturnCodeDeviceSymbolVersionInvalid` | Beckhoff: "online change. Create a new handle." |
| 0x710 | `ReturnCodeDeviceSymbolNoFound` | Symbol gone after online change |
| 0x703 | `ReturnCodeDeviceInvalidOffset` | TC3 surfaces this on cached handle post-delete |
| 0x722 | `ReturnCodeDeviceSymbolNotActive` | Beckhoff: "Release the handle and try again." |
| 0x714 | `ReturnCodeDeviceNotifyHandleInvalid` | Subscription handle invalidated |
| 0x705 | `ReturnCodeDeviceInvalidSize` | Cached `Symbol.Length` disagrees with PLC (e.g. INT↔LREAL toggle) |

### Dispatch points

`Session.handleStaleDetection(rc)` fires from:
- `ReadFromSymbol` / `WriteToSymbol` (and the `ReadMultipleSymbols` / `WriteMultipleSymbols` Sum paths)
- `AddSymbolNotifications`
- The notification listener, on receipt of a 0-byte terminal sample (PLC sends this when a subscribed handle is invalidated)
- `Symbol.parse`, on Length-vs-payload mismatch

### Rate limit (R-CACHE-013)

`reloadAttempts` is a sliding window guarded by `reloadMu`. Configured via `WithMaxSymbolVersionReloadAttempts(n)` (default 3) and `WithSymbolVersionReloadWindow(d)` (default 60s). If the window cap is exceeded, AutoReload escalates to Close — the assumption being a misbehaving PLC or hot loop.

### Update semantics for affected samples

On detection, `Update.Stale` is set to `true` and `Update.Reason` carries the detection-reason string. This is one-shot (consumed via `consumeStaleFlag(handle)` on next read). Callers should treat the value as suspect and re-resolve.

### Callback hook

`WithOnSymbolVersionChanged(fn func(reason string))` registers a callback fired on every detection, regardless of strategy. Runs in a goroutine; must not block.

---

## Reconnection

Triggered by:
- The listen worker detecting EOF / read error → `tx.disconnected.CompareAndSwap(false, true)` → schedules reconnect manager
- Explicit `Session.Reconnect()` call

Controlled by `WithAutoReconnect(true)` (default). With auto-reconnect disabled, the listen worker still flips `tx.disconnected` and fires `OnDisconnect`, but the reconnect manager is NOT spawned — the caller must call `Reconnect()` manually.

TCP keepalive: idle=3s, interval=2s, count=5 → detection within ~13 seconds.

### FSM transitions

```
Constructed → Connecting → Connected
                              │
                              ├──→ Reloading (online change AutoReload) ──┐
                              │                                            │
                              ├──→ Disconnected → Reconnecting ────────────┤
                              │                       │ (self-loop on retry)
                              ▼                       ▼
                            Closed ←──────────── Connected (back to top)
```

State is `atomic.Uint32` inside `sessionFSM`; transitions go through `CompareAndSwap` with an allow-list (`session_fsm.go`). `Closed` is terminal.

### Two "down" signals (intentional)

- **`tx.disconnected`** (atomic.Bool, in `transport`) — transport-level flag. Set by listen worker on socket error; cleared after successful dial. Checked by `sendRequest` on every send to fail-fast without touching the socket.
- **FSM state** — broader lifecycle. Reports Reconnecting / Disconnected / Reloading / Closed.

These are distinct because during Reconnecting the FSM says "down" but the transport is alive between dial-and-reload. `sendRequest` uses `isTransportDown()` (the legacy flag) so cache reloads can issue ADS commands; user-facing `IsDisconnected()` uses the FSM state.

### Backoff strategy

Stepped backoff with configurable tiers (`WithBackoff`). Resets to initial interval on each successful reconnect.

**Default tiers (used when `WithBackoff` not provided):**

| Attempt | Delay | Rationale |
|---------|-------|-----------|
| 1–3 | 1s | Fast retry for network blips |
| 4–6 | 5s | Medium backoff |
| 7–10 | 15s | Slow tier for extended outages |
| 11+ | 30s | Cap to avoid overwhelming PLC |

Custom example:
```go
ads.WithBackoff(ads.BackoffConfig{
    InitialInterval: 500 * time.Millisecond,
    InitialAttempts: 5,
    MidInterval:     3 * time.Second,
    MidAttempts:     5,
    SlowInterval:    10 * time.Second,
    SlowAttempts:    5,
    MaxInterval:     60 * time.Second,
})
```

Total attempts bounded by `WithMaxReconnectAttempts(n)` (R-RECON-005). Reaching the cap escalates to `Close()` and fires `OnDisconnect` one final time.

### Reconnect sequence

1. Close old TCP connection.
2. Stop listen and transmitWorker goroutines (`waitGroup.Wait()` in `tearDownAndReset`).
3. Reset capability flags (all per-command states back to 0=unchecked).
4. Retry TCP dial with stepped backoff.
5. Re-register AMS route if configured (probe-first; see [Smart route registration](#smart-route-registration)).
6. Reload symbols based on prior discovery mode:
   - Full discovery → re-download full symbol table
   - On-demand only → re-resolve only previously accessed symbols (graceful skip on missing unless `WithStrictReconnect`)
   - No symbols → read symbol version only
7. Filter `notificationConfigs` — drop entries for symbols no longer available.
8. Re-subscribe remaining notifications.
9. Bump epoch counter (`sess.lifecycle.epoch.Add(1)`).
10. Fire `OnReconnect`.

### Event callbacks

```go
ads.WithOnDisconnect(func() { /* runs in goroutine, must not block */ })
ads.WithOnReconnect(func() { /* runs in goroutine, must not block */ })
```

- `OnDisconnect` fires once per disconnect event, gated by `CompareAndSwap` (R-RECON-002). Not fired after `Close()`.
- `OnReconnect` fires at the end of a successful Reconnect after the epoch bump. Not fired if `Close()` happened during reconnect.

### Smart route registration

Both `Connect()` and `Reconnect()` use a probe-first approach for route registration:

1. TCP connect + start goroutines.
2. **Probe:** send `GetSymbolVersion()` — a cheap ADS command.
3. Probe OK → route exists, skip credential registration.
4. Probe fails → register route over UDP, TCP-reconnect, retry probe.
5. After repeated probe failures, the library skips probing and always registers (fallback).

`WithForceRouteRegistration()` bypasses probing entirely — always registers with credentials. Requires `WithRoute(...)`.

> Security note: route credentials passed via `WithRoute` / `AddRemoteRoute` are transmitted in plaintext over UDP. Avoid hardcoding; load from env or secret store.

### Stale-handle retry (epoch-based)

`Session.lifecycle.epoch` (atomic.Uint64) increments on each successful reconnect AND on each AutoReload. Symbol handles acquired before an epoch bump may be invalid (PLC reassigns handles after program reload).

`ReadFromSymbol`, `ReadMultipleSymbols`, `WriteToSymbol`, and `WriteMultipleSymbols` capture the epoch before performing I/O. If the op fails and the epoch has changed, they retry once with fresh handles. Bounded recursion: each retry captures a new epoch and only retries again if *another* bump happened during the retry.

### Strict reconnect mode

By default, missing on-demand symbols after reconnect are skipped with a warning (e.g., after online change removed a variable). `WithStrictReconnect(maxAttempts)` changes this:

- Missing symbol → reconnect considered failed.
- `maxAttempts = 0` → fail immediately on first missing symbol.
- `maxAttempts = N` → retry up to N times, then close the connection.
- Failure counter resets on a fully-successful reconnect (all symbols resolved).

---

## API Reference

### Session lifecycle (7)

| Method | Description |
|--------|-------------|
| `NewSession(ip, port, netid, amsPort, localNetID, localPort, requestTimeout, opts...)` | Construct Session with options |
| `Connect(local bool)` | TCP dial + start goroutines + probe/register route |
| `Close()` | Delete notifications, release handles, close TCP, terminal Closed |
| `Reconnect()` | Re-establish after failure (auto or manual) |
| `AddRoute(routeName, username, password)` | Register an AMS route over UDP after construction |
| `IsDisconnected()` | True while transport is down (auto-reconnect may resolve) |
| `IsClosed()` | True after `Close()` or a terminal FSM transition (e.g. `SymbolVersionClose`) — Session cannot be reused |

### Reading and writing (4)

| Method | Description |
|--------|-------------|
| `ReadFromSymbol(name)` | Resolve symbol + read + parse to string |
| `WriteToSymbol(name, value)` | Resolve + parse string + write |
| `ReadMultipleSymbols(names)` | Batch read via SumRead with fallback |
| `WriteMultipleSymbols(values)` | Batch write via SumWrite with fallback |

### Notifications (4)

| Method | Description |
|--------|-------------|
| `AddSymbolNotification(name, maxDelay, cycleTime, mode, ch)` | Subscribe single symbol |
| `AddSymbolNotifications(configs, ch)` | Subscribe many (uses SumAdd internally) |
| `DeleteDeviceNotification(handle)` | Unsubscribe one |
| `SumDeleteDeviceNotification(handles)` | Unsubscribe many |

### Symbol discovery (8)

| Method | Description |
|--------|-------------|
| `LoadSymbols()` | Full discovery — single request (locks PLC task) |
| `LoadSymbolsSlow(cfg)` | Full discovery in chunks (PLC-friendly) |
| `LoadSymbolList(cfg)` | Symbol names only |
| `LoadDataTypes(cfg)` | Datatype definitions only |
| `BrowseSymbols(path)` | Navigate symbol hierarchy |
| `ListSymbols()` | Get full symbol map (requires LoadSymbols) |
| `GetSymbol(name)` | Get symbol; resolve on-demand if needed |
| `RefreshSymbols()` | Reload if version changed |
| `CheckSymbolVersion()` | Check version without reload |

### SessionOption (17)

All options compose — no mutual exclusions.

| Option | Default | Description |
|--------|---------|-------------|
| `WithLogger(logger)` | `slog.Default()` | Custom `*slog.Logger` |
| `WithHostIP(ip)` | Derived from AMS NetID | IP the PLC uses to reach this client. Only affects route registration |
| `WithRoute(name, user, pass)` | No registration | Register AMS route over UDP. Credentials sent in plaintext |
| `WithForceRouteRegistration()` | Probe first | Always register with credentials. Requires `WithRoute` |
| `WithBackoff(cfg)` | 1s×3, 5s×3, 15s×4, 30s cap | Stepped reconnect backoff |
| `WithMaxReconnectAttempts(n)` | Unbounded | Hard cap on reconnect attempts before terminal Close |
| `WithRequestTimeout(d)` | Constructor arg | Per-request timeout |
| `WithAutoReconnect(bool)` | `true` | Spawn reconnect manager on transport drop |
| `WithStrictReconnect(maxAttempts)` | Graceful: warn + drop | Fail reconnect if previously-resolved symbols are missing |
| `WithOnDisconnect(fn)` | None | Callback on disconnect (goroutine, must not block) |
| `WithOnReconnect(fn)` | None | Callback after successful reconnect |
| `WithSymbolVersionStrategy(s)` | `AutoReload` | Online-change strategy: AutoReload / Close / Ignore |
| `WithMaxSymbolVersionReloadAttempts(n)` | 3 | Sliding-window cap on AutoReload firings |
| `WithSymbolVersionReloadWindow(d)` | 60s | Sliding-window duration for the cap above |
| `WithOnSymbolVersionChanged(fn)` | None | Callback fired on every online-change detection |

### Client (raw RPC, escape hatch)

`Client` is exposed for power users who need direct protocol access. Typical apps use `Session` only.

Lifecycle:

| Method | Description |
|--------|-------------|
| `Dial(...)` | Connect a raw Client |
| `Close()` | Tear down sockets + goroutines |
| `SetOnDrop(fn)` | Register unexpected-drop callback |
| `SetNotificationHandler(fn)` | Register notification dispatcher |
| `ReleaseHandle(handle)` | Release a symbol handle |

Raw RPC:

| Method | Description |
|--------|-------------|
| `Read(group, offset, length)` | ADS Read |
| `Write(group, offset, data)` | ADS Write |
| `WriteRead(group, offset, readLen, data)` | ADS ReadWrite |
| `ReadState()` | ADS / device state |
| `ReadDeviceInfo()` | Device name + version |

Sum-batch (with fallback):

| Method | Description |
|--------|-------------|
| `SumRead(requests)` | 0xF084 → 0xF083 → individual |
| `SumWrite(requests)` | 0xF081 → individual |
| `SumAddDeviceNotification(requests)` | 0xF085 → individual + mode downgrade |
| `SumDeleteDeviceNotification(handles)` | 0xF086 → individual |

Process I/O / discovery: `ReadProcessInput`, `ReadProcessOutput`, `WriteProcessOutput`, `DownloadInChunks`, `GetSymbolInfoByName`, `GetHandleByName`, `GetSymbolUploadInfo`, `GetSymbolVersion`.

### ClientOption (4)

| Option | Description |
|--------|-------------|
| `WithClientLogger(logger)` | Custom logger |
| `WithClientRequestTimeout(d)` | Per-request timeout |
| `WithNotificationHandler(fn)` | Construction-time notification dispatcher |
| `WithOnDrop(fn)` | Construction-time form of `Client.SetOnDrop()` |

---

## Connection Lifecycle

```text
1. NewSession(..., opts...)         configure target, source, timeouts, options
       │
       ▼
2. Connect(false)                   TCP dial → probe route → register if needed
       │                            → start listen + transmitWorker
       │                            FSM: Constructed → Connecting → Connected
       │
3. [optional] LoadSymbols() /       full discovery
   LoadSymbolsSlow(cfg) /           chunked discovery (PLC-friendly)
   LoadSymbolList(cfg) /            names only
   LoadDataTypes(cfg)               datatypes only
       │
       ▼
4. Use the Session:
   ReadFromSymbol / WriteToSymbol   on-demand resolution + epoch retry
   AddSymbolNotifications           batched subscribe via 0xF085 (or fallback)
   BrowseSymbols(path)              requires step 3
       │
   ┌───┴── (online change) ──────────────────────┐
   │  detection set hit (R-CACHE-009)            │
   │  strategy AutoReload:                       │
   │    FSM: Connected → Reloading → Connected   │
   │    bump epoch + LoadSymbols + resubscribe   │
   │  strategy Close:                            │
   │    fire OnDisconnect + Close()              │
   │  strategy Ignore:                           │
   │    mark stale; surface error to caller      │
   └─────────────────────────────────────────────┘
       │
   ┌───┴── (TCP drop) ───────────────────────────┐
   │  OnDisconnect fires (CAS-gated)             │
   │  FSM: Connected → Disconnected →            │
   │       Reconnecting → Connected              │
   │  if autoReconnect=true:                     │
   │    reconnect manager loops with backoff     │
   │    reset capability flags                   │
   │    re-probe / register route                │
   │    reload symbols (graceful skip)           │
   │    filter + resubscribe notifications       │
   │    bump epoch                               │
   │    OnReconnect fires                        │
   │  if autoReconnect=false:                    │
   │    caller must call Reconnect() manually    │
   └─────────────────────────────────────────────┘
       │
       ▼
5. Close()                          terminal: FSM → Closed; Session not reusable
```

---

## Data Types

Parsed PLC types serialized via `symbol_codec.go`:

| PLC Type       | Size    | Go representation          |
|----------------|---------|----------------------------|
| BOOL           | 1 byte  | `"true"` / `"false"`       |
| BYTE, USINT    | 1 byte  | Unsigned 0–255             |
| SINT           | 1 byte  | Signed −128 to 127         |
| UINT, WORD     | 2 bytes | Unsigned 0–65535           |
| INT            | 2 bytes | Signed −32768 to 32767     |
| UDINT, DWORD   | 4 bytes | Unsigned 32-bit            |
| DINT           | 4 bytes | Signed 32-bit              |
| ULINT, LWORD   | 8 bytes | Unsigned 64-bit            |
| LINT           | 8 bytes | Signed 64-bit              |
| REAL           | 4 bytes | 32-bit float               |
| LREAL          | 8 bytes | 64-bit float               |
| STRING         | varies  | Null-terminated string (write clamps to Length−1 for terminator) |
| WSTRING        | varies  | UTF-16 null-terminated     |
| TIME           | 4 bytes | `HH:MM:SS.sss`             |
| TOD            | 4 bytes | `HH:MM`                    |
| DATE           | 4 bytes | `YYYY-MM-DD`               |
| DT             | 4 bytes | `YYYY-MM-DD HH:MM:SS`      |

Structs, function blocks, and arrays are recursively expanded and serialized as JSON.

Enum types use their underlying base type. TC2 flattens enums to INT/DINT in the symbol table; TC3 preserves full enum metadata.

The codec accepts `ADST_` symbol prefixes returned by some PLC builds (e.g. `ADST_INT16`) as aliases for the canonical type names.

# go-ads Implementation Details

Library-specific implementation details, fallback strategies, and API reference for the go-ads library. For protocol-level documentation, see [PROTOCOL.md](PROTOCOL.md).

This document is the source-of-truth for architectural detail. godoc comments stay short and point here; this file holds the longer explanation. Historical behavioral specs for the v2.0 → v2.2 redesign live in [`docs/archive/specs/`](docs/archive/specs/) — kept for design-rationale reference, not maintained for v2.2.1+.

## Table of Contents

- [Architecture](#architecture)
- [Concurrency Model](#concurrency-model)
- [Sum Command Fallback Strategy](#sum-command-fallback-strategy)
- [Transmission Mode Downgrade](#transmission-mode-downgrade)
- [Symbol Discovery](#symbol-discovery)
- [Online-Change Handling](#online-change-handling)
- [Reconnection](#reconnection)
- [Notification handle hygiene](#notification-handle-hygiene)
- [Multi-Session on the same host (limitation)](#multi-session-on-the-same-host-limitation)
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

```text
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
| Ignore | `SymbolVersionIgnore` | Surface the PLC error verbatim; `markAllHandlesStale` flags affected handles so the next notification sample carries `Stale=&StaleInfo{Reason: ...}` (one-shot) |

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

On detection, `Update.Stale` is set to a non-nil `*StaleInfo{Reason: ...}` carrying the typed `Reason` enum value. This is one-shot (consumed via `consumeStaleFlag(handle)` on next read). Callers should treat the value as suspect and re-resolve.

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

```text
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

1. **Snapshot pre-reconnect notification handles** under `notifications.lock`,
   bump `lastSubscribeNs`, wipe the `activeNotifications` map (atomic
   under the lock). Snapshot lives on Reconnect's stack until step 7.
   (v2.2.1 Fix 3 — prevents PLC handle-table accumulation; see
   Beckhoff/ADS #268.)
2. `tearDownAndReset(false)` — cancel lifecycle ctx, close old TCP,
   wait for listen/transmit/recvWorker goroutines.
3. Retry TCP dial with stepped backoff (per `BackoffConfig`).
4. Re-perform local-mode handshake if applicable (in-process TC runtime).
5. Re-register AMS route if configured (probe-first; see
   [Smart route registration](#smart-route-registration)).
6. Reload symbols based on prior discovery mode:
   - Full discovery → re-download full symbol table
   - On-demand only → re-resolve only previously accessed symbols
     (graceful skip on missing unless `WithStrictReconnect`)
   - No symbols → read symbol version only
7. **Best-effort delete** the snapshot from step 1 via
   `bestEffortDeleteNotifications` on the new TCP transport. Treats
   `0x714 NotifyHandleInvalid` and `0x715 DeviceClientUnknown` as
   success-equivalent (PLC already cleaned up on its side; see
   `isBestEffortDeleteSuccess`). `savedHandles = nil` after the first
   pass so a later retry-loop iteration doesn't re-fire.
8. Filter `notificationConfigs` — drop entries for symbols no longer
   available.
9. Re-subscribe remaining notifications (fresh PLC-assigned handles
   replace the deleted ones).
10. Bump epoch counter (`sess.lifecycle.epoch.Add(1)`).
11. Fire `OnReconnect`.

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
4. Probe fails with a transport-level error (`ErrTransportClosed`, `io.EOF`,
   `syscall.ECONNRESET`) → **redial + retry probe once** before registering.
   This catches the case where the PLC RST'd due to a transient slot
   conflict (previous TCP from same source IP not yet released) rather
   than a missing route. Retry-success → skip AddRoute entirely. Retry-fail
   → register via UDP, TCP-reconnect, continue. (v2.2.2.)
5. Probe fails with a non-transport error → register immediately
   (re-dial wouldn't help an ADS-level rejection).
6. After repeated probe failures (`probeFailures >= 3`), the library skips
   probing entirely and always registers (fallback).

`WithSkipRouteRegistration()` bypasses BOTH probe and AddRoute entirely
when callers manage the route lifecycle externally (pre-registered via
TC3 UI, or fronted by a local AMS router daemon). (v2.2.2.)

`WithForceRouteRegistration()` bypasses probing — always registers with
credentials. Requires `WithRoute(...)`.

#### `ondrop` suppression during Connect

During `ensureRouteOnConnect` the `ondrop` callback on the active
`*Client` is disarmed (set to nil) and restored on function exit via
`defer`. Reason: a probe RST during the probe-RPC call would otherwise
fire `sess.triggerReconnect` via the listen goroutine, spawning a
concurrent Reconnect goroutine that competes with our own
AddRoute/redial path on `sess.client` / `tx.connection` /
`lifecycle.ctx`. (v2.2.2 — observed in cold-start hardware tests when
the PLC route was stale; symptom was duplicate "registering route" +
"FSM invalid transition" log noise and intermittent test failure.)
`dialAndStart` re-arms `ondrop` on each new Client it creates; the
retry block manually re-disarms after that re-arm.

#### Route naming convention

Route names on the PLC are unique by `routeName` string. Two
registrations with the same name but different (NetID, Address)
parameters can produce duplicate-name entries that confuse PLC
routing resolution (silent ADS RPC timeout). Callers that change
source IP across runs (DHCP, wifi↔ethernet, container vs bare metal)
SHOULD make the route name unique per source IP (e.g.,
`go-ads-{source-ip}`). The integration test helper does this
automatically when `ADS_HOST_IP` is set.

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

## Notification handle hygiene

The library actively prevents PLC notification-handle accumulation
(Beckhoff/ADS #268) via three complementary strategies introduced in
v2.2.1:

### 1. Orphan-Delete (`tryOrphanDelete`)

When the recvWorker decodes a `DeviceNotification` packet whose handle
is NOT in `activeNotifications`, the library schedules an asynchronous
best-effort Delete RPC against that handle. Catches handles leaked by
prior session/process crashes that the PLC is still firing for.

Guard rails (`cmd_notification.go`):

- **Lifecycle guards**: skip if Session is closing or actively reconnecting.
- **Throttle**: per-handle 60-second window prevents repeated Delete
  RPCs for the same noisy orphan. Throttle entry is committed only
  after sem acquire (otherwise sem-full would silently 60-second-lock
  a handle that never had an RPC attempted).
- **Bounded concurrency**: semaphore-bounded to 10 in-flight orphan
  Deletes (sized for Beckhoff's ~550-handle cap; one process should
  never need more concurrent cleanups).
- **GC**: orphanSeen map garbage-collects entries older than 5 minutes
  on each invocation.
- **Race re-check**: under `notifications.lock` immediately before
  firing the RPC, re-check that the handle is still NOT in
  `activeNotifications` — a concurrent `AddSymbolNotification` may
  legitimately have received this handle from the PLC between
  scheduling and firing. Skip the Delete if so.
- **ctx snapshot**: `lifecycle.ctx` read under `ctxMu.RLock` because
  Reconnect's `tearDownAndReset` replaces ctx under `ctxMu.Lock`.
- **Per-RPC timeout**: 5 seconds via `context.WithTimeout`.
- **Panic recover**: in the spawned goroutine — never crash the host.
- **WaitGroup tracking**: registered on `lifecycle.waitGroup` so
  `Close()` waits for in-flight orphan Deletes to complete.

Logging:

- Success (handle deleted PLC-side) → Info.
- Skipped (any guard rail tripped) → Debug.
- RPC failed with `0x714` / `0x715` (PLC already cleaned) → Debug.
- RPC failed with any other code → Warn (real failure operators must
  see).

### 2. Auto-reload pre-delete

`reloadSymbolsAndResubscribe` snapshots all current handles before the
reload, bumps `lastSubscribeNs`, wipes `activeNotifications` (all
atomically under `notifications.lock`), then `bestEffortDelete`s the
snapshot before re-subscribing. Prevents PLC retaining stale handles
across online-change boundaries.

### 3. Reconnect pre-delete

`Reconnect` performs the same snapshot+wipe+bestEffortDelete cycle on
the new TCP transport before re-subscribing. See [Reconnect sequence](#reconnect-sequence)
steps 1 and 7. `savedHandles` clears after the first cleanup pass so a
retry-loop iteration doesn't re-fire.

### Best-effort cleanup success codes

`isBestEffortDeleteSuccess(code)` returns true for:

| Code | Name | Meaning |
|------|------|---------|
| `0x000` | `NoErrors` | Actually deleted |
| `0x714` | `NotifyHandleInvalid` | Handle already gone PLC-side (route-idle, PLC reboot, prior cleanup) |
| `0x715` | `DeviceClientUnknown` | PLC dropped our client identity entirely (typical after TCP reset / reconnect); handles went with it |

Treating `0x715` as cleanup-success is a deliberate divergence from
Beckhoff's official AdsLib (which does not). The library's reconnect
path hits `0x715` routinely when PLC clears the just-severed-TCP's
endpoint; treating it as failure produces misleading WARN spam during
normal recovery with no functional benefit (the handle is gone either
way).

## Multi-Session on the same host (limitation)

TwinCAT PLCs enforce one active TCP slot per source IP, regardless of
source AMS NetID, AMS port, or route name (see [README §Limitations](README.md#limitations)
and Beckhoff/ADS #49, #72, jisotalo/ads-client #47). Two `Session`s
opened from the same source IP to the same PLC will evict each other.

The library provides three escape hatches:

- `WithLocalBindIP(ip)` — pin Session's outbound TCP source IP. Each
  Session pins to a distinct local IP via IP aliases on the host; PLC
  sees them as separate hosts. Validated at option time; invalid IPs
  log a Warn and fall through to OS-default routing.
- `WithSkipRouteRegistration()` — explicit opt-out from AddRoute/probe.
  Required when callers manage routes externally (TC3 UI pre-registered
  routes, or a local AMS router daemon front-ends the connection).
- `WithLocalAMS(AMSAddress{NetID, Port})` — override the source AMS
  identity in outgoing ADS headers. NetID defaults to auto-derivation
  from local TCP source IP; Port defaults to a random value in IANA
  dynamic range 32768-49151 (each Session = distinct AMS source
  identity, no manual coordination required).

For multi-process scenarios on a single host needing the same target
PLC, deploy a local AMS router daemon (Beckhoff's
`Beckhoff.TwinCAT.Ads.TcpRouter` or open-source `AmsRouterDaemon`) and
point all Sessions at `127.0.0.1:48898`. The library connects to any
TCP endpoint; the router multiplexes a single PLC connection.

---

## API Reference

### Session lifecycle (7)

| Method | Description |
|--------|-------------|
| `NewSession(ctx, AMSEndpoint{IP, Port, AMS}, opts...)` | Construct Session with options (no I/O) |
| `Connect(ctx)` | TCP dial + start goroutines + probe/register route. Local-mode via `WithLocalMode()` option |
| `Close() error` | Delete notifications, release handles, close TCP, terminal Closed. Implements `io.Closer` |
| `Reconnect(ctx)` | Re-establish after failure (auto or manual) |
| `AddRoute(ctx, routeName, username, password)` | Register an AMS route over UDP after construction |
| `IsDisconnected()` | True while transport is down (auto-reconnect may resolve) |
| `IsClosed()` | True after `Close()` or a terminal FSM transition (e.g. `SymbolVersionClose`) — Session cannot be reused |

### Reading and writing (4)

| Method | Description |
|--------|-------------|
| `ReadFromSymbol(ctx, name)` | Resolve symbol + read + parse to string |
| `WriteToSymbol(ctx, name, value)` | Resolve + parse string + write |
| `ReadMultipleSymbols(ctx, names)` | Batch read via SumRead with fallback |
| `WriteMultipleSymbols(ctx, values)` | Batch write via SumWrite with fallback |

### Notifications (4)

| Method | Description |
|--------|-------------|
| `AddSymbolNotification(ctx, name, maxDelay, cycleTime, mode, ch)` | Subscribe single symbol |
| `AddSymbolNotifications(ctx, configs, ch)` | Subscribe many (uses SumAdd internally) |
| `DeleteDeviceNotification(ctx, handle)` | Unsubscribe one |
| `SumDeleteDeviceNotification(ctx, handles)` | Unsubscribe many |

### Symbol discovery (8)

| Method | Description |
|--------|-------------|
| `LoadSymbols(ctx)` | Full discovery — single request (locks PLC task) |
| `LoadSymbolsSlow(ctx, cfg)` | Full discovery in chunks (PLC-friendly) |
| `LoadSymbolList(ctx, cfg)` | Symbol names only |
| `LoadDataTypes(ctx, cfg)` | Datatype definitions only |
| `BrowseSymbols(path)` | Navigate symbol hierarchy |
| `ListSymbols()` | Get full symbol map (requires LoadSymbols) |
| `GetSymbol(ctx, name)` | Get symbol; resolve on-demand if needed |
| `RefreshSymbols(ctx)` | Reload if version changed |
| `CheckSymbolVersion(ctx)` | Check version without reload |

### SessionOption

All options compose — no mutual exclusions. See [README.md → Connection
options](README.md#connection-options) for the user-facing reference;
this section covers internal behavior that does not belong in the user
quick-start.

- `WithBackoff(cfg)` shares its `BackoffConfig` validator with the
  caller-side `Validate()` method — invalid configs are rejected at
  option-application time with a Warn log; the default is kept.
- `WithStrictReconnect(maxAttempts)` only affects on-demand symbols
  (resolved via `GetSymbol` before reconnect). Symbols loaded via
  `LoadSymbols(Slow)` are not in scope.
- `WithSymbolVersionStrategy(SymbolVersionAutoReload)` gates the
  sliding-window cap (`WithMaxSymbolVersionReloadAttempts`,
  `WithSymbolVersionReloadWindow`) — these options are no-ops under
  `Close` or `Ignore` strategies.
- `WithLocalAMS(AMSAddress{Port:0})` keeps the default random AMS port
  (each Session = distinct AMS source identity per process). Pass a
  non-zero `Port` only when the deployment needs a stable AMS port
  (firewalled environments with port allow-lists, PLC-side route
  table pinning by exact port).
- `WithLocalBindIP(ip)` is parsed and validated at option time; invalid
  IPs log a Warn and leave the field nil (OS-default routing). The
  parsed `net.IP` is reused for every dial — no per-dial parse cost.
- `WithSkipRouteRegistration()` is an explicit alternative to omitting
  `WithRoute(...)`. Useful when an options chain is built uniformly
  for many Sessions and only one needs to opt out.

### Client (raw RPC, escape hatch)

`Client` is exposed for power users who need direct protocol access. Typical apps use `Session` only.

Lifecycle:

| Method | Description |
|--------|-------------|
| `Dial(...)` | Connect a raw Client |
| `Close() error` | Tear down sockets + goroutines |
| `SetOnDrop(fn)` | Register unexpected-drop callback |
| `SetNotificationHandler(fn)` | Register notification dispatcher |
| `ReleaseHandle(ctx, handle)` | Release a symbol handle |

Raw RPC (every method takes `ctx context.Context` as first arg):

| Method | Description |
|--------|-------------|
| `Read(ctx, group, offset, length)` | ADS Read |
| `Write(ctx, group, offset, data)` | ADS Write |
| `WriteRead(ctx, group, offset, readLen, data)` | ADS ReadWrite |
| `ReadState(ctx)` | ADS / device state |
| `ReadDeviceInfo(ctx)` | Device name + version |

Sum-batch (with fallback):

| Method | Description |
|--------|-------------|
| `SumRead(ctx, requests)` | 0xF084 → 0xF083 → individual |
| `SumWrite(ctx, requests)` | 0xF081 → individual |
| `SumAddDeviceNotification(ctx, requests)` | 0xF085 → individual + mode downgrade |
| `SumDeleteDeviceNotification(ctx, handles)` | 0xF086 → individual |

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
1. NewSession(ctx, AMSEndpoint{...}, opts...)   configure target, source, timeouts, options
       │
       ▼
2. Connect(ctx)                     TCP dial → probe route → register if needed
       │                            → start listen + transmitWorker
       │                            FSM: Constructed → Connecting → Connected
       │
3. [optional] LoadSymbols(ctx) /    full discovery
   LoadSymbolsSlow(ctx, cfg) /      chunked discovery (PLC-friendly)
   LoadSymbolList(ctx, cfg) /       names only
   LoadDataTypes(ctx, cfg)          datatypes only
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

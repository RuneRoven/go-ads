# go-ads Implementation Details

Library-specific implementation details, fallback strategies, and API reference for the go-ads library. For protocol-level documentation, see [PROTOCOL.md](PROTOCOL.md).

## Table of Contents

- [Architecture](#architecture)
- [Concurrency Model](#concurrency-model)
- [Sum Command Fallback Strategy](#sum-command-fallback-strategy)
- [Feature Detection Flags](#feature-detection-flags)
- [Transmission Mode Downgrade](#transmission-mode-downgrade)
- [Symbol Discovery](#symbol-discovery)
- [Reconnection](#reconnection)
- [API Reference](#api-reference)
- [Connection Lifecycle](#connection-lifecycle)
- [Data Types](#data-types)

---

## Architecture

```text
┌──────────────────────────────────────────────────┐
│                   Application                     │
│  ReadFromSymbol / WriteToSymbol / Notifications   │
├──────────────────────────────────────────────────┤
│                  ads.go / symbols.go              │
│         Symbol resolution, caching, parsing       │
├──────────────────────────────────────────────────┤
│               command*.go files                   │
│    Read, Write, ReadWrite, Notifications,         │
│    SumRead, SumWrite, SumNotification,            │
│    DeviceInfo, State                              │
├──────────────────────────────────────────────────┤
│                    comm.go                        │
│     sendRequest, listen, handleReceive,           │
│     transmitWorker (goroutines)                   │
├──────────────────────────────────────────────────┤
│                    ams.go                         │
│          AMS/TCP packet encoding                  │
├──────────────────────────────────────────────────┤
│                 connection.go                     │
│    TCP dial, connect, reconnect, close            │
├──────────────────────────────────────────────────┤
│                  route.go                         │
│        UDP route registration (port 48899)        │
└──────────────────────────────────────────────────┘
```

---

## Concurrency Model

Three goroutines per connection:

1. **listen** — reads packets from TCP, parses AMS headers, dispatches:
   - Notification packets (Command 8) handled directly
   - Response packets routed to waiting goroutine via channel keyed by invoke ID

2. **transmitWorker** — serializes outgoing packets from send channel to TCP

3. **handleReceive** — spawned per response, delivers data to the correct request channel

Request flow:
1. Allocate invoke ID (atomic counter)
2. Create response channel in `activeRequests` map
3. Send encoded packet via send channel
4. Block on response channel (with timeout)
5. Clean up map entry on return

---

## Sum Command Fallback Strategy

Follows Beckhoff's recommendation: try newest commands first, fall back on unsupported errors.

### SumRead — Three-tier fallback

```text
SumReadEx2 (0xF084)  →  SumReadEx (0xF083)  →  Individual Read
     TC3 only               TC2 + TC3            Always works
```

1. First call probes 0xF084. If unsupported error, tries 0xF083.
2. If 0xF083 also unsupported, falls back to individual reads.
3. The working command is cached in `sumReadCmd` (atomic.Uint32).
4. Subsequent calls skip the probe and use the cached command directly.

Both 0xF083 and 0xF084 return the same response format: `[N × (error(4), length(4))][data]`. A shared `parseSumReadResponse` handles both.

### SumWrite — Single fallback

```text
SumWrite (0xF081)  →  Individual Write
```

Does **not** fall back on transient errors. Writes are not idempotent — the PLC may have partially applied the batch. Only falls back on "not supported" errors.

### SumAddDeviceNotification — Single fallback

```text
SumAddDeviceNotification (0xF085)  →  Individual AddDeviceNotification
```

The fallback path automatically downgrades v2 transmission modes to v1 (see [Transmission Mode Downgrade](#transmission-mode-downgrade)).

### SumDeleteDeviceNotification — Single fallback

```text
SumDeleteDeviceNotification (0xF086)  →  Individual DeleteDeviceNotification
```

### Unsupported Error Detection

Fallback triggers when the PLC returns one of:
- `0x0701` — DeviceServiceNotSupported
- `0x0008` — UnknownCommandID
- `0x000B` — UnknownAdsCommand

Other errors (timeouts, device busy, etc.) propagate to the caller.

---

## Feature Detection Flags

Each sum command category has independent capability flags. All reset on reconnect.

| Flag                        | Type          | Values                                                    |
|-----------------------------|---------------|-----------------------------------------------------------|
| `sumReadCmd`                | atomic.Uint32 | 0=unchecked, `0xF084`=use Ex2, `0xF083`=use Ex, 1=no support |
| `sumWriteChecked`           | atomic.Bool   | Whether SumWrite has been probed                          |
| `sumWriteSupported`         | atomic.Bool   | Whether 0xF081 works                                     |
| `sumNotifChecked`           | atomic.Bool   | Whether notification commands have been probed            |
| `sumNotifSupported`         | atomic.Bool   | Whether 0xF085/0xF086 work                               |
| `chunkedDownloadChecked`    | atomic.Bool   | Whether chunked symbol download has been probed           |
| `chunkedDownloadSupported`  | atomic.Bool   | Whether offset-based reads work                           |

**Why separate flags:** Initially all sum commands shared a single flag pair. This caused bugs: SumRead succeeded on TC2 (using 0xF083), marking "supported", which prevented SumAddDeviceNotification (0xF085) from ever attempting its fallback. Each command category needs independent detection.

---

## Transmission Mode Downgrade

When the notification fallback path is active (TC2), v2 modes are automatically downgraded:

| Requested Mode     | Downgraded To   |
|--------------------|-----------------|
| ServerOnChange2 (6)| ServerOnChange (4) |
| ServerCycle2 (5)   | ServerCycle (3)    |
| All others         | Unchanged          |

This is necessary because TC2 **silently ignores** v2 modes without returning an error.

The downgrade only applies in the fallback path (individual AddDeviceNotification calls). When 0xF085 succeeds (TC3), the original mode is sent unchanged.

---

## Symbol Discovery

### Modes

| Mode | API | What's Downloaded | Capabilities |
|------|-----|-------------------|--------------|
| None (default) | `Connect()` | Nothing | On-demand reads/writes only |
| Symbol list | `LoadSymbolList()` | Symbol table (0xF00B) | Browse top-level names |
| Datatypes | `LoadDataTypes()` | Datatype table (0xF00E) | Struct child expansion |
| Full | `LoadSymbols()` | Both tables | Everything |
| Full chunked | `LoadSymbolsSlow()` | Both tables, in chunks | Everything, PLC-friendly |

### Chunked Downloads

`LoadSymbolsSlow()` and `LoadSymbolList()` download data in configurable chunks with delays between them, to minimize PLC real-time impact:

```go
conn.LoadSymbolsSlow(ads.SlowDiscoveryConfig{
    ChunkSize:  4096,                  // bytes per chunk (default: 4096)
    ChunkDelay: 100 * time.Millisecond, // delay between chunks (default: 100ms)
})
```

Falls back to single-request download if the PLC doesn't support offset-based chunked reads.

### Symbol Handles

Handles are acquired lazily via `GroupSymbolHandleByName` (0xF003) and cached. Released on `Close()` or before reconnect.

### Symbol Version

Read via `GroupSymbolVersion` (0xF008). Returns a uint8 that changes on every PLC program download. Used to detect stale symbols.

---

## Reconnection

Triggered automatically on TCP read/write errors (including keepalive failures). Controlled by `WithAutoReconnect(true)` (default).

TCP keepalive: idle=3s, interval=2s, count=5 → detection within ~13 seconds.

### Backoff Strategy

Reconnect uses a stepped backoff with configurable tiers (via `WithBackoff`). Resets to initial interval on each successful reconnect.

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

### Auto-Reconnect vs Manual

| Mode | Behavior |
|------|----------|
| `WithAutoReconnect(true)` (default) | `triggerReconnect()` launches reconnect goroutine automatically |
| `WithAutoReconnect(false)` | `triggerReconnect()` sets `disconnected=true` only. Caller must call `conn.Reconnect()` manually. `sendRequest` returns `ErrDisconnected` immediately |

### Event Callbacks

```go
ads.WithOnDisconnect(func() { /* runs in goroutine, must not block */ })
ads.WithOnReconnect(func() { /* runs in goroutine, must not block */ })
```

- `OnDisconnect`: fired in `triggerReconnect()` after setting `disconnected=true`. Not fired if `Close()` was called.
- `OnReconnect`: fired at end of successful `Reconnect()` after generation counter increment. Not fired if `Close()` was called during reconnect.

### Reconnect Sequence

1. Close old TCP connection
2. Stop listen and transmitWorker goroutines (`waitGroup.Wait()`)
3. Reset all capability flags:
   ```
   sumReadCmd = 0
   sumWriteChecked = false
   sumNotifChecked = false
   chunkedDownloadChecked = false
   ```
4. Retry TCP dial with stepped backoff (see above)
5. Re-register AMS route if configured (see [Smart Route Registration](#smart-route-registration))
6. Re-load symbols based on discovery mode:
   - Full discovery → re-download full symbol table
   - On-demand only → re-resolve only previously accessed symbols (graceful skip on missing)
   - No symbols → read symbol version only
7. Filter notification configs — remove entries for symbols no longer available
8. Re-subscribe all remaining notifications
9. Increment `reconnectGeneration` counter (see [Stale Handle Detection](#stale-handle-detection))
10. Fire `OnReconnect` callback

### Smart Route Registration

Both `Connect()` and `Reconnect()` use a probe-first approach for route registration:

1. TCP connect + start goroutines
2. **Probe:** send `GetSymbolVersion()` (lightweight ADS command)
3. Probe OK → route exists, skip credential registration
4. Probe fail → register route with credentials via UDP, TCP reconnect after
5. After 3 consecutive probe failures → skip probe, always register (fallback)

`WithForceRouteRegistration()` bypasses probing entirely — always registers with credentials.

### Stale Handle Detection

A `reconnectGeneration` counter (atomic uint64) increments on each successful reconnect. Symbol handles acquired before a reconnect may be invalid (PLC assigns new handles after program reload).

`ReadFromSymbol`, `ReadMultipleSymbols`, `WriteToSymbol`, and `WriteMultipleSymbols` capture the generation before performing I/O. If an error occurs and the generation has changed (reconnect happened mid-operation), the operation is automatically retried once with fresh handles.

```
gen = reconnectGeneration.Load()
handle = GetSymbol(name)     // may return stale handle
data = Read(handle)          // fails if handle is stale
if err != nil && reconnectGeneration.Load() != gen:
    retry once with fresh handle
```

No infinite recursion: the retry captures a new generation value, and only retries again if *another* reconnect happened during the retry.

### Strict Reconnect Mode

By default, missing on-demand symbols after reconnect are skipped with a warning (e.g., after PLC online change removes a variable). `WithStrictReconnect(maxAttempts)` changes this:

- Missing symbol → reconnect considered failed
- `maxAttempts = 0` → fail immediately on first missing symbol
- `maxAttempts = N` → retry up to N times, then close the connection
- Failure counter resets on successful reconnect (all symbols resolved)

---

## API Reference

### Connection Management

| Method | Description |
|--------|-------------|
| `NewConnection(ctx, ip, port, netid, amsPort, localNetID, localPort, timeout, opts...)` | Configure connection with options |
| `Connect(local bool)` | TCP dial + start goroutines + probe/register route |
| `Close()` | Delete notifs, release handles, close TCP |
| `Reconnect()` | Re-establish after failure (called automatically or manually) |
| `IsDisconnected()` | Check connection state |

### Connection Options

All options are composable — no mutual exclusions. If omitted, defaults apply.

| Option | Default (if omitted) | Description |
|--------|----------------------|-------------|
| `WithRoute(name, user, pass)` | No route registration | Register AMS route via UDP (probe-first, fallback to credentials) |
| `WithBackoff(cfg BackoffConfig)` | `DefaultBackoffConfig()`: 1s×3, 5s×3, 15s×4, 30s cap | Stepped reconnect backoff tiers. Used in both auto and manual `Reconnect()` |
| `WithAutoReconnect(bool)` | `true` — reconnects automatically on TCP errors | When `false`, `triggerReconnect()` only sets `disconnected=true`. `sendRequest` returns `ErrDisconnected`. Caller must call `Reconnect()` manually |
| `WithOnDisconnect(func())` | None | Callback fired when disconnect detected (goroutine, must not block). Fires regardless of auto/manual mode. Not fired after `Close()` |
| `WithOnReconnect(func())` | None | Callback fired after successful reconnect (goroutine, must not block). Fires regardless of auto/manual mode. Not fired after `Close()` |
| `WithStrictReconnect(maxAttempts)` | Graceful: missing on-demand symbols warned + removed | Fail reconnect if previously-resolved symbols are missing. `0` = fail immediately. `N > 0` = retry N times then close connection. Only affects on-demand symbols (resolved via `GetSymbol` before reconnect) |
| `WithForceRouteRegistration()` | Probe first (try ADS command, register only on failure) | Always register route with credentials. **Requires `WithRoute`** — no-op without it |
| `WithHostIP(ip)` | Derived from AMS NetID (first 4 bytes) | IP the PLC uses to reach this client (Docker/VPN/NAT). **Requires `WithRoute`** — only affects route registration |
| `WithLogger(logger)` | `slog.Default()` | Custom `*slog.Logger` for structured logging |

### Symbol Discovery

| Method | Description |
|--------|-------------|
| `LoadSymbols()` | Full discovery (locks PLC task) |
| `LoadSymbolsSlow(cfg)` | Chunked full discovery |
| `LoadSymbolList(cfg)` | Symbol names only |
| `LoadDataTypes(cfg)` | Datatype definitions only |
| `BrowseSymbols(path)` | Navigate symbol hierarchy |
| `ListSymbols()` | Get full symbol map (requires LoadSymbols) |
| `GetSymbol(name)` | Get symbol, resolve on-demand if needed |
| `RefreshSymbols()` | Reload if version changed |
| `CheckSymbolVersion()` | Check version without reload |

### Reading and Writing

| Method | Description |
|--------|-------------|
| `ReadFromSymbol(name)` | Read + parse to string (50ms cache) |
| `WriteToSymbol(name, value)` | Parse string + write |
| `ReadMultipleSymbols(names)` | Batch read via SumRead |
| `Read(group, offset, length)` | Raw ADS Read |
| `Write(group, offset, data)` | Raw ADS Write |
| `WriteRead(group, offset, readLen, data)` | Raw ADS ReadWrite |

### Batch Operations

| Method | Description |
|--------|-------------|
| `SumRead(requests)` | Batch read: 0xF084 → 0xF083 → individual |
| `SumWrite(requests)` | Batch write: 0xF081 → individual |
| `SumAddDeviceNotification(requests)` | Batch subscribe: 0xF085 → individual + mode downgrade |
| `SumDeleteDeviceNotification(handles)` | Batch unsubscribe: 0xF086 → individual |

### Notifications

| Method | Description |
|--------|-------------|
| `AddSymbolNotification(name, maxDelay, cycleTime, mode, ch)` | Subscribe single symbol |
| `AddSymbolNotifications(configs, ch)` | Subscribe multiple (uses SumAdd internally) |

### Device Info

| Method | Description |
|--------|-------------|
| `ReadDeviceInfo()` | Device name + version |
| `ReadState()` | ADS state + device state |
| `AddRemoteRoute(...)` | Register route via UDP |

---

## Connection Lifecycle

```text
1. NewConnection(..., opts...)    — configure target, source, timeouts, options
       │
2. Connect(false)                 — TCP dial → probe route → register if needed
       │                            → start listen + transmitWorker goroutines
       │
3. [LoadSymbols()]                — optional: full discovery
   [LoadSymbolsSlow(cfg)]         — optional: chunked discovery
   [LoadSymbolList(cfg)]          — optional: symbol names only
   [LoadDataTypes(cfg)]           — optional: struct expansion
       │
4. ReadFromSymbol(...)            — on-demand symbol resolution if needed
   WriteToSymbol(...)               (stale handle auto-retry on reconnect)
   AddSymbolNotifications(...)
   BrowseSymbols(path)            — requires step 3
       │
   ┌───┴──── (TCP error) ─────────────────────┐
   │  OnDisconnect callback fired              │
   │  if autoReconnect=true:                   │
   │    Reconnect() with stepped backoff       │
   │    - reset capability flags               │
   │    - probe/register route                 │
   │    - re-resolve symbols (graceful skip)   │
   │    - filter + re-subscribe notifications  │
   │    - increment reconnectGeneration        │
   │    - OnReconnect callback fired           │
   │  if autoReconnect=false:                  │
   │    disconnected=true, caller must call    │
   │    conn.Reconnect() manually              │
   └───────────────────────────────────────────┘
       │
5. Close()                        — cleanup + shutdown
```

---

## Data Types

Parsed PLC types:

| PLC Type       | Size    | Go Representation          |
|----------------|---------|----------------------------|
| BOOL           | 1 byte  | "true" / "false"           |
| BYTE, USINT    | 1 byte  | Unsigned 0-255             |
| SINT           | 1 byte  | Signed -128 to 127         |
| UINT, WORD     | 2 bytes | Unsigned 0-65535           |
| INT            | 2 bytes | Signed -32768 to 32767     |
| UDINT, DWORD   | 4 bytes | Unsigned 32-bit            |
| DINT           | 4 bytes | Signed 32-bit              |
| REAL           | 4 bytes | 32-bit float               |
| LREAL          | 8 bytes | 64-bit float               |
| STRING         | varies  | Null-terminated string     |
| TIME           | 4 bytes | `HH:MM:SS.sss`            |
| TOD            | 4 bytes | `HH:MM`                    |
| DATE           | 4 bytes | `YYYY-MM-DD`               |
| DT             | 4 bytes | `YYYY-MM-DD HH:MM:SS`     |

Structs, function blocks, and arrays are recursively expanded and serialized as JSON.

Enum types use their underlying base type. TC2 flattens enums to INT/DINT in the symbol table. TC3 preserves full enum metadata.

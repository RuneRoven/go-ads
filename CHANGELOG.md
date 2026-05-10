# Changelog

All notable changes to this project will be documented in this file.

This project uses [Conventional Commits](https://www.conventionalcommits.org/) and
[go-semantic-release](https://github.com/go-semantic-release/semantic-release) for
automated versioning and changelog generation.

## v2.1.0 — Layered architecture (breaking)

The single `Connection` god-type is split into two distinct public types:

- **`Client`** — thin Beckhoff-equivalent RPC layer. One TCP socket, raw AMS
  framing, request multiplexing, no cache, no notification persistence, no
  reconnect. Constructed via `Dial`. Suitable for one-shot consumers (CLI
  tools, web ADS browsers).
- **`Session`** — managed wrapper. Owns a `*Client` and adds the symbol
  cache, name-based read/write, persistent notifications with auto-resubscribe,
  auto-reconnect with backoff, FSM-based lifecycle, and lifecycle callbacks.
  Constructed via `NewSession`.

`Session` does NOT embed `Client` — there is no method promotion. Callers
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
  - `SymbolVersionAutoReload` (default) — re-discover symbols + resubscribe
    notifications on detection.
  - `SymbolVersionClose` — terminate the session on detection (fires
    `OnDisconnect`, then `Close()`).
  - `SymbolVersionIgnore` — surface the PLC error to the calling op and
    flag surviving notification handles' next sample with `Update.Stale=true`
    (one-shot, consumed on first delivery).
- New `Update` fields `Stale bool` + `Reason string` (R-NOT-016). Reason
  values are stable strings exported as `Reason*` constants.
- Optional callback via `WithOnSymbolVersionChanged(fn func(reason string))`
  fires once per detection. Required signal under `SymbolVersionIgnore` to
  observe symbol-removal events (the dead handle's user channel goes silent
  — no terminal Update is delivered).
- Reload cap: `WithMaxSymbolVersionReloadAttempts` (default 3) within
  `WithSymbolVersionReloadWindow` (default 60s) prevents runaway reload
  loops under recurring online-change conditions (R-CACHE-013). On cap
  exhaustion the strategy degrades to Ignore for that detection and the
  callback fires with `ReasonReloadCapExhausted`.
- Hardware-validated against TwinCAT 3 v3.1.4024.
- Migration: the `Update` struct gained two trailing fields. Field-named
  struct literals are unaffected; positional literals (rare) require an
  update.

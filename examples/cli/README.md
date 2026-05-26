# go-ads example CLI

Single CLI binary that demonstrates both library layers. The first prompt asks
which demo to run; pass `ADS_DEMO=session` or `ADS_DEMO=client` to skip the
prompt for scripted invocations.

## Demos

### 1. Session demo (interactive REPL)

Boots an `ads.Session` (cache, auto-reconnect, online-change handling,
persistent notifications, route registration) and drops into a REPL.

Live event hooks while the REPL runs:

- `WithOnDisconnect` / `WithOnReconnect` print transport state changes.
- `WithSymbolVersionStrategy(SymbolVersionAutoReload)` +
  `WithOnSymbolVersionChanged` print `[online-change] reason=...` for DP-1
  events.
- Notification updates print on a background goroutine; non-nil
  `Update.Stale` is rendered prominently as `[notify] *STALE* ...`
  carrying `Update.Stale.Reason`.

REPL commands:

| Command | Action |
|---|---|
| `list [prefix]` | List cached symbols, optionally filtered |
| `browse [path]` | `Session.BrowseSymbols` at path (default root) |
| `read <symbol>` | `Session.ReadFromSymbol` |
| `write <symbol> <value>` | `Session.WriteToSymbol` (library auto-parses value) |
| `info <symbol>` | `Session.GetSymbol` — DataType, Length, Group, Offset, Comment |
| `sub <symbol>` | `AddSymbolNotification` (background prints updates) |
| `unsub <handle>` | `DeleteDeviceNotification` |
| `reload` | `Session.RefreshSymbols` — fast single-shot reload (default `LoadSymbols`) |
| `slow-load [chunk] [delay]` | `Session.LoadSymbolsSlow` — chunked download for large/slow PLCs (defaults: 4096 bytes, 100ms) |
| `state` | `IsClosed`, `IsDisconnected`, cache count, active subs |
| `help` / `?` | Command list |
| `quit` / `exit` | Graceful shutdown |

`SIGINT` / `SIGTERM` close stdin and trigger graceful shutdown
(`Session.Close` runs from a `defer`).

### 2. Client demo (raw RPC)

Demonstrates the `ads.Client` escape hatch — no cache, no reconnect:

- `Dial` + `WithOnDrop` callback.
- `ReadDeviceInfo`, `ReadState`.
- Raw `GetSymbolInfoByName` for protocol-level symbol resolution.
- `WriteRead(GroupSymbolHandleByName)` to acquire a handle, raw
  `Read(GroupSymbolValueByHandle, ...)` for the value, and explicit
  `Write(GroupSymbolReleaseHandle, ...)` cleanup.

## Environment variables

| Variable | Default | Notes |
|----------|---------|-------|
| `ADS_PLC_IP` | `192.168.1.100` | Target PLC IPv4 |
| `ADS_TARGET_AMS` | `5.1.2.3.1.1` | Target AMS NetID |
| `ADS_TARGET_PORT` | `851` | Target AMS port |
| `ADS_LOCAL_AMS` | `auto` | Local AMS NetID (`auto` derives from local IP) |
| `ADS_LOCAL_PORT` | `10500` | Local AMS source port |
| `ADS_SYMBOL_NAME` | `MAIN.bCounter` | Symbol used by the client demo (session REPL accepts any) |
| `ADS_ROUTE_USER` | _(unset)_ | If set, register an AMS route on the PLC |
| `ADS_ROUTE_PASS` | _(unset)_ | Paired with `ADS_ROUTE_USER` |
| `ADS_ROUTE_NAME` | `go-ads-example` | Route display name on PLC |
| `ADS_DEMO` | _(unset)_ | `session` or `client` — skip the interactive prompt |

## Run

```bash
cd examples/cli
ADS_PLC_IP=192.168.1.100 \
ADS_TARGET_AMS=5.1.2.3.1.1 \
ADS_SYMBOL_NAME=MAIN.bCounter \
go run .
```

## Choosing a demo

- **Session** — almost everything. Handles reconnect, online-change, symbol
  cache, persistent notifications. Default for production use. Use the REPL
  for ad-hoc browsing, reads, writes, and notification testing.
- **Client** — raw protocol inspection, custom transport tooling, or anywhere
  you need to bypass the cache (e.g. ADS browser, debugger, transient one-shot
  CLI tooling). Single-shot.

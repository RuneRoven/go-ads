# go-ads

A pure Go library for communicating with Beckhoff TwinCAT PLCs using the ADS (Automation Device Specification) protocol.

> **v2.1 breaking change**: the previous `Connection` type has been renamed to `Session`, and the raw RPC surface has been split off into a separate `Client` type. The constructor is now `NewSession` (no context argument). See [Two layers](#two-layers) for the new architecture and the migration story. v2.0.x is deprecated.

## Features

- Connect to TwinCAT 2 and TwinCAT 3 PLCs over TCP
- Two-layer API: a Beckhoff-equivalent raw `Client` for one-shot consumers and a managed `Session` that adds caching, notification persistence, and auto-reconnect
- Read/write PLC symbols by name
- Batch read multiple symbols in a single round-trip (SumRead)
- Subscribe to symbol change notifications (single and batch)
- Automatic symbol table and datatype discovery
- Auto-reconnect with configurable backoff and notification re-subscribe
- Smart AMS route registration (probe-first, credential fallback)
- Stale handle detection across reconnects (epoch counter)
- Disconnect/reconnect event callbacks
- Symbol version change detection and refresh

## Install

```bash
go get github.com/RuneRoven/go-ads/v2
```

## Quick start

```go
package main

import (
	"fmt"
	"time"

	ads "github.com/RuneRoven/go-ads/v2"
)

func main() {
	sess, _ := ads.NewSession("192.168.1.100", 48898, "5.1.2.3.1.1", 851, "auto", 10500, 5*time.Second,
		ads.WithRoute("my-route", "Administrator", "1"),
	)
	if err := sess.Connect(false); err != nil {
		panic(err)
	}
	defer sess.Close()

	// Symbols are resolved on-demand — no full discovery needed.
	value, _ := sess.ReadFromSymbol("MAIN.myVar")
	fmt.Println("Value:", value)

	// Optional: load full symbol table for listing / struct access.
	sess.LoadSymbols()
	symbols, _ := sess.ListSymbols()
	fmt.Printf("Total symbols: %d\n", len(symbols))
}
```

## Two layers

The library exposes two types. Pick the one that fits your consumer.

### Client — raw RPC (Beckhoff-equivalent)

`Client` is a thin wrapper around one TCP connection. Each method is a single ADS round-trip. No symbol cache, no notification persistence, no auto-reconnect. If the transport drops, every subsequent call returns `ErrTransportClosed` and the caller reconstructs a new `Client`.

Use this for one-shot consumers — CLI tools, web ADS browsers doing a quick probe, scripts that send a single command and exit.

```go
client, err := ads.Dial(
    "192.168.1.100", 48898,
    ads.AMSAddress{NetID: [6]byte{5, 1, 2, 3, 1, 1}, Port: 851},
    ads.AMSAddress{NetID: [6]byte{192, 168, 1, 50, 1, 1}, Port: 30000},
    5*time.Second,
    ads.WithClientLogger(slog.Default()),
)
if err != nil {
    panic(err)
}
defer client.Close()

info, _ := client.ReadDeviceInfo()
fmt.Printf("Device: %s\n", string(info.DeviceName[:]))

handle, _ := client.GetHandleByName("MAIN.myVar")
defer client.ReleaseHandle(handle)

data, _ := client.Read(uint32(ads.GroupSymbolValueByHandle), handle, 4)
fmt.Printf("Value bytes: %v\n", data)
```

### Session — managed (long-running consumers)

`Session` wraps a `Client` and adds the value-add for long-running consumers: symbol cache, name-based read/write, notification persistence with auto-resubscribe, auto-reconnect with backoff, online-change handling, lifecycle callbacks. `Session` does NOT promote Client methods — call `sess.ReadFromSymbol(name)` for cache-aware access; raw consumers construct a separate `*Client`.

Use this for daemons, message brokers, or anything that should survive a network blip without manual intervention.

```go
sess, _ := ads.NewSession("192.168.1.100", 48898, "5.1.2.3.1.1", 851, "auto", 10500, 5*time.Second,
    ads.WithRoute("my-route", "Administrator", "1"),
    ads.WithAutoReconnect(true),
    ads.WithOnReconnect(func() { log.Println("back online") }),
)
sess.Connect(false)
defer sess.Close()

// Cache-aware read (resolves on-demand, then caches for the connection's lifetime).
value, _ := sess.ReadFromSymbol("MAIN.myVar")

// Persistent subscription (resubscribes automatically after a reconnect).
ch := make(chan *ads.Update, 64)
sess.AddSymbolNotification("MAIN.bCounter", 100*time.Millisecond, 100*time.Millisecond,
    ads.TransModeServerOnChange, ch)
for update := range ch {
    fmt.Println(update.Variable, update.Value)
}
```

## Example CLI

A ready-to-run example is included:

```bash
cd examples/simple
go run . -ip 192.168.1.100 -netid 5.1.2.3.1.1 -list
```

See `examples/simple/main.go` for all available flags.

## Connection options

All options are passed to `NewSession` and have sensible defaults:

```go
sess, _ := ads.NewSession(ip, 48898, netid, 851, "auto", 10500, 5*time.Second,
    // Route registration — probe first, register with credentials only if needed
    ads.WithRoute("my-route", "Administrator", "1"),

    // Custom logger (default: slog.Default())
    ads.WithLogger(myLogger),

    // Reconnect backoff strategy (default: 1s×3, 5s×3, 15s×4, then 30s cap)
    ads.WithBackoff(ads.BackoffConfig{
        InitialInterval: 1 * time.Second,
        InitialAttempts: 3,
        MidInterval:     5 * time.Second,
        MidAttempts:     3,
        SlowInterval:    15 * time.Second,
        SlowAttempts:    4,
        MaxInterval:     30 * time.Second,
    }),

    // Disable auto-reconnect (caller must call sess.Reconnect() manually)
    ads.WithAutoReconnect(false),

    // Event callbacks (run in goroutine, must not block)
    ads.WithOnDisconnect(func() { log.Println("disconnected") }),
    ads.WithOnReconnect(func() { log.Println("reconnected") }),

    // Fail reconnect if previously-resolved symbols are missing (e.g., after PLC online change)
    // maxAttempts=3 means retry 3 times before closing the connection
    ads.WithStrictReconnect(3),

    // Always register route with credentials (skip probe, for non-persistent route environments)
    ads.WithForceRouteRegistration(),

    // Override callback IP for Docker/VPN/NAT (default: derived from AMS NetID)
    ads.WithHostIP("192.168.1.50"),
)
```

| Option | Default | Description |
|--------|---------|-------------|
| `WithRoute(name, user, pass)` | No route | Register AMS route via UDP. Probes first, registers only if needed |
| `WithBackoff(cfg)` | 1s×3, 5s×3, 15s×4, 30s cap | Stepped reconnect backoff with configurable tiers |
| `WithAutoReconnect(bool)` | `true` | Auto-reconnect on TCP errors. When `false`, sets disconnected flag only — caller must call `Reconnect()` |
| `WithOnDisconnect(fn)` | None | Callback on disconnect detection |
| `WithOnReconnect(fn)` | None | Callback after successful reconnect |
| `WithStrictReconnect(n)` | Graceful skip (missing symbols warned + removed) | Fail if on-demand symbols missing after reconnect. `n` = max retry attempts before closing |
| `WithForceRouteRegistration()` | Probe first | Always register route with credentials (skip probe). Requires `WithRoute` |
| `WithHostIP(ip)` | Derived from AMS NetID | IP the PLC uses to reach this client (Docker/VPN/NAT). Requires `WithRoute` |
| `WithLogger(logger)` | `slog.Default()` | Custom structured logger |

### Combining options

All options are composable — no mutual exclusions. Some require others to have effect:

| Option | Requires | Notes |
|--------|----------|-------|
| `WithForceRouteRegistration()` | `WithRoute()` | No-op without route credentials |
| `WithHostIP()` | `WithRoute()` | Only affects route registration packet |
| `WithBackoff()` | — | Used in both auto and manual `Reconnect()` |
| `WithStrictReconnect()` | — | Only affects on-demand symbols (resolved via `GetSymbol` before reconnect) |
| `WithOnDisconnect()` / `WithOnReconnect()` | — | Fire regardless of auto/manual reconnect mode |
| `WithAutoReconnect(false)` | — | Backoff still applies when caller invokes `Reconnect()` manually |

## Notifications and backpressure

Notification delivery is **non-blocking**: if the receiver's channel is full, the notification is dropped and a warning is logged. This prevents goroutine accumulation and ensures the receive pipeline is never stalled by a slow consumer.

**Sizing your channel buffer:**

```go
// Small buffer — fine for low-frequency or on-change notifications
ch := make(chan *ads.Update, 64)

// Large buffer — recommended for high-frequency cyclic notifications
// (e.g. 100 symbols × 10ms cycle = 10,000 notifications/sec)
ch := make(chan *ads.Update, 4096)
```

If you see `"notification dropped (channel full)"` warnings, either:
1. Increase the channel buffer size
2. Consume notifications faster (dedicated drain goroutine)
3. Reduce the notification cycle time on the PLC side

## Process image I/O (experimental)

> **Warning:** Direct process image access bypasses the symbol table and writes raw bytes to I/O memory. Writing to the wrong offset can cause unexpected physical output changes (motors, valves, actuators). The PLC runtime may overwrite your changes on the next scan cycle. **For normal operation, use symbol-based access (`sess.ReadFromSymbol`/`sess.WriteToSymbol`).**

Process image methods live on `*Client` only — they are pure wire ops with no cache or notification dependency. Session users who need them construct a raw `*Client` via `Dial` alongside their `Session` (or use `Dial` exclusively if they never need cache-aware features).

```go
// Read 4 bytes from input image at byte offset 0
data, _ := client.ReadProcessInput(0, 4)

// Read a single input bit (byte 2, bit 3)
val, _ := client.ReadProcessInputBit(2, 3)

// Write to output image (use with extreme caution)
client.WriteProcessOutput(10, []byte{0xFF})
client.WriteProcessOutputBit(10, 0, true)

// Query input image size
size, _ := client.ReadProcessInputSize()
```

## Development

### Prerequisites

- Go 1.26+
- [golangci-lint](https://golangci-lint.run/docs/welcome/install/) v2.11+
- [gofumpt](https://github.com/mvdan/gofumpt) (`go install mvdan.cc/gofumpt@latest`)

### Before pushing

Run all checks locally — this mirrors what CI does on every PR:

```bash
make all        # format, lint, vet, test
```

Or run individual targets:

```bash
make fmt        # format code (gofumpt + goimports)
make lint       # run golangci-lint
make vet        # run go vet
make test       # run tests
make test-race  # run tests with race detector
make test-cover # run tests with coverage report
make build      # build all packages
```

### Network requirements

| Port | Protocol | Direction | Purpose | Configurable |
|------|----------|-----------|---------|--------------|
| 48898 | TCP | Client → PLC | ADS data (commands, responses, notifications) | Yes (`NewSession` port param) |
| 48899 | UDP | Client → PLC | AMS route registration (`WithRoute`) | No (Beckhoff fixed) |

Both ports must be open in firewalls between the client and PLC. If only TCP 48898 is open,
ADS works with pre-existing routes but `WithRoute` cannot register new ones.

### Docker / Container deployment

Only route credentials are needed — `WithHostIP` and `ADS_LOCAL_AMS` are optional:

```go
sess, _ := ads.NewSession(plcIP, 48898, targetAMS, 851, "auto", 10500, 5*time.Second,
    ads.WithRoute("my-route", "Administrator", "password"),
)
sess.Connect(false)
```

The PLC stores the UDP source IP (post-NAT) for the route, not the `computerName` from the
packet. Auto-derived NetID from the container IP works because ADS uses the existing TCP
connection for all communication including notifications.

> **Note:** Tested with TwinCAT 3 via Colima on macOS. More extensive testing across
> TwinCAT versions and container runtimes is ongoing. See `PROTOCOL.md` for details.

### Integration tests

Integration tests require a real Beckhoff PLC on the network. They are gated behind a build tag and skipped by default.

**Environment file format** (`.env.integration.XXX`):

```bash
## Required
ADS_PLC_IP=192.168.0.1          # PLC IP address
ADS_TARGET_AMS=5.0.0.1.1.1      # PLC AMS NetID
ADS_TARGET_PORT=851              # AMS port (851 for TC3, 801 for TC2)

## Optional — auto-derived if omitted
ADS_LOCAL_AMS=192.168.0.100.1.1 # Local AMS NetID (default: auto from local IP)
ADS_HOST_IP=192.168.0.100       # IP the PLC uses to reach us

## Optional — for auto-creating AMS route
ADS_ROUTE_USER=Administrator     # PLC admin username
ADS_ROUTE_PASS=1                 # PLC admin password

## Optional — test-specific symbol names
ADS_WRITE_BOOL=GVL_WriteTest.bWriteBool
ADS_READ_COUNTER=GVL_ProcessData.nMasterCycleCounter
```

**Running integration tests:**

```bash
# Source env and run all integration tests against a PLC
set -a && source .env.integration && set +a
go test -tags integration -v -timeout 60s

# Run a specific test
go test -tags integration -run TestIntegrationConnect -v -timeout 30s
```

**Symbol browse test** (`browse_test.go`) — connects to a PLC, loads all symbols slowly (chunked reads with delays to avoid disrupting real-time tasks), and writes a `.var` file documenting every symbol:

```bash
set -a && source .env.integration && set +a
go test -tags integration -run TestBrowseAllSymbols -v -timeout 60s
# → writes plc_192_168_0_1.var (filename derived from ADS_PLC_IP)
```

The `.var` files list all symbols with name, datatype, size, index group, offset, and comment.

### CI

CI runs automatically on pull requests to `main` with 4 parallel jobs: lint, test, test-race, and build. All must pass before merging.

## TODO

- **Enum string resolution**: Add an option to return enum constant names (e.g. `"RUNNING"`) instead of numeric values (e.g. `"2"`). Requires parsing enum constant values from the datatype table's extra data. Only possible for TC3 non-strict enums; TC3 strict enums and TC2 do not expose constant names in the datatype table.
- **TCP-based route registration**: Investigate whether AMS routes can be created via ADS system service commands over the existing TCP connection (AMS port 10000) instead of the UDP protocol (port 48899). This would eliminate the UDP 48899 firewall requirement and simplify deployment in locked-down networks. The Beckhoff `TcAmsRemoteMgr` service handles UDP route requests — a TCP equivalent may exist via the ADS system service but is unconfirmed.

## License

MIT — see [LICENSE](LICENSE) for details.

Original ADS protocol implementation based on [go-native-ads](https://gitlab.com/xilix-systems-llc/go-native-ads) by Bob Klosinski.

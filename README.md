# go-ads

A pure Go library for communicating with Beckhoff TwinCAT PLCs using the ADS (Automation Device Specification) protocol.

## Features

- Connect to TwinCAT 2 and TwinCAT 3 PLCs over TCP
- Read/write PLC symbols by name
- Batch read multiple symbols in a single round-trip (SumRead)
- Subscribe to symbol change notifications (single and batch)
- Automatic symbol table and datatype discovery
- Auto-reconnect with notification re-subscribe
- Symbol version change detection and refresh

## Install

```bash
go get github.com/RuneRoven/go-ads/v2
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"time"

	ads "github.com/RuneRoven/go-ads/v2"
)

func main() {
	ctx := context.Background()
	conn, _ := ads.NewConnection(ctx, "192.168.1.100", 48898, "5.1.2.3.1.1", 851, "auto", 10500, 5*time.Second)
	conn.Connect(false)
	defer conn.Close()

	info, _ := conn.ReadDeviceInfo()
	fmt.Printf("Device: %s\n", string(info.DeviceName[:]))

	// Symbols are resolved on-demand — no full discovery needed
	value, _ := conn.ReadFromSymbol("MAIN.myVar")
	fmt.Println("Value:", value)

	// Optional: load full symbol table for listing/struct access
	conn.LoadSymbols()
	symbols, _ := conn.ListSymbols()
	fmt.Printf("Total symbols: %d\n", len(symbols))
}
```

## Example CLI

A ready-to-run example is included:

```bash
cd examples/simple
go run . -ip 192.168.1.100 -netid 5.1.2.3.1.1 -list
```

See `examples/simple/main.go` for all available flags.

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

> **Warning:** Direct process image access bypasses the symbol table and writes raw bytes to I/O memory. Writing to the wrong offset can cause unexpected physical output changes (motors, valves, actuators). The PLC runtime may overwrite your changes on the next scan cycle. **For normal operation, use symbol-based access (`ReadFromSymbol`/`WriteToSymbol`).**

Convenience methods for diagnostics, commissioning, and I/O wiring verification:

```go
// Read 4 bytes from input image at byte offset 0
data, _ := conn.ReadProcessInput(0, 4)

// Read a single input bit (byte 2, bit 3)
val, _ := conn.ReadProcessInputBit(2, 3)

// Write to output image (use with extreme caution)
conn.WriteProcessOutput(10, []byte{0xFF})
conn.WriteProcessOutputBit(10, 0, true)

// Query input image size
size, _ := conn.ReadProcessInputSize()
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
| 48898 | TCP | Client → PLC | ADS data (commands, responses, notifications) | Yes (`NewConnection` port param) |
| 48899 | UDP | Client → PLC | AMS route registration (`WithRoute`) | No (Beckhoff fixed) |

Both ports must be open in firewalls between the client and PLC. If only TCP 48898 is open,
ADS works with pre-existing routes but `WithRoute` cannot register new ones.

### Docker / Container deployment

Only route credentials are needed — `WithHostIP` and `ADS_LOCAL_AMS` are optional:

```go
conn, _ := ads.NewConnection(ctx, plcIP, 48898, targetAMS, 851, "auto", 10500, 5*time.Second,
    ads.WithRoute("my-route", "Administrator", "password"),
)
conn.Connect(false)
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

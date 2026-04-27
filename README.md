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

## Development

### Prerequisites

- Go 1.21+
- [golangci-lint](https://golangci-lint.run/docs/welcome/install/) v2+
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

### CI

CI runs automatically on pull requests to `main` with 4 parallel jobs: lint, test, test-race, and build. All must pass before merging.

## License

MIT — see [LICENSE](LICENSE) for details.

Original ADS protocol implementation based on [go-native-ads](https://gitlab.com/xilix-systems-llc/go-native-ads) by Bob Klosinski.

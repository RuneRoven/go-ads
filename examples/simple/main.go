package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/chzyer/readline"

	"gopkg.in/yaml.v3"

	ads "github.com/RuneRoven/go-ads/v2"
)

type Config struct {
	IP         string      `yaml:"ip"`
	NetID      string      `yaml:"netid"`
	Port       int         `yaml:"port"`
	AMSPort    int         `yaml:"amsport"`
	LocalNetID string      `yaml:"localnetid"`
	LocalPort  int         `yaml:"localport"`
	Route      RouteConfig `yaml:"route"`
}

type RouteConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Name     string `yaml:"name"`
}

type browseState struct {
	path    string                  // current browse path ("" = root)
	entries []ads.SymbolBrowseEntry // last displayed entries
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{
		Port:       48898,
		AMSPort:    851,
		LocalNetID: "auto",
		LocalPort:  10500,
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func parseNetID(s string) ([6]byte, error) {
	var result [6]byte
	parts := strings.Split(s, ".")
	if len(parts) != 6 {
		return result, fmt.Errorf("invalid NetID %q: expected 6 octets", s)
	}
	for i, p := range parts {
		v, err := strconv.ParseUint(p, 10, 8)
		if err != nil {
			return result, fmt.Errorf("invalid NetID octet %q: %w", p, err)
		}
		result[i] = byte(v)
	}
	return result, nil
}

// discoverLocalIP dials the remote host via UDP to discover which local IP
// the OS would use, without sending any data.
func discoverLocalIP(remoteHost string) (string, error) {
	conn, err := net.Dial("udp4", net.JoinHostPort(remoteHost, "48899"))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	localAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return "", fmt.Errorf("unexpected address type: %T", conn.LocalAddr())
	}
	return localAddr.IP.String(), nil
}

func main() {
	configPath := "config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var conn *ads.Connection
	var connMu sync.Mutex
	updateCh := make(chan *ads.Update, 100)
	bs := &browseState{}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:      "> ",
		HistoryFile: filepath.Join(os.TempDir(), ".go-ads-history"),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing readline: %v\n", err)
		os.Exit(1)
	}
	defer rl.Close()

	// Background goroutine to print subscription updates
	go func() {
		for u := range updateCh {
			fmt.Printf("\n  [%s] %s = %s\n", u.TimeStamp.Format("15:04:05.000"), u.Variable, u.Value)
			rl.Refresh()
		}
	}()

	// Clean shutdown on context cancellation
	go func() {
		<-ctx.Done()
		fmt.Println("\nShutting down...")
		connMu.Lock()
		if conn != nil {
			conn.Close()
		}
		connMu.Unlock()
		rl.Close()
		os.Exit(0)
	}()

	fmt.Println("go-ads interactive shell")
	fmt.Println("Type 'help' for available commands")
	fmt.Println()

	for {
		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt || err == io.EOF {
				break
			}
			continue
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := parts[0]

		// Check if the command is a number (for numbered browsing)
		if num, err := strconv.Atoi(cmd); err == nil && len(parts) == 1 {
			handleBrowseByNumber(conn, bs, num)
			continue
		}

		switch cmd {
		case "help":
			printHelp()

		case "connect":
			connMu.Lock()
			if conn != nil {
				connMu.Unlock()
				fmt.Println("Already connected. Use 'quit' to disconnect first.")
				continue
			}
			conn, err = doConnect(ctx, cfg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
				conn = nil
			}
			connMu.Unlock()

		case "info":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			info, err := conn.ReadDeviceInfo()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			name := strings.TrimRight(string(info.DeviceName[:]), "\x00")
			fmt.Printf("Device: %s  Version: %d.%d.%d\n", name, info.Major, info.Minor, info.Version)

		case "state":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			state, err := conn.ReadState()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			fmt.Printf("ADS State: %d  Device State: %d\n", state.AdsState, state.DeviceState)

		case "discover":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			fmt.Println("Loading full symbol table...")
			err := conn.LoadSymbols()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			symbols, _ := conn.ListSymbols()
			fmt.Printf("Loaded %d symbols.\n", len(symbols))

		case "discover-slow":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			fmt.Println("Loading symbol table (slow/chunked)...")
			err := conn.LoadSymbolsSlow(ads.SlowDiscoveryConfig{})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			symbols, _ := conn.ListSymbols()
			fmt.Printf("Loaded %d symbols.\n", len(symbols))

		case "list":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			symbols, err := conn.ListSymbols()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v (run 'discover' first)\n", err)
				continue
			}
			// Sort by name for stable output
			names := make([]string, 0, len(symbols))
			for name := range symbols {
				names = append(names, name)
			}
			sort.Strings(names)
			fmt.Printf("Symbols (%d):\n", len(symbols))
			for _, name := range names {
				sym := symbols[name]
				fmt.Printf("  %-50s  type=%-20s  size=%d\n", name, sym.DataType, sym.Length)
			}

		case "discover-symbols":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			fmt.Println("Loading symbol list only (browse mode)...")
			err := conn.LoadSymbolList(ads.SlowDiscoveryConfig{})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			fmt.Println("Symbol list loaded. Use 'browse' to navigate.")

		case "discover-types":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			fmt.Println("Loading datatypes...")
			err := conn.LoadDataTypes(ads.SlowDiscoveryConfig{})
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			fmt.Println("Datatypes loaded. Struct children can now be expanded with 'browse'.")

		case "browse":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			path := ""
			if len(parts) >= 2 {
				path = parts[1]
			}
			doBrowse(conn, bs, path)

		case "..", "back":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			handleBack(conn, bs)

		case "read":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			if len(parts) < 2 {
				fmt.Println("Usage: read <symbol|number>")
				continue
			}
			// Check if argument is a number (read by index)
			if num, err := strconv.Atoi(parts[1]); err == nil {
				handleReadByNumber(conn, bs, num)
			} else {
				symbolName := parts[1]
				value, err := conn.ReadFromSymbol(symbolName)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					continue
				}
				fmt.Printf("%s = %s\n", symbolName, value)
			}

		case "write":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			if len(parts) < 3 {
				fmt.Println("Usage: write <symbol|number> <value>")
				continue
			}
			symbolName := parts[1]
			value := strings.Join(parts[2:], " ")
			// Resolve browse index to symbol name
			if num, err := strconv.Atoi(symbolName); err == nil {
				if len(bs.entries) == 0 {
					fmt.Println("No browse results. Use 'browse' first.")
					continue
				}
				if num < 0 || num >= len(bs.entries) {
					fmt.Printf("Index %d out of range (0-%d)\n", num, len(bs.entries)-1)
					continue
				}
				symbolName = bs.entries[num].FullName
			}
			err := conn.WriteToSymbol(symbolName, value)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Write error: %v\n", err)
				if strings.Contains(err.Error(), "aliased type") {
					fmt.Println("  hint: run 'discover' first to load type definitions")
				}
				continue
			}
			fmt.Printf("Wrote %q to %s\n", value, symbolName)
			// Read back to confirm
			readBack, err := conn.ReadFromSymbol(symbolName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Read-back error: %v\n", err)
				continue
			}
			fmt.Printf("  confirmed: %s = %s\n", symbolName, readBack)

		case "writemulti":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			if len(parts) < 2 {
				fmt.Println("Usage: writemulti <sym1>=<val1> <sym2>=<val2> ...")
				continue
			}
			values := make(map[string]string)
			for _, pair := range parts[1:] {
				eqIdx := strings.IndexByte(pair, '=')
				if eqIdx < 0 {
					fmt.Printf("Invalid pair %q (expected key=value)\n", pair)
					continue
				}
				values[pair[:eqIdx]] = pair[eqIdx+1:]
			}
			if len(values) == 0 {
				fmt.Println("No valid key=value pairs provided.")
				continue
			}
			codes, err := conn.WriteMultipleSymbols(values)
			if err != nil {
				fmt.Fprintf(os.Stderr, "WriteMultiple error: %v\n", err)
				continue
			}
			for name, code := range codes {
				fmt.Printf("  %s: return code %d\n", name, code)
			}
			// Read back each symbol to confirm
			for name := range values {
				readBack, err := conn.ReadFromSymbol(name)
				if err != nil {
					fmt.Fprintf(os.Stderr, "  read-back %s error: %v\n", name, err)
					continue
				}
				fmt.Printf("  confirmed: %s = %s\n", name, readBack)
			}

		case "subscribe":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			if len(parts) < 2 {
				fmt.Println("Usage: subscribe <symbol> [cycle_ms] [delay_ms]")
				continue
			}
			symbolName := parts[1]
			cycleTime := 1000
			maxDelay := 100
			if len(parts) >= 3 {
				if v, err := strconv.Atoi(parts[2]); err == nil {
					cycleTime = v
				}
			}
			if len(parts) >= 4 {
				if v, err := strconv.Atoi(parts[3]); err == nil {
					maxDelay = v
				}
			}
			handle, err := conn.AddSymbolNotification(symbolName, maxDelay, cycleTime, ads.TransModeServerOnChange, updateCh)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			fmt.Printf("Subscribed to %s (handle=%d, cycle=%dms, maxDelay=%dms)\n", symbolName, handle, cycleTime, maxDelay)

		case "readio":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			if len(parts) < 3 {
				fmt.Println("Usage: readio <in|out> <byte_offset> [length]")
				continue
			}
			direction := parts[1]
			offset, err := strconv.ParseUint(parts[2], 10, 32)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid offset: %v\n", err)
				continue
			}
			length := uint32(1)
			if len(parts) >= 4 {
				l, err := strconv.ParseUint(parts[3], 10, 32)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Invalid length: %v\n", err)
					continue
				}
				length = uint32(l)
			}
			var data []byte
			switch direction {
			case "in":
				data, err = conn.ReadProcessInput(uint32(offset), length)
			case "out":
				data, err = conn.ReadProcessOutput(uint32(offset), length)
			default:
				fmt.Println("Direction must be 'in' or 'out'")
				continue
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			fmt.Printf("Process %s [%d..%d]: %s\n", direction, offset, uint32(offset)+length-1, hex.EncodeToString(data))

		case "writeio":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			if len(parts) < 3 {
				fmt.Println("Usage: writeio <byte_offset> <hex_bytes>")
				fmt.Println("  Example: writeio 0 FF00A5")
				continue
			}
			offset, err := strconv.ParseUint(parts[1], 10, 32)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid offset: %v\n", err)
				continue
			}
			data, err := hex.DecodeString(parts[2])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid hex: %v\n", err)
				continue
			}
			fmt.Printf("WARNING: Writing %d bytes (%s) to output process image at offset %d\n", len(data), parts[2], offset)
			fmt.Print("This may cause physical outputs to change. Continue? [y/N]: ")
			confirm, _ := rl.Readline()
			if strings.ToLower(strings.TrimSpace(confirm)) != "y" {
				fmt.Println("Cancelled.")
				continue
			}
			err = conn.WriteProcessOutput(uint32(offset), data)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			fmt.Printf("Wrote %d bytes to output process image at offset %d\n", len(data), offset)

		case "readbit":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			if len(parts) < 3 {
				fmt.Println("Usage: readbit <in|out> <byte_offset> <bit_index>")
				continue
			}
			if len(parts) < 4 {
				fmt.Println("Usage: readbit <in|out> <byte_offset> <bit_index>")
				continue
			}
			direction := parts[1]
			offset, err := strconv.ParseUint(parts[2], 10, 32)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid offset: %v\n", err)
				continue
			}
			bitIdx, err := strconv.ParseUint(parts[3], 10, 8)
			if err != nil || bitIdx > 7 {
				fmt.Fprintf(os.Stderr, "Invalid bit index (0-7): %v\n", parts[3])
				continue
			}
			switch direction {
			case "in":
				val, err := conn.ReadProcessInputBit(uint32(offset), uint8(bitIdx))
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					continue
				}
				fmt.Printf("Input bit %d.%d = %v\n", offset, bitIdx, val)
			case "out":
				data, err := conn.ReadProcessOutput(uint32(offset), 1)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					continue
				}
				val := ads.ReadBit(data, int(bitIdx))
				fmt.Printf("Output bit %d.%d = %v\n", offset, bitIdx, val)
			default:
				fmt.Println("Direction must be 'in' or 'out'")
			}

		case "writebit":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			if len(parts) < 4 {
				fmt.Println("Usage: writebit <byte_offset> <bit_index> <true|false>")
				continue
			}
			offset, err := strconv.ParseUint(parts[1], 10, 32)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid offset: %v\n", err)
				continue
			}
			bitIdx, err := strconv.ParseUint(parts[2], 10, 8)
			if err != nil || bitIdx > 7 {
				fmt.Fprintf(os.Stderr, "Invalid bit index (0-7): %v\n", parts[2])
				continue
			}
			val, err := strconv.ParseBool(parts[3])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid value (true/false): %v\n", parts[3])
				continue
			}
			fmt.Printf("WARNING: Writing bit %d.%d = %v to output process image\n", offset, bitIdx, val)
			fmt.Print("This may cause physical outputs to change. Continue? [y/N]: ")
			confirm, _ := rl.Readline()
			if strings.ToLower(strings.TrimSpace(confirm)) != "y" {
				fmt.Println("Cancelled.")
				continue
			}
			err = conn.WriteProcessOutputBit(uint32(offset), uint8(bitIdx), val)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			fmt.Printf("Wrote output bit %d.%d = %v\n", offset, bitIdx, val)

		case "iosize":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			size, err := conn.ReadProcessInputSize()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			fmt.Printf("Process input image size: %d bytes\n", size)

		case "quit", "exit":
			fmt.Println("Shutting down...")
			if conn != nil {
				conn.Close()
			}
			return

		default:
			fmt.Printf("Unknown command: %s (type 'help')\n", cmd)
		}
	}
}

func doBrowse(conn *ads.Connection, bs *browseState, path string) {
	// Reset browse state when explicitly browsing
	bs.path = path

	entries, err := conn.BrowseSymbols(bs.path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		bs.entries = nil
		return
	}
	if len(entries) == 0 {
		fmt.Println("No entries found.")
		bs.entries = nil
		return
	}

	bs.entries = entries
	printBrowseEntries(bs.path, entries)
}

func printBrowseEntries(path string, entries []ads.SymbolBrowseEntry) {
	if path == "" {
		fmt.Printf("Root entries (%d):\n", len(entries))
	} else {
		fmt.Printf("Children of %s (%d):\n", path, len(entries))
	}
	for i, e := range entries {
		children := ""
		if e.HasChildren {
			children = "  [+]"
		}
		if e.DataType != "" {
			fmt.Printf("  [%d] %-45s %-20s %d%s\n", i, e.FullName, e.DataType, e.Size, children)
		} else {
			fmt.Printf("  [%d] %-45s%s\n", i, e.FullName, children)
		}
	}
}

func handleBrowseByNumber(conn *ads.Connection, bs *browseState, num int) {
	if conn == nil {
		fmt.Println("Not connected. Use 'connect' first.")
		return
	}
	if len(bs.entries) == 0 {
		fmt.Println("No browse results. Use 'browse' first.")
		return
	}
	if num < 0 || num >= len(bs.entries) {
		fmt.Printf("Index %d out of range (0-%d)\n", num, len(bs.entries)-1)
		return
	}

	entry := bs.entries[num]
	if entry.HasChildren {
		doBrowse(conn, bs, entry.FullName)
	} else {
		// No children — try to read the value
		value, err := conn.ReadFromSymbol(entry.FullName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", entry.FullName, err)
			return
		}
		fmt.Printf("%s = %s\n", entry.FullName, value)
	}
}

func handleReadByNumber(conn *ads.Connection, bs *browseState, num int) {
	if len(bs.entries) == 0 {
		fmt.Println("No browse results. Use 'browse' first.")
		return
	}
	if num < 0 || num >= len(bs.entries) {
		fmt.Printf("Index %d out of range (0-%d)\n", num, len(bs.entries)-1)
		return
	}

	entry := bs.entries[num]
	value, err := conn.ReadFromSymbol(entry.FullName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading %s: %v\n", entry.FullName, err)
		return
	}
	fmt.Printf("%s = %s\n", entry.FullName, value)
}

func handleBack(conn *ads.Connection, bs *browseState) {
	if bs.path == "" {
		fmt.Println("Already at root.")
		return
	}

	// Trim the last segment from the path
	dot := strings.LastIndexByte(bs.path, '.')
	if dot < 0 {
		// Going back to root
		doBrowse(conn, bs, "")
	} else {
		doBrowse(conn, bs, bs.path[:dot])
	}
}

func printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  connect                                  Register route + connect + show device info")
	fmt.Println("  discover                                 Load full symbol table from PLC")
	fmt.Println("  discover-slow                            Load symbol table in chunks (PLC-friendly)")
	fmt.Println("  discover-symbols                         Load symbol list only (for browse mode)")
	fmt.Println("  discover-types                           Load datatypes only (enables struct expansion)")
	fmt.Println("  info                                     Read device info")
	fmt.Println("  state                                    Read device state")
	fmt.Println("  list                                     List all symbols (requires discover first)")
	fmt.Println("  browse [path]                            Browse symbol hierarchy (requires discover-symbols)")
	fmt.Println("  <number>                                 Browse into entry by index from last browse result")
	fmt.Println("  ..  / back                               Navigate up one level in browse hierarchy")
	fmt.Println("  read <symbol|number>                     Read a symbol value (by name or browse index)")
	fmt.Println("  write <symbol|number> <value>            Write a value to a symbol (by name or browse index)")
	fmt.Println("  writemulti <sym1>=<val1> <sym2>=<val2>   Write multiple symbols in one round-trip")
	fmt.Println("  subscribe <symbol> [cycle_ms] [delay_ms] Subscribe to symbol changes")
	fmt.Println()
	fmt.Println("Process Image I/O:")
	fmt.Println("  readio <in|out> <offset> [length]        Read bytes from process image")
	fmt.Println("  writeio <offset> <hex_bytes>             Write hex bytes to output process image")
	fmt.Println("  readbit <in|out> <offset> <bit>          Read single bit (bit 0-7)")
	fmt.Println("  writebit <offset> <bit> <true|false>     Write single output bit")
	fmt.Println("  iosize                                   Show input process image size")
	fmt.Println()
	fmt.Println("  quit                                     Graceful shutdown")
}

func doConnect(ctx context.Context, cfg *Config) (*ads.Connection, error) {
	localNetID := cfg.LocalNetID

	// Route registration
	if cfg.Route.Username != "" {
		localIP, err := discoverLocalIP(cfg.IP)
		if err != nil {
			return nil, fmt.Errorf("failed to discover local IP: %w", err)
		}
		fmt.Printf("Local IP: %s\n", localIP)

		// Derive localNetID from discovered IP if auto
		if localNetID == "auto" || localNetID == "" {
			localNetID = localIP + ".1.1"
		}

		netid, err := parseNetID(localNetID)
		if err != nil {
			return nil, fmt.Errorf("invalid local NetID: %w", err)
		}

		routeName := cfg.Route.Name
		if routeName == "" {
			routeName = "go-ads"
		}

		fmt.Printf("Registering route %q on %s (NetID %s)...\n", routeName, cfg.IP, localNetID)
		err = ads.AddRemoteRoute(cfg.IP, netid, routeName, localIP, cfg.Route.Username, cfg.Route.Password)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: route registration failed: %v (continuing anyway)\n", err)
		} else {
			fmt.Println("Route registered.")
		}
	}

	conn, err := ads.NewConnection(ctx, cfg.IP, cfg.Port, cfg.NetID, cfg.AMSPort, localNetID, cfg.LocalPort, 5*time.Second)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Connecting to %s:%d (AMS %s:%d)...\n", cfg.IP, cfg.Port, cfg.NetID, cfg.AMSPort)
	err = conn.Connect(false)
	if err != nil {
		return nil, err
	}
	fmt.Println("Connected.")

	// Show device info
	info, err := conn.ReadDeviceInfo()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not read device info: %v\n", err)
	} else {
		name := strings.TrimRight(string(info.DeviceName[:]), "\x00")
		fmt.Printf("Device: %s  Version: %d.%d.%d\n", name, info.Major, info.Minor, info.Version)
	}

	// Show state
	state, err := conn.ReadState()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: could not read state: %v\n", err)
	} else {
		fmt.Printf("ADS State: %d  Device State: %d\n", state.AdsState, state.DeviceState)
	}

	return conn, nil
}

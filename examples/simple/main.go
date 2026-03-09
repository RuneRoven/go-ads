package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	ads "github.com/RuneRoven/go-ads"
	"gopkg.in/yaml.v3"
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
	localAddr := conn.LocalAddr().(*net.UDPAddr)
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
	updateCh := make(chan *ads.Update, 100)

	// Background goroutine to print subscription updates
	go func() {
		for u := range updateCh {
			fmt.Printf("\n  [%s] %s = %s\n> ", u.TimeStamp.Format("15:04:05.000"), u.Variable, u.Value)
		}
	}()

	// Clean shutdown on context cancellation
	go func() {
		<-ctx.Done()
		fmt.Println("\nShutting down...")
		if conn != nil {
			conn.Close()
		}
		os.Exit(0)
	}()

	fmt.Println("go-ads interactive shell")
	fmt.Println("Type 'help' for available commands")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := parts[0]

		switch cmd {
		case "help":
			printHelp()

		case "connect":
			if conn != nil {
				fmt.Println("Already connected. Use 'quit' to disconnect first.")
				continue
			}
			conn, err = doConnect(ctx, cfg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Connect failed: %v\n", err)
				conn = nil
			}

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

		case "list":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			symbols := conn.ListSymbols()
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

		case "read":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			if len(parts) < 2 {
				fmt.Println("Usage: read <symbol>")
				continue
			}
			symbolName := parts[1]
			value, err := conn.ReadFromSymbol(symbolName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			fmt.Printf("%s = %s\n", symbolName, value)

		case "subscribe":
			if conn == nil {
				fmt.Println("Not connected. Use 'connect' first.")
				continue
			}
			if len(parts) < 2 {
				fmt.Println("Usage: subscribe <symbol> [cycleTime_ms] [maxDelay_ms]")
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
			err := conn.AddSymbolNotification(symbolName, maxDelay, cycleTime, ads.TransModeServerOnChange, updateCh)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			fmt.Printf("Subscribed to %s (cycle=%dms, maxDelay=%dms)\n", symbolName, cycleTime, maxDelay)

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

func printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  connect                                  Register route + connect + show device info")
	fmt.Println("  info                                     Read device info")
	fmt.Println("  state                                    Read device state")
	fmt.Println("  list                                     List all symbols")
	fmt.Println("  read <symbol>                            Read a symbol value")
	fmt.Println("  subscribe <symbol> [cycle_ms] [delay_ms] Subscribe to symbol changes")
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

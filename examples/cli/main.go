// Example CLI — selector for two demos:
//
//   1) Session demo: managed Session with cache, auto-reconnect,
//      online-change handling, persistent notifications, Stale-flag
//      observation.
//
//   2) Client demo: raw Client (no cache, no reconnect) — escape hatch
//      for protocol-level inspection. Demonstrates ReadDeviceInfo,
//      ReadState, GetSymbolInfoByName, raw Read by handle.
//
// Connection parameters are read from environment variables — same set
// for both demos:
//
//   ADS_PLC_IP        target PLC IP (default: 192.168.1.100)
//   ADS_TARGET_AMS    target AMS NetID (default: 5.1.2.3.1.1)
//   ADS_TARGET_PORT   target AMS port (default: 851)
//   ADS_LOCAL_AMS     local AMS NetID ("auto" = derive from local IP)
//   ADS_LOCAL_PORT    local AMS port (default: 10500)
//   ADS_SYMBOL_NAME   symbol used for read/write/notification demos
//                     (default: MAIN.bCounter)
//   ADS_ROUTE_USER    optional — register route on PLC if set
//   ADS_ROUTE_PASS    optional — paired with ADS_ROUTE_USER
//   ADS_ROUTE_NAME    optional — route display name (default: go-ads-example)
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	ads "github.com/RuneRoven/go-ads/v2"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	mode := selectMode()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	switch mode {
	case "session":
		if err := runSessionDemo(ctx, logger); err != nil {
			logger.Error("session demo failed", "error", err)
			os.Exit(1)
		}
	case "client":
		if err := runClientDemo(logger); err != nil {
			logger.Error("client demo failed", "error", err)
			os.Exit(1)
		}
	}
}

// selectMode prompts the user to pick a demo. Honours ADS_DEMO=session|client
// to skip the prompt for scripted runs.
func selectMode() string {
	if v := strings.ToLower(os.Getenv("ADS_DEMO")); v == "session" || v == "client" {
		return v
	}
	fmt.Println("go-ads example CLI")
	fmt.Println()
	fmt.Println("  1) Session demo — managed wrapper (cache, auto-reconnect, notifications)")
	fmt.Println("  2) Client demo  — raw RPC (no cache, no reconnect)")
	fmt.Println()
	fmt.Print("Select [1/2]: ")
	r := bufio.NewReader(os.Stdin)
	line, err := r.ReadString('\n')
	if err != nil {
		fmt.Println("read error:", err)
		os.Exit(1)
	}
	switch strings.TrimSpace(line) {
	case "1", "session", "s":
		return "session"
	case "2", "client", "c":
		return "client"
	default:
		fmt.Println("unknown selection")
		os.Exit(1)
		return ""
	}
}

// ----- Session demo ---------------------------------------------------------

func runSessionDemo(ctx context.Context, logger *slog.Logger) error {
	ip, targetAMS, targetPort, localAMS, localPort, symbolName, routeName, routeUser, routePass := readEnv()

	opts := []ads.SessionOption{
		ads.WithLogger(logger),
		ads.WithAutoReconnect(true),
		ads.WithOnDisconnect(func() {
			logger.Warn("session: transport dropped — auto-reconnect will retry")
		}),
		ads.WithOnReconnect(func() {
			logger.Info("session: reconnected — cache + notifications restored")
		}),
		ads.WithSymbolVersionStrategy(ads.SymbolVersionAutoReload),
		ads.WithOnSymbolVersionChanged(func(reason string) {
			logger.Info("session: symbol-version event", "reason", reason)
		}),
	}
	if routeUser != "" {
		opts = append(opts, ads.WithRoute(routeName, routeUser, routePass))
	}

	sess, err := ads.NewSession(ip, 48898, targetAMS, targetPort, localAMS, localPort, 5*time.Second, opts...)
	if err != nil {
		return fmt.Errorf("NewSession: %w", err)
	}
	defer sess.Close()

	if err := sess.Connect(false); err != nil {
		return fmt.Errorf("Connect: %w", err)
	}
	logger.Info("session: connected", "plc", ip, "ams", targetAMS)

	if err := sess.LoadSymbols(); err != nil {
		return fmt.Errorf("LoadSymbols: %w", err)
	}
	logger.Info("session: symbol cache loaded")

	// Read/write round-trip — uses the cache transparently.
	val, err := sess.ReadFromSymbol(symbolName)
	if err != nil {
		logger.Warn("ReadFromSymbol failed", "symbol", symbolName, "error", err)
	} else {
		logger.Info("session: read", "symbol", symbolName, "value", val)
	}

	// Persistent notification: re-subscribed automatically across reconnect.
	updates := make(chan *ads.Update, 32)
	handle, err := sess.AddSymbolNotification(
		symbolName,
		100*time.Millisecond,    // maxDelay
		100*time.Millisecond,    // cycleTime
		ads.TransModeServerOnChange,
		updates,
	)
	if err != nil {
		logger.Warn("AddSymbolNotification failed", "symbol", symbolName, "error", err)
	} else {
		logger.Info("session: notification registered", "handle", handle, "symbol", symbolName)
	}

	logger.Info("session: streaming notifications until SIGINT/SIGTERM")
	for {
		select {
		case <-ctx.Done():
			logger.Info("session: shutdown signal received")
			return nil
		case u, ok := <-updates:
			if !ok {
				return nil
			}
			if u.Stale {
				// One-shot Stale flag (R-NOT-016) — first sample after a
				// transport recovery or symbol-version event.
				logger.Warn("update STALE",
					"symbol", u.Variable,
					"value", u.Value,
					"reason", u.Reason,
					"ts", u.TimeStamp.Format(time.RFC3339Nano))
			} else {
				logger.Info("update",
					"symbol", u.Variable,
					"value", u.Value,
					"ts", u.TimeStamp.Format(time.RFC3339Nano))
			}
		}
	}
}

// ----- Client demo ----------------------------------------------------------

func runClientDemo(logger *slog.Logger) error {
	ip, targetAMS, targetPort, localAMS, localPort, symbolName, _, _, _ := readEnv()

	target, err := parseAMS(targetAMS, uint16(targetPort))
	if err != nil {
		return fmt.Errorf("parse target AMS: %w", err)
	}
	source, err := parseAMS(localAMS, uint16(localPort))
	if err != nil {
		// "auto" / "" → leave NetID zero; caller is responsible for a sane source.
		source = ads.AMSAddress{Port: uint16(localPort)}
	}

	c, err := ads.Dial(ip, 48898, target, source, 5*time.Second,
		ads.WithClientLogger(logger),
		ads.WithOnDrop(func() {
			logger.Warn("client: transport dropped (no auto-reconnect — Session does that)")
		}),
	)
	if err != nil {
		return fmt.Errorf("Dial: %w", err)
	}
	defer c.Close()
	logger.Info("client: dialed", "plc", ip, "ams", targetAMS)

	// PLC introspection — protocol-level, no cache.
	if info, err := c.ReadDeviceInfo(); err != nil {
		logger.Warn("ReadDeviceInfo failed", "error", err)
	} else {
		name := strings.TrimRight(string(info.DeviceName[:]), "\x00")
		logger.Info("client: device info",
			"name", name,
			"version", fmt.Sprintf("%d.%d.%d", info.Major, info.Minor, info.Version))
	}

	if state, err := c.ReadState(); err != nil {
		logger.Warn("ReadState failed", "error", err)
	} else {
		logger.Info("client: ads state", "ads", state.ADSState, "device", state.DeviceState)
	}

	// Raw symbol resolution — no cache, no on-demand. Inspect what the PLC
	// actually returned for this name.
	sym, err := c.GetSymbolInfoByName(symbolName)
	if err != nil {
		logger.Warn("GetSymbolInfoByName failed", "symbol", symbolName, "error", err)
		return nil
	}
	logger.Info("client: symbol info",
		"symbol", sym.Name,
		"group", fmt.Sprintf("0x%X", sym.Group),
		"offset", fmt.Sprintf("0x%X", sym.Offset),
		"length", sym.Length,
		"type", sym.DataType)

	// Raw Read by index group/offset — the protocol-level escape hatch.
	// GroupSymbolValueByName lets us read a value by writing the symbol
	// name as the WriteRead payload; here we instead resolve a handle and
	// Read by handle to mirror what the cache-aware Session does.
	handleBytes, err := c.WriteRead(uint32(ads.GroupSymbolHandleByName), 0, 4, []byte(symbolName))
	if err != nil {
		logger.Warn("resolve handle failed", "symbol", symbolName, "error", err)
		return nil
	}
	if len(handleBytes) != 4 {
		return fmt.Errorf("unexpected handle length: %d", len(handleBytes))
	}
	handle := binary.LittleEndian.Uint32(handleBytes)
	defer func() {
		hb := make([]byte, 4)
		binary.LittleEndian.PutUint32(hb, handle)
		_ = c.Write(uint32(ads.GroupSymbolReleaseHandle), 0, hb)
	}()

	data, err := c.Read(uint32(ads.GroupSymbolValueByHandle), handle, sym.Length)
	if err != nil {
		logger.Warn("raw Read failed", "symbol", symbolName, "error", err)
		return nil
	}
	logger.Info("client: raw read",
		"symbol", symbolName,
		"bytes", fmt.Sprintf("% X", data),
		"length", len(data))

	return nil
}

// ----- shared helpers -------------------------------------------------------

func readEnv() (ip, targetAMS string, targetPort int, localAMS string, localPort int, symbolName, routeName, routeUser, routePass string) {
	ip = getEnvOrDefault("ADS_PLC_IP", "192.168.1.100")
	targetAMS = getEnvOrDefault("ADS_TARGET_AMS", "5.1.2.3.1.1")
	targetPort = getEnvIntOrDefault("ADS_TARGET_PORT", 851)
	localAMS = getEnvOrDefault("ADS_LOCAL_AMS", "auto")
	localPort = getEnvIntOrDefault("ADS_LOCAL_PORT", 10500)
	symbolName = getEnvOrDefault("ADS_SYMBOL_NAME", "MAIN.bCounter")
	routeName = getEnvOrDefault("ADS_ROUTE_NAME", "go-ads-example")
	routeUser = os.Getenv("ADS_ROUTE_USER")
	routePass = os.Getenv("ADS_ROUTE_PASS")
	return
}

func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getEnvIntOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// parseAMS converts "a.b.c.d.e.f" + port into an AMSAddress.
func parseAMS(s string, port uint16) (ads.AMSAddress, error) {
	var a ads.AMSAddress
	parts := strings.Split(s, ".")
	if len(parts) != 6 {
		return a, fmt.Errorf("AMS NetID %q: expected 6 octets", s)
	}
	for i, p := range parts {
		v, err := strconv.ParseUint(p, 10, 8)
		if err != nil {
			return a, fmt.Errorf("AMS NetID octet %q: %w", p, err)
		}
		a.NetID[i] = byte(v)
	}
	a.Port = port
	return a, nil
}

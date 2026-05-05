package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"
)

// secret is an internal wrapper around password/credential strings that
// implements String() and slog.LogValuer to return "[REDACTED]" instead
// of the raw value. Defends against accidental leaks via fmt.Sprintf("%+v",
// conn) or slog.Any("conn", conn) (F-25).
//
// Kept unexported. The Connection's public API for credentials remains
// plain string — conversion happens at the boundary.
type secret string

func (s secret) String() string {
	return "[REDACTED]"
}

func (s secret) LogValue() slog.Value {
	return slog.StringValue("[REDACTED]")
}

type Connection struct {
	ip   string
	port int

	connection  net.Conn
	connMu      sync.Mutex // protects connection field against concurrent Close/Reconnect
	target      AmsAddress
	source      AmsAddress
	callbackIP  string // IP PLC uses to reach us (for Docker/VPN; set via WithHostIP)
	sendChannel chan []byte

	symbols             map[string]*Symbol
	activeNotifications map[uint32]*Symbol
	symbolLock          sync.Mutex

	// lastSubscribeNs records the time.Now().UnixNano() of the most recent
	// successful AddSymbolNotification(s). Used by handleNotification to
	// suppress the "unknown handle" Warn when a notification packet arrives
	// at the goroutine before the activeNotifications[handle] map insert
	// completes (F-22). Window is small (sub-millisecond) but real for
	// fast PLCs and zero-MaxDelay subscriptions.
	lastSubscribeNs atomic.Int64

	datatypes map[string]SymbolUploadDataType
	ctx       context.Context
	shutdown  context.CancelFunc
	waitGroup sync.WaitGroup // tracks only infrastructure goroutines (listen, transmitWorker)

	// List of active requests that waits a response, invokeid is key and value is a channel to the request rutine
	currentRequest    atomic.Uint32
	activeRequestLock sync.Mutex
	activeRequests    map[uint32]chan []byte

	systemResponse chan []byte

	requestTimeout time.Duration

	// Stored notification configs for re-subscribe after reconnect
	notificationConfigs []NotificationConfig
	notificationChannel chan *Update

	// Symbol version tracking
	symbolVersion uint8

	// Reconnection settings
	maxReconnectAttempts int // 0 = infinite
	backoffConfig        BackoffConfig
	isLocal              bool
	disconnected         atomic.Bool
	autoReconnect        bool // default true; when false, triggerReconnect does not launch goroutine

	// Strict reconnect: fail if on-demand symbols are missing after reconnect
	strictReconnect            bool
	strictReconnectMaxAttempts int
	strictReconnectFailures    int // tracks consecutive symbol-missing reconnect failures

	// Reconnect generation counter for stale handle detection
	reconnectGeneration atomic.Uint64

	// Event callbacks (run in goroutine, must not block)
	onDisconnect func()
	onReconnect  func()

	// Route probing
	forceRouteRegistration bool
	routeProbeFailures     int

	// Symbol discovery mode tracking
	symbolsFullyLoaded bool            // true after LoadSymbols() or LoadSymbolsSlow() completes
	symbolListLoaded   bool            // true after LoadSymbolList() — symbol names available for browsing
	datatypesLoaded    bool            // true after LoadDataTypes() — struct children expandable
	onDemandSymbols    map[string]bool // tracks symbol names resolved on-demand (for reconnect)

	// Feature support flags (detected at runtime via CAS)
	// sumReadCmd: 0 = unchecked (try Ex2 first),
	// uint32(GroupSumupReadEx2) = use 0xF084,
	// uint32(GroupSumupReadEx) = use 0xF083,
	// 1 = no sum read support (individual reads)
	sumReadCmd    atomic.Uint32
	sumWriteState atomic.Uint32 // 0=unchecked, 1=supported, 2=unsupported
	sumNotifState atomic.Uint32 // 0=unchecked, 1=supported, 2=unsupported
	chunkedDownloadSupported atomic.Bool
	chunkedDownloadChecked   atomic.Bool

	reconnecting  atomic.Bool // prevents concurrent reconnect attempts
	reconnectDone chan struct{}
	reconnectMu   sync.Mutex // protects reconnectDone

	closed   atomic.Bool
	closedCh chan struct{} // closed by Close(), never written to

	ctxMu  sync.RWMutex // protects conn.ctx and conn.shutdown against concurrent access
	chanMu sync.RWMutex // protects sendChannel and systemResponse against concurrent access during reconnect

	// Route registration config (set via WithRoute, used in Connect and reconnect)
	routeName     string
	routeUsername string
	routePassword secret

	logger *slog.Logger
}

// NewConnection creates a new ADS connection. requestTimeout is the timeout for individual ADS requests.
// If requestTimeout is 0, a default of 5000ms is used.
// If localPort is 0, a default of 10500 is used (arbitrary AMS source port for protocol headers).
//
// Note: The ctx parameter is currently unused. The connection manages its own lifecycle
// context internally so that Close() can send cleanup commands regardless of the caller's
// context state. Use conn.Close() to shut down the connection.
func NewConnection(ctx context.Context, ip string, port int, netid string, amsPort int, localNetID string, localPort int, requestTimeout time.Duration, opts ...ConnectionOption) (conn *Connection, err error) {
	if requestTimeout <= 0 {
		requestTimeout = 5000 * time.Millisecond
	}
	conn = &Connection{
		ip:                   ip,
		port:                 port,
		requestTimeout:       requestTimeout,
		maxReconnectAttempts: 0, // 0 = infinite retries
		backoffConfig:        DefaultBackoffConfig(),
		autoReconnect:        true,
		logger:               slog.Default(),
	}
	for _, opt := range opts {
		opt(conn)
	}
	netIDBytes, err := stringToNetID(netid)
	if err != nil {
		return nil, fmt.Errorf("invalid target NetID: %w", err)
	}
	conn.target.NetID = netIDBytes
	conn.target.Port = uint16(amsPort)
	if localNetID != "auto" && localNetID != "" {
		localBytes, err := stringToNetID(localNetID)
		if err != nil {
			return nil, fmt.Errorf("invalid local NetID: %w", err)
		}
		conn.source.NetID = localBytes
	}
	// If localNetID is "auto" or empty, source.NetID stays zero and will be auto-derived in Connect()
	if localPort <= 0 {
		localPort = 10500
	}
	conn.source.Port = uint16(localPort)
	conn.systemResponse = make(chan []byte)
	conn.activeRequests = map[uint32]chan []byte{}
	conn.activeNotifications = make(map[uint32]*Symbol)
	conn.sendChannel = make(chan []byte)
	conn.symbols = map[string]*Symbol{}
	conn.onDemandSymbols = map[string]bool{}
	conn.closedCh = make(chan struct{})
	// Use an independent context so that Close() can still send cleanup commands
	// even after the parent context is canceled (e.g. on SIGTERM)
	conn.ctx, conn.shutdown = context.WithCancel(context.Background())
	return
}

func (conn *Connection) Connect(local bool) error {
	conn.isLocal = local
	var err error
	conn.logger.Debug("dialing", "ip", conn.ip, "port", conn.port)
	if local {
		conn.target.NetID = [6]byte{127, 0, 0, 1, 1, 1}
		conn.ip = "127.0.0.1"
	}
	tcpConn, err := net.DialTimeout("tcp", net.JoinHostPort(conn.ip, strconv.Itoa(conn.port)), conn.requestTimeout)
	if err != nil {
		conn.logger.Error("Error connecting", "error", err)
		return err
	}
	conn.connMu.Lock()
	conn.connection = tcpConn
	conn.connMu.Unlock()
	// Enable aggressive TCP keepalive to detect dead connections quickly.
	// With Idle=3s, Interval=2s, Count=5: connection declared dead after ~13s of no response.
	// This ensures cable unplugs (>13s) are detected and trigger reconnect,
	// while not affecting slow-changing notification data (keepalive is TCP-level, not app-level).
	configureKeepAlive(tcpConn)
	// Log TCP socket (transport-level only — ADS route validation happens on first ADS command)
	conn.logger.Info("TCP socket established (ADS route not yet verified)",
		"local", conn.connection.LocalAddr().String(),
		"remote", conn.connection.RemoteAddr().String())

	// Auto-derive source AMS NetID from local IP if source NetID is all zeros
	if conn.source.NetID == [6]byte{} {
		localAddr, ok := conn.connection.LocalAddr().(*net.TCPAddr)
		if !ok {
			return fmt.Errorf("unexpected local address type: %T", conn.connection.LocalAddr())
		}
		ip := localAddr.IP.To4()
		if ip != nil {
			conn.source.NetID = [6]byte{ip[0], ip[1], ip[2], ip[3], 1, 1}
			conn.logger.Info("auto-derived source AMS NetID from local IP",
				"netid", fmt.Sprintf("%d.%d.%d.%d.1.1", ip[0], ip[1], ip[2], ip[3]))
		}

		// NAT/Docker detection: compare TCP and UDP source IPs
		if conn.callbackIP == "" && ip != nil {
			udpConn, udpErr := net.DialTimeout("udp4", net.JoinHostPort(conn.ip, strconv.Itoa(routePort)), 2*time.Second)
			if udpErr == nil {
				udpAddr, ok := udpConn.LocalAddr().(*net.UDPAddr)
				udpConn.Close()
				if ok {
					udpIP := udpAddr.IP.To4()
					if udpIP != nil && !ip.Equal(udpIP) {
						conn.logger.Warn("TCP and UDP source IPs differ — possible NAT/Docker/VPN",
							"tcpIP", ip.String(), "udpIP", udpIP.String(),
							"hint", "set WithHostIP() to the IP the PLC can reach")
					}
				}
			}
		}
	}

	// Log container detection — auto-derived NetID works in containers because
	// the PLC stores the UDP source IP (post-NAT) for routes, not the computerName tag.
	if conn.callbackIP == "" && isRunningInContainer() {
		conn.logger.Info("container detected — auto-derived NetID will be used for route registration",
			"netidIP", fmt.Sprintf("%d.%d.%d.%d", conn.source.NetID[0], conn.source.NetID[1], conn.source.NetID[2], conn.source.NetID[3]))
	}

	// Log ADS-level addressing (what matters for AMS routing, may differ from TCP)
	routeHostIP := conn.callbackIP
	if routeHostIP == "" {
		routeHostIP = fmt.Sprintf("%d.%d.%d.%d (from NetID, PLC will use UDP source IP)", conn.source.NetID[0], conn.source.NetID[1], conn.source.NetID[2], conn.source.NetID[3])
	}
	conn.logger.Info("ADS addressing",
		"sourceNetID", fmt.Sprintf("%d.%d.%d.%d.%d.%d", conn.source.NetID[0], conn.source.NetID[1], conn.source.NetID[2], conn.source.NetID[3], conn.source.NetID[4], conn.source.NetID[5]),
		"routeHostIP", routeHostIP,
		"target", fmt.Sprintf("%d.%d.%d.%d.%d.%d:%d", conn.target.NetID[0], conn.target.NetID[1], conn.target.NetID[2], conn.target.NetID[3], conn.target.NetID[4], conn.target.NetID[5], conn.target.Port))

	conn.logger.Log(context.Background(), LevelTrace, "connected")
	conn.waitGroup.Add(2)
	go conn.listen()
	go conn.transmitWorker()
	if local {
		resp, err := conn.send([]byte{0, 16, 2, 0, 0, 0, 0, 0})
		if err != nil {
			conn.cleanupAfterFailedConnect()
			return fmt.Errorf("local mode handshake failed: %w", err)
		}
		buf := bytes.NewBuffer(resp)
		result := AmsAddress{}
		conn.logger.Log(context.Background(), LevelTrace, "got stuff", "stuff", buf.Bytes())
		err = binary.Read(buf, binary.LittleEndian, &result)
		if err != nil {
			conn.cleanupAfterFailedConnect()
			return fmt.Errorf("local mode binary read failed: %w", err)
		}
		conn.logger.Info("local mode handshake result", "result", result)
		conn.connMu.Lock()
		conn.source = result
		conn.connMu.Unlock()
	}

	// Smart route registration: probe PLC first, register only if route missing.
	// Route registration is UDP-based but needs goroutines running for the probe
	// (which sends an ADS command over TCP). If route is registered, TCP reconnect
	// is needed because PLC may close connections from previously-unknown NetIDs.
	if conn.routeName != "" {
		registered, err := conn.ensureRouteOnConnect()
		if err != nil {
			conn.logger.Warn("route registration failed during connect", "error", err)
		}
		if registered {
			// TCP reconnect — PLC may reset connections from previously-unknown NetIDs.
			// Shut down goroutines, close TCP, redial, restart.
			conn.tearDownAndReset(false)
			if err := conn.dialAndStart(); err != nil {
				return fmt.Errorf("TCP reconnect after route registration failed: %w", err)
			}
			conn.logger.Info("TCP reconnected after route registration")
		}

	}

	// Read symbol version for later change detection (best-effort, don't fail connect)
	version, err := conn.GetSymbolVersion()
	if err != nil {
		conn.logger.Debug("could not read symbol version during connect", "error", err)
	} else {
		conn.symbolLock.Lock()
		conn.symbolVersion = version
		conn.symbolLock.Unlock()
	}
	return nil
}

// ensureRouteOnConnect probes the PLC and registers a route if needed during Connect().
// Returns (registered bool, err error) where registered=true means a route was added
// and the caller should TCP-reconnect.
func (conn *Connection) ensureRouteOnConnect() (registered bool, err error) {
	if conn.closed.Load() {
		return false, fmt.Errorf("connection closed")
	}

	// Force mode → always register
	if conn.forceRouteRegistration {
		conn.logger.Info("registering route (force mode)")
		err = conn.AddRoute(conn.routeName, conn.routeUsername, string(conn.routePassword))
		return err == nil, err
	}

	// Probe: try a lightweight ADS command to see if route exists
	_, probeErr := conn.GetSymbolVersion()
	if probeErr == nil {
		conn.logger.Info("route already exists on PLC, skipping registration")
		conn.routeProbeFailures = 0
		return false, nil
	}

	if conn.closed.Load() {
		return false, fmt.Errorf("connection closed during route probe")
	}

	// Probe failed → register with credentials
	conn.routeProbeFailures++
	conn.logger.Info("route probe failed, registering route", "error", probeErr)
	err = conn.AddRoute(conn.routeName, conn.routeUsername, string(conn.routePassword))
	if err != nil {
		return false, err
	}
	return true, nil
}

// Close closes connection and waits for completion
func (conn *Connection) Close() {
	if !conn.closed.CompareAndSwap(false, true) {
		return // already closed
	}
	close(conn.closedCh)
	conn.logger.Info("Close called, shutting down")

	// Skip handle cleanup if already disconnected — all commands would timeout
	if !conn.disconnected.Load() {
		// Delete all active notifications (uses sum command with automatic fallback to individual)
		conn.symbolLock.Lock()
		handles := make([]uint32, 0, len(conn.activeNotifications))
		for handle := range conn.activeNotifications {
			handles = append(handles, handle)
		}
		conn.symbolLock.Unlock()
		if len(handles) > 0 {
			errors, err := conn.SumDeleteDeviceNotification(handles)
			if err != nil {
				conn.logger.Warn("failed to delete notification handles during close", "error", err)
			} else {
				for i, h := range handles {
					if errors[i] != ReturnCodeNoErrors {
						conn.logger.Warn("failed to delete notification handle", "handle", h, "error", uint32(errors[i]))
					} else {
						conn.logger.Info("removed notification handle", "handle", h)
					}
				}
			}
		}
		// Collect symbol handles under lock, then release without holding the lock
		conn.symbolLock.Lock()
		var symHandles []uint32
		for _, symbol := range conn.symbols {
			if symbol.Handle != 0 {
				symHandles = append(symHandles, symbol.Handle)
			}
		}
		conn.symbolLock.Unlock()

		// Release handles individually — ADS has no batch release command,
		// and Close() is not performance-critical.
		// F-26: re-check disconnected each iteration so a mid-loop PLC failure
		// (listen detects EOF → triggerReconnect → disconnected=true) doesn't
		// force every remaining Write to time out. Bail out early.
		for i, h := range symHandles {
			if conn.disconnected.Load() {
				conn.logger.Info("close: disconnected during handle release, stopping cleanup",
					"released", i,
					"remaining", len(symHandles)-i)
				break
			}
			handleBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(handleBytes, h)
			if err := conn.Write(uint32(GroupSymbolReleaseHandle), 0, handleBytes); err != nil {
				conn.logger.Warn("failed to release symbol handle during close", "error", err, "handle", h)
			} else {
				conn.logger.Info("handle deleted", "handle", h)
			}
		}
	} else {
		conn.logger.Info("already disconnected, skipping handle cleanup")
	}
	conn.ctxMu.RLock()
	conn.shutdown()
	conn.ctxMu.RUnlock()
	// Close the TCP connection to unblock listen() which may be stuck in ReadFull
	conn.connMu.Lock()
	if conn.connection != nil {
		conn.connection.Close()
	}
	conn.connMu.Unlock()
	// Wait for any in-progress reconnect to stop BEFORE waiting on the
	// goroutine waitGroup. Reconnect's retry loop may call waitGroup.Add(2)
	// after we Close — calling Wait first would race with that Add and
	// trigger "sync: WaitGroup misuse" (F-02). closedCh signals Reconnect
	// to exit its retry loop promptly; reconnectDone is closed when Reconnect
	// returns.
	conn.reconnectMu.Lock()
	ch := conn.reconnectDone
	conn.reconnectMu.Unlock()
	if ch != nil {
		<-ch
	}
	conn.logger.Info("Waiting for workers to close")
	conn.waitGroup.Wait()
	conn.logger.Info("Close DONE")
}

// ErrDisconnected indicates the underlying TCP connection is not available —
// either Close() has been called or a reconnect has failed. Callers should use
// errors.Is(err, ErrDisconnected) to detect this case.
var ErrDisconnected = errors.New("connection is disconnected")

// reconnectBackoff returns the delay for the given reconnect attempt number (1-indexed)
// based on the configured BackoffConfig tiers.
func (conn *Connection) reconnectBackoff(attempt int) time.Duration {
	cfg := conn.backoffConfig
	switch {
	case attempt <= cfg.InitialAttempts:
		return cfg.InitialInterval
	case attempt <= cfg.InitialAttempts+cfg.MidAttempts:
		return cfg.MidInterval
	case attempt <= cfg.InitialAttempts+cfg.MidAttempts+cfg.SlowAttempts:
		return cfg.SlowInterval
	default:
		return cfg.MaxInterval
	}
}

// reconnectSleep sleeps for the appropriate backoff duration based on the attempt
// number. Returns early if Close() is called.
func (conn *Connection) reconnectSleep(attempt int) error {
	delay := conn.reconnectBackoff(attempt)
	conn.logger.Info("reconnect backoff", "attempt", attempt, "delay", delay)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-conn.closedCh:
		return fmt.Errorf("connection closed during reconnect")
	}
}

// triggerReconnect prepares the connection state for reconnection and launches
// the Reconnect goroutine (if auto-reconnect is enabled). It sets disconnected=true
// and creates the reconnectDone channel BEFORE launching the goroutine, eliminating
// the race window where callers could see a "healthy" connection between the trigger
// and Reconnect() being scheduled.
func (conn *Connection) triggerReconnect() {
	if conn.closed.Load() {
		return
	}
	// CAS ensures only the first goroutine to detect disconnect fires the callback
	// and sets up reconnection. Subsequent callers (e.g. both listen() and transmitWorker()
	// detecting the same TCP failure) skip the callback to avoid double-firing.
	firstDetector := conn.disconnected.CompareAndSwap(false, true)
	conn.reconnectMu.Lock()
	if conn.reconnectDone == nil {
		conn.reconnectDone = make(chan struct{})
	}
	conn.reconnectMu.Unlock()

	// Fire disconnect callback in goroutine (must not block).
	// Callback must not call Connection methods — connection may be closing.
	if firstDetector && conn.onDisconnect != nil && !conn.closed.Load() {
		go conn.onDisconnect()
	}

	if conn.autoReconnect {
		go conn.Reconnect()
	} else {
		// No auto-reconnect: close reconnectDone immediately so sendRequest
		// waiters unblock with ErrDisconnected instead of hanging forever.
		conn.reconnectMu.Lock()
		if conn.reconnectDone != nil {
			close(conn.reconnectDone)
			conn.reconnectDone = nil
		}
		conn.reconnectMu.Unlock()
	}
}

// Reconnect attempts to re-establish the TCP connection, reload symbols,
// and re-subscribe to previously registered notifications.
// Uses configurable backoff (see WithBackoff) with fast initial retries and
// progressive slowdown. Backoff resets on each successful reconnect.
func (conn *Connection) Reconnect() error {
	if conn.closed.Load() {
		return fmt.Errorf("connection closed")
	}
	// Prevent concurrent reconnect attempts
	if !conn.reconnecting.CompareAndSwap(false, true) {
		conn.logger.Info("reconnect already in progress, skipping")
		return nil
	}

	// Create a channel that waiters (sendRequest) can block on.
	// triggerReconnect() may have already created it — only create if nil.
	conn.reconnectMu.Lock()
	if conn.reconnectDone == nil {
		conn.reconnectDone = make(chan struct{})
	}
	conn.reconnectMu.Unlock()

	defer func() {
		conn.reconnecting.Store(false)
		conn.reconnectMu.Lock()
		if conn.reconnectDone != nil {
			close(conn.reconnectDone)
			conn.reconnectDone = nil
		}
		conn.reconnectMu.Unlock()
	}()

	conn.logger.Info("attempting reconnect")
	conn.disconnected.Store(true)

	// Clear active notifications (old handles invalid after reconnect).
	conn.symbolLock.Lock()
	conn.activeNotifications = make(map[uint32]*Symbol)
	conn.symbolLock.Unlock()

	conn.tearDownAndReset(true)

	var lastErr error
	attempts := 0
	for {
		if conn.closed.Load() {
			return fmt.Errorf("connection closed during reconnect")
		}
		attempts++
		if conn.maxReconnectAttempts > 0 && attempts > conn.maxReconnectAttempts {
			return fmt.Errorf("reconnect failed after %d attempts: %w", conn.maxReconnectAttempts, lastErr)
		}

		// Dial TCP, configure keepalive, clear disconnected flag, start goroutines.
		// dialAndStart re-checks closed.Load() before waitGroup.Add(2) (F-02).
		if err := conn.dialAndStart(); err != nil {
			lastErr = err
			conn.logger.Warn("reconnect dial/start failed, retrying", "error", err, "ip", conn.ip, "port", conn.port, "attempt", attempts)
			if err := conn.reconnectSleep(attempts); err != nil {
				return err
			}
			continue
		}

		// Re-perform local-mode handshake if needed
		if conn.isLocal {
			if err := conn.localHandshake(); err != nil {
				lastErr = err
				conn.logger.Warn("reconnect local handshake failed, retrying", "error", err, "attempt", attempts)
				conn.resetForRetry()
				if err := conn.reconnectSleep(attempts); err != nil {
					return err
				}
				continue
			}
		}

		// Smart route registration: probe first, register only if needed.
		if err := conn.ensureRoute(attempts); err != nil {
			lastErr = err
			conn.logger.Warn("route registration failed during reconnect, retrying", "error", err, "attempt", attempts)
			conn.resetForRetry()
			if err := conn.reconnectSleep(attempts); err != nil {
				return err
			}
			continue
		}

		// Re-load symbols based on discovery mode
		if err := conn.reloadSymbols(); err != nil {
			lastErr = err
			conn.logger.Warn("reconnect symbol reload failed, retrying", "error", err, "attempt", attempts)
			conn.resetForRetry()
			if err := conn.reconnectSleep(attempts); err != nil {
				return err
			}
			continue
		}

		// Re-subscribe notifications using stored configs.
		if err := conn.resubscribeNotifications(); err != nil {
			lastErr = err
			conn.logger.Warn("reconnect notification re-subscribe failed, retrying", "error", err, "attempt", attempts)
			conn.resetForRetry()
			if err := conn.reconnectSleep(attempts); err != nil {
				return err
			}
			continue
		}

		conn.disconnected.Store(false)
		conn.reconnectGeneration.Add(1)
		conn.strictReconnectFailures = 0 // reset on success
		conn.logger.Info("reconnect successful", "attempts", attempts)

		// Fire reconnect callback in goroutine (must not block).
		// Callback must not call Connection methods — connection may be closing.
		if conn.onReconnect != nil && !conn.closed.Load() {
			go conn.onReconnect()
		}
		return nil
	}
}

// ensureRoute checks if the route exists (via probe) and registers if needed.
// On force mode or after repeated probe failures, skips the probe.
// Returns a non-nil error only if registration was attempted and failed critically
// (requiring a TCP reset / retry).
func (conn *Connection) ensureRoute(attempt int) error {
	if conn.routeName == "" {
		return nil
	}
	if conn.closed.Load() {
		return fmt.Errorf("connection closed")
	}

	// Force mode or too many probe failures → always register
	if conn.forceRouteRegistration || conn.routeProbeFailures >= 3 {
		conn.logger.Info("registering route (forced/fallback)", "probeFailures", conn.routeProbeFailures)
		if err := conn.AddRoute(conn.routeName, conn.routeUsername, string(conn.routePassword)); err != nil {
			return fmt.Errorf("route registration failed: %w", err)
		}

		conn.routeProbeFailures = 0
		return nil
	}

	// Probe: try a lightweight ADS command to see if route already exists
	_, probeErr := conn.GetSymbolVersion()
	if probeErr == nil {
		conn.logger.Debug("route still valid, skipping re-registration")
		conn.routeProbeFailures = 0
		return nil
	}

	if conn.closed.Load() {
		return fmt.Errorf("connection closed during route probe")
	}

	// Probe failed → register with credentials
	conn.routeProbeFailures++
	conn.logger.Info("route probe failed, registering route", "error", probeErr, "probeFailures", conn.routeProbeFailures)
	if err := conn.AddRoute(conn.routeName, conn.routeUsername, string(conn.routePassword)); err != nil {
		return fmt.Errorf("route registration failed after probe: %w", err)
	}

	return nil
}

// filterValidNotificationConfigs returns only configs whose symbols still exist
// in the current symbol table. Logs a warning for dropped subscriptions.
func (conn *Connection) filterValidNotificationConfigs(configs []NotificationConfig) []NotificationConfig {
	conn.symbolLock.Lock()
	defer conn.symbolLock.Unlock()

	valid := make([]NotificationConfig, 0, len(configs))
	for _, cfg := range configs {
		if _, exists := conn.symbols[symbolKey(cfg.SymbolName)]; exists {
			valid = append(valid, cfg)
		} else if _, onDemand := conn.onDemandSymbols[symbolKey(cfg.SymbolName)]; onDemand {
			valid = append(valid, cfg)
		} else {
			conn.logger.Warn("notification symbol gone after reconnect, dropping subscription",
				"symbol", cfg.SymbolName)
		}
	}
	return valid
}

// reloadSymbols re-establishes the symbol table after a reconnect, matching
// the discovery mode that was used before the connection dropped.
func (conn *Connection) reloadSymbols() error {
	conn.symbolLock.Lock()
	fullyLoaded := conn.symbolsFullyLoaded
	listLoaded := conn.symbolListLoaded
	dtLoaded := conn.datatypesLoaded
	hasOnDemand := len(conn.onDemandSymbols) > 0
	conn.symbolLock.Unlock()

	switch {
	case fullyLoaded:
		// Full discovery was done — redo it
		return conn.loadSymbols()

	case listLoaded || dtLoaded:
		// Partial discovery — re-download what was loaded
		if listLoaded {
			if err := conn.LoadSymbolList(SlowDiscoveryConfig{}); err != nil {
				return fmt.Errorf("reload symbol list: %w", err)
			}
		}
		if dtLoaded {
			if err := conn.LoadDataTypes(SlowDiscoveryConfig{}); err != nil {
				return fmt.Errorf("reload datatypes: %w", err)
			}
		}

	case hasOnDemand:
		// On-demand mode: re-resolve only the symbols that were previously loaded.
		// By default, missing symbols are skipped gracefully (PLC may have done
		// an online change). With WithStrictReconnect, missing symbols cause failure.
		conn.symbolLock.Lock()
		oldSymbols := conn.onDemandSymbols
		conn.symbols = make(map[string]*Symbol)
		conn.onDemandSymbols = make(map[string]bool)
		conn.symbolLock.Unlock()

		for name := range oldSymbols {
			if _, err := conn.GetSymbol(name); err != nil {
				if conn.strictReconnect {
					conn.strictReconnectFailures++
					if conn.strictReconnectMaxAttempts == 0 || conn.strictReconnectFailures > conn.strictReconnectMaxAttempts {
						return fmt.Errorf("re-resolve symbol %q (strict mode, %d failures): %w", name, conn.strictReconnectFailures, err)
					}
					return fmt.Errorf("re-resolve symbol %q (strict mode, attempt %d/%d): %w", name, conn.strictReconnectFailures, conn.strictReconnectMaxAttempts, err)
				}
				conn.logger.Warn("on-demand symbol unavailable after reconnect, skipping",
					"symbol", name, "error", err)
			}
		}

	default:
		// No symbols were loaded — read symbol version for future use
		version, err := conn.GetSymbolVersion()
		if err != nil {
			conn.logger.Debug("could not read symbol version during reconnect", "error", err)
		} else {
			conn.symbolLock.Lock()
			conn.symbolVersion = version
			conn.symbolLock.Unlock()
		}
	}

	return nil
}

// tearDownAndReset cancels the active goroutines, closes the TCP connection,
// and resets ctx/channels/activeRequests so the connection can be re-dialed.
// Optionally resets feature-detection flags (sumReadCmd / sumWriteState /
// sumNotifState / chunkedDownloadChecked) — needed during Reconnect when the
// PLC may have changed; not needed during initial Connect cleanup.
//
// Consolidates four previously-duplicated reset paths:
//   - Connect()'s post-route-registration TCP teardown
//   - Reconnect()'s pre-retry-loop reset
//   - cleanupAfterFailedConnect()
//   - resetForRetry()
func (conn *Connection) tearDownAndReset(resetFeatureFlags bool) {
	conn.ctxMu.RLock()
	conn.shutdown()
	conn.ctxMu.RUnlock()
	conn.connMu.Lock()
	if conn.connection != nil {
		conn.connection.Close()
	}
	conn.connMu.Unlock()
	conn.waitGroup.Wait()
	conn.ctxMu.Lock()
	conn.ctx, conn.shutdown = context.WithCancel(context.Background())
	conn.ctxMu.Unlock()
	conn.chanMu.Lock()
	conn.sendChannel = make(chan []byte)
	conn.systemResponse = make(chan []byte)
	conn.chanMu.Unlock()
	conn.activeRequestLock.Lock()
	conn.activeRequests = map[uint32]chan []byte{}
	conn.activeRequestLock.Unlock()
	if resetFeatureFlags {
		conn.sumReadCmd.Store(0)
		conn.sumWriteState.Store(0)
		conn.sumNotifState.Store(0)
		conn.chunkedDownloadChecked.Store(false)
	}
}

// dialAndStart performs net.DialTimeout, configures keepalive, clears the
// disconnected flag, and starts the listen/transmit goroutines. Used by both
// Connect()'s post-route-registration redial path and Reconnect()'s retry loop.
// Re-checks closed before waitGroup.Add(2) to prevent the F-02 sync.WaitGroup
// misuse race.
func (conn *Connection) dialAndStart() error {
	newConn, err := net.DialTimeout("tcp", net.JoinHostPort(conn.ip, strconv.Itoa(conn.port)), conn.requestTimeout)
	if err != nil {
		return err
	}
	conn.connMu.Lock()
	conn.connection = newConn
	conn.connMu.Unlock()
	configureKeepAlive(newConn)
	conn.disconnected.Store(false)
	if conn.closed.Load() {
		// Connection was Closed mid-dial. Don't Add to waitGroup.
		conn.connMu.Lock()
		newConn.Close()
		conn.connection = nil
		conn.connMu.Unlock()
		return fmt.Errorf("connection closed during dial")
	}
	conn.waitGroup.Add(2)
	go conn.listen()
	go conn.transmitWorker()
	return nil
}

// localHandshake performs the local-mode AmsAddress probe used after dial when
// isLocal is true. Updates conn.source on success.
func (conn *Connection) localHandshake() error {
	resp, err := conn.send([]byte{0, 16, 2, 0, 0, 0, 0, 0})
	if err != nil {
		return fmt.Errorf("local handshake send: %w", err)
	}
	buf := bytes.NewBuffer(resp)
	result := AmsAddress{}
	if err := binary.Read(buf, binary.LittleEndian, &result); err != nil {
		return fmt.Errorf("local handshake parse: %w", err)
	}
	conn.connMu.Lock()
	conn.source = result
	conn.connMu.Unlock()
	return nil
}

// resubscribeNotifications restores notification subscriptions stored in
// notificationConfigs after a successful reconnect. Filters out symbols that
// no longer exist after symbol reload. On error, rolls back partial PLC-side
// successes (F-12) and restores the saved configs so they can be retried by
// the next reconnect attempt.
func (conn *Connection) resubscribeNotifications() error {
	conn.symbolLock.Lock()
	savedConfigs := conn.notificationConfigs
	savedChannel := conn.notificationChannel
	conn.notificationConfigs = nil // Clear before re-adding to prevent duplicates
	conn.symbolLock.Unlock()
	if len(savedConfigs) == 0 || savedChannel == nil {
		return nil
	}
	validConfigs := conn.filterValidNotificationConfigs(savedConfigs)
	if len(validConfigs) == 0 {
		// All symbols gone (e.g., PLC online change removed all subscribed vars).
		// Clear channel reference so a future AddSymbolNotification can use a new channel.
		conn.symbolLock.Lock()
		conn.notificationChannel = nil
		conn.symbolLock.Unlock()
		return nil
	}
	// Snapshot active handles before the re-subscribe attempt. If
	// AddSymbolNotifications partially succeeds and then errors, we use the
	// snapshot diff to roll back the PLC-side registrations created during
	// this attempt (F-12). Without rollback, repeated reconnect retries
	// accumulate orphaned PLC notifications until the next TCP disconnect.
	conn.symbolLock.Lock()
	preHandles := make(map[uint32]struct{}, len(conn.activeNotifications))
	for h := range conn.activeNotifications {
		preHandles[h] = struct{}{}
	}
	conn.symbolLock.Unlock()

	err := conn.AddSymbolNotifications(validConfigs, savedChannel)
	if err != nil {
		// Identify handles created during THIS attempt and best-effort delete.
		conn.symbolLock.Lock()
		var newHandles []uint32
		for h := range conn.activeNotifications {
			if _, existed := preHandles[h]; !existed {
				newHandles = append(newHandles, h)
				// Drop client-side bookkeeping for the rollback handles.
				delete(conn.activeNotifications, h)
			}
		}
		// Restore configs so they can be retried by the next reconnect attempt.
		conn.notificationConfigs = savedConfigs
		conn.notificationChannel = savedChannel
		conn.symbolLock.Unlock()
		if len(newHandles) > 0 {
			deleted := conn.bestEffortDeleteNotifications(newHandles)
			conn.logger.Warn("resubscribe rollback: deleted partial-success handles",
				"new_handles", len(newHandles),
				"deleted", deleted)
		}
		return err
	}
	return nil
}

// cleanupAfterFailedConnect tears down goroutines, closes TCP, and resets
// channels/context so the connection can be retried via Connect().
// Used during initial Connect() when the handshake or route registration fails
// after goroutines have already been started.
func (conn *Connection) cleanupAfterFailedConnect() {
	conn.tearDownAndReset(false)
}

// resetForRetry tears down goroutines, closes the TCP connection, and resets
// channels/state so the next retry iteration starts clean.
func (conn *Connection) resetForRetry() {
	conn.disconnected.Store(true)
	conn.tearDownAndReset(false)
	// Allow route re-registration on next attempt (PLC may have rebooted)
}

// configureKeepAlive enables aggressive TCP keepalive on a connection.
// With Idle=3s, Interval=2s, Count=5: connection declared dead after ~13s of no response.
func configureKeepAlive(c net.Conn) {
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetKeepAliveConfig(net.KeepAliveConfig{
			Enable:   true,
			Idle:     3 * time.Second,
			Interval: 2 * time.Second,
			Count:    5,
		})
	}
}

// zeroOldSymbolHandles iterates the given map and sets Handle=0 on each
// *Symbol value. Used by loadSymbols before replacing conn.symbols so that
// callers holding pointers to the OLD map's *Symbol values fail fast on
// next use (Handle=0 triggers on-demand re-resolution via GetSymbol) rather
// than racing against a PLC that may have reused the old handle for a
// different symbol after reconnect (F-20). Nil-safe.
func zeroOldSymbolHandles(m map[string]*Symbol) {
	for _, s := range m {
		if s != nil {
			s.Handle = 0
		}
	}
}

// loadSymbols loads symbol table and datatypes from the PLC, and saves the symbol version.
func (conn *Connection) loadSymbols() error {
	// Read and store symbol version
	version, err := conn.GetSymbolVersion()
	if err != nil {
		conn.logger.Warn("failed to read symbol version, continuing with symbol load", "error", err)
	} else {
		conn.symbolLock.Lock()
		conn.symbolVersion = version
		conn.symbolLock.Unlock()
	}

	res, err := conn.GetSymbolUploadInfo()
	if err != nil {
		return fmt.Errorf("failed to get symbol upload info: %w", err)
	}
	datatypesResponse, err := conn.getUploadSymbolInfoDataTypes(res.DataTypeLength)
	if err != nil {
		return fmt.Errorf("failed to upload datatypes: %w", err)
	}
	datatypes, err := parseUploadSymbolInfoDataTypes(datatypesResponse)
	if err != nil {
		return fmt.Errorf("failed to parse datatypes: %w", err)
	}
	symbolsResponse, err := conn.getUploadSymbolInfoSymbols(res.SymbolLength)
	if err != nil {
		return fmt.Errorf("failed to upload symbols: %w", err)
	}
	symbols, err := parseUploadSymbolInfoSymbols(symbolsResponse, datatypes)
	if err != nil {
		return fmt.Errorf("failed to parse symbols: %w", err)
	}
	conn.symbolLock.Lock()
	// F-20: invalidate Handle on every old *Symbol before swap so external
	// callers holding old pointers (e.g. infos[i].symbol in
	// readMultipleSymbolsRetry) fail fast on next use and re-resolve via
	// GetSymbol instead of using a stale handle that the PLC may have
	// reassigned to a different symbol after reconnect.
	zeroOldSymbolHandles(conn.symbols)
	conn.datatypes = datatypes
	conn.symbols = symbols
	conn.symbolLock.Unlock()
	return nil
}

// AddRoute registers a route on the remote PLC using this connection's settings.
// It uses callbackIP (from WithHostIP) if set, otherwise derives the callback
// address from the source AMS NetID (first 4 bytes = IP).
func (conn *Connection) AddRoute(routeName, username, password string) error {
	hostIP := conn.callbackIP
	if hostIP == "" {
		hostIP = fmt.Sprintf("%d.%d.%d.%d",
			conn.source.NetID[0], conn.source.NetID[1],
			conn.source.NetID[2], conn.source.NetID[3])
	}
	return AddRemoteRouteWithLogger(conn.logger, conn.ip, conn.source.NetID, routeName, hostIP, username, password)
}

// IsDisconnected returns whether the connection is currently in a disconnected state.
func (conn *Connection) IsDisconnected() bool {
	return conn.disconnected.Load()
}

// isRunningInContainer returns true if the process is running inside a
// Docker, Podman, or Kubernetes container. Uses filesystem markers rather
// than IP range heuristics (10.x is common in industrial OT networks).
func isRunningInContainer() bool {
	// Docker/Podman marker file
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return true
	}
	// Check cgroup for container runtime (Linux only)
	data, err := os.ReadFile("/proc/1/cgroup")
	if err != nil {
		return false
	}
	s := string(data)
	return strings.Contains(s, "docker") || strings.Contains(s, "containerd") ||
		strings.Contains(s, "kubepods") || strings.Contains(s, "lxc")
}

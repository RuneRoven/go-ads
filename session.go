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
	"time"
)

// secret is an internal wrapper around password/credential strings that
// implements String() and slog.LogValuer to return "[REDACTED]" instead
// of the raw value. Defends against accidental leaks via fmt.Sprintf("%+v",
// conn) or slog.Any("conn", conn).
//
// Kept unexported. The Session's public API for credentials remains
// plain string — conversion happens at the boundary.
type secret string

func (s secret) String() string {
	return "[REDACTED]"
}

func (s secret) LogValue() slog.Value {
	return slog.StringValue("[REDACTED]")
}

type Session struct {
	ip   string
	port int

	// Underlying RPC client. nil until Connect succeeds; replaced on
	// Reconnect; shut down by Close. Phase 5.a-types declares the field;
	// Phase 5.a-dial + Phase 5.b populate and drive it.
	client *Client //nolint:unused // Phase 5.a-dial wires this.

	// TCP socket + request multiplexing + listen/transmit channels.
	tx *transport

	target     AMSAddress
	source     AMSAddress
	callbackIP string // IP PLC uses to reach us (for Docker/VPN; set via WithHostIP)

	// Symbol cache + data-type table + discovery-mode flags.
	cache *symbolCache

	// Notification state (activeNotifications, notificationConfigs,
	// notificationChannel, lastSubscribeNs).
	notifs *notificationManager

	// Lifecycle FSM (ctx, shutdown, waitGroup, reconnect/closed flags + channels,
	// generation counter, retry policy).
	lifecycle *reconnector

	requestTimeout time.Duration
	isLocal        bool

	// Event callbacks (run in goroutine, must not block)
	onDisconnect func()
	onReconnect  func()

	// Feature support state. See capabilities type for state transitions.
	capabilities capabilities

	// Route registration config (populated by WithRoute / WithForceRouteRegistration).
	route *routeManager

	logger *slog.Logger
}

// NewConnection creates a new ADS connection. requestTimeout is the timeout for individual ADS requests.
// If requestTimeout is 0, a default of 5000ms is used.
// If localPort is 0, a default of 10500 is used (arbitrary AMS source port for protocol headers).
//
// The connection manages its own lifecycle context internally so that Close()
// can send cleanup commands regardless of any caller context state.
// Use conn.Close() to shut down the connection.
func NewSession(ip string, port int, netid string, amsPort int, localNetID string, localPort int, requestTimeout time.Duration, opts ...SessionOption) (conn *Session, err error) {
	if requestTimeout <= 0 {
		requestTimeout = 5000 * time.Millisecond
	}
	conn = &Session{
		ip:             ip,
		port:           port,
		requestTimeout: requestTimeout,
		route:          &routeManager{},
		notifs:         &notificationManager{activeNotifications: make(map[uint32]*Symbol)},
		cache: &symbolCache{
			symbols:         map[string]*Symbol{},
			onDemandSymbols: map[string]bool{},
		},
		tx: &transport{
			sendChannel:    make(chan []byte),
			systemResponse: make(chan []byte),
			recvQueue:      make(chan []byte, recvQueueSize),
			activeRequests: map[uint32]chan []byte{},
		},
		lifecycle: &reconnector{
			autoReconnect:        true,
			maxReconnectAttempts: 0, // 0 = infinite retries
			backoffConfig:        DefaultBackoffConfig(),
			closedCh:             make(chan struct{}),
		},
		logger: slog.Default(),
	}
	conn.lifecycle.ctx, conn.lifecycle.shutdown = context.WithCancel(context.Background()) //nolint:gosec // cancel stored in lifecycle.shutdown, called from Close
	// FSM Phase 1 (shadow): explicit Constructed entry. Idempotent — state
	// zero value already maps to SessionStateConstructed. Real transitions
	// land in subsequent commits as readers/writers swap over.
	conn.lifecycle.state.transitionTo(SessionStateConstructed)
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
	// closedCh and ctx/shutdown initialized in struct literal above (lifecycle field).
	return
}

func (conn *Session) Connect(local bool) error {
	conn.isLocal = local
	conn.transitionState(SessionStateConnecting)
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
	conn.tx.connMu.Lock()
	conn.tx.connection = tcpConn
	conn.tx.connMu.Unlock()
	// Enable aggressive TCP keepalive to detect dead connections quickly.
	// With Idle=3s, Interval=2s, Count=5: connection declared dead after ~13s of no response.
	// This ensures cable unplugs (>13s) are detected and trigger reconnect,
	// while not affecting slow-changing notification data (keepalive is TCP-level, not app-level).
	configureKeepAlive(tcpConn)
	// Log TCP socket (transport-level only — ADS route validation happens on first ADS command)
	conn.logger.Info("TCP socket established (ADS route not yet verified)",
		"local", conn.tx.connection.LocalAddr().String(),
		"remote", conn.tx.connection.RemoteAddr().String())

	// Auto-derive source AMS NetID from local IP if source NetID is all zeros.
	// Take connMu around the read+write of conn.source so encode() at ams.go
	// (which reads conn.source under connMu) cannot interleave even in the
	// theoretical case of a goroutine surviving across reconnect cycles.
	conn.tx.connMu.Lock()
	if conn.source.NetID == [6]byte{} {
		localAddr, ok := conn.tx.connection.LocalAddr().(*net.TCPAddr)
		if !ok {
			conn.tx.connMu.Unlock()
			return fmt.Errorf("unexpected local address type: %T", conn.tx.connection.LocalAddr())
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
	conn.tx.connMu.Unlock()

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
	conn.lifecycle.waitGroup.Add(2 + recvWorkerCount)
	go conn.listen()
	go conn.transmitWorker()
	for i := 0; i < recvWorkerCount; i++ {
		go conn.recvWorker()
	}
	if local {
		resp, err := conn.send([]byte{0, 16, 2, 0, 0, 0, 0, 0})
		if err != nil {
			conn.tearDownAndReset(false)
			return fmt.Errorf("local mode handshake failed: %w", err)
		}
		buf := bytes.NewBuffer(resp)
		result := AMSAddress{}
		conn.logger.Log(context.Background(), LevelTrace, "got stuff", "stuff", buf.Bytes())
		err = binary.Read(buf, binary.LittleEndian, &result)
		if err != nil {
			conn.tearDownAndReset(false)
			return fmt.Errorf("local mode binary read failed: %w", err)
		}
		conn.logger.Info("local mode handshake result", "result", result)
		conn.tx.connMu.Lock()
		conn.source = result
		conn.tx.connMu.Unlock()
	}

	// Smart route registration: probe PLC first, register only if route missing.
	// Route registration is UDP-based but needs goroutines running for the probe
	// (which sends an ADS command over TCP). If route is registered, TCP reconnect
	// is needed because PLC may close connections from previously-unknown NetIDs.
	if conn.route.name != "" {
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
		conn.cache.lock.Lock()
		conn.cache.symbolVersion = version
		conn.cache.lock.Unlock()
	}
	conn.transitionState(SessionStateConnected)
	return nil
}

// ensureRouteOnConnect probes the PLC and registers a route if needed during Connect().
// Returns (registered bool, err error) where registered=true means a route was added
// and the caller should TCP-reconnect.
func (conn *Session) ensureRouteOnConnect() (registered bool, err error) {
	if conn.isClosed() {
		return false, fmt.Errorf("connection closed")
	}

	// Force mode → always register
	if conn.route.forceRouteRegistration {
		conn.logger.Info("registering route (force mode)")
		err = conn.AddRoute(conn.route.name, conn.route.username, string(conn.route.password))
		return err == nil, err
	}

	// Probe: try a lightweight ADS command to see if route exists
	_, probeErr := conn.GetSymbolVersion()
	if probeErr == nil {
		conn.logger.Info("route already exists on PLC, skipping registration")
		conn.route.routeProbeFailures.Store(0)
		return false, nil
	}

	if conn.isClosed() {
		return false, fmt.Errorf("connection closed during route probe")
	}

	// Probe failed → register with credentials
	conn.route.routeProbeFailures.Inc()
	conn.logger.Info("route probe failed, registering route", "error", probeErr)
	err = conn.AddRoute(conn.route.name, conn.route.username, string(conn.route.password))
	if err != nil {
		return false, err
	}
	return true, nil
}

// Close closes connection and waits for completion
func (conn *Session) Close() {
	// Capture transport-disconnected state BEFORE the FSM transitions into
	// Closed. The cleanup branch below uses this to decide whether to attempt
	// network ops; once state is Closed, isDisconnected() returns false even
	// if the transport was already gone.
	wasDisconnected := conn.isDisconnected()
	if _, ok := conn.lifecycle.state.transitionToOnce(SessionStateClosed); !ok {
		return // already closed (or transition not permitted from current state)
	}
	close(conn.lifecycle.closedCh)
	conn.logger.Info("Close called, shutting down")

	// Skip handle cleanup if already disconnected — all commands would timeout
	if !wasDisconnected {
		// Delete all active notifications (uses sum command with automatic fallback to individual)
		conn.notifs.lock.Lock()
		handles := make([]uint32, 0, len(conn.notifs.activeNotifications))
		for handle := range conn.notifs.activeNotifications {
			handles = append(handles, handle)
		}
		conn.notifs.lock.Unlock()
		if len(handles) > 0 {
			codes, err := conn.SumDeleteDeviceNotification(handles)
			if err != nil {
				conn.logger.Warn("failed to delete notification handles during close", "error", err)
			} else {
				for i, h := range handles {
					if codes[i] != ReturnCodeNoErrors {
						conn.logger.Warn("failed to delete notification handle", "handle", h, "error", uint32(codes[i]))
					} else {
						conn.logger.Info("removed notification handle", "handle", h)
					}
				}
			}
		}
		// Collect symbol handles under lock, then release without holding the lock
		conn.cache.lock.Lock()
		var symHandles []uint32
		for _, symbol := range conn.cache.symbols {
			if symbol.Handle != 0 {
				symHandles = append(symHandles, symbol.Handle)
			}
		}
		conn.cache.lock.Unlock()

		// Release handles individually — ADS has no batch release command,
		// and Close() is not performance-critical.
		// re-check disconnected each iteration so a mid-loop PLC failure
		// (listen detects EOF → triggerReconnect → disconnected=true) doesn't
		// force every remaining Write to time out. Bail out early.
		for i, h := range symHandles {
			if conn.isDisconnected() {
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
	conn.lifecycle.ctxMu.RLock()
	conn.lifecycle.shutdown()
	conn.lifecycle.ctxMu.RUnlock()
	// Close the TCP connection to unblock listen() which may be stuck in ReadFull
	conn.tx.connMu.Lock()
	if conn.tx.connection != nil {
		conn.tx.connection.Close()
	}
	conn.tx.connMu.Unlock()
	// Wait for any in-progress reconnect to stop BEFORE waiting on the
	// goroutine waitGroup. Reconnect's retry loop may call waitGroup.Add(2)
	// after we Close — calling Wait first would race with that Add and
	// trigger "sync: WaitGroup misuse". closedCh signals Reconnect
	// to exit its retry loop promptly; reconnectDone is closed when Reconnect
	// returns.
	conn.lifecycle.reconnectMu.Lock()
	ch := conn.lifecycle.reconnectDone
	conn.lifecycle.reconnectMu.Unlock()
	if ch != nil {
		<-ch
	}
	conn.logger.Info("Waiting for workers to close")
	conn.lifecycle.waitGroup.Wait()
	conn.logger.Info("Close DONE")
}

// ErrDisconnected indicates the underlying TCP connection is not available —
// either Close() has been called or a reconnect has failed. Callers should use
// errors.Is(err, ErrDisconnected) to detect this case.
var ErrDisconnected = errors.New("connection is disconnected")

// reconnectBackoff returns the delay for the given reconnect attempt number (1-indexed)
// based on the configured BackoffConfig tiers.
func (conn *Session) reconnectBackoff(attempt int) time.Duration {
	cfg := conn.lifecycle.backoffConfig
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
func (conn *Session) reconnectSleep(attempt int) error {
	delay := conn.reconnectBackoff(attempt)
	conn.logger.Info("reconnect backoff", "attempt", attempt, "delay", delay)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-conn.lifecycle.closedCh:
		return fmt.Errorf("connection closed during reconnect")
	}
}

// triggerReconnect prepares the connection state for reconnection and launches
// the Reconnect goroutine (if auto-reconnect is enabled). It sets disconnected=true
// and creates the reconnectDone channel BEFORE launching the goroutine, eliminating
// the race window where callers could see a "healthy" connection between the trigger
// and Reconnect() being scheduled.
func (conn *Session) triggerReconnect() {
	if conn.isClosed() {
		return
	}
	// CAS ensures only the first goroutine to detect disconnect fires the callback
	// and sets up reconnection. Subsequent callers (e.g. both listen() and transmitWorker()
	// detecting the same TCP failure) skip the callback to avoid double-firing.
	firstDetector := conn.lifecycle.disconnected.CompareAndSwap(false, true)
	if firstDetector {
		conn.transitionState(SessionStateDisconnected)
	}
	conn.lifecycle.reconnectMu.Lock()
	if conn.lifecycle.reconnectDone == nil {
		conn.lifecycle.reconnectDone = make(chan struct{})
	}
	conn.lifecycle.reconnectMu.Unlock()

	// Fire disconnect callback in goroutine (must not block).
	// Callback must not call Session methods — connection may be closing.
	if firstDetector && conn.onDisconnect != nil && !conn.isClosed() {
		go conn.onDisconnect()
	}

	if conn.lifecycle.autoReconnect {
		go conn.Reconnect()
	} else {
		// No auto-reconnect: close reconnectDone immediately so sendRequest
		// waiters unblock with ErrDisconnected instead of hanging forever.
		conn.lifecycle.reconnectMu.Lock()
		if conn.lifecycle.reconnectDone != nil {
			close(conn.lifecycle.reconnectDone)
			conn.lifecycle.reconnectDone = nil
		}
		conn.lifecycle.reconnectMu.Unlock()
	}
}

// Reconnect attempts to re-establish the TCP connection, reload symbols,
// and re-subscribe to previously registered notifications.
// Uses configurable backoff (see WithBackoff) with fast initial retries and
// progressive slowdown. Backoff resets on each successful reconnect.
func (conn *Session) Reconnect() error {
	if conn.isClosed() {
		return fmt.Errorf("connection closed")
	}
	// Prevent concurrent reconnect attempts. transitionToOnce returns
	// ok=false on idempotent re-entry (state already Reconnecting), which
	// is exactly the single-flight gate we want.
	if _, ok := conn.lifecycle.state.transitionToOnce(SessionStateReconnecting); !ok {
		conn.logger.Info("reconnect already in progress or not permitted from current state, skipping")
		return nil
	}

	// Create a channel that waiters (sendRequest) can block on.
	// triggerReconnect() may have already created it — only create if nil.
	conn.lifecycle.reconnectMu.Lock()
	if conn.lifecycle.reconnectDone == nil {
		conn.lifecycle.reconnectDone = make(chan struct{})
	}
	conn.lifecycle.reconnectMu.Unlock()

	defer func() {
		conn.lifecycle.reconnectMu.Lock()
		if conn.lifecycle.reconnectDone != nil {
			close(conn.lifecycle.reconnectDone)
			conn.lifecycle.reconnectDone = nil
		}
		conn.lifecycle.reconnectMu.Unlock()
	}()

	conn.logger.Info("attempting reconnect")
	conn.lifecycle.disconnected.Store(true)
	// State is already Reconnecting (transitionToOnce above). The redundant
	// transitionState call removed — Phase 1.1 wired it before the gate
	// existed.

	// Clear active notifications (old handles invalid after reconnect).
	conn.notifs.lock.Lock()
	conn.notifs.activeNotifications = make(map[uint32]*Symbol)
	conn.notifs.lock.Unlock()

	conn.tearDownAndReset(true)

	var lastErr error
	attempts := 0
	for {
		if conn.isClosed() {
			return fmt.Errorf("connection closed during reconnect")
		}
		attempts++
		if conn.lifecycle.maxReconnectAttempts > 0 && attempts > conn.lifecycle.maxReconnectAttempts {
			return fmt.Errorf("reconnect failed after %d attempts: %w", conn.lifecycle.maxReconnectAttempts, lastErr)
		}

		// Dial TCP, configure keepalive, clear disconnected flag, start goroutines.
		// dialAndStart re-checks closed.Load() before waitGroup.Add(2).
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
		if err := conn.ensureRoute(); err != nil {
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

		conn.lifecycle.disconnected.Store(false)
		conn.lifecycle.strictReconnectFailures = 0 // reset on success
		// epoch bumps inside the transition helper when target == Connected.
		conn.transitionState(SessionStateConnected)
		conn.logger.Info("reconnect successful", "attempts", attempts)

		// Fire reconnect callback in goroutine (must not block).
		// Callback must not call Session methods — connection may be closing.
		if conn.onReconnect != nil && !conn.isClosed() {
			go conn.onReconnect()
		}
		return nil
	}
}

// ensureRoute checks if the route exists (via probe) and registers if needed.
// On force mode or after repeated probe failures, skips the probe.
// Returns a non-nil error only if registration was attempted and failed critically
// (requiring a TCP reset / retry).
func (conn *Session) ensureRoute() error {
	if conn.route.name == "" {
		return nil
	}
	if conn.isClosed() {
		return fmt.Errorf("connection closed")
	}

	// Force mode or too many probe failures → always register
	probeFailures := conn.route.routeProbeFailures.Load()
	if conn.route.forceRouteRegistration || probeFailures >= 3 {
		conn.logger.Info("registering route (forced/fallback)", "probeFailures", probeFailures)
		if err := conn.AddRoute(conn.route.name, conn.route.username, string(conn.route.password)); err != nil {
			return fmt.Errorf("route registration failed: %w", err)
		}

		conn.route.routeProbeFailures.Store(0)
		return nil
	}

	// Probe: try a lightweight ADS command to see if route already exists
	_, probeErr := conn.GetSymbolVersion()
	if probeErr == nil {
		conn.logger.Debug("route still valid, skipping re-registration")
		conn.route.routeProbeFailures.Store(0)
		return nil
	}

	if conn.isClosed() {
		return fmt.Errorf("connection closed during route probe")
	}

	// Probe failed → register with credentials
	failuresAfter := conn.route.routeProbeFailures.Inc()
	conn.logger.Info("route probe failed, registering route", "error", probeErr, "probeFailures", failuresAfter)
	if err := conn.AddRoute(conn.route.name, conn.route.username, string(conn.route.password)); err != nil {
		return fmt.Errorf("route registration failed after probe: %w", err)
	}

	return nil
}

// filterValidNotificationConfigs returns only configs whose symbols still exist
// in the current symbol table. Logs a warning for dropped subscriptions.
func (conn *Session) filterValidNotificationConfigs(configs []NotificationConfig) []NotificationConfig {
	conn.cache.lock.Lock()
	defer conn.cache.lock.Unlock()

	valid := make([]NotificationConfig, 0, len(configs))
	for _, cfg := range configs {
		if _, exists := conn.cache.symbols[symbolKey(cfg.SymbolName)]; exists {
			valid = append(valid, cfg)
		} else if _, onDemand := conn.cache.onDemandSymbols[symbolKey(cfg.SymbolName)]; onDemand {
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
func (conn *Session) reloadSymbols() error {
	conn.cache.lock.Lock()
	fullyLoaded := conn.cache.symbolsFullyLoaded
	listLoaded := conn.cache.symbolListLoaded
	dtLoaded := conn.cache.datatypesLoaded
	hasOnDemand := len(conn.cache.onDemandSymbols) > 0
	conn.cache.lock.Unlock()

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
		conn.cache.lock.Lock()
		oldSymbols := conn.cache.onDemandSymbols
		conn.cache.symbols = make(map[string]*Symbol)
		conn.cache.onDemandSymbols = make(map[string]bool)
		conn.bumpEpoch()
		conn.cache.lock.Unlock()

		for name := range oldSymbols {
			if _, err := conn.getSymbol(name); err != nil {
				if conn.lifecycle.strictReconnect {
					conn.lifecycle.strictReconnectFailures++
					if conn.lifecycle.strictReconnectMaxAttempts == 0 || conn.lifecycle.strictReconnectFailures > conn.lifecycle.strictReconnectMaxAttempts {
						return fmt.Errorf("re-resolve symbol %q (strict mode, %d failures): %w", name, conn.lifecycle.strictReconnectFailures, err)
					}
					return fmt.Errorf("re-resolve symbol %q (strict mode, attempt %d/%d): %w", name, conn.lifecycle.strictReconnectFailures, conn.lifecycle.strictReconnectMaxAttempts, err)
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
			conn.cache.lock.Lock()
			conn.cache.symbolVersion = version
			conn.cache.lock.Unlock()
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
//   - resetForRetry()
func (conn *Session) tearDownAndReset(resetFeatureFlags bool) {
	conn.lifecycle.ctxMu.RLock()
	conn.lifecycle.shutdown()
	conn.lifecycle.ctxMu.RUnlock()
	conn.tx.connMu.Lock()
	if conn.tx.connection != nil {
		conn.tx.connection.Close()
	}
	conn.tx.connMu.Unlock()
	conn.lifecycle.waitGroup.Wait()
	conn.lifecycle.ctxMu.Lock()
	conn.lifecycle.ctx, conn.lifecycle.shutdown = context.WithCancel(context.Background()) //nolint:gosec // cancel stored in lifecycle.shutdown, called from Close
	conn.lifecycle.ctxMu.Unlock()
	conn.tx.chanMu.Lock()
	conn.tx.sendChannel = make(chan []byte)
	conn.tx.systemResponse = make(chan []byte)
	conn.tx.recvQueue = make(chan []byte, recvQueueSize)
	conn.tx.chanMu.Unlock()
	conn.tx.activeRequestLock.Lock()
	conn.tx.activeRequests = map[uint32]chan []byte{}
	conn.tx.activeRequestLock.Unlock()
	if resetFeatureFlags {
		conn.capabilities.reset()
	}
}

// dialAndStart performs net.DialTimeout, configures keepalive, clears the
// disconnected flag, and starts the listen/transmit goroutines. Used by both
// Connect()'s post-route-registration redial path and Reconnect()'s retry loop.
// Re-checks closed before waitGroup.Add(2) to prevent the sync.WaitGroup
// misuse race.
func (conn *Session) dialAndStart() error {
	newConn, err := net.DialTimeout("tcp", net.JoinHostPort(conn.ip, strconv.Itoa(conn.port)), conn.requestTimeout)
	if err != nil {
		return err
	}
	conn.tx.connMu.Lock()
	conn.tx.connection = newConn
	conn.tx.connMu.Unlock()
	configureKeepAlive(newConn)
	conn.lifecycle.disconnected.Store(false)
	if conn.isClosed() {
		// Session was Closed mid-dial. Don't Add to waitGroup.
		conn.tx.connMu.Lock()
		newConn.Close()
		conn.tx.connection = nil
		conn.tx.connMu.Unlock()
		return fmt.Errorf("connection closed during dial")
	}
	conn.lifecycle.waitGroup.Add(2 + recvWorkerCount)
	go conn.listen()
	go conn.transmitWorker()
	for i := 0; i < recvWorkerCount; i++ {
		go conn.recvWorker()
	}
	return nil
}

// localHandshake performs the local-mode AMSAddress probe used after dial when
// isLocal is true. Updates conn.source on success.
func (conn *Session) localHandshake() error {
	resp, err := conn.send([]byte{0, 16, 2, 0, 0, 0, 0, 0})
	if err != nil {
		return fmt.Errorf("local handshake send: %w", err)
	}
	buf := bytes.NewBuffer(resp)
	result := AMSAddress{}
	if err := binary.Read(buf, binary.LittleEndian, &result); err != nil {
		return fmt.Errorf("local handshake parse: %w", err)
	}
	conn.tx.connMu.Lock()
	conn.source = result
	conn.tx.connMu.Unlock()
	return nil
}

// resubscribeNotifications restores notification subscriptions stored in
// notificationConfigs after a successful reconnect. Filters out symbols that
// no longer exist after symbol reload. On error, rolls back partial PLC-side
// successes and restores the saved configs so they can be retried by
// the next reconnect attempt.
func (conn *Session) resubscribeNotifications() error {
	conn.notifs.lock.Lock()
	savedConfigs := conn.notifs.notificationConfigs
	savedChannel := conn.notifs.notificationChannel
	conn.notifs.notificationConfigs = nil // Clear before re-adding to prevent duplicates
	conn.notifs.lock.Unlock()
	if len(savedConfigs) == 0 || savedChannel == nil {
		return nil
	}
	validConfigs := conn.filterValidNotificationConfigs(savedConfigs)
	if len(validConfigs) == 0 {
		// All symbols gone (e.g., PLC online change removed all subscribed vars).
		// Clear channel reference so a future AddSymbolNotification can use a new channel.
		conn.notifs.lock.Lock()
		conn.notifs.notificationChannel = nil
		conn.notifs.lock.Unlock()
		return nil
	}
	// Snapshot active handles before the re-subscribe attempt. If
	// AddSymbolNotifications partially succeeds and then errors, we use the
	// snapshot diff to roll back the PLC-side registrations created during
	// this attempt. Without rollback, repeated reconnect retries
	// accumulate orphaned PLC notifications until the next TCP disconnect.
	conn.notifs.lock.Lock()
	preHandles := make(map[uint32]struct{}, len(conn.notifs.activeNotifications))
	for h := range conn.notifs.activeNotifications {
		preHandles[h] = struct{}{}
	}
	conn.notifs.lock.Unlock()

	subResults, err := conn.AddSymbolNotifications(validConfigs, savedChannel)

	// Collect Skipped+Handle entries: AddSymbolNotifications surfaces handles
	// for items where the PLC accepted but the library refused to commit
	// (concurrent-subscribe TOCTOU loss, cache-stranded post-roundtrip).
	// Those handles are NOT in activeNotifications - they would leak unless
	// we explicitly release them. Re-append the corresponding config to
	// notificationConfigs (with attempt counter incremented) so the NEXT
	// reconnect retries; drop after resubscribeMaxAttempts to prevent infinite
	// churn on persistently-flapping symbols.
	var orphanHandles []uint32
	var retryConfigs []NotificationConfig
	var droppedConfigs []string
	for i, r := range subResults {
		if r.Skipped != nil && r.Handle != 0 {
			orphanHandles = append(orphanHandles, r.Handle)
		}
		if r.Skipped != nil && i < len(validConfigs) {
			cfg := validConfigs[i]
			cfg.resubscribeAttempts++
			if cfg.resubscribeAttempts >= resubscribeMaxAttempts {
				droppedConfigs = append(droppedConfigs, cfg.SymbolName)
				continue
			}
			retryConfigs = append(retryConfigs, cfg)
		}
	}
	if len(orphanHandles) > 0 {
		deleted := conn.bestEffortDeleteNotifications(orphanHandles)
		conn.logger.Warn("resubscribe: released PLC handles for Skipped+Handle entries",
			"orphan_handles", len(orphanHandles),
			"deleted", deleted)
	}
	if len(retryConfigs) > 0 {
		conn.notifs.lock.Lock()
		conn.notifs.notificationConfigs = append(conn.notifs.notificationConfigs, retryConfigs...)
		conn.notifs.lock.Unlock()
		conn.logger.Info("resubscribe: queued Skipped configs for next reconnect retry",
			"retry_count", len(retryConfigs))
	}
	if len(droppedConfigs) > 0 {
		conn.logger.Warn("resubscribe: dropping configs after max retries",
			"dropped", droppedConfigs,
			"max_attempts", resubscribeMaxAttempts)
	}

	if err != nil {
		// Identify handles created during THIS attempt and best-effort delete.
		conn.notifs.lock.Lock()
		var newHandles []uint32
		for h := range conn.notifs.activeNotifications {
			if _, existed := preHandles[h]; !existed {
				newHandles = append(newHandles, h)
				// Drop client-side bookkeeping for the rollback handles.
				delete(conn.notifs.activeNotifications, h)
			}
		}
		// Restore configs so they can be retried by the next reconnect attempt.
		conn.notifs.notificationConfigs = savedConfigs
		conn.notifs.notificationChannel = savedChannel
		conn.notifs.lock.Unlock()
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

// resetForRetry tears down goroutines, closes the TCP connection, and resets
// channels/state so the next retry iteration starts clean.
func (conn *Session) resetForRetry() {
	conn.lifecycle.disconnected.Store(true)
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

// zeroOldSymbolHandles invalidates each *Symbol in the given map. Sets
// Handle=0 so callers holding pointers to OLD-map values force on-demand
// re-resolution via GetSymbol (defends against the PLC reusing the old
// handle for a different symbol after reconnect), and clears cached
// Value/Valid/ValueParsed/LastUpdateTime so a Read within MinUpdateInterval
// of reconnect does not return stale pre-disconnect data. Nil-safe.
func zeroOldSymbolHandles(m map[string]*Symbol) {
	for _, s := range m {
		if s != nil {
			s.Handle = 0
			s.Value = ""
			s.Valid = false
			s.ValueParsed = false
			s.LastUpdateTime = time.Time{}
		}
	}
}

// loadSymbols loads symbol table and datatypes from the PLC, and saves the symbol version.
func (conn *Session) loadSymbols() error {
	// Read and store symbol version
	version, err := conn.GetSymbolVersion()
	if err != nil {
		conn.logger.Warn("failed to read symbol version, continuing with symbol load", "error", err)
	} else {
		conn.cache.lock.Lock()
		conn.cache.symbolVersion = version
		conn.cache.lock.Unlock()
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
	conn.cache.lock.Lock()
	// invalidate Handle on every old *Symbol before swap so external
	// callers holding old pointers (e.g. infos[i].symbol in
	// readMultipleSymbolsRetry) fail fast on next use and re-resolve via
	// GetSymbol instead of using a stale handle that the PLC may have
	// reassigned to a different symbol after reconnect.
	zeroOldSymbolHandles(conn.cache.symbols)
	conn.cache.datatypes = datatypes
	conn.cache.symbols = symbols
	conn.bumpEpoch()
	conn.cache.lock.Unlock()
	return nil
}

// AddRoute registers a route on the remote PLC using this connection's settings.
// It uses callbackIP (from WithHostIP) if set, otherwise derives the callback
// address from the source AMS NetID (first 4 bytes = IP).
func (conn *Session) AddRoute(routeName, username, password string) error {
	hostIP := conn.callbackIP
	if hostIP == "" {
		hostIP = fmt.Sprintf("%d.%d.%d.%d",
			conn.source.NetID[0], conn.source.NetID[1],
			conn.source.NetID[2], conn.source.NetID[3])
	}
	return AddRemoteRouteWithLogger(conn.logger, conn.ip, conn.source.NetID, routeName, hostIP, username, password)
}

// IsDisconnected returns whether the connection is currently in a disconnected state.
func (conn *Session) IsDisconnected() bool {
	return conn.isDisconnected()
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

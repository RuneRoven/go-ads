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
)

// sessionLifecycle owns Session's lifecycle plumbing: cancellation context,
// goroutine waitgroup, single-flight reconnect signaling, the explicit FSM
// state + unified epoch counter, and the retry policy. Folded into session.go
// because every field here is owned and mutated by Session methods only —
// no other type touches it.
type sessionLifecycle struct {
	ctxMu     sync.RWMutex // protects ctx and shutdown against concurrent access during reconnect
	ctx       context.Context
	shutdown  context.CancelFunc
	waitGroup sync.WaitGroup

	reconnectMu   sync.Mutex // protects reconnectDone
	reconnectDone chan struct{}

	closedCh chan struct{}

	// state is the explicit FSM state plus the unified epoch counter
	// (specs/09-fsm-design.md). FSM is the source of truth for closed and
	// reconnecting. epoch replaces the previous cache.generation and
	// reconnectGeneration counters and bumps on every Connected entry plus
	// on user-driven cache swaps that don't (yet) transition through
	// Reloading.
	state sessionFSM

	autoReconnect              bool
	maxReconnectAttempts       int
	backoffConfig              BackoffConfig
	strictReconnect            bool
	strictReconnectMaxAttempts int
	strictReconnectFailures    int
}

// secret is an internal wrapper around password/credential strings that
// implements String() and slog.LogValuer to return "[REDACTED]" instead
// of the raw value. Defends against accidental leaks via fmt.Sprintf("%+v",
// sess) or slog.Any("sess", sess).
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

// Session is the managed wrapper around a single ADS *Client. It owns the
// symbol cache, persistent notifications (with auto-resubscribe across
// reconnect), the lifecycle FSM, the auto-reconnect retry loop, and the
// route-registration state. Construct via NewSession; call Connect to dial
// the PLC and start the workers.
//
// Session does NOT embed *Client. Raw RPC methods (Read, Write, Sum*,
// process-image, etc.) are NOT exposed on Session; reach them by
// constructing a separate *Client via Dial. Session's public surface is
// limited to the cache-aware / persistence-aware / lifecycle-aware
// methods: ReadFromSymbol, WriteToSymbol, ReadMultipleSymbols,
// WriteMultipleSymbols, GetSymbol, ListSymbols, BrowseSymbols,
// AddSymbolNotification(s), LoadSymbols, LoadSymbolsSlow, LoadSymbolList,
// LoadDataTypes, RefreshSymbols, CheckSymbolVersion, AddRoute, Connect,
// Close, Reconnect, IsDisconnected.
//
// See specs/09-fsm-design.md for the full layered architecture.
type Session struct {
	ip   string
	port int

	// Underlying RPC client. nil until Connect succeeds; replaced on
	// Reconnect; shut down by Close.
	client *Client

	// TCP socket + request multiplexing + listen/transmit channels.
	tx *transport

	target     AMSAddress
	source     AMSAddress
	callbackIP string // IP PLC uses to reach us (for Docker/VPN; set via WithHostIP)

	// Symbol cache + data-type table + discovery-mode flags.
	cache *symbolCache

	// Notification state (activeNotifications, notificationConfigs,
	// notificationChannel, lastSubscribeNs).
	notifications *notificationManager

	// Lifecycle FSM (ctx, shutdown, waitGroup, reconnect/closed flags + channels,
	// generation counter, retry policy).
	lifecycle *sessionLifecycle

	requestTimeout time.Duration
	isLocal        bool

	// Event callbacks (run in goroutine, must not block)
	onDisconnect func()
	onReconnect  func()

	// Online-change handling (R-SES-011, R-CACHE-013).
	versionStrategy   SymbolVersionStrategy
	versionCallback   func(reason string)
	maxReloadAttempts int
	reloadWindow      time.Duration
	reloadAttempts    []time.Time
	reloadMu          sync.Mutex
	staleHandles      map[uint32]string
	staleHandlesMu    sync.Mutex

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
// Use sess.Close() to shut down the connection.
func NewSession(ip string, port int, netid string, amsPort int, localNetID string, localPort int, requestTimeout time.Duration, opts ...SessionOption) (sess *Session, err error) {
	if requestTimeout <= 0 {
		requestTimeout = 5000 * time.Millisecond
	}
	sess = &Session{
		ip:             ip,
		port:           port,
		requestTimeout: requestTimeout,
		route:          &routeManager{},
		notifications:  &notificationManager{activeNotifications: make(map[uint32]*Symbol)},
		cache: &symbolCache{
			symbols:         map[string]*Symbol{},
			onDemandSymbols: map[string]bool{},
		},
		tx: &transport{
			sendChannel:    make(chan []byte),
			systemResponse: make(chan []byte, 1),
			recvQueue:      make(chan []byte, recvQueueSize),
			activeRequests: map[uint32]chan []byte{},
		},
		lifecycle: &sessionLifecycle{
			autoReconnect:        true,
			maxReconnectAttempts: 0, // 0 = infinite retries
			backoffConfig:        DefaultBackoffConfig(),
			closedCh:             make(chan struct{}),
		},
		logger: slog.Default(),
	}
	sess.lifecycle.ctx, sess.lifecycle.shutdown = context.WithCancel(context.Background()) //nolint:gosec // cancel stored in lifecycle.shutdown, called from Close
	// idempotent: zero state is Constructed already
	sess.lifecycle.state.transitionTo(SessionStateConstructed)
	// Online-change defaults (R-SES-011, R-CACHE-013). Applied before opts so
	// callers can override. versionStrategy zero-value = SymbolVersionAutoReload
	// is intentional — no init needed (verified by
	// TestSymbolVersionStrategy_ZeroValueIsAutoReload).
	sess.maxReloadAttempts = 3
	sess.reloadWindow = 60 * time.Second
	for _, opt := range opts {
		opt(sess)
	}
	netIDBytes, err := stringToNetID(netid)
	if err != nil {
		return nil, fmt.Errorf("invalid target NetID: %w", err)
	}
	sess.target.NetID = netIDBytes
	sess.target.Port = uint16(amsPort)
	if localNetID != "auto" && localNetID != "" {
		localBytes, err := stringToNetID(localNetID)
		if err != nil {
			return nil, fmt.Errorf("invalid local NetID: %w", err)
		}
		sess.source.NetID = localBytes
	}
	// If localNetID is "auto" or empty, source.NetID stays zero and will be auto-derived in Connect()
	if localPort <= 0 {
		localPort = 10500
	}
	sess.source.Port = uint16(localPort)
	// closedCh and ctx/shutdown initialized in struct literal above (lifecycle field).
	return
}

// Connect dials the PLC and transitions the session to Connected.
// local=true targets the in-process TwinCAT runtime (127.0.0.1).
func (sess *Session) Connect(local bool) error {
	sess.isLocal = local
	sess.transitionState(SessionStateConnecting)
	var err error
	sess.logger.Debug("dialing", "ip", sess.ip, "port", sess.port)
	if local {
		sess.target.NetID = [6]byte{127, 0, 0, 1, 1, 1}
		sess.ip = "127.0.0.1"
	}
	tcpConn, err := net.DialTimeout("tcp", net.JoinHostPort(sess.ip, strconv.Itoa(sess.port)), sess.requestTimeout)
	if err != nil {
		sess.logger.Error("Error connecting", "error", err)
		return err
	}
	sess.tx.connMu.Lock()
	sess.tx.connection = tcpConn
	sess.tx.connMu.Unlock()
	// Enable aggressive TCP keepalive to detect dead connections quickly.
	// With Idle=3s, Interval=2s, Count=5: connection declared dead after ~13s of no response.
	// This ensures cable unplugs (>13s) are detected and trigger reconnect,
	// while not affecting slow-changing notification data (keepalive is TCP-level, not app-level).
	configureKeepAlive(tcpConn)
	// Log TCP socket (transport-level only — ADS route validation happens on first ADS command)
	sess.logger.Info("TCP socket established (ADS route not yet verified)",
		"local", sess.tx.connection.LocalAddr().String(),
		"remote", sess.tx.connection.RemoteAddr().String())

	// Auto-derive source AMS NetID from local IP if source NetID is all zeros.
	// Take connMu around the read+write of sess.source so encode() at ams.go
	// (which reads sess.source under connMu) cannot interleave even in the
	// theoretical case of a goroutine surviving across reconnect cycles.
	sess.tx.connMu.Lock()
	if sess.source.NetID == [6]byte{} {
		localAddr, ok := sess.tx.connection.LocalAddr().(*net.TCPAddr)
		if !ok {
			sess.tx.connMu.Unlock()
			return fmt.Errorf("unexpected local address type: %T", sess.tx.connection.LocalAddr())
		}
		ip := localAddr.IP.To4()
		if ip != nil {
			sess.source.NetID = [6]byte{ip[0], ip[1], ip[2], ip[3], 1, 1}
			sess.logger.Info("auto-derived source AMS NetID from local IP",
				"netid", fmt.Sprintf("%d.%d.%d.%d.1.1", ip[0], ip[1], ip[2], ip[3]))
		}

		// NAT/Docker detection: compare TCP and UDP source IPs
		if sess.callbackIP == "" && ip != nil {
			udpConn, udpErr := net.DialTimeout("udp4", net.JoinHostPort(sess.ip, strconv.Itoa(routePort)), 2*time.Second)
			if udpErr == nil {
				udpAddr, ok := udpConn.LocalAddr().(*net.UDPAddr)
				udpConn.Close()
				if ok {
					udpIP := udpAddr.IP.To4()
					if udpIP != nil && !ip.Equal(udpIP) {
						sess.logger.Warn("TCP and UDP source IPs differ — possible NAT/Docker/VPN",
							"tcpIP", ip.String(), "udpIP", udpIP.String(),
							"hint", "set WithHostIP() to the IP the PLC can reach")
					}
				}
			}
		}
	}
	sess.tx.connMu.Unlock()

	// Log container detection — auto-derived NetID works in containers because
	// the PLC stores the UDP source IP (post-NAT) for routes, not the computerName tag.
	if sess.callbackIP == "" && isRunningInContainer() {
		sess.logger.Info("container detected — auto-derived NetID will be used for route registration",
			"netidIP", fmt.Sprintf("%d.%d.%d.%d", sess.source.NetID[0], sess.source.NetID[1], sess.source.NetID[2], sess.source.NetID[3]))
	}

	// Log ADS-level addressing (what matters for AMS routing, may differ from TCP)
	routeHostIP := sess.callbackIP
	if routeHostIP == "" {
		routeHostIP = fmt.Sprintf("%d.%d.%d.%d (from NetID, PLC will use UDP source IP)", sess.source.NetID[0], sess.source.NetID[1], sess.source.NetID[2], sess.source.NetID[3])
	}
	sess.logger.Info("ADS addressing",
		"sourceNetID", fmt.Sprintf("%d.%d.%d.%d.%d.%d", sess.source.NetID[0], sess.source.NetID[1], sess.source.NetID[2], sess.source.NetID[3], sess.source.NetID[4], sess.source.NetID[5]),
		"routeHostIP", routeHostIP,
		"target", fmt.Sprintf("%d.%d.%d.%d.%d.%d:%d", sess.target.NetID[0], sess.target.NetID[1], sess.target.NetID[2], sess.target.NetID[3], sess.target.NetID[4], sess.target.NetID[5], sess.target.Port))

	sess.logger.Log(context.Background(), LevelTrace, "connected")
	// Allocate the underlying Client and start its workers. Session and
	// Client share the *transport pointer (no re-dial); the Client owns
	// the listen / transmit / recvWorker goroutines. handleNotification is
	// installed via callback so cache-aware dispatch fires for inbound
	// DeviceNotification packets, and triggerReconnect is installed as the
	// on-drop hook so transport-down signals enter Session's reconnect FSM.
	sess.client = &Client{
		ip:             sess.ip,
		port:           sess.port,
		target:         sess.target,
		source:         sess.source,
		requestTimeout: sess.requestTimeout,
		logger:         sess.logger,
		tx:             sess.tx,
		ctx:            sess.lifecycle.ctx,
		cancel:         sess.lifecycle.shutdown,
	}
	sess.client.SetNotificationHandler(sess.handleNotification)
	sess.client.SetOnDrop(sess.triggerReconnect)
	sess.client.startWorkers()
	if local {
		resp, err := sess.client.send([]byte{0, 16, 2, 0, 0, 0, 0, 0})
		if err != nil {
			sess.tearDownAndReset(false)
			return fmt.Errorf("local mode handshake failed: %w", err)
		}
		buf := bytes.NewBuffer(resp)
		result := AMSAddress{}
		sess.logger.Log(context.Background(), LevelTrace, "got stuff", "stuff", buf.Bytes())
		err = binary.Read(buf, binary.LittleEndian, &result)
		if err != nil {
			sess.tearDownAndReset(false)
			return fmt.Errorf("local mode binary read failed: %w", err)
		}
		sess.logger.Info("local mode handshake result", "result", result)
		sess.tx.connMu.Lock()
		sess.source = result
		sess.tx.connMu.Unlock()
	}

	// Smart route registration: probe PLC first, register only if route missing.
	// Route registration is UDP-based but needs goroutines running for the probe
	// (which sends an ADS command over TCP). If route is registered, TCP reconnect
	// is needed because PLC may close connections from previously-unknown NetIDs.
	if sess.route.name != "" {
		registered, err := sess.ensureRouteOnConnect()
		if err != nil {
			sess.logger.Warn("route registration failed during connect", "error", err)
		}
		if registered {
			// TCP reconnect — PLC may reset connections from previously-unknown NetIDs.
			// Shut down goroutines, close TCP, redial, restart.
			sess.tearDownAndReset(false)
			if err := sess.dialAndStart(); err != nil {
				return fmt.Errorf("TCP reconnect after route registration failed: %w", err)
			}
			sess.logger.Info("TCP reconnected after route registration")
		}

	}

	// Read symbol version for later change detection (best-effort, don't fail connect)
	version, err := sess.client.GetSymbolVersion()
	if err != nil {
		sess.logger.Debug("could not read symbol version during connect", "error", err)
	} else {
		sess.cache.lock.Lock()
		sess.cache.symbolVersion = version
		sess.cache.lock.Unlock()
	}
	sess.transitionState(SessionStateConnected)
	return nil
}

// ensureRouteOnConnect probes the PLC and registers a route if needed during Connect().
// Returns (registered bool, err error) where registered=true means a route was added
// and the caller should TCP-reconnect.
func (sess *Session) ensureRouteOnConnect() (registered bool, err error) {
	if sess.isClosed() {
		return false, fmt.Errorf("connection closed")
	}

	// Force mode → always register
	if sess.route.forceRouteRegistration {
		sess.logger.Info("registering route (force mode)")
		err = sess.AddRoute(sess.route.name, sess.route.username, string(sess.route.password))
		return err == nil, err
	}

	// Probe: try a lightweight ADS command to see if route exists
	_, probeErr := sess.client.GetSymbolVersion()
	if probeErr == nil {
		sess.logger.Info("route already exists on PLC, skipping registration")
		sess.route.routeProbeFailures.Store(0)
		return false, nil
	}

	if sess.isClosed() {
		return false, fmt.Errorf("connection closed during route probe")
	}

	// Probe failed → register with credentials
	sess.route.routeProbeFailures.Inc()
	sess.logger.Info("route probe failed, registering route", "error", probeErr)
	err = sess.AddRoute(sess.route.name, sess.route.username, string(sess.route.password))
	if err != nil {
		return false, err
	}
	return true, nil
}

// handleStaleDetection runs the configured online-change strategy when a
// PLC return code from the R-CACHE-009 detection set surfaces. It returns
// (true, reason) when the code triggered stale-cache handling and
// (false, "") for unrelated codes (no-op).
//
// The user-supplied callback fires in its own goroutine to honor R-SES-007:
// callers MUST NOT block in the callback.
//
// Strategy dispatch:
//   - SymbolVersionIgnore: surface the error unchanged. Subsequent
//     notification samples are flagged Stale by R-NOT-017.
//   - SymbolVersionClose: terminate the session asynchronously
//     (R-CACHE-011).
//   - SymbolVersionAutoReload: trigger full reload + resub (R-CACHE-010).
func (sess *Session) handleStaleDetection(rc ReturnCode) (stale bool, reason string) {
	stale, reason = detectStaleCache(rc)
	if !stale {
		return false, ""
	}
	sess.logger.Warn("stale-cache detection",
		"code", rc, "reason", reason, "strategy", sess.versionStrategy)
	if sess.versionCallback != nil {
		go sess.versionCallback(reason)
	}
	switch sess.versionStrategy {
	case SymbolVersionIgnore:
		// Mark all active notification handles stale — next sample for each
		// handle will carry Update.Stale=true with this reason. The original
		// error surfaces to the calling op via the existing errors.As
		// intercept.
		sess.markAllHandlesStale(reason)
	case SymbolVersionClose:
		go sess.closeOnStaleDetection(reason)
	case SymbolVersionAutoReload:
		go sess.autoReloadOnStaleDetection(reason)
	}
	return true, reason
}

// markSymbolStale flags the next notification sample for handle h to be
// delivered with Update.Stale=true and Update.Reason=reason. One-shot —
// consumed on first delivery via consumeStaleFlag (R-NOT-017).
func (sess *Session) markSymbolStale(handle uint32, reason string) {
	sess.staleHandlesMu.Lock()
	defer sess.staleHandlesMu.Unlock()
	if sess.staleHandles == nil {
		sess.staleHandles = map[uint32]string{}
	}
	sess.staleHandles[handle] = reason
}

// consumeStaleFlag returns the pending stale reason for handle h and
// clears the entry. Returns ("", false) if no pending flag (R-NOT-017).
func (sess *Session) consumeStaleFlag(handle uint32) (string, bool) {
	sess.staleHandlesMu.Lock()
	defer sess.staleHandlesMu.Unlock()
	r, ok := sess.staleHandles[handle]
	if ok {
		delete(sess.staleHandles, handle)
	}
	return r, ok
}

// markAllHandlesStale flags every active notification handle's next sample
// with reason. Lock order: notifications.lock → staleHandlesMu (acquired
// inside markSymbolStale). Never acquires cache.lock here (R-CACHE-008).
//
// Nil-guard: bare Session{} unit tests may construct without the
// notification manager; production NewSession always sets it.
func (sess *Session) markAllHandlesStale(reason string) {
	if sess.notifications == nil {
		return
	}
	sess.notifications.lock.Lock()
	handles := make([]uint32, 0, len(sess.notifications.activeNotifications))
	for h := range sess.notifications.activeNotifications {
		handles = append(handles, h)
	}
	sess.notifications.lock.Unlock()
	for _, h := range handles {
		sess.markSymbolStale(h, reason)
	}
}

// closeOnStaleDetection terminates the session under SymbolVersionClose
// (R-CACHE-011). Runs in its own goroutine to keep the calling Read/Write
// path non-blocking.
//
// Fires onDisconnect (in its own goroutine, per R-SES-007 non-blocking
// contract) before Close() so observers see the lifecycle event even
// though the termination is locally initiated. Skipped if the session is
// already Closed (idempotent re-entry).
func (sess *Session) closeOnStaleDetection(reason string) {
	sess.logger.Info("Close strategy fired on stale-cache detection", "reason", reason)
	if sess.onDisconnect != nil && !sess.isClosed() {
		go sess.onDisconnect()
	}
	sess.Close()
}

// tryRecordReloadAttempt prunes attempts outside the sliding window then
// records a new attempt and returns true. Returns false when the cap is
// exhausted within the window — caller MUST then degrade to Ignore
// (R-CACHE-013).
func (sess *Session) tryRecordReloadAttempt() bool {
	sess.reloadMu.Lock()
	defer sess.reloadMu.Unlock()
	now := time.Now()
	cutoff := now.Add(-sess.reloadWindow)
	pruned := sess.reloadAttempts[:0]
	for _, t := range sess.reloadAttempts {
		if t.After(cutoff) {
			pruned = append(pruned, t)
		}
	}
	sess.reloadAttempts = pruned
	if len(sess.reloadAttempts) >= sess.maxReloadAttempts {
		return false
	}
	sess.reloadAttempts = append(sess.reloadAttempts, now)
	return true
}

// autoReloadOnStaleDetection runs full re-discovery + resubscribe under
// SymbolVersionAutoReload (R-CACHE-010). Capped by R-CACHE-013 — on cap
// exhaustion, logs WARN, fires callback with ReasonReloadCapExhausted,
// and degrades to Ignore semantics for this call (no further reload
// attempts until window slides out).
//
// Sequence: markAllHandlesStale(ReasonReloadInProgress) → bumpEpoch →
// zero old handles → LoadSymbols → resubscribeNotifications →
// fire onReconnect.
func (sess *Session) autoReloadOnStaleDetection(reason string) {
	if !sess.tryRecordReloadAttempt() {
		sess.logger.Warn("reload cap exhausted - degrading to Ignore",
			"reason", reason, "max", sess.maxReloadAttempts, "window", sess.reloadWindow)
		if sess.versionCallback != nil {
			go sess.versionCallback(ReasonReloadCapExhausted)
		}
		return
	}

	if sess.isClosed() {
		sess.logger.Debug("auto-reload skipped - session closed", "reason", reason)
		return
	}

	// Mark surviving handles Stale during reload window — any sample that
	// sneaks through the old handle pre-resubscribe carries
	// Reason=ReasonReloadInProgress so consumers can distinguish in-flight
	// from post-reload data.
	sess.markAllHandlesStale(ReasonReloadInProgress)

	sess.logger.Info("auto-reload starting", "reason", reason)
	// Bump epoch first so any in-flight retry helpers observing epoch
	// will see the change immediately (R-CACHE-003).
	sess.bumpEpoch()
	// Zero old handles so callers holding *Symbol pointers force
	// on-demand re-resolution (R-CACHE-004).
	sess.cache.lock.Lock()
	zeroOldSymbolHandles(sess.cache.symbols)
	sess.cache.lock.Unlock()

	if err := sess.reloadSymbolsAndResubscribe(); err != nil {
		sess.logger.Error("auto-reload failed", "err", err)
		return
	}
	sess.logger.Info("auto-reload complete")
	if sess.onReconnect != nil {
		go sess.onReconnect()
	}
}

// reloadSymbolsAndResubscribe re-runs symbol discovery + notification
// resub. Re-uses existing reconnect-path helpers (R-NOT-013).
func (sess *Session) reloadSymbolsAndResubscribe() error {
	if err := sess.LoadSymbols(); err != nil {
		return fmt.Errorf("LoadSymbols: %w", err)
	}
	return sess.resubscribeNotifications()
}

// Close closes connection and waits for completion
func (sess *Session) Close() {
	// Capture transport-disconnected state BEFORE the FSM transitions into
	// Closed. The cleanup branch below uses this to decide whether to attempt
	// network ops; once state is Closed, isDisconnected() returns false even
	// if the transport was already gone.
	wasDisconnected := sess.isDisconnected()
	if _, ok := sess.lifecycle.state.transitionToOnce(SessionStateClosed); !ok {
		return // already closed (or transition not permitted from current state)
	}
	close(sess.lifecycle.closedCh)
	sess.logger.Info("Close called, shutting down")

	// Skip handle cleanup if already disconnected — all commands would timeout
	if !wasDisconnected {
		// Delete all active notifications (uses sum command with automatic fallback to individual)
		sess.notifications.lock.Lock()
		handles := make([]uint32, 0, len(sess.notifications.activeNotifications))
		for handle := range sess.notifications.activeNotifications {
			handles = append(handles, handle)
		}
		sess.notifications.lock.Unlock()
		if len(handles) > 0 {
			codes, err := sess.SumDeleteDeviceNotification(handles)
			if err != nil {
				sess.logger.Warn("failed to delete notification handles during close", "error", err)
			} else {
				for i, h := range handles {
					if codes[i] != ReturnCodeNoErrors {
						sess.logger.Warn("failed to delete notification handle", "handle", h, "error", uint32(codes[i]))
					} else {
						sess.logger.Info("removed notification handle", "handle", h)
					}
				}
			}
		}
		// Collect symbol handles under lock, then release without holding the lock
		sess.cache.lock.Lock()
		var symHandles []uint32
		for _, symbol := range sess.cache.symbols {
			if symbol.Handle != 0 {
				symHandles = append(symHandles, symbol.Handle)
			}
		}
		sess.cache.lock.Unlock()

		// Release handles individually — ADS has no batch release command,
		// and Close() is not performance-critical.
		// re-check disconnected each iteration so a mid-loop PLC failure
		// (listen detects EOF → triggerReconnect → disconnected=true) doesn't
		// force every remaining Write to time out. Bail out early.
		for i, h := range symHandles {
			if sess.isDisconnected() {
				sess.logger.Info("close: disconnected during handle release, stopping cleanup",
					"released", i,
					"remaining", len(symHandles)-i)
				break
			}
			handleBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(handleBytes, h)
			if err := sess.client.Write(uint32(GroupSymbolReleaseHandle), 0, handleBytes); err != nil {
				sess.logger.Warn("failed to release symbol handle during close", "error", err, "handle", h)
			} else {
				sess.logger.Info("handle deleted", "handle", h)
			}
		}
	} else {
		sess.logger.Info("already disconnected, skipping handle cleanup")
	}
	sess.lifecycle.ctxMu.RLock()
	sess.lifecycle.shutdown()
	sess.lifecycle.ctxMu.RUnlock()
	// Close the TCP connection to unblock listen() which may be stuck in ReadFull
	sess.tx.connMu.Lock()
	if sess.tx.connection != nil {
		sess.tx.connection.Close()
	}
	sess.tx.connMu.Unlock()
	// Wait for any in-progress reconnect to stop BEFORE waiting on the
	// goroutine waitGroup. Reconnect's retry loop may call waitGroup.Add(2)
	// after we Close — calling Wait first would race with that Add and
	// trigger "sync: WaitGroup misuse". closedCh signals Reconnect
	// to exit its retry loop promptly; reconnectDone is closed when Reconnect
	// returns.
	sess.lifecycle.reconnectMu.Lock()
	ch := sess.lifecycle.reconnectDone
	sess.lifecycle.reconnectMu.Unlock()
	if ch != nil {
		<-ch
	}
	sess.logger.Info("Waiting for workers to close")
	if sess.client != nil {
		sess.client.waitGroup.Wait()
	}
	sess.lifecycle.waitGroup.Wait()
	sess.logger.Info("Close DONE")
}

// ErrDisconnected indicates the underlying TCP connection is not available —
// either Close() has been called or a reconnect has failed. Callers should use
// errors.Is(err, ErrDisconnected) to detect this case.
var ErrDisconnected = errors.New("connection is disconnected")

// reconnectBackoff returns the delay for the given reconnect attempt number (1-indexed)
// based on the configured BackoffConfig tiers.
func (sess *Session) reconnectBackoff(attempt int) time.Duration {
	cfg := sess.lifecycle.backoffConfig
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
func (sess *Session) reconnectSleep(attempt int) error {
	delay := sess.reconnectBackoff(attempt)
	sess.logger.Info("reconnect backoff", "attempt", attempt, "delay", delay)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-sess.lifecycle.closedCh:
		return fmt.Errorf("connection closed during reconnect")
	}
}

// triggerReconnect prepares the connection state for reconnection and launches
// the Reconnect goroutine (if auto-reconnect is enabled). It sets disconnected=true
// and creates the reconnectDone channel BEFORE launching the goroutine, eliminating
// the race window where callers could see a "healthy" connection between the trigger
// and Reconnect() being scheduled.
func (sess *Session) triggerReconnect() {
	if sess.isClosed() {
		return
	}
	// CAS ensures only the first goroutine to detect disconnect fires the callback
	// and sets up reconnection. Subsequent callers (e.g. both listen() and transmitWorker()
	// detecting the same TCP failure) skip the callback to avoid double-firing.
	firstDetector := sess.tx.disconnected.CompareAndSwap(false, true)
	if firstDetector {
		sess.transitionState(SessionStateDisconnected)
	}
	sess.lifecycle.reconnectMu.Lock()
	if sess.lifecycle.reconnectDone == nil {
		sess.lifecycle.reconnectDone = make(chan struct{})
	}
	sess.lifecycle.reconnectMu.Unlock()

	// Fire disconnect callback in goroutine (must not block).
	// Callback must not call Session methods — connection may be closing.
	if firstDetector && sess.onDisconnect != nil && !sess.isClosed() {
		go sess.onDisconnect()
	}

	if sess.lifecycle.autoReconnect {
		go func() { _ = sess.Reconnect() }()
	} else {
		// No auto-reconnect: close reconnectDone immediately so sendRequest
		// waiters unblock with ErrDisconnected instead of hanging forever.
		sess.lifecycle.reconnectMu.Lock()
		if sess.lifecycle.reconnectDone != nil {
			close(sess.lifecycle.reconnectDone)
			sess.lifecycle.reconnectDone = nil
		}
		sess.lifecycle.reconnectMu.Unlock()
	}
}

// Reconnect attempts to re-establish the TCP connection, reload symbols,
// and re-subscribe to previously registered notifications.
// Uses configurable backoff (see WithBackoff) with fast initial retries and
// progressive slowdown. Backoff resets on each successful reconnect.
func (sess *Session) Reconnect() error {
	if sess.isClosed() {
		return fmt.Errorf("connection closed")
	}
	// Prevent concurrent reconnect attempts. transitionToOnce returns
	// ok=false on idempotent re-entry (state already Reconnecting), which
	// is exactly the single-flight gate we want.
	if _, ok := sess.lifecycle.state.transitionToOnce(SessionStateReconnecting); !ok {
		sess.logger.Info("reconnect already in progress or not permitted from current state, skipping")
		return nil
	}

	// Create a channel that waiters (sendRequest) can block on.
	// triggerReconnect() may have already created it — only create if nil.
	sess.lifecycle.reconnectMu.Lock()
	if sess.lifecycle.reconnectDone == nil {
		sess.lifecycle.reconnectDone = make(chan struct{})
	}
	sess.lifecycle.reconnectMu.Unlock()

	defer func() {
		sess.lifecycle.reconnectMu.Lock()
		if sess.lifecycle.reconnectDone != nil {
			close(sess.lifecycle.reconnectDone)
			sess.lifecycle.reconnectDone = nil
		}
		sess.lifecycle.reconnectMu.Unlock()
	}()

	sess.logger.Info("attempting reconnect")
	sess.tx.disconnected.Store(true)
	// State is already Reconnecting (transitionToOnce above).

	// Clear active notifications (old handles invalid after reconnect).
	sess.notifications.lock.Lock()
	sess.notifications.activeNotifications = make(map[uint32]*Symbol)
	sess.notifications.lock.Unlock()

	sess.tearDownAndReset(true)

	var lastErr error
	attempts := 0
	for {
		if sess.isClosed() {
			return fmt.Errorf("connection closed during reconnect")
		}
		attempts++
		if sess.lifecycle.maxReconnectAttempts > 0 && attempts > sess.lifecycle.maxReconnectAttempts {
			return fmt.Errorf("reconnect failed after %d attempts: %w", sess.lifecycle.maxReconnectAttempts, lastErr)
		}

		// Dial TCP, configure keepalive, clear disconnected flag, start goroutines.
		// dialAndStart re-checks closed.Load() before waitGroup.Add(2).
		if err := sess.dialAndStart(); err != nil {
			lastErr = err
			sess.logger.Warn("reconnect dial/start failed, retrying", "error", err, "ip", sess.ip, "port", sess.port, "attempt", attempts)
			if err := sess.reconnectSleep(attempts); err != nil {
				return err
			}
			continue
		}

		// Re-perform local-mode handshake if needed
		if sess.isLocal {
			if err := sess.localHandshake(); err != nil {
				lastErr = err
				sess.logger.Warn("reconnect local handshake failed, retrying", "error", err, "attempt", attempts)
				sess.resetForRetry()
				if err := sess.reconnectSleep(attempts); err != nil {
					return err
				}
				continue
			}
		}

		// Smart route registration: probe first, register only if needed.
		if err := sess.ensureRoute(); err != nil {
			lastErr = err
			sess.logger.Warn("route registration failed during reconnect, retrying", "error", err, "attempt", attempts)
			sess.resetForRetry()
			if err := sess.reconnectSleep(attempts); err != nil {
				return err
			}
			continue
		}

		// Re-load symbols based on discovery mode
		if err := sess.reloadSymbols(); err != nil {
			lastErr = err
			sess.logger.Warn("reconnect symbol reload failed, retrying", "error", err, "attempt", attempts)
			sess.resetForRetry()
			if err := sess.reconnectSleep(attempts); err != nil {
				return err
			}
			continue
		}

		// Re-subscribe notifications using stored configs.
		if err := sess.resubscribeNotifications(); err != nil {
			lastErr = err
			sess.logger.Warn("reconnect notification re-subscribe failed, retrying", "error", err, "attempt", attempts)
			sess.resetForRetry()
			if err := sess.reconnectSleep(attempts); err != nil {
				return err
			}
			continue
		}

		sess.tx.disconnected.Store(false)
		sess.lifecycle.strictReconnectFailures = 0 // reset on success
		// epoch bumps inside the transition helper when target == Connected.
		sess.transitionState(SessionStateConnected)
		sess.logger.Info("reconnect successful", "attempts", attempts)

		// Fire reconnect callback in goroutine (must not block).
		// Callback must not call Session methods — connection may be closing.
		if sess.onReconnect != nil && !sess.isClosed() {
			go sess.onReconnect()
		}
		return nil
	}
}

// ensureRoute checks if the route exists (via probe) and registers if needed.
// On force mode or after repeated probe failures, skips the probe.
// Returns a non-nil error only if registration was attempted and failed critically
// (requiring a TCP reset / retry).
func (sess *Session) ensureRoute() error {
	if sess.route.name == "" {
		return nil
	}
	if sess.isClosed() {
		return fmt.Errorf("connection closed")
	}

	// Force mode or too many probe failures → always register
	probeFailures := sess.route.routeProbeFailures.Load()
	if sess.route.forceRouteRegistration || probeFailures >= 3 {
		sess.logger.Info("registering route (forced/fallback)", "probeFailures", probeFailures)
		if err := sess.AddRoute(sess.route.name, sess.route.username, string(sess.route.password)); err != nil {
			return fmt.Errorf("route registration failed: %w", err)
		}

		sess.route.routeProbeFailures.Store(0)
		return nil
	}

	// Probe: try a lightweight ADS command to see if route already exists
	_, probeErr := sess.client.GetSymbolVersion()
	if probeErr == nil {
		sess.logger.Debug("route still valid, skipping re-registration")
		sess.route.routeProbeFailures.Store(0)
		return nil
	}

	if sess.isClosed() {
		return fmt.Errorf("connection closed during route probe")
	}

	// Probe failed → register with credentials
	failuresAfter := sess.route.routeProbeFailures.Inc()
	sess.logger.Info("route probe failed, registering route", "error", probeErr, "probeFailures", failuresAfter)
	if err := sess.AddRoute(sess.route.name, sess.route.username, string(sess.route.password)); err != nil {
		return fmt.Errorf("route registration failed after probe: %w", err)
	}

	return nil
}

// filterValidNotificationConfigs returns only configs whose symbols still exist
// in the current symbol table. Logs a warning for dropped subscriptions.
func (sess *Session) filterValidNotificationConfigs(configs []NotificationConfig) []NotificationConfig {
	sess.cache.lock.Lock()
	defer sess.cache.lock.Unlock()

	valid := make([]NotificationConfig, 0, len(configs))
	for _, cfg := range configs {
		if _, exists := sess.cache.symbols[symbolKey(cfg.SymbolName)]; exists {
			valid = append(valid, cfg)
		} else if _, onDemand := sess.cache.onDemandSymbols[symbolKey(cfg.SymbolName)]; onDemand {
			valid = append(valid, cfg)
		} else {
			sess.logger.Warn("notification symbol gone after reconnect, dropping subscription",
				"symbol", cfg.SymbolName)
		}
	}
	return valid
}

// reloadSymbols re-establishes the symbol table after a reconnect, matching
// the discovery mode that was used before the connection dropped.
func (sess *Session) reloadSymbols() error {
	sess.cache.lock.Lock()
	fullyLoaded := sess.cache.symbolsFullyLoaded
	listLoaded := sess.cache.symbolListLoaded
	dtLoaded := sess.cache.datatypesLoaded
	hasOnDemand := len(sess.cache.onDemandSymbols) > 0
	sess.cache.lock.Unlock()

	switch {
	case fullyLoaded:
		// Full discovery was done — redo it
		return sess.loadSymbols()

	case listLoaded || dtLoaded:
		// Partial discovery — re-download what was loaded
		if listLoaded {
			if err := sess.LoadSymbolList(SlowDiscoveryConfig{}); err != nil {
				return fmt.Errorf("reload symbol list: %w", err)
			}
		}
		if dtLoaded {
			if err := sess.LoadDataTypes(SlowDiscoveryConfig{}); err != nil {
				return fmt.Errorf("reload datatypes: %w", err)
			}
		}

	case hasOnDemand:
		// On-demand mode: re-resolve only the symbols that were previously loaded.
		// By default, missing symbols are skipped gracefully (PLC may have done
		// an online change). With WithStrictReconnect, missing symbols cause failure.
		sess.cache.lock.Lock()
		oldSymbols := sess.cache.onDemandSymbols
		sess.cache.symbols = make(map[string]*Symbol)
		sess.cache.onDemandSymbols = make(map[string]bool)
		sess.bumpEpoch()
		sess.cache.lock.Unlock()

		for name := range oldSymbols {
			if _, err := sess.getSymbol(name); err != nil {
				if sess.lifecycle.strictReconnect {
					sess.lifecycle.strictReconnectFailures++
					if sess.lifecycle.strictReconnectMaxAttempts == 0 || sess.lifecycle.strictReconnectFailures > sess.lifecycle.strictReconnectMaxAttempts {
						return fmt.Errorf("re-resolve symbol %q (strict mode, %d failures): %w", name, sess.lifecycle.strictReconnectFailures, err)
					}
					return fmt.Errorf("re-resolve symbol %q (strict mode, attempt %d/%d): %w", name, sess.lifecycle.strictReconnectFailures, sess.lifecycle.strictReconnectMaxAttempts, err)
				}
				sess.logger.Warn("on-demand symbol unavailable after reconnect, skipping",
					"symbol", name, "error", err)
			}
		}

	default:
		// No symbols were loaded — read symbol version for future use
		version, err := sess.client.GetSymbolVersion()
		if err != nil {
			sess.logger.Debug("could not read symbol version during reconnect", "error", err)
		} else {
			sess.cache.lock.Lock()
			sess.cache.symbolVersion = version
			sess.cache.lock.Unlock()
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
func (sess *Session) tearDownAndReset(resetFeatureFlags bool) {
	sess.lifecycle.ctxMu.RLock()
	sess.lifecycle.shutdown()
	sess.lifecycle.ctxMu.RUnlock()
	sess.tx.connMu.Lock()
	if sess.tx.connection != nil {
		sess.tx.connection.Close()
	}
	sess.tx.connMu.Unlock()
	// Wait for the previous batch of Client workers (listen, transmit,
	// recvWorker) to exit. They share ctx with lifecycle.ctx; the cancel
	// above plus the closed TCP socket trigger their exit.
	if sess.client != nil {
		sess.client.waitGroup.Wait()
	}
	sess.lifecycle.waitGroup.Wait()
	sess.lifecycle.ctxMu.Lock()
	sess.lifecycle.ctx, sess.lifecycle.shutdown = context.WithCancel(context.Background()) //nolint:gosec // cancel stored in lifecycle.shutdown, called from Close
	sess.lifecycle.ctxMu.Unlock()
	sess.tx.chanMu.Lock()
	sess.tx.sendChannel = make(chan []byte)
	sess.tx.systemResponse = make(chan []byte, 1)
	sess.tx.recvQueue = make(chan []byte, recvQueueSize)
	sess.tx.chanMu.Unlock()
	sess.tx.activeRequestLock.Lock()
	sess.tx.activeRequests = map[uint32]chan []byte{}
	sess.tx.activeRequestLock.Unlock()
	// Capability state lives on Client. A fresh Client (allocated in
	// dialAndStart on each reconnect attempt) has zero-value capabilities,
	// equivalent to a reset. resetFeatureFlags is therefore implicit and
	// the parameter is retained only for symmetric API with prior callers.
	_ = resetFeatureFlags
}

// dialAndStart performs net.DialTimeout, configures keepalive, clears the
// disconnected flag, and starts the listen/transmit goroutines. Used by both
// Connect()'s post-route-registration redial path and Reconnect()'s retry loop.
// Re-checks closed before waitGroup.Add(2) to prevent the sync.WaitGroup
// misuse race.
func (sess *Session) dialAndStart() error {
	newConn, err := net.DialTimeout("tcp", net.JoinHostPort(sess.ip, strconv.Itoa(sess.port)), sess.requestTimeout)
	if err != nil {
		return err
	}
	sess.tx.connMu.Lock()
	sess.tx.connection = newConn
	sess.tx.connMu.Unlock()
	configureKeepAlive(newConn)
	sess.tx.disconnected.Store(false)
	if sess.isClosed() {
		// Session was Closed mid-dial. Don't Add to waitGroup.
		sess.tx.connMu.Lock()
		newConn.Close()
		sess.tx.connection = nil
		sess.tx.connMu.Unlock()
		return fmt.Errorf("connection closed during dial")
	}
	// Allocate a fresh Client (or rewire the existing one's transport
	// references — fields that change after a redial: ctx, source, tx
	// pointer is the same).
	sess.lifecycle.ctxMu.RLock()
	freshCtx := sess.lifecycle.ctx
	sess.lifecycle.ctxMu.RUnlock()
	sess.client = &Client{
		ip:             sess.ip,
		port:           sess.port,
		target:         sess.target,
		source:         sess.source,
		requestTimeout: sess.requestTimeout,
		logger:         sess.logger,
		tx:             sess.tx,
		ctx:            freshCtx,
		cancel:         sess.lifecycle.shutdown,
	}
	sess.client.SetNotificationHandler(sess.handleNotification)
	sess.client.SetOnDrop(sess.triggerReconnect)
	sess.client.startWorkers()
	return nil
}

// localHandshake performs the local-mode AMSAddress probe used after dial when
// isLocal is true. Updates sess.source on success.
func (sess *Session) localHandshake() error {
	resp, err := sess.client.send([]byte{0, 16, 2, 0, 0, 0, 0, 0})
	if err != nil {
		return fmt.Errorf("local handshake send: %w", err)
	}
	buf := bytes.NewBuffer(resp)
	result := AMSAddress{}
	if err := binary.Read(buf, binary.LittleEndian, &result); err != nil {
		return fmt.Errorf("local handshake parse: %w", err)
	}
	sess.tx.connMu.Lock()
	sess.source = result
	sess.tx.connMu.Unlock()
	return nil
}

// resubscribeNotifications restores notification subscriptions stored in
// notificationConfigs after a successful reconnect. Filters out symbols that
// no longer exist after symbol reload. On error, rolls back partial PLC-side
// successes and restores the saved configs so they can be retried by
// the next reconnect attempt.
func (sess *Session) resubscribeNotifications() error {
	sess.notifications.lock.Lock()
	savedConfigs := sess.notifications.notificationConfigs
	savedChannel := sess.notifications.notificationChannel
	sess.notifications.notificationConfigs = nil // Clear before re-adding to prevent duplicates
	sess.notifications.lock.Unlock()
	if len(savedConfigs) == 0 || savedChannel == nil {
		return nil
	}
	validConfigs := sess.filterValidNotificationConfigs(savedConfigs)
	if len(validConfigs) == 0 {
		// All symbols gone (e.g., PLC online change removed all subscribed vars).
		// Clear channel reference so a future AddSymbolNotification can use a new channel.
		sess.notifications.lock.Lock()
		sess.notifications.notificationChannel = nil
		sess.notifications.lock.Unlock()
		return nil
	}
	// Snapshot active handles before the re-subscribe attempt. If
	// AddSymbolNotifications partially succeeds and then errors, we use the
	// snapshot diff to roll back the PLC-side registrations created during
	// this attempt. Without rollback, repeated reconnect retries
	// accumulate orphaned PLC notifications until the next TCP disconnect.
	sess.notifications.lock.Lock()
	preHandles := make(map[uint32]struct{}, len(sess.notifications.activeNotifications))
	for h := range sess.notifications.activeNotifications {
		preHandles[h] = struct{}{}
	}
	sess.notifications.lock.Unlock()

	subResults, err := sess.AddSymbolNotifications(validConfigs, savedChannel)

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
		deleted := sess.bestEffortDeleteNotifications(orphanHandles)
		sess.logger.Warn("resubscribe: released PLC handles for Skipped+Handle entries",
			"orphan_handles", len(orphanHandles),
			"deleted", deleted)
	}
	if len(retryConfigs) > 0 {
		sess.notifications.lock.Lock()
		sess.notifications.notificationConfigs = append(sess.notifications.notificationConfigs, retryConfigs...)
		sess.notifications.lock.Unlock()
		sess.logger.Info("resubscribe: queued Skipped configs for next reconnect retry",
			"retry_count", len(retryConfigs))
	}
	if len(droppedConfigs) > 0 {
		sess.logger.Warn("resubscribe: dropping configs after max retries",
			"dropped", droppedConfigs,
			"max_attempts", resubscribeMaxAttempts)
	}

	if err != nil {
		// Identify handles created during THIS attempt and best-effort delete.
		sess.notifications.lock.Lock()
		var newHandles []uint32
		for h := range sess.notifications.activeNotifications {
			if _, existed := preHandles[h]; !existed {
				newHandles = append(newHandles, h)
				// Drop client-side bookkeeping for the rollback handles.
				delete(sess.notifications.activeNotifications, h)
			}
		}
		// Restore configs so they can be retried by the next reconnect attempt.
		sess.notifications.notificationConfigs = savedConfigs
		sess.notifications.notificationChannel = savedChannel
		sess.notifications.lock.Unlock()
		if len(newHandles) > 0 {
			deleted := sess.bestEffortDeleteNotifications(newHandles)
			sess.logger.Warn("resubscribe rollback: deleted partial-success handles",
				"new_handles", len(newHandles),
				"deleted", deleted)
		}
		return err
	}
	return nil
}

// resetForRetry tears down goroutines, closes the TCP connection, and resets
// channels/state so the next retry iteration starts clean.
func (sess *Session) resetForRetry() {
	sess.tx.disconnected.Store(true)
	sess.tearDownAndReset(false)
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
func (sess *Session) loadSymbols() error {
	// Read and store symbol version
	version, err := sess.client.GetSymbolVersion()
	if err != nil {
		sess.logger.Warn("failed to read symbol version, continuing with symbol load", "error", err)
	} else {
		sess.cache.lock.Lock()
		sess.cache.symbolVersion = version
		sess.cache.lock.Unlock()
	}

	res, err := sess.client.GetSymbolUploadInfo()
	if err != nil {
		return fmt.Errorf("failed to get symbol upload info: %w", err)
	}
	datatypesResponse, err := sess.client.DownloadDataTypes(res.DataTypeLength)
	if err != nil {
		return fmt.Errorf("failed to upload datatypes: %w", err)
	}
	datatypes, err := parseUploadSymbolInfoDataTypes(datatypesResponse)
	if err != nil {
		return fmt.Errorf("failed to parse datatypes: %w", err)
	}
	symbolsResponse, err := sess.client.DownloadSymbolList(res.SymbolLength)
	if err != nil {
		return fmt.Errorf("failed to upload symbols: %w", err)
	}
	symbols, err := parseUploadSymbolInfoSymbols(symbolsResponse, datatypes)
	if err != nil {
		return fmt.Errorf("failed to parse symbols: %w", err)
	}
	sess.cache.lock.Lock()
	// invalidate Handle on every old *Symbol before swap so external
	// callers holding old pointers (e.g. infos[i].symbol in
	// readMultipleSymbolsRetry) fail fast on next use and re-resolve via
	// GetSymbol instead of using a stale handle that the PLC may have
	// reassigned to a different symbol after reconnect.
	zeroOldSymbolHandles(sess.cache.symbols)
	sess.cache.datatypes = datatypes
	sess.cache.symbols = symbols
	sess.bumpEpoch()
	sess.cache.lock.Unlock()
	return nil
}

// AddRoute registers a route on the remote PLC using this connection's settings.
// It uses callbackIP (from WithHostIP) if set, otherwise derives the callback
// address from the source AMS NetID (first 4 bytes = IP).
func (sess *Session) AddRoute(routeName, username, password string) error {
	hostIP := sess.callbackIP
	if hostIP == "" {
		hostIP = fmt.Sprintf("%d.%d.%d.%d",
			sess.source.NetID[0], sess.source.NetID[1],
			sess.source.NetID[2], sess.source.NetID[3])
	}
	return AddRemoteRouteWithLogger(sess.logger, sess.ip, sess.source.NetID, routeName, hostIP, username, password)
}

// IsDisconnected returns whether the connection is currently in a disconnected state.
func (sess *Session) IsDisconnected() bool {
	return sess.isDisconnected()
}

// IsClosed reports whether the session has reached the terminal Closed
// state. A closed session cannot be reused; construct a new one via
// NewSession. Distinct from IsDisconnected (transient transport loss).
func (sess *Session) IsClosed() bool {
	return sess.isClosed()
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

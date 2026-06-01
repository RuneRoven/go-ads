package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// randomAMSPort returns a random AMS source port in the IANA dynamic/private
// range (32768-49151). Sessions get a unique port at construction so multiple
// processes from the same host (same source NetID) appear as distinct clients
// to the PLC and so a process restart doesn't reuse a prior process's port.
// The PLC's notification handle table is keyed by {source NetID, source AMS
// port, handle}, so a new port = new identity = old subscriptions auto-age
// via route-idle-timeout rather than competing with the new connection.
//
// WithLocalAMS(AMSAddress{Port: N}) overrides for deployments that need a
// stable port (e.g. firewalled environments with port allow-lists).
func randomAMSPort() uint16 {
	const minPort, span = 32768, 49151 - 32768 + 1
	return uint16(minPort + rand.IntN(span)) //nolint:gosec // non-cryptographic port selection
}

// dialTCP opens the outbound TCP connection to sess.ip:sess.port honoring
// sess.localBindIP if set. Used by Connect() and Reconnect() so both paths
// share the same source-IP binding semantics. Default behavior (empty
// localBindIP) lets the OS pick source IP via routing table — usual case.
// Setting localBindIP supports multi-Session deployments on hosts with
// IP aliases, where each Session pins to a distinct local IP so the PLC
// sees them as separate hosts (one TCP slot per source IP — see Beckhoff
// ADS #49 / #72).
func (sess *Session) dialTCP() (net.Conn, error) {
	dialer := net.Dialer{Timeout: sess.requestTimeout}
	if sess.localBindIP != nil {
		dialer.LocalAddr = &net.TCPAddr{IP: sess.localBindIP}
	}
	return dialer.Dial("tcp", net.JoinHostPort(sess.ip, strconv.Itoa(sess.port)))
}

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

	closedCh   chan struct{}
	closedOnce sync.Once // guards close(closedCh) so Close() and Reconnect-exhaustion can both fire safely

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

	// Flap detection: a successful Connect/Reconnect that drops again within
	// flapWindow counts as a flap. flapCount increments per flap and feeds
	// reconnectBackoff() so the existing BackoffConfig tiers also govern
	// cross-cycle behaviour, not just within-one-Reconnect retries. Without
	// this, a PLC that RSTs every connection produces "successful attempts=1"
	// every few ms because each new Reconnect() starts with fresh local
	// attempts=0 — burning ephemeral ports and hammering the PLC. flapCount
	// resets after the connection stays up for flapResetWindow.
	flapMu          sync.Mutex
	lastConnectedAt time.Time
	flapCount       int
}

const (
	flapWindow      = 5 * time.Second
	flapResetWindow = 60 * time.Second
)

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
	// Reconnect; shut down by Close. atomic.Pointer so concurrent reads on
	// user RPC paths cannot race the publish in Connect / dialAndStart.
	client atomic.Pointer[Client]

	// TCP socket + request multiplexing + listen/transmit channels.
	tx *transport

	target       AMSAddress
	source       AMSAddress
	callbackIP   string // IP PLC uses to reach us (for Docker/VPN; set via WithHostIP)
	localBindIP  net.IP // Force outbound TCP source IP (multi-session per host; set via WithLocalBindIP). nil = OS default routing.

	// symbol cache + data-type table + discovery-mode flags.
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
	versionCallback   func(reason Reason)
	maxReloadAttempts int
	reloadWindow      time.Duration
	reloadAttempts    []time.Time
	reloadMu          sync.Mutex
	reloadInProgress  atomic.Bool
	staleHandles      map[uint32]Reason
	staleHandlesMu    sync.Mutex

	// Route registration config (populated by WithRoute / WithForceRouteRegistration).
	route *routeManager

	logger *slog.Logger
}

// AMSEndpoint identifies a remote ADS endpoint at the TCP and AMS layers.
// IP+Port locate the TwinCAT runtime over TCP (port 48898 by default);
// AMS is the target AMSAddress carried in every ADS request header.
type AMSEndpoint struct {
	IP   string
	Port int
	AMS  AMSAddress
}

// NewSession creates a new ADS session targeting remote. The session does
// no I/O until Connect; ctx is captured for the long-lived session
// lifecycle and cancelling it shuts the session down.
//
// Local AMS NetID defaults to auto-derivation from the local TCP source IP.
// Local AMS port defaults to a random value in the IANA dynamic range
// (32768-49151) so each Session instance — including each process restart —
// appears as a distinct AMS client to the PLC, preventing notification-handle
// table collisions between concurrent or successive sessions sharing a host.
// Override either with WithLocalAMS(AMSAddress{NetID:..., Port:...}).
//
// Use sess.Close() to shut down the session.
func NewSession(ctx context.Context, remote AMSEndpoint, opts ...SessionOption) (sess *Session, err error) {
	if remote.IP == "" {
		return nil, fmt.Errorf("ads: NewSession: remote.IP must be set")
	}
	if remote.Port <= 0 {
		remote.Port = 48898 // TwinCAT TCP default
	}
	if remote.AMS.NetID == [6]byte{} {
		return nil, fmt.Errorf("ads: NewSession: remote.AMS.NetID must be set")
	}
	if remote.AMS.Port == 0 {
		return nil, fmt.Errorf("ads: NewSession: remote.AMS.Port must be set (e.g. PortR0PlcTc3 = 851)")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	sessCtx, cancel := context.WithCancel(ctx)
	sess = &Session{
		ip:             remote.IP,
		port:           remote.Port,
		target:         remote.AMS,
		requestTimeout: 5 * time.Second,
		route:          &routeManager{},
		notifications: &notificationManager{
			activeNotifications: make(map[uint32]activeNotification),
			configsByKey:        make(map[string]struct{}),
			orphanSeen:          make(map[uint32]time.Time),
			orphanSem:           make(chan struct{}, orphanDeleteMaxConcurrency),
		},
		cache: &symbolCache{
			symbols:         map[string]*symbol{},
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
			ctx:                  sessCtx,
			shutdown:             cancel,
		},
		logger: slog.Default(),
	}
	// idempotent: zero state is Constructed already
	sess.lifecycle.state.transitionTo(SessionStateConstructed)
	// Online-change defaults (R-SES-011, R-CACHE-013). Applied before opts so
	// callers can override.
	sess.maxReloadAttempts = 3
	sess.reloadWindow = 60 * time.Second
	// Default local AMS port: random in IANA dynamic range so each process /
	// each session presents a distinct AMS source identity to the PLC. See
	// randomAMSPort doc for the rationale. WithLocalAMS overrides for stable-
	// port deployments (firewalled environments, container port allow-lists).
	sess.source.Port = randomAMSPort()
	for _, opt := range opts {
		opt(sess)
	}
	return sess, nil
}

// Connect dials the PLC and transitions the session to Connected. Local-mode
// (in-process TwinCAT runtime at 127.0.0.1) is selected via WithLocalMode at
// NewSession time.
//
// Not safe for concurrent invocation on the same Session — the FSM gate via
// transitionToOnce(Connecting) serializes callers, but races on sess.tx /
// sess.client publishing would still leak resources. Call once per Session.
func (sess *Session) Connect(ctx context.Context) (retErr error) {
	local := sess.isLocal
	// transitionToOnce returns ok=false if another goroutine already won
	// the Constructed→Connecting transition; reject concurrent calls rather
	// than letting both race on socket + client publish.
	if _, ok := sess.lifecycle.state.transitionToOnce(SessionStateConnecting); !ok {
		return fmt.Errorf("ads: Connect already in progress or session past Constructed state")
	}
	// Roll back Connecting→Disconnected on any error return so the caller
	// can retry Connect via the Disconnected→Connecting edge. Without this,
	// the FSM is stranded in Connecting and Reconnect (auto-path only) is
	// the sole recovery, forcing callers to construct a new Session.
	defer func() {
		if retErr != nil && sess.lifecycle.state.load() == SessionStateConnecting {
			sess.lifecycle.state.transitionTo(SessionStateDisconnected)
		}
	}()
	var err error
	sess.logger.Debug("dialing", "ip", sess.ip, "port", sess.port)
	if local {
		sess.target.NetID = [6]byte{127, 0, 0, 1, 1, 1}
		sess.ip = "127.0.0.1"
	}
	tcpConn, err := sess.dialTCP()
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
			// If callbackIP is set (WithHostIP), use it for source NetID so that AMS
			// packet headers match the route the PLC registered. On multi-homed machines
			// the TCP source IP can differ from the IP used for route registration.
			if sess.callbackIP != "" {
				if cbIP := net.ParseIP(sess.callbackIP).To4(); cbIP != nil {
					ip = cbIP
				}
			}
			sess.source.NetID = [6]byte{ip[0], ip[1], ip[2], ip[3], 1, 1}
			sess.logger.Info("auto-derived source AMS NetID from local IP",
				"netid", sess.source.NetIDString())
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
		"sourceNetID", sess.source.NetIDString(),
		"routeHostIP", routeHostIP,
		"target", sess.target.String())

	sess.logger.Log(context.Background(), LevelTrace, "connected")
	// Allocate the underlying Client and start its workers. Session and
	// Client share the *transport pointer (no re-dial); the Client owns
	// the listen / transmit / recvWorker goroutines. handleNotification is
	// installed via callback so cache-aware dispatch fires for inbound
	// DeviceNotification packets, and triggerReconnect is installed as the
	// on-drop hook so transport-down signals enter Session's reconnect FSM.
	newClient := &Client{
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
	newClient.SetNotificationHandler(sess.handleNotification)
	newClient.SetOnDrop(sess.triggerReconnect)
	newClient.startWorkers()
	// Publish only after handlers + workers are wired so concurrent readers
	// never observe a half-initialized Client.
	sess.client.Store(newClient)
	if local {
		resp, err := newClient.send([]byte{0, 16, 2, 0, 0, 0, 0, 0})
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
	// shouldSkip() covers both "no WithRoute set" and explicit WithSkipRouteRegistration.
	if !sess.route.shouldSkip() {
		registered, err := sess.ensureRouteOnConnect(ctx)
		if err != nil {
			// WithRoute is an explicit caller requirement — silently swallowing
			// the error and continuing leads to every subsequent ADS command
			// failing with ReturnCodeGlobalTargetNotFound, leaving the caller
			// to debug a connection that "succeeded" but doesn't work.
			return fmt.Errorf("route registration failed during connect: %w", err)
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
	version, err := sess.client.Load().GetSymbolVersion(ctx)
	if err != nil {
		sess.logger.Debug("could not read symbol version during connect", "error", err)
	} else {
		sess.cache.lock.Lock()
		sess.cache.symbolVersion = version
		sess.cache.lock.Unlock()
	}
	sess.transitionState(SessionStateConnected)
	sess.lifecycle.flapMu.Lock()
	sess.lifecycle.lastConnectedAt = time.Now()
	sess.lifecycle.flapMu.Unlock()
	return nil
}

// routeProbeRetryDelay is how long we wait between the first probe attempt
// and the redial-retry. Picked to give a TwinCAT PLC time to release a TCP
// slot held by a recently-closed connection from the same source IP
// (~500ms is empirically enough on TC3 4024.x; tunable if needed).
const routeProbeRetryDelay = 500 * time.Millisecond

// isProbeRetryable reports whether a route-probe error is a transport-level
// flap worth a TCP redial + probe retry before falling back to AddRoute.
//
// PLC RST mid-probe can mean either "route is actually missing" (PLC's
// AmsRouter rejects unknown source NetID) OR "PLC slot is still bound to
// a previous TCP from same source IP" (transient — clears once the stale
// slot times out). Both surface identically as ErrTransportClosed /
// wrapped io.EOF / ECONNRESET. Without retry, the transient case spuriously
// fires AddRoute, which on TC3 also evicts any concurrent sibling TCP from
// the same source IP (see README §Limitations, Beckhoff/ADS #49). One
// retry distinguishes:
//
//   - retry succeeds → route exists, slot conflict was transient → skip AddRoute
//   - retry also fails → real missing route → register
//
// context.DeadlineExceeded is INTENTIONALLY excluded: that signals the
// caller's Connect ctx expired, not a transient PLC slot conflict. Retrying
// would burn the caller's already-expired deadline through a 500ms sleep,
// a redial, and a doomed second probe before surfacing the original error.
//
// Returns false for ADS-level errors (e.g., ReturnCodeRouterNotInitialized,
// concrete protocol-level rejection), since those indicate something more
// specific than transport flap and re-dial won't change the outcome.
func isProbeRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrTransportClosed) || errors.Is(err, io.EOF) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	return false
}

// ensureRouteOnConnect probes the PLC and registers a route if needed during Connect().
// Returns (registered bool, err error) where registered=true means a route was added
// and the caller should TCP-reconnect.
//
// On transport-level probe failure (RST/EOF/timeout), redials the TCP
// connection and retries the probe once before falling back to AddRoute.
// This avoids spurious route registration when the PLC RST'd due to a
// transient slot conflict rather than a missing route.
func (sess *Session) ensureRouteOnConnect(ctx context.Context) (registered bool, err error) {
	if sess.isClosed() {
		return false, fmt.Errorf("connection closed")
	}

	// Force mode → always register
	if sess.route.forceRouteRegistration {
		sess.logger.Info("registering route (force mode)")
		err = sess.AddRoute(ctx, sess.route.name, sess.route.username, string(sess.route.password))
		return err == nil, err
	}

	// First probe attempt
	probeErr := sess.probeRoute(ctx)
	if probeErr == nil {
		sess.logger.Info("route already exists on PLC, skipping registration")
		sess.route.routeProbeFailures.Store(0)
		return false, nil
	}

	if sess.isClosed() {
		return false, fmt.Errorf("connection closed during route probe")
	}

	// Transport-level failure may be transient (PLC slot conflict from
	// previous TCP not yet released). Redial + retry probe once before
	// concluding the route is missing.
	if isProbeRetryable(probeErr) {
		sess.logger.Info("route probe failed at transport layer, retrying once before AddRoute",
			"error", probeErr, "delay", routeProbeRetryDelay)
		// Disarm ondrop on the old Client before tearing down its TCP.
		// Otherwise listen's RST handler races to fire sess.triggerReconnect
		// (the registered ondrop) and spawns a Reconnect goroutine that
		// then competes with this retry path on sess.client / tx state.
		// dialAndStart re-wires ondrop on the new Client before startWorkers.
		if oldClient := sess.client.Load(); oldClient != nil {
			oldClient.SetOnDrop(nil)
		}
		// ctx-aware sleep: honor caller cancellation. Plain time.Sleep
		// would block the full delay even if the caller has given up.
		select {
		case <-time.After(routeProbeRetryDelay):
		case <-ctx.Done():
			return false, fmt.Errorf("route probe retry aborted: %w", ctx.Err())
		}
		sess.tearDownAndReset(false)
		if dialErr := sess.dialAndStart(); dialErr != nil {
			return false, fmt.Errorf("redial during route probe retry: %w", dialErr)
		}
		if sess.isClosed() {
			return false, fmt.Errorf("connection closed during route probe retry")
		}
		retryErr := sess.probeRoute(ctx)
		if retryErr == nil {
			sess.logger.Info("route already exists on PLC (confirmed after retry)")
			sess.route.routeProbeFailures.Store(0)
			return false, nil
		}
		probeErr = fmt.Errorf("probe failed after retry: %w", retryErr)
	}

	// Definite probe failure → register
	sess.route.routeProbeFailures.Add(1)
	sess.logger.Info("route probe failed, registering route", "error", probeErr)
	err = sess.AddRoute(ctx, sess.route.name, sess.route.username, string(sess.route.password))
	if err != nil {
		return false, err
	}
	return true, nil
}

// probeRoute sends a lightweight ADS command (GetSymbolVersion) to verify
// the PLC accepts our source NetID. Returns nil if the round-trip succeeds
// (route is valid) or the underlying error otherwise.
func (sess *Session) probeRoute(ctx context.Context) error {
	_, err := sess.client.Load().GetSymbolVersion(ctx)
	return err
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
func (sess *Session) handleStaleDetection(rc ReturnCode) (stale bool, reason Reason) {
	stale, reason = detectStaleCache(rc)
	if !stale {
		return false, ""
	}
	sess.logger.Warn("stale-cache detection",
		"code", rc, "reason", reason, "strategy", sess.versionStrategy)
	switch sess.versionStrategy {
	case SymbolVersionIgnore:
		// Mark all active notification handles stale — next sample for each
		// handle will carry Update.Stale=true with this reason. The original
		// error surfaces to the calling op via the existing errors.As
		// intercept.
		sess.markAllHandlesStale(reason)
		if sess.versionCallback != nil {
			go sess.versionCallback(reason)
		}
	case SymbolVersionClose:
		if sess.versionCallback != nil {
			go sess.versionCallback(reason)
		}
		go sess.closeOnStaleDetection(reason)
	case SymbolVersionAutoReload:
		// CAS gates both the reload goroutine AND the callback so N
		// concurrent triggers fire one callback total (R-SES-011
		// "once per detection"). Without this, the callback was launched
		// unconditionally above and N triggers fired N callbacks.
		if sess.reloadInProgress.CompareAndSwap(false, true) {
			if sess.versionCallback != nil {
				go sess.versionCallback(reason)
			}
			go sess.autoReloadOnStaleDetection(reason)
		}
	}
	return true, reason
}

// markSymbolStale flags the next notification sample for handle h to be
// delivered with Update.Stale=true and Update.Reason=reason. One-shot —
// consumed on first delivery via consumeStaleFlag (R-NOT-017).
func (sess *Session) markSymbolStale(handle uint32, reason Reason) {
	sess.staleHandlesMu.Lock()
	defer sess.staleHandlesMu.Unlock()
	if sess.staleHandles == nil {
		sess.staleHandles = map[uint32]Reason{}
	}
	sess.staleHandles[handle] = reason
}

// consumeStaleFlag returns the pending stale reason for handle h and
// clears the entry. Returns ("", false) if no pending flag (R-NOT-017).
func (sess *Session) consumeStaleFlag(handle uint32) (Reason, bool) {
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
func (sess *Session) markAllHandlesStale(reason Reason) {
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
func (sess *Session) closeOnStaleDetection(reason Reason) {
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
func (sess *Session) autoReloadOnStaleDetection(reason Reason) {
	defer sess.reloadInProgress.Store(false)
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
	// Zero old handles so callers holding *symbol pointers force
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
// resub after an online-change detection. Re-uses existing reconnect-path
// helpers (R-NOT-013).
//
// Before resubscribing, explicitly Delete the old PLC notification handles.
// The PLC's per-source-NetID handle table indexes subscriptions by handle
// ID — without this cleanup, AddSymbolNotifications below would allocate a
// fresh handle for every symbol while the old handles remain consuming
// table slots until route-idle-timeout (~10 min). Under repeated online
// changes this floods the TwinCAT AMS router (Beckhoff issue #268).
//
// Transport is alive at this point — PLC just sent us a stale-cache code
// (0x711/0x705/0x710/...) over the active connection. bestEffortDelete
// treats 0x714 (NotifyHandleInvalid) as success-equivalent so any handles
// the PLC already auto-invalidated from the online-change don't error.
func (sess *Session) reloadSymbolsAndResubscribe() error {
	// Snapshot old handles + bump lastSubscribeNs so concurrent
	// handleNotification dispatches arriving for these handles during the
	// delete window log as Debug (first-sample-race branch) rather than Warn.
	// Clear activeNotifications under the same lock so AddSymbolNotifications
	// below sees a fresh map and the orphan-delete-on-unknown-handle path
	// (Fix 1) treats lingering samples as race-window noise, not orphans.
	sess.notifications.lock.Lock()
	oldHandles := make([]uint32, 0, len(sess.notifications.activeNotifications))
	for h := range sess.notifications.activeNotifications {
		oldHandles = append(oldHandles, h)
	}
	sess.notifications.lastSubscribeNs.Store(time.Now().UnixNano())
	sess.notifications.activeNotifications = make(map[uint32]activeNotification)
	sess.notifications.lock.Unlock()

	if len(oldHandles) > 0 {
		deleted := sess.bestEffortDeleteNotifications(sess.lifecycle.ctx, oldHandles)
		sess.logger.Info("auto-reload: deleted old PLC notification handles before resubscribe",
			"requested", len(oldHandles), "deleted", deleted)
	}

	if err := sess.LoadSymbols(sess.lifecycle.ctx); err != nil {
		return fmt.Errorf("LoadSymbols: %w", err)
	}
	return sess.resubscribeNotifications()
}

// markClosed closes the closedCh signal channel exactly once. Safe for
// concurrent invocation from Close() and from Reconnect-exhaustion path.
func (sess *Session) markClosed() {
	sess.lifecycle.closedOnce.Do(func() {
		close(sess.lifecycle.closedCh)
	})
}

// releasePLCResources releases PLC-side notification subscriptions and (when
// transport is still alive) PLC-side symbol handles. Both Close() and the
// Reconnect-exhaustion path call this so PLC state isn't stranded when the
// session terminates via either entry point.
//
// Notification cleanup runs even when disconnected: PLC tracks subscriptions
// per source AMS NetID, so a session that closes without deleting its
// notification handles leaves the PLC delivering them to the next session
// that opens with the same NetID. bestEffortDelete logs failures but never
// returns an error, so the call is safe even with a dead transport.
//
// symbol handle release is skipped when disconnected — unlike notifications,
// stranded symbol handles do not generate side effects on subsequent
// sessions, so the cost of leaking them is just a small PLC-side handle
// table entry that the PLC reaps on route timeout.
func (sess *Session) releasePLCResources(wasDisconnected bool) {
	sess.notifications.lock.Lock()
	handles := make([]uint32, 0, len(sess.notifications.activeNotifications))
	for handle := range sess.notifications.activeNotifications {
		handles = append(handles, handle)
	}
	sess.notifications.lock.Unlock()
	if len(handles) > 0 {
		deleted := sess.bestEffortDeleteNotifications(sess.lifecycle.ctx, handles)
		sess.logger.Info("releasePLCResources: best-effort notification cleanup",
			"requested", len(handles), "deleted", deleted,
			"wasDisconnected", wasDisconnected)
	}

	if wasDisconnected {
		sess.logger.Info("already disconnected, skipping handle cleanup")
		return
	}
	// Collect symbol handles under lock, then release without holding the lock.
	sess.cache.lock.Lock()
	symHandles := make([]uint32, 0, len(sess.cache.symbols))
	for _, symbol := range sess.cache.symbols {
		if symbol.Handle != 0 {
			symHandles = append(symHandles, symbol.Handle)
		}
	}
	sess.cache.lock.Unlock()

	// Release handles individually — ADS has no batch release command.
	// Re-check disconnected each iteration so a mid-loop PLC failure
	// doesn't force every remaining Write to time out.
	for i, h := range symHandles {
		if sess.isDisconnected() {
			sess.logger.Info("releasePLCResources: disconnected during handle release, stopping cleanup",
				"released", i,
				"remaining", len(symHandles)-i)
			break
		}
		handleBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(handleBytes, h)
		if err := sess.client.Load().Write(sess.lifecycle.ctx, uint32(GroupSymbolReleaseHandle), 0, handleBytes); err != nil {
			sess.logger.Warn("failed to release symbol handle", "error", err, "handle", h)
		} else {
			sess.logger.Info("handle deleted", "handle", h)
		}
	}
}

// Close releases PLC-side notification subscriptions, releases the cached
// PLC-side symbol handles when transport is still alive, cancels the
// session context, closes the underlying TCP socket, and waits for the
// listen + transmit + recv workers to exit. Idempotent — subsequent calls
// return nil without re-running cleanup.
//
// Returns nil on success; future implementations may surface specific
// failure modes via the error return (TCP close failure, PLC handle
// release failure). Implements io.Closer.
func (sess *Session) Close() error {
	// Capture transport-disconnected state BEFORE the FSM transitions into
	// Closed. The cleanup branch below uses this to decide whether to attempt
	// network ops; once state is Closed, isDisconnected() returns false even
	// if the transport was already gone.
	wasDisconnected := sess.isDisconnected()
	if _, ok := sess.lifecycle.state.transitionToOnce(SessionStateClosed); !ok {
		return nil // already closed (or transition not permitted from current state)
	}
	sess.markClosed()
	sess.logger.Info("Close called, shutting down")

	sess.releasePLCResources(wasDisconnected)
	// Capture cancel under RLock then release before invoking — see
	// tearDownAndReset for the symmetric pattern. Holding RLock across the
	// cancel() blocks tearDownAndReset's ctxMu.Lock replacement.
	sess.lifecycle.ctxMu.RLock()
	cancel := sess.lifecycle.shutdown
	sess.lifecycle.ctxMu.RUnlock()
	cancel()
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
	if c := sess.client.Load(); c != nil {
		c.waitGroup.Wait()
	}
	sess.lifecycle.waitGroup.Wait()
	sess.logger.Info("Close DONE")
	return nil
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
		go func() { _ = sess.Reconnect(sess.lifecycle.ctx) }()
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
func (sess *Session) Reconnect(ctx context.Context) error {
	// closeReconnectDone closes the reconnectDone channel if still open and
	// nils it. Mutex + nil-check is safe against concurrent callers — only
	// the first observer of a non-nil channel closes it.
	closeReconnectDone := func() {
		sess.lifecycle.reconnectMu.Lock()
		if sess.lifecycle.reconnectDone != nil {
			close(sess.lifecycle.reconnectDone)
			sess.lifecycle.reconnectDone = nil
		}
		sess.lifecycle.reconnectMu.Unlock()
	}

	if sess.isClosed() {
		// triggerReconnect may have created reconnectDone before Close ran.
		// Close it so Session.Close()'s reconnectDone wait unblocks instead
		// of hanging forever.
		closeReconnectDone()
		return fmt.Errorf("connection closed")
	}
	// Prevent concurrent reconnect attempts. transitionToOnce returns
	// ok=false on idempotent re-entry (state already Reconnecting), which
	// is exactly the single-flight gate we want.
	if _, ok := sess.lifecycle.state.transitionToOnce(SessionStateReconnecting); !ok {
		sess.logger.Info("reconnect already in progress or not permitted from current state, skipping")
		// If FSM rejected because state is terminal (Closed), close the
		// orphan reconnectDone so Close() unblocks. The winning goroutine
		// (if any) ran BEFORE state became Closed; we are running AFTER.
		// Safe vs winner: closeReconnectDone is mutex-protected + nil-check.
		if sess.isClosed() {
			closeReconnectDone()
		}
		return nil
	}

	// Create a channel that waiters (sendRequest) can block on.
	// triggerReconnect() may have already created it — only create if nil.
	sess.lifecycle.reconnectMu.Lock()
	if sess.lifecycle.reconnectDone == nil {
		sess.lifecycle.reconnectDone = make(chan struct{})
	}
	sess.lifecycle.reconnectMu.Unlock()

	defer closeReconnectDone()

	// Flap detection: a successful Connected → drop within flapWindow indicates
	// the previous reconnect cycle didn't really stabilize (typical when the
	// PLC RSTs every connection because its route table or connection-tracking
	// is saturated). Increment flapCount and sleep reconnectBackoff(flapCount)
	// before dialing so the existing stepped backoff also throttles cross-cycle
	// reconnect storms — not just within-one-Reconnect retries. Reset when the
	// last connection lived longer than flapResetWindow.
	sess.lifecycle.flapMu.Lock()
	lastConn := sess.lifecycle.lastConnectedAt
	if !lastConn.IsZero() {
		elapsed := time.Since(lastConn)
		switch {
		case elapsed < flapWindow:
			sess.lifecycle.flapCount++
		case elapsed > flapResetWindow:
			sess.lifecycle.flapCount = 0
		}
	}
	flapCount := sess.lifecycle.flapCount
	sess.lifecycle.flapMu.Unlock()

	if flapCount > 0 {
		delay := sess.reconnectBackoff(flapCount)
		sess.logger.Warn("connection flapping, applying cross-cycle cooldown before reconnect",
			"flapCount", flapCount, "delay", delay,
			"lastConnectedAgo", time.Since(lastConn))
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-sess.lifecycle.closedCh:
			timer.Stop()
			return fmt.Errorf("connection closed during flap cooldown")
		}
	}

	sess.logger.Info("attempting reconnect")
	sess.tx.disconnected.Store(true)
	// State is already Reconnecting (transitionToOnce above).

	// Clear active notifications (old handles invalid after reconnect) but
	// snapshot the handle list first. After dialAndStart brings up a new
	// transport with the same source NetID + port (Fix 4 makes the port
	// stable per-session), the PLC's notification handle table may still
	// hold these handles if the TCP drop was transient (within route-idle
	// timeout). Issue an explicit bestEffortDelete on the new transport so
	// the PLC table doesn't carry orphans across our reconnect — without
	// this cleanup, repeated TCP flaps accumulate handle slots and
	// eventually crash the TwinCAT AMS router (Beckhoff issue #268).
	sess.notifications.lock.Lock()
	savedHandles := make([]uint32, 0, len(sess.notifications.activeNotifications))
	for h := range sess.notifications.activeNotifications {
		savedHandles = append(savedHandles, h)
	}
	sess.notifications.activeNotifications = make(map[uint32]activeNotification)
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
			sess.logger.Error("max reconnect attempts exhausted, closing session",
				"maxAttempts", sess.lifecycle.maxReconnectAttempts, "error", lastErr)
			// Transition FSM to Closed so future Reconnect() calls are rejected
			// instead of silently no-op'ing on the stuck Reconnecting state.
			// Use transitionToOnce so a concurrent Close() wins cleanly.
			if _, ok := sess.lifecycle.state.transitionToOnce(SessionStateClosed); ok {
				// We won the transition: take ownership of PLC-side cleanup so
				// notification + handle leaks don't strand on the PLC. A subsequent
				// user Close() will short-circuit on transitionToOnce ok=false and
				// would otherwise skip cleanup entirely.
				sess.releasePLCResources(true) // transport already dead by exhaustion
			}
			// closedCh close is idempotent via closedOnce, safe whether Close()
			// or this branch ran first. Unblocks reconnectSleep / waitForReconnect.
			sess.markClosed()
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

		// Transport is fully restored. Before re-subscribing, issue a best-
		// effort delete for the pre-reconnect handles snapshotted above.
		// PLC may already have reaped them (route-idle-timeout or PLC reboot)
		// — bestEffortDelete treats 0x714 NotifyHandleInvalid as success-
		// equivalent, so already-aged handles don't error. Clear savedHandles
		// after the first successful pass so a later retry iteration (after
		// resubscribe failure → resetForRetry → loop) doesn't re-fire on an
		// already-cleaned PLC table.
		if len(savedHandles) > 0 {
			deleted := sess.bestEffortDeleteNotifications(sess.lifecycle.ctx, savedHandles)
			sess.logger.Info("reconnect: cleaned up pre-reconnect notification handles",
				"requested", len(savedHandles), "deleted", deleted)
			savedHandles = nil
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
		sess.lifecycle.flapMu.Lock()
		sess.lifecycle.lastConnectedAt = time.Now()
		sess.lifecycle.flapMu.Unlock()
		sess.logger.Info("reconnect successful", "attempts", attempts, "flapCount", flapCount)

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
	if sess.route.shouldSkip() {
		return nil
	}
	if sess.isClosed() {
		return fmt.Errorf("connection closed")
	}

	// Force mode or too many probe failures → always register
	probeFailures := sess.route.routeProbeFailures.Load()
	if sess.route.forceRouteRegistration || probeFailures >= 3 {
		sess.logger.Info("registering route (forced/fallback)", "probeFailures", probeFailures)
		if err := sess.AddRoute(sess.lifecycle.ctx, sess.route.name, sess.route.username, string(sess.route.password)); err != nil {
			return fmt.Errorf("route registration failed: %w", err)
		}

		sess.route.routeProbeFailures.Store(0)
		return nil
	}

	// Probe: try a lightweight ADS command to see if route already exists
	_, probeErr := sess.client.Load().GetSymbolVersion(sess.lifecycle.ctx)
	if probeErr == nil {
		sess.logger.Debug("route still valid, skipping re-registration")
		sess.route.routeProbeFailures.Store(0)
		return nil
	}

	if sess.isClosed() {
		return fmt.Errorf("connection closed during route probe")
	}

	// Probe failed → register with credentials
	failuresAfter := sess.route.routeProbeFailures.Add(1)
	sess.logger.Info("route probe failed, registering route", "error", probeErr, "probeFailures", failuresAfter)
	if err := sess.AddRoute(sess.lifecycle.ctx, sess.route.name, sess.route.username, string(sess.route.password)); err != nil {
		return fmt.Errorf("route registration failed after probe: %w", err)
	}

	return nil
}

// filterValidPending returns only pending entries whose symbols still exist
// in the current symbol table. Logs a warning for dropped subscriptions.
func (sess *Session) filterValidPending(entries []pendingNotification) []pendingNotification {
	sess.cache.lock.Lock()
	defer sess.cache.lock.Unlock()

	valid := make([]pendingNotification, 0, len(entries))
	for _, entry := range entries {
		name := entry.Config.SymbolName
		if _, exists := sess.cache.symbols[symbolKey(name)]; exists {
			valid = append(valid, entry)
		} else if _, onDemand := sess.cache.onDemandSymbols[symbolKey(name)]; onDemand {
			valid = append(valid, entry)
		} else {
			sess.logger.Warn("notification symbol gone after reconnect, dropping subscription",
				"symbol", name)
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
		return sess.loadSymbols(sess.lifecycle.ctx)

	case listLoaded || dtLoaded:
		// Partial discovery — re-download what was loaded
		if listLoaded {
			if err := sess.LoadSymbolList(sess.lifecycle.ctx, SlowDiscoveryConfig{}); err != nil {
				return fmt.Errorf("reload symbol list: %w", err)
			}
		}
		if dtLoaded {
			if err := sess.LoadDataTypes(sess.lifecycle.ctx, SlowDiscoveryConfig{}); err != nil {
				return fmt.Errorf("reload datatypes: %w", err)
			}
		}

	case hasOnDemand:
		// On-demand mode: re-resolve only the symbols that were previously loaded.
		// By default, missing symbols are skipped gracefully (PLC may have done
		// an online change). With WithStrictReconnect, missing symbols cause failure.
		//
		// Snapshot the requested set BEFORE wiping cache.symbols; do NOT also
		// wipe onDemandSymbols here. Failed resolutions leave their name in
		// onDemandSymbols so the NEXT reconnect retry still sees the full
		// requested set. Without this, partial-success on retry N silently
		// drops the failed names from retry N+1's set, masking symbols that
		// would have come back after a transient PLC condition cleared.
		sess.cache.lock.Lock()
		oldSymbols := make(map[string]bool, len(sess.cache.onDemandSymbols))
		for k, v := range sess.cache.onDemandSymbols {
			oldSymbols[k] = v
		}
		sess.cache.symbols = make(map[string]*symbol)
		sess.bumpEpoch()
		sess.cache.lock.Unlock()

		for name := range oldSymbols {
			if _, err := sess.getSymbol(sess.lifecycle.ctx, name); err != nil {
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
		version, err := sess.client.Load().GetSymbolVersion(sess.lifecycle.ctx)
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
// The resetFeatureFlags parameter is retained for API symmetry with prior
// callers but is now a no-op — capability state lives on *Client, and
// dialAndStart allocates a fresh zero-valued Client on every attempt.
//
// Used by three reset paths:
//   - Connect()'s post-route-registration TCP teardown
//   - Reconnect()'s pre-retry-loop reset
//   - resetForRetry()
func (sess *Session) tearDownAndReset(resetFeatureFlags bool) {
	// Capture cancel under RLock then release before invoking. Calling the
	// cancel under RLock would deadlock against the subsequent ctxMu.Lock
	// at the ctx replacement below if shutdown ever became a function that
	// took the same lock.
	sess.lifecycle.ctxMu.RLock()
	cancel := sess.lifecycle.shutdown
	sess.lifecycle.ctxMu.RUnlock()
	cancel()
	sess.tx.connMu.Lock()
	if sess.tx.connection != nil {
		sess.tx.connection.Close()
	}
	sess.tx.connMu.Unlock()
	// Wait for the previous batch of Client workers (listen, transmit,
	// recvWorker) to exit. They share ctx with lifecycle.ctx; the cancel
	// above plus the closed TCP socket trigger their exit.
	if c := sess.client.Load(); c != nil {
		c.waitGroup.Wait()
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
	newConn, err := sess.dialTCP()
	if err != nil {
		return err
	}
	sess.tx.connMu.Lock()
	sess.tx.connection = newConn
	sess.tx.connMu.Unlock()
	configureKeepAlive(newConn)
	if sess.isClosed() {
		// Session was Closed mid-dial. Don't Add to waitGroup.
		sess.tx.connMu.Lock()
		newConn.Close()
		sess.tx.connection = nil
		sess.tx.connMu.Unlock()
		return fmt.Errorf("connection closed during dial")
	}
	// Allocate a fresh Client. Capture ctx + cancel under a single RLock so
	// a concurrent tearDownAndReset replacement cannot split the pair.
	sess.lifecycle.ctxMu.RLock()
	freshCtx := sess.lifecycle.ctx
	freshCancel := sess.lifecycle.shutdown
	sess.lifecycle.ctxMu.RUnlock()
	newClient := &Client{
		ip:             sess.ip,
		port:           sess.port,
		target:         sess.target,
		source:         sess.source,
		requestTimeout: sess.requestTimeout,
		logger:         sess.logger,
		tx:             sess.tx,
		ctx:            freshCtx,
		cancel:         freshCancel,
	}
	newClient.SetNotificationHandler(sess.handleNotification)
	newClient.SetOnDrop(sess.triggerReconnect)
	newClient.startWorkers()
	// Publish only after handlers + workers are wired so concurrent readers
	// never observe a half-initialized Client. Clear disconnected AFTER
	// startWorkers so a user RPC that observes disconnected=false is
	// guaranteed to find transmitWorker actually running.
	sess.client.Store(newClient)
	sess.tx.disconnected.Store(false)
	return nil
}

// localHandshake performs the local-mode AMSAddress probe used after dial when
// isLocal is true. Updates sess.source on success.
func (sess *Session) localHandshake() error {
	resp, err := sess.client.Load().send([]byte{0, 16, 2, 0, 0, 0, 0, 0})
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
	savedPending := sess.notifications.pending
	savedChannel := sess.notifications.notificationChannel
	// Clear via resetConfigs so the key-index mirror is wiped in lockstep —
	// AddSymbolNotifications dup-checks against the mirror and would reject
	// every resubscribe if the old keys remained.
	sess.notifications.resetConfigs(nil)
	sess.notifications.lock.Unlock()
	if len(savedPending) == 0 || savedChannel == nil {
		return nil
	}
	validPending := sess.filterValidPending(savedPending)
	validConfigs := make([]NotificationConfig, len(validPending))
	for i, p := range validPending {
		validConfigs[i] = p.Config
	}
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

	subResults, err := sess.AddSymbolNotifications(sess.lifecycle.ctx, validConfigs, savedChannel)

	// Collect Skipped+Handle entries: AddSymbolNotifications surfaces handles
	// for items where the PLC accepted but the library refused to commit
	// (concurrent-subscribe TOCTOU loss, cache-stranded post-roundtrip).
	// Those handles are NOT in activeNotifications - they would leak unless
	// we explicitly release them. Re-append the corresponding config to
	// notificationConfigs (with attempt counter incremented) so the NEXT
	// reconnect retries; drop after resubscribeMaxAttempts to prevent infinite
	// churn on persistently-flapping symbols.
	var orphanHandles []uint32
	var retryEntries []pendingNotification
	var droppedConfigs []string
	for i, r := range subResults {
		if r.Skipped != nil && r.Handle != 0 {
			orphanHandles = append(orphanHandles, r.Handle)
		}
		if r.Skipped != nil && i < len(validPending) {
			entry := validPending[i]
			entry.resubscribeAttempts++
			if entry.resubscribeAttempts >= resubscribeMaxAttempts {
				droppedConfigs = append(droppedConfigs, entry.Config.SymbolName)
				continue
			}
			retryEntries = append(retryEntries, entry)
		}
	}
	if len(orphanHandles) > 0 {
		deleted := sess.bestEffortDeleteNotifications(sess.lifecycle.ctx, orphanHandles)
		sess.logger.Warn("resubscribe: released PLC handles for Skipped+Handle entries",
			"orphan_handles", len(orphanHandles),
			"deleted", deleted)
	}
	if len(retryEntries) > 0 {
		sess.notifications.lock.Lock()
		for _, p := range retryEntries {
			sess.notifications.addPending(p)
		}
		sess.notifications.lock.Unlock()
		sess.logger.Info("resubscribe: queued Skipped configs for next reconnect retry",
			"retry_count", len(retryEntries))
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
		// resetConfigs rebuilds the key-index mirror to match savedPending.
		sess.notifications.resetConfigs(savedPending)
		sess.notifications.notificationChannel = savedChannel
		sess.notifications.lock.Unlock()
		if len(newHandles) > 0 {
			deleted := sess.bestEffortDeleteNotifications(sess.lifecycle.ctx, newHandles)
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

// zeroOldSymbolHandles invalidates each *symbol in the given map. Sets
// Handle=0 so callers holding pointers to OLD-map values force on-demand
// re-resolution via GetSymbol (defends against the PLC reusing the old
// handle for a different symbol after reconnect), and clears cached
// Value/Valid/ValueParsed/LastUpdateTime so a Read within MinUpdateInterval
// of reconnect does not return stale pre-disconnect data. Nil-safe.
func zeroOldSymbolHandles(m map[string]*symbol) {
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
func (sess *Session) loadSymbols(ctx context.Context) error {
	c := sess.client.Load()
	// Read and store symbol version
	version, err := c.GetSymbolVersion(ctx)
	if err != nil {
		sess.logger.Warn("failed to read symbol version, continuing with symbol load", "error", err)
	} else {
		sess.cache.lock.Lock()
		sess.cache.symbolVersion = version
		sess.cache.lock.Unlock()
	}

	res, err := c.GetSymbolUploadInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to get symbol upload info: %w", err)
	}
	datatypesResponse, err := c.DownloadDataTypes(ctx, res.DataTypeLength)
	if err != nil {
		return fmt.Errorf("failed to upload datatypes: %w", err)
	}
	datatypes, err := parseUploadSymbolInfoDataTypes(datatypesResponse)
	if err != nil {
		return fmt.Errorf("failed to parse datatypes: %w", err)
	}
	symbolsResponse, err := c.DownloadSymbolList(ctx, res.SymbolLength)
	if err != nil {
		return fmt.Errorf("failed to upload symbols: %w", err)
	}
	symbols, err := parseUploadSymbolInfoSymbols(symbolsResponse, datatypes)
	if err != nil {
		return fmt.Errorf("failed to parse symbols: %w", err)
	}
	sess.cache.lock.Lock()
	// invalidate Handle on every old *symbol before swap so external
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
func (sess *Session) AddRoute(ctx context.Context, routeName, username, password string) error {
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

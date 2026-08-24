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
	ctxMu sync.RWMutex // protects ctx and shutdown against concurrent access during reconnect
	// parentCtx is the original context.Context passed to NewSession. ctx
	// (the active lifecycle ctx) is re-derived from parentCtx after every
	// tearDownAndReset so cancelling the original NewSession ctx still
	// shuts the session down — even across multiple Reconnect cycles. Set
	// once at construction; never replaced.
	parentCtx context.Context
	ctx       context.Context
	shutdown  context.CancelFunc
	waitGroup sync.WaitGroup

	reconnectMu   sync.Mutex // protects reconnectDone
	reconnectDone chan struct{}

	// unservedCooldown is how long the reconnect loop goes completely quiet — no
	// sockets, no route registration — after unservedAttemptsBeforeCooldown
	// consecutive attempts where the TCP dial SUCCEEDED but the PLC answered
	// nothing.
	//
	// That combination is not "the PLC is down"; it is a router that has our IP
	// in a state it will not serve. The TwinCAT router expects exactly one TCP
	// connection per remote IP and drops the older one whenever a new connection
	// arrives (Beckhoff/ADS#85), so redialing every backoff sustains the problem:
	// each new dial costs the router the connection it just rebuilt. Observed on a
	// TC/RTOS device in this lab for months, and it tends to serve again once
	// something stops connecting for a while. Zero means the default.
	unservedCooldown time.Duration

	// reconnectAttempts counts dials made by the current reconnect loop. Exposed
	// for tests only.
	reconnectAttempts atomic.Int64

	// reconnectOwner is the single-flight gate: the goroutine that flips it
	// false -> true owns the reconnect and clears it on the way out.
	//
	// The gate used to be the FSM transition result — "state is already
	// Reconnecting" was read as "someone is working on it". Nothing enforced
	// that. A state left behind by an attempt that ended without resolving it
	// made every later Reconnect log "already in progress" and return nil, so the
	// session sat in Reconnecting forever with IsClosed() false: no data, and no
	// consumer signal to rebuild. Ownership is now explicit, and the FSM's
	// Reconnecting self-loop lets a new owner take over an abandoned state.
	reconnectOwner atomic.Bool

	// connecting is true for the whole of Connect, and suppresses the automatic
	// Reconnect that a drop would otherwise spawn.
	//
	// Connect owns sess.tx / sess.client / lifecycle.ctx end to end — it dials,
	// publishes, probes, and on any failure tears down and rolls back — but ondrop
	// is armed across most of that (the local handshake, the liveness probe, the
	// runtime-state read; the route stage disarms, and its helpers re-arm from a
	// defer in the middle of Connect). A rival Reconnect running tearDownAndReset
	// under Connect cancelled the context Connect's fresh Client was holding and
	// closed the socket it had just dialled, and vice versa, leaving a session the
	// caller had been told failed sitting Connected on a second connection with
	// IsClosed() false — socket, workers, runtime watcher and an unbounded
	// reconnect loop all leaked, and not recoverable in place because a retry is
	// refused as "Connect already in progress".
	//
	// A flag rather than tighter SetOnDrop bookkeeping: five sites arm or re-arm,
	// two of them from a defer mid-Connect, and TestAwaitRouteActive_RestoresClientState
	// correctly pins that re-arm. One gate in triggerReconnect is immune to all of it.
	connecting atomic.Bool

	closedCh   chan struct{}
	closedOnce sync.Once
	// shutdownOnce guards the terminal teardown (see Session.shutdownTransport).
	// It exists because that work has two entry points — Close() and
	// giveUpReconnecting() — and gating it on winning the FSM transition to Closed
	// meant whichever lost did nothing at all: a give-up left the socket, the 48898
	// listener and every worker in place, and the user's later Close() returned nil
	// without touching them.
	shutdownOnce sync.Once
	// spawnMu makes "is the session closed" and "register a goroutine" one atomic
	// decision. A bare isClosed() check before waitGroup.Add is a TOCTOU: Close can
	// run to completion in the gap, its Wait returns with the counter at 0, and the
	// late Add is then exactly what sync.WaitGroup reports as
	// "Add called concurrently with Wait" — a panic that takes the process down.
	// Reachable from a user goroutine, not just internal ones: AddSymbolNotification
	// -> endSubscribe -> replayEarlySamples -> dispatchSample -> tryOrphanDelete.
	spawnMu sync.Mutex // guards close(closedCh) so Close() and Reconnect-exhaustion can both fire safely

	// state is the explicit FSM state plus the unified epoch counter
	// (docs/archive/specs/09-fsm-design.md). FSM is the source of truth for closed and
	// reconnecting. epoch replaces the previous cache.generation and
	// reconnectGeneration counters and bumps on every Connected entry plus
	// on user-driven cache swaps that don't (yet) transition through
	// Reloading.
	state sessionFSM

	// connectedGen counts genuine (re)entries into Connected, and nothing else.
	// It exists for the heartbeat watcher: a reconnect resubscribes everything and
	// registers a fresh beat, so the silence count the watcher accumulated before
	// the drop describes subscriptions that no longer exist. Carrying it across the
	// gap declares the rebuilt session dead on its first Connected tick.
	//
	// Deliberately NOT epoch: bumpEpoch also fires on cache.symbols swaps, so a
	// flapping symbol version would reset the detector every tick and mask a real
	// stall forever.
	//
	// The counter must therefore advance ONLY when the subscriptions were actually
	// rebuilt. enterConnected enforces that by bumping only for
	// Connecting/Reconnecting -> Connected. Note Reloading -> Connected is a legal
	// FSM edge that no production path takes today (SessionStateReloading is never
	// entered outside tests); if the reload path is ever wired through it, it must
	// keep going through enterConnected's filter and stay unbumped, or every
	// AutoReload cycle re-introduces exactly the masking that ruled epoch out.
	connectedGen atomic.Uint64

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

// enterConnected announces Connected and advances lifecycle.connectedGen when the
// session really came from a connect or a reconnect — i.e. when its subscriptions
// have just been rebuilt. Every production transition into Connected goes through
// here; see the connectedGen comment for why the from-filter is the whole point.
//
// Logging matches transitionState so the FSM trace is unchanged.
func (sess *Session) enterConnected() {
	from, ok := sess.lifecycle.state.transitionTo(SessionStateConnected)
	if !ok {
		sess.logger.Warn("FSM invalid transition (ignoring)",
			"from", from, "to", SessionStateConnected)
		return
	}
	sess.logger.Log(context.Background(), LevelTrace, "FSM transition",
		"from", from, "to", SessionStateConnected)
	if from == SessionStateConnecting || from == SessionStateReconnecting {
		sess.lifecycle.connectedGen.Add(1)
	}
}

// connectedGen returns the connected-generation counter. Read lock-free, one Load
// per heartbeat tick; a stale read costs one extra tick of delay and never a
// skipped recovery.
func (sess *Session) connectedGen() uint64 {
	return sess.lifecycle.connectedGen.Load()
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
// See docs/archive/specs/09-fsm-design.md for the full layered architecture.
type Session struct {
	ip   string
	port int

	// Underlying RPC client. nil until Connect succeeds; replaced on
	// Reconnect; shut down by Close. atomic.Pointer so concurrent reads on
	// user RPC paths cannot race the publish in Connect / dialAndStart.
	client atomic.Pointer[Client]

	// TCP socket + request multiplexing + listen/transmit channels.
	tx *transport

	target      AMSAddress
	source      AMSAddress
	callbackIP  string // IP PLC uses to reach us (for Docker/VPN; set via WithHostIP)
	localBindIP net.IP // Force outbound TCP source IP (multi-session per host; set via WithLocalBindIP). nil = OS default routing.

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

	// peerListenPort, when non-zero, is the TCP port on which the session accepts
	// a connection the PLC opens back to us, for devices that answer there rather
	// than on the connection we opened. See WithAmsPeerListen.
	peerListenPort int
	peerLn         net.Listener
	// peerWG tracks the accept loop. Deliberately NOT lifecycle.waitGroup: that
	// one is waited on by tearDownAndReset, which runs on every reconnect, while
	// this listener lives for the whole session and only closes in Close — so
	// sharing the group deadlocks the first teardown.
	peerWG sync.WaitGroup
	// peerMu guards peerLn. sync.Once orders only the goroutines that call Do, and
	// Close never does: it reads peerLn from whatever goroutine called Close while
	// a Connect may still be inside startPeerListener. Unsynchronised, Close can
	// read nil, skip closing the listener, and then block forever in peerWG.Wait()
	// on an accept loop nothing will ever wake — the loop's isClosed() escape only
	// runs after a connection arrives, which on a dead PLC never happens.
	peerMu      sync.Mutex
	peerStopped bool
	// peerFallbackDisabled turns off the automatic attempt described in
	// tryPeerFallback. See WithoutAmsPeerFallback.
	peerFallbackDisabled bool
	// peerConnsAdopted counts the inbound connections this session has handed to a
	// Client. Non-zero means the device really does answer on a connection it opens
	// to us, which is what forgetPeerRouteHostIfUnused needs to know: a Connect that
	// succeeded with this at zero got its answers on our own connection and must not
	// leave the device remembered. Atomic because the accept loop writes it while
	// Connect reads it.
	peerConnsAdopted atomic.Int64

	// Heartbeat: an internal cyclic notification whose silence proves the caller's
	// subscriptions have died. See notificationManager.heartbeatHandle.
	heartbeatInterval time.Duration
	heartbeatMissed   int
	heartbeatDisabled bool
	heartbeatOnce     sync.Once
	heartbeatWG       sync.WaitGroup

	// runtimeState is the last ADS state read from the system service port, and
	// runtimeStateNs when. Zero (ADSStateInvalid) means "not known yet" — the gates
	// below only refuse on a POSITIVE non-RUN reading, never on absence of one, so a
	// device that does not serve the system service port behaves as before.
	//
	// Measured on TC3.1.4024 in CONFIG: the runtime port answers every request with
	// AMS ErrorCode 6 (target port not found) while port 10000 reports ADSState=15.
	// Without asking 10000 the session cannot tell "the runtime is not running" from
	// "this device is broken", and it retries either way.
	runtimeState   atomic.Uint32
	runtimeStateNs atomic.Int64
	// stateWG, not lifecycle.waitGroup: the watch lives for the whole session, and
	// tearDownAndReset waits lifecycle.waitGroup on every reconnect — putting it
	// there deadlocked the first reconnect, exactly as it once did for the peer
	// accept loop.
	stateWG   sync.WaitGroup
	stateOnce sync.Once

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

	// targetCheck is the policy for a caller-supplied target NetID that the
	// device disagrees with. Write-once at construction via WithTargetCheck.
	targetCheck TargetCheck

	// routerPort is the UDP port for route registration and identify. Set from
	// AMSEndpoint.RouterPort; 0 means the protocol default.
	routerPort int

	logger *slog.Logger
}

// AMSEndpoint identifies a remote ADS endpoint at the TCP and AMS layers.
// IP+Port locate the TwinCAT runtime over TCP (port 48898 by default);
// AMS is the target AMSAddress carried in every ADS request header.
type AMSEndpoint struct {
	// IP is the host or address of the PLC (or of the NAT that forwards to it).
	IP string
	// Port is the TCP port carrying AMS. Defaults to 48898, TwinCAT's own.
	Port int
	// AMS is the target AMS address. A zero NetID and/or Port is resolved from
	// the device — see NewSession.
	AMS AMSAddress
	// RouterPort is the UDP port of the AMS router, used to register a route and
	// to identify the device. Defaults to 48899, TwinCAT's own.
	//
	// Set it when the PLC is reached through NAT with port forwarding. NAT maps
	// one external port per internal port, so the forwarded UDP port is normally
	// a different number than the forwarded TCP port and cannot be derived from
	// Port — e.g. external TCP 5534 -> 48898 and external UDP 6499 -> 48899 give
	// AMSEndpoint{IP: natHost, Port: 5534, RouterPort: 6499}.
	//
	// Only the two UDP calls use it. Notifications need no inbound forward: they
	// arrive on the TCP connection this side opened.
	RouterPort int
}

// NewSession creates a new ADS session targeting remote. The session does
// no I/O until Connect; ctx is captured for the long-lived session
// lifecycle and cancelling it shuts the session down.
//
// Target address: remote.AMS may be left incomplete. A zero NetID and/or a
// zero AMS port are resolved by asking the device for its own identity over
// UDP (see IdentifyRemote) — one round-trip, read-only, no route or
// credentials needed. The resolved port follows the TwinCAT-major-version
// convention (801 on TC2, 851 on TC3), so a project with several runtimes must
// still set the port explicitly. This is the only case where NewSession does
// I/O, and it happens because omitting the address is itself a request to look
// it up.
//
// A target that IS fully specified is left alone here and checked against the
// device by Connect, so a wrong or stale NetID surfaces as a named error rather
// than as a session that connects and then answers nothing. See WithTargetCheck.
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
	if remote.RouterPort <= 0 {
		remote.RouterPort = routePort // TwinCAT UDP default
	}
	if ctx == nil {
		ctx = context.Background()
	}
	sessCtx, cancel := context.WithCancel(ctx) //nolint:gosec // cancel stored in lifecycle.shutdown, called from Close + tearDownAndReset
	sess = &Session{
		ip:             remote.IP,
		port:           remote.Port,
		routerPort:     remote.RouterPort,
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
			activeRequests: map[uint32]chan amsReply{},
		},
		lifecycle: &sessionLifecycle{
			autoReconnect:        true,
			maxReconnectAttempts: 0, // 0 = infinite retries
			backoffConfig:        DefaultBackoffConfig(),
			closedCh:             make(chan struct{}),
			parentCtx:            ctx,
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
	// Verify a caller-supplied target NetID against the device by default, but
	// only warn on a mismatch — see TargetCheck for why it cannot be an error.
	sess.targetCheck = TargetCheckWarn
	// Default local AMS port: random in IANA dynamic range so each process /
	// each session presents a distinct AMS source identity to the PLC. See
	// randomAMSPort doc for the rationale. WithLocalAMS overrides for stable-
	// port deployments (firewalled environments, container port allow-lists).
	sess.source.Port = randomAMSPort()
	for _, opt := range opts {
		opt(sess)
	}
	// Resolve whatever the caller left out of the target address by asking the
	// PLC's router for its own identity. Decided from sess.target AFTER the
	// options ran, not from the constructor argument: reading pre-option state
	// to make a post-option decision is how the two silently diverge.
	//
	// Only when something is actually missing: omitting the address IS the
	// request to look it up, so the I/O is what the caller asked for. A fully
	// specified target reaches Connect without any I/O here — verifying one is
	// Connect's job, not the constructor's.
	//
	// Never in local mode. There the target is loopback by definition and
	// Connect overwrites NetID and IP regardless (see WithLocalMode), so a probe
	// against the caller-supplied address is at best a wasted round-trip that
	// logs a discovered address Connect is about to discard — and at worst it
	// fails construction for a mode that needs no network to resolve anything.
	needsDiscovery := !sess.isLocal &&
		(sess.target.NetID == [6]byte{} || sess.target.Port == 0)
	if needsDiscovery {
		if err := sess.discoverTarget(ctx); err != nil {
			cancel()
			return nil, err
		}
	}
	return sess, nil
}

// discoverTarget fills in a missing target NetID and/or AMS port from the
// remote's identify response. A wrong or absent NetID is the most common ADS
// misconfiguration and the hardest to recognise — the router accepts the TCP
// socket and then drops every request — so resolving it from the device beats
// making the caller carry it.
func (sess *Session) discoverTarget(ctx context.Context) error {
	id, err := identifyRemoteFrom(ctx, sess.logger, sess.localBindIP, sess.ip, sess.effectiveRouterPort())
	if err != nil {
		return fmt.Errorf("ads: NewSession: target AMS address incomplete and discovery failed "+
			"(set remote.AMS explicitly if the device does not answer the identify service): %w", err)
	}
	return sess.applyDiscoveredIdentity(id)
}

// applyDiscoveredIdentity fills the missing halves of the target address from a
// probe result. Split from the round-trip so the decisions are testable without
// a device.
func (sess *Session) applyDiscoveredIdentity(id RemoteIdentity) error {
	if sess.target.NetID == [6]byte{} {
		sess.target.NetID = id.AMS.NetID
	}
	if sess.target.Port == 0 {
		// The port is a per-major-version convention, not a discovered value, so
		// it needs a version to be a convention AT ALL. With none reported,
		// planting the TwinCAT 3 default would silently address a runtime that
		// may not exist — reproducing the very failure this feature removes.
		// Refuse instead and say what to do.
		if id.Major == 0 {
			return fmt.Errorf("ads: NewSession: %s (%s) reported no TwinCAT version, so the runtime "+
				"AMS port cannot be inferred; set remote.AMS.Port explicitly (801 on TwinCAT 2, 851 on TwinCAT 3)",
				sess.ip, id.HostName)
		}
		// Logged because a multi-runtime project needs 811/852/... and must
		// override it.
		sess.target.Port = id.RuntimePort()
	}
	sess.logger.Info("discovered target AMS address",
		"host", sess.ip,
		"netID", sess.target.NetIDString(),
		"port", sess.target.Port,
		"hostName", id.HostName,
		"twinCAT", id.Version())
	return nil
}

// targetVerifyTimeout bounds the verification round-trip. Shorter than
// identifyTimeout because verification is optional: a device that does not
// answer must cost a caller who already knows the address almost nothing.
const targetVerifyTimeout = time.Second

// verifyTarget asks the device what NetID it has and compares it with the one
// the caller supplied. Catching a wrong NetID here turns the worst failure mode
// in ADS — the router accepts the socket and then silently drops every request
// — into an immediate, named answer.
//
// An unanswered probe is never a failure, in ANY TargetCheck mode: a device can
// serve ADS on TCP 48898 with UDP 48899 firewalled off, which was observed on a
// Windows TwinCAT host during development. Blocking that host's sessions over a
// check that could not run would trade a real capability for a diagnostic. Only
// a definite mismatch is actionable, so only a definite mismatch is reported.
func (sess *Session) verifyTarget(ctx context.Context) error {
	verifyCtx, cancel := context.WithTimeout(ctx, targetVerifyTimeout)
	defer cancel()
	id, err := identifyRemoteFrom(verifyCtx, sess.logger, sess.localBindIP, sess.ip, sess.effectiveRouterPort())
	if err != nil {
		// Info, not Debug: the operator needs to know the guard did not run.
		// Silence here would read as "verified" at default log level.
		sess.logger.Info("target NetID not verified — device did not answer the identify service (UDP firewalled?); continuing",
			"host", sess.ip, "router_port", sess.effectiveRouterPort(),
			"target", sess.target.NetIDString(), "error", err)
		return nil
	}
	return sess.applyTargetCheck(id)
}

// applyTargetCheck decides what a verification result means. Split from the
// round-trip so the policy is testable without a device.
func (sess *Session) applyTargetCheck(id RemoteIdentity) error {
	if id.AMS.NetID == sess.target.NetID {
		sess.logger.Debug("target NetID confirmed by device",
			"host", sess.ip, "netID", sess.target.NetIDString(),
			"hostName", id.HostName, "twinCAT", id.Version())
		return nil
	}
	// A mismatch is usually a wrong or stale NetID, but it is also exactly what
	// a legitimate routed setup looks like: point at a gateway and the NetID you
	// want belongs to a device behind it, not to the responder. Nothing in the
	// response separates the two, which is why the default warns rather than
	// refusing — see WithTargetCheck.
	const hint = "usually a wrong or stale target NetID; legitimate when this host is a router and the target sits behind it"
	if sess.targetCheck == TargetCheckError {
		return fmt.Errorf("ads: target NetID %s does not match the NetID %s reported by %s (%s, TwinCAT %s): %s",
			sess.target.NetIDString(), id.AMS.NetIDString(), sess.ip, id.HostName, id.Version(), hint)
	}
	sess.logger.Warn("target NetID differs from the NetID this device reports for itself",
		"host", sess.ip,
		"configured", sess.target.NetIDString(),
		"reported", id.AMS.NetIDString(),
		"hostName", id.HostName,
		"twinCAT", id.Version(),
		"hint", hint)
	return nil
}

// Connect dials the PLC and transitions the session to Connected. Local-mode
// (in-process TwinCAT runtime at 127.0.0.1) is selected via WithLocalMode at
// NewSession time.
//
// Connect may bind a LISTENING socket on the AMS port (48898 by default) without
// being asked: some devices answer only on a connection they open to us, and a
// session that cannot accept it sees every request time out. That happens when a
// connect proves the device otherwise silent, and — for a device already shown to
// behave this way in this process — before probing at all. WithAmsPeerListen(port)
// moves it; WithoutAmsPeerFallback() refuses it entirely.
//
// A failed Connect leaves the session usable for a retry (the FSM rolls back to
// Disconnected) and releases the inbound listener it bound, so a caller who
// discards the session after an error leaks neither the port nor its goroutines.
// Close is still the caller's responsibility for everything else.
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
	// Suppress the auto-reconnect spawn for the whole of Connect: see
	// lifecycle.connecting. Registered BEFORE the rollback and listener defers so
	// it runs AFTER them (LIFO) — the flag has to still be set while Connect does
	// its own teardown, or the rival is merely delayed into the same window.
	sess.lifecycle.connecting.Store(true)
	defer func() {
		sess.lifecycle.connecting.Store(false)
		// A drop that landed while the spawn was suppressed and that Connect
		// itself never noticed — a reset after the last request, say — is
		// Connect's to adopt: on the success path nothing else will, and
		// tx.disconnected is the sole record of it. Without this, the
		// suppression would trade a rival Reconnect for a lost drop, which is
		// the same invisible-stuck state one layer down.
		//
		// The ordering interlocks: a drop suppressed by the gate was recorded
		// before the gate read the flag, so it is visible here; a drop landing
		// after the store goes down the normal path and any duplicate this
		// spawns loses the reconnectOwner CAS.
		//
		// Narrow on purpose (retErr == nil): after an error the caller's
		// contract is to throw the session away, and adopting there would race
		// a caller's own retry.
		if retErr == nil && !sess.isClosed() && sess.tx.disconnected.Load() {
			sess.triggerReconnect()
		}
	}()
	// Roll back Connecting→Disconnected on any error return so the caller
	// can retry Connect via the Disconnected→Connecting edge. Without this,
	// the FSM is stranded in Connecting and Reconnect (auto-path only) is
	// the sole recovery, forcing callers to construct a new Session.
	defer func() {
		if retErr != nil && sess.lifecycle.state.load() == SessionStateConnecting {
			sess.lifecycle.state.transitionTo(SessionStateDisconnected)
		}
	}()
	// Release the inbound listener on every error return, and only on those. Connect
	// binds it in three places (the WithAmsPeerListen pre-bind below, the
	// known-peer-host pre-bind after the dial, and tryPeerFallback), and a Connect
	// that then fails used to leave all of them: port 48898 held plus the accept
	// loop and the workers it feeds, once per attempt. Callers treat a Connect error
	// as "throw the session away and build a new one" — nobody calls Close on it —
	// so the leak was unbounded.
	//
	// releasePeerListener, NOT stopPeerListener: the latter latches peerStopped and
	// would turn a legal retry from Disconnected into a permanent bind refusal.
	defer func() {
		if retErr != nil {
			sess.releasePeerListener()
		}
	}()
	// Before dialing: some devices answer only on a connection they open to us,
	// so the listener has to be up before the first request goes out or its
	// response has nowhere to land. See WithAmsPeerListen.
	if sess.peerListenPort != 0 {
		if lerr := sess.startPeerListener(); lerr != nil {
			return lerr
		}
	}

	var err error
	sess.logger.Debug("dialing", "ip", sess.ip, "port", sess.port)
	if local {
		sess.target.NetID = [6]byte{127, 0, 0, 1, 1, 1}
		sess.ip = "127.0.0.1"
	}
	// Check the target NetID against the device before spending a dial on it.
	// Skipped in local mode, where the target was just overwritten with the
	// loopback NetID and there is nothing to compare against.
	if !local && sess.targetCheck != TargetCheckOff {
		if err := sess.verifyTarget(ctx); err != nil {
			return err
		}
	}
	tcpConn, err := sess.dialTCP()
	if err != nil {
		// Warn, not Error: the error is also returned, the session stays retryable
		// (the deferred rollback restores Disconnected), and a reconnecting caller
		// dials again — so this is a transient condition being retried, not
		// something only a human can clear.
		sess.logger.Warn("could not dial the PLC", "ip", sess.ip, "port", sess.port, "error", err)
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
			udpConn, udpErr := net.DialTimeout("udp4", net.JoinHostPort(sess.ip, strconv.Itoa(sess.effectiveRouterPort())), 2*time.Second)
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

	// One snapshot for the three log lines below, taken under the lock that guards
	// the field (see sourceAddr): a rival Reconnect's localHandshake can be writing
	// it, and these lines are exactly the diagnostics an operator uses to decide
	// whether their NetID configuration is wrong.
	sourceAddr := sess.sourceAddr()
	sourceIP := sourceAddr.NetID
	// Log container detection — auto-derived NetID works in containers because
	// the PLC stores the UDP source IP (post-NAT) for routes, not the computerName tag.
	if sess.callbackIP == "" && isRunningInContainer() {
		sess.logger.Info("container detected — auto-derived NetID will be used for route registration",
			"netidIP", fmt.Sprintf("%d.%d.%d.%d", sourceIP[0], sourceIP[1], sourceIP[2], sourceIP[3]))
	}

	// Log ADS-level addressing (what matters for AMS routing, may differ from TCP)
	routeHostIP := sess.callbackIP
	if routeHostIP == "" {
		routeHostIP = fmt.Sprintf("%d.%d.%d.%d (from NetID, PLC will use UDP source IP)", sourceIP[0], sourceIP[1], sourceIP[2], sourceIP[3])
	}
	sess.logger.Info("ADS addressing",
		"sourceNetID", sourceAddr.NetIDString(),
		"routeHostIP", routeHostIP,
		"target", sess.target.String())

	sess.logger.Log(context.Background(), LevelTrace, "connected")
	// Session and Client share the *transport pointer (no re-dial); the Client
	// owns the listen / transmit / recvWorker goroutines.
	newClient := sess.publishWiredClient()
	// Same rule as dialAndStart: clear AFTER the workers are up, so anything that
	// observes disconnected=false finds transmitWorker actually running.
	//
	// Connect never cleared this flag, which was invisible only because a rival
	// Reconnect used to do it: the flag starts false, so the first Connect never
	// noticed, and a retry after a Connect that recorded a drop only worked because
	// the drop had spawned a Reconnect whose dialAndStart cleared it. With that
	// spawn suppressed (lifecycle.connecting) the stale true survived into the
	// retry, and every request on the retry's perfectly good socket failed
	// ErrTransportClosed — a session that cannot be reconnected in place and cannot
	// be retried either. A drop landing in the gap above is not erased for long:
	// the liveness probe is the very next thing to run and fails on the dead
	// socket, which records it again.
	sess.tx.disconnected.Store(false)
	// If this device has already been shown to answer on a connection it opens to
	// us, bind the listener before probing anything. Otherwise every session pays
	// the full probe timeout plus the route-activation budget to rediscover it, and
	// registers a route it did not need — measured at ~18s per Connect against
	// 192.168.3.224, against ~18ms once the answer can be heard.
	if !sess.peerFallbackDisabled && isKnownPeerRouteHost(sess.peerRouteCacheKey()) {
		if err := sess.startPeerListener(); err != nil {
			// Warn, not Debug: this names a port conflict an operator has to resolve
			// (usually a local TwinCAT router owning 48898), and swallowing it is
			// what made the same failure invisible before.
			sess.logger.Warn("could not bind the inbound AMS port for a device known to need it; requests may all time out",
				"host", sess.ip, "error", err,
				"hint", "pass WithAmsPeerListen(port) to use a different port, or WithoutAmsPeerFallback() to opt out")
		} else {
			sess.logger.Info("device is known to answer on its own connection; listening before probing", "host", sess.ip)
		}
	}
	if local {
		resp, err := newClient.send([]byte{0, 16, 2, 0, 0, 0, 0, 0})
		if err != nil {
			sess.tearDownAndReset()
			return fmt.Errorf("local mode handshake failed: %w", err)
		}
		buf := bytes.NewBuffer(resp)
		result := AMSAddress{}
		sess.logger.Log(context.Background(), LevelTrace, "got stuff", "stuff", buf.Bytes())
		err = binary.Read(buf, binary.LittleEndian, &result)
		if err != nil {
			sess.tearDownAndReset()
			return fmt.Errorf("local mode binary read failed: %w", err)
		}
		sess.logger.Info("local mode handshake result", "result", result)
		sess.tx.connMu.Lock()
		sess.source = result
		sess.tx.connMu.Unlock()
		// The Client was published before the handshake could run, holding a copy of
		// the pre-handshake address; without this every later request goes out with
		// the placeholder rather than what the router just told us to use.
		newClient.setSource(result)
	}

	// A successful route-activation probe already carries the symbol version, so
	// track whether we have one and skip the duplicate read further down.
	var (
		symbolVersion uint8
		haveVersion   bool
	)
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
			//
			// Tear down first: a Client is already published on an open socket, and
			// the deferred re-arm has restored onDrop. Returning without this leaves
			// workers running on a transport the caller believes is dead, and a
			// legal retry from Disconnected would publish a second Client onto the
			// same shared tx.
			sess.tearDownAndReset()
			return fmt.Errorf("route registration failed during connect: %w", err)
		}
		if registered {
			// TCP reconnect — PLC may reset connections from previously-unknown NetIDs.
			// Shut down goroutines, close TCP, redial, restart.
			sess.tearDownAndReset()
			sess.lifecycle.reconnectAttempts.Add(1)
			if err := sess.dialAndStart(); err != nil {
				return fmt.Errorf("TCP reconnect after route registration failed: %w", err)
			}
			sess.logger.Info("TCP reconnected after route registration")
			// Don't report a working session until the PLC actually serves the
			// new route. Returning here with the router still catching up hands
			// the caller a Connect that succeeded and a session where every
			// command times out.
			// Connect's own ctx: no teardown replaces it, so passing it for every
			// attempt is safe. See awaitRouteActive on why this is a function.
			probedVersion, err := sess.awaitRouteActive(func() context.Context { return ctx })
			if err != nil {
				// A device that answers on a connection IT opens to us cannot pass
				// this probe, however healthy the route is: our socket stays silent
				// because the reply goes to the other one. Measured on TC3.1.4026
				// (192.168.3.224), which does exactly that. Try the fallback before
				// condemning the route — this is what the peer listener is for, and
				// until now the route branch returned before it could ever run.
				if errors.Is(err, ErrRuntimeNotRunning) {
					// The route works; the runtime does not. Come up and wait — the
					// state poll and the gates on the symbol calls carry it from here.
					haveVersion = false
				} else if rescued, ferr := sess.tryPeerFallback(ctx); rescued {
					rememberPeerRouteHost(sess.peerRouteCacheKey())
					sess.logger.Warn("route probe was silent, but the PLC answers on a connection it opens to us; continuing",
						"host", sess.ip)
					haveVersion = false
				} else {
					if ferr != nil {
						err = fmt.Errorf("%w (the peer-route fallback could not run: %w)", err, ferr)
					}
					// The redial published a Client, so this path owns tearing it down.
					sess.tearDownAndReset()
					return fmt.Errorf("route registration during connect: %w", err)
				}
			} else {
				// The ordinary probe worked, so this device does not need the inbound
				// listener any more (route table repaired, or it never did). The
				// remembered fact is dropped for every path at the end of Connect —
				// see forgetPeerRouteHostIfUnused — rather than only for this one.
				//
				// The winning probe WAS a GetSymbolVersion, so seed the cache from it
				// instead of issuing the identical request again below.
				haveVersion, symbolVersion = true, probedVersion
			}
		}

	}

	// Read symbol version for later change detection. Failing to read it is not
	// fatal — a TC2 target can refuse this one service while being perfectly
	// alive — but total SILENCE is: a PLC that accepts the socket and answers
	// nothing must not yield a Session that reports Connected.
	//
	// That state is real and long-lived: measured on a TC/RTOS device, Connect
	// returned nil in 5.01s, IsClosed() stayed false, and every request timed out
	// afterwards. The liveness check used to live only inside the
	// route-registration branch above, so a caller with a pre-registered route got
	// no check at all.
	if !haveVersion {
		symbolVersion, err = sess.client.Load().GetSymbolVersion(ctx)
		haveVersion = err == nil
		switch {
		case err == nil:
		case errors.Is(err, ErrTransportClosed):
			// The link died while we were connecting. Reporting success here hands
			// the caller a Session whose transport is already gone.
			sess.tearDownAndReset()
			sess.transitionState(SessionStateDisconnected)
			return fmt.Errorf("transport dropped during connect to %s: %w", sess.ip, err)
		case isUnservedError(err):
			// Second opinion before condemning the link: ReadState is the most
			// universally supported service there is, so if THAT is also met with
			// silence the device is not talking to us at all.
			if _, serr := sess.client.Load().ReadState(ctx); isUnservedError(serr) {
				// Total silence. Before giving up: the device may be answering on a
				// connection it opens to us rather than on ours.
				rescued, ferr := sess.tryPeerFallback(ctx)
				if rescued {
					rememberPeerRouteHost(sess.peerRouteCacheKey())
					haveVersion = false // the probe above never got a value
					break
				}
				sess.tearDownAndReset()
				sess.transitionState(SessionStateDisconnected)
				hint := "a stale or duplicate route entry for source NetID " + sess.sourceAddr().NetIDString() +
					", or another client using this IP, can hold a TwinCAT router in this state"
				if ferr != nil {
					hint += "; the peer-route fallback could not run: " + ferr.Error()
				}
				return fmt.Errorf("PLC at %s accepted the connection but answered neither GetSymbolVersion nor ReadState: %w (%s)",
					sess.ip, err, hint)
			}
			sess.logger.Debug("could not read symbol version during connect, but the PLC is answering", "error", err)
		default:
			sess.logger.Debug("could not read symbol version during connect", "error", err)
		}
	}
	if haveVersion {
		sess.cache.lock.Lock()
		sess.cache.symbolVersion = symbolVersion
		sess.cache.lock.Unlock()
	}
	// Poll the system service for the runtime state from here on. Connect itself
	// deliberately still succeeds against a runtime in CONFIG — the session is
	// usable, it just has no runtime to talk to yet — and the poll is what lets the
	// symbol and subscription calls say so instead of failing obscurely.
	sess.startRuntimeStateWatch()
	// One synchronous read before returning, so the very first LoadSymbols or
	// subscribe already has evidence. Waiting for the poller's first tick would
	// leave that call to fail the old obscure way — measured on a PLC in CONFIG:
	// "ADS error in Read: 0xF008", which is an index group, not a return code.
	if state, serr := sess.runtimeStateQuietly(ctx); serr != nil {
		sess.logger.Debug("could not read the runtime state from the system service at connect", "error", serr)
	} else if state != ADSStateRun {
		sess.logger.Warn("connected, but the PLC runtime is not in RUN: symbol and subscription calls will refuse until it returns",
			"state", uint16(state), "detail", "in CONFIG the runtime port does not exist, so those calls cannot succeed")
	}
	// This Connect worked. If it never heard from the device on a connection the
	// device opened, that device does not need the inbound listener, whatever an
	// earlier session concluded. Deliberately here rather than in the branches: only
	// a Connect that is about to report success has earned the right to overwrite
	// what a previous one learned.
	sess.forgetPeerRouteHostIfUnused()
	sess.enterConnected()
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

	// Disarm ondrop for the entire ensureRouteOnConnect call. PLC RST
	// during the probe (typical when the route is missing or stale) would
	// otherwise fire sess.triggerReconnect via the listen goroutine's
	// callOnDrop, spawning a Reconnect goroutine that races our own
	// AddRoute/redial path on sess.client / tx.connection / lifecycle.ctx
	// (observed as concurrent "registering route" + "FSM invalid
	// transition" log noise during cold-start when the PLC route doesn't
	// yet match the current source IP). Re-armed at the end of the
	// function via defer; intermediate dialAndStart calls in the retry
	// path also re-arm on each new Client they create, but we override
	// those back to nil for the duration of this routine.
	// beginHandshake rides along with the ondrop disarm: a probe that times out
	// or gets RST is the expected first step of the cold-start flow, so it must
	// not surface as ERROR. See Client.beginHandshake.
	if oldClient := sess.client.Load(); oldClient != nil {
		oldClient.SetOnDrop(nil)
		oldClient.beginHandshake()
	}
	defer func() {
		if c := sess.client.Load(); c != nil {
			c.endHandshake()
			c.SetOnDrop(sess.triggerReconnect)
		}
	}()

	// Force mode → always register
	if sess.route.forceRouteRegistration {
		sess.logger.Info("registering route (force mode)")
		err = sess.AddRoute(ctx, sess.route.name, sess.route.username, string(sess.route.password))
		if err == nil {
			sess.route.markRegistered()
		}
		return err == nil, err
	}

	// First probe attempt
	_, probeErr := sess.probeRouteVersion(ctx)
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
		// ctx-aware sleep: honor caller cancellation. Plain time.Sleep
		// would block the full delay even if the caller has given up.
		select {
		case <-time.After(routeProbeRetryDelay):
		case <-ctx.Done():
			return false, fmt.Errorf("route probe retry aborted: %w", ctx.Err())
		}
		sess.tearDownAndReset()
		if dialErr := sess.dialAndStart(); dialErr != nil {
			return false, fmt.Errorf("redial during route probe retry: %w", dialErr)
		}
		// dialAndStart re-armed ondrop on the new Client; disarm again
		// for the remainder of ensureRouteOnConnect (the deferred
		// re-arm at function exit restores the production handler).
		if c := sess.client.Load(); c != nil {
			c.SetOnDrop(nil)
			c.beginHandshake()
		}
		if sess.isClosed() {
			return false, fmt.Errorf("connection closed during route probe retry")
		}
		_, retryErr := sess.probeRouteVersion(ctx)
		if retryErr == nil {
			sess.logger.Info("route already exists on PLC (confirmed after retry)")
			sess.route.routeProbeFailures.Store(0)
			return false, nil
		}
		probeErr = fmt.Errorf("probe failed after retry: %w", retryErr)
	}

	// Definite probe failure → register, unless this session did so recently.
	sess.route.routeProbeFailures.Add(1)
	if !sess.route.mayRegister() {
		sess.logger.Info("route probe failed but this session already registered the route; not registering again",
			"error", probeErr)
		return false, nil
	}
	sess.logger.Info("route probe failed, registering route", "error", probeErr)
	err = sess.AddRoute(ctx, sess.route.name, sess.route.username, string(sess.route.password))
	if err == nil {
		sess.route.markRegistered()
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Route activation: AddRoute is a UDP call to the PLC's AMS router, and the
// router acknowledges the entry before it is necessarily serving it. Until it
// does, ADS requests for our NetID are dropped with no reply at all — not an
// error code, silence. Observed on TC/RTOS 3.1.4026 (192.168.3.224) as a
// Connect that returned success followed by a session where ReadState,
// ReadDeviceInfo and LoadSymbols all timed out; the route worked on the next
// process start. Re-probe until the router answers so Connect either hands
// back a session that works or fails honestly.
// The default ceiling is generous on purpose: the wait ends on the first
// successful probe, so the only case that pays it is a route that never comes
// live, where taking 10s to report an honest failure beats reporting success.
// Override with WithRouteActivationTimeout.
const (
	defaultRouteActivationTimeout = 10 * time.Second
	routeActivationPollDelay      = 250 * time.Millisecond
	minRouteActivationProbe       = 500 * time.Millisecond
	maxRouteActivationProbe       = 2 * time.Second
)

// routeActivationBudget returns the total wait and the per-probe timeout
// derived from it. Deriving the probe timeout keeps a shortened total
// coherent — a fixed probe timeout larger than the total would allow exactly
// one attempt, and that attempt would overrun the budget.
func (sess *Session) routeActivationBudget() (total, probe time.Duration) {
	total = sess.route.activationTimeout
	if total <= 0 {
		total = defaultRouteActivationTimeout
	}
	probe = total / 4
	if probe < minRouteActivationProbe {
		probe = minRouteActivationProbe
	}
	if probe > maxRouteActivationProbe {
		probe = maxRouteActivationProbe
	}
	return total, probe
}

// effectiveRouterPort is the UDP router port for this session, falling back to
// the protocol default for a Session built without going through NewSession
// (test helpers do this).
func (sess *Session) effectiveRouterPort() int {
	if sess.routerPort > 0 {
		return sess.routerPort
	}
	return routePort
}

// currentLifecycleCtx returns the live lifecycle context.
//
// tearDownAndReset cancels the old context and installs a fresh one under
// ctxMu, so anything spanning a teardown must re-read it rather than capture
// it — a captured one is cancelled out from underneath the caller. Reading it
// bare also races the replacement.
func (sess *Session) currentLifecycleCtx() context.Context {
	sess.lifecycle.ctxMu.RLock()
	defer sess.lifecycle.ctxMu.RUnlock()
	return sess.lifecycle.ctx
}

// awaitRouteActive re-probes the PLC after a route registration until one
// probe round-trips, redialing when the PLC drops the connection mid-probe
// (it does that for a source NetID it does not serve yet, so a redial is part
// of waiting rather than a failure). It returns the symbol version the winning
// probe read, so the caller need not ask again.
//
// ctxFor supplies the context for each attempt and is called fresh every time,
// which is the whole reason it is a function: this routine's own redial calls
// tearDownAndReset, which cancels and replaces lifecycle.ctx. A caller on the
// reconnect path that passed lifecycle.ctx by value would have the loop cancel
// its own context on the first redial — every later probe born already dead,
// reported as caller cancellation. Connect passes its own caller context, which
// no teardown touches; the reconnect path passes sess.currentLifecycleCtx.
//
// Call immediately after a successful AddRoute. ondrop is disarmed for the
// duration so a PLC-initiated RST cannot spawn a competing Reconnect
// goroutine while we own the transport — same rationale as
// ensureRouteOnConnect. Client.beginHandshake keeps the expected faults off
// the ERROR channel while we are still working.
func (sess *Session) awaitRouteActive(ctxFor func() context.Context) (uint8, error) {
	if c := sess.client.Load(); c != nil {
		c.SetOnDrop(nil)
		c.beginHandshake()
	}
	defer func() {
		if c := sess.client.Load(); c != nil {
			c.endHandshake()
			c.SetOnDrop(sess.triggerReconnect)
		}
	}()

	total, probeTimeout := sess.routeActivationBudget()
	deadline := time.Now().Add(total)
	var lastErr error
	for attempt := 1; ; attempt++ {
		if sess.isClosed() {
			return 0, fmt.Errorf("connection closed while waiting for route activation")
		}
		ctx := ctxFor()
		probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
		version, err := sess.probeRouteVersion(probeCtx)
		cancel()
		lastErr = err
		if lastErr == nil {
			sess.route.routeProbeFailures.Store(0)
			if attempt > 1 {
				sess.logger.Info("route active after registration", "probeAttempts", attempt)
			}
			return version, nil
		}
		// The probe reads the symbol version, which lives on the RUNTIME port — and
		// that port does not exist while the system is in CONFIG (measured: every
		// request answered with AMS ErrorCode 6). The route is not the problem there,
		// so ask the system service, which stays up: if it answers, the route is
		// demonstrably being served and the only thing missing is a running runtime.
		// Without this a session that starts while the PLC is in CONFIG dies here
		// instead of coming up and waiting, which is the whole point of the gates on
		// the symbol and subscription calls.
		if c := sess.client.Load(); c != nil {
			stateCtx, stateCancel := context.WithTimeout(ctxFor(), probeTimeout)
			state, serr := c.ReadStateOnPort(stateCtx, PortSystemService)
			stateCancel()
			if serr == nil {
				sess.recordRuntimeState(state.ADSState)
				if state.ADSState != ADSStateRun {
					sess.route.routeProbeFailures.Store(0)
					sess.logger.Warn("route is active but the PLC runtime is not in RUN; connecting anyway and waiting for it",
						"state", uint16(state.ADSState), "probeAttempts", attempt)
					// ErrRuntimeNotRunning, not (0, nil): the route is proven but no
					// version was read, and reporting success with version 0 made the
					// caller record 0 as the real symbol version — which disables
					// online-change detection (consumeHeartbeat requires known != 0)
					// and skips the liveness block that would otherwise run.
					return 0, fmt.Errorf("route is served but %w (ADS state %d)", ErrRuntimeNotRunning, uint16(state.ADSState))
				}
			}
		}
		if !time.Now().Before(deadline) {
			break
		}
		sess.logger.Debug("route not served by PLC yet, re-probing",
			"attempt", attempt, "error", lastErr, "delay", routeActivationPollDelay)
		if isProbeRetryable(lastErr) {
			sess.tearDownAndReset()
			if dialErr := sess.dialAndStart(); dialErr != nil {
				return 0, fmt.Errorf("redial while waiting for route activation: %w", dialErr)
			}
			// dialAndStart armed ondrop on the new Client; disarm again for the
			// rest of the wait (the deferred restore handles function exit).
			if c := sess.client.Load(); c != nil {
				c.SetOnDrop(nil)
				c.beginHandshake()
			}
		}
		select {
		case <-time.After(routeActivationPollDelay):
		case <-ctxFor().Done():
			return 0, fmt.Errorf("route activation wait aborted: %w", ctxFor().Err())
		}
	}
	return 0, fmt.Errorf("route %q was registered but the PLC did not serve it within %v: %w",
		sess.route.name, total, lastErr)
}

// probeRouteVersion sends a lightweight ADS command (GetSymbolVersion) to verify
// the PLC accepts our source NetID, returning the version it read.
//
// The route is proven by the round-trip succeeding at all; the version is a free
// by-product, so a caller that needs it can skip a second identical round-trip.
func (sess *Session) probeRouteVersion(ctx context.Context) (uint8, error) {
	return sess.client.Load().GetSymbolVersion(ctx)
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
	if err := sess.Close(); err != nil {
		sess.logger.Warn("Close error during stale-detection shutdown", "err", err)
	}
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
	// Transport is alive here, so quiesce dispatch: samples still arriving for the
	// handles being deleted are race-window noise, not orphans.
	oldHandles := sess.takeNotificationHandles(true)
	sess.releaseNotificationHandles(sess.currentLifecycleCtx(), oldHandles, "auto-reload before resubscribe")

	if err := sess.LoadSymbols(sess.currentLifecycleCtx()); err != nil {
		return fmt.Errorf("LoadSymbols: %w", err)
	}
	return sess.resubscribeNotifications()
}

// trackGoroutine registers a background goroutine with the session's WaitGroup and
// starts it, refusing once the session is closed. Reports whether it started.
//
// Callers that hold resources for the goroutine (a semaphore slot, a throttle
// entry) must release them when this returns false.
func (sess *Session) trackGoroutine(fn func()) bool {
	return sess.trackGoroutineOn(&sess.lifecycle.waitGroup, fn)
}

// trackGoroutineOn is trackGoroutine against a specific WaitGroup.
//
// Choose it by lifetime, and get this wrong and reconnect deadlocks:
// lifecycle.waitGroup is waited by tearDownAndReset on EVERY reconnect, so only
// goroutines that finish on their own belong there (an orphan delete). Anything
// that lives as long as the session — the peer accept loop, the heartbeat watcher,
// the runtime-state watch — needs its own group, waited only by Close.
func (sess *Session) trackGoroutineOn(wg *sync.WaitGroup, fn func()) bool {
	sess.lifecycle.spawnMu.Lock()
	defer sess.lifecycle.spawnMu.Unlock()
	// closedCh, not isClosed(): isClosed() reads the FSM, and closedCh is closed
	// first (and by paths like giveUpReconnecting that signal shutdown before the
	// state settles). The earliest signal is the one that has to gate the Add.
	select {
	case <-sess.lifecycle.closedCh:
		return false
	default:
	}
	if sess.isClosed() {
		return false
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		fn()
	}()
	return true
}

// admitBackgroundWork reports whether work that touches PLC state may still start.
//
// Shares spawnMu with markClosed and trackGoroutineOn, so "the session is not
// closed" and "we have begun" are one decision rather than two — a bare isClosed()
// check leaves a window in which Close can complete its PLC-side release and the
// work then re-registers handles nothing will ever delete.
func (sess *Session) admitBackgroundWork() bool {
	sess.lifecycle.spawnMu.Lock()
	defer sess.lifecycle.spawnMu.Unlock()
	select {
	case <-sess.lifecycle.closedCh:
		return false
	default:
	}
	return !sess.isClosed()
}

// markClosed closes the closedCh signal channel exactly once. Safe for
// concurrent invocation from Close() and from Reconnect-exhaustion path.
func (sess *Session) markClosed() {
	// Under spawnMu so it pairs with trackGoroutine: once this returns, every
	// subsequent registration attempt sees the session closed and declines, so the
	// Wait that follows cannot race an Add.
	sess.lifecycle.spawnMu.Lock()
	defer sess.lifecycle.spawnMu.Unlock()
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
	// Terminal, so no need to quiesce dispatch. Takes the heartbeat with it, which
	// is why Close no longer releases that separately.
	handles := sess.takeNotificationHandles(false)
	if len(handles) > 0 {
		deleted := sess.bestEffortDeleteNotifications(sess.currentLifecycleCtx(), handles)
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
		if err := sess.client.Load().Write(sess.currentLifecycleCtx(), uint32(GroupSymbolReleaseHandle), 0, handleBytes); err != nil {
			sess.logger.Warn("failed to release symbol handle", "error", err, "handle", h)
		} else {
			sess.logger.Info("handle deleted", "handle", h)
		}
	}
}

// shutdownTransport performs the terminal half of teardown that never blocks:
// PLC-side resource release, peer listener stop, context cancel, socket close and
// peer-connection close. Everything here is what makes the workers return on their
// own, so a caller that cannot wait for them (giveUpReconnecting runs *inside* the
// Reconnect goroutine, and Close's waits include reconnectDone) still leaves nothing
// behind.
//
// Runs exactly once per session, whichever path gets here first.
func (sess *Session) shutdownTransport(wasDisconnected bool) {
	sess.lifecycle.shutdownOnce.Do(func() {
		// Stop accepting inbound PLC connections before tearing the transport down,
		// so the accept loop cannot hand one to a Client that is going away.
		sess.stopPeerListener()
		// releasePLCResources collects the heartbeat along with the caller's handles
		// (see takeNotificationHandles), so it does not need releasing separately.
		sess.releasePLCResources(wasDisconnected)
		// Capture cancel under RLock then release before invoking — see
		// tearDownAndReset for the symmetric pattern. Holding RLock across the
		// cancel() blocks tearDownAndReset's ctxMu.Lock replacement.
		sess.lifecycle.ctxMu.RLock()
		cancel := sess.lifecycle.shutdown
		sess.lifecycle.ctxMu.RUnlock()
		if cancel != nil {
			cancel()
		}
		// Close the TCP connection to unblock listen(), which may be stuck in ReadFull.
		sess.tx.connMu.Lock()
		if sess.tx.connection != nil {
			_ = sess.tx.connection.Close()
		}
		sess.tx.connMu.Unlock()
		if c := sess.client.Load(); c != nil {
			c.markDropped() // same reason as in tearDownAndReset
			// Adopted inbound connections have their own readers; closing the sockets
			// is what lets those readers return.
			c.closePeerConns()
		}
	})
}

// Close releases PLC-side notification subscriptions, releases the cached
// PLC-side symbol handles when transport is still alive, cancels the
// session context, closes the underlying TCP socket, and waits for the
// listen + transmit + recv workers to exit. Idempotent — the teardown itself
// runs once (shutdownTransport), and a repeat call only re-waits on workers
// that have already finished.
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
	// Deliberately NOT gated on winning this transition. giveUpReconnecting may
	// already have moved the FSM to Closed, and returning early there left the
	// socket, the 48898 listener and every worker in place: the caller's Close is
	// the only place that can wait for those workers, because giveUpReconnecting
	// runs inside the very goroutine this function waits for.
	sess.lifecycle.state.transitionToOnce(SessionStateClosed)
	sess.markClosed()
	sess.logger.Info("Close called, shutting down")
	sess.shutdownTransport(wasDisconnected)
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
	// The heartbeat watcher is the one goroutine on these paths whose exit Close did
	// not observe: heartbeatWG was Add'ed and Done'd but never waited, so Close
	// could return while the watcher was still inside a recovery. Waited after the
	// cancel and the socket close above, so anything it has in flight aborts rather
	// than holding this up.
	sess.heartbeatWG.Wait()
	sess.stateWG.Wait()
	sess.logger.Info("Waiting for workers to close")
	if c := sess.client.Load(); c != nil {
		// Repeated from shutdownTransport on purpose: a reconnect in flight when the
		// teardown ran may have swapped in a different *Client, and waiting on one
		// whose sockets are still open never returns.
		c.markDropped()
		c.closePeerConns()
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
func (sess *Session) reconnectSleep(ctx context.Context, attempt int) error {
	delay := sess.reconnectBackoff(attempt)
	sess.logger.Info("reconnect backoff", "attempt", attempt, "delay", delay)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		// The caller gave up mid-backoff. Give-up is terminal for the session —
		// see Reconnect's doc.
		return sess.giveUpReconnecting(fmt.Errorf("reconnect aborted during backoff: %w", ctx.Err()))
	case <-sess.lifecycle.closedCh:
		return fmt.Errorf("connection closed during reconnect")
	}
}

// unservedCooldownDuration is the configured quiet period, or the default.
func (sess *Session) unservedCooldownDuration() time.Duration {
	if d := sess.lifecycle.unservedCooldown; d > 0 {
		return d
	}
	return defaultUnservedCooldown
}

// reconnectAttemptsForTest exposes the dial counter to tests in this package.
func (l *sessionLifecycle) reconnectAttemptsForTest() int64 {
	return l.reconnectAttempts.Load()
}

// isDeviceAnswer reports whether err carries an answer from the far side — an
// ADS return code from the device, or a rejection from its AMS router — as
// opposed to silence (a timeout, a dead link, a cancelled context), which says
// nothing about what is or is not there.
//
// A router rejection is an answer, and for a port that does not exist it is the
// most direct evidence there is: a request to an absent port comes back with AMS
// ErrorCode 0x06 (defs.go). AMSError deliberately does not unwrap to ReturnCode,
// so both cases have to be asked about separately.
func isDeviceAnswer(err error) bool {
	var rc ReturnCode
	var amsErr AMSError
	return errors.As(err, &rc) || errors.As(err, &amsErr)
}

// isUnservedError reports whether err means "the PLC accepted our connection and
// then said nothing", as opposed to a refused dial or a PLC-side verdict. A
// timeout with no ADS return code is the signature: the request went out and
// nothing came back.
func isUnservedError(err error) bool {
	if err == nil {
		return false
	}
	var rc ReturnCode
	if errors.As(err, &rc) {
		return false // the PLC answered, even if the answer was an error
	}
	// ErrTransportClosed means the link died under us, which is a drop rather
	// than a refusal to serve — only a plain deadline counts here.
	return errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrTransportClosed)
}

// coolDownAfterUnserved holds the loop off with nothing open, so the router can
// settle. Returns an error only if the session is closed or the caller gave up
// while waiting.
func (sess *Session) coolDownAfterUnserved(ctx context.Context, attempts int, cause error) error {
	d := sess.unservedCooldownDuration()
	sess.logger.Warn("PLC accepted the connection but answered nothing; backing off completely before trying again",
		"unservedAttempts", attempts, "cooldown", d, "error", cause,
		"hint", "a stale or duplicate route entry for this source NetID, or another client on this IP, can hold a TwinCAT router in this state")
	// Nothing open while we wait: the point is to stop competing for the
	// router's one-connection-per-IP slot.
	sess.tearDownAndReset()

	// Permit one route registration on the next attempt. Re-registering the
	// correct route is the measured recovery for a router that has stopped
	// answering — two TC3 devices mute, both restored by exactly this — so a
	// session that has concluded the PLC is silent should be allowed to try it.
	if !sess.route.shouldSkip() {
		sess.route.allowHealingRegistration()
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return sess.giveUpReconnecting(fmt.Errorf("reconnect aborted during unserved cooldown: %w", ctx.Err()))
	case <-sess.lifecycle.closedCh:
		return fmt.Errorf("connection closed during unserved cooldown")
	}
}

// giveUpReconnecting ends a reconnect for good: the FSM goes to Closed, PLC-side
// resources are released by whoever wins that transition, and closedCh is closed
// so waiters unblock. Shared by attempt exhaustion and caller cancellation, which
// differ only in the error they report.
//
// Closing rather than parking in Reconnecting is the whole point: a consumer's
// only liveness signal is IsClosed(), so "gave up but still alive" is a state it
// can neither observe nor recover from.
func (sess *Session) giveUpReconnecting(cause error) error {
	sess.lifecycle.state.transitionToOnce(SessionStateClosed)
	// Idempotent via closedOnce, whichever of Close() and this ran first.
	sess.markClosed()
	// Full teardown, not just the PLC-side release: this path is reachable without
	// the user ever calling Close (WithMaxReconnectAttempts, a cancelled Reconnect),
	// and leaving the socket and the 48898 listener open then leaked them for the
	// life of the process. wasDisconnected=true: the transport is gone by
	// definition once we give up on it.
	//
	// The blocking half of Close's teardown stays in Close. This runs inside the
	// Reconnect goroutine, so waiting for reconnectDone here would deadlock — but
	// the workers need no waiting to exit, only the cancel and the socket close
	// that shutdownTransport performs.
	sess.shutdownTransport(true)
	return cause
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
	// Fire disconnect callback in goroutine (must not block).
	// Callback must not call Session methods — connection may be closing.
	if firstDetector && sess.onDisconnect != nil && !sess.isClosed() {
		go sess.onDisconnect()
	}

	// Connect owns the transport end to end; a rival Reconnect must not tear it
	// down under it (see lifecycle.connecting). The drop is already recorded in
	// tx.disconnected, so Connect's own error handling — or its exit adopt —
	// schedules the redial.
	//
	// Placement is load-bearing at both ends:
	//
	//   - Above the CAS, the drop is never recorded and the exit adopt has nothing
	//     to see, so the drop is lost rather than deferred.
	//   - Above the callback dispatch — which is where the obvious reading of
	//     "immediately after the CAS" puts it — WithOnDisconnect never fires for a
	//     drop during Connect. The callback is gated on firstDetector, so the exit
	//     adopt's second pass, finding the CAS already lost, cannot make up for it:
	//     a consumer whose only drop signal is the callback gets none at all.
	//
	// So: after the callback, before the reconnectDone channel. Returning before
	// the channel exists is deliberate too — it is closed only by Reconnect, and
	// the Reconnect this would have spawned is exactly what is being suppressed,
	// so creating it here would leave Session.Close's unconditional wait on it
	// hanging forever (verified: the package times out) and park the symbol
	// helpers' waitForReconnect. The exit adopt re-enters here with the flag
	// clear and creates it then.
	if sess.lifecycle.connecting.Load() {
		return
	}

	sess.lifecycle.reconnectMu.Lock()
	if sess.lifecycle.reconnectDone == nil {
		sess.lifecycle.reconnectDone = make(chan struct{})
	}
	sess.lifecycle.reconnectMu.Unlock()

	if sess.lifecycle.autoReconnect {
		// Background, not the lifecycle context: the auto path must not treat a
		// session-context replacement as "the caller gave up", which now closes
		// the session. Cancellation of an auto-reconnect is Close's job.
		go func() { _ = sess.Reconnect(context.Background()) }()
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

// unservedAttemptsBeforeCooldown is how many consecutive dial-succeeds-then-
// silence attempts are tolerated before the loop goes quiet. Small on purpose: a
// PLC that accepts and does not answer will not change its mind within a few
// hundred milliseconds, and every extra dial makes a router livelock worse.
const unservedAttemptsBeforeCooldown = 3

// defaultUnservedCooldown is long enough for the router to finish whatever it is
// doing with our IP, and short enough that a session recovers on its own. The
// field evidence is "it works again after you stop trying for a bit"; this is that
// pause, made deliberate.
const defaultUnservedCooldown = 30 * time.Second

// preReconnectReleaseAttempts caps how many reconnect attempts will re-try the
// best-effort delete of the handles held before the drop. Retrying matters — the
// link is usually still down on the first attempt, so the delete cannot land —
// but reconnect attempts are unbounded by default, so an unreleasable handle must
// not buy a round trip on every one of them forever. The orphan reaper is the
// backstop after that.
const preReconnectReleaseAttempts = 3

// Reconnect attempts to re-establish the TCP connection, reload symbols,
// and re-subscribe to previously registered notifications.
// Uses configurable backoff (see WithBackoff) with fast initial retries and
// progressive slowdown. Backoff resets on each successful reconnect.
//
// Cancelling ctx gives up and CLOSES the session, exactly as exhausting
// WithMaxReconnectAttempts does. It is not a pause: Reconnecting has no exit to
// Disconnected (see the FSM table), and a session left there is invisible to a
// consumer that polls IsClosed() to decide when to rebuild — no data would flow
// and nothing would ever retry. So cancellation means "this session is done",
// and PLC-side resources are released on the way out.
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
	// Single-flight on explicit ownership, not on the FSM state: see
	// sessionLifecycle.reconnectOwner for why the state cannot serve as the gate.
	if !sess.lifecycle.reconnectOwner.CompareAndSwap(false, true) {
		sess.logger.Info("reconnect already in progress, skipping")
		return nil
	}
	// One defer for the whole hand-off, in this order on purpose.
	//
	// The tail of a successful reconnect marks the transport live and goes
	// Connected while this ownership flag is still held. A drop landing in that
	// gap is seen by triggerReconnect, which takes the session to Disconnected and
	// spawns a Reconnect — and that one loses the CAS above and returns. So the
	// drop is acknowledged by the FSM and then abandoned: nothing owns a retry,
	// and heartbeatWatch skips while disconnected. The session sits Disconnected
	// with IsClosed() false forever, which is the one state a consumer polling
	// IsClosed() cannot see.
	//
	// Releasing ownership is therefore not enough; whoever releases it has to look
	// for a drop that arrived while it was finishing and adopt it. Closing
	// reconnectDone first keeps Close()'s unconditional wait on that channel from
	// hanging on an orphan triggerReconnect created in the same gap.
	defer func() {
		closeReconnectDone()
		sess.lifecycle.reconnectOwner.Store(false)
		if sess.lifecycle.autoReconnect && !sess.isClosed() && sess.tx.disconnected.Load() {
			sess.logger.Info("adopting a drop that arrived while the previous reconnect was finishing")
			go func() { _ = sess.Reconnect(context.Background()) }()
		}
	}()

	// transitionToOnce reports ok=false both for an illegal transition and for
	// "already in that state". Only the first is a refusal: we hold the ownership
	// flag, so an existing Reconnecting state has no live owner and is ours to
	// take over.
	if from, ok := sess.lifecycle.state.transitionToOnce(SessionStateReconnecting); !ok && from != SessionStateReconnecting {
		sess.logger.Info("reconnect not permitted from the current state, skipping", "state", from)
		// The deferred hand-off closes reconnectDone on this path too. Close()
		// waits on that channel with no timeout and no closedCh alternative, so
		// leaving an orphan open here is a hang whether or not we are closed.
		return nil
	} else if !ok {
		sess.logger.Warn("taking over a reconnect state left behind by an earlier attempt")
	}

	// Create a channel that waiters (sendRequest) can block on.
	// triggerReconnect() may have already created it — only create if nil.
	sess.lifecycle.reconnectMu.Lock()
	if sess.lifecycle.reconnectDone == nil {
		sess.lifecycle.reconnectDone = make(chan struct{})
	}
	sess.lifecycle.reconnectMu.Unlock()

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
		case <-ctx.Done():
			timer.Stop()
			return sess.giveUpReconnecting(fmt.Errorf("reconnect aborted during flap cooldown: %w", ctx.Err()))
		case <-sess.lifecycle.closedCh:
			timer.Stop()
			return fmt.Errorf("connection closed during flap cooldown")
		}
	}

	sess.logger.Info("attempting reconnect")
	sess.tx.disconnected.Store(true)
	// State is already Reconnecting (transitionToOnce above).

	// Clear active notifications (old handles are never reused after a reconnect)
	// but snapshot the handle list first, because the PLC may still hold those
	// registrations.
	//
	// Measured 2026-08-23 with a proxy severing the link, same source AmsAddr on
	// both sides of the outage:
	//   - TC3.1.4024/CE, silent loss (the PLC never saw a FIN): the handles are
	//     ALIVE and start streaming to the new connection. Deleting them returns
	//     0x0 — real registrations.
	//   - Any device, clean disconnect (FIN reached the PLC): already invalidated,
	//     0x714 or 0x715.
	//   - TC2 2.10: gone within 2s either way; the delete is a formality.
	// So the delete below is load bearing exactly in the field case — cable,
	// switch, NAT idle-out — where nothing told the PLC we left. Without it a
	// flapping session accumulates handle slots and eventually crashes the
	// TwinCAT AMS router (Beckhoff issue #268), and the stale handles stream
	// alongside the new ones until the orphan reaper catches them.
	//
	// Note when reading the log: bestEffortDelete counts 0x714/0x715 as
	// success-equivalent, so "deleted=N" does not prove N registrations existed.
	//
	// No dispatch quiescing: the transport is about to be torn down, so nothing
	// more can arrive on it. The heartbeat comes along in this snapshot, which is
	// what lets establishHeartbeat register a fresh one after the resubscribe.
	savedHandles := sess.takeNotificationHandles(false)

	sess.tearDownAndReset()

	var lastErr error
	attempts := 0
	releaseTries := 0
	unserved := 0

	// retryAfter handles every post-dial failure identically: record it, tear the
	// half-built connection down, then either wait out the backoff or — when the
	// PLC has accepted us and then answered nothing several times running — go
	// quiet altogether. Returning a non-nil error means the loop must exit.
	//
	// One helper rather than a copy per step, because the previous four copies
	// were what let the unserved case be handled in one place and missed in the
	// others.
	retryAfter := func(err error, stage string) error {
		lastErr = err
		sess.logger.Warn("reconnect step failed, retrying",
			"stage", stage, "error", err, "attempt", attempts)
		sess.resetForRetry()
		if isUnservedError(err) {
			unserved++
			if unserved >= unservedAttemptsBeforeCooldown {
				unserved = 0
				return sess.coolDownAfterUnserved(ctx, unservedAttemptsBeforeCooldown, err)
			}
		} else {
			unserved = 0
		}
		return sess.reconnectSleep(ctx, attempts)
	}

	for {
		if sess.isClosed() {
			return fmt.Errorf("connection closed during reconnect")
		}
		attempts++
		if err := ctx.Err(); err != nil {
			sess.logger.Info("reconnect abandoned: caller context done", "error", err, "attempts", attempts-1)
			return sess.giveUpReconnecting(fmt.Errorf("reconnect aborted: %w", err))
		}

		if sess.lifecycle.maxReconnectAttempts > 0 && attempts > sess.lifecycle.maxReconnectAttempts {
			sess.logger.Error("max reconnect attempts exhausted, closing session",
				"maxAttempts", sess.lifecycle.maxReconnectAttempts, "error", lastErr)
			// lastErr can be nil: a path that retries without recording an error
			// (waiting for a runtime that is not running) leaves it unset, and
			// wrapping nil with %w prints "%!w(<nil>)" — which is what this said
			// before.
			giveUp := fmt.Errorf("reconnect failed after %d attempts", sess.lifecycle.maxReconnectAttempts)
			if lastErr != nil {
				giveUp = fmt.Errorf("reconnect failed after %d attempts: %w", sess.lifecycle.maxReconnectAttempts, lastErr)
			}
			return sess.giveUpReconnecting(giveUp)
		}

		// Dial TCP, configure keepalive, clear disconnected flag, start goroutines.
		// dialAndStart re-checks closed.Load() before waitGroup.Add(2).
		sess.lifecycle.reconnectAttempts.Add(1)
		if err := sess.dialAndStart(); err != nil {
			lastErr = err
			sess.logger.Warn("reconnect dial/start failed, retrying", "error", err, "ip", sess.ip, "port", sess.port, "attempt", attempts)
			if err := sess.reconnectSleep(ctx, attempts); err != nil {
				return err
			}
			continue
		}

		// Re-perform local-mode handshake if needed
		if sess.isLocal {
			if err := sess.localHandshake(); err != nil {
				if rerr := retryAfter(err, "local handshake"); rerr != nil {
					return rerr
				}
				continue
			}
		}

		// Smart route registration: probe first, register only if needed.
		if err := sess.ensureRoute(); err != nil {
			if rerr := retryAfter(err, "route"); rerr != nil {
				return rerr
			}
			continue
		}

		// Release the pre-reconnect handles now, not after the reload: this is the
		// first point where the transport is up AND routed, which is all a Delete
		// needs. Waiting until after reloadSymbols meant a session whose dial and
		// route came up but whose reload kept failing never issued these deletes at
		// all, and every retry cycle left another set of handles in the PLC's table.
		//
		// Only forget the snapshot once every handle is accounted for. Earlier than
		// this the transport may look dialled without being usable — with route
		// registration skipped there is no probe to prove otherwise — and clearing
		// the snapshot on a release that did not land loses the only record of
		// registrations the PLC still holds. Measured against a flapping TC2:
		// "requested=3 deleted=0" on every cycle, three more entries stranded each
		// time.
		if len(savedHandles) > 0 {
			releaseTries++
			deleted := sess.bestEffortDeleteNotifications(sess.currentLifecycleCtx(), savedHandles)
			sess.logger.Info("reconnect: cleaned up pre-reconnect notification handles",
				"requested", len(savedHandles), "deleted", deleted)
			switch {
			case deleted >= len(savedHandles):
				savedHandles = nil
			case releaseTries >= preReconnectReleaseAttempts:
				// Bounded on purpose: reconnect attempts are unbounded by default,
				// and a PLC that keeps refusing leaves the orphan reaper as the
				// backstop — it deletes these if they ever stream again.
				sess.logger.Warn("reconnect: giving up on releasing pre-reconnect notification handles",
					"unreleased", len(savedHandles)-deleted, "attempts", releaseTries)
				savedHandles = nil
			default:
				sess.logger.Warn("reconnect: keeping unreleased notification handles for the next attempt",
					"unreleased", len(savedHandles)-deleted, "attempt", releaseTries)
			}
		}

		// Re-load symbols based on discovery mode
		if err := sess.reloadSymbols(); err != nil {
			if rerr := retryAfter(err, "symbol reload"); rerr != nil {
				return rerr
			}
			continue
		}

		// Re-subscribe notifications using stored configs.
		if err := sess.resubscribeNotifications(); err != nil {
			if errors.Is(err, ErrRuntimeNotRunning) {
				// The transport is fine and the route is served; the runtime is not
				// running. Counting this as a reconnect attempt spends the budget at
				// the backoff rate with no network involved and eventually closes a
				// session whose only problem is a PLC in CONFIG — the opposite of
				// "stay up and wait". Report it, sleep, and try again without
				// consuming an attempt.
				sess.logger.Info("reconnect: transport restored but the PLC runtime is not running; waiting for it",
					"error", err)
				// resetForRetry, exactly as retryAfter does: the loop dials a fresh
				// connection every iteration and only a teardown stops the previous
				// Client's workers. Skipping it redialed on top of a live Client —
				// caught by -race as a write/read conflict on tx.connection.
				sess.resetForRetry()
				if serr := sess.reconnectSleep(ctx, attempts); serr != nil {
					return serr
				}
				// Give the attempt back. `continue` alone returns to the attempts++ at
				// the loop head, so the budget burned anyway — which is the whole
				// defect: a PLC in CONFIG walked the session through every attempt and
				// closed it, just more slowly.
				attempts--
				continue
			}
			if rerr := retryAfter(err, "notification resubscribe"); rerr != nil {
				return rerr
			}
			continue
		}

		// Deliberately no disconnected.Store(false) here. dialAndStart already
		// cleared the flag once the workers were up, so on the happy path this was a
		// no-op; its only effect was to erase a drop that landed in the tail between
		// that clear and here. tx.disconnected is the SOLE record of such a drop —
		// the FSM has no Reconnecting->Disconnected edge — so erasing it blinded the
		// adopt check in the deferred hand-off below, and the session sat Connected
		// on a dead socket with IsClosed() false: no data, and no signal a consumer
		// polling IsClosed() could act on.
		sess.lifecycle.strictReconnectFailures = 0 // reset on success
		// epoch bumps inside the transition helper when target == Connected;
		// enterConnected additionally advances the connected generation, which is
		// what tells the heartbeat watcher to drop the pre-drop silence count.
		sess.enterConnected()
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

	// Force mode or too many probe failures → register, unless this session
	// registered recently. See routeManager.registered and mayRegister: re-registering the
	// same route cannot fix anything, and on some firmware it leaves a duplicate
	// runtime entry that breaks the device outright.
	//
	// Force is the one caller-declared exception, and it bypasses the latch here
	// exactly as it already does on Connect (ensureRouteOnConnect). Gating it was
	// what made WithForceRouteRegistration mean "register once, then stop" —
	// contradicting its godoc, README.md and R-ROUTE-005, and failing the very case
	// it is documented for: a device that forgets its route table on reboot was
	// reconnected to without a re-registration. The route-table cost is stated in
	// the option's godoc and is opt-in; sessions that do not set it are unaffected.
	probeFailures := sess.route.routeProbeFailures.Load()
	if sess.route.forceRouteRegistration || probeFailures >= 3 {
		if !sess.route.forceRouteRegistration && !sess.route.mayRegister() {
			sess.logger.Debug("route already registered by this session; not registering again",
				"probeFailures", probeFailures)
			_, err := sess.awaitRouteActive(sess.currentLifecycleCtx)
			return err
		}
		sess.logger.Info("registering route (forced/fallback)", "probeFailures", probeFailures)
		if err := sess.AddRoute(sess.currentLifecycleCtx(), sess.route.name, sess.route.username, string(sess.route.password)); err != nil {
			return fmt.Errorf("route registration failed: %w", err)
		}
		sess.route.markRegistered()
		// Same activation lag as on connect. Failing here feeds the reconnect
		// loop's own retry rather than letting reloadSymbols be the first thing
		// to discover the route isn't live yet. currentLifecycleCtx, not a
		// captured ctx: awaitRouteActive's redial replaces lifecycle.ctx.
		if _, err := sess.awaitRouteActive(sess.currentLifecycleCtx); err != nil {
			return err
		}
		sess.route.routeProbeFailures.Store(0)
		return nil
	}

	// Probe: try a lightweight ADS command to see if route already exists
	_, probeErr := sess.probeRouteVersion(sess.currentLifecycleCtx())
	if probeErr == nil {
		sess.logger.Debug("route still valid, skipping re-registration")
		sess.route.routeProbeFailures.Store(0)
		return nil
	}

	if sess.isClosed() {
		return fmt.Errorf("connection closed during route probe")
	}

	// Probe failed → register with credentials, unless we already did recently.
	failuresAfter := sess.route.routeProbeFailures.Add(1)
	if !sess.route.mayRegister() {
		sess.logger.Debug("route probe failed but this session already registered the route; waiting for it to be served instead of registering again",
			"error", probeErr, "probeFailures", failuresAfter)
		_, err := sess.awaitRouteActive(sess.currentLifecycleCtx)
		return err
	}
	sess.logger.Info("route probe failed, registering route", "error", probeErr, "probeFailures", failuresAfter)
	if err := sess.AddRoute(sess.currentLifecycleCtx(), sess.route.name, sess.route.username, string(sess.route.password)); err != nil {
		return fmt.Errorf("route registration failed after probe: %w", err)
	}
	sess.route.markRegistered()
	_, err := sess.awaitRouteActive(sess.currentLifecycleCtx)
	return err
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
		return sess.loadSymbols(sess.currentLifecycleCtx())

	case listLoaded || dtLoaded:
		// Partial discovery — re-download what was loaded
		if listLoaded {
			if err := sess.LoadSymbolList(sess.currentLifecycleCtx(), SlowDiscoveryConfig{}); err != nil {
				return fmt.Errorf("reload symbol list: %w", err)
			}
		}
		if dtLoaded {
			if err := sess.LoadDataTypes(sess.currentLifecycleCtx(), SlowDiscoveryConfig{}); err != nil {
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
			if _, err := sess.getSymbol(sess.currentLifecycleCtx(), name); err != nil {
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
		version, err := sess.client.Load().GetSymbolVersion(sess.currentLifecycleCtx())
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
// callers but is now a no-op — capability state lives on *Client, and
// dialAndStart allocates a fresh zero-valued Client on every attempt.
//
// Used by three reset paths:
//   - Connect()'s post-route-registration TCP teardown
//   - Reconnect()'s pre-retry-loop reset
//   - resetForRetry()
func (sess *Session) tearDownAndReset() {
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
	// above plus the closed TCP socket trigger their exit. Adopted inbound
	// connections are closed first: their readers block on a socket nothing else
	// touches, so leaving them open deadlocks this wait — observed hanging a real
	// session against a peer-route device.
	if c := sess.client.Load(); c != nil {
		// Release anything waiting on this transport with the reason, rather than
		// letting it sit out its full request timeout: readFrames returns on
		// ctx.Done() without calling callOnDrop, so nothing else closes `dropped`
		// on a session-initiated teardown.
		c.markDropped()
		c.closePeerConns()
		c.waitGroup.Wait()
	}
	// spawnMu across the Wait: trackGoroutineOn takes it to Add, so holding it here
	// makes "no new goroutines while we wait for the current ones" true. Without it
	// an orphan delete registering during a Connect-phase teardown can Add while
	// this Wait is in progress, which is documented WaitGroup misuse and panics.
	sess.lifecycle.spawnMu.Lock()
	sess.lifecycle.waitGroup.Wait()
	sess.lifecycle.spawnMu.Unlock()
	sess.lifecycle.ctxMu.Lock()
	// Re-derive lifecycle.ctx from the original NewSession parent so caller
	// cancellation continues to shut the session down after Reconnect. Prior
	// behaviour used context.Background() here, which detached the session
	// from its constructor parent after the first tearDownAndReset.
	parent := sess.lifecycle.parentCtx
	if parent == nil {
		// Defensive: test fixtures that build a Session literal directly
		// without going through NewSession may leave parentCtx nil. Fall
		// back to Background so tearDownAndReset stays panic-free; the
		// production constructor always sets parentCtx.
		parent = context.Background()
	}
	sess.lifecycle.ctx, sess.lifecycle.shutdown = context.WithCancel(parent) //nolint:gosec // cancel stored in lifecycle.shutdown, called from Close
	sess.lifecycle.ctxMu.Unlock()
	sess.tx.chanMu.Lock()
	sess.tx.sendChannel = make(chan []byte)
	sess.tx.systemResponse = make(chan []byte, 1)
	sess.tx.recvQueue = make(chan []byte, recvQueueSize)
	sess.tx.chanMu.Unlock()
	sess.tx.activeRequestLock.Lock()
	sess.tx.activeRequests = map[uint32]chan amsReply{}
	sess.tx.activeRequestLock.Unlock()
	// Capability state lives on Client. A fresh Client (allocated in
	// dialAndStart on each reconnect attempt) has zero-value capabilities,
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
	sess.publishWiredClient()
	// Clear disconnected AFTER the workers are up, so a user RPC that observes
	// disconnected=false is guaranteed to find transmitWorker actually running.
	sess.tx.disconnected.Store(false)
	return nil
}

// sourceAddr returns the source AMS address under the mutex that guards it.
//
// tx.connMu is the field's lock: Connect takes it around the auto-derive
// (session.go, "Auto-derive source AMS NetID") and around the local-mode
// handshake's assignment, and localHandshake does the same on the reconnect
// goroutine. Every reader outside those critical sections must come through here —
// the exported AddRoute is callable from any goroutine, and Client.encodeTo
// (ams.go) and Client.sourceAddr take the same lock for the same reason.
//
// Returns the whole AMSAddress, not just the NetID: publishWiredClient needs the
// port too, and one accessor for the field beats two that could disagree about
// which lock protects it.
func (sess *Session) sourceAddr() AMSAddress {
	sess.tx.connMu.Lock()
	defer sess.tx.connMu.Unlock()
	return sess.source
}

// publishWiredClient allocates the Client for the connection currently on
// sess.tx, wires its handlers, starts its workers and publishes it.
//
// Both connect paths need exactly this, and the ctx/cancel pair must be captured
// under one RLock: tearDownAndReset replaces them together, so reading them
// separately can hand the Client a context from one generation and the cancel of
// the next. Having one copy of that means the invariant lives with the code
// rather than in two comments.
//
// The publish is last on purpose: concurrent readers must never see a
// half-initialised Client.
func (sess *Session) publishWiredClient() *Client {
	sess.lifecycle.ctxMu.RLock()
	clientCtx := sess.lifecycle.ctx
	clientCancel := sess.lifecycle.shutdown
	sess.lifecycle.ctxMu.RUnlock()

	c := &Client{
		ip:             sess.ip,
		port:           sess.port,
		target:         sess.target,
		source:         sess.sourceAddr(),
		requestTimeout: sess.requestTimeout,
		logger:         sess.logger,
		tx:             sess.tx,
		dropped:        make(chan struct{}),
		ctx:            clientCtx,
		cancel:         clientCancel,
	}
	// handleNotification gives the Client cache-aware dispatch for inbound
	// DeviceNotification packets; triggerReconnect routes transport-down into the
	// Session's reconnect FSM.
	c.SetNotificationHandler(sess.handleNotification)
	c.SetOnDrop(sess.triggerReconnect)
	c.startWorkers()
	sess.client.Store(c)
	return c
}

// peerFallbackProbes is how many requests the automatic fallback sends after
// starting the listener. Each carries the session's own request timeout, and the
// device has to dial in and answer one of them.
const peerFallbackProbes = 3

// tryPeerFallback is the automatic half of peer-route support: when a PLC has
// accepted our connection and answered nothing at all, bind the AMS port and see
// whether it is answering on a connection it opens to us instead.
//
// Why this is worth binding a socket for: the responses are not lost, they are
// being delivered somewhere we are not listening (see AcceptPeerConn for the
// decoded evidence). A caller that only ever calls Connect — which is how the
// benthos plugin uses this library — has no way to reach for an option, so a
// session that could work would simply never work. The bind only happens for a
// session that is otherwise dead, and it is announced at WARN because a device in
// this state is worth an operator's attention.
//
// peerRouteHosts remembers which devices answer on a connection they open to us,
// keyed by host, for the life of the process.
//
// Learning this costs a full route-probe timeout plus the route-activation budget —
// about 15s on a device in that state, measured on 192.168.3.224 — and it is a
// property of the DEVICE, not of one session. Re-learning it on every Connect made
// a suite of ~50 sessions spend a quarter of an hour discovering the same fact.
//
// Only ever causes the inbound listener to be bound earlier, so a stale entry
// (device's route table repaired since) costs a bound port and nothing else: the
// normal probe still runs and still wins.
// Keyed by host AND port, not host alone: every scriptable stub in the test suite
// lives on 127.0.0.1, so an IP-only key made one rescued stub speak for all of them
// and every later session pre-bound the protocol port they then fought over. Real
// devices all sit on 48898, so the port adds nothing there and costs nothing.
var peerRouteHosts sync.Map // "host:port" -> struct{}

func (sess *Session) peerRouteCacheKey() string {
	return net.JoinHostPort(sess.ip, strconv.Itoa(sess.port))
}

func rememberPeerRouteHost(key string) { peerRouteHosts.Store(key, struct{}{}) }

// forgetPeerRouteHost drops a remembered device. Call it through
// Session.forgetPeerRouteHostIfUnused rather than directly, so the entry is only
// dropped when the session proved the device does not need it.
func forgetPeerRouteHost(key string) { peerRouteHosts.Delete(key) }

// forgetPeerRouteHostIfUnused drops the remembered fact for this device when the
// session reached Connected without a single inbound connection from it.
//
// This is the self-invalidation the comment on peerRouteHosts describes, and until
// now it lived inside Connect's route-registration branch — so a caller that does
// not use WithRoute (umh-core in a container, and every session in the default
// configuration) never invalidated anything. One observation then governed every
// later session in the process: each of them pre-bound the inbound AMS port —
// wildcard :48898 with no WithAmsPeerListen — for its whole lifetime, long after
// the device's route table had been repaired.
//
// The inbound-connection count is what makes this safe to call unconditionally. A
// Connect can succeed BECAUSE the listener was pre-bound and the device answered
// there, and dropping the entry then would throw away the ~15s of probing that
// learned it; a device that dialled us has peerConnsAdopted > 0 and keeps its entry.
//
// No expiry and no reaper: with this in place an entry is dropped by the first
// Connect that does not need it, which is a better clock than any timeout, and a
// timeout would need a goroutine or a lazy sweep for no gain.
func (sess *Session) forgetPeerRouteHostIfUnused() {
	if sess.peerConnsAdopted.Load() != 0 {
		return
	}
	forgetPeerRouteHost(sess.peerRouteCacheKey())
}

func isKnownPeerRouteHost(key string) bool {
	_, ok := peerRouteHosts.Load(key)
	return ok
}

// Returns rescued=true when a request succeeded after the listener came up.
func (sess *Session) tryPeerFallback(ctx context.Context) (rescued bool, why error) {
	if sess.peerFallbackDisabled {
		return false, fmt.Errorf("peer-route fallback disabled by WithoutAmsPeerFallback")
	}
	sess.peerMu.Lock()
	listening := sess.peerLn != nil
	sess.peerMu.Unlock()
	if !listening {
		if err := sess.startPeerListener(); err != nil {
			return false, err
		}
		sess.logger.Info("PLC answered nothing on our connection; listening for one it may open to us",
			"port", sess.peerListenPortOrDefault())
	}
	// Probe either way. Returning early when a listener already existed was wrong
	// twice over: a caller that set WithAmsPeerListen never got the fallback's
	// probes at all, and pre-binding for a device already KNOWN to answer on its own
	// connection turned the fast path into a guaranteed failure. Whether the device
	// answers there is exactly what the probes below determine.

	for attempt := 1; attempt <= peerFallbackProbes; attempt++ {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		if _, err := sess.client.Load().GetSymbolVersion(ctx); err == nil {
			sess.logger.Warn("PLC answers on a connection it opens to us, not on ours; using that connection",
				"attempts", attempt, "listenPort", sess.peerListenPortOrDefault(),
				"detail", "the device treats its route to this host as a peer route. "+
					"Its route table most likely holds more than one entry for this source NetID; "+
					"a client that cannot accept the inbound connection sees every request time out")
			return true, nil
		}
		select {
		case <-time.After(200 * time.Millisecond):
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return false, nil
}

// peerListenPortOrDefault is the port the peer listener uses.
func (sess *Session) peerListenPortOrDefault() int {
	if sess.peerListenPort != 0 {
		return sess.peerListenPort
	}
	return amsPeerListenPort
}

// amsPeerListenPort is where a TwinCAT peer router expects to reach us: the same
// port a PLC serves ADS on. Only relevant with WithAmsPeerListen.
const amsPeerListenPort = 48898

// startPeerListener begins accepting connections the PLC opens to us, once per
// session. Each accepted connection is handed to whichever Client is current, so
// it keeps working across reconnects — the PLC dials again after every drop.
//
// Errors are returned rather than logged-and-ignored: a session that needs this
// cannot work without it, and the usual cause (a local TwinCAT router already
// owns the port) is worth saying out loud.
// A mutex, not sync.Once. Once runs its body once whether it succeeded or not, and
// the error was a closure local — so after a failed bind every later call returned
// nil with nothing listening. A Connect retry (legal: the rollback leaves
// Disconnected, and Disconnected -> Connecting is an allowed edge) then proceeded
// believing the listener was up, and tryPeerFallback went on to log "listening for
// one it may open to us" and probe three times against a port it had never bound,
// swallowing the one hint that says a local TwinCAT router owns 48898.
func (sess *Session) startPeerListener() error {
	sess.peerMu.Lock()
	defer sess.peerMu.Unlock()
	if sess.peerLn != nil {
		return nil // already listening
	}
	if sess.peerStopped {
		return fmt.Errorf("session is shutting down; not binding the inbound AMS port")
	}
	select {
	case <-sess.lifecycle.closedCh:
		return fmt.Errorf("session is closed; not binding the inbound AMS port")
	default:
	}
	port := sess.peerListenPort
	if port == 0 {
		port = amsPeerListenPort
	}
	ln, err := net.Listen("tcp4", net.JoinHostPort("", strconv.Itoa(port)))
	if err != nil {
		// Deliberately retryable: the port may be free by the next attempt, and a
		// caller who retries Connect is entitled to a real answer either way.
		return fmt.Errorf("listening for the PLC's own connection on port %d: %w "+
			"(a local TwinCAT router or another ADS client may already own it)", port, err)
	}
	sess.peerLn = ln
	sess.logger.Info("listening for inbound PLC connections (peer-route support)", "port", port)
	sess.peerWG.Add(1)
	go sess.peerAcceptLoop(ln)
	return nil
}

// peerAcceptLoop attaches every inbound connection to the current Client.
func (sess *Session) peerAcceptLoop(ln net.Listener) {
	defer sess.peerWG.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return // listener closed by Close
		}
		if sess.isClosed() {
			_ = conn.Close()
			return
		}
		c := sess.client.Load()
		if c == nil {
			// No Client yet (or between teardown and redial): the PLC will dial
			// again once we have one, so dropping this is safe.
			sess.logger.Debug("inbound PLC connection arrived with no active client; closing",
				"remote", conn.RemoteAddr().String())
			_ = conn.Close()
			continue
		}
		// Counted before the hand-off, and never decremented: the question this
		// answers is "did this device ever dial us", and a Client that refuses the
		// connection because it is being torn down does not make the answer no.
		sess.peerConnsAdopted.Add(1)
		c.AcceptPeerConn(conn)
	}
}

// stopPeerListener closes the listener, latches the session against ever binding
// again, and waits for the accept loop to exit. For terminal teardown only.
func (sess *Session) stopPeerListener() {
	sess.peerMu.Lock()
	// Latch, so a Connect that was about to bind cannot do so after this returns.
	// Without it: a Connect descheduled just before the bind, a concurrent Close
	// reading peerLn as nil and waiting on an empty peerWG, and then the bind
	// landing — leaving 48898 held and an accept loop running in a session the
	// caller believes is fully closed. The accept loop's own isClosed() escape only
	// runs after a connection arrives, which on a dead PLC never happens.
	//
	// Set BEFORE releasePeerListener takes the lock: startPeerListener holds peerMu
	// across its net.Listen, so once the latch is visible any bind either already
	// completed (and releasePeerListener sees the listener) or is refused.
	sess.peerStopped = true
	sess.peerMu.Unlock()
	sess.releasePeerListener()
}

// releasePeerListener closes the listener and waits for the accept loop to exit,
// WITHOUT latching peerStopped.
//
// This is what a failed Connect needs. The session stays usable for a retry (the
// FSM rolls back to Disconnected, and Disconnected -> Connecting is a legal edge),
// so latching here would permanently refuse the bind and make every retry fail
// with "session is shutting down" — while NOT releasing at all left port 48898
// held and its accept loop running per failed attempt, which callers that respond
// to a Connect error by discarding the session then leaked for the process's life.
func (sess *Session) releasePeerListener() {
	sess.peerMu.Lock()
	ln := sess.peerLn
	sess.peerLn = nil
	sess.peerMu.Unlock()
	if ln != nil {
		_ = ln.Close()
	}
	sess.peerWG.Wait()
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
	if c := sess.client.Load(); c != nil {
		c.setSource(result) // same reason as the Connect path
	}
	return nil
}

// resubscribeNotifications restores notification subscriptions stored in
// notificationConfigs after a successful reconnect. Filters out symbols that
// no longer exist after symbol reload. On error, rolls back partial PLC-side
// successes and restores the saved configs so they can be retried by
// the next reconnect attempt.
func (sess *Session) resubscribeNotifications() error {
	// One re-subscribe at a time, whichever path asked for it. See
	// notificationManager.resubscribeMu: the snapshot-then-clear at the top of this
	// function is what makes an overlap destructive.
	sess.notifications.resubscribeMu.Lock()
	defer sess.notifications.resubscribeMu.Unlock()
	return sess.resubscribeNotificationsLocked()
}

// resubscribeNotificationsLocked is resubscribeNotifications with resubscribeMu
// already held. Callers whose whole sequence must be atomic — snapshot the intent,
// try, restore on failure — hold the mutex across all of it and call this, rather
// than letting another path slip in between their snapshot and their restore.
func (sess *Session) resubscribeNotificationsLocked() error {
	sess.notifications.lock.Lock()
	savedPending := sess.notifications.pending
	savedChannel := sess.notifications.notificationChannel
	// Nothing to resubscribe, so nothing may be destroyed on the way out. The clear
	// used to happen before this guard, so a no-op that reports success wiped the
	// caller's declared intent: with a non-empty pending and no bound channel — the
	// state left by re-queued retry entries plus a full user teardown — every symbol
	// the caller never cancelled was silently dropped from the resubscribe set, with
	// "reconnect successful" logged over the top.
	if len(savedPending) == 0 || savedChannel == nil {
		sess.notifications.lock.Unlock()
		if len(savedPending) > 0 {
			// Not silent, and not a Warn: the caller cannot act on this, and the
			// intent is being KEPT. It matters only when reading back why a
			// resubscribe registered nothing.
			sess.logger.Debug("re-subscribe skipped: no channel is bound; keeping the declared subscriptions on file",
				"configs", len(savedPending))
		}
		return nil
	}
	// Clear via resetConfigs so the key-index mirror is wiped in lockstep — the
	// resubscribe below re-files every entry it commits, and a stale mirror would
	// leave the intent describing symbols this attempt has already replaced.
	sess.notifications.resetConfigs(nil)
	sess.notifications.lock.Unlock()
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

	subResults, err := sess.AddSymbolNotifications(sess.currentLifecycleCtx(), validConfigs, savedChannel)

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
		deleted := sess.bestEffortDeleteNotifications(sess.currentLifecycleCtx(), orphanHandles)
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
			deleted := sess.bestEffortDeleteNotifications(sess.currentLifecycleCtx(), newHandles)
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
	sess.tearDownAndReset()
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
	// One snapshot, taken here on the caller's goroutine, used for both the derived
	// host IP and the registration itself. AddRoute is exported and callable from any
	// goroutine while localHandshake writes sess.source under connMu — and the
	// goroutine below outlives this call when ctx is cancelled, so reading the field
	// there was an unsynchronised read on a detached goroutine. A torn NetID
	// registers a route for an identity that exists nowhere: the PLC answers nothing
	// and a junk entry is left in its route table.
	netID := sess.sourceAddr().NetID
	hostIP := sess.callbackIP
	if hostIP == "" {
		hostIP = fmt.Sprintf("%d.%d.%d.%d", netID[0], netID[1], netID[2], netID[3])
	}
	// AddRemoteRouteWithLogger uses a fixed 5s UDP read deadline internally
	// and has no context parameter. Wrap in goroutine + select so caller
	// cancellation unblocks AddRoute promptly even though the underlying
	// UDP socket keeps draining toward its own deadline in the background.
	done := make(chan error, 1)
	go func() {
		done <- addRemoteRouteFrom(sess.logger, sess.localBindIP, sess.ip, sess.effectiveRouterPort(), netID, routeName, hostIP, username, password)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("AddRoute aborted: %w", ctx.Err())
	}
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

// ErrRuntimeNotRunning is returned by symbol and subscription calls when the
// device's system service reports the runtime is not in RUN.
//
// Deliberately a refusal rather than a retry: in CONFIG the runtime port does not
// exist, so a symbol load or subscribe cannot succeed, and attempting it produces a
// misleading error (an AMS "port not found" that the library used to surface as a
// fabricated ADS code). The session itself stays up and keeps polling, so the
// caller can simply try again once the state changes.
var ErrRuntimeNotRunning = errors.New("ads: PLC runtime is not in RUN")

// RuntimeState reads the device's ADS state from the system service port.
//
// This is the state of the SYSTEM, not of the runtime port the session talks to:
// ADSStateConfig means the PLC is in configuration mode and no runtime port is
// serving. Answers while the runtime is unavailable, which is the point.
func (sess *Session) RuntimeState(ctx context.Context) (ADSState, error) {
	c := sess.client.Load()
	if c == nil {
		return ADSStateInvalid, ErrTransportClosed
	}
	state, err := c.ReadStateOnPort(ctx, PortSystemService)
	if err != nil {
		return ADSStateInvalid, err
	}
	sess.recordRuntimeState(state.ADSState)
	return state.ADSState, nil
}

// runtimeStateQuietly is RuntimeState with the transport-fault logging suppressed,
// for the probe at connect: a device without a system service port answers every one
// of these with an AMS error, and readStateOn reports that at Error in steady state.
func (sess *Session) runtimeStateQuietly(ctx context.Context) (ADSState, error) {
	c := sess.client.Load()
	if c == nil {
		return ADSStateInvalid, ErrTransportClosed
	}
	c.beginHandshake()
	defer c.endHandshake()
	state, err := c.ReadStateOnPort(ctx, PortSystemService)
	if err != nil {
		return ADSStateInvalid, err
	}
	sess.recordRuntimeState(state.ADSState)
	return state.ADSState, nil
}

func (sess *Session) recordRuntimeState(state ADSState) {
	previous := ADSState(sess.runtimeState.Swap(uint32(state)))
	sess.runtimeStateNs.Store(time.Now().UnixNano())
	if previous == state || previous == ADSStateInvalid {
		return
	}
	sess.logger.Info("PLC runtime state changed", "from", previous, "to", state)
	// No nudge into the heartbeat watcher here. One was written — force a recovery on
	// the transition back into RUN — and then removed: with deferrals no longer
	// counted as failures the interval never inflates while a runtime is unavailable,
	// so no test could distinguish the nudge being present from absent. Unprovable
	// machinery, and a coupling from the state poll into notification internals.
}

// runtimeStateTTL is how long a state reading is trusted. Beyond it the session
// treats the state as unknown, which means the gates permit again.
//
// Without an expiry a reading was permanent: the watch gives up after a run of
// failures, and nothing then cleared the last value — so a session that saw CONFIG
// and then lost the system service refused every symbol and subscribe call for its
// whole life, with no poller left to notice the runtime coming back. Failing OPEN is
// the right direction here: the worst case is the old behaviour (attempt it and let
// the PLC answer), where failing closed is a session that never works again.
const runtimeStateTTL = 30 * time.Second

// knownRuntimeState returns the last observed state and whether one was observed
// recently enough to act on.
func (sess *Session) knownRuntimeState() (ADSState, bool) {
	state := ADSState(sess.runtimeState.Load())
	if state == ADSStateInvalid {
		return state, false
	}
	// Wall clock, deliberately: a clock step here can only make a fresh reading look
	// stale, which permits — the safe direction. (Contrast the heartbeat detector,
	// where a step in either direction was harmful, so that one counts ticks.)
	if at := sess.runtimeStateNs.Load(); at != 0 && time.Since(time.Unix(0, at)) > runtimeStateTTL {
		return ADSStateInvalid, false
	}
	return state, true
}

// requireRunningRuntime refuses an operation that cannot work outside RUN.
//
// Gated on evidence: with no reading (a device that does not serve the system
// service port, or a session that has not polled yet) it permits the operation
// rather than inventing a reason to fail.
// runtimeDefinitelyNotServing lists the states in which a runtime port provably
// does not serve, so the call cannot succeed and attempting it only produces a
// confusing AMS "port not found".
//
// A whitelist of bad states, not "anything that is not RUN": the only measured
// evidence is TC3.1.4024 reporting CONFIG (15), and refusing on every state this
// code has not been taught about would turn an unfamiliar-but-working device into
// one where nothing can be subscribed at all — with no PLC error to explain it.
func runtimeDefinitelyNotServing(state ADSState) bool {
	switch state {
	case ADSStateConfig, ADSStateReconfig:
		// The measured cases. TC3.1.4024 in CONFIG reports 15 and answers every
		// request to a runtime port with AMS ErrorCode 6 (target port not found).
		return true
	default:
		// Everything else is permitted, including STOP and SHUTDOWN, which an
		// earlier version of this list refused on inference rather than evidence.
		// STOP in particular was observed only as a ~4s way-point during a CONFIG ->
		// RUN switch; whether a device can idle there with a serving runtime port is
		// unknown, and refusing every subscribe on such a device — with no PLC error
		// to explain it — is a worse failure than attempting the call and letting the
		// PLC answer, which is what this library did before the gate existed.
		return false
	}
}

func (sess *Session) requireRunningRuntime(what string) error {
	if state, known := sess.knownRuntimeState(); known && runtimeDefinitelyNotServing(state) {
		return fmt.Errorf("%s: %w (ADS state %d); the runtime port is not serving, so this cannot succeed until it returns to RUN",
			what, ErrRuntimeNotRunning, uint16(state))
	}
	return nil
}

// startRuntimeStateWatch polls the system service for the runtime state, once per
// session, at the heartbeat interval.
//
// Polling is the only option here: there is nothing to subscribe to that survives
// the transition being watched — in CONFIG the runtime port that would carry a
// notification does not exist. It is one small request per interval to a port that
// is up whenever the device is, and it is what lets the session say "the runtime is
// in CONFIG" instead of retrying blindly.
//
// Gives up after a run of failures so a device without a system service port costs
// nothing: the gates fall back to permitting, which is the pre-existing behaviour.
func (sess *Session) startRuntimeStateWatch() {
	sess.stateOnce.Do(func() {
		started := sess.trackGoroutineOn(&sess.stateWG, func() {
			interval := sess.heartbeatCycle()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			failures := 0
			const giveUpAfter = 5
			for {
				select {
				case <-sess.lifecycle.closedCh:
					return
				case <-ticker.C:
				}
				// Connected, not merely "not disconnected": dialAndStart clears
				// tx.disconnected BEFORE ensureRoute, awaitRouteActive, reloadSymbols
				// and resubscribeNotifications run, so isDisconnected() is false for
				// that whole stretch. Polling through it means firing requests at a
				// router that is by construction not serving us yet, counting each
				// timeout as evidence the device has no system service, and adding
				// traffic to a router already refusing this IP.
				if sess.lifecycle.state.load() != SessionStateConnected {
					continue
				}
				c := sess.client.Load()
				if c == nil || (c.ctx != nil && c.ctx.Err() != nil) {
					continue
				}
				// requestTimeout, not the tick period: a device answering slower than
				// one interval would otherwise be declared unable to answer at all.
				pollTimeout := sess.requestTimeout
				if pollTimeout < interval {
					pollTimeout = interval
				}
				ctx, cancel := context.WithTimeout(sess.currentLifecycleCtx(), pollTimeout)
				// Quietly: on a device with no system service every poll fails, and
				// readStateOn logs a transport fault at Error in steady state, which
				// is exactly the log-based health signal transportFaultLevel exists
				// to protect.
				c.beginHandshake()
				state, err := c.ReadStateOnPort(ctx, PortSystemService)
				c.endHandshake()
				cancel()
				if err != nil {
					// Only an answer counts as evidence. A timeout says nothing about
					// whether the port exists — it is what a busy device, a congested
					// link, or a router mid-activation produces — so counting those
					// towards "this device has no system service" retired the feature
					// on healthy hardware.
					if !isDeviceAnswer(err) {
						sess.logger.Debug("runtime-state poll did not get an answer; not counting it against the device",
							"error", err)
						continue
					}
					failures++
					sess.logger.Debug("the system service refused the runtime-state read",
						"error", err, "attempt", failures)
					if failures >= giveUpAfter {
						// Clear the last reading on the way out, or it stands forever
						// with nothing left to refresh it and every gated call keeps
						// refusing. knownRuntimeState's TTL would eventually do this
						// too; doing it here makes the hand-off immediate.
						sess.runtimeState.Store(uint32(ADSStateInvalid))
						sess.logger.Info("this device does not answer on the system service port; runtime-state reporting is off for this session, and symbol calls will be attempted as before",
							"port", uint32(PortSystemService), "attempts", failures)
						return
					}
					continue
				}
				failures = 0
				sess.recordRuntimeState(state.ADSState)
			}
		})
		if !started {
			sess.logger.Debug("not starting the runtime-state watch: the session is closed")
		}
	})
}

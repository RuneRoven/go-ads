// Package ads is a pure-Go client for the Beckhoff TwinCAT ADS protocol.
//
// The package exposes two layers:
//
//   - Client (this file): a thin Beckhoff-equivalent RPC layer. One TCP
//     connection, raw AMS framing, request multiplexing, no cache, no
//     reconnect, no notification persistence. Construct via Dial; once the
//     transport drops, every subsequent method returns ErrTransportClosed
//     and the caller reconstructs a new Client. Suitable for one-shot
//     consumers (CLI tools, web ADS browsers).
//
//   - Session (session.go): a managed wrapper that adds the symbol cache,
//     name-based read/write, persistent notifications with auto-resubscribe
//     after a reconnect, auto-reconnect with backoff, lifecycle callbacks,
//     and an explicit FSM (docs/archive/specs/09-fsm-design.md). Construct via
//     NewSession + Connect.
//
// Session does NOT embed *Client; pick a layer at construction time. Raw
// methods (Read, Write, Sum*, AddDeviceNotification, ReadProcess*, etc.)
// live on *Client only. Cache-aware methods (ReadFromSymbol,
// AddSymbolNotification, LoadSymbols, …) live on *Session only.
package ads

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// droppedResponseGrace is how long a request already waiting for a reply keeps
// waiting after the transport is observed dead.
//
// A drop is detected on the listen goroutine, which closes the dropped channel
// immediately — but a reply it parsed a moment earlier is still travelling
// recvQueue to a recvWorker. Without this grace the drop signal wins that race
// and an answer we already received is thrown away: measured at 40/40 replies
// lost against a stub that answers and then closes, which is exactly what a PLC
// does on a route-idle timeout, a runtime restart, or an RST after answering.
//
// Only a request with a reply in flight pays it, and only once — a multi-request
// operation aborts on the first failure — so the fast-fail this exists to
// support keeps almost all of its benefit.
const droppedResponseGrace = 100 * time.Millisecond

// ErrTransportClosed is returned by every Client method after the underlying
// TCP transport has been closed (Close called, drop detected, dial failed).
// Callers reconstruct a new *Client to re-establish.
var ErrTransportClosed = errors.New("ads: client transport closed")

// resetAfterConnectHint explains a TCP reset that lands right after a successful
// connect. Shared by the transport-fault log line and by the error Connect returns,
// so the two cannot drift — and because the log line alone is not enough: a consumer
// that surfaces the error and not this library's logger otherwise sees only
// "client transport closed", which is the exact ambiguity this package exists to
// remove. Measured against a PLC whose route table had been wiped.
//
// Five plausible causes, named in the order they are worth checking. Listing only
// the route one has previously sent people after a mistyped NetID, so all five stay.
// Both ends of the addressing go in the message, so the reader can check them
// without reconstructing the config.
//
// The eviction cause is last but not rare: a Beckhoff AMS router serves one TCP
// per host and closes the older one (Beckhoff/ADS#49), so a second client on this
// IP -- or a redial storm of our own -- produces a reset indistinguishable at the
// wire level from a missing route. A field investigation spent hours re-reading
// route tables because the hint named only the route causes.
func resetAfterConnectHint(source, target AMSAddress) string {
	return fmt.Sprintf("a reset right after TCP connect means one of: "+
		"no route is registered on the PLC for our NetID (%s), "+
		"the target NetID (%s) does not exist on this PLC, "+
		"the route credentials were rejected, "+
		"AMS port %d addresses no running runtime (expect 851 on TwinCAT 3, 801 on TwinCAT 2), "+
		"or another client on this host IP took the router's single per-host TCP slot and evicted us",
		source.NetIDString(), target.NetIDString(), target.Port)
}

// ErrRouteNotServed reports a connection that was dropped without ever carrying
// an AMS frame: the addressing or the route is the thing to look at. Distinct
// from ErrEstablishedDropped so a consumer can branch without matching strings.
var ErrRouteNotServed = errors.New("connection dropped before the PLC served any frame")

// ErrEstablishedDropped reports a connection that had been carrying AMS frames
// and was then dropped by the PLC or something on the path. The route
// demonstrably existed, so route tables are the wrong place to look; eviction by
// another client on this host IP, a runtime restart, or the network path are the
// candidates.
var ErrEstablishedDropped = errors.New("established connection dropped by the PLC or the network")

// framesSeen reports how many AMS frames this client has decoded across both
// socket directions.
func (c *Client) framesSeen() uint64 {
	return c.framesPrimary.Load() + c.framesPeer.Load()
}

// wasEstablished reports whether this client ever carried an AMS frame, which is
// what separates a drop worth diagnosing as a route problem from one worth
// diagnosing as a lost connection.
//
// Note what it does NOT mean: a frame carrying an AMS ErrorCode (a router
// rejection, a port with no runtime behind it) counts. "The router talked to us"
// is the claim, not "the route worked".
func (c *Client) wasEstablished() bool { return c.framesSeen() > 0 }

// uptimeAttr renders how long this client's connection had been up, for a drop
// log line. Returns a nil-valued attr for a zero dialedAt so the paths that never
// set it (raw Dial before its DialTimeout, test-built literals) print nothing
// instead of a duration measured from the zero time.
func (c *Client) uptimeAttr() slog.Attr {
	if c.dialedAt.IsZero() {
		return slog.Attr{}
	}
	return slog.Duration("uptime", time.Since(c.dialedAt))
}

// localPort reports the local TCP port of the primary connection, or 0 when
// there is none.
//
// Read it BEFORE the socket is closed: LocalAddr on a closed connection is not
// reliable, and the port is the field every drop investigation needed to
// correlate the event against a packet capture.
func (c *Client) localPort() int {
	c.tx.connMu.Lock()
	conn := c.tx.connection
	c.tx.connMu.Unlock()
	if conn == nil {
		return 0
	}
	addr, ok := conn.LocalAddr().(*net.TCPAddr)
	if !ok {
		return 0
	}
	return addr.Port
}

// logDropVerdict reports a primary-transport drop, split by whether this
// connection had ever carried a frame.
//
// Before the split, both cases produced the same line and the same route hint,
// because the only evidence consulted was the errno — and EOF/ECONNRESET look
// identical whether the socket is 20ms or 20h old. A field investigation lost
// hours re-reading route tables on drops of sessions that had been delivering
// samples for half an hour.
//
// Both branches keep "transport down" in the message and go through
// transportFaultLevel(): the level is gated for the handshake case (an expected
// RST during a cold-start probe is not an ERROR), and an AST guard in the tests
// enforces both properties.
func (c *Client) logDropVerdict(err error) {
	attrs := []any{
		"error", err,
		"localPort", c.localPort(),
		"framesPrimary", c.framesPrimary.Load(),
		"framesPeer", c.framesPeer.Load(),
	}
	if up := c.uptimeAttr(); up.Key != "" {
		attrs = append(attrs, up)
	}
	if c.wasEstablished() {
		c.logger.Log(c.ctx, c.transportFaultLevel(),
			"PLC dropped an established connection, transport down",
			append(attrs,
				"hint", establishedDropHint(),
				"sourceNetID", c.sourceAddr().NetIDString(),
				"targetNetID", c.target.NetIDString(),
				"targetPort", c.target.Port)...)
		return
	}
	if isLikelyMissingRoute(err) {
		c.logger.Log(c.ctx, c.transportFaultLevel(), "PLC closed connection, transport down",
			append(attrs,
				"hint", resetAfterConnectHint(c.sourceAddr(), c.target),
				"sourceNetID", c.sourceAddr().NetIDString(),
				"targetNetID", c.target.NetIDString(),
				"targetPort", c.target.Port)...)
		return
	}
	c.logger.Log(c.ctx, c.transportFaultLevel(), "listen read error, transport down", attrs...)
}

// establishedDropHint explains a reset that arrives on a connection which had
// been working. The route is demonstrably not the problem — it was being served
// one frame ago — so the causes worth naming are the ones that end a healthy
// connection.
func establishedDropHint() string {
	return "this connection had been carrying AMS frames, so the route was being served: " +
		"look at another client on this host IP taking the router's single per-host TCP slot, " +
		"a PLC runtime restart or CONFIG toggle, or the network path (a VPN or subnet router in between)"
}

// isLikelyMissingRoute returns true if err indicates a likely missing-AMS-route
// condition (PLC closed the TCP connection because no route exists for our
// NetID). Detects wrapped io.EOF and ECONNRESET via the standard
// errors.Is/As mechanism. Used by listen to add a hint to the
// "transport down" log line.
func isLikelyMissingRoute(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		if errors.Is(netErr.Err, syscall.ECONNRESET) {
			return true
		}
	}
	return errors.Is(err, syscall.ECONNRESET)
}

// NotificationHandler is the callback the recvWorker invokes when it decodes
// a DeviceNotification packet. Session installs its handleNotification here;
// raw Client consumers (web ADS browser, CLI tools, AMS routers) install
// their own. ctx is the Client's internal worker context — observe Done()
// to abort long-running handler work on Close. handle, timestamp, content
// are the parsed notification fields (handle = PLC-assigned ID, timestamp =
// Windows FILETIME, content = raw payload bytes).
type NotificationHandler func(ctx context.Context, handle uint32, timestamp uint64, content []byte)

// Client is the Beckhoff-equivalent thin RPC layer. One TCP connection, raw
// AMS framing, request multiplexing via InvokeID, listen + transmit + recv
// worker goroutines. No cache, no FSM, no reconnect, no callbacks.
//
// Lifetime states are exactly two: alive (Dial succeeded, Close not yet
// called, transport not yet observed as dropped) and closed. Once closed,
// every public method returns ErrTransportClosed.
type Client struct {
	ip   string
	port int

	target AMSAddress
	source AMSAddress

	requestTimeout time.Duration
	logger         *slog.Logger

	tx *transport

	capabilities capabilities //nolint:unused // capability state lives here, accessed via Client methods.

	// notify is invoked from recvWorker when a DeviceNotification packet
	// arrives. nil means raw Client (no dispatch). Session installs a
	// closure pointing at its handleNotification method.
	notifyMu sync.RWMutex
	notify   NotificationHandler

	// ondrop is invoked once on listen / transmitWorker error. nil means
	// raw Client (no auto-recovery — caller observes via ErrTransportClosed
	// from subsequent RPCs). Session installs s.triggerReconnect.
	ondropMu sync.RWMutex
	ondrop   func()

	// handshaking counts route probe / registration regions in flight; see
	// beginHandshake for why transport faults are demoted to Debug then. A
	// counter rather than a flag so overlapping regions cannot have the inner
	// one re-enable ERROR logging while the outer is still running — the same
	// reason subscribeInFlight is a counter.
	handshaking atomic.Int64

	// framesPrimary/framesPeer count AMS frames this client has decoded, split by
	// which socket they arrived on. Together they answer the question that decides
	// how a drop is reported: did this connection ever work?
	//
	// Two counters rather than one, because either single counter is wrong:
	//
	//   - Counting only the primary socket misclassifies a whole device class. On
	//     TC3.1.4026/RTOS the PLC accepts our requests on the connection we opened
	//     but answers on one IT opens back to us (see AcceptPeerConn), so a
	//     perfectly healthy routed session decodes ZERO frames on its primary for
	//     its entire life. Voting "never served" on those drops would give them the
	//     route-suspect diagnosis and the slow, never-served backoff.
	//   - One shared counter lets a peer frame vote the primary "established",
	//     which is the inverse error.
	//
	// The verdict (dropVerdict) is "any frame on any socket of this client", and
	// both counts go in the log so the operator can see which socket was silent.
	framesPrimary atomic.Uint64
	framesPeer    atomic.Uint64

	// dialedAt is when this client's connection was established, used for the
	// uptime on a drop. Set in the composite literal before the client is
	// published and never written again — do NOT move it to an assignment after
	// startWorkers, because readFrames reads it from the listen goroutine.
	//
	// Zero is a legitimate value: raw Dial builds the literal before
	// net.DialTimeout, and ~30 test sites build &Client{} without it. Every log
	// site must go through uptimeAttr, which prints nothing for a zero value
	// rather than a ~2000-year duration.
	dialedAt time.Time

	// dropped is closed when THIS client's connection is known to be gone.
	// disconnected stops new requests; this releases the ones already blocked
	// on a response that will never come, which a flag cannot do.
	//
	// Per-Client, not per-transport, and immutable for the client's lifetime:
	// a Session reuses one transport across reconnects but allocates a fresh
	// Client each time, so "this connection died" is a client-scoped fact. Held
	// on the transport it was signalled by a stale client's listen goroutine
	// after the replacement had already re-armed it, which killed every request
	// on the new connection.
	dropped  chan struct{}
	dropOnce sync.Once

	// Internal cancellation for the worker goroutines. Independent of any
	// caller context — Close cancels this to stop workers.
	ctx       context.Context
	cancel    context.CancelFunc
	waitGroup sync.WaitGroup

	closeOnce sync.Once

	// peerConns are inbound connections the PLC opened to us; see AcceptPeerConn.
	// peerClosed latches once closePeerConns has run: the accept loop outlives a
	// teardown by design, so it can still offer connections to a Client that is
	// going away, and adopting one then breaks the wait for this Client's workers.
	peerMu     sync.Mutex
	peerConns  []net.Conn
	peerClosed bool
}

// setSource replaces the source AMS address this Client stamps on every request.
//
// Local mode learns its real address from the router only AFTER the Client has been
// published (the handshake is a request, so it needs a live Client to send it). The
// Client held a by-value copy taken at construction, and nothing ever updated it —
// so every request for the rest of the session carried the auto-derived placeholder
// (127.0.0.1.1.1 and a random port) instead of the address the router assigned.
// This is also what makes encode's connMu snapshot of c.source meaningful; before
// this existed, that lock guarded a field nobody wrote.
func (c *Client) setSource(addr AMSAddress) {
	c.tx.connMu.Lock()
	c.source = addr
	c.tx.connMu.Unlock()
}

// sourceAddr returns the source AMS address under the mutex that guards it.
// readFrames logs it from the listen goroutine while a local-mode handshake may
// be writing it via setSource — both callers of setSource publish the Client,
// and so start the workers, before the handshake that assigns the address.
// encodeTo (ams.go) already takes connMu for the same reason.
func (c *Client) sourceAddr() AMSAddress {
	c.tx.connMu.Lock()
	defer c.tx.connMu.Unlock()
	return c.source
}

// markDropped releases every request waiting on this client's connection.
// Idempotent: a drop can be observed by listen and the transmit worker both.
func (c *Client) markDropped() {
	c.dropOnce.Do(func() {
		if c.dropped != nil {
			close(c.dropped)
		}
	})
}

// Dial opens one TCP connection to ip:port, configures TCP keepalive, and
// spawns the listen / transmit / recvWorker goroutines. Returns a usable
// Client. See docs/archive/specs/09-fsm-design.md "Layer 2: Client (raw RPC)".
func Dial(
	ip string,
	port int,
	target, source AMSAddress,
	requestTimeout time.Duration,
	opts ...ClientOption,
) (*Client, error) {
	if requestTimeout <= 0 {
		requestTimeout = 5 * time.Second
	}
	c := &Client{
		ip:             ip,
		port:           port,
		target:         target,
		source:         source,
		requestTimeout: requestTimeout,
		logger:         slog.Default(),
		dropped:        make(chan struct{}),
		tx: &transport{
			sendChannel:    make(chan []byte),
			systemResponse: make(chan []byte, 1),
			recvQueue:      make(chan []byte, recvQueueSize),
			activeRequests: map[uint32]chan amsReply{},
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	c.ctx, c.cancel = context.WithCancel(context.Background()) //nolint:gosec // cancel stored on c.cancel and called from Close.

	tcpConn, err := net.DialTimeout(
		"tcp",
		net.JoinHostPort(ip, strconv.Itoa(port)),
		requestTimeout,
	)
	if err != nil {
		c.cancel()
		return nil, fmt.Errorf("ads: dial %s:%d: %w", ip, port, err)
	}
	c.tx.connection = tcpConn
	configureKeepAlive(tcpConn)
	// Before startWorkers, so nothing can be reading it concurrently. The literal
	// above is built before the dial, so this is the first point at which there is
	// a connection time to record.
	c.dialedAt = time.Now()
	c.startWorkers()
	return c, nil
}

// Close cancels worker goroutines, closes the TCP connection, and waits for
// workers to exit. Idempotent: subsequent calls are no-ops returning nil.
// Sets tx.disconnected so any subsequent RPC method returns
// ErrTransportClosed immediately.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.tx.disconnected.Store(true)
		c.closePeerConns()
		c.markDropped()
		c.cancel()
		c.tx.connMu.Lock()
		if c.tx.connection != nil {
			_ = c.tx.connection.Close()
		}
		c.tx.connMu.Unlock()
		c.waitGroup.Wait()
	})
	return nil
}

// ClientOption configures optional construction parameters for Dial.
type ClientOption func(*Client)

// WithClientLogger sets the slog.Logger for a Client. Nil is ignored.
func WithClientLogger(logger *slog.Logger) ClientOption {
	return func(c *Client) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithClientRequestTimeout overrides the per-request and dial timeout.
// Values <= 0 are ignored (the default of 5s applies).
func WithClientRequestTimeout(d time.Duration) ClientOption {
	return func(c *Client) {
		if d > 0 {
			c.requestTimeout = d
		}
	}
}

// WithNotificationHandler installs a callback for inbound DeviceNotification
// packets. Session installs its own handler internally; raw Client consumers
// (CLI, web ADS browser) install their own to receive notifications, or
// leave nil to drop them silently.
func WithNotificationHandler(fn NotificationHandler) ClientOption {
	return func(c *Client) {
		c.notify = fn
	}
}

// SetNotificationHandler installs (or replaces) the callback for inbound
// DeviceNotification packets. nil disables dispatch (packets dropped after
// a Debug log entry). Concurrent-safe; the recvWorker reads under RLock.
func (c *Client) SetNotificationHandler(fn NotificationHandler) {
	c.notifyMu.Lock()
	c.notify = fn
	c.notifyMu.Unlock()
}

// WithOnDrop registers a callback fired on unexpected transport drop.
// See SetOnDrop for the runtime equivalent.
func WithOnDrop(fn func()) ClientOption {
	return func(c *Client) {
		c.SetOnDrop(fn)
	}
}

// SetOnDrop registers a callback fired on unexpected transport drop.
// Prefer WithOnDrop at construction time.
func (c *Client) SetOnDrop(fn func()) {
	c.ondropMu.Lock()
	c.ondrop = fn
	c.ondropMu.Unlock()
}

// beginHandshake marks a route probe / registration region as in flight. During the handshake a dropped connection and an unanswered request
// are expected states, not faults: the normal cold-start flow is probe → PLC
// rejects an unknown NetID → register route → redial → probe again. Logging
// those at ERROR misreports a connect that is still progressing, and
// downstream log-based health checks (umh-core's IsLogsFine fails a data-flow
// component on any level=error line in its recent window) hold the component
// in a starting state even though the PLC is connected and streaming.
//
// Errors are still returned to the caller unchanged — only the log level moves.
func (c *Client) beginHandshake() {
	c.handshaking.Add(1)
}

// endHandshake closes a region opened by beginHandshake. Pairs must match; a
// count that never returns to zero would silence real faults for the life of
// the client, so callers defer this immediately.
func (c *Client) endHandshake() {
	if c.handshaking.Add(-1) >= 0 {
		return
	}
	// Unbalanced release: a negative count reads as "not handshaking" only by
	// accident, and would then need two begins to demote again. Clamp with a CAS
	// rather than a Store so a region opened AFTER this decision is not erased —
	// note the narrower claim: a begin that raced the decrement itself is already
	// folded into it by the Add, and this cannot recover that. The branch is only
	// reachable via a begin/end imbalance, which is a programming error, so say so
	// once instead of silently repairing it forever.
	c.logger.Warn("endHandshake without a matching beginHandshake; clamping",
		"count", c.handshaking.Load())
	for {
		n := c.handshaking.Load()
		if n >= 0 {
			return
		}
		if c.handshaking.CompareAndSwap(n, 0) {
			return
		}
	}
}

// transportFaultLevel returns the level to log a transport fault at: Debug
// while a handshake is in flight, Error otherwise.
//
// Use it for every TRANSPORT fault — the link went away, a request went
// unanswered, a write failed — because all of those are expected states of the
// probe → register → redial cold start. Do NOT use it for protocol or
// programming faults (header/body parse, packet exceeds the sanity limit,
// binary.Write failure): a handshake never legitimately produces those, and
// demoting them would hide corruption. One un-gated transport site is enough to
// re-trip a downstream log-based health check, so new fault paths need this
// distinction made deliberately.
func (c *Client) transportFaultLevel() slog.Level {
	if c.handshaking.Load() > 0 {
		return slog.LevelDebug
	}
	return slog.LevelError
}

func (c *Client) callOnDrop() {
	// Release every request on THIS client — the ones already blocked as well as
	// any issued later — so they fail fast with ErrTransportClosed instead of
	// waiting out a timeout on a dead socket. Measured cost of not doing this: a
	// 40-symbol notification batch that lost the link at symbol 3 took 3m10s to
	// return, holding the subscribe window (and so disabling the orphan reaper)
	// for all of it.
	//
	// Deliberately does NOT touch tx.disconnected. That flag lives on the
	// transport, which a Session reuses across reconnects, and Session already
	// owns it (triggerReconnect, Reconnect, resetForRetry, Close). Setting it
	// here let a stale client's listen goroutine flip it back to true after the
	// replacement had cleared it, which made every probe on the new connection
	// fail instantly with ErrTransportClosed — route registration could then
	// never complete.
	c.markDropped()
	c.ondropMu.RLock()
	fn := c.ondrop
	c.ondropMu.RUnlock()
	if fn != nil {
		fn()
	}
}

// startWorkers spawns the listen, transmit, and recvWorker goroutines.
// Called by Dial after the TCP socket is established, and by Session at
// every successful dial / redial. Each goroutine ends on c.ctx.Done() or
// transport-level error.
func (c *Client) startWorkers() {
	c.waitGroup.Add(2 + recvWorkerCount)
	go c.listen()
	go c.transmitWorker()
	for i := 0; i < recvWorkerCount; i++ {
		go c.recvWorker()
	}
}

func (c *Client) listen() {
	defer c.waitGroup.Done()
	// Snapshot under the mutex that guards it. A reconnect writes tx.connection
	// while dialing, and reading it bare here raced that write — reported by -race
	// against the reconnect loop, which dials a fresh connection per attempt.
	c.tx.connMu.Lock()
	conn := c.tx.connection
	c.tx.connMu.Unlock()
	if conn == nil {
		c.logger.Debug("listen: no connection to read from")
		return
	}
	c.readFrames(conn, true)
}

// AcceptPeerConn adopts a TCP connection the PLC opened TO US and reads AMS
// frames from it into the same response mux as our own connection.
//
// Some devices treat a registered route as a peer router: they accept and process
// our requests on the connection we opened, then send every response over a
// connection they open back to us on 48898. Measured on TC3.1.4026 (TC/RTOS);
// TC2 2.10 and TC3.1.4024/CE answer on our connection instead. Beckhoff's own
// Linux AdsLib never listens, so it cannot talk to a device in that state at all.
//
// Frames carry their own invokeID, so responses match up regardless of which
// socket they arrive on, and notifications dispatch normally.
func (c *Client) AcceptPeerConn(conn net.Conn) {
	// Refuse once the adopted connections have been dropped for a teardown. The
	// PLC re-dials after every drop, which is exactly when teardown runs, and
	// tearDownAndReset does closePeerConns() and then waits on this WaitGroup:
	// adopting here would append to a slice nobody will close again (a leaked fd
	// and a half-open socket the PLC counts against its one-connection-per-IP
	// limit), block that wait forever in io.ReadFull, and Add to a WaitGroup whose
	// Wait may already be running at zero, which panics the process.
	//
	// The Add stays inside the same critical section as the flag check, so it
	// cannot slip past a closePeerConns that has already returned.
	c.peerMu.Lock()
	if c.peerClosed || (c.ctx != nil && c.ctx.Err() != nil) {
		c.peerMu.Unlock()
		c.logger.Debug("refusing an inbound PLC connection: this client is being torn down (the PLC will dial again)",
			"remote", conn.RemoteAddr().String())
		_ = conn.Close()
		return
	}
	c.peerConns = append(c.peerConns, conn)
	c.waitGroup.Add(1)
	c.peerMu.Unlock()

	c.logger.Info("adopted an inbound connection from the PLC (peer-route behaviour)",
		"remote", conn.RemoteAddr().String())
	go func() {
		defer c.waitGroup.Done()
		// This reader owns the socket. readFrames returns on ctx.Done() and on any
		// read error without touching conn, so without this every connection the
		// PLC ever opened would cost an fd for the life of the Client.
		defer func() {
			_ = conn.Close()
			c.forgetPeerConn(conn)
		}()
		c.readFrames(conn, false)
	}()
}

// forgetPeerConn drops one adopted connection from the list. Without it the slice
// only ever grows: a device that re-dials on each of its own drops would
// accumulate an entry per connection for the life of the Client.
func (c *Client) forgetPeerConn(conn net.Conn) {
	c.peerMu.Lock()
	defer c.peerMu.Unlock()
	for i, existing := range c.peerConns {
		if existing == conn {
			c.peerConns = append(c.peerConns[:i], c.peerConns[i+1:]...)
			return
		}
	}
}

// closePeerConns drops every adopted inbound connection and refuses further ones.
// Called from Close and from tearDownAndReset so a reader blocked on one cannot
// hold up the wait for this Client's workers.
func (c *Client) closePeerConns() {
	c.peerMu.Lock()
	conns := c.peerConns
	c.peerConns = nil
	c.peerClosed = true
	c.peerMu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

// readFrames pumps AMS frames from conn into the transport's queues.
//
// primary distinguishes our own connection from an adopted inbound one. Losing
// the primary connection means the transport is down and reconnect must fire;
// losing an inbound one does not — the PLC can simply dial again, and treating it
// as a drop would tear down a working session.
func (c *Client) readFrames(conn net.Conn, primary bool) {
	reader := bufio.NewReader(conn)
	const maxAMSPacket = 4 * 1024 * 1024
	var hdrBytes [6]byte
	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info("exit listen")
			return
		default:
		}
		if _, err := io.ReadFull(reader, hdrBytes[:]); err != nil {
			select {
			case <-c.ctx.Done():
				return
			default:
			}
			if !primary {
				c.logger.Debug("inbound PLC connection closed", "error", err)
				return
			}
			c.logDropVerdict(err)
			c.callOnDrop()
			return
		}
		var tcpHeader amsTCPHeader
		if err := binary.Read(bytes.NewReader(hdrBytes[:]), binary.LittleEndian, &tcpHeader); err != nil {
			c.logger.Error("listen header decode error, transport down", "error", err, "primary", primary)
			if primary {
				c.callOnDrop()
			}
			return
		}
		if tcpHeader.Length > maxAMSPacket {
			c.logger.Error("AMS packet length exceeds sanity limit, transport down",
				"length", tcpHeader.Length, "primary", primary)
			if primary {
				c.callOnDrop()
			}
			return
		}
		data := make([]byte, tcpHeader.Length)
		select {
		case <-c.ctx.Done():
			return
		default:
		}
		if _, err := io.ReadFull(reader, data); err != nil {
			select {
			case <-c.ctx.Done():
				return
			default:
			}
			if !primary {
				c.logger.Debug("inbound PLC connection failed mid-frame", "error", err)
				return
			}
			c.logger.Log(c.ctx, c.transportFaultLevel(), "listen body read error, transport down", "error", err)
			c.callOnDrop()
			return
		}
		// A full frame arrived. Counted here rather than in listen because this is
		// the only decode loop and it serves both socket directions; see
		// framesPrimary for why the split matters.
		if primary {
			c.framesPrimary.Add(1)
		} else {
			c.framesPeer.Add(1)
		}
		if tcpHeader.System > 0 {
			select {
			case c.tx.systemResponse <- data:
			case <-c.ctx.Done():
				return
			}
		} else {
			select {
			case c.tx.recvQueue <- data:
			case <-c.ctx.Done():
				return
			default:
				c.logger.Warn("recvQueue full, dropping inbound packet (PLC overrun or slow handler)",
					"queue_size", recvQueueSize,
					"workers", recvWorkerCount,
					"packet_bytes", len(data))
			}
		}
	}
}

func (c *Client) recvWorker() {
	defer c.waitGroup.Done()
	for {
		select {
		case <-c.ctx.Done():
			return
		case data, ok := <-c.tx.recvQueue:
			if !ok {
				return
			}
			c.handleReceive(c.ctx, data)
		}
	}
}

func (c *Client) handleReceive(ctx context.Context, data []byte) {
	c.logger.Log(context.Background(), LevelTrace, "in read")
	if len(data) < 32 {
		c.logger.Error("header too short")
		return
	}
	buff := bytes.NewBuffer(data)
	header := AMSHeader{}
	if err := binary.Read(buff, binary.LittleEndian, &header); err != nil {
		c.logger.Error("Error parsing header", "error", err)
		return
	}
	c.logger.Log(context.Background(), LevelTrace, "header info", "header", header)
	adsData := data[32:]
	if len(adsData) != int(header.Length) {
		c.logger.Error("Error parsing body")
		return
	}
	switch header.Command {
	case CommandIDDeviceNotification:
		if err := c.deviceNotification(ctx, adsData); err != nil {
			c.logger.Error("device notification decode failed", "error", err)
		}
	default:
		c.logger.Log(context.Background(), LevelTrace, "default receive")
		c.tx.activeRequestLock.Lock()
		response, ok := c.tx.activeRequests[header.InvokeID]
		c.tx.activeRequestLock.Unlock()
		if ok {
			select {
			case <-ctx.Done():
				c.logger.Info("receive channel timed out",
					"id", header.InvokeID, "command", header.Command)
				return
			case response <- amsReply{data: adsData, amsErr: ReturnCode(header.ErrorCode)}:
				c.logger.Log(context.Background(), LevelTrace, "Successfully delivered answer",
					"id", header.InvokeID, "command", header.Command)
			}
		} else {
			// Stale invokeID. Expected during reconnect cleanup or shutdown:
			// activeRequests was cleared, late PLC responses arrive after.
			// Always Debug — Client has no FSM context to distinguish more
			// precisely; production debugging uses the InvokeID + command
			// pair to spot true protocol bugs.
			c.logger.Debug("received packet with unknown invokeID",
				"invokeID", header.InvokeID,
				"command", header.Command)
		}
	}
}

func (c *Client) transmitWorker() {
	defer c.waitGroup.Done()
	c.tx.connMu.Lock()
	conn := c.tx.connection
	c.tx.connMu.Unlock()
	if conn == nil {
		c.logger.Debug("transmitWorker: no connection to write to")
		return
	}
	writer := bufio.NewWriter(conn)
	for {
		select {
		case <-c.ctx.Done():
			c.logger.Debug("Exit transmitWorker")
			return
		case data := <-c.tx.sendChannel:
			c.logger.Log(context.Background(), LevelTrace, fmt.Sprintf("Sending %d bytes", len(data)))
			if _, err := writer.Write(data); err != nil {
				c.logger.Log(c.ctx, c.transportFaultLevel(), "error sending data on conn, transport down", "error", err)
				c.callOnDrop()
				return
			}
			if err := writer.Flush(); err != nil {
				c.logger.Log(c.ctx, c.transportFaultLevel(), "error flushing data on conn, transport down", "error", err)
				c.callOnDrop()
				return
			}
		}
	}
}

func (c *Client) deviceNotification(ctx context.Context, in []byte) error {
	var stream NotificationStream
	var header StampHeader
	var sample NotificationSample
	var content []byte
	data := bytes.NewBuffer(in)
	if err := binary.Read(data, binary.LittleEndian, &stream); err != nil {
		return fmt.Errorf("unable to read notification: %w", err)
	}
	for i := uint32(0); i < stream.Stamps; i++ {
		if err := binary.Read(data, binary.LittleEndian, &header); err != nil {
			return fmt.Errorf("error reading stamp header: %w", err)
		}
		for j := uint32(0); j < header.Samples; j++ {
			if err := binary.Read(data, binary.LittleEndian, &sample); err != nil {
				return fmt.Errorf("error reading notification sample: %w", err)
			}
			if sample.Size > uint32(data.Len()) {
				return fmt.Errorf("notification sample size %d exceeds remaining data %d",
					sample.Size, data.Len())
			}
			content = make([]byte, sample.Size)
			n, err := data.Read(content)
			if err != nil {
				return fmt.Errorf("error reading notification content: %w", err)
			}
			if n != int(sample.Size) {
				return fmt.Errorf("short read on notification content: got %d of %d bytes",
					n, sample.Size)
			}
			c.dispatchNotification(ctx, sample.Handle, header.Timestamp, content)
		}
	}
	return nil
}

// ReleaseHandle releases a symbol handle previously acquired via
// GetHandleByName. Wraps Write to GroupSymbolReleaseHandle so the
// Beckhoff-equivalent surface includes a symmetric release primitive.
func (c *Client) ReleaseHandle(ctx context.Context, handle uint32) error {
	handleBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(handleBytes, handle)
	return c.Write(ctx, uint32(GroupSymbolReleaseHandle), 0, handleBytes)
}

// send is the local-mode handshake primitive. NOT safe for concurrent use —
// it consumes from the shared systemResponse channel. Used by Session for
// the local AMS handshake during Connect/Reconnect.
func (c *Client) send(data []byte) ([]byte, error) {
	c.tx.currentRequest.Add(1)
	c.tx.chanMu.RLock()
	sendCh := c.tx.sendChannel
	sysCh := c.tx.systemResponse
	c.tx.chanMu.RUnlock()
	dropped := c.dropped
	ctx, cancel := context.WithTimeout(c.ctx, c.requestTimeout)
	defer cancel()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("send aborted, context canceled: %w", ctx.Err())
	case <-dropped:
		return nil, ErrTransportClosed
	case sendCh <- data:
	}
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			err := fmt.Errorf("request aborted, deadline exceeded: %w", ctx.Err())
			c.logger.Log(ctx, c.transportFaultLevel(), "send aborted due to timeout", "error", err)
			return nil, err
		}
		err := fmt.Errorf("request aborted, shutdown initiated: %w", ctx.Err())
		c.logger.Log(ctx, c.transportFaultLevel(), "send aborted due to shutdown", "error", err)
		return nil, err
	case <-dropped:
		grace := time.NewTimer(droppedResponseGrace)
		defer grace.Stop()
		select {
		case response := <-sysCh:
			return response, nil
		case <-grace.C:
			return nil, ErrTransportClosed
		case <-ctx.Done():
			return nil, ErrTransportClosed
		}
	case response := <-sysCh:
		return response, nil
	}
}

// sendRequest is the single-shot RPC primitive used by every Client RPC
// method. Encodes the AMS frame, registers a per-invoke response channel,
// pushes the frame to the transmit worker, and waits for the response or
// context cancel / timeout.
//
// The caller's ctx is merged with c.requestTimeout via context.WithTimeout;
// whichever fires first cancels the wait. Pass context.Background() to
// preserve the v2.1 "timeout-only" semantic.
//
// Returns ErrTransportClosed immediately if the transport is known dead
// (Close called or drop detected). Otherwise no retry — drops mid-flight
// surface as context.Canceled / DeadlineExceeded; Session wraps this with
// wait-for-reconnect retry semantics in its own helpers.
func (c *Client) sendRequest(ctx context.Context, command CommandID, data []byte) ([]byte, error) {
	return c.sendRequestTo(ctx, c.target, command, data)
}

// sendRequestTo is sendRequest addressed to an explicit AMS target — used to reach
// another port on the same device (the system service) over this one connection.
func (c *Client) sendRequestTo(ctx context.Context, target AMSAddress, command CommandID, data []byte) ([]byte, error) {
	if c.tx.disconnected.Load() {
		return nil, ErrTransportClosed
	}
	c.tx.activeRequestLock.Lock()
	id := c.tx.currentRequest.Add(1)
	responseCh := make(chan amsReply, 1)
	c.tx.activeRequests[id] = responseCh
	c.tx.activeRequestLock.Unlock()
	defer func() {
		c.tx.activeRequestLock.Lock()
		delete(c.tx.activeRequests, id)
		c.tx.activeRequestLock.Unlock()
	}()
	c.logger.Log(context.Background(), LevelTrace, "encoding packet",
		"command", command, "data", data, "id", id)

	pack, err := c.encodeTo(target, command, data, id)
	if err != nil {
		c.logger.Error("Error during sendRequest encode", "error", err)
		return nil, err
	}
	c.tx.chanMu.RLock()
	sendCh := c.tx.sendChannel
	c.tx.chanMu.RUnlock()
	dropped := c.dropped
	// Merge caller ctx with c.requestTimeout. ctx==nil falls back to the
	// Client's own ctx so callers passing context.Background() still get
	// the configured request timeout AND respect Close-driven cancel.
	if ctx == nil {
		ctx = c.ctx
	}
	ctx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			c.logger.Log(ctx, c.transportFaultLevel(), "sendRequest aborted due to timeout")
		} else {
			c.logger.Info("sendRequest aborted due to shutdown")
		}
		return nil, ctx.Err()
	case <-dropped:
		return nil, ErrTransportClosed
	case sendCh <- pack:
	}
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			c.logger.Log(ctx, c.transportFaultLevel(), "sendRequest aborted due to timeout")
		} else {
			c.logger.Info("sendRequest aborted due to shutdown")
		}
		return nil, ctx.Err()
	case <-dropped:
		// The transport died while this request was in flight. Give a reply that
		// is already in the receive pipeline a bounded moment to land before
		// giving up — see droppedResponseGrace.
		grace := time.NewTimer(droppedResponseGrace)
		defer grace.Stop()
		select {
		case reply := <-responseCh:
			return reply.payload()
		case <-grace.C:
			return nil, ErrTransportClosed
		case <-ctx.Done():
			return nil, ErrTransportClosed
		}
	case reply := <-responseCh:
		return reply.payload()
	}
}

func (c *Client) dispatchNotification(ctx context.Context, handle uint32, ts uint64, content []byte) {
	c.notifyMu.RLock()
	fn := c.notify
	c.notifyMu.RUnlock()
	if fn == nil {
		c.logger.Debug("DeviceNotification dropped (no handler installed)", "handle", handle)
		return
	}
	fn(ctx, handle, ts, content)
}

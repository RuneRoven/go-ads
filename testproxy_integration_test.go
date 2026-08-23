//go:build integration

package ads

import (
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testproxy_integration_test.go — a loopback TCP proxy in front of a real PLC, so
// link faults become method calls instead of someone walking to the rack.
//
// Why bother when a stub server can also close a socket: the stub is not a PLC.
// It has no notification table that survives the outage, no route entry, no
// symbol version, and no opinion about a client that reconnects with the same
// source NetID inside the route-idle timeout. Those are exactly the things the
// recovery paths interact with, and they are where the last three real bugs lived.
// With the proxy the PLC stays completely untouched — only the wire between us
// dies — which is the one scenario a power cycle cannot produce.
//
// The session must reach the PLC over TCP only: no WithRoute (route registration
// is UDP straight to sess.ip, which would be the proxy) and TargetCheckOff (the
// identify probe likewise). Both are safe here because the route for this host's
// NetID already exists on the lab PLCs, and the outbound connection still leaves
// from this machine, so the PLC sees the source IP its route entry names.

type linkState int32

const (
	// linkUp forwards normally.
	linkUp linkState = iota
	// linkBlackhole holds sockets open and stops moving bytes in either
	// direction, and accepts new connections without dialing the PLC. This is
	// what an unplugged cable looks like from the client: requests time out
	// rather than being refused, and a redial hangs instead of failing fast.
	linkBlackhole
	// linkCut closes live connections and immediately closes anything new. The
	// client sees EOF now rather than a timeout later — a switch dropping the
	// port, or the PLC's stack resetting.
	linkCut
)

type tcpProxy struct {
	t        *testing.T
	ln       net.Listener
	target   string
	state    atomic.Int32
	wg       sync.WaitGroup
	closed   chan struct{}
	connMu   sync.Mutex
	conns    map[net.Conn]struct{}
	accepted atomic.Int64
	toPLC    atomic.Int64
	toClient atomic.Int64
}

// startTCPProxy listens on loopback and forwards to target ("host:port").
func startTCPProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &tcpProxy{
		t:      t,
		ln:     ln,
		target: target,
		closed: make(chan struct{}),
		conns:  map[net.Conn]struct{}{},
	}
	p.wg.Add(1)
	go p.acceptLoop()
	t.Cleanup(p.close)
	return p
}

func (p *tcpProxy) host() string { return "127.0.0.1" }

func (p *tcpProxy) port() int {
	addr, ok := p.ln.Addr().(*net.TCPAddr)
	if !ok {
		p.t.Fatalf("unexpected proxy addr type %T", p.ln.Addr())
	}
	return addr.Port
}

// blackhole simulates a pulled cable: bytes stop moving, sockets stay open.
func (p *tcpProxy) blackhole() {
	p.state.Store(int32(linkBlackhole))
	p.t.Logf("[proxy] link blackholed (bytes stop, sockets held)")
}

// cut simulates a port going down: live connections die now.
func (p *tcpProxy) cut() {
	p.state.Store(int32(linkCut))
	p.connMu.Lock()
	n := len(p.conns)
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
	p.connMu.Unlock()
	p.t.Logf("[proxy] link cut (%d connection(s) closed)", n)
}

// restore puts the link back. Existing connections are not resurrected — a real
// link coming back does not revive a dead TCP session either, which is the point:
// the library has to redial.
func (p *tcpProxy) restore() {
	p.state.Store(int32(linkUp))
	p.connMu.Lock()
	for c := range p.conns {
		_ = c.Close() // anything accepted while down is unusable
	}
	p.conns = map[net.Conn]struct{}{}
	p.connMu.Unlock()
	p.t.Logf("[proxy] link restored")
}

func (p *tcpProxy) stats() (accepted, toPLC, toClient int64) {
	return p.accepted.Load(), p.toPLC.Load(), p.toClient.Load()
}

func (p *tcpProxy) close() {
	select {
	case <-p.closed:
		return
	default:
	}
	close(p.closed)
	_ = p.ln.Close()
	p.connMu.Lock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
	p.connMu.Unlock()
	p.wg.Wait()
}

func (p *tcpProxy) acceptLoop() {
	defer p.wg.Done()
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.accepted.Add(1)
		p.wg.Add(1)
		go p.serve(client)
	}
}

func (p *tcpProxy) serve(client net.Conn) {
	defer p.wg.Done()
	defer func() { _ = client.Close() }()

	switch linkState(p.state.Load()) {
	case linkCut:
		// Refuse instantly: EOF on the client's first read.
		return
	case linkBlackhole:
		// Accept and hold. No upstream connection at all, so the client's
		// request simply never gets an answer — a redial into a dead link.
		p.track(client)
		defer p.untrack(client)
		p.holdUntilUp(client)
		return
	}

	upstream, err := net.DialTimeout("tcp4", p.target, 5*time.Second)
	if err != nil {
		p.t.Logf("[proxy] upstream dial %s failed: %v", p.target, err)
		return
	}
	defer func() { _ = upstream.Close() }()
	p.track(client)
	p.track(upstream)
	defer p.untrack(client)
	defer p.untrack(upstream)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.pump(client, upstream, &p.toPLC) }()
	go func() { defer wg.Done(); p.pump(upstream, client, &p.toClient) }()
	wg.Wait()
}

// pump copies src -> dst while the link is up. While blackholed it keeps reading
// and discards, so the sender sees a healthy TCP window and no answer — the
// asymmetry that makes a pulled cable look different from a closed port.
func (p *tcpProxy) pump(src, dst net.Conn, counter *atomic.Int64) {
	buf := make([]byte, 32*1024)
	for {
		select {
		case <-p.closed:
			return
		default:
		}
		_ = src.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, err := src.Read(buf)
		if n > 0 {
			if linkState(p.state.Load()) == linkUp {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return
				}
				counter.Add(int64(n))
			}
			// else: swallowed on purpose
		}
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			if err == io.EOF {
				return
			}
			return
		}
	}
}

// holdUntilUp keeps a connection accepted-but-unserved until the link returns or
// the proxy shuts down.
func (p *tcpProxy) holdUntilUp(c net.Conn) {
	for {
		select {
		case <-p.closed:
			return
		case <-time.After(200 * time.Millisecond):
		}
		if linkState(p.state.Load()) == linkUp {
			// Force the client to redial into a serving path.
			_ = c.Close()
			return
		}
		// Drain and discard so the client's writes do not block forever.
		_ = c.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		var scratch [4096]byte
		_, _ = c.Read(scratch[:])
	}
}

func (p *tcpProxy) track(c net.Conn) {
	p.connMu.Lock()
	p.conns[c] = struct{}{}
	p.connMu.Unlock()
}

func (p *tcpProxy) untrack(c net.Conn) {
	p.connMu.Lock()
	delete(p.conns, c)
	p.connMu.Unlock()
}

func (p *tcpProxy) String() string {
	return fmt.Sprintf("proxy %s:%d -> %s", p.host(), p.port(), p.target)
}

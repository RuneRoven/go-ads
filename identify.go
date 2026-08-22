package ads

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"
)

// Target discovery over the AMS router's identify service.
//
// A caller normally has to know the PLC's AmsNetId before it can talk to it,
// and a wrong one is the single most common ADS misconfiguration: the router
// accepts the TCP socket and then silently drops every request, which looks
// nothing like an addressing mistake. The router will however tell you its own
// NetID if asked over UDP — the same port and framing used to register a route
// (48899), with service id 1 instead of 6.
//
// The request is read-only. It registers nothing, needs no route and no
// credentials, and it answers before any route exists, which is what makes it
// usable to bootstrap a connection rather than only to inspect one.
//
// Verified against TwinCAT 2.10 (CX), TwinCAT 3.1.4024 (CX) and TwinCAT 3.1.4026
// on TC/RTOS. All three answer a request carrying a zero source NetID, so
// discovery needs no local configuration at all.
const (
	routeServiceIdentify = 1

	// Response tags. Tag 5 is the computer name in both directions (see
	// tagComputerName, used when registering a route). Tag 3 carries the
	// TwinCAT version. Tag 4 holds a system-info blob whose layout differs per
	// platform — surfaced raw in RemoteIdentity.Tags rather than guessed at.
	tagSystemVersion uint16 = 3

	// identifyTimeout is the total budget for a probe, retransmits included.
	identifyTimeout = 3 * time.Second
	// identifyAttempts is how many times the request is sent inside that budget.
	// UDP has no delivery guarantee and this runs on plant networks: a single
	// dropped datagram was observed failing NewSession outright, which is a poor
	// trade for one lost packet.
	identifyAttempts = 3
	// identifyReadBuf is a full UDP datagram. Sized so a legitimate response can
	// never be silently truncated and then reported as a malformed one.
	identifyReadBuf = 64 * 1024
)

// RemoteIdentity is what a TwinCAT AMS router reports about ITSELF.
// Obtained with IdentifyRemote.
//
// Read AMS carefully: it is the identity of the router answering at that IP,
// not "the NetID of the PLC behind it". An AMS router routes to many NetIDs, so
// on an embedded target (a CX, where the router and the runtime are the same
// device) this is the PLC's NetID, but on an engineering PC or a gateway
// fronting other PLCs it is that machine's NetID and the PLC you want is one
// of the entries in its route table. Nothing in the response distinguishes the
// two cases — HostName and Version describe the responder, and NetIDs are not
// derived from the IP, so they cannot be cross-checked either.
type RemoteIdentity struct {
	// AMS is the router's own address. The port is the router's (10000), NOT a
	// PLC runtime port — see RuntimePort.
	AMS AMSAddress
	// HostName is the device's computer name, e.g. "CX-4285CB". Empty if the
	// device did not report one.
	HostName string
	// Major, Minor and Build are the reported TwinCAT version, e.g. 3, 1, 4024.
	// Zero if the device did not report a version.
	Major uint8
	Minor uint8
	Build uint16
	// Tags is every tag exactly as received, keyed by Beckhoff tag id,
	// including the ones this package does not interpret. Present so callers
	// can inspect platform details (tag 4 embeds "TC/RTOS" on that platform)
	// without this package pretending to decode a layout it has not verified.
	Tags map[uint16][]byte
}

// Version renders the reported TwinCAT version, e.g. "3.1.4024".
func (id RemoteIdentity) Version() string {
	return fmt.Sprintf("%d.%d.%d", id.Major, id.Minor, id.Build)
}

// RuntimePort returns the conventional AMS port of the FIRST PLC runtime for
// the reported TwinCAT major version: PortR0PlcRts1 (801) on TwinCAT 2,
// PortR0PlcTc3 (851) otherwise.
//
// This is a convention, not a discovered value — the identify service reports
// the router's own port and never a runtime port. A TwinCAT 2 project with
// several runtimes uses 811, 821, 831; a TwinCAT 3 one uses 852, 853, … and a
// caller targeting any of those must say so explicitly.
func (id RemoteIdentity) RuntimePort() uint16 {
	if id.Major == 2 {
		return uint16(PortR0PlcRts1)
	}
	return uint16(PortR0PlcTc3)
}

// IdentifyRemote asks the device at host for its own AMS NetID and system
// details. host is an IP or hostname, optionally with a port ("10.0.0.5:6499")
// for a device reached through NAT port forwarding; bare hosts use the
// protocol's own UDP port.
//
// Read-only — see the block comment above. Honors ctx's deadline if it is
// sooner than the default 3s.
//
// The answer describes the ROUTER at that address, which is the PLC only when
// the two are the same device. See RemoteIdentity.
func IdentifyRemote(ctx context.Context, host string) (RemoteIdentity, error) {
	return IdentifyRemoteWithLogger(ctx, getDefaultLogger(), host)
}

// IdentifyRemoteWithLogger is IdentifyRemote with an explicit logger.
func IdentifyRemoteWithLogger(ctx context.Context, logger *slog.Logger, host string) (RemoteIdentity, error) {
	h, port := splitHostRouterPort(host)
	return identifyRemoteFrom(ctx, logger, nil, h, port)
}

// splitHostRouterPort accepts either a bare host or host:port and returns the
// host with the router UDP port to use. A PLC behind NAT answers on a forwarded
// port that cannot be derived from anything else, and these standalone helpers
// take no port argument — so they read it off the host string rather than
// forcing callers into the Session API for a one-shot probe.
func splitHostRouterPort(host string) (string, int) {
	h, portStr, err := net.SplitHostPort(host)
	if err != nil {
		return host, routePort
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return host, routePort
	}
	return h, port
}

// identifyRemoteFrom is IdentifyRemoteWithLogger with an explicit local source
// IP, so a session that pins its outbound interface probes over the same one.
// Otherwise discovery and verification can traverse a different NIC than the
// ADS traffic they are meant to describe. localIP nil keeps OS-default routing.
// port is the UDP port to probe; production always passes routePort. It exists
// so tests can run a stub responder on an ephemeral port rather than needing the
// protocol's fixed port to be free on the machine.
func identifyRemoteFrom(ctx context.Context, logger *slog.Logger, localIP net.IP, host string, port int) (RemoteIdentity, error) {
	if logger == nil {
		logger = getDefaultLogger()
	}
	if host == "" {
		return RemoteIdentity{}, fmt.Errorf("identify: host must be set")
	}
	if err := ctx.Err(); err != nil {
		return RemoteIdentity{}, fmt.Errorf("identify %s: %w", host, err)
	}

	// Resolve through a ctx-aware resolver: net.ResolveUDPAddr ignores the
	// context, so a hostname could block on the system resolver well past the
	// caller's deadline — inside NewSession, which advertises a few milliseconds.
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return RemoteIdentity{}, fmt.Errorf("identify %s: resolve: %w", host, err)
	}
	var addr *net.UDPAddr
	for _, ip := range ips {
		if v4 := ip.IP.To4(); v4 != nil {
			addr = &net.UDPAddr{IP: v4, Port: port}
			break
		}
	}
	if addr == nil {
		return RemoteIdentity{}, fmt.Errorf("identify %s: no IPv4 address (the AMS router protocol is IPv4-only)", host)
	}
	var laddr *net.UDPAddr
	if localIP != nil {
		laddr = &net.UDPAddr{IP: localIP}
	}
	conn, err := net.DialUDP("udp4", laddr, addr)
	if err != nil {
		return RemoteIdentity{}, fmt.Errorf("identify %s: dial UDP: %w", host, err)
	}
	defer func() { _ = conn.Close() }()

	// Random invokeID so a stray or spoofed datagram cannot be mistaken for
	// our answer — same reasoning as AddRemoteRouteWithLogger.
	var invokeIDBuf [4]byte
	if _, err := cryptorand.Read(invokeIDBuf[:]); err != nil {
		return RemoteIdentity{}, fmt.Errorf("identify %s: generate invokeID: %w", host, err)
	}
	invokeID := binary.LittleEndian.Uint32(invokeIDBuf[:])

	deadline := time.Now().Add(identifyTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	packet := buildIdentifyPacket(invokeID)
	respBuf := make([]byte, identifyReadBuf)

	// Retransmit the same request — same invokeID, so a late reply to an earlier
	// attempt still matches — and within each attempt keep reading until the
	// window closes, discarding datagrams that are not ours. Port 48899 also
	// carries route registration, so a stray or late reply from the same host
	// must not consume our only read.
	var lastErr error
	for attempt := 1; attempt <= identifyAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return RemoteIdentity{}, fmt.Errorf("identify %s: %w", host, err)
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		// Split what is left over the attempts still to come.
		window := remaining / time.Duration(identifyAttempts-attempt+1)
		if attempt == identifyAttempts {
			window = remaining
		}
		if _, err := conn.Write(packet); err != nil {
			return RemoteIdentity{}, fmt.Errorf("identify %s: send: %w", host, err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(window)); err != nil {
			return RemoteIdentity{}, fmt.Errorf("identify %s: set read deadline: %w", host, err)
		}
		for {
			n, err := conn.Read(respBuf)
			if err != nil {
				lastErr = err
				break // window expired: retransmit
			}
			if n == len(respBuf) {
				return RemoteIdentity{}, fmt.Errorf("identify %s: response of %d bytes filled the read buffer", host, n)
			}
			if !identifyResponseIsOurs(respBuf[:n], invokeID) {
				logger.Debug("identify: ignoring unrelated datagram on port 48899",
					"host", host, "bytes", n)
				continue
			}
			id, err := parseIdentifyResponse(respBuf[:n], invokeID)
			if err != nil {
				// Ours by cookie/invokeID/service but undecodable: a real fault,
				// not a stray, so do not keep waiting on it.
				return RemoteIdentity{}, fmt.Errorf("identify %s: %w", host, err)
			}
			if attempt > 1 {
				logger.Debug("identify succeeded after a retransmit", "host", host, "attempt", attempt)
			}
			logger.Debug("identified remote",
				"host", host, "netID", id.AMS.NetIDString(),
				"hostName", id.HostName, "twinCAT", id.Version())
			return id, nil
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no response within %v", identifyTimeout)
	}
	return RemoteIdentity{}, fmt.Errorf("identify %s: no answer after %d attempts: %w", host, identifyAttempts, lastErr)
}

// identifyResponseIsOurs reports whether a datagram is the answer to our
// request, checking only the header fields that identify it. Anything else on
// this port belongs to someone else and must be skipped rather than treated as
// a malformed reply.
func identifyResponseIsOurs(data []byte, invokeID uint32) bool {
	if len(data) < 24 {
		return false
	}
	return binary.LittleEndian.Uint32(data[0:]) == routeCookie &&
		binary.LittleEndian.Uint32(data[4:]) == invokeID &&
		binary.LittleEndian.Uint32(data[8:]) == (0x80000000|routeServiceIdentify)
}

// buildIdentifyPacket builds the 24-byte, tag-less identify request. The source
// AmsAddr is left zero: every TwinCAT version tested answers regardless, so
// discovery does not need to know the local NetID — which matters, because the
// local NetID is often derived from a connection that does not exist yet.
func buildIdentifyPacket(invokeID uint32) []byte {
	packet := make([]byte, 24)
	binary.LittleEndian.PutUint32(packet[0:], routeCookie)
	binary.LittleEndian.PutUint32(packet[4:], invokeID)
	binary.LittleEndian.PutUint32(packet[8:], routeServiceIdentify)
	// packet[12:20] source AmsAddr — zero NetID, zero port.
	// packet[20:24] tagCount — zero.
	return packet
}

// parseIdentifyResponse decodes an identify response.
// Layout: cookie(4) + invokeID(4) + serviceId(4) + AmsAddr(8) + tagCount(4) + tags,
// where each tag is id(2) + length(2) + data. Unlike a route response, the
// AmsAddr here is the REMOTE's own address rather than an echo of ours — that
// is the whole point of the service.
func parseIdentifyResponse(data []byte, expectedInvokeID uint32) (RemoteIdentity, error) {
	if len(data) < 24 {
		return RemoteIdentity{}, fmt.Errorf("identify response too short: %d bytes", len(data))
	}
	if cookie := binary.LittleEndian.Uint32(data[0:]); cookie != routeCookie {
		return RemoteIdentity{}, fmt.Errorf("unexpected identify response cookie: 0x%08X", cookie)
	}
	if got := binary.LittleEndian.Uint32(data[4:]); got != expectedInvokeID {
		return RemoteIdentity{}, fmt.Errorf("identify response invokeID mismatch: got 0x%08X, expected 0x%08X (possible spoof or stray datagram)", got, expectedInvokeID)
	}
	if svc := binary.LittleEndian.Uint32(data[8:]); svc != (0x80000000 | routeServiceIdentify) {
		return RemoteIdentity{}, fmt.Errorf("unexpected identify response serviceId: 0x%08X", svc)
	}

	id := RemoteIdentity{Tags: map[uint16][]byte{}}
	copy(id.AMS.NetID[:], data[12:18])
	id.AMS.Port = binary.LittleEndian.Uint16(data[18:20])
	if id.AMS.NetID == [6]byte{} {
		return RemoteIdentity{}, fmt.Errorf("identify response reported a zero NetID")
	}

	tagCount := binary.LittleEndian.Uint32(data[20:24])
	offset := 24
	for i := uint32(0); i < tagCount; i++ {
		if offset+4 > len(data) {
			return RemoteIdentity{}, fmt.Errorf("identify response truncated: incomplete tag %d header", i)
		}
		tid := binary.LittleEndian.Uint16(data[offset:])
		tlen := int(binary.LittleEndian.Uint16(data[offset+2:]))
		offset += 4
		if offset+tlen > len(data) {
			return RemoteIdentity{}, fmt.Errorf("identify response truncated: tag %d data exceeds response", tid)
		}
		value := make([]byte, tlen)
		copy(value, data[offset:offset+tlen])
		offset += tlen
		id.Tags[tid] = value

		switch {
		case tid == tagComputerName:
			id.HostName = trimCString(value)
		case tid == tagSystemVersion && tlen >= 4:
			id.Major = value[0]
			id.Minor = value[1]
			id.Build = binary.LittleEndian.Uint16(value[2:4])
		}
	}
	return id, nil
}

// trimCString cuts a NUL-terminated string at its first NUL. Beckhoff pads
// name tags with trailing NULs, and keeping them would put them in log output
// and comparisons.
func trimCString(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}

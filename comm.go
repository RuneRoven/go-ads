package ads

import (
	"errors"
	"io"
	"net"
	"syscall"
)

// isRouteHintErr returns true if err indicates a likely missing-AMS-route condition
// (PLC closed the TCP connection because no route exists for our NetID).
// Detects wrapped io.EOF and ECONNRESET via the standard errors.Is/As mechanism.
func isRouteHintErr(err error) bool {
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
	if errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	return false
}

// send delegates to the underlying Client. Local-mode handshake only —
// the only caller is Session.localHandshake. Other RPC paths now bypass
// Session and call methods directly on s.client.
func (conn *Session) send(data []byte) ([]byte, error) {
	if conn.client == nil {
		return nil, ErrDisconnected
	}
	return conn.client.send(data)
}

// listen, recvWorker, handleReceive, transmitWorker, sendRequest,
// sendRequestOnce were migrated onto *Client in Phase 5.a-dial / 5.b.0.
// See client.go.

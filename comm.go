package ads

import (
	"context"
	"errors"
	"fmt"
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

// send delegates to the underlying Client. Local-mode handshake only.
func (conn *Session) send(data []byte) ([]byte, error) {
	if conn.client == nil {
		return nil, ErrDisconnected
	}
	return conn.client.send(data)
}

func (conn *Session) sendRequest(command CommandID, data []byte) (response []byte, err error) {
	if conn == nil {
		return nil, fmt.Errorf("sendRequest called on nil connection")
	}
	const maxAttempts = 2
	for attempt := 0; attempt < maxAttempts; attempt++ {
		response, err = conn.sendRequestOnce(command, data)
		if err == nil {
			return response, nil
		}
		// Only retry on context.Canceled (reconnect killed our context),
		// never on DeadlineExceeded (that's the caller's RequestTimeout).
		if !errors.Is(err, context.Canceled) {
			return nil, err
		}
		// Don't retry if the connection is permanently closed.
		if conn.isClosed() {
			return nil, err
		}
		// Only retry if a reconnect is actually in progress.
		if !conn.isReconnecting() {
			return nil, err
		}
		// Wait for reconnect to finish before retrying.
		conn.lifecycle.reconnectMu.Lock()
		ch := conn.lifecycle.reconnectDone
		conn.lifecycle.reconnectMu.Unlock()
		if ch == nil {
			return nil, err
		}
		conn.logger.Info("sendRequest retrying after reconnect",
			"attempt", attempt+1,
			"command", command)
		select {
		case <-ch:
			// Reconnect finished — loop will retry if connection is healthy.
			if conn.isTransportDown() {
				return nil, ErrDisconnected
			}
		case <-conn.lifecycle.closedCh:
			return nil, fmt.Errorf("connection closed while waiting for reconnect: %w", err)
		}
	}
	return nil, err
}

// sendRequestOnce wraps the raw Client.sendRequest with Session-level
// wait-for-reconnect semantics. If the transport is down at entry, it
// waits on the active reconnect channel and re-checks before giving up.
// Otherwise it forwards directly to Client.sendRequest.
func (conn *Session) sendRequestOnce(command CommandID, data []byte) ([]byte, error) {
	if conn.client == nil {
		return nil, ErrDisconnected
	}
	if conn.isTransportDown() {
		conn.lifecycle.reconnectMu.Lock()
		ch := conn.lifecycle.reconnectDone
		conn.lifecycle.reconnectMu.Unlock()
		if ch == nil {
			return nil, ErrDisconnected
		}
		conn.logger.Debug("sendRequest waiting for reconnect to complete")
		conn.lifecycle.ctxMu.RLock()
		ctxDone := conn.lifecycle.ctx.Done()
		conn.lifecycle.ctxMu.RUnlock()
		select {
		case <-ch:
			if conn.isTransportDown() {
				return nil, ErrDisconnected
			}
		case <-ctxDone:
			// Reconnect cancelled our ctx mid-wait. Return context.Canceled
			// so the outer sendRequest retry loop re-engages.
			return nil, context.Canceled
		}
	}
	return conn.client.sendRequest(command, data)
}

// listen, recvWorker, handleReceive, transmitWorker were migrated onto
// *Client in Phase 5.a-dial. See client.go.

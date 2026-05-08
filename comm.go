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

// send is used exclusively for the local-mode handshake during Connect/Reconnect.
// It uses the shared systemResponse channel and is NOT safe for concurrent callers.
// For normal ADS commands, use sendRequest/sendRequestOnce which use per-invoke channels.
func (conn *Session) send(data []byte) (response []byte, err error) {
	conn.tx.currentRequest.Inc()
	conn.tx.chanMu.RLock()
	sendCh := conn.tx.sendChannel
	sysCh := conn.tx.systemResponse
	conn.tx.chanMu.RUnlock()
	conn.lifecycle.ctxMu.RLock()
	currentCtx := conn.lifecycle.ctx
	conn.lifecycle.ctxMu.RUnlock()
	ctx, cancel := context.WithTimeout(currentCtx, conn.requestTimeout)
	defer cancel()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("send aborted, context canceled: %w", ctx.Err())
	case sendCh <- data:
	}

	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("request aborted, deadline exceeded: %w", ctx.Err())
			conn.logger.Error("send aborted due to timeout", "error", err)
		} else {
			err = fmt.Errorf("request aborted, shutdown initiated: %w", ctx.Err())
			conn.logger.Error("send aborted due to shutdown", "error", err)
		}
		return nil, err
	case response = <-sysCh:
		return response, nil
	}
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

func (conn *Session) sendRequestOnce(command CommandID, data []byte) (response []byte, err error) {
	if conn.isTransportDown() {
		// If a reconnect is in progress, wait for it to finish before giving up
		conn.lifecycle.reconnectMu.Lock()
		ch := conn.lifecycle.reconnectDone
		conn.lifecycle.reconnectMu.Unlock()
		if ch != nil {
			conn.logger.Debug("sendRequest waiting for reconnect to complete")
			conn.lifecycle.ctxMu.RLock()
			ctxDone := conn.lifecycle.ctx.Done()
			conn.lifecycle.ctxMu.RUnlock()
			select {
			case <-ch:
				// Reconnect finished — check if we're still disconnected
				if conn.isTransportDown() {
					return nil, ErrDisconnected
				}
			case <-ctxDone:
				// Reconnect cancelled our ctx mid-wait. Return context.Canceled
				// so the outer sendRequest retry loop re-engages.
				return nil, context.Canceled
			}
		} else {
			return nil, ErrDisconnected
		}
	}
	conn.tx.activeRequestLock.Lock()
	// First, request a new invoke id
	id := conn.tx.currentRequest.Inc()
	// Create a channel for the response
	conn.tx.activeRequests[id] = make(chan []byte, 1)
	conn.tx.activeRequestLock.Unlock()
	defer func() {
		conn.tx.activeRequestLock.Lock()
		delete(conn.tx.activeRequests, id)
		conn.tx.activeRequestLock.Unlock()
	}()
	conn.logger.Log(context.Background(), LevelTrace, "encoding packet",
		"command", command,
		"data", data,
		"id", id)

	pack, err := conn.encode(command, data, id)
	if err != nil {
		conn.logger.Error("Error during sendrequest encode", "error", err)
		return nil, err
	}
	conn.lifecycle.ctxMu.RLock()
	currentCtx := conn.lifecycle.ctx
	conn.lifecycle.ctxMu.RUnlock()
	conn.tx.chanMu.RLock()
	sendCh := conn.tx.sendChannel
	conn.tx.chanMu.RUnlock()
	ctx, cancel := context.WithTimeout(currentCtx, conn.requestTimeout)
	defer cancel()
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			conn.logger.Error("sendRequest aborted due to timeout")
		} else {
			conn.logger.Info("sendRequest aborted due to shutdown")
		}
		return nil, ctx.Err()
	case sendCh <- pack:
	}
	// Capture channel reference under lock to avoid concurrent map read
	conn.tx.activeRequestLock.Lock()
	responseCh := conn.tx.activeRequests[id]
	conn.tx.activeRequestLock.Unlock()
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			conn.logger.Error("sendRequest aborted due to timeout")
		} else {
			conn.logger.Info("sendRequest aborted due to shutdown")
		}
		return nil, ctx.Err()
	case response = <-responseCh:
		return response, nil
	}
}

// listen, recvWorker, handleReceive, transmitWorker were migrated onto
// *Client in Phase 5.a-dial. See client.go.

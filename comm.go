package ads

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
)

func (conn *Connection) send(data []byte) (response []byte, err error) {
	conn.currentRequest.Inc()
	conn.chanMu.RLock()
	sendCh := conn.sendChannel
	sysCh := conn.systemResponse
	conn.chanMu.RUnlock()
	ctx, cancel := context.WithCancel(conn.ctx)
	defer cancel()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("send aborted, context canceled: %w", ctx.Err())
	case sendCh <- data:
	}

	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("request aborted, deadline exceeded: %w", ctx.Err())
			conn.logger.Error("sendRequest aborted due to timeout", "error", err)
		} else {
			err = fmt.Errorf("request aborted, shutdown initiated: %w", ctx.Err())
			conn.logger.Error("sendRequest aborted due to shutdown", "error", err)
		}
		return nil, err
	case response = <-sysCh:
		return response, nil
	}
}

func (conn *Connection) sendRequest(command CommandID, data []byte) (response []byte, err error) {
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
		if conn.closed.Load() {
			return nil, err
		}
		// Only retry if a reconnect is actually in progress.
		if !conn.reconnecting.Load() {
			return nil, err
		}
		// Wait for reconnect to finish before retrying.
		conn.reconnectMu.Lock()
		ch := conn.reconnectDone
		conn.reconnectMu.Unlock()
		if ch == nil {
			return nil, err
		}
		conn.logger.Info("sendRequest retrying after reconnect",
			"attempt", attempt+1,
			"command", command)
		select {
		case <-ch:
			// Reconnect finished — loop will retry if connection is healthy.
			if conn.disconnected.Load() {
				return nil, ErrDisconnected
			}
		case <-conn.closedCh:
			return nil, fmt.Errorf("connection closed while waiting for reconnect: %w", err)
		}
	}
	return nil, err
}

func (conn *Connection) sendRequestOnce(command CommandID, data []byte) (response []byte, err error) {
	if conn.disconnected.Load() {
		// If a reconnect is in progress, wait for it to finish before giving up
		conn.reconnectMu.Lock()
		ch := conn.reconnectDone
		conn.reconnectMu.Unlock()
		if ch != nil {
			conn.logger.Debug("sendRequest waiting for reconnect to complete")
			conn.ctxMu.RLock()
			ctxDone := conn.ctx.Done()
			conn.ctxMu.RUnlock()
			select {
			case <-ch:
				// Reconnect finished — check if we're still disconnected
				if conn.disconnected.Load() {
					return nil, ErrDisconnected
				}
			case <-ctxDone:
				return nil, ErrDisconnected
			}
		} else {
			return nil, ErrDisconnected
		}
	}
	conn.activeRequestLock.Lock()
	// First, request a new invoke id
	id := conn.currentRequest.Inc()
	// Create a channel for the response
	conn.activeRequests[id] = make(chan []byte, 1)
	conn.activeRequestLock.Unlock()
	defer func() {
		conn.activeRequestLock.Lock()
		delete(conn.activeRequests, id)
		conn.activeRequestLock.Unlock()
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
	conn.ctxMu.RLock()
	currentCtx := conn.ctx
	conn.ctxMu.RUnlock()
	conn.chanMu.RLock()
	sendCh := conn.sendChannel
	conn.chanMu.RUnlock()
	ctx, cancel := context.WithTimeout(currentCtx, conn.RequestTimeout)
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
	conn.activeRequestLock.Lock()
	responseCh := conn.activeRequests[id]
	conn.activeRequestLock.Unlock()
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

func (conn *Connection) listen() {
	defer conn.waitGroup.Done()
	reader := bufio.NewReader(conn.connection)
	buff := bytes.Buffer{}
	for {
		tcpHeader := amsTCPHeader{}
		data := make([]byte, 6)
		select {
		case <-conn.ctx.Done():
			conn.logger.Info("exit listen")
			return
		default:
			_, err := io.ReadFull(reader, data)
			if err != nil {
				select {
				case <-conn.ctx.Done():
					// Shutdown was requested, don't reconnect
					return
				default:
				}
				// EOF or connection reset often means PLC has no AMS route for our NetID
				hint := ""
				if err.Error() == "EOF" || strings.Contains(err.Error(), "connection reset") {
					hint = "PLC may not have an AMS route for this NetID — check route credentials or register route via WithRoute()"
				}
				if hint != "" {
					conn.logger.Error("PLC closed connection, triggering reconnect", "error", err, "hint", hint,
						"sourceNetID", fmt.Sprintf("%d.%d.%d.%d.%d.%d", conn.source.NetID[0], conn.source.NetID[1], conn.source.NetID[2], conn.source.NetID[3], conn.source.NetID[4], conn.source.NetID[5]))
				} else {
					conn.logger.Error("listen read error, triggering reconnect", "error", err)
				}
				conn.triggerReconnect()
				return
			}
		}
		buff.Write(data)
		err := binary.Read(&buff, binary.LittleEndian, &tcpHeader)
		if err != nil {
			conn.logger.Error("error during header read", "error", err)
			continue
		}
		const maxAMSPacket = 4 * 1024 * 1024 // 4 MB sanity limit
		if tcpHeader.Length > maxAMSPacket {
			conn.logger.Error("AMS packet length exceeds sanity limit, triggering reconnect", "length", tcpHeader.Length)
			conn.triggerReconnect()
			return
		}
		data = make([]byte, tcpHeader.Length)
		select {
		case <-conn.ctx.Done():
			return
		default:
			_, err := io.ReadFull(reader, data)
			if err != nil {
				select {
				case <-conn.ctx.Done():
					return
				default:
				}
				conn.logger.Error("listen body read error, triggering reconnect", "error", err)
				conn.triggerReconnect()
				return
			}
		}
		if tcpHeader.System > 0 {
			select {
			case conn.systemResponse <- data:
			case <-conn.ctx.Done():
				return
			}
		} else {
			go conn.handleReceive(conn.ctx, data)
		}
	}
}

func (conn *Connection) handleReceive(ctx context.Context, data []byte) {
	conn.logger.Log(context.Background(), LevelTrace, "in read")
	if len(data) < 32 {
		conn.logger.Error("header too short")
		return
	}
	buff := bytes.NewBuffer(data)
	header := amsHeader{}
	err := binary.Read(buff, binary.LittleEndian, &header)
	if err != nil {
		conn.logger.Error("Error parsing header", "error", err)
		return
	}
	conn.logger.Log(context.Background(), LevelTrace, "header info", "header", header)

	adsData := data[32:]
	if len(adsData) != int(header.Length) {
		conn.logger.Error("Error parsing body")
		return
	}

	switch header.Command {
	case CommandIDDeviceNotification:
		err := conn.DeviceNotification(ctx, adsData)
		if err != nil {
			conn.logger.Error("error", "error", err)
		}
	default:
		conn.logger.Log(context.Background(), LevelTrace, "default receive")
		// Look up response channel under lock, then release before channel send
		// to avoid deadlock if sendRequest's cleanup defer also acquires the lock.
		conn.activeRequestLock.Lock()
		response, ok := conn.activeRequests[header.InvokeID]
		conn.activeRequestLock.Unlock()
		if ok {
			select {
			case <-ctx.Done():
				conn.logger.Info("receive channel timed out",
					"id", header.InvokeID,
					"command", header.Command)
				return
			case response <- adsData:
				conn.logger.Log(context.Background(), LevelTrace, "Successfully delivered answer",
					"id", header.InvokeID,
					"command", header.Command)
			}
		} else {
			conn.logger.Error("received packet with unknown invokeID",
				"data", buff.Bytes(),
				"invokeId", header.InvokeID)
		}
	}
}

func (conn *Connection) transmitWorker() {
	defer conn.waitGroup.Done()
	writer := bufio.NewWriter(conn.connection)
	ctx, cancel := context.WithCancel(conn.ctx)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			conn.logger.Debug("Exit transmitWorker")
			return
		case data := <-conn.sendChannel:
			conn.logger.Log(context.Background(), LevelTrace, fmt.Sprintf("Sending %d bytes", len(data)))
			_, err := writer.Write(data)
			if err != nil {
				conn.logger.Error("error sending data on conn, triggering reconnect", "error", err)
				conn.triggerReconnect()
				return
			}
			if err := writer.Flush(); err != nil {
				conn.logger.Error("error flushing data on conn, triggering reconnect", "error", err)
				conn.triggerReconnect()
				return
			}
		}
	}
}

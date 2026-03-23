package ads

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"
)

func (conn *Connection) send(data []byte) (response []byte, err error) {
	conn.currentRequest.Inc()
	ctx, cancel := context.WithCancel(conn.ctx)
	defer cancel()
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("send aborted, context canceled: %w", ctx.Err())
	case conn.sendChannel <- data:
	}

	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("request aborted, deadline exceeded: %w", ctx.Err())
			log.Error().
				Err(err).
				Msg("sendRequest aborted due to timeout")
		} else {
			err = fmt.Errorf("request aborted, shutdown initiated: %w", ctx.Err())
			log.Error().
				Err(err).
				Msg("sendRequest aborted due to shutdown")
		}
		return nil, err
	case response = <-conn.systemResponse:
		return response, nil
	}
}

func (conn *Connection) sendRequest(command CommandID, data []byte) (response []byte, err error) {
	if conn == nil {
		return nil, fmt.Errorf("sendRequest called on nil connection")
	}
	if conn.disconnected.Load() {
		// If a reconnect is in progress, wait for it to finish before giving up
		conn.reconnectMu.Lock()
		ch := conn.reconnectDone
		conn.reconnectMu.Unlock()
		if ch != nil {
			log.Debug().Msg("sendRequest waiting for reconnect to complete")
			select {
			case <-ch:
				// Reconnect finished — check if we're still disconnected
				if conn.disconnected.Load() {
					return nil, ErrDisconnected
				}
			case <-conn.ctx.Done():
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
	conn.activeRequests[id] = make(chan []byte)
	conn.activeRequestLock.Unlock()
	defer func() {
		conn.activeRequestLock.Lock()
		delete(conn.activeRequests, id)
		conn.activeRequestLock.Unlock()
	}()
	log.Trace().
		Interface("command", command).
		Bytes("data", data).
		Uint32("id", id).
		Msg("encoding packet")

	pack, err := conn.encode(command, data, id)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error during sendrequest encode")
		return nil, err
	}
	ctx, cancel := context.WithTimeout(conn.ctx, conn.RequestTimeout)
	defer cancel()
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			log.Error().
				Msg("sendRequest aborted due to timeout")
		} else {
			log.Info().
				Msg("sendRequest aborted due to shutdown")
		}
		return nil, ctx.Err()
	case conn.sendChannel <- pack:
	}
	// Capture channel reference under lock to avoid concurrent map read
	conn.activeRequestLock.Lock()
	responseCh := conn.activeRequests[id]
	conn.activeRequestLock.Unlock()
	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			log.Error().
				Msg("sendRequest aborted due to timeout")
		} else {
			log.Info().
				Msg("sendRequest aborted due to shutdown")
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
			log.Info().Msg("exit listen")
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
				log.Error().Err(err).Msg("listen read error, triggering reconnect")
				go conn.Reconnect()
				return
			}
		}
		buff.Write(data)
		err := binary.Read(&buff, binary.LittleEndian, &tcpHeader)
		if err != nil {
			log.Error().Err(err).Msg("error during header read")
			continue
		}
		const maxAMSPacket = 4 * 1024 * 1024 // 4 MB sanity limit
		if tcpHeader.Length > maxAMSPacket {
			log.Error().Uint32("length", tcpHeader.Length).Msg("AMS packet length exceeds sanity limit, triggering reconnect")
			go conn.Reconnect()
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
				log.Error().Err(err).Msg("listen body read error, triggering reconnect")
				go conn.Reconnect()
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
	log.Trace().
		Msg("in read")
	if len(data) < 32 {
		log.Error().
			Msg("header too short")
		return
	}
	buff := bytes.NewBuffer(data)
	header := amsHeader{}
	err := binary.Read(buff, binary.LittleEndian, &header)
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error parsing header")
		return
	}
	log.Trace().
		Interface("header", header).
		Msg("header info")

	adsData := data[32:]
	if len(adsData) != int(header.Length) {
		log.Error().
			Err(err).
			Msg("Error parsing body")
		return
	}

	switch header.Command {
	case CommandIDDeviceNotification:
		err := conn.DeviceNotification(ctx, adsData)
		if err != nil {
			log.Error().
				Err(err).
				Msg("error")
		}
	default:
		log.Trace().
			Msg("default receive")
		// Look up response channel under lock, then release before channel send
		// to avoid deadlock if sendRequest's cleanup defer also acquires the lock.
		conn.activeRequestLock.Lock()
		response, ok := conn.activeRequests[header.InvokeID]
		conn.activeRequestLock.Unlock()
		if ok {
			select {
			case <-ctx.Done():
				log.Info().
					Uint32("id", header.InvokeID).
					Interface("command", header.Command).
					Msg("receive channel timed out")
				return
			case response <- adsData:
				log.Trace().
					Uint32("id", header.InvokeID).
					Interface("command", header.Command).
					Msg("Successfully delivered answer")
			}
		} else {
			log.Error().
				Bytes("data", buff.Bytes()).
				Uint32("invokeId", header.InvokeID).
				Msg("received packet with unknown invokeID")
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
			log.Debug().
				Msg("Exit transmitWorker")
			return
		case data := <-conn.sendChannel:
			log.Trace().
				Msgf("Sending %d bytes", len(data))
			_, err := writer.Write(data)
			if err != nil {
				log.Error().
					Err(err).
					Msg("error sending data on conn, triggering reconnect")
				go conn.Reconnect()
				return
			}
			if err := writer.Flush(); err != nil {
				log.Error().Err(err).Msg("error flushing data on conn, triggering reconnect")
				go conn.Reconnect()
				return
			}
		}
	}
}

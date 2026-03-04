package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"go.uber.org/atomic"
)

type Connection struct {
	ip   string
	port int

	connection  net.Conn
	target      AmsAddress
	source      AmsAddress
	sendChannel chan []byte

	symbols             map[string]*Symbol
	activeNotifications map[uint32]*Symbol
	symbolLock          sync.Mutex

	datatypes map[string]SymbolUploadDataType
	ctx       context.Context
	shutdown  context.CancelFunc
	waitGroup sync.WaitGroup // tracks only infrastructure goroutines (listen, transmitWorker)

	// List of active requests that waits a response, invokeid is key and value is a channel to the request rutine
	currentRequest    atomic.Uint32
	activeRequestLock sync.Mutex
	activeRequests    map[uint32]chan []byte

	systemResponse chan []byte

	RequestTimeout time.Duration

	// Stored notification configs for re-subscribe after reconnect
	notificationConfigs []NotificationConfig
	notificationChannel chan *Update

	// Symbol version tracking
	symbolVersion uint32

	// Reconnection settings
	reconnectInterval    time.Duration
	maxReconnectAttempts int // 0 = infinite
	isLocal              bool
	disconnected         atomic.Bool

	// Feature support flags (detected at runtime)
	sumReadSupported atomic.Bool
	sumReadChecked   atomic.Bool

	reconnecting atomic.Bool // prevents concurrent reconnect attempts
}


// NewConnection creates a new ADS connection. requestTimeout is the timeout for individual ADS requests.
// If requestTimeout is 0, a default of 5000ms is used.
func NewConnection(ctx context.Context, ip string, port int, netid string, amsPort int, localNetID string, localPort int, requestTimeout time.Duration) (conn *Connection, err error) {
	if requestTimeout <= 0 {
		requestTimeout = 5000 * time.Millisecond
	}
	conn = &Connection{
		ip:                   ip,
		port:                 port,
		RequestTimeout:       requestTimeout,
		reconnectInterval:    5 * time.Second,
		maxReconnectAttempts: 0, // 0 = infinite retries
	}
	conn.target.NetID = stringToNetID(netid)
	conn.target.Port = uint16(amsPort)
	if localNetID != "auto" && localNetID != "" {
		conn.source.NetID = stringToNetID(localNetID)
	}
	// If localNetID is "auto" or empty, source.NetID stays zero and will be auto-derived in Connect()
	conn.source.Port = uint16(localPort)
	conn.systemResponse = make(chan []byte)
	conn.activeRequests = map[uint32]chan []byte{}
	conn.activeNotifications = make(map[uint32]*Symbol)
	conn.sendChannel = make(chan []byte)
	// Use an independent context so that Close() can still send cleanup commands
	// even after the parent context is canceled (e.g. on SIGTERM)
	conn.ctx, conn.shutdown = context.WithCancel(context.Background())
	return
}

func (conn *Connection) Connect(local bool) error {
	conn.isLocal = local
	var err error
	log.Debug().
		Msgf("Dialing ip: %s port: %d", conn.ip, conn.port)
	if local {
		conn.target.NetID = [6]byte{127, 0, 0, 1, 1, 1}
		conn.ip = "127.0.0.1"
	}
	conn.connection, err = net.Dial("tcp", net.JoinHostPort(conn.ip, fmt.Sprintf("%d", conn.port)))
	if err != nil {
		log.Error().
			Err(err).
			Msg("Error connecting")
		return err
	}
	// Enable aggressive TCP keepalive to detect dead connections quickly.
	// With Idle=3s, Interval=2s, Count=5: connection declared dead after ~13s of no response.
	// This ensures cable unplugs (>13s) are detected and trigger reconnect,
	// while not affecting slow-changing notification data (keepalive is TCP-level, not app-level).
	if tcpConn, ok := conn.connection.(*net.TCPConn); ok {
		tcpConn.SetKeepAliveConfig(net.KeepAliveConfig{
			Enable:   true,
			Idle:     3 * time.Second,
			Interval: 2 * time.Second,
			Count:    5,
		})
	}
	// Auto-derive source AMS NetID from local IP if source NetID is all zeros
	if conn.source.NetID == [6]byte{} {
		localAddr := conn.connection.LocalAddr().(*net.TCPAddr)
		ip := localAddr.IP.To4()
		if ip != nil {
			conn.source.NetID = [6]byte{ip[0], ip[1], ip[2], ip[3], 1, 1}
			log.Info().
				Str("netid", fmt.Sprintf("%d.%d.%d.%d.1.1", ip[0], ip[1], ip[2], ip[3])).
				Msg("auto-derived source AMS NetID from local IP")
		}
	}

	log.Trace().Msgf("Connected")
	go conn.listen()
	go conn.transmitWorker()
	if local {
		resp, _ := conn.send([]byte{0, 16, 2, 0, 0, 0, 0, 0})
		buf := bytes.NewBuffer(resp)
		result := AmsAddress{}
		log.Trace().
			Bytes("stuff", buf.Bytes()).Msg("got stuff")
		err = binary.Read(buf, binary.LittleEndian, &result)
		log.Info().
			Msgf("result %d", result)
		if err != nil {
			log.Error().
				Msgf("ERROR %v", err)
		}
		conn.source = result
	}
	err = conn.loadSymbols()
	if err != nil {
		log.Error().Err(err).Msg("failed to load symbols during connect")
		return err
	}
	return nil
}

// Close closes connection and waits for completion
func (conn *Connection) Close() {
	log.Info().Msg("Close called, shutting down")

	// Delete all active notifications (uses sum command with automatic fallback to individual)
	handles := make([]uint32, 0, len(conn.activeNotifications))
	for handle := range conn.activeNotifications {
		handles = append(handles, handle)
	}
	if len(handles) > 0 {
		errors, err := conn.SumDeleteDeviceNotification(handles)
		if err != nil {
			log.Warn().Err(err).Msg("failed to delete notification handles during close")
		} else {
			for i, h := range handles {
				if errors[i] != ReturnCodeNoErrors {
					log.Warn().Uint32("handle", h).Uint32("error", uint32(errors[i])).Msg("failed to delete notification handle")
				} else {
					log.Info().Uint32("handle", h).Msg("removed notification handle")
				}
			}
		}
	}
	for _, symbol := range conn.symbols {
		if symbol.Handle != 0 {
			log.Info().
				Uint32("handle", symbol.Handle).
				Msg("Handle deleted")
			handleBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(handleBytes, symbol.Handle)
			conn.Write(uint32(GroupSymbolReleaseHandle), 0, handleBytes)
		}
	}
	conn.shutdown()
	// Close the TCP connection to unblock listen() which may be stuck in ReadFull
	if conn.connection != nil {
		conn.connection.Close()
	}
	log.Info().
		Msg("Waiting for workers to close")
	conn.waitGroup.Wait()
	log.Info().
		Msg("Close DONE")
}

// ErrDisconnected is returned when attempting to send on a closed connection.
var ErrDisconnected = fmt.Errorf("connection is disconnected")

// Reconnect attempts to re-establish the TCP connection, reload symbols,
// and re-subscribe to previously registered notifications.
func (conn *Connection) Reconnect() error {
	// Prevent concurrent reconnect attempts
	if !conn.reconnecting.CompareAndSwap(false, true) {
		log.Info().Msg("reconnect already in progress, skipping")
		return nil
	}
	defer conn.reconnecting.Store(false)

	log.Info().Msg("attempting reconnect")
	conn.disconnected.Store(true)

	// Close existing TCP connection if still open
	if conn.connection != nil {
		conn.connection.Close()
	}

	// Cancel old goroutines and wait
	conn.shutdown()
	conn.waitGroup.Wait()

	// Reset context
	conn.ctx, conn.shutdown = context.WithCancel(context.Background())

	// Reset channels, feature flags, and active notifications (old handles are invalid)
	conn.sendChannel = make(chan []byte)
	conn.systemResponse = make(chan []byte)
	conn.activeRequests = map[uint32]chan []byte{}
	conn.activeNotifications = make(map[uint32]*Symbol)
	conn.sumReadChecked.Store(false)

	var lastErr error
	attempts := 0
	for {
		attempts++
		if conn.maxReconnectAttempts > 0 && attempts > conn.maxReconnectAttempts {
			return fmt.Errorf("reconnect failed after %d attempts: %w", conn.maxReconnectAttempts, lastErr)
		}

		var err error
		conn.connection, err = net.Dial("tcp", net.JoinHostPort(conn.ip, fmt.Sprintf("%d", conn.port)))
		if err != nil {
			lastErr = err
			log.Warn().Err(err).Int("attempt", attempts).Msg("reconnect dial failed, retrying")
			time.Sleep(conn.reconnectInterval)
			continue
		}
		// Enable TCP keepalive to detect dead connections
		if tcpConn, ok := conn.connection.(*net.TCPConn); ok {
			tcpConn.SetKeepAlive(true)
			tcpConn.SetKeepAlivePeriod(5 * time.Second)
		}

		// Clear disconnected flag so sendRequest works during symbol load
		conn.disconnected.Store(false)

		// Re-start goroutines
		go conn.listen()
		go conn.transmitWorker()

		// Re-load symbols
		err = conn.loadSymbols()
		if err != nil {
			lastErr = err
			conn.disconnected.Store(true)
			log.Warn().Err(err).Int("attempt", attempts).Msg("reconnect symbol load failed, retrying")
			// Stop goroutines before next attempt
			conn.shutdown()
			if conn.connection != nil {
				conn.connection.Close()
			}
			conn.waitGroup.Wait()
			conn.ctx, conn.shutdown = context.WithCancel(context.Background())
			conn.sendChannel = make(chan []byte)
			conn.systemResponse = make(chan []byte)
			conn.activeRequests = map[uint32]chan []byte{}
			time.Sleep(conn.reconnectInterval)
			continue
		}

		// Re-subscribe notifications using stored configs (don't re-append)
		if len(conn.notificationConfigs) > 0 && conn.notificationChannel != nil {
			savedConfigs := conn.notificationConfigs
			conn.notificationConfigs = nil // Clear before re-adding to prevent duplicates
			err = conn.AddSymbolNotifications(savedConfigs, conn.notificationChannel)
			if err != nil {
				log.Warn().Err(err).Msg("reconnect notification re-subscribe failed")
			}
		}

		conn.disconnected.Store(false)
		log.Info().Int("attempts", attempts).Msg("reconnect successful")
		return nil
	}
}

// loadSymbols loads symbol table and datatypes from the PLC, and saves the symbol version.
func (conn *Connection) loadSymbols() error {
	// Read and store symbol version
	version, err := conn.GetSymbolVersion()
	if err != nil {
		log.Warn().Err(err).Msg("failed to read symbol version, continuing with symbol load")
	} else {
		conn.symbolVersion = version
	}

	res, err := conn.GetSymbolUploadInfo()
	if err != nil {
		return fmt.Errorf("failed to get symbol upload info: %w", err)
	}
	datatypesResponse, err := conn.GetUploadSymbolInfoDataTypes(res.DataTypeLength)
	if err != nil {
		return fmt.Errorf("failed to upload datatypes: %w", err)
	}
	datatypes, err := ParseUploadSymbolInfoDataTypes(datatypesResponse)
	if err != nil {
		return fmt.Errorf("failed to parse datatypes: %w", err)
	}
	conn.datatypes = datatypes
	symbolsResponse, err := conn.GetUploadSymbolInfoSymbols(res.SymbolLength)
	if err != nil {
		return fmt.Errorf("failed to upload symbols: %w", err)
	}
	symbols, err := ParseUploadSymbolInfoSymbols(symbolsResponse, datatypes)
	if err != nil {
		return fmt.Errorf("failed to parse symbols: %w", err)
	}
	conn.symbols = symbols
	return nil
}

// IsDisconnected returns whether the connection is currently in a disconnected state.
func (conn *Connection) IsDisconnected() bool {
	return conn.disconnected.Load()
}

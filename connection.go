package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"go.uber.org/atomic"
)

type Connection struct {
	ip   string
	port int

	connection  net.Conn
	connMu      sync.Mutex // protects connection field against concurrent Close/Reconnect
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

	// Symbol discovery mode tracking
	symbolsFullyLoaded bool            // true after LoadSymbols() or LoadSymbolsSlow() completes
	symbolListLoaded   bool            // true after LoadSymbolList() — symbol names available for browsing
	datatypesLoaded    bool            // true after LoadDataTypes() — struct children expandable
	onDemandSymbols    map[string]bool // tracks symbol names resolved on-demand (for reconnect)

	// Feature support flags (detected at runtime)
	sumReadSupported         atomic.Bool
	sumReadChecked           atomic.Bool
	chunkedDownloadSupported atomic.Bool
	chunkedDownloadChecked   atomic.Bool

	reconnecting  atomic.Bool // prevents concurrent reconnect attempts
	reconnectDone chan struct{}
	reconnectMu   sync.Mutex // protects reconnectDone

	logger *slog.Logger
}

// NewConnection creates a new ADS connection. requestTimeout is the timeout for individual ADS requests.
// If requestTimeout is 0, a default of 5000ms is used.
func NewConnection(ctx context.Context, ip string, port int, netid string, amsPort int, localNetID string, localPort int, requestTimeout time.Duration, opts ...ConnectionOption) (conn *Connection, err error) {
	if requestTimeout <= 0 {
		requestTimeout = 5000 * time.Millisecond
	}
	conn = &Connection{
		ip:                   ip,
		port:                 port,
		RequestTimeout:       requestTimeout,
		reconnectInterval:    5 * time.Second,
		maxReconnectAttempts: 0, // 0 = infinite retries
		logger:               slog.Default(),
	}
	for _, opt := range opts {
		opt(conn)
	}
	netIDBytes, err := stringToNetID(netid)
	if err != nil {
		return nil, fmt.Errorf("invalid target NetID: %w", err)
	}
	conn.target.NetID = netIDBytes
	conn.target.Port = uint16(amsPort)
	if localNetID != "auto" && localNetID != "" {
		localBytes, err := stringToNetID(localNetID)
		if err != nil {
			return nil, fmt.Errorf("invalid local NetID: %w", err)
		}
		conn.source.NetID = localBytes
	}
	// If localNetID is "auto" or empty, source.NetID stays zero and will be auto-derived in Connect()
	conn.source.Port = uint16(localPort)
	conn.systemResponse = make(chan []byte)
	conn.activeRequests = map[uint32]chan []byte{}
	conn.activeNotifications = make(map[uint32]*Symbol)
	conn.sendChannel = make(chan []byte)
	conn.symbols = map[string]*Symbol{}
	conn.onDemandSymbols = map[string]bool{}
	// Use an independent context so that Close() can still send cleanup commands
	// even after the parent context is canceled (e.g. on SIGTERM)
	conn.ctx, conn.shutdown = context.WithCancel(context.Background())
	return
}

func (conn *Connection) Connect(local bool) error {
	conn.isLocal = local
	var err error
	conn.logger.Debug("dialing", "ip", conn.ip, "port", conn.port)
	if local {
		conn.target.NetID = [6]byte{127, 0, 0, 1, 1, 1}
		conn.ip = "127.0.0.1"
	}
	tcpConn, err := net.Dial("tcp", net.JoinHostPort(conn.ip, strconv.Itoa(conn.port)))
	if err != nil {
		conn.logger.Error("Error connecting", "error", err)
		return err
	}
	conn.connMu.Lock()
	conn.connection = tcpConn
	conn.connMu.Unlock()
	// Enable aggressive TCP keepalive to detect dead connections quickly.
	// With Idle=3s, Interval=2s, Count=5: connection declared dead after ~13s of no response.
	// This ensures cable unplugs (>13s) are detected and trigger reconnect,
	// while not affecting slow-changing notification data (keepalive is TCP-level, not app-level).
	if tc, ok := tcpConn.(*net.TCPConn); ok {
		tc.SetKeepAliveConfig(net.KeepAliveConfig{
			Enable:   true,
			Idle:     3 * time.Second,
			Interval: 2 * time.Second,
			Count:    5,
		})
	}
	// Auto-derive source AMS NetID from local IP if source NetID is all zeros
	if conn.source.NetID == [6]byte{} {
		localAddr, ok := conn.connection.LocalAddr().(*net.TCPAddr)
		if !ok {
			return fmt.Errorf("unexpected local address type: %T", conn.connection.LocalAddr())
		}
		ip := localAddr.IP.To4()
		if ip != nil {
			conn.source.NetID = [6]byte{ip[0], ip[1], ip[2], ip[3], 1, 1}
			conn.logger.Info("auto-derived source AMS NetID from local IP",
				"netid", fmt.Sprintf("%d.%d.%d.%d.1.1", ip[0], ip[1], ip[2], ip[3]))
		}
	}

	conn.logger.Log(context.Background(), LevelTrace, "connected")
	conn.waitGroup.Add(2)
	go conn.listen()
	go conn.transmitWorker()
	if local {
		resp, err := conn.send([]byte{0, 16, 2, 0, 0, 0, 0, 0})
		if err != nil {
			return fmt.Errorf("local mode handshake failed: %w", err)
		}
		buf := bytes.NewBuffer(resp)
		result := AmsAddress{}
		conn.logger.Log(context.Background(), LevelTrace, "got stuff", "stuff", buf.Bytes())
		err = binary.Read(buf, binary.LittleEndian, &result)
		if err != nil {
			return fmt.Errorf("local mode binary read failed: %w", err)
		}
		conn.logger.Info("local mode handshake result", "result", result)
		conn.source = result
	}
	// Read symbol version for later change detection (best-effort, don't fail connect)
	version, err := conn.GetSymbolVersion()
	if err != nil {
		conn.logger.Debug("could not read symbol version during connect", "error", err)
	} else {
		conn.symbolLock.Lock()
		conn.symbolVersion = version
		conn.symbolLock.Unlock()
	}
	return nil
}

// Close closes connection and waits for completion
func (conn *Connection) Close() {
	conn.logger.Info("Close called, shutting down")

	// Delete all active notifications (uses sum command with automatic fallback to individual)
	conn.symbolLock.Lock()
	handles := make([]uint32, 0, len(conn.activeNotifications))
	for handle := range conn.activeNotifications {
		handles = append(handles, handle)
	}
	conn.symbolLock.Unlock()
	if len(handles) > 0 {
		errors, err := conn.SumDeleteDeviceNotification(handles)
		if err != nil {
			conn.logger.Warn("failed to delete notification handles during close", "error", err)
		} else {
			for i, h := range handles {
				if errors[i] != ReturnCodeNoErrors {
					conn.logger.Warn("failed to delete notification handle", "handle", h, "error", uint32(errors[i]))
				} else {
					conn.logger.Info("removed notification handle", "handle", h)
				}
			}
		}
	}
	// Collect symbol handles under lock, then release without holding the lock
	conn.symbolLock.Lock()
	var symHandles []uint32
	for _, symbol := range conn.symbols {
		if symbol.Handle != 0 {
			symHandles = append(symHandles, symbol.Handle)
		}
	}
	conn.symbolLock.Unlock()

	// Release handles individually — ADS has no batch release command,
	// and Close() is not performance-critical.
	for _, h := range symHandles {
		handleBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(handleBytes, h)
		if err := conn.Write(uint32(GroupSymbolReleaseHandle), 0, handleBytes); err != nil {
			conn.logger.Warn("failed to release symbol handle during close", "error", err, "handle", h)
		} else {
			conn.logger.Info("handle deleted", "handle", h)
		}
	}
	conn.shutdown()
	// Close the TCP connection to unblock listen() which may be stuck in ReadFull
	conn.connMu.Lock()
	if conn.connection != nil {
		conn.connection.Close()
	}
	conn.connMu.Unlock()
	conn.logger.Info("Waiting for workers to close")
	conn.waitGroup.Wait()
	conn.logger.Info("Close DONE")
}

// ErrDisconnected is returned when attempting to send on a closed connection.
var ErrDisconnected = fmt.Errorf("connection is disconnected")

// Reconnect attempts to re-establish the TCP connection, reload symbols,
// and re-subscribe to previously registered notifications.
func (conn *Connection) Reconnect() error {
	// Prevent concurrent reconnect attempts
	if !conn.reconnecting.CompareAndSwap(false, true) {
		conn.logger.Info("reconnect already in progress, skipping")
		return nil
	}

	// Create a channel that waiters (sendRequest) can block on
	conn.reconnectMu.Lock()
	conn.reconnectDone = make(chan struct{})
	conn.reconnectMu.Unlock()

	defer func() {
		conn.reconnecting.Store(false)
		conn.reconnectMu.Lock()
		if conn.reconnectDone != nil {
			close(conn.reconnectDone)
			conn.reconnectDone = nil
		}
		conn.reconnectMu.Unlock()
	}()

	conn.logger.Info("attempting reconnect")
	conn.disconnected.Store(true)

	// Close existing TCP connection if still open
	conn.connMu.Lock()
	if conn.connection != nil {
		conn.connection.Close()
	}
	conn.connMu.Unlock()

	// Cancel old goroutines and wait
	conn.shutdown()
	conn.waitGroup.Wait()

	// Reset context
	conn.ctx, conn.shutdown = context.WithCancel(context.Background())

	// Reset channels, feature flags, and active notifications (old handles are invalid)
	conn.sendChannel = make(chan []byte)
	conn.systemResponse = make(chan []byte)
	conn.activeRequestLock.Lock()
	conn.activeRequests = map[uint32]chan []byte{}
	conn.activeRequestLock.Unlock()
	conn.symbolLock.Lock()
	conn.activeNotifications = make(map[uint32]*Symbol)
	conn.symbolLock.Unlock()
	conn.sumReadChecked.Store(false)
	conn.chunkedDownloadChecked.Store(false)

	var lastErr error
	attempts := 0
	for {
		attempts++
		if conn.maxReconnectAttempts > 0 && attempts > conn.maxReconnectAttempts {
			return fmt.Errorf("reconnect failed after %d attempts: %w", conn.maxReconnectAttempts, lastErr)
		}

		var err error
		newConn, err := net.Dial("tcp", net.JoinHostPort(conn.ip, strconv.Itoa(conn.port)))
		if err != nil {
			lastErr = err
			conn.logger.Warn("reconnect dial failed, retrying", "error", err, "attempt", attempts)
			time.Sleep(conn.reconnectInterval)
			continue
		}
		conn.connMu.Lock()
		conn.connection = newConn
		conn.connMu.Unlock()
		// Enable aggressive TCP keepalive to detect dead connections quickly
		if tcpConn, ok := newConn.(*net.TCPConn); ok {
			tcpConn.SetKeepAliveConfig(net.KeepAliveConfig{
				Enable:   true,
				Idle:     3 * time.Second,
				Interval: 2 * time.Second,
				Count:    5,
			})
		}

		// Clear disconnected flag so sendRequest works during symbol load
		conn.disconnected.Store(false)

		// Re-start goroutines
		conn.waitGroup.Add(2)
		go conn.listen()
		go conn.transmitWorker()

		// Re-perform local-mode handshake if needed
		if conn.isLocal {
			resp, err := conn.send([]byte{0, 16, 2, 0, 0, 0, 0, 0})
			if err != nil {
				lastErr = err
				conn.logger.Warn("reconnect local handshake failed, retrying", "error", err, "attempt", attempts)
				conn.resetForRetry()
				time.Sleep(conn.reconnectInterval)
				continue
			}
			buf := bytes.NewBuffer(resp)
			result := AmsAddress{}
			if err = binary.Read(buf, binary.LittleEndian, &result); err != nil {
				conn.logger.Warn("reconnect local handshake parse failed, retrying", "error", err, "attempt", attempts)
				conn.resetForRetry()
				time.Sleep(conn.reconnectInterval)
				continue
			}
			conn.source = result
		}

		// Re-load symbols based on discovery mode
		if err := conn.reloadSymbols(); err != nil {
			lastErr = err
			conn.logger.Warn("reconnect symbol reload failed, retrying", "error", err, "attempt", attempts)
			conn.resetForRetry()
			time.Sleep(conn.reconnectInterval)
			continue
		}

		// Re-subscribe notifications using stored configs (don't re-append)
		conn.symbolLock.Lock()
		savedConfigs := conn.notificationConfigs
		savedChannel := conn.notificationChannel
		conn.notificationConfigs = nil // Clear before re-adding to prevent duplicates
		conn.symbolLock.Unlock()
		if len(savedConfigs) > 0 && savedChannel != nil {
			err = conn.AddSymbolNotifications(savedConfigs, savedChannel)
			if err != nil {
				conn.logger.Warn("reconnect notification re-subscribe failed, retrying", "error", err, "attempt", attempts)
				// Restore configs so they can be retried on the next attempt
				conn.symbolLock.Lock()
				conn.notificationConfigs = savedConfigs
				conn.notificationChannel = savedChannel
				conn.symbolLock.Unlock()
				conn.resetForRetry()
				time.Sleep(conn.reconnectInterval)
				continue
			}
		}

		conn.disconnected.Store(false)
		conn.logger.Info("reconnect successful", "attempts", attempts)
		return nil
	}
}

// reloadSymbols re-establishes the symbol table after a reconnect, matching
// the discovery mode that was used before the connection dropped.
func (conn *Connection) reloadSymbols() error {
	conn.symbolLock.Lock()
	fullyLoaded := conn.symbolsFullyLoaded
	listLoaded := conn.symbolListLoaded
	dtLoaded := conn.datatypesLoaded
	hasOnDemand := len(conn.onDemandSymbols) > 0
	conn.symbolLock.Unlock()

	switch {
	case fullyLoaded:
		// Full discovery was done — redo it
		return conn.loadSymbols()

	case listLoaded || dtLoaded:
		// Partial discovery — re-download what was loaded
		if listLoaded {
			if err := conn.LoadSymbolList(SlowDiscoveryConfig{}); err != nil {
				return fmt.Errorf("reload symbol list: %w", err)
			}
		}
		if dtLoaded {
			if err := conn.LoadDataTypes(SlowDiscoveryConfig{}); err != nil {
				return fmt.Errorf("reload datatypes: %w", err)
			}
		}

	case hasOnDemand:
		// On-demand mode: re-resolve only the symbols that were previously loaded
		conn.symbolLock.Lock()
		oldSymbols := conn.onDemandSymbols
		conn.symbols = make(map[string]*Symbol)
		conn.onDemandSymbols = make(map[string]bool)
		conn.symbolLock.Unlock()

		for name := range oldSymbols {
			if _, err := conn.GetSymbol(name); err != nil {
				return fmt.Errorf("re-resolve symbol %q: %w", name, err)
			}
		}

	default:
		// No symbols were loaded — read symbol version for future use
		version, err := conn.GetSymbolVersion()
		if err != nil {
			conn.logger.Debug("could not read symbol version during reconnect", "error", err)
		} else {
			conn.symbolLock.Lock()
			conn.symbolVersion = version
			conn.symbolLock.Unlock()
		}
	}

	return nil
}

// resetForRetry tears down goroutines, closes the TCP connection, and resets
// channels/state so the next retry iteration starts clean.
func (conn *Connection) resetForRetry() {
	conn.disconnected.Store(true)
	conn.shutdown()
	conn.connMu.Lock()
	if conn.connection != nil {
		conn.connection.Close()
	}
	conn.connMu.Unlock()
	conn.waitGroup.Wait()
	conn.ctx, conn.shutdown = context.WithCancel(context.Background())
	conn.sendChannel = make(chan []byte)
	conn.systemResponse = make(chan []byte)
	conn.activeRequestLock.Lock()
	conn.activeRequests = map[uint32]chan []byte{}
	conn.activeRequestLock.Unlock()
}

// loadSymbols loads symbol table and datatypes from the PLC, and saves the symbol version.
func (conn *Connection) loadSymbols() error {
	// Read and store symbol version
	version, err := conn.GetSymbolVersion()
	if err != nil {
		conn.logger.Warn("failed to read symbol version, continuing with symbol load", "error", err)
	} else {
		conn.symbolLock.Lock()
		conn.symbolVersion = version
		conn.symbolLock.Unlock()
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
	symbolsResponse, err := conn.GetUploadSymbolInfoSymbols(res.SymbolLength)
	if err != nil {
		return fmt.Errorf("failed to upload symbols: %w", err)
	}
	symbols, err := ParseUploadSymbolInfoSymbols(symbolsResponse, datatypes)
	if err != nil {
		return fmt.Errorf("failed to parse symbols: %w", err)
	}
	conn.symbolLock.Lock()
	conn.datatypes = datatypes
	conn.symbols = symbols
	conn.symbolLock.Unlock()
	return nil
}

// IsDisconnected returns whether the connection is currently in a disconnected state.
func (conn *Connection) IsDisconnected() bool {
	return conn.disconnected.Load()
}

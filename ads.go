package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

func (conn *Connection) GetSymbol(symbolName string) (*Symbol, error) {
	conn.symbolLock.Lock()
	defer conn.symbolLock.Unlock()
	localSymbol, ok := conn.symbols[symbolName]
	if ok {
		if localSymbol.Handle == 0 {
			handle, err := conn.GetHandleByName(symbolName)
			if err != nil {
				return nil, err
			}
			localSymbol.Handle = handle
		}
		log.Trace().
			Interface("symbol", localSymbol).
			Msg("symbol got")
		return localSymbol, nil
	}
	err := fmt.Errorf("symbol does not exist")
	log.Error().
		Err(err).
		Str("symbol name", symbolName).
		Msg("error getting handle by name")
	return nil, err
}

func (conn *Connection) GetHandleByName(symbolName string) (handle uint32, err error) {
	resp, err := conn.WriteRead(uint32(GroupSymbolHandleByName), 0, 4, []byte(symbolName))
	if err != nil {
		log.Error().
			Err(err).
			Str("symbol name", symbolName).
			Msg("error getting handle by name")
		return 0, err
	}
	handle = binary.LittleEndian.Uint32(resp)
	return handle, err
}

func (conn *Connection) WriteToSymbol(symbolName string, value string) error {
	symbol, err := conn.GetSymbol(symbolName)
	conn.symbolLock.Lock()
	defer conn.symbolLock.Unlock()
	if err != nil {
		log.Error().
			Err(err).
			Msg("error getting symbol")
		return err
	}
	data, err := symbol.writeToNode(value, 0, conn.datatypes)
	if err != nil {
		log.Error().
			Err(err).
			Msg("error during write to symbol")
		return err
	}
	err = conn.Write(uint32(GroupSymbolValueByHandle), symbol.Handle, data)
	if err != nil {
		log.Error().
			Err(err).
			Msg("error during write to symbol")
		return err
	}
	log.Trace().
		Str("symbol", symbolName).
		Str("Value", value).
		Msg("wrote to symbol")
	return err
}

func (conn *Connection) ReadFromSymbol(symbolName string) (string, error) {
	symbol, err := conn.GetSymbol(symbolName)
	conn.symbolLock.Lock()
	defer conn.symbolLock.Unlock()
	if err != nil {
		log.Error().
			Err(err).
			Str("symbol", symbolName).
			Msg("error getting symbol")
		return "", err
	}
	now := time.Now()
	if now.Sub(symbol.LastUpdateTime) < symbol.MinUpdateInterval && symbol.Value != "" {
		return symbol.Value, nil
	}
	data, err := conn.Read(uint32(GroupSymbolValueByHandle), symbol.Handle, symbol.Length)
	if err != nil {
		log.Error().
			Err(err).
			Str("symbol", symbolName).
			Msg("error during read symbol")
		return "", err
	}
	log.Trace().
		Str("symbol", symbolName).
		Str("Value", symbol.Value).
		Msg("Rdebug")
	value, err := symbol.parse(data, 0)
	if err != nil {
		log.Error().
			Err(err).
			Str("symbol", symbolName).
			Msg("error during parse symbol")
		return "", err
	}
	log.Trace().
		Str("symbol", symbolName).
		Str("Value", value).
		Msg("Read from symbol")
	symbol.LastUpdateTime = now
	symbol.Value = value
	return value, nil
}

func (conn *Connection) GetSymbolUploadInfo() (uploadInfo SymbolUploadInfo, err error) {
	// Try extended info (0xF00F) first, fall back to basic info (0xF00C)
	res, err := conn.Read(uint32(GroupSymbolUploadInfo2), 0, 24)
	if err != nil {
		log.Debug().Err(err).Msg("GroupSymbolUploadInfo2 not supported, falling back to GroupSymbolUploadInfo")
		res, err = conn.Read(uint32(GroupSymbolUploadInfo), 0, 16)
		if err != nil {
			return uploadInfo, fmt.Errorf("GetSymbolUploadInfo failed: %w", err)
		}
	}
	buff := bytes.NewBuffer(res)
	binary.Read(buff, binary.LittleEndian, &uploadInfo.SymbolCount)
	binary.Read(buff, binary.LittleEndian, &uploadInfo.SymbolLength)
	binary.Read(buff, binary.LittleEndian, &uploadInfo.DataTypeCount)
	binary.Read(buff, binary.LittleEndian, &uploadInfo.DataTypeLength)
	if buff.Len() >= 8 {
		binary.Read(buff, binary.LittleEndian, &uploadInfo.ExtraCount)
		binary.Read(buff, binary.LittleEndian, &uploadInfo.ExtraLength)
	}
	return
}

func (conn *Connection) GetUploadSymbolInfoSymbols(length uint32) (data []byte, err error) {
	res, err := conn.Read(uint32(GroupSymbolUpload), 0, length) //UploadSymbolInfo;
	if err != nil {
		return nil, fmt.Errorf("GetUploadSymbolInfoSymbols failed: %w", err)
	}
	return res, nil
}

func (conn *Connection) GetUploadSymbolInfoDataTypes(length uint32) (data []byte, err error) {
	data, err = conn.Read(
		uint32(GroupSymbolDataTypeUpload),
		0x0,
		length)
	if err != nil {
		return nil, fmt.Errorf("error doing DT UPLOAD %d", err)
	}
	return data, nil
}

// GetSymbolVersion reads the current symbol version from the PLC.
func (conn *Connection) GetSymbolVersion() (uint32, error) {
	data, err := conn.Read(uint32(GroupSymbolVersion), 0, 4)
	if err != nil {
		return 0, fmt.Errorf("failed to read symbol version: %w", err)
	}
	if len(data) < 4 {
		return 0, fmt.Errorf("symbol version response too short: %d bytes", len(data))
	}
	return binary.LittleEndian.Uint32(data), nil
}

// CheckSymbolVersion compares the current PLC symbol version against the stored version.
// Returns true if the version has changed.
func (conn *Connection) CheckSymbolVersion() (changed bool, err error) {
	version, err := conn.GetSymbolVersion()
	if err != nil {
		return false, err
	}
	if version != conn.symbolVersion {
		log.Info().
			Uint32("old", conn.symbolVersion).
			Uint32("new", version).
			Msg("symbol version changed")
		return true, nil
	}
	return false, nil
}

// RefreshSymbols reloads the symbol table if the symbol version has changed.
// It releases old handles, reloads symbol/datatype tables, and re-acquires handles for active symbols.
func (conn *Connection) RefreshSymbols() error {
	changed, err := conn.CheckSymbolVersion()
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}

	// Release old handles
	conn.symbolLock.Lock()
	for _, symbol := range conn.symbols {
		if symbol.Handle != 0 {
			handleBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(handleBytes, symbol.Handle)
			conn.Write(uint32(GroupSymbolReleaseHandle), 0, handleBytes)
			symbol.Handle = 0
		}
	}
	conn.symbolLock.Unlock()

	// Reload symbols
	err = conn.loadSymbols()
	if err != nil {
		return fmt.Errorf("failed to refresh symbols: %w", err)
	}

	log.Info().Uint32("version", conn.symbolVersion).Msg("symbols refreshed")
	return nil
}

func (conn *Connection) AddSymbolNotification(symbolName string, maxDelay int, cycleTime int, transMode TransMode, updateReceiver chan *Update) error {
	symbol, err := conn.GetSymbol(symbolName)
	if err != nil {
		log.
			Error().
			Str("symbol", symbolName).
			Err(err).
			Msg("error getting symbol")
		return err
	}
	handle, err := conn.AddDeviceNotification(
		uint32(GroupSymbolValueByHandle),
		symbol.Handle,
		symbol.Length,
		transMode,
		time.Duration(maxDelay)*time.Millisecond,
		time.Duration(cycleTime)*time.Millisecond)
	if err != nil {
		return err
	}
	log.Info().
		Int("handle", int(handle)).
		Str("symbol", symbolName).
		Msg("notification created")
	conn.symbolLock.Lock()
	defer conn.symbolLock.Unlock()
	symbol.Notification = updateReceiver
	conn.activeNotifications[handle] = symbol
	return nil
}

type Update struct {
	Variable  string
	Value     string
	TimeStamp time.Time
}

// NotificationConfig holds configuration for a symbol notification, used for batch add and reconnect re-subscribe.
type NotificationConfig struct {
	SymbolName       string
	MaxDelay         int
	CycleTime        int
	TransmissionMode TransMode
}

// ReadMultipleSymbols reads multiple symbols in a single ADS round-trip using SumRead.
// Returns a map of symbol name to parsed string value.
func (conn *Connection) ReadMultipleSymbols(names []string) (map[string]string, error) {
	if len(names) == 0 {
		return nil, nil
	}

	// Resolve symbols and build SumRead requests
	type symbolInfo struct {
		name   string
		symbol *Symbol
	}
	var infos []symbolInfo
	var requests []SumReadRequest

	for _, name := range names {
		symbol, err := conn.GetSymbol(name)
		if err != nil {
			log.Error().Err(err).Str("symbol", name).Msg("error getting symbol for batch read")
			continue
		}
		infos = append(infos, symbolInfo{name: name, symbol: symbol})
		requests = append(requests, SumReadRequest{
			Group:  uint32(GroupSymbolValueByHandle),
			Offset: symbol.Handle,
			Length: symbol.Length,
		})
	}

	if len(requests) == 0 {
		return nil, fmt.Errorf("no valid symbols found for batch read")
	}

	results, err := conn.SumRead(requests)
	if err != nil {
		return nil, fmt.Errorf("batch read failed: %w", err)
	}

	values := make(map[string]string, len(results))
	conn.symbolLock.Lock()
	defer conn.symbolLock.Unlock()

	for i, result := range results {
		if result.Error != ReturnCodeNoErrors {
			log.Warn().
				Str("symbol", infos[i].name).
				Uint32("error", uint32(result.Error)).
				Msg("symbol read error in batch")
			continue
		}
		value, err := infos[i].symbol.parse(result.Data, 0)
		if err != nil {
			log.Error().Err(err).Str("symbol", infos[i].name).Msg("error parsing symbol in batch read")
			continue
		}
		now := time.Now()
		infos[i].symbol.LastUpdateTime = now
		infos[i].symbol.Value = value
		values[infos[i].name] = value
	}

	return values, nil
}

// AddSymbolNotifications adds multiple symbol notifications in a single ADS round-trip using SumAddDeviceNotification.
func (conn *Connection) AddSymbolNotifications(configs []NotificationConfig, ch chan *Update) error {
	if len(configs) == 0 {
		return nil
	}

	// Resolve symbols and build requests
	type symbolInfo struct {
		config NotificationConfig
		symbol *Symbol
	}
	var infos []symbolInfo
	var requests []SumNotificationRequest

	for _, cfg := range configs {
		symbol, err := conn.GetSymbol(cfg.SymbolName)
		if err != nil {
			log.Error().Err(err).Str("symbol", cfg.SymbolName).Msg("error getting symbol for batch notification")
			continue
		}
		infos = append(infos, symbolInfo{config: cfg, symbol: symbol})
		requests = append(requests, SumNotificationRequest{
			Group:            uint32(GroupSymbolValueByHandle),
			Offset:           symbol.Handle,
			Length:           symbol.Length,
			TransmissionMode: cfg.TransmissionMode,
			MaxDelay:         time.Duration(cfg.MaxDelay) * time.Millisecond,
			CycleTime:        time.Duration(cfg.CycleTime) * time.Millisecond,
		})
	}

	if len(requests) == 0 {
		return fmt.Errorf("no valid symbols for batch notification add")
	}

	handles, errors, err := conn.SumAddDeviceNotification(requests)
	if err != nil {
		return fmt.Errorf("batch add notification failed: %w", err)
	}

	conn.symbolLock.Lock()
	defer conn.symbolLock.Unlock()

	for i, h := range handles {
		if errors[i] != ReturnCodeNoErrors {
			log.Error().
				Str("symbol", infos[i].config.SymbolName).
				Uint32("error", uint32(errors[i])).
				Msg("error adding notification in batch")
			continue
		}
		infos[i].symbol.Notification = ch
		conn.activeNotifications[h] = infos[i].symbol
		log.Info().
			Uint32("handle", h).
			Str("symbol", infos[i].config.SymbolName).
			Msg("batch notification created")
	}

	// Store notification configs and channel for reconnect
	conn.notificationConfigs = append(conn.notificationConfigs, configs...)
	conn.notificationChannel = ch

	return nil
}

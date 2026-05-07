package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
)

// Single-symbol ADS commands: Read, Write, WriteRead, ReadState.
// Basic per-symbol primitives used directly via Connection.{Read,Write,WriteRead}
// and indirectly via ReadFromSymbol/WriteToSymbol in symbol_access.go.

func (conn *Connection) Read(group uint32, offset uint32, length uint32) (data []byte, err error) {
	request := new(bytes.Buffer)
	type readCommandPacket struct {
		Group  uint32
		Offset uint32
		Length uint32
	}
	content := readCommandPacket{
		group,
		offset,
		length,
	}

	err = binary.Write(request, binary.LittleEndian, content)
	if err != nil {
		conn.logger.Error("binary.Write failed", "error", err)
		return nil, err
	}

	conn.logger.Log(context.Background(), LevelTrace, "request", "request", content)

	// Try to send the request
	resp, err := conn.sendRequest(CommandIDRead, request.Bytes())
	if err != nil {
		conn.logger.Error("send request failed", "error", err)
		return
	}

	// Check the result error code
	type readResponse struct {
		Error  ReturnCode
		Length uint32
	}
	respBuff := bytes.NewBuffer(resp)
	response := &readResponse{}
	err = binary.Read(respBuff, binary.LittleEndian, response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Read response: %w", err)
	}
	if response.Error > 0 {
		err = fmt.Errorf("ADS error in Read: %w", response.Error)
		return
	}
	// validate that the body has the bytes the response header declared.
	// Returning the raw remaining buffer would silently pass through truncated
	// or padded payloads. Trust the declared Length; reject undersized.
	if uint64(respBuff.Len()) < uint64(response.Length) {
		return nil, fmt.Errorf("Read: declared length %d, body has %d bytes", response.Length, respBuff.Len())
	}
	return respBuff.Next(int(response.Length)), nil
}

// Write - ADS command id: 3
func (conn *Connection) Write(group uint32, offset uint32, data []byte) error {
	type writeCommandPacket struct {
		Group  uint32
		Offset uint32
		Length uint32
	}
	request := new(bytes.Buffer)
	writeRequest := writeCommandPacket{
		group,
		offset,
		uint32(len(data)),
	}

	err := binary.Write(request, binary.LittleEndian, writeRequest)
	if err != nil {
		conn.logger.Error("binary.Write failed", "error", err)
		return err
	}
	err = binary.Write(request, binary.LittleEndian, data)
	if err != nil {
		conn.logger.Error("binary.Write failed", "error", err)
		return err
	}

	// Try to send the request
	resp, err := conn.sendRequest(CommandIDWrite, request.Bytes())
	if err != nil {
		conn.logger.Error("error during send request for write", "error", err)
		return err
	}
	respBuffer := bytes.NewBuffer(resp)
	var respCode ReturnCode
	// Check the result error code
	if err = binary.Read(respBuffer, binary.LittleEndian, &respCode); err != nil {
		return fmt.Errorf("failed to parse Write response: %w", err)
	}
	if respCode > 0 {
		return fmt.Errorf("ADS error in Write: %w", respCode)
	}

	return nil
}

func (conn *Connection) WriteRead(group uint32, offset uint32, readLength uint32, send []byte) (data []byte, err error) {
	request := new(bytes.Buffer)
	type writeReadCommandPacket struct {
		Group       uint32
		Offset      uint32
		ReadLength  uint32
		WriteLength uint32
	}
	content := writeReadCommandPacket{
		group,
		offset,
		readLength,
		uint32(len(send)),
	}

	type readResponse struct {
		Error  ReturnCode
		Length uint32
	}

	err = binary.Write(request, binary.LittleEndian, content)
	if err != nil {
		return nil, fmt.Errorf("binary.Write failed: %w", err)
	}
	err = binary.Write(request, binary.LittleEndian, send)
	if err != nil {
		return nil, fmt.Errorf("binary.Write failed: %w", err)
	}

	conn.logger.Log(context.Background(), LevelTrace, "request", "request", request)

	// Try to send the request
	resp, err := conn.sendRequest(CommandIDReadWrite, request.Bytes())
	if err != nil {
		return
	}

	// Check the result error code
	respBuff := bytes.NewBuffer(resp)
	response := &readResponse{}
	if err = binary.Read(respBuff, binary.LittleEndian, response); err != nil {
		return nil, fmt.Errorf("failed to parse WriteRead response: %w", err)
	}
	if response.Error > 0 {
		return nil, fmt.Errorf("ADS error in WriteRead: %w", response.Error)
	}
	// validate body length against declared Length. See commandRead.go.
	if uint64(respBuff.Len()) < uint64(response.Length) {
		return nil, fmt.Errorf("WriteRead: declared length %d, body has %d bytes", response.Length, respBuff.Len())
	}
	return respBuff.Next(int(response.Length)), nil
}

// ReadStateResponse - ADS command id: 4
// States holds the ADS and device state returned by ReadState.
type States struct {
	ADSState    ADSState
	DeviceState uint16
}

func (conn *Connection) ReadState() (response States, err error) {
	// Try to send the request
	resp, err := conn.sendRequest(CommandIDReadState, []byte{})
	if err != nil {
		conn.logger.Error("Error during read state", "error", err)
		return
	}
	conn.logger.Log(context.Background(), LevelTrace, "response from plc for state", "data", resp)
	type readStateResponse struct {
		Error ReturnCode
		States
	}
	stateResponse := &readStateResponse{}
	buff := bytes.NewBuffer(resp)
	if err = binary.Read(buff, binary.LittleEndian, stateResponse); err != nil {
		return response, err
	}
	if stateResponse.Error > 0 {
		return response, fmt.Errorf("ADS error in ReadState: %w", stateResponse.Error)
	}
	conn.logger.Debug("read state response",
		"ADSState", uint16(stateResponse.ADSState),
		"deviceState", stateResponse.DeviceState)

	return stateResponse.States, nil
}

// DeviceInfo connected device info
type DeviceInfo struct {
	Major      uint8
	Minor      uint8
	Version    uint16
	DeviceName [16]byte
}

func (conn *Connection) ReadDeviceInfo() (response DeviceInfo, err error) {
	// Try to send the request
	resp, err := conn.sendRequest(CommandIDReadDeviceInfo, []byte{})
	if err != nil {
		return
	}

	// Check the response length
	if len(resp) != 24 {
		return response, fmt.Errorf("wrong length of response! Got %d bytes and it should be 24", len(resp))
	}
	type readDeviceInfoResponse struct {
		Error      ReturnCode
		DeviceInfo DeviceInfo
	}
	respBuffer := bytes.NewBuffer(resp)
	deviceInfoResponse := readDeviceInfoResponse{}
	if err = binary.Read(respBuffer, binary.LittleEndian, &deviceInfoResponse); err != nil {
		return response, fmt.Errorf("failed to parse ReadDeviceInfo response: %w", err)
	}
	if deviceInfoResponse.Error > 0 {
		err = fmt.Errorf("ADS error in ReadDeviceInfo: %w", deviceInfoResponse.Error)
		return
	}

	return deviceInfoResponse.DeviceInfo, nil
}

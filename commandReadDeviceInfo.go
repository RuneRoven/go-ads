package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

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

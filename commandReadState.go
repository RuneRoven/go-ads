package ads

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
)

// ReadStateResponse - ADS command id: 4
// States holds the ADS and device state returned by ReadState.
type States struct {
	AdsState    AdsState
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
		"adsState", uint16(stateResponse.AdsState),
		"deviceState", stateResponse.DeviceState)

	return stateResponse.States, nil
}

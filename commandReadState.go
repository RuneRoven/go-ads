package ads

import (
	"bytes"
	"encoding/binary"

	"github.com/rs/zerolog/log"
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
		log.Error().
			Err(err).
			Msg("Error during read state")
		return
	}
	log.Trace().
		Bytes("data", resp).
		Msg("response from plc for state")
	type readStateResponse struct {
		Error ReturnCode
		States
	}
	stateResponse := &readStateResponse{}
	buff := bytes.NewBuffer(resp)
	if err = binary.Read(buff, binary.LittleEndian, stateResponse); err != nil {
		return response, err
	}
	log.Debug().
		Uint16("adsState", uint16(stateResponse.AdsState)).
		Uint16("deviceState", stateResponse.DeviceState).
		Msg("read state response")

	return stateResponse.States, nil
}

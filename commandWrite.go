package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/rs/zerolog/log"
)

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
		log.Error().Err(err).Msg("binary.Write failed")
		return err
	}
	err = binary.Write(request, binary.LittleEndian, data)
	if err != nil {
		log.Error().Err(err).Msg("binary.Write failed")
		return err
	}

	// Try to send the request
	resp, err := conn.sendRequest(CommandIDWrite, request.Bytes())
	if err != nil {
		log.Error().
			Err(err).
			Msg("error during send request for write")
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

package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/rs/zerolog/log"
)

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
		log.Error().Err(err).Msg("binary.Write failed")
		return nil, err
	}

	log.Trace().
		Interface("request", content).
		Msg("request")

	// Try to send the request
	resp, err := conn.sendRequest(CommandIDRead, request.Bytes())
	if err != nil {
		log.Error().Err(err).Msg("send request failed")
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
	return respBuff.Bytes(), nil
}

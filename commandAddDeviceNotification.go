package ads

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
)

func (conn *Connection) AddDeviceNotification(
	group uint32,
	offset uint32,
	length uint32,
	transmissionMode TransMode,
	maxDelay time.Duration,
	cycleTime time.Duration) (handle uint32, err error) {
	request := new(bytes.Buffer)
	type addDeviceNotificationCommandPacket struct {
		Group            uint32
		Offset           uint32
		Length           uint32
		TransmissionMode uint32
		MaxDelay         uint32
		CycleTime        uint32
		Reserved         [16]byte
	}

	var content = addDeviceNotificationCommandPacket{
		group,
		offset,
		length,
		uint32(transmissionMode),
		uint32(maxDelay.Nanoseconds() / 100),  // 1 = 1ms (alt 100ns?)
		uint32(cycleTime.Nanoseconds() / 100), // 1 = 1ms
		[16]byte{},
	}
	if err = binary.Write(request, binary.LittleEndian, content); err != nil {
		return 0, fmt.Errorf("binary.Write failed: %w", err)
	}
	type addDeviceNotificationResponse struct {
		Error  ReturnCode
		Handle uint32
	}
	// Try to send the request
	resp, err := conn.sendRequest(CommandIDAddDeviceNotification, request.Bytes())
	if err != nil {
		return
	}
	respBuffer := bytes.NewBuffer(resp)
	notificationResponse := addDeviceNotificationResponse{}
	err = binary.Read(respBuffer, binary.LittleEndian, &notificationResponse)
	if err != nil {
		log.Error().
			Err(err).
			Msg("failed to parse notification response")
		return 0, err
	}
	if notificationResponse.Error != 0 {
		log.Error().
			Uint32("errorCode", uint32(notificationResponse.Error)).
			Msg("failed to add notification handler")
		return 0, fmt.Errorf("unable to create notification: %w", notificationResponse.Error)
	}
	log.Trace().
		Uint32("handle", notificationResponse.Handle).
		Msg("added notification handler")

	return notificationResponse.Handle, nil
}

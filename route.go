package ads

import (
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"time"
)

// UDP route registration constants
const (
	routePort       = 48899
	routeCookie     = 0x71146603
	routeServiceAdd = 6

	tagPassword      uint16 = 2
	tagComputerName  uint16 = 5
	tagNetID         uint16 = 7
	tagRouteName     uint16 = 12
	tagUsername      uint16 = 13
	tagResponseError uint16 = 1
)

// AddRemoteRoute registers a route on the remote PLC via the Beckhoff UDP protocol (port 48899).
// This tells the PLC how to reach this client's AmsNetId.
//
// Security: credentials are transmitted in cleartext over UDP. This is a limitation of
// Beckhoff's route registration protocol — there is no encrypted alternative.
// Ensure this is only called on trusted networks.
//
// Parameters:
//   - remoteHost: IP or hostname of the PLC
//   - localNetId: the AMS NetID this client will use as source
//   - routeName: name for the route entry on the PLC
//   - computerName: the IP/hostname the PLC should use to connect back to this client
//   - username: PLC admin username (typically "Administrator")
//   - password: PLC admin password
func AddRemoteRoute(remoteHost string, localNetId [6]byte, routeName string, computerName string, username string, password string) error {
	return AddRemoteRouteWithLogger(getDefaultLogger(), remoteHost, localNetId, routeName, computerName, username, password)
}

// AddRemoteRouteWithLogger is like AddRemoteRoute but accepts an explicit logger.
func AddRemoteRouteWithLogger(logger *slog.Logger, remoteHost string, localNetId [6]byte, routeName string, computerName string, username string, password string) error {
	logger.Info("registering route",
		"remoteHost", remoteHost,
		"localNetId", fmt.Sprintf("%d.%d.%d.%d.%d.%d", localNetId[0], localNetId[1], localNetId[2], localNetId[3], localNetId[4], localNetId[5]),
		"computerName", computerName,
		"routeName", routeName,
		"hasAuth", username != "")
	addr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", remoteHost, routePort))
	if err != nil {
		return fmt.Errorf("failed to resolve remote host: %w", err)
	}

	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		return fmt.Errorf("failed to dial UDP: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// F-24: generate random invokeId via crypto/rand for response-echo
	// validation in parseRouteResponse. Defends against UDP spoofing on the
	// local network — an attacker would need to predict the random per-call
	// value to inject a fake "success" response.
	var invokeIdBuf [4]byte
	if _, err := cryptorand.Read(invokeIdBuf[:]); err != nil {
		return fmt.Errorf("generate invokeId: %w", err)
	}
	invokeId := binary.LittleEndian.Uint32(invokeIdBuf[:])

	// Build the route request packet
	packet := buildRoutePacket(localNetId, routeName, computerName, username, password, invokeId)

	_, err = conn.Write(packet)
	if err != nil {
		return fmt.Errorf("failed to send route request: %w", err)
	}

	// Wait for response
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("failed to set read deadline: %w", err)
	}
	respBuf := make([]byte, 1024)
	n, err := conn.Read(respBuf)
	if err != nil {
		return fmt.Errorf("failed to read route response: %w", err)
	}

	return parseRouteResponse(logger, respBuf[:n], invokeId)
}

// buildRoutePacket constructs a UDP route registration packet.
// invokeId is set by the caller (per ADS InvokeID semantics) to identify the
// command and validate the response echo. Use a random uint32 from crypto/rand
// to defend against UDP spoofing on the local network (F-24).
func buildRoutePacket(localNetId [6]byte, routeName string, computerName string, username string, password string, invokeId uint32) []byte {
	// Build tags
	tags := [][]byte{
		buildTag(tagNetID, localNetId[:]),
		buildTag(tagPassword, appendNull([]byte(password))),
		buildTag(tagComputerName, appendNull([]byte(computerName))),
		buildTag(tagRouteName, appendNull([]byte(routeName))),
		buildTag(tagUsername, appendNull([]byte(username))),
	}

	var tagsData []byte
	for _, tag := range tags {
		tagsData = append(tagsData, tag...)
	}

	// Header: cookie(4) + invokeId(4) + serviceId(4) + AmsAddr(8) + tagCount(4)
	header := make([]byte, 24)
	binary.LittleEndian.PutUint32(header[0:], routeCookie)
	binary.LittleEndian.PutUint32(header[4:], invokeId) // F-24: caller-provided random invokeId for echo validation
	binary.LittleEndian.PutUint32(header[8:], routeServiceAdd)
	// AmsAddr: NetID(6) + Port(2) — port is 0 per Beckhoff spec
	copy(header[12:18], localNetId[:])
	binary.LittleEndian.PutUint16(header[18:], 0)
	binary.LittleEndian.PutUint32(header[20:], uint32(len(tags)))

	return append(header, tagsData...)
}

// buildTag creates a single tag: tagID(2) + length(2) + data.
func buildTag(tagID uint16, data []byte) []byte {
	tag := make([]byte, 4+len(data))
	binary.LittleEndian.PutUint16(tag[0:], tagID)
	binary.LittleEndian.PutUint16(tag[2:], uint16(len(data)))
	copy(tag[4:], data)
	return tag
}

// appendNull appends a null terminator to a byte slice.
func appendNull(data []byte) []byte {
	return append(data, 0)
}

// parseRouteResponse validates the route registration response.
// Response format: cookie(4) + invokeId(4) + serviceId(4) + AmsAddr(8) + tagCount(4) + tags...
//
// expectedInvokeId is the value the caller provided in the request; the PLC
// echoes it per ADS InvokeID semantics and we reject mismatches as possible
// UDP-spoofing attempts (F-24).
func parseRouteResponse(logger *slog.Logger, data []byte, expectedInvokeId uint32) error {
	logger.Debug("route response raw bytes", hexAttr("response", data), "length", len(data))

	if len(data) < 24 {
		return fmt.Errorf("route response too short: %d bytes", len(data))
	}

	cookie := binary.LittleEndian.Uint32(data[0:])
	if cookie != routeCookie {
		return fmt.Errorf("unexpected route response cookie: 0x%08X", cookie)
	}

	// F-24: validate invokeId echo. Defends against UDP spoofing on the local
	// network — an attacker would need to predict the random per-call invokeId
	// to inject a fake "success" response.
	gotInvokeId := binary.LittleEndian.Uint32(data[4:])
	if gotInvokeId != expectedInvokeId {
		return fmt.Errorf("route response invokeId mismatch: got 0x%08X, expected 0x%08X (possible spoof or PLC misbehavior)", gotInvokeId, expectedInvokeId)
	}

	serviceId := binary.LittleEndian.Uint32(data[8:])
	// Response serviceId has the RESPONSE flag (0x80000000) set
	if serviceId != (0x80000000 | routeServiceAdd) {
		return fmt.Errorf("unexpected route response serviceId: 0x%08X", serviceId)
	}

	// Skip AmsAddr (8 bytes at offset 12), tagCount is at offset 20
	tagCount := binary.LittleEndian.Uint32(data[20:])
	logger.Debug("route response tags", "tagCount", tagCount)
	offset := 24
	for i := uint32(0); i < tagCount; i++ {
		if offset+4 > len(data) {
			return fmt.Errorf("route response truncated: incomplete tag %d header", i)
		}
		tid := binary.LittleEndian.Uint16(data[offset:])
		tlen := binary.LittleEndian.Uint16(data[offset+2:])
		offset += 4
		if offset+int(tlen) > len(data) {
			return fmt.Errorf("route response truncated: tag %d data exceeds response", tid)
		}
		logger.Debug("route response tag", "tagID", tid, "tagLen", tlen, hexAttr("tagData", data[offset:offset+int(tlen)]))
		if tid == tagResponseError && tlen >= 4 {
			errCode := binary.LittleEndian.Uint32(data[offset:])
			if errCode != 0 {
				return fmt.Errorf("route registration failed with error code: %d", errCode)
			}
			logger.Info("route registration successful")
			return nil
		}
		offset += int(tlen)
	}

	logger.Info("route registration response received (no error tag found, assuming success)")
	return nil
}

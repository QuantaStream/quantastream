package qsmysql

import "fmt"

// HandshakeResponse41 is the decoded subset of a MySQL client handshake response.
type HandshakeResponse41 struct {
	CapabilityFlags CapabilityFlag
	MaxPacketSize   uint32
	CharacterSet    CharacterSet
	Username        string
	AuthResponse    []byte
	Database        string
	AuthPluginName  string
}

// DecodeHandshakeResponse41 decodes the capability-41 client handshake response subset QuantaStream needs.
func DecodeHandshakeResponse41(payload []byte) (HandshakeResponse41, error) {
	if len(payload) < 36 {
		return HandshakeResponse41{}, fmt.Errorf("mysql handshake response too short: %d bytes", len(payload))
	}
	response := HandshakeResponse41{
		CapabilityFlags: CapabilityFlag(readUint32LE(payload[0:4])),
		MaxPacketSize:   readUint32LE(payload[4:8]),
		CharacterSet:    CharacterSet(payload[8]),
	}
	if response.CapabilityFlags&CapabilityProtocol41 == 0 {
		return HandshakeResponse41{}, fmt.Errorf("mysql handshake response requires CLIENT_PROTOCOL_41")
	}
	offset := 32
	username, next, ok := readNullTerminatedString(payload, offset)
	if !ok {
		return HandshakeResponse41{}, fmt.Errorf("mysql handshake response missing username terminator")
	}
	response.Username = username
	offset = next
	if response.CapabilityFlags&CapabilitySecureConnection != 0 {
		if offset >= len(payload) {
			return HandshakeResponse41{}, fmt.Errorf("mysql handshake response missing auth response length")
		}
		authLength := int(payload[offset])
		offset++
		if offset+authLength > len(payload) {
			return HandshakeResponse41{}, fmt.Errorf("mysql handshake response auth response truncated")
		}
		response.AuthResponse = append([]byte(nil), payload[offset:offset+authLength]...)
		offset += authLength
	} else {
		auth, next, ok := readNullTerminatedBytes(payload, offset)
		if !ok {
			return HandshakeResponse41{}, fmt.Errorf("mysql handshake response missing auth response terminator")
		}
		response.AuthResponse = auth
		offset = next
	}
	if response.CapabilityFlags&CapabilityConnectWithDB != 0 {
		database, next, ok := readNullTerminatedString(payload, offset)
		if !ok {
			return HandshakeResponse41{}, fmt.Errorf("mysql handshake response missing database terminator")
		}
		response.Database = database
		offset = next
	}
	if response.CapabilityFlags&CapabilityPluginAuth != 0 && offset < len(payload) {
		plugin, _, ok := readNullTerminatedString(payload, offset)
		if !ok {
			return HandshakeResponse41{}, fmt.Errorf("mysql handshake response missing auth plugin terminator")
		}
		response.AuthPluginName = plugin
	}
	return response, nil
}

func readUint32LE(data []byte) uint32 {
	return uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
}

func readNullTerminatedString(data []byte, offset int) (string, int, bool) {
	raw, next, ok := readNullTerminatedBytes(data, offset)
	return string(raw), next, ok
}

func readNullTerminatedBytes(data []byte, offset int) ([]byte, int, bool) {
	if offset > len(data) {
		return nil, offset, false
	}
	for i := offset; i < len(data); i++ {
		if data[i] == 0 {
			return append([]byte(nil), data[offset:i]...), i + 1, true
		}
	}
	return nil, len(data), false
}

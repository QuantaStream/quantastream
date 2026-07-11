package qsmysql

import "fmt"

const defaultAuthPluginName = "caching_sha2_password"

// HandshakeV10 is the MySQL protocol 10 initial server greeting.
type HandshakeV10 struct {
	ServerVersion   string
	ConnectionID    uint32
	AuthPluginData  []byte
	CapabilityFlags CapabilityFlag
	CharacterSet    CharacterSet
	StatusFlags     StatusFlag
	AuthPluginName  string
}

// NewDefaultHandshake returns a deterministic, testable protocol 10 greeting model.
func NewDefaultHandshake(connectionID uint32, authPluginData []byte) HandshakeV10 {
	return HandshakeV10{
		ServerVersion:   "8.0.0-quantastream",
		ConnectionID:    connectionID,
		AuthPluginData:  append([]byte(nil), authPluginData...),
		CapabilityFlags: CapabilityLongPassword | CapabilityProtocol41 | CapabilitySecureConnection | CapabilityPluginAuth,
		CharacterSet:    CharacterSetUTF8MB4GeneralCI,
		StatusFlags:     StatusAutocommit,
		AuthPluginName:  defaultAuthPluginName,
	}
}

// Payload encodes the handshake as a MySQL protocol 10 payload.
func (h HandshakeV10) Payload() ([]byte, error) {
	if h.ServerVersion == "" {
		return nil, fmt.Errorf("mysql handshake requires server version")
	}
	if len(h.AuthPluginData) < 8 {
		return nil, fmt.Errorf("mysql handshake auth plugin data must be at least 8 bytes")
	}
	authPluginName := h.AuthPluginName
	if authPluginName == "" {
		authPluginName = defaultAuthPluginName
	}
	capabilities := h.CapabilityFlags
	if capabilities == 0 {
		capabilities = CapabilityLongPassword | CapabilityProtocol41 | CapabilitySecureConnection | CapabilityPluginAuth
	}
	charset := h.CharacterSet
	if charset == 0 {
		charset = CharacterSetUTF8MB4GeneralCI
	}
	status := h.StatusFlags
	if status == 0 {
		status = StatusAutocommit
	}
	if len(h.AuthPluginData)+1 > 255 {
		return nil, fmt.Errorf("mysql handshake auth plugin data length %d exceeds max 254", len(h.AuthPluginData))
	}
	authLength := byte(len(h.AuthPluginData) + 1)
	part2Length := int(authLength) - 8
	if part2Length < 13 {
		part2Length = 13
	}

	payload := make([]byte, 0, 80+len(h.ServerVersion)+len(h.AuthPluginData)+len(authPluginName))
	payload = append(payload, 0x0a)
	payload = append(payload, []byte(h.ServerVersion)...)
	payload = append(payload, 0x00)
	payload = appendUint32LE(payload, h.ConnectionID)
	payload = append(payload, h.AuthPluginData[:8]...)
	payload = append(payload, 0x00)
	payload = appendUint16LE(payload, uint16(capabilities))
	payload = append(payload, byte(charset))
	payload = appendUint16LE(payload, uint16(status))
	payload = appendUint16LE(payload, uint16(uint32(capabilities)>>16))
	payload = append(payload, authLength)
	payload = append(payload, make([]byte, 10)...)
	payload = append(payload, authPart2(h.AuthPluginData[8:], part2Length)...)
	payload = append(payload, []byte(authPluginName)...)
	payload = append(payload, 0x00)
	return payload, nil
}

// Packet encodes the greeting as sequence id 0.
func (h HandshakeV10) Packet() ([]byte, error) {
	payload, err := h.Payload()
	if err != nil {
		return nil, err
	}
	return EncodePacket(Packet{SequenceID: 0, Payload: payload})
}

func authPart2(data []byte, length int) []byte {
	result := make([]byte, length)
	copy(result, data)
	return result
}

func appendUint16LE(out []byte, value uint16) []byte {
	return append(out, byte(value), byte(value>>8))
}

func appendUint32LE(out []byte, value uint32) []byte {
	return append(out, byte(value), byte(value>>8), byte(value>>16), byte(value>>24))
}

package qsmysql

import "fmt"

// Packet is one MySQL packet with the 4-byte header decoded away.
type Packet struct {
	SequenceID byte
	Payload    []byte
}

// EncodePacket returns a single MySQL packet header and payload.
func EncodePacket(packet Packet) ([]byte, error) {
	if len(packet.Payload) > MaxPayloadLength {
		return nil, fmt.Errorf("mysql packet payload length %d exceeds max %d", len(packet.Payload), MaxPayloadLength)
	}
	encoded := make([]byte, PacketHeaderLength+len(packet.Payload))
	length := len(packet.Payload)
	encoded[0] = byte(length)
	encoded[1] = byte(length >> 8)
	encoded[2] = byte(length >> 16)
	encoded[3] = packet.SequenceID
	copy(encoded[PacketHeaderLength:], packet.Payload)
	return encoded, nil
}

// DecodePacket decodes exactly one MySQL packet.
func DecodePacket(data []byte) (Packet, error) {
	if len(data) < PacketHeaderLength {
		return Packet{}, fmt.Errorf("mysql packet too short: %d bytes", len(data))
	}
	length := int(data[0]) | int(data[1])<<8 | int(data[2])<<16
	if len(data)-PacketHeaderLength != length {
		return Packet{}, fmt.Errorf("mysql packet payload length mismatch: header=%d actual=%d", length, len(data)-PacketHeaderLength)
	}
	payload := append([]byte(nil), data[PacketHeaderLength:]...)
	return Packet{SequenceID: data[3], Payload: payload}, nil
}

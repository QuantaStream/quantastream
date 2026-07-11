package qsmysql

import (
	"bytes"
	"testing"
)

func TestHandshakePacketEncodesProtocol10Greeting(t *testing.T) {
	handshake := NewDefaultHandshake(42, []byte("12345678901234567890"))
	packetBytes, err := handshake.Packet()
	if err != nil {
		t.Fatalf("Packet failed: %v", err)
	}
	packet, err := DecodePacket(packetBytes)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	if packet.SequenceID != 0 {
		t.Fatalf("sequence = %d, want 0", packet.SequenceID)
	}
	payload := packet.Payload
	if payload[0] != 0x0a {
		t.Fatalf("protocol = %d, want 10", payload[0])
	}
	if !bytes.Contains(payload, []byte("8.0.0-quantastream\x00")) {
		t.Fatalf("payload missing server version: %q", payload)
	}
	if !bytes.Contains(payload, []byte(defaultAuthPluginName+"\x00")) {
		t.Fatalf("payload missing auth plugin name: %q", payload)
	}
}

func TestHandshakePayloadPlacesPluginNameAfterAdvertisedAuthData(t *testing.T) {
	handshake := NewDefaultHandshake(42, []byte("12345678901234567890"))
	payload, err := handshake.Payload()
	if err != nil {
		t.Fatalf("Payload failed: %v", err)
	}

	plugin, err := handshakePayloadPluginName(payload)
	if err != nil {
		t.Fatalf("parse handshake plugin: %v", err)
	}
	if plugin != defaultAuthPluginName {
		t.Fatalf("plugin = %q, want %q", plugin, defaultAuthPluginName)
	}
}

func handshakePayloadPluginName(payload []byte) (string, error) {
	offset := 1
	for offset < len(payload) && payload[offset] != 0 {
		offset++
	}
	offset += 1 + 4 + 8 + 1 + 2 + 1 + 2 + 2
	authLength := int(payload[offset])
	offset += 1 + 10
	part2Length := authLength - 8
	if part2Length < 13 {
		part2Length = 13
	}
	offset += part2Length
	end := bytes.IndexByte(payload[offset:], 0)
	if end < 0 {
		return "", bytes.ErrTooLarge
	}
	return string(payload[offset : offset+end]), nil
}

func TestHandshakePayloadRejectsShortAuthPluginData(t *testing.T) {
	handshake := NewDefaultHandshake(1, []byte("short"))
	if _, err := handshake.Payload(); err == nil {
		t.Fatal("expected short auth plugin data to fail")
	}
}

func TestHandshakePayloadRejectsOversizedAuthPluginData(t *testing.T) {
	handshake := NewDefaultHandshake(1, bytes.Repeat([]byte{'a'}, 255))
	if _, err := handshake.Payload(); err == nil {
		t.Fatal("expected oversized auth plugin data to fail")
	}
}

func TestDefaultHandshakeAdvertisesSessionTrack(t *testing.T) {
	handshake := NewDefaultHandshake(42, []byte("12345678901234567890"))
	if handshake.CapabilityFlags&CapabilitySessionTrack == 0 {
		t.Fatalf("capabilities = 0x%x, want CLIENT_SESSION_TRACK", uint32(handshake.CapabilityFlags))
	}
	payload, err := handshake.Payload()
	if err != nil {
		t.Fatalf("Payload failed: %v", err)
	}
	encodedCapabilities := handshakePayloadCapabilityFlags(payload)
	if encodedCapabilities&CapabilitySessionTrack == 0 {
		t.Fatalf("encoded capabilities = 0x%x, want CLIENT_SESSION_TRACK", uint32(encodedCapabilities))
	}
}

func handshakePayloadCapabilityFlags(payload []byte) CapabilityFlag {
	offset := 1
	for offset < len(payload) && payload[offset] != 0 {
		offset++
	}
	offset += 1 + 4 + 8 + 1
	lower := uint16(payload[offset]) | uint16(payload[offset+1])<<8
	offset += 2 + 1 + 2
	upper := uint16(payload[offset]) | uint16(payload[offset+1])<<8
	return CapabilityFlag(uint32(lower) | uint32(upper)<<16)
}

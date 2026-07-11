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

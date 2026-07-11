package qsmysql

import (
	"bytes"
	"testing"
)

func TestEncodePacketUsesThreeByteLittleEndianLengthAndSequence(t *testing.T) {
	encoded, err := EncodePacket(Packet{SequenceID: 7, Payload: []byte("select 1")})
	if err != nil {
		t.Fatalf("EncodePacket failed: %v", err)
	}
	wantPrefix := []byte{8, 0, 0, 7}
	if !bytes.Equal(encoded[:PacketHeaderLength], wantPrefix) {
		t.Fatalf("header = %v, want %v", encoded[:PacketHeaderLength], wantPrefix)
	}
	if string(encoded[PacketHeaderLength:]) != "select 1" {
		t.Fatalf("payload = %q", encoded[PacketHeaderLength:])
	}
}

func TestDecodePacketCopiesPayload(t *testing.T) {
	encoded, err := EncodePacket(Packet{SequenceID: 2, Payload: []byte("abc")})
	if err != nil {
		t.Fatalf("EncodePacket failed: %v", err)
	}
	packet, err := DecodePacket(encoded)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	encoded[PacketHeaderLength] = 'z'
	if packet.SequenceID != 2 || string(packet.Payload) != "abc" {
		t.Fatalf("packet = %#v, want copied payload", packet)
	}
}

func TestDecodePacketRejectsLengthMismatch(t *testing.T) {
	if _, err := DecodePacket([]byte{2, 0, 0, 1, 'a'}); err == nil {
		t.Fatal("expected length mismatch to fail")
	}
}

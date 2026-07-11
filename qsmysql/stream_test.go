package qsmysql

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

func TestStreamReadsPacketFramesFromReader(t *testing.T) {
	first, err := EncodePacket(Packet{SequenceID: 1, Payload: []byte("hello")})
	if err != nil {
		t.Fatalf("EncodePacket first failed: %v", err)
	}
	second, err := EncodePacket(Packet{SequenceID: 2, Payload: []byte("world")})
	if err != nil {
		t.Fatalf("EncodePacket second failed: %v", err)
	}
	reader := bytes.NewReader(append(first, second...))
	stream := NewStream(reader, nil)
	packet, err := stream.ReadPacket(context.Background())
	if err != nil {
		t.Fatalf("ReadPacket first failed: %v", err)
	}
	if packet.SequenceID != 1 || string(packet.Payload) != "hello" {
		t.Fatalf("first packet = %#v", packet)
	}
	packet, err = stream.ReadPacket(context.Background())
	if err != nil {
		t.Fatalf("ReadPacket second failed: %v", err)
	}
	if packet.SequenceID != 2 || string(packet.Payload) != "world" {
		t.Fatalf("second packet = %#v", packet)
	}
}

func TestStreamWritesPacketFramesToWriter(t *testing.T) {
	var writer bytes.Buffer
	stream := NewStream(nil, &writer)
	if err := stream.WritePacket(context.Background(), Packet{SequenceID: 7, Payload: []byte("ok")}); err != nil {
		t.Fatalf("WritePacket failed: %v", err)
	}
	packet, err := DecodePacket(writer.Bytes())
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	if packet.SequenceID != 7 || string(packet.Payload) != "ok" {
		t.Fatalf("packet = %#v", packet)
	}
}

func TestStreamWriteReportsShortWrite(t *testing.T) {
	stream := NewStream(nil, shortWriter{})
	if err := stream.WritePacket(context.Background(), Packet{Payload: []byte("ok")}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("err = %v", err)
	}
}

type shortWriter struct{}

func (shortWriter) Write(data []byte) (int, error) {
	return len(data) - 1, nil
}

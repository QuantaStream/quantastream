package qsmysql

import (
	"context"
	"fmt"
	"testing"
)

type testPacketReader struct {
	packets []Packet
}

func (r *testPacketReader) ReadPacket(ctx context.Context) (Packet, error) {
	if len(r.packets) == 0 {
		return Packet{}, fmt.Errorf("no packets")
	}
	packet := r.packets[0]
	r.packets = r.packets[1:]
	return packet, nil
}

type testPacketWriter struct {
	packets []Packet
}

func (w *testPacketWriter) WritePacket(ctx context.Context, packet Packet) error {
	w.packets = append(w.packets, packet)
	return nil
}

type testCommandHandler struct {
	got Command
}

func (h *testCommandHandler) HandleCommand(ctx context.Context, command Command) (CommandResponse, error) {
	h.got = command
	if command.Kind == CommandKindQuit {
		return QuitResponse(), nil
	}
	return PingResponse(), nil
}

func TestCommandLoopDecodesHandlesAndWritesResponse(t *testing.T) {
	reader := &testPacketReader{packets: []Packet{{SequenceID: 0, Payload: []byte{byte(CommandPing)}}}}
	writer := &testPacketWriter{}
	handler := &testCommandHandler{}

	response, err := (CommandLoop{Reader: reader, Writer: writer, Handler: handler}).ServeNext(context.Background())
	if err != nil {
		t.Fatalf("ServeNext failed: %v", err)
	}
	if handler.got.Kind != CommandKindPing {
		t.Fatalf("handler command = %#v", handler.got)
	}
	if response.Kind != CommandResponseOK || len(writer.packets) != 1 || writer.packets[0].Payload[0] != okPacketHeader {
		t.Fatalf("response = %#v written=%#v", response, writer.packets)
	}
}

func TestCommandLoopWritesErrorForDecodeFailure(t *testing.T) {
	reader := &testPacketReader{packets: []Packet{{SequenceID: 0, Payload: []byte{0xff}}}}
	writer := &testPacketWriter{}

	response, err := (CommandLoop{Reader: reader, Writer: writer, Handler: &testCommandHandler{}}).ServeNext(context.Background())
	if err != nil {
		t.Fatalf("ServeNext failed: %v", err)
	}
	if response.Kind != CommandResponseError || len(writer.packets) != 1 || writer.packets[0].Payload[0] != errPacketHeader {
		t.Fatalf("response = %#v written=%#v", response, writer.packets)
	}
}

package qsmysql

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
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

type statementOKCommandHandler struct {
	statement qsbridge.StatementResult
}

func (h statementOKCommandHandler) HandleCommand(ctx context.Context, command Command) (CommandResponse, error) {
	return StatementOKResponse(h.statement), nil
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

func TestCommandLoopAttachesSessionMetadata(t *testing.T) {
	reader := &testPacketReader{packets: []Packet{{SequenceID: 0, Payload: []byte{byte(CommandPing)}}}}
	writer := &testPacketWriter{}
	handler := &testCommandHandler{}

	_, err := (CommandLoop{
		Reader:       reader,
		Writer:       writer,
		Handler:      handler,
		ConnectionID: 42,
		Username:     "bench",
		Database:     "analytics",
	}).ServeNext(context.Background())
	if err != nil {
		t.Fatalf("ServeNext failed: %v", err)
	}
	if handler.got.ConnectionID != 42 || handler.got.Username != "bench" || handler.got.Database != "analytics" {
		t.Fatalf("command metadata = %#v, want connection/user/database", handler.got)
	}
}

func TestCommandLoopShapesOKForSessionTrackCapability(t *testing.T) {
	reader := &testPacketReader{packets: []Packet{{SequenceID: 0, Payload: []byte{byte(CommandPing)}}}}
	writer := &testPacketWriter{}

	response, err := (CommandLoop{
		Reader:          reader,
		Writer:          writer,
		Handler:         &testCommandHandler{},
		CapabilityFlags: CapabilitySessionTrack,
	}).ServeNext(context.Background())
	if err != nil {
		t.Fatalf("ServeNext failed: %v", err)
	}
	if response.Kind != CommandResponseOK || len(writer.packets) != 1 {
		t.Fatalf("response = %#v written=%#v", response, writer.packets)
	}
	if got := writer.packets[0].Payload; !bytes.Equal(got, []byte{okPacketHeader, 0, 0, byte(StatusAutocommit), 0, 0, 0, 0}) {
		t.Fatalf("written OK payload = %v, want session-track shape", got)
	}
}

func TestCommandLoopReencodesOKStatusForSessionTrackCapability(t *testing.T) {
	reader := &testPacketReader{packets: []Packet{{SequenceID: 0, Payload: []byte{byte(CommandQuery), 'c'}}}}
	writer := &testPacketWriter{}

	response, err := (CommandLoop{
		Reader:          reader,
		Writer:          writer,
		Handler:         statementOKCommandHandler{statement: qsbridge.StatementResult{Status: "View sample_view created"}},
		CapabilityFlags: CapabilitySessionTrack,
	}).ServeNext(context.Background())
	if err != nil {
		t.Fatalf("ServeNext failed: %v", err)
	}
	if response.Kind != CommandResponseOK || len(writer.packets) != 1 {
		t.Fatalf("response = %#v written=%#v", response, writer.packets)
	}
	want := OKPayloadWithCapabilities(qsbridge.StatementResult{Status: "View sample_view created"}, CapabilitySessionTrack)
	if got := writer.packets[0].Payload; !bytes.Equal(got, want) {
		t.Fatalf("written OK payload = %v, want %v", got, want)
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

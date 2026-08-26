package qsmysql

import (
	"bytes"
	"context"
	"fmt"
	"strings"
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
		Roles:        []qsbridge.RoleName{"reader"},
	}).ServeNext(context.Background())
	if err != nil {
		t.Fatalf("ServeNext failed: %v", err)
	}
	if handler.got.ConnectionID != 42 || handler.got.Username != "bench" || handler.got.Database != "analytics" {
		t.Fatalf("command metadata = %#v, want connection/user/database", handler.got)
	}
	if len(handler.got.Roles) != 1 || handler.got.Roles[0] != "reader" {
		t.Fatalf("command roles = %#v, want reader", handler.got.Roles)
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

func TestCommandLoopLogsCommandTrace(t *testing.T) {
	reader := &testPacketReader{packets: []Packet{{SequenceID: 0, Payload: append([]byte{byte(CommandQuery)}, []byte("select  * from t")...)}}}
	writer := &testPacketWriter{}
	var events []CommandTraceEvent

	response, err := (CommandLoop{
		Reader:       reader,
		Writer:       writer,
		Handler:      &testCommandHandler{},
		ConnectionID: 7,
		Username:     "bench",
		Database:     "quanta",
		CommandLogger: CommandLoggerFunc(func(event CommandTraceEvent) {
			events = append(events, event)
		}),
	}).ServeNext(context.Background())
	if err != nil {
		t.Fatalf("ServeNext failed: %v", err)
	}
	if response.Kind != CommandResponseOK {
		t.Fatalf("response kind = %s, want ok", response.Kind)
	}
	if len(events) != 1 {
		t.Fatalf("trace events = %d, want 1", len(events))
	}
	event := events[0]
	if event.ConnectionID != 7 || event.Username != "bench" || event.Database != "quanta" {
		t.Fatalf("trace metadata = %#v", event)
	}
	if event.Kind != CommandKindQuery || event.SQL != "select  * from t" || event.ResponseKind != CommandResponseOK || event.Error != "" {
		t.Fatalf("trace event = %#v", event)
	}
	line := event.LogLine()
	for _, want := range []string{"MYSQL_COMMAND_TRACE", "connection_id=7", `user="bench"`, `db="quanta"`, "kind=query", `sql="select * from t"`, "response=ok"} {
		if !strings.Contains(line, want) {
			t.Fatalf("trace line %q missing %q", line, want)
		}
	}
}

func TestCommandLoopLogsDecodeFailureTrace(t *testing.T) {
	reader := &testPacketReader{packets: []Packet{{SequenceID: 0, Payload: []byte{0xff}}}}
	writer := &testPacketWriter{}
	var events []CommandTraceEvent

	response, err := (CommandLoop{
		Reader:       reader,
		Writer:       writer,
		Handler:      &testCommandHandler{},
		ConnectionID: 9,
		CommandLogger: CommandLoggerFunc(func(event CommandTraceEvent) {
			events = append(events, event)
		}),
	}).ServeNext(context.Background())
	if err != nil {
		t.Fatalf("ServeNext failed: %v", err)
	}
	if response.Kind != CommandResponseError {
		t.Fatalf("response kind = %s, want error", response.Kind)
	}
	if len(events) != 1 {
		t.Fatalf("trace events = %d, want 1", len(events))
	}
	if events[0].Kind != CommandKindDecodeError || events[0].ResponseKind != CommandResponseError || !strings.Contains(events[0].Error, "unsupported mysql command byte") {
		t.Fatalf("decode trace event = %#v", events[0])
	}
}

func TestCommandTraceLongDataDoesNotIncludePayload(t *testing.T) {
	command := Command{
		Kind:        CommandKindStmtSendLongData,
		StatementID: 77,
		LongData: PreparedLongDataCommand{
			StatementID: 77,
			ParameterID: 1,
			Data:        []byte("secret-value"),
		},
	}
	line := command.TraceEvent().LogLine()
	for _, want := range []string{"kind=stmt_send_long_data", "statement_id=77", "parameter_id=2", "long_data_bytes=12"} {
		if !strings.Contains(line, want) {
			t.Fatalf("trace line %q missing %q", line, want)
		}
	}
	if strings.Contains(line, "secret-value") {
		t.Fatalf("trace line leaked long-data payload: %q", line)
	}
}

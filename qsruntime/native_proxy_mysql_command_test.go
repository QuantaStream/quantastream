package qsruntime

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsmysql"
)

func TestNativeProxyFrontDoorHandlesPingAndQuitCommands(t *testing.T) {
	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{}, NativeProxyFrontDoorConfig{})

	ping, err := frontDoor.HandleMySQLCommand(context.Background(), qsmysql.Command{Kind: qsmysql.CommandKindPing}, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("HandleMySQLCommand ping failed: %v", err)
	}
	if ping.Kind != qsmysql.CommandResponseOK || len(ping.Packets) != 1 {
		t.Fatalf("ping response = %#v", ping)
	}

	quit, err := frontDoor.HandleMySQLCommand(context.Background(), qsmysql.Command{Kind: qsmysql.CommandKindQuit}, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("HandleMySQLCommand quit failed: %v", err)
	}
	if quit.Kind != qsmysql.CommandResponseClose || !quit.Close {
		t.Fatalf("quit response = %#v", quit)
	}
}

func TestNativeProxyFrontDoorHandlesQueryCommandAsResultset(t *testing.T) {
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Index:   "orders",
			Rownums: []qsbridge.QuantaRownum{7},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Field: qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(7)},
				},
			}},
		}}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{})

	response, err := frontDoor.HandleMySQLCommand(
		context.Background(),
		qsmysql.Command{Kind: qsmysql.CommandKindQuery, SQL: "select o_orderkey from orders where o_orderkey >= 7"},
		qsbridge.ExecutionOptions{},
	)
	if err != nil {
		t.Fatalf("HandleMySQLCommand query failed: %v", err)
	}
	if response.Kind != qsmysql.CommandResponseQuery || len(response.Packets) != 5 {
		t.Fatalf("query response = %#v", response)
	}
	if string(response.Packets[3].Payload) != "\x017" {
		t.Fatalf("row payload = %v", response.Packets[3].Payload)
	}
}

func TestNativeProxyFrontDoorHandlesQueryDiagnosticsAsError(t *testing.T) {
	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{Runtime: newTestSQLRuntime(t)}, NativeProxyFrontDoorConfig{})

	response, err := frontDoor.HandleMySQLCommand(
		context.Background(),
		qsmysql.Command{Kind: qsmysql.CommandKindQuery, SQL: "select from"},
		qsbridge.ExecutionOptions{},
	)
	if err != nil {
		t.Fatalf("HandleMySQLCommand query failed: %v", err)
	}
	if response.Kind != qsmysql.CommandResponseError || len(response.Packets) != 1 || response.Packets[0].Payload[0] != 0xff {
		t.Fatalf("error response = %#v", response)
	}
}

func TestNativeProxyFrontDoorHandlesMySQLMetadataSelectsAtProtocolBoundary(t *testing.T) {
	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{Runtime: newTestSQLRuntime(t)}, NativeProxyFrontDoorConfig{})

	tests := []struct {
		sql     string
		payload string
	}{
		{sql: "SELECT @@max_allowed_packet", payload: "\x0867108864"},
		{sql: "select database()", payload: "\x09analytics"},
		{sql: "select version()", payload: "\x128.0.0-quantastream"},
		{sql: "select connection_id()", payload: "\x0242"},
	}
	for _, test := range tests {
		response, err := frontDoor.HandleMySQLCommand(
			context.Background(),
			qsmysql.Command{Kind: qsmysql.CommandKindQuery, SQL: test.sql, ConnectionID: 42, Database: "analytics"},
			qsbridge.ExecutionOptions{},
		)
		if err != nil {
			t.Fatalf("HandleMySQLCommand(%q) failed: %v", test.sql, err)
		}
		if response.Kind != qsmysql.CommandResponseQuery || len(response.Packets) != 5 {
			t.Fatalf("response for %q = %#v", test.sql, response)
		}
		if string(response.Packets[3].Payload) != test.payload {
			t.Fatalf("row payload for %q = %q, want %q", test.sql, string(response.Packets[3].Payload), test.payload)
		}
	}
}

func TestNativeProxyMySQLSessionProfileExposesLastQueryInstrumentation(t *testing.T) {
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		recorder := ExecutionInstrumentationFromContext(ctx)
		recorder.ObserveDuration("test_executor", "phase_elapsed", 3*time.Millisecond, "unit")
		recorder.ObserveCount("test_executor", "row_count", 7, "unit")
		recorder.ObserveEvent("test_executor", "strategy", "native", "unit")
		return ExecutionResult{Count: 7}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{
		Server: NativeProxyServerConfig{ContextWrapper: WithQueryScratchpad},
	})
	handler := NativeProxyMySQLCommandHandler{
		FrontDoor: frontDoor,
		Profile:   NewNativeProxyMySQLSessionProfile(),
	}

	if _, err := handler.HandleCommand(context.Background(), qsmysql.Command{Kind: qsmysql.CommandKindQuery, SQL: "select count(*) from orders"}); err != nil {
		t.Fatalf("HandleCommand query failed: %v", err)
	}
	response, err := handler.HandleCommand(context.Background(), qsmysql.Command{Kind: qsmysql.CommandKindQuery, SQL: "show quantastream profile"})
	if err != nil {
		t.Fatalf("HandleCommand profile failed: %v", err)
	}
	if response.Kind != qsmysql.CommandResponseQuery {
		t.Fatalf("profile response = %#v", response)
	}
	payloads := nativeProxyTestPacketPayloadText(response.Packets)
	for _, want := range []string{"test_executor", "phase_elapsed", "row_count", "strategy", "native"} {
		if !strings.Contains(payloads, want) {
			t.Fatalf("profile payloads missing %q: %q", want, payloads)
		}
	}
}

func TestNativeProxyMySQLCommandHandlerExecutesPreparedStatement(t *testing.T) {
	var gotRequest ExecutionRequest
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{Count: 1}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{})
	handler := NativeProxyMySQLCommandHandler{
		FrontDoor: frontDoor,
		Profile:   NewNativeProxyMySQLSessionProfile(),
	}

	prepare, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind: qsmysql.CommandKindStmtPrepare,
		SQL:  "delete from orders where o_orderkey = ?",
	})
	if err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	if prepare.Kind != qsmysql.CommandResponsePrepared || len(prepare.Packets) != 3 {
		t.Fatalf("prepare response = %#v", prepare)
	}

	execute, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:        qsmysql.CommandKindStmtExecute,
		StatementID: 1,
		Execute:     nativeProxyPreparedExecuteCommand(1, qsmysql.ColumnTypeLongLong, int64(7)),
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if execute.Kind != qsmysql.CommandResponseOK || len(execute.Packets) != 1 {
		t.Fatalf("execute response = %#v", execute)
	}
	if gotRequest.Mutation.Kind != qsbridge.MutationDelete || gotRequest.Mutation.Target.Table != "orders" {
		t.Fatalf("mutation = %#v, want delete against orders", gotRequest.Mutation)
	}
	if len(gotRequest.Query.Fragments) != 1 {
		t.Fatalf("fragments = %#v, want one bound mutation predicate", gotRequest.Query.Fragments)
	}
	fragment := gotRequest.Query.Fragments[0]
	if fragment.Index != "orders" || fragment.Field != "o_orderkey" || !fragment.HasLiteral {
		t.Fatalf("fragment = %#v, want bound orders.o_orderkey literal", fragment)
	}
	if fragment.Literal.Kind != qsbridge.ValueInt || fragment.Literal.Value != int64(7) {
		t.Fatalf("fragment literal = %#v, want int64(7)", fragment.Literal)
	}

	reset, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:        qsmysql.CommandKindStmtReset,
		StatementID: 1,
	})
	if err != nil {
		t.Fatalf("reset failed: %v", err)
	}
	if reset.Kind != qsmysql.CommandResponseOK {
		t.Fatalf("reset response = %#v", reset)
	}

	closeResponse, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:        qsmysql.CommandKindStmtClose,
		StatementID: 1,
	})
	if err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if closeResponse.Kind != qsmysql.CommandResponseNoResponse || len(closeResponse.Packets) != 0 {
		t.Fatalf("close response = %#v", closeResponse)
	}
}

func TestNativeProxyMySQLCommandHandlerPreparedStatementReturnsBinaryRows(t *testing.T) {
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Index:   "orders",
			Rownums: []qsbridge.QuantaRownum{7},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Field:  qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(7)}},
			}},
		}}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{})
	handler := NativeProxyMySQLCommandHandler{
		FrontDoor: frontDoor,
		Profile:   NewNativeProxyMySQLSessionProfile(),
	}

	if _, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind: qsmysql.CommandKindStmtPrepare,
		SQL:  "select o_orderkey from orders where o_orderkey = ?",
	}); err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	response, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:        qsmysql.CommandKindStmtExecute,
		StatementID: 1,
		Execute:     nativeProxyPreparedExecuteCommand(1, qsmysql.ColumnTypeLongLong, int64(7)),
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if response.Kind != qsmysql.CommandResponseQuery || len(response.Packets) != 5 {
		t.Fatalf("query response = %#v", response)
	}
	row := response.Packets[3].Payload
	if len(row) != 10 || row[0] != 0 || row[1] != 0 || row[2] != 7 {
		t.Fatalf("binary row payload = %v", row)
	}
}

func TestNativeProxyFrontDoorServeMySQLCommandUsesPacketLoop(t *testing.T) {
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Index:   "orders",
			Rownums: []qsbridge.QuantaRownum{7},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Field:  qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(7)}},
			}},
		}}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{})
	reader := &nativeProxyTestPacketReader{packets: []qsmysql.Packet{{Payload: append([]byte{byte(qsmysql.CommandQuery)}, []byte("select o_orderkey from orders where o_orderkey >= 7")...)}}}
	writer := &nativeProxyTestPacketWriter{}

	response, err := frontDoor.ServeMySQLCommand(context.Background(), reader, writer, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ServeMySQLCommand failed: %v", err)
	}
	if response.Kind != qsmysql.CommandResponseQuery || len(writer.packets) != 5 || string(writer.packets[3].Payload) != "\x017" {
		t.Fatalf("response = %#v written=%#v", response, writer.packets)
	}
}

func nativeProxyPreparedExecuteCommand(statementID uint32, parameterType qsmysql.ColumnType, value any) qsmysql.PreparedExecuteCommand {
	payload := []byte{0, 1, byte(parameterType), 0}
	switch typed := value.(type) {
	case int64:
		payload = appendNativeProxyUint64LE(payload, uint64(typed))
	case float64:
		payload = appendNativeProxyUint64LE(payload, math.Float64bits(typed))
	case string:
		payload = append(payload, byte(len(typed)))
		payload = append(payload, typed...)
	}
	return qsmysql.PreparedExecuteCommand{
		StatementID:      statementID,
		IterationCount:   1,
		ParameterPayload: payload,
	}
}

func appendNativeProxyUint64LE(out []byte, value uint64) []byte {
	return append(out,
		byte(value),
		byte(value>>8),
		byte(value>>16),
		byte(value>>24),
		byte(value>>32),
		byte(value>>40),
		byte(value>>48),
		byte(value>>56),
	)
}

func nativeProxyTestPacketPayloadText(packets []qsmysql.Packet) string {
	parts := make([]string, 0, len(packets))
	for _, packet := range packets {
		parts = append(parts, string(packet.Payload))
	}
	return strings.Join(parts, "\n")
}

type nativeProxyTestPacketReader struct {
	packets []qsmysql.Packet
}

func (r *nativeProxyTestPacketReader) ReadPacket(ctx context.Context) (qsmysql.Packet, error) {
	packet := r.packets[0]
	r.packets = r.packets[1:]
	return packet, nil
}

type nativeProxyTestPacketWriter struct {
	packets []qsmysql.Packet
}

func (w *nativeProxyTestPacketWriter) WritePacket(ctx context.Context, packet qsmysql.Packet) error {
	w.packets = append(w.packets, packet)
	return nil
}

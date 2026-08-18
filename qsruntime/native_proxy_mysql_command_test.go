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

func TestNativeProxyMySQLCommandHandlerPersistsTemporaryTableMetadataInSession(t *testing.T) {
	executed := false
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithCatalog(t, qsbridge.MemoryCatalog{
		Schemas:   []qsbridge.CatalogSchemaDefinition{{Name: "quanta"}},
		Functions: qsbridge.BuiltinSQLFunctionDefinitions(),
	}, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executed = true
		return ExecutionResult{}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{})
	handler := NativeProxyMySQLCommandHandler{
		FrontDoor: frontDoor,
		Profile:   NewNativeProxyMySQLSessionProfile(),
	}

	create, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:     qsmysql.CommandKindQuery,
		SQL:      "create temporary table scratch_keys (customer_key bigint not null, primary key (customer_key))",
		Database: "quanta",
	})
	if err != nil {
		t.Fatalf("HandleCommand create temporary table failed: %v", err)
	}
	if create.Kind != qsmysql.CommandResponseOK {
		t.Fatalf("create response = %#v, want OK", create)
	}
	if executed {
		t.Fatalf("CREATE TEMPORARY TABLE should not dispatch to the direct executor")
	}
	session := handler.Profile.Session()
	if len(session.TemporaryTables) != 1 {
		t.Fatalf("temporary tables = %#v, want one", session.TemporaryTables)
	}

	describe, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:     qsmysql.CommandKindQuery,
		SQL:      "show columns from scratch_keys",
		Database: "quanta",
	})
	if err != nil {
		t.Fatalf("HandleCommand show columns failed: %v", err)
	}
	if describe.Kind != qsmysql.CommandResponseQuery {
		t.Fatalf("describe response = %#v, want query", describe)
	}
	payloads := nativeProxyTestPacketPayloadText(describe.Packets)
	if !strings.Contains(payloads, "customer_key") || !strings.Contains(payloads, "PRI") {
		t.Fatalf("describe payloads missing temporary table metadata: %q", payloads)
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

func TestNativeProxyMySQLCommandHandlerPreparedExecuteReusesCachedTypes(t *testing.T) {
	var requests []ExecutionRequest
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		requests = append(requests, request)
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Index:   "orders",
			Rownums: []qsbridge.QuantaRownum{8},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Field:  qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(8)}},
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
	if _, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:        qsmysql.CommandKindStmtExecute,
		StatementID: 1,
		Execute:     nativeProxyPreparedExecuteCommand(1, qsmysql.ColumnTypeLongLong, int64(7)),
	}); err != nil {
		t.Fatalf("first execute failed: %v", err)
	}

	cachedPayload := []byte{0, 0}
	cachedPayload = appendNativeProxyUint64LE(cachedPayload, 8)
	if _, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:        qsmysql.CommandKindStmtExecute,
		StatementID: 1,
		Execute:     nativeProxyPreparedExecuteCommandWithPayload(1, cachedPayload),
	}); err != nil {
		t.Fatalf("cached execute failed: %v", err)
	}

	if len(requests) != 2 {
		t.Fatalf("requests = %d, want two executes", len(requests))
	}
	fragment := requests[1].Query.Fragments[0]
	if fragment.Literal.Kind != qsbridge.ValueInt || fragment.Literal.Value != int64(8) {
		t.Fatalf("cached execute literal = %#v, want int64(8)", fragment.Literal)
	}
}

func TestNativeProxyMySQLCommandHandlerPreparedBatchInsert(t *testing.T) {
	var gotRequest ExecutionRequest
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{Statement: qsbridge.StatementResult{AffectedRows: 2}}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{})
	handler := NativeProxyMySQLCommandHandler{
		FrontDoor: frontDoor,
		Profile:   NewNativeProxyMySQLSessionProfile(),
	}

	prepare, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind: qsmysql.CommandKindStmtPrepare,
		SQL:  "insert into orders (o_orderkey, o_orderpriority) values (?, ?), (?, ?)",
	})
	if err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	statementID := nativeProxyPreparedStatementID(t, prepare)
	execute, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:        qsmysql.CommandKindStmtExecute,
		StatementID: statementID,
		Execute: nativeProxyPreparedExecuteCommandWithValues(statementID, []nativeProxyPreparedValue{
			{Type: qsmysql.ColumnTypeLongLong, Value: int64(9001)},
			{Type: qsmysql.ColumnTypeVarString, Value: "1-URGENT"},
			{Type: qsmysql.ColumnTypeLongLong, Value: int64(9002)},
			{Type: qsmysql.ColumnTypeVarString, Value: "2-HIGH"},
		}),
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if execute.Kind != qsmysql.CommandResponseOK {
		t.Fatalf("execute response = %#v, want OK", execute)
	}
	if gotRequest.Mutation.Kind != qsbridge.MutationInsert || gotRequest.Mutation.Target.Table != "orders" {
		t.Fatalf("mutation = %#v, want orders insert", gotRequest.Mutation)
	}
	if len(gotRequest.Mutation.Rows) != 2 {
		t.Fatalf("mutation rows = %#v, want two rows", gotRequest.Mutation.Rows)
	}
	if got := nativeProxyLiteralValue(t, gotRequest.Mutation.Rows[0].Values[0]); got != int64(9001) {
		t.Fatalf("first order key = %#v, want 9001", got)
	}
	if got := nativeProxyLiteralValue(t, gotRequest.Mutation.Rows[1].Values[1]); got != "2-HIGH" {
		t.Fatalf("second priority = %#v, want 2-HIGH", got)
	}
}

func TestNativeProxyMySQLCommandHandlerPreparedLongData(t *testing.T) {
	var gotRequest ExecutionRequest
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Index:   "orders",
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Field:  qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Type: qsbridge.DataTypeInt, Visible: true},
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(1)}},
			}},
		}}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{})
	profile := NewNativeProxyMySQLSessionProfile()
	handler := NativeProxyMySQLCommandHandler{FrontDoor: frontDoor, Profile: profile}

	prepare, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind: qsmysql.CommandKindStmtPrepare,
		SQL:  "select o_orderkey from orders where o_orderpriority = ?",
	})
	if err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	statementID := nativeProxyPreparedStatementID(t, prepare)
	longData, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:        qsmysql.CommandKindStmtSendLongData,
		StatementID: statementID,
		LongData: qsmysql.PreparedLongDataCommand{
			StatementID: statementID,
			ParameterID: 0,
			Data:        []byte("1-"),
		},
	})
	if err != nil {
		t.Fatalf("send long data failed: %v", err)
	}
	if longData.Kind != qsmysql.CommandResponseNoResponse {
		t.Fatalf("long data response = %#v, want no response", longData)
	}
	if _, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:        qsmysql.CommandKindStmtSendLongData,
		StatementID: statementID,
		LongData: qsmysql.PreparedLongDataCommand{
			StatementID: statementID,
			ParameterID: 0,
			Data:        []byte("URGENT"),
		},
	}); err != nil {
		t.Fatalf("second send long data failed: %v", err)
	}
	execute, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:        qsmysql.CommandKindStmtExecute,
		StatementID: statementID,
		Execute: nativeProxyPreparedExecuteCommandWithPayload(statementID, []byte{
			0,
			1,
			byte(qsmysql.ColumnTypeLongBlob), 0,
		}),
	})
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if execute.Kind != qsmysql.CommandResponseQuery {
		t.Fatalf("execute response = %#v, want query", execute)
	}
	fragment := gotRequest.Query.Fragments[0]
	if fragment.Literal.Kind != qsbridge.ValueString || fragment.Literal.Value != "1-URGENT" {
		t.Fatalf("long-data literal = %#v, want concatenated string", fragment.Literal)
	}
	if values := profile.PreparedLongDataValues(qsbridge.PreparedStatementHandle{ID: qsbridge.PreparedStatementID(statementID)}); len(values) != 0 {
		t.Fatalf("long data values after execute = %#v, want cleared", values)
	}
}

func TestNativeProxyMySQLCommandHandlerPreparedResetClearsLongData(t *testing.T) {
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Count: 1}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{})
	profile := NewNativeProxyMySQLSessionProfile()
	handler := NativeProxyMySQLCommandHandler{FrontDoor: frontDoor, Profile: profile}

	prepare, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind: qsmysql.CommandKindStmtPrepare,
		SQL:  "select o_orderkey from orders where o_orderpriority = ?",
	})
	if err != nil {
		t.Fatalf("prepare failed: %v", err)
	}
	statementID := nativeProxyPreparedStatementID(t, prepare)
	if _, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:        qsmysql.CommandKindStmtSendLongData,
		StatementID: statementID,
		LongData:    qsmysql.PreparedLongDataCommand{StatementID: statementID, ParameterID: 0, Data: []byte("payload")},
	}); err != nil {
		t.Fatalf("send long data failed: %v", err)
	}
	if values := profile.PreparedLongDataValues(qsbridge.PreparedStatementHandle{ID: qsbridge.PreparedStatementID(statementID)}); len(values) != 1 {
		t.Fatalf("long data values before reset = %#v, want one", values)
	}
	reset, err := handler.HandleCommand(context.Background(), qsmysql.Command{
		Kind:        qsmysql.CommandKindStmtReset,
		StatementID: statementID,
	})
	if err != nil {
		t.Fatalf("reset failed: %v", err)
	}
	if reset.Kind != qsmysql.CommandResponseOK {
		t.Fatalf("reset response = %#v, want OK", reset)
	}
	if values := profile.PreparedLongDataValues(qsbridge.PreparedStatementHandle{ID: qsbridge.PreparedStatementID(statementID)}); len(values) != 0 {
		t.Fatalf("long data values after reset = %#v, want cleared", values)
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
	return nativeProxyPreparedExecuteCommandWithValues(statementID, []nativeProxyPreparedValue{{Type: parameterType, Value: value}})
}

func nativeProxyPreparedStatementID(t *testing.T, response qsmysql.CommandResponse) uint32 {
	t.Helper()
	if response.Kind != qsmysql.CommandResponsePrepared || len(response.Packets) == 0 {
		t.Fatalf("prepare response = %#v, want prepared response", response)
	}
	payload := response.Packets[0].Payload
	if len(payload) < 5 {
		t.Fatalf("prepare OK payload = %v, want statement id", payload)
	}
	return uint32(payload[1]) | uint32(payload[2])<<8 | uint32(payload[3])<<16 | uint32(payload[4])<<24
}

type nativeProxyPreparedValue struct {
	Type     qsmysql.ColumnType
	Unsigned bool
	Value    any
}

func nativeProxyPreparedExecuteCommandWithValues(statementID uint32, values []nativeProxyPreparedValue) qsmysql.PreparedExecuteCommand {
	nullBitmapLength := (len(values) + 7) / 8
	payload := make([]byte, nullBitmapLength, nullBitmapLength+1+len(values)*10)
	payload = append(payload, 1)
	for _, value := range values {
		flags := byte(0)
		if value.Unsigned {
			flags = 0x80
		}
		payload = append(payload, byte(value.Type), flags)
	}
	for _, value := range values {
		payload = appendNativeProxyPreparedValue(payload, value)
	}
	return nativeProxyPreparedExecuteCommandWithPayload(statementID, payload)
}

func nativeProxyPreparedExecuteCommandWithPayload(statementID uint32, payload []byte) qsmysql.PreparedExecuteCommand {
	return qsmysql.PreparedExecuteCommand{
		StatementID:      statementID,
		IterationCount:   1,
		ParameterPayload: append([]byte(nil), payload...),
	}
}

func appendNativeProxyPreparedValue(payload []byte, value nativeProxyPreparedValue) []byte {
	switch typed := value.Value.(type) {
	case int64:
		return appendNativeProxyUint64LE(payload, uint64(typed))
	case uint64:
		return appendNativeProxyUint64LE(payload, typed)
	case float64:
		return appendNativeProxyUint64LE(payload, math.Float64bits(typed))
	case string:
		payload = append(payload, byte(len(typed)))
		return append(payload, typed...)
	default:
		return payload
	}
}

func nativeProxyLiteralValue(t *testing.T, expr qsbridge.Expr) any {
	t.Helper()
	literal, ok := expr.(qsbridge.LiteralExpr)
	if !ok {
		t.Fatalf("expr = %#v, want literal", expr)
	}
	return literal.Value
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

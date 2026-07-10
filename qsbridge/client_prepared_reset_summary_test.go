package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientPreparedResetReturnsResetRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	preparedRegistry := NewMemoryPreparedStatementRegistry()
	longDataRegistry := NewMemoryPreparedLongDataRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityStatementResults)
	description := preparedRegistry.Register(PreparedPlan{
		Handle:     PreparedStatementHandle{Name: "stmt_blob"},
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeString, Nullable: true}},
		Supported:  true,
	})
	fragment := PreparedLongDataFragment{
		Handle:     description.Handle,
		Parameter:  IndexedParameterValue(1, ValueString, nil),
		ChunkBytes: 12,
	}
	if stored := service.StoreClientPreparedLongData(connection, preparedRegistry, longDataRegistry, fragment); !stored.Supported() {
		t.Fatalf("stored = %#v, want supported long-data setup", stored)
	}
	reset := service.ResetClientPreparedStatement(connection, preparedRegistry, longDataRegistry, description.Handle)

	exchange := service.SummarizeClientPreparedReset(connection, reset)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported prepared reset summary", exchange)
	}
	row := exchange.Rows[0]
	if row.StatementID != description.Handle.ID || row.StatementName != description.Handle.Name {
		t.Fatalf("row = %#v, want prepared handle metadata", row)
	}
	if !row.Reset || !row.ClearedLongData || row.ResponseKind != ClientResponseStatement || row.Status != "Prepared statement reset" || !row.Supported {
		t.Fatalf("row = %#v, want reset response metadata", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 8 {
		t.Fatalf("result/schema = %#v/%#v, want prepared reset summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != int(description.Handle.ID) || resultRow[2].Value != true || resultRow[5].Value != "Prepared statement reset" {
		t.Fatalf("result row = %#v, want reset cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPreparedResetReportsResetDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityStatementResults)
	reset := service.ResetClientPreparedStatement(connection, NewMemoryPreparedStatementRegistry(), NewMemoryPreparedLongDataRegistry(), PreparedStatementHandle{ID: 99})

	exchange := service.SummarizeClientPreparedReset(connection, reset)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, reset diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported || exchange.Rows[0].Reset {
		t.Fatalf("rows = %#v, want unsupported reset row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientPreparedResetFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientPreparedReset(connection, ClientPreparedResetExchange{Handle: PreparedStatementHandle{ID: 1}})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block reset summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientPreparedResetCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	preparedRegistry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityStatementResults)
	description := preparedRegistry.Register(PreparedPlan{Handle: PreparedStatementHandle{Name: "stmt_blob"}, Supported: true})
	reset := service.ResetClientPreparedStatement(connection, preparedRegistry, nil, description.Handle)

	exchange := service.SummarizeClientPreparedReset(connection, reset)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Reset.Connection.Attributes["client"] = "mutated"
	exchange.Reset.Response.StatementResponse.Status = "mutated"
	exchange.Rows[0].Status = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][5].Value = "mutated"

	again := service.SummarizeClientPreparedReset(connection, reset)
	if again.Connection.Attributes["client"] != "mysql" || again.Reset.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v/%#v", again.Connection.Attributes, again.Reset.Connection.Attributes)
	}
	if again.Reset.Response.StatementResponse.Status != "Prepared statement reset" || again.Rows[0].Status != "Prepared statement reset" {
		t.Fatalf("reset summary leaked mutation: %#v/%#v", again.Reset.Response, again.Rows)
	}
	if again.Result.Columns[0].Name != "Statement_id" || again.ResultSchema.Columns[0].Name != "Statement_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][5].Value != "Prepared statement reset" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

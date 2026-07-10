package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientPreparedCloseReturnsCloseRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityStatementResults)
	description := registry.Register(PreparedPlan{Handle: PreparedStatementHandle{Name: "stmt_orders"}, Supported: true})
	closed := service.CloseClientPreparedStatement(connection, registry, description.Handle)

	exchange := service.SummarizeClientPreparedClose(connection, closed)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported prepared close summary", exchange)
	}
	row := exchange.Rows[0]
	if row.StatementID != description.Handle.ID || row.StatementName != description.Handle.Name {
		t.Fatalf("row = %#v, want prepared handle metadata", row)
	}
	if !row.Closed || row.ResponseKind != ClientResponseStatement || row.Status != "Prepared statement closed" || !row.Supported {
		t.Fatalf("row = %#v, want close response metadata", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 7 {
		t.Fatalf("result/schema = %#v/%#v, want prepared close summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != int(description.Handle.ID) || resultRow[2].Value != true || resultRow[4].Value != "Prepared statement closed" {
		t.Fatalf("result row = %#v, want close cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPreparedCloseReportsCloseDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityStatementResults)
	closed := service.CloseClientPreparedStatement(connection, NewMemoryPreparedStatementRegistry(), PreparedStatementHandle{ID: 99})

	exchange := service.SummarizeClientPreparedClose(connection, closed)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, close diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported || exchange.Rows[0].Closed {
		t.Fatalf("rows = %#v, want unsupported close row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientPreparedCloseFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientPreparedClose(connection, ClientPreparedCloseExchange{Request: PreparedStatementCloseRequest{Handle: PreparedStatementHandle{ID: 1}}})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block close summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientPreparedCloseCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements, ProtocolCapabilityStatementResults)
	description := registry.Register(PreparedPlan{Handle: PreparedStatementHandle{Name: "stmt_orders"}, Supported: true})
	closed := service.CloseClientPreparedStatement(connection, registry, description.Handle)

	exchange := service.SummarizeClientPreparedClose(connection, closed)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Close.Connection.Attributes["client"] = "mutated"
	exchange.Close.Response.StatementResponse.Status = "mutated"
	exchange.Rows[0].Status = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][4].Value = "mutated"

	again := service.SummarizeClientPreparedClose(connection, closed)
	if again.Connection.Attributes["client"] != "mysql" || again.Close.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v/%#v", again.Connection.Attributes, again.Close.Connection.Attributes)
	}
	if again.Close.Response.StatementResponse.Status != "Prepared statement closed" || again.Rows[0].Status != "Prepared statement closed" {
		t.Fatalf("close summary leaked mutation: %#v/%#v", again.Close.Response, again.Rows)
	}
	if again.Result.Columns[0].Name != "Statement_id" || again.ResultSchema.Columns[0].Name != "Statement_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][4].Value != "Prepared statement closed" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

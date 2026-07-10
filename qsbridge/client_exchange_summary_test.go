package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientExchangeReturnsAggregateRow(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityMultiStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	clientExchange := service.PrepareConnectionClientExchange(
		connection,
		ConnectionPlanOptions{CatalogVersion: "catalog-v1"},
		ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)}},
		"select 1",
		"select 2",
	)
	exchange := service.SummarizeClientExchange(clientExchange)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported exchange summary", exchange)
	}
	row := exchange.Rows[0]
	if row.User != "moli" || row.Schema != "quanta" || row.Protocol != ProtocolMySQL || !row.Supported {
		t.Fatalf("row = %#v, want connection metadata", row)
	}
	if row.StatementCount != 2 || row.HandoffCount != 2 || row.PreviewCount != 2 || row.ResponseCount != 2 {
		t.Fatalf("row = %#v, want aggregate exchange counts", row)
	}
	if row.ReadCount != 2 || row.WriteCount != 0 || row.SelectLifecycleCount != 2 || row.MutationLifecycleCount != 0 {
		t.Fatalf("row = %#v, want two read SELECT lifecycle statements", row)
	}
	if row.NativeCount != 2 || row.QueryResponses != 2 || row.ErrorResponses != 0 {
		t.Fatalf("row = %#v, want native query response counts", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 21 {
		t.Fatalf("result/schema = %#v/%#v, want exchange summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != "moli" || resultRow[3].Value != true || resultRow[4].Value != 2 || resultRow[5].Value != 2 || resultRow[7].Value != 2 || resultRow[17].Value != 2 {
		t.Fatalf("result row = %#v, want summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientExchangeReportsUnsupportedExchangeAsData(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)

	clientExchange := service.PrepareConnectionClientExchange(
		clientStatementConnection(),
		ConnectionPlanOptions{},
		ClientHandoffOptions{},
		"select 1",
		"select 2",
	)
	exchange := service.SummarizeClientExchange(clientExchange)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, request diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported {
		t.Fatalf("rows = %#v, want unsupported request row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.StatementCount != 2 || row.HandoffCount != 0 || row.ResponseCount != 0 {
		t.Fatalf("row = %#v, want short-circuited aggregate counts", row)
	}
	if row.ReadCount != 0 || row.WriteCount != 0 || row.SelectLifecycleCount != 0 || row.MutationLifecycleCount != 0 {
		t.Fatalf("row = %#v, want no lifecycle counts without handoffs", row)
	}
	if !containsDiagnosticCode(row.DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", row.DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientExchangeFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}
	clientExchange := service.PrepareConnectionClientExchange(connection, ConnectionPlanOptions{}, ClientHandoffOptions{}, "select 1")

	exchange := service.SummarizeClientExchange(clientExchange)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless summary", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientExchangeCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	clientExchange := service.PrepareConnectionClientExchange(
		connection,
		ConnectionPlanOptions{},
		ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)}},
		"select 1",
	)

	exchange := service.SummarizeClientExchange(clientExchange)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Exchange.Request.Statements[0].SQL = "mutated"
	exchange.Exchange.Handoff.Statements[0].Plan.Prepared.Parameters[0].Type = DataTypeString
	exchange.Exchange.Preview.Statements[0].Schema.Columns[0].Name = "mutated"
	exchange.Rows[0].Schema = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.SummarizeClientExchange(clientExchange)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Exchange.Request.Statements[0].SQL != "select 1" {
		t.Fatalf("request metadata leaked mutation: %#v", again.Exchange.Request.Statements)
	}
	if again.Exchange.Handoff.Statements[0].Plan.Prepared.Parameters[0].Type != DataTypeInt {
		t.Fatalf("handoff metadata leaked mutation: %#v", again.Exchange.Handoff.Statements[0].Plan.Prepared.Parameters)
	}
	if again.Exchange.Preview.Statements[0].Schema.Columns[0].Name != "order_id" {
		t.Fatalf("preview metadata leaked mutation: %#v", again.Exchange.Preview.Statements[0].Schema.Columns)
	}
	if again.Rows[0].Schema != "quanta" {
		t.Fatalf("row leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "User" || again.ResultSchema.Columns[0].Name != "User" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != "quanta" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

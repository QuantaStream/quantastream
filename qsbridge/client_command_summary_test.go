package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientCommandReturnsCommandRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemorySessionRegistry()
	connection := clientStatementConnection()
	connection.Session.ID = "session-1"
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	registry.Put(connection.Session)

	command := service.PrepareClientCommand(connection, registry, ClientCommandInitSchema, "analytics", ClientCommandOptions{ApplySession: true})
	exchange := service.SummarizeClientCommand(connection, command)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported command summary row", exchange)
	}
	row := exchange.Rows[0]
	if row.Command != ClientCommandInitSchema || !row.PayloadPresent || row.PayloadLength != len("analytics") {
		t.Fatalf("row = %#v, want init-schema payload metadata", row)
	}
	if !row.SessionApplied || row.SessionActions != 1 || row.Status != "Database changed" || !row.Supported {
		t.Fatalf("row = %#v, want applied schema command metadata", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 11 {
		t.Fatalf("result/schema = %#v/%#v, want command summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != string(ClientCommandInitSchema) || resultRow[1].Value != true || resultRow[6].Value != "Database changed" {
		t.Fatalf("result row = %#v, want command summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientCommandReportsCommandDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	command := service.PrepareClientCommand(connection, nil, ClientCommandKind("unknown"), "", ClientCommandOptions{})

	exchange := service.SummarizeClientCommand(connection, command)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, unsupported command should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported {
		t.Fatalf("rows = %#v, want unsupported command row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientCommandFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientCommand(connection, ClientCommandExchange{Kind: ClientCommandPing})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientCommandCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)
	command := service.PrepareClientCommand(connection, nil, ClientCommandPing, "", ClientCommandOptions{})

	exchange := service.SummarizeClientCommand(connection, command)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Command.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].Status = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.SummarizeClientCommand(connection, command)
	if again.Connection.Attributes["client"] != "mysql" || again.Command.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection metadata leaked mutation: %#v/%#v", again.Connection.Attributes, again.Command.Connection.Attributes)
	}
	if again.Rows[0].Status != "OK" || again.Rows[0].Command != ClientCommandPing {
		t.Fatalf("row leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Command" || again.ResultSchema.Columns[0].Name != "Command" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != string(ClientCommandPing) {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

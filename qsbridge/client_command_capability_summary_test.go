package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientCommandCapabilitiesReportsSupportedCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityStatementResults,
		ProtocolCapabilitySessionActions,
	)

	exchange := service.SummarizeClientCommandCapabilities(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported command capability summary", exchange)
	}
	row := exchange.Row
	if row.CommandCount != 4 || row.SupportedCount != 4 || row.UnsupportedCount != 0 || !row.AllSupported {
		t.Fatalf("row = %#v, want all command capabilities supported", row)
	}
	if row.RequiresPayloadCount != 1 || row.SessionActionCount != 2 || row.ClosesConnectionCount != 1 {
		t.Fatalf("row = %#v, want command shape counts", row)
	}
	if row.StatementResultCapabilityCount != 4 || row.SessionActionCapabilityCount != 2 {
		t.Fatalf("row = %#v, want required capability counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 9 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 4 || resultRow[1].Value != 4 || resultRow[8].Value != true {
		t.Fatalf("result row = %#v, want supported command counts", resultRow)
	}
}

func TestPlanningServiceSummarizeClientCommandCapabilitiesReportsUnsupportedCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)

	exchange := service.SummarizeClientCommandCapabilities(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, command capability summary should still be reportable", exchange)
	}
	if exchange.Row.SupportedCount != 2 || exchange.Row.UnsupportedCount != 2 || exchange.Row.AllSupported {
		t.Fatalf("row = %#v, want reset/init unsupported without session actions", exchange.Row)
	}
}

func TestPlanningServiceSummarizeClientCommandCapabilitiesFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientCommandCapabilities(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || exchange.Result.RowsReturned != 0 {
		t.Fatalf("result = %#v, want failed rowless summary", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientCommandCapabilitiesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.SummarizeClientCommandCapabilities(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.CommandCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientCommandCapabilities(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.CommandCount != 4 {
		t.Fatalf("row leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Command_count" || again.ResultSchema.Columns[0].Name != "Command_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 4 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

package qsbridge

import "testing"

func TestDefaultDispatchTargetSummaryAggregatesProfiles(t *testing.T) {
	summary := DefaultDispatchTargetSummary()
	if summary.TargetCount != 3 {
		t.Fatalf("summary = %#v, want three dispatch targets", summary)
	}
	if summary.RuntimeOwnedCount != 2 || summary.RequiresExecutorCount != 2 ||
		summary.ConfigurableCount != 2 || summary.TerminalCount != 1 {
		t.Fatalf("summary = %#v, want executor-backed and terminal target counts", summary)
	}
}

func TestPlanningServiceSummarizeClientDispatchTargetsReturnsOneRow(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.SummarizeClientDispatchTargets(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported dispatch target summary metadata", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one dispatch target summary", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 5 || exchange.ResultSchema.Columns[0].Name != "Targets" {
		t.Fatalf("schema = %#v, want dispatch target summary columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 1 {
		t.Fatalf("result = %#v, want one summary row", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientDispatchTargetsReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.SummarizeClientDispatchTargets(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientDispatchTargetsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.SummarizeClientDispatchTargets(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].TargetCount = -1
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.SummarizeClientDispatchTargets(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].TargetCount != 3 {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Targets" || again.ResultSchema.Columns[0].Name != "Targets" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

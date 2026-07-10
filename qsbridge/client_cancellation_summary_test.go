package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientCancellationReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryExecutionRegistry()
	connection := clientStatementConnection()
	registry.Register(testExecutionRequestForRegistry("req-1", true))

	cancelled := service.CancelClientExecutionRequest(connection, registry, "req-1", CancellationClientRequest, "kill query")
	exchange := service.SummarizeClientCancellation(connection, cancelled)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported cancellation summary", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one cancellation summary row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.RequestID != "req-1" || row.Kind != ExecutionRequestSingle || row.PreviousStatus != ExecutionPending {
		t.Fatalf("row = %#v, want single pending request metadata", row)
	}
	if !row.Recorded || !row.Supported || row.ResultStatus != ExecutionCanceled || row.BatchStatus != "" {
		t.Fatalf("row = %#v, want recorded supported single cancellation", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 11 {
		t.Fatalf("result/schema = %#v/%#v, want one summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != "req-1" || resultRow[5].Value != true || resultRow[7].Value != string(ExecutionCanceled) {
		t.Fatalf("result row = %#v, want cancellation cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientCancellationKeepsFailureAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	cancelled := service.CancelClientExecutionRequest(connection, NewMemoryExecutionRegistry(), "missing", "", "")
	exchange := service.SummarizeClientCancellation(connection, cancelled)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want summary exchange supported for failed cancellation metadata", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported || exchange.Rows[0].Recorded {
		t.Fatalf("rows = %#v, want failed unrecorded cancellation row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("row diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result = %#v, want successful summary result", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientCancellationCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryExecutionRegistry()
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	request := testExecutionRequestForRegistry("req-1", true)
	request.ResultColumns = []ResultColumn{{Name: "original"}}
	registry.Register(request)

	cancelled := service.CancelClientExecutionRequest(connection, registry, "req-1", CancellationClientRequest, "kill query")
	exchange := service.SummarizeClientCancellation(connection, cancelled)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Cancellation.Record.Request.ResultColumns[0].Name = "mutated"
	exchange.Rows[0].Message = "mutated"
	exchange.Result.Chunks[0].Rows[0][10].Value = "mutated"

	again := service.SummarizeClientCancellation(connection, cancelled)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Cancellation.Record.Request.ResultColumns[0].Name != "original" || again.Rows[0].Message != "kill query" {
		t.Fatalf("summary leaked mutation: %#v/%#v", again.Cancellation.Record.Request.ResultColumns, again.Rows)
	}
	if again.Result.Chunks[0].Rows[0][10].Value != "kill query" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

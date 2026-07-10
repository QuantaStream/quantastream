package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientExecutorStatusReportsConfiguredBoundaries(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	status := service.ListClientExecutorStatus(clientStatementConnection(), ExecutionDispatcher{
		Native: &recordingNativeExecutor{},
		Legacy: &recordingLegacyExecutor{},
	})

	exchange := service.SummarizeClientExecutorStatus(status)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported executor status summary", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one executor status summary", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.ExecutorCount != 2 || row.ConfiguredCount != 2 || row.MissingCount != 0 {
		t.Fatalf("row = %#v, want two configured executors", row)
	}
	if !row.AllConfigured || !row.AllSingleRequest || !row.AllBatchRequest {
		t.Fatalf("row = %#v, want all executor boundaries configured", row)
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.ResultSchema.Columns[0].Name != "Executors" ||
		exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want executor status summary result", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientExecutorStatusReportsMissingBoundaries(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	status := service.ListClientExecutorStatus(clientStatementConnection(), ExecutionDispatcher{})

	exchange := service.SummarizeClientExecutorStatus(status)
	row := exchange.Rows[0]
	if row.ExecutorCount != 2 || row.ConfiguredCount != 0 || row.MissingCount != 2 {
		t.Fatalf("row = %#v, want missing native and legacy executors", row)
	}
	if row.AllConfigured || row.AllSingleRequest || row.AllBatchRequest {
		t.Fatalf("row = %#v, missing executors should not report all configured", row)
	}
}

func TestPlanningServiceSummarizeClientExecutorStatusReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}
	status := service.ListClientExecutorStatus(connection, ExecutionDispatcher{Native: &recordingNativeExecutor{}})

	exchange := service.SummarizeClientExecutorStatus(status)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientExecutorStatusCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	status := service.ListClientExecutorStatus(connection, ExecutionDispatcher{})

	exchange := service.SummarizeClientExecutorStatus(status)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Status.Rows[0].Detail = "mutated"
	exchange.Rows[0].ExecutorCount = -1
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.SummarizeClientExecutorStatus(status)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Status.Rows[0].Detail == "mutated" {
		t.Fatalf("nested status leaked mutation: %#v", again.Status.Rows[0])
	}
	if again.Rows[0].ExecutorCount != 2 {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Executors" || again.ResultSchema.Columns[0].Name != "Executors" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

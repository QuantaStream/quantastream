package qsbridge

import "testing"

func TestPlanningServiceListClientExecutorStatusReportsConfiguredBoundaries(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	dispatcher := ExecutionDispatcher{
		Native: &recordingNativeExecutor{},
		Legacy: &recordingLegacyExecutor{},
	}

	exchange := service.ListClientExecutorStatus(clientStatementConnection(), dispatcher)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported executor status", exchange)
	}
	if len(exchange.Rows) != 2 {
		t.Fatalf("rows = %#v, want native and legacy executor rows", exchange.Rows)
	}
	for _, row := range exchange.Rows {
		if !row.Configured || !row.SingleRequest || !row.BatchRequest {
			t.Fatalf("row = %#v, want configured single and batch support", row)
		}
	}
	if exchange.Rows[0].Target != DispatchTargetNative || exchange.Rows[1].Target != DispatchTargetLegacy {
		t.Fatalf("rows = %#v, want native then legacy", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 5 || exchange.ResultSchema.Columns[0].Name != "Executor" || exchange.Result.RowsReturned != 2 {
		t.Fatalf("result/schema = %#v/%#v, want executor status result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != string(DispatchTargetNative) || resultRow[1].Value != true {
		t.Fatalf("result row = %#v, want configured native executor", resultRow)
	}
}

func TestPlanningServiceListClientExecutorStatusReportsMissingBoundaries(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)

	exchange := service.ListClientExecutorStatus(clientStatementConnection(), ExecutionDispatcher{})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, missing executors should still be reportable", exchange)
	}
	for _, row := range exchange.Rows {
		if row.Configured || row.SingleRequest || row.BatchRequest {
			t.Fatalf("row = %#v, want missing executor boundary", row)
		}
	}
}

func TestPlanningServiceListClientExecutorStatusFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientExecutorStatus(connection, ExecutionDispatcher{Native: &recordingNativeExecutor{}})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block status", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless status", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientExecutorStatusCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientExecutorStatus(connection, ExecutionDispatcher{})
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].Detail = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientExecutorStatus(connection, ExecutionDispatcher{})
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].Detail == "mutated" {
		t.Fatalf("row leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Executor" || again.ResultSchema.Columns[0].Name != "Executor" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != string(DispatchTargetNative) {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientExecutionProcessesReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryExecutionRegistry()
	connection := clientStatementConnection()
	first := testExecutionRequestForRegistry("req-b", true)
	second := BatchExecutionRequest{
		Prepared: PreparedPlan{
			Session: SessionContext{User: "moli"},
		},
		Options:       ExecutionOptions{RequestID: "req-a", Cancelable: false},
		ParameterSets: []ParameterBindingSet{{}},
	}
	registry.Register(first)
	registry.RegisterBatch(second)
	registry.MarkStatus("req-b", ExecutionStreaming)

	exchange := service.SummarizeClientExecutionProcesses(connection, registry)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported process-list summary", exchange)
	}
	row := exchange.Row
	if row.ProcessCount != 2 || row.SingleRequestCount != 1 || row.BatchRequestCount != 1 {
		t.Fatalf("row = %#v, want process kind counts", row)
	}
	if row.PendingCount != 1 || row.StreamingCount != 1 || row.CancelableCount != 1 {
		t.Fatalf("row = %#v, want process status and cancelable counts", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 9 {
		t.Fatalf("result/schema = %#v/%#v, want one-row process-list summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 2 || resultRow[2].Value != 1 || resultRow[4].Value != 1 {
		t.Fatalf("result row = %#v, want process-list summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientExecutionProcessesReportsMissingRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.SummarizeClientExecutionProcesses(connection, nil)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing registry diagnostic", exchange)
	}
	if exchange.Row.ProcessCount != 0 || exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 9 {
		t.Fatalf("row/result/schema = %#v/%#v/%#v, want failed process-list summary envelope", exchange.Row, exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientExecutionProcessesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryExecutionRegistry()
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	registry.Register(testExecutionRequestForRegistry("req-1", true))

	exchange := service.SummarizeClientExecutionProcesses(connection, registry)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.ProcessCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientExecutionProcesses(connection, registry)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.ProcessCount != 1 || again.Row.CancelableCount != 1 || again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("process-list summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}

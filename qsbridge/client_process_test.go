package qsbridge

import "testing"

func TestPlanningServiceListClientExecutionProcessesReturnsSortedRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryExecutionRegistry()
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	first := testExecutionRequestForRegistry("req-b", true)
	first.Bound.Prepared.SQL = "select * from orders"
	first.Bound.Prepared.Session.CurrentSchema = "quanta"
	second := BatchExecutionRequest{
		Prepared: PreparedPlan{
			SQL:     "insert into orders values (?)",
			Session: SessionContext{User: "moli", CurrentSchema: "quanta"},
		},
		Options:       ExecutionOptions{RequestID: "req-a", Cancelable: false},
		ParameterSets: []ParameterBindingSet{{}},
	}
	registry.Register(first)
	registry.RegisterBatch(second)
	registry.MarkStatus("req-b", ExecutionStreaming)

	exchange := service.ListClientExecutionProcesses(connection, registry)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported process-list metadata", exchange)
	}
	if len(exchange.Records) != 2 || exchange.Records[0].ID != "req-a" || exchange.Records[1].ID != "req-b" {
		t.Fatalf("records = %#v, want sorted request ids", exchange.Records)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 2 || len(exchange.ResultSchema.Columns) != 7 {
		t.Fatalf("result/schema = %#v/%#v, want process-list result metadata", exchange.Result, exchange.ResultSchema)
	}
	firstRow := exchange.Result.Chunks[0].Rows[0]
	if firstRow[0].Value != "req-a" || firstRow[1].Value != "batch" || firstRow[6].Value != "insert into orders values (?)" {
		t.Fatalf("first row = %#v, want batch process metadata", firstRow)
	}
	secondRow := exchange.Result.Chunks[0].Rows[1]
	if secondRow[2].Value != "streaming" || secondRow[5].Value != true || secondRow[6].Value != "select * from orders" {
		t.Fatalf("second row = %#v, want streaming cancelable process metadata", secondRow)
	}
}

func TestPlanningServiceListClientExecutionProcessesReportsMissingRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientExecutionProcesses(connection, nil)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing registry diagnostic", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Diagnostics)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 7 {
		t.Fatalf("result/schema = %#v/%#v, want failed process-list envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientExecutionProcessesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryExecutionRegistry()
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	request := testExecutionRequestForRegistry("req-1", true)
	request.Bound.Prepared.SQL = "select 1"
	registry.Register(request)

	exchange := service.ListClientExecutionProcesses(connection, registry)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Records[0].Session.User = "mutated"
	exchange.Result.Chunks[0].Rows[0][6].Value = "mutated"

	again := service.ListClientExecutionProcesses(connection, registry)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Records[0].Session.User != "moli" || again.Result.Chunks[0].Rows[0][6].Value != "select 1" {
		t.Fatalf("process-list leaked mutation: %#v/%#v", again.Records, again.Result.Chunks)
	}
}

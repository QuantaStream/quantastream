package qsbridge

import "testing"

func TestPlanningServiceCancelClientExecutionRequestMarksSingleRequest(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryExecutionRegistry()
	connection := clientStatementConnection()
	registry.Register(testExecutionRequestForRegistry("req-1", true))

	cancelled := service.CancelClientExecutionRequest(connection, registry, "req-1", CancellationClientRequest, "kill query")
	if !cancelled.Supported() {
		t.Fatalf("cancelled = %#v, want supported cancellation", cancelled)
	}
	if cancelled.Cancellation.RequestID != "req-1" || cancelled.Cancellation.Message != "kill query" {
		t.Fatalf("cancellation = %#v, want request id and message", cancelled.Cancellation)
	}
	if cancelled.Result.Status != ExecutionCanceled || !cancelled.Result.Complete {
		t.Fatalf("result = %#v, want canceled result envelope", cancelled.Result)
	}
	record, ok := registry.Get("req-1")
	if !ok || record.Status != ExecutionCancelRequested {
		t.Fatalf("record = %#v ok=%v, want cancel requested", record, ok)
	}
}

func TestPlanningServiceCancelClientExecutionRequestMarksBatchRequest(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryExecutionRegistry()
	connection := clientStatementConnection()
	registry.RegisterBatch(BatchExecutionRequest{
		Prepared: PreparedPlan{
			Session: SessionContext{User: "moli"},
		},
		Options:       ExecutionOptions{RequestID: "batch-1", Cancelable: true},
		ParameterSets: []ParameterBindingSet{{}},
		Result:        ResultShape{Kind: ResultStatement},
	})

	cancelled := service.CancelClientExecutionRequest(connection, registry, "batch-1", CancellationTimeout, "deadline")
	if !cancelled.Supported() {
		t.Fatalf("cancelled = %#v, want supported batch cancellation", cancelled)
	}
	if cancelled.BatchResult.Status != ExecutionCanceled || !cancelled.BatchResult.Complete {
		t.Fatalf("batch result = %#v, want canceled batch envelope", cancelled.BatchResult)
	}
	if cancelled.Result.Status != "" {
		t.Fatalf("single result = %#v, want empty for batch cancellation", cancelled.Result)
	}
}

func TestPlanningServiceCancelClientExecutionRequestReportsMissingRegistryAndID(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	missingRegistry := service.CancelClientExecutionRequest(connection, nil, "req-1", CancellationClientRequest, "kill query")
	if missingRegistry.Supported() || !containsDiagnosticCode(missingRegistry.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing registry = %#v, want invalid execution option", missingRegistry)
	}
	if missingRegistry.Result.Status != ExecutionFailed || !missingRegistry.Result.Complete {
		t.Fatalf("result = %#v, want failed complete result", missingRegistry.Result)
	}

	missingID := service.CancelClientExecutionRequest(connection, NewMemoryExecutionRegistry(), "", CancellationClientRequest, "")
	if missingID.Supported() || !containsDiagnosticCode(missingID.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing id = %#v, want invalid execution option", missingID)
	}
}

func TestPlanningServiceCancelClientExecutionRequestReportsMissingOrNonCancelableRequest(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryExecutionRegistry()
	connection := clientStatementConnection()

	missing := service.CancelClientExecutionRequest(connection, registry, "missing", "", "")
	if missing.Supported() || missing.Cancellation.Supported() {
		t.Fatalf("missing = %#v, want unsupported cancellation", missing)
	}
	if !containsDiagnosticCode(missing.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", missing.Diagnostics)
	}

	registry.Register(testExecutionRequestForRegistry("req-1", false))
	cancelled := service.CancelClientExecutionRequest(connection, registry, "req-1", CancellationTimeout, "deadline")
	if cancelled.Supported() || cancelled.Cancellation.Supported() {
		t.Fatalf("cancelled = %#v, want non-cancelable request to be unsupported", cancelled)
	}
	record, ok := registry.Get("req-1")
	if !ok || record.Status != ExecutionPending {
		t.Fatalf("record = %#v ok=%v, want still pending", record, ok)
	}
}

func TestPlanningServiceCancelClientExecutionRequestCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryExecutionRegistry()
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	request := testExecutionRequestForRegistry("req-1", true)
	request.ResultColumns = []ResultColumn{{Name: "original"}}
	registry.Register(request)

	cancelled := service.CancelClientExecutionRequest(connection, registry, "req-1", CancellationClientRequest, "kill query")
	cancelled.Connection.Attributes["client"] = "mutated"
	cancelled.Record.Request.ResultColumns[0].Name = "mutated"

	if connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", connection.Attributes)
	}
	record, ok := registry.Get("req-1")
	if !ok || record.Request.ResultColumns[0].Name != "original" {
		t.Fatalf("registry record leaked mutation: %#v ok=%v", record, ok)
	}
}

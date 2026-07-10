package qsbridge

import "testing"

func TestMemoryExecutionRegistryRegistersSingleRequest(t *testing.T) {
	registry := NewMemoryExecutionRegistry()
	request := testExecutionRequestForRegistry("req-1", true)

	record := registry.Register(request)
	if record.ID != "req-1" || record.Kind != ExecutionRequestSingle || record.Status != ExecutionPending {
		t.Fatalf("record = %#v, want pending single request", record)
	}
	stored, ok := registry.Get("req-1")
	if !ok {
		t.Fatalf("expected registered request")
	}
	if stored.Request.Options.RequestID != "req-1" || stored.Session.User != "moli" {
		t.Fatalf("stored = %#v, want copied request/session metadata", stored)
	}
}

func TestMemoryExecutionRegistryRegistersBatchRequest(t *testing.T) {
	registry := NewMemoryExecutionRegistry()
	request := BatchExecutionRequest{
		Prepared: PreparedPlan{
			Session: SessionContext{User: "moli"},
		},
		Options:       ExecutionOptions{RequestID: "batch-1", Cancelable: true},
		ParameterSets: []ParameterBindingSet{{}},
	}

	record := registry.RegisterBatch(request)
	if record.ID != "batch-1" || record.Kind != ExecutionRequestBatch || record.Status != ExecutionPending {
		t.Fatalf("record = %#v, want pending batch request", record)
	}
	stored, ok := registry.Get("batch-1")
	if !ok {
		t.Fatalf("expected registered batch request")
	}
	if stored.Batch.Options.RequestID != "batch-1" || stored.Session.User != "moli" {
		t.Fatalf("stored = %#v, want copied batch/session metadata", stored)
	}
}

func TestMemoryExecutionRegistryRequiresRequestID(t *testing.T) {
	registry := NewMemoryExecutionRegistry()
	record := registry.Register(ExecutionRequest{})
	if record.Status != ExecutionFailed {
		t.Fatalf("record status = %q, want failed", record.Status)
	}
	if !containsDiagnosticCode(record.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", record.Diagnostics.Codes())
	}
	if _, ok := registry.Get(""); ok {
		t.Fatalf("empty request id should not be registered")
	}
}

func TestMemoryExecutionRegistryMarksStatusAndRemoves(t *testing.T) {
	registry := NewMemoryExecutionRegistry()
	registry.Register(testExecutionRequestForRegistry("req-1", true))

	if !registry.MarkStatus("req-1", ExecutionStreaming) {
		t.Fatalf("expected status update")
	}
	record, ok := registry.Get("req-1")
	if !ok || record.Status != ExecutionStreaming {
		t.Fatalf("record = %#v ok=%v, want streaming", record, ok)
	}
	if !registry.Remove("req-1") {
		t.Fatalf("expected remove")
	}
	if _, ok := registry.Get("req-1"); ok {
		t.Fatalf("expected request to be removed")
	}
	if registry.MarkStatus("req-1", ExecutionComplete) {
		t.Fatalf("missing request should not update")
	}
}

func TestMemoryExecutionRegistryListsRegisteredRequests(t *testing.T) {
	registry := NewMemoryExecutionRegistry()
	first := testExecutionRequestForRegistry("req-1", true)
	first.Bound.Prepared.SQL = "select 1"
	second := testExecutionRequestForRegistry("req-2", false)
	second.Bound.Prepared.SQL = "select 2"
	registry.Register(first)
	registry.Register(second)

	records := registry.List()
	if len(records) != 2 {
		t.Fatalf("records = %#v, want two registered requests", records)
	}
	records[0].Session.User = "mutated"
	again := registry.List()
	for _, record := range again {
		if record.Session.User != "moli" {
			t.Fatalf("registry list leaked mutation: %#v", again)
		}
	}
}

func TestMemoryExecutionRegistryCancelUsesRegisteredOptions(t *testing.T) {
	registry := NewMemoryExecutionRegistry()
	registry.Register(testExecutionRequestForRegistry("req-1", true))

	cancel := registry.Cancel("req-1", CancellationClientRequest, "kill query")
	if !cancel.Supported() {
		t.Fatalf("cancel = %#v, want supported", cancel)
	}
	if cancel.RequestID != "req-1" || cancel.Message != "kill query" {
		t.Fatalf("cancel = %#v, want request id and message", cancel)
	}
	record, ok := registry.Get("req-1")
	if !ok || record.Status != ExecutionCancelRequested {
		t.Fatalf("record = %#v ok=%v, want cancel requested", record, ok)
	}
}

func TestMemoryExecutionRegistryCancelReportsMissingOrNonCancelableRequest(t *testing.T) {
	registry := NewMemoryExecutionRegistry()

	missing := registry.Cancel("missing", "", "")
	if missing.Supported() {
		t.Fatalf("expected missing cancel to be unsupported")
	}
	if !containsDiagnosticCode(missing.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing diagnostics = %#v, want invalid execution option", missing.Diagnostics.Codes())
	}

	registry.Register(testExecutionRequestForRegistry("req-1", false))
	cancel := registry.Cancel("req-1", CancellationTimeout, "deadline")
	if cancel.Supported() {
		t.Fatalf("expected non-cancelable request cancel to be unsupported")
	}
	record, ok := registry.Get("req-1")
	if !ok || record.Status != ExecutionPending {
		t.Fatalf("record = %#v ok=%v, want still pending", record, ok)
	}
}

func TestMemoryExecutionRegistryCopiesMutableRecords(t *testing.T) {
	registry := NewMemoryExecutionRegistry()
	request := testExecutionRequestForRegistry("req-1", true)
	request.ResultColumns = []ResultColumn{{Name: "original"}}
	registry.Register(request)

	record, ok := registry.Get("req-1")
	if !ok {
		t.Fatalf("expected registered request")
	}
	record.Request.ResultColumns[0].Name = "mutated"
	record.Session.User = "mutated"

	second, ok := registry.Get("req-1")
	if !ok {
		t.Fatalf("expected second registered request")
	}
	if second.Request.ResultColumns[0].Name != "original" || second.Session.User != "moli" {
		t.Fatalf("registry leaked mutable record: %#v", second)
	}
}

func TestMemoryExecutionRegistryClear(t *testing.T) {
	registry := NewMemoryExecutionRegistry()
	registry.Register(testExecutionRequestForRegistry("req-1", true))
	registry.Clear()
	if _, ok := registry.Get("req-1"); ok {
		t.Fatalf("expected clear to remove request")
	}
}

func testExecutionRequestForRegistry(id ExecutionRequestID, cancelable bool) ExecutionRequest {
	return ExecutionRequest{
		Bound: BoundPlan{
			Prepared: PreparedPlan{
				Session: SessionContext{User: "moli"},
			},
		},
		Options: ExecutionOptions{
			RequestID:  id,
			Cancelable: cancelable,
		},
		Supported: true,
	}
}

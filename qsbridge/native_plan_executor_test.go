package qsbridge

import "testing"

func TestPlanOnlyNativeExecutorCompletesSelectEnvelope(t *testing.T) {
	request := testPreparedSelectPlan(t).PreparedPlan().ExecutionRequest(
		ExecutionOptions{RequestID: "plan-only-1", TraceExplain: true},
		IndexedParameterValue(1, ValueFloat, 100),
	)

	result := PlanOnlyNativeExecutor{}.ExecuteNative(request)
	if result.RequestID != "plan-only-1" || result.Kind != ResultQuery || result.Status != ExecutionComplete || !result.Complete {
		t.Fatalf("result = %#v, want completed query envelope", result)
	}
	if len(result.Columns) != 1 || result.Columns[0].Name != "order_id" {
		t.Fatalf("columns = %#v, want order_id", result.Columns)
	}
	if len(result.Chunks) != 1 || !result.Chunks[0].Final || result.RowsReturned != 0 {
		t.Fatalf("chunks = %#v rows=%d, want one final empty chunk", result.Chunks, result.RowsReturned)
	}
	if result.Profile.AccessIntent != PhysicalAccessRead || result.Profile.Lifecycle != ClientPlanLifecycleSelect || result.Profile.LogicalPlan == "" {
		t.Fatalf("profile = %#v, want select explain metadata", result.Profile)
	}
}

func TestPlanOnlyNativeExecutorReturnsDiagnosticsForInvalidRequest(t *testing.T) {
	request := testPreparedSelectPlan(t).PreparedPlan().ExecutionRequest(
		ExecutionOptions{RequestID: "bad-params"},
		IndexedParameterValue(1, ValueString, "bad"),
	)

	result := PlanOnlyNativeExecutor{}.ExecuteNative(request)
	if result.Status != ExecutionFailed || result.Complete {
		t.Fatalf("result = %#v, want failed incomplete result", result)
	}
	if !containsDiagnosticCode(result.Diagnostics.Codes(), DiagnosticParameterTypeMismatch) {
		t.Fatalf("diagnostics = %#v, want parameter mismatch", result.Diagnostics.Codes())
	}
}

func TestPlanOnlyNativeExecutorCompletesBatchItems(t *testing.T) {
	prepared := testPreparedSelectPlan(t).PreparedPlan()
	request := prepared.BatchExecutionRequest(
		ExecutionOptions{RequestID: "batch-plan-only"},
		ParameterValues(IndexedParameterValue(1, ValueFloat, 100)),
		ParameterValues(IndexedParameterValue(1, ValueFloat, 200)),
	)

	result := PlanOnlyNativeExecutor{}.ExecuteNativeBatch(request)
	if result.RequestID != "batch-plan-only" || result.Status != ExecutionComplete || !result.Complete {
		t.Fatalf("batch result = %#v, want completed batch", result)
	}
	if len(result.Items) != 2 {
		t.Fatalf("items = %#v, want two item envelopes", result.Items)
	}
	for i, item := range result.Items {
		if item.Kind != ResultQuery || item.Status != ExecutionComplete || !item.Complete || len(item.Chunks) != 1 || !item.Chunks[0].Final {
			t.Fatalf("item %d = %#v, want completed query envelope", i, item)
		}
	}
}

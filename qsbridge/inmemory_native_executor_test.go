package qsbridge

import "testing"

func TestInMemoryNativeExecutorAttachesProfileCounters(t *testing.T) {
	handoff := simpleRunnerPlanningService().PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o.o_orderkey as order_id from orders as o order by o.o_orderkey"},
		ExecutionOptions{RequestID: "profile-1", IncludeProfile: true, BatchSize: 1, MaxRows: 2},
	)
	result := ExecutionDispatcher{
		Native: NewInMemoryNativeExecutor(simpleRunnerOrdersFixture()...),
	}.Dispatch(handoff)

	if result.Status != ExecutionComplete || !result.Complete {
		t.Fatalf("result status/complete = %q/%v, want complete/true", result.Status, result.Complete)
	}
	if result.RowsReturned != 2 || len(result.Chunks) != 2 {
		t.Fatalf("rows/chunks = %d/%d, want 2/2", result.RowsReturned, len(result.Chunks))
	}
	if got := inMemoryProfileCounterValue(result.Profile, "matched_candidates"); got != 3 {
		t.Fatalf("matched candidates = %d, want 3", got)
	}
	if got := inMemoryProfileCounterValue(result.Profile, "delivered_rows"); got != 2 {
		t.Fatalf("delivered rows = %d, want 2", got)
	}
	if got := inMemoryProfileCounterValue(result.Profile, "result_chunks"); got != 2 {
		t.Fatalf("result chunks = %d, want 2", got)
	}
}

func inMemoryProfileCounterValue(profile ExecutionProfile, name string) uint64 {
	for _, counter := range profile.Counters {
		if counter.Name == name {
			return counter.Value
		}
	}
	return 0
}

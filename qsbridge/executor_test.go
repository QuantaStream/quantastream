package qsbridge

import "testing"

func TestExecutionDispatcherDispatchesNativeRequest(t *testing.T) {
	native := &recordingNativeExecutor{}
	dispatcher := ExecutionDispatcher{Native: native}
	handoff := handoffPlanningService().PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "native-1"},
		IndexedParameterValue(1, ValueInt, 7),
	)

	result := dispatcher.Dispatch(handoff)
	if native.single.Options.RequestID != "native-1" {
		t.Fatalf("native request = %#v, want request id native-1", native.single)
	}
	if result.RequestID != "native-1" || result.Status != ExecutionComplete {
		t.Fatalf("result = %#v, want native complete result", result)
	}
}

func TestExecutionDispatcherDispatchesLegacyFallbackRequest(t *testing.T) {
	legacy := &recordingLegacyExecutor{}
	dispatcher := ExecutionDispatcher{Legacy: legacy}
	service := handoffPlanningService()
	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}
	handoff := service.PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "legacy-1"},
		IndexedParameterValue(1, ValueInt, 7),
	)

	result := dispatcher.Dispatch(handoff)
	if legacy.single.Options.RequestID != "legacy-1" {
		t.Fatalf("legacy request = %#v, want request id legacy-1", legacy.single)
	}
	if result.RequestID != "legacy-1" || result.Status != ExecutionComplete {
		t.Fatalf("result = %#v, want legacy complete result", result)
	}
}

func TestExecutionDispatcherReturnsDiagnosticsForRejectedHandoff(t *testing.T) {
	service := handoffPlanningService()
	service.Routing = NativeOnlyRoutingPolicy()
	handoff := service.PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "reject-1"},
		IndexedParameterValue(1, ValueString, "bad"),
	)

	result := ExecutionDispatcher{}.Dispatch(handoff)
	if result.Status != ExecutionFailed || result.Supported() {
		t.Fatalf("result = %#v, want failed unsupported result", result)
	}
	if !containsDiagnosticCode(result.Diagnostics.Codes(), DiagnosticRouteRejected) {
		t.Fatalf("diagnostics = %#v, want route rejected", result.Diagnostics.Codes())
	}
}

func TestExecutionDispatcherReportsMissingExecutor(t *testing.T) {
	handoff := handoffPlanningService().PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "missing-1"},
		IndexedParameterValue(1, ValueInt, 7),
	)

	result := ExecutionDispatcher{}.Dispatch(handoff)
	if result.Status != ExecutionFailed {
		t.Fatalf("status = %q, want failed", result.Status)
	}
	if !containsDiagnosticCode(result.Diagnostics.Codes(), DiagnosticInternalInvariant) {
		t.Fatalf("diagnostics = %#v, want missing executor diagnostic", result.Diagnostics.Codes())
	}
}

func TestExecutionDispatcherDispatchesBatchBoundaries(t *testing.T) {
	native := &recordingNativeExecutor{}
	legacy := &recordingLegacyExecutor{}
	dispatcher := ExecutionDispatcher{Native: native, Legacy: legacy}
	service := handoffPlanningService()

	nativeHandoff := service.PrepareRoutedAuthorizedBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "batch-native-1"},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
	)
	nativeResult := dispatcher.DispatchBatch(nativeHandoff)
	if native.batch.Options.RequestID != "batch-native-1" || nativeResult.RequestID != "batch-native-1" {
		t.Fatalf("native batch request/result = %#v/%#v, want request id", native.batch, nativeResult)
	}

	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}
	legacyHandoff := service.PrepareRoutedAuthorizedBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "batch-legacy-1"},
		ParameterValues(IndexedParameterValue(1, ValueInt, 8)),
	)
	legacyResult := dispatcher.DispatchBatch(legacyHandoff)
	if legacy.batch.Options.RequestID != "batch-legacy-1" || legacyResult.RequestID != "batch-legacy-1" {
		t.Fatalf("legacy batch request/result = %#v/%#v, want request id", legacy.batch, legacyResult)
	}
}

type recordingNativeExecutor struct {
	single ExecutionRequest
	batch  BatchExecutionRequest
}

func (e *recordingNativeExecutor) ExecuteNative(request ExecutionRequest) ExecutionResult {
	e.single = request
	return request.EmptyResult().WithCompleteForTest()
}

func (e *recordingNativeExecutor) ExecuteNativeBatch(request BatchExecutionRequest) BatchExecutionResult {
	e.batch = request
	return request.EmptyResult().WithComplete()
}

type recordingLegacyExecutor struct {
	single FallbackRequest
	batch  BatchFallbackRequest
}

func (e *recordingLegacyExecutor) ExecuteLegacy(request FallbackRequest) ExecutionResult {
	e.single = request
	return ExecutionResult{
		RequestID: request.Options.RequestID,
		Status:    ExecutionComplete,
		Kind:      ResultStatement,
		Complete:  true,
	}
}

func (e *recordingLegacyExecutor) ExecuteLegacyBatch(request BatchFallbackRequest) BatchExecutionResult {
	e.batch = request
	return BatchExecutionResult{
		RequestID: request.Options.RequestID,
		Status:    ExecutionComplete,
		Kind:      ResultStatement,
		Complete:  true,
	}
}

func (r ExecutionResult) WithCompleteForTest() ExecutionResult {
	r.Status = ExecutionComplete
	r.Complete = true
	return r
}

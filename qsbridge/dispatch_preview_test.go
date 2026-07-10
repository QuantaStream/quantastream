package qsbridge

import "testing"

func TestExecutionDispatcherPreviewReportsNativeExecutor(t *testing.T) {
	handoff := handoffPlanningService().PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "preview-native"},
		IndexedParameterValue(1, ValueInt, 7),
	)

	preview := ExecutionDispatcher{Native: &recordingNativeExecutor{}}.Preview(handoff)
	if preview.Handoff != ExecutionHandoffNative || preview.Target != DispatchTargetNative {
		t.Fatalf("preview = %#v, want native dispatch target", preview)
	}
	if !preview.Supported || !preview.ExecutorConfigured || !preview.WillDispatch {
		t.Fatalf("preview = %#v, want supported configured dispatch", preview)
	}
	if len(preview.Diagnostics) != 0 {
		t.Fatalf("diagnostics = %#v, want none", preview.Diagnostics)
	}
}

func TestExecutionDispatcherPreviewReportsLegacyFallbackExecutor(t *testing.T) {
	service := handoffPlanningService()
	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}
	handoff := service.PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "preview-legacy"},
		IndexedParameterValue(1, ValueInt, 7),
	)

	preview := ExecutionDispatcher{Legacy: &recordingLegacyExecutor{}}.Preview(handoff)
	if preview.Handoff != ExecutionHandoffLegacyFallback || preview.Target != DispatchTargetLegacy {
		t.Fatalf("preview = %#v, want legacy dispatch target", preview)
	}
	if !preview.Supported || !preview.ExecutorConfigured || !preview.WillDispatch {
		t.Fatalf("preview = %#v, want supported configured dispatch", preview)
	}
}

func TestExecutionDispatcherPreviewReportsMissingExecutorWithoutDispatch(t *testing.T) {
	handoff := handoffPlanningService().PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "preview-missing"},
		IndexedParameterValue(1, ValueInt, 7),
	)

	preview := ExecutionDispatcher{}.Preview(handoff)
	if preview.Target != DispatchTargetNative || preview.ExecutorConfigured || preview.WillDispatch {
		t.Fatalf("preview = %#v, want native target with missing executor", preview)
	}
	if preview.Supported != true {
		t.Fatalf("preview = %#v, missing executor should not change handoff support", preview)
	}
	if !containsDiagnosticCode(preview.Diagnostics.Codes(), DiagnosticInternalInvariant) {
		t.Fatalf("diagnostics = %#v, want missing executor diagnostic", preview.Diagnostics.Codes())
	}
}

func TestExecutionDispatcherPreviewReportsRejectedHandoff(t *testing.T) {
	service := handoffPlanningService()
	service.Routing = NativeOnlyRoutingPolicy()
	handoff := service.PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "preview-rejected"},
		IndexedParameterValue(1, ValueString, "bad"),
	)

	preview := ExecutionDispatcher{Native: &recordingNativeExecutor{}}.Preview(handoff)
	if preview.Handoff != ExecutionHandoffRejected || preview.Target != DispatchTargetNone {
		t.Fatalf("preview = %#v, want rejected no-dispatch target", preview)
	}
	if preview.Supported || preview.ExecutorConfigured || preview.WillDispatch {
		t.Fatalf("preview = %#v, rejected handoff should not dispatch", preview)
	}
	if !containsDiagnosticCode(preview.Diagnostics.Codes(), DiagnosticRouteRejected) {
		t.Fatalf("diagnostics = %#v, want route rejection", preview.Diagnostics.Codes())
	}
}

func TestExecutionDispatcherPreviewCopiesDiagnostics(t *testing.T) {
	service := handoffPlanningService()
	service.Routing = NativeOnlyRoutingPolicy()
	handoff := service.PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "preview-copy"},
		IndexedParameterValue(1, ValueString, "bad"),
	)

	preview := ExecutionDispatcher{}.Preview(handoff)
	preview.Diagnostics[0].Message = "mutated"

	again := ExecutionDispatcher{}.Preview(handoff)
	if again.Diagnostics[0].Message == "mutated" {
		t.Fatalf("preview diagnostics leaked mutation: %#v", again.Diagnostics)
	}
}

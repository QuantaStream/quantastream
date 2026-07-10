package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestExecutorSelectorSelectsDirectExecutor(t *testing.T) {
	direct := ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Count: 1}, nil
	})
	legacy := ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Count: 2}, nil
	})
	selected, diagnostics := ExecutorSelector{Direct: direct, Legacy: legacy}.Select(DirectQIABRoute())

	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	result, err := selected.Execute(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if err != nil {
		t.Fatalf("execute selected: %v", err)
	}
	if result.Count != 1 {
		t.Fatalf("count = %d, want direct count 1", result.Count)
	}
}

func TestExecutorSelectorSelectsLegacyExecutor(t *testing.T) {
	direct := ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Count: 1}, nil
	})
	legacy := ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Count: 2}, nil
	})
	route := LegacyGRPCRoute(RuntimeTarget{NodeID: "node-a"})
	selected, diagnostics := ExecutorSelector{Direct: direct, Legacy: legacy}.Select(route)

	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	result, err := selected.Execute(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}).WithRoute(route))
	if err != nil {
		t.Fatalf("execute selected: %v", err)
	}
	if result.Count != 2 {
		t.Fatalf("count = %d, want legacy count 2", result.Count)
	}
}

func TestExecutorSelectorReportsMissingDirectExecutor(t *testing.T) {
	_, diagnostics := ExecutorSelector{}.Select(DirectQIABRoute())

	if !diagnostics.BlocksNative() {
		t.Fatalf("expected missing direct executor diagnostics")
	}
	if got := diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}

func TestExecutorSelectorReportsMissingLegacyExecutor(t *testing.T) {
	_, diagnostics := ExecutorSelector{Direct: ExecutorFunc(nil)}.
		Select(LegacyProxyRoute(RuntimeTarget{Address: "127.0.0.1:4000"}))

	if !diagnostics.BlocksNative() {
		t.Fatalf("expected missing legacy executor diagnostics")
	}
	if got := diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}

func TestExecutorSelectorRejectsUnknownRoute(t *testing.T) {
	_, diagnostics := ExecutorSelector{
		Direct: ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
			return ExecutionResult{}, nil
		}),
	}.Select(ExecutionRoute{Path: ExecutionPath("future_path")})

	if !diagnostics.BlocksNative() {
		t.Fatalf("expected unknown route diagnostics")
	}
	if got := diagnostics.Codes()[0]; got != qsbridge.DiagnosticRouteRejected {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticRouteRejected)
	}
}

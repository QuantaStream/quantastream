package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestExecutorFuncExecutesRequest(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	called := false
	executor := ExecutorFunc(func(ctx context.Context, got ExecutionRequest) (ExecutionResult, error) {
		called = true
		if got.Route.Path != ExecutionPathDirectQIAB {
			t.Fatalf("route path = %q, want %q", got.Route.Path, ExecutionPathDirectQIAB)
		}
		return ExecutionResult{Count: 7}, nil
	})

	result, err := executor.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !called {
		t.Fatalf("executor func was not called")
	}
	if result.Count != 7 {
		t.Fatalf("count = %d, want 7", result.Count)
	}
}

func TestRoutePolicyExecutorAllowsDirectRoute(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	executor := RoutePolicyExecutor{
		Policy: DirectOnlyRoutePolicy(),
		Next: ExecutorFunc(func(ctx context.Context, got ExecutionRequest) (ExecutionResult, error) {
			return ExecutionResult{Count: uint64(got.FragmentCount()) + 1}, nil
		}),
	}

	result, err := executor.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", result.Diagnostics)
	}
	if result.Count != 1 {
		t.Fatalf("count = %d, want 1", result.Count)
	}
}

func TestRoutePolicyExecutorRejectsCompatibilityRouteWhenDirectOnly(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}).
		WithRoute(LegacyGRPCRoute(RuntimeTarget{NodeID: "node-a"}))
	executor := RoutePolicyExecutor{
		Policy: DirectOnlyRoutePolicy(),
		Next: ExecutorFunc(func(ctx context.Context, got ExecutionRequest) (ExecutionResult, error) {
			t.Fatalf("wrapped executor should not be called")
			return ExecutionResult{}, nil
		}),
	}

	result, err := executor.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected route rejection diagnostics")
	}
	if got := result.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticRouteRejected {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticRouteRejected)
	}
}

func TestRoutePolicyExecutorReportsMissingWrappedExecutor(t *testing.T) {
	executor := RoutePolicyExecutor{Policy: DirectOnlyRoutePolicy()}

	result, err := executor.Execute(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected missing executor diagnostic")
	}
	if got := result.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}

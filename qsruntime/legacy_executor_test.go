package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestLegacyRuntimeFuncExecutesCompatibilityRequest(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}).
		WithRoute(LegacyGRPCRoute(RuntimeTarget{NodeID: "node-a"}))
	runtime := LegacyRuntimeFunc(func(ctx context.Context, got ExecutionRequest) (ExecutionResult, error) {
		if !got.Route.CompatibilityPath() {
			t.Fatalf("route CompatibilityPath() = false, want true")
		}
		return ExecutionResult{Count: 5}, nil
	})

	result, err := runtime.ExecuteLegacy(context.Background(), request)
	if err != nil {
		t.Fatalf("execute legacy: %v", err)
	}
	if result.Count != 5 {
		t.Fatalf("count = %d, want 5", result.Count)
	}
}

func TestLegacyExecutorDelegatesToLegacyRuntime(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}).
		WithRoute(LegacyProxyRoute(RuntimeTarget{Address: "127.0.0.1:4000"}))
	called := false
	executor := LegacyExecutor{
		Runtime: LegacyRuntimeFunc(func(ctx context.Context, got ExecutionRequest) (ExecutionResult, error) {
			called = true
			if !got.Route.UsesConsul() {
				t.Fatalf("route UsesConsul() = false, want true")
			}
			return ExecutionResult{Count: 13}, nil
		}),
	}

	result, err := executor.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !called {
		t.Fatalf("legacy runtime was not called")
	}
	if result.Count != 13 {
		t.Fatalf("count = %d, want 13", result.Count)
	}
}

func TestLegacyExecutorRejectsDirectRoute(t *testing.T) {
	executor := LegacyExecutor{
		Runtime: LegacyRuntimeFunc(func(ctx context.Context, got ExecutionRequest) (ExecutionResult, error) {
			t.Fatalf("legacy runtime should not be called")
			return ExecutionResult{}, nil
		}),
	}

	result, err := executor.Execute(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
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

func TestLegacyExecutorReportsMissingRuntime(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}).
		WithRoute(LegacyGRPCRoute(RuntimeTarget{NodeID: "node-a"}))

	result, err := LegacyExecutor{}.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected missing runtime diagnostic")
	}
	if got := result.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}

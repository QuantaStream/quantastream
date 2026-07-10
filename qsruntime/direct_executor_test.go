package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestDirectRuntimeFuncExecutesDirectRequest(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	runtime := DirectRuntimeFunc(func(ctx context.Context, got ExecutionRequest) (ExecutionResult, error) {
		if !got.Route.Direct() {
			t.Fatalf("route Direct() = false, want true")
		}
		return ExecutionResult{Count: 3}, nil
	})

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Count != 3 {
		t.Fatalf("count = %d, want 3", result.Count)
	}
}

func TestDirectExecutorDelegatesToDirectRuntime(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	called := false
	executor := DirectExecutor{
		Runtime: DirectRuntimeFunc(func(ctx context.Context, got ExecutionRequest) (ExecutionResult, error) {
			called = true
			if !got.Route.Target.Local {
				t.Fatalf("target local = false, want true")
			}
			return ExecutionResult{Count: 11}, nil
		}),
	}

	result, err := executor.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !called {
		t.Fatalf("direct runtime was not called")
	}
	if result.Count != 11 {
		t.Fatalf("count = %d, want 11", result.Count)
	}
}

func TestDirectExecutorRejectsCompatibilityRoute(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}).
		WithRoute(LegacyProxyRoute(RuntimeTarget{Address: "127.0.0.1:4000"}))
	executor := DirectExecutor{
		Runtime: DirectRuntimeFunc(func(ctx context.Context, got ExecutionRequest) (ExecutionResult, error) {
			t.Fatalf("direct runtime should not be called")
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

func TestDirectExecutorReportsMissingRuntime(t *testing.T) {
	result, err := DirectExecutor{}.Execute(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
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

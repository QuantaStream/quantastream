package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestExecutionServiceRunsDirectRequest(t *testing.T) {
	service := NewExecutionService(
		ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
			if !request.Route.Direct() {
				t.Fatalf("route Direct() = false, want true")
			}
			return ExecutionResult{Count: 21}, nil
		}),
		nil,
	)

	result, err := service.Execute(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", result.Diagnostics)
	}
	if result.Count != 21 {
		t.Fatalf("count = %d, want 21", result.Count)
	}
}

func TestExecutionServiceRunsLegacyRequest(t *testing.T) {
	route := LegacyGRPCRoute(RuntimeTarget{NodeID: "node-a"})
	service := NewExecutionService(
		nil,
		ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
			if !request.Route.CompatibilityPath() {
				t.Fatalf("route CompatibilityPath() = false, want true")
			}
			return ExecutionResult{Count: 34}, nil
		}),
	)

	result, err := service.Execute(
		context.Background(),
		NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}).WithRoute(route),
	)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", result.Diagnostics)
	}
	if result.Count != 34 {
		t.Fatalf("count = %d, want 34", result.Count)
	}
}

func TestExecutionServiceReturnsSelectorDiagnostics(t *testing.T) {
	service := NewExecutionService(nil, nil)

	result, err := service.Execute(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected selector diagnostics")
	}
	if got := result.Diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}

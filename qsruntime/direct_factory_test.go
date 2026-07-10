package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestDirectRuntimeBuilderValidatesConfigBeforeFactory(t *testing.T) {
	builder := DirectRuntimeBuilder{
		Config: NewDirectRuntimeConfig("", "", -1, 0),
		Factory: DirectRuntimeFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error) {
			t.Fatalf("factory should not be called for invalid config")
			return nil, nil, nil
		}),
	}

	runtime, diagnostics, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if runtime != nil {
		t.Fatalf("runtime = %#v, want nil", runtime)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected invalid config diagnostics")
	}
	if got := diagnostics.Codes()[0]; got != qsbridge.DiagnosticInvalidExecutionOption {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInvalidExecutionOption)
	}
}

func TestDirectRuntimeBuilderReportsMissingFactory(t *testing.T) {
	builder := DirectRuntimeBuilder{Config: NewDirectRuntimeConfig("", "", 0, 0)}

	runtime, diagnostics, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if runtime != nil {
		t.Fatalf("runtime = %#v, want nil", runtime)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected missing factory diagnostics")
	}
	if got := diagnostics.Codes()[0]; got != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, qsbridge.DiagnosticInternalInvariant)
	}
}

func TestDirectRuntimeBuilderAppliesDefaultsBeforeFactory(t *testing.T) {
	var gotConfig DirectRuntimeConfig
	wantRuntime := DirectRuntimeFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Count: 1}, nil
	})
	builder := DirectRuntimeBuilder{
		Config: NewDirectRuntimeConfig("/tmp/quanta", "", 0, 4),
		Factory: DirectRuntimeFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error) {
			gotConfig = config
			return wantRuntime, nil, nil
		}),
	}

	runtime, diagnostics, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if runtime == nil {
		t.Fatalf("runtime = nil, want direct runtime")
	}
	if gotConfig.ServicePort != DefaultDirectServicePort {
		t.Fatalf("factory service port = %d, want %d", gotConfig.ServicePort, DefaultDirectServicePort)
	}
	if gotConfig.SessionPoolSize != 4 {
		t.Fatalf("factory session pool size = %d, want 4", gotConfig.SessionPoolSize)
	}
}

func TestNewDirectExecutionServiceBuildsDirectExecutor(t *testing.T) {
	builder := DirectRuntimeBuilder{
		Config: NewDirectRuntimeConfig("", "", 0, 0),
		Factory: DirectRuntimeFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error) {
			return DirectRuntimeFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
				return ExecutionResult{Count: 55}, nil
			}), nil, nil
		}),
	}

	service, diagnostics, err := NewDirectExecutionService(context.Background(), builder, nil)
	if err != nil {
		t.Fatalf("new direct service: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	result, err := service.Execute(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if result.Count != 55 {
		t.Fatalf("count = %d, want 55", result.Count)
	}
}

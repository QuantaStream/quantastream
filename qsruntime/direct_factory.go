package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// DirectRuntimeFactory builds a direct runtime from validated configuration.
type DirectRuntimeFactory interface {
	NewDirectRuntime(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error)
}

// DirectRuntimeFactoryFunc adapts a function to DirectRuntimeFactory.
type DirectRuntimeFactoryFunc func(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error)

// NewDirectRuntime calls f(ctx, config).
func (f DirectRuntimeFactoryFunc) NewDirectRuntime(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error) {
	return f(ctx, config)
}

// DirectRuntimeBuilder validates configuration before constructing a direct runtime.
type DirectRuntimeBuilder struct {
	Config  DirectRuntimeConfig
	Factory DirectRuntimeFactory
}

// Build validates the config and delegates direct runtime construction to the factory.
func (b DirectRuntimeBuilder) Build(ctx context.Context) (DirectRuntime, qsbridge.DiagnosticSet, error) {
	config := b.Config.WithDefaults()
	if diagnostics := config.Validate(); diagnostics.BlocksNative() {
		return nil, diagnostics, nil
	}
	if b.Factory == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"direct runtime builder has no factory",
			),
		}, nil
	}
	return b.Factory.NewDirectRuntime(ctx, config)
}

// NewDirectExecutionService builds an execution service configured with a direct runtime.
func NewDirectExecutionService(ctx context.Context, builder DirectRuntimeBuilder, legacy Executor) (ExecutionService, qsbridge.DiagnosticSet, error) {
	runtime, diagnostics, err := builder.Build(ctx)
	if err != nil || diagnostics.BlocksNative() {
		return ExecutionService{}, diagnostics, err
	}
	return NewExecutionService(DirectExecutor{Runtime: runtime}, legacy), nil, nil
}

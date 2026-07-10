package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// LegacyRuntime executes requests through a legacy compatibility boundary.
type LegacyRuntime interface {
	ExecuteLegacy(ctx context.Context, request ExecutionRequest) (ExecutionResult, error)
}

// LegacyRuntimeFunc adapts a function to the LegacyRuntime interface.
type LegacyRuntimeFunc func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error)

// ExecuteLegacy calls f(ctx, request).
func (f LegacyRuntimeFunc) ExecuteLegacy(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	return f(ctx, request)
}

// LegacyExecutor adapts a LegacyRuntime to the generic Executor contract.
type LegacyExecutor struct {
	Runtime LegacyRuntime
}

// Execute validates the compatibility route and delegates to the legacy runtime.
func (e LegacyExecutor) Execute(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	return RoutePolicyExecutor{
		Policy: CompatibilityOnlyRoutePolicy(),
		Next: ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
			if e.Runtime == nil {
				return ExecutionResult{
					Diagnostics: qsbridge.DiagnosticSet{
						qsbridge.ErrorDiagnostic(
							qsbridge.DiagnosticInternalInvariant,
							qsbridge.PhaseExecute,
							"legacy executor has no runtime",
						),
					},
				}, nil
			}
			return e.Runtime.ExecuteLegacy(ctx, request)
		}),
	}.Execute(ctx, request)
}

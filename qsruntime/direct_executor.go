package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// DirectRuntime executes requests through the in-process Quanta runtime boundary.
type DirectRuntime interface {
	ExecuteDirect(ctx context.Context, request ExecutionRequest) (ExecutionResult, error)
}

// DirectRuntimeFunc adapts a function to the DirectRuntime interface.
type DirectRuntimeFunc func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error)

// ExecuteDirect calls f(ctx, request).
func (f DirectRuntimeFunc) ExecuteDirect(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	return f(ctx, request)
}

// DirectExecutor adapts a DirectRuntime to the generic Executor contract.
type DirectExecutor struct {
	Runtime DirectRuntime
}

// Execute validates the direct route and delegates to the direct runtime.
func (e DirectExecutor) Execute(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	return RoutePolicyExecutor{
		Policy: DirectOnlyRoutePolicy(),
		Next: ExecutorFunc(func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
			if e.Runtime == nil {
				return ExecutionResult{
					Diagnostics: qsbridge.DiagnosticSet{
						qsbridge.ErrorDiagnostic(
							qsbridge.DiagnosticInternalInvariant,
							qsbridge.PhaseExecute,
							"direct executor has no runtime",
						),
					},
				}, nil
			}
			return e.Runtime.ExecuteDirect(ctx, request)
		}),
	}.Execute(ctx, request)
}

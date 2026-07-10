package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// Executor executes a neutral qsruntime request and returns a neutral result.
type Executor interface {
	Execute(ctx context.Context, request ExecutionRequest) (ExecutionResult, error)
}

// ExecutorFunc adapts a function to the Executor interface.
type ExecutorFunc func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error)

// Execute calls f(ctx, request).
func (f ExecutorFunc) Execute(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	return f(ctx, request)
}

// RoutePolicy controls which execution paths are available to a runtime executor.
type RoutePolicy struct {
	AllowDirect        bool
	AllowCompatibility bool
}

// DirectOnlyRoutePolicy returns the QIAB-first policy for direct in-process execution.
func DirectOnlyRoutePolicy() RoutePolicy {
	return RoutePolicy{AllowDirect: true}
}

// CompatibilityOnlyRoutePolicy returns a policy for legacy adapter execution.
func CompatibilityOnlyRoutePolicy() RoutePolicy {
	return RoutePolicy{AllowCompatibility: true}
}

// AllRoutePolicy returns a policy that accepts direct and compatibility routes.
func AllRoutePolicy() RoutePolicy {
	return RoutePolicy{
		AllowDirect:        true,
		AllowCompatibility: true,
	}
}

// Allows reports whether the route is accepted by the policy.
func (p RoutePolicy) Allows(route ExecutionRoute) bool {
	if route.Direct() {
		return p.AllowDirect
	}
	if route.CompatibilityPath() {
		return p.AllowCompatibility
	}
	return false
}

// Diagnostics returns route-rejection diagnostics for a disallowed route.
func (p RoutePolicy) Diagnostics(route ExecutionRoute) qsbridge.DiagnosticSet {
	if p.Allows(route) {
		return nil
	}
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticRouteRejected,
			qsbridge.PhaseExecute,
			"execution route is not enabled: "+string(route.Path),
		),
	}
}

// RoutePolicyExecutor validates request routes before delegating to another executor.
type RoutePolicyExecutor struct {
	Policy RoutePolicy
	Next   Executor
}

// Execute validates the request route and delegates to the wrapped executor.
func (e RoutePolicyExecutor) Execute(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	if diagnostics := e.Policy.Diagnostics(request.Route); diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, nil
	}
	if e.Next == nil {
		return ExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(
					qsbridge.DiagnosticInternalInvariant,
					qsbridge.PhaseExecute,
					"route policy executor has no wrapped executor",
				),
			},
		}, nil
	}
	return e.Next.Execute(ctx, request)
}

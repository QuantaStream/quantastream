package qsruntime

import "github.com/QuantaStream/quantastream/qsbridge"

// ExecutorSelector chooses the executor that should handle a routed request.
type ExecutorSelector struct {
	Direct Executor
	Legacy Executor
}

// Select returns the executor for the supplied route.
func (s ExecutorSelector) Select(route ExecutionRoute) (Executor, qsbridge.DiagnosticSet) {
	if route.Direct() {
		if s.Direct == nil {
			return nil, missingExecutorDiagnostics("direct executor is not configured")
		}
		return s.Direct, nil
	}
	if route.CompatibilityPath() {
		if s.Legacy == nil {
			return nil, missingExecutorDiagnostics("legacy executor is not configured")
		}
		return s.Legacy, nil
	}
	return nil, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticRouteRejected,
			qsbridge.PhaseExecute,
			"execution route is not recognized: "+string(route.Path),
		),
	}
}

// SelectRequest returns the executor for the request route.
func (s ExecutorSelector) SelectRequest(request ExecutionRequest) (Executor, qsbridge.DiagnosticSet) {
	return s.Select(request.Route)
}

func missingExecutorDiagnostics(message string) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticInternalInvariant,
			qsbridge.PhaseExecute,
			message,
		),
	}
}

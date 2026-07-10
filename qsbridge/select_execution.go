package qsbridge

// SimpleSelectExecutionResult turns a planned SELECT into a completed empty result envelope.
//
// This is a vertical scaffold for the future native executor boundary: parser,
// binder, planner, profile, result schema, and adapter result shape can be
// exercised before storage-backed row production exists.
func (r PlanResult) SimpleSelectExecutionResult(options ExecutionOptions) ExecutionResult {
	prepared := r.PreparedPlan()
	if prepared.Kind != QueryKindSelect {
		prepared.Diagnostics = append(prepared.Diagnostics, ErrorDiagnostic(
			DiagnosticUnsupportedSQL,
			PhaseExecute,
			"simple SELECT execution result requires a SELECT plan",
		))
		prepared.Supported = false
	}

	bound := BoundPlan{
		Prepared:    prepared,
		Diagnostics: cloneDiagnosticSet(prepared.Diagnostics),
		Supported:   prepared.Supported,
	}
	request := bound.ExecutionRequest(options)
	return completePlanOnlyExecutionResult(request.EmptyResult(), request.SupportedForExecution())
}

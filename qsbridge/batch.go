package qsbridge

// ParameterValueSet is one execute-time value set for a prepared batch request.
type ParameterValueSet struct {
	Values []ParameterValue
}

// ParameterValues creates one copied execute-time value set.
func ParameterValues(values ...ParameterValue) ParameterValueSet {
	return ParameterValueSet{Values: append([]ParameterValue(nil), values...)}
}

// BatchExecutionRequest is a non-executing descriptor for prepared batch execution.
//
// It validates every parameter set against one prepared plan and carries the
// same adapter-facing metadata as a single execution request. It does not loop
// over a mutator, call a native executor, or invoke the legacy runtime.
type BatchExecutionRequest struct {
	Prepared       PreparedPlan
	Options        ExecutionOptions
	ParameterSets  []ParameterBindingSet
	Diagnostics    DiagnosticSet
	Supported      bool
	Result         ResultShape
	ResultColumns  []ResultColumn
	Statement      StatementResult
	SessionActions []SessionAction
	Access         []AccessRequirement
}

// BatchExecutionRequest validates a prepared batch request without executing it.
func (p PreparedPlan) BatchExecutionRequest(options ExecutionOptions, sets ...ParameterValueSet) BatchExecutionRequest {
	optionDiagnostics := options.Diagnostics()
	bindings := make([]ParameterBindingSet, 0, len(sets))
	diagnostics := mergeDiagnosticSets(p.Diagnostics, optionDiagnostics)
	if len(sets) == 0 {
		diagnostics = mergeDiagnosticSets(diagnostics, DiagnosticSet{
			ErrorDiagnostic(
				DiagnosticInvalidExecutionOption,
				PhaseExecute,
				"batch execution requires at least one parameter set",
			),
		})
	}
	for _, set := range sets {
		binding := BindParameterValues(p.Parameters, set.Values...)
		bindings = append(bindings, cloneParameterBindingSet(binding))
		diagnostics = mergeDiagnosticSets(diagnostics, binding.Diagnostics)
	}
	supported := p.Supported && !diagnostics.BlocksNative()
	return BatchExecutionRequest{
		Prepared:       clonePreparedPlan(p),
		Options:        options,
		ParameterSets:  bindings,
		Diagnostics:    diagnostics,
		Supported:      supported,
		Result:         p.Result,
		ResultColumns:  append([]ResultColumn(nil), p.ResultColumns...),
		Statement:      cloneStatementResult(p.Statement),
		SessionActions: p.SessionActions(),
		Access:         cloneAccessRequirements(p.Access),
	}
}

// SupportedForExecution reports whether the batch request is valid for a future executor.
func (r BatchExecutionRequest) SupportedForExecution() bool {
	return r.Supported && !r.Diagnostics.BlocksNative()
}

// Route chooses native, fallback, or rejection for this batch execution request.
func (r BatchExecutionRequest) Route(policy RoutingPolicy) RouteDecision {
	return routeFromDiagnostics(r.SupportedForExecution(), r.Diagnostics, policy, true)
}

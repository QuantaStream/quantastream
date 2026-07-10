package qsbridge

// FallbackRequest is the adapter-facing descriptor for invoking the legacy path.
//
// It exists so protocol adapters can hand unsupported or policy-forced SQL to
// the compatibility engine without reaching through qsbridge internals. The
// request is metadata-only; qsbridge does not call the legacy runtime.
type FallbackRequest struct {
	SQL           string
	DefaultSchema string
	Session       SessionContext
	Options       ExecutionOptions
	Parameters    []ParameterBinding
	Diagnostics   DiagnosticSet
	Route         RouteDecision
}

// BatchFallbackRequest is the adapter-facing descriptor for legacy batch execution.
//
// It mirrors FallbackRequest, but preserves one bound parameter set per batch
// item so protocol adapters do not need to reinterpret execute-time values.
type BatchFallbackRequest struct {
	SQL           string
	DefaultSchema string
	Session       SessionContext
	Options       ExecutionOptions
	ParameterSets []ParameterBindingSet
	Diagnostics   DiagnosticSet
	Route         RouteDecision
}

// Supported reports whether the routed request can proceed through its chosen path.
func (r RoutedExecutionRequest) Supported() bool {
	return r.Route.Supported()
}

// Supported reports whether the routed batch request can proceed through its chosen path.
func (r RoutedBatchExecutionRequest) Supported() bool {
	return r.Route.Supported()
}

// NativeRequest returns the native execution descriptor when routing selected native.
func (r RoutedExecutionRequest) NativeRequest() (ExecutionRequest, bool) {
	if r.Route.Kind != RouteNative {
		return ExecutionRequest{}, false
	}
	return cloneExecutionRequest(r.Request), true
}

// NativeRequest returns the native batch descriptor when routing selected native.
func (r RoutedBatchExecutionRequest) NativeRequest() (BatchExecutionRequest, bool) {
	if r.Route.Kind != RouteNative {
		return BatchExecutionRequest{}, false
	}
	return cloneBatchExecutionRequest(r.Request), true
}

// LegacyFallbackRequest returns the fallback descriptor when routing selected legacy.
func (r RoutedExecutionRequest) LegacyFallbackRequest() (FallbackRequest, bool) {
	if r.Route.Kind != RouteLegacyFallback {
		return FallbackRequest{}, false
	}
	return r.FallbackRequest(), true
}

// LegacyFallbackRequest returns the batch fallback descriptor when routing selected legacy.
func (r RoutedBatchExecutionRequest) LegacyFallbackRequest() (BatchFallbackRequest, bool) {
	if r.Route.Kind != RouteLegacyFallback {
		return BatchFallbackRequest{}, false
	}
	return r.FallbackRequest(), true
}

// Supported reports whether this handoff is intentionally routed to legacy.
func (r FallbackRequest) Supported() bool {
	return r.Route.Kind == RouteLegacyFallback
}

// FallbackRequest returns the legacy handoff descriptor for this routed request.
func (r RoutedExecutionRequest) FallbackRequest() FallbackRequest {
	return r.Request.FallbackRequest(r.Route)
}

// FallbackRequest returns the legacy batch handoff descriptor for this routed request.
func (r RoutedBatchExecutionRequest) FallbackRequest() BatchFallbackRequest {
	return r.Request.FallbackRequest(r.Route)
}

// FallbackRequest returns the legacy handoff descriptor for this execution request.
func (r ExecutionRequest) FallbackRequest(route RouteDecision) FallbackRequest {
	prepared := r.Bound.Prepared
	return FallbackRequest{
		SQL:           prepared.SQL,
		DefaultSchema: prepared.DefaultSchema,
		Session:       prepared.Session.Clone(),
		Options:       r.Options,
		Parameters:    cloneParameterBindings(r.Bound.Parameters.Bindings),
		Diagnostics:   fallbackDiagnostics(r, route),
		Route:         cloneRouteDecision(route),
	}
}

// Supported reports whether this batch handoff is intentionally routed to legacy.
func (r BatchFallbackRequest) Supported() bool {
	return r.Route.Kind == RouteLegacyFallback
}

// FallbackRequest returns the legacy handoff descriptor for this batch request.
func (r BatchExecutionRequest) FallbackRequest(route RouteDecision) BatchFallbackRequest {
	prepared := r.Prepared
	return BatchFallbackRequest{
		SQL:           prepared.SQL,
		DefaultSchema: prepared.DefaultSchema,
		Session:       prepared.Session.Clone(),
		Options:       r.Options,
		ParameterSets: cloneParameterBindingSets(r.ParameterSets),
		Diagnostics:   batchFallbackDiagnostics(r, route),
		Route:         cloneRouteDecision(route),
	}
}

func fallbackDiagnostics(request ExecutionRequest, route RouteDecision) DiagnosticSet {
	if len(route.Diagnostics) > 0 {
		return cloneDiagnosticSet(route.Diagnostics)
	}
	return cloneDiagnosticSet(request.Diagnostics)
}

func batchFallbackDiagnostics(request BatchExecutionRequest, route RouteDecision) DiagnosticSet {
	if len(route.Diagnostics) > 0 {
		return cloneDiagnosticSet(route.Diagnostics)
	}
	return cloneDiagnosticSet(request.Diagnostics)
}

func cloneRouteDecision(decision RouteDecision) RouteDecision {
	decision.Diagnostics = cloneDiagnosticSet(decision.Diagnostics)
	return decision
}

func cloneParameterBindings(bindings []ParameterBinding) []ParameterBinding {
	if len(bindings) == 0 {
		return nil
	}
	return append([]ParameterBinding(nil), bindings...)
}

func cloneParameterBindingSets(sets []ParameterBindingSet) []ParameterBindingSet {
	if len(sets) == 0 {
		return nil
	}
	cloned := make([]ParameterBindingSet, 0, len(sets))
	for _, set := range sets {
		cloned = append(cloned, cloneParameterBindingSet(set))
	}
	return cloned
}

func cloneExecutionRequest(request ExecutionRequest) ExecutionRequest {
	cloned := request
	cloned.Bound = cloneBoundPlan(request.Bound)
	cloned.Diagnostics = cloneDiagnosticSet(request.Diagnostics)
	cloned.ResultColumns = append([]ResultColumn(nil), request.ResultColumns...)
	cloned.Statement = cloneStatementResult(request.Statement)
	cloned.SessionActions = cloneSessionActions(request.SessionActions)
	cloned.Access = cloneAccessRequirements(request.Access)
	cloned.Result.Columns = append([]FieldRef(nil), request.Result.Columns...)
	cloned.Result.Statement = cloneStatementResult(request.Result.Statement)
	return cloned
}

func cloneBatchExecutionRequest(request BatchExecutionRequest) BatchExecutionRequest {
	cloned := request
	cloned.Prepared = clonePreparedPlan(request.Prepared)
	cloned.ParameterSets = cloneParameterBindingSets(request.ParameterSets)
	cloned.Diagnostics = cloneDiagnosticSet(request.Diagnostics)
	cloned.ResultColumns = append([]ResultColumn(nil), request.ResultColumns...)
	cloned.Statement = cloneStatementResult(request.Statement)
	cloned.SessionActions = cloneSessionActions(request.SessionActions)
	cloned.Access = cloneAccessRequirements(request.Access)
	return cloned
}

func cloneBoundPlan(plan BoundPlan) BoundPlan {
	cloned := plan
	cloned.Prepared = clonePreparedPlan(plan.Prepared)
	cloned.Parameters = cloneParameterBindingSet(plan.Parameters)
	cloned.Diagnostics = cloneDiagnosticSet(plan.Diagnostics)
	return cloned
}

func cloneParameterBindingSet(set ParameterBindingSet) ParameterBindingSet {
	return ParameterBindingSet{
		Bindings:    cloneParameterBindings(set.Bindings),
		Diagnostics: cloneDiagnosticSet(set.Diagnostics),
	}
}

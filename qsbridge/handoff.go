package qsbridge

// ExecutionHandoffKind is the final adapter-facing execution path decision.
type ExecutionHandoffKind string

const (
	// ExecutionHandoffNative means the request may be sent to a native executor.
	ExecutionHandoffNative ExecutionHandoffKind = "native"
	// ExecutionHandoffLegacyFallback means the request may be sent to legacy compatibility.
	ExecutionHandoffLegacyFallback ExecutionHandoffKind = "legacy_fallback"
	// ExecutionHandoffProtocolRejected means the protocol cannot carry the requested shape.
	ExecutionHandoffProtocolRejected ExecutionHandoffKind = "protocol_rejected"
	// ExecutionHandoffRejected means routing policy rejected the request.
	ExecutionHandoffRejected ExecutionHandoffKind = "rejected"
	// ExecutionHandoffDenied means authorization rejected the request.
	ExecutionHandoffDenied ExecutionHandoffKind = "denied"
)

// RoutedAuthorizedExecutionRequest combines routing and authorization decisions.
//
// This is the final metadata-only handoff descriptor that protocol adapters can
// inspect before choosing native execution, legacy fallback, or an error
// response. qsbridge still does not execute either path.
type RoutedAuthorizedExecutionRequest struct {
	Prepared      PreparedPlan
	Request       ExecutionRequest
	Route         RouteDecision
	Authorization AuthorizationDecision
}

// RoutedAuthorizedBatchExecutionRequest combines batch routing and authorization decisions.
type RoutedAuthorizedBatchExecutionRequest struct {
	Prepared      PreparedPlan
	Request       BatchExecutionRequest
	Route         RouteDecision
	Authorization AuthorizationDecision
}

// ProtocolRoutedAuthorizedExecutionRequest combines protocol, routing, and authorization decisions.
type ProtocolRoutedAuthorizedExecutionRequest struct {
	Prepared      PreparedPlan
	Request       ExecutionRequest
	Protocol      ProtocolNegotiation
	Route         RouteDecision
	Authorization AuthorizationDecision
}

// ProtocolRoutedAuthorizedBatchExecutionRequest combines protocol, batch routing, and authorization decisions.
type ProtocolRoutedAuthorizedBatchExecutionRequest struct {
	Prepared      PreparedPlan
	Request       BatchExecutionRequest
	Protocol      ProtocolNegotiation
	Route         RouteDecision
	Authorization AuthorizationDecision
}

// ExecutionHandoffOutcome is the protocol-facing summary of a final handoff.
type ExecutionHandoffOutcome struct {
	Kind           ExecutionHandoffKind
	Supported      bool
	AccessIntent   PhysicalAccessIntent
	Lifecycle      ClientPlanLifecycleKind
	LifecycleSteps int
	Diagnostics    DiagnosticSet
}

// RoutedAuthorizedExecutionRequest returns the final handoff decision for a prepared plan.
func (s PlanningService) RoutedAuthorizedExecutionRequest(prepared PreparedPlan, options ExecutionOptions, values ...ParameterValue) RoutedAuthorizedExecutionRequest {
	request := prepared.ExecutionRequest(options, values...)
	return s.authorizeAndRoute(prepared, request)
}

// PrepareRoutedAuthorizedExecutionRequest prepares SQL and returns the final handoff decision.
func (s PlanningService) PrepareRoutedAuthorizedExecutionRequest(request PlanRequest, options ExecutionOptions, values ...ParameterValue) RoutedAuthorizedExecutionRequest {
	prepared, execution := s.PrepareExecutionRequest(request, options, values...)
	return s.authorizeAndRoute(prepared, execution)
}

// RoutedAuthorizedBatchExecutionRequest returns the final handoff decision for a prepared batch.
func (s PlanningService) RoutedAuthorizedBatchExecutionRequest(prepared PreparedPlan, options ExecutionOptions, sets ...ParameterValueSet) RoutedAuthorizedBatchExecutionRequest {
	request := prepared.BatchExecutionRequest(options, sets...)
	return s.authorizeAndRouteBatch(prepared, request)
}

// PrepareRoutedAuthorizedBatchExecutionRequest prepares SQL and returns the final batch handoff decision.
func (s PlanningService) PrepareRoutedAuthorizedBatchExecutionRequest(request PlanRequest, options ExecutionOptions, sets ...ParameterValueSet) RoutedAuthorizedBatchExecutionRequest {
	prepared, execution := s.PrepareBatchExecutionRequest(request, options, sets...)
	return s.authorizeAndRouteBatch(prepared, execution)
}

// ProtocolRoutedAuthorizedExecutionRequest returns protocol, routing, and authorization decisions.
func (s PlanningService) ProtocolRoutedAuthorizedExecutionRequest(prepared PreparedPlan, profile ProtocolProfile, mode ProtocolExecutionMode, options ExecutionOptions, values ...ParameterValue) ProtocolRoutedAuthorizedExecutionRequest {
	request := prepared.ExecutionRequest(options, values...)
	return s.authorizeRouteAndNegotiate(prepared, request, profile.NegotiateExecution(mode, options))
}

// PrepareProtocolRoutedAuthorizedExecutionRequest prepares SQL and returns protocol-aware final handoff metadata.
func (s PlanningService) PrepareProtocolRoutedAuthorizedExecutionRequest(request PlanRequest, profile ProtocolProfile, mode ProtocolExecutionMode, options ExecutionOptions, values ...ParameterValue) ProtocolRoutedAuthorizedExecutionRequest {
	prepared, execution := s.PrepareExecutionRequest(request, options, values...)
	return s.authorizeRouteAndNegotiate(prepared, execution, profile.NegotiateExecution(mode, options))
}

// ProtocolRoutedAuthorizedBatchExecutionRequest returns batch protocol, routing, and authorization decisions.
func (s PlanningService) ProtocolRoutedAuthorizedBatchExecutionRequest(prepared PreparedPlan, profile ProtocolProfile, options ExecutionOptions, sets ...ParameterValueSet) ProtocolRoutedAuthorizedBatchExecutionRequest {
	request := prepared.BatchExecutionRequest(options, sets...)
	return s.authorizeRouteAndNegotiateBatch(prepared, request, profile.NegotiateExecution(ProtocolBatchExecution, options))
}

// PrepareProtocolRoutedAuthorizedBatchExecutionRequest prepares SQL and returns protocol-aware batch handoff metadata.
func (s PlanningService) PrepareProtocolRoutedAuthorizedBatchExecutionRequest(request PlanRequest, profile ProtocolProfile, options ExecutionOptions, sets ...ParameterValueSet) ProtocolRoutedAuthorizedBatchExecutionRequest {
	prepared, execution := s.PrepareBatchExecutionRequest(request, options, sets...)
	return s.authorizeRouteAndNegotiateBatch(prepared, execution, profile.NegotiateExecution(ProtocolBatchExecution, options))
}

func (s PlanningService) authorizeAndRoute(prepared PreparedPlan, request ExecutionRequest) RoutedAuthorizedExecutionRequest {
	return RoutedAuthorizedExecutionRequest{
		Prepared:      prepared,
		Request:       request,
		Route:         request.Route(s.Routing),
		Authorization: request.AuthorizationRequest().Authorize(s.Authorizer),
	}
}

func (s PlanningService) authorizeAndRouteBatch(prepared PreparedPlan, request BatchExecutionRequest) RoutedAuthorizedBatchExecutionRequest {
	return RoutedAuthorizedBatchExecutionRequest{
		Prepared:      prepared,
		Request:       request,
		Route:         request.Route(s.Routing),
		Authorization: request.AuthorizationRequest().Authorize(s.Authorizer),
	}
}

func (s PlanningService) authorizeRouteAndNegotiate(prepared PreparedPlan, request ExecutionRequest, protocol ProtocolNegotiation) ProtocolRoutedAuthorizedExecutionRequest {
	return ProtocolRoutedAuthorizedExecutionRequest{
		Prepared:      prepared,
		Request:       request,
		Protocol:      protocol,
		Route:         request.Route(s.Routing),
		Authorization: request.AuthorizationRequest().Authorize(s.Authorizer),
	}
}

func (s PlanningService) authorizeRouteAndNegotiateBatch(prepared PreparedPlan, request BatchExecutionRequest, protocol ProtocolNegotiation) ProtocolRoutedAuthorizedBatchExecutionRequest {
	return ProtocolRoutedAuthorizedBatchExecutionRequest{
		Prepared:      prepared,
		Request:       request,
		Protocol:      protocol,
		Route:         request.Route(s.Routing),
		Authorization: request.AuthorizationRequest().Authorize(s.Authorizer),
	}
}

// Supported reports whether the request can proceed through its final handoff path.
func (r RoutedAuthorizedExecutionRequest) Supported() bool {
	return r.Authorization.Supported() && r.Route.Supported()
}

// Supported reports whether the batch request can proceed through its final handoff path.
func (r RoutedAuthorizedBatchExecutionRequest) Supported() bool {
	return r.Authorization.Supported() && r.Route.Supported()
}

// Supported reports whether the request can proceed through protocol and final handoff checks.
func (r ProtocolRoutedAuthorizedExecutionRequest) Supported() bool {
	return r.Authorization.Supported() && r.Protocol.Supported() && r.Route.Supported()
}

// Supported reports whether the batch request can proceed through protocol and final handoff checks.
func (r ProtocolRoutedAuthorizedBatchExecutionRequest) Supported() bool {
	return r.Authorization.Supported() && r.Protocol.Supported() && r.Route.Supported()
}

// HandoffKind returns the final execution path or error class for adapters.
func (r RoutedAuthorizedExecutionRequest) HandoffKind() ExecutionHandoffKind {
	if !r.Authorization.Supported() {
		return ExecutionHandoffDenied
	}
	switch r.Route.Kind {
	case RouteNative:
		return ExecutionHandoffNative
	case RouteLegacyFallback:
		return ExecutionHandoffLegacyFallback
	default:
		return ExecutionHandoffRejected
	}
}

// HandoffKind returns the final batch execution path or error class for adapters.
func (r RoutedAuthorizedBatchExecutionRequest) HandoffKind() ExecutionHandoffKind {
	if !r.Authorization.Supported() {
		return ExecutionHandoffDenied
	}
	switch r.Route.Kind {
	case RouteNative:
		return ExecutionHandoffNative
	case RouteLegacyFallback:
		return ExecutionHandoffLegacyFallback
	default:
		return ExecutionHandoffRejected
	}
}

// HandoffKind returns the final protocol-aware execution path or error class.
func (r ProtocolRoutedAuthorizedExecutionRequest) HandoffKind() ExecutionHandoffKind {
	if !r.Authorization.Supported() {
		return ExecutionHandoffDenied
	}
	if !r.Protocol.Supported() {
		return ExecutionHandoffProtocolRejected
	}
	switch r.Route.Kind {
	case RouteNative:
		return ExecutionHandoffNative
	case RouteLegacyFallback:
		return ExecutionHandoffLegacyFallback
	default:
		return ExecutionHandoffRejected
	}
}

// HandoffKind returns the final protocol-aware batch execution path or error class.
func (r ProtocolRoutedAuthorizedBatchExecutionRequest) HandoffKind() ExecutionHandoffKind {
	if !r.Authorization.Supported() {
		return ExecutionHandoffDenied
	}
	if !r.Protocol.Supported() {
		return ExecutionHandoffProtocolRejected
	}
	switch r.Route.Kind {
	case RouteNative:
		return ExecutionHandoffNative
	case RouteLegacyFallback:
		return ExecutionHandoffLegacyFallback
	default:
		return ExecutionHandoffRejected
	}
}

// Diagnostics returns final handoff diagnostics for protocol error mapping.
func (r RoutedAuthorizedExecutionRequest) Diagnostics() DiagnosticSet {
	diagnostics := cloneDiagnosticSet(r.Route.Diagnostics)
	if len(diagnostics) == 0 {
		diagnostics = cloneDiagnosticSet(r.Request.Diagnostics)
	}
	return mergeDiagnosticSets(diagnostics, r.Authorization.Diagnostics)
}

// Outcome returns the protocol-facing summary of this final handoff.
func (r RoutedAuthorizedExecutionRequest) Outcome() ExecutionHandoffOutcome {
	return ExecutionHandoffOutcome{
		Kind:           r.HandoffKind(),
		Supported:      r.Supported(),
		AccessIntent:   r.Prepared.AccessIntent(),
		Lifecycle:      clientPlanLifecycleKind(r.Prepared.Kind),
		LifecycleSteps: clientPlanLifecycleStepCount(r.Prepared.Kind),
		Diagnostics:    r.Diagnostics(),
	}
}

// ProtocolErrors converts final handoff diagnostics into protocol-facing errors.
func (r RoutedAuthorizedExecutionRequest) ProtocolErrors() []ProtocolError {
	return r.Diagnostics().ProtocolErrors()
}

// FirstProtocolError returns the first blocking final handoff error, if any.
func (r RoutedAuthorizedExecutionRequest) FirstProtocolError() (ProtocolError, bool) {
	return r.Diagnostics().FirstProtocolError()
}

// Diagnostics returns final batch handoff diagnostics for protocol error mapping.
func (r RoutedAuthorizedBatchExecutionRequest) Diagnostics() DiagnosticSet {
	diagnostics := cloneDiagnosticSet(r.Route.Diagnostics)
	if len(diagnostics) == 0 {
		diagnostics = cloneDiagnosticSet(r.Request.Diagnostics)
	}
	return mergeDiagnosticSets(diagnostics, r.Authorization.Diagnostics)
}

// Diagnostics returns final protocol-aware handoff diagnostics for error mapping.
func (r ProtocolRoutedAuthorizedExecutionRequest) Diagnostics() DiagnosticSet {
	diagnostics := cloneDiagnosticSet(r.Route.Diagnostics)
	if len(diagnostics) == 0 {
		diagnostics = cloneDiagnosticSet(r.Request.Diagnostics)
	}
	diagnostics = mergeDiagnosticSets(diagnostics, r.Protocol.Diagnostics)
	return mergeDiagnosticSets(diagnostics, r.Authorization.Diagnostics)
}

// Diagnostics returns final protocol-aware batch handoff diagnostics for error mapping.
func (r ProtocolRoutedAuthorizedBatchExecutionRequest) Diagnostics() DiagnosticSet {
	diagnostics := cloneDiagnosticSet(r.Route.Diagnostics)
	if len(diagnostics) == 0 {
		diagnostics = cloneDiagnosticSet(r.Request.Diagnostics)
	}
	diagnostics = mergeDiagnosticSets(diagnostics, r.Protocol.Diagnostics)
	return mergeDiagnosticSets(diagnostics, r.Authorization.Diagnostics)
}

// Outcome returns the protocol-facing summary of this final batch handoff.
func (r RoutedAuthorizedBatchExecutionRequest) Outcome() ExecutionHandoffOutcome {
	return ExecutionHandoffOutcome{
		Kind:           r.HandoffKind(),
		Supported:      r.Supported(),
		AccessIntent:   r.Prepared.AccessIntent(),
		Lifecycle:      clientPlanLifecycleKind(r.Prepared.Kind),
		LifecycleSteps: clientPlanLifecycleStepCount(r.Prepared.Kind),
		Diagnostics:    r.Diagnostics(),
	}
}

// Outcome returns the protocol-facing summary of this final handoff.
func (r ProtocolRoutedAuthorizedExecutionRequest) Outcome() ExecutionHandoffOutcome {
	return ExecutionHandoffOutcome{
		Kind:           r.HandoffKind(),
		Supported:      r.Supported(),
		AccessIntent:   r.Prepared.AccessIntent(),
		Lifecycle:      clientPlanLifecycleKind(r.Prepared.Kind),
		LifecycleSteps: clientPlanLifecycleStepCount(r.Prepared.Kind),
		Diagnostics:    r.Diagnostics(),
	}
}

// Outcome returns the protocol-facing summary of this final batch handoff.
func (r ProtocolRoutedAuthorizedBatchExecutionRequest) Outcome() ExecutionHandoffOutcome {
	return ExecutionHandoffOutcome{
		Kind:           r.HandoffKind(),
		Supported:      r.Supported(),
		AccessIntent:   r.Prepared.AccessIntent(),
		Lifecycle:      clientPlanLifecycleKind(r.Prepared.Kind),
		LifecycleSteps: clientPlanLifecycleStepCount(r.Prepared.Kind),
		Diagnostics:    r.Diagnostics(),
	}
}

// ProtocolErrors converts final batch handoff diagnostics into protocol-facing errors.
func (r RoutedAuthorizedBatchExecutionRequest) ProtocolErrors() []ProtocolError {
	return r.Diagnostics().ProtocolErrors()
}

// FirstProtocolError returns the first blocking final batch handoff error, if any.
func (r RoutedAuthorizedBatchExecutionRequest) FirstProtocolError() (ProtocolError, bool) {
	return r.Diagnostics().FirstProtocolError()
}

// ProtocolErrors converts final protocol-aware handoff diagnostics into protocol-facing errors.
func (r ProtocolRoutedAuthorizedExecutionRequest) ProtocolErrors() []ProtocolError {
	return r.Diagnostics().ProtocolErrors()
}

// FirstProtocolError returns the first blocking protocol-aware handoff error, if any.
func (r ProtocolRoutedAuthorizedExecutionRequest) FirstProtocolError() (ProtocolError, bool) {
	return r.Diagnostics().FirstProtocolError()
}

// ProtocolErrors converts final protocol-aware batch handoff diagnostics into protocol-facing errors.
func (r ProtocolRoutedAuthorizedBatchExecutionRequest) ProtocolErrors() []ProtocolError {
	return r.Diagnostics().ProtocolErrors()
}

// FirstProtocolError returns the first blocking protocol-aware batch handoff error, if any.
func (r ProtocolRoutedAuthorizedBatchExecutionRequest) FirstProtocolError() (ProtocolError, bool) {
	return r.Diagnostics().FirstProtocolError()
}

// ProtocolErrors converts outcome diagnostics into protocol-facing errors.
func (o ExecutionHandoffOutcome) ProtocolErrors() []ProtocolError {
	return o.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking outcome error, if any.
func (o ExecutionHandoffOutcome) FirstProtocolError() (ProtocolError, bool) {
	return o.Diagnostics.FirstProtocolError()
}

// NativeRequest returns the native execution descriptor when final handoff selected native.
func (r RoutedAuthorizedExecutionRequest) NativeRequest() (ExecutionRequest, bool) {
	if r.HandoffKind() != ExecutionHandoffNative {
		return ExecutionRequest{}, false
	}
	return cloneExecutionRequest(r.Request), true
}

// NativeRequest returns the native batch descriptor when final handoff selected native.
func (r RoutedAuthorizedBatchExecutionRequest) NativeRequest() (BatchExecutionRequest, bool) {
	if r.HandoffKind() != ExecutionHandoffNative {
		return BatchExecutionRequest{}, false
	}
	return cloneBatchExecutionRequest(r.Request), true
}

// LegacyFallbackRequest returns the fallback descriptor when final handoff selected legacy.
func (r RoutedAuthorizedExecutionRequest) LegacyFallbackRequest() (FallbackRequest, bool) {
	if r.HandoffKind() != ExecutionHandoffLegacyFallback {
		return FallbackRequest{}, false
	}
	return RoutedExecutionRequest{
		Prepared: r.Prepared,
		Request:  r.Request,
		Route:    r.Route,
	}.FallbackRequest(), true
}

// LegacyFallbackRequest returns the fallback descriptor when final batch handoff selected legacy.
func (r RoutedAuthorizedBatchExecutionRequest) LegacyFallbackRequest() (BatchFallbackRequest, bool) {
	if r.HandoffKind() != ExecutionHandoffLegacyFallback {
		return BatchFallbackRequest{}, false
	}
	return r.Request.FallbackRequest(r.Route), true
}

// NativeRequest returns the native execution descriptor when protocol-aware handoff selected native.
func (r ProtocolRoutedAuthorizedExecutionRequest) NativeRequest() (ExecutionRequest, bool) {
	if r.HandoffKind() != ExecutionHandoffNative {
		return ExecutionRequest{}, false
	}
	return cloneExecutionRequest(r.Request), true
}

// NativeRequest returns the native batch descriptor when protocol-aware handoff selected native.
func (r ProtocolRoutedAuthorizedBatchExecutionRequest) NativeRequest() (BatchExecutionRequest, bool) {
	if r.HandoffKind() != ExecutionHandoffNative {
		return BatchExecutionRequest{}, false
	}
	return cloneBatchExecutionRequest(r.Request), true
}

// LegacyFallbackRequest returns the fallback descriptor when protocol-aware handoff selected legacy.
func (r ProtocolRoutedAuthorizedExecutionRequest) LegacyFallbackRequest() (FallbackRequest, bool) {
	if r.HandoffKind() != ExecutionHandoffLegacyFallback {
		return FallbackRequest{}, false
	}
	return RoutedExecutionRequest{
		Prepared: r.Prepared,
		Request:  r.Request,
		Route:    r.Route,
	}.FallbackRequest(), true
}

// LegacyFallbackRequest returns the fallback descriptor when protocol-aware batch handoff selected legacy.
func (r ProtocolRoutedAuthorizedBatchExecutionRequest) LegacyFallbackRequest() (BatchFallbackRequest, bool) {
	if r.HandoffKind() != ExecutionHandoffLegacyFallback {
		return BatchFallbackRequest{}, false
	}
	return r.Request.FallbackRequest(r.Route), true
}

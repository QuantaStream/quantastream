package qsbridge

// PlanningService is the adapter-facing entry point for native planning scaffolding.
//
// It composes parsing, binding, planning, prepared-plan snapshots, optional
// process-local caching, parameter binding, and execution-request construction.
// It intentionally does not execute plans or mutate sessions.
type PlanningService struct {
	Planner    Planner
	Cache      PreparedPlanCache
	Routing    RoutingPolicy
	Authorizer AccessAuthorizer
}

// NewPlanningService creates a service facade over planner and optional cache.
func NewPlanningService(planner Planner, cache PreparedPlanCache) PlanningService {
	return PlanningService{Planner: planner, Cache: cache}
}

// RoutedExecutionRequest combines preparation, execution-request validation, and routing.
type RoutedExecutionRequest struct {
	Prepared PreparedPlan
	Request  ExecutionRequest
	Route    RouteDecision
}

// RoutedBatchExecutionRequest combines batch metadata with a route decision.
type RoutedBatchExecutionRequest struct {
	Prepared PreparedPlan
	Request  BatchExecutionRequest
	Route    RouteDecision
}

// AuthorizedExecutionRequest combines execution metadata with authorization metadata.
type AuthorizedExecutionRequest struct {
	Prepared      PreparedPlan
	Request       ExecutionRequest
	Authorization AuthorizationDecision
}

// AuthorizedBatchExecutionRequest combines batch metadata with authorization metadata.
type AuthorizedBatchExecutionRequest struct {
	Prepared      PreparedPlan
	Request       BatchExecutionRequest
	Authorization AuthorizationDecision
}

// ProtocolExecutionRequest combines execution metadata with protocol negotiation.
type ProtocolExecutionRequest struct {
	Prepared PreparedPlan
	Request  ExecutionRequest
	Protocol ProtocolNegotiation
}

// ProtocolBatchExecutionRequest combines batch metadata with protocol negotiation.
type ProtocolBatchExecutionRequest struct {
	Prepared PreparedPlan
	Request  BatchExecutionRequest
	Protocol ProtocolNegotiation
}

// SupportedForProtocol reports whether both execution metadata and protocol negotiation are valid.
func (r ProtocolExecutionRequest) SupportedForProtocol() bool {
	return r.Request.SupportedForExecution() && r.Protocol.Supported()
}

// Diagnostics returns request and protocol diagnostics in first-seen order.
func (r ProtocolExecutionRequest) Diagnostics() DiagnosticSet {
	return mergeDiagnosticSets(r.Request.Diagnostics, r.Protocol.Diagnostics)
}

// SupportedForProtocol reports whether both batch metadata and protocol negotiation are valid.
func (r ProtocolBatchExecutionRequest) SupportedForProtocol() bool {
	return r.Request.SupportedForExecution() && r.Protocol.Supported()
}

// Diagnostics returns request and protocol diagnostics in first-seen order.
func (r ProtocolBatchExecutionRequest) Diagnostics() DiagnosticSet {
	return mergeDiagnosticSets(r.Request.Diagnostics, r.Protocol.Diagnostics)
}

// Prepare parses, binds, plans, and snapshots SQL using planner defaults.
func (s PlanningService) Prepare(sql string) PreparedPlan {
	return s.PrepareWithRequest(PlanRequest{SQL: sql})
}

// PrepareWithRequest parses, binds, plans, snapshots, and optionally caches SQL.
func (s PlanningService) PrepareWithRequest(request PlanRequest) PreparedPlan {
	request = s.Planner.withDefaults(request)
	key := request.CacheKey()
	if s.Cache != nil {
		if plan, ok := s.Cache.Get(key); ok {
			return plan
		}
	}

	prepared := s.Planner.PlanWithRequest(request).PreparedPlan()
	if s.Cache != nil && !prepared.Diagnostics.BlocksNative() {
		s.Cache.Put(prepared)
	}
	return prepared
}

// ExecutionRequest creates a non-executing handoff descriptor from a prepared plan.
func (s PlanningService) ExecutionRequest(prepared PreparedPlan, options ExecutionOptions, values ...ParameterValue) ExecutionRequest {
	_ = s
	return prepared.ExecutionRequest(options, values...)
}

// PrepareExecutionRequest prepares SQL and returns a non-executing handoff descriptor.
func (s PlanningService) PrepareExecutionRequest(request PlanRequest, options ExecutionOptions, values ...ParameterValue) (PreparedPlan, ExecutionRequest) {
	prepared := s.PrepareWithRequest(request)
	return prepared, prepared.ExecutionRequest(options, values...)
}

// AuthorizedExecutionRequest prepares, binds, and authorizes an execution request.
func (s PlanningService) AuthorizedExecutionRequest(request PlanRequest, options ExecutionOptions, values ...ParameterValue) AuthorizedExecutionRequest {
	prepared, execution := s.PrepareExecutionRequest(request, options, values...)
	return AuthorizedExecutionRequest{
		Prepared:      prepared,
		Request:       execution,
		Authorization: execution.AuthorizationRequest().Authorize(s.Authorizer),
	}
}

// AuthorizedBatchExecutionRequest prepares, binds, and authorizes a batch execution request.
func (s PlanningService) AuthorizedBatchExecutionRequest(request PlanRequest, options ExecutionOptions, sets ...ParameterValueSet) AuthorizedBatchExecutionRequest {
	prepared, execution := s.PrepareBatchExecutionRequest(request, options, sets...)
	return AuthorizedBatchExecutionRequest{
		Prepared:      prepared,
		Request:       execution,
		Authorization: execution.AuthorizationRequest().Authorize(s.Authorizer),
	}
}

// ProtocolExecutionRequest creates an execution request plus protocol negotiation metadata.
func (s PlanningService) ProtocolExecutionRequest(prepared PreparedPlan, profile ProtocolProfile, mode ProtocolExecutionMode, options ExecutionOptions, values ...ParameterValue) ProtocolExecutionRequest {
	request := s.ExecutionRequest(prepared, options, values...)
	return ProtocolExecutionRequest{
		Prepared: prepared,
		Request:  request,
		Protocol: profile.NegotiateExecution(mode, options),
	}
}

// PrepareProtocolExecutionRequest prepares SQL and returns execution plus protocol metadata.
func (s PlanningService) PrepareProtocolExecutionRequest(request PlanRequest, profile ProtocolProfile, mode ProtocolExecutionMode, options ExecutionOptions, values ...ParameterValue) ProtocolExecutionRequest {
	prepared, execution := s.PrepareExecutionRequest(request, options, values...)
	return ProtocolExecutionRequest{
		Prepared: prepared,
		Request:  execution,
		Protocol: profile.NegotiateExecution(mode, options),
	}
}

// BatchExecutionRequest creates a non-executing batch descriptor from a prepared plan.
func (s PlanningService) BatchExecutionRequest(prepared PreparedPlan, options ExecutionOptions, sets ...ParameterValueSet) BatchExecutionRequest {
	_ = s
	return prepared.BatchExecutionRequest(options, sets...)
}

// PrepareBatchExecutionRequest prepares SQL and returns a batch handoff descriptor.
func (s PlanningService) PrepareBatchExecutionRequest(request PlanRequest, options ExecutionOptions, sets ...ParameterValueSet) (PreparedPlan, BatchExecutionRequest) {
	prepared := s.PrepareWithRequest(request)
	return prepared, prepared.BatchExecutionRequest(options, sets...)
}

// ProtocolBatchExecutionRequest creates a batch request plus protocol negotiation metadata.
func (s PlanningService) ProtocolBatchExecutionRequest(prepared PreparedPlan, profile ProtocolProfile, options ExecutionOptions, sets ...ParameterValueSet) ProtocolBatchExecutionRequest {
	request := s.BatchExecutionRequest(prepared, options, sets...)
	return ProtocolBatchExecutionRequest{
		Prepared: prepared,
		Request:  request,
		Protocol: profile.NegotiateExecution(ProtocolBatchExecution, options),
	}
}

// PrepareProtocolBatchExecutionRequest prepares SQL and returns batch plus protocol metadata.
func (s PlanningService) PrepareProtocolBatchExecutionRequest(request PlanRequest, profile ProtocolProfile, options ExecutionOptions, sets ...ParameterValueSet) ProtocolBatchExecutionRequest {
	prepared, execution := s.PrepareBatchExecutionRequest(request, options, sets...)
	return ProtocolBatchExecutionRequest{
		Prepared: prepared,
		Request:  execution,
		Protocol: profile.NegotiateExecution(ProtocolBatchExecution, options),
	}
}

// RoutedExecutionRequest returns a route decision for an already prepared plan.
func (s PlanningService) RoutedExecutionRequest(prepared PreparedPlan, options ExecutionOptions, values ...ParameterValue) RoutedExecutionRequest {
	request := prepared.ExecutionRequest(options, values...)
	return RoutedExecutionRequest{
		Prepared: prepared,
		Request:  request,
		Route:    request.Route(s.Routing),
	}
}

// PrepareRoutedExecutionRequest prepares SQL and returns an execution request plus route decision.
func (s PlanningService) PrepareRoutedExecutionRequest(request PlanRequest, options ExecutionOptions, values ...ParameterValue) RoutedExecutionRequest {
	prepared, execution := s.PrepareExecutionRequest(request, options, values...)
	return RoutedExecutionRequest{
		Prepared: prepared,
		Request:  execution,
		Route:    execution.Route(s.Routing),
	}
}

// RoutedBatchExecutionRequest returns a route decision for an already prepared batch plan.
func (s PlanningService) RoutedBatchExecutionRequest(prepared PreparedPlan, options ExecutionOptions, sets ...ParameterValueSet) RoutedBatchExecutionRequest {
	request := prepared.BatchExecutionRequest(options, sets...)
	return RoutedBatchExecutionRequest{
		Prepared: prepared,
		Request:  request,
		Route:    request.Route(s.Routing),
	}
}

// PrepareRoutedBatchExecutionRequest prepares SQL and returns a batch request plus route decision.
func (s PlanningService) PrepareRoutedBatchExecutionRequest(request PlanRequest, options ExecutionOptions, sets ...ParameterValueSet) RoutedBatchExecutionRequest {
	prepared, execution := s.PrepareBatchExecutionRequest(request, options, sets...)
	return RoutedBatchExecutionRequest{
		Prepared: prepared,
		Request:  execution,
		Route:    execution.Route(s.Routing),
	}
}

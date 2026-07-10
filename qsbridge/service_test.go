package qsbridge

import "testing"

func TestPlanningServicePrepareUsesPreparedPlanCache(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
		Scope:         PhysicalScope{Placement: PlacementLocal},
	}, NewMemoryPreparedPlanCache())

	first := service.Prepare("select o_orderkey from orders where o_orderkey = ?")
	if first.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", first.Diagnostics)
	}
	second := service.Prepare("select o_orderkey from orders where o_orderkey = ?")
	if parser.count != 1 {
		t.Fatalf("parser count = %d, want cached second prepare", parser.count)
	}
	if second.CacheKey().Digest != first.CacheKey().Digest {
		t.Fatalf("second cache key = %q, want first %q", second.CacheKey().Digest, first.CacheKey().Digest)
	}

	first.Parameters[0].Type = DataTypeString
	third := service.Prepare("select o_orderkey from orders where o_orderkey = ?")
	if third.Parameters[0].Type != DataTypeInt {
		t.Fatalf("cached prepared plan leaked mutable parameter metadata")
	}
}

func TestPlanningServicePrepareWithRequestSeparatesSessionBoundaries(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, NewMemoryPreparedPlanCache())

	reader := service.PrepareWithRequest(PlanRequest{
		SQL:     "select o_orderkey from orders where o_orderkey = ?",
		Session: SessionContext{User: "reader"},
	})
	writer := service.PrepareWithRequest(PlanRequest{
		SQL:     "select o_orderkey from orders where o_orderkey = ?",
		Session: SessionContext{User: "writer"},
	})
	if reader.CacheKey().Digest == writer.CacheKey().Digest {
		t.Fatalf("cache key crossed session user boundary")
	}
	if parser.count != 2 {
		t.Fatalf("parser count = %d, want separate prepares for separate users", parser.count)
	}
}

func TestPlanningServicePrepareExecutionRequestComposesPrepareBindAndOptions(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)

	prepared, request := service.PrepareExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "req-1", MaxRows: 10},
		IndexedParameterValue(1, ValueInt, 42),
	)
	if prepared.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected prepared diagnostics: %#v", prepared.Diagnostics)
	}
	if !request.SupportedForExecution() {
		t.Fatalf("unexpected execution diagnostics: %#v", request.Diagnostics)
	}
	if request.Options.RequestID != "req-1" {
		t.Fatalf("request id = %q, want req-1", request.Options.RequestID)
	}
	if len(request.Bound.Parameters.Bindings) != 1 || request.Bound.Parameters.Bindings[0].Value.Value != 42 {
		t.Fatalf("bindings = %#v, want bound int value", request.Bound.Parameters.Bindings)
	}
}

func TestPlanningServiceAuthorizedExecutionRequestComposesAuthorization(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	service.Authorizer = denyingAuthorizer{}

	authorized := service.AuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, 7),
	)
	if authorized.Prepared.Diagnostics.BlocksNative() || !authorized.Request.SupportedForExecution() {
		t.Fatalf("unexpected prepare/execution diagnostics: %#v %#v", authorized.Prepared.Diagnostics, authorized.Request.Diagnostics)
	}
	if authorized.Authorization.Supported() {
		t.Fatalf("expected authorization to be denied")
	}
	if got := authorized.Authorization.Diagnostics.Codes()[0]; got != DiagnosticAccessDenied {
		t.Fatalf("diagnostic = %q, want access denied", got)
	}
}

func TestPlanningServicePrepareProtocolExecutionRequestNegotiatesProtocol(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)
	profile := NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityPreparedStatements,
		ProtocolCapabilityStreamingResults,
		ProtocolCapabilityForwardOnlyCursor,
	)

	protocol := service.PrepareProtocolExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		profile,
		ProtocolPreparedExecution,
		ExecutionOptions{RequestID: "proto-1", Streaming: true, Cursor: CursorForwardOnly},
		IndexedParameterValue(1, ValueInt, 7),
	)
	if !protocol.SupportedForProtocol() {
		t.Fatalf("diagnostics = %#v, want supported protocol request", protocol.Diagnostics())
	}
	if protocol.Protocol.Mode != ProtocolPreparedExecution {
		t.Fatalf("mode = %q, want prepared", protocol.Protocol.Mode)
	}
	if len(protocol.Request.Bound.Parameters.Bindings) != 1 || protocol.Request.Bound.Parameters.Bindings[0].Value.Value != 7 {
		t.Fatalf("bindings = %#v, want bound int value", protocol.Request.Bound.Parameters.Bindings)
	}
}

func TestPlanningServiceProtocolExecutionRequestReportsProtocolDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	prepared := PreparedPlan{Supported: true}
	profile := NewProtocolProfile(ProtocolHTTP, "http")

	protocol := service.ProtocolExecutionRequest(
		prepared,
		profile,
		ProtocolPreparedExecution,
		ExecutionOptions{Streaming: true},
	)
	if protocol.SupportedForProtocol() {
		t.Fatalf("expected protocol request to be unsupported")
	}
	if !protocol.Request.SupportedForExecution() {
		t.Fatalf("request diagnostics = %#v, expected execution request itself to remain valid", protocol.Request.Diagnostics)
	}
	codes := protocol.Diagnostics().Codes()
	if got := len(codes); got != 2 {
		t.Fatalf("diagnostic codes = %#v, want prepared and streaming protocol blockers", codes)
	}
	if !containsDiagnosticCode(codes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", codes)
	}
}

func TestPlanningServicePrepareRoutedExecutionRequestUsesCompatibilityRouting(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)

	routed := service.PrepareRoutedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "route-1"},
		IndexedParameterValue(1, ValueInt, 7),
	)
	if routed.Prepared.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected prepared diagnostics: %#v", routed.Prepared.Diagnostics)
	}
	if !routed.Request.SupportedForExecution() {
		t.Fatalf("unexpected execution diagnostics: %#v", routed.Request.Diagnostics)
	}
	if routed.Route.Kind != RouteNative || routed.Route.Reason != RouteReasonNativeSupported {
		t.Fatalf("route = %#v, want native supported", routed.Route)
	}
}

func TestPlanningServicePrepareBatchExecutionRequestComposesPrepareAndBinding(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)

	prepared, request := service.PrepareBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "batch-1"},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
		ParameterValues(IndexedParameterValue(1, ValueInt, 8)),
	)
	if prepared.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected prepared diagnostics: %#v", prepared.Diagnostics)
	}
	if !request.SupportedForExecution() {
		t.Fatalf("unexpected batch diagnostics: %#v", request.Diagnostics)
	}
	if len(request.ParameterSets) != 2 || request.ParameterSets[1].Bindings[0].Value.Value != 8 {
		t.Fatalf("parameter sets = %#v, want second bound value 8", request.ParameterSets)
	}
}

func TestPlanningServicePrepareProtocolBatchExecutionRequestNegotiatesBatch(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)
	profile := NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityBatchExecution)

	protocol := service.PrepareProtocolBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		profile,
		ExecutionOptions{RequestID: "proto-batch-1"},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
		ParameterValues(IndexedParameterValue(1, ValueInt, 8)),
	)
	if !protocol.SupportedForProtocol() {
		t.Fatalf("diagnostics = %#v, want supported batch protocol request", protocol.Diagnostics())
	}
	if protocol.Protocol.Mode != ProtocolBatchExecution {
		t.Fatalf("mode = %q, want batch", protocol.Protocol.Mode)
	}
	if len(protocol.Request.ParameterSets) != 2 {
		t.Fatalf("parameter sets = %#v, want two sets", protocol.Request.ParameterSets)
	}
}

func TestPlanningServiceAuthorizedBatchExecutionRequestComposesAuthorization(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	service.Authorizer = denyingAuthorizer{}

	authorized := service.AuthorizedBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
	)
	if authorized.Prepared.Diagnostics.BlocksNative() || !authorized.Request.SupportedForExecution() {
		t.Fatalf("unexpected prepare/batch diagnostics: %#v %#v", authorized.Prepared.Diagnostics, authorized.Request.Diagnostics)
	}
	if authorized.Authorization.Supported() {
		t.Fatalf("expected batch authorization to be denied")
	}
	if got := authorized.Authorization.Diagnostics.Codes()[0]; got != DiagnosticAccessDenied {
		t.Fatalf("diagnostic = %q, want access denied", got)
	}
}

func TestPlanningServicePrepareRoutedExecutionRequestHonorsNativeOnlyPolicy(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)
	service.Routing = NativeOnlyRoutingPolicy()

	routed := service.PrepareRoutedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueString, "bad"),
	)
	if routed.Route.Kind != RouteRejected {
		t.Fatalf("route = %#v, want rejected native-only route", routed.Route)
	}
	if !containsDiagnosticCode(routed.Route.Diagnostics.Codes(), DiagnosticRouteRejected) {
		t.Fatalf("route diagnostics = %#v, want route rejected", routed.Route.Diagnostics.Codes())
	}
}

func TestPlanningServicePrepareRoutedBatchExecutionRequestUsesCompatibilityRouting(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)

	routed := service.PrepareRoutedBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "batch-route-1"},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
		ParameterValues(IndexedParameterValue(1, ValueInt, 8)),
	)
	if routed.Prepared.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected prepared diagnostics: %#v", routed.Prepared.Diagnostics)
	}
	if !routed.Request.SupportedForExecution() {
		t.Fatalf("unexpected batch diagnostics: %#v", routed.Request.Diagnostics)
	}
	if routed.Route.Kind != RouteNative || routed.Route.Reason != RouteReasonNativeSupported {
		t.Fatalf("route = %#v, want native supported", routed.Route)
	}
}

func TestPlanningServiceRoutedExecutionRequestCanForceLegacy(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	service.Routing = LegacyOnlyRoutingPolicy()
	prepared := PreparedPlan{Supported: true}

	routed := service.RoutedExecutionRequest(prepared, ExecutionOptions{})
	if routed.Route.Kind != RouteLegacyFallback || routed.Route.Reason != RouteReasonLegacyForced {
		t.Fatalf("route = %#v, want forced legacy fallback", routed.Route)
	}
}

func TestPlanningServiceRoutedBatchExecutionRequestCanForceLegacy(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	service.Routing = LegacyOnlyRoutingPolicy()
	prepared := PreparedPlan{Supported: true}

	routed := service.RoutedBatchExecutionRequest(prepared, ExecutionOptions{}, ParameterValues())
	if routed.Route.Kind != RouteLegacyFallback || routed.Route.Reason != RouteReasonLegacyForced {
		t.Fatalf("route = %#v, want forced legacy fallback", routed.Route)
	}
}

func TestPlanningServiceDoesNotCacheBlockingPrepare(t *testing.T) {
	parser := &countingParserBridge{
		diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseParse, "bad sql"),
		},
	}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, NewMemoryPreparedPlanCache())

	first := service.Prepare("select")
	second := service.Prepare("select")
	if !first.Diagnostics.BlocksNative() || !second.Diagnostics.BlocksNative() {
		t.Fatalf("expected blocking diagnostics")
	}
	if parser.count != 2 {
		t.Fatalf("parser count = %d, want blocking prepares to avoid cache", parser.count)
	}
}

type countingParserBridge struct {
	statement   UnboundStatement
	diagnostics DiagnosticSet
	count       int
}

func (p *countingParserBridge) Parse(sql string) (UnboundStatement, DiagnosticSet) {
	p.count++
	statement := p.statement
	if statement.SQL == "" {
		statement.SQL = sql
	}
	return statement, p.diagnostics
}

func serviceSelectStatement() UnboundStatement {
	return UnboundStatement{
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables: []UnboundTable{{Name: "orders", Alias: "o"}},
			Projection: []UnboundProjection{{
				Expr:  UnboundField("o", "o_orderkey"),
				Alias: "order_id",
				Type:  DataTypeInt,
			}},
			Predicates: []UnboundPredicate{{
				Expr: UnboundBinary(
					BinaryOpEqual,
					UnboundField("o", "o_orderkey"),
					UnboundParameter(1, DataTypeInt),
				),
				Placement: PredicatePushdown,
				Scope:     PredicateScopeWhere,
			}},
			Result: ResultShape{Kind: ResultQuery},
		},
	}
}

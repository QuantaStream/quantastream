package qsbridge

import "testing"

func TestRoutedExecutionRequestFallbackRequestCarriesLegacyInputs(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session: SessionContext{
			ID:            "session-1",
			User:          "moli",
			Roles:         []RoleName{"reader"},
			CurrentSchema: "quanta",
		},
	}, nil)
	service.Routing = LegacyOnlyRoutingPolicy()

	routed := service.PrepareRoutedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "legacy-1", MaxRows: 10},
		IndexedParameterValue(1, ValueInt, 99),
	)
	fallback := routed.FallbackRequest()

	if !fallback.Supported() {
		t.Fatalf("fallback = %#v, want supported legacy handoff", fallback)
	}
	if fallback.Route.Kind != RouteLegacyFallback || fallback.Route.Reason != RouteReasonLegacyForced {
		t.Fatalf("route = %#v, want forced legacy fallback", fallback.Route)
	}
	if fallback.SQL != "select o_orderkey from orders where o_orderkey = ?" {
		t.Fatalf("sql = %q, want original sql", fallback.SQL)
	}
	if fallback.DefaultSchema != "quanta" || fallback.Session.User != "moli" {
		t.Fatalf("session/schema = %#v/%q, want copied context", fallback.Session, fallback.DefaultSchema)
	}
	if fallback.Options.RequestID != "legacy-1" || fallback.Options.MaxRows != 10 {
		t.Fatalf("options = %#v, want execution options", fallback.Options)
	}
	if len(fallback.Parameters) != 1 || fallback.Parameters[0].Value.Value != 99 {
		t.Fatalf("parameters = %#v, want bound int value", fallback.Parameters)
	}
}

func TestFallbackRequestCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session: SessionContext{
			Roles: []RoleName{"reader"},
		},
	}, nil)
	service.Routing = LegacyOnlyRoutingPolicy()

	routed := service.PrepareRoutedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, 7),
	)
	fallback := routed.FallbackRequest()
	fallback.Session.Roles[0] = "mutated"
	fallback.Parameters[0].Ref.Type = DataTypeString

	second := routed.FallbackRequest()
	if second.Session.Roles[0] != "reader" {
		t.Fatalf("session roles leaked mutation: %#v", second.Session.Roles)
	}
	if second.Parameters[0].Ref.Type != DataTypeInt {
		t.Fatalf("parameter bindings leaked mutation: %#v", second.Parameters)
	}
}

func TestFallbackRequestForNativeRouteIsNotSupported(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)

	routed := service.PrepareRoutedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, 7),
	)
	fallback := routed.FallbackRequest()
	if fallback.Supported() {
		t.Fatalf("fallback = %#v, want native route to avoid fallback support", fallback)
	}
	if fallback.Route.Kind != RouteNative {
		t.Fatalf("route = %#v, want native route", fallback.Route)
	}
}

func TestRoutedExecutionRequestNativeRequestReturnsCopiedNativeHandoff(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)

	routed := service.PrepareRoutedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, 7),
	)
	native, ok := routed.NativeRequest()
	if !ok {
		t.Fatalf("route = %#v, want native request", routed.Route)
	}
	native.ResultColumns[0].Name = "mutated"
	native.Bound.Parameters.Bindings[0].Ref.Type = DataTypeString

	second, ok := routed.NativeRequest()
	if !ok {
		t.Fatalf("route = %#v, want second native request", routed.Route)
	}
	if second.ResultColumns[0].Name != "order_id" {
		t.Fatalf("native request leaked result column mutation: %#v", second.ResultColumns)
	}
	if second.Bound.Parameters.Bindings[0].Ref.Type != DataTypeInt {
		t.Fatalf("native request leaked binding mutation: %#v", second.Bound.Parameters.Bindings)
	}
	if _, ok := routed.LegacyFallbackRequest(); ok {
		t.Fatalf("native route unexpectedly returned fallback handoff")
	}
}

func TestRoutedExecutionRequestLegacyFallbackRequestReturnsOnlyForFallback(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)
	service.Routing = LegacyOnlyRoutingPolicy()

	routed := service.PrepareRoutedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, 7),
	)
	fallback, ok := routed.LegacyFallbackRequest()
	if !ok || !fallback.Supported() {
		t.Fatalf("fallback = %#v ok=%v, want legacy handoff", fallback, ok)
	}
	if _, ok := routed.NativeRequest(); ok {
		t.Fatalf("legacy route unexpectedly returned native request")
	}
}

func TestRoutedBatchExecutionRequestLegacyFallbackRequestCarriesParameterSets(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{Roles: []RoleName{"writer"}},
	}, nil)
	service.Routing = LegacyOnlyRoutingPolicy()

	routed := service.PrepareRoutedBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "batch-legacy-1"},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
		ParameterValues(IndexedParameterValue(1, ValueInt, 8)),
	)
	fallback, ok := routed.LegacyFallbackRequest()
	if !ok || !fallback.Supported() {
		t.Fatalf("fallback = %#v ok=%v, want legacy batch handoff", fallback, ok)
	}
	if fallback.Options.RequestID != "batch-legacy-1" || len(fallback.ParameterSets) != 2 {
		t.Fatalf("fallback = %#v, want copied options and two parameter sets", fallback)
	}
	fallback.Session.Roles[0] = "mutated"
	fallback.ParameterSets[1].Bindings[0].Ref.Type = DataTypeString
	second, ok := routed.LegacyFallbackRequest()
	if !ok {
		t.Fatalf("expected second batch fallback")
	}
	if second.Session.Roles[0] != "writer" {
		t.Fatalf("fallback leaked session mutation: %#v", second.Session.Roles)
	}
	if second.ParameterSets[1].Bindings[0].Ref.Type != DataTypeInt {
		t.Fatalf("fallback leaked parameter set mutation: %#v", second.ParameterSets)
	}
	if _, ok := routed.NativeRequest(); ok {
		t.Fatalf("legacy route unexpectedly returned native batch request")
	}
}

func TestFallbackRequestCarriesRejectedRouteDiagnostics(t *testing.T) {
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
	fallback := routed.FallbackRequest()
	if fallback.Supported() {
		t.Fatalf("fallback = %#v, want rejected route to avoid fallback support", fallback)
	}
	if fallback.Route.Kind != RouteRejected {
		t.Fatalf("route = %#v, want rejected route", fallback.Route)
	}
	if !containsDiagnosticCode(fallback.Diagnostics.Codes(), DiagnosticRouteRejected) {
		t.Fatalf("diagnostics = %#v, want route rejected", fallback.Diagnostics.Codes())
	}
	if routed.Supported() {
		t.Fatalf("route = %#v, want rejected routed request to be unsupported", routed.Route)
	}
}

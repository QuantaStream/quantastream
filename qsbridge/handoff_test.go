package qsbridge

import "testing"

func TestRoutedAuthorizedExecutionRequestSelectsNativeWhenRoutedAndAuthorized(t *testing.T) {
	service := handoffPlanningService()

	handoff := service.PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "native-1"},
		IndexedParameterValue(1, ValueInt, 7),
	)
	if !handoff.Supported() {
		t.Fatalf("handoff = %#v, want supported native handoff", handoff)
	}
	if handoff.HandoffKind() != ExecutionHandoffNative {
		t.Fatalf("handoff kind = %q, want native", handoff.HandoffKind())
	}
	native, ok := handoff.NativeRequest()
	if !ok {
		t.Fatalf("expected native request")
	}
	if native.Options.RequestID != "native-1" {
		t.Fatalf("request id = %q, want native-1", native.Options.RequestID)
	}
	if _, ok := handoff.LegacyFallbackRequest(); ok {
		t.Fatalf("did not expect fallback request")
	}
}

func TestRoutedAuthorizedExecutionRequestSelectsFallbackWhenNativeRoutingDisabled(t *testing.T) {
	service := handoffPlanningService()
	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}

	handoff := service.PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "fallback-1"},
		IndexedParameterValue(1, ValueInt, 7),
	)
	if handoff.HandoffKind() != ExecutionHandoffLegacyFallback {
		t.Fatalf("handoff kind = %q, want legacy fallback", handoff.HandoffKind())
	}
	fallback, ok := handoff.LegacyFallbackRequest()
	if !ok {
		t.Fatalf("expected fallback request")
	}
	if fallback.Route.Reason != RouteReasonNativeDisabled {
		t.Fatalf("fallback route = %#v, want native disabled", fallback.Route)
	}
	if fallback.SQL == "" || fallback.Options.RequestID != "fallback-1" {
		t.Fatalf("fallback = %#v, want SQL and options copied", fallback)
	}
	if _, ok := handoff.NativeRequest(); ok {
		t.Fatalf("did not expect native request")
	}
}

func TestRoutedAuthorizedExecutionRequestDeniesBeforeHandoff(t *testing.T) {
	service := handoffPlanningService()
	service.Authorizer = denyingAuthorizer{}
	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}

	handoff := service.PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, 7),
	)
	if handoff.Supported() {
		t.Fatalf("expected denied handoff to be unsupported")
	}
	if handoff.HandoffKind() != ExecutionHandoffDenied {
		t.Fatalf("handoff kind = %q, want denied", handoff.HandoffKind())
	}
	if !containsDiagnosticCode(handoff.Diagnostics().Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", handoff.Diagnostics().Codes())
	}
	if _, ok := handoff.NativeRequest(); ok {
		t.Fatalf("did not expect native request")
	}
	if _, ok := handoff.LegacyFallbackRequest(); ok {
		t.Fatalf("did not expect fallback request")
	}
	outcome := handoff.Outcome()
	if outcome.Kind != ExecutionHandoffDenied || outcome.Supported || outcome.AccessIntent != PhysicalAccessRead || outcome.Lifecycle != ClientPlanLifecycleSelect || outcome.LifecycleSteps != 7 {
		t.Fatalf("outcome = %#v, want denied unsupported", outcome)
	}
	protocol, ok := outcome.FirstProtocolError()
	if !ok {
		t.Fatalf("expected denied protocol error")
	}
	if protocol.SQLState != SQLStateSyntaxError || protocol.VendorCode != mysqlErrorAccessDenied {
		t.Fatalf("protocol = %#v, want access denied mapping", protocol)
	}
}

func TestRoutedAuthorizedExecutionRequestRejectsNativeOnlyPolicy(t *testing.T) {
	service := handoffPlanningService()
	service.Routing = NativeOnlyRoutingPolicy()

	handoff := service.PrepareRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueString, "bad"),
	)
	if handoff.HandoffKind() != ExecutionHandoffRejected {
		t.Fatalf("handoff kind = %q, want rejected", handoff.HandoffKind())
	}
	if handoff.Supported() {
		t.Fatalf("expected rejected handoff to be unsupported")
	}
	if !containsDiagnosticCode(handoff.Diagnostics().Codes(), DiagnosticRouteRejected) {
		t.Fatalf("diagnostics = %#v, want route rejected", handoff.Diagnostics().Codes())
	}
	if len(handoff.ProtocolErrors()) == 0 {
		t.Fatalf("expected rejected handoff protocol errors")
	}
	outcome := handoff.Outcome()
	if outcome.Kind != ExecutionHandoffRejected || outcome.Supported || outcome.AccessIntent != PhysicalAccessRead || outcome.Lifecycle != ClientPlanLifecycleSelect || outcome.LifecycleSteps != 7 {
		t.Fatalf("outcome = %#v, want rejected unsupported", outcome)
	}
}

func TestRoutedAuthorizedExecutionRequestOutcomeCarriesWriteIntent(t *testing.T) {
	service := handoffPlanningService()
	handoff := service.RoutedAuthorizedExecutionRequest(PreparedPlan{
		Kind:      QueryKindUpdate,
		Supported: true,
		Result:    ResultShape{Kind: ResultStatement},
		Statement: StatementResult{AffectedRows: 1},
	}, ExecutionOptions{})

	outcome := handoff.Outcome()
	if outcome.AccessIntent != PhysicalAccessWrite || outcome.Lifecycle != ClientPlanLifecycleMutation || outcome.LifecycleSteps != 7 {
		t.Fatalf("outcome = %#v, want write intent and mutation lifecycle", outcome)
	}
}

func TestProtocolRoutedAuthorizedExecutionRequestSelectsNative(t *testing.T) {
	service := handoffPlanningService()
	profile := NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)

	handoff := service.PrepareProtocolRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		profile,
		ProtocolPreparedExecution,
		ExecutionOptions{RequestID: "protocol-native-1"},
		IndexedParameterValue(1, ValueInt, 7),
	)
	if !handoff.Supported() {
		t.Fatalf("diagnostics = %#v, want supported protocol handoff", handoff.Diagnostics())
	}
	if handoff.HandoffKind() != ExecutionHandoffNative {
		t.Fatalf("handoff kind = %q, want native", handoff.HandoffKind())
	}
	native, ok := handoff.NativeRequest()
	if !ok {
		t.Fatalf("expected native request")
	}
	if native.Options.RequestID != "protocol-native-1" {
		t.Fatalf("request id = %q, want protocol-native-1", native.Options.RequestID)
	}
}

func TestProtocolRoutedAuthorizedExecutionRequestRejectsUnsupportedProtocolBeforeRoute(t *testing.T) {
	service := handoffPlanningService()
	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}
	profile := NewProtocolProfile(ProtocolHTTP, "http")

	handoff := service.PrepareProtocolRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		profile,
		ProtocolPreparedExecution,
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, 7),
	)
	if handoff.HandoffKind() != ExecutionHandoffProtocolRejected {
		t.Fatalf("handoff kind = %q, want protocol rejected", handoff.HandoffKind())
	}
	if handoff.Supported() {
		t.Fatalf("expected protocol handoff to be unsupported")
	}
	if !containsDiagnosticCode(handoff.Diagnostics().Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want protocol execution option diagnostic", handoff.Diagnostics().Codes())
	}
	if _, ok := handoff.LegacyFallbackRequest(); ok {
		t.Fatalf("did not expect fallback while protocol is rejected")
	}
	outcome := handoff.Outcome()
	if outcome.Kind != ExecutionHandoffProtocolRejected || outcome.Supported || outcome.AccessIntent != PhysicalAccessRead || outcome.Lifecycle != ClientPlanLifecycleSelect || outcome.LifecycleSteps != 7 {
		t.Fatalf("outcome = %#v, want protocol rejected unsupported", outcome)
	}
}

func TestProtocolRoutedAuthorizedExecutionRequestDeniesBeforeProtocol(t *testing.T) {
	service := handoffPlanningService()
	service.Authorizer = denyingAuthorizer{}
	profile := NewProtocolProfile(ProtocolHTTP, "http")

	handoff := service.PrepareProtocolRoutedAuthorizedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		profile,
		ProtocolPreparedExecution,
		ExecutionOptions{},
		IndexedParameterValue(1, ValueInt, 7),
	)
	if handoff.HandoffKind() != ExecutionHandoffDenied {
		t.Fatalf("handoff kind = %q, want denied", handoff.HandoffKind())
	}
	if !containsDiagnosticCode(handoff.Diagnostics().Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", handoff.Diagnostics().Codes())
	}
	if len(handoff.ProtocolErrors()) == 0 {
		t.Fatalf("expected protocol-aware denial errors")
	}
}

func TestRoutedAuthorizedBatchExecutionRequestSelectsNativeWhenRoutedAndAuthorized(t *testing.T) {
	service := handoffPlanningService()

	handoff := service.PrepareRoutedAuthorizedBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "batch-native-1", BatchSize: 2},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
		ParameterValues(IndexedParameterValue(1, ValueInt, 8)),
	)
	if !handoff.Supported() {
		t.Fatalf("handoff = %#v, want supported native batch handoff", handoff)
	}
	if handoff.HandoffKind() != ExecutionHandoffNative {
		t.Fatalf("handoff kind = %q, want native", handoff.HandoffKind())
	}
	native, ok := handoff.NativeRequest()
	if !ok {
		t.Fatalf("expected native batch request")
	}
	if native.Options.RequestID != "batch-native-1" || len(native.ParameterSets) != 2 {
		t.Fatalf("native batch = %#v, want copied options and two sets", native)
	}
	native.ParameterSets[0].Bindings[0].Ref.Type = DataTypeString
	second, ok := handoff.NativeRequest()
	if !ok {
		t.Fatalf("expected second native batch request")
	}
	if second.ParameterSets[0].Bindings[0].Ref.Type != DataTypeInt {
		t.Fatalf("native batch leaked mutable parameter set: %#v", second.ParameterSets)
	}
}

func TestRoutedAuthorizedBatchExecutionRequestSelectsFallbackWhenNativeRoutingDisabled(t *testing.T) {
	service := handoffPlanningService()
	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}

	handoff := service.PrepareRoutedAuthorizedBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "batch-fallback-1"},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
		ParameterValues(IndexedParameterValue(1, ValueInt, 8)),
	)
	if handoff.HandoffKind() != ExecutionHandoffLegacyFallback {
		t.Fatalf("handoff kind = %q, want legacy fallback", handoff.HandoffKind())
	}
	fallback, ok := handoff.LegacyFallbackRequest()
	if !ok {
		t.Fatalf("expected fallback batch request")
	}
	if fallback.Route.Reason != RouteReasonNativeDisabled || len(fallback.ParameterSets) != 2 {
		t.Fatalf("fallback = %#v, want native-disabled route and two sets", fallback)
	}
	fallback.ParameterSets[1].Bindings[0].Ref.Type = DataTypeString
	second, ok := handoff.LegacyFallbackRequest()
	if !ok {
		t.Fatalf("expected second fallback batch request")
	}
	if second.ParameterSets[1].Bindings[0].Ref.Type != DataTypeInt {
		t.Fatalf("fallback batch leaked mutable parameter set: %#v", second.ParameterSets)
	}
	if _, ok := handoff.NativeRequest(); ok {
		t.Fatalf("did not expect native batch request")
	}
}

func TestRoutedAuthorizedBatchExecutionRequestDeniesBeforeHandoff(t *testing.T) {
	service := handoffPlanningService()
	service.Authorizer = denyingAuthorizer{}
	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}

	handoff := service.PrepareRoutedAuthorizedBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
	)
	if handoff.Supported() {
		t.Fatalf("expected denied batch handoff to be unsupported")
	}
	if handoff.HandoffKind() != ExecutionHandoffDenied {
		t.Fatalf("handoff kind = %q, want denied", handoff.HandoffKind())
	}
	if !containsDiagnosticCode(handoff.Diagnostics().Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", handoff.Diagnostics().Codes())
	}
	if _, ok := handoff.NativeRequest(); ok {
		t.Fatalf("did not expect native batch request")
	}
	if _, ok := handoff.LegacyFallbackRequest(); ok {
		t.Fatalf("did not expect fallback batch request")
	}
	protocol, ok := handoff.FirstProtocolError()
	if !ok {
		t.Fatalf("expected denied batch protocol error")
	}
	if protocol.SQLState != SQLStateSyntaxError || protocol.VendorCode != mysqlErrorAccessDenied {
		t.Fatalf("protocol = %#v, want access denied mapping", protocol)
	}
}

func TestRoutedAuthorizedBatchExecutionRequestRejectsNativeOnlyPolicy(t *testing.T) {
	service := handoffPlanningService()
	service.Routing = NativeOnlyRoutingPolicy()

	handoff := service.PrepareRoutedAuthorizedBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		ParameterValues(IndexedParameterValue(1, ValueString, "bad")),
	)
	if handoff.HandoffKind() != ExecutionHandoffRejected {
		t.Fatalf("handoff kind = %q, want rejected", handoff.HandoffKind())
	}
	if handoff.Supported() {
		t.Fatalf("expected rejected batch handoff to be unsupported")
	}
	if !containsDiagnosticCode(handoff.Diagnostics().Codes(), DiagnosticRouteRejected) {
		t.Fatalf("diagnostics = %#v, want route rejected", handoff.Diagnostics().Codes())
	}
}

func TestProtocolRoutedAuthorizedBatchExecutionRequestSelectsNative(t *testing.T) {
	service := handoffPlanningService()
	profile := NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityBatchExecution)

	handoff := service.PrepareProtocolRoutedAuthorizedBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		profile,
		ExecutionOptions{RequestID: "protocol-batch-native-1"},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
	)
	if !handoff.Supported() {
		t.Fatalf("diagnostics = %#v, want supported protocol batch handoff", handoff.Diagnostics())
	}
	if handoff.HandoffKind() != ExecutionHandoffNative {
		t.Fatalf("handoff kind = %q, want native", handoff.HandoffKind())
	}
	native, ok := handoff.NativeRequest()
	if !ok {
		t.Fatalf("expected native batch request")
	}
	if native.Options.RequestID != "protocol-batch-native-1" {
		t.Fatalf("request id = %q, want protocol-batch-native-1", native.Options.RequestID)
	}
}

func TestProtocolRoutedAuthorizedBatchExecutionRequestRejectsUnsupportedProtocol(t *testing.T) {
	service := handoffPlanningService()
	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}
	profile := NewProtocolProfile(ProtocolHTTP, "http")

	handoff := service.PrepareProtocolRoutedAuthorizedBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		profile,
		ExecutionOptions{},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
	)
	if handoff.HandoffKind() != ExecutionHandoffProtocolRejected {
		t.Fatalf("handoff kind = %q, want protocol rejected", handoff.HandoffKind())
	}
	if handoff.Supported() {
		t.Fatalf("expected protocol batch handoff to be unsupported")
	}
	if !containsDiagnosticCode(handoff.Diagnostics().Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want protocol execution option diagnostic", handoff.Diagnostics().Codes())
	}
	if _, ok := handoff.LegacyFallbackRequest(); ok {
		t.Fatalf("did not expect fallback while protocol is rejected")
	}
}

func TestRoutedAuthorizedBatchExecutionRequestOutcomeMapsInvalidParameters(t *testing.T) {
	service := handoffPlanningService()

	handoff := service.PrepareRoutedAuthorizedBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		ParameterValues(IndexedParameterValue(1, ValueString, "bad")),
	)
	outcome := handoff.Outcome()
	if outcome.Kind != ExecutionHandoffLegacyFallback || !outcome.Supported || outcome.AccessIntent != PhysicalAccessRead || outcome.Lifecycle != ClientPlanLifecycleSelect || outcome.LifecycleSteps != 7 {
		t.Fatalf("outcome = %#v, want compatible fallback for invalid batch values", outcome)
	}
	protocol, ok := outcome.FirstProtocolError()
	if !ok {
		t.Fatalf("expected invalid-parameter protocol error")
	}
	if protocol.SQLState != SQLStateInvalidParameter || protocol.VendorCode != mysqlErrorInvalidParameter {
		t.Fatalf("protocol = %#v, want invalid parameter mapping", protocol)
	}
}

func handoffPlanningService() PlanningService {
	return NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
}

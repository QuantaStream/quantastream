package qsbridge

import "testing"

func TestPlanningServicePrepareClientStatementHandoffBundleRoutesEachStatement(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityMultiStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1", "select 2")

	handoff := service.PrepareClientStatementHandoffBundle(bundle, ClientHandoffOptions{
		Mode:    ProtocolSimpleExecution,
		Options: ExecutionOptions{MaxRows: 10},
		Values:  []ParameterValue{IndexedParameterValue(1, ValueInt, 7)},
		Statements: []ClientStatementExecutionOptions{{
			Ordinal: 1,
			Options: ExecutionOptions{
				RequestID: "second",
				MaxRows:   5,
			},
		}},
	})
	if !handoff.Supported() {
		t.Fatalf("diagnostics = %#v, want supported handoff bundle", handoff.Diagnostics)
	}
	if parser.count != 2 {
		t.Fatalf("parser count = %d, want one prepare per statement", parser.count)
	}
	if len(handoff.Statements) != 2 {
		t.Fatalf("statements = %#v, want two handoffs", handoff.Statements)
	}
	if handoff.Statements[0].Handoff.HandoffKind() != ExecutionHandoffNative || handoff.Statements[1].Handoff.HandoffKind() != ExecutionHandoffNative {
		t.Fatalf("handoff kinds = %q/%q, want native", handoff.Statements[0].Handoff.HandoffKind(), handoff.Statements[1].Handoff.HandoffKind())
	}
	first, ok := handoff.Statements[0].Handoff.NativeRequest()
	if !ok {
		t.Fatalf("expected first native request")
	}
	second, ok := handoff.Statements[1].Handoff.NativeRequest()
	if !ok {
		t.Fatalf("expected second native request")
	}
	if first.Options.MaxRows != 10 {
		t.Fatalf("first options = %#v, want default max rows", first.Options)
	}
	if second.Options.RequestID != "second" || second.Options.MaxRows != 5 {
		t.Fatalf("second options = %#v, want per-statement override", second.Options)
	}
}

func TestPlanningServicePrepareClientStatementHandoffBundleSkipsUnsupportedBundle(t *testing.T) {
	parser := &countingParserBridge{statement: serviceSelectStatement()}
	service := NewPlanningService(Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)
	bundle := NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1", "select 2")

	handoff := service.PrepareClientStatementHandoffBundle(bundle, ClientHandoffOptions{})
	if handoff.Supported() {
		t.Fatalf("expected unsupported bundle handoff")
	}
	if parser.count != 0 {
		t.Fatalf("parser count = %d, want unsupported bundle to avoid planning", parser.count)
	}
	if len(handoff.Statements) != 0 {
		t.Fatalf("statements = %#v, want none", handoff.Statements)
	}
	if !containsDiagnosticCode(handoff.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", handoff.Diagnostics)
	}
}

func TestPlanningServicePrepareClientStatementHandoffBundlePreservesFallback(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}
	bundle := NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1")

	handoff := service.PrepareClientStatementHandoffBundle(bundle, ClientHandoffOptions{})
	if !handoff.Supported() {
		t.Fatalf("diagnostics = %#v, want compatibility fallback to be supported", handoff.Diagnostics)
	}
	if handoff.Statements[0].Handoff.HandoffKind() != ExecutionHandoffLegacyFallback {
		t.Fatalf("handoff kind = %q, want fallback", handoff.Statements[0].Handoff.HandoffKind())
	}
	fallback, ok := handoff.Statements[0].Handoff.LegacyFallbackRequest()
	if !ok {
		t.Fatalf("expected fallback request")
	}
	if fallback.Route.Reason != RouteReasonNativeDisabled || fallback.SQL == "" {
		t.Fatalf("fallback = %#v, want native-disabled SQL handoff", fallback)
	}
}

func TestPlanningServicePrepareClientStatementHandoffBundleDeniesBeforeProtocol(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	service.Authorizer = denyingAuthorizer{}
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolHTTP, "http")
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1")

	handoff := service.PrepareClientStatementHandoffBundle(bundle, ClientHandoffOptions{
		Mode: ProtocolPreparedExecution,
	})
	if handoff.Supported() {
		t.Fatalf("expected denied bundle handoff")
	}
	if got := handoff.Statements[0].Handoff.HandoffKind(); got != ExecutionHandoffDenied {
		t.Fatalf("handoff kind = %q, want denied", got)
	}
	if !containsDiagnosticCode(handoff.Diagnostics.Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", handoff.Diagnostics.Codes())
	}
}

func TestClientHandoffBundleCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1")
	options := ClientHandoffOptions{
		Statements: []ClientStatementExecutionOptions{{
			Ordinal: 0,
			Values:  []ParameterValue{IndexedParameterValue(1, ValueInt, 7)},
		}},
	}

	handoff := service.PrepareClientStatementHandoffBundle(bundle, options)
	handoff.Connection.Attributes["client"] = "mutated"
	handoff.Statements[0].Options.Values[0].Kind = ValueString
	native, ok := handoff.Statements[0].Handoff.NativeRequest()
	if !ok {
		t.Fatalf("expected native request")
	}
	native.Bound.Parameters.Bindings[0].Value.Kind = ValueString

	again := service.PrepareClientStatementHandoffBundle(bundle, options)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Statements[0].Options.Values[0].Kind != ValueInt {
		t.Fatalf("statement values leaked mutation: %#v", again.Statements[0].Options.Values)
	}
	second, ok := again.Statements[0].Handoff.NativeRequest()
	if !ok {
		t.Fatalf("expected second native request")
	}
	if second.Bound.Parameters.Bindings[0].Value.Kind != ValueInt {
		t.Fatalf("native request leaked mutation: %#v", second.Bound.Parameters.Bindings)
	}
}

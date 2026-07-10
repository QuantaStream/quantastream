package qsbridge

import "testing"

func TestPlanningServiceListClientRouteDecisionsReturnsNativeRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, clientRouteDecisionOptions())

	exchange := service.ListClientRouteDecisions(handoff)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported route decision rows", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Handoff != ExecutionHandoffNative {
		t.Fatalf("rows = %#v, want one native handoff row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.Ordinal != 0 || row.SQL != "select 1" || !row.Supported {
		t.Fatalf("row = %#v, want statement identity and supported flag", row)
	}
	if row.AccessIntent != PhysicalAccessRead || row.Lifecycle != ClientPlanLifecycleSelect || row.LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want read select lifecycle", row)
	}
	if row.Route != RouteNative || row.RouteReason != RouteReasonNativeSupported || !row.NativeEligible {
		t.Fatalf("row = %#v, want native route decision", row)
	}
	if row.ProtocolMode != ProtocolSimpleExecution || !row.ProtocolSupported || !row.Authorized {
		t.Fatalf("row = %#v, want protocol and authorization success", row)
	}
	if len(exchange.ResultSchema.Columns) != 14 || exchange.ResultSchema.Columns[0].Name != "Ordinal" || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want route decision result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[2].Value != string(ExecutionHandoffNative) || resultRow[4].Value != string(PhysicalAccessRead) || resultRow[5].Value != string(ClientPlanLifecycleSelect) || resultRow[6].Value != 7 || resultRow[7].Value != string(RouteNative) {
		t.Fatalf("result row = %#v, want native handoff, lifecycle, and route values", resultRow)
	}
}

func TestPlanningServiceListClientRouteDecisionsReportsFallback(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}
	bundle := NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, clientRouteDecisionOptions())

	exchange := service.ListClientRouteDecisions(handoff)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want fallback route rows to be supported metadata", exchange)
	}
	row := exchange.Rows[0]
	if row.Handoff != ExecutionHandoffLegacyFallback || row.Route != RouteLegacyFallback || row.RouteReason != RouteReasonNativeDisabled {
		t.Fatalf("row = %#v, want native-disabled fallback decision", row)
	}
	if row.NativeEligible {
		t.Fatalf("row = %#v, want fallback row to be non-native eligible", row)
	}
}

func TestPlanningServiceListClientRouteDecisionsReportsProtocolAndAccessFailuresAsRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolHTTP, "http")
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1")
	options := clientRouteDecisionOptions()
	options.Mode = ProtocolPreparedExecution
	protocolHandoff := service.PrepareClientStatementHandoffBundle(bundle, options)

	protocolExchange := service.ListClientRouteDecisions(protocolHandoff)
	if !protocolExchange.Supported() {
		t.Fatalf("exchange = %#v, protocol failure should be report data", protocolExchange)
	}
	protocolRow := protocolExchange.Rows[0]
	if protocolRow.Handoff != ExecutionHandoffProtocolRejected || protocolRow.ProtocolSupported || protocolRow.Supported {
		t.Fatalf("row = %#v, want protocol rejection reported as row data", protocolRow)
	}
	if !containsDiagnosticCode(protocolRow.Diagnostics, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want protocol capability diagnostic", protocolRow.Diagnostics)
	}

	deniedService := service
	deniedService.Authorizer = denyingAuthorizer{}
	deniedHandoff := deniedService.PrepareClientStatementHandoffBundle(bundle, clientRouteDecisionOptions())
	deniedExchange := deniedService.ListClientRouteDecisions(deniedHandoff)
	if !deniedExchange.Supported() {
		t.Fatalf("exchange = %#v, access failure should be report data", deniedExchange)
	}
	deniedRow := deniedExchange.Rows[0]
	if deniedRow.Handoff != ExecutionHandoffDenied || deniedRow.Authorized || deniedRow.Supported {
		t.Fatalf("row = %#v, want access denial reported as row data", deniedRow)
	}
	if !containsDiagnosticCode(deniedRow.Diagnostics, DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied diagnostic", deniedRow.Diagnostics)
	}
}

func TestPlanningServiceListClientRouteDecisionsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, clientRouteDecisionOptions())

	exchange := service.ListClientRouteDecisions(handoff)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block exchange", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientRouteDecisionsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, clientRouteDecisionOptions())

	exchange := service.ListClientRouteDecisions(handoff)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Diagnostics = append(exchange.Diagnostics, ErrorDiagnostic(DiagnosticRouteRejected, PhasePlan, "mutated"))
	exchange.Rows[0].SQL = "mutated"
	exchange.Rows[0].Diagnostics = append(exchange.Rows[0].Diagnostics, DiagnosticRouteRejected)
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.ListClientRouteDecisions(handoff)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if len(again.Diagnostics) != len(handoff.Diagnostics) {
		t.Fatalf("diagnostics leaked mutation: %#v", again.Diagnostics)
	}
	if again.Rows[0].SQL != "select 1" || containsDiagnosticCode(again.Rows[0].Diagnostics, DiagnosticRouteRejected) {
		t.Fatalf("row leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Ordinal" || again.ResultSchema.Columns[0].Name != "Ordinal" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != "select 1" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func clientRouteDecisionOptions() ClientHandoffOptions {
	return ClientHandoffOptions{
		Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)},
	}
}

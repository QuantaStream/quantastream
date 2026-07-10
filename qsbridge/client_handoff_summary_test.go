package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientHandoffBundleCountsOutcomes(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityMultiStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1", "select 2")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, clientHandoffSummaryOptions())

	exchange := service.SummarizeClientHandoffBundle(handoff)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported handoff summary", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one summary row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if !row.Supported || row.StatementCount != 2 || row.NativeCount != 2 {
		t.Fatalf("row = %#v, want two native handoffs", row)
	}
	if row.ReadCount != 2 || row.WriteCount != 0 || row.SelectLifecycleCount != 2 || row.MutationLifecycleCount != 0 {
		t.Fatalf("row = %#v, want two read SELECT lifecycle statements", row)
	}
	if row.LegacyFallbackCount != 0 || row.RejectedCount != 0 || row.DeniedCount != 0 || row.ProtocolRejectedCount != 0 {
		t.Fatalf("row = %#v, want no non-native outcomes", row)
	}
	if len(exchange.ResultSchema.Columns) != 15 || exchange.ResultSchema.Columns[0].Name != "User" {
		t.Fatalf("schema = %#v, want handoff summary columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 1 || exchange.Result.Chunks[0].Rows[0][5].Value != 2 || exchange.Result.Chunks[0].Rows[0][9].Value != 2 {
		t.Fatalf("result = %#v, want native count row", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientHandoffBundleCountsWriteLifecycle(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	update := PreparedPlan{
		Kind:      QueryKindUpdate,
		Supported: true,
	}
	handoff := ClientHandoffBundle{
		Connection: connection,
		Statements: []ClientStatementHandoff{{
			Statement: ClientStatementText{Ordinal: 0, SQL: "update orders set o_totalprice = ? where o_orderkey = ?"},
			Plan: ClientStatementPlan{
				Statement: ClientStatementText{Ordinal: 0, SQL: "update orders set o_totalprice = ? where o_orderkey = ?"},
				Prepared:  update,
			},
			Handoff: ProtocolRoutedAuthorizedExecutionRequest{
				Prepared:      update,
				Route:         RouteDecision{Kind: RouteNative},
				Authorization: AuthorizationDecision{Allowed: true},
			},
		}},
	}

	exchange := service.SummarizeClientHandoffBundle(handoff)
	if !exchange.Supported() || len(exchange.Rows) != 1 {
		t.Fatalf("exchange = %#v, want supported write handoff summary", exchange)
	}
	row := exchange.Rows[0]
	if row.ReadCount != 0 || row.WriteCount != 1 || row.SelectLifecycleCount != 0 || row.MutationLifecycleCount != 1 {
		t.Fatalf("row = %#v, want one write mutation lifecycle statement", row)
	}
	if row.NativeCount != 1 || row.StatementCount != 1 {
		t.Fatalf("row = %#v, want one native statement", row)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[6].Value != 1 || resultRow[8].Value != 1 || resultRow[9].Value != 1 {
		t.Fatalf("result row = %#v, want write, mutation, and native counts", resultRow)
	}
}

func TestPlanningServiceSummarizeClientHandoffBundleCountsFallbackAndFailures(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}
	fallback := service.PrepareClientStatementHandoffBundle(
		NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1"),
		clientHandoffSummaryOptions(),
	)

	fallbackExchange := service.SummarizeClientHandoffBundle(fallback)
	if !fallbackExchange.Supported() {
		t.Fatalf("fallback exchange = %#v, want supported metadata", fallbackExchange)
	}
	if row := fallbackExchange.Rows[0]; !row.Supported || row.LegacyFallbackCount != 1 || row.NativeCount != 0 {
		t.Fatalf("fallback row = %#v, want one legacy fallback", row)
	}
	if row := fallbackExchange.Rows[0]; row.ReadCount != 1 || row.SelectLifecycleCount != 1 {
		t.Fatalf("fallback row = %#v, want read SELECT lifecycle count", row)
	}

	protocolService := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolHTTP, "http")
	protocolRejected := protocolService.PrepareClientStatementHandoffBundle(
		NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1"),
		ClientHandoffOptions{Mode: ProtocolPreparedExecution},
	)
	protocolExchange := protocolService.SummarizeClientHandoffBundle(protocolRejected)
	if row := protocolExchange.Rows[0]; row.Supported || row.ProtocolRejectedCount != 1 {
		t.Fatalf("protocol row = %#v, want one protocol rejection", row)
	}
	if !containsDiagnosticCode(protocolExchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want protocol diagnostic", protocolExchange.Rows[0].DiagnosticCodes)
	}

	deniedService := protocolService
	deniedService.Authorizer = denyingAuthorizer{}
	denied := deniedService.PrepareClientStatementHandoffBundle(
		NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1"),
		clientHandoffSummaryOptions(),
	)
	deniedExchange := deniedService.SummarizeClientHandoffBundle(denied)
	if row := deniedExchange.Rows[0]; row.Supported || row.DeniedCount != 1 {
		t.Fatalf("denied row = %#v, want one access denial", row)
	}
}

func TestPlanningServiceSummarizeClientHandoffBundleReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}
	handoff := service.PrepareClientStatementHandoffBundle(
		NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1"),
		clientHandoffSummaryOptions(),
	)

	exchange := service.SummarizeClientHandoffBundle(handoff)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientHandoffBundleCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	handoff := service.PrepareClientStatementHandoffBundle(
		NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1"),
		clientHandoffSummaryOptions(),
	)

	exchange := service.SummarizeClientHandoffBundle(handoff)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Bundle.Statements[0].Statement.SQL = "mutated"
	exchange.Rows[0].DiagnosticCodes = append(exchange.Rows[0].DiagnosticCodes, DiagnosticRouteRejected)
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][9].Value = "mutated"

	again := service.SummarizeClientHandoffBundle(handoff)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Bundle.Statements[0].Statement.SQL != "select 1" {
		t.Fatalf("bundle leaked mutation: %#v", again.Bundle.Statements[0].Statement)
	}
	if containsDiagnosticCode(again.Rows[0].DiagnosticCodes, DiagnosticRouteRejected) {
		t.Fatalf("row diagnostics leaked mutation: %#v", again.Rows[0].DiagnosticCodes)
	}
	if again.Result.Columns[0].Name != "User" || again.ResultSchema.Columns[0].Name != "User" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][9].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func clientHandoffSummaryOptions() ClientHandoffOptions {
	return ClientHandoffOptions{
		Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)},
	}
}

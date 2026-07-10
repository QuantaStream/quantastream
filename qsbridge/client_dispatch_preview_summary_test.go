package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientDispatchPreviewsCountsDispatchableTargets(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityMultiStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	handoff := service.PrepareClientStatementHandoffBundle(
		NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select 1", "select 2"),
		clientRouteDecisionOptions(),
	)
	preview := service.ListClientDispatchPreviews(ExecutionDispatcher{Native: &recordingNativeExecutor{}}, handoff)

	exchange := service.SummarizeClientDispatchPreviews(preview)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported dispatch summary", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one summary row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if !row.Supported || !row.AllDispatchable || row.PreviewCount != 2 || row.NativeTargetCount != 2 {
		t.Fatalf("row = %#v, want two dispatchable native targets", row)
	}
	if row.ConfiguredCount != 2 || row.MissingExecutorCount != 0 || row.WillDispatchCount != 2 {
		t.Fatalf("row = %#v, want configured dispatch counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 13 || exchange.ResultSchema.Columns[0].Name != "User" {
		t.Fatalf("schema = %#v, want dispatch summary columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 1 || exchange.Result.Chunks[0].Rows[0][6].Value != 2 {
		t.Fatalf("result = %#v, want native target count row", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientDispatchPreviewsCountsMissingExecutors(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	handoff := service.PrepareClientStatementHandoffBundle(
		NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1"),
		clientRouteDecisionOptions(),
	)
	preview := service.ListClientDispatchPreviews(ExecutionDispatcher{}, handoff)

	exchange := service.SummarizeClientDispatchPreviews(preview)
	row := exchange.Rows[0]
	if row.AllDispatchable || row.NativeTargetCount != 1 || row.MissingExecutorCount != 1 || row.WillDispatchCount != 0 {
		t.Fatalf("row = %#v, want one missing native executor", row)
	}
	if !containsDiagnosticCode(row.DiagnosticCodes, DiagnosticInternalInvariant) {
		t.Fatalf("diagnostics = %#v, want missing executor diagnostic", row.DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientDispatchPreviewsCountsFallbackAndRejectedTargets(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}
	fallbackHandoff := service.PrepareClientStatementHandoffBundle(
		NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1"),
		clientRouteDecisionOptions(),
	)
	fallbackPreview := service.ListClientDispatchPreviews(ExecutionDispatcher{Legacy: &recordingLegacyExecutor{}}, fallbackHandoff)
	fallbackExchange := service.SummarizeClientDispatchPreviews(fallbackPreview)
	if row := fallbackExchange.Rows[0]; !row.AllDispatchable || row.LegacyTargetCount != 1 || row.WillDispatchCount != 1 {
		t.Fatalf("fallback row = %#v, want dispatchable fallback", row)
	}

	rejectedService := service
	rejectedService.Routing = NativeOnlyRoutingPolicy()
	rejectedHandoff := rejectedService.PrepareClientStatementHandoffBundle(
		NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1"),
		ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueString, "bad")}},
	)
	rejectedPreview := rejectedService.ListClientDispatchPreviews(ExecutionDispatcher{Native: &recordingNativeExecutor{}}, rejectedHandoff)
	rejectedExchange := rejectedService.SummarizeClientDispatchPreviews(rejectedPreview)
	if row := rejectedExchange.Rows[0]; row.AllDispatchable || row.NoTargetCount != 1 || row.WillDispatchCount != 0 {
		t.Fatalf("rejected row = %#v, want no-target non-dispatch", row)
	}
}

func TestPlanningServiceSummarizeClientDispatchPreviewsReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
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
		clientRouteDecisionOptions(),
	)
	preview := service.ListClientDispatchPreviews(ExecutionDispatcher{Native: &recordingNativeExecutor{}}, handoff)

	exchange := service.SummarizeClientDispatchPreviews(preview)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientDispatchPreviewsCopiesMutableState(t *testing.T) {
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
		clientRouteDecisionOptions(),
	)
	preview := service.ListClientDispatchPreviews(ExecutionDispatcher{Native: &recordingNativeExecutor{}}, handoff)

	exchange := service.SummarizeClientDispatchPreviews(preview)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Preview.Rows[0].SQL = "mutated"
	exchange.Rows[0].DiagnosticCodes = append(exchange.Rows[0].DiagnosticCodes, DiagnosticRouteRejected)
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][6].Value = "mutated"

	again := service.SummarizeClientDispatchPreviews(preview)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Preview.Rows[0].SQL != "select 1" {
		t.Fatalf("preview leaked mutation: %#v", again.Preview.Rows[0])
	}
	if containsDiagnosticCode(again.Rows[0].DiagnosticCodes, DiagnosticRouteRejected) {
		t.Fatalf("row diagnostics leaked mutation: %#v", again.Rows[0].DiagnosticCodes)
	}
	if again.Result.Columns[0].Name != "User" || again.ResultSchema.Columns[0].Name != "User" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][6].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

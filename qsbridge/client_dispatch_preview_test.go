package qsbridge

import "testing"

func TestPlanningServiceListClientDispatchPreviewsReturnsNativeRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	bundle := NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, clientRouteDecisionOptions())

	exchange := service.ListClientDispatchPreviews(ExecutionDispatcher{Native: &recordingNativeExecutor{}}, handoff)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported dispatch preview rows", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.Handoff != ExecutionHandoffNative || row.Target != DispatchTargetNative {
		t.Fatalf("row = %#v, want native dispatch target", row)
	}
	if row.AccessIntent != PhysicalAccessRead || row.Lifecycle != ClientPlanLifecycleSelect || row.LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want read select lifecycle", row)
	}
	if !row.Supported || !row.ExecutorConfigured || !row.WillDispatch {
		t.Fatalf("row = %#v, want dispatchable native row", row)
	}
	if len(exchange.ResultSchema.Columns) != 12 || exchange.ResultSchema.Columns[0].Name != "Ordinal" || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want dispatch preview result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[3].Value != string(DispatchTargetNative) || resultRow[4].Value != string(PhysicalAccessRead) || resultRow[5].Value != string(ClientPlanLifecycleSelect) || resultRow[6].Value != 7 || resultRow[9].Value != true {
		t.Fatalf("result row = %#v, want native dispatch target, lifecycle, and will-dispatch flag", resultRow)
	}
}

func TestPlanningServiceListClientDispatchPreviewsReportsMissingExecutor(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	bundle := NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, clientRouteDecisionOptions())

	exchange := service.ListClientDispatchPreviews(ExecutionDispatcher{}, handoff)
	row := exchange.Rows[0]
	if row.Target != DispatchTargetNative || row.ExecutorConfigured || row.WillDispatch {
		t.Fatalf("row = %#v, want missing native executor", row)
	}
	if !containsDiagnosticCode(row.Diagnostics, DiagnosticInternalInvariant) {
		t.Fatalf("diagnostics = %#v, want missing executor diagnostic", row.Diagnostics)
	}
}

func TestPlanningServiceListClientDispatchPreviewsReportsFallbackAndRejectedRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	service.Routing = RoutingPolicy{NativeRouting: NativeRouteDisabled}
	bundle := NewClientStatementBundle(clientStatementConnection(), ConnectionPlanOptions{}, "select 1")
	fallbackHandoff := service.PrepareClientStatementHandoffBundle(bundle, clientRouteDecisionOptions())

	fallbackExchange := service.ListClientDispatchPreviews(ExecutionDispatcher{Legacy: &recordingLegacyExecutor{}}, fallbackHandoff)
	fallbackRow := fallbackExchange.Rows[0]
	if fallbackRow.Handoff != ExecutionHandoffLegacyFallback || fallbackRow.Target != DispatchTargetLegacy || !fallbackRow.WillDispatch {
		t.Fatalf("row = %#v, want dispatchable fallback row", fallbackRow)
	}

	rejectedService := service
	rejectedService.Routing = NativeOnlyRoutingPolicy()
	rejectedHandoff := rejectedService.PrepareClientStatementHandoffBundle(bundle, ClientHandoffOptions{
		Values: []ParameterValue{IndexedParameterValue(1, ValueString, "bad")},
	})
	rejectedExchange := rejectedService.ListClientDispatchPreviews(ExecutionDispatcher{Native: &recordingNativeExecutor{}}, rejectedHandoff)
	rejectedRow := rejectedExchange.Rows[0]
	if rejectedRow.Handoff != ExecutionHandoffRejected || rejectedRow.Target != DispatchTargetNone || rejectedRow.WillDispatch {
		t.Fatalf("row = %#v, want rejected no-dispatch row", rejectedRow)
	}
	if !containsDiagnosticCode(rejectedRow.Diagnostics, DiagnosticRouteRejected) {
		t.Fatalf("diagnostics = %#v, want route rejection", rejectedRow.Diagnostics)
	}
}

func TestPlanningServiceListClientDispatchPreviewsFailsForConnectionDiagnostics(t *testing.T) {
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

	exchange := service.ListClientDispatchPreviews(ExecutionDispatcher{Native: &recordingNativeExecutor{}}, handoff)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block exchange", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientDispatchPreviewsCopiesMutableState(t *testing.T) {
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

	exchange := service.ListClientDispatchPreviews(ExecutionDispatcher{}, handoff)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Diagnostics = append(exchange.Diagnostics, ErrorDiagnostic(DiagnosticRouteRejected, PhasePlan, "mutated"))
	exchange.Rows[0].SQL = "mutated"
	exchange.Rows[0].Diagnostics = append(exchange.Rows[0].Diagnostics, DiagnosticRouteRejected)
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.ListClientDispatchPreviews(ExecutionDispatcher{}, handoff)
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

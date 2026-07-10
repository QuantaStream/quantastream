package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientFallbackRequestReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session: SessionContext{
			User:          "moli",
			Roles:         []RoleName{"reader"},
			CurrentSchema: "quanta",
		},
	}, nil)
	service.Routing = LegacyOnlyRoutingPolicy()
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	routed := service.PrepareRoutedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "legacy-1", MaxRows: 10},
		IndexedParameterValue(1, ValueInt, 99),
	)
	exchange := service.SummarizeClientFallbackRequest(connection, routed.FallbackRequest())
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported fallback summary", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one fallback row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.Kind != ClientFallbackSingle || row.Route != RouteLegacyFallback || row.RouteReason != RouteReasonLegacyForced {
		t.Fatalf("row = %#v, want forced single legacy fallback", row)
	}
	if row.RequestID != "legacy-1" || row.Schema != "quanta" || row.User != "moli" || row.ParameterCount != 1 || row.MaxRows != 10 {
		t.Fatalf("row = %#v, want fallback request metadata", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 17 {
		t.Fatalf("result/schema = %#v/%#v, want fallback summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != string(ClientFallbackSingle) || resultRow[5].Value != string(RouteLegacyFallback) || resultRow[13].Value != 1 {
		t.Fatalf("result row = %#v, want fallback metadata cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientBatchFallbackRequestReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	service.Routing = LegacyOnlyRoutingPolicy()
	connection := clientStatementConnection()

	routed := service.PrepareRoutedBatchExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "batch-legacy-1", BatchSize: 50},
		ParameterValues(IndexedParameterValue(1, ValueInt, 7)),
		ParameterValues(IndexedParameterValue(1, ValueInt, 8)),
	)
	fallback, ok := routed.LegacyFallbackRequest()
	if !ok {
		t.Fatalf("expected batch fallback")
	}

	exchange := service.SummarizeClientBatchFallbackRequest(connection, fallback)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported batch fallback summary", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Kind != ClientFallbackBatch || exchange.Rows[0].ParameterSetCount != 2 {
		t.Fatalf("rows = %#v, want batch fallback with two parameter sets", exchange.Rows)
	}
	if exchange.Result.Chunks[0].Rows[0][14].Value != 2 {
		t.Fatalf("result row = %#v, want parameter set count", exchange.Result.Chunks[0].Rows[0])
	}
}

func TestPlanningServiceSummarizeClientFallbackRequestKeepsRejectedRouteAsData(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}, nil)
	service.Routing = NativeOnlyRoutingPolicy()
	connection := clientStatementConnection()

	routed := service.PrepareRoutedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{},
		IndexedParameterValue(1, ValueString, "bad"),
	)
	exchange := service.SummarizeClientFallbackRequest(connection, routed.FallbackRequest())
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want summary exchange supported", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported || exchange.Rows[0].Route != RouteRejected {
		t.Fatalf("rows = %#v, want rejected fallback row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticRouteRejected) {
		t.Fatalf("diagnostics = %#v, want route rejected", exchange.Rows[0].DiagnosticCodes)
	}
}

func TestPlanningServiceSummarizeClientFallbackRequestCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli", Roles: []RoleName{"reader"}},
	}, nil)
	service.Routing = LegacyOnlyRoutingPolicy()
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	routed := service.PrepareRoutedExecutionRequest(
		PlanRequest{SQL: "select o_orderkey from orders where o_orderkey = ?"},
		ExecutionOptions{RequestID: "legacy-1"},
		IndexedParameterValue(1, ValueInt, 99),
	)
	fallback := routed.FallbackRequest()
	exchange := service.SummarizeClientFallbackRequest(connection, fallback)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].Roles[0] = "mutated"
	exchange.Result.Chunks[0].Rows[0][4].Value = "mutated"

	again := service.SummarizeClientFallbackRequest(connection, fallback)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].Roles[0] != "reader" || again.Result.Chunks[0].Rows[0][4].Value != "reader" {
		t.Fatalf("fallback summary leaked mutation: %#v/%#v", again.Rows, again.Result.Chunks)
	}
}

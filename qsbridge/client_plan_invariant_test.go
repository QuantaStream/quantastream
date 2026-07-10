package qsbridge

import "testing"

func TestPlanningServiceListClientPreparedPlanInvariantsReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	prepared := service.PrepareWithRequest(connection.PlanRequest("select 1", ConnectionPlanOptions{}))

	exchange := service.ListClientPreparedPlanInvariants(connection, prepared)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported invariant metadata", exchange)
	}
	if len(exchange.Rows) != 8 || len(exchange.Report.Checks) != 8 {
		t.Fatalf("rows/report = %#v/%#v, want invariant rows", exchange.Rows, exchange.Report.Checks)
	}
	if !planInvariantHasStatus(exchange.Rows, "parameters_match_query", PlanInvariantOK) {
		t.Fatalf("rows = %#v, want parameter invariant ok", exchange.Rows)
	}
	if !planInvariantHasStatus(exchange.Rows, "cache_key_available", PlanInvariantOK) {
		t.Fatalf("rows = %#v, want cache-key invariant ok", exchange.Rows)
	}
	if !planInvariantHasStatus(exchange.Rows, "scalar_subquery_placeholders", PlanInvariantOK) {
		t.Fatalf("rows = %#v, want scalar placeholder invariant ok", exchange.Rows)
	}
	if !planInvariantHasStatus(exchange.Rows, "correlated_aggregate_placeholders", PlanInvariantOK) {
		t.Fatalf("rows = %#v, want correlated aggregate placeholder invariant ok", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 4 || exchange.ResultSchema.Columns[0].Name != "Invariant" {
		t.Fatalf("schema = %#v, want invariant result schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one result row per invariant", exchange.Result)
	}
}

func TestPlanningServiceListClientPreparedPlanInvariantsReportsInvariantFailure(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	prepared := PreparedPlan{
		Kind:      QueryKindUpdate,
		Supported: true,
		Query: QueryIR{
			Kind:       QueryKindSelect,
			Projection: []ProjectionColumn{{Alias: "order_id", Type: DataTypeInt}},
		},
	}

	exchange := service.ListClientPreparedPlanInvariants(connection, prepared)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want invariant failure to block invariant metadata result", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInternalInvariant) {
		t.Fatalf("result/diagnostics = %#v/%#v, want failed internal invariant result", exchange.Result, exchange.Diagnostics)
	}
}

func TestPlanningServiceListClientPreparedPlanInvariantsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientPreparedPlanInvariants(connection, PreparedPlan{})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block invariant metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientPreparedPlanInvariantsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	prepared := service.PrepareWithRequest(connection.PlanRequest("select 1", ConnectionPlanOptions{}))

	exchange := service.ListClientPreparedPlanInvariants(connection, prepared)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Report.Checks[0].Name = "mutated"
	exchange.Rows[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientPreparedPlanInvariants(connection, prepared)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Report.Checks[0].Name == "mutated" || again.Rows[0].Name == "mutated" {
		t.Fatalf("report/rows leaked mutation: %#v/%#v", again.Report.Checks[0], again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Invariant" || again.ResultSchema.Columns[0].Name != "Invariant" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

package qsbridge

import "testing"

func TestPlanningServiceListClientPreparedSQLFeatureCoverageReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	prepared := service.PrepareWithRequest(connection.PlanRequest("select 1", ConnectionPlanOptions{}))

	exchange := service.ListClientPreparedSQLFeatureCoverage(connection, prepared)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported SQL feature coverage metadata", exchange)
	}
	if len(exchange.Rows) == 0 || len(exchange.Rows) != len(exchange.Report.Matrix.Features) {
		t.Fatalf("rows/matrix = %d/%d, want one coverage row per feature", len(exchange.Rows), len(exchange.Report.Matrix.Features))
	}
	projection := sqlFeatureCoverageByName(t, exchange.Rows, "select_projection")
	if !projection.Present || !projection.Supported {
		t.Fatalf("projection coverage = %#v, want present supported projection", projection)
	}
	predicate := sqlFeatureCoverageByName(t, exchange.Rows, "predicate_pushdown")
	if !predicate.Present || !predicate.Supported {
		t.Fatalf("predicate coverage = %#v, want present supported predicate", predicate)
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.ResultSchema.Columns[0].Name != "Feature" {
		t.Fatalf("schema = %#v, want SQL feature coverage schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one result row per coverage row", exchange.Result)
	}
}

func TestPlanningServiceListClientPreparedSQLFeatureCoverageFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientPreparedSQLFeatureCoverage(connection, PreparedPlan{})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block SQL feature coverage metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientPreparedSQLFeatureCoverageCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	prepared := service.PrepareWithRequest(connection.PlanRequest("select 1", ConnectionPlanOptions{}))

	exchange := service.ListClientPreparedSQLFeatureCoverage(connection, prepared)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Report.Coverage[0].Feature.Name = "mutated"
	exchange.Rows[0].Feature.Name = "mutated"
	exchange.Rows[0].Capabilities = append(exchange.Rows[0].Capabilities, CapabilityScalarSubquery)
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientPreparedSQLFeatureCoverage(connection, prepared)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Report.Coverage[0].Feature.Name == "mutated" || again.Rows[0].Feature.Name == "mutated" || coverageHasCapability(again.Rows[0], CapabilityScalarSubquery) {
		t.Fatalf("report/rows leaked mutation: %#v/%#v", again.Report.Coverage[0], again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Feature" || again.ResultSchema.Columns[0].Name != "Feature" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

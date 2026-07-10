package qsbridge

import "testing"

func TestPlanningServiceListClientReadinessReturnsSummaryRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientReadiness(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported readiness metadata", exchange)
	}
	if len(exchange.Rows) != len(exchange.Report.Rows) || len(exchange.Rows) != 10 {
		t.Fatalf("rows/report = %d/%d, want one row per readiness summary", len(exchange.Rows), len(exchange.Report.Rows))
	}
	if readinessCount(exchange.Rows, "compatibility", CompatibilityStatusMetadataOnly) == 0 {
		t.Fatalf("rows = %#v, want metadata-only compatibility summary", exchange.Rows)
	}
	if readinessCount(exchange.Rows, "sql_feature", CompatibilityStatusMetadataOnly) == 0 {
		t.Fatalf("rows = %#v, want metadata-only SQL feature summary", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 4 || exchange.ResultSchema.Columns[0].Name != "Scope" {
		t.Fatalf("schema = %#v, want readiness result schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one result row per readiness summary", exchange.Result)
	}
}

func TestPlanningServiceListClientReadinessFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientReadiness(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block readiness metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientReadinessCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientReadiness(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Report.Rows[0].Count = 999
	exchange.Rows[0].Count = 999
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientReadiness(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Report.Rows[0].Count == 999 || again.Rows[0].Count == 999 {
		t.Fatalf("report/rows leaked mutation: %#v/%#v", again.Report.Rows[0], again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Scope" || again.ResultSchema.Columns[0].Name != "Scope" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

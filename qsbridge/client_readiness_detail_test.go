package qsbridge

import "testing"

func TestPlanningServiceListClientReadinessDetailsReturnsManifestItems(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientReadinessDetails(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported readiness details", exchange)
	}
	if len(exchange.Rows) != len(exchange.Report.Details) {
		t.Fatalf("rows/details = %d/%d, want one row per readiness detail", len(exchange.Rows), len(exchange.Report.Details))
	}
	if len(exchange.Rows) != len(exchange.Report.Compatibility.Capabilities)+len(exchange.Report.SQLFeatures.Features) {
		t.Fatalf("rows = %d, want compatibility plus SQL feature items", len(exchange.Rows))
	}
	if !readinessDetailsContain(exchange.Rows, "compatibility", "structured_explain", string(CompatibilityLayerClient), CompatibilityStatusMetadataOnly) {
		t.Fatalf("rows = %#v, want structured explain compatibility detail", exchange.Rows)
	}
	if !readinessDetailsContain(exchange.Rows, "sql_feature", "explain_and_management_metadata", string(SQLFeatureProtocol), CompatibilityStatusMetadataOnly) {
		t.Fatalf("rows = %#v, want explain/management SQL feature detail", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 7 || exchange.ResultSchema.Columns[0].Name != "Scope" {
		t.Fatalf("schema = %#v, want readiness detail result schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one row per readiness detail", exchange.Result)
	}
}

func TestPlanningServiceListClientReadinessDetailsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientReadinessDetails(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block readiness details", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientReadinessDetailsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientReadinessDetails(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Report.Details[0].Name = "mutated"
	exchange.Rows[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientReadinessDetails(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Report.Details[0].Name == "mutated" || again.Rows[0].Name == "mutated" {
		t.Fatalf("details leaked mutation: %#v/%#v", again.Report.Details[0], again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Scope" || again.ResultSchema.Columns[0].Name != "Scope" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func readinessDetailsContain(rows []ReadinessDetailRow, scope string, name string, category string, status CompatibilityStatus) bool {
	for _, row := range rows {
		if row.Scope == scope && row.Name == name && row.Category == category && row.Status == status {
			return true
		}
	}
	return false
}

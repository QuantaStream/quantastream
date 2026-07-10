package qsbridge

import "testing"

func TestPlanningServiceListClientSQLFeaturesReturnsMatrixRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientSQLFeatures(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported SQL feature metadata", exchange)
	}
	if len(exchange.Rows) == 0 || len(exchange.Rows) != len(exchange.Matrix.Features) {
		t.Fatalf("rows/matrix = %d/%d, want one row per SQL feature", len(exchange.Rows), len(exchange.Matrix.Features))
	}
	if !sqlFeatureRowsContain(exchange.Rows, "outer_join", SQLFeatureJoin, CompatibilityStatusDeferred) {
		t.Fatalf("rows = %#v, want deferred outer join feature", exchange.Rows)
	}
	if !sqlFeatureRowsContain(exchange.Rows, "predicate_pushdown", SQLFeaturePredicate, CompatibilityStatusNativePlanning) {
		t.Fatalf("rows = %#v, want native predicate pushdown feature", exchange.Rows)
	}
	if !sqlFeatureRowsContain(exchange.Rows, "explain_and_management_metadata", SQLFeatureProtocol, CompatibilityStatusMetadataOnly) {
		t.Fatalf("rows = %#v, want metadata-only explain/management feature", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 6 || exchange.ResultSchema.Columns[0].Name != "Feature" {
		t.Fatalf("schema = %#v, want SQL feature result schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one result row per SQL feature", exchange.Result)
	}
	for _, row := range exchange.Result.Chunks[0].Rows {
		if row[0].Value == "outer_join" {
			if row[3].Value != joinPlanCapabilities([]PlanCapability{CapabilityOuterJoin, CapabilityNullExtension}) || row[4].Value != string(DiagnosticOuterJoin) {
				t.Fatalf("outer join row = %#v, want capabilities and diagnostic code", row)
			}
			return
		}
	}
	t.Fatalf("result rows = %#v, missing outer join row", exchange.Result.Chunks[0].Rows)
}

func TestPlanningServiceListClientSQLFeaturesFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientSQLFeatures(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block SQL feature metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientSQLFeaturesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientSQLFeatures(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Matrix.Features[0].Name = "mutated"
	exchange.Rows[0].Name = "mutated"
	exchange.Rows[0].Capabilities = append(exchange.Rows[0].Capabilities, CapabilityScalarSubquery)
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientSQLFeatures(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Matrix.Features[0].Name == "mutated" || again.Rows[0].Name == "mutated" || sqlFeatureHasCapability(again.Rows[0], CapabilityScalarSubquery) {
		t.Fatalf("matrix/rows leaked mutation: %#v/%#v", again.Matrix.Features[0], again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Feature" || again.ResultSchema.Columns[0].Name != "Feature" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func sqlFeatureRowsContain(rows []SQLFeature, name string, category SQLFeatureCategory, status CompatibilityStatus) bool {
	for _, row := range rows {
		if row.Name == name && row.Category == category && row.Status == status {
			return true
		}
	}
	return false
}

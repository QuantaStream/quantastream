package qsbridge

import "testing"

func TestPlanningServiceListClientCompatibilityReturnsManifestRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientCompatibility(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported compatibility metadata", exchange)
	}
	if len(exchange.Rows) == 0 || len(exchange.Rows) != len(exchange.Profile.Capabilities) {
		t.Fatalf("rows/profile = %d/%d, want one row per capability", len(exchange.Rows), len(exchange.Profile.Capabilities))
	}
	if !compatibilityRowsContain(exchange.Rows, "client_statement_flow", CompatibilityLayerClient, CompatibilityStatusMetadataOnly) {
		t.Fatalf("rows = %#v, want client statement flow metadata capability", exchange.Rows)
	}
	if !compatibilityRowsContain(exchange.Rows, "structured_explain", CompatibilityLayerClient, CompatibilityStatusMetadataOnly) {
		t.Fatalf("rows = %#v, want structured explain metadata capability", exchange.Rows)
	}
	if !compatibilityRowsContain(exchange.Rows, "plan_cache_policy", CompatibilityLayerClient, CompatibilityStatusMetadataOnly) {
		t.Fatalf("rows = %#v, want plan cache policy metadata capability", exchange.Rows)
	}
	if !compatibilityRowsContain(exchange.Rows, "native_executor", CompatibilityLayerExecutor, CompatibilityStatusBoundaryOnly) {
		t.Fatalf("rows = %#v, want native executor boundary capability", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 6 || exchange.ResultSchema.Columns[0].Name != "Capability" {
		t.Fatalf("schema = %#v, want compatibility result schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) {
		t.Fatalf("result = %#v, want one result row per compatibility row", exchange.Result)
	}
	first := exchange.Result.Chunks[0].Rows[0]
	if first[0].Kind != ValueString || first[3].Kind != ValueBool || first[4].Kind != ValueBool {
		t.Fatalf("first result row = %#v, want typed compatibility metadata cells", first)
	}
}

func TestPlanningServiceListClientCompatibilityFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientCompatibility(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block compatibility metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientCompatibilityCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientCompatibility(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Profile.Capabilities[0].Name = "mutated"
	exchange.Rows[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientCompatibility(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Profile.Capabilities[0].Name == "mutated" || again.Rows[0].Name == "mutated" {
		t.Fatalf("profile/rows leaked mutation: %#v/%#v", again.Profile.Capabilities[0], again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Capability" || again.ResultSchema.Columns[0].Name != "Capability" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func compatibilityRowsContain(rows []CompatibilityCapability, name string, layer CompatibilityLayer, status CompatibilityStatus) bool {
	for _, row := range rows {
		if row.Name == name && row.Layer == layer && row.Status == status {
			return true
		}
	}
	return false
}

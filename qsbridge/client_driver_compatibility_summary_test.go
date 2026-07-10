package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientDriverCompatibilityReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)

	exchange := service.SummarizeClientDriverCompatibility(clientStatementConnection(), "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported driver compatibility summary", exchange)
	}
	row := exchange.Row
	if row.ProfileCount != 7 || row.MySQLProtocolCount != 5 || row.GoProtocolCount != 1 || row.GRPCProtocolCount != 1 {
		t.Fatalf("row = %#v, want protocol counts for default driver matrix", row)
	}
	if row.BoundaryOnlyCount != 7 || row.TypedAPICount != 2 {
		t.Fatalf("row = %#v, want boundary-only and typed API counts", row)
	}
	if row.PreparedStatementCount != 7 || row.BatchExecutionCount != 7 {
		t.Fatalf("row = %#v, want prepared and batch support counts", row)
	}
	if row.StreamingResultCount != 3 || row.CancellationCount != 3 || row.StructuredExplainCount != 2 || row.PlanCachePolicyCount != 2 {
		t.Fatalf("row = %#v, want advanced capability counts", row)
	}
	if row.PasswordAuthTargetCount != 5 || row.TokenAuthTargetCount != 2 {
		t.Fatalf("row = %#v, want auth-family target counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 14 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 7 || resultRow[5].Value != 2 || resultRow[13].Value != 2 {
		t.Fatalf("result row = %#v, want summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientDriverCompatibilityFiltersByPattern(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)

	exchange := service.SummarizeClientDriverCompatibility(clientStatementConnection(), "grpc")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported filtered driver summary", exchange)
	}
	if exchange.Row.ProfileCount != 1 || exchange.Row.GRPCProtocolCount != 1 || exchange.Row.TypedAPICount != 1 {
		t.Fatalf("row = %#v, want one gRPC target", exchange.Row)
	}
	if exchange.Row.PasswordAuthTargetCount != 0 || exchange.Row.TokenAuthTargetCount != 1 {
		t.Fatalf("row = %#v, want token auth for gRPC target", exchange.Row)
	}
}

func TestPlanningServiceSummarizeClientDriverCompatibilityReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticNativeBlocker, PhaseBind, "blocked")}

	exchange := service.SummarizeClientDriverCompatibility(connection, "")
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocked connection", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || exchange.Result.RowsReturned != 0 {
		t.Fatalf("result = %#v, want failed rowless driver compatibility summary", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientDriverCompatibilityCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.SummarizeClientDriverCompatibility(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.ProfileCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientDriverCompatibility(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.ProfileCount != 7 {
		t.Fatalf("summary leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Profile_count" || again.ResultSchema.Columns[0].Name != "Profile_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 7 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

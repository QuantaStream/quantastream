package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientCancellationProfilesReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)

	exchange := service.SummarizeClientCancellationProfiles(clientStatementConnection())
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported cancellation profile summary", exchange)
	}
	row := exchange.Row
	if row.ProfileCount != 3 || row.RequiresRequestIDCount != 3 || row.RequiresRegistryCount != 3 {
		t.Fatalf("row = %#v, want all profiles to require request id and registry", row)
	}
	if row.ClientInitiatedCount != 1 || row.TimeoutDrivenCount != 1 || row.ShutdownDrivenCount != 1 || row.ForceAllowedCount != 1 {
		t.Fatalf("row = %#v, want one count for each cancellation mode", row)
	}
	if len(exchange.ResultSchema.Columns) != 7 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 3 || resultRow[6].Value != 1 {
		t.Fatalf("result row = %#v, want profile and force counts", resultRow)
	}
}

func TestPlanningServiceSummarizeClientCancellationProfilesFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientCancellationProfiles(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block summary", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || exchange.Result.RowsReturned != 0 {
		t.Fatalf("result = %#v, want failed rowless summary", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientCancellationProfilesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.SummarizeClientCancellationProfiles(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.ProfileCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientCancellationProfiles(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.ProfileCount != 3 {
		t.Fatalf("row leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Profile_count" || again.ResultSchema.Columns[0].Name != "Profile_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 3 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

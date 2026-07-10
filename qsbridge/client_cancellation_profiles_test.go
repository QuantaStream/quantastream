package qsbridge

import "testing"

func TestPlanningServiceListClientCancellationProfilesReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)

	exchange := service.ListClientCancellationProfiles(clientStatementConnection())
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported cancellation profiles", exchange)
	}
	if len(exchange.Rows) != 3 {
		t.Fatalf("rows = %#v, want three cancellation profiles", exchange.Rows)
	}
	if exchange.Rows[0].Reason != CancellationClientRequest || !exchange.Rows[0].ClientInitiated {
		t.Fatalf("first row = %#v, want client-request profile", exchange.Rows[0])
	}
	if exchange.Rows[1].Reason != CancellationTimeout || !exchange.Rows[1].TimeoutDriven {
		t.Fatalf("second row = %#v, want timeout profile", exchange.Rows[1])
	}
	if exchange.Rows[2].Reason != CancellationShutdown || !exchange.Rows[2].ShutdownDriven || !exchange.Rows[2].ForceAllowed {
		t.Fatalf("third row = %#v, want forceable shutdown profile", exchange.Rows[2])
	}
	for _, row := range exchange.Rows {
		if !row.RequiresRequestID || !row.RequiresRegistry {
			t.Fatalf("row = %#v, want request id and registry requirements", row)
		}
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.ResultSchema.Columns[0].Name != "Reason" || exchange.Result.RowsReturned != 3 {
		t.Fatalf("result/schema = %#v/%#v, want cancellation profile result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != string(CancellationClientRequest) || resultRow[3].Value != true {
		t.Fatalf("result row = %#v, want client cancellation cells", resultRow)
	}
}

func TestPlanningServiceListClientCancellationProfilesFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientCancellationProfiles(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block profiles", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless profiles", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientCancellationProfilesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientCancellationProfiles(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].Detail = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientCancellationProfiles(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].Detail == "mutated" {
		t.Fatalf("row leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Reason" || again.ResultSchema.Columns[0].Name != "Reason" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != string(CancellationClientRequest) {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

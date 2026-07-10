package qsbridge

import "testing"

func TestPlanningServiceListClientCommandCapabilitiesReportsProtocolSupport(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(
		ProtocolMySQL,
		"mysql",
		ProtocolCapabilityStatementResults,
		ProtocolCapabilitySessionActions,
	)

	exchange := service.ListClientCommandCapabilities(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported command capability metadata", exchange)
	}
	if len(exchange.Rows) != 4 {
		t.Fatalf("rows = %#v, want four command capability rows", exchange.Rows)
	}
	for _, row := range exchange.Rows {
		if !row.Supported {
			t.Fatalf("row = %#v, want protocol-supported command", row)
		}
	}
	init := exchange.Rows[3]
	if init.Command != ClientCommandInitSchema || !init.RequiresPayload || !init.SessionAction {
		t.Fatalf("init row = %#v, want init-schema command metadata", init)
	}
	if len(exchange.ResultSchema.Columns) != 7 || exchange.ResultSchema.Columns[0].Name != "Command" || exchange.Result.RowsReturned != 4 {
		t.Fatalf("result/schema = %#v/%#v, want command capability result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != string(ClientCommandPing) || resultRow[1].Value != true {
		t.Fatalf("result row = %#v, want supported ping command", resultRow)
	}
}

func TestPlanningServiceListClientCommandCapabilitiesReportsUnsupportedProtocolRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults)

	exchange := service.ListClientCommandCapabilities(connection)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, command capability report should still be supported", exchange)
	}
	if !exchange.Rows[0].Supported || !exchange.Rows[1].Supported {
		t.Fatalf("rows = %#v, ping and quit should only need statement responses", exchange.Rows)
	}
	if exchange.Rows[2].Supported || exchange.Rows[3].Supported {
		t.Fatalf("rows = %#v, reset/init should need session actions", exchange.Rows)
	}
}

func TestPlanningServiceListClientCommandCapabilitiesFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientCommandCapabilities(connection)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block report", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless report", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientCommandCapabilitiesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientCommandCapabilities(connection)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].Detail = "mutated"
	exchange.Rows[0].RequiredCapabilities[0] = ProtocolCapabilityCancellation
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientCommandCapabilities(connection)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].Detail == "mutated" || again.Rows[0].RequiredCapabilities[0] != ProtocolCapabilityStatementResults {
		t.Fatalf("row leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Command" || again.ResultSchema.Columns[0].Name != "Command" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != string(ClientCommandPing) {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

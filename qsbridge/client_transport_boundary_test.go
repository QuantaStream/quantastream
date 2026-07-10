package qsbridge

import "testing"

func TestPlanningServiceListClientTransportBoundariesReturnsAllRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientTransportBoundaries(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported transport boundary metadata", exchange)
	}
	if len(exchange.Boundaries) == 0 {
		t.Fatal("expected transport boundaries")
	}
	if !transportBoundariesContain(exchange.Boundaries, TransportKindMySQLWire) ||
		!transportBoundariesContain(exchange.Boundaries, TransportKindQuantaInternal) ||
		!transportBoundariesContain(exchange.Boundaries, TransportKindInProcess) {
		t.Fatalf("boundaries = %#v, want client, internal, and in-process rows", exchange.Boundaries)
	}
	if len(exchange.ResultSchema.Columns) != 9 || exchange.ResultSchema.Columns[0].Name != "Role" {
		t.Fatalf("schema = %#v, want transport boundary columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Boundaries)) {
		t.Fatalf("result = %#v, want one row per boundary", exchange.Result)
	}
}

func TestPlanningServiceListClientTransportBoundariesFiltersRole(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientTransportBoundaries(connection, TransportRoleInProcess)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported in-process boundary metadata", exchange)
	}
	if len(exchange.Boundaries) != 1 || exchange.Boundaries[0].Kind != TransportKindInProcess {
		t.Fatalf("boundaries = %#v, want only in-process transport", exchange.Boundaries)
	}
	if exchange.Boundaries[0].Networked {
		t.Fatalf("boundary = %#v, want QIAB direct path to be non-networked", exchange.Boundaries[0])
	}
}

func TestPlanningServiceListClientTransportBoundariesReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.ListClientTransportBoundaries(connection, TransportRoleClientProtocol)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Boundaries) != 0 {
		t.Fatalf("result = %#v boundaries=%#v, want failed rowless exchange", exchange.Result, exchange.Boundaries)
	}
}

func TestPlanningServiceListClientTransportBoundariesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientTransportBoundaries(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Boundaries[0].Detail = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][8].Value = "mutated"

	again := service.ListClientTransportBoundaries(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Boundaries[0].Detail == "mutated" {
		t.Fatalf("boundaries leaked mutation: %#v", again.Boundaries[0])
	}
	if again.Result.Columns[0].Name != "Role" || again.ResultSchema.Columns[0].Name != "Role" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][8].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

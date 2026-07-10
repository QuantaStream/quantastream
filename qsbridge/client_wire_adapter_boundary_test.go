package qsbridge

import "testing"

func TestPlanningServiceListClientWireAdapterBoundariesReturnsMySQLRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientWireAdapterBoundaries(connection, ProtocolUnknown)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported wire adapter boundary metadata", exchange)
	}
	if exchange.Protocol != ProtocolMySQL {
		t.Fatalf("protocol = %q, want connection protocol", exchange.Protocol)
	}
	if len(exchange.Boundaries) == 0 {
		t.Fatal("expected wire adapter boundaries")
	}
	if !wireAdapterBoundariesContain(exchange.Boundaries, ProtocolMySQL, WireAdapterConcernPacketIO) ||
		!wireAdapterBoundariesContain(exchange.Boundaries, ProtocolUnknown, WireAdapterConcernSQLPlanning) {
		t.Fatalf("boundaries = %#v, want MySQL packet IO and common SQL planning", exchange.Boundaries)
	}
	if len(exchange.ResultSchema.Columns) != 6 || exchange.ResultSchema.Columns[0].Name != "Concern" {
		t.Fatalf("schema = %#v, want wire adapter boundary columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Boundaries)) {
		t.Fatalf("result = %#v, want one row per boundary", exchange.Result)
	}
}

func TestPlanningServiceListClientWireAdapterBoundariesFiltersExplicitProtocol(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientWireAdapterBoundaries(connection, ProtocolGRPC)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported gRPC boundary metadata", exchange)
	}
	if !wireAdapterBoundariesContain(exchange.Boundaries, ProtocolGRPC, WireAdapterConcernPacketIO) {
		t.Fatalf("boundaries = %#v, want gRPC transport boundary", exchange.Boundaries)
	}
	if wireAdapterBoundariesContain(exchange.Boundaries, ProtocolMySQL, WireAdapterConcernPacketIO) {
		t.Fatalf("boundaries = %#v, did not expect MySQL packet IO in explicit gRPC view", exchange.Boundaries)
	}
}

func TestPlanningServiceListClientWireAdapterBoundariesReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.ListClientWireAdapterBoundaries(connection, ProtocolMySQL)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Boundaries) != 0 {
		t.Fatalf("result = %#v boundaries=%#v, want failed rowless exchange", exchange.Result, exchange.Boundaries)
	}
}

func TestPlanningServiceListClientWireAdapterBoundariesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientWireAdapterBoundaries(connection, ProtocolUnknown)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Boundaries[0].Detail = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][5].Value = "mutated"

	again := service.ListClientWireAdapterBoundaries(connection, ProtocolUnknown)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Boundaries[0].Detail == "mutated" {
		t.Fatalf("boundaries leaked mutation: %#v", again.Boundaries[0])
	}
	if again.Result.Columns[0].Name != "Concern" || again.ResultSchema.Columns[0].Name != "Concern" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][5].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

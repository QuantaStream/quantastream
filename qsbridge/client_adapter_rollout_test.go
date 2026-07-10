package qsbridge

import "testing"

func TestPlanningServiceListClientAdapterRolloutReturnsSurfaceRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")

	exchange := service.ListClientAdapterRollout(connection, AdapterSurfaceMySQLServer)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter rollout metadata", exchange)
	}
	if len(exchange.Steps) != 5 || exchange.Steps[0].Surface != AdapterSurfaceMySQLServer {
		t.Fatalf("steps = %#v, want MySQL rollout steps", exchange.Steps)
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("schema = %#v, want adapter rollout columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Steps)) {
		t.Fatalf("result = %#v, want one row per rollout step", exchange.Result)
	}
}

func TestPlanningServiceListClientAdapterRolloutReturnsAllRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientAdapterRollout(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported adapter rollout metadata", exchange)
	}
	if len(exchange.Steps) != 20 {
		t.Fatalf("steps = %#v, want five phases for four adapter surfaces", exchange.Steps)
	}
}

func TestPlanningServiceListClientAdapterRolloutReturnsFailedEnvelopeForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked")}

	exchange := service.ListClientAdapterRollout(connection, AdapterSurfaceMySQLServer)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want failed connection metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Steps) != 0 {
		t.Fatalf("result = %#v steps=%#v, want failed rowless exchange", exchange.Result, exchange.Steps)
	}
}

func TestPlanningServiceListClientAdapterRolloutCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.ListClientAdapterRollout(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Steps[0].Detail = "mutated"
	exchange.Steps[0].Requires[0] = AdapterContractTopology
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][7].Value = "mutated"

	again := service.ListClientAdapterRollout(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Steps[0].Detail == "mutated" || again.Steps[0].Requires[0] == AdapterContractTopology {
		t.Fatalf("steps leaked mutation: %#v", again.Steps[0])
	}
	if again.Result.Columns[0].Name != "Surface" || again.ResultSchema.Columns[0].Name != "Surface" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][7].Value == "mutated" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

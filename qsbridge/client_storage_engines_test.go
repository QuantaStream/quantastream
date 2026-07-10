package qsbridge

import "testing"

func TestPlanningServiceListClientStorageEnginesFiltersAndBuildsResult(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	engines := []ClientStorageEngine{
		{Name: "memory", Support: ClientStorageEngineSupportNo, Comment: "temporary memory engine"},
		{Name: "quanta_bitmap", Support: ClientStorageEngineSupportDefault, Comment: "Quanta bitmap storage", Transactions: false, XA: false, Savepoints: false},
		{Name: "quanta_columnar", Support: ClientStorageEngineSupportDisabled, Comment: "future columnar storage"},
	}

	exchange := service.ListClientStorageEngines(connection, engines, "quanta_%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported storage engine metadata", exchange)
	}
	if exchange.Pattern != "quanta_%" || len(exchange.Engines) != 2 {
		t.Fatalf("exchange = %#v, want filtered quanta engines", exchange)
	}
	if exchange.Engines[0].Name != "quanta_bitmap" || exchange.Engines[1].Name != "quanta_columnar" {
		t.Fatalf("engines = %#v, want sorted filtered engines", exchange.Engines)
	}
	if len(exchange.ResultSchema.Columns) != 6 || exchange.ResultSchema.Columns[0].Name != "Engine" || exchange.ResultSchema.Columns[3].Name != "Transactions" {
		t.Fatalf("schema = %#v, want storage engine result schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 2 || exchange.Result.Chunks[0].Rows[0][1].Value != string(ClientStorageEngineSupportDefault) {
		t.Fatalf("result rows = %#v, want storage engine rows", exchange.Result.Chunks)
	}
}

func TestPlanningServiceListClientStorageEnginesReturnsFailedEnvelopeForDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "blocked"),
	}

	exchange := service.ListClientStorageEngines(connection, []ClientStorageEngine{{Name: "quanta_bitmap"}}, "")
	if exchange.Supported() || len(exchange.Engines) != 0 {
		t.Fatalf("exchange = %#v, want unsupported rowless metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 6 {
		t.Fatalf("result/schema = %#v/%#v, want failed storage engine envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientStorageEnginesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	engines := []ClientStorageEngine{{
		Name:    "quanta_bitmap",
		Support: ClientStorageEngineSupportDefault,
		Comment: "Quanta bitmap storage",
	}}

	exchange := service.ListClientStorageEngines(connection, engines, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Engines[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientStorageEngines(connection, engines, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Engines[0].Name != "quanta_bitmap" {
		t.Fatalf("engines leaked mutation: %#v", again.Engines)
	}
	if again.Result.Columns[0].Name != "Engine" || again.ResultSchema.Columns[0].Name != "Engine" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != "quanta_bitmap" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

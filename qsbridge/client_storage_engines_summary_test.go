package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientStorageEnginesFiltersAndBuildsResult(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	engines := []ClientStorageEngine{
		{Name: "memory", Support: ClientStorageEngineSupportNo, Comment: "temporary memory engine"},
		{Name: "quanta_bitmap", Support: ClientStorageEngineSupportDefault, Comment: "Quanta bitmap storage", Transactions: true, XA: true, Savepoints: true},
		{Name: "quanta_columnar", Support: ClientStorageEngineSupportDisabled, Comment: "future columnar storage"},
	}

	exchange := service.SummarizeClientStorageEngines(connection, engines, "quanta_%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported storage engine summary", exchange)
	}
	if exchange.Pattern != "quanta_%" {
		t.Fatalf("exchange = %#v, want pattern retained", exchange)
	}
	row := exchange.Row
	if row.EngineCount != 2 || row.DefaultCount != 1 || row.AvailableCount != 1 || row.DisabledCount != 1 {
		t.Fatalf("row = %#v, want filtered support counts", row)
	}
	if row.UnavailableCount != 0 || row.TransactionsCount != 1 || row.XACount != 1 || row.SavepointsCount != 1 {
		t.Fatalf("row = %#v, want capability counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.ResultSchema.Columns[0].Name != "Engine_count" {
		t.Fatalf("schema = %#v, want storage engine summary schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 1 || exchange.Result.Chunks[0].Rows[0][0].Value != 2 || exchange.Result.Chunks[0].Rows[0][4].Value != 1 {
		t.Fatalf("result rows = %#v, want storage engine summary row", exchange.Result.Chunks)
	}
}

func TestPlanningServiceSummarizeClientStorageEnginesCountsUnavailableEngines(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	engines := []ClientStorageEngine{
		{Name: "archive", Support: ClientStorageEngineSupportNo},
		{Name: "unknown"},
		{Name: "memory", Support: ClientStorageEngineSupportYes},
	}

	exchange := service.SummarizeClientStorageEngines(connection, engines, "")
	if exchange.Row.EngineCount != 3 || exchange.Row.AvailableCount != 1 || exchange.Row.UnavailableCount != 2 {
		t.Fatalf("row = %#v, want unavailable/default support counts", exchange.Row)
	}
}

func TestPlanningServiceSummarizeClientStorageEnginesReturnsFailedEnvelopeForDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "blocked"),
	}

	exchange := service.SummarizeClientStorageEngines(connection, []ClientStorageEngine{{Name: "quanta_bitmap"}}, "")
	if exchange.Supported() || exchange.Row.EngineCount != 0 {
		t.Fatalf("exchange = %#v, want unsupported rowless metadata", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 8 {
		t.Fatalf("result/schema = %#v/%#v, want failed storage engine summary envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientStorageEnginesCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	engines := []ClientStorageEngine{{
		Name:    "quanta_bitmap",
		Support: ClientStorageEngineSupportDefault,
		Comment: "Quanta bitmap storage",
	}}

	exchange := service.SummarizeClientStorageEngines(connection, engines, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.EngineCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientStorageEngines(connection, engines, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.EngineCount != 1 || again.Row.DefaultCount != 1 {
		t.Fatalf("row leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Engine_count" || again.ResultSchema.Columns[0].Name != "Engine_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

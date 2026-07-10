package qsbridge

import "testing"

func TestPlanningServiceListClientCursorsReturnsRegistryRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	registry := NewMemoryCursorRegistry()
	registry.Open(CursorDescriptor{
		ID:        "cursor-b",
		RequestID: "request-b",
		Mode:      CursorForwardOnly,
		State:     CursorStateOpen,
		BatchSize: 25,
		MaxRows:   100,
	})
	registry.Open(CursorDescriptor{
		ID:        "cursor-a",
		RequestID: "request-a",
		Mode:      CursorForwardOnly,
		State:     CursorStateOpen,
		BatchSize: 10,
		MaxRows:   50,
	})
	registry.Advance("cursor-a", 20, false)

	exchange := service.ListClientCursors(connection, registry)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported cursor inventory", exchange)
	}
	if len(exchange.Rows) != 2 {
		t.Fatalf("rows = %#v, want two cursor rows", exchange.Rows)
	}
	first := exchange.Rows[0]
	if first.ID != "cursor-a" || first.RequestID != "request-a" || first.Position != 20 || !first.Open {
		t.Fatalf("first row = %#v, want ordered advanced cursor-a", first)
	}
	if exchange.Rows[1].ID != "cursor-b" {
		t.Fatalf("rows = %#v, want cursor rows sorted by id", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.ResultSchema.Columns[0].Name != "Cursor_id" || exchange.Result.RowsReturned != 2 {
		t.Fatalf("result/schema = %#v/%#v, want cursor status result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != "cursor-a" || resultRow[6].Value != 20 {
		t.Fatalf("result row = %#v, want cursor-a position 20", resultRow)
	}
}

func TestPlanningServiceListClientCursorsReportsMissingRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.ListClientCursors(clientStatementConnection(), nil)

	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing registry to block inventory", exchange)
	}
	if !containsDiagnosticCode(exchange.ExchangeDiagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.ExchangeDiagnostics.Codes())
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed complete result", exchange.Result)
	}
}

func TestPlanningServiceListClientCursorsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientCursors(connection, NewMemoryCursorRegistry())
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block inventory", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless inventory", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientCursorsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	registry := NewMemoryCursorRegistry()
	registry.Open(CursorDescriptor{
		ID:        "cursor-a",
		RequestID: "request-a",
		Mode:      CursorForwardOnly,
		State:     CursorStateOpen,
		BatchSize: 10,
	})

	exchange := service.ListClientCursors(connection, registry)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].ID = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientCursors(connection, registry)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].ID != "cursor-a" {
		t.Fatalf("row leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Cursor_id" || again.ResultSchema.Columns[0].Name != "Cursor_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != "cursor-a" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

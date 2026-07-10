package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientCursorsReturnsRegistryCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	registry := NewMemoryCursorRegistry()
	registry.Open(CursorDescriptor{
		ID:        "cursor-open",
		RequestID: "request-open",
		Mode:      CursorForwardOnly,
		State:     CursorStateOpen,
		BatchSize: 10,
		MaxRows:   100,
	})
	registry.Open(CursorDescriptor{
		ID:        "cursor-exhausted",
		RequestID: "request-exhausted",
		Mode:      CursorForwardOnly,
		State:     CursorStateOpen,
		BatchSize: 25,
		MaxRows:   50,
	})
	registry.Advance("cursor-exhausted", 30, true)
	registry.Open(CursorDescriptor{
		ID:        "cursor-closed",
		RequestID: "request-closed",
		Mode:      CursorScrollable,
		State:     CursorStateOpen,
		BatchSize: 5,
	})
	registry.Close("cursor-closed")

	exchange := service.SummarizeClientCursors(connection, registry)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported cursor summary", exchange)
	}
	row := exchange.Row
	if row.CursorCount != 3 || row.OpenCount != 1 || row.ExhaustedCount != 1 || row.ClosedCount != 1 {
		t.Fatalf("row = %#v, want cursor lifecycle counts", row)
	}
	if row.ForwardOnlyCount != 2 || row.ScrollableCount != 1 {
		t.Fatalf("row = %#v, want cursor mode counts", row)
	}
	if row.TotalBatchSize != 40 || row.TotalMaxRows != 150 || row.TotalPosition != 30 {
		t.Fatalf("row = %#v, want aggregate cursor sizing", row)
	}
	if row.PositionedCount != 1 || row.ConfiguredMaxRows != 2 {
		t.Fatalf("row = %#v, want positioned/max-row counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 11 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one cursor summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 3 || resultRow[1].Value != 1 || resultRow[8].Value != 30 {
		t.Fatalf("result row = %#v, want cursor summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientCursorsReportsMissingRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.SummarizeClientCursors(clientStatementConnection(), nil)

	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing registry to block summary", exchange)
	}
	if !containsDiagnosticCode(exchange.ExchangeDiagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.ExchangeDiagnostics.Codes())
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed complete result", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientCursorsCopiesMutableState(t *testing.T) {
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

	exchange := service.SummarizeClientCursors(connection, registry)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.CursorCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientCursors(connection, registry)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.CursorCount != 1 || again.Row.OpenCount != 1 {
		t.Fatalf("row leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Cursor_count" || again.ResultSchema.Columns[0].Name != "Cursor_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

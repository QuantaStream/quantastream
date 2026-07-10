package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientCursorFetchReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	registry := NewMemoryCursorRegistry()
	_, _ = registry.Open(CursorDescriptor{
		ID:        "cursor-1",
		RequestID: "req-1",
		Mode:      CursorForwardOnly,
		State:     CursorStateOpen,
		BatchSize: 10,
		MaxRows:   25,
		Position:  20,
	})
	fetch := service.PrepareClientCursorFetch(connection, registry, "cursor-1", 10)

	exchange := service.SummarizeClientCursorFetch(connection, fetch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported cursor fetch summary", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one cursor fetch row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.CursorID != "cursor-1" || row.RequestID != "req-1" || row.Mode != CursorForwardOnly || row.State != CursorStateOpen {
		t.Fatalf("row = %#v, want cursor identity", row)
	}
	if row.Position != 20 || row.BatchSize != 10 || row.MaxRows != 25 || row.RequestedRows != 5 || !row.Final || !row.Supported {
		t.Fatalf("row = %#v, want clipped final fetch metadata", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 11 {
		t.Fatalf("result/schema = %#v/%#v, want cursor fetch summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != "cursor-1" || resultRow[7].Value != 5 || resultRow[8].Value != true || resultRow[9].Value != true {
		t.Fatalf("result row = %#v, want cursor fetch cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientCursorFetchReturnsValidationDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	registry := NewMemoryCursorRegistry()
	_, _ = registry.Open(CursorDescriptor{
		ID:        "cursor-1",
		RequestID: "req-1",
		Mode:      CursorForwardOnly,
		State:     CursorStateOpen,
	})
	fetch := service.PrepareClientCursorFetch(connection, registry, "cursor-1", 0)

	exchange := service.SummarizeClientCursorFetch(connection, fetch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, validation diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Supported {
		t.Fatalf("rows = %#v, want unsupported fetch row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result = %#v, want complete summary result", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientCursorFetchFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}
	fetch := ClientCursorFetchExchange{
		Cursor: CursorDescriptor{ID: "cursor-1", State: CursorStateOpen},
	}

	exchange := service.SummarizeClientCursorFetch(connection, fetch)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block exchange", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientCursorFetchCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	fetch := ClientCursorFetchExchange{
		Connection:    connection,
		Cursor:        CursorDescriptor{ID: "cursor-1", RequestID: "req-1", Mode: CursorForwardOnly, State: CursorStateOpen, BatchSize: 10},
		RequestedRows: 10,
	}

	exchange := service.SummarizeClientCursorFetch(connection, fetch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Fetch.Connection.Attributes["client"] = "mutated"
	exchange.Fetch.Cursor.ID = "mutated"
	exchange.Rows[0].CursorID = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.SummarizeClientCursorFetch(connection, fetch)
	if again.Connection.Attributes["client"] != "mysql" || again.Fetch.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: exchange=%#v fetch=%#v", again.Connection.Attributes, again.Fetch.Connection.Attributes)
	}
	if again.Fetch.Cursor.ID != "cursor-1" || again.Rows[0].CursorID != "cursor-1" {
		t.Fatalf("cursor fetch summary leaked mutation: fetch=%#v rows=%#v", again.Fetch, again.Rows)
	}
	if again.Result.Columns[0].Name != "Cursor_id" || again.ResultSchema.Columns[0].Name != "Cursor_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != "cursor-1" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

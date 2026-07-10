package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientCursorLifecycleReturnsOpenAdvanceCloseRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityForwardOnlyCursor)
	registry := NewMemoryCursorRegistry()
	result := ExecutionRequest{
		Options: ExecutionOptions{RequestID: "req-1", Cursor: CursorForwardOnly, BatchSize: 10, MaxRows: 25},
		Result:  ResultShape{Kind: ResultQuery},
	}.EmptyResult()

	opened := service.OpenClientResultCursor(connection, registry, result)
	openSummary := service.SummarizeClientCursorOpen(connection, opened)
	if !openSummary.Supported() || len(openSummary.Rows) != 1 {
		t.Fatalf("open summary = %#v, want supported row", openSummary)
	}
	openRow := openSummary.Rows[0]
	if openRow.Operation != ClientCursorLifecycleOpen || openRow.CursorID != "req-1" || !openRow.Applied || !openRow.Supported {
		t.Fatalf("open row = %#v, want applied open metadata", openRow)
	}
	if openRow.BatchSize != 10 || openRow.MaxRows != 25 {
		t.Fatalf("open row = %#v, want cursor sizing metadata", openRow)
	}

	advanced := service.AdvanceClientCursor(connection, registry, "req-1", 3, false)
	advanceSummary := service.SummarizeClientCursorAdvance(connection, advanced)
	if !advanceSummary.Supported() || len(advanceSummary.Rows) != 1 {
		t.Fatalf("advance summary = %#v, want supported row", advanceSummary)
	}
	advanceRow := advanceSummary.Rows[0]
	if advanceRow.Operation != ClientCursorLifecycleAdvance || advanceRow.Position != 3 || advanceRow.State != CursorStateOpen || !advanceRow.Applied {
		t.Fatalf("advance row = %#v, want position 3/open", advanceRow)
	}

	closed := service.CloseClientCursor(connection, registry, "req-1")
	closeSummary := service.SummarizeClientCursorClose(connection, closed)
	if !closeSummary.Supported() || len(closeSummary.Rows) != 1 {
		t.Fatalf("close summary = %#v, want supported row", closeSummary)
	}
	closeRow := closeSummary.Rows[0]
	if closeRow.Operation != ClientCursorLifecycleClose || closeRow.State != CursorStateClosed || !closeRow.Applied {
		t.Fatalf("close row = %#v, want closed metadata", closeRow)
	}
	if closeSummary.Result.RowsReturned != 1 || len(closeSummary.ResultSchema.Columns) != 11 {
		t.Fatalf("result/schema = %#v/%#v, want cursor lifecycle result", closeSummary.Result, closeSummary.ResultSchema)
	}
	resultRow := closeSummary.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != string(ClientCursorLifecycleClose) || resultRow[4].Value != string(CursorStateClosed) || resultRow[8].Value != true {
		t.Fatalf("result row = %#v, want close cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientCursorLifecycleReturnsValidationDiagnosticsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	result := ExecutionRequest{
		Options: ExecutionOptions{RequestID: "req-1", Cursor: CursorForwardOnly},
		Result:  ResultShape{Kind: ResultQuery},
	}.EmptyResult()
	opened := service.OpenClientResultCursor(connection, NewMemoryCursorRegistry(), result)

	exchange := service.SummarizeClientCursorOpen(connection, opened)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, validation diagnostics should be row data", exchange)
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].Applied || exchange.Rows[0].Supported {
		t.Fatalf("rows = %#v, want unsupported open row", exchange.Rows)
	}
	if !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.Rows[0].DiagnosticCodes)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result = %#v, want complete summary result", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientCursorLifecycleFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}
	closed := ClientCursorCloseExchange{
		Cursor: CursorDescriptor{ID: "cursor-1", State: CursorStateClosed},
		Closed: true,
	}

	exchange := service.SummarizeClientCursorClose(connection, closed)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block exchange", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceSummarizeClientCursorLifecycleCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	advanced := ClientCursorAdvanceExchange{
		Connection: connection,
		Cursor:     CursorDescriptor{ID: "cursor-1", RequestID: "req-1", Mode: CursorForwardOnly, State: CursorStateOpen, Position: 4},
		Advanced:   true,
	}

	exchange := service.SummarizeClientCursorAdvance(connection, advanced)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Rows[0].CursorID = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.SummarizeClientCursorAdvance(connection, advanced)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].CursorID != "cursor-1" || again.Rows[0].Position != 4 {
		t.Fatalf("cursor lifecycle summary leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Operation" || again.ResultSchema.Columns[0].Name != "Operation" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != "cursor-1" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

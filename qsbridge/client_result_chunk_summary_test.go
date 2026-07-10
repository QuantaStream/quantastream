package qsbridge

import "testing"

func TestPlanningServiceListClientResultChunkSummaryReturnsChunkRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	execution := ExecutionResult{
		RequestID:    "stream-1",
		Status:       ExecutionStreaming,
		Kind:         ResultQuery,
		RowsReturned: 3,
		Cursor: CursorDescriptor{
			ID:    "stream-1",
			State: CursorStateOpen,
		},
		Chunks: []ResultChunk{
			{
				Sequence: 1,
				Rows: []ResultRow{
					{metadataIntCell(1)},
					{metadataIntCell(2)},
				},
			},
			{
				Sequence: 2,
				Rows: []ResultRow{
					{metadataIntCell(3)},
					nil,
				},
				Final: true,
			},
		},
	}

	exchange := service.ListClientResultChunkSummary(connection, execution)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported chunk summary", exchange)
	}
	if len(exchange.Rows) != 2 {
		t.Fatalf("rows = %#v, want two chunk rows", exchange.Rows)
	}
	if exchange.Rows[0].Chunk != 0 || exchange.Rows[0].Sequence != 1 || exchange.Rows[0].Rows != 2 || exchange.Rows[0].Final {
		t.Fatalf("first row = %#v, want first streaming chunk metadata", exchange.Rows[0])
	}
	if exchange.Rows[1].Chunk != 1 || exchange.Rows[1].Sequence != 2 || exchange.Rows[1].Rows != 1 || !exchange.Rows[1].Final {
		t.Fatalf("second row = %#v, want final chunk metadata", exchange.Rows[1])
	}
	if exchange.Rows[0].Cursor != "stream-1" || exchange.Rows[0].CursorState != CursorStateOpen || exchange.Rows[0].RowsReturned != 3 {
		t.Fatalf("row = %#v, want cursor/result state", exchange.Rows[0])
	}
	if exchange.Result.RowsReturned != 2 || len(exchange.ResultSchema.Columns) != 11 {
		t.Fatalf("result/schema = %#v/%#v, want chunk summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[1]
	if resultRow[0].Value != "stream-1" || resultRow[1].Value != 1 || resultRow[3].Value != 1 || resultRow[4].Value != true {
		t.Fatalf("result row = %#v, want second chunk cells", resultRow)
	}
}

func TestPlanningServiceListClientResultChunkSummaryCarriesDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	execution := ExecutionResult{
		RequestID: "stream-1",
		Status:    ExecutionFailed,
		Chunks: []ResultChunk{{
			Sequence: 1,
			Rows:     []ResultRow{{metadataIntCell(1)}},
		}},
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseExecute, "chunk failed"),
		},
	}

	exchange := service.ListClientResultChunkSummary(connection, execution)
	if exchange.Supported() {
		t.Fatalf("expected execution diagnostics to block chunk summary")
	}
	if len(exchange.Rows) != 1 || !containsDiagnosticCode(exchange.Rows[0].DiagnosticCodes, DiagnosticInternalInvariant) {
		t.Fatalf("rows = %#v, want chunk diagnostics", exchange.Rows)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed summary envelope", exchange.Result)
	}
}

func TestPlanningServiceListClientResultChunkSummaryCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	execution := ExecutionResult{
		RequestID:    "stream-1",
		Status:       ExecutionComplete,
		Kind:         ResultQuery,
		RowsReturned: 1,
		Chunks: []ResultChunk{{
			Sequence: 1,
			Final:    true,
			Rows:     []ResultRow{{metadataIntCell(1)}},
		}},
	}

	exchange := service.ListClientResultChunkSummary(connection, execution)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Execution.RequestID = "mutated"
	exchange.Rows[0].RequestID = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientResultChunkSummary(connection, execution)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Execution.RequestID != "stream-1" || again.Rows[0].RequestID != "stream-1" {
		t.Fatalf("chunk summary leaked mutation: execution=%#v rows=%#v", again.Execution, again.Rows)
	}
	if again.Result.Columns[0].Name != "Request_id" || again.ResultSchema.Columns[0].Name != "Request_id" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != "stream-1" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

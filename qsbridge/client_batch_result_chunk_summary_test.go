package qsbridge

import "testing"

func TestPlanningServiceListClientBatchResultChunkSummaryReturnsItemChunkRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	batch := BatchExecutionResult{
		RequestID: "batch-1",
		Status:    ExecutionStreaming,
		Kind:      ResultQuery,
	}.WithItem(ExecutionResult{
		RequestID:    "item-1",
		Status:       ExecutionStreaming,
		Kind:         ResultQuery,
		RowsReturned: 3,
		Cursor:       CursorDescriptor{ID: "item-1", State: CursorStateOpen},
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
				Rows:     []ResultRow{{metadataIntCell(3)}},
				Final:    true,
			},
		},
	}).WithItem(ExecutionResult{
		RequestID:    "item-2",
		Status:       ExecutionComplete,
		Kind:         ResultQuery,
		RowsReturned: 1,
		Chunks: []ResultChunk{{
			Sequence: 1,
			Rows:     []ResultRow{{metadataIntCell(4)}},
			Final:    true,
		}},
	})

	exchange := service.ListClientBatchResultChunkSummary(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported batch chunk summary", exchange)
	}
	if len(exchange.Rows) != 3 {
		t.Fatalf("rows = %#v, want three chunk rows", exchange.Rows)
	}
	if exchange.Rows[0].Item != 0 || exchange.Rows[0].BatchID != "batch-1" || exchange.Rows[0].RequestID != "item-1" || exchange.Rows[0].Rows != 2 || exchange.Rows[0].Final {
		t.Fatalf("first row = %#v, want first item first chunk", exchange.Rows[0])
	}
	if exchange.Rows[1].Item != 0 || exchange.Rows[1].Chunk != 1 || exchange.Rows[1].Sequence != 2 || !exchange.Rows[1].Final {
		t.Fatalf("second row = %#v, want first item final chunk", exchange.Rows[1])
	}
	if exchange.Rows[2].Item != 1 || exchange.Rows[2].RequestID != "item-2" || exchange.Rows[2].RowsReturned != 1 {
		t.Fatalf("third row = %#v, want second item chunk", exchange.Rows[2])
	}
	if exchange.Result.RowsReturned != 3 || len(exchange.ResultSchema.Columns) != 13 {
		t.Fatalf("result/schema = %#v/%#v, want batch chunk summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[1]
	if resultRow[0].Value != 0 || resultRow[2].Value != "item-1" || resultRow[3].Value != 1 || resultRow[6].Value != true {
		t.Fatalf("result row = %#v, want first item final chunk cells", resultRow)
	}
}

func TestPlanningServiceListClientBatchResultChunkSummaryCarriesDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	batch := BatchExecutionResult{
		RequestID: "batch-1",
		Status:    ExecutionFailed,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseExecute, "batch failed"),
		},
	}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Status:    ExecutionFailed,
		Chunks: []ResultChunk{{
			Sequence: 1,
			Rows:     []ResultRow{{metadataIntCell(1)}},
		}},
	})

	exchange := service.ListClientBatchResultChunkSummary(connection, batch)
	if exchange.Supported() {
		t.Fatalf("expected batch diagnostics to block chunk summary")
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].RequestID != "item-1" {
		t.Fatalf("rows = %#v, want item chunk metadata", exchange.Rows)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed summary envelope", exchange.Result)
	}
}

func TestPlanningServiceListClientBatchResultChunkSummaryCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID:    "item-1",
		Status:       ExecutionComplete,
		Kind:         ResultQuery,
		RowsReturned: 1,
		Chunks: []ResultChunk{{
			Rows:  []ResultRow{{metadataIntCell(1)}},
			Final: true,
		}},
	})

	exchange := service.ListClientBatchResultChunkSummary(connection, batch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Batch.Items[0].RequestID = "mutated"
	exchange.Rows[0].RequestID = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][2].Value = "mutated"

	again := service.ListClientBatchResultChunkSummary(connection, batch)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Batch.Items[0].RequestID != "item-1" || again.Rows[0].RequestID != "item-1" {
		t.Fatalf("batch chunk summary leaked mutation: batch=%#v rows=%#v", again.Batch, again.Rows)
	}
	if again.Result.Columns[0].Name != "Item" || again.ResultSchema.Columns[0].Name != "Item" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][2].Value != "item-1" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

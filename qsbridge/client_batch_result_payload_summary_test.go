package qsbridge

import "testing"

func TestPlanningServiceListClientBatchResultPayloadSummaryReturnsItemColumnRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	batch := BatchExecutionResult{
		RequestID: "batch-1",
		Status:    ExecutionComplete,
		Kind:      ResultQuery,
		Complete:  true,
	}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Status:    ExecutionComplete,
		Kind:      ResultQuery,
		Columns: []ResultColumn{
			{Name: "id", Type: DataTypeInt},
			{Name: "name", Type: DataTypeString},
		},
		Chunks: []ResultChunk{{
			Rows: []ResultRow{
				{metadataIntCell(1), metadataStringCell("Abe")},
				{metadataIntCell(2), {Kind: ValueNull}},
			},
			Final: true,
		}},
	}).WithItem(ExecutionResult{
		RequestID: "item-2",
		Status:    ExecutionComplete,
		Kind:      ResultQuery,
		Columns:   []ResultColumn{{Name: "id", Type: DataTypeInt}},
		Chunks: []ResultChunk{{
			Rows: []ResultRow{
				{metadataIntCell(3)},
			},
			Final: true,
		}},
	})

	exchange := service.ListClientBatchResultPayloadSummary(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported batch payload summary", exchange)
	}
	if len(exchange.Rows) != 3 {
		t.Fatalf("rows = %#v, want three item payload rows", exchange.Rows)
	}
	if exchange.Rows[0].Item != 0 || exchange.Rows[0].BatchID != "batch-1" || exchange.Rows[0].RequestID != "item-1" || exchange.Rows[0].ColumnName != "id" {
		t.Fatalf("first row = %#v, want first item id metadata", exchange.Rows[0])
	}
	if exchange.Rows[1].Item != 0 || exchange.Rows[1].ColumnName != "name" || exchange.Rows[1].Cells != 2 || exchange.Rows[1].NullCells != 1 {
		t.Fatalf("second row = %#v, want first item name metadata", exchange.Rows[1])
	}
	if exchange.Rows[2].Item != 1 || exchange.Rows[2].RequestID != "item-2" || exchange.Rows[2].ColumnName != "id" || exchange.Rows[2].Cells != 1 {
		t.Fatalf("third row = %#v, want second item id metadata", exchange.Rows[2])
	}
	if exchange.Result.RowsReturned != 3 || len(exchange.ResultSchema.Columns) != 12 {
		t.Fatalf("result/schema = %#v/%#v, want batch payload summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[1]
	if resultRow[0].Value != 0 || resultRow[2].Value != "item-1" || resultRow[5].Value != "name" || resultRow[11].Value != "null,string" {
		t.Fatalf("result row = %#v, want first item name cells", resultRow)
	}
}

func TestPlanningServiceListClientBatchResultPayloadSummaryCarriesDiagnostics(t *testing.T) {
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
		Columns:   []ResultColumn{{Name: "id", Type: DataTypeInt}},
	})

	exchange := service.ListClientBatchResultPayloadSummary(connection, batch)
	if exchange.Supported() {
		t.Fatalf("expected batch diagnostics to block payload summary")
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].ColumnName != "id" {
		t.Fatalf("rows = %#v, want item payload metadata", exchange.Rows)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed summary envelope", exchange.Result)
	}
}

func TestPlanningServiceListClientBatchResultPayloadSummaryCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Status:    ExecutionComplete,
		Kind:      ResultQuery,
		Columns:   []ResultColumn{{Name: "id", Type: DataTypeInt}},
		Chunks: []ResultChunk{{
			Rows:  []ResultRow{{metadataIntCell(1)}},
			Final: true,
		}},
	})

	exchange := service.ListClientBatchResultPayloadSummary(connection, batch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Batch.Items[0].RequestID = "mutated"
	exchange.Rows[0].ColumnName = "mutated"
	exchange.Rows[0].ValueKinds[0] = ValueString
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][5].Value = "mutated"

	again := service.ListClientBatchResultPayloadSummary(connection, batch)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Batch.Items[0].RequestID != "item-1" || again.Rows[0].ColumnName != "id" || joinValueKinds(again.Rows[0].ValueKinds) != "int" {
		t.Fatalf("batch payload summary leaked mutation: batch=%#v rows=%#v", again.Batch, again.Rows)
	}
	if again.Result.Columns[0].Name != "Item" || again.ResultSchema.Columns[0].Name != "Item" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][5].Value != "id" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

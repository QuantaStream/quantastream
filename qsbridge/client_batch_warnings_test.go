package qsbridge

import "testing"

func TestPlanningServiceListClientBatchWarningsReturnsItemNoticeRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Statement: StatementResult{
			Notices: []StatementNotice{
				{Level: StatementNoticeWarning, Code: "1265", SQLState: "01000", Message: "Data truncated"},
				{Level: StatementNoticeNote, Code: "1003", SQLState: "01000", Message: "Query rewritten"},
			},
		},
	}).WithItem(ExecutionResult{
		RequestID: "item-2",
		Statement: StatementResult{
			Notices: []StatementNotice{
				{Level: StatementNoticeError, Code: "1105", SQLState: "HY000", Message: "Unknown error"},
			},
		},
	})

	exchange := service.ListClientBatchWarnings(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported batch warning metadata", exchange)
	}
	if exchange.WarningCount != 3 || len(exchange.Rows) != 3 {
		t.Fatalf("count/rows = %d/%#v, want three warning details", exchange.WarningCount, exchange.Rows)
	}
	if exchange.Rows[0].Item != 0 || exchange.Rows[0].RequestID != "item-1" || exchange.Rows[0].Level != StatementNoticeWarning {
		t.Fatalf("first row = %#v, want item-1 warning", exchange.Rows[0])
	}
	if exchange.Rows[2].Item != 1 || exchange.Rows[2].RequestID != "item-2" || exchange.Rows[2].Level != StatementNoticeError {
		t.Fatalf("third row = %#v, want item-2 error", exchange.Rows[2])
	}
	if exchange.Result.RowsReturned != 3 || len(exchange.ResultSchema.Columns) != 6 {
		t.Fatalf("result/schema = %#v/%#v, want batch warning rows", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[2]
	if resultRow[0].Value != 1 || resultRow[1].Value != "item-2" || resultRow[2].Value != "error" || resultRow[5].Value != "Unknown error" {
		t.Fatalf("result row = %#v, want item-2 error cells", resultRow)
	}
}

func TestPlanningServiceListClientBatchWarningsPreservesCountWithoutDetailRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Statement: StatementResult{Warnings: 2},
	})

	exchange := service.ListClientBatchWarnings(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported count-only warning metadata", exchange)
	}
	if exchange.WarningCount != 2 || len(exchange.Rows) != 0 || exchange.Result.RowsReturned != 0 {
		t.Fatalf("exchange/result = %#v/%#v, want count without detail rows", exchange, exchange.Result)
	}
}

func TestPlanningServiceListClientBatchWarningsCarriesDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	batch := BatchExecutionResult{
		RequestID: "batch-1",
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseExecute, "batch failed"),
		},
	}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Statement: StatementResult{
			Notices: []StatementNotice{{Level: StatementNoticeWarning, Message: "warning"}},
		},
	})

	exchange := service.ListClientBatchWarnings(connection, batch)
	if exchange.Supported() {
		t.Fatalf("expected batch diagnostics to block warning metadata")
	}
	if len(exchange.Rows) != 1 || exchange.Rows[0].RequestID != "item-1" {
		t.Fatalf("rows = %#v, want warning rows retained as metadata", exchange.Rows)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed warning envelope", exchange.Result)
	}
}

func TestPlanningServiceListClientBatchWarningsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Statement: StatementResult{
			Notices: []StatementNotice{{Level: StatementNoticeWarning, Message: "original"}},
		},
	})

	exchange := service.ListClientBatchWarnings(connection, batch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Batch.Items[0].RequestID = "mutated"
	exchange.Batch.Items[0].Statement.Notices[0].Message = "mutated"
	exchange.Rows[0].Message = "mutated"
	exchange.Result.Chunks[0].Rows[0][5].Value = "mutated"

	again := service.ListClientBatchWarnings(connection, batch)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Batch.Items[0].RequestID != "item-1" || again.Batch.Items[0].Statement.Notices[0].Message != "original" {
		t.Fatalf("batch warning metadata leaked mutation: %#v", again.Batch)
	}
	if again.Rows[0].Message != "original" || again.Result.Chunks[0].Rows[0][5].Value != "original" {
		t.Fatalf("warning rows leaked mutation: %#v/%#v", again.Rows, again.Result.Chunks)
	}
}

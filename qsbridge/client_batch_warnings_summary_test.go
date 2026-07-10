package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientBatchWarningsReturnsItemNoticeCounts(t *testing.T) {
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

	exchange := service.SummarizeClientBatchWarnings(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported batch warning summary", exchange)
	}
	row := exchange.Row
	if row.WarningCount != 3 || row.DetailRowCount != 3 || row.WarningRows != 1 || row.NoteRows != 1 || row.ErrorRows != 1 {
		t.Fatalf("row = %#v, want warning/note/error counts", row)
	}
	if row.CodedRows != 3 || row.SQLStateRows != 3 || row.ItemsWithDetails != 2 || row.DistinctRequestIDs != 2 {
		t.Fatalf("row = %#v, want coded/state/item/request counts", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 9 {
		t.Fatalf("result/schema = %#v/%#v, want batch warning summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 3 || resultRow[2].Value != 1 || resultRow[7].Value != 2 {
		t.Fatalf("result row = %#v, want batch warning summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientBatchWarningsPreservesCountWithoutDetailRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Statement: StatementResult{Warnings: 2},
	})

	exchange := service.SummarizeClientBatchWarnings(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported count-only warning summary", exchange)
	}
	if exchange.Row.WarningCount != 2 || exchange.Row.DetailRowCount != 0 || exchange.Row.ItemsWithDetails != 0 {
		t.Fatalf("row = %#v, want warning count without detail rows", exchange.Row)
	}
}

func TestPlanningServiceSummarizeClientBatchWarningsCarriesDiagnostics(t *testing.T) {
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

	exchange := service.SummarizeClientBatchWarnings(connection, batch)
	if exchange.Supported() {
		t.Fatalf("expected batch diagnostics to block warning summary")
	}
	if exchange.Row.WarningCount != 1 || exchange.Row.DetailRowCount != 1 {
		t.Fatalf("row = %#v, want warning rows retained as metadata", exchange.Row)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed warning summary envelope", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientBatchWarningsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Statement: StatementResult{
			Notices: []StatementNotice{{Level: StatementNoticeWarning, Message: "original"}},
		},
	})

	exchange := service.SummarizeClientBatchWarnings(connection, batch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Batch.Items[0].RequestID = "mutated"
	exchange.Batch.Items[0].Statement.Notices[0].Message = "mutated"
	exchange.Row.WarningCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientBatchWarnings(connection, batch)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Batch.Items[0].RequestID != "item-1" || again.Batch.Items[0].Statement.Notices[0].Message != "original" {
		t.Fatalf("batch warning summary metadata leaked mutation: %#v", again.Batch)
	}
	if again.Row.WarningCount != 1 || again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("warning summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}

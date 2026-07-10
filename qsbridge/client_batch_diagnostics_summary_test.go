package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientBatchDiagnosticsReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey"}
	duplicate := ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "missing table")
	batch := BatchExecutionResult{
		RequestID: "batch-1",
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "batch failed"),
			duplicate,
		},
	}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Diagnostics: DiagnosticSet{
			duplicate,
			{
				Code:     DiagnosticUnsupportedPredicate,
				Severity: SeverityWarning,
				Phase:    PhaseClassify,
				Message:  "residual predicate",
				Span:     SourceSpan{StartLine: 2, StartCol: 7, EndLine: 2, EndCol: 24},
				Fields:   []FieldRef{orderKey},
			},
		},
	}).WithItem(ExecutionResult{
		RequestID: "item-2",
		Diagnostics: DiagnosticSet{{
			Code:     DiagnosticNativeBlocker,
			Severity: SeverityInfo,
			Phase:    PhasePlan,
			Message:  "note",
		}},
	})

	exchange := service.SummarizeClientBatchDiagnostics(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, supplied batch diagnostics should not block summary result", exchange)
	}
	row := exchange.Row
	if row.DiagnosticCount != 4 || row.BatchCount != 1 || row.ItemCount != 3 {
		t.Fatalf("row = %#v, want deduplicated batch and item counts", row)
	}
	if row.ErrorCount != 2 || row.WarningCount != 1 || row.InfoCount != 1 || row.FieldRowCount != 1 || row.SpannedRowCount != 1 {
		t.Fatalf("row = %#v, want severity, field, and span counts", row)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 8 {
		t.Fatalf("result/schema = %#v/%#v, want batch diagnostic summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 4 || resultRow[1].Value != 1 || resultRow[2].Value != 3 || resultRow[4].Value != 1 {
		t.Fatalf("result row = %#v, want batch diagnostic summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientBatchDiagnosticsSupportsEmptyBatch(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.SummarizeClientBatchDiagnostics(connection, BatchExecutionResult{})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported empty batch diagnostic summary", exchange)
	}
	if exchange.Row.DiagnosticCount != 0 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("row/result = %#v/%#v, want empty summary row", exchange.Row, exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientBatchDiagnosticsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}
	batch := BatchExecutionResult{
		RequestID:   "batch-1",
		Diagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "missing table")},
	}

	exchange := service.SummarizeClientBatchDiagnostics(connection, batch)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block exchange", exchange)
	}
	if exchange.Row.DiagnosticCount != 0 || exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 8 {
		t.Fatalf("row/result/schema = %#v/%#v/%#v, want failed summary envelope", exchange.Row, exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientBatchDiagnosticsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	batch := BatchExecutionResult{RequestID: "batch-1"}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Diagnostics: DiagnosticSet{{
			Code:     DiagnosticUnsupportedPredicate,
			Severity: SeverityWarning,
			Phase:    PhaseClassify,
			Message:  "original",
			Fields:   []FieldRef{{Table: orders, Name: "o_orderkey"}},
		}},
	})

	exchange := service.SummarizeClientBatchDiagnostics(connection, batch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Batch.Items[0].Diagnostics[0].Message = "mutated"
	exchange.Row.DiagnosticCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientBatchDiagnostics(connection, batch)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Batch.Items[0].Diagnostics[0].Message != "original" {
		t.Fatalf("batch diagnostic metadata leaked mutation: %#v", again.Batch)
	}
	if again.Row.DiagnosticCount != 1 || again.Row.WarningCount != 1 || again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("batch diagnostic summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}

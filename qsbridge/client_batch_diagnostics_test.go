package qsbridge

import "testing"

func TestPlanningServiceListClientBatchDiagnosticsReturnsBatchAndItemRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey"}
	batch := BatchExecutionResult{
		RequestID: "batch-1",
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "batch failed"),
		},
	}.WithItem(ExecutionResult{
		RequestID: "item-1",
		Diagnostics: DiagnosticSet{{
			Code:     DiagnosticUnsupportedPredicate,
			Severity: SeverityWarning,
			Phase:    PhaseClassify,
			Message:  "residual predicate",
			Span:     SourceSpan{StartLine: 2, StartCol: 7, EndLine: 2, EndCol: 24},
			Fields:   []FieldRef{orderKey},
		}},
	})

	exchange := service.ListClientBatchDiagnostics(connection, batch)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, supplied diagnostics should not block batch diagnostic-list result", exchange)
	}
	if len(exchange.Rows) != 2 {
		t.Fatalf("rows = %#v, want batch and item diagnostics", exchange.Rows)
	}
	if exchange.Rows[0].Scope != ClientBatchDiagnosticScopeBatch || exchange.Rows[0].Item != -1 || exchange.Rows[0].RequestID != "batch-1" {
		t.Fatalf("first row = %#v, want batch diagnostic row", exchange.Rows[0])
	}
	if exchange.Rows[0].SQLState != SQLStateGeneralError || exchange.Rows[0].VendorCode != mysqlErrorUnknown {
		t.Fatalf("first protocol mapping = %#v, want generic execution option mapping", exchange.Rows[0])
	}
	second := exchange.Rows[1]
	if second.Scope != ClientBatchDiagnosticScopeItem || second.Item != 0 || second.RequestID != "item-1" {
		t.Fatalf("second row = %#v, want item diagnostic row", second)
	}
	if second.Severity != SeverityWarning || second.Code != DiagnosticUnsupportedPredicate || len(second.Fields) != 1 || second.Fields[0].Name != "o_orderkey" {
		t.Fatalf("second row = %#v, want warning field metadata", second)
	}
	if exchange.Result.RowsReturned != 2 || len(exchange.ResultSchema.Columns) != 14 {
		t.Fatalf("result/schema = %#v/%#v, want batch diagnostic result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[1]
	if resultRow[0].Value != string(ClientBatchDiagnosticScopeItem) || resultRow[1].Value != 0 || resultRow[6].Value != string(DiagnosticUnsupportedPredicate) || resultRow[13].Value != "o.o_orderkey" {
		t.Fatalf("result row = %#v, want item diagnostic cells", resultRow)
	}
}

func TestPlanningServiceListClientBatchDiagnosticsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}
	batch := BatchExecutionResult{
		RequestID: "batch-1",
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "missing table"),
		},
	}

	exchange := service.ListClientBatchDiagnostics(connection, batch)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block exchange", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientBatchDiagnosticsCopiesMutableState(t *testing.T) {
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

	exchange := service.ListClientBatchDiagnostics(connection, batch)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Batch.Items[0].Diagnostics[0].Message = "mutated"
	exchange.Rows[0].Fields[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][8].Value = "mutated"

	again := service.ListClientBatchDiagnostics(connection, batch)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Batch.Items[0].Diagnostics[0].Message != "original" || again.Rows[0].Fields[0].Name != "o_orderkey" {
		t.Fatalf("batch diagnostic metadata leaked mutation: batch=%#v rows=%#v", again.Batch, again.Rows)
	}
	if again.Result.Columns[0].Name != "Scope" || again.ResultSchema.Columns[0].Name != "Scope" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][8].Value != "unsupported_predicate: original" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

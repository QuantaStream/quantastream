package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientDiagnosticsReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey"}
	diagnostics := DiagnosticSet{
		ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "missing table"),
		{
			Code:     DiagnosticUnsupportedPredicate,
			Severity: SeverityWarning,
			Phase:    PhaseClassify,
			Message:  "residual predicate",
			Span:     SourceSpan{StartLine: 2, StartCol: 7, EndLine: 2, EndCol: 24},
			Fields:   []FieldRef{orderKey},
		},
		{
			Code:     DiagnosticNativeBlocker,
			Severity: SeverityInfo,
			Phase:    PhasePlan,
			Message:  "note",
		},
	}

	exchange := service.SummarizeClientDiagnostics(connection, diagnostics)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported diagnostic summary", exchange)
	}
	row := exchange.Row
	if row.DiagnosticCount != 3 || row.ErrorCount != 1 || row.WarningCount != 1 || row.InfoCount != 1 {
		t.Fatalf("row = %#v, want severity counts", row)
	}
	if row.FieldRowCount != 1 || row.SpannedRowCount != 1 {
		t.Fatalf("row = %#v, want field/span counts", row)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 6 {
		t.Fatalf("result/schema = %#v/%#v, want diagnostic summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 3 || resultRow[1].Value != 1 || resultRow[2].Value != 1 || resultRow[4].Value != 1 {
		t.Fatalf("result row = %#v, want diagnostic summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientDiagnosticsSupportsEmptyDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.SummarizeClientDiagnostics(connection, nil)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported empty diagnostic summary", exchange)
	}
	if exchange.Row.DiagnosticCount != 0 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("row/result = %#v/%#v, want empty diagnostic summary row", exchange.Row, exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientDiagnosticsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientDiagnostics(connection, DiagnosticSet{
		ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "missing table"),
	})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block exchange", exchange)
	}
	if exchange.Row.DiagnosticCount != 0 || exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 6 {
		t.Fatalf("row/result/schema = %#v/%#v/%#v, want failed summary envelope", exchange.Row, exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientDiagnosticsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	diagnostics := DiagnosticSet{{
		Code:     DiagnosticUnsupportedPredicate,
		Severity: SeverityWarning,
		Phase:    PhaseClassify,
		Message:  "original",
		Fields:   []FieldRef{{Table: orders, Name: "o_orderkey"}},
	}}

	exchange := service.SummarizeClientDiagnostics(connection, diagnostics)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Diagnostics[0].Message = "mutated"
	exchange.Row.DiagnosticCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientDiagnostics(connection, diagnostics)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Diagnostics[0].Message != "original" {
		t.Fatalf("diagnostic metadata leaked mutation: %#v", again.Diagnostics)
	}
	if again.Row.DiagnosticCount != 1 || again.Row.WarningCount != 1 || again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("diagnostic summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}

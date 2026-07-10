package qsbridge

import "testing"

func TestPlanningServiceListClientDiagnosticsReturnsDiagnosticRows(t *testing.T) {
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
	}

	exchange := service.ListClientDiagnostics(connection, diagnostics)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported diagnostic rows", exchange)
	}
	if len(exchange.Rows) != 2 || len(exchange.Diagnostics) != 2 {
		t.Fatalf("exchange = %#v, want two copied diagnostics", exchange)
	}
	if exchange.Rows[0].SQLState != SQLStateBaseTableNotFound || exchange.Rows[0].VendorCode != mysqlErrorTableNotFound {
		t.Fatalf("first row = %#v, want table-not-found protocol mapping", exchange.Rows[0])
	}
	if len(exchange.ResultSchema.Columns) != 11 || exchange.ResultSchema.Columns[0].Name != "Level" || exchange.ResultSchema.Columns[10].Name != "Fields" {
		t.Fatalf("schema = %#v, want diagnostic result schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 2 {
		t.Fatalf("result = %#v, want two diagnostic rows", exchange.Result)
	}
	second := exchange.Result.Chunks[0].Rows[1]
	if second[0].Value != string(SeverityWarning) || second[3].Value != string(DiagnosticUnsupportedPredicate) || second[6].Value != 2 || second[10].Value != "o.o_orderkey" {
		t.Fatalf("second result row = %#v, want warning field metadata", second)
	}
}

func TestPlanningServiceListClientDiagnosticsTreatsSuppliedErrorsAsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	diagnostics := DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "bad option"),
	}

	exchange := service.ListClientDiagnostics(connection, diagnostics)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, supplied diagnostics should not block diagnostic-list result", exchange)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result = %#v, want complete diagnostic-list result", exchange.Result)
	}
}

func TestPlanningServiceListClientDiagnosticsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.ListClientDiagnostics(connection, DiagnosticSet{
		ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "missing table"),
	})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block exchange", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServiceListClientDiagnosticsCopiesMutableState(t *testing.T) {
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

	exchange := service.ListClientDiagnostics(connection, diagnostics)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Diagnostics[0].Message = "mutated"
	exchange.Rows[0].Fields[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][5].Value = "mutated"

	again := service.ListClientDiagnostics(connection, diagnostics)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Diagnostics[0].Message != "original" || again.Rows[0].Fields[0].Name != "o_orderkey" {
		t.Fatalf("diagnostic metadata leaked mutation: diagnostics=%#v rows=%#v", again.Diagnostics, again.Rows)
	}
	if again.Result.Columns[0].Name != "Level" || again.ResultSchema.Columns[0].Name != "Level" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][5].Value != "unsupported_predicate: original" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

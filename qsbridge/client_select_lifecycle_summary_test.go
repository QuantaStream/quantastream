package qsbridge

import "testing"

func TestPlanningServicePrepareClientSelectLifecycleSummaryBuildsSelectRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	lifecycle := testPreparedSelectPlan(t).SimpleSelectLifecycle()

	exchange := service.PrepareClientSelectLifecycleSummary(connection, lifecycle)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported SELECT lifecycle summary", exchange)
	}
	if len(exchange.Rows) != len(lifecycle.Steps) {
		t.Fatalf("rows = %d, want %d lifecycle stages", len(exchange.Rows), len(lifecycle.Steps))
	}
	queryIR := clientSelectLifecycleRowByStage(t, exchange.Rows, SelectLifecycleQueryIR)
	if queryIR.Kind != QueryKindSelect || queryIR.RequiredFields != 2 || queryIR.ResultColumns != 1 {
		t.Fatalf("query IR row = %#v, want SELECT field and result counts", queryIR)
	}
	physical := clientSelectLifecycleRowByStage(t, exchange.Rows, SelectLifecyclePhysicalPlan)
	if physical.PhysicalRoot == "" || !physical.Complete || !physical.Supported {
		t.Fatalf("physical row = %#v, want supported physical root", physical)
	}
	if len(exchange.ResultSchema.Columns) != 12 || exchange.ResultSchema.Columns[0].Name != "Stage" {
		t.Fatalf("schema = %#v, want SELECT lifecycle schema", exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[2]
	if resultRow[0].Value != string(SelectLifecycleQueryIR) || resultRow[1].Value != string(QueryKindSelect) || resultRow[7].Value != 1 {
		t.Fatalf("result row = %#v, want query IR SELECT cells", resultRow)
	}
}

func TestPlanningServicePrepareClientSelectLifecycleSummaryKeepsLifecycleDiagnosticsAsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	lifecycle := testDeletePlanForSelectLifecycle(t).SimpleSelectLifecycle()

	exchange := service.PrepareClientSelectLifecycleSummary(connection, lifecycle)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported lifecycle for mutation", exchange)
	}
	if len(exchange.Rows) != len(lifecycle.Steps) || exchange.Result.RowsReturned != uint64(len(lifecycle.Steps)) {
		t.Fatalf("rows/result = %#v/%#v, want lifecycle diagnostics as row data", exchange.Rows, exchange.Result)
	}
	diagnostics := clientSelectLifecycleRowByStage(t, exchange.Rows, SelectLifecycleDiagnostics)
	if diagnostics.Supported || !containsDiagnosticCode(diagnostics.DiagnosticCodes, DiagnosticUnsupportedSQL) {
		t.Fatalf("diagnostics row = %#v, want unsupported SQL diagnostic", diagnostics)
	}
}

func TestPlanningServicePrepareClientSelectLifecycleSummaryFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.PrepareClientSelectLifecycleSummary(connection, testPreparedSelectPlan(t).SimpleSelectLifecycle())
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block exchange", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServicePrepareClientSelectLifecycleSummaryCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	lifecycle := testPreparedSelectPlan(t).SimpleSelectLifecycle()

	exchange := service.PrepareClientSelectLifecycleSummary(connection, lifecycle)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Lifecycle.Sources[0] = "mutated"
	exchange.Lifecycle.Steps[0].Diagnostics = []DiagnosticCode{DiagnosticNativeBlocker}
	exchange.Rows[0].Detail = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.PrepareClientSelectLifecycleSummary(connection, lifecycle)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Lifecycle.Sources[0] != "quanta.orders.as o" || len(again.Lifecycle.Steps[0].Diagnostics) != 0 {
		t.Fatalf("lifecycle leaked mutation: %#v", again.Lifecycle)
	}
	if again.Rows[0].Detail == "mutated" {
		t.Fatalf("rows leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Stage" || again.ResultSchema.Columns[0].Name != "Stage" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != string(SelectLifecycleParse) {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func clientSelectLifecycleRowByStage(t *testing.T, rows []ClientSelectLifecycleSummaryRow, stage SelectLifecycleStage) ClientSelectLifecycleSummaryRow {
	t.Helper()
	for _, row := range rows {
		if row.Stage == stage {
			return row
		}
	}
	t.Fatalf("stage %s not found in %#v", stage, rows)
	return ClientSelectLifecycleSummaryRow{}
}

func testDeletePlanForSelectLifecycle(t *testing.T) PlanResult {
	t.Helper()
	parser := stubParserBridge{statement: UnboundStatement{
		SQL:  "delete from orders where o_orderkey = ?",
		Kind: QueryKindDelete,
		Delete: UnboundDelete{
			Table: UnboundTable{Name: "orders"},
			Predicates: []UnboundPredicate{{
				Expr: UnboundBinary(
					BinaryOpEqual,
					UnboundField("", "o_orderkey"),
					UnboundParameter(1, DataTypeInt),
				),
			}},
			Result: ResultShape{Kind: ResultStatement},
		},
	}}
	return Planner{Parser: parser, Catalog: testBindCatalog(), DefaultSchema: "quanta"}.Plan(parser.statement.SQL)
}

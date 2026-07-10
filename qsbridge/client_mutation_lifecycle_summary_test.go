package qsbridge

import "testing"

func TestPlanningServicePrepareClientMutationLifecycleSummaryBuildsUpdateRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	lifecycle := testUpdateMutationLifecycle(t)

	exchange := service.PrepareClientMutationLifecycleSummary(connection, lifecycle)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported mutation lifecycle summary", exchange)
	}
	if len(exchange.Rows) != len(lifecycle.Steps) {
		t.Fatalf("rows = %d, want %d lifecycle stages", len(exchange.Rows), len(lifecycle.Steps))
	}
	queryIR := clientMutationLifecycleRowByStage(t, exchange.Rows, MutationLifecycleQueryIR)
	if queryIR.Kind != QueryKindUpdate || queryIR.Mutation != MutationUpdate || queryIR.AccessIntent != PhysicalAccessWrite {
		t.Fatalf("query IR row = %#v, want update/write metadata", queryIR)
	}
	if queryIR.Target != "quanta.orders.as o" || queryIR.Assignments != 1 || queryIR.Predicates != 1 || queryIR.ParameterCount != 2 {
		t.Fatalf("query IR row = %#v, want target and mutation counts", queryIR)
	}
	if len(exchange.ResultSchema.Columns) != 16 || exchange.ResultSchema.Columns[0].Name != "Stage" {
		t.Fatalf("schema = %#v, want mutation lifecycle schema", exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[2]
	if resultRow[0].Value != string(MutationLifecycleQueryIR) || resultRow[2].Value != string(MutationUpdate) || resultRow[3].Value != string(PhysicalAccessWrite) {
		t.Fatalf("result row = %#v, want query IR update/write cells", resultRow)
	}
}

func TestPlanningServicePrepareClientMutationLifecycleSummaryKeepsLifecycleDiagnosticsAsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	lifecycle := testPreparedSelectPlan(t).MutationLifecycle()

	exchange := service.PrepareClientMutationLifecycleSummary(connection, lifecycle)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported lifecycle for SELECT", exchange)
	}
	if len(exchange.Rows) != len(lifecycle.Steps) || exchange.Result.RowsReturned != uint64(len(lifecycle.Steps)) {
		t.Fatalf("rows/result = %#v/%#v, want lifecycle diagnostics as row data", exchange.Rows, exchange.Result)
	}
	diagnostics := clientMutationLifecycleRowByStage(t, exchange.Rows, MutationLifecycleDiagnostics)
	if diagnostics.Supported || !containsDiagnosticCode(diagnostics.DiagnosticCodes, DiagnosticUnsupportedSQL) {
		t.Fatalf("diagnostics row = %#v, want unsupported SQL diagnostic", diagnostics)
	}
}

func TestPlanningServicePrepareClientMutationLifecycleSummaryFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.PrepareClientMutationLifecycleSummary(connection, testUpdateMutationLifecycle(t))
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block exchange", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.Rows) != 0 {
		t.Fatalf("result = %#v rows=%#v, want failed rowless exchange", exchange.Result, exchange.Rows)
	}
}

func TestPlanningServicePrepareClientMutationLifecycleSummaryCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	lifecycle := testUpdateMutationLifecycle(t)

	exchange := service.PrepareClientMutationLifecycleSummary(connection, lifecycle)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Lifecycle.Columns[0] = "mutated"
	exchange.Lifecycle.Steps[0].Diagnostics = []DiagnosticCode{DiagnosticNativeBlocker}
	exchange.Rows[0].Target = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.PrepareClientMutationLifecycleSummary(connection, lifecycle)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Lifecycle.Columns[0] != "o.o_totalprice" || len(again.Lifecycle.Steps[0].Diagnostics) != 0 {
		t.Fatalf("lifecycle leaked mutation: %#v", again.Lifecycle)
	}
	if again.Rows[0].Target != "quanta.orders.as o" {
		t.Fatalf("rows leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Stage" || again.ResultSchema.Columns[0].Name != "Stage" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != string(MutationLifecycleParse) {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func testUpdateMutationLifecycle(t *testing.T) MutationLifecycle {
	t.Helper()
	parser := stubParserBridge{statement: UnboundStatement{
		SQL:  "update orders set o_totalprice = ? where o_orderkey = ?",
		Kind: QueryKindUpdate,
		Update: UnboundUpdate{
			Table: UnboundTable{Name: "orders", Alias: "o"},
			Assignments: []UnboundAssignment{{
				Column: "o_totalprice",
				Value:  UnboundParameter(1, DataTypeFloat),
			}},
			Predicates: []UnboundPredicate{{
				Expr: UnboundBinary(
					BinaryOpEqual,
					UnboundField("o", "o_orderkey"),
					UnboundParameter(2, DataTypeInt),
				),
				Placement: PredicatePushdown,
				Scope:     PredicateScopeWhere,
			}},
			Result: ResultShape{
				Kind: ResultStatement,
				Statement: StatementResult{
					AffectedRows: 1,
					Status:       "Rows matched: 1",
				},
			},
		},
	}}
	planner := Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementPrimary, Cache: CacheQuery},
	}
	lifecycle := planner.Plan("update orders set o_totalprice = ? where o_orderkey = ?").MutationLifecycle()
	if !lifecycle.Supported {
		t.Fatalf("lifecycle = %#v, want supported update", lifecycle)
	}
	return lifecycle
}

func clientMutationLifecycleRowByStage(t *testing.T, rows []ClientMutationLifecycleSummaryRow, stage MutationLifecycleStage) ClientMutationLifecycleSummaryRow {
	t.Helper()
	for _, row := range rows {
		if row.Stage == stage {
			return row
		}
	}
	t.Fatalf("stage %s not found in %#v", stage, rows)
	return ClientMutationLifecycleSummaryRow{}
}

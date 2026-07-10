package qsbridge

import "testing"

func TestPlanningServicePrepareClientOptimizerTraceBuildsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	part := TableInstance{ID: "part", Table: "part", Alias: "p"}
	partName := FieldRef{Table: part, Name: "p_name", Index: IndexBackingString}
	trace := NewOptimizationTrace()
	applied := RewriteAppliedRecord(
		RewritePredicatePushdown,
		"moved predicate",
		"filter(residual)",
		"filter(pushdown)",
	)
	applied.Capabilities = []PlanCapability{CapabilityEncodingEquality}
	trace.Add(applied)
	trace.Add(RewriteAdvisoryRecord(
		RewriteHiddenProjection,
		"add hidden field",
		partName,
	))

	exchange := service.PrepareClientOptimizerTrace(connection, trace)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported optimizer trace", exchange)
	}
	if len(exchange.Rows) != 2 || len(exchange.Trace.Rewrites) != 2 {
		t.Fatalf("exchange = %#v, want copied rewrite rows", exchange)
	}
	if exchange.Rows[0].Rule != RewritePredicatePushdown || exchange.Rows[0].Status != RewriteApplied {
		t.Fatalf("first row = %#v, want applied predicate pushdown", exchange.Rows[0])
	}
	if len(exchange.ResultSchema.Columns) != 10 || exchange.ResultSchema.Columns[0].Name != "Rule" || exchange.ResultSchema.Columns[9].Name != "Fields" {
		t.Fatalf("schema = %#v, want optimizer trace columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 2 {
		t.Fatalf("result = %#v, want two optimizer rows", exchange.Result)
	}
	first := exchange.Result.Chunks[0].Rows[0]
	if first[0].Value != string(RewritePredicatePushdown) || first[1].Value != string(RewriteApplied) ||
		first[2].Value != string(RewriteCategoryCompatibility) || first[3].Value != string(RewriteImpactLogicalShape) ||
		first[7].Value != string(CapabilityEncodingEquality) {
		t.Fatalf("first result row = %#v, want applied rewrite metadata", first)
	}
	second := exchange.Result.Chunks[0].Rows[1]
	if second[2].Value != string(RewriteCategoryPerformance) || second[3].Value != string(RewriteImpactDiagnosticsOnly) ||
		second[9].Value != "p.p_name" {
		t.Fatalf("second result row = %#v, want field list", second)
	}
}

func TestPlanningServicePrepareClientOptimizerTraceIncludesDetectedTopNAdvisory(t *testing.T) {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal},
	}
	service := NewPlanningService(planner, nil)
	connection := clientStatementConnection()

	plan := planner.Plan("select l.l_shipmode as ship_mode, count(*) as line_count from lineitem as l group by l.l_shipmode order by line_count desc limit 5")
	if plan.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", plan.Diagnostics)
	}
	exchange := service.PrepareClientOptimizerTrace(connection, plan.Inspection.Optimization)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported optimizer trace", exchange)
	}
	rowIndex := -1
	for i, record := range exchange.Rows {
		if record.Rule == RewriteTopNGroupedCount {
			rowIndex = i
			break
		}
	}
	if rowIndex == -1 {
		t.Fatalf("optimizer rows = %#v, want topn advisory", exchange.Rows)
	}
	row := exchange.Result.Chunks[0].Rows[rowIndex]
	if row[0].Value != string(RewriteTopNGroupedCount) || row[1].Value != string(RewriteAdvisory) ||
		row[7].Value != string(CapabilityNativeTopN) || row[9].Value != "l.l_shipmode" {
		t.Fatalf("optimizer result row = %#v, want topn advisory metadata", row)
	}
}

func TestPlanningServicePrepareClientOptimizerTraceReturnsFailedEnvelopeForBlockingDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	trace := NewOptimizationTrace()
	trace.Add(RewriteBlockedRecord(
		RewriteOuterJoinBoundary,
		"outer join boundary",
		DiagnosticSet{ErrorDiagnostic(DiagnosticOuterJoin, PhasePlan, "cannot cross null-extension boundary")},
	))

	exchange := service.PrepareClientOptimizerTrace(connection, trace)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocking optimizer diagnostics", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticOuterJoin) {
		t.Fatalf("diagnostics = %#v, want outer join diagnostic", exchange.Diagnostics)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("result/schema = %#v/%#v, want failed optimizer trace envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServicePrepareClientOptimizerTraceCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	part := TableInstance{ID: "part", Table: "part", Alias: "p"}
	partName := FieldRef{Table: part, Name: "p_name", Index: IndexBackingString}
	trace := NewOptimizationTrace()
	trace.Add(RewriteAdvisoryRecord(RewriteHiddenProjection, "add hidden field", partName))

	exchange := service.PrepareClientOptimizerTrace(connection, trace)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Trace.Rewrites[0].Fields[0].Name = "mutated"
	exchange.Rows[0].Fields[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][9].Value = "mutated"

	again := service.PrepareClientOptimizerTrace(connection, trace)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Trace.Rewrites[0].Fields[0].Name != "p_name" || again.Rows[0].Fields[0].Name != "p_name" {
		t.Fatalf("optimizer trace leaked mutation: trace=%#v rows=%#v", again.Trace, again.Rows)
	}
	if again.Result.Columns[0].Name != "Rule" || again.ResultSchema.Columns[0].Name != "Rule" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][9].Value != "p.p_name" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

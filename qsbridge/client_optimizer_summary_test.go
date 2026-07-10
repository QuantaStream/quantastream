package qsbridge

import "testing"

func TestPlanningServicePrepareClientOptimizerSummaryBuildsOneSummaryRow(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	trace := NewOptimizationTrace()
	trace.Add(RewriteAppliedRecord(RewritePredicatePushdown, "moved predicate", "filter(residual)", "filter(pushdown)"))
	trace.Add(RewriteAdvisoryRecord(RewriteHiddenProjection, "add hidden field"))
	trace.Add(RewriteRecord{
		Rule:     RewriteJoinReorder,
		Status:   RewriteSkipped,
		Category: RewriteCategoryPhysical,
		Impact:   RewriteImpactPhysicalShape,
		Reason:   "single-table query",
	})

	exchange := service.PrepareClientOptimizerSummary(connection, trace)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported optimizer summary", exchange)
	}
	if !exchange.Summary.Supported || exchange.Summary.Total != 3 || exchange.Summary.Applied != 1 ||
		exchange.Summary.Advisory != 1 || exchange.Summary.Skipped != 1 {
		t.Fatalf("summary = %#v, want aggregate rewrite counts", exchange.Summary)
	}
	if exchange.Summary.Compatibility != 1 || exchange.Summary.Performance != 1 || exchange.Summary.Physical != 1 {
		t.Fatalf("summary = %#v, want category counts", exchange.Summary)
	}
	if exchange.Summary.LogicalImpact != 1 || exchange.Summary.PhysicalImpact != 1 || exchange.Summary.DiagnosticOnly != 1 {
		t.Fatalf("summary = %#v, want impact counts", exchange.Summary)
	}
	if len(exchange.ResultSchema.Columns) != 16 || exchange.ResultSchema.Columns[0].Name != "Supported" {
		t.Fatalf("schema = %#v, want optimizer summary columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 1 || exchange.Result.Chunks[0].Rows[0][1].Value != 3 {
		t.Fatalf("result = %#v, want one optimizer summary row", exchange.Result)
	}
}

func TestPlanningServicePrepareClientOptimizerSummaryReturnsFailedEnvelopeForBlockingDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	trace := NewOptimizationTrace()
	trace.Add(RewriteBlockedRecord(
		RewriteOuterJoinBoundary,
		"outer join boundary",
		DiagnosticSet{ErrorDiagnostic(DiagnosticOuterJoin, PhasePlan, "cannot cross null-extension boundary")},
	))

	exchange := service.PrepareClientOptimizerSummary(connection, trace)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocking optimizer diagnostics", exchange)
	}
	if exchange.Summary.Supported || exchange.Summary.Blocked != 1 || exchange.Summary.Blocking != 1 {
		t.Fatalf("summary = %#v, want blocked optimizer summary", exchange.Summary)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 16 {
		t.Fatalf("result/schema = %#v/%#v, want failed optimizer summary envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServicePrepareClientOptimizerSummaryCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	trace := NewOptimizationTrace()
	trace.Add(RewriteAdvisoryRecord(RewriteHiddenProjection, "add hidden field"))

	exchange := service.PrepareClientOptimizerSummary(connection, trace)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Trace.Rewrites[0].Reason = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.PrepareClientOptimizerSummary(connection, trace)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Trace.Rewrites[0].Reason != "add hidden field" {
		t.Fatalf("trace leaked mutation: %#v", again.Trace)
	}
	if again.Result.Columns[0].Name != "Supported" || again.ResultSchema.Columns[0].Name != "Supported" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

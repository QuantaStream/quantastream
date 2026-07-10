package qsbridge

import (
	"strings"
	"testing"
)

func TestPlanningServicePrepareClientExplainBundleBuildsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	report := sampleInspectionReportForExplainOptions()
	report.Diagnostics = DiagnosticSet{{
		Code:     DiagnosticNativeBlocker,
		Severity: SeverityWarning,
		Phase:    PhaseClassify,
		Message:  "note",
	}}
	bundle := ExplainInspectionReport(report, VerboseExplainOptions())

	exchange := service.PrepareClientExplainBundle(connection, bundle)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported explain bundle", exchange)
	}
	if len(exchange.Rows) != 12 {
		t.Fatalf("rows = %#v, want summary plus all selected explain rows", exchange.Rows)
	}
	if exchange.Rows[0].Section != "summary" || exchange.Rows[0].Name != "supported" || exchange.Rows[0].Value != "true" {
		t.Fatalf("first row = %#v, want supported summary", exchange.Rows[0])
	}
	if exchange.Rows[1].Name != "access_intent" || exchange.Rows[1].Value != string(PhysicalAccessRead) {
		t.Fatalf("access intent row = %#v, want read intent", exchange.Rows[1])
	}
	if exchange.Rows[2].Name != "lifecycle" || exchange.Rows[2].Value != string(ClientPlanLifecycleSelect) {
		t.Fatalf("lifecycle row = %#v, want select lifecycle", exchange.Rows[2])
	}
	if exchange.Rows[3].Name != "lifecycle_steps" || exchange.Rows[3].Value != "7" {
		t.Fatalf("lifecycle steps row = %#v, want select lifecycle step count", exchange.Rows[3])
	}
	if exchange.Rows[4].Name != "sections" || exchange.Rows[4].Value != "logical,physical,optimizer,optimizer_summary,diagnostics,functions,native_blockers" {
		t.Fatalf("sections row = %#v, want selected sections", exchange.Rows[4])
	}
	if !hasClientExplainRow(exchange.Rows, ExplainSectionLogical, string(PlanNodeScan), "logical") {
		t.Fatalf("rows = %#v, want logical scan row", exchange.Rows)
	}
	if !hasClientExplainRow(exchange.Rows, ExplainSectionPhysical, string(PhysicalNodeScan), "physical") {
		t.Fatalf("rows = %#v, want physical scan row", exchange.Rows)
	}
	if !hasClientExplainRow(exchange.Rows, ExplainSectionOptimizer, string(RewritePredicatePushdown), string(RewriteApplied)) {
		t.Fatalf("rows = %#v, want optimizer rewrite row", exchange.Rows)
	}
	if !hasClientExplainRow(exchange.Rows, ExplainSectionOptimizerSummary, "summary", "true") {
		t.Fatalf("rows = %#v, want optimizer summary row", exchange.Rows)
	}
	if !hasClientExplainRow(exchange.Rows, ExplainSectionDiagnostics, string(DiagnosticNativeBlocker), string(SeverityWarning)) {
		t.Fatalf("rows = %#v, want diagnostic row", exchange.Rows)
	}
	if !hasClientExplainRow(exchange.Rows, ExplainSectionFunctions, "topn", string(FunctionUsageAggregate)) {
		t.Fatalf("rows = %#v, want function usage row", exchange.Rows)
	}
	if !hasClientExplainRow(exchange.Rows, ExplainSectionNativeBlockers, string(DiagnosticNativeBlocker), "") {
		t.Fatalf("rows = %#v, want native blocker row", exchange.Rows)
	}
	if len(exchange.ResultSchema.Columns) != 4 || exchange.ResultSchema.Columns[0].Name != "Section" {
		t.Fatalf("schema = %#v, want explain bundle schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != uint64(len(exchange.Rows)) || exchange.Result.Chunks[0].Rows[1][2].Value != exchange.Rows[1].Value {
		t.Fatalf("result = %#v, want explain bundle rows", exchange.Result)
	}
}

func TestPlanningServicePrepareClientExplainBundleReturnsFailedEnvelopeForDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	bundle := ExplainBundle{
		Supported: false,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticNativeBlocker, PhasePlan, "explain failed"),
		},
	}

	exchange := service.PrepareClientExplainBundle(connection, bundle)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocking explain diagnostics", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 4 {
		t.Fatalf("result/schema = %#v/%#v, want failed explain envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServicePrepareClientExplainBundleExposesTopNPhysicalStrategy(t *testing.T) {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal},
	}
	plan := planner.Plan("select topn(l.l_shipmode) as shipmode_topn from lineitem as l")
	if plan.Diagnostics.BlocksNative() {
		t.Fatalf("plan diagnostics: %#v", plan.Diagnostics)
	}

	service := NewPlanningService(Planner{}, nil)
	bundle := ExplainInspectionReport(plan.Inspection, VerboseExplainOptions())
	exchange := service.PrepareClientExplainBundle(clientStatementConnection(), bundle)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported explain bundle", exchange)
	}
	if !hasClientExplainRowDetail(exchange.Rows, ExplainSectionPhysical, string(PhysicalNodeAggregate), "strategies=quanta_topn") {
		t.Fatalf("rows = %#v, want physical aggregate row with quanta_topn strategy", exchange.Rows)
	}
}

func TestPlanningServicePrepareClientExplainBundleCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	report := sampleInspectionReportForExplainOptions()
	report.Diagnostics = DiagnosticSet{{
		Code:     DiagnosticNativeBlocker,
		Severity: SeverityWarning,
		Phase:    PhaseClassify,
		Message:  "note",
	}}
	bundle := ExplainInspectionReport(report, VerboseExplainOptions())

	exchange := service.PrepareClientExplainBundle(connection, bundle)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Bundle.Logical.Nodes[0].Summary = "mutated"
	exchange.Bundle.Optimization.Rewrites[0].Reason = "mutated"
	exchange.Rows[0].Value = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][2].Value = "mutated"

	again := service.PrepareClientExplainBundle(connection, bundle)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Bundle.Logical.Nodes[0].Summary != "logical" || again.Bundle.Optimization.Rewrites[0].Reason != "moved predicate" {
		t.Fatalf("bundle leaked mutation: %#v", again.Bundle)
	}
	if again.Rows[0].Value != "true" {
		t.Fatalf("rows leaked mutation: %#v", again.Rows)
	}
	if again.Result.Columns[0].Name != "Section" || again.ResultSchema.Columns[0].Name != "Section" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][2].Value != "true" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

func hasClientExplainRow(rows []ClientExplainRow, section ExplainSection, name string, value string) bool {
	for _, row := range rows {
		if row.Section == section && row.Name == name && row.Value == value {
			return true
		}
	}
	return false
}

func hasClientExplainRowDetail(rows []ClientExplainRow, section ExplainSection, name string, detail string) bool {
	for _, row := range rows {
		if row.Section == section && row.Name == name && strings.Contains(row.Detail, detail) {
			return true
		}
	}
	return false
}

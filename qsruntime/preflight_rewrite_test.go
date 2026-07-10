package qsruntime

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestSQLRuntimeExecuteSQLUsesPreflightBoundary(t *testing.T) {
	source, err := os.ReadFile("sql_runtime.go")
	if err != nil {
		t.Fatalf("read sql_runtime.go: %v", err)
	}
	text := string(source)
	if !strings.Contains(text, "applyPreflightRewrites(ctx") {
		t.Fatalf("ExecuteSQL should route compatibility rewrites through applyPreflightRewrites")
	}
	for _, forbidden := range []string{
		"rewriteCorrelatedAverageQuantitySubquery(ctx",
		"rewriteUncorrelatedHavingScalarSubquery(ctx",
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("ExecuteSQL should not call %s directly", forbidden)
		}
	}
}

func TestPreflightRewriteInventoryNamesPlannerNativeSubqueryIntentGap(t *testing.T) {
	for _, item := range preflightRewriteInventory() {
		if !strings.Contains(item.Reason, "subquery intent is not planner-native") {
			t.Fatalf("rewrite rule %q reason = %q, want planner-native subquery intent wording", item.Rule, item.Reason)
		}
	}
}

func TestNextPreflightNativePromotionCandidateIsAggregateThresholdLookup(t *testing.T) {
	candidate := nextPreflightNativePromotionCandidate()
	if candidate.HelperKind != PreflightHelperPlanAggregateThresholdLookup {
		t.Fatalf("candidate helper kind = %q, want aggregate-threshold lookup", candidate.HelperKind)
	}
	if candidate.Rule != qsbridge.RewriteCorrelatedAggregatePreflight {
		t.Fatalf("candidate rule = %q, want correlated aggregate preflight", candidate.Rule)
	}
	if candidate.Follows != qsbridge.NativeSubqueryStepParentKeyLookup {
		t.Fatalf("candidate follows = %q, want parent-key lookup", candidate.Follows)
	}
	if !strings.Contains(candidate.Reason, "bitmap-native") {
		t.Fatalf("candidate reason = %q, want bitmap-native execution note", candidate.Reason)
	}
}

func TestPreflightRewriteRulesHaveInventory(t *testing.T) {
	runtime := SQLRuntime{}
	inventory := preflightRewriteInventoryByRule()
	rules := runtime.preflightRewriteRules()

	if len(inventory) != len(rules) {
		t.Fatalf("inventory entries = %d, rewrite rules = %d", len(inventory), len(rules))
	}

	for _, rule := range rules {
		item, ok := inventory[rule.RuleID()]
		if !ok {
			t.Fatalf("rewrite rule %q has no inventory entry", rule.RuleID())
		}
		for label, value := range map[string]string{
			"reason":                item.Reason,
			"source SQL shape":      item.SourceSQLShape,
			"temporary strategy":    item.TemporaryStrategy,
			"future IR replacement": item.FutureIRReplacement,
		} {
			if strings.TrimSpace(value) == "" {
				t.Fatalf("rewrite rule %q has empty %s", rule.RuleID(), label)
			}
		}
		if len(item.HelperPlanKinds) == 0 {
			t.Fatalf("rewrite rule %q has no helper plan kind inventory", rule.RuleID())
		}
		if len(item.RegressionCoverage) == 0 {
			t.Fatalf("rewrite rule %q has no regression coverage notes", rule.RuleID())
		}
	}
}

func TestPreflightRewriteInventoryMatchesDebtDocument(t *testing.T) {
	doc, err := os.ReadFile("../docs/SQL_REWRITE_DEBT.md")
	if err != nil {
		t.Fatalf("read SQL_REWRITE_DEBT.md: %v", err)
	}
	text := string(doc)
	for _, item := range preflightRewriteInventory() {
		if !strings.Contains(text, "`"+string(item.Rule)+"`") {
			t.Fatalf("SQL_REWRITE_DEBT.md missing rule %q", item.Rule)
		}
		for _, helperKind := range item.HelperPlanKinds {
			if !strings.Contains(text, "`"+string(helperKind)+"`") {
				t.Fatalf("SQL_REWRITE_DEBT.md missing helper kind %q for rule %q", helperKind, item.Rule)
			}
		}
		for _, coverage := range item.RegressionCoverage {
			coverageFile := strings.Fields(coverage)[0]
			if !strings.Contains(text, coverageFile) {
				t.Fatalf("SQL_REWRITE_DEBT.md missing regression coverage file %q for rule %q", coverageFile, item.Rule)
			}
		}
	}
}

func TestPreflightRewriteDescriptorSummariesExposeTypedShape(t *testing.T) {
	scalarSQL := `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`
	scalar, ok := findUncorrelatedHavingScalarSubquery(scalarSQL)
	if !ok {
		t.Fatalf("scalar descriptor not found")
	}
	scalarSummary := scalar.descriptorSummary()
	if scalarSummary.Rule != qsbridge.RewriteScalarSubqueryPreflight {
		t.Fatalf("scalar rule = %q", scalarSummary.Rule)
	}
	if scalarSummary.SourceSQLShape != "having(comparison(scalar_subquery))" {
		t.Fatalf("scalar source shape = %q", scalarSummary.SourceSQLShape)
	}
	if scalarSummary.ReplacementSQLShape != "having(comparison(literal))" {
		t.Fatalf("scalar replacement shape = %q", scalarSummary.ReplacementSQLShape)
	}
	if scalarSummary.Range.Start != scalar.ReplacementStart || scalarSummary.Range.End != scalar.ReplacementEnd {
		t.Fatalf("scalar range = %#v, want %d-%d", scalarSummary.Range, scalar.ReplacementStart, scalar.ReplacementEnd)
	}
	if value, ok := descriptorAttributeValue(scalarSummary, "subquery_sql"); !ok || !strings.Contains(value, "select o_orderkey") {
		t.Fatalf("scalar subquery attribute = %q, %v", value, ok)
	}
	if scalarSummary.SubqueryIntent == nil || !scalarSummary.SubqueryIntent.Valid() || scalarSummary.SubqueryIntent.Kind != qsbridge.SubqueryIntentScalar {
		t.Fatalf("scalar subquery intent = %#v", scalarSummary.SubqueryIntent)
	}
	if got, want := len(scalarSummary.HelperPlans), 1; got != want {
		t.Fatalf("scalar helper plans = %d, want %d", got, want)
	}
	if scalarSummary.HelperPlans[0].Kind != PreflightHelperPlanScalarSubquery {
		t.Fatalf("scalar helper kind = %q", scalarSummary.HelperPlans[0].Kind)
	}
	if scalarSummary.HelperPlans[0].Lifecycle != qsbridge.SubqueryStepNativeReady || scalarSummary.HelperPlans[0].NativeStep == nil {
		t.Fatalf("scalar helper lifecycle/native step = %#v", scalarSummary.HelperPlans[0])
	}
	if got, want := len(scalarSummary.NativeSteps), 1; got != want {
		t.Fatalf("scalar native steps = %d, want %d: %#v", got, want, scalarSummary.NativeSteps)
	}
	if scalarSummary.NativeSteps[0].Kind != qsbridge.NativeSubqueryStepScalarMaterialization {
		t.Fatalf("scalar native step = %#v", scalarSummary.NativeSteps[0])
	}
	if !strings.Contains(scalarSummary.HelperPlans[0].SQL, "select o_orderkey") {
		t.Fatalf("scalar helper SQL = %q", scalarSummary.HelperPlans[0].SQL)
	}

	correlatedSQL := `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#45'
  and p.p_container = 'MED JAR'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`
	correlated, ok := findCorrelatedAverageQuantityPredicate(correlatedSQL)
	if !ok {
		t.Fatalf("correlated descriptor not found")
	}
	correlatedSummary := correlated.descriptorSummary()
	if correlatedSummary.Rule != qsbridge.RewriteCorrelatedAggregatePreflight {
		t.Fatalf("correlated rule = %q", correlatedSummary.Rule)
	}
	if correlatedSummary.SourceSQLShape != "predicate(correlated_aggregate_subquery)" {
		t.Fatalf("correlated source shape = %q", correlatedSummary.SourceSQLShape)
	}
	if correlatedSummary.ReplacementSQLShape != "predicate(disjunction(per_key_thresholds))" {
		t.Fatalf("correlated replacement shape = %q", correlatedSummary.ReplacementSQLShape)
	}
	if value, ok := descriptorAttributeValue(correlatedSummary, "aggregate_function"); !ok || value != "avg" {
		t.Fatalf("aggregate attribute = %q, %v", value, ok)
	}
	if value, ok := descriptorAttributeValue(correlatedSummary, "factor"); !ok || value != "0.2" {
		t.Fatalf("factor attribute = %q, %v", value, ok)
	}
	for _, field := range []string{
		"l.l_quantity",
		"l2.l_quantity",
		"l2.l_partkey",
		"p.p_partkey",
		"p.p_brand",
		"p.p_container",
	} {
		if !descriptorHasQualifiedField(correlatedSummary.Fields, field) {
			t.Fatalf("correlated fields = %#v, missing %s", descriptorQualifiedFieldNames(correlatedSummary.Fields), field)
		}
	}
	if correlatedSummary.SubqueryIntent == nil || !correlatedSummary.SubqueryIntent.Valid() || correlatedSummary.SubqueryIntent.Kind != qsbridge.SubqueryIntentCorrelatedAggregate {
		t.Fatalf("correlated subquery intent = %#v", correlatedSummary.SubqueryIntent)
	}
	if got, want := len(correlatedSummary.HelperPlans), 2; got != want {
		t.Fatalf("correlated helper plans = %d, want %d", got, want)
	}
	if correlatedSummary.HelperPlans[0].Kind != PreflightHelperPlanParentKeyLookup {
		t.Fatalf("first correlated helper kind = %q", correlatedSummary.HelperPlans[0].Kind)
	}
	if correlatedSummary.HelperPlans[1].Kind != PreflightHelperPlanAggregateThresholdLookup {
		t.Fatalf("second correlated helper kind = %q", correlatedSummary.HelperPlans[1].Kind)
	}
	if !scalarSummary.Complete() {
		t.Fatalf("scalar descriptor completeness = %#v", scalarSummary.CompletenessReport())
	}
	if !correlatedSummary.Complete() {
		t.Fatalf("correlated descriptor completeness = %#v", correlatedSummary.CompletenessReport())
	}
}

func TestPreflightRewriteDescriptorCompletenessReportsMissingFields(t *testing.T) {
	report := (PreflightRewriteDescriptorSummary{
		Rule:           qsbridge.RewriteScalarSubqueryPreflight,
		SourceSQLShape: "having(comparison(scalar_subquery))",
		HelperPlans: []PreflightRewriteHelperPlanDescriptor{{
			Name: "scalar_subquery_value",
			Kind: PreflightHelperPlanScalarSubquery,
		}},
	}).CompletenessReport()

	if report.Complete {
		t.Fatalf("completeness report = %#v, want incomplete descriptor", report)
	}
	for _, missing := range []string{"replacement_sql_shape", "text_range", "subquery_intent"} {
		if !stringSliceContains(report.Missing, missing) {
			t.Fatalf("descriptor missing = %#v, want %s", report.Missing, missing)
		}
	}
	if len(report.Helpers) != 1 || report.Helpers[0].Complete {
		t.Fatalf("helper completeness = %#v, want incomplete helper report", report.Helpers)
	}
	for _, missing := range []string{"purpose", "outputs", "materialization", "bitmap_native_target"} {
		if !stringSliceContains(report.Helpers[0].Missing, missing) {
			t.Fatalf("helper missing = %#v, want %s", report.Helpers[0].Missing, missing)
		}
	}
}

func TestPreflightRewriteInspectionIncludesDescriptorSummary(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: 7}},
			}},
		}}, nil
	})

	result, err := runtime.applyPreflightRewrites(context.Background(), `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("preflight rewrites: %v", err)
	}
	if got, want := len(result.Preflight.Rewrites), 2; got != want {
		t.Fatalf("preflight rewrites = %d, want %d", got, want)
	}
	if result.Preflight.Rewrites[0].Descriptor != nil {
		t.Fatalf("skipped correlated rewrite descriptor = %#v, want nil", result.Preflight.Rewrites[0].Descriptor)
	}
	inspection := result.Preflight.Rewrites[1]
	if !inspection.Applied {
		t.Fatalf("scalar rewrite applied = false")
	}
	if inspection.Descriptor == nil {
		t.Fatalf("scalar rewrite descriptor = nil")
	}
	if inspection.Descriptor.Rule != qsbridge.RewriteScalarSubqueryPreflight {
		t.Fatalf("descriptor rule = %q", inspection.Descriptor.Rule)
	}
	if value, ok := descriptorAttributeValue(*inspection.Descriptor, "subquery_sql"); !ok || !strings.Contains(value, "select o_orderkey") {
		t.Fatalf("descriptor subquery attribute = %q, %v", value, ok)
	}
}

func TestPreflightRewriteDescriptorReportIsCompact(t *testing.T) {
	descriptor, ok := findCorrelatedAverageQuantityPredicate(`select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#45'
  and p.p_container = 'MED JAR'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`)
	if !ok {
		t.Fatalf("correlated descriptor not found")
	}

	report := descriptor.descriptorSummary().Report()
	if report.Rule != qsbridge.RewriteCorrelatedAggregatePreflight {
		t.Fatalf("report rule = %q", report.Rule)
	}
	if report.SourceSQLShape != "predicate(correlated_aggregate_subquery)" {
		t.Fatalf("source shape = %q", report.SourceSQLShape)
	}
	if report.ReplacementSQLShape != "predicate(disjunction(per_key_thresholds))" {
		t.Fatalf("replacement shape = %q", report.ReplacementSQLShape)
	}
	if report.Start < 0 || report.End <= report.Start {
		t.Fatalf("report range = %d-%d", report.Start, report.End)
	}
	if !report.NativeReplacementReady || len(report.NativeReplacementMissing) != 0 {
		t.Fatalf("native replacement readiness = %v missing=%#v", report.NativeReplacementReady, report.NativeReplacementMissing)
	}
	for _, attribute := range []string{"aggregate_function", "factor", "required_filter"} {
		if !strings.Contains(strings.Join(report.Attributes, ","), attribute) {
			t.Fatalf("report attributes = %#v, missing %s", report.Attributes, attribute)
		}
	}
	for _, field := range []string{"l.l_quantity", "l2.l_quantity", "p.p_brand", "p.p_container"} {
		if !strings.Contains(strings.Join(report.Fields, ","), field) {
			t.Fatalf("report fields = %#v, missing %s", report.Fields, field)
		}
	}
}

func TestPreflightRewriteInspectionDescriptorReportHandlesSkippedRules(t *testing.T) {
	inspection := PreflightRewriteInspection{Rule: qsbridge.RewriteScalarSubqueryPreflight}
	if report, ok := inspection.DescriptorReport(); ok || report.Rule != "" {
		t.Fatalf("descriptor report = %#v, %v; want empty skipped report", report, ok)
	}

	descriptor, ok := findUncorrelatedHavingScalarSubquery(`select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`)
	if !ok {
		t.Fatalf("scalar descriptor not found")
	}
	summary := descriptor.descriptorSummary()
	inspection = PreflightRewriteInspection{
		Rule:       qsbridge.RewriteScalarSubqueryPreflight,
		Applied:    true,
		Descriptor: &summary,
	}
	report, ok := inspection.DescriptorReport()
	if !ok {
		t.Fatalf("descriptor report missing")
	}
	if report.Rule != qsbridge.RewriteScalarSubqueryPreflight || report.SourceSQLShape == "" || len(report.Attributes) == 0 {
		t.Fatalf("descriptor report = %#v", report)
	}
}

func TestPreflightRewriteSummaryDescriptorReports(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: 7}},
			}},
		}}, nil
	})

	result, err := runtime.applyPreflightRewrites(context.Background(), `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("preflight rewrites: %v", err)
	}

	reports := result.Preflight.DescriptorReports()
	if got, want := len(reports), 1; got != want {
		t.Fatalf("descriptor reports = %d, want %d: %#v", got, want, reports)
	}
	if reports[0].Rule != qsbridge.RewriteScalarSubqueryPreflight {
		t.Fatalf("descriptor report rule = %q", reports[0].Rule)
	}
	if reports[0].SourceSQLShape != "having(comparison(scalar_subquery))" {
		t.Fatalf("descriptor report source shape = %q", reports[0].SourceSQLShape)
	}
	if len(reports[0].Attributes) == 0 {
		t.Fatalf("descriptor report attributes = %#v", reports[0].Attributes)
	}
	if got, want := len(reports[0].HelperPlans), 1; got != want {
		t.Fatalf("descriptor report helper plans = %d, want %d", got, want)
	}
	if reports[0].HelperPlans[0].Kind != PreflightHelperPlanScalarSubquery {
		t.Fatalf("descriptor report helper kind = %q", reports[0].HelperPlans[0].Kind)
	}
	if reports[0].HelperPlans[0].Lifecycle != qsbridge.SubqueryStepNativeReady || reports[0].HelperPlans[0].NativeStep == nil {
		t.Fatalf("descriptor helper lifecycle/native step = %#v", reports[0].HelperPlans[0])
	}
	if got, want := len(reports[0].NativeSteps), 1; got != want {
		t.Fatalf("descriptor native steps = %d, want %d: %#v", got, want, reports[0].NativeSteps)
	}
	if reports[0].NativeSteps[0].Kind != qsbridge.NativeSubqueryStepScalarMaterialization {
		t.Fatalf("descriptor native step = %#v", reports[0].NativeSteps[0])
	}
	if reports[0].SubqueryIntent == nil || reports[0].SubqueryIntent.Kind != qsbridge.SubqueryIntentScalar {
		t.Fatalf("descriptor report subquery intent = %#v", reports[0].SubqueryIntent)
	}
	if got, want := reports[0].SubqueryIntent.HelperKinds, []string{"scalar_subquery"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("descriptor report subquery helper kinds = %#v, want %#v", got, want)
	}
	if !reports[0].NativeReplacementReady || len(reports[0].NativeReplacementMissing) != 0 {
		t.Fatalf("descriptor report readiness = %v missing=%#v", reports[0].NativeReplacementReady, reports[0].NativeReplacementMissing)
	}
}

func TestPreflightRewriteInventoryDescriptorsAreNativeReplacementReady(t *testing.T) {
	knownSQL := map[qsbridge.RewriteRuleID]string{
		qsbridge.RewriteScalarSubqueryPreflight: `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`,
		qsbridge.RewriteCorrelatedAggregatePreflight: `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#23'
  and p.p_container = 'MED BOX'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`,
	}
	for _, item := range preflightRewriteInventory() {
		sql, ok := knownSQL[item.Rule]
		if !ok {
			t.Fatalf("rewrite rule %q has no known descriptor SQL", item.Rule)
		}
		descriptor, ok := preflightRewriteDescriptor(item.Rule, sql)
		if !ok {
			t.Fatalf("rewrite rule %q did not produce a descriptor", item.Rule)
		}
		if report := descriptor.CompletenessReport(); !report.Complete {
			t.Fatalf("rewrite rule %q readiness = %#v", item.Rule, report)
		}
	}
}

func TestPreflightRewriteDescriptorSubqueryIntentMatchesHelperPlans(t *testing.T) {
	for _, rule := range []qsbridge.RewriteRuleID{
		qsbridge.RewriteScalarSubqueryPreflight,
		qsbridge.RewriteCorrelatedAggregatePreflight,
	} {
		summarySQL := map[qsbridge.RewriteRuleID]string{
			qsbridge.RewriteScalarSubqueryPreflight: `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`,
			qsbridge.RewriteCorrelatedAggregatePreflight: `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#23'
  and p.p_container = 'MED BOX'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`,
		}[rule]
		summary, ok := preflightRewriteDescriptor(rule, summarySQL)
		if !ok {
			t.Fatalf("descriptor for rule %q not found", rule)
		}
		if summary.SubqueryIntent == nil || !summary.SubqueryIntent.Valid() {
			t.Fatalf("subquery intent for rule %q = %#v", rule, summary.SubqueryIntent)
		}
		helperKinds := make([]string, 0, len(summary.HelperPlans))
		for _, helper := range summary.HelperPlans {
			helperKinds = append(helperKinds, string(helper.Kind))
		}
		intentKinds := summary.SubqueryIntent.HelperKinds()
		if len(helperKinds) != len(intentKinds) {
			t.Fatalf("rule %q helper kinds = %#v, intent kinds = %#v", rule, helperKinds, intentKinds)
		}
		for i := range helperKinds {
			if helperKinds[i] != intentKinds[i] {
				t.Fatalf("rule %q helper kind[%d] = %q, intent kind = %q", rule, i, helperKinds[i], intentKinds[i])
			}
		}
	}
}

func TestPreflightRewriteDescriptorReportsExposeSubqueryIntent(t *testing.T) {
	descriptor, ok := findCorrelatedAverageQuantityPredicate(`select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#23'
  and p.p_container = 'MED BOX'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`)
	if !ok {
		t.Fatalf("correlated descriptor not found")
	}

	report := descriptor.descriptorSummary().Report()
	if report.SubqueryIntent == nil || report.SubqueryIntent.Kind != qsbridge.SubqueryIntentCorrelatedAggregate {
		t.Fatalf("subquery intent report = %#v", report.SubqueryIntent)
	}
	if report.SubqueryIntent.CorrelatedAggregate == nil || report.SubqueryIntent.CorrelatedAggregate.AggregateFunction != "avg" {
		t.Fatalf("correlated aggregate report = %#v", report.SubqueryIntent.CorrelatedAggregate)
	}
	if len(report.HelperPlans) != 2 {
		t.Fatalf("helper plan reports = %#v, want two", report.HelperPlans)
	}
	if got, want := report.HelperPlans[0].InputFields, []string{"p.p_brand", "p.p_container"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("parent key input fields = %#v, want %#v", got, want)
	}
	if got, want := report.HelperPlans[0].OutputFields, []string{"p.p_partkey"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("parent key output fields = %#v, want %#v", got, want)
	}
	if got, want := report.HelperPlans[1].InputFields, []string{"l2.l_partkey", "l2.l_quantity"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("threshold input fields = %#v, want %#v", got, want)
	}
	if got, want := report.SubqueryIntent.HelperKinds, []string{"parent_key_lookup", "aggregate_threshold_lookup"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("subquery helper kinds = %#v, want %#v", got, want)
	}
}

func TestScalarPreflightDescriptorIntentCanSeedPlannerPlaceholder(t *testing.T) {
	descriptor, ok := findUncorrelatedHavingScalarSubquery(`select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`)
	if !ok {
		t.Fatalf("scalar descriptor not found")
	}
	summary := descriptor.descriptorSummary()
	if summary.SubqueryIntent == nil {
		t.Fatalf("subquery intent = nil")
	}

	orders := qsbridge.TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderPriority := qsbridge.FieldRef{Table: orders, Name: "o_orderpriority", Index: qsbridge.IndexStringEnum}
	plan := qsbridge.BuildLogicalPlan(qsbridge.QueryIR{
		Kind:       qsbridge.QueryKindSelect,
		Sources:    []qsbridge.TableInstance{orders},
		Subqueries: []qsbridge.SubqueryPlanIntent{*summary.SubqueryIntent},
		GroupBy:    []qsbridge.Expr{qsbridge.Field(orderPriority)},
		Aggregates: []qsbridge.Aggregate{{Function: "count", Alias: "order_count"}},
		Projection: []qsbridge.ProjectionColumn{{Expr: qsbridge.Field(orderPriority)}},
	})

	var scalarNodes int
	qsbridge.WalkLogicalPlan(plan.Root, func(node qsbridge.LogicalNode) bool {
		if scalar, ok := node.(qsbridge.ScalarSubqueryNode); ok {
			scalarNodes++
			if got, want := scalar.ScalarOutputNames(), []string{"scalar_subquery_value"}; len(got) != len(want) || got[0] != want[0] {
				t.Fatalf("scalar outputs = %#v, want %#v", got, want)
			}
		}
		return true
	})
	if scalarNodes != 1 {
		t.Fatalf("scalar placeholder nodes = %d, want 1", scalarNodes)
	}
}

func TestCorrelatedPreflightDescriptorIntentCanSeedPlannerPlaceholder(t *testing.T) {
	descriptor, ok := findCorrelatedAverageQuantityPredicate(`select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#23'
  and p.p_container = 'MED BOX'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`)
	if !ok {
		t.Fatalf("correlated descriptor not found")
	}
	summary := descriptor.descriptorSummary()
	if summary.SubqueryIntent == nil {
		t.Fatalf("subquery intent = nil")
	}

	part := qsbridge.TableInstance{ID: "part", Table: "part", Alias: "p"}
	partKey := qsbridge.FieldRef{Table: part, Name: "p_partkey", Index: qsbridge.IndexBSI}
	plan := qsbridge.BuildLogicalPlan(qsbridge.QueryIR{
		Kind:       qsbridge.QueryKindSelect,
		Sources:    []qsbridge.TableInstance{part},
		Subqueries: []qsbridge.SubqueryPlanIntent{*summary.SubqueryIntent},
		Projection: []qsbridge.ProjectionColumn{{Expr: qsbridge.Field(partKey)}},
	})

	var correlatedNodes int
	qsbridge.WalkLogicalPlan(plan.Root, func(node qsbridge.LogicalNode) bool {
		if _, ok := node.(qsbridge.CorrelatedAggregateSubqueryNode); ok {
			correlatedNodes++
		}
		return true
	})
	if correlatedNodes != 1 {
		t.Fatalf("correlated placeholder nodes = %d, want 1", correlatedNodes)
	}
	helperPlans := qsbridge.LowerSubqueryHelperPlans(plan.Root)
	if got, want := len(helperPlans), 2; got != want {
		t.Fatalf("helper plans = %d, want %d: %#v", got, want, helperPlans)
	}
	if helperPlans[0].Kind != qsbridge.SubqueryHelperPlanParentKeyLookup || helperPlans[1].Kind != qsbridge.SubqueryHelperPlanAggregateThresholdLookup {
		t.Fatalf("helper plan kinds = %#v, want parent-key then aggregate-threshold", helperPlans)
	}
}

type testNativeSubqueryStepExecutor struct {
	executions int
	last       qsbridge.NativeSubqueryStepExecutionRequest
	result     qsbridge.NativeSubqueryStepExecutionResult
}

func (e *testNativeSubqueryStepExecutor) ExecuteNativeSubqueryStep(ctx context.Context, request qsbridge.NativeSubqueryStepExecutionRequest) (qsbridge.NativeSubqueryStepExecutionResult, error) {
	e.executions++
	e.last = request
	if e.result.Step.Name == "" {
		outputName := request.Step.Outputs[0]
		return qsbridge.NativeSubqueryStepExecutionResult{
			Step: request.Step,
			Outputs: map[string]qsbridge.ResultCell{
				outputName: {Kind: qsbridge.ValueInt, Value: int64(29)},
			},
		}, nil
	}
	return e.result, nil
}

func TestScalarPreflightRoutesThroughNativeSubqueryStepExecutor(t *testing.T) {
	executor := &testNativeSubqueryStepExecutor{}
	runtime := SQLRuntime{NativeSubquerySteps: executor}

	result, err := runtime.applyPreflightRewrites(context.Background(), `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("preflight rewrites: %v", err)
	}
	if executor.executions != 1 {
		t.Fatalf("native executions = %d, want 1", executor.executions)
	}
	if executor.last.Step.Kind != qsbridge.NativeSubqueryStepScalarMaterialization {
		t.Fatalf("native step = %#v", executor.last.Step)
	}
	if !strings.Contains(result.SQL, "having c > 29") {
		t.Fatalf("rewritten SQL = %q, want scalar literal from native executor", result.SQL)
	}
	reports := result.Preflight.HelperExecutionReports()
	if got, want := len(reports), 1; got != want {
		t.Fatalf("helper reports = %d, want %d: %#v", got, want, reports)
	}
	if reports[0].NativeTrace == nil || reports[0].NativeTrace.StepKind != qsbridge.NativeSubqueryStepScalarMaterialization {
		t.Fatalf("native trace = %#v", reports[0].NativeTrace)
	}
	if reports[0].Payload.Scalar == nil || !reports[0].Payload.Scalar.Materialized {
		t.Fatalf("scalar payload report = %#v", reports[0].Payload.Scalar)
	}
}

func TestPreflightRewriteSummaryReportsScalarHelperExecutionPayload(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: 7}},
			}},
		}}, nil
	})

	result, err := runtime.applyPreflightRewrites(context.Background(), `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("preflight rewrites: %v", err)
	}

	reports := result.Preflight.HelperExecutionReports()
	if got, want := len(reports), 1; got != want {
		t.Fatalf("helper reports = %d, want %d: %#v", got, want, reports)
	}
	if reports[0].Plan.Kind != PreflightHelperPlanScalarSubquery || reports[0].Payload.Scalar == nil {
		t.Fatalf("scalar helper report = %#v", reports[0])
	}
	if reports[0].Payload.Scalar.OutputName != "scalar_subquery_value" || reports[0].SQL == "" {
		t.Fatalf("scalar payload report = %#v", reports[0])
	}
	if reports[0].NativeStep == nil || reports[0].NativeStep.Kind != qsbridge.NativeSubqueryStepScalarMaterialization {
		t.Fatalf("scalar native step report = %#v", reports[0].NativeStep)
	}
	if reports[0].NativeTrace == nil || reports[0].NativeTrace.OutputCount != 1 {
		t.Fatalf("scalar native trace = %#v", reports[0].NativeTrace)
	}
	if reports[0].Payload.Scalar == nil || !reports[0].Payload.Scalar.Materialized {
		t.Fatalf("scalar payload report = %#v", reports[0].Payload.Scalar)
	}
}

func TestPreflightRewriteSummaryReportsCorrelatedHelperExecutionPayloads(t *testing.T) {
	runtime := SQLRuntime{PreflightHelpers: &nativePrototypePreflightHelperExecutor{}}
	query := `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#23'
  and p.p_container = 'MED BOX'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`

	result, err := runtime.applyPreflightRewrites(context.Background(), query, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("preflight rewrites: %v", err)
	}

	reports := result.Preflight.HelperExecutionReports()
	if got, want := len(reports), 2; got != want {
		t.Fatalf("helper reports = %d, want %d: %#v", got, want, reports)
	}
	if reports[0].Plan.Kind != PreflightHelperPlanParentKeyLookup || reports[0].Payload.ParentKeyLookup == nil {
		t.Fatalf("parent-key helper report = %#v", reports[0])
	}
	if reports[0].Payload.ParentKeyLookup.Filters[0] != "p_brand=Brand#23" || reports[0].Payload.ParentKeyLookup.Filters[1] != "p_container=MED BOX" {
		t.Fatalf("parent-key filters = %#v", reports[0].Payload.ParentKeyLookup.Filters)
	}
	if reports[0].NativeStep == nil || reports[0].NativeStep.Kind != qsbridge.NativeSubqueryStepParentKeyLookup || reports[0].NativeTrace == nil {
		t.Fatalf("parent-key native report = step=%#v trace=%#v", reports[0].NativeStep, reports[0].NativeTrace)
	}
	if reports[1].Plan.Kind != PreflightHelperPlanAggregateThresholdLookup || reports[1].Payload.AggregateThresholdLookup == nil {
		t.Fatalf("aggregate helper report = %#v", reports[1])
	}
	if reports[1].Payload.AggregateThresholdLookup.PartKeyCount != 2 || reports[1].Payload.AggregateThresholdLookup.Factor != 0.2 {
		t.Fatalf("aggregate payload report = %#v", reports[1].Payload.AggregateThresholdLookup)
	}
	if reports[1].NativeStep == nil || reports[1].NativeStep.Kind != qsbridge.NativeSubqueryStepAggregateThresholdLookup || reports[1].NativeTrace == nil {
		t.Fatalf("aggregate native report = step=%#v trace=%#v", reports[1].NativeStep, reports[1].NativeTrace)
	}
}

func TestPreflightRewriteSummaryReportsScalarHelperWhenExecutionBlocks(t *testing.T) {
	executor := &testPreflightHelperExecutor{
		result: PreflightHelperExecutionResult{
			Diagnostics: qsbridge.DiagnosticSet{qsbridge.ErrorDiagnostic(qsbridge.DiagnosticScalarSubquery, qsbridge.PhaseExecute, "scalar helper blocked")},
		},
	}
	runtime := SQLRuntime{PreflightHelpers: executor}

	result, err := runtime.applyPreflightRewrites(context.Background(), `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("preflight rewrites: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocking scalar helper diagnostic", result.Diagnostics)
	}
	if message := result.Diagnostics[0].Message; !strings.Contains(message, "rule=scalar_subquery_preflight") || !strings.Contains(message, "helper=scalar_subquery") || !strings.Contains(message, "phase=execute") {
		t.Fatalf("diagnostic message = %q, want normalized helper context", message)
	}
	reports := result.Preflight.HelperExecutionReports()
	if got, want := len(reports), 1; got != want {
		t.Fatalf("helper reports = %d, want %d: %#v", got, want, reports)
	}
	if reports[0].Plan.Kind != PreflightHelperPlanScalarSubquery || reports[0].Payload.Scalar == nil {
		t.Fatalf("helper report = %#v, want scalar payload report", reports[0])
	}
}

func TestPreflightRewriteSummaryReportsCorrelatedHelperWhenMaterializationBlocks(t *testing.T) {
	executor := &testPreflightHelperExecutor{
		result: PreflightHelperExecutionResult{
			Result: SQLExecutionResult{Runtime: ExecutionResult{RowSet: helperRowSetColumns(
				[]qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(101)}},
				[]qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(202)}},
			)}},
		},
	}
	runtime := SQLRuntime{PreflightHelpers: executor}
	query := `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#23'
  and p.p_container = 'MED BOX'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`

	result, err := runtime.applyPreflightRewrites(context.Background(), query, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("preflight rewrites: %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocking correlated helper diagnostic", result.Diagnostics)
	}
	if message := result.Diagnostics[0].Message; !strings.Contains(message, "rule=correlated_aggregate_preflight") || !strings.Contains(message, "helper=parent_key_lookup") || !strings.Contains(message, "phase=execute") {
		t.Fatalf("diagnostic message = %q, want normalized helper context", message)
	}
	reports := result.Preflight.HelperExecutionReports()
	if got, want := len(reports), 1; got != want {
		t.Fatalf("helper reports = %d, want %d: %#v", got, want, reports)
	}
	if reports[0].Plan.Kind != PreflightHelperPlanParentKeyLookup || reports[0].Payload.ParentKeyLookup == nil {
		t.Fatalf("helper report = %#v, want parent-key payload report", reports[0])
	}
}

func TestPreflightRewriteInspectionRecordsDurations(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: 7}},
			}},
		}}, nil
	})

	result, err := runtime.applyPreflightRewrites(context.Background(), `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("preflight rewrites: %v", err)
	}
	if result.Preflight.Total == 0 {
		t.Fatalf("preflight summary total = 0")
	}
	var summed time.Duration
	for _, rewrite := range result.Preflight.Rewrites {
		if rewrite.Duration < 0 {
			t.Fatalf("rewrite duration = %s", rewrite.Duration)
		}
		summed += rewrite.Duration
	}
	if result.Preflight.Duration != summed {
		t.Fatalf("summary duration = %s, want %s", result.Preflight.Duration, summed)
	}
}

func TestPreflightRewritesUseHelperExecutionBoundary(t *testing.T) {
	tests := []struct {
		path string
		call string
	}{
		{path: "scalar_subquery_rewrite.go", call: "executeScalarNativeSubqueryStep(ctx"},
		{path: "correlated_subquery_rewrite.go", call: "executeParentKeyNativeSubqueryStep(ctx"},
		{path: "correlated_subquery_rewrite.go", call: "executeAggregateThresholdNativeSubqueryStep(ctx"},
	}
	for _, tt := range tests {
		source, err := os.ReadFile(tt.path)
		if err != nil {
			t.Fatalf("read %s: %v", tt.path, err)
		}
		text := string(source)
		if strings.Contains(text, ".ExecuteSQL(ctx") {
			t.Fatalf("%s should route preflight helper work through an execution boundary", tt.path)
		}
		if !strings.Contains(text, tt.call) {
			t.Fatalf("%s should call %s", tt.path, tt.call)
		}
	}
}

type testPreflightHelperExecutor struct {
	executions  int
	lastRuntime SQLRuntime
	lastRequest PreflightHelperExecutionRequest
	result      PreflightHelperExecutionResult
}

func (e *testPreflightHelperExecutor) ExecutePreflightHelper(ctx context.Context, runtime SQLRuntime, request PreflightHelperExecutionRequest) (PreflightHelperExecutionResult, error) {
	e.executions++
	e.lastRuntime = runtime
	e.lastRequest = request
	return e.result, nil
}

func TestPreflightHelperExecutionUsesInjectedExecutor(t *testing.T) {
	executor := &testPreflightHelperExecutor{
		result: PreflightHelperExecutionResult{
			Plan: PreflightRewriteHelperPlanDescriptor{Name: "native_scalar", Kind: PreflightHelperPlanScalarSubquery},
			SQL:  "native helper",
			Result: SQLExecutionResult{Runtime: ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
				Rownums: []qsbridge.QuantaRownum{1},
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: 11}},
				}},
			}}},
		},
	}
	runtime := SQLRuntime{PreflightHelpers: executor}
	helpSQL := "select count(*) as native_scalar from orders"

	helper, err := runtime.executePreflightHelper(context.Background(), PreflightHelperExecutionRequest{
		Plan: PreflightRewriteHelperPlanDescriptor{Name: "scalar_subquery_value", Kind: PreflightHelperPlanScalarSubquery},
		SQL:  helpSQL,
		Payload: PreflightHelperPayload{Scalar: &PreflightScalarHelperPayload{
			SubquerySQL: "select count(*) from orders",
			OutputName:  "scalar_subquery_value",
		}},
		Options: qsbridge.ExecutionOptions{},
	})
	if err != nil {
		t.Fatalf("execute helper: %v", err)
	}
	if executor.executions != 1 {
		t.Fatalf("executions = %d, want 1", executor.executions)
	}
	if executor.lastRequest.SQL != helpSQL {
		t.Fatalf("last request SQL = %q, want %q", executor.lastRequest.SQL, helpSQL)
	}
	if helper.SQL != "native helper" {
		t.Fatalf("helper SQL = %q, want injected result", helper.SQL)
	}
	cell, diagnostics := scalarSubqueryResultCell(helper.Result.Runtime.RowSet)
	if diagnostics.BlocksNative() || cell.Value != 11 {
		t.Fatalf("helper cell = %#v diagnostics = %#v", cell, diagnostics)
	}
}

type nativePrototypePreflightHelperExecutor struct {
	requests []PreflightHelperExecutionRequest
}

func (e *nativePrototypePreflightHelperExecutor) ExecutePreflightHelper(ctx context.Context, runtime SQLRuntime, request PreflightHelperExecutionRequest) (PreflightHelperExecutionResult, error) {
	e.requests = append(e.requests, request)
	switch request.Plan.Kind {
	case PreflightHelperPlanScalarSubquery:
		return PreflightHelperExecutionResult{
			Plan:   request.Plan,
			SQL:    "native:" + request.Payload.Scalar.OutputName,
			Result: SQLExecutionResult{Runtime: ExecutionResult{RowSet: helperRowSet([]qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(17)}})}},
		}, nil
	case PreflightHelperPlanParentKeyLookup:
		keys := []qsbridge.ResultCell{
			{Kind: qsbridge.ValueInt, Value: int64(101)},
			{Kind: qsbridge.ValueInt, Value: int64(202)},
		}
		return PreflightHelperExecutionResult{
			Plan:   request.Plan,
			SQL:    "native:" + request.Payload.ParentKeyLookup.Output,
			Result: SQLExecutionResult{Runtime: ExecutionResult{RowSet: helperRowSet(keys)}},
		}, nil
	case PreflightHelperPlanAggregateThresholdLookup:
		return PreflightHelperExecutionResult{
			Plan: request.Plan,
			SQL:  "native:" + request.Payload.AggregateThresholdLookup.ValueOutput,
			Result: SQLExecutionResult{Runtime: ExecutionResult{RowSet: helperRowSetColumns(
				[]qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(101)}},
				[]qsbridge.ResultCell{{Kind: qsbridge.ValueFloat, Value: float64(50)}},
			)}},
		}, nil
	default:
		return PreflightHelperExecutionResult{
			Plan:        request.Plan,
			SQL:         "native:unsupported",
			Diagnostics: request.ValidatePayload(),
		}, nil
	}
}

func helperRowSet(values []qsbridge.ResultCell) qsbridge.QuantaProjectedRowSet {
	return helperRowSetColumns(values)
}

func helperRowSetColumns(columns ...[]qsbridge.ResultCell) qsbridge.QuantaProjectedRowSet {
	rowCount := 0
	if len(columns) > 0 {
		rowCount = len(columns[0])
	}
	rownums := make([]qsbridge.QuantaRownum, rowCount)
	for i := range rownums {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
	}
	vectors := make([]qsbridge.QuantaProjectionVector, 0, len(columns))
	for _, values := range columns {
		vectors = append(vectors, qsbridge.QuantaProjectionVector{
			Field:  qsbridge.QuantaProjectionField{Field: "helper_value", Visible: true},
			Values: values,
		})
	}
	return qsbridge.QuantaProjectedRowSet{
		Rownums:           rownums,
		ProjectionVectors: vectors,
	}
}

func TestPreflightHelperExecutionValidatesPayloadByKind(t *testing.T) {
	executor := &testPreflightHelperExecutor{}
	runtime := SQLRuntime{PreflightHelpers: executor}

	helper, err := runtime.executePreflightHelper(context.Background(), PreflightHelperExecutionRequest{
		Plan: PreflightRewriteHelperPlanDescriptor{Name: "scalar_subquery_value", Kind: PreflightHelperPlanScalarSubquery},
		SQL:  "select count(*) as scalar_subquery_value from orders",
	})
	if err != nil {
		t.Fatalf("execute helper: %v", err)
	}
	if executor.executions != 0 {
		t.Fatalf("executor should not be called for invalid payload")
	}
	if !helper.Diagnostics.BlocksNative() || helper.Diagnostics[0].Code != qsbridge.DiagnosticScalarSubquery {
		t.Fatalf("helper diagnostics = %#v", helper.Diagnostics)
	}
}

func TestScalarPreflightExecutesNativeRuntimeStep(t *testing.T) {
	helper := &testPreflightHelperExecutor{}
	var gotRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{RowSet: helperRowSet([]qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(7)}})}, nil
	})
	runtime.PreflightHelpers = helper

	query := `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`
	rewritten, diagnostics, _, reports, err, ok := runtime.rewriteUncorrelatedHavingScalarSubquery(context.Background(), query, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if !ok {
		t.Fatalf("rewrite ok = false, want true")
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if helper.executions != 0 {
		t.Fatalf("helper executions = %d, want native runtime path", helper.executions)
	}
	if !strings.Contains(rewritten, "having c > 7") {
		t.Fatalf("rewritten SQL = %q, want scalar literal", rewritten)
	}
	if gotRequest.SourceIndexes[0] != "orders" || gotRequest.FragmentCount() != 1 || gotRequest.ProjectionCount() != 1 {
		t.Fatalf("native request indexes/fragments/projection = %#v/%d/%d", gotRequest.SourceIndexes, gotRequest.FragmentCount(), gotRequest.ProjectionCount())
	}
	fragment := gotRequest.Query.Fragments[0]
	if fragment.Field != "o_orderkey" || fragment.BSIOp != qsbridge.QuantaBSIOpGE || fragment.Value == nil || fragment.Value.Int64() != 1 {
		t.Fatalf("scalar predicate fragment = %#v", fragment)
	}
	if got, want := len(reports), 1; got != want {
		t.Fatalf("reports = %d, want %d: %#v", got, want, reports)
	}
	if reports[0].NativeTrace == nil || reports[0].NativeTrace.StepKind != qsbridge.NativeSubqueryStepScalarMaterialization || reports[0].NativeTrace.ExecutionMode != "native_runtime_scalar_materialization" || reports[0].NativeTrace.OutputCount != 1 || reports[0].NativeTrace.RowCount != 1 {
		t.Fatalf("native trace = %#v", reports[0].NativeTrace)
	}
	if reports[0].Payload.Scalar == nil || !reports[0].Payload.Scalar.Materialized {
		t.Fatalf("scalar payload report = %#v", reports[0].Payload.Scalar)
	}
}

func TestNativePrototypePreflightHelperUsesTypedScalarPayload(t *testing.T) {
	executor := &nativePrototypePreflightHelperExecutor{}
	runtime := SQLRuntime{PreflightHelpers: executor}
	request := PreflightHelperExecutionRequest{
		Plan: PreflightRewriteHelperPlanDescriptor{Name: "scalar_subquery_value", Kind: PreflightHelperPlanScalarSubquery},
		SQL:  "select count(*) as scalar_subquery_value from orders",
		Payload: PreflightHelperPayload{Scalar: &PreflightScalarHelperPayload{
			SubquerySQL: "select count(*) from orders",
			OutputName:  "scalar_subquery_value",
		}},
	}

	helper, err := runtime.executePreflightHelper(context.Background(), request)
	if err != nil {
		t.Fatalf("execute helper: %v", err)
	}
	if got, want := len(executor.requests), 1; got != want {
		t.Fatalf("requests = %d, want %d", got, want)
	}
	if executor.requests[0].Payload.Scalar.OutputName != "scalar_subquery_value" {
		t.Fatalf("scalar payload = %#v", executor.requests[0].Payload.Scalar)
	}
	cell, diagnostics := scalarSubqueryResultCell(helper.Result.Runtime.RowSet)
	if diagnostics.BlocksNative() || cell.Value != int64(17) {
		t.Fatalf("helper cell = %#v diagnostics = %#v", cell, diagnostics)
	}
}

func TestParentKeyPreflightExecutesNativeRuntimeStep(t *testing.T) {
	helper := &testPreflightHelperExecutor{}
	var gotRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{RowSet: helperRowSet([]qsbridge.ResultCell{
			{Kind: qsbridge.ValueInt, Value: int64(101)},
			{Kind: qsbridge.ValueInt, Value: int64(202)},
		})}, nil
	})
	runtime.PreflightHelpers = helper

	keys, diagnostics, reports, err := runtime.correlatedAveragePartKeys(context.Background(), "p", "Brand#23", "MED BOX", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("part keys: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if helper.executions != 0 {
		t.Fatalf("helper executions = %d, want native runtime path", helper.executions)
	}
	if got, want := keys, []int64{101, 202}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("keys = %#v, want %#v", got, want)
	}
	if gotRequest.SourceIndexes[0] != "part" || gotRequest.ProjectionCount() == 0 || gotRequest.FragmentCount() != 2 {
		t.Fatalf("native request indexes/projection/fragments = %#v/%d/%d", gotRequest.SourceIndexes, gotRequest.ProjectionCount(), gotRequest.FragmentCount())
	}
	if gotRequest.Query.Fragments[0].Field != "p_brand" || gotRequest.Query.Fragments[0].BSIOp != qsbridge.QuantaBSIOpEQ {
		t.Fatalf("brand fragment = %#v", gotRequest.Query.Fragments[0])
	}
	if gotRequest.Query.Fragments[1].Field != "p_container" {
		t.Fatalf("container fragment = %#v", gotRequest.Query.Fragments[1])
	}
	if got, want := len(reports), 1; got != want {
		t.Fatalf("reports = %d, want %d: %#v", got, want, reports)
	}
	if reports[0].NativeTrace == nil || reports[0].NativeTrace.StepKind != qsbridge.NativeSubqueryStepParentKeyLookup || reports[0].NativeTrace.ExecutionMode != "native_runtime_parent_key_lookup" || reports[0].NativeTrace.RowCount != 2 {
		t.Fatalf("native trace = %#v", reports[0].NativeTrace)
	}
}

func TestAggregateThresholdPreflightExecutesNativeRuntimeStep(t *testing.T) {
	helper := &testPreflightHelperExecutor{}
	var gotRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{RowSet: helperRowSetColumns(
			[]qsbridge.ResultCell{
				{Kind: qsbridge.ValueInt, Value: int64(101)},
				{Kind: qsbridge.ValueInt, Value: int64(202)},
			},
			[]qsbridge.ResultCell{
				{Kind: qsbridge.ValueFloat, Value: float64(50)},
				{Kind: qsbridge.ValueFloat, Value: float64(100)},
			},
		)}, nil
	})
	runtime.PreflightHelpers = helper

	thresholds, diagnostics, reports, err := runtime.correlatedAverageThresholds(context.Background(), []int64{101, 202}, 0.2, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("thresholds: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if helper.executions != 0 {
		t.Fatalf("helper executions = %d, want native runtime path", helper.executions)
	}
	if len(thresholds) != 2 || thresholds[0].PartKey != 101 || thresholds[0].Threshold != 10 || thresholds[1].PartKey != 202 || thresholds[1].Threshold != 20 {
		t.Fatalf("thresholds = %#v", thresholds)
	}
	if len(gotRequest.SourceIndexes) != 2 || gotRequest.SourceIndexes[0] != "part" || gotRequest.SourceIndexes[1] != "lineitem" || gotRequest.FragmentCount() != 0 || gotRequest.ProjectionCount() != 2 {
		t.Fatalf("native request indexes/fragments/projection = %#v/%d/%d", gotRequest.SourceIndexes, gotRequest.FragmentCount(), gotRequest.ProjectionCount())
	}
	if !gotRequest.HasCandidateSet || gotRequest.CandidateSet.Index != "part" || len(gotRequest.CandidateSet.Rownums) != 2 || gotRequest.CandidateSet.Rownums[0] != 101 || gotRequest.CandidateSet.Rownums[1] != 202 {
		t.Fatalf("native request candidate set = %#v", gotRequest.CandidateSet)
	}
	if len(gotRequest.Joins) != 1 || gotRequest.Joins[0].Left.Name != "p_partkey" || gotRequest.Joins[0].Right.Name != "l_partkey" || gotRequest.Joins[0].Encoding.Kind != qsbridge.RelationshipEncodingVector {
		t.Fatalf("native request join edge = %#v", gotRequest.Joins)
	}
	if len(gotRequest.GroupBy) != 1 || len(gotRequest.SQLAggregates) != 1 {
		t.Fatalf("native request group/aggregates = %d/%d", len(gotRequest.GroupBy), len(gotRequest.SQLAggregates))
	}
	aggregate := gotRequest.SQLAggregates[0]
	if aggregate.Function != "avg" || aggregate.Alias != "threshold" || aggregate.Type != qsbridge.DataTypeFloat {
		t.Fatalf("aggregate = %#v", aggregate)
	}
	if got, want := len(reports), 1; got != want {
		t.Fatalf("reports = %d, want %d: %#v", got, want, reports)
	}
	if reports[0].NativeTrace == nil || reports[0].NativeTrace.StepKind != qsbridge.NativeSubqueryStepAggregateThresholdLookup || reports[0].NativeTrace.ExecutionMode != "native_runtime_aggregate_threshold_lookup" || reports[0].NativeTrace.RowCount != 2 {
		t.Fatalf("native trace = %#v", reports[0].NativeTrace)
	}
}

func TestNativePrototypePreflightHelperUsesTypedParentKeyPayload(t *testing.T) {
	executor := &nativePrototypePreflightHelperExecutor{}
	runtime := SQLRuntime{PreflightHelpers: executor}

	keys, diagnostics, reports, err := runtime.correlatedAveragePartKeys(context.Background(), "p", "Brand#23", "MED BOX", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("part keys: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if got, want := keys, []int64{101, 202}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("keys = %#v, want %#v", got, want)
	}
	if got := len(executor.requests); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
	if got := len(reports); got != 1 || reports[0].Plan.Kind != PreflightHelperPlanParentKeyLookup {
		t.Fatalf("reports = %#v, want one parent-key report", reports)
	}
	payload := executor.requests[0].Payload.ParentKeyLookup
	if payload == nil || payload.Table != "part" || payload.Alias != "p" || payload.KeyField != "p_partkey" {
		t.Fatalf("parent-key payload = %#v", payload)
	}
	if len(payload.Filters) != 2 || payload.Filters[0].Value != "Brand#23" || payload.Filters[1].Value != "MED BOX" {
		t.Fatalf("parent-key filters = %#v", payload.Filters)
	}
	if reports[0].NativeTrace == nil || reports[0].NativeTrace.StepKind != qsbridge.NativeSubqueryStepParentKeyLookup {
		t.Fatalf("parent-key native trace = %#v", reports[0].NativeTrace)
	}
}

func TestNativePrototypePreflightHelperUsesTypedAggregatePayload(t *testing.T) {
	executor := &nativePrototypePreflightHelperExecutor{}
	runtime := SQLRuntime{PreflightHelpers: executor}

	thresholds, diagnostics, reports, err := runtime.correlatedAverageThresholds(context.Background(), []int64{101}, 0.2, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("thresholds: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if len(thresholds) != 1 || thresholds[0].PartKey != 101 || thresholds[0].Threshold != 10 {
		t.Fatalf("thresholds = %#v", thresholds)
	}
	if got := len(executor.requests); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
	if got := len(reports); got != 1 || reports[0].Plan.Kind != PreflightHelperPlanAggregateThresholdLookup {
		t.Fatalf("reports = %#v, want one aggregate-threshold report", reports)
	}
	payload := executor.requests[0].Payload.AggregateThresholdLookup
	if payload == nil || payload.Table != "lineitem" || payload.KeyField != "l_partkey" || payload.ValueField != "l_quantity" {
		t.Fatalf("aggregate payload = %#v", payload)
	}
	if payload.AggregateFunction != "avg" || payload.Factor != 0.2 || len(payload.PartKeys) != 1 || payload.PartKeys[0] != 101 {
		t.Fatalf("aggregate payload = %#v", payload)
	}
	if reports[0].NativeTrace == nil || reports[0].NativeTrace.StepKind != qsbridge.NativeSubqueryStepAggregateThresholdLookup {
		t.Fatalf("aggregate native trace = %#v", reports[0].NativeTrace)
	}
}

func TestPreflightHelperExecutionRequestReportsScalarPayload(t *testing.T) {
	request := PreflightHelperExecutionRequest{
		Plan: PreflightRewriteHelperPlanDescriptor{Name: "scalar_subquery_value", Kind: PreflightHelperPlanScalarSubquery},
		SQL:  "select count(*) as scalar_subquery_value from orders",
		Payload: PreflightHelperPayload{Scalar: &PreflightScalarHelperPayload{
			SubquerySQL: "select count(*) from orders",
			OutputName:  "scalar_subquery_value",
		}},
	}

	report := request.Report()
	if report.Plan.Kind != PreflightHelperPlanScalarSubquery || report.SQL == "" {
		t.Fatalf("request report = %#v", report)
	}
	if report.Payload.Scalar == nil || report.Payload.Scalar.OutputName != "scalar_subquery_value" || report.Payload.Scalar.SubquerySQL == "" {
		t.Fatalf("scalar payload report = %#v", report.Payload.Scalar)
	}
}

func TestPreflightHelperExecutionRequestReportsParentKeyPayload(t *testing.T) {
	request := correlatedParentKeyHelperRequest("p", "Brand#23", "MED BOX", "select p.p_partkey from part as p", qsbridge.ExecutionOptions{})

	report := request.Report()
	if report.Plan.Kind != PreflightHelperPlanParentKeyLookup {
		t.Fatalf("request report = %#v", report)
	}
	payload := report.Payload.ParentKeyLookup
	if payload == nil || payload.Table != "part" || payload.Alias != "p" || payload.KeyField != "p_partkey" {
		t.Fatalf("parent-key payload report = %#v", payload)
	}
	if len(payload.Filters) != 2 || payload.Filters[0] != "p_brand=Brand#23" || payload.Filters[1] != "p_container=MED BOX" {
		t.Fatalf("parent-key filters = %#v", payload.Filters)
	}
}

func TestPreflightHelperExecutionRequestReportsAggregatePayload(t *testing.T) {
	request := correlatedThresholdHelperRequest([]int64{101, 202}, []qsbridge.QuantaRownum{11, 22}, 0.2, "select l_partkey, avg(l_quantity) from lineitem", qsbridge.ExecutionOptions{})

	report := request.Report()
	if report.Plan.Kind != PreflightHelperPlanAggregateThresholdLookup {
		t.Fatalf("request report = %#v", report)
	}
	payload := report.Payload.AggregateThresholdLookup
	if payload == nil || payload.Table != "lineitem" || payload.AggregateFunction != "avg" || payload.PartKeyCount != 2 {
		t.Fatalf("aggregate payload report = %#v", payload)
	}
	if payload.Factor != 0.2 || payload.KeyOutput != "lineitem.l_partkey" || payload.ValueOutput != "threshold" {
		t.Fatalf("aggregate payload report = %#v", payload)
	}
}

func TestPreflightHelperExecutionDelegatesThroughRuntimeBoundary(t *testing.T) {
	var executions int
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		executions++
		if len(request.SourceIndexes) != 1 || request.SourceIndexes[0] != "orders" {
			t.Fatalf("source indexes = %#v, want orders", request.SourceIndexes)
		}
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: 7}},
			}},
		}}, nil
	})

	helpSQL := "select o_orderkey as scalar_subquery_value from orders where o_orderkey >= 1"
	helper, err := runtime.executePreflightHelper(context.Background(), PreflightHelperExecutionRequest{
		Plan: PreflightRewriteHelperPlanDescriptor{Name: "scalar_subquery_value", Kind: PreflightHelperPlanScalarSubquery},
		SQL:  helpSQL,
		Payload: PreflightHelperPayload{Scalar: &PreflightScalarHelperPayload{
			SubquerySQL: "select o_orderkey from orders where o_orderkey >= 1",
			OutputName:  "scalar_subquery_value",
		}},
		Options: qsbridge.ExecutionOptions{},
	})
	if err != nil {
		t.Fatalf("execute helper: %v", err)
	}
	if executions != 1 {
		t.Fatalf("executions = %d, want 1", executions)
	}
	if helper.SQL != helpSQL {
		t.Fatalf("helper SQL = %q, want %q", helper.SQL, helpSQL)
	}
	if helper.Plan.Kind != PreflightHelperPlanScalarSubquery {
		t.Fatalf("helper plan kind = %q", helper.Plan.Kind)
	}
	if helper.Payload.Scalar == nil || helper.Payload.Scalar.OutputName != "scalar_subquery_value" {
		t.Fatalf("helper payload = %#v", helper.Payload)
	}
	if helper.Diagnostics.BlocksNative() {
		t.Fatalf("helper diagnostics = %#v", helper.Diagnostics)
	}
	cell, diagnostics := scalarSubqueryResultCell(helper.Result.Runtime.RowSet)
	if diagnostics.BlocksNative() || cell.Value != 7 {
		t.Fatalf("helper cell = %#v diagnostics = %#v", cell, diagnostics)
	}
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

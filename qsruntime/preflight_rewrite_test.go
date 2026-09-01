package qsruntime

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestSQLRuntimeExecuteSQLUsesNativeSubqueryPreparationBoundary(t *testing.T) {
	source, err := os.ReadFile("sql_runtime.go")
	if err != nil {
		t.Fatalf("read sql_runtime.go: %v", err)
	}
	text := string(source)
	if !strings.Contains(text, "materializeCorrelatedAggregatePredicates(ctx") {
		t.Fatalf("ExecuteSQL should route correlated aggregate intent through native subquery preparation")
	}
	if strings.Contains(text, "applyPreflightRewrites(ctx") {
		t.Fatalf("ExecuteSQL should not route active SQL rewrite debt through applyPreflightRewrites")
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

func TestPreflightRewriteInventoryHasNoActiveRules(t *testing.T) {
	if got := len(preflightRewriteInventory()); got != 0 {
		t.Fatalf("active preflight rewrite inventory entries = %d, want none", got)
	}
}

func TestNextPreflightNativePromotionCandidateIsEmptyAfterDebtRetired(t *testing.T) {
	candidate := nextPreflightNativePromotionCandidate()
	if candidate.HelperKind != "" {
		t.Fatalf("candidate helper kind = %q, want none", candidate.HelperKind)
	}
	if candidate.Rule != "" {
		t.Fatalf("candidate rule = %q, want none", candidate.Rule)
	}
	if candidate.Follows != "" {
		t.Fatalf("candidate follows = %q, want none", candidate.Follows)
	}
	if candidate.Reason != "" {
		t.Fatalf("candidate reason = %q, want none", candidate.Reason)
	}
}

func TestPreflightSurfaceInventoryClassifiesTypedNativeStepsAndFallbackDebt(t *testing.T) {
	byHelperKind := make(map[PreflightRewriteHelperPlanKind]PreflightSurfaceInventory)
	var fallbackDebt []PreflightSurfaceInventory
	for _, item := range preflightSurfaceInventory() {
		if strings.TrimSpace(item.Name) == "" || strings.TrimSpace(item.CurrentContract) == "" || strings.TrimSpace(item.DeletionCondition) == "" {
			t.Fatalf("surface inventory entry is incomplete: %#v", item)
		}
		if item.HelperKind != "" {
			byHelperKind[item.HelperKind] = item
		}
		if item.Disposition == PreflightSurfaceCompatibilityFallback {
			fallbackDebt = append(fallbackDebt, item)
		}
	}

	for _, item := range preflightRewriteInventory() {
		for _, helperKind := range item.HelperPlanKinds {
			surface, ok := byHelperKind[helperKind]
			if !ok {
				t.Fatalf("helper kind %q from rule %q has no surface inventory entry", helperKind, item.Rule)
			}
			if surface.Disposition != PreflightSurfaceTypedNativeStep {
				t.Fatalf("helper kind %q disposition = %q, want typed native step", helperKind, surface.Disposition)
			}
			if surface.NativeStepKind == "" {
				t.Fatalf("helper kind %q has no native step kind", helperKind)
			}
		}
	}

	if len(fallbackDebt) != 1 || fallbackDebt[0].Name != "sql_backed_preflight_helper_executor" {
		t.Fatalf("fallback debt = %#v, want only sql-backed preflight helper executor", fallbackDebt)
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

func TestPreflightRewriteInventoryAndSurfaceContractsStaySelfContained(t *testing.T) {
	for _, item := range preflightRewriteInventory() {
		if item.Rule == "" {
			t.Fatalf("active rewrite inventory item has no rule: %#v", item)
		}
		if strings.TrimSpace(item.Reason) == "" || strings.TrimSpace(item.FutureIRReplacement) == "" {
			t.Fatalf("active rewrite inventory item is incomplete: %#v", item)
		}
		if len(item.HelperPlanKinds) == 0 || len(item.RegressionCoverage) == 0 {
			t.Fatalf("active rewrite inventory item is missing helper or coverage notes: %#v", item)
		}
	}
	for _, item := range preflightSurfaceInventory() {
		if strings.TrimSpace(item.Name) == "" {
			t.Fatalf("surface inventory item has no name: %#v", item)
		}
		if item.Disposition == "" || strings.TrimSpace(item.CurrentContract) == "" || strings.TrimSpace(item.DeletionCondition) == "" {
			t.Fatalf("surface inventory item is incomplete: %#v", item)
		}
	}
}

func TestPreflightRewriteDescriptorSummariesExposeCorrelatedTypedShape(t *testing.T) {
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
	correlated := testCorrelatedAverageQuantityDescriptor(t, correlatedSQL)
	correlatedSummary := correlated.descriptorSummary()
	if correlatedSummary.Rule != qsbridge.RewriteCorrelatedAggregatePreflight {
		t.Fatalf("correlated rule = %q", correlatedSummary.Rule)
	}
	if correlatedSummary.SourceSQLShape != "predicate(correlated_aggregate_subquery)" {
		t.Fatalf("correlated source shape = %q", correlatedSummary.SourceSQLShape)
	}
	if correlatedSummary.ReplacementSQLShape != "native_predicate(correlated_aggregate_threshold)" {
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
	correlatedIntent := correlatedSummary.SubqueryIntent.CorrelatedAggregate
	if correlatedIntent == nil {
		t.Fatalf("correlated aggregate intent is nil")
	}
	if correlatedIntent.OuterValue.QualifiedName() != "l.l_quantity" ||
		correlatedIntent.InnerValue.QualifiedName() != "l2.l_quantity" ||
		correlatedIntent.InnerKey.QualifiedName() != "l2.l_partkey" ||
		correlatedIntent.OuterKey.QualifiedName() != "p.p_partkey" {
		t.Fatalf("typed correlated aggregate refs = %#v", correlatedIntent)
	}
	if got, want := len(correlatedIntent.RequiredFilterFields), 2; got != want {
		t.Fatalf("typed correlated required filters = %#v, want %d", correlatedIntent.RequiredFilterFields, want)
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
	runtime := newTestSQLRuntime(t)

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
	if got, want := len(result.Preflight.Rewrites), 0; got != want {
		t.Fatalf("preflight rewrites = %d, want %d", got, want)
	}
	if result.Preflight.Total != 0 || result.Preflight.Applied != 0 || result.Preflight.Skipped != 0 || result.Preflight.Duration != 0 {
		t.Fatalf("preflight summary = %#v, want empty no-op pass", result.Preflight)
	}
}

func TestPreflightRewriteDescriptorReportIsCompact(t *testing.T) {
	descriptor := testCorrelatedAverageQuantityDescriptor(t, `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#45'
  and p.p_container = 'MED JAR'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`)

	report := descriptor.descriptorSummary().Report()
	if report.Rule != qsbridge.RewriteCorrelatedAggregatePreflight {
		t.Fatalf("report rule = %q", report.Rule)
	}
	if report.SourceSQLShape != "predicate(correlated_aggregate_subquery)" {
		t.Fatalf("source shape = %q", report.SourceSQLShape)
	}
	if report.ReplacementSQLShape != "native_predicate(correlated_aggregate_threshold)" {
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
}

func TestPreflightRewriteSummaryOmitsScalarDescriptorReportsAfterTypedPromotion(t *testing.T) {
	runtime := newTestSQLRuntime(t)

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
	if got, want := len(reports), 0; got != want {
		t.Fatalf("descriptor reports = %d, want %d after scalar typed promotion: %#v", got, want, reports)
	}
}

func TestPreflightRewriteInventoryDescriptorsAreNativeReplacementReady(t *testing.T) {
	knownSQL := map[qsbridge.RewriteRuleID]string{
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
		descriptor, ok := testPreflightRewriteDescriptor(t, item.Rule, sql)
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
		qsbridge.RewriteCorrelatedAggregatePreflight,
	} {
		summarySQL := map[qsbridge.RewriteRuleID]string{
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
		summary, ok := testPreflightRewriteDescriptor(t, rule, summarySQL)
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
	descriptor := testCorrelatedAverageQuantityDescriptor(t, `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#23'
  and p.p_container = 'MED BOX'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`)

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

func TestCorrelatedPreflightDescriptorIntentCanSeedPlannerPlaceholder(t *testing.T) {
	descriptor := testCorrelatedAverageQuantityDescriptor(t, `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#23'
  and p.p_container = 'MED BOX'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`)
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

type captureParentKeyNativeStepExecutor struct {
	last qsbridge.NativeSubqueryStepExecutionRequest
}

func (e *captureParentKeyNativeStepExecutor) ExecuteNativeSubqueryStep(ctx context.Context, request qsbridge.NativeSubqueryStepExecutionRequest) (qsbridge.NativeSubqueryStepExecutionResult, error) {
	e.last = request
	return qsbridge.NativeSubqueryStepExecutionResult{
		Step: request.Step,
		RowSet: helperRowSet([]qsbridge.ResultCell{
			{Kind: qsbridge.ValueInt, Value: int64(101)},
		}),
	}, nil
}

func TestCorrelatedAveragePartKeySeedsUsesBoundFallbackParameters(t *testing.T) {
	executor := &captureParentKeyNativeStepExecutor{}
	runtime := newTestSQLRuntime(t)
	runtime.NativeSubquerySteps = executor
	brand := "Brand#45\\"
	container := "MED JAR' OR '1'='1"

	seeds, diagnostics, reports, err := runtime.correlatedAveragePartKeySeeds(
		context.Background(),
		"p",
		brand,
		container,
		qsbridge.ExecutionOptions{},
		qsbridge.IndexedParameterValue(7, qsbridge.ValueString, "outer-query-value"),
	)

	if err != nil {
		t.Fatalf("correlated part-key seeds: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if len(seeds) != 1 || seeds[0].PartKey != 101 {
		t.Fatalf("seeds = %#v, want one part key", seeds)
	}
	if len(reports) != 1 {
		t.Fatalf("reports = %d, want 1", len(reports))
	}
	const wantSQL = "select p.p_partkey from part as p where p.p_brand = ? and p.p_container = ? order by p.p_partkey"
	if reports[0].SQL != wantSQL {
		t.Fatalf("helper SQL = %q, want %q", reports[0].SQL, wantSQL)
	}
	if strings.Contains(reports[0].SQL, "Brand#45") || strings.Contains(reports[0].SQL, "MED JAR") {
		t.Fatalf("helper SQL contains client literals: %q", reports[0].SQL)
	}
	values := executor.last.Parameters
	if len(values) != 2 {
		t.Fatalf("native step parameters = %#v, want only helper-scoped brand/container values", values)
	}
	if values[0].Index != 1 || values[0].Kind != qsbridge.ValueString || values[0].Value != brand {
		t.Fatalf("brand parameter = %#v", values[0])
	}
	if values[1].Index != 2 || values[1].Kind != qsbridge.ValueString || values[1].Value != container {
		t.Fatalf("container parameter = %#v", values[1])
	}
}

func TestScalarSubqueryMaterializationReplacesHavingExpressionLiteral(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(29)}},
			}},
		}}, nil
	})
	service := qsbridge.NewPlanningService(runtime.Planner(), nil)

	_, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{SQL: `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`}, qsbridge.ExecutionOptions{})
	materialized, diagnostics, err := runtime.materializeScalarSubqueries(context.Background(), request)
	if err != nil {
		t.Fatalf("materialize scalar: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	binary, ok := materialized.Bound.Prepared.Query.Having[0].Expr.(qsbridge.BinaryExpr)
	if !ok {
		t.Fatalf("having expr = %T, want binary", materialized.Bound.Prepared.Query.Having[0].Expr)
	}
	literal, ok := binary.Right.(qsbridge.LiteralExpr)
	if !ok || literal.Kind != qsbridge.ValueInt || scalarSubqueryTestIntLiteralValue(literal) != 29 {
		t.Fatalf("having right = %#v, want materialized int literal 29", binary.Right)
	}
}

func TestScalarSubqueryMaterializationReplacesWhereExpressionLiteral(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: 7}},
			}},
		}}, nil
	})
	service := qsbridge.NewPlanningService(runtime.Planner(), nil)

	_, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{SQL: `select o_orderkey
from orders
where o_orderkey > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`}, qsbridge.ExecutionOptions{})
	materialized, diagnostics, err := runtime.materializeScalarSubqueries(context.Background(), request)
	if err != nil {
		t.Fatalf("materialize scalar: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	binary, ok := materialized.Bound.Prepared.Query.Predicates[0].Expr.(qsbridge.BinaryExpr)
	if !ok {
		t.Fatalf("where expr = %T, want binary", materialized.Bound.Prepared.Query.Predicates[0].Expr)
	}
	literal, ok := binary.Right.(qsbridge.LiteralExpr)
	if !ok || literal.Kind != qsbridge.ValueInt || scalarSubqueryTestIntLiteralValue(literal) != 7 {
		t.Fatalf("where right = %#v, want materialized int literal 7", binary.Right)
	}
}

func TestSQLRuntimeExecuteSQLMaterializesSelectListScalarSubquery(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueFloat, Value: 55.5}},
			}},
		}}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "select (select avg(o_orderkey) from orders) as average_orderkey", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if result.Runtime.Count != 1 || result.Runtime.RowSet.CandidateCount() != 1 {
		t.Fatalf("runtime count/candidates = %d/%d, want 1/1", result.Runtime.Count, result.Runtime.RowSet.CandidateCount())
	}
	if got, want := len(result.Runtime.RowSet.ProjectionVectors), 1; got != want {
		t.Fatalf("projection vectors = %d, want %d", got, want)
	}
	vector := result.Runtime.RowSet.ProjectionVectors[0]
	if vector.Field.Field != "average_orderkey" || vector.Field.Type != qsbridge.DataTypeFloat {
		t.Fatalf("vector field = %#v, want average_orderkey float", vector.Field)
	}
	if got := vector.Values[0]; got.Kind != qsbridge.ValueFloat || got.Value != 55.5 {
		t.Fatalf("scalar value = %#v, want 55.5", got)
	}
	if got, want := len(result.Intermediate.Fragments), 0; got != want {
		t.Fatalf("fragments = %d, want no bitmap lowering for projection-only select", got)
	}
}

func TestScalarSubqueryMaterializedFloatThresholdLowersAgainstIntegerBSI(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueFloat, Value: 7.5}},
			}},
		}}, nil
	})
	service := qsbridge.NewPlanningService(runtime.Planner(), nil)

	_, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{SQL: `select o_orderkey
from orders
where o_orderkey > (
  select avg(o_orderkey)
  from orders
)`}, qsbridge.ExecutionOptions{})
	materialized, diagnostics, err := runtime.materializeScalarSubqueries(context.Background(), request)
	if err != nil {
		t.Fatalf("materialize scalar: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}

	intermediate, diagnostics := qsbridge.QuantaIntermediateLowerer{}.LowerExecutionRequest(materialized)
	if diagnostics.BlocksNative() {
		t.Fatalf("lower diagnostics = %#v, want none", diagnostics)
	}
	if len(intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1: %#v", len(intermediate.Fragments), intermediate.Fragments)
	}
	fragment := intermediate.Fragments[0]
	if fragment.Field != "o_orderkey" || fragment.BSIOp != qsbridge.QuantaBSIOpGT {
		t.Fatalf("fragment = %#v, want o_orderkey GT", fragment)
	}
	if fragment.Value == nil || fragment.Value.Int64() != 7 {
		t.Fatalf("fragment value = %v, want 7 for o_orderkey > 7.5", fragment.Value)
	}
}

func scalarSubqueryTestIntLiteralValue(literal qsbridge.LiteralExpr) int64 {
	switch value := literal.Value.(type) {
	case int:
		return int64(value)
	case int8:
		return int64(value)
	case int16:
		return int64(value)
	case int32:
		return int64(value)
	case int64:
		return value
	default:
		return 0
	}
}

func TestPreflightRewriteSummaryReportsCorrelatedHelperExecutionPayloads(t *testing.T) {
	runtime := newTestSQLRuntime(t)
	runtime.Environment.Execution.Selector.Direct = nil
	runtime.PreflightHelpers = &nativePrototypePreflightHelperExecutor{}
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

	service := qsbridge.NewPlanningService(runtime.Planner(), nil)
	_, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{SQL: query}, qsbridge.ExecutionOptions{})
	_, summary, diagnostics, err := runtime.materializeCorrelatedAggregatePredicates(context.Background(), request)
	if err != nil {
		t.Fatalf("native correlated aggregate preparation: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if summary.CorrelatedAggregates != 1 {
		t.Fatalf("native subquery summary = %#v, want one correlated aggregate", summary)
	}

	reports := summary.HelperExecutionReports()
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

func TestPreflightRewritePrefersTypedCorrelatedAggregateIntent(t *testing.T) {
	runtime := newTestSQLRuntime(t)
	runtime.NativeSubquerySteps = q17TypedPathNativeStepExecutor{}
	query := `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#23'
  and p.p_container = 'MED BOX'
  and l.l_quantity < (
    select avg(l2.l_quantity) * 0.2
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`

	if _, ok := runtime.correlatedAverageQuantityTypedMatch(query); !ok {
		plan := runtime.Plan(query)
		t.Fatalf("typed correlated aggregate match not found: subqueries=%d diagnostics=%#v", len(plan.Query.Subqueries), plan.Diagnostics)
	}
	service := qsbridge.NewPlanningService(runtime.Planner(), nil)
	_, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{SQL: query}, qsbridge.ExecutionOptions{})
	request, summary, diagnostics, err := runtime.materializeCorrelatedAggregatePredicates(context.Background(), request)
	if err != nil {
		t.Fatalf("native correlated aggregate preparation: %v", err)
	}
	if summary.CorrelatedAggregates != 1 || diagnostics.BlocksNative() {
		t.Fatalf("native preparation = correlated:%d diagnostics:%#v", summary.CorrelatedAggregates, diagnostics)
	}
	if request.Bound.Prepared.SQL != query {
		t.Fatalf("prepared SQL = %q, want original SQL", request.Bound.Prepared.SQL)
	}
	if got := len(request.Bound.Prepared.Query.Subqueries); got != 0 {
		t.Fatalf("remaining subqueries = %d, want consumed correlated aggregate intent", got)
	}
	if got, want := len(summary.NativePredicates.CorrelatedAggregate), 1; got != want {
		t.Fatalf("native correlated aggregate predicates = %d, want %d", got, want)
	}
	predicate := summary.NativePredicates.CorrelatedAggregate[0]
	if predicate.KeyField.Name != "p_partkey" || predicate.ValueField.Name != "l_quantity" || predicate.Operator != qsbridge.BinaryOpLess {
		t.Fatalf("native correlated aggregate predicate = %#v", predicate)
	}
	if len(predicate.Thresholds) != 1 || predicate.Thresholds[0].Key != 101 || predicate.Thresholds[0].Threshold != 10 {
		t.Fatalf("native thresholds = %#v, want p_partkey 101 threshold 10", predicate.Thresholds)
	}
	if got, want := len(summary.Optimization.Rewrites), 1; got != want {
		t.Fatalf("rewrite trace count = %d, want %d: %#v", got, want, summary.Optimization.Rewrites)
	}
	if summary.Optimization.Rewrites[0].Rule != qsbridge.RewriteCorrelatedAggregateNativePredicate {
		t.Fatalf("rewrite trace = %#v, want native correlated aggregate predicate", summary.Optimization.Rewrites[0])
	}
	expression := correlatedAverageNativePredicateExpressionReport(predicate)
	if expression.Kind != qsbridge.ExprKind("native_correlated_aggregate_predicate") || expression.Operator != qsbridge.BinaryOpLess || expression.BranchCount != 1 {
		t.Fatalf("replacement expression report = %#v", expression)
	}
}

type q17TypedPathNativeStepExecutor struct{}

func (q17TypedPathNativeStepExecutor) ExecuteNativeSubqueryStep(ctx context.Context, request qsbridge.NativeSubqueryStepExecutionRequest) (qsbridge.NativeSubqueryStepExecutionResult, error) {
	switch request.Step.Kind {
	case qsbridge.NativeSubqueryStepParentKeyLookup:
		return qsbridge.NativeSubqueryStepExecutionResult{
			Step: request.Step,
			RowSet: helperRowSetColumns([]qsbridge.ResultCell{
				{Kind: qsbridge.ValueInt, Value: int64(101)},
				{Kind: qsbridge.ValueInt, Value: int64(202)},
			}),
		}, nil
	case qsbridge.NativeSubqueryStepAggregateThresholdLookup:
		return qsbridge.NativeSubqueryStepExecutionResult{
			Step: request.Step,
			RowSet: helperRowSetColumns(
				[]qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(101)}},
				[]qsbridge.ResultCell{{Kind: qsbridge.ValueFloat, Value: float64(50)}},
			),
		}, nil
	default:
		return qsbridge.NativeSubqueryStepExecutionResult{Step: request.Step}, nil
	}
}

func TestScalarSubqueryMaterializationReportsInvalidScalarShape(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Count: 2}, nil
	})
	service := qsbridge.NewPlanningService(runtime.Planner(), nil)

	_, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{SQL: `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`}, qsbridge.ExecutionOptions{})
	_, diagnostics, err := runtime.materializeScalarSubqueries(context.Background(), request)
	if err != nil {
		t.Fatalf("materialize scalar: %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocking scalar helper diagnostic", diagnostics)
	}
	if message := diagnostics[0].Message; !strings.Contains(message, "scalar subquery must return exactly one row and one column") {
		t.Fatalf("diagnostic message = %q", message)
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
	runtime := newTestSQLRuntime(t)
	runtime.Environment.Execution.Selector.Direct = nil
	runtime.PreflightHelpers = executor
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

	service := qsbridge.NewPlanningService(runtime.Planner(), nil)
	_, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{SQL: query}, qsbridge.ExecutionOptions{})
	_, summary, diagnostics, err := runtime.materializeCorrelatedAggregatePredicates(context.Background(), request)
	if err != nil {
		t.Fatalf("native correlated aggregate preparation: %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocking correlated helper diagnostic", diagnostics)
	}
	if message := diagnostics[0].Message; !strings.Contains(message, "rule=correlated_aggregate_preflight") || !strings.Contains(message, "helper=parent_key_lookup") || !strings.Contains(message, "phase=execute") {
		t.Fatalf("diagnostic message = %q, want normalized helper context", message)
	}
	reports := summary.HelperExecutionReports()
	if got, want := len(reports), 1; got != want {
		t.Fatalf("helper reports = %d, want %d: %#v", got, want, reports)
	}
	if reports[0].Plan.Kind != PreflightHelperPlanParentKeyLookup || reports[0].Payload.ParentKeyLookup == nil {
		t.Fatalf("helper report = %#v, want parent-key payload report", reports[0])
	}
}

func TestPreflightRewriteInspectionRecordsNoActiveDurations(t *testing.T) {
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
	if result.Preflight.Total != 0 || result.Preflight.Duration != 0 || len(result.Preflight.Rewrites) != 0 {
		t.Fatalf("preflight summary = %#v, want no active rewrite timing", result.Preflight)
	}
}

func TestPreflightRewritesUseHelperExecutionBoundary(t *testing.T) {
	tests := []struct {
		path string
		call string
	}{
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

func TestNativePreflightStepFallsBackOnlyWhenNativeStepMissing(t *testing.T) {
	tests := []struct {
		name       string
		kind       PreflightRewriteHelperPlanKind
		stepKind   qsbridge.NativeSubqueryStepKind
		outputName string
		payload    PreflightHelperPayload
		execute    func(SQLRuntime, context.Context, PreflightHelperExecutionRequest) (PreflightHelperExecutionResult, error)
	}{
		{
			name:       "scalar",
			kind:       PreflightHelperPlanScalarSubquery,
			stepKind:   qsbridge.NativeSubqueryStepScalarMaterialization,
			outputName: "scalar_subquery_value",
			payload: PreflightHelperPayload{Scalar: &PreflightScalarHelperPayload{
				SubquerySQL: "select count(*) from orders",
				OutputName:  "scalar_subquery_value",
			}},
			execute: func(runtime SQLRuntime, ctx context.Context, request PreflightHelperExecutionRequest) (PreflightHelperExecutionResult, error) {
				return runtime.executeScalarNativeSubqueryStep(ctx, request)
			},
		},
		{
			name:       "parent key",
			kind:       PreflightHelperPlanParentKeyLookup,
			stepKind:   qsbridge.NativeSubqueryStepParentKeyLookup,
			outputName: "p_partkey",
			payload: PreflightHelperPayload{ParentKeyLookup: &PreflightParentKeyLookupPayload{
				Table:    "part",
				Alias:    "p",
				KeyField: "p_partkey",
				Filters:  []PreflightHelperEqualityFilter{{Field: "p_brand", Value: "Brand#23"}},
				Output:   "p_partkey",
			}},
			execute: func(runtime SQLRuntime, ctx context.Context, request PreflightHelperExecutionRequest) (PreflightHelperExecutionResult, error) {
				return runtime.executeParentKeyNativeSubqueryStep(ctx, request)
			},
		},
		{
			name:       "aggregate threshold",
			kind:       PreflightHelperPlanAggregateThresholdLookup,
			stepKind:   qsbridge.NativeSubqueryStepAggregateThresholdLookup,
			outputName: "quantity_threshold",
			payload: PreflightHelperPayload{AggregateThresholdLookup: &PreflightAggregateThresholdLookupPayload{
				Table:             "lineitem",
				KeyField:          "l_partkey",
				AggregateFunction: "avg",
				ValueField:        "l_quantity",
				PartKeys:          []int64{101},
				ParentRownums:     []qsbridge.QuantaRownum{1},
				Factor:            0.2,
				KeyOutput:         "l_partkey",
				ValueOutput:       "quantity_threshold",
			}},
			execute: func(runtime SQLRuntime, ctx context.Context, request PreflightHelperExecutionRequest) (PreflightHelperExecutionResult, error) {
				return runtime.executeAggregateThresholdNativeSubqueryStep(ctx, request)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := qsbridge.NativeSubqueryStep{
				Name:          tt.outputName,
				Kind:          tt.stepKind,
				Lifecycle:     qsbridge.SubqueryStepNativeReady,
				Outputs:       []string{tt.outputName},
				ExecutionMode: "test_native_step",
			}
			request := PreflightHelperExecutionRequest{
				Plan: PreflightRewriteHelperPlanDescriptor{
					Name:       tt.outputName,
					Kind:       tt.kind,
					Outputs:    []string{tt.outputName},
					NativeStep: &step,
				},
				SQL:     "select helper_value",
				Payload: tt.payload,
			}
			helper := &testPreflightHelperExecutor{
				result: PreflightHelperExecutionResult{
					Plan:   request.Plan,
					SQL:    "fallback:" + tt.outputName,
					Result: SQLExecutionResult{Runtime: ExecutionResult{RowSet: helperRowSet([]qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(11)}})}},
				},
			}
			native := &testNativeSubqueryStepExecutor{
				result: qsbridge.NativeSubqueryStepExecutionResult{
					Step: step,
					Outputs: map[string]qsbridge.ResultCell{
						tt.outputName: {Kind: qsbridge.ValueInt, Value: int64(44)},
					},
					RowSet: helperRowSet([]qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(44)}}),
				},
			}
			runtime := SQLRuntime{
				PreflightHelpers:    helper,
				NativeSubquerySteps: native,
			}

			result, err := tt.execute(runtime, context.Background(), request)
			if err != nil {
				t.Fatalf("native preflight step: %v", err)
			}
			if helper.executions != 0 {
				t.Fatalf("helper executions with native step = %d, want 0", helper.executions)
			}
			if native.executions != 1 || native.last.Step.Kind != tt.stepKind {
				t.Fatalf("native executions = %d request = %#v, want step kind %s", native.executions, native.last, tt.stepKind)
			}
			if result.NativeTrace == nil || result.NativeTrace.ExecutionMode != "test_native_step" {
				t.Fatalf("native trace = %#v, want test_native_step", result.NativeTrace)
			}
			if tt.kind == PreflightHelperPlanScalarSubquery && (result.Payload.Scalar == nil || result.Payload.Scalar.Materialized.Value != int64(44)) {
				t.Fatalf("scalar payload = %#v, want materialized native output", result.Payload.Scalar)
			}

			request.Plan.NativeStep = nil
			helper.executions = 0
			native.executions = 0
			fallback, err := tt.execute(runtime, context.Background(), request)
			if err != nil {
				t.Fatalf("fallback preflight step: %v", err)
			}
			if helper.executions != 1 {
				t.Fatalf("helper executions without native step = %d, want 1", helper.executions)
			}
			if native.executions != 0 {
				t.Fatalf("native executions without native step = %d, want 0", native.executions)
			}
			if fallback.SQL != "fallback:"+tt.outputName {
				t.Fatalf("fallback SQL = %q", fallback.SQL)
			}
		})
	}
}

func TestScalarNativeStepExecutesRuntimeRequest(t *testing.T) {
	helper := &testPreflightHelperExecutor{}
	var gotRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{RowSet: helperRowSet([]qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(7)}})}, nil
	})
	runtime.PreflightHelpers = helper

	step := qsbridge.NativeSubqueryStep{
		Name:               "scalar_subquery_value",
		Kind:               qsbridge.NativeSubqueryStepScalarMaterialization,
		Lifecycle:          qsbridge.SubqueryStepNativeReady,
		SubqueryKind:       qsbridge.SubqueryIntentScalar,
		Outputs:            []string{"scalar_subquery_value"},
		Materialization:    "one-row single-cell scalar",
		BitmapNativeTarget: "planner scalar expression input evaluated without SQL text replacement",
		ExecutionMode:      "sql_backed_until_bitmap_native_executor_exists",
	}
	helperResult, err := runtime.executeScalarNativeSubqueryStep(context.Background(), PreflightHelperExecutionRequest{
		Plan: PreflightRewriteHelperPlanDescriptor{
			Name:               "scalar_subquery_value",
			Kind:               PreflightHelperPlanScalarSubquery,
			Purpose:            "evaluate a scalar subquery through the native-step boundary",
			SQL:                "select o_orderkey as scalar_subquery_value from orders where o_orderkey >= 1",
			Outputs:            []string{"scalar_subquery_value"},
			Materialization:    "one-row single-cell scalar",
			BitmapNativeTarget: "planner scalar expression input evaluated without SQL text replacement",
			Lifecycle:          qsbridge.SubqueryStepNativeReady,
			NativeStep:         &step,
		},
		SQL: "select o_orderkey as scalar_subquery_value from orders where o_orderkey >= 1",
		Payload: PreflightHelperPayload{Scalar: &PreflightScalarHelperPayload{
			SubquerySQL: "select o_orderkey from orders where o_orderkey >= 1",
			OutputName:  "scalar_subquery_value",
		}},
		Options: qsbridge.ExecutionOptions{},
	})
	if err != nil {
		t.Fatalf("scalar native step: %v", err)
	}
	if helperResult.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", helperResult.Diagnostics)
	}
	if helper.executions != 0 {
		t.Fatalf("helper executions = %d, want native runtime path", helper.executions)
	}
	if gotRequest.SourceIndexes[0] != "orders" || gotRequest.FragmentCount() != 1 || gotRequest.ProjectionCount() != 1 {
		t.Fatalf("native request indexes/fragments/projection = %#v/%d/%d", gotRequest.SourceIndexes, gotRequest.FragmentCount(), gotRequest.ProjectionCount())
	}
	fragment := gotRequest.Query.Fragments[0]
	if fragment.Field != "o_orderkey" || fragment.BSIOp != qsbridge.QuantaBSIOpGE || fragment.Value == nil || fragment.Value.Int64() != 1 {
		t.Fatalf("scalar predicate fragment = %#v", fragment)
	}
	if helperResult.NativeTrace == nil || helperResult.NativeTrace.StepKind != qsbridge.NativeSubqueryStepScalarMaterialization || helperResult.NativeTrace.ExecutionMode != "native_runtime_scalar_materialization" || helperResult.NativeTrace.OutputCount != 1 || helperResult.NativeTrace.RowCount != 1 {
		t.Fatalf("native trace = %#v", helperResult.NativeTrace)
	}
	if helperResult.Payload.Scalar == nil || helperResult.Payload.Scalar.Materialized.Value != int64(7) {
		t.Fatalf("scalar payload = %#v", helperResult.Payload.Scalar)
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

func TestParentKeyNativeStepUsesTypedPayloadNotHelperSQL(t *testing.T) {
	helper := &testPreflightHelperExecutor{}
	var gotRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{RowSet: helperRowSet([]qsbridge.ResultCell{
			{Kind: qsbridge.ValueInt, Value: int64(101)},
		})}, nil
	})
	runtime.PreflightHelpers = helper

	request := correlatedParentKeyHelperRequest("p", "Brand#23", "MED BOX", "this is not SQL", qsbridge.ExecutionOptions{})
	result, err := runtime.executeParentKeyNativeSubqueryStep(context.Background(), request)
	if err != nil {
		t.Fatalf("execute parent-key native step: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if helper.executions != 0 {
		t.Fatalf("helper executions = %d, want typed native path", helper.executions)
	}
	if gotRequest.SourceIndexes[0] != "part" || gotRequest.ProjectionCount() == 0 || gotRequest.FragmentCount() != 2 {
		t.Fatalf("native request indexes/projection/fragments = %#v/%d/%d", gotRequest.SourceIndexes, gotRequest.ProjectionCount(), gotRequest.FragmentCount())
	}
	if gotRequest.Query.Fragments[0].Field != "p_brand" || gotRequest.Query.Fragments[1].Field != "p_container" {
		t.Fatalf("fragments = %#v", gotRequest.Query.Fragments)
	}
	if result.SQL != "this is not SQL" {
		t.Fatalf("helper SQL = %q, want preserved debug text", result.SQL)
	}
	if result.NativeTrace == nil || result.NativeTrace.ExecutionMode != "native_runtime_parent_key_lookup" || result.NativeTrace.RowCount != 1 {
		t.Fatalf("native trace = %#v", result.NativeTrace)
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

func testCorrelatedAverageQuantityDescriptor(t *testing.T, sql string) correlatedAverageQuantityDescriptor {
	t.Helper()
	runtime := newTestSQLRuntime(t)
	match, ok := runtime.correlatedAverageQuantityTypedMatch(sql)
	if !ok {
		plan := runtime.Plan(sql)
		t.Fatalf("correlated typed descriptor not found: subqueries=%d diagnostics=%#v", len(plan.Query.Subqueries), plan.Diagnostics)
	}
	return match.Descriptor
}

func testPreflightRewriteDescriptor(t *testing.T, rule qsbridge.RewriteRuleID, sql string) (*PreflightRewriteDescriptorSummary, bool) {
	t.Helper()
	runtime := newTestSQLRuntime(t)
	switch rule {
	case qsbridge.RewriteCorrelatedAggregatePreflight:
		return runtime.correlatedAverageQuantityRewriteDescriptor(sql)
	default:
		return nil, false
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

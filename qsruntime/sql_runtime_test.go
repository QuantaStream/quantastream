package qsruntime

import (
	"context"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestFindCorrelatedAverageQuantityPredicate(t *testing.T) {
	sql := `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#45'
  and p.p_container = 'MED JAR'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`

	descriptor, ok := findCorrelatedAverageQuantityPredicate(sql)
	if !ok {
		t.Fatalf("correlated average predicate not found")
	}
	if descriptor.OuterLineitem != "l" || descriptor.InnerLineitem != "l2" || descriptor.OuterPart != "p" || descriptor.Factor != 0.2 {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	if descriptor.AggregateFunction != "avg" || descriptor.OuterQuantityRef != "l.l_quantity" || descriptor.InnerQuantityRef != "l2.l_quantity" {
		t.Fatalf("descriptor aggregate refs = %#v", descriptor)
	}
	if descriptor.InnerCorrelatedKey != "l2.l_partkey" || descriptor.OuterCorrelatedKey != "p.p_partkey" {
		t.Fatalf("descriptor correlated keys = %#v", descriptor)
	}
	if len(descriptor.RequiredPartFilters) != 2 || descriptor.RequiredPartFilters[0] != "p.p_brand" || descriptor.RequiredPartFilters[1] != "p.p_container" {
		t.Fatalf("descriptor required filters = %#v", descriptor.RequiredPartFilters)
	}
	brand, container, ok := correlatedPartFilters(sql, descriptor.OuterPart)
	if !ok || brand != "Brand#45" || container != "MED JAR" {
		t.Fatalf("filters = %q/%q ok=%v", brand, container, ok)
	}
}

func TestAliasScalarSubqueryProjectionAddsStableAlias(t *testing.T) {
	sql := `select sum(ps2.ps_supplycost * ps2.ps_availqty) * 0.0001
from partsupp as ps2
where ps2.ps_partkey >= 1`
	got := aliasScalarSubqueryProjection(sql)
	if !strings.Contains(got, "as scalar_subquery_value from partsupp as ps2") {
		t.Fatalf("aliased SQL = %q", got)
	}
}

func TestFindUncorrelatedHavingScalarSubqueryExtractsQ11Shape(t *testing.T) {
	sql := `select ps.ps_partkey,
       sum(ps.ps_supplycost * ps.ps_availqty) as part_value
from partsupp as ps
inner join supplier as s on s.s_suppkey = ps.ps_suppkey
inner join nation as n on n.n_nationkey = s.s_nationkey
where n.n_name = 'GERMANY'
group by ps.ps_partkey
having part_value > (
  select sum(ps2.ps_supplycost * ps2.ps_availqty) * 0.0001
  from partsupp as ps2
  inner join supplier as s2 on s2.s_suppkey = ps2.ps_suppkey
  inner join nation as n2 on n2.n_nationkey = s2.s_nationkey
  where n2.n_name = 'GERMANY'
)
order by part_value desc
limit 10`

	descriptor, ok := findUncorrelatedHavingScalarSubquery(sql)
	if !ok {
		t.Fatalf("scalar HAVING subquery not found")
	}
	if !strings.HasPrefix(descriptor.SubquerySQL, "select sum(ps2.ps_supplycost") || !strings.Contains(descriptor.SubquerySQL, "from partsupp as ps2") || !strings.Contains(descriptor.SubquerySQL, "where n2.n_name = 'GERMANY'") {
		t.Fatalf("subquery = %q", descriptor.SubquerySQL)
	}
	if !strings.Contains(descriptor.ComparisonSQL, "part_value >") {
		t.Fatalf("comparison = %q, want HAVING comparison", descriptor.ComparisonSQL)
	}
	rewritten := descriptor.rewriteWithLiteral(sql, "95025.42544399995")
	if strings.Contains(rewritten, "select sum(ps2") {
		t.Fatalf("rewritten still contains subquery: %s", rewritten)
	}
	if !strings.Contains(rewritten, "having part_value > 95025.42544399995") {
		t.Fatalf("rewritten HAVING missing literal: %s", rewritten)
	}
}

func TestFindUncorrelatedHavingScalarSubquery(t *testing.T) {
	sql := `select ps_partkey, sum(ps_supplycost * ps_availqty) as part_value
from partsupp
group by ps_partkey
having part_value > (
  select sum(ps2.ps_supplycost * ps2.ps_availqty) * 0.0001
  from partsupp as ps2
  where ps2.ps_partkey >= 1
)
order by part_value desc`

	descriptor, ok := findUncorrelatedHavingScalarSubquery(sql)
	if !ok {
		t.Fatalf("scalar HAVING subquery not found")
	}
	if got := descriptor.SubquerySQL; got != "select sum(ps2.ps_supplycost * ps2.ps_availqty) * 0.0001\n  from partsupp as ps2\n  where ps2.ps_partkey >= 1" {
		t.Fatalf("subquery = %q", got)
	}
	if got := descriptor.rewriteWithLiteral(sql, "42"); !strings.Contains(got, "having part_value > 42") {
		t.Fatalf("rewritten = %q, want literal replacement", got)
	}
}

func TestScalarSubqueryResultCellReadsUnnamedProjectionVector(t *testing.T) {
	cell, diagnostics := scalarSubqueryResultCell(qsbridge.QuantaProjectedRowSet{
		Rownums: []qsbridge.QuantaRownum{1},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{{
			Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueFloat, Value: 95025.42544399995}},
		}},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if cell.Value != 95025.42544399995 {
		t.Fatalf("cell = %#v", cell)
	}
}

func TestScalarSubqueryLiteralFormatsNumericCells(t *testing.T) {
	literal, diagnostics := scalarSubqueryLiteral(qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: 95025.425444})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if literal != "95025.425444" {
		t.Fatalf("literal = %q, want 95025.425444", literal)
	}
}

func TestRewriteUncorrelatedHavingScalarSubqueryReturnsOptimizationTrace(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: 7}},
			}},
		}}, nil
	})

	query := `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`
	rewritten, diagnostics, trace, reports, err, ok := runtime.rewriteUncorrelatedHavingScalarSubquery(context.Background(), query, qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if !ok {
		t.Fatalf("rewrite ok = false, want true")
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if got, want := len(reports), 1; got != want || reports[0].Plan.Kind != PreflightHelperPlanScalarSubquery {
		t.Fatalf("helper reports = %#v, want one scalar report", reports)
	}
	if !strings.Contains(rewritten, "having c > 7") {
		t.Fatalf("rewritten SQL = %q, want scalar literal", rewritten)
	}
	if got, want := len(trace.Rewrites), 1; got != want {
		t.Fatalf("rewrite count = %d, want %d: %#v", got, want, trace.Rewrites)
	}
	if trace.Rewrites[0].Rule != qsbridge.RewriteScalarSubqueryPreflight || trace.Rewrites[0].Status != qsbridge.RewriteApplied {
		t.Fatalf("rewrite trace = %#v, want scalar preflight applied", trace.Rewrites[0])
	}
}

func TestSQLRuntimeApplyPreflightRewritesRunsOrderedBoundary(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: 7}},
			}},
		}}, nil
	})

	query := `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`
	result, err := runtime.applyPreflightRewrites(context.Background(), query, qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("preflight rewrites: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if !strings.Contains(result.SQL, "having c > 7") {
		t.Fatalf("rewritten SQL = %q, want scalar literal", result.SQL)
	}
	if got, want := len(result.Optimization.Rewrites), 1; got != want {
		t.Fatalf("rewrite count = %d, want %d: %#v", got, want, result.Optimization.Rewrites)
	}
	if result.Optimization.Rewrites[0].Rule != qsbridge.RewriteScalarSubqueryPreflight {
		t.Fatalf("rewrite = %#v, want scalar preflight", result.Optimization.Rewrites[0])
	}
	if result.Preflight.Total != 2 || result.Preflight.Applied != 1 || result.Preflight.Skipped != 1 {
		t.Fatalf("preflight summary = %#v, want one applied and one skipped", result.Preflight)
	}
	if result.Preflight.Rewrites[0].Rule != qsbridge.RewriteCorrelatedAggregatePreflight || result.Preflight.Rewrites[0].Applied {
		t.Fatalf("first preflight inspection = %#v, want skipped correlated rewrite", result.Preflight.Rewrites[0])
	}
	if result.Preflight.Rewrites[1].Rule != qsbridge.RewriteScalarSubqueryPreflight || !result.Preflight.Rewrites[1].Applied {
		t.Fatalf("second preflight inspection = %#v, want applied scalar rewrite", result.Preflight.Rewrites[1])
	}
}

func TestSQLRuntimeExecuteSQLExposesPreflightSummary(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: 7}},
			}},
		}}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`, qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Preflight.Total != 2 || result.Preflight.Applied != 1 || result.Preflight.Skipped != 1 {
		t.Fatalf("preflight summary = %#v, want one applied and one skipped", result.Preflight)
	}
	helperReports := result.Preflight.HelperExecutionReports()
	if got, want := len(helperReports), 1; got != want {
		t.Fatalf("helper reports = %d, want %d: %#v", got, want, helperReports)
	}
	if helperReports[0].Plan.Kind != PreflightHelperPlanScalarSubquery || helperReports[0].Payload.Scalar == nil {
		t.Fatalf("helper report = %#v, want scalar payload report", helperReports[0])
	}
}

func TestSQLRuntimePreflightRewriteTracesArePlannerVisible(t *testing.T) {
	trace := qsbridge.NewOptimizationTrace()
	trace = mergeRuntimeOptimizationTrace(trace, scalarSubqueryRewriteTrace())
	trace = mergeRuntimeOptimizationTrace(trace, correlatedAverageRewriteTrace())

	if !trace.Supported {
		t.Fatalf("trace supported = false, want true")
	}
	if got, want := len(trace.Rewrites), 2; got != want {
		t.Fatalf("rewrite count = %d, want %d: %#v", got, want, trace.Rewrites)
	}
	if trace.Rewrites[0].Rule != qsbridge.RewriteScalarSubqueryPreflight || trace.Rewrites[0].Status != qsbridge.RewriteApplied {
		t.Fatalf("first rewrite = %#v, want scalar preflight applied", trace.Rewrites[0])
	}
	if trace.Rewrites[1].Rule != qsbridge.RewriteCorrelatedAggregatePreflight || trace.Rewrites[1].Status != qsbridge.RewriteApplied {
		t.Fatalf("second rewrite = %#v, want correlated aggregate preflight applied", trace.Rewrites[1])
	}

	runtime := newTestSQLRuntime(t)
	service := qsbridge.NewPlanningService(runtime.Planner(), nil)
	prepared, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{
		SQL:          "select o_orderkey from orders where o_orderkey >= 1",
		Optimization: trace,
	}, qsbridge.ExecutionOptions{})

	if request.Diagnostics.BlocksNative() {
		t.Fatalf("request diagnostics = %#v, want none", request.Diagnostics)
	}
	rewrites := prepared.Inspection.Optimization.Rewrites
	if got, want := len(rewrites), 2; got != want {
		t.Fatalf("prepared rewrites = %d, want %d: %#v", got, want, rewrites)
	}
	if rewrites[0].Rule != qsbridge.RewriteScalarSubqueryPreflight || rewrites[1].Rule != qsbridge.RewriteCorrelatedAggregatePreflight {
		t.Fatalf("prepared rewrite rules = %#v", rewrites)
	}
}

func TestSQLRuntimeBuilderRequiresParser(t *testing.T) {
	runtime, diagnostics, err := SQLRuntimeBuilder{}.Build(context.Background())

	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if runtime.Environment.Ready() {
		t.Fatalf("runtime = %#v, want not ready", runtime)
	}
	assertRuntimeDiagnosticCode(t, diagnostics, qsbridge.DiagnosticInternalInvariant)
}

func TestSQLRuntimePlansThroughEnvironmentCatalog(t *testing.T) {
	runtime := newTestSQLRuntime(t)

	result := runtime.Plan("select o_orderkey from orders")

	if result.Diagnostics.BlocksNative() {
		t.Fatalf("plan diagnostics = %#v, want none", result.Diagnostics)
	}
	if result.DefaultSchema != "quanta" {
		t.Fatalf("default schema = %q, want quanta", result.DefaultSchema)
	}
	if result.CatalogVersion != "test-catalog-v1" {
		t.Fatalf("catalog version = %q, want test-catalog-v1", result.CatalogVersion)
	}
	if len(result.Query.Sources) != 1 || result.Query.Sources[0].Table != "orders" {
		t.Fatalf("sources = %#v, want orders", result.Query.Sources)
	}
}

func TestSQLRuntimeExecutesPreparedNeutralRequest(t *testing.T) {
	runtime := newTestSQLRuntime(t)

	result, err := runtime.ExecutePrepared(context.Background(), NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))

	if err != nil {
		t.Fatalf("execute prepared: %v", err)
	}
	if result.Count != 11 {
		t.Fatalf("count = %d, want 11", result.Count)
	}
}

func TestSQLRuntimeExecuteSQLPlansLowersAndRunsSimpleSelect(t *testing.T) {
	var gotRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{Count: 3}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "select o_orderkey from orders where o_orderkey >= 100", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if result.Prepared.Kind != qsbridge.QueryKindSelect {
		t.Fatalf("prepared kind = %s, want SELECT", result.Prepared.Kind)
	}
	if len(result.Intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want one: %#v", len(result.Intermediate.Fragments), result.Intermediate.Fragments)
	}
	fragment := result.Intermediate.Fragments[0]
	if fragment.Index != "orders" || fragment.Field != "o_orderkey" || fragment.BSIOp != qsbridge.QuantaBSIOpGE {
		t.Fatalf("fragment = %#v, want orders.o_orderkey >= 100", fragment)
	}
	if gotRequest.FragmentCount() != 1 || gotRequest.ProjectionCount() != 1 {
		t.Fatalf("runtime request fragments/projections = %d/%d, want 1/1", gotRequest.FragmentCount(), gotRequest.ProjectionCount())
	}
	if result.Runtime.Count != 3 {
		t.Fatalf("runtime count = %d, want 3", result.Runtime.Count)
	}
}

func TestSQLRuntimeExecuteSQLExpandsWildcardSelect(t *testing.T) {
	var gotRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{Count: 3}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "select * from orders", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if len(result.Prepared.ResultColumns) != 3 {
		t.Fatalf("result columns = %d, want fixture order fields", len(result.Prepared.ResultColumns))
	}
	if gotRequest.ProjectionCount() != 3 {
		t.Fatalf("runtime projection count = %d, want 3", gotRequest.ProjectionCount())
	}
	if gotRequest.ProjectionOrder[0].Name != "o_orderkey" || gotRequest.ProjectionOrder[2].Name != "o_orderpriority" {
		t.Fatalf("projection order = %#v, want catalog order", gotRequest.ProjectionOrder)
	}
}

func TestSQLRuntimeExecuteSQLRunsInsertMutationWithoutLowering(t *testing.T) {
	var gotRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{Statement: qsbridge.StatementResult{AffectedRows: 1, LastInsertID: 9001}}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "insert into orders (o_orderkey, o_orderpriority) values (9001, '1-URGENT')", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if result.Prepared.Kind != qsbridge.QueryKindInsert {
		t.Fatalf("prepared kind = %s, want INSERT", result.Prepared.Kind)
	}
	if len(result.Intermediate.Fragments) != 0 {
		t.Fatalf("fragments = %d, want no SELECT lowering for insert", len(result.Intermediate.Fragments))
	}
	if gotRequest.Mutation.Kind != qsbridge.MutationInsert {
		t.Fatalf("runtime mutation = %q, want insert", gotRequest.Mutation.Kind)
	}
	if gotRequest.Mutation.Target.Table != "orders" {
		t.Fatalf("runtime target = %#v, want orders", gotRequest.Mutation.Target)
	}
	if len(gotRequest.Mutation.Columns) != 2 || gotRequest.Mutation.Columns[0].Name != "o_orderkey" {
		t.Fatalf("runtime columns = %#v, want order columns", gotRequest.Mutation.Columns)
	}
	if result.Runtime.Statement.AffectedRows != 1 {
		t.Fatalf("affected rows = %d, want 1", result.Runtime.Statement.AffectedRows)
	}
}

func TestSQLRuntimeExecuteSQLReturnsCommitStatementWithoutExecution(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("commit should not execute direct runtime")
		return ExecutionResult{}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), "commit", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if result.Prepared.Kind != qsbridge.QueryKindSession {
		t.Fatalf("prepared kind = %s, want session", result.Prepared.Kind)
	}
	actions := result.Runtime.Statement.SessionActions
	if len(actions) != 1 || actions[0].Kind != qsbridge.SessionActionCommitTransaction {
		t.Fatalf("runtime session actions = %#v, want commit", actions)
	}
	if len(result.Intermediate.Fragments) != 0 {
		t.Fatalf("fragments = %d, want no SELECT lowering for commit", len(result.Intermediate.Fragments))
	}
}

func TestSQLRuntimeInspectSQLPlansLowersAndInspectsWithoutExecution(t *testing.T) {
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		t.Fatalf("inspect should not execute runtime")
		return ExecutionResult{}, nil
	})

	result := runtime.InspectSQL("select o_orderkey from orders where o_orderkey >= 100", qsbridge.ExecutionOptions{})

	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if result.Prepared.Kind != qsbridge.QueryKindSelect {
		t.Fatalf("prepared kind = %s, want SELECT", result.Prepared.Kind)
	}
	if len(result.Intermediate.Fragments) != 1 {
		t.Fatalf("fragments = %d, want one: %#v", len(result.Intermediate.Fragments), result.Intermediate.Fragments)
	}
	if result.Runtime.SelectedExecutor != ExecutionInspectionExecutorDirect {
		t.Fatalf("selected executor = %q, want direct", result.Runtime.SelectedExecutor)
	}
	if result.Runtime.CallPlan.RootIndex != "orders" {
		t.Fatalf("call plan root = %q, want orders", result.Runtime.CallPlan.RootIndex)
	}
	if !result.Runtime.CallPlan.Contains(LegacyExecutionStepBuildBitmapQuery) {
		t.Fatalf("call plan missing bitmap query step: %v", result.Runtime.CallPlan.Steps)
	}
}

func TestSQLRuntimeInspectSQLLowersMixedBooleanFilterDespiteNativeBlocker(t *testing.T) {
	runtime := newTestSQLRuntime(t)

	result := runtime.InspectSQL(
		"select o_orderkey from orders where (o_orderkey = 7 and o_orderkey > 1) or (o_orderkey = 8 and o_orderkey > 2)",
		qsbridge.ExecutionOptions{},
	)

	if result.Supported() {
		t.Fatalf("result supported, want mixed boolean blocker")
	}
	assertRuntimeDiagnosticCode(t, result.Diagnostics, qsbridge.DiagnosticMixedBooleanPredicate)
	filter := result.Intermediate.Filter
	if filter.Operation != qsbridge.QuantaFilterUnion {
		t.Fatalf("filter operation = %s, want %s: %#v", filter.Operation, qsbridge.QuantaFilterUnion, filter)
	}
	if len(filter.Children) != 2 {
		t.Fatalf("filter children = %d, want 2: %#v", len(filter.Children), filter.Children)
	}
	for branchIndex, branch := range filter.Children {
		if branch.Operation != qsbridge.QuantaFilterIntersect {
			t.Fatalf("branch %d operation = %s, want %s: %#v", branchIndex, branch.Operation, qsbridge.QuantaFilterIntersect, branch)
		}
	}
}

func TestSQLRuntimeInspectSQLReturnsParserDiagnostics(t *testing.T) {
	runtime := newTestSQLRuntime(t)

	result := runtime.InspectSQL("select from", qsbridge.ExecutionOptions{})

	if result.Supported() {
		t.Fatalf("result = %#v, want parser diagnostics", result)
	}
	assertRuntimeDiagnosticCode(t, result.Diagnostics, qsbridge.DiagnosticParserBoundary)
}

func TestSQLRuntimeExecuteSQLReturnsParserDiagnostics(t *testing.T) {
	runtime := newTestSQLRuntime(t)

	result, err := runtime.ExecuteSQL(context.Background(), "select from", qsbridge.ExecutionOptions{})

	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Supported() {
		t.Fatalf("result = %#v, want parser diagnostics", result)
	}
	assertRuntimeDiagnosticCode(t, result.Diagnostics, qsbridge.DiagnosticParserBoundary)
}

func newTestSQLRuntime(t *testing.T) SQLRuntime {
	t.Helper()
	return newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Count: 11}, nil
	})
}

func newTestSQLRuntimeWithDirect(t *testing.T, execute func(context.Context, ExecutionRequest) (ExecutionResult, error)) SQLRuntime {
	t.Helper()
	builder := SQLRuntimeBuilder{
		Parser:         qsbridge.SimpleParserBridge{},
		DefaultSchema:  "quanta",
		CatalogVersion: qsbridge.CatalogVersion("test-catalog-v1"),
		EnvironmentBuilder: RuntimeEnvironmentBuilder{
			Config:         NewDirectRuntimeConfig("", "", 0, 0),
			CatalogFactory: LegacyTableCacheCatalogFactory{TableCache: legacyCatalogTestCache()},
			DirectFactory: DirectRuntimeFactoryFunc(func(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error) {
				return DirectRuntimeFunc(execute), nil, nil
			}),
		},
	}
	runtime, diagnostics, err := builder.Build(context.Background())
	if err != nil {
		t.Fatalf("build sql runtime: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("build diagnostics = %#v, want none", diagnostics)
	}
	return runtime
}

package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestCorrelatedAverageQuantityTypedMatchBuildsDescriptorAndFilters(t *testing.T) {
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

	runtime := newTestSQLRuntime(t)
	match, ok := runtime.correlatedAverageQuantityTypedMatch(sql)
	if !ok {
		plan := runtime.Plan(sql)
		t.Fatalf("correlated average typed intent not found: subqueries=%d diagnostics=%#v", len(plan.Query.Subqueries), plan.Diagnostics)
	}
	descriptor := match.Descriptor
	if descriptor.OuterLineitem != "l" || descriptor.InnerLineitem != "l2" || descriptor.OuterPart != "p" || descriptor.Factor != 0.2 {
		t.Fatalf("descriptor = %#v", descriptor)
	}
	if descriptor.AggregateFunction != "avg" || descriptor.OuterQuantity.qualifiedName() != "l.l_quantity" || descriptor.InnerQuantity.qualifiedName() != "l2.l_quantity" {
		t.Fatalf("descriptor aggregate refs = %#v", descriptor)
	}
	if descriptor.InnerQuantity.Table != "lineitem" || descriptor.OuterQuantity.Table != "lineitem" || descriptor.OuterKey.Table != "part" {
		t.Fatalf("descriptor typed fields = %#v", descriptor)
	}
	if descriptor.InnerKey.qualifiedName() != "l2.l_partkey" || descriptor.OuterKey.qualifiedName() != "p.p_partkey" {
		t.Fatalf("descriptor correlated keys = %#v", descriptor)
	}
	if got := correlatedQualifiedNames(descriptor.RequiredFilters); len(got) != 2 || got[0] != "p.p_brand" || got[1] != "p.p_container" {
		t.Fatalf("descriptor required filters = %#v", got)
	}
	brand, container, ok := match.requiredPartFilters()
	if !ok || brand != "Brand#45" || container != "MED JAR" {
		t.Fatalf("filters = %q/%q ok=%v", brand, container, ok)
	}
}

func TestCorrelatedAverageThresholdPredicateExprBuildsTypedBranches(t *testing.T) {
	runtime := newTestSQLRuntime(t)
	sql := `select sum(l.l_extendedprice) / 7.0 as avg_yearly
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#45'
  and p.p_container = 'MED JAR'
  and l.l_quantity < (
    select avg(l2.l_quantity) * 0.2
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`

	match, ok := runtime.correlatedAverageQuantityTypedMatch(sql)
	if !ok {
		t.Fatalf("typed correlated aggregate match not found")
	}
	expr := correlatedAverageThresholdPredicateExpr(match.Descriptor, []q17PartThreshold{
		{PartKey: 101, Threshold: 10},
		{PartKey: 202, Threshold: 20.5},
	})

	or, ok := expr.(qsbridge.BinaryExpr)
	if !ok || or.Op != qsbridge.BinaryOpOr {
		t.Fatalf("expr = %#v, want OR binary", expr)
	}
	assertCorrelatedAverageThresholdBranch(t, or.Left, 101, 10)
	assertCorrelatedAverageThresholdBranch(t, or.Right, 202, 20.5)
}

func TestCorrelatedAverageThresholdPredicateExprBuildsFalsePredicateForEmptyThresholds(t *testing.T) {
	expr := correlatedAverageThresholdPredicateExpr(correlatedAverageQuantityDescriptor{}, nil)

	binary, ok := expr.(qsbridge.BinaryExpr)
	if !ok || binary.Op != qsbridge.BinaryOpEqual {
		t.Fatalf("expr = %#v, want equality false predicate", expr)
	}
	left, leftOK := binary.Left.(qsbridge.LiteralExpr)
	right, rightOK := binary.Right.(qsbridge.LiteralExpr)
	if !leftOK || !rightOK || left.Kind != qsbridge.ValueInt || right.Kind != qsbridge.ValueInt || left.Value != int64(1) || right.Value != int64(0) {
		t.Fatalf("false predicate = %#v", binary)
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

func assertCorrelatedAverageThresholdBranch(t *testing.T, expr qsbridge.Expr, partKey int64, threshold float64) {
	t.Helper()
	and, ok := expr.(qsbridge.BinaryExpr)
	if !ok || and.Op != qsbridge.BinaryOpAnd {
		t.Fatalf("branch = %#v, want AND binary", expr)
	}
	keyEq, ok := and.Left.(qsbridge.BinaryExpr)
	if !ok || keyEq.Op != qsbridge.BinaryOpEqual {
		t.Fatalf("key branch = %#v, want equality", and.Left)
	}
	keyField, ok := keyEq.Left.(qsbridge.FieldExpr)
	if !ok || keyField.Ref.Name != "p_partkey" || keyField.Ref.Table.RefName() != "p" {
		t.Fatalf("key field = %#v", keyEq.Left)
	}
	keyValue, ok := keyEq.Right.(qsbridge.LiteralExpr)
	if !ok || keyValue.Kind != qsbridge.ValueInt || keyValue.Value != partKey {
		t.Fatalf("key value = %#v, want %d", keyEq.Right, partKey)
	}
	quantityLess, ok := and.Right.(qsbridge.BinaryExpr)
	if !ok || quantityLess.Op != qsbridge.BinaryOpLess {
		t.Fatalf("quantity branch = %#v, want less-than", and.Right)
	}
	quantityField, ok := quantityLess.Left.(qsbridge.FieldExpr)
	if !ok || quantityField.Ref.Name != "l_quantity" || quantityField.Ref.Table.RefName() != "l" {
		t.Fatalf("quantity field = %#v", quantityLess.Left)
	}
	thresholdValue, ok := quantityLess.Right.(qsbridge.LiteralExpr)
	if !ok || thresholdValue.Kind != qsbridge.ValueFloat || thresholdValue.Value != threshold {
		t.Fatalf("threshold value = %#v, want %v", quantityLess.Right, threshold)
	}
}

func TestSQLRuntimeApplyPreflightRewritesRunsOrderedBoundary(t *testing.T) {
	runtime := newTestSQLRuntime(t)

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
	if result.SQL != query {
		t.Fatalf("preflight SQL = %q, want original scalar SQL", result.SQL)
	}
	if got, want := len(result.Optimization.Rewrites), 0; got != want {
		t.Fatalf("rewrite count = %d, want %d: %#v", got, want, result.Optimization.Rewrites)
	}
	if result.Preflight.Total != 1 || result.Preflight.Applied != 0 || result.Preflight.Skipped != 1 {
		t.Fatalf("preflight summary = %#v, want one skipped correlated rewrite", result.Preflight)
	}
	if result.Preflight.Rewrites[0].Rule != qsbridge.RewriteCorrelatedAggregatePreflight || result.Preflight.Rewrites[0].Applied {
		t.Fatalf("first preflight inspection = %#v, want skipped correlated rewrite", result.Preflight.Rewrites[0])
	}
}

func TestSQLRuntimeExecuteSQLMaterializesScalarSubqueryInBoundQuery(t *testing.T) {
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
	if result.Preflight.Total != 1 || result.Preflight.Applied != 0 || result.Preflight.Skipped != 1 {
		t.Fatalf("preflight summary = %#v, want scalar materialization outside preflight", result.Preflight)
	}
	if got := result.Preflight.HelperExecutionReports(); len(got) != 0 {
		t.Fatalf("helper reports = %#v, want none for typed scalar materialization", got)
	}
	if got, want := len(result.Request.Bound.Prepared.Query.Having), 1; got != want {
		t.Fatalf("having predicates = %d, want %d", got, want)
	}
	binary, ok := result.Request.Bound.Prepared.Query.Having[0].Expr.(qsbridge.BinaryExpr)
	if !ok {
		t.Fatalf("having expr = %T, want binary", result.Request.Bound.Prepared.Query.Having[0].Expr)
	}
	literal, ok := binary.Right.(qsbridge.LiteralExpr)
	if !ok || literal.Kind != qsbridge.ValueInt || scalarSubqueryTestIntLiteralValue(literal) != 7 {
		t.Fatalf("having right = %#v, want materialized int literal 7", binary.Right)
	}
}

func TestSQLRuntimePreflightRewriteTracesArePlannerVisible(t *testing.T) {
	trace := qsbridge.NewOptimizationTrace()
	trace = mergeRuntimeOptimizationTrace(trace, correlatedAverageRewriteTrace())

	if !trace.Supported {
		t.Fatalf("trace supported = false, want true")
	}
	if got, want := len(trace.Rewrites), 1; got != want {
		t.Fatalf("rewrite count = %d, want %d: %#v", got, want, trace.Rewrites)
	}
	if trace.Rewrites[0].Rule != qsbridge.RewriteCorrelatedAggregatePreflight || trace.Rewrites[0].Status != qsbridge.RewriteApplied {
		t.Fatalf("rewrite = %#v, want correlated aggregate preflight applied", trace.Rewrites[0])
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
	if got, want := len(rewrites), 1; got != want {
		t.Fatalf("prepared rewrites = %d, want %d: %#v", got, want, rewrites)
	}
	if rewrites[0].Rule != qsbridge.RewriteCorrelatedAggregatePreflight {
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

func TestSQLRuntimeExecuteSQLMaterializesTrueExistsGate(t *testing.T) {
	var parentRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		if len(request.Query.Fragments) > 0 {
			return ExecutionResult{Count: 1}, nil
		}
		parentRequest = request
		return ExecutionResult{Count: 11}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `
		select count(*)
		from orders
		where exists (
			select o_orderkey
			from orders
			where o_orderkey = 1
		)
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if parentRequest.HasCandidateSet {
		t.Fatalf("parent request has candidate set for true EXISTS gate: %#v", parentRequest.CandidateSet)
	}
	if got, want := len(result.Request.Bound.Prepared.Query.Predicates), 0; got != want {
		t.Fatalf("prepared predicates = %d, want true gate pruned", got)
	}
}

func TestSQLRuntimeExecuteSQLMaterializesFalseExistsGateAsEmptyCandidateSet(t *testing.T) {
	var parentRequest ExecutionRequest
	runtime := newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		if request.HasCandidateSet {
			parentRequest = request
			return ExecutionResult{Count: uint64(len(request.CandidateSet.Rownums))}, nil
		}
		return ExecutionResult{Count: 0}, nil
	})

	result, err := runtime.ExecuteSQL(context.Background(), `
		select count(*)
		from orders
		where exists (
			select o_orderkey
			from orders
			where o_orderkey = -999
		)
	`, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("execute sql: %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v runtime=%#v", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if !parentRequest.HasCandidateSet {
		t.Fatalf("parent request missing empty candidate set for false EXISTS gate")
	}
	if parentRequest.CandidateSet.Index != "orders" || len(parentRequest.CandidateSet.Rownums) != 0 {
		t.Fatalf("candidate set = %#v, want empty orders set", parentRequest.CandidateSet)
	}
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

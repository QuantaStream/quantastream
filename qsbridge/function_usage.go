package qsbridge

// FunctionUsageContext identifies where a bound function call appears in a query.
type FunctionUsageContext string

const (
	// FunctionUsageProjection means the function appears in the SELECT list.
	FunctionUsageProjection FunctionUsageContext = "projection"
	// FunctionUsagePredicate means the function appears in a WHERE predicate.
	FunctionUsagePredicate FunctionUsageContext = "predicate"
	// FunctionUsageJoinPredicate means the function appears in a JOIN ON predicate.
	FunctionUsageJoinPredicate FunctionUsageContext = "join_predicate"
	// FunctionUsageGroupBy means the function appears in GROUP BY.
	FunctionUsageGroupBy FunctionUsageContext = "group_by"
	// FunctionUsageAggregateInput means the function appears inside an aggregate input.
	FunctionUsageAggregateInput FunctionUsageContext = "aggregate_input"
	// FunctionUsageAggregate means the function is an aggregate slot.
	FunctionUsageAggregate FunctionUsageContext = "aggregate"
	// FunctionUsageAggregateFilter means the function appears inside an aggregate filter.
	FunctionUsageAggregateFilter FunctionUsageContext = "aggregate_filter"
	// FunctionUsageHaving means the function appears in HAVING.
	FunctionUsageHaving FunctionUsageContext = "having"
	// FunctionUsageOrderBy means the function appears in ORDER BY.
	FunctionUsageOrderBy FunctionUsageContext = "order_by"
	// FunctionUsageMutationValue means the function appears in INSERT or UPDATE values.
	FunctionUsageMutationValue FunctionUsageContext = "mutation_value"
	// FunctionUsageMutationPredicate means the function appears in mutation predicates.
	FunctionUsageMutationPredicate FunctionUsageContext = "mutation_predicate"
)

// FunctionUsage records one bound function call occurrence in query order.
type FunctionUsage struct {
	Name          string
	Origin        FunctionOrigin
	Placement     FunctionPlacement
	Context       FunctionUsageContext
	ReturnType    DataType
	Deterministic bool
}

// FunctionUsages returns bound function call occurrences in first-seen query order.
func (q QueryIR) FunctionUsages() []FunctionUsage {
	usages := make([]FunctionUsage, 0)
	collectQueryFunctionUsages(q, &usages)
	return usages
}

func collectQueryFunctionUsages(query QueryIR, usages *[]FunctionUsage) {
	for _, branch := range query.UnionAll {
		collectQueryFunctionUsages(branch, usages)
	}
	for _, column := range query.Projection {
		collectExprFunctionUsages(column.Expr, FunctionUsageProjection, usages)
	}
	for _, predicate := range query.Predicates {
		collectExprFunctionUsages(predicate.Expr, FunctionUsagePredicate, usages)
	}
	for _, edge := range query.Joins {
		for _, predicate := range edge.On {
			collectExprFunctionUsages(predicate.Expr, FunctionUsageJoinPredicate, usages)
		}
	}
	for _, expr := range query.GroupBy {
		collectExprFunctionUsages(expr, FunctionUsageGroupBy, usages)
	}
	for _, aggregate := range query.Aggregates {
		appendAggregateFunctionUsage(aggregate, usages)
		collectExprFunctionUsages(aggregate.Input, FunctionUsageAggregateInput, usages)
		collectExprFunctionUsages(aggregate.Filter, FunctionUsageAggregateFilter, usages)
	}
	for _, predicate := range query.Having {
		collectExprFunctionUsages(predicate.Expr, FunctionUsageHaving, usages)
	}
	for _, sort := range query.OrderBy {
		collectExprFunctionUsages(sort.Expr, FunctionUsageOrderBy, usages)
	}
	for _, row := range query.Mutation.Rows {
		for _, value := range row.Values {
			collectExprFunctionUsages(value, FunctionUsageMutationValue, usages)
		}
	}
	for _, assignment := range query.Mutation.Assignments {
		collectExprFunctionUsages(assignment.Value, FunctionUsageMutationValue, usages)
	}
	for _, predicate := range query.Mutation.Predicates {
		collectExprFunctionUsages(predicate.Expr, FunctionUsageMutationPredicate, usages)
	}
}

func collectExprFunctionUsages(expr Expr, context FunctionUsageContext, usages *[]FunctionUsage) {
	switch n := expr.(type) {
	case nil:
		return
	case CallExpr:
		appendFunctionUsage(n, context, usages)
		for _, arg := range n.Args {
			collectExprFunctionUsages(arg, context, usages)
		}
	case *CallExpr:
		if n != nil {
			appendFunctionUsage(*n, context, usages)
			for _, arg := range n.Args {
				collectExprFunctionUsages(arg, context, usages)
			}
		}
	case BinaryExpr:
		collectExprFunctionUsages(n.Left, context, usages)
		collectExprFunctionUsages(n.Right, context, usages)
	case *BinaryExpr:
		if n != nil {
			collectExprFunctionUsages(n.Left, context, usages)
			collectExprFunctionUsages(n.Right, context, usages)
		}
	}
}

func appendFunctionUsage(call CallExpr, context FunctionUsageContext, usages *[]FunctionUsage) {
	*usages = append(*usages, FunctionUsage{
		Name:          call.Name,
		Origin:        call.Origin,
		Placement:     call.Placement,
		Context:       context,
		ReturnType:    call.Type,
		Deterministic: call.Deterministic,
	})
}

func appendAggregateFunctionUsage(aggregate Aggregate, usages *[]FunctionUsage) {
	if aggregate.Function == "" {
		return
	}
	placement := aggregate.Placement
	if placement == FunctionPlacementUnknown {
		placement = FunctionPlacementAggregate
	}
	*usages = append(*usages, FunctionUsage{
		Name:          aggregate.Function,
		Origin:        aggregate.Origin,
		Placement:     placement,
		Context:       FunctionUsageAggregate,
		ReturnType:    aggregate.Type,
		Deterministic: aggregate.Deterministic,
	})
}

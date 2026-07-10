package qsbridge

// SubqueryIntentKind classifies subquery work before the planner chooses an
// execution strategy.
type SubqueryIntentKind string

const (
	// SubqueryIntentScalar evaluates a subquery that must produce one scalar value.
	SubqueryIntentScalar SubqueryIntentKind = "scalar_subquery"
	// SubqueryIntentCorrelatedAggregate evaluates an aggregate subquery correlated to outer rows.
	SubqueryIntentCorrelatedAggregate SubqueryIntentKind = "correlated_aggregate_subquery"
)

// SubqueryHelperIntent describes helper work implied by a subquery intent.
//
// Helper intent is executor-neutral. SQL text may exist as fallback/debug
// context while the planner and executor grow native bitmap implementations.
type SubqueryHelperIntent struct {
	Name               string
	Kind               string
	Inputs             []string
	Outputs            []string
	Materialization    string
	BitmapNativeTarget string
}

// ScalarSubqueryIntent records the planner-visible shape of a scalar subquery.
type ScalarSubqueryIntent struct {
	SubquerySQL string
	OutputName  string
	Scope       PredicateScope
}

// CorrelatedAggregateSubqueryIntent records the planner-visible shape of a
// correlated aggregate subquery before it becomes native execution nodes.
type CorrelatedAggregateSubqueryIntent struct {
	AggregateFunction string
	Factor            float64
	OuterValueRef     string
	InnerValueRef     string
	InnerKeyRef       string
	OuterKeyRef       string
	RequiredFilters   []string
	Scope             PredicateScope
}

// SubqueryPlanIntent is the durable planner vocabulary for subquery work that
// may still be executed through temporary preflight helpers today.
type SubqueryPlanIntent struct {
	Kind                SubqueryIntentKind
	Capability          PlanCapability
	HelperIntents       []SubqueryHelperIntent
	Scalar              *ScalarSubqueryIntent
	CorrelatedAggregate *CorrelatedAggregateSubqueryIntent
}

// Valid reports whether the intent has the shape required by its kind.
func (i SubqueryPlanIntent) Valid() bool {
	switch i.Kind {
	case SubqueryIntentScalar:
		return i.Scalar != nil && i.Scalar.OutputName != ""
	case SubqueryIntentCorrelatedAggregate:
		return i.CorrelatedAggregate != nil && i.CorrelatedAggregate.AggregateFunction != "" && i.CorrelatedAggregate.InnerKeyRef != "" && i.CorrelatedAggregate.OuterKeyRef != ""
	default:
		return false
	}
}

// HelperKinds returns helper kind names in planner-observed order.
func (i SubqueryPlanIntent) HelperKinds() []string {
	kinds := make([]string, 0, len(i.HelperIntents))
	for _, helper := range i.HelperIntents {
		kinds = append(kinds, helper.Kind)
	}
	return kinds
}

// SubqueryPlanIntentReport is a compact inspection view of subquery planning intent.
type SubqueryPlanIntentReport struct {
	Kind                SubqueryIntentKind
	Capability          PlanCapability
	HelperKinds         []string
	Scalar              *ScalarSubqueryIntentReport
	CorrelatedAggregate *CorrelatedAggregateSubqueryIntentReport
}

// ScalarSubqueryIntentReport summarizes scalar subquery planning intent.
type ScalarSubqueryIntentReport struct {
	OutputName  string
	SubquerySQL string
	Scope       PredicateScope
}

// CorrelatedAggregateSubqueryIntentReport summarizes correlated aggregate subquery planning intent.
type CorrelatedAggregateSubqueryIntentReport struct {
	AggregateFunction string
	Factor            float64
	OuterValueRef     string
	InnerValueRef     string
	InnerKeyRef       string
	OuterKeyRef       string
	RequiredFilters   []string
	Scope             PredicateScope
}

// Report returns a compact inspection view of the subquery planning intent.
func (i SubqueryPlanIntent) Report() SubqueryPlanIntentReport {
	report := SubqueryPlanIntentReport{
		Kind:        i.Kind,
		Capability:  i.Capability,
		HelperKinds: i.HelperKinds(),
	}
	if i.Scalar != nil {
		report.Scalar = &ScalarSubqueryIntentReport{
			OutputName:  i.Scalar.OutputName,
			SubquerySQL: i.Scalar.SubquerySQL,
			Scope:       i.Scalar.Scope,
		}
	}
	if i.CorrelatedAggregate != nil {
		report.CorrelatedAggregate = &CorrelatedAggregateSubqueryIntentReport{
			AggregateFunction: i.CorrelatedAggregate.AggregateFunction,
			Factor:            i.CorrelatedAggregate.Factor,
			OuterValueRef:     i.CorrelatedAggregate.OuterValueRef,
			InnerValueRef:     i.CorrelatedAggregate.InnerValueRef,
			InnerKeyRef:       i.CorrelatedAggregate.InnerKeyRef,
			OuterKeyRef:       i.CorrelatedAggregate.OuterKeyRef,
			RequiredFilters:   append([]string(nil), i.CorrelatedAggregate.RequiredFilters...),
			Scope:             i.CorrelatedAggregate.Scope,
		}
	}
	return report
}

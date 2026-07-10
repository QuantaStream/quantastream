package qsbridge

import "testing"

func TestSubqueryPlanIntentValidatesScalarShape(t *testing.T) {
	intent := SubqueryPlanIntent{
		Kind:       SubqueryIntentScalar,
		Capability: CapabilityScalarSubquery,
		Scalar: &ScalarSubqueryIntent{
			SubquerySQL: "select count(*) from orders",
			OutputName:  "scalar_subquery_value",
			Scope:       PredicateScopeHaving,
		},
		HelperIntents: []SubqueryHelperIntent{{Name: "scalar_subquery_value", Kind: "scalar_subquery"}},
	}

	if !intent.Valid() {
		t.Fatalf("intent should be valid: %#v", intent)
	}
	if got, want := intent.HelperKinds(), []string{"scalar_subquery"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("helper kinds = %#v, want %#v", got, want)
	}
}

func TestSubqueryPlanIntentValidatesCorrelatedAggregateShape(t *testing.T) {
	intent := SubqueryPlanIntent{
		Kind:       SubqueryIntentCorrelatedAggregate,
		Capability: CapabilityScalarSubquery,
		CorrelatedAggregate: &CorrelatedAggregateSubqueryIntent{
			AggregateFunction: "avg",
			Factor:            0.2,
			OuterValueRef:     "l.l_quantity",
			InnerValueRef:     "l2.l_quantity",
			InnerKeyRef:       "l2.l_partkey",
			OuterKeyRef:       "p.p_partkey",
			RequiredFilters:   []string{"p.p_brand", "p.p_container"},
			Scope:             PredicateScopeWhere,
		},
		HelperIntents: []SubqueryHelperIntent{
			{Name: "part_key_lookup", Kind: "parent_key_lookup"},
			{Name: "aggregate_threshold_lookup", Kind: "aggregate_threshold_lookup"},
		},
	}

	if !intent.Valid() {
		t.Fatalf("intent should be valid: %#v", intent)
	}
	if got, want := intent.HelperKinds(), []string{"parent_key_lookup", "aggregate_threshold_lookup"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("helper kinds = %#v, want %#v", got, want)
	}
}

func TestQueryIRCarriesSubqueryPlanIntents(t *testing.T) {
	query := QueryIR{
		Kind: QueryKindSelect,
		Subqueries: []SubqueryPlanIntent{{
			Kind:       SubqueryIntentScalar,
			Capability: CapabilityScalarSubquery,
			Scalar:     &ScalarSubqueryIntent{OutputName: "scalar_subquery_value", Scope: PredicateScopeHaving},
		}},
	}

	if got, want := len(query.Subqueries), 1; got != want {
		t.Fatalf("subqueries = %d, want %d", got, want)
	}
	if !query.Subqueries[0].Valid() {
		t.Fatalf("subquery intent should be valid: %#v", query.Subqueries[0])
	}
}

func TestSubqueryPlanIntentReportSummarizesScalarShape(t *testing.T) {
	intent := SubqueryPlanIntent{
		Kind:       SubqueryIntentScalar,
		Capability: CapabilityScalarSubquery,
		Scalar: &ScalarSubqueryIntent{
			SubquerySQL: "select count(*) from orders",
			OutputName:  "scalar_subquery_value",
			Scope:       PredicateScopeHaving,
		},
		HelperIntents: []SubqueryHelperIntent{{Name: "scalar_subquery_value", Kind: "scalar_subquery"}},
	}

	report := intent.Report()
	if report.Kind != SubqueryIntentScalar || report.Capability != CapabilityScalarSubquery {
		t.Fatalf("report = %#v", report)
	}
	if report.Scalar == nil || report.Scalar.OutputName != "scalar_subquery_value" || report.Scalar.Scope != PredicateScopeHaving {
		t.Fatalf("scalar report = %#v", report.Scalar)
	}
	if got, want := report.HelperKinds, []string{"scalar_subquery"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("helper kinds = %#v, want %#v", got, want)
	}
}

func TestSubqueryPlanIntentReportSummarizesCorrelatedAggregateShape(t *testing.T) {
	intent := SubqueryPlanIntent{
		Kind:       SubqueryIntentCorrelatedAggregate,
		Capability: CapabilityScalarSubquery,
		CorrelatedAggregate: &CorrelatedAggregateSubqueryIntent{
			AggregateFunction: "avg",
			Factor:            0.2,
			OuterValueRef:     "l.l_quantity",
			InnerValueRef:     "l2.l_quantity",
			InnerKeyRef:       "l2.l_partkey",
			OuterKeyRef:       "p.p_partkey",
			RequiredFilters:   []string{"p.p_brand", "p.p_container"},
			Scope:             PredicateScopeWhere,
		},
		HelperIntents: []SubqueryHelperIntent{
			{Name: "part_key_lookup", Kind: "parent_key_lookup"},
			{Name: "aggregate_threshold_lookup", Kind: "aggregate_threshold_lookup"},
		},
	}

	report := intent.Report()
	if report.Kind != SubqueryIntentCorrelatedAggregate || report.CorrelatedAggregate == nil {
		t.Fatalf("report = %#v", report)
	}
	if report.CorrelatedAggregate.AggregateFunction != "avg" || report.CorrelatedAggregate.Factor != 0.2 {
		t.Fatalf("correlated aggregate report = %#v", report.CorrelatedAggregate)
	}
	if got, want := report.HelperKinds, []string{"parent_key_lookup", "aggregate_threshold_lookup"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("helper kinds = %#v, want %#v", got, want)
	}
}

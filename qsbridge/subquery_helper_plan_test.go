package qsbridge

import "testing"

func TestLowerSubqueryHelperPlansFromLogicalPlaceholders(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Index: IndexBSI}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders},
		Subqueries: []SubqueryPlanIntent{{
			Kind:       SubqueryIntentScalar,
			Capability: CapabilityScalarSubquery,
			HelperIntents: []SubqueryHelperIntent{{
				Name:               "scalar_order_count",
				Kind:               string(SubqueryHelperPlanScalarSubquery),
				Outputs:            []string{"scalar_order_count"},
				Materialization:    "one-row single-cell scalar",
				BitmapNativeTarget: "planner scalar expression input evaluated without SQL text replacement",
			}},
			Scalar: &ScalarSubqueryIntent{
				OutputName: "scalar_order_count",
				Scope:      PredicateScopeHaving,
			},
		}, {
			Kind:       SubqueryIntentCorrelatedAggregate,
			Capability: CapabilityScalarSubquery,
			HelperIntents: []SubqueryHelperIntent{{
				Name:               "correlated_parent_keys",
				Kind:               string(SubqueryHelperPlanParentKeyLookup),
				Inputs:             []string{"p.p_brand", "p.p_container"},
				Outputs:            []string{"p.p_partkey"},
				Materialization:    "parent key set",
				BitmapNativeTarget: "bitmap-filtered parent key rownum set",
			}, {
				Name:               "correlated_average_thresholds",
				Kind:               string(SubqueryHelperPlanAggregateThresholdLookup),
				Inputs:             []string{"l2.l_partkey", "l2.l_quantity"},
				Outputs:            []string{"p.p_partkey", "threshold"},
				Materialization:    "per-key aggregate threshold map",
				BitmapNativeTarget: "aggregate-threshold helper kernel feeding bitmap predicate branches",
			}},
			CorrelatedAggregate: &CorrelatedAggregateSubqueryIntent{
				AggregateFunction: "avg",
				InnerKeyRef:       "l2.l_partkey",
				OuterKeyRef:       "p.p_partkey",
			},
		}},
		Projection: []ProjectionColumn{{Expr: Field(orderKey)}},
	}

	plans := LowerSubqueryHelperPlans(BuildLogicalPlan(query).Root)
	if got, want := len(plans), 3; got != want {
		t.Fatalf("helper plans = %d, want %d: %#v", got, want, plans)
	}
	if !subqueryHelperPlanKindPresent(plans, SubqueryHelperPlanScalarSubquery) {
		t.Fatalf("helper plans = %#v, missing scalar helper", plans)
	}
	if !subqueryHelperPlanKindPresent(plans, SubqueryHelperPlanParentKeyLookup) {
		t.Fatalf("helper plans = %#v, missing parent-key helper", plans)
	}
	if !subqueryHelperPlanKindPresent(plans, SubqueryHelperPlanAggregateThresholdLookup) {
		t.Fatalf("helper plans = %#v, missing aggregate-threshold helper", plans)
	}
	steps := nativeSubqueryStepsFromHelperPlans(plans)
	if got, want := len(steps), 3; got != want {
		t.Fatalf("native steps = %d, want %d: %#v", got, want, steps)
	}
	if steps[0].Kind != NativeSubqueryStepParentKeyLookup || steps[1].Kind != NativeSubqueryStepAggregateThresholdLookup || steps[2].Kind != NativeSubqueryStepScalarMaterialization {
		t.Fatalf("native steps = %#v, want parent-key, aggregate-threshold, scalar", steps)
	}
}

func TestLowerSubqueryHelperPlansProvidesFallbackSketches(t *testing.T) {
	root := ScalarSubqueryNode{Intents: []SubqueryPlanIntent{{
		Kind: SubqueryIntentScalar,
		Scalar: &ScalarSubqueryIntent{
			OutputName: "scalar_value",
		},
	}}}

	plans := LowerSubqueryHelperPlans(root)
	if got, want := len(plans), 1; got != want {
		t.Fatalf("helper plans = %d, want %d: %#v", got, want, plans)
	}
	if plans[0].Name != "scalar_value" || plans[0].Kind != SubqueryHelperPlanScalarSubquery {
		t.Fatalf("fallback helper plan = %#v", plans[0])
	}
}

func subqueryHelperPlanKindPresent(plans []SubqueryHelperPlan, kind SubqueryHelperPlanKind) bool {
	for _, plan := range plans {
		if plan.Kind == kind {
			return true
		}
	}
	return false
}

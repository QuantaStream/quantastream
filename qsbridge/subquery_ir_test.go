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

func TestSubqueryPlanIntentValidatesTypedCorrelatedAggregateShape(t *testing.T) {
	outerLineitem := TableInstance{Table: "lineitem", Alias: "l"}
	innerLineitem := TableInstance{Table: "lineitem", Alias: "l2"}
	part := TableInstance{Table: "part", Alias: "p"}
	intent := SubqueryPlanIntent{
		Kind:       SubqueryIntentCorrelatedAggregate,
		Capability: CapabilityScalarSubquery,
		CorrelatedAggregate: &CorrelatedAggregateSubqueryIntent{
			AggregateFunction: "avg",
			Factor:            0.2,
			OuterValue:        FieldRef{Table: outerLineitem, Name: "l_quantity", Type: DataTypeInt},
			InnerValue:        FieldRef{Table: innerLineitem, Name: "l_quantity", Type: DataTypeInt},
			InnerKey:          FieldRef{Table: innerLineitem, Name: "l_partkey", Type: DataTypeInt},
			OuterKey:          FieldRef{Table: part, Name: "p_partkey", Type: DataTypeInt},
			RequiredFilterFields: []FieldRef{
				{Table: part, Name: "p_brand", Type: DataTypeString},
				{Table: part, Name: "p_container", Type: DataTypeString},
			},
			Scope: PredicateScopeWhere,
		},
	}

	if !intent.Valid() {
		t.Fatalf("intent should be valid with typed refs: %#v", intent)
	}
	report := intent.Report()
	if report.CorrelatedAggregate == nil {
		t.Fatalf("report = %#v, want correlated aggregate report", report)
	}
	if report.CorrelatedAggregate.OuterValueRef != "l.l_quantity" ||
		report.CorrelatedAggregate.InnerValueRef != "l2.l_quantity" ||
		report.CorrelatedAggregate.InnerKeyRef != "l2.l_partkey" ||
		report.CorrelatedAggregate.OuterKeyRef != "p.p_partkey" {
		t.Fatalf("correlated aggregate refs = %#v", report.CorrelatedAggregate)
	}
	if got, want := report.CorrelatedAggregate.RequiredFilters, []string{"p.p_brand", "p.p_container"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("required filters = %#v, want %#v", got, want)
	}
}

func TestSubqueryPlanIntentValidatesCorrelatedMembershipShape(t *testing.T) {
	lineitem1 := TableInstance{Table: "lineitem", Alias: "l1"}
	lineitem2 := TableInstance{Table: "lineitem", Alias: "l2"}
	intent := SubqueryPlanIntent{
		Kind:       SubqueryIntentCorrelatedMembership,
		Capability: CapabilitySemiMembership,
		CorrelatedMembership: &CorrelatedMembershipSubqueryIntent{
			Operation: RelationshipJoinOperationSemi,
			OuterDomain: RownumDomain{
				Table: lineitem1,
				Role:  "l1",
			},
			InnerDomain: RownumDomain{
				Table: lineitem2,
				Role:  "l2",
			},
			OuterKeyRef:            "l1.l_orderkey",
			InnerKeyRef:            "l2.l_orderkey",
			CrossDomainPredicates:  []string{"l2.l_suppkey <> l1.l_suppkey"},
			OutputName:             "q21_other_supplier_exists",
			BitmapNativeTarget:     "semi membership over sibling lineitem aliases",
			Scope:                  PredicateScopeWhere,
			RepeatedPhysicalSource: true,
		},
	}

	if !intent.Valid() {
		t.Fatalf("intent should be valid: %#v", intent)
	}
	report := intent.Report()
	if report.CorrelatedMembership == nil {
		t.Fatalf("report = %#v, want correlated membership report", report)
	}
	if report.CorrelatedMembership.Operation != RelationshipJoinOperationSemi ||
		report.CorrelatedMembership.OuterDomain != "l1" ||
		report.CorrelatedMembership.InnerDomain != "l2" ||
		!report.CorrelatedMembership.RepeatedPhysicalSource {
		t.Fatalf("correlated membership report = %#v", report.CorrelatedMembership)
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

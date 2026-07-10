package qsbridge

import "testing"

func TestPreparedPlanPlanInvariantsPassForPreparedSnapshot(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	prepared := service.PrepareWithRequest(PlanRequest{SQL: "select 1"})

	report := prepared.PlanInvariants()
	if !report.Supported() {
		t.Fatalf("report = %#v, want prepared snapshot invariants to pass", report)
	}
	if len(report.Checks) != 8 {
		t.Fatalf("checks = %#v, want eight invariant checks", report.Checks)
	}
	for _, check := range report.Checks {
		if check.Status != PlanInvariantOK {
			t.Fatalf("check = %#v, want all invariants ok", check)
		}
	}
	if !planInvariantHasStatus(report.Checks, "cache_key_available", PlanInvariantOK) {
		t.Fatalf("checks = %#v, want cache key invariant ok", report.Checks)
	}
	if !planInvariantHasStatus(report.Checks, "scalar_subquery_placeholders", PlanInvariantOK) {
		t.Fatalf("checks = %#v, want scalar placeholder invariant ok", report.Checks)
	}
	if !planInvariantHasStatus(report.Checks, "correlated_aggregate_placeholders", PlanInvariantOK) {
		t.Fatalf("checks = %#v, want correlated aggregate placeholder invariant ok", report.Checks)
	}
}

func TestPreparedPlanPlanInvariantsReportInconsistency(t *testing.T) {
	prepared := PreparedPlan{
		Kind:      QueryKindUpdate,
		Supported: true,
		Query: QueryIR{
			Kind: QueryKindSelect,
			Projection: []ProjectionColumn{{
				Alias: "order_id",
				Type:  DataTypeInt,
			}},
		},
	}

	report := prepared.PlanInvariants()
	if report.Supported() {
		t.Fatalf("report = %#v, want inconsistent prepared plan to fail invariants", report)
	}
	if !planInvariantHasStatus(report.Checks, "kind_matches_query", PlanInvariantError) {
		t.Fatalf("checks = %#v, want kind invariant error", report.Checks)
	}
	if !planInvariantHasStatus(report.Checks, "result_columns_match_query", PlanInvariantError) {
		t.Fatalf("checks = %#v, want result column invariant error", report.Checks)
	}
	if !containsDiagnosticCode(report.Diagnostics.Codes(), DiagnosticInternalInvariant) {
		t.Fatalf("diagnostics = %#v, want internal invariant diagnostic", report.Diagnostics)
	}
}

func TestPreparedPlanPlanInvariantsRejectMissingScalarPlaceholderOutputs(t *testing.T) {
	prepared := PreparedPlan{
		SQL:       "select scalar",
		Kind:      QueryKindSelect,
		Supported: true,
		Query:     QueryIR{Kind: QueryKindSelect},
		Logical: LogicalPlan{Root: ScalarSubqueryNode{Intents: []SubqueryPlanIntent{{
			Kind:   SubqueryIntentScalar,
			Scalar: &ScalarSubqueryIntent{},
		}}}},
	}

	report := prepared.PlanInvariants()
	if report.Supported() {
		t.Fatalf("report = %#v, want missing scalar output invariant to fail", report)
	}
	if !planInvariantHasStatus(report.Checks, "scalar_subquery_placeholders", PlanInvariantError) {
		t.Fatalf("checks = %#v, want scalar placeholder invariant error", report.Checks)
	}
	if !containsDiagnosticCode(report.Diagnostics.Codes(), DiagnosticInternalInvariant) {
		t.Fatalf("diagnostics = %#v, want internal invariant diagnostic", report.Diagnostics)
	}
}

func TestPreparedPlanPlanInvariantsAcceptScalarPlaceholderOutputs(t *testing.T) {
	prepared := PreparedPlan{
		SQL:       "select scalar",
		Kind:      QueryKindSelect,
		Supported: true,
		Query:     QueryIR{Kind: QueryKindSelect},
		Logical: LogicalPlan{Root: ScalarSubqueryNode{Intents: []SubqueryPlanIntent{{
			Kind: SubqueryIntentScalar,
			Scalar: &ScalarSubqueryIntent{
				OutputName: "scalar_subquery_value",
			},
		}}}},
	}

	report := prepared.PlanInvariants()
	if !planInvariantHasStatus(report.Checks, "scalar_subquery_placeholders", PlanInvariantOK) {
		t.Fatalf("checks = %#v, want scalar placeholder invariant ok", report.Checks)
	}
}

func TestPreparedPlanPlanInvariantsRejectInvalidCorrelatedAggregatePlaceholders(t *testing.T) {
	prepared := PreparedPlan{
		SQL:       "select correlated",
		Kind:      QueryKindSelect,
		Supported: true,
		Query:     QueryIR{Kind: QueryKindSelect},
		Logical: LogicalPlan{Root: CorrelatedAggregateSubqueryNode{Intents: []SubqueryPlanIntent{{
			Kind: SubqueryIntentCorrelatedAggregate,
			CorrelatedAggregate: &CorrelatedAggregateSubqueryIntent{
				AggregateFunction: "avg",
				InnerKeyRef:       "l2.l_partkey",
			},
		}}}},
	}

	report := prepared.PlanInvariants()
	if report.Supported() {
		t.Fatalf("report = %#v, want invalid correlated aggregate invariant to fail", report)
	}
	if !planInvariantHasStatus(report.Checks, "correlated_aggregate_placeholders", PlanInvariantError) {
		t.Fatalf("checks = %#v, want correlated aggregate placeholder invariant error", report.Checks)
	}
}

func TestPreparedPlanPlanInvariantsCloneCopiesMutableState(t *testing.T) {
	prepared := PreparedPlan{
		Kind:      QueryKindSelect,
		Supported: true,
		Query: QueryIR{
			Kind: QueryKindSelect,
		},
	}

	report := prepared.PlanInvariants()
	cloned := report.Clone()
	cloned.Prepared.Kind = QueryKindUpdate
	cloned.Checks[0].Name = "mutated"
	cloned.Diagnostics = append(cloned.Diagnostics, ErrorDiagnostic(DiagnosticInternalInvariant, PhasePlan, "mutated"))

	again := prepared.PlanInvariants()
	if again.Prepared.Kind != QueryKindSelect {
		t.Fatalf("prepared leaked mutation: %#v", again.Prepared)
	}
	if again.Checks[0].Name == "mutated" || len(again.Diagnostics) != 0 {
		t.Fatalf("checks/diagnostics leaked mutation: %#v/%#v", again.Checks, again.Diagnostics)
	}
}

func planInvariantHasStatus(checks []PlanInvariantCheck, name string, status PlanInvariantStatus) bool {
	for _, check := range checks {
		if check.Name == name && check.Status == status {
			return true
		}
	}
	return false
}

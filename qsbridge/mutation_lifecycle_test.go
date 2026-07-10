package qsbridge

import "testing"

func TestPlanResultMutationLifecycleTracesUpdatePlanningSpine(t *testing.T) {
	parser := stubParserBridge{statement: UnboundStatement{
		SQL:  "update orders set o_totalprice = ? where o_orderkey = ?",
		Kind: QueryKindUpdate,
		Update: UnboundUpdate{
			Table: UnboundTable{Name: "orders", Alias: "o"},
			Assignments: []UnboundAssignment{{
				Column: "o_totalprice",
				Value:  UnboundParameter(1, DataTypeFloat),
			}},
			Predicates: []UnboundPredicate{{
				Expr: UnboundBinary(
					BinaryOpEqual,
					UnboundField("o", "o_orderkey"),
					UnboundParameter(2, DataTypeInt),
				),
				Placement: PredicatePushdown,
				Scope:     PredicateScopeWhere,
			}},
			Result: ResultShape{
				Kind: ResultStatement,
				Statement: StatementResult{
					AffectedRows: 1,
					Status:       "Rows matched: 1",
				},
			},
		},
	}}
	planner := Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementPrimary, Cache: CacheQuery},
	}

	result := planner.Plan("update orders set o_totalprice = ? where o_orderkey = ?")
	trace := result.MutationLifecycle()
	if !trace.Supported {
		t.Fatalf("trace supported = false, diagnostics=%#v", trace.Diagnostics)
	}
	if trace.Kind != QueryKindUpdate || trace.Mutation != MutationUpdate || trace.AccessIntent != PhysicalAccessWrite {
		t.Fatalf("trace kind/mutation/intent = %q/%q/%q, want update/update/write", trace.Kind, trace.Mutation, trace.AccessIntent)
	}
	if trace.Target != "quanta.orders.as o" {
		t.Fatalf("target = %q, want qualified aliased orders target", trace.Target)
	}
	if len(trace.Columns) != 1 || trace.Columns[0] != "o.o_totalprice" {
		t.Fatalf("columns = %#v, want updated totalprice column", trace.Columns)
	}
	if trace.ParameterCount != 2 || trace.Statement.AffectedRows != 1 {
		t.Fatalf("parameters/statement = %d/%#v, want two parameters and OK metadata", trace.ParameterCount, trace.Statement)
	}
	assertMutationLifecycleStages(t, trace.Steps, []MutationLifecycleStage{
		MutationLifecycleParse,
		MutationLifecycleBind,
		MutationLifecycleQueryIR,
		MutationLifecycleLogicalPlan,
		MutationLifecyclePhysicalPlan,
		MutationLifecycleStatementResult,
		MutationLifecycleDiagnostics,
	})
	for _, step := range trace.Steps {
		if !step.Complete || !step.Supported {
			t.Fatalf("step %s = complete:%v supported:%v detail:%q diagnostics:%#v", step.Stage, step.Complete, step.Supported, step.Detail, step.Diagnostics)
		}
	}
	queryIR := mutationLifecycleStepByStage(t, trace.Steps, MutationLifecycleQueryIR)
	if queryIR.Assignments != 1 || queryIR.Predicates != 1 || queryIR.ParameterCount != 2 {
		t.Fatalf("query IR step = %#v, want assignment, predicate, and parameter counts", queryIR)
	}
	logical := mutationLifecycleStepByStage(t, trace.Steps, MutationLifecycleLogicalPlan)
	if logical.LogicalRoot != PlanNodeStatement {
		t.Fatalf("logical root = %q, want statement", logical.LogicalRoot)
	}
	physical := mutationLifecycleStepByStage(t, trace.Steps, MutationLifecyclePhysicalPlan)
	if physical.PhysicalRoot != PhysicalNodeStatement {
		t.Fatalf("physical root = %q, want physical statement", physical.PhysicalRoot)
	}
}

func TestPlanResultMutationLifecycleRejectsSelect(t *testing.T) {
	result := testPreparedSelectPlan(t)
	trace := result.MutationLifecycle()
	if trace.Supported {
		t.Fatalf("trace supported = true, want false for select")
	}
	if !trace.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want non-mutation lifecycle blocker", trace.Diagnostics)
	}
	if got := trace.Diagnostics.Codes()[0]; got != DiagnosticUnsupportedSQL {
		t.Fatalf("diagnostic = %q, want unsupported sql", got)
	}
	queryIR := mutationLifecycleStepByStage(t, trace.Steps, MutationLifecycleQueryIR)
	if queryIR.Complete || queryIR.Supported {
		t.Fatalf("query IR step = complete:%v supported:%v, want false/false for select", queryIR.Complete, queryIR.Supported)
	}
	diagnostics := mutationLifecycleStepByStage(t, trace.Steps, MutationLifecycleDiagnostics)
	if diagnostics.Supported {
		t.Fatalf("diagnostics step supported = true, want false")
	}
}

func assertMutationLifecycleStages(t *testing.T, steps []MutationLifecycleStep, want []MutationLifecycleStage) {
	t.Helper()
	if len(steps) != len(want) {
		t.Fatalf("steps = %d, want %d: %#v", len(steps), len(want), steps)
	}
	for i := range want {
		if steps[i].Stage != want[i] {
			t.Fatalf("steps[%d] = %s, want %s", i, steps[i].Stage, want[i])
		}
	}
}

func mutationLifecycleStepByStage(t *testing.T, steps []MutationLifecycleStep, stage MutationLifecycleStage) MutationLifecycleStep {
	t.Helper()
	for _, step := range steps {
		if step.Stage == stage {
			return step
		}
	}
	t.Fatalf("stage %s not found in %#v", stage, steps)
	return MutationLifecycleStep{}
}

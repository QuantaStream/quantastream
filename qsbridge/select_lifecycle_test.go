package qsbridge

import "testing"

func TestPlanResultSimpleSelectLifecycleTracesPlanningSpine(t *testing.T) {
	parser := stubParserBridge{statement: UnboundStatement{
		SQL:  "select o_orderkey from orders where o_totalprice >= ? order by o_orderkey limit 10",
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables: []UnboundTable{{Name: "orders", Alias: "o"}},
			Projection: []UnboundProjection{{
				Expr:  UnboundField("o", "o_orderkey"),
				Alias: "order_id",
				Type:  DataTypeInt,
			}},
			Predicates: []UnboundPredicate{{
				Expr: UnboundBinary(
					BinaryOpGreaterEqual,
					UnboundField("o", "o_totalprice"),
					UnboundParameter(1, DataTypeFloat),
				),
				Placement: PredicatePushdown,
				Scope:     PredicateScopeWhere,
			}},
			OrderBy: []UnboundSort{{
				Expr:      UnboundField("o", "o_orderkey"),
				Direction: SortAscending,
			}},
			Result: ResultShape{Kind: ResultQuery, Limit: 10},
		},
	}}
	planner := Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal, Cache: CacheQuery},
	}

	result := planner.Plan("select o_orderkey from orders where o_totalprice >= ? order by o_orderkey limit 10")
	trace := result.SimpleSelectLifecycle()
	if !trace.Supported {
		t.Fatalf("trace supported = false, diagnostics=%#v", trace.Diagnostics)
	}
	if trace.Kind != QueryKindSelect {
		t.Fatalf("trace kind = %q, want select", trace.Kind)
	}
	if len(trace.Sources) != 1 || trace.Sources[0] != "quanta.orders.as o" {
		t.Fatalf("sources = %#v, want qualified aliased source", trace.Sources)
	}
	if len(trace.RequiredFields) != 2 {
		t.Fatalf("required fields = %#v, want orderkey and totalprice", trace.RequiredFields)
	}
	if len(trace.ResultColumns) != 1 || trace.ResultColumns[0].Name != "order_id" {
		t.Fatalf("result columns = %#v, want order_id", trace.ResultColumns)
	}
	assertLifecycleStages(t, trace.Steps, []SelectLifecycleStage{
		SelectLifecycleParse,
		SelectLifecycleBind,
		SelectLifecycleQueryIR,
		SelectLifecycleLogicalPlan,
		SelectLifecyclePhysicalPlan,
		SelectLifecycleResultSchema,
		SelectLifecycleDiagnostics,
	})
	for _, step := range trace.Steps {
		if !step.Complete || !step.Supported {
			t.Fatalf("step %s = complete:%v supported:%v detail:%q diagnostics:%#v", step.Stage, step.Complete, step.Supported, step.Detail, step.Diagnostics)
		}
	}
	logical := lifecycleStepByStage(t, trace.Steps, SelectLifecycleLogicalPlan)
	if logical.LogicalRoot != PlanNodeLimit {
		t.Fatalf("logical root = %q, want limit", logical.LogicalRoot)
	}
	physical := lifecycleStepByStage(t, trace.Steps, SelectLifecyclePhysicalPlan)
	if physical.PhysicalRoot != PhysicalNodeLimit {
		t.Fatalf("physical root = %q, want physical limit", physical.PhysicalRoot)
	}
	schema := lifecycleStepByStage(t, trace.Steps, SelectLifecycleResultSchema)
	if schema.ResultColumns != 1 {
		t.Fatalf("schema result columns = %d, want 1", schema.ResultColumns)
	}
}

func TestPlanResultSimpleSelectLifecycleRejectsNonSelect(t *testing.T) {
	parser := stubParserBridge{statement: UnboundStatement{
		SQL:  "delete from orders where o_orderkey = ?",
		Kind: QueryKindDelete,
		Delete: UnboundDelete{
			Table: UnboundTable{Name: "orders"},
			Predicates: []UnboundPredicate{{
				Expr: UnboundBinary(
					BinaryOpEqual,
					UnboundField("", "o_orderkey"),
					UnboundParameter(1, DataTypeInt),
				),
			}},
			Result: ResultShape{Kind: ResultStatement},
		},
	}}
	planner := Planner{Parser: parser, Catalog: testBindCatalog(), DefaultSchema: "quanta"}

	trace := planner.Plan("delete from orders where o_orderkey = ?").SimpleSelectLifecycle()
	if trace.Supported {
		t.Fatalf("trace supported = true, want false for non-select")
	}
	if !trace.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want non-select lifecycle blocker", trace.Diagnostics)
	}
	if got := trace.Diagnostics.Codes()[0]; got != DiagnosticUnsupportedSQL {
		t.Fatalf("diagnostic = %q, want unsupported sql", got)
	}
	queryIR := lifecycleStepByStage(t, trace.Steps, SelectLifecycleQueryIR)
	if queryIR.Complete || queryIR.Supported {
		t.Fatalf("query IR step = complete:%v supported:%v, want false/false for non-select", queryIR.Complete, queryIR.Supported)
	}
	diagnostics := lifecycleStepByStage(t, trace.Steps, SelectLifecycleDiagnostics)
	if diagnostics.Supported {
		t.Fatalf("diagnostics step supported = true, want false")
	}
}

func assertLifecycleStages(t *testing.T, steps []SelectLifecycleStep, want []SelectLifecycleStage) {
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

func lifecycleStepByStage(t *testing.T, steps []SelectLifecycleStep, stage SelectLifecycleStage) SelectLifecycleStep {
	t.Helper()
	for _, step := range steps {
		if step.Stage == stage {
			return step
		}
	}
	t.Fatalf("stage %s not found in %#v", stage, steps)
	return SelectLifecycleStep{}
}

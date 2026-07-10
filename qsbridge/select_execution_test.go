package qsbridge

import "testing"

func TestPlanResultSimpleSelectExecutionResultReturnsCompletedQueryEnvelope(t *testing.T) {
	parser := stubParserBridge{statement: UnboundStatement{
		SQL:  "select o_orderkey from orders order by o_orderkey limit 10",
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables: []UnboundTable{{Name: "orders", Alias: "o"}},
			Projection: []UnboundProjection{{
				Expr:  UnboundField("o", "o_orderkey"),
				Alias: "order_id",
				Type:  DataTypeInt,
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

	result := planner.Plan("select o_orderkey from orders order by o_orderkey limit 10").SimpleSelectExecutionResult(ExecutionOptions{
		RequestID:      "select-1",
		TraceExplain:   true,
		IncludeProfile: true,
	})
	if result.RequestID != "select-1" || result.Kind != ResultQuery || result.Status != ExecutionComplete || !result.Complete {
		t.Fatalf("result envelope = request:%q kind:%q status:%q complete:%v", result.RequestID, result.Kind, result.Status, result.Complete)
	}
	if len(result.Columns) != 1 || result.Columns[0].Name != "order_id" {
		t.Fatalf("columns = %#v, want order_id", result.Columns)
	}
	if len(result.Chunks) != 1 || !result.Chunks[0].Final || result.RowsReturned != 0 {
		t.Fatalf("chunks = %#v rows=%d, want one final empty chunk", result.Chunks, result.RowsReturned)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want non-blocking", result.Diagnostics)
	}
	if result.Profile.AccessIntent != PhysicalAccessRead || result.Profile.Lifecycle != ClientPlanLifecycleSelect || result.Profile.LifecycleSteps != 7 {
		t.Fatalf("profile lifecycle = %#v, want read/select/7", result.Profile)
	}
	if !result.Profile.TraceExplain || !result.Profile.IncludeProfile || result.Profile.LogicalPlan == "" || result.Profile.PhysicalPlan == "" {
		t.Fatalf("profile = %#v, want requested explain/profile metadata", result.Profile)
	}
}

func TestPlanResultSimpleSelectExecutionResultRejectsNonSelect(t *testing.T) {
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

	result := planner.Plan("delete from orders where o_orderkey = ?").SimpleSelectExecutionResult(ExecutionOptions{
		RequestID: "delete-1",
	})
	if result.Status != ExecutionFailed || result.Complete {
		t.Fatalf("result status = %q complete=%v, want failed/incomplete", result.Status, result.Complete)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want non-select blocker", result.Diagnostics)
	}
	if got := result.Diagnostics.Codes()[0]; got != DiagnosticUnsupportedSQL {
		t.Fatalf("diagnostic = %q, want unsupported sql", got)
	}
}

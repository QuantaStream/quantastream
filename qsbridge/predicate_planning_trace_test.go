package qsbridge

import "testing"

func TestPlanResultPredicatePlanningTraceShowsBSIRangePushdown(t *testing.T) {
	parser := stubParserBridge{statement: UnboundStatement{
		SQL:  "select o_orderkey from orders where o_totalprice >= ?",
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
			Result: ResultShape{Kind: ResultQuery},
		},
	}}
	trace := testPredicatePlanningTrace(t, parser)
	step := predicateTraceStep(t, trace, 0)

	if step.Placement != PredicatePushdown || step.Operator != BinaryOpGreaterEqual {
		t.Fatalf("step placement/operator = %q/%q, want pushdown/>=", step.Placement, step.Operator)
	}
	if !planTraceHas(step.InferredCapabilities, CapabilityEncodingRange) {
		t.Fatalf("inferred capabilities = %#v, want EncodingRange", step.InferredCapabilities)
	}
	if len(step.FieldEvidence) != 1 || step.FieldEvidence[0].Encoding != EncodingNumericBSI {
		t.Fatalf("field evidence = %#v, want numeric BSI", step.FieldEvidence)
	}
	if !predicateTraceHas(step.PredicateCapabilities, PredicateCapabilityRange) {
		t.Fatalf("predicate capabilities = %#v, want range", step.PredicateCapabilities)
	}
}

func TestPlanResultPredicatePlanningTraceShowsStringEnumPrefixLike(t *testing.T) {
	parser := stubParserBridge{statement: UnboundStatement{
		SQL:  "select l_shipmode from lineitem where l_shipmode like 'AIR%'",
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables: []UnboundTable{{Name: "lineitem", Alias: "l"}},
			Projection: []UnboundProjection{{
				Expr:  UnboundField("l", "l_shipmode"),
				Alias: "ship_mode",
				Type:  DataTypeString,
			}},
			Predicates: []UnboundPredicate{{
				Expr: UnboundBinary(
					BinaryOpLike,
					UnboundField("l", "l_shipmode"),
					UnboundLiteral(ValueString, "AIR%"),
				),
				Placement: PredicatePushdown,
				Scope:     PredicateScopeWhere,
			}},
			Result: ResultShape{Kind: ResultQuery},
		},
	}}
	trace := testPredicatePlanningTrace(t, parser)
	step := predicateTraceStep(t, trace, 0)

	if step.Operator != BinaryOpLike {
		t.Fatalf("operator = %q, want LIKE", step.Operator)
	}
	if !planTraceHas(step.InferredCapabilities, CapabilityStringEnumPrefixLike) {
		t.Fatalf("inferred capabilities = %#v, want StringEnumPrefixLike", step.InferredCapabilities)
	}
	if !planTraceHas(step.InferredCapabilities, CapabilityEncodingPrefix) {
		t.Fatalf("inferred capabilities = %#v, want EncodingPrefix", step.InferredCapabilities)
	}
	if len(step.FieldEvidence) != 1 || step.FieldEvidence[0].Dictionary != "quanta.lineitem.l_shipmode" {
		t.Fatalf("field evidence = %#v, want l_shipmode dictionary", step.FieldEvidence)
	}
}

func TestPlanResultPredicatePlanningTraceShowsResidualBackingString(t *testing.T) {
	parser := stubParserBridge{statement: UnboundStatement{
		SQL:  "select c_name from customer where c_name = ?",
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables: []UnboundTable{{Name: "customer", Alias: "c"}},
			Projection: []UnboundProjection{{
				Expr:  UnboundField("c", "c_name"),
				Alias: "name",
				Type:  DataTypeString,
			}},
			Predicates: []UnboundPredicate{{
				Expr: UnboundBinary(
					BinaryOpEqual,
					UnboundField("c", "c_name"),
					UnboundParameter(1, DataTypeString),
				),
				Placement: PredicateResidualScan,
				Scope:     PredicateScopeWhere,
			}},
			Result: ResultShape{Kind: ResultQuery},
		},
	}}
	trace := testPredicatePlanningTrace(t, parser)
	step := predicateTraceStep(t, trace, 0)

	if step.Placement != PredicateResidualScan {
		t.Fatalf("placement = %q, want residual scan", step.Placement)
	}
	if !planTraceHas(step.InferredCapabilities, CapabilityResidualScan) {
		t.Fatalf("inferred capabilities = %#v, want residual scan", step.InferredCapabilities)
	}
	if len(step.FieldEvidence) != 1 || step.FieldEvidence[0].Encoding != EncodingBackingString {
		t.Fatalf("field evidence = %#v, want backing string", step.FieldEvidence)
	}
	if !step.FieldEvidence[0].Searchable || step.FieldEvidence[0].RehydrationStore != "kv" {
		t.Fatalf("field evidence = %#v, want searchable kv-backed string", step.FieldEvidence[0])
	}
}

func testPredicatePlanningTrace(t *testing.T, parser ParserBridge) PredicatePlanningTrace {
	t.Helper()
	planner := Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal, Cache: CacheQuery},
	}
	trace := planner.Plan("select test").PredicatePlanningTrace()
	if !trace.Supported {
		t.Fatalf("trace supported = false diagnostics=%#v", trace.Diagnostics)
	}
	if len(trace.Predicates) != 1 {
		t.Fatalf("predicate count = %d, want 1: %#v", len(trace.Predicates), trace.Predicates)
	}
	return trace
}

func predicateTraceStep(t *testing.T, trace PredicatePlanningTrace, index int) PredicatePlanningStep {
	t.Helper()
	if index < 0 || index >= len(trace.Predicates) {
		t.Fatalf("predicate index %d outside %#v", index, trace.Predicates)
	}
	return trace.Predicates[index]
}

func planTraceHas(capabilities []PlanCapability, want PlanCapability) bool {
	for _, capability := range capabilities {
		if capability == want {
			return true
		}
	}
	return false
}

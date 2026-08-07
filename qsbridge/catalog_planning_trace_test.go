package qsbridge

import "testing"

func TestPlanResultCatalogPlanningTraceShowsNumericBSITraits(t *testing.T) {
	parser := stubParserBridge{statement: UnboundStatement{
		SQL:  "select o_totalprice from orders where o_totalprice >= ?",
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables: []UnboundTable{{Name: "orders", Alias: "o"}},
			Projection: []UnboundProjection{{
				Expr:  UnboundField("o", "o_totalprice"),
				Alias: "total_price",
				Type:  DataTypeFloat,
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
	trace := testCatalogPlanningTrace(t, parser)
	field := catalogTraceFieldByName(t, trace, "o.o_totalprice")

	if field.Encoding != EncodingNumericBSI || field.LegacyEncoding != "FloatScaleBSI" {
		t.Fatalf("field encoding = %q/%q, want numeric BSI FloatScaleBSI", field.Encoding, field.LegacyEncoding)
	}
	if field.Scale != 2 {
		t.Fatalf("field scale = %d, want 2", field.Scale)
	}
	if field.Rehydration != RehydrationInline || field.RequiresLookup {
		t.Fatalf("rehydration = %q lookup=%v, want inline without lookup", field.Rehydration, field.RequiresLookup)
	}
	if !predicateTraceHas(field.PredicateCapabilities, PredicateCapabilityRange) {
		t.Fatalf("predicate capabilities = %#v, want range", field.PredicateCapabilities)
	}
	if !projectionTraceHas(field.ProjectionCapabilities, ProjectionCapabilityInline) {
		t.Fatalf("projection capabilities = %#v, want inline", field.ProjectionCapabilities)
	}
}

func TestPlanResultCatalogPlanningTraceShowsStringEnumDictionaryTraits(t *testing.T) {
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
	trace := testCatalogPlanningTrace(t, parser)
	field := catalogTraceFieldByName(t, trace, "l.l_shipmode")

	if field.Encoding != EncodingStringEnum || field.LegacyEncoding != "StringEnum" {
		t.Fatalf("field encoding = %q/%q, want StringEnum", field.Encoding, field.LegacyEncoding)
	}
	if field.Dictionary != "quanta.lineitem.l_shipmode" || field.DictionaryVersion != "v1" {
		t.Fatalf("dictionary = %q version=%q, want l_shipmode v1", field.Dictionary, field.DictionaryVersion)
	}
	if !dictionaryTraceHas(field.DictionaryCapabilities, DictionaryCapabilityPrefixMatch) {
		t.Fatalf("dictionary capabilities = %#v, want prefix match", field.DictionaryCapabilities)
	}
	if field.Rehydration != RehydrationLookup || !field.RequiresLookup {
		t.Fatalf("rehydration = %q lookup=%v, want lookup", field.Rehydration, field.RequiresLookup)
	}
	if !predicateTraceHas(field.PredicateCapabilities, PredicateCapabilityPrefix) {
		t.Fatalf("predicate capabilities = %#v, want prefix", field.PredicateCapabilities)
	}
}

func TestPlanResultCatalogPlanningTraceShowsBackingStringTraits(t *testing.T) {
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
	trace := testCatalogPlanningTrace(t, parser)
	field := catalogTraceFieldByName(t, trace, "c.c_name")

	if field.Encoding != EncodingStringLexBSI || field.LegacyEncoding != "StringLexBSI" {
		t.Fatalf("field encoding = %q/%q, want StringLexBSI", field.Encoding, field.LegacyEncoding)
	}
	if !field.Searchable || field.SearchMode != "text" {
		t.Fatalf("search = %v/%q, want text searchable", field.Searchable, field.SearchMode)
	}
	if field.Rehydration != RehydrationInline || field.RequiresLookup {
		t.Fatalf("rehydration = %q store=%q lookup=%v, want inline lexical value", field.Rehydration, field.RehydrationStore, field.RequiresLookup)
	}
	if !predicateTraceHas(field.PredicateCapabilities, PredicateCapabilityEquality) ||
		!predicateTraceHas(field.PredicateCapabilities, PredicateCapabilityRange) ||
		!predicateTraceHas(field.PredicateCapabilities, PredicateCapabilityPrefix) {
		t.Fatalf("predicate capabilities = %#v, want lexical capabilities", field.PredicateCapabilities)
	}
}

func testCatalogPlanningTrace(t *testing.T, parser ParserBridge) CatalogPlanningTrace {
	t.Helper()
	planner := Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal, Cache: CacheQuery},
	}
	trace := planner.Plan("select test").CatalogPlanningTrace()
	if !trace.Supported {
		t.Fatalf("trace supported = false diagnostics=%#v", trace.Diagnostics)
	}
	return trace
}

func catalogTraceFieldByName(t *testing.T, trace CatalogPlanningTrace, name string) CatalogPlanningFieldTrace {
	t.Helper()
	for _, field := range trace.Fields {
		if field.Field == name {
			return field
		}
	}
	t.Fatalf("field %q not found in %#v", name, trace.Fields)
	return CatalogPlanningFieldTrace{}
}

func predicateTraceHas(capabilities []PredicateCapability, want PredicateCapability) bool {
	for _, capability := range capabilities {
		if capability == want {
			return true
		}
	}
	return false
}

func projectionTraceHas(capabilities []ProjectionCapability, want ProjectionCapability) bool {
	for _, capability := range capabilities {
		if capability == want {
			return true
		}
	}
	return false
}

func dictionaryTraceHas(capabilities []DictionaryCapability, want DictionaryCapability) bool {
	for _, capability := range capabilities {
		if capability == want {
			return true
		}
	}
	return false
}

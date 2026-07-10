package qsbridge

import "testing"

func TestPlannerPlanBuildsPlanningEnvelope(t *testing.T) {
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
			Result: ResultShape{Kind: ResultQuery, Limit: 10},
		},
	}}
	planner := Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope: PhysicalScope{
			Placement: PlacementLocal,
			Cache:     CacheQuery,
		},
	}

	result := planner.Plan("select o_orderkey from orders where o_totalprice >= ?")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", result.Diagnostics)
	}
	if !result.Supported {
		t.Fatalf("expected supported planning result")
	}
	if result.Unbound.Kind != QueryKindSelect || result.Query.Kind != QueryKindSelect {
		t.Fatalf("statement/query kinds = %q/%q, want select/select", result.Unbound.Kind, result.Query.Kind)
	}
	if result.Logical.Root == nil || result.Physical.Root == nil {
		t.Fatalf("expected logical and physical roots")
	}
	if result.Inspection.Query.Kind != QueryKindSelect {
		t.Fatalf("inspection kind = %q, want select", result.Inspection.Query.Kind)
	}
	if len(result.Inspection.Query.Parameters) != 1 || result.Inspection.Query.Parameters[0].Index != 1 {
		t.Fatalf("inspection parameters = %#v, want placeholder 1", result.Inspection.Query.Parameters)
	}
	if result.Physical.Root.PhysicalScope().Placement != PlacementLocal {
		t.Fatalf("physical placement = %q, want local", result.Physical.Root.PhysicalScope().Placement)
	}
}

func TestPlannerPlanWithRequestOverridesDefaults(t *testing.T) {
	parser := stubParserBridge{statement: UnboundStatement{
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables: []UnboundTable{{Name: "orders"}},
			Projection: []UnboundProjection{{
				Expr: UnboundField("", "o_orderkey"),
				Type: DataTypeInt,
			}},
			Result: ResultShape{Kind: ResultQuery},
		},
	}}
	planner := Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "missing",
		Scope:         PhysicalScope{Placement: PlacementLocal},
	}

	result := planner.PlanWithRequest(PlanRequest{
		SQL:           "select o_orderkey from orders",
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementPrimary},
	})
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", result.Diagnostics)
	}
	if result.Physical.Root.PhysicalScope().Placement != PlacementPrimary {
		t.Fatalf("physical placement = %q, want primary", result.Physical.Root.PhysicalScope().Placement)
	}
}

func TestPlannerAddsOptimizationCandidateAdvisories(t *testing.T) {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal},
	}

	result := planner.Plan("select l.l_shipmode as ship_mode, count(*) as line_count from lineitem as l group by l.l_shipmode order by line_count desc limit 5")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", result.Diagnostics)
	}
	rewrites := result.Inspection.Optimization.Rewrites
	topN, ok := plannerTestRewriteByRule(rewrites, RewriteTopNGroupedCount)
	if !ok {
		t.Fatalf("optimizer rewrites = %#v, want topn advisory", rewrites)
	}
	if topN.Status != RewriteAdvisory {
		t.Fatalf("rewrite = %#v, want advisory status", topN)
	}
}

func TestPlannerMergesRequestOptimizationTraceWithDetectedAdvisories(t *testing.T) {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal},
	}
	requestTrace := NewOptimizationTrace()
	requestTrace.Add(RewriteAdvisoryRecord(RewritePredicatePushdown, "caller supplied advisory"))

	result := planner.PlanWithRequest(PlanRequest{
		SQL:          "select l.l_shipmode as ship_mode, count(*) as line_count from lineitem as l group by l.l_shipmode order by line_count desc limit 5",
		Optimization: requestTrace,
	})
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", result.Diagnostics)
	}
	rewrites := result.Inspection.Optimization.Rewrites
	if rewrites[0].Rule != RewritePredicatePushdown {
		t.Fatalf("rewrites = %#v, want preserved caller trace first", rewrites)
	}
	if _, ok := plannerTestRewriteByRule(rewrites, RewriteTopNGroupedCount); !ok {
		t.Fatalf("rewrites = %#v, want detected topn advisory", rewrites)
	}
}

func TestPlannerClassifiesSameRowDateComparisonAsNativeCandidate(t *testing.T) {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal},
	}

	result := planner.Plan("select count(*) from lineitem as l where l.l_commitdate < l.l_receiptdate")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", result.Diagnostics)
	}
	if len(result.Query.Predicates) != 1 {
		t.Fatalf("predicates = %#v, want one", result.Query.Predicates)
	}
	hasPredicateCapability := false
	for _, capability := range result.Query.Predicates[0].Capabilities {
		if capability == CapabilityNativeSameRowBSIComparison {
			hasPredicateCapability = true
			break
		}
	}
	if !hasPredicateCapability {
		t.Fatalf("predicate capabilities = %#v, want same-row BSI comparison", result.Query.Predicates[0].Capabilities)
	}
	if !ClassifyNative(result.Query).HasCapability(CapabilityNativeSameRowBSIComparison) {
		t.Fatalf("classification capabilities = %#v, want same-row BSI comparison", ClassifyNative(result.Query).Capabilities)
	}
	plans := SameRowComparisonPlans(result.Query)
	if len(plans) != 1 || plans[0].Left.Name != "l_commitdate" || plans[0].Right.Name != "l_receiptdate" {
		t.Fatalf("same-row plans = %#v, want commitdate < receiptdate", plans)
	}
}

func plannerTestRewriteByRule(rewrites []RewriteRecord, rule RewriteRuleID) (RewriteRecord, bool) {
	for _, rewrite := range rewrites {
		if rewrite.Rule == rule {
			return rewrite, true
		}
	}
	return RewriteRecord{}, false
}

func TestPlannerPropagatesCatalogVersionIntoPlanAndPreparedCacheKey(t *testing.T) {
	parser := stubParserBridge{statement: UnboundStatement{
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables: []UnboundTable{{Name: "orders"}},
			Projection: []UnboundProjection{{
				Expr: UnboundField("", "o_orderkey"),
				Type: DataTypeInt,
			}},
			Result: ResultShape{Kind: ResultQuery},
		},
	}}
	planner := Planner{
		Parser:         parser,
		Catalog:        testBindCatalog(),
		DefaultSchema:  "quanta",
		CatalogVersion: "catalog-v1",
	}

	result := planner.Plan("select o_orderkey from orders")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", result.Diagnostics)
	}
	if result.CatalogVersion != "catalog-v1" {
		t.Fatalf("catalog version = %q, want planner default", result.CatalogVersion)
	}
	prepared := result.PreparedPlan()
	if prepared.CatalogVersion != "catalog-v1" {
		t.Fatalf("prepared catalog version = %q, want planner default", prepared.CatalogVersion)
	}

	changed := planner.PlanWithRequest(PlanRequest{
		SQL:            "select o_orderkey from orders",
		CatalogVersion: "catalog-v2",
	})
	if changed.CacheKey().Digest == result.CacheKey().Digest {
		t.Fatalf("cache key did not change across catalog versions")
	}
}

func TestPlannerUsesSessionSchemaWhenRequestSchemaIsEmpty(t *testing.T) {
	parser := stubParserBridge{statement: UnboundStatement{
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables: []UnboundTable{{Name: "orders"}},
			Projection: []UnboundProjection{{
				Expr: UnboundField("", "o_orderkey"),
				Type: DataTypeInt,
			}},
			Result: ResultShape{Kind: ResultQuery},
		},
	}}
	planner := Planner{
		Parser:  parser,
		Catalog: testBindCatalog(),
		Session: SessionContext{
			ID:            "session-1",
			User:          "moli",
			Roles:         []RoleName{"reader"},
			CurrentSchema: "quanta",
			TimeZone:      "UTC",
			SQLModes:      []SQLMode{"ansi_quotes"},
			Variables:     map[string]string{"autocommit": "1"},
		},
	}

	result := planner.Plan("select o_orderkey from orders")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", result.Diagnostics)
	}
	if result.Session.CurrentSchema != "quanta" || result.Session.User != "moli" {
		t.Fatalf("session = %#v, want copied session context", result.Session)
	}
	if !result.Session.HasRole("reader") || !result.Session.HasSQLMode("ansi_quotes") {
		t.Fatalf("session role/mode helpers failed: %#v", result.Session)
	}

	result.Session.Roles[0] = "mutated"
	result.Session.Variables["autocommit"] = "0"
	if planner.Session.Roles[0] != "reader" || planner.Session.Variables["autocommit"] != "1" {
		t.Fatalf("planner session was mutated through result: %#v", planner.Session)
	}
}

func TestPlannerStopsOnParserDiagnostics(t *testing.T) {
	planner := Planner{
		Parser: stubParserBridge{
			diagnostics: DiagnosticSet{
				ErrorDiagnostic(DiagnosticParserBoundary, PhaseParse, "could not parse sql"),
			},
		},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}

	result := planner.Plan("select")
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected parser diagnostics to block native planning")
	}
	if result.Query.Kind != "" {
		t.Fatalf("query kind = %q, want empty query after parser blocker", result.Query.Kind)
	}
	if result.Logical.Root != nil || result.Physical.Root != nil {
		t.Fatalf("expected no plans after parser blocker")
	}
}

func TestPlannerReportsNilParser(t *testing.T) {
	result := Planner{Catalog: testBindCatalog(), DefaultSchema: "quanta"}.Plan("select 1")
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("expected nil parser diagnostic")
	}
	if got := result.Diagnostics.Codes()[0]; got != DiagnosticInternalInvariant {
		t.Fatalf("diagnostic code = %q, want %q", got, DiagnosticInternalInvariant)
	}
}

type stubParserBridge struct {
	statement   UnboundStatement
	diagnostics DiagnosticSet
}

func (p stubParserBridge) Parse(sql string) (UnboundStatement, DiagnosticSet) {
	statement := p.statement
	if statement.SQL == "" {
		statement.SQL = sql
	}
	return statement, p.diagnostics
}

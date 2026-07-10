package qsbridge

import "testing"

func TestPlanRequestCacheKeyIsDeterministicForUnorderedSessionAndScopeSets(t *testing.T) {
	first := PlanRequest{
		SQL:            "select * from orders",
		DefaultSchema:  "quanta",
		CatalogVersion: "catalog-v1",
		Session: SessionContext{
			User:      "moli",
			Roles:     []RoleName{"writer", "reader"},
			SQLModes:  []SQLMode{"ansi_quotes", "strict"},
			TimeZone:  "UTC",
			Variables: map[string]string{"b": "2", "a": "1"},
		},
		Scope: PhysicalScope{
			Shards:    ShardSet{Shards: []ShardID{"shard-b", "shard-a"}},
			Replicas:  []ReplicaID{"replica-2", "replica-1"},
			Routing:   "orders",
			Placement: PlacementLocal,
			Cache:     CacheQuery,
		},
	}
	second := first
	second.Session.Roles = []RoleName{"reader", "writer"}
	second.Session.SQLModes = []SQLMode{"strict", "ansi_quotes"}
	second.Session.Variables = map[string]string{"a": "1", "b": "2"}
	second.Scope.Shards.Shards = []ShardID{"shard-a", "shard-b"}
	second.Scope.Replicas = []ReplicaID{"replica-1", "replica-2"}

	firstKey := first.CacheKey()
	secondKey := second.CacheKey()
	if firstKey.Digest == "" {
		t.Fatalf("expected digest")
	}
	if firstKey.Digest != secondKey.Digest {
		t.Fatalf("digest changed for unordered equivalent inputs: %q != %q", firstKey.Digest, secondKey.Digest)
	}
	if firstKey.Roles[0] != "reader" || firstKey.SQLModes[0] != "ansi_quotes" {
		t.Fatalf("key did not canonicalize roles/modes: %#v", firstKey)
	}
}

func TestPlanRequestCacheKeyChangesAcrossPlanningBoundaries(t *testing.T) {
	base := PlanRequest{
		SQL:           "select * from orders",
		DefaultSchema: "quanta",
		Session: SessionContext{
			User:          "moli",
			Roles:         []RoleName{"reader"},
			SQLModes:      []SQLMode{"strict"},
			CurrentSchema: "quanta",
		},
		Scope: PhysicalScope{Placement: PlacementLocal},
	}

	baseKey := base.CacheKey()
	changedSQL := base
	changedSQL.SQL = "select * from customer"
	changedSchema := base
	changedSchema.DefaultSchema = "other"
	changedMode := base
	changedMode.Session.SQLModes = []SQLMode{"ansi_quotes"}
	changedScope := base
	changedScope.Scope.Placement = PlacementPrimary
	changedCatalog := base
	changedCatalog.CatalogVersion = "catalog-v2"

	for name, key := range map[string]PlanCacheKey{
		"sql":     changedSQL.CacheKey(),
		"schema":  changedSchema.CacheKey(),
		"mode":    changedMode.CacheKey(),
		"scope":   changedScope.CacheKey(),
		"catalog": changedCatalog.CacheKey(),
	} {
		if key.Digest == baseKey.Digest {
			t.Fatalf("%s digest = base digest %q, want distinct", name, key.Digest)
		}
	}
}

func TestDefaultPlanCacheKeyPolicySeparatesPlanningFromDisplayAndExecuteFactors(t *testing.T) {
	policies := DefaultPlanCacheKeyPolicy()
	if len(policies) == 0 {
		t.Fatal("DefaultPlanCacheKeyPolicy returned no policies")
	}

	for _, factor := range []PlanCacheFactor{
		PlanCacheFactorSQL,
		PlanCacheFactorSchema,
		PlanCacheFactorCatalogVersion,
		PlanCacheFactorUser,
		PlanCacheFactorRoles,
		PlanCacheFactorSQLModes,
		PlanCacheFactorTimeZone,
		PlanCacheFactorVariables,
		PlanCacheFactorPhysicalScope,
	} {
		if !PlanCacheFactorParticipates(factor) {
			t.Fatalf("%s should participate in prepared-plan cache identity", factor)
		}
	}

	for _, factor := range []PlanCacheFactor{
		PlanCacheFactorOptimizationTrace,
		PlanCacheFactorExplainOptions,
		PlanCacheFactorProfileOptions,
		PlanCacheFactorParameterValues,
		PlanCacheFactorResultBatching,
		PlanCacheFactorCursorMode,
	} {
		if PlanCacheFactorParticipates(factor) {
			t.Fatalf("%s should not participate in prepared-plan cache identity", factor)
		}
	}

	explainPolicy, ok := planCachePolicyByFactor(policies, PlanCacheFactorExplainOptions)
	if !ok || explainPolicy.Participation != PlanCacheParticipationDisplayOnly {
		t.Fatalf("explain policy = %#v/%v, want display-only exclusion", explainPolicy, ok)
	}
	parameterPolicy, ok := planCachePolicyByFactor(policies, PlanCacheFactorParameterValues)
	if !ok || parameterPolicy.Participation != PlanCacheParticipationExecuteOnly {
		t.Fatalf("parameter policy = %#v/%v, want execute-only exclusion", parameterPolicy, ok)
	}
}

func TestDefaultPlanCacheKeyPolicyReturnsCopies(t *testing.T) {
	first := DefaultPlanCacheKeyPolicy()
	first[0].Reason = "mutated"

	second := DefaultPlanCacheKeyPolicy()
	if second[0].Reason == "mutated" {
		t.Fatalf("plan cache key policies leaked mutable state: %#v", second[0])
	}
}

func TestPlanAndPreparedResultsExposeCacheKey(t *testing.T) {
	result := PlanResult{
		SQL:            "select * from orders",
		DefaultSchema:  "quanta",
		CatalogVersion: "catalog-v1",
		Session:        SessionContext{User: "moli", Roles: []RoleName{"reader"}},
		Scope:          PhysicalScope{Placement: PlacementLocal},
	}
	prepared := result.PreparedPlan()

	if result.CacheKey().Digest == "" {
		t.Fatalf("expected plan result cache key")
	}
	if prepared.CacheKey().Digest != result.CacheKey().Digest {
		t.Fatalf("prepared cache key = %q, want plan result cache key %q", prepared.CacheKey().Digest, result.CacheKey().Digest)
	}

	prepared.Session.Roles[0] = "mutated"
	if result.Session.Roles[0] != "reader" {
		t.Fatalf("prepared plan leaked mutable session roles")
	}
}

func planCachePolicyByFactor(policies []PlanCacheKeyPolicy, factor PlanCacheFactor) (PlanCacheKeyPolicy, bool) {
	for _, policy := range policies {
		if policy.Factor == factor {
			return policy, true
		}
	}
	return PlanCacheKeyPolicy{}, false
}

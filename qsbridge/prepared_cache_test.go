package qsbridge

import "testing"

func TestMemoryPreparedPlanCacheStoresAndReturnsPreparedPlan(t *testing.T) {
	cache := NewMemoryPreparedPlanCache()
	plan := PreparedPlan{
		Handle:        PreparedStatementHandle{ID: 7, Name: "stmt"},
		SQL:           "select o_orderkey from orders where o_orderkey = ?",
		DefaultSchema: "quanta",
		Session: SessionContext{
			User:      "moli",
			Roles:     []RoleName{"reader"},
			Variables: map[string]string{"autocommit": "1"},
		},
		Scope:         PhysicalScope{Placement: PlacementLocal},
		Kind:          QueryKindSelect,
		Parameters:    []ParameterRef{{Index: 1, Type: DataTypeInt}},
		ResultColumns: []ResultColumn{{Name: "o_orderkey", Type: DataTypeInt}},
		Query: QueryIR{
			Kind: QueryKindSelect,
			Projection: []ProjectionColumn{{
				Alias: "o_orderkey",
				Type:  DataTypeInt,
			}},
		},
		Supported: true,
	}

	cache.Put(plan)
	cached, ok := cache.Get(plan.CacheKey())
	if !ok {
		t.Fatalf("expected cached prepared plan")
	}
	if cached.SQL != plan.SQL || cached.Kind != QueryKindSelect {
		t.Fatalf("cached plan = %#v, want original SQL/select kind", cached)
	}
	if cached.Handle.ID != 7 || cached.Handle.Name != "stmt" {
		t.Fatalf("cached handle = %#v, want prepared statement handle", cached.Handle)
	}
	if len(cached.Parameters) != 1 || cached.Parameters[0].Type != DataTypeInt {
		t.Fatalf("cached parameters = %#v, want int parameter", cached.Parameters)
	}
	if len(cached.ResultColumns) != 1 || cached.ResultColumns[0].Name != "o_orderkey" {
		t.Fatalf("cached result columns = %#v, want o_orderkey", cached.ResultColumns)
	}
}

func TestMemoryPreparedPlanCacheReturnsCopies(t *testing.T) {
	cache := NewMemoryPreparedPlanCache()
	plan := PreparedPlan{
		SQL:           "select o_orderkey from orders",
		DefaultSchema: "quanta",
		Session: SessionContext{
			Roles:     []RoleName{"reader"},
			Variables: map[string]string{"autocommit": "1"},
		},
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeInt}},
		Query: QueryIR{
			Projection: []ProjectionColumn{{Alias: "o_orderkey"}},
		},
	}

	cache.Put(plan)
	cached, ok := cache.Get(plan.CacheKey())
	if !ok {
		t.Fatalf("expected cached prepared plan")
	}
	cached.Session.Roles[0] = "mutated"
	cached.Session.Variables["autocommit"] = "0"
	cached.Parameters[0].Type = DataTypeString
	cached.Query.Projection[0].Alias = "mutated"

	cachedAgain, ok := cache.Get(plan.CacheKey())
	if !ok {
		t.Fatalf("expected cached prepared plan after mutation")
	}
	if cachedAgain.Session.Roles[0] != "reader" || cachedAgain.Session.Variables["autocommit"] != "1" {
		t.Fatalf("cached session was mutated: %#v", cachedAgain.Session)
	}
	if cachedAgain.Parameters[0].Type != DataTypeInt {
		t.Fatalf("cached parameter type = %q, want int", cachedAgain.Parameters[0].Type)
	}
	if cachedAgain.Query.Projection[0].Alias != "o_orderkey" {
		t.Fatalf("cached query projection alias = %q, want o_orderkey", cachedAgain.Query.Projection[0].Alias)
	}
}

func TestMemoryPreparedPlanCacheDeleteAndClear(t *testing.T) {
	cache := NewMemoryPreparedPlanCache()
	first := PreparedPlan{SQL: "select * from orders", DefaultSchema: "quanta"}
	second := PreparedPlan{SQL: "select * from customer", DefaultSchema: "quanta"}

	cache.Put(first)
	cache.Put(second)
	cache.Delete(first.CacheKey())
	if _, ok := cache.Get(first.CacheKey()); ok {
		t.Fatalf("expected first plan to be deleted")
	}
	if _, ok := cache.Get(second.CacheKey()); !ok {
		t.Fatalf("expected second plan to remain cached")
	}

	cache.Clear()
	if _, ok := cache.Get(second.CacheKey()); ok {
		t.Fatalf("expected clear to remove second plan")
	}
}

func TestMemoryPreparedPlanCacheSeparatesPlanningBoundaries(t *testing.T) {
	cache := NewMemoryPreparedPlanCache()
	first := PreparedPlan{
		SQL:           "select * from orders",
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "reader"},
	}
	second := first
	second.Session.User = "writer"

	cache.Put(first)
	cache.Put(second)
	cachedFirst, ok := cache.Get(first.CacheKey())
	if !ok {
		t.Fatalf("expected first plan")
	}
	cachedSecond, ok := cache.Get(second.CacheKey())
	if !ok {
		t.Fatalf("expected second plan")
	}
	if cachedFirst.Session.User != "reader" || cachedSecond.Session.User != "writer" {
		t.Fatalf("cache crossed session user boundary: first=%#v second=%#v", cachedFirst.Session, cachedSecond.Session)
	}
}

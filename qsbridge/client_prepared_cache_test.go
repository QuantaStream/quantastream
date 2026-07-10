package qsbridge

import "testing"

func TestPlanningServiceListClientPreparedPlanCacheReturnsEntries(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	cache := NewMemoryPreparedPlanCache()
	cache.Put(PreparedPlan{
		Handle:         PreparedStatementHandle{ID: 7, Name: "stmt_orders"},
		SQL:            "select o_orderkey from orders where o_orderkey = ?",
		DefaultSchema:  "quanta",
		CatalogVersion: "v1",
		Session:        SessionContext{User: "moli"},
		Kind:           QueryKindSelect,
		Parameters:     []ParameterRef{{Index: 1, Type: DataTypeInt}},
		ResultColumns:  []ResultColumn{{Name: "o_orderkey", Type: DataTypeInt}},
		Scope:          PhysicalScope{Placement: PlacementLocal, Cache: CacheSession},
		Supported:      true,
	})

	exchange := service.ListClientPreparedPlanCache(connection, cache, "stmt_%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported cache metadata", exchange)
	}
	if exchange.Pattern != "stmt_%" || len(exchange.Entries) != 1 {
		t.Fatalf("exchange = %#v, want one filtered cache entry", exchange)
	}
	entry := exchange.Entries[0]
	if entry.Handle.ID != 7 || entry.Handle.Name != "stmt_orders" || entry.Schema != "quanta" || entry.CatalogVersion != "v1" {
		t.Fatalf("entry = %#v, want prepared plan metadata", entry)
	}
	if entry.User != "moli" || entry.Kind != QueryKindSelect || !entry.Supported || entry.ParameterCount != 1 || entry.ResultColumnCount != 1 {
		t.Fatalf("entry = %#v, want planning metadata counts", entry)
	}
	if entry.AccessIntent != PhysicalAccessRead || entry.Lifecycle != ClientPlanLifecycleSelect || entry.LifecycleSteps != 7 {
		t.Fatalf("entry = %#v, want read/select lifecycle metadata", entry)
	}
	if entry.Scope.Placement != PlacementLocal || entry.Scope.Cache != CacheSession {
		t.Fatalf("entry scope = %#v, want placement/cache metadata", entry.Scope)
	}
	if len(exchange.ResultSchema.Columns) != 16 || exchange.ResultSchema.Columns[0].Name != "Digest" || exchange.ResultSchema.Columns[15].Name != "SQL" {
		t.Fatalf("schema = %#v, want prepared cache result columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 1 || exchange.Result.Chunks[0].Rows[0][1].Value != 7 || exchange.Result.Chunks[0].Rows[0][2].Value != "stmt_orders" {
		t.Fatalf("result rows = %#v, want prepared cache row", exchange.Result.Chunks)
	}
	if exchange.Result.Chunks[0].Rows[0][7].Value != string(PhysicalAccessRead) || exchange.Result.Chunks[0].Rows[0][8].Value != string(ClientPlanLifecycleSelect) {
		t.Fatalf("result rows = %#v, want prepared cache lifecycle cells", exchange.Result.Chunks)
	}
}

func TestPlanningServiceListClientPreparedPlanCacheReportsMissingInspector(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientPreparedPlanCache(connection, nil, "")
	if exchange.Supported() || !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("exchange = %#v, want missing cache inspector diagnostic", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 16 {
		t.Fatalf("result/schema = %#v/%#v, want failed prepared cache envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientPreparedPlanCacheCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	cache := NewMemoryPreparedPlanCache()
	cache.Put(PreparedPlan{
		Handle:        PreparedStatementHandle{ID: 7, Name: "stmt_orders"},
		SQL:           "select o_orderkey from orders",
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
		Kind:          QueryKindSelect,
		Scope:         PhysicalScope{Shards: ShardSet{Shards: []ShardID{"s1"}}},
		Supported:     true,
	})

	exchange := service.ListClientPreparedPlanCache(connection, cache, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Entries[0].Handle.Name = "mutated"
	exchange.Entries[0].Key.Roles = append(exchange.Entries[0].Key.Roles, "mutated")
	exchange.Entries[0].Scope.Shards.Shards[0] = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][2].Value = "mutated"

	again := service.ListClientPreparedPlanCache(connection, cache, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Entries[0].Handle.Name != "stmt_orders" || len(again.Entries[0].Key.Roles) != 0 {
		t.Fatalf("cache entries leaked mutation: %#v", again.Entries)
	}
	if again.Entries[0].Scope.Shards.Shards[0] != "s1" {
		t.Fatalf("scope leaked mutation: %#v", again.Entries[0].Scope)
	}
	if again.Result.Columns[0].Name != "Digest" || again.ResultSchema.Columns[0].Name != "Digest" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][2].Value != "stmt_orders" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

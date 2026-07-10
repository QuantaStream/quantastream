package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientPreparedPlanCacheReturnsFilteredCounts(t *testing.T) {
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
	cache.Put(PreparedPlan{
		Handle:        PreparedStatementHandle{ID: 8},
		SQL:           "select c_custkey from customer",
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
		Kind:          QueryKindSelect,
		Scope:         PhysicalScope{Placement: PlacementFollower},
		Supported:     false,
		Diagnostics:   DiagnosticSet{ErrorDiagnostic(DiagnosticNativeBlocker, PhasePlan, "unsupported")},
	})
	cache.Put(PreparedPlan{
		Handle:        PreparedStatementHandle{ID: 9, Name: "stmt_region"},
		SQL:           "select r_regionkey from region",
		DefaultSchema: "warehouse",
		Session:       SessionContext{User: "ana"},
		Kind:          QueryKindSelect,
		Scope:         PhysicalScope{Placement: PlacementPrimary},
		Supported:     true,
	})

	exchange := service.SummarizeClientPreparedPlanCache(connection, cache, "%select%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported prepared cache summary", exchange)
	}
	row := exchange.Row
	if row.EntryCount != 3 || row.NamedStatementCount != 2 || row.SupportedCount != 2 || row.UnsupportedCount != 1 {
		t.Fatalf("row = %#v, want prepared cache support counts", row)
	}
	if row.ParameterCount != 1 || row.ResultColumnCount != 1 {
		t.Fatalf("row = %#v, want shape counts", row)
	}
	if row.ReadIntentCount != 3 || row.WriteIntentCount != 0 || row.SelectLifecycleCount != 3 || row.MutationLifecycleCount != 0 {
		t.Fatalf("row = %#v, want read/select lifecycle counts", row)
	}
	if row.PrimaryPlacementCount != 1 || row.LocalPlacementCount != 1 || row.FollowerPlacementCount != 1 || row.SessionCacheCount != 1 {
		t.Fatalf("row = %#v, want placement/cache counts", row)
	}
	if row.DistinctSchemaCount != 2 || row.DistinctUserCount != 2 {
		t.Fatalf("row = %#v, want distinct schema/user counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 16 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one prepared cache summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 3 || resultRow[2].Value != 2 || resultRow[6].Value != 3 || resultRow[12].Value != 1 {
		t.Fatalf("result row = %#v, want prepared cache summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPreparedPlanCacheHonorsPattern(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	cache := NewMemoryPreparedPlanCache()
	cache.Put(PreparedPlan{
		Handle:        PreparedStatementHandle{ID: 7, Name: "stmt_orders"},
		SQL:           "select o_orderkey from orders",
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
		Supported:     true,
	})
	cache.Put(PreparedPlan{
		Handle:        PreparedStatementHandle{ID: 8, Name: "stmt_customer"},
		SQL:           "select c_custkey from customer",
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
		Supported:     true,
	})

	exchange := service.SummarizeClientPreparedPlanCache(clientStatementConnection(), cache, "%orders%")
	if exchange.Row.EntryCount != 1 || exchange.Row.NamedStatementCount != 1 {
		t.Fatalf("row = %#v, want pattern-filtered prepared cache summary", exchange.Row)
	}
}

func TestPlanningServiceSummarizeClientPreparedPlanCacheReportsMissingInspector(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.SummarizeClientPreparedPlanCache(clientStatementConnection(), nil, "")

	if exchange.Supported() || !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("exchange = %#v, want missing cache inspector diagnostic", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 16 {
		t.Fatalf("result/schema = %#v/%#v, want failed prepared cache summary envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientPreparedPlanCacheCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	cache := NewMemoryPreparedPlanCache()
	cache.Put(PreparedPlan{
		Handle:        PreparedStatementHandle{ID: 7, Name: "stmt_orders"},
		SQL:           "select o_orderkey from orders",
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
		Supported:     true,
	})

	exchange := service.SummarizeClientPreparedPlanCache(connection, cache, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.EntryCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientPreparedPlanCache(connection, cache, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.EntryCount != 1 || again.Row.NamedStatementCount != 1 {
		t.Fatalf("row leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Entry_count" || again.ResultSchema.Columns[0].Name != "Entry_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

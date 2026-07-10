package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientPreparedStatementsReturnsRegistryCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	registry.Register(PreparedPlan{
		Handle:         PreparedStatementHandle{Name: "stmt_orders"},
		SQL:            "select o_orderkey from orders where o_orderkey = ?",
		DefaultSchema:  "quanta",
		CatalogVersion: "v1",
		Session:        SessionContext{User: "moli"},
		Kind:           QueryKindSelect,
		Supported:      true,
		Parameters:     []ParameterRef{{Index: 1, Type: DataTypeInt}},
		ResultColumns:  []ResultColumn{{Name: "o_orderkey", Type: DataTypeInt}},
		Scope:          PhysicalScope{Placement: PlacementPrimary, Cache: CacheSession},
	})
	registry.Register(PreparedPlan{
		SQL:            "select unsupported",
		DefaultSchema:  "quanta",
		CatalogVersion: "v1",
		Session:        SessionContext{User: "moli"},
		Kind:           QueryKindSelect,
		Supported:      false,
		Diagnostics:    DiagnosticSet{ErrorDiagnostic(DiagnosticNativeBlocker, PhasePlan, "unsupported")},
		Scope:          PhysicalScope{Placement: PlacementFollower},
	})

	exchange := service.SummarizeClientPreparedStatements(connection, registry)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported prepared statement summary", exchange)
	}
	row := exchange.Row
	if row.StatementCount != 2 || row.NamedStatementCount != 1 || row.SupportedCount != 1 || row.UnsupportedCount != 1 {
		t.Fatalf("row = %#v, want prepared statement support counts", row)
	}
	if row.ParameterCount != 1 || row.ResultColumnCount != 1 || row.DiagnosticCount != 1 {
		t.Fatalf("row = %#v, want shape and diagnostic counts", row)
	}
	if row.ReadIntentCount != 2 || row.WriteIntentCount != 0 || row.SelectLifecycleCount != 2 || row.MutationLifecycleCount != 0 {
		t.Fatalf("row = %#v, want read/select lifecycle counts", row)
	}
	if row.PrimaryPlacementCount != 1 || row.FollowerPlacementCount != 1 || row.SessionCacheCount != 1 {
		t.Fatalf("row = %#v, want placement and cache counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 15 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 2 || resultRow[2].Value != 1 || resultRow[6].Value != 2 || resultRow[10].Value != 1 {
		t.Fatalf("result row = %#v, want prepared status summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientPreparedStatementsReportsMissingRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.SummarizeClientPreparedStatements(clientStatementConnection(ClientCapabilityPreparedStatements), nil)

	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing registry to block summary", exchange)
	}
	if !containsDiagnosticCode(exchange.ExchangeDiagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", exchange.ExchangeDiagnostics.Codes())
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed complete result", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientPreparedStatementsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	registry := NewMemoryPreparedStatementRegistry()
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	registry.Register(PreparedPlan{
		Handle:    PreparedStatementHandle{Name: "stmt_orders"},
		SQL:       "select o_orderkey from orders where o_orderkey = ?",
		Supported: true,
		Parameters: []ParameterRef{
			{Index: 1, Type: DataTypeInt},
		},
	})

	exchange := service.SummarizeClientPreparedStatements(connection, registry)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.StatementCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientPreparedStatements(connection, registry)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.StatementCount != 1 || again.Row.ParameterCount != 1 {
		t.Fatalf("row leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Statement_count" || again.ResultSchema.Columns[0].Name != "Statement_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

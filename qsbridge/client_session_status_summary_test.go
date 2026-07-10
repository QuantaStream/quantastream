package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientSessionsReturnsRegistryCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	registry := NewMemorySessionRegistry()
	registry.Put(SessionContext{
		ID:            "session-a",
		User:          "alice",
		CurrentSchema: "quanta",
		TimeZone:      "America/Costa_Rica",
		Roles:         []RoleName{"admin", "writer"},
		SQLModes:      []SQLMode{"STRICT"},
		Variables:     map[string]string{"sql_mode": "STRICT", "autocommit": "1"},
	})
	registry.Put(SessionContext{
		ID:            "session-b",
		User:          "bob",
		CurrentSchema: "analytics",
		TimeZone:      "UTC",
		Roles:         []RoleName{"reader"},
		SQLModes:      []SQLMode{"ANSI", "NO_ZERO_DATE"},
		Variables:     map[string]string{"autocommit": "1"},
	})
	registry.Put(SessionContext{
		ID:   "session-c",
		User: "alice",
	})

	exchange := service.SummarizeClientSessions(connection, registry)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported session summary", exchange)
	}
	row := exchange.Row
	if row.SessionCount != 3 || row.SchemaSelectedCount != 2 || row.TimeZoneCount != 2 {
		t.Fatalf("row = %#v, want session/schema/time-zone counts", row)
	}
	if row.RoleCount != 3 || row.SQLModeCount != 3 || row.VariableCount != 3 {
		t.Fatalf("row = %#v, want role/sql-mode/variable totals", row)
	}
	if row.SessionsWithRoles != 2 || row.SessionsWithSQLModes != 2 || row.SessionsWithVariables != 2 {
		t.Fatalf("row = %#v, want per-session feature counts", row)
	}
	if row.DistinctUserCount != 2 || row.DistinctSchemaCount != 2 {
		t.Fatalf("row = %#v, want distinct user/schema counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 11 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one session summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 3 || resultRow[3].Value != 3 || resultRow[9].Value != 2 {
		t.Fatalf("result row = %#v, want session summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientSessionsReportsMissingRegistry(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	exchange := service.SummarizeClientSessions(clientStatementConnection(), nil)

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

func TestPlanningServiceSummarizeClientSessionsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	registry := NewMemorySessionRegistry()
	registry.Put(SessionContext{
		ID:            "session-a",
		User:          "alice",
		CurrentSchema: "quanta",
		Roles:         []RoleName{"admin"},
		Variables:     map[string]string{"autocommit": "1"},
	})

	exchange := service.SummarizeClientSessions(connection, registry)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.SessionCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientSessions(connection, registry)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.SessionCount != 1 || again.Row.SchemaSelectedCount != 1 || again.Row.RoleCount != 1 {
		t.Fatalf("row leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Session_count" || again.ResultSchema.Columns[0].Name != "Session_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

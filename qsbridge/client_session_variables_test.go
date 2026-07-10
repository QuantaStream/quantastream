package qsbridge

import "testing"

func TestPlanningServiceListClientSessionVariablesReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = "quanta"
	connection.Session.TimeZone = "UTC"
	connection.Session.SQLModes = []SQLMode{"ANSI_QUOTES", "STRICT_TRANS_TABLES"}
	connection.Session.Variables = map[string]string{
		"autocommit":    "1",
		"max_statement": "1000",
	}

	exchange := service.ListClientSessionVariables(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported session variable metadata", exchange)
	}
	if len(exchange.Variables) != 5 || exchange.Variables[0].Name != "autocommit" || exchange.Variables[1].Name != "database" {
		t.Fatalf("variables = %#v, want sorted built-in and session variables", exchange.Variables)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 5 || len(exchange.ResultSchema.Columns) != 2 {
		t.Fatalf("result/schema = %#v/%#v, want variable result metadata", exchange.Result, exchange.ResultSchema)
	}
	if exchange.Result.Chunks[0].Rows[1][0].Value != "database" || exchange.Result.Chunks[0].Rows[1][1].Value != "quanta" {
		t.Fatalf("database row = %#v, want selected schema", exchange.Result.Chunks[0].Rows[1])
	}
	if exchange.Result.Chunks[0].Rows[3][0].Value != "sql_mode" || exchange.Result.Chunks[0].Rows[3][1].Value != "ANSI_QUOTES,STRICT_TRANS_TABLES" {
		t.Fatalf("sql_mode row = %#v, want joined sorted SQL modes", exchange.Result.Chunks[0].Rows[3])
	}
}

func TestPlanningServiceListClientSessionVariablesFiltersByWildcard(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.Variables = map[string]string{
		"autocommit":      "1",
		"auto_increment":  "1",
		"character_set":   "utf8mb4",
		"completion_type": "NO_CHAIN",
	}

	exchange := service.ListClientSessionVariables(connection, "auto%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported filtered variables", exchange)
	}
	if len(exchange.Variables) != 2 || exchange.Variables[0].Name != "auto_increment" || exchange.Variables[1].Name != "autocommit" {
		t.Fatalf("variables = %#v, want auto-prefixed variables", exchange.Variables)
	}
	if exchange.Result.RowsReturned != 2 {
		t.Fatalf("rows returned = %d, want two filtered rows", exchange.Result.RowsReturned)
	}
}

func TestPlanningServiceListClientSessionVariablesReturnsFailedEnvelopeForUnsupportedConnection(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := ConnectionContext{
		Diagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "denied")},
	}

	exchange := service.ListClientSessionVariables(connection, "")
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 2 {
		t.Fatalf("result/schema = %#v/%#v, want failed variable envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientSessionVariablesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Session.Variables = map[string]string{"autocommit": "1"}

	exchange := service.ListClientSessionVariables(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Variables[0].Value = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.ListClientSessionVariables(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Variables[0].Value != "1" || again.Result.Chunks[0].Rows[0][1].Value != "1" {
		t.Fatalf("variable metadata leaked mutation: %#v/%#v", again.Variables, again.Result.Chunks)
	}
}

package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientSessionVariablesReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = "quanta"
	connection.Session.TimeZone = "UTC"
	connection.Session.SQLModes = []SQLMode{"ANSI_QUOTES", "STRICT_TRANS_TABLES"}
	connection.Session.Variables = map[string]string{
		"autocommit":    "1",
		"max_statement": "1000",
	}

	exchange := service.SummarizeClientSessionVariables(connection, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported session variable summary", exchange)
	}
	row := exchange.Row
	if row.VariableCount != 5 || row.BuiltInCount != 3 || row.AdapterVariableCount != 2 {
		t.Fatalf("row = %#v, want built-in and adapter variable counts", row)
	}
	if row.EmptyValueCount != 0 || row.NumericValueCount != 2 || row.SelectedSchemaCount != 1 || row.SQLModeCount != 1 || row.TimeZoneCount != 1 {
		t.Fatalf("row = %#v, want value and built-in presence counts", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 8 {
		t.Fatalf("result/schema = %#v/%#v, want session variable summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 5 || resultRow[1].Value != 3 || resultRow[4].Value != 2 {
		t.Fatalf("result row = %#v, want session variable summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientSessionVariablesFiltersByWildcard(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.Variables = map[string]string{
		"autocommit":      "1",
		"auto_increment":  "1",
		"character_set":   "utf8mb4",
		"completion_type": "NO_CHAIN",
	}

	exchange := service.SummarizeClientSessionVariables(connection, "auto%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported filtered variable summary", exchange)
	}
	if exchange.Row.VariableCount != 2 || exchange.Row.AdapterVariableCount != 2 || exchange.Row.BuiltInCount != 0 {
		t.Fatalf("row = %#v, want auto-prefixed variable summary", exchange.Row)
	}
}

func TestPlanningServiceSummarizeClientSessionVariablesReturnsFailedEnvelopeForUnsupportedConnection(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := ConnectionContext{
		Diagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "denied")},
	}

	exchange := service.SummarizeClientSessionVariables(connection, "")
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 8 {
		t.Fatalf("result/schema = %#v/%#v, want failed variable summary envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientSessionVariablesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Session.Variables = map[string]string{"autocommit": "1"}

	exchange := service.SummarizeClientSessionVariables(connection, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.VariableCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientSessionVariables(connection, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.VariableCount != 4 || again.Row.AdapterVariableCount != 1 || again.Result.Chunks[0].Rows[0][0].Value != 4 {
		t.Fatalf("session variable summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}

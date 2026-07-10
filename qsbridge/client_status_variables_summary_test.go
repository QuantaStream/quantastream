package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientStatusVariablesReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	variables := []ClientStatusVariable{
		{Name: "Threads_connected", Value: "2"},
		{Name: "Connections", Value: "10"},
		{Name: "Com_select", Value: "42"},
		{Name: "Com_insert", Value: "3"},
		{Name: "Uptime_since_flush_status", Value: ""},
	}

	exchange := service.SummarizeClientStatusVariables(connection, variables, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported status variable summary", exchange)
	}
	row := exchange.Row
	if row.VariableCount != 5 || row.EmptyValueCount != 1 || row.NumericValueCount != 4 {
		t.Fatalf("row = %#v, want variable and value counts", row)
	}
	if row.CommandStatusCount != 2 || row.ThreadStatusCount != 1 || row.ConnectionStatusCount != 1 {
		t.Fatalf("row = %#v, want status prefix counts", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 6 {
		t.Fatalf("result/schema = %#v/%#v, want status summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 5 || resultRow[2].Value != 4 || resultRow[3].Value != 2 {
		t.Fatalf("result row = %#v, want status summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientStatusVariablesFiltersByWildcard(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	variables := []ClientStatusVariable{
		{Name: "Com_select", Value: "12"},
		{Name: "Com_insert", Value: "3"},
		{Name: "Connections", Value: "10"},
	}

	exchange := service.SummarizeClientStatusVariables(connection, variables, "Com_%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported filtered status variable summary", exchange)
	}
	if exchange.Row.VariableCount != 2 || exchange.Row.CommandStatusCount != 2 || exchange.Row.ConnectionStatusCount != 0 {
		t.Fatalf("row = %#v, want Com-prefixed status summary", exchange.Row)
	}
}

func TestPlanningServiceSummarizeClientStatusVariablesReturnsFailedEnvelopeForUnsupportedConnection(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := ConnectionContext{
		Diagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "denied")},
	}

	exchange := service.SummarizeClientStatusVariables(connection, nil, "")
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 6 {
		t.Fatalf("result/schema = %#v/%#v, want failed status variable summary envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientStatusVariablesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	variables := []ClientStatusVariable{{Name: "Connections", Value: "10"}}

	exchange := service.SummarizeClientStatusVariables(connection, variables, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.VariableCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientStatusVariables(connection, variables, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.VariableCount != 1 || again.Row.NumericValueCount != 1 || again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("status summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}

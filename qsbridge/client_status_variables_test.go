package qsbridge

import "testing"

func TestPlanningServiceListClientStatusVariablesReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	variables := []ClientStatusVariable{
		{Name: "Threads_connected", Value: "2"},
		{Name: "Connections", Value: "10"},
		{Name: "Questions", Value: "42"},
	}

	exchange := service.ListClientStatusVariables(connection, variables, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported status variable metadata", exchange)
	}
	if len(exchange.Variables) != 3 || exchange.Variables[0].Name != "Connections" || exchange.Variables[2].Name != "Threads_connected" {
		t.Fatalf("variables = %#v, want sorted status variables", exchange.Variables)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 3 || len(exchange.ResultSchema.Columns) != 2 {
		t.Fatalf("result/schema = %#v/%#v, want status variable result metadata", exchange.Result, exchange.ResultSchema)
	}
	if exchange.Result.Chunks[0].Rows[1][0].Value != "Questions" || exchange.Result.Chunks[0].Rows[1][1].Value != "42" {
		t.Fatalf("question row = %#v, want status value", exchange.Result.Chunks[0].Rows[1])
	}
}

func TestPlanningServiceListClientStatusVariablesFiltersByWildcard(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	variables := []ClientStatusVariable{
		{Name: "Com_select", Value: "12"},
		{Name: "Com_insert", Value: "3"},
		{Name: "Connections", Value: "10"},
	}

	exchange := service.ListClientStatusVariables(connection, variables, "Com_%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported filtered status variables", exchange)
	}
	if len(exchange.Variables) != 2 || exchange.Variables[0].Name != "Com_insert" || exchange.Variables[1].Name != "Com_select" {
		t.Fatalf("variables = %#v, want Com-prefixed status variables", exchange.Variables)
	}
	if exchange.Result.RowsReturned != 2 {
		t.Fatalf("rows returned = %d, want two filtered rows", exchange.Result.RowsReturned)
	}
}

func TestPlanningServiceListClientStatusVariablesReturnsFailedEnvelopeForUnsupportedConnection(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := ConnectionContext{
		Diagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "denied")},
	}

	exchange := service.ListClientStatusVariables(connection, nil, "")
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 2 {
		t.Fatalf("result/schema = %#v/%#v, want failed status variable envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientStatusVariablesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	variables := []ClientStatusVariable{{Name: "Connections", Value: "10"}}

	exchange := service.ListClientStatusVariables(connection, variables, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Variables[0].Value = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.ListClientStatusVariables(connection, variables, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Variables[0].Value != "10" || again.Result.Chunks[0].Rows[0][1].Value != "10" {
		t.Fatalf("status metadata leaked mutation: %#v/%#v", again.Variables, again.Result.Chunks)
	}
}

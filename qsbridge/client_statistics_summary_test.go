package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientStatisticsReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	variables := []ClientStatusVariable{
		{Name: "Threads_connected", Value: "2"},
		{Name: "Connections", Value: "10"},
		{Name: "Questions", Value: "42"},
		{Name: "Com_select", Value: "7"},
		{Name: "Com_insert", Value: ""},
	}

	exchange := service.SummarizeClientStatistics(connection, variables)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported statistics summary", exchange)
	}
	row := exchange.Row
	if row.VariableCount != 5 || row.EmptyValueCount != 1 || row.NumericValueCount != 4 {
		t.Fatalf("row = %#v, want value counts", row)
	}
	if row.CommandVariableCount != 2 || row.ThreadVariableCount != 1 || row.ConnectionCount != 1 {
		t.Fatalf("row = %#v, want status-family counts", row)
	}
	if row.SummaryLength != len(exchange.Summary) || exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 7 {
		t.Fatalf("row/result/schema = %#v/%#v/%#v, want statistics summary result", row, exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 5 || resultRow[2].Value != 1 || resultRow[4].Value != 2 {
		t.Fatalf("result row = %#v, want statistics summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientStatisticsReturnsUnsupportedConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := ConnectionContext{
		Diagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "denied")},
	}

	exchange := service.SummarizeClientStatistics(connection, []ClientStatusVariable{{Name: "Connections", Value: "10"}})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection", exchange)
	}
	if exchange.Row.VariableCount != 0 || exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 7 {
		t.Fatalf("row/result/schema = %#v/%#v/%#v, want failed statistics summary envelope", exchange.Row, exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientStatisticsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	variables := []ClientStatusVariable{{Name: "Connections", Value: "10"}}

	exchange := service.SummarizeClientStatistics(connection, variables)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Variables[0].Value = "mutated"
	exchange.Row.VariableCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientStatistics(connection, variables)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Variables[0].Value != "10" || again.Summary != "Connections: 10" {
		t.Fatalf("statistics metadata leaked mutation: %#v/%q", again.Variables, again.Summary)
	}
	if again.Row.VariableCount != 1 || again.Row.ConnectionCount != 1 || again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("statistics summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}

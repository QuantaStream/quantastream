package qsbridge

import "testing"

func TestPlanningServiceListClientStatementWarningsReturnsNoticeRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	response := StatementResult{
		Notices: []StatementNotice{
			{Level: StatementNoticeWarning, Code: "1265", SQLState: "01000", Message: "Data truncated"},
			{Level: StatementNoticeNote, Code: "1003", SQLState: "01000", Message: "Query rewritten"},
			{Level: StatementNoticeError, Code: "1105", SQLState: "HY000", Message: "Unknown error"},
		},
	}.ProtocolStatementResponse(NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults))

	exchange := service.ListClientStatementWarnings(connection, response)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported warning metadata", exchange)
	}
	if exchange.WarningCount != 3 || len(exchange.Notices) != 3 {
		t.Fatalf("count/notices = %d/%#v, want three warning details", exchange.WarningCount, exchange.Notices)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 3 || len(exchange.ResultSchema.Columns) != 4 {
		t.Fatalf("result/schema = %#v/%#v, want warning rows and schema", exchange.Result, exchange.ResultSchema)
	}
	rows := exchange.Result.Chunks[0].Rows
	if rows[0][0].Value != "warning" || rows[0][1].Value != "1265" || rows[0][3].Value != "Data truncated" {
		t.Fatalf("first row = %#v, want warning detail", rows[0])
	}
	if rows[1][0].Value != "note" || rows[2][0].Value != "error" {
		t.Fatalf("levels = %#v/%#v, want note and error rows", rows[1], rows[2])
	}
}

func TestPlanningServiceListClientStatementWarningsSupportsEmptyWarnings(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientStatementWarnings(connection, ProtocolStatementResponse{})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported empty warning metadata", exchange)
	}
	if exchange.WarningCount != 0 || exchange.Result.RowsReturned != 0 || len(exchange.Result.Chunks) != 1 {
		t.Fatalf("exchange/result = %#v/%#v, want empty warning result", exchange, exchange.Result)
	}
}

func TestPlanningServiceListClientStatementWarningsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	response := StatementResult{
		Notices: []StatementNotice{{Message: "original"}},
	}.ProtocolStatementResponse(NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults))

	exchange := service.ListClientStatementWarnings(connection, response)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Notices[0].Message = "mutated"
	exchange.Result.Chunks[0].Rows[0][3].Value = "mutated"

	again := service.ListClientStatementWarnings(connection, response)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Notices[0].Message != "original" || again.Result.Chunks[0].Rows[0][3].Value != "original" {
		t.Fatalf("warning metadata leaked mutation: %#v/%#v", again.Notices, again.Result.Chunks)
	}
}

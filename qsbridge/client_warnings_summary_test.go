package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientStatementWarningsReturnsNoticeCounts(t *testing.T) {
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

	exchange := service.SummarizeClientStatementWarnings(connection, response)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported warning summary", exchange)
	}
	row := exchange.Row
	if row.WarningCount != 3 || row.NoticeCount != 3 || row.WarningRows != 1 || row.NoteRows != 1 || row.ErrorRows != 1 {
		t.Fatalf("row = %#v, want warning/note/error counts", row)
	}
	if row.CodedRows != 3 || row.SQLStateRows != 3 {
		t.Fatalf("row = %#v, want code and SQLState counts", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 7 {
		t.Fatalf("result/schema = %#v/%#v, want warning summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 3 || resultRow[2].Value != 1 || resultRow[4].Value != 1 {
		t.Fatalf("result row = %#v, want warning summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientStatementWarningsSupportsEmptyWarnings(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.SummarizeClientStatementWarnings(connection, ProtocolStatementResponse{})
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported empty warning summary", exchange)
	}
	if exchange.Row.WarningCount != 0 || exchange.Row.NoticeCount != 0 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("exchange/result = %#v/%#v, want empty warning summary row", exchange, exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientStatementWarningsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	response := StatementResult{
		Notices: []StatementNotice{{Message: "original"}},
	}.ProtocolStatementResponse(NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults))

	exchange := service.SummarizeClientStatementWarnings(connection, response)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Response.Notices[0].Message = "mutated"
	exchange.Row.WarningCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientStatementWarnings(connection, response)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Response.Notices[0].Message != "original" || again.Row.WarningCount != 1 {
		t.Fatalf("warning summary leaked mutation: response=%#v row=%#v", again.Response, again.Row)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

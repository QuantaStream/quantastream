package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientSQLModesReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.SQLModes = []SQLMode{"ANSI_QUOTES", "NO_ZERO_DATE"}
	modes := []ClientSQLMode{
		{Name: "STRICT_TRANS_TABLES", Description: "strict transactional mode", Supported: true, Default: true},
		{Name: "ANSI_QUOTES", Description: "double quotes identify names", Supported: true},
		{Name: "NO_ZERO_DATE", Description: "legacy zero-date behavior", Supported: false, Default: true},
	}

	exchange := service.SummarizeClientSQLModes(connection, modes, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported sql mode summary", exchange)
	}
	row := exchange.Row
	if row.ModeCount != 3 || row.SupportedCount != 2 || row.UnsupportedCount != 1 {
		t.Fatalf("row = %#v, want support counts", row)
	}
	if row.DefaultCount != 2 || row.EnabledCount != 2 || row.DefaultEnabledCount != 1 || row.SupportedEnabledCount != 1 {
		t.Fatalf("row = %#v, want default and enabled counts", row)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 7 {
		t.Fatalf("result/schema = %#v/%#v, want sql mode summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 3 || resultRow[1].Value != 2 || resultRow[4].Value != 2 || resultRow[6].Value != 1 {
		t.Fatalf("result row = %#v, want sql mode summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientSQLModesFiltersPattern(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.SQLModes = []SQLMode{"STRICT_TRANS_TABLES"}
	modes := []ClientSQLMode{
		{Name: "STRICT_TRANS_TABLES", Description: "strict transactional mode", Supported: true, Default: true},
		{Name: "ANSI_QUOTES", Description: "double quotes identify names", Supported: true},
	}

	exchange := service.SummarizeClientSQLModes(connection, modes, "%strict%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported filtered sql mode summary", exchange)
	}
	if exchange.Row.ModeCount != 1 || exchange.Row.SupportedCount != 1 || exchange.Row.EnabledCount != 1 || exchange.Row.DefaultEnabledCount != 1 {
		t.Fatalf("row = %#v, want strict-only summary", exchange.Row)
	}
}

func TestPlanningServiceSummarizeClientSQLModesReturnsFailedEnvelopeForUnsupportedConnection(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := ConnectionContext{
		Diagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticAccessDenied, PhaseBind, "denied")},
	}

	exchange := service.SummarizeClientSQLModes(connection, []ClientSQLMode{{Name: "ANSI_QUOTES", Supported: true}}, "")
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported connection", exchange)
	}
	if exchange.Row.ModeCount != 0 || exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 7 {
		t.Fatalf("row/result/schema = %#v/%#v/%#v, want failed sql mode summary envelope", exchange.Row, exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientSQLModesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	modes := []ClientSQLMode{{Name: "ANSI_QUOTES", Supported: true}}

	exchange := service.SummarizeClientSQLModes(connection, modes, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.ModeCount = 99
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientSQLModes(connection, modes, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.ModeCount != 1 || again.Row.SupportedCount != 1 || again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("sql mode summary leaked mutation: row=%#v result=%#v", again.Row, again.Result.Chunks)
	}
}

package qsbridge

import "testing"

func TestPlanningServiceListClientSQLModesReturnsSupportedAndEnabledModes(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.SQLModes = []SQLMode{"ANSI_QUOTES"}
	modes := []ClientSQLMode{
		{Name: "STRICT_TRANS_TABLES", Description: "strict transactional mode", Supported: true, Default: true},
		{Name: "ANSI_QUOTES", Description: "double quotes identify names", Supported: true},
	}

	exchange := service.ListClientSQLModes(connection, modes, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported sql mode metadata", exchange)
	}
	if len(exchange.Modes) != 2 {
		t.Fatalf("modes = %#v, want two modes", exchange.Modes)
	}
	if exchange.Modes[0].Name != "ANSI_QUOTES" || !exchange.Modes[0].Enabled {
		t.Fatalf("first mode = %#v, want enabled ANSI_QUOTES sorted first", exchange.Modes[0])
	}
	if exchange.Modes[1].Name != "STRICT_TRANS_TABLES" || !exchange.Modes[1].Default || exchange.Modes[1].Enabled {
		t.Fatalf("second mode = %#v, want default strict mode disabled in session", exchange.Modes[1])
	}
	if len(exchange.ResultSchema.Columns) != 5 || exchange.Result.RowsReturned != 2 {
		t.Fatalf("result/schema = %#v/%#v, want sql mode rows", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != "ANSI_QUOTES" || resultRow[2].Value != true || resultRow[4].Value != true {
		t.Fatalf("result row = %#v, want enabled ANSI_QUOTES row", resultRow)
	}
}

func TestPlanningServiceListClientSQLModesFiltersPattern(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	modes := []ClientSQLMode{
		{Name: "STRICT_TRANS_TABLES", Description: "strict transactional mode", Supported: true},
		{Name: "ANSI_QUOTES", Description: "double quotes identify names", Supported: true},
	}

	exchange := service.ListClientSQLModes(connection, modes, "%strict%")
	if !exchange.Supported() || len(exchange.Modes) != 1 {
		t.Fatalf("exchange = %#v, want one filtered mode", exchange)
	}
	if exchange.Modes[0].Name != "STRICT_TRANS_TABLES" {
		t.Fatalf("mode = %#v, want strict mode", exchange.Modes[0])
	}
}

func TestPlanningServiceListClientSQLModesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	modes := []ClientSQLMode{{Name: "ANSI_QUOTES", Supported: true}}

	exchange := service.ListClientSQLModes(connection, modes, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Modes[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientSQLModes(connection, modes, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Modes[0].Name != "ANSI_QUOTES" {
		t.Fatalf("modes leaked mutation: %#v", again.Modes[0])
	}
	if again.Result.Columns[0].Name != "Mode" || again.ResultSchema.Columns[0].Name != "Mode" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != "ANSI_QUOTES" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

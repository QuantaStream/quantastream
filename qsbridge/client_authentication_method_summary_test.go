package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientAuthenticationMethodsReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.SummarizeClientAuthenticationMethods(connection, DefaultClientAuthenticationMethods(), "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported authentication method summary", exchange)
	}
	row := exchange.Row
	if row.MethodCount != 5 || row.DefaultCount != 1 || row.EnabledCount != 2 || row.DisabledCount != 3 {
		t.Fatalf("row = %#v, want default inventory counts", row)
	}
	if row.PasswordExchangeCount != 3 || row.TokenExchangeCount != 2 || row.ExternalIdentityCount != 2 {
		t.Fatalf("row = %#v, want exchange family counts", row)
	}
	if row.MySQLPasswordCount != 3 || row.JWTCount != 1 || row.OAuthCount != 1 {
		t.Fatalf("row = %#v, want method family counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 10 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 5 || resultRow[2].Value != 2 || resultRow[9].Value != 1 {
		t.Fatalf("result row = %#v, want authentication summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientAuthenticationMethodsFiltersPattern(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.SummarizeClientAuthenticationMethods(connection, DefaultClientAuthenticationMethods(), "%jwt%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported filtered authentication summary", exchange)
	}
	if exchange.Row.MethodCount != 1 || exchange.Row.JWTCount != 1 || exchange.Row.TokenExchangeCount != 1 {
		t.Fatalf("row = %#v, want one JWT token method", exchange.Row)
	}
	if exchange.Row.EnabledCount != 0 || exchange.Row.DisabledCount != 1 {
		t.Fatalf("row = %#v, want disabled enterprise hook", exchange.Row)
	}
}

func TestPlanningServiceSummarizeClientAuthenticationMethodsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Diagnostics = DiagnosticSet{ErrorDiagnostic(DiagnosticNativeBlocker, PhaseBind, "blocked")}

	exchange := service.SummarizeClientAuthenticationMethods(connection, DefaultClientAuthenticationMethods(), "")
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want blocked connection", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || exchange.Result.RowsReturned != 0 {
		t.Fatalf("result = %#v, want failed rowless authentication summary", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientAuthenticationMethodsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}

	exchange := service.SummarizeClientAuthenticationMethods(connection, DefaultClientAuthenticationMethods(), "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.MethodCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientAuthenticationMethods(connection, DefaultClientAuthenticationMethods(), "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.MethodCount != 5 {
		t.Fatalf("summary leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Method_count" || again.ResultSchema.Columns[0].Name != "Method_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 5 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientAuthenticationBuildsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := AuthenticationRequest{
		SessionID:     "session-1",
		User:          "alice",
		DefaultSchema: "quanta",
		Method:        AuthenticationMethodMySQLPassword,
		ClientAddress: "127.0.0.1:4000",
		Attributes:    map[string]string{"driver": "mysql"},
	}
	decision := request.Allow(AuthenticationPrincipal{
		Roles:      []RoleName{"reader", "writer"},
		Attributes: map[string]string{"tenant": "demo"},
	})

	exchange := service.SummarizeClientAuthentication(NewProtocolProfile(ProtocolMySQL, "mysql"), request, decision)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported authentication summary", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one authentication row", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.SessionID != "session-1" || row.Method != AuthenticationMethodMySQLPassword || row.PrincipalUser != "alice" || len(row.PrincipalRoles) != 2 || row.AttributeCount != 1 {
		t.Fatalf("row = %#v, want authenticated principal metadata", row)
	}
	if len(exchange.ResultSchema.Columns) != 10 || exchange.ResultSchema.Columns[0].Name != "Session_id" {
		t.Fatalf("schema = %#v, want authentication summary columns", exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[1].Value != string(AuthenticationMethodMySQLPassword) || resultRow[5].Value != true || resultRow[7].Value != "reader,writer" {
		t.Fatalf("result row = %#v, want authentication metadata", resultRow)
	}
}

func TestPlanningServiceSummarizeClientAuthenticationReturnsFailedEnvelopeForDeny(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := AuthenticationRequest{SessionID: "session-1", User: "alice", Method: AuthenticationMethodJWT}
	decision := request.Deny("token expired")

	exchange := service.SummarizeClientAuthentication(NewProtocolProfile(ProtocolMySQL, "mysql"), request, decision)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want denied authentication", exchange)
	}
	if !containsDiagnosticCode(exchange.Decision.Diagnostics.Codes(), DiagnosticAccessDenied) {
		t.Fatalf("diagnostics = %#v, want access denied", exchange.Decision.Diagnostics)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("result/schema = %#v/%#v, want failed authentication envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientAuthenticationCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	request := AuthenticationRequest{
		SessionID:  "session-1",
		User:       "alice",
		Method:     AuthenticationMethodOAuth,
		Attributes: map[string]string{"driver": "mysql"},
	}
	decision := request.Allow(AuthenticationPrincipal{
		Roles:      []RoleName{"reader"},
		Attributes: map[string]string{"tenant": "demo"},
	})

	exchange := service.SummarizeClientAuthentication(NewProtocolProfile(ProtocolMySQL, "mysql"), request, decision)
	exchange.Request.Attributes["driver"] = "mutated"
	exchange.Decision.Principal.Roles[0] = "mutated"
	exchange.Rows[0].PrincipalRoles[0] = "mutated"
	exchange.Result.Chunks[0].Rows[0][7].Value = "mutated"

	again := service.SummarizeClientAuthentication(NewProtocolProfile(ProtocolMySQL, "mysql"), request, decision)
	if again.Request.Attributes["driver"] != "mysql" {
		t.Fatalf("request leaked mutation: %#v", again.Request.Attributes)
	}
	if again.Decision.Principal.Roles[0] != "reader" || again.Rows[0].PrincipalRoles[0] != "reader" {
		t.Fatalf("principal roles leaked mutation: %#v/%#v", again.Decision.Principal.Roles, again.Rows[0].PrincipalRoles)
	}
	if again.Result.Chunks[0].Rows[0][7].Value != "reader" {
		t.Fatalf("result row leaked mutation: %#v", again.Result.Chunks[0].Rows[0])
	}
}

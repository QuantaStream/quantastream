package qsbridge

import "testing"

func TestPlanningServiceListClientAuthenticationMethodsReturnsSortedMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	methods := []ClientAuthenticationMethod{
		{
			Method:           AuthenticationMethodOAuth,
			Plugin:           "openid_connect",
			Description:      "enterprise identity provider",
			Enabled:          true,
			TokenExchange:    true,
			ExternalIdentity: true,
		},
		{
			Method:           AuthenticationMethodMySQLPassword,
			Plugin:           "mysql_native_password",
			Description:      "mysql-compatible password flow",
			Default:          true,
			Enabled:          true,
			PasswordExchange: true,
		},
	}

	exchange := service.ListClientAuthenticationMethods(connection, methods, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported authentication method metadata", exchange)
	}
	if len(exchange.Methods) != 2 {
		t.Fatalf("methods = %#v, want two methods", exchange.Methods)
	}
	if exchange.Methods[0].Method != AuthenticationMethodMySQLPassword || !exchange.Methods[0].PasswordExchange {
		t.Fatalf("first method = %#v, want mysql password sorted first", exchange.Methods[0])
	}
	if exchange.Methods[1].Method != AuthenticationMethodOAuth || !exchange.Methods[1].ExternalIdentity {
		t.Fatalf("second method = %#v, want oauth method", exchange.Methods[1])
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.Result.RowsReturned != 2 {
		t.Fatalf("result/schema = %#v/%#v, want auth method rows", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != string(AuthenticationMethodMySQLPassword) || resultRow[3].Value != true || resultRow[5].Value != true {
		t.Fatalf("result row = %#v, want default mysql password method", resultRow)
	}
}

func TestDefaultClientAuthenticationMethodsExposeMySQLAndEnterpriseHooks(t *testing.T) {
	methods := DefaultClientAuthenticationMethods()
	if len(methods) != 5 {
		t.Fatalf("methods = %#v, want built-in mysql plus enterprise hooks", methods)
	}
	if methods[0].Method != AuthenticationMethodMySQLPassword || methods[0].Plugin != string(AuthenticationPluginCachingSHA2Password) || !methods[0].Default || !methods[0].Enabled {
		t.Fatalf("first method = %#v, want default caching_sha2 password", methods[0])
	}
	if methods[3].Method != AuthenticationMethodJWT || !methods[3].ExternalIdentity || methods[3].Enabled {
		t.Fatalf("jwt method = %#v, want disabled enterprise hook", methods[3])
	}
	methods[0].Plugin = "mutated"
	if DefaultClientAuthenticationMethods()[0].Plugin != string(AuthenticationPluginCachingSHA2Password) {
		t.Fatalf("default auth methods leaked mutation")
	}
}

func TestPlanningServiceListClientAuthenticationMethodsFiltersPattern(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	methods := []ClientAuthenticationMethod{
		{Method: AuthenticationMethodMySQLPassword, Plugin: "mysql_native_password"},
		{Method: AuthenticationMethodJWT, Plugin: "bearer_jwt"},
	}

	exchange := service.ListClientAuthenticationMethods(connection, methods, "%jwt%")
	if !exchange.Supported() || len(exchange.Methods) != 1 {
		t.Fatalf("exchange = %#v, want one filtered method", exchange)
	}
	if exchange.Methods[0].Method != AuthenticationMethodJWT {
		t.Fatalf("method = %#v, want jwt", exchange.Methods[0])
	}
}

func TestPlanningServiceListClientAuthenticationMethodsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	methods := []ClientAuthenticationMethod{{Method: AuthenticationMethodMySQLPassword, Plugin: "mysql_native_password"}}

	exchange := service.ListClientAuthenticationMethods(connection, methods, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Methods[0].Plugin = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][1].Value = "mutated"

	again := service.ListClientAuthenticationMethods(connection, methods, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Methods[0].Plugin != "mysql_native_password" {
		t.Fatalf("methods leaked mutation: %#v", again.Methods[0])
	}
	if again.Result.Columns[0].Name != "Method" || again.ResultSchema.Columns[0].Name != "Method" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][1].Value != "mysql_native_password" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

package qsbridge

import "testing"

func TestPlanningServiceListClientParameterBindingsReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	prepared := PreparedPlan{
		Supported: true,
		Parameters: []ParameterRef{
			{Index: 1, Type: DataTypeInt},
			{Name: "city", Type: DataTypeString, Nullable: true},
		},
	}

	exchange := service.ListClientParameterBindings(
		connection,
		prepared,
		NamedParameterValue("city", ValueString, "Seattle"),
		IndexedParameterValue(1, ValueInt, 7),
	)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported parameter binding metadata", exchange)
	}
	if !exchange.Bindings.Supported() || len(exchange.Bindings.Bindings) != 2 {
		t.Fatalf("bindings = %#v, want two valid bindings", exchange.Bindings)
	}
	if len(exchange.Rows) != 2 || !exchange.Rows[0].Bound || !exchange.Rows[1].Bound {
		t.Fatalf("rows = %#v, want two bound rows", exchange.Rows)
	}
	if exchange.Rows[0].Parameter != "?1" || exchange.Rows[0].SuppliedKind != ValueInt {
		t.Fatalf("first row = %#v, want positional int binding", exchange.Rows[0])
	}
	if exchange.Rows[1].Parameter != ":city" || exchange.Rows[1].Name != "city" || exchange.Rows[1].SuppliedKind != ValueString {
		t.Fatalf("second row = %#v, want named string binding", exchange.Rows[1])
	}
	if exchange.Result.RowsReturned != 2 || len(exchange.ResultSchema.Columns) != 9 {
		t.Fatalf("result/schema = %#v/%#v, want binding rows", exchange.Result, exchange.ResultSchema)
	}
	row := exchange.Result.Chunks[0].Rows[1]
	if row[1].Value != ":city" || row[7].Value != true || row[8].Value != "" {
		t.Fatalf("result row = %#v, want bound named parameter cells", row)
	}
}

func TestPlanningServiceListClientParameterBindingsReportsInvalidRowsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	prepared := PreparedPlan{
		Supported: true,
		Parameters: []ParameterRef{
			{Index: 1, Type: DataTypeInt, Nullable: false},
			{Index: 2, Type: DataTypeString},
		},
	}

	exchange := service.ListClientParameterBindings(
		connection,
		prepared,
		IndexedParameterValue(1, ValueString, "bad"),
		IndexedParameterValue(3, ValueInt, 8),
	)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want metadata exchange to remain supported", exchange)
	}
	for _, code := range []DiagnosticCode{
		DiagnosticParameterTypeMismatch,
		DiagnosticParameterMissing,
		DiagnosticParameterExtra,
	} {
		if !containsDiagnosticCode(exchange.Bindings.Diagnostics.Codes(), code) {
			t.Fatalf("diagnostics = %#v, want %s", exchange.Bindings.Diagnostics, code)
		}
	}
	if len(exchange.Rows) != 3 {
		t.Fatalf("rows = %#v, want required rows plus extra row", exchange.Rows)
	}
	if exchange.Rows[0].Diagnostic != DiagnosticParameterTypeMismatch || exchange.Rows[1].Diagnostic != DiagnosticParameterMissing || exchange.Rows[2].Diagnostic != DiagnosticParameterExtra {
		t.Fatalf("rows = %#v, want type/missing/extra diagnostics", exchange.Rows)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 3 {
		t.Fatalf("result = %#v, want successful metadata result despite binding diagnostics", exchange.Result)
	}
}

func TestPlanningServiceListClientParameterBindingsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	prepared := PreparedPlan{
		Supported:  true,
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeInt}},
	}

	exchange := service.ListClientParameterBindings(connection, prepared, IndexedParameterValue(1, ValueInt, 7))
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Prepared.Parameters[0].Type = DataTypeString
	exchange.Values[0].Kind = ValueString
	exchange.Rows[0].RequiredType = DataTypeString
	exchange.Result.Chunks[0].Rows[0][3].Value = "mutated"

	again := service.ListClientParameterBindings(connection, prepared, IndexedParameterValue(1, ValueInt, 7))
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Prepared.Parameters[0].Type != DataTypeInt || again.Values[0].Kind != ValueInt || again.Rows[0].RequiredType != DataTypeInt {
		t.Fatalf("binding metadata leaked mutation: %#v/%#v/%#v", again.Prepared.Parameters, again.Values, again.Rows)
	}
	if again.Result.Chunks[0].Rows[0][3].Value != string(DataTypeInt) {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

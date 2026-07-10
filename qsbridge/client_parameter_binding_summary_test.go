package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientParameterBindingsReturnsCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	prepared := PreparedPlan{
		Supported: true,
		Parameters: []ParameterRef{
			{Index: 1, Type: DataTypeInt},
			{Name: "city", Type: DataTypeString, Nullable: true},
		},
	}

	exchange := service.SummarizeClientParameterBindings(
		connection,
		prepared,
		NamedParameterValue("city", ValueString, "Seattle"),
		IndexedParameterValue(1, ValueInt, 7),
	)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported parameter summary", exchange)
	}
	if !exchange.Bindings.Supported() {
		t.Fatalf("bindings = %#v, want supported bindings", exchange.Bindings)
	}
	row := exchange.Row
	if row.ParameterCount != 2 || row.RequiredCount != 2 || row.NamedCount != 1 || row.PositionalCount != 1 {
		t.Fatalf("row = %#v, want named and positional parameter counts", row)
	}
	if row.NullableCount != 1 || row.PresentCount != 2 || row.BoundCount != 2 {
		t.Fatalf("row = %#v, want present bound counts", row)
	}
	if row.MissingCount != 0 || row.ExtraCount != 0 || row.TypeMismatchCount != 0 || row.NullNotAllowedCount != 0 {
		t.Fatalf("row = %#v, did not expect binding diagnostics", row)
	}
	if len(exchange.ResultSchema.Columns) != 11 || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want one summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[0].Value != 2 || resultRow[2].Value != 1 || resultRow[6].Value != 2 {
		t.Fatalf("result row = %#v, want binding summary cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientParameterBindingsReportsInvalidCountsAsData(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	prepared := PreparedPlan{
		Supported: true,
		Parameters: []ParameterRef{
			{Index: 1, Type: DataTypeInt, Nullable: false},
			{Index: 2, Type: DataTypeString},
			{Index: 4, Type: DataTypeInt, Nullable: false},
		},
	}

	exchange := service.SummarizeClientParameterBindings(
		connection,
		prepared,
		IndexedParameterValue(1, ValueString, "bad"),
		IndexedParameterValue(3, ValueInt, 8),
		IndexedParameterValue(4, ValueNull, nil),
	)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want metadata exchange to remain supported", exchange)
	}
	row := exchange.Row
	if row.ParameterCount != 4 || row.RequiredCount != 3 || row.PresentCount != 3 || row.BoundCount != 0 {
		t.Fatalf("row = %#v, want invalid binding shape counts", row)
	}
	if row.TypeMismatchCount != 1 || row.MissingCount != 1 || row.ExtraCount != 1 || row.NullNotAllowedCount != 1 {
		t.Fatalf("row = %#v, want all invalid binding diagnostic counts", row)
	}
}

func TestPlanningServiceSummarizeClientParameterBindingsFailsForConnectionDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "connection blocked"),
	}

	exchange := service.SummarizeClientParameterBindings(connection, PreparedPlan{})
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want connection diagnostics to block exchange", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || exchange.Result.RowsReturned != 0 {
		t.Fatalf("result = %#v, want failed rowless summary", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientParameterBindingsCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	prepared := PreparedPlan{
		Supported:  true,
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeInt}},
	}

	exchange := service.SummarizeClientParameterBindings(connection, prepared, IndexedParameterValue(1, ValueInt, 7))
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Prepared.Parameters[0].Type = DataTypeString
	exchange.Values[0].Kind = ValueString
	exchange.Row.ParameterCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientParameterBindings(connection, prepared, IndexedParameterValue(1, ValueInt, 7))
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Prepared.Parameters[0].Type != DataTypeInt || again.Values[0].Kind != ValueInt {
		t.Fatalf("binding metadata leaked mutation: %#v/%#v", again.Prepared.Parameters, again.Values)
	}
	if again.Row.ParameterCount != 1 {
		t.Fatalf("summary leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Parameter_count" || again.ResultSchema.Columns[0].Name != "Parameter_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

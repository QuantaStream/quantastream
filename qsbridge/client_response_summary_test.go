package qsbridge

import "testing"

func TestPlanningServiceListClientResponseSummaryReturnsOrderedRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityMultiStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	exchange := service.PrepareConnectionClientExchange(
		connection,
		ConnectionPlanOptions{},
		ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)}},
		"select 1",
		"select 2",
	)
	sequence := exchange.ResponseSequence()

	summary := service.ListClientResponseSummary(sequence)
	if !summary.Supported() {
		t.Fatalf("summary = %#v, want supported response summary", summary)
	}
	if len(summary.Rows) != 2 {
		t.Fatalf("rows = %#v, want two response rows", summary.Rows)
	}
	first := summary.Rows[0]
	if first.Ordinal != 0 || first.Kind != ClientResponseQuery || !first.MoreResults || first.Final {
		t.Fatalf("first row = %#v, want first query with more results", first)
	}
	if first.AccessIntent != PhysicalAccessRead || first.Lifecycle != ClientPlanLifecycleSelect || first.LifecycleSteps != 7 {
		t.Fatalf("first row = %#v, want read select lifecycle", first)
	}
	if first.SchemaColumns != 1 || first.SQL != "select 1" {
		t.Fatalf("first row = %#v, want schema and SQL metadata", first)
	}
	second := summary.Rows[1]
	if second.Ordinal != 1 || second.Kind != ClientResponseQuery || second.MoreResults || !second.Final {
		t.Fatalf("second row = %#v, want final query response", second)
	}
	if len(summary.ResultSchema.Columns) != 18 || summary.Result.RowsReturned != 2 {
		t.Fatalf("result/schema = %#v/%#v, want response summary rows", summary.Result, summary.ResultSchema)
	}
	resultRow := summary.Result.Chunks[0].Rows[0]
	if resultRow[1].Value != string(ClientResponseQuery) || resultRow[3].Value != string(PhysicalAccessRead) || resultRow[4].Value != string(ClientPlanLifecycleSelect) || resultRow[5].Value != 7 || resultRow[8].Value != true || resultRow[15].Value != "more_results,query" {
		t.Fatalf("result row = %#v, want query response flags", resultRow)
	}
}

func TestPlanningServiceListClientResponseSummaryReportsErrorItems(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	service.Routing = NativeOnlyRoutingPolicy()
	exchange := service.PrepareConnectionClientExchange(
		clientStatementConnection(),
		ConnectionPlanOptions{},
		ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueString, "bad")}},
		"select 1",
	)

	summary := service.ListClientResponseSummary(exchange.ResponseSequence())
	if summary.Supported() {
		t.Fatalf("expected error response summary to carry diagnostics")
	}
	if len(summary.Rows) != 1 || summary.Rows[0].Kind != ClientResponseError || summary.Rows[0].ErrorCount == 0 {
		t.Fatalf("rows = %#v, want error response row", summary.Rows)
	}
	if summary.Result.Status != ExecutionFailed || !summary.Result.Complete {
		t.Fatalf("result = %#v, want failed response summary envelope", summary.Result)
	}
}

func TestPlanningServiceListClientResponseSummaryCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	exchange := service.PrepareConnectionClientExchange(
		connection,
		ConnectionPlanOptions{},
		ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)}},
		"select 1",
	)

	summary := service.ListClientResponseSummary(exchange.ResponseSequence())
	summary.Connection.Attributes["client"] = "mutated"
	summary.Rows[0].SQL = "mutated"
	summary.Rows[0].Flags[0] = ClientResponseFlagError
	summary.Result.Columns[0].Name = "mutated"
	summary.ResultSchema.Columns[0].Name = "mutated"
	summary.Result.Chunks[0].Rows[0][17].Value = "mutated"

	again := service.ListClientResponseSummary(exchange.ResponseSequence())
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Rows[0].SQL != "select 1" || !clientResponseFlagsContain(again.Rows[0].Flags, ClientResponseFlagQuery) {
		t.Fatalf("rows leaked mutation: %#v", again.Rows[0])
	}
	if again.Result.Columns[0].Name != "Ordinal" || again.ResultSchema.Columns[0].Name != "Ordinal" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][17].Value != "select 1" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

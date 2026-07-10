package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientResultSchemasReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select o_orderkey from orders")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)}})
	preview := handoff.ResultPreviewBundle()

	exchange := service.SummarizeClientResultSchemas(connection, preview)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported result schema summary", exchange)
	}
	if len(exchange.Rows) != 1 {
		t.Fatalf("rows = %#v, want one result column", exchange.Rows)
	}
	row := exchange.Rows[0]
	if row.StatementOrdinal != 0 || row.ColumnOrdinal != 1 || row.Name != "order_id" {
		t.Fatalf("row = %#v, want first order_id column", row)
	}
	if row.AccessIntent != PhysicalAccessRead || row.Lifecycle != ClientPlanLifecycleSelect || row.LifecycleSteps != 7 {
		t.Fatalf("row = %#v, want read select lifecycle", row)
	}
	if row.Source != "o.o_orderkey" || row.LogicalType != DataTypeInt || row.WireType != "MYSQL_TYPE_LONGLONG" {
		t.Fatalf("row = %#v, want source and wire metadata", row)
	}
	if exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 13 {
		t.Fatalf("result/schema = %#v/%#v, want schema summary row", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[2].Value != string(PhysicalAccessRead) || resultRow[3].Value != string(ClientPlanLifecycleSelect) || resultRow[4].Value != 7 || resultRow[5].Value != "order_id" || resultRow[9].Value != "MYSQL_TYPE_LONGLONG" {
		t.Fatalf("result row = %#v, want column metadata cells", resultRow)
	}
}

func TestPlanningServiceSummarizeClientResultSchemasSkipsStatementResponses(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSessionStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityStatementResults, ProtocolCapabilitySessionActions)
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "use analytics")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, ClientHandoffOptions{})
	preview := handoff.ResultPreviewBundle()

	exchange := service.SummarizeClientResultSchemas(connection, preview)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported statement-only schema summary", exchange)
	}
	if len(exchange.Rows) != 0 || exchange.Result.RowsReturned != 0 {
		t.Fatalf("rows/result = %#v/%#v, want no query schema rows", exchange.Rows, exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientResultSchemasCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql")
	bundle := NewClientStatementBundle(connection, ConnectionPlanOptions{}, "select o_orderkey from orders")
	handoff := service.PrepareClientStatementHandoffBundle(bundle, ClientHandoffOptions{Values: []ParameterValue{IndexedParameterValue(1, ValueInt, 7)}})
	preview := handoff.ResultPreviewBundle()

	exchange := service.SummarizeClientResultSchemas(connection, preview)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Preview.Statements[0].Schema.Columns[0].Flags[0] = ProtocolColumnNullable
	exchange.Rows[0].Flags[0] = ProtocolColumnNullable
	exchange.Result.Chunks[0].Rows[0][11].Value = "mutated"

	again := service.SummarizeClientResultSchemas(connection, preview)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Preview.Statements[0].Schema.Columns[0].Flags[0] != ProtocolColumnDirectSource || again.Rows[0].Flags[0] != ProtocolColumnDirectSource {
		t.Fatalf("schema flags leaked mutation: %#v/%#v", again.Preview.Statements[0].Schema.Columns[0].Flags, again.Rows[0].Flags)
	}
	if again.Result.Chunks[0].Rows[0][11].Value != "direct_source,numeric" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

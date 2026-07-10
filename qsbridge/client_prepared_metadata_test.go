package qsbridge

import "testing"

func TestPlanningServiceListClientPreparedMetadataReturnsParameterAndResultRows(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Protocol = NewProtocolProfile(ProtocolMySQL, "mysql", ProtocolCapabilityPreparedStatements)
	prepared := service.PrepareWithRequest(connection.PlanRequest(
		"select o_orderkey from orders where o_orderkey = ?",
		ConnectionPlanOptions{},
	)).WithHandle(PreparedStatementHandle{ID: 7, Name: "stmt_orders"})

	exchange := service.ListClientPreparedMetadata(connection, prepared)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported prepare metadata", exchange)
	}
	if len(exchange.Rows) != 2 {
		t.Fatalf("rows = %#v, want parameter and result column", exchange.Rows)
	}
	parameter := exchange.Rows[0]
	if parameter.Section != ClientPreparedMetadataParameter || parameter.Ordinal != 1 || parameter.Name != "?1" {
		t.Fatalf("parameter row = %#v, want first positional parameter", parameter)
	}
	if parameter.StatementID != 7 || parameter.StatementName != "stmt_orders" || parameter.LogicalType != DataTypeInt || parameter.WireType != "MYSQL_TYPE_LONGLONG" {
		t.Fatalf("parameter row = %#v, want MySQL int parameter metadata", parameter)
	}
	if parameter.AccessIntent != PhysicalAccessRead || parameter.Lifecycle != ClientPlanLifecycleSelect || parameter.LifecycleSteps != 7 {
		t.Fatalf("parameter row = %#v, want read/select lifecycle metadata", parameter)
	}
	resultColumn := exchange.Rows[1]
	if resultColumn.Section != ClientPreparedMetadataResultColumn || resultColumn.Ordinal != 1 || resultColumn.Name != "order_id" {
		t.Fatalf("result row = %#v, want order_id result column", resultColumn)
	}
	if resultColumn.Source != "o.o_orderkey" || resultColumn.WireType != "MYSQL_TYPE_LONGLONG" {
		t.Fatalf("result row = %#v, want source and wire metadata", resultColumn)
	}
	if exchange.Result.RowsReturned != 2 || len(exchange.ResultSchema.Columns) != 15 {
		t.Fatalf("result/schema = %#v/%#v, want two metadata rows", exchange.Result, exchange.ResultSchema)
	}
	row := exchange.Result.Chunks[0].Rows[0]
	if row[0].Value != string(ClientPreparedMetadataParameter) || row[2].Value != 7 || row[4].Value != string(PhysicalAccessRead) || row[7].Value != "?1" {
		t.Fatalf("result row = %#v, want parameter metadata cells", row)
	}
}

func TestPlanningServiceListClientPreparedMetadataReportsBlockingDiagnostics(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	prepared := PreparedPlan{
		SQL:       "select * from missing",
		Supported: false,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "missing table"),
		},
	}

	exchange := service.ListClientPreparedMetadata(connection, prepared)
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want unsupported prepare metadata", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticCatalogTableNotFound) {
		t.Fatalf("diagnostics = %#v, want unknown table", exchange.Diagnostics)
	}
	if len(exchange.Rows) != 0 || exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("rows/result = %#v/%#v, want failed rowless metadata", exchange.Rows, exchange.Result)
	}
}

func TestPlanningServiceListClientPreparedMetadataCopiesMutableState(t *testing.T) {
	service := NewPlanningService(Planner{
		Parser:        stubParserBridge{statement: serviceSelectStatement()},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Session:       SessionContext{User: "moli"},
	}, nil)
	connection := clientStatementConnection(ClientCapabilityPreparedStatements)
	connection.Attributes = map[string]string{"client": "mysql"}
	prepared := service.PrepareWithRequest(connection.PlanRequest(
		"select o_orderkey from orders where o_orderkey = ?",
		ConnectionPlanOptions{},
	)).WithHandle(PreparedStatementHandle{ID: 7, Name: "stmt_orders"})

	exchange := service.ListClientPreparedMetadata(connection, prepared)
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Prepared.Parameters[0].Type = DataTypeString
	exchange.Rows[0].Name = "mutated"
	exchange.Rows[1].Flags[0] = ProtocolColumnNullable
	exchange.Result.Chunks[0].Rows[0][4].Value = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"

	again := service.ListClientPreparedMetadata(connection, prepared)
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Prepared.Parameters[0].Type != DataTypeInt || again.Rows[0].Name != "?1" {
		t.Fatalf("prepared metadata leaked mutation: %#v/%#v", again.Prepared.Parameters, again.Rows)
	}
	if len(again.Rows[1].Flags) == 0 || again.Rows[1].Flags[0] != ProtocolColumnDirectSource {
		t.Fatalf("result flags leaked mutation: %#v", again.Rows[1].Flags)
	}
	if again.Result.Chunks[0].Rows[0][7].Value != "?1" || again.ResultSchema.Columns[0].Name != "Section" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Chunks, again.ResultSchema)
	}
}

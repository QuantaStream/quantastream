package qsbridge

import "testing"

func TestPlanningServiceListClientCatalogSchemas(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	catalog := MemoryCatalog{Tables: []TableDefinition{
		{Schema: "analytics", Name: "events"},
		{Schema: "quanta", Name: "orders"},
	}}

	exchange := service.ListClientCatalogSchemas(connection, catalog)
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported schema metadata", exchange)
	}
	if exchange.Kind != ClientCatalogSchemas || len(exchange.Schemas) != 2 {
		t.Fatalf("exchange = %#v, want two schemas", exchange)
	}
	if exchange.Result.Status != ExecutionComplete || len(exchange.ResultSchema.Columns) != 1 || exchange.ResultSchema.Columns[0].Name != "Database" {
		t.Fatalf("result/schema = %#v/%#v, want database metadata result", exchange.Result, exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 2 || exchange.Result.Chunks[0].Rows[0][0].Value != "analytics" {
		t.Fatalf("result rows = %#v, want sorted schema rows", exchange.Result)
	}
}

func TestPlanningServiceListClientCatalogTablesUsesSelectedSchema(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = "quanta"
	catalog := MemoryCatalog{Tables: []TableDefinition{
		{Schema: "analytics", Name: "events"},
		{Schema: "quanta", Name: "orders"},
	}}

	exchange := service.ListClientCatalogTables(connection, catalog, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported table metadata", exchange)
	}
	if exchange.Schema != "quanta" || len(exchange.Tables) != 1 || exchange.Tables[0].Name != "orders" {
		t.Fatalf("exchange = %#v, want selected schema table metadata", exchange)
	}
	if len(exchange.ResultSchema.Columns) != 6 || exchange.ResultSchema.Columns[0].Name != "Schema" || exchange.ResultSchema.Columns[2].Name != "Engine" {
		t.Fatalf("schema = %#v, want table metadata columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 1 || exchange.Result.Chunks[0].Rows[0][1].Value != "orders" {
		t.Fatalf("result rows = %#v, want orders table row", exchange.Result.Chunks)
	}
}

func TestPlanningServiceListClientCatalogColumns(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "orders",
		Fields: []FieldDefinition{{Name: "o_orderkey", Type: DataTypeInt}},
	}}}

	exchange := service.ListClientCatalogColumns(connection, catalog, "quanta", "orders")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported column metadata", exchange)
	}
	if exchange.Kind != ClientCatalogColumns || len(exchange.Columns) != 1 || exchange.Columns[0].Name != "o_orderkey" {
		t.Fatalf("exchange = %#v, want order column metadata", exchange)
	}
	if len(exchange.ResultSchema.Columns) != 7 || exchange.ResultSchema.Columns[0].Name != "Field" || exchange.ResultSchema.Columns[1].WireType != "MYSQL_TYPE_VAR_STRING" {
		t.Fatalf("schema = %#v, want column metadata schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 1 || exchange.Result.Chunks[0].Rows[0][0].Value != "o_orderkey" || exchange.Result.Chunks[0].Rows[0][1].Value != "int" {
		t.Fatalf("result rows = %#v, want order key column row", exchange.Result.Chunks)
	}
}

func TestPlanningServiceListClientTableFieldsFiltersByWildcard(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = "quanta"
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "orders",
		Fields: []FieldDefinition{
			{Name: "o_orderkey", Type: DataTypeInt},
			{Name: "o_orderdate", Type: DataTypeTime},
			{Name: "o_custkey", PhysicalName: "customer_id", Type: DataTypeInt},
		},
	}}}

	exchange := service.ListClientTableFields(connection, catalog, "", "orders", "o_order%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported field-list metadata", exchange)
	}
	if exchange.Schema != "quanta" || exchange.Table != "orders" || exchange.Pattern != "o_order%" {
		t.Fatalf("exchange = %#v, want selected schema and pattern metadata", exchange)
	}
	if len(exchange.Columns) != 2 || exchange.Columns[0].Name != "o_orderkey" || exchange.Columns[1].Name != "o_orderdate" {
		t.Fatalf("columns = %#v, want order columns only", exchange.Columns)
	}
	if exchange.Result.RowsReturned != 2 || exchange.Result.Chunks[0].Rows[1][0].Value != "o_orderdate" {
		t.Fatalf("result rows = %#v, want filtered field-list rows", exchange.Result.Chunks)
	}

	physical := service.ListClientTableFields(connection, catalog, "", "orders", "customer_%")
	if len(physical.Columns) != 1 || physical.Columns[0].Name != "o_custkey" {
		t.Fatalf("physical columns = %#v, want physical-name wildcard match", physical.Columns)
	}
}

func TestPlanningServiceListClientCatalogMetadataReportsMissingInputs(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = ""
	catalog := MemoryCatalog{}

	tables := service.ListClientCatalogTables(connection, catalog, "")
	if tables.Supported() || !containsDiagnosticCode(tables.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("tables = %#v, want missing schema diagnostic", tables)
	}
	if tables.Result.Status != ExecutionFailed || !tables.Result.Complete || len(tables.ResultSchema.Columns) != 6 {
		t.Fatalf("tables result/schema = %#v/%#v, want failed table metadata envelope", tables.Result, tables.ResultSchema)
	}
	if len(tables.Result.Chunks) != 0 || tables.Result.RowsReturned != 0 {
		t.Fatalf("tables result = %#v, want failed rowless metadata envelope", tables.Result)
	}
	columns := service.ListClientCatalogColumns(connection, catalog, "quanta", "")
	if columns.Supported() || !containsDiagnosticCode(columns.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("columns = %#v, want missing table diagnostic", columns)
	}
	if columns.Result.Status != ExecutionFailed || !columns.Result.Complete || len(columns.ResultSchema.Columns) != 7 {
		t.Fatalf("columns result/schema = %#v/%#v, want failed column metadata envelope", columns.Result, columns.ResultSchema)
	}
}

func TestPlanningServiceListClientCatalogMetadataReportsUnsupportedCatalog(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientCatalogSchemas(connection, nil)
	if exchange.Supported() || !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("exchange = %#v, want unsupported catalog diagnostic", exchange)
	}
}

func TestPlanningServiceListClientCatalogMetadataCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "orders",
		Fields: []FieldDefinition{{Name: "o_orderkey"}},
	}}}

	exchange := service.ListClientCatalogColumns(connection, catalog, "quanta", "orders")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Columns[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientCatalogColumns(connection, catalog, "quanta", "orders")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Columns[0].Name != "o_orderkey" {
		t.Fatalf("column metadata leaked mutation: %#v", again.Columns)
	}
	if again.Result.Columns[0].Name != "Field" || again.ResultSchema.Columns[0].Name != "Field" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != "o_orderkey" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

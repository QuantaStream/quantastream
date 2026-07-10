package qsbridge

import "testing"

func TestPlanningServiceListClientTableStatusUsesSelectedSchemaAndPattern(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = "quanta"
	catalog := MemoryCatalog{Tables: []TableDefinition{
		{
			Schema: "analytics",
			Name:   "events",
		},
		{
			Schema: "quanta",
			Name:   "orders",
			Fields: []FieldDefinition{
				{Name: "o_orderkey", Type: DataTypeInt, PrimaryKey: true},
				{Name: "o_orderdate", Type: DataTypeTime},
			},
			Relationships: []RelationshipDefinition{
				{Name: "orders_customer", FromTable: "orders", FromField: "o_custkey", ToTable: "customer", ToField: "c_custkey"},
			},
			Storage: StorageProfile{
				Engine:      "bitmap",
				Index:       IndexBSI,
				Partitioned: true,
				Searchable:  true,
				Replicated:  true,
			},
		},
		{
			Schema: "quanta",
			Name:   "partsupp",
			Fields: []FieldDefinition{{Name: "ps_partkey", Type: DataTypeInt}},
		},
	}}

	exchange := service.ListClientTableStatus(connection, catalog, "", "ord%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported table status metadata", exchange)
	}
	if exchange.Schema != "quanta" || exchange.Pattern != "ord%" {
		t.Fatalf("exchange = %#v, want selected schema and pattern", exchange)
	}
	if len(exchange.Tables) != 1 {
		t.Fatalf("tables = %#v, want one matching table", exchange.Tables)
	}
	status := exchange.Tables[0]
	if status.Name != "orders" || status.Engine != "bitmap" || status.Index != string(IndexBSI) {
		t.Fatalf("status = %#v, want orders storage traits", status)
	}
	if !status.Partitioned || !status.Searchable || !status.Replicated || status.Fields != 2 || status.Relationships != 1 {
		t.Fatalf("status = %#v, want table trait counts", status)
	}
	if len(exchange.ResultSchema.Columns) != 9 || exchange.ResultSchema.Columns[1].Name != "Name" || exchange.ResultSchema.Columns[7].Name != "Fields" {
		t.Fatalf("schema = %#v, want table status result schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 1 || exchange.Result.Chunks[0].Rows[0][1].Value != "orders" || exchange.Result.Chunks[0].Rows[0][7].Value != 2 {
		t.Fatalf("result rows = %#v, want orders table status row", exchange.Result.Chunks)
	}
}

func TestPlanningServiceListClientTableStatusReportsMissingInputs(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = ""

	exchange := service.ListClientTableStatus(connection, MemoryCatalog{}, "", "")
	if exchange.Supported() || !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("exchange = %#v, want missing schema diagnostic", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 9 {
		t.Fatalf("result/schema = %#v/%#v, want failed table status envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientTableStatusReportsCatalogErrors(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientTableStatus(connection, MemoryCatalog{}, "missing", "")
	if exchange.Supported() || !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticCatalogSchemaNotFound) {
		t.Fatalf("exchange = %#v, want missing schema diagnostic", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || exchange.Result.RowsReturned != 0 {
		t.Fatalf("result = %#v, want failed rowless table status result", exchange.Result)
	}
}

func TestPlanningServiceListClientTableStatusReportsUnsupportedCatalog(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientTableStatus(connection, nil, "quanta", "")
	if exchange.Supported() || !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("exchange = %#v, want unsupported catalog diagnostic", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed unsupported-catalog result", exchange.Result)
	}
}

func TestPlanningServiceListClientTableStatusCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "orders",
		Fields: []FieldDefinition{{Name: "o_orderkey"}},
		Storage: StorageProfile{
			Engine: "bitmap",
		},
	}}}

	exchange := service.ListClientTableStatus(connection, catalog, "quanta", "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Tables[0].Name = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientTableStatus(connection, catalog, "quanta", "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Tables[0].Name != "orders" {
		t.Fatalf("table status leaked mutation: %#v", again.Tables)
	}
	if again.Result.Columns[0].Name != "Schema" || again.ResultSchema.Columns[0].Name != "Schema" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != "quanta" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

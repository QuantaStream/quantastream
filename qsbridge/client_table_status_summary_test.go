package qsbridge

import "testing"

func TestPlanningServiceSummarizeClientTableStatusUsesSelectedSchemaAndPattern(t *testing.T) {
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
			Name:   "order_notes",
			Fields: []FieldDefinition{{Name: "note", Type: DataTypeString}},
			Storage: StorageProfile{
				Engine: "bitmap",
				Index:  IndexBackingString,
			},
		},
		{
			Schema: "quanta",
			Name:   "partsupp",
			Fields: []FieldDefinition{{Name: "ps_partkey", Type: DataTypeInt}},
		},
	}}

	exchange := service.SummarizeClientTableStatus(connection, catalog, "", "ord%")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported table status summary", exchange)
	}
	if exchange.Schema != "quanta" || exchange.Pattern != "ord%" {
		t.Fatalf("exchange = %#v, want selected schema and pattern", exchange)
	}
	row := exchange.Row
	if row.TableCount != 2 || row.PartitionedCount != 1 || row.SearchableCount != 1 || row.ReplicatedCount != 1 {
		t.Fatalf("row = %#v, want filtered table trait counts", row)
	}
	if row.FieldCount != 3 || row.RelationshipCount != 1 || row.DistinctEngineCount != 1 || row.DistinctIndexCount != 2 {
		t.Fatalf("row = %#v, want filtered table aggregate counts", row)
	}
	if len(exchange.ResultSchema.Columns) != 8 || exchange.ResultSchema.Columns[0].Name != "Table_count" {
		t.Fatalf("schema = %#v, want table status summary schema", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 1 || exchange.Result.Chunks[0].Rows[0][0].Value != 2 || exchange.Result.Chunks[0].Rows[0][4].Value != 3 {
		t.Fatalf("result rows = %#v, want table status summary row", exchange.Result.Chunks)
	}
}

func TestPlanningServiceSummarizeClientTableStatusReportsMissingInputs(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = ""

	exchange := service.SummarizeClientTableStatus(connection, MemoryCatalog{}, "", "")
	if exchange.Supported() || !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("exchange = %#v, want missing schema diagnostic", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 8 {
		t.Fatalf("result/schema = %#v/%#v, want failed table status summary envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceSummarizeClientTableStatusReportsCatalogErrors(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.SummarizeClientTableStatus(connection, MemoryCatalog{}, "missing", "")
	if exchange.Supported() || !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticCatalogSchemaNotFound) {
		t.Fatalf("exchange = %#v, want missing schema diagnostic", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || exchange.Result.RowsReturned != 0 {
		t.Fatalf("result = %#v, want failed rowless table status summary result", exchange.Result)
	}
}

func TestPlanningServiceSummarizeClientTableStatusCopiesMutableMetadata(t *testing.T) {
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

	exchange := service.SummarizeClientTableStatus(connection, catalog, "quanta", "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Row.TableCount = 99
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = 99

	again := service.SummarizeClientTableStatus(connection, catalog, "quanta", "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Row.TableCount != 1 || again.Row.FieldCount != 1 {
		t.Fatalf("row leaked mutation: %#v", again.Row)
	}
	if again.Result.Columns[0].Name != "Table_count" || again.ResultSchema.Columns[0].Name != "Table_count" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != 1 {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

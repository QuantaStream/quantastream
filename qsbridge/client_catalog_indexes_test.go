package qsbridge

import "testing"

func TestPlanningServiceListClientCatalogIndexesReturnsKeyRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = "quanta"
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "lineitem",
		Fields: []FieldDefinition{
			{Name: "l_orderkey", Type: DataTypeInt, Index: IndexBSI},
			{Name: "l_linenumber", Type: DataTypeInt, Index: IndexBSI, PrimaryKey: true},
			{Name: "l_shipdate", Type: DataTypeTime, Index: IndexDateTime, PrimaryKey: true},
		},
		Relationships: []RelationshipDefinition{{
			Name:      "lineitem_orders",
			FromTable: "lineitem",
			FromField: "l_orderkey",
			ToTable:   "orders",
			ToField:   "o_orderkey",
			Encoding:  RelationshipEncodingProfile{Kind: RelationshipEncodingVector},
		}},
	}}}

	exchange := service.ListClientCatalogIndexes(connection, catalog, "", "lineitem")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported index metadata", exchange)
	}
	if exchange.Schema != "quanta" || exchange.Table != "lineitem" {
		t.Fatalf("exchange = %#v, want selected schema and table", exchange)
	}
	if len(exchange.Indexes) != 4 {
		t.Fatalf("indexes = %#v, want primary, field index, and relationship index rows", exchange.Indexes)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 4 || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("result/schema = %#v/%#v, want index metadata result", exchange.Result, exchange.ResultSchema)
	}
	primary := exchange.Indexes[0]
	if primary.KeyName != "PRIMARY" || primary.ColumnName != "l_linenumber" || !primary.Unique || primary.Sequence != 1 {
		t.Fatalf("primary row = %#v, want first primary key column", primary)
	}
	relationship := exchange.Indexes[3]
	if relationship.KeyName != "lineitem_orders" || relationship.ReferencedTable != "orders" || relationship.ReferencedColumn != "o_orderkey" {
		t.Fatalf("relationship row = %#v, want relationship target metadata", relationship)
	}
	if exchange.Result.Chunks[0].Rows[3][7].Value != "lineitem_orders" || exchange.Result.Chunks[0].Rows[3][8].Value != "orders" {
		t.Fatalf("relationship result row = %#v, want relationship metadata", exchange.Result.Chunks[0].Rows[3])
	}
}

func TestPlanningServiceListClientCatalogIndexesReportsMissingInputs(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = ""
	catalog := MemoryCatalog{}

	missingSchema := service.ListClientCatalogIndexes(connection, catalog, "", "orders")
	if missingSchema.Supported() || !containsDiagnosticCode(missingSchema.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing schema = %#v, want invalid execution option", missingSchema)
	}
	if missingSchema.Result.Status != ExecutionFailed || !missingSchema.Result.Complete || len(missingSchema.ResultSchema.Columns) != 10 {
		t.Fatalf("missing schema result/schema = %#v/%#v, want failed index envelope", missingSchema.Result, missingSchema.ResultSchema)
	}

	missingTable := service.ListClientCatalogIndexes(connection, catalog, "quanta", "")
	if missingTable.Supported() || !containsDiagnosticCode(missingTable.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing table = %#v, want invalid execution option", missingTable)
	}
}

func TestPlanningServiceListClientCatalogIndexesReportsCatalogMiss(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientCatalogIndexes(connection, MemoryCatalog{}, "quanta", "missing")
	if exchange.Supported() {
		t.Fatalf("exchange = %#v, want missing table diagnostic", exchange)
	}
	if !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticCatalogTableNotFound) {
		t.Fatalf("diagnostics = %#v, want table not found", exchange.Diagnostics)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete {
		t.Fatalf("result = %#v, want failed complete result", exchange.Result)
	}
}

func TestPlanningServiceListClientCatalogIndexesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "orders",
		Fields: []FieldDefinition{{Name: "o_orderkey", Index: IndexBSI, PrimaryKey: true}},
	}}}

	exchange := service.ListClientCatalogIndexes(connection, catalog, "quanta", "orders")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Indexes[0].ColumnName = "mutated"
	exchange.Result.Chunks[0].Rows[0][4].Value = "mutated"

	again := service.ListClientCatalogIndexes(connection, catalog, "quanta", "orders")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Indexes[0].ColumnName != "o_orderkey" || again.Result.Chunks[0].Rows[0][4].Value != "o_orderkey" {
		t.Fatalf("index metadata leaked mutation: %#v/%#v", again.Indexes, again.Result.Chunks)
	}
}

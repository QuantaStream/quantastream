package qsbridge

import "testing"

func TestPlanningServiceListClientCatalogConstraintsReturnsKeysAndRelationships(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = "quanta"
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "orders",
		Fields: []FieldDefinition{
			{Name: "o_orderkey", Type: DataTypeInt, PrimaryKey: true},
			{Name: "o_custkey", Type: DataTypeInt},
		},
		Relationships: []RelationshipDefinition{{
			Name:      "orders_customer",
			FromTable: "orders",
			FromField: "o_custkey",
			ToTable:   "customer",
			ToField:   "c_custkey",
		}},
	}}}

	exchange := service.ListClientCatalogConstraints(connection, catalog, "", "orders", "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported constraint metadata", exchange)
	}
	if exchange.Schema != "quanta" || exchange.Table != "orders" || len(exchange.Constraints) != 2 {
		t.Fatalf("exchange = %#v, want selected schema constraints", exchange)
	}
	primary := exchange.Constraints[0]
	foreign := exchange.Constraints[1]
	if primary.ConstraintName != "PRIMARY" || primary.ConstraintType != ClientCatalogConstraintPrimaryKey || primary.ColumnName != "o_orderkey" || primary.OrdinalPosition != 1 {
		t.Fatalf("primary = %#v, want primary key constraint", primary)
	}
	if foreign.ConstraintName != "orders_customer" || foreign.ConstraintType != ClientCatalogConstraintForeignKey || foreign.ColumnName != "o_custkey" {
		t.Fatalf("foreign = %#v, want foreign key constraint", foreign)
	}
	if foreign.ReferencedSchema != "quanta" || foreign.ReferencedTable != "customer" || foreign.ReferencedColumn != "c_custkey" {
		t.Fatalf("foreign = %#v, want referenced table metadata", foreign)
	}
	if len(exchange.ResultSchema.Columns) != 9 || exchange.ResultSchema.Columns[2].Name != "Constraint_name" || exchange.ResultSchema.Columns[5].Name != "Ordinal_position" {
		t.Fatalf("schema = %#v, want constraint result columns", exchange.ResultSchema)
	}
	if exchange.Result.RowsReturned != 2 || exchange.Result.Chunks[0].Rows[1][2].Value != "orders_customer" {
		t.Fatalf("result rows = %#v, want constraint rows", exchange.Result.Chunks)
	}
}

func TestPlanningServiceListClientCatalogConstraintsFiltersByNameOrColumn(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "orders",
		Fields: []FieldDefinition{
			{Name: "o_orderkey", PrimaryKey: true},
			{Name: "o_linenumber", PrimaryKey: true},
		},
		Relationships: []RelationshipDefinition{{
			Name:      "orders_customer",
			FromField: "o_custkey",
			ToTable:   "customer",
			ToField:   "c_custkey",
		}},
	}}}

	byName := service.ListClientCatalogConstraints(connection, catalog, "quanta", "orders", "orders_%")
	if len(byName.Constraints) != 1 || byName.Constraints[0].ConstraintName != "orders_customer" {
		t.Fatalf("constraints = %#v, want relationship matched by name", byName.Constraints)
	}
	byColumn := service.ListClientCatalogConstraints(connection, catalog, "quanta", "orders", "o_order%")
	if len(byColumn.Constraints) != 1 || byColumn.Constraints[0].ColumnName != "o_orderkey" {
		t.Fatalf("constraints = %#v, want primary key matched by column", byColumn.Constraints)
	}
}

func TestPlanningServiceListClientCatalogConstraintsReportsMissingInputs(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = ""
	catalog := MemoryCatalog{}

	missingSchema := service.ListClientCatalogConstraints(connection, catalog, "", "orders", "")
	if missingSchema.Supported() || !containsDiagnosticCode(missingSchema.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing schema = %#v, want invalid option diagnostic", missingSchema)
	}
	if missingSchema.Result.Status != ExecutionFailed || !missingSchema.Result.Complete || len(missingSchema.ResultSchema.Columns) != 9 {
		t.Fatalf("missing schema result/schema = %#v/%#v, want failed envelope", missingSchema.Result, missingSchema.ResultSchema)
	}
	missingTable := service.ListClientCatalogConstraints(connection, catalog, "quanta", "", "")
	if missingTable.Supported() || !containsDiagnosticCode(missingTable.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing table = %#v, want invalid option diagnostic", missingTable)
	}
}

func TestPlanningServiceListClientCatalogConstraintsReportsCatalogErrors(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientCatalogConstraints(connection, MemoryCatalog{}, "quanta", "missing", "")
	if exchange.Supported() || !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticCatalogTableNotFound) {
		t.Fatalf("exchange = %#v, want missing table diagnostic", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || exchange.Result.RowsReturned != 0 {
		t.Fatalf("result = %#v, want failed rowless constraint result", exchange.Result)
	}
}

func TestPlanningServiceListClientCatalogConstraintsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "orders",
		Fields: []FieldDefinition{{Name: "o_orderkey", PrimaryKey: true}},
	}}}

	exchange := service.ListClientCatalogConstraints(connection, catalog, "quanta", "orders", "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Constraints[0].ColumnName = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientCatalogConstraints(connection, catalog, "quanta", "orders", "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Constraints[0].ColumnName != "o_orderkey" {
		t.Fatalf("constraints leaked mutation: %#v", again.Constraints)
	}
	if again.Result.Columns[0].Name != "Constraint_schema" || again.ResultSchema.Columns[0].Name != "Constraint_schema" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != "quanta" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

package qsbridge

import "testing"

func TestPlanningServiceListClientCatalogRelationshipsReturnsRows(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = "quanta"
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "lineitem",
		Relationships: []RelationshipDefinition{{
			Name:        "lineitem_orders",
			FromTable:   "lineitem",
			FromField:   "l_orderkey",
			ToTable:     "orders",
			ToField:     "o_orderkey",
			Direction:   JoinChildToParent,
			Cardinality: "many_to_one",
			Encoding: RelationshipEncodingProfile{
				Kind: RelationshipEncodingVector,
				Capabilities: RelationshipCapabilities{
					RelationshipCapabilityJoinReduction,
					RelationshipCapabilityParentLookup,
				},
			},
		}},
	}}}

	exchange := service.ListClientCatalogRelationships(connection, catalog, "", "lineitem")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported relationship metadata", exchange)
	}
	if exchange.Schema != "quanta" || exchange.Table != "lineitem" || len(exchange.Relationships) != 1 {
		t.Fatalf("exchange = %#v, want selected table relationship", exchange)
	}
	relationship := exchange.Relationships[0]
	if relationship.Name != "lineitem_orders" || relationship.ColumnName != "l_orderkey" || relationship.ReferencedTable != "orders" || relationship.ReferencedColumn != "o_orderkey" {
		t.Fatalf("relationship = %#v, want relationship target metadata", relationship)
	}
	if exchange.Result.Status != ExecutionComplete || exchange.Result.RowsReturned != 1 || len(exchange.ResultSchema.Columns) != 10 {
		t.Fatalf("result/schema = %#v/%#v, want relationship result metadata", exchange.Result, exchange.ResultSchema)
	}
	row := exchange.Result.Chunks[0].Rows[0]
	if row[2].Value != "lineitem_orders" || row[6].Value != string(JoinChildToParent) || row[9].Value != "join_reduction,parent_lookup" {
		t.Fatalf("row = %#v, want relationship row metadata", row)
	}
}

func TestPlanningServiceListClientCatalogRelationshipsReportsMissingInputs(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = ""
	catalog := MemoryCatalog{}

	missingSchema := service.ListClientCatalogRelationships(connection, catalog, "", "lineitem")
	if missingSchema.Supported() || !containsDiagnosticCode(missingSchema.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing schema = %#v, want invalid execution option", missingSchema)
	}
	if missingSchema.Result.Status != ExecutionFailed || !missingSchema.Result.Complete || len(missingSchema.ResultSchema.Columns) != 10 {
		t.Fatalf("missing schema result/schema = %#v/%#v, want failed relationship envelope", missingSchema.Result, missingSchema.ResultSchema)
	}

	missingTable := service.ListClientCatalogRelationships(connection, catalog, "quanta", "")
	if missingTable.Supported() || !containsDiagnosticCode(missingTable.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing table = %#v, want invalid execution option", missingTable)
	}
}

func TestPlanningServiceListClientCatalogRelationshipsReportsCatalogMiss(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientCatalogRelationships(connection, MemoryCatalog{}, "quanta", "missing")
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

func TestPlanningServiceListClientCatalogRelationshipsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "lineitem",
		Relationships: []RelationshipDefinition{{
			Name:      "lineitem_orders",
			FromField: "l_orderkey",
			ToTable:   "orders",
			ToField:   "o_orderkey",
			Encoding: RelationshipEncodingProfile{
				Kind:         RelationshipEncodingVector,
				Capabilities: RelationshipCapabilities{RelationshipCapabilityParentLookup},
			},
		}},
	}}}

	exchange := service.ListClientCatalogRelationships(connection, catalog, "quanta", "lineitem")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Relationships[0].ColumnName = "mutated"
	exchange.Relationships[0].Capabilities[0] = RelationshipCapabilityNullExtension
	exchange.Result.Chunks[0].Rows[0][3].Value = "mutated"

	again := service.ListClientCatalogRelationships(connection, catalog, "quanta", "lineitem")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Relationships[0].ColumnName != "l_orderkey" || again.Result.Chunks[0].Rows[0][3].Value != "l_orderkey" {
		t.Fatalf("relationship metadata leaked mutation: %#v/%#v", again.Relationships, again.Result.Chunks)
	}
	if again.Relationships[0].Capabilities[0] != RelationshipCapabilityParentLookup {
		t.Fatalf("relationship capabilities leaked mutation: %#v", again.Relationships[0].Capabilities)
	}
}

package qsbridge

import "testing"

func TestPlanningServiceListClientCatalogSummaryReturnsSchemaCounts(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	catalog := MemoryCatalog{
		Schemas: []CatalogSchemaDefinition{{Name: "empty"}},
		Tables: []TableDefinition{
			{
				Schema: "quanta",
				Name:   "part",
				Fields: []FieldDefinition{
					{
						Name:  "p_type",
						Index: IndexStringEnum,
						Dictionary: DictionaryDefinition{
							Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs},
						},
					},
					{
						Name: "p_name",
						Encoding: EncodingProfile{
							Search: SearchProfile{Enabled: true, Mode: "text"},
						},
					},
				},
				Relationships: []RelationshipDefinition{{Name: "part_partsupp"}},
			},
			{
				Schema: "quanta",
				Name:   "partsupp",
				Fields: []FieldDefinition{{Name: "ps_partkey"}, {Name: "ps_suppkey"}},
			},
			{
				Schema: "audit",
				Name:   "events",
				Fields: []FieldDefinition{{Name: "event_id"}},
			},
		},
	}

	exchange := service.ListClientCatalogSummary(connection, catalog, "")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported catalog summary", exchange)
	}
	if len(exchange.Summaries) != 3 {
		t.Fatalf("summaries = %#v, want three schema summaries", exchange.Summaries)
	}
	audit, empty, quanta := exchange.Summaries[0], exchange.Summaries[1], exchange.Summaries[2]
	if audit.Schema != "audit" || audit.Tables != 1 || audit.Columns != 1 {
		t.Fatalf("audit summary = %#v, want one table and one column", audit)
	}
	if empty.Schema != "empty" || empty.Tables != 0 || empty.Columns != 0 {
		t.Fatalf("empty summary = %#v, want explicit empty schema", empty)
	}
	if quanta.Schema != "quanta" || quanta.Tables != 2 || quanta.Columns != 4 || quanta.Relationships != 1 {
		t.Fatalf("quanta summary = %#v, want table/column/relationship counts", quanta)
	}
	if quanta.DictionaryFields != 1 || quanta.StringEnums != 1 || quanta.SearchableFields != 1 {
		t.Fatalf("quanta summary = %#v, want dictionary/string/search counts", quanta)
	}
	if len(exchange.ResultSchema.Columns) != 7 || exchange.ResultSchema.Columns[0].Name != "Schema" || exchange.Result.RowsReturned != 3 {
		t.Fatalf("result/schema = %#v/%#v, want summary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[2]
	if resultRow[0].Value != "quanta" || resultRow[4].Value != 1 || resultRow[6].Value != 1 {
		t.Fatalf("result row = %#v, want quanta summary counts", resultRow)
	}
}

func TestPlanningServiceListClientCatalogSummaryFiltersEmptySchema(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	catalog := MemoryCatalog{Schemas: []CatalogSchemaDefinition{{Name: "empty"}}}

	exchange := service.ListClientCatalogSummary(connection, catalog, "empty")
	if !exchange.Supported() || len(exchange.Summaries) != 1 {
		t.Fatalf("exchange = %#v, want one empty schema summary", exchange)
	}
	if exchange.Summaries[0].Schema != "empty" || exchange.Summaries[0].Tables != 0 {
		t.Fatalf("summary = %#v, want empty schema row", exchange.Summaries[0])
	}
}

func TestPlanningServiceListClientCatalogSummaryFiltersSchema(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	catalog := MemoryCatalog{Tables: []TableDefinition{
		{Schema: "quanta", Name: "orders", Fields: []FieldDefinition{{Name: "o_orderkey"}}},
		{Schema: "audit", Name: "events", Fields: []FieldDefinition{{Name: "event_id"}}},
	}}

	exchange := service.ListClientCatalogSummary(connection, catalog, "quanta")
	if !exchange.Supported() || len(exchange.Summaries) != 1 {
		t.Fatalf("exchange = %#v, want one filtered summary", exchange)
	}
	if exchange.Summaries[0].Schema != "quanta" || exchange.Summaries[0].Tables != 1 {
		t.Fatalf("summary = %#v, want quanta only", exchange.Summaries[0])
	}
}

func TestPlanningServiceListClientCatalogSummaryReportsUnsupportedCatalog(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()

	exchange := service.ListClientCatalogSummary(connection, nil, "")
	if exchange.Supported() || !containsDiagnosticCode(exchange.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("exchange = %#v, want unsupported catalog diagnostic", exchange)
	}
	if exchange.Result.Status != ExecutionFailed || !exchange.Result.Complete || len(exchange.ResultSchema.Columns) != 7 {
		t.Fatalf("result/schema = %#v/%#v, want failed summary envelope", exchange.Result, exchange.ResultSchema)
	}
}

func TestPlanningServiceListClientCatalogSummaryCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "orders",
		Fields: []FieldDefinition{{Name: "o_orderkey"}},
	}}}

	exchange := service.ListClientCatalogSummary(connection, catalog, "")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Summaries[0].Schema = "mutated"
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][0].Value = "mutated"

	again := service.ListClientCatalogSummary(connection, catalog, "")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Summaries[0].Schema != "quanta" {
		t.Fatalf("summary leaked mutation: %#v", again.Summaries[0])
	}
	if again.Result.Columns[0].Name != "Schema" || again.ResultSchema.Columns[0].Name != "Schema" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][0].Value != "quanta" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

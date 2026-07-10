package qsbridge

import "testing"

func TestPlanningServiceListClientCatalogEncodingsReturnsFieldProfiles(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "part",
		Fields: []FieldDefinition{
			{
				Name:  "p_type",
				Type:  DataTypeString,
				Index: IndexStringEnum,
				Encoding: EncodingProfile{
					Kind:         EncodingStringEnum,
					LegacyName:   "StringEnum",
					Multiplicity: MultiplicityScalar,
					Rehydration:  RehydrationProfile{Kind: RehydrationLookup, Store: "dictionary"},
					PredicateCapabilities: PredicateCapabilities{
						PredicateCapabilityEquality,
						PredicateCapabilityPrefix,
					},
					ProjectionCapabilities: ProjectionCapabilities{
						ProjectionCapabilityLookup,
						ProjectionCapabilityOriginalValue,
					},
				},
			},
			{
				Name:         "p_name",
				PhysicalName: "p_name_hash",
				Type:         DataTypeString,
				Index:        IndexBackingString,
				Encoding: EncodingProfile{
					Kind:           EncodingStringLexBSI,
					PrefixLength:   12,
					MaxLength:      0,
					RemainderStore: "kv",
					Search:         SearchProfile{Enabled: true, Mode: "text"},
					Rehydration:    RehydrationProfile{Kind: RehydrationLookup, Store: "kv"},
					PredicateCapabilities: PredicateCapabilities{
						PredicateCapabilityEquality,
						PredicateCapabilityPrefix,
						PredicateCapabilityContains,
					},
				},
			},
		},
	}}}

	exchange := service.ListClientCatalogEncodings(connection, catalog, "quanta", "part")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported encoding metadata", exchange)
	}
	if len(exchange.Encodings) != 2 {
		t.Fatalf("encodings = %#v, want two field encodings", exchange.Encodings)
	}
	first := exchange.Encodings[0]
	if first.Field != "p_type" || first.Encoding != EncodingStringEnum || first.Multiplicity != MultiplicityScalar {
		t.Fatalf("first encoding = %#v, want string enum scalar field", first)
	}
	if !first.PredicateCapabilities.Has(PredicateCapabilityPrefix) || first.RehydrationStore != "dictionary" {
		t.Fatalf("first encoding = %#v, want prefix and dictionary metadata", first)
	}
	second := exchange.Encodings[1]
	if second.Field != "p_name" || second.Encoding != EncodingStringLexBSI || !second.Searchable {
		t.Fatalf("second encoding = %#v, want searchable StringLexBSI field", second)
	}
	if second.PrefixLength != 12 || second.RemainderStore != "kv" {
		t.Fatalf("second encoding = %#v, want prefix and remainder store metadata", second)
	}
	if len(exchange.ResultSchema.Columns) != 21 || exchange.ResultSchema.Columns[0].Name != "Schema" || exchange.Result.RowsReturned != 2 {
		t.Fatalf("result/schema = %#v/%#v, want encoding result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[2].Value != "p_type" || resultRow[6].Value != string(EncodingStringEnum) {
		t.Fatalf("result row = %#v, want p_type StringEnum row", resultRow)
	}
}

func TestPlanningServiceListClientCatalogEncodingsUsesSelectedSchema(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = "quanta"
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "orders",
		Fields: []FieldDefinition{{Name: "o_orderdate", Type: DataTypeTime, Encoding: LegacyEncodingProfile("SysMillisBSI", LegacyEncodingOptions{})}},
	}}}

	exchange := service.ListClientCatalogEncodings(connection, catalog, "", "orders")
	if !exchange.Supported() || exchange.Schema != "quanta" || len(exchange.Encodings) != 1 {
		t.Fatalf("exchange = %#v, want selected schema encoding metadata", exchange)
	}
	if exchange.Encodings[0].TimeGranularity != TimeGranularityMillisecond {
		t.Fatalf("encoding = %#v, want millisecond time granularity", exchange.Encodings[0])
	}
}

func TestPlanningServiceListClientCatalogEncodingsReportsMissingInputs(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = ""

	missingSchema := service.ListClientCatalogEncodings(connection, MemoryCatalog{}, "", "orders")
	if missingSchema.Supported() || !containsDiagnosticCode(missingSchema.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing schema = %#v, want invalid execution option", missingSchema)
	}
	if missingSchema.Result.Status != ExecutionFailed || !missingSchema.Result.Complete || len(missingSchema.ResultSchema.Columns) != 21 {
		t.Fatalf("missing schema result/schema = %#v/%#v, want failed encoding envelope", missingSchema.Result, missingSchema.ResultSchema)
	}
	missingTable := service.ListClientCatalogEncodings(connection, MemoryCatalog{}, "quanta", "")
	if missingTable.Supported() || !containsDiagnosticCode(missingTable.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing table = %#v, want invalid execution option", missingTable)
	}
}

func TestPlanningServiceListClientCatalogEncodingsCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "part",
		Fields: []FieldDefinition{{
			Name: "p_type",
			Encoding: EncodingProfile{
				Kind:                  EncodingStringEnum,
				PredicateCapabilities: PredicateCapabilities{PredicateCapabilityEquality},
			},
		}},
	}}}

	exchange := service.ListClientCatalogEncodings(connection, catalog, "quanta", "part")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Encodings[0].Field = "mutated"
	exchange.Encodings[0].PredicateCapabilities[0] = PredicateCapabilityRange
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][2].Value = "mutated"

	again := service.ListClientCatalogEncodings(connection, catalog, "quanta", "part")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Encodings[0].Field != "p_type" || !again.Encodings[0].PredicateCapabilities.Has(PredicateCapabilityEquality) {
		t.Fatalf("encoding metadata leaked mutation: %#v", again.Encodings[0])
	}
	if again.Result.Columns[0].Name != "Schema" || again.ResultSchema.Columns[0].Name != "Schema" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][2].Value != "p_type" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

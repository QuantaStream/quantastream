package qsbridge

import "testing"

func TestPlanningServiceListClientCatalogDictionariesReturnsDictionaryMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "part",
		Fields: []FieldDefinition{
			{
				Name: "p_type",
				Type: DataTypeString,
				Dictionary: DictionaryDefinition{
					Version:     "v7",
					Cardinality: 310,
					UpdateMode:  DictionaryUpdateStatic,
					Capabilities: DictionaryCapabilities{
						DictionaryCapabilityStableIDs,
						DictionaryCapabilityPrefixMatch,
					},
				},
			},
			{
				Name: "p_name",
				Type: DataTypeString,
			},
		},
	}}}

	exchange := service.ListClientCatalogDictionaries(connection, catalog, "quanta", "part")
	if !exchange.Supported() {
		t.Fatalf("exchange = %#v, want supported dictionary metadata", exchange)
	}
	if len(exchange.Dictionaries) != 1 {
		t.Fatalf("dictionaries = %#v, want one dictionary-backed field", exchange.Dictionaries)
	}
	dictionary := exchange.Dictionaries[0]
	if dictionary.Field != "p_type" || dictionary.Dictionary != "quanta.part.p_type" {
		t.Fatalf("dictionary = %#v, want synthesized p_type dictionary ref", dictionary)
	}
	if dictionary.Version != "v7" || dictionary.Cardinality != 310 {
		t.Fatalf("dictionary = %#v, want version and cardinality metadata", dictionary)
	}
	if dictionary.UpdateMode != DictionaryUpdateStatic || dictionary.Consistency != DictionaryConsistencySnapshot {
		t.Fatalf("dictionary = %#v, want static snapshot metadata", dictionary)
	}
	if dictionary.RequiresInvalidation {
		t.Fatalf("dictionary = %#v, static snapshot should not require invalidation", dictionary)
	}
	if !dictionary.Capabilities.Has(DictionaryCapabilityPrefixMatch) || dictionary.Capabilities.Has(DictionaryCapabilityContainsMatch) {
		t.Fatalf("dictionary = %#v, want prefix capability only", dictionary)
	}
	if len(exchange.ResultSchema.Columns) != 14 || exchange.ResultSchema.Columns[0].Name != "Schema" || exchange.Result.RowsReturned != 1 {
		t.Fatalf("result/schema = %#v/%#v, want dictionary result", exchange.Result, exchange.ResultSchema)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[2].Value != "p_type" || resultRow[3].Value != "quanta.part.p_type" || resultRow[7].Value != true ||
		resultRow[11].Value != string(DictionaryUpdateStatic) || resultRow[12].Value != string(DictionaryConsistencySnapshot) ||
		resultRow[13].Value != false {
		t.Fatalf("result row = %#v, want p_type dictionary flags", resultRow)
	}
}

func TestPlanningServiceListClientCatalogDictionariesReportsMutableVersionedMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "stream_spots",
		Fields: []FieldDefinition{{
			Name: "spotter",
			Dictionary: DictionaryDefinition{
				Version: "spotter-v42",
				Capabilities: DictionaryCapabilities{
					DictionaryCapabilityStableIDs,
					DictionaryCapabilityMutable,
				},
			},
		}},
	}}}

	exchange := service.ListClientCatalogDictionaries(connection, catalog, "quanta", "stream_spots")
	if !exchange.Supported() || len(exchange.Dictionaries) != 1 {
		t.Fatalf("exchange = %#v, want mutable dictionary metadata", exchange)
	}
	dictionary := exchange.Dictionaries[0]
	if dictionary.UpdateMode != DictionaryUpdateAppendOnly || dictionary.Consistency != DictionaryConsistencyVersionedDistributed {
		t.Fatalf("dictionary = %#v, want append-only versioned metadata", dictionary)
	}
	if !dictionary.RequiresInvalidation {
		t.Fatalf("dictionary = %#v, want invalidation requirement", dictionary)
	}
	resultRow := exchange.Result.Chunks[0].Rows[0]
	if resultRow[10].Value != true || resultRow[11].Value != string(DictionaryUpdateAppendOnly) ||
		resultRow[12].Value != string(DictionaryConsistencyVersionedDistributed) || resultRow[13].Value != true {
		t.Fatalf("result row = %#v, want mutable dictionary flags", resultRow)
	}
}

func TestPlanningServiceListClientCatalogDictionariesUsesExplicitRefAndSelectedSchema(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = "quanta"
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "lineitem",
		Fields: []FieldDefinition{{
			Name: "l_shipmode",
			Dictionary: DictionaryDefinition{
				Ref:          DictionaryRef{Schema: "shared", Table: "lineitem_dictionary", Field: "shipmode"},
				Version:      "shipmode-v1",
				Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs},
			},
		}},
	}}}

	exchange := service.ListClientCatalogDictionaries(connection, catalog, "", "lineitem")
	if !exchange.Supported() || exchange.Schema != "quanta" || len(exchange.Dictionaries) != 1 {
		t.Fatalf("exchange = %#v, want selected schema dictionary metadata", exchange)
	}
	if exchange.Dictionaries[0].Dictionary != "shared.lineitem_dictionary.shipmode" {
		t.Fatalf("dictionary = %#v, want explicit dictionary ref", exchange.Dictionaries[0])
	}
}

func TestPlanningServiceListClientCatalogDictionariesReportsMissingInputs(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Session.CurrentSchema = ""

	missingSchema := service.ListClientCatalogDictionaries(connection, MemoryCatalog{}, "", "orders")
	if missingSchema.Supported() || !containsDiagnosticCode(missingSchema.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing schema = %#v, want invalid execution option", missingSchema)
	}
	if missingSchema.Result.Status != ExecutionFailed || !missingSchema.Result.Complete || len(missingSchema.ResultSchema.Columns) != 14 {
		t.Fatalf("missing schema result/schema = %#v/%#v, want failed dictionary envelope", missingSchema.Result, missingSchema.ResultSchema)
	}
	missingTable := service.ListClientCatalogDictionaries(connection, MemoryCatalog{}, "quanta", "")
	if missingTable.Supported() || !containsDiagnosticCode(missingTable.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("missing table = %#v, want invalid execution option", missingTable)
	}
}

func TestPlanningServiceListClientCatalogDictionariesCopiesMutableMetadata(t *testing.T) {
	service := NewPlanningService(Planner{}, nil)
	connection := clientStatementConnection()
	connection.Attributes = map[string]string{"client": "mysql"}
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "part",
		Fields: []FieldDefinition{{
			Name: "p_type",
			Dictionary: DictionaryDefinition{
				Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs},
			},
		}},
	}}}

	exchange := service.ListClientCatalogDictionaries(connection, catalog, "quanta", "part")
	exchange.Connection.Attributes["client"] = "mutated"
	exchange.Dictionaries[0].Field = "mutated"
	exchange.Dictionaries[0].Capabilities[0] = DictionaryCapabilityMutable
	exchange.Result.Columns[0].Name = "mutated"
	exchange.ResultSchema.Columns[0].Name = "mutated"
	exchange.Result.Chunks[0].Rows[0][2].Value = "mutated"

	again := service.ListClientCatalogDictionaries(connection, catalog, "quanta", "part")
	if again.Connection.Attributes["client"] != "mysql" {
		t.Fatalf("connection attributes leaked mutation: %#v", again.Connection.Attributes)
	}
	if again.Dictionaries[0].Field != "p_type" || !again.Dictionaries[0].Capabilities.Has(DictionaryCapabilityStableIDs) {
		t.Fatalf("dictionary metadata leaked mutation: %#v", again.Dictionaries[0])
	}
	if again.Result.Columns[0].Name != "Schema" || again.ResultSchema.Columns[0].Name != "Schema" {
		t.Fatalf("result metadata leaked mutation: %#v/%#v", again.Result.Columns, again.ResultSchema.Columns)
	}
	if again.Result.Chunks[0].Rows[0][2].Value != "p_type" {
		t.Fatalf("result rows leaked mutation: %#v", again.Result.Chunks)
	}
}

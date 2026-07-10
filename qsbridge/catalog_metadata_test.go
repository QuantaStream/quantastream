package qsbridge

import "testing"

func TestMemoryCatalogMetadataListsSchemasTablesAndColumns(t *testing.T) {
	catalog := MemoryCatalog{
		Schemas: []CatalogSchemaDefinition{{Name: "empty"}, {Name: "quanta"}},
		Tables: []TableDefinition{
			{Schema: "analytics", Name: "events", Fields: []FieldDefinition{{Name: "event_id", Type: DataTypeInt}}},
			{Schema: "quanta", Name: "orders", Fields: []FieldDefinition{{Name: "o_orderkey", Type: DataTypeInt}}},
			{Schema: "quanta", Name: "lineitem", Fields: []FieldDefinition{{Name: "l_orderkey", Type: DataTypeInt}}},
		},
	}

	schemas, diagnostics := catalog.ListSchemas()
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected schema diagnostics: %#v", diagnostics)
	}
	if len(schemas) != 3 || schemas[0].Name != "analytics" || schemas[1].Name != "empty" || schemas[2].Name != "quanta" {
		t.Fatalf("schemas = %#v, want sorted explicit and table-derived schemas", schemas)
	}

	tables, diagnostics := catalog.ListTables("quanta")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected table diagnostics: %#v", diagnostics)
	}
	if len(tables) != 2 || tables[0].Name != "lineitem" || tables[1].Name != "orders" {
		t.Fatalf("tables = %#v, want sorted quanta tables", tables)
	}
	tables, diagnostics = catalog.ListTables("empty")
	if diagnostics.BlocksNative() || len(tables) != 0 {
		t.Fatalf("empty schema tables = %#v diagnostics = %#v, want supported empty table list", tables, diagnostics)
	}

	columns, diagnostics := catalog.ListColumns("quanta", "orders")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected column diagnostics: %#v", diagnostics)
	}
	if len(columns) != 1 || columns[0].Name != "o_orderkey" {
		t.Fatalf("columns = %#v, want order key", columns)
	}
}

func TestMemoryCatalogMetadataListsFunctions(t *testing.T) {
	catalog := MemoryCatalog{Functions: []FunctionDefinition{
		{Name: "substr", Kind: FunctionScalar, Arguments: []DataType{DataTypeString, DataTypeInt}, ReturnType: DataTypeString, Aliases: []string{"substring"}, Native: true},
		{Name: "sum", Kind: FunctionAggregate, Arguments: []DataType{DataTypeFloat}, ReturnType: DataTypeFloat, Native: true},
	}}

	functions, diagnostics := catalog.ListFunctions()
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected function diagnostics: %#v", diagnostics)
	}
	if len(functions) != 2 || functions[0].Name != "substr" || functions[1].Name != "sum" {
		t.Fatalf("functions = %#v, want sorted function metadata", functions)
	}
	functions[0].Aliases[0] = "mutated"
	functions, diagnostics = catalog.ListFunctions()
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected function diagnostics after mutation: %#v", diagnostics)
	}
	if functions[0].Aliases[0] != "substring" {
		t.Fatalf("function metadata leaked mutation: %#v", functions)
	}
}

func TestMemoryCatalogMetadataReportsMissingSchemaOrTable(t *testing.T) {
	catalog := MemoryCatalog{}
	_, diagnostics := catalog.ListTables("missing")
	if !containsDiagnosticCode(diagnostics.Codes(), DiagnosticCatalogSchemaNotFound) {
		t.Fatalf("diagnostics = %#v, want schema not found", diagnostics)
	}
	_, diagnostics = catalog.ListColumns("quanta", "missing")
	if !containsDiagnosticCode(diagnostics.Codes(), DiagnosticCatalogTableNotFound) {
		t.Fatalf("diagnostics = %#v, want table not found", diagnostics)
	}
}

func TestCatalogMetadataCopiesMutableDefinitions(t *testing.T) {
	catalog := MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "orders",
		Fields: []FieldDefinition{{
			Name: "o_orderkey",
			Dictionary: DictionaryDefinition{
				Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs},
			},
		}},
	}}}

	tables, diagnostics := catalog.ListTables("quanta")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected table diagnostics: %#v", diagnostics)
	}
	tables[0].Fields[0].Name = "mutated"
	tables[0].Fields[0].Dictionary.Capabilities[0] = DictionaryCapabilityPrefixMatch

	columns, diagnostics := catalog.ListColumns("quanta", "orders")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected column diagnostics: %#v", diagnostics)
	}
	if columns[0].Name != "o_orderkey" || columns[0].Dictionary.Capabilities[0] != DictionaryCapabilityStableIDs {
		t.Fatalf("catalog metadata leaked mutation: %#v", columns)
	}
}

func TestCachedCatalogDelegatesMetadataEnumeration(t *testing.T) {
	catalog := NewCachedCatalog(MemoryCatalog{Tables: []TableDefinition{{
		Schema: "quanta",
		Name:   "orders",
		Fields: []FieldDefinition{{Name: "o_orderkey"}},
	}}})

	schemas, diagnostics := catalog.ListSchemas()
	if diagnostics.BlocksNative() || len(schemas) != 1 || schemas[0].Name != "quanta" {
		t.Fatalf("schemas = %#v diagnostics = %#v, want delegated schema", schemas, diagnostics)
	}
	tables, diagnostics := catalog.ListTables("quanta")
	if diagnostics.BlocksNative() || len(tables) != 1 || tables[0].Name != "orders" {
		t.Fatalf("tables = %#v diagnostics = %#v, want delegated table", tables, diagnostics)
	}
	columns, diagnostics := catalog.ListColumns("quanta", "orders")
	if diagnostics.BlocksNative() || len(columns) != 1 || columns[0].Name != "o_orderkey" {
		t.Fatalf("columns = %#v diagnostics = %#v, want delegated column", columns, diagnostics)
	}
}

func TestCachedCatalogDelegatesFunctionEnumeration(t *testing.T) {
	catalog := NewCachedCatalog(MemoryCatalog{Functions: []FunctionDefinition{{
		Name:    "lower",
		Aliases: []string{"lcase"},
		Native:  true,
	}}})

	functions, diagnostics := catalog.ListFunctions()
	if diagnostics.BlocksNative() || len(functions) != 1 || functions[0].Name != "lower" {
		t.Fatalf("functions = %#v diagnostics = %#v, want delegated function", functions, diagnostics)
	}
	functions[0].Aliases[0] = "mutated"
	functions, diagnostics = catalog.ListFunctions()
	if diagnostics.BlocksNative() || functions[0].Aliases[0] != "lcase" {
		t.Fatalf("functions = %#v diagnostics = %#v, want copied function metadata", functions, diagnostics)
	}
}

func TestCachedCatalogReportsUnsupportedMetadataBackend(t *testing.T) {
	catalog := NewCachedCatalog(&countingCatalog{})
	_, diagnostics := catalog.ListSchemas()
	if !containsDiagnosticCode(diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", diagnostics)
	}
}

package qsbridge

import "testing"

func TestCachedCatalogCachesTableRelationshipAndFunctionLookups(t *testing.T) {
	backend := &countingCatalog{
		table: TableDefinition{
			Schema: "quanta",
			Name:   "part",
			Fields: []FieldDefinition{{Name: "p_partkey", Type: DataTypeInt}},
		},
		relationship: RelationshipDefinition{Name: "part_partsupp"},
		function:     FunctionDefinition{Name: "lower", Aliases: []string{"lcase"}, ReturnType: DataTypeString},
	}
	catalog := NewCachedCatalog(backend)

	for i := 0; i < 2; i++ {
		if _, diagnostics := catalog.Table("quanta", "part"); diagnostics.BlocksNative() {
			t.Fatalf("unexpected table diagnostics: %#v", diagnostics)
		}
		if _, diagnostics := catalog.Relationship("part_partsupp"); diagnostics.BlocksNative() {
			t.Fatalf("unexpected relationship diagnostics: %#v", diagnostics)
		}
		if _, diagnostics := catalog.Function("lower"); diagnostics.BlocksNative() {
			t.Fatalf("unexpected function diagnostics: %#v", diagnostics)
		}
	}

	if backend.tableCalls != 1 {
		t.Fatalf("table calls = %d, want 1", backend.tableCalls)
	}
	if backend.relationshipCalls != 1 {
		t.Fatalf("relationship calls = %d, want 1", backend.relationshipCalls)
	}
	if backend.functionCalls != 1 {
		t.Fatalf("function calls = %d, want 1", backend.functionCalls)
	}
}

func TestCachedCatalogCachesDiagnosticsUntilInvalidated(t *testing.T) {
	backend := &countingCatalog{
		tableDiagnostics: DiagnosticSet{ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "table missing")},
	}
	catalog := NewCachedCatalog(backend)

	for i := 0; i < 2; i++ {
		_, diagnostics := catalog.Table("quanta", "missing")
		assertSingleDiagnosticCode(t, diagnostics, DiagnosticCatalogTableNotFound)
	}
	if backend.tableCalls != 1 {
		t.Fatalf("table calls = %d, want 1 cached miss", backend.tableCalls)
	}

	backend.table = TableDefinition{Schema: "quanta", Name: "missing"}
	backend.tableDiagnostics = nil
	catalog.InvalidateTable("quanta", "missing")
	table, diagnostics := catalog.Table("quanta", "missing")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics after invalidation: %#v", diagnostics)
	}
	if table.Name != "missing" {
		t.Fatalf("table name = %q, want missing", table.Name)
	}
	if backend.tableCalls != 2 {
		t.Fatalf("table calls = %d, want refresh after invalidation", backend.tableCalls)
	}
}

func TestCachedCatalogClearRefreshesAllMetadata(t *testing.T) {
	backend := &countingCatalog{
		table:        TableDefinition{Name: "part"},
		relationship: RelationshipDefinition{Name: "part_partsupp"},
		function:     FunctionDefinition{Name: "lower"},
	}
	catalog := NewCachedCatalog(backend)

	_, _ = catalog.Table("", "part")
	_, _ = catalog.Relationship("part_partsupp")
	_, _ = catalog.Function("lower")
	catalog.Clear()
	_, _ = catalog.Table("", "part")
	_, _ = catalog.Relationship("part_partsupp")
	_, _ = catalog.Function("lower")

	if backend.tableCalls != 2 || backend.relationshipCalls != 2 || backend.functionCalls != 2 {
		t.Fatalf(
			"calls = table:%d relationship:%d function:%d, want all refreshed",
			backend.tableCalls,
			backend.relationshipCalls,
			backend.functionCalls,
		)
	}
}

func TestCachedCatalogReturnsCopies(t *testing.T) {
	backend := &countingCatalog{
		table: TableDefinition{
			Name: "part",
			Fields: []FieldDefinition{{
				Name: "p_type",
				Dictionary: DictionaryDefinition{
					Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs},
				},
			}},
		},
		function: FunctionDefinition{Name: "substr", Aliases: []string{"substring"}},
	}
	catalog := NewCachedCatalog(backend)

	table, _ := catalog.Table("", "part")
	table.Fields[0].Name = "mutated"
	table.Fields[0].Dictionary.Capabilities[0] = DictionaryCapabilityMutable
	table, _ = catalog.Table("", "part")
	if table.Fields[0].Name != "p_type" {
		t.Fatalf("cached table mutation leaked: %#v", table.Fields)
	}
	if table.Fields[0].Dictionary.Capabilities[0] != DictionaryCapabilityStableIDs {
		t.Fatalf("cached dictionary mutation leaked: %#v", table.Fields[0].Dictionary.Capabilities)
	}

	function, _ := catalog.Function("substr")
	function.Aliases[0] = "mutated"
	function, _ = catalog.Function("substr")
	if function.Aliases[0] != "substring" {
		t.Fatalf("cached function mutation leaked: %#v", function.Aliases)
	}
}

type countingCatalog struct {
	table                   TableDefinition
	tableDiagnostics        DiagnosticSet
	tableCalls              int
	relationship            RelationshipDefinition
	relationshipDiagnostics DiagnosticSet
	relationshipCalls       int
	function                FunctionDefinition
	functionDiagnostics     DiagnosticSet
	functionCalls           int
}

func (c *countingCatalog) Table(string, string) (TableDefinition, DiagnosticSet) {
	c.tableCalls++
	return c.table, c.tableDiagnostics
}

func (c *countingCatalog) Relationship(string) (RelationshipDefinition, DiagnosticSet) {
	c.relationshipCalls++
	return c.relationship, c.relationshipDiagnostics
}

func (c *countingCatalog) Function(string) (FunctionDefinition, DiagnosticSet) {
	c.functionCalls++
	return c.function, c.functionDiagnostics
}

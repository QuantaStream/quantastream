package qsbridge

import "testing"

func TestTableDefinitionFieldLookupByLogicalAndPhysicalName(t *testing.T) {
	table := TableDefinition{
		Name: "lineitem",
		Fields: []FieldDefinition{
			{Name: "l_shipdate", PhysicalName: "shipdate_bsi", Type: DataTypeTime, Index: IndexDateTime},
		},
	}

	field, ok := table.Field("l_shipdate")
	if !ok {
		t.Fatalf("expected field lookup by logical name")
	}
	if field.PhysicalName != "shipdate_bsi" {
		t.Fatalf("PhysicalName = %q, want shipdate_bsi", field.PhysicalName)
	}

	field, ok = table.Field("shipdate_bsi")
	if !ok {
		t.Fatalf("expected field lookup by physical name")
	}
	if field.Name != "l_shipdate" {
		t.Fatalf("Name = %q, want l_shipdate", field.Name)
	}
}

func TestFieldDefinitionRefBindsTableAndRoles(t *testing.T) {
	table := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	field := FieldDefinition{
		Name:         "o_orderkey",
		PhysicalName: "orderkey",
		Type:         DataTypeInt,
		Index:        IndexBSI,
		Dictionary: DictionaryDefinition{
			Ref:          DictionaryRef{Table: "orders", Field: "o_orderkey"},
			Version:      "v1",
			Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs},
		},
	}

	ref := field.Ref(table, FieldRoleJoinInput|FieldRoleHidden)
	if got, want := ref.QualifiedName(), "o.o_orderkey"; got != want {
		t.Fatalf("QualifiedName() = %q, want %q", got, want)
	}
	if !ref.Roles.Has(FieldRoleJoinInput | FieldRoleHidden) {
		t.Fatalf("expected join and hidden roles")
	}
	if ref.Type != DataTypeInt {
		t.Fatalf("Type = %q, want %q", ref.Type, DataTypeInt)
	}
	if ref.PhysicalName != "orderkey" {
		t.Fatalf("PhysicalName = %q, want orderkey", ref.PhysicalName)
	}
	if ref.Dictionary.Ref.Field != "o_orderkey" {
		t.Fatalf("Dictionary.Ref.Field = %q, want o_orderkey", ref.Dictionary.Ref.Field)
	}
	if !ref.Dictionary.Supports(DictionaryCapabilityStableIDs) {
		t.Fatalf("expected dictionary capability to bind into field ref")
	}
}

func TestTableDefinitionInstance(t *testing.T) {
	table := TableDefinition{Schema: "quanta", Name: "part"}
	instance := table.Instance("part_1", "p")

	if instance.ID != "part_1" || instance.Schema != "quanta" || instance.Table != "part" || instance.Alias != "p" {
		t.Fatalf("unexpected table instance: %#v", instance)
	}
}

func TestRelationshipDefinitionEdge(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	relationship := RelationshipDefinition{
		Name:        "lineitem_orders",
		Direction:   JoinChildToParent,
		Cardinality: "many_to_one",
		Encoding: RelationshipEncodingProfile{
			Kind:       RelationshipEncodingVector,
			LegacyName: "ParentRelation",
			Capabilities: RelationshipCapabilities{
				RelationshipCapabilityParentLookup,
				RelationshipCapabilityJoinReduction,
			},
		},
	}

	edge := relationship.Edge(
		FieldRef{Table: lineitem, Name: "l_orderkey"},
		FieldRef{Table: orders, Name: "o_orderkey"},
	)
	if !edge.Supported() {
		t.Fatalf("expected catalog relationship edge to be legal")
	}
	if edge.Direction != JoinChildToParent {
		t.Fatalf("Direction = %q, want %q", edge.Direction, JoinChildToParent)
	}
	if !relationship.Encoding.Supports(RelationshipCapabilityJoinReduction) {
		t.Fatalf("expected relationship encoding to advertise join reduction")
	}
	if !edge.Encoding.Supports(RelationshipCapabilityJoinReduction) {
		t.Fatalf("expected edge to carry relationship encoding")
	}
}

func TestFunctionDefinitionMatchesAliases(t *testing.T) {
	function := FunctionDefinition{Name: "substr", Aliases: []string{"substring", "mid"}, Native: true}

	for _, name := range []string{"substr", "SUBSTRING", "mid"} {
		if !function.Matches(name) {
			t.Fatalf("expected %q to match function", name)
		}
	}
	if function.Matches("lower") {
		t.Fatalf("did not expect lower to match substr")
	}
}

func TestFunctionDefinitionEffectivePlacementDefaultsFromKind(t *testing.T) {
	tests := []struct {
		name     string
		function FunctionDefinition
		want     FunctionPlacement
	}{
		{name: "scalar", function: FunctionDefinition{Kind: FunctionScalar}, want: FunctionPlacementExpression},
		{name: "aggregate", function: FunctionDefinition{Kind: FunctionAggregate}, want: FunctionPlacementAggregate},
		{name: "table", function: FunctionDefinition{Kind: FunctionTable}, want: FunctionPlacementTable},
		{name: "explicit predicate", function: FunctionDefinition{Kind: FunctionScalar, Placement: FunctionPlacementPredicate}, want: FunctionPlacementPredicate},
		{name: "unknown", function: FunctionDefinition{}, want: FunctionPlacementUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.function.EffectivePlacement(); got != tt.want {
				t.Fatalf("EffectivePlacement() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMemoryCatalogLookups(t *testing.T) {
	catalog := MemoryCatalog{
		Tables: []TableDefinition{{
			Schema: "quanta",
			Name:   "part",
			Fields: []FieldDefinition{{Name: "p_partkey", Type: DataTypeInt}},
		}},
		Relationships: []RelationshipDefinition{{Name: "part_partsupp"}},
		Functions:     []FunctionDefinition{{Name: "lower", Aliases: []string{"lcase"}, ReturnType: DataTypeString}},
	}

	table, diagnostics := catalog.Table("quanta", "part")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected table diagnostics: %#v", diagnostics)
	}
	if table.Name != "part" {
		t.Fatalf("table name = %q, want part", table.Name)
	}

	relationship, diagnostics := catalog.Relationship("part_partsupp")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected relationship diagnostics: %#v", diagnostics)
	}
	if relationship.Name != "part_partsupp" {
		t.Fatalf("relationship name = %q, want part_partsupp", relationship.Name)
	}

	function, diagnostics := catalog.Function("LCASE")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected function diagnostics: %#v", diagnostics)
	}
	if function.Name != "lower" {
		t.Fatalf("function name = %q, want lower", function.Name)
	}
}

func TestMemoryCatalogRelationshipLookupClonesEncodingCapabilities(t *testing.T) {
	catalog := MemoryCatalog{
		Relationships: []RelationshipDefinition{{
			Name: "partsupp_supplier",
			Encoding: RelationshipEncodingProfile{
				Kind: RelationshipEncodingVector,
				Capabilities: RelationshipCapabilities{
					RelationshipCapabilityParentLookup,
					RelationshipCapabilityAntiJoinDifference,
				},
			},
		}},
	}

	relationship, diagnostics := catalog.Relationship("partsupp_supplier")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected relationship diagnostics: %#v", diagnostics)
	}
	if !relationship.Encoding.SupportsAntiJoinDifference() {
		t.Fatalf("expected anti-join difference capability")
	}

	relationship.Encoding.Capabilities[0] = RelationshipCapabilityNullExtension
	relationship, diagnostics = catalog.Relationship("partsupp_supplier")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected relationship diagnostics after mutation: %#v", diagnostics)
	}
	if !relationship.Encoding.Supports(RelationshipCapabilityParentLookup) {
		t.Fatalf("catalog lookup leaked mutable relationship capabilities")
	}
}

func TestMemoryCatalogMissingLookupsReturnDiagnostics(t *testing.T) {
	catalog := MemoryCatalog{}

	_, diagnostics := catalog.Table("quanta", "missing")
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticCatalogTableNotFound)

	_, diagnostics = catalog.Relationship("missing_relationship")
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticCatalogRelationshipNotFound)

	_, diagnostics = catalog.Function("missing_function")
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticCatalogFunctionNotFound)
}

func assertSingleDiagnosticCode(t *testing.T, diagnostics DiagnosticSet, code DiagnosticCode) {
	t.Helper()
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected blocking diagnostic")
	}
	codes := diagnostics.Codes()
	if len(codes) != 1 || codes[0] != code {
		t.Fatalf("codes = %#v, want [%q]", codes, code)
	}
}

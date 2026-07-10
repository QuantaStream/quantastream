package qsbridge

import "testing"

func TestNodeCatalogViewKeepsOnlyPhysicalMetadata(t *testing.T) {
	table := catalogViewTestTable()

	view := NewNodeCatalogView([]TableDefinition{table})

	if len(view.Tables) != 1 {
		t.Fatalf("tables = %d, want 1", len(view.Tables))
	}
	nodeTable := view.Tables[0]
	if nodeTable.Name != "orders" || !nodeTable.Storage.Partitioned {
		t.Fatalf("node table = %#v, want physical orders metadata", nodeTable)
	}
	if len(nodeTable.Fields) != 2 {
		t.Fatalf("node fields = %#v, want two fields", nodeTable.Fields)
	}
	priority := nodeTable.Fields[1]
	if priority.Name != "o_orderpriority" || priority.Encoding.Kind != EncodingStringEnum ||
		priority.Encoding.Rehydration.Store != "dictionary" {
		t.Fatalf("priority node field = %#v, want physical StringEnum encoding metadata", priority)
	}
	if len(view.Relationships) != 1 || view.Relationships[0].Encoding.Kind != RelationshipEncodingVector {
		t.Fatalf("relationships = %#v, want physical relationship vector metadata", view.Relationships)
	}
}

func TestQueryCatalogViewKeepsSemanticMetadata(t *testing.T) {
	table := catalogViewTestTable()
	functions := []FunctionDefinition{{Name: "substr", Origin: FunctionOriginMySQLCompatible}}

	view := NewQueryCatalogView([]TableDefinition{table}, nil, functions)

	if len(view.Tables) != 1 {
		t.Fatalf("tables = %d, want 1", len(view.Tables))
	}
	queryTable := view.Tables[0]
	priority := queryTable.Fields[1]
	if priority.Dictionary.Ref.Table != "orders" || priority.Dictionary.Cardinality != 5 ||
		!priority.Dictionary.Supports(DictionaryCapabilityStableIDs) {
		t.Fatalf("priority dictionary = %#v, want query-facing semantic dictionary metadata", priority.Dictionary)
	}
	if len(queryTable.Relationships) != 1 || queryTable.Relationships[0].Name != "orders.customer" {
		t.Fatalf("query relationships = %#v, want semantic relationship definitions", queryTable.Relationships)
	}
	if len(view.Functions) != 1 || view.Functions[0].Name != "substr" {
		t.Fatalf("functions = %#v, want copied function metadata", view.Functions)
	}
}

func TestCatalogViewsDoNotAliasMutableEncodingOrDictionarySlices(t *testing.T) {
	table := catalogViewTestTable()

	nodeView := NewNodeCatalogView([]TableDefinition{table})
	queryView := NewQueryCatalogView([]TableDefinition{table}, nil, nil)

	nodeView.Tables[0].Fields[1].Encoding.PredicateCapabilities[0] = PredicateCapabilityRange
	queryView.Tables[0].Fields[1].Dictionary.Capabilities[0] = DictionaryCapabilityPrefixMatch

	if table.Fields[1].Encoding.PredicateCapabilities[0] != PredicateCapabilityEquality {
		t.Fatalf("node view mutation leaked into source encoding: %#v", table.Fields[1].Encoding.PredicateCapabilities)
	}
	if table.Fields[1].Dictionary.Capabilities[0] != DictionaryCapabilityStableIDs {
		t.Fatalf("query view mutation leaked into source dictionary: %#v", table.Fields[1].Dictionary.Capabilities)
	}
}

func catalogViewTestTable() TableDefinition {
	return TableDefinition{
		Schema: "quanta",
		Name:   "orders",
		Storage: StorageProfile{
			Engine:      "legacy_quanta",
			Partitioned: true,
		},
		Fields: []FieldDefinition{
			{
				Name:       "o_orderkey",
				Type:       DataTypeInt,
				Index:      IndexBSI,
				PrimaryKey: true,
				Encoding:   NewNumericBSIProfile(0, true),
			},
			{
				Name:  "o_orderpriority",
				Type:  DataTypeString,
				Index: IndexStringEnum,
				Encoding: EncodingProfile{
					Kind:        EncodingStringEnum,
					Rehydration: RehydrationProfile{Kind: RehydrationLookup, Store: "dictionary"},
					PredicateCapabilities: PredicateCapabilities{
						PredicateCapabilityEquality,
						PredicateCapabilityMembership,
					},
				},
				Dictionary: DictionaryDefinition{
					Ref:          DictionaryRef{Schema: "quanta", Table: "orders", Field: "o_orderpriority"},
					Cardinality:  5,
					Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs},
				},
			},
		},
		Relationships: []RelationshipDefinition{{
			Name:      "orders.customer",
			FromTable: "orders",
			FromField: "o_custkey",
			ToTable:   "customer",
			ToField:   "c_custkey",
			Direction: JoinChildToParent,
			Encoding: RelationshipEncodingProfile{
				Kind: RelationshipEncodingVector,
			},
		}},
	}
}

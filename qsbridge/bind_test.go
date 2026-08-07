package qsbridge

import "testing"

func TestBindContextAddTableUsesDefaultSchemaAndAlias(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")

	table, diagnostics := context.AddTable(UnboundTable{Name: "orders", Alias: "o", Role: "fact"})
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if table.Instance.Schema != "quanta" {
		t.Fatalf("Schema = %q, want quanta", table.Instance.Schema)
	}
	if table.Instance.RefName() != "o" {
		t.Fatalf("RefName() = %q, want o", table.Instance.RefName())
	}
	if table.Instance.Role != "fact" {
		t.Fatalf("Role = %q, want fact", table.Instance.Role)
	}

	sources := context.Sources()
	if len(sources) != 1 || sources[0].RefName() != "o" {
		t.Fatalf("Sources() = %#v, want alias o", sources)
	}
}

func TestBindContextRejectsDuplicateTableReference(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	if _, diagnostics := context.AddTable(UnboundTable{Name: "orders", Alias: "o"}); diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}

	_, diagnostics := context.AddTable(UnboundTable{Name: "orders", Alias: "o"})
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticDuplicateTableAlias)
	assertSingleDiagnosticCode(t, context.Diagnostics(), DiagnosticDuplicateTableAlias)
}

func TestBindContextResolveTableByAlias(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	if _, diagnostics := context.AddTable(UnboundTable{Name: "orders", Alias: "o"}); diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}

	table, diagnostics := context.ResolveTable("o")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if table.Instance.Table != "orders" {
		t.Fatalf("table = %q, want orders", table.Instance.Table)
	}

	_, diagnostics = context.ResolveTable("orders")
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticTableAliasNotFound)
}

func TestBindContextResolveUnaliasedTableByBaseName(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	table, diagnostics := context.AddTable(UnboundTable{Name: "orders"})
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if table.Instance.RefName() != "orders" {
		t.Fatalf("RefName() = %q, want orders", table.Instance.RefName())
	}
}

func TestBindContextResolveQualifiedField(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	if _, diagnostics := context.AddTable(UnboundTable{Name: "orders", Alias: "o"}); diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}

	ref, diagnostics := context.ResolveField("o", "o_orderkey", FieldRoleJoinInput)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if got, want := ref.QualifiedName(), "o.o_orderkey"; got != want {
		t.Fatalf("QualifiedName() = %q, want %q", got, want)
	}
	if !ref.Roles.Has(FieldRoleJoinInput) {
		t.Fatalf("expected join input role")
	}
	if ref.Index != IndexBSI {
		t.Fatalf("Index = %q, want %q", ref.Index, IndexBSI)
	}
}

func TestBindContextResolveFieldPreservesEncodingProfile(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	if _, diagnostics := context.AddTable(UnboundTable{Name: "customer", Alias: "c"}); diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}

	ref, diagnostics := context.ResolveField("c", "c_name", FieldRoleVisible)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if ref.Encoding.Kind != EncodingStringLexBSI {
		t.Fatalf("Encoding.Kind = %q, want %q", ref.Encoding.Kind, EncodingStringLexBSI)
	}
	if ref.Encoding.RequiresLookup() {
		t.Fatalf("expected full-inline StringLexBSI projection to avoid lookup")
	}
	if !ref.Encoding.Searchable() {
		t.Fatalf("expected searchable encoding to survive binding")
	}
	if !ref.Encoding.SupportsPrefix() {
		t.Fatalf("expected StringLexBSI to support prefix capability")
	}
}

func TestBindContextResolveRownumPseudoField(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	if _, diagnostics := context.AddTable(UnboundTable{Name: "customer", Alias: "c"}); diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}

	ref, diagnostics := context.ResolveField("", "@rownum", FieldRoleVisible)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if got, want := ref.QualifiedName(), "c.@rownum"; got != want {
		t.Fatalf("QualifiedName() = %q, want %q", got, want)
	}
	if ref.PhysicalName != "@rownum" {
		t.Fatalf("PhysicalName = %q, want @rownum", ref.PhysicalName)
	}
	if ref.Index != IndexBitmap || ref.Type != DataTypeInt {
		t.Fatalf("field metadata = index %q type %q, want bitmap/int", ref.Index, ref.Type)
	}
}

func TestBindPredicateKeepsStringLexRangeAsPushdown(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	if _, diagnostics := context.AddTable(UnboundTable{Name: "customer", Alias: "c"}); diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}

	predicate, diagnostics := BindPredicate(context, UnboundPredicate{
		Expr: UnboundBinary(
			BinaryOpBetween,
			UnboundField("c", "c_name"),
			UnboundList(UnboundLiteral(ValueString, "Customer#000000105"), UnboundLiteral(ValueString, "Customer#000000108")),
		),
		Placement: PredicatePushdown,
		Scope:     PredicateScopeWhere,
	}, FieldRoleResidualInput)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if predicate.Placement != PredicatePushdown {
		t.Fatalf("Placement = %q, want %q", predicate.Placement, PredicatePushdown)
	}
}

func TestBindPredicateKeepsBSIRangeAsPushdown(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	if _, diagnostics := context.AddTable(UnboundTable{Name: "orders", Alias: "o"}); diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}

	predicate, diagnostics := BindPredicate(context, UnboundPredicate{
		Expr: UnboundBinary(
			BinaryOpBetween,
			UnboundField("o", "o_orderkey"),
			UnboundList(UnboundLiteral(ValueInt, int64(100)), UnboundLiteral(ValueInt, int64(200))),
		),
		Placement: PredicatePushdown,
		Scope:     PredicateScopeWhere,
	}, FieldRoleResidualInput)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if predicate.Placement != PredicatePushdown {
		t.Fatalf("Placement = %q, want %q", predicate.Placement, PredicatePushdown)
	}
}

func TestBindContextResolveUnqualifiedUniqueField(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	for _, table := range []UnboundTable{{Name: "orders", Alias: "o"}, {Name: "customer", Alias: "c"}} {
		if _, diagnostics := context.AddTable(table); diagnostics.BlocksNative() {
			t.Fatalf("unexpected diagnostics for %s: %#v", table.Name, diagnostics)
		}
	}

	ref, diagnostics := context.ResolveField("", "o_orderdate", FieldRoleResidualInput)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if got, want := ref.QualifiedName(), "o.o_orderdate"; got != want {
		t.Fatalf("QualifiedName() = %q, want %q", got, want)
	}
}

func TestBindContextResolveUnqualifiedAmbiguousField(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	for _, table := range []UnboundTable{{Name: "orders", Alias: "o"}, {Name: "customer", Alias: "c"}} {
		if _, diagnostics := context.AddTable(table); diagnostics.BlocksNative() {
			t.Fatalf("unexpected diagnostics for %s: %#v", table.Name, diagnostics)
		}
	}

	_, diagnostics := context.ResolveField("", "shared_key", FieldRoleVisible)
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticAmbiguousField)
}

func TestBindContextResolveMissingTableAndField(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	if _, diagnostics := context.AddTable(UnboundTable{Name: "orders", Alias: "o"}); diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}

	_, diagnostics := context.ResolveTable("missing")
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticTableAliasNotFound)

	_, diagnostics = context.ResolveField("o", "missing_field", FieldRoleVisible)
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticCatalogFieldNotFound)
}

func TestBindContextResolveFunctionAlias(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")

	function, diagnostics := context.ResolveFunction("LCASE")
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if function.Name != "lower" {
		t.Fatalf("function name = %q, want lower", function.Name)
	}
}

func TestBindContextResolveMissingFunction(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")

	_, diagnostics := context.ResolveFunction("missing_function")
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticCatalogFunctionNotFound)
}

func TestBindSelectExpandsWildcardProjectionInCatalogOrder(t *testing.T) {
	query, diagnostics := BindSelect(NewBindContext(testBindCatalog(), "quanta"), UnboundSelect{
		Tables: []UnboundTable{{Name: "orders", Alias: "o"}},
		Projection: []UnboundProjection{{
			Expr: UnboundField("", "*"),
		}},
		Result: ResultShape{Kind: ResultQuery},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if len(query.Projection) != 7 {
		t.Fatalf("projections = %d, want all orders fields", len(query.Projection))
	}
	first, ok := query.Projection[0].Expr.(FieldExpr)
	if !ok || first.Ref.QualifiedName() != "o.o_orderkey" {
		t.Fatalf("first projection = %#v, want o.o_orderkey", query.Projection[0])
	}
	last, ok := query.Projection[6].Expr.(FieldExpr)
	if !ok || last.Ref.QualifiedName() != "o.shared_key" {
		t.Fatalf("last projection = %#v, want o.shared_key", query.Projection[6])
	}
}

func testBindCatalog() MemoryCatalog {
	return MemoryCatalog{
		Tables: []TableDefinition{
			{
				Schema: "quanta",
				Name:   "orders",
				Fields: []FieldDefinition{
					{Name: "o_orderkey", Type: DataTypeInt, Index: IndexBSI},
					{Name: "o_orderdate", Type: DataTypeTime, Index: IndexDateTime, Encoding: LegacyEncodingProfile("SysMillisBSI", LegacyEncodingOptions{})},
					{Name: "o_totalprice", Type: DataTypeFloat, Index: IndexBSI, Encoding: LegacyEncodingProfile("FloatScaleBSI", LegacyEncodingOptions{Scale: 2})},
					{Name: "o_discount", Type: DataTypeFloat, Index: IndexBSI, Encoding: LegacyEncodingProfile("FloatScaleBSI", LegacyEncodingOptions{Scale: 2})},
					{Name: "o_confirmed", Type: DataTypeBool, Encoding: LegacyEncodingProfile("BoolDirect", LegacyEncodingOptions{})},
					{Name: "o_custkey", Type: DataTypeInt, Index: IndexBSI},
					{Name: "shared_key", Type: DataTypeInt, Index: IndexBSI},
				},
			},
			{
				Schema: "quanta",
				Name:   "customer",
				Fields: []FieldDefinition{
					{Name: "c_custkey", Type: DataTypeInt, Index: IndexBSI},
					{Name: "c_name", Type: DataTypeString, Index: IndexBSI, Encoding: LegacyEncodingProfile("StringLexBSI", LegacyEncodingOptions{Searchable: true, PrefixLength: 10, MaxLength: 10})},
					{Name: "shared_key", Type: DataTypeInt, Index: IndexBSI},
				},
			},
			{
				Schema: "quanta",
				Name:   "lineitem",
				Fields: []FieldDefinition{
					{Name: "l_commitdate", Type: DataTypeTime, Index: IndexDateTime, Encoding: LegacyEncodingProfile("SysMillisBSI", LegacyEncodingOptions{})},
					{Name: "l_receiptdate", Type: DataTypeTime, Index: IndexDateTime, Encoding: LegacyEncodingProfile("SysMillisBSI", LegacyEncodingOptions{})},
					{
						Name:     "l_shipmode",
						Type:     DataTypeString,
						Index:    IndexStringEnum,
						Encoding: LegacyEncodingProfile("StringEnum", LegacyEncodingOptions{}),
						Dictionary: DictionaryDefinition{
							Ref:          DictionaryRef{Schema: "quanta", Table: "lineitem", Field: "l_shipmode"},
							Version:      "v1",
							Capabilities: DictionaryCapabilities{DictionaryCapabilityStableIDs, DictionaryCapabilityPrefixMatch},
						},
					},
				},
			},
		},
		Functions: []FunctionDefinition{
			{Name: "lower", Kind: FunctionScalar, Origin: FunctionOriginMySQLCompatible, Placement: FunctionPlacementExpression, Arguments: []DataType{DataTypeString}, ReturnType: DataTypeString, Aliases: []string{"lcase"}, Native: true, Deterministic: true},
			{Name: "count", Kind: FunctionAggregate, ReturnType: DataTypeInt, Native: true},
			{Name: "sum", Kind: FunctionAggregate, Arguments: []DataType{DataTypeFloat}, ReturnType: DataTypeFloat, Native: true},
			{Name: "avg", Kind: FunctionAggregate, Arguments: []DataType{DataTypeFloat}, ReturnType: DataTypeFloat, Native: true},
			{Name: "min", Kind: FunctionAggregate, Arguments: []DataType{DataTypeFloat}, ReturnType: DataTypeFloat, Native: true},
			{Name: "max", Kind: FunctionAggregate, Arguments: []DataType{DataTypeFloat}, ReturnType: DataTypeFloat, Native: true},
			{Name: "topn", Kind: FunctionAggregate, Origin: FunctionOriginQuantaCustom, Placement: FunctionPlacementAggregate, Arguments: []DataType{DataTypeString}, ReturnType: DataTypeString, Native: true},
		},
		Relationships: []RelationshipDefinition{
			{
				Name:        "orders_customer",
				FromTable:   "orders",
				FromField:   "o_custkey",
				ToTable:     "customer",
				ToField:     "c_custkey",
				Direction:   JoinChildToParent,
				Cardinality: "many_to_one",
				Encoding: RelationshipEncodingProfile{
					Kind:       RelationshipEncodingVector,
					LegacyName: "ParentRelation",
					Capabilities: RelationshipCapabilities{
						RelationshipCapabilityParentLookup,
						RelationshipCapabilityJoinReduction,
						RelationshipCapabilityAntiJoinDifference,
					},
				},
			},
		},
	}
}

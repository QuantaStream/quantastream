package qsbridge

import "testing"

func TestClassifyNativeSupportedQueryCollectsCapabilitiesAndFields(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipMode := FieldRef{Table: lineitem, Name: "l_shipmode", Index: IndexStringEnum}
	quantity := FieldRef{Table: lineitem, Name: "l_quantity", Index: IndexBSI}

	query := QueryIR{
		Kind: QueryKindSelect,
		Predicates: []Predicate{
			{
				Expr:         Binary(BinaryOpIn, Field(shipMode), Literal(ValueString, "MAIL")),
				Placement:    PredicatePushdown,
				Capabilities: []PlanCapability{CapabilityBitmapPushdown},
			},
			{
				Expr:      Binary(BinaryOpGreater, Field(quantity), Literal(ValueInt, 10)),
				Placement: PredicateResidualScan,
			},
		},
		Projection: []ProjectionColumn{{Expr: Field(shipMode)}},
	}

	classification := ClassifyNative(query)
	if !classification.Supported {
		t.Fatalf("expected query to be supported")
	}
	if !classification.HasCapability(CapabilityBitmapPushdown) {
		t.Fatalf("expected bitmap pushdown capability")
	}
	if !classification.HasCapability(CapabilityResidualScan) {
		t.Fatalf("expected residual scan capability")
	}
	if len(classification.Fields) != 2 {
		t.Fatalf("Fields returned %d refs, want 2", len(classification.Fields))
	}
}

func TestClassifyNativeCarriesDiagnosticsForBlockers(t *testing.T) {
	query := QueryIR{
		Kind:     QueryKindSelect,
		Blockers: []NativeBlocker{{Code: DiagnosticScalarSubquery, Capability: CapabilityScalarSubquery, Reason: "scalar subquery"}},
	}

	classification := ClassifyNative(query)
	if classification.Supported {
		t.Fatalf("expected blocker to make query unsupported")
	}
	codes := classification.Diagnostics.Codes()
	if len(codes) != 1 || codes[0] != DiagnosticScalarSubquery {
		t.Fatalf("diagnostic codes = %#v", codes)
	}
}

func TestClassifyNativeDeduplicatesCapabilities(t *testing.T) {
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	acctbal := FieldRef{Table: customer, Name: "c_acctbal", Index: IndexBSI}
	query := QueryIR{
		Kind: QueryKindSelect,
		Predicates: []Predicate{
			{
				Expr:         Binary(BinaryOpGreater, Field(acctbal), Literal(ValueInt, 0)),
				Placement:    PredicatePushdown,
				Capabilities: []PlanCapability{CapabilityBSIPushdown},
			},
			{
				Expr:         Binary(BinaryOpLess, Field(acctbal), Literal(ValueInt, 1000)),
				Placement:    PredicatePushdown,
				Capabilities: []PlanCapability{CapabilityBSIPushdown},
			},
		},
	}

	classification := ClassifyNative(query)
	if len(classification.Capabilities) != 1 {
		t.Fatalf("Capabilities returned %#v, want one deduped capability", classification.Capabilities)
	}
	if !classification.HasCapability(CapabilityBSIPushdown) {
		t.Fatalf("expected BSI pushdown capability")
	}
}

func TestClassifyNativeAddsGroupedJoinCapability(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey"}
	lineOrderKey := FieldRef{Table: lineitem, Name: "l_orderkey"}

	query := QueryIR{
		Kind: QueryKindSelect,
		Joins: []JoinEdge{{
			Left:      lineOrderKey,
			Right:     orderKey,
			Direction: JoinChildToParent,
			Legal:     true,
		}},
		GroupBy: []Expr{Field(orderKey)},
	}

	classification := ClassifyNative(query)
	if !classification.HasCapability(CapabilityGroupedJoin) {
		t.Fatalf("expected grouped join capability")
	}
	if classification.Diagnostics.BlocksNative() {
		t.Fatalf("did not expect grouped join diagnostics: %#v", classification.Diagnostics)
	}
}

func TestClassifyNativeAddsParentToChildExpansionCapability(t *testing.T) {
	parent := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	child := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	query := QueryIR{
		Kind: QueryKindSelect,
		Joins: []JoinEdge{{
			Left:      FieldRef{Table: parent, Name: "o_orderkey"},
			Right:     FieldRef{Table: child, Name: "l_orderkey"},
			Direction: JoinParentToChild,
			Legal:     true,
		}},
	}

	classification := ClassifyNative(query)
	if !classification.HasCapability(CapabilityParentToChildExpansion) {
		t.Fatalf("expected parent-to-child expansion capability")
	}
}

func TestClassifyNativeAddsRelationshipEncodingCapabilities(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	query := QueryIR{
		Kind: QueryKindSelect,
		Joins: []JoinEdge{{
			Left:      FieldRef{Table: lineitem, Name: "l_orderkey"},
			Right:     FieldRef{Table: orders, Name: "o_orderkey"},
			Direction: JoinChildToParent,
			Encoding: RelationshipEncodingProfile{
				Kind: RelationshipEncodingVector,
				Capabilities: RelationshipCapabilities{
					RelationshipCapabilityParentLookup,
					RelationshipCapabilityJoinReduction,
					RelationshipCapabilityAntiJoinDifference,
				},
			},
			Legal: true,
		}},
	}

	classification := ClassifyNative(query)
	if !classification.HasCapability(CapabilityRelationshipParentLookup) {
		t.Fatalf("capabilities = %#v, want relationship parent lookup", classification.Capabilities)
	}
	if !classification.HasCapability(CapabilityRelationshipJoinReduction) {
		t.Fatalf("capabilities = %#v, want relationship join reduction", classification.Capabilities)
	}
	if !classification.HasCapability(CapabilityRelationshipAntiJoinDifference) {
		t.Fatalf("capabilities = %#v, want relationship anti-join difference", classification.Capabilities)
	}
}

func TestClassifyNativeAddsAntiMembershipCapabilities(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	query := QueryIR{
		Kind: QueryKindSelect,
		Memberships: []MembershipEdge{{
			Left:  FieldRef{Table: orders, Name: "o_custkey"},
			Right: FieldRef{Table: customer, Name: "c_custkey"},
			Kind:  MembershipAnti,
			Legal: true,
		}},
	}

	classification := ClassifyNative(query)
	if !classification.HasCapability(CapabilityAntiMembership) {
		t.Fatalf("expected anti membership capability")
	}
	if !classification.HasCapability(CapabilityBitmapDifference) {
		t.Fatalf("expected bitmap difference capability")
	}
}

func TestClassifyNativeAddsMembershipRelationshipEncodingCapabilities(t *testing.T) {
	partsupp := TableInstance{ID: "partsupp", Table: "partsupp", Alias: "ps"}
	supplier := TableInstance{ID: "supplier", Table: "supplier", Alias: "s"}
	query := QueryIR{
		Kind: QueryKindSelect,
		Memberships: []MembershipEdge{{
			Left:  FieldRef{Table: partsupp, Name: "ps_suppkey"},
			Right: FieldRef{Table: supplier, Name: "s_suppkey"},
			Kind:  MembershipAnti,
			Encoding: RelationshipEncodingProfile{
				Kind: RelationshipEncodingVector,
				Capabilities: RelationshipCapabilities{
					RelationshipCapabilityParentLookup,
					RelationshipCapabilitySemiJoin,
					RelationshipCapabilityAntiJoinDifference,
				},
			},
			Legal: true,
		}},
	}

	classification := ClassifyNative(query)
	if !classification.HasCapability(CapabilityAntiMembership) {
		t.Fatalf("capabilities = %#v, want anti membership", classification.Capabilities)
	}
	if !classification.HasCapability(CapabilityRelationshipParentLookup) {
		t.Fatalf("capabilities = %#v, want relationship parent lookup", classification.Capabilities)
	}
	if !classification.HasCapability(CapabilityRelationshipSemiJoin) {
		t.Fatalf("capabilities = %#v, want relationship semi-join", classification.Capabilities)
	}
	if !classification.HasCapability(CapabilityRelationshipAntiJoinDifference) {
		t.Fatalf("capabilities = %#v, want relationship anti-join difference", classification.Capabilities)
	}
}

func TestClassifyNativeAddsOuterJoinCapabilities(t *testing.T) {
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	query := QueryIR{
		Kind: QueryKindSelect,
		Joins: []JoinEdge{{
			Left:  FieldRef{Table: customer, Name: "c_custkey"},
			Right: FieldRef{Table: orders, Name: "o_custkey"},
			Kind:  JoinKindLeftOuter,
			Nulls: NullExtensionRight,
			Legal: true,
		}},
	}

	classification := ClassifyNative(query)
	if !classification.HasCapability(CapabilityOuterJoin) {
		t.Fatalf("expected outer join capability")
	}
	if !classification.HasCapability(CapabilityNullExtension) {
		t.Fatalf("expected null extension capability")
	}
	if !classification.HasCapability(CapabilityBitmapDifference) {
		t.Fatalf("expected bitmap difference capability")
	}
}

func TestStringEnumPredicateCapabilityClassifiesEquality(t *testing.T) {
	field := testStringEnumField(DictionaryCapabilityStableIDs)
	predicate := Predicate{
		Expr:      Binary(BinaryOpEqual, Field(field), Literal(ValueString, "AIR")),
		Placement: PredicatePushdown,
	}

	capability, ok := StringEnumPredicateCapability(predicate)
	if !ok || capability != CapabilityStringEnumEquality {
		t.Fatalf("capability = %q, %v; want %q true", capability, ok, CapabilityStringEnumEquality)
	}
}

func TestStringEnumPredicateCapabilityClassifiesReversedEquality(t *testing.T) {
	field := testStringEnumField(DictionaryCapabilityStableIDs)
	predicate := Predicate{
		Expr:      Binary(BinaryOpEqual, Literal(ValueString, "AIR"), Field(field)),
		Placement: PredicatePushdown,
	}

	capability, ok := StringEnumPredicateCapability(predicate)
	if !ok || capability != CapabilityStringEnumEquality {
		t.Fatalf("capability = %q, %v; want %q true", capability, ok, CapabilityStringEnumEquality)
	}
}

func TestStringEnumPredicateCapabilityClassifiesMembership(t *testing.T) {
	field := testStringEnumField(DictionaryCapabilityStableIDs)
	predicate := Predicate{
		Expr:      Binary(BinaryOpIn, Field(field), List(Literal(ValueString, "AIR"), Literal(ValueString, "MAIL"))),
		Placement: PredicatePushdown,
	}

	capability, ok := StringEnumPredicateCapability(predicate)
	if !ok || capability != CapabilityStringEnumMembership {
		t.Fatalf("capability = %q, %v; want %q true", capability, ok, CapabilityStringEnumMembership)
	}
}

func TestStringEnumPredicateCapabilityRejectsNonStringMembership(t *testing.T) {
	field := testStringEnumField(DictionaryCapabilityStableIDs)
	predicate := Predicate{
		Expr:      Binary(BinaryOpIn, Field(field), List(Literal(ValueInt, 7))),
		Placement: PredicatePushdown,
	}

	if capability, ok := StringEnumPredicateCapability(predicate); ok || capability != "" {
		t.Fatalf("capability = %q, %v; want no StringEnum membership", capability, ok)
	}
}

func TestStringEnumPredicateCapabilityClassifiesLikePatterns(t *testing.T) {
	tests := []struct {
		name       string
		op         BinaryOp
		pattern    string
		dictionary DictionaryCapability
		want       PlanCapability
	}{
		{
			name:       "exact",
			op:         BinaryOpLike,
			pattern:    "AIR",
			dictionary: DictionaryCapabilityStableIDs,
			want:       CapabilityStringEnumEquality,
		},
		{
			name:       "prefix",
			op:         BinaryOpLike,
			pattern:    "PROMO%",
			dictionary: DictionaryCapabilityPrefixMatch,
			want:       CapabilityStringEnumPrefixLike,
		},
		{
			name:       "not-prefix",
			op:         BinaryOpNotLike,
			pattern:    "PROMO%",
			dictionary: DictionaryCapabilityPrefixMatch,
			want:       CapabilityStringEnumPrefixLike,
		},
		{
			name:       "contains",
			op:         BinaryOpLike,
			pattern:    "%green%",
			dictionary: DictionaryCapabilityContainsMatch,
			want:       CapabilityStringEnumContainsLike,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			field := testStringEnumField(test.dictionary)
			predicate := Predicate{
				Expr:      Binary(test.op, Field(field), Literal(ValueString, test.pattern)),
				Placement: PredicatePushdown,
			}

			capability, ok := StringEnumPredicateCapability(predicate)
			if !ok || capability != test.want {
				t.Fatalf("capability = %q, %v; want %q true", capability, ok, test.want)
			}
		})
	}
}

func TestStringEnumPredicateCapabilityRejectsUnsupportedShapes(t *testing.T) {
	field := testStringEnumField(DictionaryCapabilityPrefixMatch)
	tests := []Predicate{
		{Expr: Binary(BinaryOpLess, Field(field), Literal(ValueString, "AIR")), Placement: PredicatePushdown},
		{Expr: Binary(BinaryOpLike, Field(field), Literal(ValueString, "%BRASS")), Placement: PredicatePushdown},
		{Expr: Binary(BinaryOpNotLike, Field(testStringEnumField(DictionaryCapabilityContainsMatch)), Literal(ValueString, "%BRASS%")), Placement: PredicatePushdown},
		{Expr: Binary(BinaryOpLike, Field(field), Literal(ValueString, "A_R")), Placement: PredicatePushdown},
		{Expr: Binary(BinaryOpEqual, Field(FieldRef{Name: "plain", Index: IndexBackingString}), Literal(ValueString, "AIR")), Placement: PredicatePushdown},
	}

	for _, predicate := range tests {
		if capability, ok := StringEnumPredicateCapability(predicate); ok {
			t.Fatalf("unexpected capability = %q for %#v", capability, predicate.Expr)
		}
	}
}

func TestClassifyNativeAddsStringEnumCapabilities(t *testing.T) {
	field := testStringEnumField(DictionaryCapabilityPrefixMatch)
	query := QueryIR{
		Kind: QueryKindSelect,
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpLike, Field(field), Literal(ValueString, "AIR%")),
			Placement: PredicatePushdown,
		}},
	}

	classification := ClassifyNative(query)
	if !classification.HasCapability(CapabilityStringEnumPrefixLike) {
		t.Fatalf("expected StringEnum prefix LIKE capability")
	}
}

func TestEncodingPredicateCapabilitiesClassifyProfileBackedPredicates(t *testing.T) {
	part := TableInstance{ID: "part", Table: "part", Alias: "p"}
	tests := []struct {
		name      string
		field     FieldRef
		predicate Predicate
		want      PlanCapability
	}{
		{
			name: "numeric range",
			field: FieldRef{
				Table: part,
				Name:  "p_size",
				Encoding: EncodingProfile{
					Kind: EncodingNumericBSI,
					PredicateCapabilities: PredicateCapabilities{
						PredicateCapabilityRange,
					},
				},
			},
			want: CapabilityEncodingRange,
		},
		{
			name: "backing string equality",
			field: FieldRef{
				Table: part,
				Name:  "p_name",
				Encoding: EncodingProfile{
					Kind: EncodingBackingString,
					PredicateCapabilities: PredicateCapabilities{
						PredicateCapabilityEquality,
					},
				},
			},
			want: CapabilityEncodingEquality,
		},
		{
			name: "lex string prefix",
			field: FieldRef{
				Table: part,
				Name:  "p_name",
				Encoding: EncodingProfile{
					Kind: EncodingStringLexBSI,
					PredicateCapabilities: PredicateCapabilities{
						PredicateCapabilityPrefix,
					},
				},
			},
			want: CapabilityEncodingPrefix,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			switch test.want {
			case CapabilityEncodingRange:
				test.predicate = Predicate{Expr: Binary(BinaryOpGreater, Field(test.field), Literal(ValueInt, 10)), Placement: PredicatePushdown}
			case CapabilityEncodingEquality:
				test.predicate = Predicate{Expr: Binary(BinaryOpEqual, Field(test.field), Literal(ValueString, "green")), Placement: PredicatePushdown}
			case CapabilityEncodingPrefix:
				test.predicate = Predicate{Expr: Binary(BinaryOpLike, Field(test.field), Literal(ValueString, "green%")), Placement: PredicatePushdown}
			}

			capabilities := EncodingPredicateCapabilities(test.predicate)
			if !planCapabilitiesContain(capabilities, test.want) {
				t.Fatalf("capabilities = %#v, want %q", capabilities, test.want)
			}
		})
	}
}

func TestClassifyNativeAddsEncodingPredicateCapabilities(t *testing.T) {
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	acctbal := FieldRef{
		Table: customer,
		Name:  "c_acctbal",
		Encoding: EncodingProfile{
			Kind: EncodingNumericBSI,
			PredicateCapabilities: PredicateCapabilities{
				PredicateCapabilityRange,
			},
		},
	}
	query := QueryIR{
		Kind: QueryKindSelect,
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpGreater, Field(acctbal), Literal(ValueInt, 0)),
			Placement: PredicatePushdown,
		}},
	}

	classification := ClassifyNative(query)
	if !classification.HasCapability(CapabilityEncodingRange) {
		t.Fatalf("classification capabilities = %#v, want encoding range", classification.Capabilities)
	}
}

func planCapabilitiesContain(capabilities []PlanCapability, want PlanCapability) bool {
	for _, capability := range capabilities {
		if capability == want {
			return true
		}
	}
	return false
}

func testStringEnumField(capabilities ...DictionaryCapability) FieldRef {
	return FieldRef{
		Table: TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"},
		Name:  "l_shipmode",
		Index: IndexStringEnum,
		Dictionary: DictionaryDefinition{
			Ref:          DictionaryRef{Table: "lineitem", Field: "l_shipmode"},
			Version:      "v1",
			Capabilities: append(DictionaryCapabilities(nil), capabilities...),
		},
	}
}

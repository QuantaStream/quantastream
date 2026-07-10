package qsbridge

import "testing"

func TestDiagnosticErrorUsesStableCodeAndMessage(t *testing.T) {
	diagnostic := ErrorDiagnostic(DiagnosticScalarSubquery, PhasePlan, "scalar subqueries are not planned yet")

	if got, want := diagnostic.Error(), "scalar_subquery: scalar subqueries are not planned yet"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
	if !diagnostic.BlocksNative() {
		t.Fatalf("expected error diagnostic to block native planning")
	}
}

func TestDiagnosticSetReportsBlockersAndCodes(t *testing.T) {
	set := DiagnosticSet{
		{Code: DiagnosticParserBoundary, Severity: SeverityWarning},
		ErrorDiagnostic(DiagnosticOuterJoin, PhasePlan, "outer joins are not implemented"),
	}

	if !set.BlocksNative() {
		t.Fatalf("expected set to block native planning")
	}

	codes := set.Codes()
	if len(codes) != 2 {
		t.Fatalf("Codes() returned %d codes, want 2", len(codes))
	}
	if codes[0] != DiagnosticParserBoundary || codes[1] != DiagnosticOuterJoin {
		t.Fatalf("unexpected codes: %#v", codes)
	}
}

func TestNativeBlockerDefaultsToClassifyDiagnostic(t *testing.T) {
	blocker := NativeBlocker{
		Capability: CapabilityScalarSubquery,
		Reason:     "scalar subqueries are not planned yet",
	}

	diagnostic := blocker.Diagnostic()
	if diagnostic.Code != DiagnosticNativeBlocker {
		t.Fatalf("Code = %q, want %q", diagnostic.Code, DiagnosticNativeBlocker)
	}
	if diagnostic.Phase != PhaseClassify {
		t.Fatalf("Phase = %q, want %q", diagnostic.Phase, PhaseClassify)
	}
	if diagnostic.Capability != CapabilityScalarSubquery {
		t.Fatalf("Capability = %q, want %q", diagnostic.Capability, CapabilityScalarSubquery)
	}
}

func TestPredicateDiagnosticCarriesFieldsAndCapability(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	quantity := FieldRef{Table: lineitem, Name: "l_quantity", Index: IndexBSI}
	predicate := Predicate{
		Expr:         Binary(BinaryOpGreater, Field(quantity), Literal(ValueInt, 10)),
		Placement:    PredicateUnsupported,
		Capabilities: []PlanCapability{CapabilityBSIPushdown},
		Unsupported:  "unsupported BSI comparison",
	}

	diagnostic := PredicateDiagnostic(predicate)
	if diagnostic.Code != DiagnosticUnsupportedPredicate {
		t.Fatalf("Code = %q, want %q", diagnostic.Code, DiagnosticUnsupportedPredicate)
	}
	if diagnostic.Capability != CapabilityBSIPushdown {
		t.Fatalf("Capability = %q, want %q", diagnostic.Capability, CapabilityBSIPushdown)
	}
	if len(diagnostic.Fields) != 1 || diagnostic.Fields[0].QualifiedName() != "l.l_quantity" {
		t.Fatalf("unexpected fields: %#v", diagnostic.Fields)
	}
}

func TestJoinDiagnosticClassifiesParentToChildDirection(t *testing.T) {
	parent := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	child := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	edge := JoinEdge{
		Left:        FieldRef{Table: parent, Name: "o_orderkey"},
		Right:       FieldRef{Table: child, Name: "l_orderkey"},
		Direction:   JoinParentToChild,
		Unsupported: "parent-to-child expansion is not planned",
	}

	diagnostic := JoinDiagnostic(edge)
	if diagnostic.Code != DiagnosticUnsupportedJoinDirection {
		t.Fatalf("Code = %q, want %q", diagnostic.Code, DiagnosticUnsupportedJoinDirection)
	}
	if len(diagnostic.Fields) != 2 {
		t.Fatalf("expected join diagnostic fields")
	}
}

func TestMembershipDiagnosticPrefersRelationshipCapability(t *testing.T) {
	partsupp := TableInstance{ID: "partsupp", Table: "partsupp", Alias: "ps"}
	supplier := TableInstance{ID: "supplier", Table: "supplier", Alias: "s"}
	edge := MembershipEdge{
		Left:  FieldRef{Table: partsupp, Name: "ps_suppkey"},
		Right: FieldRef{Table: supplier, Name: "s_suppkey"},
		Kind:  MembershipAnti,
		Encoding: RelationshipEncodingProfile{
			Kind: RelationshipEncodingVector,
			Capabilities: RelationshipCapabilities{
				RelationshipCapabilityAntiJoinDifference,
			},
		},
		Unsupported: "anti membership cannot be lowered",
	}

	diagnostic := MembershipDiagnostic(edge)
	if diagnostic.Code != DiagnosticUnsupportedMembership {
		t.Fatalf("Code = %q, want %q", diagnostic.Code, DiagnosticUnsupportedMembership)
	}
	if diagnostic.Capability != CapabilityRelationshipAntiJoinDifference {
		t.Fatalf("Capability = %q, want %q", diagnostic.Capability, CapabilityRelationshipAntiJoinDifference)
	}
	if len(diagnostic.Fields) != 2 {
		t.Fatalf("expected membership diagnostic fields")
	}
}

package qsbridge

import "testing"

func TestTableInstanceKeepsRepeatedAliasesDistinct(t *testing.T) {
	france := TableInstance{ID: "nation_supplier", Table: "nation", Alias: "n1", Role: "supplier_nation"}
	germany := TableInstance{ID: "nation_customer", Table: "nation", Alias: "n2", Role: "customer_nation"}

	if france.ID == germany.ID {
		t.Fatalf("expected repeated table aliases to have distinct instance IDs")
	}
	if france.RefName() != "n1" || germany.RefName() != "n2" {
		t.Fatalf("unexpected ref names: %q %q", france.RefName(), germany.RefName())
	}
}

func TestFieldRefQualifiedNameUsesAlias(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	field := FieldRef{Table: orders, Name: "o_orderdate", Index: IndexDateTime, Roles: FieldRoleVisible | FieldRoleSortKey}

	if got, want := field.QualifiedName(), "o.o_orderdate"; got != want {
		t.Fatalf("QualifiedName() = %q, want %q", got, want)
	}
	if !field.Roles.Has(FieldRoleVisible | FieldRoleSortKey) {
		t.Fatalf("expected visible and sort-key roles to be present")
	}
	if field.Roles.Has(FieldRoleJoinInput) {
		t.Fatalf("did not expect join-input role")
	}
}

func TestPredicateSupportedRequiresPlacementAndNoReason(t *testing.T) {
	supported := Predicate{Placement: PredicatePushdown, Capabilities: []PlanCapability{CapabilityBitmapPushdown}}
	unsupported := Predicate{Placement: PredicateUnsupported, Unsupported: "mixed-table residual"}

	if !supported.Supported() {
		t.Fatalf("expected pushdown predicate to be supported")
	}
	if unsupported.Supported() {
		t.Fatalf("expected unsupported predicate to remain unsupported")
	}
}

func TestResultShapeSeparatesVisibleAndHiddenFields(t *testing.T) {
	customer := TableInstance{ID: "customers", Table: "customers_qa"}
	visible := FieldRef{Table: customer, Name: "first_name", Roles: FieldRoleVisible}
	hidden := FieldRef{Table: customer, Name: "age", Roles: FieldRoleHidden | FieldRoleSortKey}
	shape := ResultShape{
		Kind:         ResultQuery,
		Columns:      []FieldRef{visible},
		Hidden:       []FieldRef{hidden},
		Limit:        10,
		Materialized: true,
	}

	if shape.Kind != ResultQuery {
		t.Fatalf("expected query result shape")
	}
	if len(shape.Columns) != 1 || len(shape.Hidden) != 1 {
		t.Fatalf("expected one visible and one hidden field")
	}
	if !shape.Hidden[0].Roles.Has(FieldRoleHidden | FieldRoleSortKey) {
		t.Fatalf("expected hidden sort key")
	}
}

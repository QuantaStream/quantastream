package qsbridge

import "testing"

func TestRelationshipTupleRowSetExpandsSiblingChildren(t *testing.T) {
	rowSet := NewRelationshipTupleRowSet("p", []QuantaRownum{1, 2})
	rowSet = rowSet.Expand(RelationshipTupleExpansion{
		ParentRole: "p",
		ChildRole:  "l",
		ChildRowsByParent: map[QuantaRownum][]QuantaRownum{
			1: {11, 12},
			2: {21},
		},
	})
	rowSet = rowSet.Expand(RelationshipTupleExpansion{
		ParentRole: "p",
		ChildRole:  "ps",
		ChildRowsByParent: map[QuantaRownum][]QuantaRownum{
			1: {101, 102},
			2: {201},
		},
	})

	assertRelationshipTupleRows(t, rowSet, []map[TableInstanceID]QuantaRownum{
		{"p": 1, "l": 11, "ps": 101},
		{"p": 1, "l": 11, "ps": 102},
		{"p": 1, "l": 12, "ps": 101},
		{"p": 1, "l": 12, "ps": 102},
		{"p": 2, "l": 21, "ps": 201},
	})
	roles := rowSet.Roles()
	if len(roles) != 3 || roles[0] != "l" || roles[1] != "p" || roles[2] != "ps" {
		t.Fatalf("roles = %#v, want l,p,ps", roles)
	}
}

func TestRelationshipTupleRowSetExpandsSiblingGraphFromRoot(t *testing.T) {
	rowSet := NewRelationshipTupleRowSetFromRootExpansions("p", []QuantaRownum{1, 2}, []RelationshipTupleExpansion{
		{
			ParentRole: "p",
			ChildRole:  "l",
			ChildRowsByParent: map[QuantaRownum][]QuantaRownum{
				1: {11, 12},
				2: {21},
			},
		},
		{
			ParentRole: "p",
			ChildRole:  "ps",
			ChildRowsByParent: map[QuantaRownum][]QuantaRownum{
				1: {101, 102},
				2: {201},
			},
		},
	})

	assertRelationshipTupleRows(t, rowSet, []map[TableInstanceID]QuantaRownum{
		{"p": 1, "l": 11, "ps": 101},
		{"p": 1, "l": 11, "ps": 102},
		{"p": 1, "l": 12, "ps": 101},
		{"p": 1, "l": 12, "ps": 102},
		{"p": 2, "l": 21, "ps": 201},
	})
}

func TestRelationshipTupleRowSetNullExtendsMissingChildren(t *testing.T) {
	rowSet := NewRelationshipTupleRowSet("p", []QuantaRownum{1, 2})
	rowSet = rowSet.Expand(RelationshipTupleExpansion{
		ParentRole: "p",
		ChildRole:  "o",
		NullExtend: true,
		ChildRowsByParent: map[QuantaRownum][]QuantaRownum{
			1: {101, 102},
		},
	})
	rowSet = rowSet.Expand(RelationshipTupleExpansion{
		ParentRole: "o",
		ChildRole:  "l",
		NullExtend: true,
		ChildRowsByParent: map[QuantaRownum][]QuantaRownum{
			101: {1001},
		},
	})

	assertRelationshipTupleRows(t, rowSet, []map[TableInstanceID]QuantaRownum{
		{"p": 1, "o": 101, "l": 1001},
		{"p": 1, "o": 102},
		{"p": 2},
	})
	assertRelationshipTupleMissingRoles(t, rowSet, map[int][]TableInstanceID{
		1: {"l"},
		2: {"o", "l"},
	})
}

func TestRelationshipTupleRowSetFromAlignedRownums(t *testing.T) {
	rowSet, diagnostics := NewRelationshipTupleRowSetFromAlignedRownums(map[string][]QuantaRownum{
		"l": {101, 102},
		"o": {11, 12},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}

	assertRelationshipTupleRows(t, rowSet, []map[TableInstanceID]QuantaRownum{
		{"l": 101, "o": 11},
		{"l": 102, "o": 12},
	})
}

func TestRelationshipTupleRowSetProjectsRoleQualifiedValues(t *testing.T) {
	rowSet := RelationshipTupleRowSet{Rows: []RelationshipTupleRow{
		{Rownums: map[TableInstanceID]QuantaRownum{"l": 11, "o": 101}},
	}}
	fields := []QuantaProjectionField{
		{Index: "lineitem", Role: "l", Field: "l_quantity", PhysicalName: "l_quantity", Visible: true},
		{Index: "orders", Role: "o", Field: "o_totalprice", PhysicalName: "o_totalprice", Visible: true},
	}
	values := RelationshipTupleValueStore{
		RelationshipTupleValueKeyForField(fields[0]): {11: {Kind: ValueInt, Value: int64(17)}},
		RelationshipTupleValueKeyForField(fields[1]): {101: {Kind: ValueFloat, Value: float64(42)}},
	}

	projected, diagnostics := rowSet.ToProjectedRowSet("tuple", fields, values)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if got, want := projected.CandidateCount(), 1; got != want {
		t.Fatalf("candidate count = %d, want %d", got, want)
	}
	if got := projected.ProjectionVectors[0].Values[0].Value; got != int64(17) {
		t.Fatalf("quantity = %#v, want 17", got)
	}
	if got := projected.ProjectionVectors[1].Values[0].Value; got != float64(42) {
		t.Fatalf("totalprice = %#v, want 42", got)
	}
}

func TestRelationshipTupleRowSetProjectsNullForMissingRole(t *testing.T) {
	rowSet := RelationshipTupleRowSet{Rows: []RelationshipTupleRow{
		{Rownums: map[TableInstanceID]QuantaRownum{"c": 1, "o": 101}},
		{Rownums: map[TableInstanceID]QuantaRownum{"c": 2}},
	}}
	fields := []QuantaProjectionField{
		{Index: "customer", Role: "c", Field: "c_name", PhysicalName: "c_name", Visible: true},
		{Index: "orders", Role: "o", Field: "o_orderkey", PhysicalName: "o_orderkey", Visible: true},
	}
	values := RelationshipTupleValueStore{
		RelationshipTupleValueKeyForField(fields[0]): {
			1: {Kind: ValueString, Value: "Abe"},
			2: {Kind: ValueString, Value: "Annie"},
		},
		RelationshipTupleValueKeyForField(fields[1]): {
			101: {Kind: ValueInt, Value: int64(101)},
		},
	}

	projected, diagnostics := rowSet.ToProjectedRowSet("tuple", fields, values)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if got := projected.ProjectionVectors[1].Values[0].Value; got != int64(101) {
		t.Fatalf("first order = %#v, want 101", got)
	}
	if got := projected.ProjectionVectors[1].Values[1]; got.Kind != ValueNull || got.Value != nil {
		t.Fatalf("second order = %#v, want NULL", got)
	}
}

func assertRelationshipTupleRows(t *testing.T, rowSet RelationshipTupleRowSet, want []map[TableInstanceID]QuantaRownum) {
	t.Helper()
	if got := len(rowSet.Rows); got != len(want) {
		t.Fatalf("rows = %d, want %d: %#v", got, len(want), rowSet.Rows)
	}
	for i, wantRow := range want {
		for role, wantRownum := range wantRow {
			got, ok := rowSet.Rows[i].Rownum(role)
			if !ok {
				t.Fatalf("row %d missing role %s: %#v", i, role, rowSet.Rows[i])
			}
			if got != wantRownum {
				t.Fatalf("row %d role %s = %d, want %d", i, role, got, wantRownum)
			}
		}
	}
}

func assertRelationshipTupleMissingRoles(t *testing.T, rowSet RelationshipTupleRowSet, want map[int][]TableInstanceID) {
	t.Helper()
	for rowIndex, roles := range want {
		if rowIndex < 0 || rowIndex >= len(rowSet.Rows) {
			t.Fatalf("row index %d outside rowset %#v", rowIndex, rowSet.Rows)
		}
		for _, role := range roles {
			if _, ok := rowSet.Rows[rowIndex].Rownum(role); ok {
				t.Fatalf("row %d unexpectedly has role %s: %#v", rowIndex, role, rowSet.Rows[rowIndex])
			}
		}
	}
}

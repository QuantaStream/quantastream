package qsbridge

import "testing"

func TestRelationshipTupleValueKeyForFieldUsesRoleAndPhysicalName(t *testing.T) {
	key := RelationshipTupleValueKeyForField(QuantaProjectionField{
		Index:        "lineitem",
		Role:         "l",
		Field:        "l_suppkey",
		PhysicalName: "l_suppkey_physical",
	})

	if key.Role != "l" || key.Index != "lineitem" || key.Field != "l_suppkey_physical" {
		t.Fatalf("key = %#v, want role l index lineitem physical field", key)
	}
}

func TestRelationshipTupleValueKeyForFieldDefaultsRoleAndField(t *testing.T) {
	key := RelationshipTupleValueKeyForField(QuantaProjectionField{
		Index: "orders",
		Field: "o_orderkey",
	})

	if key.Role != "orders" || key.Index != "orders" || key.Field != "o_orderkey" {
		t.Fatalf("key = %#v, want default role orders and field o_orderkey", key)
	}
}

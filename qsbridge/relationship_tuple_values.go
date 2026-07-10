package qsbridge

// RelationshipTupleValueKey identifies a materialized tuple value by role and field.
//
// The key is intentionally role-aware because relationship-vector graph
// execution can carry multiple rownum domains for the same physical table.
type RelationshipTupleValueKey struct {
	Role  TableInstanceID
	Index string
	Field string
}

// RelationshipTupleValueStore stores materialized field values by role, field, and rownum.
type RelationshipTupleValueStore map[RelationshipTupleValueKey]map[QuantaRownum]ResultCell

// RelationshipTupleValueKeyForField returns the tuple value key for a projection field.
func RelationshipTupleValueKeyForField(field QuantaProjectionField) RelationshipTupleValueKey {
	role := field.Role
	if role == "" {
		role = TableInstanceID(field.Index)
	}
	name := field.PhysicalName
	if name == "" {
		name = field.Field
	}
	return RelationshipTupleValueKey{Role: role, Index: field.Index, Field: name}
}

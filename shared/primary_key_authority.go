package shared

import "strings"

const (
	// CompoundPrimaryKeyAuthorityFieldName is the hidden BSI field used to map
	// encoded compound primary-key values to rownums.
	CompoundPrimaryKeyAuthorityFieldName = "__qs_pk_authority"
)

// EnsureCompoundPrimaryKeyAuthorityAttribute adds the hidden BSI authority
// field required for compact compound primary-key lookup. The attribute is
// catalog-visible for storage/replay, but marked system so row ingestion skips
// normal payload mapping.
func EnsureCompoundPrimaryKeyAuthorityAttribute(table *BasicTable) bool {
	if table == nil || !HasCompoundPrimaryKey(table.PrimaryKey) {
		return false
	}
	for _, attr := range table.Attributes {
		if strings.EqualFold(attr.FieldName, CompoundPrimaryKeyAuthorityFieldName) {
			return false
		}
	}
	table.Attributes = append(table.Attributes, BasicAttribute{
		FieldName:       CompoundPrimaryKeyAuthorityFieldName,
		Type:            "Integer",
		MappingStrategy: "IntBSI",
		Desc:            "QuantaStream compound primary-key authority",
		System:          true,
	})
	return true
}

// HasCompoundPrimaryKey reports whether the primary-key declaration contains
// more than one field.
func HasCompoundPrimaryKey(primaryKey string) bool {
	count := 0
	for _, part := range strings.Split(primaryKey, "+") {
		if strings.TrimSpace(part) == "" {
			continue
		}
		count++
		if count > 1 {
			return true
		}
	}
	return false
}

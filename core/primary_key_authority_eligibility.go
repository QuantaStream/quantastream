package core

import "strings"

const (
	// BSIPrimaryKeyAuthorityModeUnsupported means the table must stay on the
	// current fallback path until a broader authority design supports it.
	BSIPrimaryKeyAuthorityModeUnsupported = "unsupported"

	// BSIPrimaryKeyAuthorityModeDirectColumnID means the primary-key value is
	// already the rownum/column-id authority, so a value-to-rownum BSI lookup is
	// not required.
	BSIPrimaryKeyAuthorityModeDirectColumnID = "direct_column_id"

	// BSIPrimaryKeyAuthorityModeSingleColumnBSI means the existing
	// catalog-designated primary-key BSI can answer value-to-rownum lookup.
	BSIPrimaryKeyAuthorityModeSingleColumnBSI = "single_column_bsi"
)

// BSIPrimaryKeyAuthorityEligibility describes whether a table can use native
// BSI primary-key authority without creating a separate authority index.
type BSIPrimaryKeyAuthorityEligibility struct {
	Eligible           bool
	Mode               string
	Reason             string
	TableName          string
	PrimaryKey         string
	FieldName          string
	MappingStrategy    string
	ColumnID           bool
	RequiresShardScope bool
}

// ObserveBSIPrimaryKeyAuthorityEligibility classifies the narrow go-forward
// simple-key authority shape. It does not alter resolver behavior.
func ObserveBSIPrimaryKeyAuthorityEligibility(table *Table) BSIPrimaryKeyAuthorityEligibility {
	observation := BSIPrimaryKeyAuthorityEligibility{
		Mode:   BSIPrimaryKeyAuthorityModeUnsupported,
		Reason: "table is missing",
	}
	if table == nil || table.BasicTable == nil {
		return observation
	}
	observation.TableName = table.Name
	observation.PrimaryKey = strings.TrimSpace(table.PrimaryKey)
	observation.RequiresShardScope = strings.TrimSpace(table.TimeQuantumField) != ""
	if observation.PrimaryKey == "" {
		observation.Reason = "primary key is missing"
		return observation
	}

	fields := primaryKeyAuthorityDeclaredFields(observation.PrimaryKey)
	if len(fields) != 1 {
		observation.Reason = "primary key is compound"
		return observation
	}
	observation.FieldName = fields[0]

	attr, err := table.GetAttribute(observation.FieldName)
	if err != nil || attr == nil || attr.BasicAttribute == nil {
		observation.Reason = "primary key field is missing from catalog"
		return observation
	}
	observation.MappingStrategy = attr.MappingStrategy
	observation.ColumnID = attr.ColumnID

	if !MapperTypeFromString(attr.MappingStrategy).IsBSI() {
		observation.Reason = "primary key field is not BSI-backed"
		return observation
	}
	observation.Eligible = true
	if attr.ColumnID {
		observation.Mode = BSIPrimaryKeyAuthorityModeDirectColumnID
		observation.Reason = "primary key maps directly to rownums"
		return observation
	}
	observation.Mode = BSIPrimaryKeyAuthorityModeSingleColumnBSI
	observation.Reason = "single-column primary key can use existing BSI authority"
	return observation
}

func primaryKeyAuthorityDeclaredFields(primaryKey string) []string {
	parts := strings.Split(primaryKey, "+")
	fields := make([]string, 0, len(parts))
	for _, part := range parts {
		field := strings.TrimSpace(part)
		if field != "" {
			fields = append(fields, field)
		}
	}
	return fields
}

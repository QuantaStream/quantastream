package qsbridge

import "strings"

const (
	// InformationSchemaName is the MySQL metadata schema namespace.
	InformationSchemaName = "information_schema"
	// InformationSchemaTablesName identifies INFORMATION_SCHEMA.TABLES.
	InformationSchemaTablesName = "tables"
	// InformationSchemaColumnsName identifies INFORMATION_SCHEMA.COLUMNS.
	InformationSchemaColumnsName = "columns"
	// InformationSchemaStatisticsName identifies INFORMATION_SCHEMA.STATISTICS.
	InformationSchemaStatisticsName = "statistics"
)

// InformationSchemaTableDefinition returns the small virtual catalog surface
// currently exposed for MySQL compatibility metadata queries.
func InformationSchemaTableDefinition(schema string, name string) (TableDefinition, bool) {
	if !strings.EqualFold(strings.TrimSpace(schema), InformationSchemaName) {
		return TableDefinition{}, false
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case InformationSchemaTablesName:
		return TableDefinition{
			Schema: InformationSchemaName,
			Name:   InformationSchemaTablesName,
			Fields: []FieldDefinition{
				{Name: "TABLE_SCHEMA", Type: DataTypeString},
				{Name: "TABLE_NAME", Type: DataTypeString},
				{Name: "TABLE_TYPE", Type: DataTypeString},
			},
		}, true
	case InformationSchemaColumnsName:
		return TableDefinition{
			Schema: InformationSchemaName,
			Name:   InformationSchemaColumnsName,
			Fields: []FieldDefinition{
				{Name: "TABLE_SCHEMA", Type: DataTypeString},
				{Name: "TABLE_NAME", Type: DataTypeString},
				{Name: "COLUMN_NAME", Type: DataTypeString},
				{Name: "ORDINAL_POSITION", Type: DataTypeInt},
				{Name: "COLUMN_DEFAULT", Type: DataTypeString, Nullable: true},
				{Name: "IS_NULLABLE", Type: DataTypeString},
				{Name: "DATA_TYPE", Type: DataTypeString},
				{Name: "COLUMN_TYPE", Type: DataTypeString},
				{Name: "COLUMN_KEY", Type: DataTypeString},
				{Name: "EXTRA", Type: DataTypeString},
			},
		}, true
	case InformationSchemaStatisticsName:
		return TableDefinition{
			Schema: InformationSchemaName,
			Name:   InformationSchemaStatisticsName,
			Fields: []FieldDefinition{
				{Name: "TABLE_SCHEMA", Type: DataTypeString},
				{Name: "TABLE_NAME", Type: DataTypeString},
				{Name: "NON_UNIQUE", Type: DataTypeInt},
				{Name: "INDEX_SCHEMA", Type: DataTypeString},
				{Name: "INDEX_NAME", Type: DataTypeString},
				{Name: "SEQ_IN_INDEX", Type: DataTypeInt},
				{Name: "COLUMN_NAME", Type: DataTypeString},
				{Name: "COLLATION", Type: DataTypeString, Nullable: true},
				{Name: "CARDINALITY", Type: DataTypeInt, Nullable: true},
				{Name: "SUB_PART", Type: DataTypeInt, Nullable: true},
				{Name: "PACKED", Type: DataTypeString, Nullable: true},
				{Name: "NULLABLE", Type: DataTypeString},
				{Name: "INDEX_TYPE", Type: DataTypeString},
				{Name: "COMMENT", Type: DataTypeString},
				{Name: "INDEX_COMMENT", Type: DataTypeString},
				{Name: "IS_VISIBLE", Type: DataTypeString},
				{Name: "EXPRESSION", Type: DataTypeString, Nullable: true},
			},
		}, true
	default:
		return TableDefinition{}, false
	}
}

// IsInformationSchemaTable reports whether a bound table instance names one of
// the virtual MySQL metadata tables.
func IsInformationSchemaTable(table TableInstance) bool {
	_, ok := InformationSchemaTableDefinition(table.Schema, table.Table)
	return ok
}

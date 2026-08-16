package qsbridge

import "strings"

const (
	// InformationSchemaName is the MySQL metadata schema namespace.
	InformationSchemaName = "information_schema"
	// InformationSchemaTablesName identifies INFORMATION_SCHEMA.TABLES.
	InformationSchemaTablesName = "tables"
	// InformationSchemaColumnsName identifies INFORMATION_SCHEMA.COLUMNS.
	InformationSchemaColumnsName = "columns"
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

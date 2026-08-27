package qsbridge

import "strings"

const (
	// InformationSchemaName is the MySQL metadata schema namespace.
	InformationSchemaName = "information_schema"
	// InformationSchemaTablesName identifies INFORMATION_SCHEMA.TABLES.
	InformationSchemaTablesName = "tables"
	// InformationSchemaSchemataName identifies INFORMATION_SCHEMA.SCHEMATA.
	InformationSchemaSchemataName = "schemata"
	// InformationSchemaColumnsName identifies INFORMATION_SCHEMA.COLUMNS.
	InformationSchemaColumnsName = "columns"
	// InformationSchemaStatisticsName identifies INFORMATION_SCHEMA.STATISTICS.
	InformationSchemaStatisticsName = "statistics"
	// InformationSchemaTableConstraintsName identifies INFORMATION_SCHEMA.TABLE_CONSTRAINTS.
	InformationSchemaTableConstraintsName = "table_constraints"
	// InformationSchemaKeyColumnUsageName identifies INFORMATION_SCHEMA.KEY_COLUMN_USAGE.
	InformationSchemaKeyColumnUsageName = "key_column_usage"
	// InformationSchemaReferentialConstraintsName identifies INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS.
	InformationSchemaReferentialConstraintsName = "referential_constraints"
	// InformationSchemaCharacterSetsName identifies INFORMATION_SCHEMA.CHARACTER_SETS.
	InformationSchemaCharacterSetsName = "character_sets"
	// InformationSchemaCollationsName identifies INFORMATION_SCHEMA.COLLATIONS.
	InformationSchemaCollationsName = "collations"
)

// InformationSchemaTableDefinition returns the small virtual catalog surface
// currently exposed for MySQL compatibility metadata queries.
func InformationSchemaTableDefinition(schema string, name string) (TableDefinition, bool) {
	if !strings.EqualFold(strings.TrimSpace(schema), InformationSchemaName) {
		return TableDefinition{}, false
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case InformationSchemaSchemataName:
		return TableDefinition{
			Schema: InformationSchemaName,
			Name:   InformationSchemaSchemataName,
			Fields: []FieldDefinition{
				{Name: "CATALOG_NAME", Type: DataTypeString},
				{Name: "SCHEMA_NAME", Type: DataTypeString},
				{Name: "DEFAULT_CHARACTER_SET_NAME", Type: DataTypeString},
				{Name: "DEFAULT_COLLATION_NAME", Type: DataTypeString},
				{Name: "SQL_PATH", Type: DataTypeString, Nullable: true},
				{Name: "DEFAULT_ENCRYPTION", Type: DataTypeString},
			},
		}, true
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
	case InformationSchemaTableConstraintsName:
		return TableDefinition{
			Schema: InformationSchemaName,
			Name:   InformationSchemaTableConstraintsName,
			Fields: []FieldDefinition{
				{Name: "CONSTRAINT_CATALOG", Type: DataTypeString},
				{Name: "CONSTRAINT_SCHEMA", Type: DataTypeString},
				{Name: "CONSTRAINT_NAME", Type: DataTypeString},
				{Name: "TABLE_SCHEMA", Type: DataTypeString},
				{Name: "TABLE_NAME", Type: DataTypeString},
				{Name: "CONSTRAINT_TYPE", Type: DataTypeString},
				{Name: "ENFORCED", Type: DataTypeString},
			},
		}, true
	case InformationSchemaKeyColumnUsageName:
		return TableDefinition{
			Schema: InformationSchemaName,
			Name:   InformationSchemaKeyColumnUsageName,
			Fields: []FieldDefinition{
				{Name: "CONSTRAINT_CATALOG", Type: DataTypeString},
				{Name: "CONSTRAINT_SCHEMA", Type: DataTypeString},
				{Name: "CONSTRAINT_NAME", Type: DataTypeString},
				{Name: "TABLE_CATALOG", Type: DataTypeString},
				{Name: "TABLE_SCHEMA", Type: DataTypeString},
				{Name: "TABLE_NAME", Type: DataTypeString},
				{Name: "COLUMN_NAME", Type: DataTypeString},
				{Name: "ORDINAL_POSITION", Type: DataTypeInt},
				{Name: "POSITION_IN_UNIQUE_CONSTRAINT", Type: DataTypeInt, Nullable: true},
				{Name: "REFERENCED_TABLE_SCHEMA", Type: DataTypeString, Nullable: true},
				{Name: "REFERENCED_TABLE_NAME", Type: DataTypeString, Nullable: true},
				{Name: "REFERENCED_COLUMN_NAME", Type: DataTypeString, Nullable: true},
			},
		}, true
	case InformationSchemaReferentialConstraintsName:
		return TableDefinition{
			Schema: InformationSchemaName,
			Name:   InformationSchemaReferentialConstraintsName,
			Fields: []FieldDefinition{
				{Name: "CONSTRAINT_CATALOG", Type: DataTypeString},
				{Name: "CONSTRAINT_SCHEMA", Type: DataTypeString},
				{Name: "CONSTRAINT_NAME", Type: DataTypeString},
				{Name: "UNIQUE_CONSTRAINT_CATALOG", Type: DataTypeString},
				{Name: "UNIQUE_CONSTRAINT_SCHEMA", Type: DataTypeString},
				{Name: "UNIQUE_CONSTRAINT_NAME", Type: DataTypeString},
				{Name: "MATCH_OPTION", Type: DataTypeString},
				{Name: "UPDATE_RULE", Type: DataTypeString},
				{Name: "DELETE_RULE", Type: DataTypeString},
				{Name: "TABLE_NAME", Type: DataTypeString},
				{Name: "REFERENCED_TABLE_NAME", Type: DataTypeString},
			},
		}, true
	case InformationSchemaCharacterSetsName:
		return TableDefinition{
			Schema: InformationSchemaName,
			Name:   InformationSchemaCharacterSetsName,
			Fields: []FieldDefinition{
				{Name: "CHARACTER_SET_NAME", Type: DataTypeString},
				{Name: "DEFAULT_COLLATE_NAME", Type: DataTypeString},
				{Name: "DESCRIPTION", Type: DataTypeString},
				{Name: "MAXLEN", Type: DataTypeInt},
			},
		}, true
	case InformationSchemaCollationsName:
		return TableDefinition{
			Schema: InformationSchemaName,
			Name:   InformationSchemaCollationsName,
			Fields: []FieldDefinition{
				{Name: "COLLATION_NAME", Type: DataTypeString},
				{Name: "CHARACTER_SET_NAME", Type: DataTypeString},
				{Name: "ID", Type: DataTypeInt},
				{Name: "IS_DEFAULT", Type: DataTypeString},
				{Name: "IS_COMPILED", Type: DataTypeString},
				{Name: "SORTLEN", Type: DataTypeInt},
				{Name: "PAD_ATTRIBUTE", Type: DataTypeString},
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

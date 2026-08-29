package qsruntime

import (
	"fmt"
	"sort"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func (r SQLRuntime) informationSchemaExecutionResult(request qsbridge.ExecutionRequest) (ExecutionResult, qsbridge.DiagnosticSet, bool) {
	query := request.Bound.Prepared.Query
	if query.Kind != qsbridge.QueryKindSelect || len(query.Sources) != 1 || !qsbridge.IsInformationSchemaTable(query.Sources[0]) {
		return ExecutionResult{}, nil, false
	}
	rows, diagnostics := r.informationSchemaRows(query.Sources[0])
	if diagnostics.BlocksNative() {
		return ExecutionResult{}, diagnostics, true
	}
	rows = filterInformationSchemaRows(rows, query.Predicates)
	if query.WhereExpr != nil {
		rows = filterInformationSchemaRowsByExpr(rows, query.WhereExpr)
	}
	rows = orderInformationSchemaRows(rows, query.OrderBy)
	rows = sliceInformationSchemaRows(rows, query.Result)
	result, projectionDiagnostics := informationSchemaProjectedResult(rows, query.Projection)
	return result, projectionDiagnostics, true
}

func normalizeInformationSchemaResultColumns(request qsbridge.ExecutionRequest) qsbridge.ExecutionRequest {
	query := request.Bound.Prepared.Query
	if query.Kind != qsbridge.QueryKindSelect || len(query.Sources) != 1 || !qsbridge.IsInformationSchemaTable(query.Sources[0]) {
		return request
	}
	request.ResultColumns = normalizeInformationSchemaColumns(request.ResultColumns)
	request.Bound.Prepared.ResultColumns = normalizeInformationSchemaColumns(request.Bound.Prepared.ResultColumns)
	return request
}

func normalizeInformationSchemaColumns(columns []qsbridge.ResultColumn) []qsbridge.ResultColumn {
	normalized := append([]qsbridge.ResultColumn(nil), columns...)
	for i := range normalized {
		normalized[i].Name = informationSchemaProjectionColumnName(normalized[i].Name)
	}
	return normalized
}

type informationSchemaRow map[string]qsbridge.ResultCell

const (
	informationSchemaDefaultCatalog   = "def"
	informationSchemaDefaultCharset   = "utf8mb4"
	informationSchemaDefaultCollation = "utf8mb4_0900_ai_ci"
)

func (r SQLRuntime) informationSchemaRows(source qsbridge.TableInstance) ([]informationSchemaRow, qsbridge.DiagnosticSet) {
	switch strings.ToLower(strings.TrimSpace(source.Table)) {
	case qsbridge.InformationSchemaSchemataName:
		return r.informationSchemaSchemataRows()
	case qsbridge.InformationSchemaTablesName:
		return r.informationSchemaTableRows()
	case qsbridge.InformationSchemaViewsName:
		return r.informationSchemaViewRows()
	case qsbridge.InformationSchemaColumnsName:
		return r.informationSchemaColumnRows()
	case qsbridge.InformationSchemaStatisticsName:
		return r.informationSchemaStatisticsRows()
	case qsbridge.InformationSchemaTableConstraintsName:
		return r.informationSchemaTableConstraintsRows()
	case qsbridge.InformationSchemaKeyColumnUsageName:
		return r.informationSchemaKeyColumnUsageRows()
	case qsbridge.InformationSchemaReferentialConstraintsName:
		return r.informationSchemaReferentialConstraintsRows()
	case qsbridge.InformationSchemaCharacterSetsName:
		return informationSchemaCharacterSetRows(), nil
	case qsbridge.InformationSchemaCollationsName:
		return informationSchemaCollationRows(), nil
	default:
		return nil, nil
	}
}

func (r SQLRuntime) informationSchemaSchemataRows() ([]informationSchemaRow, qsbridge.DiagnosticSet) {
	schemas := informationSchemaSchemaNames(r.planningCatalog(), r.DefaultSchema)
	rows := make([]informationSchemaRow, 0, len(schemas))
	for _, schema := range schemas {
		rows = append(rows, informationSchemaRow{
			"CATALOG_NAME":               describeStringCell(informationSchemaDefaultCatalog),
			"SCHEMA_NAME":                describeStringCell(schema),
			"DEFAULT_CHARACTER_SET_NAME": describeStringCell(informationSchemaDefaultCharset),
			"DEFAULT_COLLATION_NAME":     describeStringCell(informationSchemaDefaultCollation),
			"SQL_PATH":                   describeNullCell(),
			"DEFAULT_ENCRYPTION":         describeStringCell("NO"),
		})
	}
	sortInformationSchemaRows(rows, "SCHEMA_NAME")
	return rows, nil
}

func (r SQLRuntime) informationSchemaTableRows() ([]informationSchemaRow, qsbridge.DiagnosticSet) {
	catalog := r.planningCatalog()
	tables, diagnostics := r.catalogTablesForInformationSchema()
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	rows := make([]informationSchemaRow, 0, len(tables))
	for _, table := range tables {
		rows = append(rows, informationSchemaTableRow(table.Schema, table.Name, "BASE TABLE"))
	}
	if viewMetadata, ok := catalog.(qsbridge.CatalogViewMetadata); ok {
		for _, schema := range informationSchemaSchemaNames(catalog, r.DefaultSchema) {
			views, viewDiagnostics := viewMetadata.ListViews(schema)
			if viewDiagnostics.BlocksNative() {
				return nil, viewDiagnostics
			}
			for _, view := range views {
				schemaName := strings.TrimSpace(view.Schema)
				if schemaName == "" {
					schemaName = schema
				}
				rows = append(rows, informationSchemaTableRow(schemaName, view.Name, "VIEW"))
			}
		}
	}
	sortInformationSchemaRows(rows, "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE")
	return rows, nil
}

func informationSchemaTableRow(schemaName string, tableName string, tableType string) informationSchemaRow {
	row := informationSchemaRow{
		"TABLE_CATALOG":   describeStringCell(informationSchemaDefaultCatalog),
		"TABLE_SCHEMA":    describeStringCell(schemaName),
		"TABLE_NAME":      describeStringCell(tableName),
		"TABLE_TYPE":      describeStringCell(tableType),
		"ENGINE":          describeStringCell("QUANTASTREAM"),
		"VERSION":         describeIntCell(10),
		"ROW_FORMAT":      describeStringCell("Compressed"),
		"TABLE_ROWS":      describeNullCell(),
		"AVG_ROW_LENGTH":  describeNullCell(),
		"DATA_LENGTH":     describeNullCell(),
		"MAX_DATA_LENGTH": describeNullCell(),
		"INDEX_LENGTH":    describeNullCell(),
		"DATA_FREE":       describeNullCell(),
		"AUTO_INCREMENT":  describeNullCell(),
		"CREATE_TIME":     describeNullCell(),
		"UPDATE_TIME":     describeNullCell(),
		"CHECK_TIME":      describeNullCell(),
		"TABLE_COLLATION": describeStringCell(informationSchemaDefaultCollation),
		"CHECKSUM":        describeNullCell(),
		"CREATE_OPTIONS":  describeStringCell(""),
		"TABLE_COMMENT":   describeStringCell(tableType),
	}
	if strings.EqualFold(tableType, "VIEW") {
		row["ENGINE"] = describeNullCell()
		row["VERSION"] = describeNullCell()
		row["ROW_FORMAT"] = describeNullCell()
	}
	return row
}

func (r SQLRuntime) informationSchemaViewRows() ([]informationSchemaRow, qsbridge.DiagnosticSet) {
	catalog := r.planningCatalog()
	viewMetadata, ok := catalog.(qsbridge.CatalogViewMetadata)
	if !ok {
		return []informationSchemaRow{}, nil
	}
	rows := make([]informationSchemaRow, 0)
	for _, schema := range informationSchemaSchemaNames(catalog, r.DefaultSchema) {
		views, diagnostics := viewMetadata.ListViews(schema)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		for _, view := range views {
			schemaName := strings.TrimSpace(view.Schema)
			if schemaName == "" {
				schemaName = schema
			}
			rows = append(rows, informationSchemaViewRow(schemaName, view))
		}
	}
	sortInformationSchemaRows(rows, "TABLE_SCHEMA", "TABLE_NAME")
	return rows, nil
}

func informationSchemaViewRow(schemaName string, view qsbridge.SQLViewDefinition) informationSchemaRow {
	viewSQL := informationSchemaViewDefinitionSQL(view)
	return informationSchemaRow{
		"TABLE_CATALOG":        describeStringCell(informationSchemaDefaultCatalog),
		"TABLE_SCHEMA":         describeStringCell(schemaName),
		"TABLE_NAME":           describeStringCell(view.Name),
		"VIEW_DEFINITION":      describeStringCell(viewSQL),
		"CHECK_OPTION":         describeStringCell("NONE"),
		"IS_UPDATABLE":         describeStringCell("NO"),
		"DEFINER":              describeStringCell("qstream@%"),
		"SECURITY_TYPE":        describeStringCell("DEFINER"),
		"CHARACTER_SET_CLIENT": describeStringCell(informationSchemaDefaultCharset),
		"COLLATION_CONNECTION": describeStringCell(informationSchemaDefaultCollation),
	}
}

func informationSchemaViewDefinitionSQL(view qsbridge.SQLViewDefinition) string {
	viewSQL := strings.TrimSpace(view.CanonicalSQL)
	if viewSQL == "" {
		viewSQL = strings.TrimSpace(view.SQL)
	}
	if !strings.HasPrefix(strings.ToLower(viewSQL), "create ") {
		return viewSQL
	}
	lowerSQL := strings.ToLower(viewSQL)
	if idx := strings.Index(lowerSQL, " as "); idx >= 0 && idx+4 < len(viewSQL) {
		return strings.TrimSpace(viewSQL[idx+4:])
	}
	return viewSQL
}

func (r SQLRuntime) informationSchemaColumnRows() ([]informationSchemaRow, qsbridge.DiagnosticSet) {
	tables, diagnostics := r.catalogTablesForInformationSchema()
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	rows := make([]informationSchemaRow, 0)
	for _, table := range tables {
		relationshipColumns := informationSchemaRelationshipChildColumns(table)
		for i, field := range table.Fields {
			fieldRef := field.Ref(qsbridge.TableInstance{Schema: table.Schema, Table: table.Name}, 0)
			rows = append(rows, informationSchemaRow{
				"TABLE_CATALOG":            describeStringCell(informationSchemaDefaultCatalog),
				"TABLE_SCHEMA":             describeStringCell(table.Schema),
				"TABLE_NAME":               describeStringCell(table.Name),
				"COLUMN_NAME":              describeStringCell(field.Name),
				"ORDINAL_POSITION":         describeIntCell(int64(i + 1)),
				"COLUMN_DEFAULT":           describeNullCell(),
				"IS_NULLABLE":              describeStringCell(describeNullability(fieldRef)),
				"DATA_TYPE":                describeStringCell(informationSchemaDataType(fieldRef)),
				"CHARACTER_MAXIMUM_LENGTH": informationSchemaCharacterMaximumLength(fieldRef),
				"CHARACTER_OCTET_LENGTH":   informationSchemaCharacterOctetLength(fieldRef),
				"NUMERIC_PRECISION":        informationSchemaNumericPrecision(fieldRef),
				"NUMERIC_SCALE":            informationSchemaNumericScale(fieldRef),
				"DATETIME_PRECISION":       informationSchemaDatetimePrecision(fieldRef),
				"CHARACTER_SET_NAME":       informationSchemaCharacterSetName(fieldRef),
				"COLLATION_NAME":           informationSchemaCollationName(fieldRef),
				"COLUMN_TYPE":              describeStringCell(describeSQLType(fieldRef)),
				"COLUMN_KEY":               describeStringCell(informationSchemaColumnKey(fieldRef, relationshipColumns)),
				"EXTRA":                    describeStringCell(describeExtra(fieldRef)),
				"PRIVILEGES":               describeStringCell("select,insert,update,references"),
				"COLUMN_COMMENT":           describeStringCell(""),
				"GENERATION_EXPRESSION":    describeStringCell(""),
				"SRS_ID":                   describeNullCell(),
			})
		}
	}
	sortInformationSchemaRows(rows, "TABLE_SCHEMA", "TABLE_NAME", "ORDINAL_POSITION")
	return rows, nil
}

func informationSchemaCharacterMaximumLength(field qsbridge.FieldRef) qsbridge.ResultCell {
	if field.Type != qsbridge.DataTypeString {
		return describeNullCell()
	}
	length := field.Encoding.MaxLength
	if length <= 0 {
		length = 255
	}
	return describeIntCell(int64(length))
}

func informationSchemaCharacterOctetLength(field qsbridge.FieldRef) qsbridge.ResultCell {
	if field.Type != qsbridge.DataTypeString {
		return describeNullCell()
	}
	length := field.Encoding.MaxLength
	if length <= 0 {
		length = 255
	}
	return describeIntCell(int64(length * 4))
}

func informationSchemaNumericPrecision(field qsbridge.FieldRef) qsbridge.ResultCell {
	switch field.Type {
	case qsbridge.DataTypeBool:
		return describeIntCell(3)
	case qsbridge.DataTypeInt:
		return describeIntCell(10)
	case qsbridge.DataTypeFloat:
		if field.Encoding.Scale > 0 {
			return describeIntCell(15)
		}
		return describeIntCell(22)
	default:
		return describeNullCell()
	}
}

func informationSchemaNumericScale(field qsbridge.FieldRef) qsbridge.ResultCell {
	switch field.Type {
	case qsbridge.DataTypeBool, qsbridge.DataTypeInt:
		return describeIntCell(0)
	case qsbridge.DataTypeFloat:
		if field.Encoding.Scale > 0 {
			return describeIntCell(int64(field.Encoding.Scale))
		}
		return describeNullCell()
	default:
		return describeNullCell()
	}
}

func informationSchemaDatetimePrecision(field qsbridge.FieldRef) qsbridge.ResultCell {
	if field.Type == qsbridge.DataTypeTime {
		return describeIntCell(0)
	}
	return describeNullCell()
}

func informationSchemaCharacterSetName(field qsbridge.FieldRef) qsbridge.ResultCell {
	if field.Type == qsbridge.DataTypeString {
		return describeStringCell(informationSchemaDefaultCharset)
	}
	return describeNullCell()
}

func informationSchemaCollationName(field qsbridge.FieldRef) qsbridge.ResultCell {
	if field.Type == qsbridge.DataTypeString {
		return describeStringCell(informationSchemaDefaultCollation)
	}
	return describeNullCell()
}

func (r SQLRuntime) informationSchemaStatisticsRows() ([]informationSchemaRow, qsbridge.DiagnosticSet) {
	tables, diagnostics := r.catalogTablesForInformationSchema()
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	rows := make([]informationSchemaRow, 0)
	for _, table := range tables {
		schemaName := strings.TrimSpace(table.Schema)
		if schemaName == "" {
			schemaName = r.DefaultSchema
		}
		tableName := strings.TrimSpace(table.Name)
		target := qsbridge.TableInstance{
			ID:     qsbridge.TableInstanceID(tableName),
			Schema: schemaName,
			Table:  tableName,
			Role:   tableName,
		}
		columns := make([]qsbridge.FieldRef, 0, len(table.Fields))
		for _, field := range table.Fields {
			columns = append(columns, field.Ref(target, qsbridge.FieldRoleVisible))
		}
		for _, indexRow := range showIndexRows(target, columns) {
			rows = append(rows, informationSchemaRow{
				"TABLE_SCHEMA":  describeStringCell(schemaName),
				"TABLE_NAME":    describeStringCell(tableName),
				"NON_UNIQUE":    describeIntCell(indexRow.NonUnique),
				"INDEX_SCHEMA":  describeStringCell(schemaName),
				"INDEX_NAME":    describeStringCell(indexRow.KeyName),
				"SEQ_IN_INDEX":  describeIntCell(indexRow.SeqInIndex),
				"COLUMN_NAME":   describeStringCell(indexRow.ColumnName),
				"COLLATION":     describeStringCell(indexRow.Collation),
				"CARDINALITY":   describeNullCell(),
				"SUB_PART":      describeNullCell(),
				"PACKED":        describeNullCell(),
				"NULLABLE":      describeStringCell(indexRow.Null),
				"INDEX_TYPE":    describeStringCell(indexRow.IndexType),
				"COMMENT":       describeStringCell(indexRow.Comment),
				"INDEX_COMMENT": describeStringCell(indexRow.IndexComment),
				"IS_VISIBLE":    describeStringCell("YES"),
				"EXPRESSION":    describeNullCell(),
			})
		}
	}
	sortInformationSchemaRows(rows, "TABLE_SCHEMA", "TABLE_NAME", "INDEX_NAME", "SEQ_IN_INDEX", "COLUMN_NAME")
	return rows, nil
}

func (r SQLRuntime) informationSchemaTableConstraintsRows() ([]informationSchemaRow, qsbridge.DiagnosticSet) {
	tables, diagnostics := r.catalogTablesForInformationSchema()
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	rows := make([]informationSchemaRow, 0)
	for _, table := range tables {
		schemaName := informationSchemaTableSchema(table, r.DefaultSchema)
		for _, field := range table.Fields {
			if !field.PrimaryKey {
				continue
			}
			rows = append(rows, informationSchemaRow{
				"CONSTRAINT_CATALOG": describeStringCell(informationSchemaDefaultCatalog),
				"CONSTRAINT_SCHEMA":  describeStringCell(schemaName),
				"CONSTRAINT_NAME":    describeStringCell("PRIMARY"),
				"TABLE_SCHEMA":       describeStringCell(schemaName),
				"TABLE_NAME":         describeStringCell(table.Name),
				"CONSTRAINT_TYPE":    describeStringCell("PRIMARY KEY"),
				"ENFORCED":           describeStringCell("YES"),
			})
			break
		}
		for _, relationship := range table.Relationships {
			if !informationSchemaRelationshipAppliesToChildTable(table, relationship) {
				continue
			}
			rows = append(rows, informationSchemaRow{
				"CONSTRAINT_CATALOG": describeStringCell(informationSchemaDefaultCatalog),
				"CONSTRAINT_SCHEMA":  describeStringCell(schemaName),
				"CONSTRAINT_NAME":    describeStringCell(informationSchemaRelationshipName(table, relationship)),
				"TABLE_SCHEMA":       describeStringCell(schemaName),
				"TABLE_NAME":         describeStringCell(table.Name),
				"CONSTRAINT_TYPE":    describeStringCell("FOREIGN KEY"),
				"ENFORCED":           describeStringCell("YES"),
			})
		}
	}
	sortInformationSchemaRows(rows, "TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME", "CONSTRAINT_TYPE")
	return rows, nil
}

func (r SQLRuntime) informationSchemaKeyColumnUsageRows() ([]informationSchemaRow, qsbridge.DiagnosticSet) {
	tables, diagnostics := r.catalogTablesForInformationSchema()
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	parentPrimaryColumns := informationSchemaPrimaryColumnsByTable(tables, r.DefaultSchema)
	rows := make([]informationSchemaRow, 0)
	for _, table := range tables {
		schemaName := informationSchemaTableSchema(table, r.DefaultSchema)
		primarySeq := 1
		for _, field := range table.Fields {
			if !field.PrimaryKey {
				continue
			}
			rows = append(rows, informationSchemaRow{
				"CONSTRAINT_CATALOG":            describeStringCell(informationSchemaDefaultCatalog),
				"CONSTRAINT_SCHEMA":             describeStringCell(schemaName),
				"CONSTRAINT_NAME":               describeStringCell("PRIMARY"),
				"TABLE_CATALOG":                 describeStringCell(informationSchemaDefaultCatalog),
				"TABLE_SCHEMA":                  describeStringCell(schemaName),
				"TABLE_NAME":                    describeStringCell(table.Name),
				"COLUMN_NAME":                   describeStringCell(field.Name),
				"ORDINAL_POSITION":              describeIntCell(int64(primarySeq)),
				"POSITION_IN_UNIQUE_CONSTRAINT": describeNullCell(),
				"REFERENCED_TABLE_SCHEMA":       describeNullCell(),
				"REFERENCED_TABLE_NAME":         describeNullCell(),
				"REFERENCED_COLUMN_NAME":        describeNullCell(),
			})
			primarySeq++
		}
		for _, relationship := range table.Relationships {
			if !informationSchemaRelationshipAppliesToChildTable(table, relationship) {
				continue
			}
			rows = append(rows, informationSchemaRow{
				"CONSTRAINT_CATALOG":            describeStringCell(informationSchemaDefaultCatalog),
				"CONSTRAINT_SCHEMA":             describeStringCell(schemaName),
				"CONSTRAINT_NAME":               describeStringCell(informationSchemaRelationshipName(table, relationship)),
				"TABLE_CATALOG":                 describeStringCell(informationSchemaDefaultCatalog),
				"TABLE_SCHEMA":                  describeStringCell(schemaName),
				"TABLE_NAME":                    describeStringCell(table.Name),
				"COLUMN_NAME":                   describeStringCell(informationSchemaRelationshipChildField(relationship)),
				"ORDINAL_POSITION":              describeIntCell(1),
				"POSITION_IN_UNIQUE_CONSTRAINT": describeIntCell(1),
				"REFERENCED_TABLE_SCHEMA":       describeStringCell(schemaName),
				"REFERENCED_TABLE_NAME":         describeStringCell(relationship.ParentTable()),
				"REFERENCED_COLUMN_NAME":        describeStringCell(informationSchemaRelationshipParentField(relationship, schemaName, parentPrimaryColumns)),
			})
		}
	}
	sortInformationSchemaRows(rows, "TABLE_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME", "ORDINAL_POSITION", "COLUMN_NAME")
	return rows, nil
}

func (r SQLRuntime) informationSchemaReferentialConstraintsRows() ([]informationSchemaRow, qsbridge.DiagnosticSet) {
	tables, diagnostics := r.catalogTablesForInformationSchema()
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	rows := make([]informationSchemaRow, 0)
	for _, table := range tables {
		schemaName := informationSchemaTableSchema(table, r.DefaultSchema)
		for _, relationship := range table.Relationships {
			if !informationSchemaRelationshipAppliesToChildTable(table, relationship) {
				continue
			}
			rows = append(rows, informationSchemaRow{
				"CONSTRAINT_CATALOG":        describeStringCell(informationSchemaDefaultCatalog),
				"CONSTRAINT_SCHEMA":         describeStringCell(schemaName),
				"CONSTRAINT_NAME":           describeStringCell(informationSchemaRelationshipName(table, relationship)),
				"UNIQUE_CONSTRAINT_CATALOG": describeStringCell(informationSchemaDefaultCatalog),
				"UNIQUE_CONSTRAINT_SCHEMA":  describeStringCell(schemaName),
				"UNIQUE_CONSTRAINT_NAME":    describeStringCell("PRIMARY"),
				"MATCH_OPTION":              describeStringCell("NONE"),
				"UPDATE_RULE":               describeStringCell("RESTRICT"),
				"DELETE_RULE":               describeStringCell("RESTRICT"),
				"TABLE_NAME":                describeStringCell(table.Name),
				"REFERENCED_TABLE_NAME":     describeStringCell(relationship.ParentTable()),
			})
		}
	}
	sortInformationSchemaRows(rows, "CONSTRAINT_SCHEMA", "TABLE_NAME", "CONSTRAINT_NAME")
	return rows, nil
}

func informationSchemaCharacterSetRows() []informationSchemaRow {
	rows := []informationSchemaRow{{
		"CHARACTER_SET_NAME":   describeStringCell(informationSchemaDefaultCharset),
		"DEFAULT_COLLATE_NAME": describeStringCell(informationSchemaDefaultCollation),
		"DESCRIPTION":          describeStringCell("UTF-8 Unicode"),
		"MAXLEN":               describeIntCell(4),
	}}
	sortInformationSchemaRows(rows, "CHARACTER_SET_NAME")
	return rows
}

func informationSchemaCollationRows() []informationSchemaRow {
	rows := []informationSchemaRow{
		{
			"COLLATION_NAME":     describeStringCell("utf8mb4_0900_ai_ci"),
			"CHARACTER_SET_NAME": describeStringCell(informationSchemaDefaultCharset),
			"ID":                 describeIntCell(255),
			"IS_DEFAULT":         describeStringCell("Yes"),
			"IS_COMPILED":        describeStringCell("Yes"),
			"SORTLEN":            describeIntCell(0),
			"PAD_ATTRIBUTE":      describeStringCell("NO PAD"),
		},
		{
			"COLLATION_NAME":     describeStringCell("utf8mb4_bin"),
			"CHARACTER_SET_NAME": describeStringCell(informationSchemaDefaultCharset),
			"ID":                 describeIntCell(46),
			"IS_DEFAULT":         describeStringCell(""),
			"IS_COMPILED":        describeStringCell("Yes"),
			"SORTLEN":            describeIntCell(1),
			"PAD_ATTRIBUTE":      describeStringCell("PAD SPACE"),
		},
	}
	sortInformationSchemaRows(rows, "CHARACTER_SET_NAME", "COLLATION_NAME")
	return rows
}

func (r SQLRuntime) catalogTablesForInformationSchema() ([]qsbridge.TableDefinition, qsbridge.DiagnosticSet) {
	catalog := r.planningCatalog()
	metadata, ok := catalog.(qsbridge.CatalogMetadata)
	if !ok {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInvalidExecutionOption, qsbridge.PhaseExecute, "information_schema requires catalog metadata enumeration"),
		}
	}
	tables := make([]qsbridge.TableDefinition, 0)
	for _, schema := range informationSchemaSchemaNames(catalog, r.DefaultSchema) {
		schemaTables, diagnostics := metadata.ListTables(schema)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		for _, table := range schemaTables {
			schemaName := strings.TrimSpace(table.Schema)
			if schemaName == "" {
				table.Schema = schema
			}
			tables = append(tables, table)
		}
	}
	sort.SliceStable(tables, func(i, j int) bool {
		if !strings.EqualFold(tables[i].Schema, tables[j].Schema) {
			return strings.ToLower(tables[i].Schema) < strings.ToLower(tables[j].Schema)
		}
		return strings.ToLower(tables[i].Name) < strings.ToLower(tables[j].Name)
	})
	return tables, nil
}

func informationSchemaSchemaNames(catalog qsbridge.Catalog, defaultSchema string) []string {
	seen := make(map[string]string)
	if metadata, ok := catalog.(qsbridge.CatalogMetadata); ok {
		schemas, diagnostics := metadata.ListSchemas()
		if !diagnostics.BlocksNative() {
			for _, schema := range schemas {
				name := strings.TrimSpace(schema.Name)
				if name == "" || strings.EqualFold(name, qsbridge.InformationSchemaName) {
					continue
				}
				seen[strings.ToLower(name)] = name
			}
		}
	}
	defaultSchema = strings.TrimSpace(defaultSchema)
	if defaultSchema != "" && !strings.EqualFold(defaultSchema, qsbridge.InformationSchemaName) {
		seen[strings.ToLower(defaultSchema)] = defaultSchema
	}
	keys := make([]string, 0, len(seen))
	for key := range seen {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	names := make([]string, 0, len(keys))
	for _, key := range keys {
		names = append(names, seen[key])
	}
	return names
}

func informationSchemaTableSchema(table qsbridge.TableDefinition, defaultSchema string) string {
	schemaName := strings.TrimSpace(table.Schema)
	if schemaName == "" {
		schemaName = strings.TrimSpace(defaultSchema)
	}
	return schemaName
}

func informationSchemaRelationshipChildColumns(table qsbridge.TableDefinition) map[string]struct{} {
	columns := make(map[string]struct{})
	for _, relationship := range table.Relationships {
		if !informationSchemaRelationshipAppliesToChildTable(table, relationship) {
			continue
		}
		field := informationSchemaRelationshipChildField(relationship)
		if field != "" {
			columns[strings.ToLower(field)] = struct{}{}
		}
	}
	return columns
}

func informationSchemaColumnKey(field qsbridge.FieldRef, relationshipColumns map[string]struct{}) string {
	if field.PrimaryKey {
		return "PRI"
	}
	if _, ok := relationshipColumns[strings.ToLower(field.Name)]; ok {
		return "MUL"
	}
	return describeKey(field)
}

func informationSchemaPrimaryColumnsByTable(tables []qsbridge.TableDefinition, defaultSchema string) map[string][]string {
	columns := make(map[string][]string)
	for _, table := range tables {
		schemaName := informationSchemaTableSchema(table, defaultSchema)
		key := informationSchemaTableKey(schemaName, table.Name)
		for _, field := range table.Fields {
			if field.PrimaryKey {
				columns[key] = append(columns[key], field.Name)
			}
		}
	}
	return columns
}

func informationSchemaTableKey(schema string, table string) string {
	return strings.ToLower(strings.TrimSpace(schema)) + "\x00" + strings.ToLower(strings.TrimSpace(table))
}

func informationSchemaRelationshipAppliesToChildTable(table qsbridge.TableDefinition, relationship qsbridge.RelationshipDefinition) bool {
	childTable := strings.TrimSpace(relationship.ChildTable())
	return childTable == "" || strings.EqualFold(childTable, table.Name)
}

func informationSchemaRelationshipChildField(relationship qsbridge.RelationshipDefinition) string {
	switch relationship.Direction {
	case qsbridge.JoinParentToChild:
		return relationship.ToField
	case qsbridge.JoinChildToParent:
		return relationship.FromField
	default:
		if relationship.FromField != "" {
			return relationship.FromField
		}
		return relationship.ToField
	}
}

func informationSchemaRelationshipParentField(relationship qsbridge.RelationshipDefinition, schemaName string, primaryColumns map[string][]string) string {
	field := ""
	switch relationship.Direction {
	case qsbridge.JoinParentToChild:
		field = relationship.FromField
	case qsbridge.JoinChildToParent:
		field = relationship.ToField
	default:
		if relationship.ToField != "" {
			field = relationship.ToField
			break
		}
		field = relationship.FromField
	}
	if strings.TrimSpace(field) != "" {
		return field
	}
	parentColumns := primaryColumns[informationSchemaTableKey(schemaName, relationship.ParentTable())]
	if len(parentColumns) == 1 {
		return parentColumns[0]
	}
	childKey := informationSchemaRelationshipKeySuffix(informationSchemaRelationshipChildField(relationship))
	if childKey != "" {
		for _, parentColumn := range parentColumns {
			if informationSchemaRelationshipKeySuffix(parentColumn) == childKey {
				return parentColumn
			}
		}
	}
	return field
}

func informationSchemaRelationshipName(table qsbridge.TableDefinition, relationship qsbridge.RelationshipDefinition) string {
	name := strings.TrimSpace(relationship.Name)
	if name != "" {
		return name
	}
	childField := strings.TrimSpace(informationSchemaRelationshipChildField(relationship))
	parentTable := strings.TrimSpace(relationship.ParentTable())
	if childField == "" {
		childField = "field"
	}
	if parentTable == "" {
		parentTable = "parent"
	}
	return "fk_" + table.Name + "_" + parentTable + "_" + childField
}

func informationSchemaRelationshipKeySuffix(field string) string {
	field = strings.ToLower(strings.TrimSpace(field))
	if field == "" {
		return ""
	}
	if index := strings.Index(field, "_"); index >= 0 && index+1 < len(field) {
		return field[index+1:]
	}
	return field
}

func informationSchemaProjectedResult(rows []informationSchemaRow, projections []qsbridge.ProjectionColumn) (ExecutionResult, qsbridge.DiagnosticSet) {
	rownums := make([]qsbridge.QuantaRownum, len(rows))
	vectors := make([]qsbridge.QuantaProjectionVector, 0, len(projections))
	for _, projection := range projections {
		column := projection.ResultColumn()
		vector := describeProjectionVector(informationSchemaProjectionColumnName(column.Name), column.Type, len(rows))
		for i, row := range rows {
			cell, ok := informationSchemaProjectionCell(row, projection.Expr)
			if !ok {
				return ExecutionResult{}, qsbridge.DiagnosticSet{
					qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "information_schema only supports direct column and literal projections"),
				}
			}
			rownums[i] = qsbridge.QuantaRownum(i + 1)
			vector.Values[i] = cell
		}
		vectors = append(vectors, vector)
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "information_schema",
			Rownums:           rownums,
			ProjectionVectors: vectors,
		},
		Count: uint64(len(rows)),
	}, nil
}

func informationSchemaProjectionColumnName(name string) string {
	name = strings.TrimSpace(name)
	if len(name) < 2 {
		return name
	}
	first := name[0]
	last := name[len(name)-1]
	if (first == '\'' && last == '\'') || (first == '"' && last == '"') || (first == '`' && last == '`') {
		return name[1 : len(name)-1]
	}
	return name
}

func informationSchemaProjectionCell(row informationSchemaRow, expr qsbridge.Expr) (qsbridge.ResultCell, bool) {
	switch typed := expr.(type) {
	case qsbridge.FieldExpr:
		return rowCell(row, typed.Ref.Name), true
	case *qsbridge.FieldExpr:
		if typed != nil {
			return rowCell(row, typed.Ref.Name), true
		}
	case qsbridge.LiteralExpr:
		return informationSchemaLiteralCell(typed), true
	case *qsbridge.LiteralExpr:
		if typed != nil {
			return informationSchemaLiteralCell(*typed), true
		}
	}
	return qsbridge.ResultCell{}, false
}

func informationSchemaLiteralCell(literal qsbridge.LiteralExpr) qsbridge.ResultCell {
	switch literal.Kind {
	case qsbridge.ValueNull:
		return describeNullCell()
	case qsbridge.ValueBool:
		return qsbridge.ResultCell{Kind: qsbridge.ValueBool, Value: literal.Value}
	case qsbridge.ValueInt:
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: literal.Value}
	case qsbridge.ValueFloat:
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: literal.Value}
	case qsbridge.ValueTime:
		return qsbridge.ResultCell{Kind: qsbridge.ValueTime, Value: literal.Value}
	default:
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: resultCellString(qsbridge.ResultCell{Value: literal.Value})}
	}
}

func informationSchemaProjectionField(projection qsbridge.ProjectionColumn) (string, bool) {
	switch expr := projection.Expr.(type) {
	case qsbridge.FieldExpr:
		return expr.Ref.Name, true
	case *qsbridge.FieldExpr:
		if expr != nil {
			return expr.Ref.Name, true
		}
	}
	return "", false
}

func filterInformationSchemaRows(rows []informationSchemaRow, predicates []qsbridge.Predicate) []informationSchemaRow {
	if len(predicates) == 0 {
		return rows
	}
	filtered := make([]informationSchemaRow, 0, len(rows))
	for _, row := range rows {
		if informationSchemaRowMatches(row, predicates) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func filterInformationSchemaRowsByExpr(rows []informationSchemaRow, expr qsbridge.Expr) []informationSchemaRow {
	filtered := make([]informationSchemaRow, 0, len(rows))
	for _, row := range rows {
		if informationSchemaPredicateMatches(row, expr) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func informationSchemaRowMatches(row informationSchemaRow, predicates []qsbridge.Predicate) bool {
	for _, predicate := range predicates {
		if !informationSchemaPredicateMatches(row, predicate.Expr) {
			return false
		}
	}
	return true
}

func informationSchemaPredicateMatches(row informationSchemaRow, expr qsbridge.Expr) bool {
	binary, ok := expr.(qsbridge.BinaryExpr)
	if !ok {
		if pointer, pointerOK := expr.(*qsbridge.BinaryExpr); pointerOK && pointer != nil {
			binary = *pointer
			ok = true
		}
	}
	if !ok {
		return true
	}
	if binary.Op == qsbridge.BinaryOpAnd {
		return informationSchemaPredicateMatches(row, binary.Left) && informationSchemaPredicateMatches(row, binary.Right)
	}
	if binary.Op == qsbridge.BinaryOpOr {
		return informationSchemaPredicateMatches(row, binary.Left) || informationSchemaPredicateMatches(row, binary.Right)
	}
	if binary.Op == qsbridge.BinaryOpIn || binary.Op == qsbridge.BinaryOpNotIn {
		field, list, ok := informationSchemaFieldList(binary.Left, binary.Right)
		if !ok {
			return true
		}
		matches := informationSchemaLiteralListContains(list, resultCellString(rowCell(row, field)))
		if binary.Op == qsbridge.BinaryOpNotIn {
			return !matches
		}
		return matches
	}
	field, literal, ok := informationSchemaFieldLiteral(binary.Left, binary.Right)
	if !ok {
		field, literal, ok = informationSchemaFieldLiteral(binary.Right, binary.Left)
	}
	if !ok {
		if nullField, nullOK := informationSchemaFieldNull(binary.Left, binary.Right); nullOK {
			isNull := rowCell(row, nullField).Value == nil
			if binary.Op == qsbridge.BinaryOpNotEqual {
				return !isNull
			}
			return isNull
		}
		if nullField, nullOK := informationSchemaFieldNull(binary.Right, binary.Left); nullOK {
			isNull := rowCell(row, nullField).Value == nil
			if binary.Op == qsbridge.BinaryOpNotEqual {
				return !isNull
			}
			return isNull
		}
	}
	if !ok {
		return true
	}
	value := resultCellString(rowCell(row, field))
	switch binary.Op {
	case qsbridge.BinaryOpEqual:
		return strings.EqualFold(value, literal)
	case qsbridge.BinaryOpLike:
		return sqlLikeMatch(value, literal)
	default:
		return true
	}
}

func informationSchemaFieldList(left qsbridge.Expr, right qsbridge.Expr) (string, qsbridge.ListExpr, bool) {
	field, ok := left.(qsbridge.FieldExpr)
	if !ok {
		if pointer, pointerOK := left.(*qsbridge.FieldExpr); pointerOK && pointer != nil {
			field = *pointer
			ok = true
		}
	}
	if !ok {
		return "", qsbridge.ListExpr{}, false
	}
	list, ok := right.(qsbridge.ListExpr)
	if !ok {
		if pointer, pointerOK := right.(*qsbridge.ListExpr); pointerOK && pointer != nil {
			list = *pointer
			ok = true
		}
	}
	if !ok {
		return "", qsbridge.ListExpr{}, false
	}
	return field.Ref.Name, list, true
}

func informationSchemaLiteralListContains(list qsbridge.ListExpr, value string) bool {
	for _, item := range list.Items {
		literal, ok := item.(qsbridge.LiteralExpr)
		if !ok {
			if pointer, pointerOK := item.(*qsbridge.LiteralExpr); pointerOK && pointer != nil {
				literal = *pointer
				ok = true
			}
		}
		if !ok {
			continue
		}
		itemValue, _ := literal.Value.(string)
		if strings.EqualFold(value, itemValue) {
			return true
		}
	}
	return false
}

func informationSchemaFieldLiteral(left qsbridge.Expr, right qsbridge.Expr) (string, string, bool) {
	field, ok := left.(qsbridge.FieldExpr)
	if !ok {
		if pointer, pointerOK := left.(*qsbridge.FieldExpr); pointerOK && pointer != nil {
			field = *pointer
			ok = true
		}
	}
	if !ok {
		return "", "", false
	}
	literal, ok := right.(qsbridge.LiteralExpr)
	if !ok {
		if pointer, pointerOK := right.(*qsbridge.LiteralExpr); pointerOK && pointer != nil {
			literal = *pointer
			ok = true
		}
	}
	if !ok || literal.Kind != qsbridge.ValueString {
		return "", "", false
	}
	value, _ := literal.Value.(string)
	return field.Ref.Name, value, true
}

func informationSchemaFieldNull(left qsbridge.Expr, right qsbridge.Expr) (string, bool) {
	field, ok := left.(qsbridge.FieldExpr)
	if !ok {
		if pointer, pointerOK := left.(*qsbridge.FieldExpr); pointerOK && pointer != nil {
			field = *pointer
			ok = true
		}
	}
	if !ok {
		return "", false
	}
	literal, ok := right.(qsbridge.LiteralExpr)
	if !ok {
		if pointer, pointerOK := right.(*qsbridge.LiteralExpr); pointerOK && pointer != nil {
			literal = *pointer
			ok = true
		}
	}
	if !ok || literal.Kind != qsbridge.ValueNull {
		return "", false
	}
	return field.Ref.Name, true
}

func orderInformationSchemaRows(rows []informationSchemaRow, orderBy []qsbridge.SortSpec) []informationSchemaRow {
	if len(orderBy) == 0 || len(rows) < 2 {
		return rows
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for _, sortSpec := range orderBy {
			field, ok := informationSchemaSortField(sortSpec)
			if !ok {
				continue
			}
			cmp := compareInformationSchemaCells(rowCell(rows[i], field), rowCell(rows[j], field))
			if cmp == 0 {
				continue
			}
			if sortSpec.Direction == qsbridge.SortDescending {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	return rows
}

func informationSchemaSortField(sortSpec qsbridge.SortSpec) (string, bool) {
	field, ok := sortSpec.Expr.(qsbridge.FieldExpr)
	if !ok {
		if pointer, pointerOK := sortSpec.Expr.(*qsbridge.FieldExpr); pointerOK && pointer != nil {
			field = *pointer
			ok = true
		}
	}
	if !ok {
		return "", false
	}
	return field.Ref.Name, true
}

func sliceInformationSchemaRows(rows []informationSchemaRow, result qsbridge.ResultShape) []informationSchemaRow {
	offset := result.Offset
	if offset > len(rows) {
		return nil
	}
	rows = rows[offset:]
	if result.HasLimit && result.Limit < len(rows) {
		return rows[:result.Limit]
	}
	return rows
}

func sortInformationSchemaRows(rows []informationSchemaRow, fields ...string) {
	sort.SliceStable(rows, func(i, j int) bool {
		for _, field := range fields {
			cmp := compareInformationSchemaCells(rowCell(rows[i], field), rowCell(rows[j], field))
			if cmp == 0 {
				continue
			}
			return cmp < 0
		}
		return false
	})
}

func compareInformationSchemaCells(left qsbridge.ResultCell, right qsbridge.ResultCell) int {
	if left.Kind == qsbridge.ValueInt && right.Kind == qsbridge.ValueInt {
		leftInt, leftOK := informationSchemaResultCellInt64(left)
		rightInt, rightOK := informationSchemaResultCellInt64(right)
		if leftOK && rightOK {
			switch {
			case leftInt < rightInt:
				return -1
			case leftInt > rightInt:
				return 1
			default:
				return 0
			}
		}
	}
	leftString := strings.ToLower(resultCellString(left))
	rightString := strings.ToLower(resultCellString(right))
	switch {
	case leftString < rightString:
		return -1
	case leftString > rightString:
		return 1
	default:
		return 0
	}
}

func informationSchemaResultCellInt64(cell qsbridge.ResultCell) (int64, bool) {
	switch value := cell.Value.(type) {
	case int:
		return int64(value), true
	case int64:
		return value, true
	case uint64:
		if value <= uint64(^uint64(0)>>1) {
			return int64(value), true
		}
		return 0, false
	default:
		return 0, false
	}
}

func rowCell(row informationSchemaRow, field string) qsbridge.ResultCell {
	if cell, ok := row[strings.ToUpper(strings.TrimSpace(field))]; ok {
		return cell
	}
	return describeNullCell()
}

func resultCellString(cell qsbridge.ResultCell) string {
	if cell.Value == nil {
		return ""
	}
	switch value := cell.Value.(type) {
	case string:
		return value
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

func informationSchemaDataType(field qsbridge.FieldRef) string {
	switch field.Type {
	case qsbridge.DataTypeBool:
		return "tinyint"
	case qsbridge.DataTypeInt:
		return "int"
	case qsbridge.DataTypeFloat:
		if field.Encoding.Scale > 0 {
			return "decimal"
		}
		return "double"
	case qsbridge.DataTypeString:
		return "varchar"
	case qsbridge.DataTypeTime:
		return "datetime"
	default:
		return "unknown"
	}
}

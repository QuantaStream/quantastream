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

type informationSchemaRow map[string]qsbridge.ResultCell

func (r SQLRuntime) informationSchemaRows(source qsbridge.TableInstance) ([]informationSchemaRow, qsbridge.DiagnosticSet) {
	switch strings.ToLower(strings.TrimSpace(source.Table)) {
	case qsbridge.InformationSchemaSchemataName:
		return r.informationSchemaSchemataRows()
	case qsbridge.InformationSchemaTablesName:
		return r.informationSchemaTableRows()
	case qsbridge.InformationSchemaColumnsName:
		return r.informationSchemaColumnRows()
	case qsbridge.InformationSchemaStatisticsName:
		return r.informationSchemaStatisticsRows()
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
			"CATALOG_NAME":               describeStringCell("def"),
			"SCHEMA_NAME":                describeStringCell(schema),
			"DEFAULT_CHARACTER_SET_NAME": describeStringCell("utf8mb4"),
			"DEFAULT_COLLATION_NAME":     describeStringCell("utf8mb4_0900_ai_ci"),
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
		rows = append(rows, informationSchemaRow{
			"TABLE_SCHEMA": describeStringCell(table.Schema),
			"TABLE_NAME":   describeStringCell(table.Name),
			"TABLE_TYPE":   describeStringCell("BASE TABLE"),
		})
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
				rows = append(rows, informationSchemaRow{
					"TABLE_SCHEMA": describeStringCell(schemaName),
					"TABLE_NAME":   describeStringCell(view.Name),
					"TABLE_TYPE":   describeStringCell("VIEW"),
				})
			}
		}
	}
	sortInformationSchemaRows(rows, "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE")
	return rows, nil
}

func (r SQLRuntime) informationSchemaColumnRows() ([]informationSchemaRow, qsbridge.DiagnosticSet) {
	tables, diagnostics := r.catalogTablesForInformationSchema()
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	rows := make([]informationSchemaRow, 0)
	for _, table := range tables {
		for i, field := range table.Fields {
			fieldRef := field.Ref(qsbridge.TableInstance{Schema: table.Schema, Table: table.Name}, 0)
			rows = append(rows, informationSchemaRow{
				"TABLE_SCHEMA":     describeStringCell(table.Schema),
				"TABLE_NAME":       describeStringCell(table.Name),
				"COLUMN_NAME":      describeStringCell(field.Name),
				"ORDINAL_POSITION": describeIntCell(int64(i + 1)),
				"COLUMN_DEFAULT":   describeNullCell(),
				"IS_NULLABLE":      describeStringCell(describeNullability(fieldRef)),
				"DATA_TYPE":        describeStringCell(informationSchemaDataType(fieldRef)),
				"COLUMN_TYPE":      describeStringCell(describeSQLType(fieldRef)),
				"COLUMN_KEY":       describeStringCell(describeKey(fieldRef)),
				"EXTRA":            describeStringCell(describeExtra(fieldRef)),
			})
		}
	}
	sortInformationSchemaRows(rows, "TABLE_SCHEMA", "TABLE_NAME", "ORDINAL_POSITION")
	return rows, nil
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

func informationSchemaCharacterSetRows() []informationSchemaRow {
	rows := []informationSchemaRow{{
		"CHARACTER_SET_NAME":   describeStringCell("utf8mb4"),
		"DEFAULT_COLLATE_NAME": describeStringCell("utf8mb4_0900_ai_ci"),
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
			"CHARACTER_SET_NAME": describeStringCell("utf8mb4"),
			"ID":                 describeIntCell(255),
			"IS_DEFAULT":         describeStringCell("Yes"),
			"IS_COMPILED":        describeStringCell("Yes"),
			"SORTLEN":            describeIntCell(0),
			"PAD_ATTRIBUTE":      describeStringCell("NO PAD"),
		},
		{
			"COLLATION_NAME":     describeStringCell("utf8mb4_bin"),
			"CHARACTER_SET_NAME": describeStringCell("utf8mb4"),
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

func informationSchemaProjectedResult(rows []informationSchemaRow, projections []qsbridge.ProjectionColumn) (ExecutionResult, qsbridge.DiagnosticSet) {
	rownums := make([]qsbridge.QuantaRownum, len(rows))
	vectors := make([]qsbridge.QuantaProjectionVector, 0, len(projections))
	for _, projection := range projections {
		field, ok := informationSchemaProjectionField(projection)
		if !ok {
			return ExecutionResult{}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "information_schema only supports direct column projections"),
			}
		}
		column := projection.ResultColumn()
		vector := describeProjectionVector(column.Name, column.Type, len(rows))
		for i, row := range rows {
			rownums[i] = qsbridge.QuantaRownum(i + 1)
			vector.Values[i] = rowCell(row, field)
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

package qsruntime

import (
	"strconv"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func showCreateViewRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	target := request.Bound.Prepared.Query.Mutation.Target
	viewName := strings.TrimSpace(target.Table)
	if viewName == "" {
		viewName = strings.TrimSpace(string(target.ID))
	}
	sql := strings.TrimSpace(request.Bound.Prepared.Query.Mutation.ViewSQL)
	createSQL := strings.TrimSpace(sql)
	if createSQL != "" && !strings.HasPrefix(strings.ToLower(createSQL), "create ") {
		createSQL = "CREATE VIEW " + showCreateQualifiedViewName(target.Schema, viewName) + " AS " + createSQL
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:   "catalog",
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{
				{
					Field:  qsbridge.QuantaProjectionField{Index: "catalog", Field: "View", Type: qsbridge.DataTypeString, Visible: true},
					Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: viewName}},
				},
				{
					Field:  qsbridge.QuantaProjectionField{Index: "catalog", Field: "Create View", Type: qsbridge.DataTypeString, Visible: true},
					Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: createSQL}},
				},
			},
		},
		Count: 1,
	}
}

func showCreateTableRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	query := request.Bound.Prepared.Query
	target := query.Mutation.Target
	tableName := strings.TrimSpace(target.Table)
	if tableName == "" {
		tableName = strings.TrimSpace(string(target.ID))
	}
	createSQL := renderCreateTableSQL(tableName, query.Mutation.Columns)
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:   "catalog",
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{
				{
					Field:  qsbridge.QuantaProjectionField{Index: "catalog", Field: "Table", Type: qsbridge.DataTypeString, Visible: true},
					Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: tableName}},
				},
				{
					Field:  qsbridge.QuantaProjectionField{Index: "catalog", Field: "Create Table", Type: qsbridge.DataTypeString, Visible: true},
					Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: createSQL}},
				},
			},
		},
		Count: 1,
	}
}

func showCreateQualifiedViewName(schema string, viewName string) string {
	schema = strings.TrimSpace(schema)
	viewName = strings.TrimSpace(viewName)
	if schema == "" {
		return viewName
	}
	return schema + "." + viewName
}

func renderCreateTableSQL(tableName string, columns []qsbridge.FieldRef) string {
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		tableName = "unknown"
	}
	lines := make([]string, 0, len(columns)+1)
	primaryKeys := make([]string, 0)
	for _, column := range columns {
		columnName := strings.TrimSpace(column.Name)
		if columnName == "" {
			continue
		}
		line := "  " + quoteSQLIdentifier(columnName) + " " + describeSQLType(column)
		if column.PrimaryKey || !column.Nullable {
			line += " NOT NULL"
		} else {
			line += " DEFAULT NULL"
		}
		lines = append(lines, line)
		if column.PrimaryKey {
			primaryKeys = append(primaryKeys, quoteSQLIdentifier(columnName))
		}
	}
	if len(primaryKeys) > 0 {
		lines = append(lines, "  PRIMARY KEY ("+strings.Join(primaryKeys, ", ")+")")
	}
	if len(lines) == 0 {
		return "CREATE TABLE " + quoteSQLIdentifier(tableName) + " ()"
	}
	return "CREATE TABLE " + quoteSQLIdentifier(tableName) + " (\n" + strings.Join(lines, ",\n") + "\n)"
}

func quoteSQLIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

func showDatabasesRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	schemas := request.Bound.Prepared.Query.Catalog.Schemas
	rownums := make([]qsbridge.QuantaRownum, len(schemas))
	vector := describeProjectionVector("Database", qsbridge.DataTypeString, len(schemas))
	for i, schema := range schemas {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		vector.Values[i] = describeStringCell(schema)
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: []qsbridge.QuantaProjectionVector{vector},
		},
		Count: uint64(len(schemas)),
	}
}

func showTablesRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	query := request.Bound.Prepared.Query
	tables := query.Catalog.Objects
	columnName := showTablesRuntimeColumnName(query.Catalog.Schema)
	if len(query.Result.Columns) > 0 && strings.TrimSpace(query.Result.Columns[0].Name) != "" {
		columnName = query.Result.Columns[0].Name
	}
	rownums := make([]qsbridge.QuantaRownum, len(tables))
	vector := describeProjectionVector(columnName, qsbridge.DataTypeString, len(tables))
	for i, table := range tables {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		vector.Values[i] = describeStringCell(table.Table)
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: []qsbridge.QuantaProjectionVector{vector},
		},
		Count: uint64(len(tables)),
	}
}

func showTablesRuntimeColumnName(schemaName string) string {
	schemaName = strings.TrimSpace(schemaName)
	if schemaName == "" {
		return "Tables_in"
	}
	return "Tables_in_" + schemaName
}

func describeRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	columns := request.Bound.Prepared.Query.Mutation.Columns
	rownums := make([]qsbridge.QuantaRownum, len(columns))
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("Field", qsbridge.DataTypeString, len(columns)),
		describeProjectionVector("Type", qsbridge.DataTypeString, len(columns)),
		describeProjectionVector("Null", qsbridge.DataTypeString, len(columns)),
		describeProjectionVector("Key", qsbridge.DataTypeString, len(columns)),
		describeProjectionVector("Default", qsbridge.DataTypeString, len(columns)),
		describeProjectionVector("Extra", qsbridge.DataTypeString, len(columns)),
	}
	for i, column := range columns {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		vectors[0].Values[i] = describeStringCell(column.Name)
		vectors[1].Values[i] = describeStringCell(describeSQLType(column))
		vectors[2].Values[i] = describeStringCell(describeNullability(column))
		vectors[3].Values[i] = describeStringCell(describeKey(column))
		vectors[4].Values[i] = qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
		vectors[5].Values[i] = describeStringCell(describeExtra(column))
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: vectors,
		},
		Count: uint64(len(columns)),
	}
}

func describeProjectionVector(name string, dataType qsbridge.DataType, rows int) qsbridge.QuantaProjectionVector {
	return qsbridge.QuantaProjectionVector{
		Field:  qsbridge.QuantaProjectionField{Index: "catalog", Field: name, Type: dataType, Visible: true},
		Values: make([]qsbridge.ResultCell, rows),
	}
}

func describeStringCell(value string) qsbridge.ResultCell {
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: value}
}

func describeSQLType(field qsbridge.FieldRef) string {
	switch field.Type {
	case qsbridge.DataTypeBool:
		return "tinyint(1)"
	case qsbridge.DataTypeInt:
		return "int"
	case qsbridge.DataTypeFloat:
		if field.Encoding.Scale > 0 {
			return "decimal(15," + strconv.Itoa(field.Encoding.Scale) + ")"
		}
		return "double"
	case qsbridge.DataTypeString:
		if field.Encoding.MaxLength > 0 {
			return "varchar(" + strconv.Itoa(field.Encoding.MaxLength) + ")"
		}
		return "varchar"
	case qsbridge.DataTypeTime:
		return "datetime"
	default:
		return "unknown"
	}
}

func describeNullability(field qsbridge.FieldRef) string {
	if field.Nullable {
		return "YES"
	}
	return "NO"
}

func describeKey(field qsbridge.FieldRef) string {
	if field.PrimaryKey {
		return "PRI"
	}
	return ""
}

func describeExtra(field qsbridge.FieldRef) string {
	if field.Encoding.LegacyName != "" {
		return "mapper=" + field.Encoding.LegacyName
	}
	if field.Encoding.Kind != "" {
		return "mapper=" + string(field.Encoding.Kind)
	}
	if field.Index != "" {
		return "mapper=" + string(field.Index)
	}
	return ""
}

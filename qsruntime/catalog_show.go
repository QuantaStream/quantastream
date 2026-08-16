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
				{
					Field:  qsbridge.QuantaProjectionField{Index: "catalog", Field: "character_set_client", Type: qsbridge.DataTypeString, Visible: true},
					Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "utf8mb4"}},
				},
				{
					Field:  qsbridge.QuantaProjectionField{Index: "catalog", Field: "collation_connection", Type: qsbridge.DataTypeString, Visible: true},
					Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: "utf8mb4_0900_ai_ci"}},
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

func showCreateDatabaseRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	schemaName := strings.TrimSpace(request.Bound.Prepared.Query.Catalog.Schema)
	if schemaName == "" {
		schemaName = strings.TrimSpace(request.Bound.Prepared.Session.CurrentSchema)
	}
	createSQL := "CREATE DATABASE " + quoteSQLIdentifier(schemaName) + " /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */"
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:   "catalog",
			Rownums: []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{
				{
					Field:  qsbridge.QuantaProjectionField{Index: "catalog", Field: "Database", Type: qsbridge.DataTypeString, Visible: true},
					Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueString, Value: schemaName}},
				},
				{
					Field:  qsbridge.QuantaProjectionField{Index: "catalog", Field: "Create Database", Type: qsbridge.DataTypeString, Visible: true},
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

func showIndexRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	query := request.Bound.Prepared.Query
	rows := showIndexRows(query.Mutation.Target, query.Mutation.Columns)
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("Table", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Non_unique", qsbridge.DataTypeInt, len(rows)),
		describeProjectionVector("Key_name", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Seq_in_index", qsbridge.DataTypeInt, len(rows)),
		describeProjectionVector("Column_name", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Collation", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Cardinality", qsbridge.DataTypeInt, len(rows)),
		describeProjectionVector("Sub_part", qsbridge.DataTypeInt, len(rows)),
		describeProjectionVector("Packed", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Null", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Index_type", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Comment", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Index_comment", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Visible", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Expression", qsbridge.DataTypeString, len(rows)),
	}
	rownums := make([]qsbridge.QuantaRownum, len(rows))
	for i, row := range rows {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		vectors[0].Values[i] = describeStringCell(row.Table)
		vectors[1].Values[i] = describeIntCell(row.NonUnique)
		vectors[2].Values[i] = describeStringCell(row.KeyName)
		vectors[3].Values[i] = describeIntCell(row.SeqInIndex)
		vectors[4].Values[i] = describeStringCell(row.ColumnName)
		vectors[5].Values[i] = describeStringCell(row.Collation)
		vectors[6].Values[i] = describeNullCell()
		vectors[7].Values[i] = describeNullCell()
		vectors[8].Values[i] = describeNullCell()
		vectors[9].Values[i] = describeStringCell(row.Null)
		vectors[10].Values[i] = describeStringCell(row.IndexType)
		vectors[11].Values[i] = describeStringCell(row.Comment)
		vectors[12].Values[i] = describeStringCell(row.IndexComment)
		vectors[13].Values[i] = describeStringCell("YES")
		vectors[14].Values[i] = describeNullCell()
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: vectors,
		},
		Count: uint64(len(rows)),
	}
}

type showIndexRow struct {
	Table        string
	NonUnique    int64
	KeyName      string
	SeqInIndex   int64
	ColumnName   string
	Collation    string
	Null         string
	IndexType    string
	Comment      string
	IndexComment string
}

func showIndexRows(target qsbridge.TableInstance, columns []qsbridge.FieldRef) []showIndexRow {
	tableName := strings.TrimSpace(target.Table)
	if tableName == "" {
		tableName = strings.TrimSpace(string(target.ID))
	}
	rows := make([]showIndexRow, 0, len(columns))
	primarySeq := int64(1)
	for _, column := range columns {
		if !column.PrimaryKey {
			continue
		}
		rows = append(rows, showIndexRowForColumn(tableName, column, "PRIMARY", 0, primarySeq, "primary_key=true"))
		primarySeq++
	}
	for _, column := range columns {
		if column.PrimaryKey || !showIndexColumnHasMapper(column) {
			continue
		}
		keyName := "qs_" + strings.TrimSpace(column.Name)
		rows = append(rows, showIndexRowForColumn(tableName, column, keyName, 1, 1, ""))
	}
	return rows
}

func showIndexRowForColumn(tableName string, column qsbridge.FieldRef, keyName string, nonUnique int64, seq int64, prefix string) showIndexRow {
	comment := describeExtra(column)
	indexComment := strings.TrimSpace(strings.Join(nonEmptyStrings(prefix, comment), " "))
	nullValue := ""
	if column.Nullable {
		nullValue = "YES"
	}
	return showIndexRow{
		Table:        tableName,
		NonUnique:    nonUnique,
		KeyName:      keyName,
		SeqInIndex:   seq,
		ColumnName:   column.Name,
		Collation:    "A",
		Null:         nullValue,
		IndexType:    "QUANTA",
		Comment:      comment,
		IndexComment: indexComment,
	}
}

func showIndexColumnHasMapper(column qsbridge.FieldRef) bool {
	return strings.TrimSpace(describeExtra(column)) != ""
}

func nonEmptyStrings(values ...string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func showTableStatusRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	query := request.Bound.Prepared.Query
	objects := query.Catalog.Objects
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("Name", qsbridge.DataTypeString, len(objects)),
		describeProjectionVector("Engine", qsbridge.DataTypeString, len(objects)),
		describeProjectionVector("Version", qsbridge.DataTypeInt, len(objects)),
		describeProjectionVector("Row_format", qsbridge.DataTypeString, len(objects)),
		describeProjectionVector("Rows", qsbridge.DataTypeInt, len(objects)),
		describeProjectionVector("Avg_row_length", qsbridge.DataTypeInt, len(objects)),
		describeProjectionVector("Data_length", qsbridge.DataTypeInt, len(objects)),
		describeProjectionVector("Max_data_length", qsbridge.DataTypeInt, len(objects)),
		describeProjectionVector("Index_length", qsbridge.DataTypeInt, len(objects)),
		describeProjectionVector("Data_free", qsbridge.DataTypeInt, len(objects)),
		describeProjectionVector("Auto_increment", qsbridge.DataTypeInt, len(objects)),
		describeProjectionVector("Create_time", qsbridge.DataTypeTime, len(objects)),
		describeProjectionVector("Update_time", qsbridge.DataTypeTime, len(objects)),
		describeProjectionVector("Check_time", qsbridge.DataTypeTime, len(objects)),
		describeProjectionVector("Collation", qsbridge.DataTypeString, len(objects)),
		describeProjectionVector("Checksum", qsbridge.DataTypeInt, len(objects)),
		describeProjectionVector("Create_options", qsbridge.DataTypeString, len(objects)),
		describeProjectionVector("Comment", qsbridge.DataTypeString, len(objects)),
	}
	rownums := make([]qsbridge.QuantaRownum, len(objects))
	for i, object := range objects {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		objectType := "BASE TABLE"
		if i < len(query.Catalog.ObjectTypes) && strings.TrimSpace(query.Catalog.ObjectTypes[i]) != "" {
			objectType = query.Catalog.ObjectTypes[i]
		}
		isView := strings.EqualFold(objectType, "VIEW")
		vectors[0].Values[i] = describeStringCell(object.Table)
		if isView {
			vectors[1].Values[i] = describeNullCell()
			vectors[2].Values[i] = describeNullCell()
			vectors[3].Values[i] = describeNullCell()
		} else {
			vectors[1].Values[i] = describeStringCell("QUANTA")
			vectors[2].Values[i] = describeIntCell(10)
			vectors[3].Values[i] = describeStringCell("Compressed")
		}
		for column := 4; column <= 13; column++ {
			vectors[column].Values[i] = describeNullCell()
		}
		vectors[14].Values[i] = describeStringCell("utf8mb4_0900_ai_ci")
		vectors[15].Values[i] = describeNullCell()
		vectors[16].Values[i] = describeStringCell("")
		vectors[17].Values[i] = describeStringCell(objectType)
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: vectors,
		},
		Count: uint64(len(objects)),
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
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector(columnName, qsbridge.DataTypeString, len(tables)),
	}
	if query.Catalog.Full {
		typeColumn := "Table_type"
		if len(query.Result.Columns) > 1 && strings.TrimSpace(query.Result.Columns[1].Name) != "" {
			typeColumn = query.Result.Columns[1].Name
		}
		vectors = append(vectors, describeProjectionVector(typeColumn, qsbridge.DataTypeString, len(tables)))
	}
	for i, table := range tables {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		vectors[0].Values[i] = describeStringCell(table.Table)
		if query.Catalog.Full && len(vectors) > 1 {
			objectType := "BASE TABLE"
			if i < len(query.Catalog.ObjectTypes) && strings.TrimSpace(query.Catalog.ObjectTypes[i]) != "" {
				objectType = query.Catalog.ObjectTypes[i]
			}
			vectors[1].Values[i] = describeStringCell(objectType)
		}
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: vectors,
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

func showVariablesRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	rows := showVariableRows(request.Bound.Prepared.Query.Catalog.Pattern)
	rownums := make([]qsbridge.QuantaRownum, len(rows))
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("Variable_name", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Value", qsbridge.DataTypeString, len(rows)),
	}
	for i, row := range rows {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		vectors[0].Values[i] = describeStringCell(row.name)
		vectors[1].Values[i] = describeStringCell(row.value)
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: vectors,
		},
		Count: uint64(len(rows)),
	}
}

type showVariableRow struct {
	name  string
	value string
}

func showVariableRows(pattern string) []showVariableRow {
	all := []showVariableRow{
		{name: "autocommit", value: "ON"},
		{name: "character_set_client", value: "utf8mb4"},
		{name: "character_set_connection", value: "utf8mb4"},
		{name: "character_set_results", value: "utf8mb4"},
		{name: "collation_connection", value: "utf8mb4_0900_ai_ci"},
		{name: "lower_case_table_names", value: "0"},
		{name: "sql_mode", value: ""},
		{name: "version", value: "8.0.0-quantastream"},
		{name: "version_comment", value: "QuantaStream"},
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return all
	}
	filtered := make([]showVariableRow, 0, len(all))
	for _, row := range all {
		if sqlLikeMatch(row.name, pattern) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func showWarningsRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	_ = request
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:   "catalog",
			Rownums: []qsbridge.QuantaRownum{},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{
				describeProjectionVector("Level", qsbridge.DataTypeString, 0),
				describeProjectionVector("Code", qsbridge.DataTypeInt, 0),
				describeProjectionVector("Message", qsbridge.DataTypeString, 0),
			},
		},
		Count: 0,
	}
}

func showCharacterSetRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	rows := showCharacterSetRows(request.Bound.Prepared.Query.Catalog.Pattern)
	rownums := make([]qsbridge.QuantaRownum, len(rows))
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("Charset", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Description", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Default collation", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Maxlen", qsbridge.DataTypeInt, len(rows)),
	}
	for i, row := range rows {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		vectors[0].Values[i] = describeStringCell(row.charset)
		vectors[1].Values[i] = describeStringCell(row.description)
		vectors[2].Values[i] = describeStringCell(row.defaultCollation)
		vectors[3].Values[i] = describeIntCell(row.maxlen)
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: vectors,
		},
		Count: uint64(len(rows)),
	}
}

type showCharacterSetRow struct {
	charset          string
	description      string
	defaultCollation string
	maxlen           int64
}

func showCharacterSetRows(pattern string) []showCharacterSetRow {
	all := []showCharacterSetRow{
		{charset: "utf8mb4", description: "UTF-8 Unicode", defaultCollation: "utf8mb4_0900_ai_ci", maxlen: 4},
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return all
	}
	filtered := make([]showCharacterSetRow, 0, len(all))
	for _, row := range all {
		if sqlLikeMatch(row.charset, pattern) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func showCollationRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	rows := showCollationRows(request.Bound.Prepared.Query.Catalog.Pattern)
	rownums := make([]qsbridge.QuantaRownum, len(rows))
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("Collation", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Charset", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Id", qsbridge.DataTypeInt, len(rows)),
		describeProjectionVector("Default", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Compiled", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Sortlen", qsbridge.DataTypeInt, len(rows)),
	}
	for i, row := range rows {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		vectors[0].Values[i] = describeStringCell(row.collation)
		vectors[1].Values[i] = describeStringCell(row.charset)
		vectors[2].Values[i] = describeIntCell(row.id)
		vectors[3].Values[i] = describeStringCell(row.defaultValue)
		vectors[4].Values[i] = describeStringCell(row.compiled)
		vectors[5].Values[i] = describeIntCell(row.sortlen)
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: vectors,
		},
		Count: uint64(len(rows)),
	}
}

type showCollationRow struct {
	collation    string
	charset      string
	id           int64
	defaultValue string
	compiled     string
	sortlen      int64
}

func showCollationRows(pattern string) []showCollationRow {
	all := []showCollationRow{
		{collation: "utf8mb4_0900_ai_ci", charset: "utf8mb4", id: 255, defaultValue: "Yes", compiled: "Yes", sortlen: 0},
		{collation: "utf8mb4_bin", charset: "utf8mb4", id: 46, defaultValue: "", compiled: "Yes", sortlen: 1},
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return all
	}
	filtered := make([]showCollationRow, 0, len(all))
	for _, row := range all {
		if sqlLikeMatch(row.collation, pattern) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func sqlLikeMatch(value string, pattern string) bool {
	value = strings.ToLower(value)
	pattern = strings.ToLower(pattern)
	if pattern == "%" {
		return true
	}
	if !strings.Contains(pattern, "%") {
		return value == pattern
	}
	parts := strings.Split(pattern, "%")
	position := 0
	for i, part := range parts {
		if part == "" {
			continue
		}
		found := strings.Index(value[position:], part)
		if found < 0 {
			return false
		}
		if i == 0 && !strings.HasPrefix(pattern, "%") && found != 0 {
			return false
		}
		position += found + len(part)
	}
	last := parts[len(parts)-1]
	if last != "" && !strings.HasSuffix(pattern, "%") && !strings.HasSuffix(value, last) {
		return false
	}
	return true
}

func describeRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	query := request.Bound.Prepared.Query
	columns := query.Mutation.Columns
	full := query.Catalog.Full
	rownums := make([]qsbridge.QuantaRownum, len(columns))
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("Field", qsbridge.DataTypeString, len(columns)),
		describeProjectionVector("Type", qsbridge.DataTypeString, len(columns)),
	}
	if full {
		vectors = append(vectors, describeProjectionVector("Collation", qsbridge.DataTypeString, len(columns)))
	}
	vectors = append(vectors,
		describeProjectionVector("Null", qsbridge.DataTypeString, len(columns)),
		describeProjectionVector("Key", qsbridge.DataTypeString, len(columns)),
		describeProjectionVector("Default", qsbridge.DataTypeString, len(columns)),
		describeProjectionVector("Extra", qsbridge.DataTypeString, len(columns)),
	)
	if full {
		vectors = append(vectors,
			describeProjectionVector("Privileges", qsbridge.DataTypeString, len(columns)),
			describeProjectionVector("Comment", qsbridge.DataTypeString, len(columns)),
		)
	}
	for i, column := range columns {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		vectors[0].Values[i] = describeStringCell(column.Name)
		vectors[1].Values[i] = describeStringCell(describeSQLType(column))
		offset := 2
		if full {
			vectors[offset].Values[i] = describeCollation(column)
			offset++
		}
		vectors[offset].Values[i] = describeStringCell(describeNullability(column))
		vectors[offset+1].Values[i] = describeStringCell(describeKey(column))
		vectors[offset+2].Values[i] = describeNullCell()
		vectors[offset+3].Values[i] = describeStringCell(describeExtra(column))
		if full {
			vectors[offset+4].Values[i] = describeStringCell("select,insert,update,references")
			vectors[offset+5].Values[i] = describeStringCell("")
		}
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

func describeCollation(field qsbridge.FieldRef) qsbridge.ResultCell {
	if field.Type == qsbridge.DataTypeString {
		return describeStringCell("utf8mb4_0900_ai_ci")
	}
	return describeNullCell()
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

func describeIntCell(value int64) qsbridge.ResultCell {
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: value}
}

func describeNullCell() qsbridge.ResultCell {
	return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
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

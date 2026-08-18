package qsruntime

import (
	"fmt"
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
	indexComment := strings.TrimSpace(strings.Join(nonEmptyStrings(prefix, describeIndexParameters(column)), " "))
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
		IndexType:    "QUANTASTREAM",
		Comment:      comment,
		IndexComment: indexComment,
	}
}

func showIndexColumnHasMapper(column qsbridge.FieldRef) bool {
	return strings.TrimSpace(describeExtra(column)) != ""
}

func describeIndexParameters(field qsbridge.FieldRef) string {
	encoding := field.Encoding
	params := make([]string, 0, 6)
	if encoding.Multiplicity == qsbridge.MultiplicitySet {
		params = append(params, "multiplicity=set")
	}
	if encoding.Scale > 0 {
		params = append(params, "scale="+strconv.Itoa(encoding.Scale))
	}
	if encoding.Granularity != qsbridge.TimeGranularityUnknown {
		params = append(params, "granularity="+string(encoding.Granularity))
	}
	if encoding.PrefixLength > 0 {
		params = append(params, "prefix_length="+strconv.Itoa(encoding.PrefixLength))
	}
	if encoding.MaxLength > 0 {
		params = append(params, "max_length="+strconv.Itoa(encoding.MaxLength))
	}
	if strings.TrimSpace(encoding.RemainderStore) != "" {
		params = append(params, "remainder_store="+strings.TrimSpace(encoding.RemainderStore))
	}
	if encoding.Search.Enabled {
		params = append(params, "searchable=true")
		if strings.TrimSpace(encoding.Search.Mode) != "" {
			params = append(params, "search_mode="+strings.TrimSpace(encoding.Search.Mode))
		}
	}
	return strings.Join(params, " ")
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
			vectors[1].Values[i] = describeStringCell("QUANTASTREAM")
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
	objectTypes := query.Catalog.ObjectTypes
	if query.Catalog.Full && strings.TrimSpace(query.Catalog.Pattern) != "" {
		tables, objectTypes = filterShowTablesByObjectType(tables, objectTypes, query.Catalog.Pattern)
	}
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
			if i < len(objectTypes) && strings.TrimSpace(objectTypes[i]) != "" {
				objectType = objectTypes[i]
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

func filterShowTablesByObjectType(tables []qsbridge.TableInstance, objectTypes []string, pattern string) ([]qsbridge.TableInstance, []string) {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return tables, objectTypes
	}
	filteredTables := make([]qsbridge.TableInstance, 0, len(tables))
	filteredTypes := make([]string, 0, len(tables))
	for i, table := range tables {
		objectType := "BASE TABLE"
		if i < len(objectTypes) && strings.TrimSpace(objectTypes[i]) != "" {
			objectType = objectTypes[i]
		}
		if strings.EqualFold(objectType, pattern) || sqlLikeMatch(objectType, pattern) {
			filteredTables = append(filteredTables, table)
			filteredTypes = append(filteredTypes, objectType)
		}
	}
	return filteredTables, filteredTypes
}

func showOpenTablesRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	query := request.Bound.Prepared.Query
	objects := query.Catalog.Objects
	rownums := make([]qsbridge.QuantaRownum, len(objects))
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("Database", qsbridge.DataTypeString, len(objects)),
		describeProjectionVector("Table", qsbridge.DataTypeString, len(objects)),
		describeProjectionVector("In_use", qsbridge.DataTypeInt, len(objects)),
		describeProjectionVector("Name_locked", qsbridge.DataTypeInt, len(objects)),
	}
	for i, object := range objects {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		vectors[0].Values[i] = describeStringCell(object.Schema)
		vectors[1].Values[i] = describeStringCell(object.Table)
		vectors[2].Values[i] = describeIntCell(0)
		vectors[3].Values[i] = describeIntCell(0)
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

func showTableTypesRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	return showEnginesRuntimeResult(request)
}

func showFunctionStatusRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	query := request.Bound.Prepared.Query
	functions := query.Catalog.Functions
	rownums := make([]qsbridge.QuantaRownum, len(functions))
	vectors := routineStatusProjectionVectors(len(functions))
	dbName := routineStatusDatabase(request)
	definer := routineStatusDefiner(request)
	for i, function := range functions {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		vectors[0].Values[i] = describeStringCell(dbName)
		vectors[1].Values[i] = describeStringCell(function.Name)
		vectors[2].Values[i] = describeStringCell("FUNCTION")
		vectors[3].Values[i] = describeStringCell(definer)
		vectors[4].Values[i] = describeNullCell()
		vectors[5].Values[i] = describeNullCell()
		vectors[6].Values[i] = describeStringCell("DEFINER")
		vectors[7].Values[i] = describeStringCell(functionStatusComment(function))
		vectors[8].Values[i] = describeStringCell("utf8mb4")
		vectors[9].Values[i] = describeStringCell("utf8mb4_0900_ai_ci")
		vectors[10].Values[i] = describeStringCell("utf8mb4_0900_ai_ci")
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: vectors,
		},
		Count: uint64(len(functions)),
	}
}

func showProcedureStatusRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	return emptyMetadataRuntimeResult(request.Bound.Prepared.Query.Result.Columns)
}

func showTriggersRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	return emptyMetadataRuntimeResult(request.Bound.Prepared.Query.Result.Columns)
}

func showEventsRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	return emptyMetadataRuntimeResult(request.Bound.Prepared.Query.Result.Columns)
}

func routineStatusProjectionVectors(count int) []qsbridge.QuantaProjectionVector {
	return []qsbridge.QuantaProjectionVector{
		describeProjectionVector("Db", qsbridge.DataTypeString, count),
		describeProjectionVector("Name", qsbridge.DataTypeString, count),
		describeProjectionVector("Type", qsbridge.DataTypeString, count),
		describeProjectionVector("Definer", qsbridge.DataTypeString, count),
		describeProjectionVector("Modified", qsbridge.DataTypeTime, count),
		describeProjectionVector("Created", qsbridge.DataTypeTime, count),
		describeProjectionVector("Security_type", qsbridge.DataTypeString, count),
		describeProjectionVector("Comment", qsbridge.DataTypeString, count),
		describeProjectionVector("character_set_client", qsbridge.DataTypeString, count),
		describeProjectionVector("collation_connection", qsbridge.DataTypeString, count),
		describeProjectionVector("Database Collation", qsbridge.DataTypeString, count),
	}
}

func emptyMetadataRuntimeResult(columns []qsbridge.FieldRef) ExecutionResult {
	vectors := make([]qsbridge.QuantaProjectionVector, 0, len(columns))
	for _, column := range columns {
		vectors = append(vectors, describeProjectionVector(column.Name, column.Type, 0))
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           []qsbridge.QuantaRownum{},
			ProjectionVectors: vectors,
		},
		Count: 0,
	}
}

func routineStatusDatabase(request qsbridge.ExecutionRequest) string {
	if schema := strings.TrimSpace(request.Bound.Prepared.Query.Catalog.Schema); schema != "" {
		return schema
	}
	if schema := strings.TrimSpace(request.Bound.Prepared.Session.CurrentSchema); schema != "" {
		return schema
	}
	return "quanta"
}

func routineStatusDefiner(request qsbridge.ExecutionRequest) string {
	user := strings.TrimSpace(string(request.Bound.Prepared.Session.User))
	if user == "" {
		user = "MOLIG004"
	}
	return user + "@%"
}

func functionStatusComment(function qsbridge.FunctionDefinition) string {
	origin := strings.TrimSpace(string(function.Origin))
	if origin == "" {
		origin = "unknown"
	}
	placement := strings.TrimSpace(string(function.EffectivePlacement()))
	if placement == "" {
		placement = "unknown"
	}
	return fmt.Sprintf("kind=%s origin=%s placement=%s native=%t deterministic=%t", function.Kind, origin, placement, function.Native, function.Deterministic)
}

func showTablesRuntimeColumnName(schemaName string) string {
	schemaName = strings.TrimSpace(schemaName)
	if schemaName == "" {
		return "Tables_in"
	}
	return "Tables_in_" + schemaName
}

func (r SQLRuntime) showVariablesRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	catalog := request.Bound.Prepared.Query.Catalog
	rows := r.showVariableRows(catalog.Pattern, catalog.Patterns)
	rownums := make([]qsbridge.QuantaRownum, len(rows))
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("variable_name", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("value", qsbridge.DataTypeString, len(rows)),
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

func (r SQLRuntime) showVariableRows(pattern string, patterns []string) []showVariableRow {
	all := []showVariableRow{
		{name: "autocommit", value: showVariableAutocommit(r.Session.Variables["autocommit"])},
		{name: "character_set_client", value: "utf8mb4"},
		{name: "character_set_connection", value: "utf8mb4"},
		{name: "character_set_results", value: "utf8mb4"},
		{name: "character_set_server", value: "utf8mb4"},
		{name: "collation_connection", value: "utf8mb4_0900_ai_ci"},
		{name: "collation_server", value: "utf8mb4_0900_ai_ci"},
		{name: "license", value: "Apache-2.0"},
		{name: "lower_case_table_names", value: "0"},
		{name: "max_allowed_packet", value: "67108864"},
		{name: "protocol_version", value: "10"},
		{name: "sql_mode", value: strings.Join(sqlModeStrings(r.Session.SQLModes), ",")},
		{name: "time_zone", value: runtimeSessionTimeZone(r.Session)},
		{name: "version", value: "8.0.0-quantastream"},
		{name: "version_comment", value: "QuantaStream"},
		{name: "version_compile_machine", value: "x86_64"},
		{name: "version_compile_os", value: "Linux"},
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" && len(patterns) == 0 {
		return all
	}
	filtered := make([]showVariableRow, 0, len(all))
	for _, row := range all {
		if catalogMetadataValueMatches(row.name, pattern, patterns) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func showVariableAutocommit(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "0", "off", "false":
		return "OFF"
	default:
		return "ON"
	}
}

func showStatusRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	catalog := request.Bound.Prepared.Query.Catalog
	rows := showStatusRows(catalog.Pattern, catalog.Patterns)
	rownums := make([]qsbridge.QuantaRownum, len(rows))
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("variable_name", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("value", qsbridge.DataTypeString, len(rows)),
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

type showStatusRow struct {
	name  string
	value string
}

func showStatusRows(pattern string, patterns []string) []showStatusRow {
	all := []showStatusRow{
		{name: "Connections", value: "1"},
		{name: "Questions", value: "0"},
		{name: "Threads_connected", value: "1"},
		{name: "Uptime", value: "0"},
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" && len(patterns) == 0 {
		return all
	}
	filtered := make([]showStatusRow, 0, len(all))
	for _, row := range all {
		if catalogMetadataValueMatches(row.name, pattern, patterns) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func showWarningsRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	return emptyDiagnosticMetadataRuntimeResult(request.Bound.Prepared.Query.Result.Columns)
}

func showErrorsRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	return emptyDiagnosticMetadataRuntimeResult(request.Bound.Prepared.Query.Result.Columns)
}

func emptyDiagnosticMetadataRuntimeResult(columns []qsbridge.FieldRef) ExecutionResult {
	if len(columns) == 0 {
		columns = []qsbridge.FieldRef{
			{Name: "Level", Type: qsbridge.DataTypeString},
			{Name: "Code", Type: qsbridge.DataTypeInt},
			{Name: "Message", Type: qsbridge.DataTypeString},
		}
	}
	vectors := make([]qsbridge.QuantaProjectionVector, 0, len(columns))
	for _, column := range columns {
		vectors = append(vectors, describeProjectionVector(column.Name, column.Type, 0))
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           []qsbridge.QuantaRownum{},
			ProjectionVectors: vectors,
		},
		Count: 0,
	}
}

func showDiagnosticCountRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	columnName := "@@session.warning_count"
	if columns := request.Bound.Prepared.Query.Result.Columns; len(columns) > 0 && strings.TrimSpace(columns[0].Name) != "" {
		columnName = columns[0].Name
	}
	vector := describeProjectionVector(columnName, qsbridge.DataTypeInt, 1)
	vector.Values[0] = describeIntCell(0)
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           []qsbridge.QuantaRownum{1},
			ProjectionVectors: []qsbridge.QuantaProjectionVector{vector},
		},
		Count: 1,
	}
}

func explainRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	query := request.Bound.Prepared.Query
	explainedSQL := strings.TrimSpace(query.Catalog.Pattern)
	tableName := explainTableName(explainedSQL)
	rownums := []qsbridge.QuantaRownum{1}
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("id", qsbridge.DataTypeInt, 1),
		describeProjectionVector("select_type", qsbridge.DataTypeString, 1),
		describeProjectionVector("table", qsbridge.DataTypeString, 1),
		describeProjectionVector("partitions", qsbridge.DataTypeString, 1),
		describeProjectionVector("type", qsbridge.DataTypeString, 1),
		describeProjectionVector("possible_keys", qsbridge.DataTypeString, 1),
		describeProjectionVector("key", qsbridge.DataTypeString, 1),
		describeProjectionVector("key_len", qsbridge.DataTypeString, 1),
		describeProjectionVector("ref", qsbridge.DataTypeString, 1),
		describeProjectionVector("rows", qsbridge.DataTypeInt, 1),
		describeProjectionVector("filtered", qsbridge.DataTypeFloat, 1),
		describeProjectionVector("extra", qsbridge.DataTypeString, 1),
	}
	vectors[0].Values[0] = describeIntCell(1)
	vectors[1].Values[0] = describeStringCell("SIMPLE")
	if tableName == "" {
		vectors[2].Values[0] = describeNullCell()
		vectors[3].Values[0] = describeNullCell()
		vectors[4].Values[0] = describeNullCell()
		vectors[5].Values[0] = describeNullCell()
		vectors[6].Values[0] = describeNullCell()
		vectors[7].Values[0] = describeNullCell()
		vectors[8].Values[0] = describeNullCell()
		vectors[9].Values[0] = describeNullCell()
		vectors[10].Values[0] = describeNullCell()
		vectors[11].Values[0] = describeStringCell("No tables used")
	} else {
		vectors[2].Values[0] = describeStringCell(tableName)
		vectors[3].Values[0] = describeNullCell()
		vectors[4].Values[0] = describeStringCell("QUANTASTREAM")
		vectors[5].Values[0] = describeNullCell()
		vectors[6].Values[0] = describeNullCell()
		vectors[7].Values[0] = describeNullCell()
		vectors[8].Values[0] = describeNullCell()
		vectors[9].Values[0] = describeIntCell(0)
		vectors[10].Values[0] = describeFloatCell(100)
		vectors[11].Values[0] = describeStringCell("QuantaStream native plan; use captured profiles for runtime probes")
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: vectors,
		},
		Count: 1,
	}
}

func explainTableName(sql string) string {
	fields := strings.Fields(sql)
	for i := 0; i+1 < len(fields); i++ {
		if !strings.EqualFold(fields[i], "from") {
			continue
		}
		name := strings.Trim(fields[i+1], "`\"'(),")
		if dot := strings.LastIndex(name, "."); dot >= 0 && dot+1 < len(name) {
			name = name[dot+1:]
		}
		return name
	}
	return ""
}

func showProcesslistRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	session := request.Bound.Prepared.Session
	user := strings.TrimSpace(string(session.User))
	if user == "" {
		user = "MOLIG004"
	}
	info := strings.TrimSpace(request.Bound.Prepared.SQL)
	if !request.Bound.Prepared.Query.Catalog.Full && len(info) > 100 {
		info = info[:100]
	}
	rownums := []qsbridge.QuantaRownum{1}
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("Id", qsbridge.DataTypeInt, 1),
		describeProjectionVector("User", qsbridge.DataTypeString, 1),
		describeProjectionVector("Host", qsbridge.DataTypeString, 1),
		describeProjectionVector("db", qsbridge.DataTypeString, 1),
		describeProjectionVector("Command", qsbridge.DataTypeString, 1),
		describeProjectionVector("Time", qsbridge.DataTypeInt, 1),
		describeProjectionVector("State", qsbridge.DataTypeString, 1),
		describeProjectionVector("Info", qsbridge.DataTypeString, 1),
	}
	vectors[0].Values[0] = describeIntCell(1)
	vectors[1].Values[0] = describeStringCell(user)
	vectors[2].Values[0] = describeStringCell("localhost")
	if strings.TrimSpace(session.CurrentSchema) == "" {
		vectors[3].Values[0] = describeNullCell()
	} else {
		vectors[3].Values[0] = describeStringCell(session.CurrentSchema)
	}
	vectors[4].Values[0] = describeStringCell("Query")
	vectors[5].Values[0] = describeIntCell(0)
	vectors[6].Values[0] = describeStringCell("executing")
	vectors[7].Values[0] = describeStringCell(info)
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: vectors,
		},
		Count: 1,
	}
}

func showEnginesRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	_ = request
	rownums := []qsbridge.QuantaRownum{1}
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("Engine", qsbridge.DataTypeString, 1),
		describeProjectionVector("Support", qsbridge.DataTypeString, 1),
		describeProjectionVector("Comment", qsbridge.DataTypeString, 1),
		describeProjectionVector("Transactions", qsbridge.DataTypeString, 1),
		describeProjectionVector("XA", qsbridge.DataTypeString, 1),
		describeProjectionVector("Savepoints", qsbridge.DataTypeString, 1),
	}
	vectors[0].Values[0] = describeStringCell("QUANTASTREAM")
	vectors[1].Values[0] = describeStringCell("DEFAULT")
	vectors[2].Values[0] = describeStringCell("QuantaStream bitmap-native storage engine")
	vectors[3].Values[0] = describeStringCell("NO")
	vectors[4].Values[0] = describeStringCell("NO")
	vectors[5].Values[0] = describeStringCell("NO")
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: vectors,
		},
		Count: 1,
	}
}

func showPluginsRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	_ = request
	rownums := []qsbridge.QuantaRownum{1}
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("Name", qsbridge.DataTypeString, 1),
		describeProjectionVector("Status", qsbridge.DataTypeString, 1),
		describeProjectionVector("Type", qsbridge.DataTypeString, 1),
		describeProjectionVector("Library", qsbridge.DataTypeString, 1),
		describeProjectionVector("License", qsbridge.DataTypeString, 1),
	}
	vectors[0].Values[0] = describeStringCell("QUANTASTREAM")
	vectors[1].Values[0] = describeStringCell("ACTIVE")
	vectors[2].Values[0] = describeStringCell("STORAGE ENGINE")
	vectors[3].Values[0] = describeNullCell()
	vectors[4].Values[0] = describeStringCell("PROPRIETARY")
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: vectors,
		},
		Count: 1,
	}
}

func showPrivilegesRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	_ = request
	rows := []struct {
		privilege string
		context   string
		comment   string
	}{
		{privilege: "Alter", context: "Tables", comment: "To alter the table"},
		{privilege: "Create", context: "Databases,Tables,Indexes", comment: "To create databases and tables"},
		{privilege: "Create view", context: "Tables", comment: "To create new views"},
		{privilege: "Delete", context: "Tables", comment: "To delete existing rows"},
		{privilege: "Drop", context: "Databases,Tables", comment: "To drop databases, tables, and views"},
		{privilege: "Index", context: "Tables", comment: "To inspect bitmap mapper indexes"},
		{privilege: "Insert", context: "Tables", comment: "To insert data into tables"},
		{privilege: "Select", context: "Tables", comment: "To retrieve rows from tables and views"},
		{privilege: "Show databases", context: "Server Admin", comment: "To see all databases with SHOW DATABASES"},
		{privilege: "Show view", context: "Tables", comment: "To see views with SHOW CREATE VIEW"},
		{privilege: "Update", context: "Tables", comment: "To update existing rows"},
		{privilege: "Usage", context: "Server Admin", comment: "No privileges - allow connect only"},
	}
	rownums := make([]qsbridge.QuantaRownum, len(rows))
	vectors := []qsbridge.QuantaProjectionVector{
		describeProjectionVector("Privilege", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Context", qsbridge.DataTypeString, len(rows)),
		describeProjectionVector("Comment", qsbridge.DataTypeString, len(rows)),
	}
	for i, row := range rows {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		vectors[0].Values[i] = describeStringCell(row.privilege)
		vectors[1].Values[i] = describeStringCell(row.context)
		vectors[2].Values[i] = describeStringCell(row.comment)
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

func showGrantsRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	session := request.Bound.Prepared.Session
	user := strings.TrimSpace(string(session.User))
	if user == "" {
		user = "MOLIG004"
	}
	account := quoteSQLString(user) + "@'%'"
	scope := "*.*"
	if schema := strings.TrimSpace(session.CurrentSchema); schema != "" {
		scope = quoteSQLIdentifier(schema) + ".*"
	}
	rows := []string{
		"GRANT USAGE ON *.* TO " + account,
		"GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, SHOW VIEW ON " + scope + " TO " + account,
	}
	rownums := make([]qsbridge.QuantaRownum, len(rows))
	columnName := "Grants for " + user + "@%"
	vector := describeProjectionVector(columnName, qsbridge.DataTypeString, len(rows))
	for i, row := range rows {
		rownums[i] = qsbridge.QuantaRownum(i + 1)
		vector.Values[i] = describeStringCell(row)
	}
	return ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Index:             "catalog",
			Rownums:           rownums,
			ProjectionVectors: []qsbridge.QuantaProjectionVector{vector},
		},
		Count: uint64(len(rows)),
	}
}

func quoteSQLString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func showCharacterSetRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	catalog := request.Bound.Prepared.Query.Catalog
	rows := showCharacterSetRows(catalog.Pattern, catalog.Patterns)
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

func showCharacterSetRows(pattern string, patterns []string) []showCharacterSetRow {
	all := []showCharacterSetRow{
		{charset: "utf8mb4", description: "UTF-8 Unicode", defaultCollation: "utf8mb4_0900_ai_ci", maxlen: 4},
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" && len(patterns) == 0 {
		return all
	}
	filtered := make([]showCharacterSetRow, 0, len(all))
	for _, row := range all {
		if catalogMetadataValueMatches(row.charset, pattern, patterns) ||
			catalogMetadataValueMatches(row.description, pattern, patterns) ||
			catalogMetadataValueMatches(row.defaultCollation, pattern, patterns) ||
			catalogMetadataValueMatches(strconv.FormatInt(row.maxlen, 10), pattern, patterns) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func showCollationRuntimeResult(request qsbridge.ExecutionRequest) ExecutionResult {
	catalog := request.Bound.Prepared.Query.Catalog
	rows := showCollationRows(catalog.Pattern, catalog.Patterns)
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

func showCollationRows(pattern string, patterns []string) []showCollationRow {
	all := []showCollationRow{
		{collation: "utf8mb4_0900_ai_ci", charset: "utf8mb4", id: 255, defaultValue: "Yes", compiled: "Yes", sortlen: 0},
		{collation: "utf8mb4_bin", charset: "utf8mb4", id: 46, defaultValue: "", compiled: "Yes", sortlen: 1},
	}
	pattern = strings.TrimSpace(pattern)
	if pattern == "" && len(patterns) == 0 {
		return all
	}
	filtered := make([]showCollationRow, 0, len(all))
	for _, row := range all {
		if catalogMetadataValueMatches(row.collation, pattern, patterns) ||
			catalogMetadataValueMatches(row.charset, pattern, patterns) ||
			catalogMetadataValueMatches(strconv.FormatInt(row.id, 10), pattern, patterns) ||
			catalogMetadataValueMatches(row.defaultValue, pattern, patterns) ||
			catalogMetadataValueMatches(row.compiled, pattern, patterns) ||
			catalogMetadataValueMatches(strconv.FormatInt(row.sortlen, 10), pattern, patterns) {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

func catalogMetadataValueMatches(value string, pattern string, patterns []string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern != "" && sqlLikeMatch(value, pattern) {
		return true
	}
	for _, candidate := range patterns {
		candidate = strings.TrimSpace(candidate)
		if candidate != "" && sqlLikeMatch(value, candidate) {
			return true
		}
	}
	return false
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
		if extra := describeExtra(column); extra != "" {
			vectors[offset+3].Values[i] = describeStringCell(extra)
		} else {
			vectors[offset+3].Values[i] = describeStringCell("NULL")
		}
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

func describeFloatCell(value float64) qsbridge.ResultCell {
	return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: value}
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

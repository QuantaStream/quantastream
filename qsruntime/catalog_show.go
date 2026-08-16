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

func showCreateQualifiedViewName(schema string, viewName string) string {
	schema = strings.TrimSpace(schema)
	viewName = strings.TrimSpace(viewName)
	if schema == "" {
		return viewName
	}
	return schema + "." + viewName
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

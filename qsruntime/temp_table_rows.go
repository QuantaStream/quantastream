package qsruntime

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func (r SQLRuntime) insertTemporaryTableRuntimeResult(ctx context.Context, request qsbridge.ExecutionRequest) (ExecutionResult, qsbridge.DiagnosticSet, bool) {
	runtimeRequest := NewSQLExecutionRequest(qsbridge.QuantaIntermediateQuery{}, request)
	mutation := runtimeRequest.Mutation
	if mutation.Kind != qsbridge.MutationInsert {
		return ExecutionResult{}, nil, false
	}
	table, key, ok := r.temporaryTableFromInstance(mutation.Target)
	if !ok {
		return ExecutionResult{}, nil, false
	}
	var rows []qsbridge.TemporaryTableRow
	var diagnostics qsbridge.DiagnosticSet
	if strings.TrimSpace(mutation.SourceSQL) != "" {
		rows, diagnostics = r.temporaryTableInsertSelectRows(ctx, table, key, mutation, request.Options)
	} else {
		rows, diagnostics = r.temporaryTableInsertRows(table, key, mutation)
	}
	if diagnostics.BlocksNative() {
		return ExecutionResult{
			Diagnostics: diagnostics,
			Statement:   qsbridge.StatementResult{Status: "INSERT temporary table failed"},
		}, diagnostics, true
	}
	return ExecutionResult{
		Count: uint64(len(rows)),
		Statement: qsbridge.StatementResult{
			AffectedRows: uint64(len(rows)),
			Status:       fmt.Sprintf("%d rows inserted", len(rows)),
			SessionActions: []qsbridge.SessionAction{{
				Kind:  qsbridge.SessionActionInsertTemporaryRows,
				Name:  table.Name,
				Value: table.Schema,
				Rows:  rows,
			}},
		},
	}, nil, true
}

func (r SQLRuntime) selectTemporaryTableRuntimeResult(request qsbridge.ExecutionRequest) (ExecutionResult, qsbridge.DiagnosticSet, bool) {
	query := request.Bound.Prepared.Query
	if query.Kind != qsbridge.QueryKindSelect || len(query.Sources) != 1 {
		return ExecutionResult{}, nil, false
	}
	table, key, ok := r.temporaryTableFromInstance(query.Sources[0])
	if !ok {
		return ExecutionResult{}, nil, false
	}
	runtimeRequest := NewSQLExecutionRequest(qsbridge.QuantaIntermediateQuery{}, request)
	runtimeRequest = bindTemporaryTableRuntimeParameters(runtimeRequest, request.Bound.Parameters)
	if diagnostics := temporaryTableSelectShapeDiagnostics(runtimeRequest); diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, diagnostics, true
	}

	rowSet := temporaryTableMaterializedRowSet(table, query.Sources[0], r.Session.TemporaryTableRows[key])
	var diagnostics qsbridge.DiagnosticSet
	rowSet, diagnostics = temporaryTableFilterRows(runtimeRequest, rowSet)
	if diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, diagnostics, true
	}
	if len(runtimeRequest.GroupBy) > 0 {
		result := directBitmapMaterializedGroupedAggregateResult(runtimeRequest, rowSet, ExecutionResult{})
		return result, result.Diagnostics, true
	}
	if len(runtimeRequest.SQLAggregates) > 0 {
		result := ExecutionResult{}
		if directBitmapSingleTopNAggregate(runtimeRequest.SQLAggregates) {
			result = directBitmapTopNAggregateResult(runtimeRequest, rowSet, result)
		} else {
			result = directBitmapMaterializedAggregateResult(runtimeRequest, rowSet, result)
		}
		return result, result.Diagnostics, true
	}
	rowSet, diagnostics = directBitmapOrderProjectedRows(runtimeRequest, rowSet)
	if diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, diagnostics, true
	}
	rowSet, diagnostics = directBitmapEvaluateProjectionRowSet(runtimeRequest, rowSet)
	if diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, diagnostics, true
	}
	if runtimeRequest.Result.Distinct {
		rowSet = directBitmapDistinctProjectedRowSet(rowSet)
	}
	rowSet = directBitmapLimitProjectedRowSet(rowSet, runtimeRequest.Result.Offset, runtimeRequest.Result.Limit, runtimeRequest.Result.HasResultLimit())
	if len(runtimeRequest.Projection) == 0 {
		rowSet = directBitmapOrderVisibleProjectedRowSet(rowSet, runtimeRequest.ProjectionOrder)
	}
	if validation := rowSet.ValidateShape(); validation.BlocksNative() {
		return ExecutionResult{Diagnostics: validation}, validation, true
	}
	return ExecutionResult{
		RowSet: rowSet,
		Count:  uint64(rowSet.CandidateCount()),
	}, nil, true
}

func (r SQLRuntime) temporaryTableFromInstance(instance qsbridge.TableInstance) (qsbridge.TableDefinition, string, bool) {
	tableName := strings.TrimSpace(instance.Table)
	if tableName == "" {
		tableName = strings.TrimSpace(string(instance.ID))
	}
	if tableName == "" {
		return qsbridge.TableDefinition{}, "", false
	}
	schemaName := strings.TrimSpace(instance.Schema)
	if schemaName == "" {
		schemaName = r.Session.EffectiveSchema(r.DefaultSchema)
	}
	key := temporaryTableSessionKey(schemaName, tableName)
	table, ok := r.Session.TemporaryTables[key]
	return table, key, ok
}

func (r SQLRuntime) temporaryTableInsertRows(table qsbridge.TableDefinition, key string, mutation qsbridge.MutationShape) ([]qsbridge.TemporaryTableRow, qsbridge.DiagnosticSet) {
	columnIndexes, diagnostics := temporaryTableInsertColumnIndexes(table, mutation.Columns)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	existingKeys := temporaryTablePrimaryKeySet(table, r.Session.TemporaryTableRows[key].Rows)
	rows := make([]qsbridge.TemporaryTableRow, 0, len(mutation.Rows))
	for rowIndex, row := range mutation.Rows {
		if len(row.Values) != len(columnIndexes) {
			return nil, temporaryTableRowsDiagnostics(fmt.Sprintf("INSERT row %d has %d values for %d columns", rowIndex+1, len(row.Values), len(columnIndexes)))
		}
		materialized, diagnostics := temporaryTableInsertRow(table, columnIndexes, row)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		keyValue, hasPrimaryKey := temporaryTablePrimaryKey(table, materialized)
		if hasPrimaryKey {
			if _, exists := existingKeys[keyValue]; exists {
				return nil, temporaryTableRowsDiagnostics("duplicate primary key for temporary table " + qualifiedRuntimeTableName(table.Schema, table.Name))
			}
			existingKeys[keyValue] = struct{}{}
		}
		rows = append(rows, materialized)
	}
	return rows, nil
}

func (r SQLRuntime) temporaryTableInsertSelectRows(ctx context.Context, table qsbridge.TableDefinition, key string, mutation qsbridge.MutationShape, options qsbridge.ExecutionOptions) ([]qsbridge.TemporaryTableRow, qsbridge.DiagnosticSet) {
	columnIndexes, diagnostics := temporaryTableInsertColumnIndexes(table, mutation.Columns)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	sourceSQL := strings.TrimSpace(mutation.SourceSQL)
	if sourceSQL == "" {
		return nil, temporaryTableRowsDiagnostics("INSERT SELECT requires a SELECT statement")
	}
	sourceResult, err := r.ExecuteSQL(ctx, sourceSQL, options)
	if err != nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "INSERT SELECT temporary table source failed: "+err.Error()),
		}
	}
	if sourceResult.Diagnostics.BlocksNative() || sourceResult.Runtime.Diagnostics.BlocksNative() {
		diagnostics := append(qsbridge.DiagnosticSet(nil), sourceResult.Diagnostics...)
		diagnostics = append(diagnostics, sourceResult.Runtime.Diagnostics...)
		return nil, diagnostics
	}
	if len(sourceResult.Request.ResultColumns) != len(columnIndexes) {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "INSERT SELECT result shape changed during execution"),
		}
	}
	sourceRowSet := sourceResult.Runtime.RowSet
	if sourceOrder := projectionOrder(sourceResult.Request.Bound.Prepared.Query.Projection); len(sourceOrder) > 0 {
		sourceRowSet = directBitmapOrderVisibleProjectedRowSet(sourceRowSet, sourceOrder)
	}
	sourceRowSet = temporaryTableOrderVisibleProjectedRowSetByResultColumns(sourceRowSet, sourceResult.Request.ResultColumns)
	chunk, diagnostics := sourceRowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	existingKeys := temporaryTablePrimaryKeySet(table, r.Session.TemporaryTableRows[key].Rows)
	rows := make([]qsbridge.TemporaryTableRow, 0, len(chunk.Rows))
	for rowIndex, row := range chunk.Rows {
		if len(row) != len(columnIndexes) {
			return nil, temporaryTableRowsDiagnostics(fmt.Sprintf("INSERT SELECT row %d has %d values for %d columns", rowIndex+1, len(row), len(columnIndexes)))
		}
		materialized, diagnostics := temporaryTableInsertResultRow(table, columnIndexes, row)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		keyValue, hasPrimaryKey := temporaryTablePrimaryKey(table, materialized)
		if hasPrimaryKey {
			if _, exists := existingKeys[keyValue]; exists {
				return nil, temporaryTableRowsDiagnostics("duplicate primary key for temporary table " + qualifiedRuntimeTableName(table.Schema, table.Name))
			}
			existingKeys[keyValue] = struct{}{}
		}
		rows = append(rows, materialized)
	}
	return rows, nil
}

func temporaryTableInsertColumnIndexes(table qsbridge.TableDefinition, columns []qsbridge.FieldRef) ([]int, qsbridge.DiagnosticSet) {
	if len(columns) == 0 {
		indexes := make([]int, len(table.Fields))
		for i := range indexes {
			indexes[i] = i
		}
		return indexes, nil
	}
	fieldIndexes := temporaryTableFieldIndexes(table)
	indexes := make([]int, 0, len(columns))
	seen := make(map[int]struct{}, len(columns))
	for _, column := range columns {
		name := temporaryTableFieldName(column)
		index, ok := fieldIndexes[strings.ToLower(name)]
		if !ok {
			return nil, temporaryTableRowsDiagnostics("temporary table column not found: " + name)
		}
		if _, exists := seen[index]; exists {
			return nil, temporaryTableRowsDiagnostics("duplicate temporary table INSERT column: " + name)
		}
		seen[index] = struct{}{}
		indexes = append(indexes, index)
	}
	return indexes, nil
}

func temporaryTableInsertRow(table qsbridge.TableDefinition, columnIndexes []int, row qsbridge.MutationRow) (qsbridge.TemporaryTableRow, qsbridge.DiagnosticSet) {
	values := make(qsbridge.ResultRow, len(table.Fields))
	for i := range values {
		values[i] = qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
	}
	for valueIndex, fieldIndex := range columnIndexes {
		cell, diagnostics := temporaryTableEvaluateStaticExpr(row.Values[valueIndex])
		if diagnostics.BlocksNative() {
			return qsbridge.TemporaryTableRow{}, diagnostics
		}
		cell, diagnostics = temporaryTableCoerceCell(cell, table.Fields[fieldIndex])
		if diagnostics.BlocksNative() {
			return qsbridge.TemporaryTableRow{}, diagnostics
		}
		values[fieldIndex] = cell
	}
	for fieldIndex, field := range table.Fields {
		if field.Nullable || !directBitmapNullCell(values[fieldIndex]) {
			continue
		}
		return qsbridge.TemporaryTableRow{}, temporaryTableRowsDiagnostics("temporary table column cannot be NULL: " + field.Name)
	}
	return qsbridge.TemporaryTableRow{Values: values}, nil
}

func temporaryTableInsertResultRow(table qsbridge.TableDefinition, columnIndexes []int, row qsbridge.ResultRow) (qsbridge.TemporaryTableRow, qsbridge.DiagnosticSet) {
	values := make(qsbridge.ResultRow, len(table.Fields))
	for i := range values {
		values[i] = qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
	}
	for valueIndex, fieldIndex := range columnIndexes {
		cell, diagnostics := temporaryTableCoerceCell(row[valueIndex], table.Fields[fieldIndex])
		if diagnostics.BlocksNative() {
			return qsbridge.TemporaryTableRow{}, diagnostics
		}
		values[fieldIndex] = cell
	}
	for fieldIndex, field := range table.Fields {
		if field.Nullable || !directBitmapNullCell(values[fieldIndex]) {
			continue
		}
		return qsbridge.TemporaryTableRow{}, temporaryTableRowsDiagnostics("temporary table column cannot be NULL: " + field.Name)
	}
	return qsbridge.TemporaryTableRow{Values: values}, nil
}

func temporaryTableOrderVisibleProjectedRowSetByResultColumns(rowSet qsbridge.QuantaProjectedRowSet, columns []qsbridge.ResultColumn) qsbridge.QuantaProjectedRowSet {
	if len(columns) == 0 || len(rowSet.ProjectionVectors) == 0 {
		return rowSet
	}
	ordered := make([]qsbridge.QuantaProjectionVector, 0, len(rowSet.ProjectionVectors))
	used := make([]bool, len(rowSet.ProjectionVectors))
	for _, column := range columns {
		for i, vector := range rowSet.ProjectionVectors {
			if used[i] || !vector.Field.Visible || !temporaryTableProjectionVectorMatchesResultColumn(vector, column) {
				continue
			}
			ordered = append(ordered, vector)
			used[i] = true
			break
		}
	}
	for i, vector := range rowSet.ProjectionVectors {
		if used[i] || !vector.Field.Visible {
			continue
		}
		ordered = append(ordered, vector)
	}
	for i, vector := range rowSet.ProjectionVectors {
		if used[i] || vector.Field.Visible {
			continue
		}
		ordered = append(ordered, vector)
	}
	rowSet.ProjectionVectors = ordered
	return rowSet
}

func temporaryTableProjectionVectorMatchesResultColumn(vector qsbridge.QuantaProjectionVector, column qsbridge.ResultColumn) bool {
	fieldName := strings.TrimSpace(vector.Field.Field)
	if fieldName == "" {
		fieldName = strings.TrimSpace(vector.Field.PhysicalName)
	}
	if fieldName == "" {
		return false
	}
	if strings.EqualFold(fieldName, strings.TrimSpace(column.Name)) {
		return true
	}
	source := strings.TrimSpace(column.Source)
	if source == "" {
		return false
	}
	qualified := strings.Trim(strings.TrimSpace(vector.Field.Index)+"."+fieldName, ".")
	if strings.EqualFold(qualified, source) {
		return true
	}
	if dot := strings.LastIndex(source, "."); dot >= 0 && dot+1 < len(source) {
		return strings.EqualFold(fieldName, source[dot+1:])
	}
	return false
}

func temporaryTableEvaluateStaticExpr(expr qsbridge.Expr) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if expr == nil {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	if literal, ok := directBitmapLiteralExpr(expr); ok {
		return directBitmapLiteralCell(literal), nil
	}
	if binary, ok := directBitmapBinaryExpr(expr); ok {
		switch binary.Op {
		case qsbridge.BinaryOpAnd, qsbridge.BinaryOpOr,
			qsbridge.BinaryOpEqual, qsbridge.BinaryOpNotEqual,
			qsbridge.BinaryOpLess, qsbridge.BinaryOpLessEqual,
			qsbridge.BinaryOpGreater, qsbridge.BinaryOpGreaterEqual,
			qsbridge.BinaryOpLike, qsbridge.BinaryOpNotLike,
			qsbridge.BinaryOpRegexp, qsbridge.BinaryOpNotRegexp,
			qsbridge.BinaryOpIn, qsbridge.BinaryOpNotIn,
			qsbridge.BinaryOpBetween, qsbridge.BinaryOpNotBetween:
			matched, diagnostics := temporaryTableEvaluateStaticBoolExpr(binary)
			if diagnostics.BlocksNative() {
				return qsbridge.ResultCell{}, diagnostics
			}
			return qsbridge.ResultCell{Kind: qsbridge.ValueBool, Value: matched}, nil
		}
	}
	return directBitmapEvaluateMaterializedExpr(expr, qsbridge.QuantaProjectedRowSet{}, 0)
}

func temporaryTableEvaluateStaticBoolExpr(expr qsbridge.Expr) (bool, qsbridge.DiagnosticSet) {
	binary, ok := directBitmapBinaryExpr(expr)
	if !ok {
		cell, diagnostics := temporaryTableEvaluateStaticExpr(expr)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		return temporaryTableTruthyCell(cell), nil
	}
	switch binary.Op {
	case qsbridge.BinaryOpAnd, qsbridge.BinaryOpOr:
		left, diagnostics := temporaryTableEvaluateStaticBoolExpr(binary.Left)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		right, diagnostics := temporaryTableEvaluateStaticBoolExpr(binary.Right)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		if binary.Op == qsbridge.BinaryOpAnd {
			return left && right, nil
		}
		return left || right, nil
	case qsbridge.BinaryOpEqual, qsbridge.BinaryOpNotEqual,
		qsbridge.BinaryOpLess, qsbridge.BinaryOpLessEqual,
		qsbridge.BinaryOpGreater, qsbridge.BinaryOpGreaterEqual:
		left, right, diagnostics := temporaryTableEvaluateStaticPair(binary)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		return directBitmapResidualCompareCells(binary.Op, left, right), nil
	case qsbridge.BinaryOpLike, qsbridge.BinaryOpNotLike:
		left, right, diagnostics := temporaryTableEvaluateStaticPair(binary)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		matched := directBitmapResidualLikeCells(left, right)
		if binary.Op == qsbridge.BinaryOpNotLike {
			return !matched, nil
		}
		return matched, nil
	case qsbridge.BinaryOpRegexp, qsbridge.BinaryOpNotRegexp:
		left, right, diagnostics := temporaryTableEvaluateStaticPair(binary)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		matched := directBitmapResidualRegexpCells(left, right)
		if binary.Op == qsbridge.BinaryOpNotRegexp {
			return !matched, nil
		}
		return matched, nil
	case qsbridge.BinaryOpIn, qsbridge.BinaryOpNotIn:
		left, diagnostics := temporaryTableEvaluateStaticExpr(binary.Left)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		list, ok := directBitmapListExpr(binary.Right)
		if !ok {
			return false, temporaryTableRowsDiagnostics("temporary table IN expression requires a list")
		}
		matched := false
		for _, item := range list.Items {
			right, diagnostics := temporaryTableEvaluateStaticExpr(item)
			if diagnostics.BlocksNative() {
				return false, diagnostics
			}
			if directBitmapResidualCompareCells(qsbridge.BinaryOpEqual, left, right) {
				matched = true
				break
			}
		}
		if binary.Op == qsbridge.BinaryOpNotIn {
			return !matched, nil
		}
		return matched, nil
	case qsbridge.BinaryOpBetween, qsbridge.BinaryOpNotBetween:
		left, diagnostics := temporaryTableEvaluateStaticExpr(binary.Left)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		list, ok := directBitmapListExpr(binary.Right)
		if !ok || len(list.Items) != 2 {
			return false, temporaryTableRowsDiagnostics("temporary table BETWEEN expression requires two bounds")
		}
		lower, diagnostics := temporaryTableEvaluateStaticExpr(list.Items[0])
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		upper, diagnostics := temporaryTableEvaluateStaticExpr(list.Items[1])
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		matched := directBitmapResidualCompareCells(qsbridge.BinaryOpGreaterEqual, left, lower) &&
			directBitmapResidualCompareCells(qsbridge.BinaryOpLessEqual, left, upper)
		if binary.Op == qsbridge.BinaryOpNotBetween {
			return !matched, nil
		}
		return matched, nil
	default:
		return false, temporaryTableRowsDiagnostics(fmt.Sprintf("temporary table boolean expression does not support operator %s", binary.Op))
	}
}

func temporaryTableEvaluateStaticPair(binary qsbridge.BinaryExpr) (qsbridge.ResultCell, qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	left, diagnostics := temporaryTableEvaluateStaticExpr(binary.Left)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, qsbridge.ResultCell{}, diagnostics
	}
	right, diagnostics := temporaryTableEvaluateStaticExpr(binary.Right)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, qsbridge.ResultCell{}, diagnostics
	}
	return left, right, nil
}

func temporaryTableCoerceCell(cell qsbridge.ResultCell, field qsbridge.FieldDefinition) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if directBitmapNullCell(cell) || field.Type == qsbridge.DataTypeUnknown {
		return cell, nil
	}
	switch field.Type {
	case qsbridge.DataTypeBool:
		value, ok := temporaryTableBoolValue(cell)
		if !ok {
			return qsbridge.ResultCell{}, temporaryTableRowsDiagnostics("temporary table column requires bool value: " + field.Name)
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueBool, Value: value}, nil
	case qsbridge.DataTypeInt:
		value, ok := temporaryTableIntValue(cell)
		if !ok {
			return qsbridge.ResultCell{}, temporaryTableRowsDiagnostics("temporary table column requires integer value: " + field.Name + " got " + temporaryTableCellDebug(cell))
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: value}, nil
	case qsbridge.DataTypeFloat:
		value, ok := directBitmapNumericCellValue(cell)
		if !ok {
			return qsbridge.ResultCell{}, temporaryTableRowsDiagnostics("temporary table column requires numeric value: " + field.Name)
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: value}, nil
	case qsbridge.DataTypeString:
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: fmt.Sprint(cell.Value)}, nil
	case qsbridge.DataTypeTime:
		if value, ok := cell.Value.(time.Time); ok {
			return qsbridge.ResultCell{Kind: qsbridge.ValueTime, Value: value.UTC()}, nil
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueTime, Value: fmt.Sprint(cell.Value)}, nil
	default:
		return cell, nil
	}
}

func temporaryTableCellDebug(cell qsbridge.ResultCell) string {
	value := fmt.Sprint(cell.Value)
	if len(value) > 80 {
		value = value[:80] + "..."
	}
	return fmt.Sprintf("kind=%s type=%T value=%q", cell.Kind, cell.Value, value)
}

func temporaryTableBoolValue(cell qsbridge.ResultCell) (bool, bool) {
	switch value := cell.Value.(type) {
	case bool:
		return value, true
	case string:
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "true", "1", "yes", "on":
			return true, true
		case "false", "0", "no", "off":
			return false, true
		}
	}
	if numeric, ok := directBitmapNumericCellValue(cell); ok {
		return numeric != 0, true
	}
	return false, false
}

func temporaryTableIntValue(cell qsbridge.ResultCell) (int64, bool) {
	switch value := cell.Value.(type) {
	case *big.Int:
		if value == nil || !value.IsInt64() {
			return 0, false
		}
		return value.Int64(), true
	case big.Int:
		if !value.IsInt64() {
			return 0, false
		}
		return value.Int64(), true
	}
	if numeric, ok := directBitmapNumericCellValue(cell); ok {
		if math.Trunc(numeric) != numeric {
			return 0, false
		}
		return int64(numeric), true
	}
	value, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(cell.Value)), 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

func temporaryTableTruthyCell(cell qsbridge.ResultCell) bool {
	if directBitmapNullCell(cell) {
		return false
	}
	if value, ok := temporaryTableBoolValue(cell); ok {
		return value
	}
	if numeric, ok := directBitmapNumericCellValue(cell); ok {
		return numeric != 0
	}
	return fmt.Sprint(cell.Value) != ""
}

func temporaryTablePrimaryKeySet(table qsbridge.TableDefinition, rows []qsbridge.TemporaryTableRow) map[string]struct{} {
	keys := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if key, ok := temporaryTablePrimaryKey(table, row); ok {
			keys[key] = struct{}{}
		}
	}
	return keys
}

func temporaryTablePrimaryKey(table qsbridge.TableDefinition, row qsbridge.TemporaryTableRow) (string, bool) {
	parts := make([]string, 0, len(table.Fields))
	for index, field := range table.Fields {
		if !field.PrimaryKey {
			continue
		}
		if index >= len(row.Values) || directBitmapNullCell(row.Values[index]) {
			return "", false
		}
		parts = append(parts, string(row.Values[index].Kind)+"="+fmt.Sprint(row.Values[index].Value))
	}
	if len(parts) == 0 {
		return "", false
	}
	return strings.Join(parts, "\x00"), true
}

func temporaryTableMaterializedRowSet(table qsbridge.TableDefinition, source qsbridge.TableInstance, data qsbridge.TemporaryTableData) qsbridge.QuantaProjectedRowSet {
	indexName := strings.TrimSpace(source.Table)
	if indexName == "" {
		indexName = table.Name
	}
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:             indexName,
		Rownums:           make([]qsbridge.QuantaRownum, len(data.Rows)),
		ProjectionVectors: make([]qsbridge.QuantaProjectionVector, 0, len(table.Fields)),
	}
	for rowIndex := range data.Rows {
		rowSet.Rownums[rowIndex] = qsbridge.QuantaRownum(rowIndex + 1)
	}
	for fieldIndex, field := range table.Fields {
		fieldName := strings.TrimSpace(field.PhysicalName)
		if fieldName == "" {
			fieldName = field.Name
		}
		vector := qsbridge.QuantaProjectionVector{
			Field: qsbridge.QuantaProjectionField{
				Index:        indexName,
				Field:        fieldName,
				PhysicalName: fieldName,
				Type:         field.Type,
				Visible:      true,
			},
			Values: make([]qsbridge.ResultCell, len(data.Rows)),
		}
		for rowIndex, row := range data.Rows {
			if fieldIndex < len(row.Values) {
				vector.Values[rowIndex] = row.Values[fieldIndex]
				continue
			}
			vector.Values[rowIndex] = qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
	}
	return rowSet
}

func temporaryTableFilterRows(request ExecutionRequest, rowSet qsbridge.QuantaProjectedRowSet) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet) {
	if len(request.Predicates) == 0 || rowSet.CandidateCount() == 0 {
		return rowSet, nil
	}
	keep := make([]int, 0, rowSet.CandidateCount())
	for rowIndex := 0; rowIndex < rowSet.CandidateCount(); rowIndex++ {
		matched, diagnostics := directBitmapEvaluateResidualPredicates(request.Predicates, rowSet, rowIndex)
		if diagnostics.BlocksNative() {
			return qsbridge.QuantaProjectedRowSet{}, diagnostics
		}
		if matched {
			keep = append(keep, rowIndex)
		}
	}
	return directBitmapProjectedRowSetRows(rowSet, keep), nil
}

func temporaryTableSelectShapeDiagnostics(request ExecutionRequest) qsbridge.DiagnosticSet {
	if len(request.Sources) != 1 {
		return temporaryTableRowsDiagnostics("temporary table SELECT supports one source")
	}
	if len(request.Joins) > 0 || len(request.Memberships) > 0 {
		return temporaryTableRowsDiagnostics("temporary table SELECT does not support joins yet")
	}
	if len(request.Aggregates) > 0 {
		return temporaryTableRowsDiagnostics("temporary table SELECT does not support legacy aggregate requests")
	}
	if len(request.Having) > 0 && len(request.GroupBy) == 0 {
		return temporaryTableRowsDiagnostics("temporary table SELECT does not support HAVING without GROUP BY yet")
	}
	return nil
}

func bindTemporaryTableRuntimeParameters(request ExecutionRequest, parameters qsbridge.ParameterBindingSet) ExecutionRequest {
	for index := range request.Projection {
		request.Projection[index].Expr = bindMutationParameterExpr(request.Projection[index].Expr, parameters)
	}
	for index := range request.Predicates {
		request.Predicates[index].Expr = bindMutationParameterExpr(request.Predicates[index].Expr, parameters)
	}
	for index := range request.OrderBy {
		request.OrderBy[index].Expr = bindMutationParameterExpr(request.OrderBy[index].Expr, parameters)
	}
	return request
}

func temporaryTableFieldIndexes(table qsbridge.TableDefinition) map[string]int {
	indexes := make(map[string]int, len(table.Fields)*2)
	for i, field := range table.Fields {
		if field.Name != "" {
			indexes[strings.ToLower(field.Name)] = i
		}
		if field.PhysicalName != "" {
			indexes[strings.ToLower(field.PhysicalName)] = i
		}
	}
	return indexes
}

func temporaryTableFieldName(field qsbridge.FieldRef) string {
	if strings.TrimSpace(field.PhysicalName) != "" {
		return strings.TrimSpace(field.PhysicalName)
	}
	return strings.TrimSpace(field.Name)
}

func temporaryTableRowsDiagnostics(message string) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "temporary table rows: "+message),
	}
}

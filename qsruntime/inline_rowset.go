package qsruntime

import (
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type inlineRowSetRow struct {
	Cells map[string]qsbridge.ResultCell
}

func (r SQLRuntime) inlineRowSetRuntimeResult(request qsbridge.ExecutionRequest) (ExecutionResult, qsbridge.DiagnosticSet, bool) {
	query := request.Bound.Prepared.Query
	if query.Kind != qsbridge.QueryKindSelect || len(query.InlineRows) == 0 {
		return ExecutionResult{}, nil, false
	}
	runtimeRequest := NewSQLExecutionRequest(qsbridge.QuantaIntermediateQuery{}, request)
	runtimeRequest = bindTemporaryTableRuntimeParameters(runtimeRequest, request.Bound.Parameters)
	if diagnostics := inlineRowSetSelectShapeDiagnostics(runtimeRequest, query); diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, diagnostics, true
	}
	rowSet, diagnostics := inlineRowSetMaterializedRowSet(query)
	if diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, diagnostics, true
	}
	rowSet, diagnostics = inlineRowSetApplyMemberships(runtimeRequest, rowSet)
	if diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, diagnostics, true
	}
	rowSet, diagnostics = inlineRowSetFilterRows(runtimeRequest, rowSet)
	if diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, diagnostics, true
	}
	if len(runtimeRequest.GroupBy) > 0 {
		result := directBitmapMaterializedGroupedAggregateResult(runtimeRequest, rowSet, ExecutionResult{})
		return result, result.Diagnostics, true
	}
	if len(runtimeRequest.SQLAggregates) > 0 {
		result := directBitmapMaterializedAggregateResult(runtimeRequest, rowSet, ExecutionResult{})
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

func inlineRowSetSelectShapeDiagnostics(request ExecutionRequest, query qsbridge.QueryIR) qsbridge.DiagnosticSet {
	if len(query.InlineRows) != len(query.Sources) {
		return inlineRowSetDiagnostics("inline UNION ALL derived sources cannot be mixed with stored table sources yet")
	}
	if len(query.Subqueries) > 0 {
		return inlineRowSetDiagnostics("inline UNION ALL derived sources do not support nested subqueries yet")
	}
	for _, membership := range request.Memberships {
		if membership.RightInlineRows == nil {
			return inlineRowSetDiagnostics("inline UNION ALL derived source membership requires an inline right-hand rowset")
		}
	}
	if len(request.Joins) > 0 && len(request.Joins) != len(request.Sources)-1 {
		return inlineRowSetDiagnostics("inline UNION ALL derived source joins must follow source order")
	}
	for _, join := range request.Joins {
		if join.Operator != "" && join.Operator != qsbridge.BinaryOpEqual {
			return inlineRowSetDiagnostics("inline UNION ALL derived source joins only support equality")
		}
		if len(join.On) > 0 {
			return inlineRowSetDiagnostics("inline UNION ALL derived source joins do not support extra ON predicates yet")
		}
		switch join.Kind {
		case "", qsbridge.JoinKindInner, qsbridge.JoinKindLeftOuter:
		default:
			return inlineRowSetDiagnostics("inline UNION ALL derived source joins only support INNER and LEFT JOIN")
		}
	}
	return nil
}

func inlineRowSetMaterializedRowSet(query qsbridge.QueryIR) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet) {
	rowSetsBySource := make(map[qsbridge.TableInstanceID]qsbridge.InlineRowSet, len(query.InlineRows))
	for _, rowSet := range query.InlineRows {
		rowSetsBySource[rowSet.Source.ID] = rowSet
	}
	if len(query.Sources) == 0 {
		return qsbridge.QuantaProjectedRowSet{}, inlineRowSetDiagnostics("inline UNION ALL query has no sources")
	}
	first, ok := rowSetsBySource[query.Sources[0].ID]
	if !ok {
		return qsbridge.QuantaProjectedRowSet{}, inlineRowSetDiagnostics("inline UNION ALL query source is missing row data")
	}
	rows := inlineRowSetRowsForSource(first)
	for joinIndex, join := range query.Joins {
		nextSourceIndex := joinIndex + 1
		if nextSourceIndex >= len(query.Sources) {
			return qsbridge.QuantaProjectedRowSet{}, inlineRowSetDiagnostics("inline UNION ALL join has no right-side source")
		}
		next, ok := rowSetsBySource[query.Sources[nextSourceIndex].ID]
		if !ok {
			return qsbridge.QuantaProjectedRowSet{}, inlineRowSetDiagnostics("inline UNION ALL join source is missing row data")
		}
		rows = inlineRowSetJoinRows(rows, inlineRowSetRowsForSource(next), next, join)
	}
	return inlineRowSetRowsToMaterializedRowSet(query, rows, rowSetsBySource), nil
}

func inlineRowSetRowsForSource(rowSet qsbridge.InlineRowSet) []inlineRowSetRow {
	rows := make([]inlineRowSetRow, 0, len(rowSet.Rows))
	for _, sourceRow := range rowSet.Rows {
		row := inlineRowSetRow{Cells: make(map[string]qsbridge.ResultCell, len(rowSet.Fields)*2)}
		for fieldIndex, field := range rowSet.Fields {
			cell := qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
			if fieldIndex < len(sourceRow) {
				cell = sourceRow[fieldIndex]
			}
			inlineRowSetPutCell(row.Cells, rowSet.Source, field, cell)
		}
		rows = append(rows, row)
	}
	return rows
}

func inlineRowSetJoinRows(leftRows []inlineRowSetRow, rightRows []inlineRowSetRow, rightSource qsbridge.InlineRowSet, join qsbridge.JoinEdge) []inlineRowSetRow {
	joined := make([]inlineRowSetRow, 0)
	for _, left := range leftRows {
		matched := false
		for _, right := range rightRows {
			if !inlineRowSetJoinMatches(left, right, join) {
				continue
			}
			matched = true
			joined = append(joined, inlineRowSetMergeRows(left, right))
		}
		if !matched && join.Kind == qsbridge.JoinKindLeftOuter {
			joined = append(joined, inlineRowSetMergeRows(left, inlineRowSetNullExtendedRow(rightSource)))
		}
	}
	return joined
}

func inlineRowSetJoinMatches(left inlineRowSetRow, right inlineRowSetRow, join qsbridge.JoinEdge) bool {
	leftCell, leftOK := inlineRowSetJoinedCell(join.Left, left, right)
	rightCell, rightOK := inlineRowSetJoinedCell(join.Right, left, right)
	if !leftOK || !rightOK || directBitmapNullCell(leftCell) || directBitmapNullCell(rightCell) {
		return false
	}
	return directBitmapCellEqual(leftCell, rightCell)
}

func inlineRowSetJoinedCell(field qsbridge.FieldRef, left inlineRowSetRow, right inlineRowSetRow) (qsbridge.ResultCell, bool) {
	for _, key := range inlineRowSetFieldLookupKeys(field) {
		if cell, ok := left.Cells[key]; ok {
			return cell, true
		}
		if cell, ok := right.Cells[key]; ok {
			return cell, true
		}
	}
	return qsbridge.ResultCell{}, false
}

func inlineRowSetMergeRows(left inlineRowSetRow, right inlineRowSetRow) inlineRowSetRow {
	cells := make(map[string]qsbridge.ResultCell, len(left.Cells)+len(right.Cells))
	for key, cell := range left.Cells {
		cells[key] = cell
	}
	for key, cell := range right.Cells {
		cells[key] = cell
	}
	return inlineRowSetRow{Cells: cells}
}

func inlineRowSetNullExtendedRow(rowSet qsbridge.InlineRowSet) inlineRowSetRow {
	row := inlineRowSetRow{Cells: make(map[string]qsbridge.ResultCell, len(rowSet.Fields)*2)}
	cell := qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
	for _, field := range rowSet.Fields {
		inlineRowSetPutCell(row.Cells, rowSet.Source, field, cell)
	}
	return row
}

func inlineRowSetRowsToMaterializedRowSet(query qsbridge.QueryIR, rows []inlineRowSetRow, rowSetsBySource map[qsbridge.TableInstanceID]qsbridge.InlineRowSet) qsbridge.QuantaProjectedRowSet {
	indexName := query.Sources[0].Table
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:             indexName,
		Rownums:           make([]qsbridge.QuantaRownum, len(rows)),
		ProjectionVectors: make([]qsbridge.QuantaProjectionVector, 0),
	}
	for rowIndex := range rows {
		rowSet.Rownums[rowIndex] = qsbridge.QuantaRownum(rowIndex + 1)
	}
	for _, source := range query.Sources {
		inlineRows, ok := rowSetsBySource[source.ID]
		if !ok {
			continue
		}
		for _, field := range inlineRows.Fields {
			fieldName := strings.TrimSpace(field.PhysicalName)
			if fieldName == "" {
				fieldName = strings.TrimSpace(field.Name)
			}
			vector := qsbridge.QuantaProjectionVector{
				Field: qsbridge.QuantaProjectionField{
					Index:        source.Table,
					Field:        fieldName,
					PhysicalName: fieldName,
					Type:         field.Type,
					Visible:      true,
				},
				Values: make([]qsbridge.ResultCell, len(rows)),
			}
			for rowIndex, row := range rows {
				cell, ok := row.Cells[inlineRowSetFieldKey(source.ID, fieldName)]
				if !ok {
					cell = qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
				}
				vector.Values[rowIndex] = cell
			}
			rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
		}
	}
	return rowSet
}

func inlineRowSetMaterializedSingle(rowSet qsbridge.InlineRowSet) qsbridge.QuantaProjectedRowSet {
	return inlineRowSetRowsToMaterializedRowSet(qsbridge.QueryIR{
		Sources:    []qsbridge.TableInstance{rowSet.Source},
		InlineRows: []qsbridge.InlineRowSet{rowSet},
	}, inlineRowSetRowsForSource(rowSet), map[qsbridge.TableInstanceID]qsbridge.InlineRowSet{
		rowSet.Source.ID: rowSet,
	})
}

func inlineRowSetApplyMemberships(request ExecutionRequest, rowSet qsbridge.QuantaProjectedRowSet) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet) {
	if len(request.Memberships) == 0 || rowSet.CandidateCount() == 0 {
		return rowSet, nil
	}
	filtered := rowSet
	var diagnostics qsbridge.DiagnosticSet
	for _, membership := range request.Memberships {
		filtered, diagnostics = inlineRowSetApplyMembership(filtered, membership)
		if diagnostics.BlocksNative() {
			return qsbridge.QuantaProjectedRowSet{}, diagnostics
		}
	}
	return filtered, nil
}

func inlineRowSetApplyMembership(rowSet qsbridge.QuantaProjectedRowSet, membership qsbridge.MembershipEdge) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet) {
	if membership.RightInlineRows == nil {
		return qsbridge.QuantaProjectedRowSet{}, inlineRowSetDiagnostics("membership right-hand rowset is missing")
	}
	rightRowSet := inlineRowSetMaterializedSingle(*membership.RightInlineRows)
	if len(membership.Predicates) > 0 {
		var diagnostics qsbridge.DiagnosticSet
		rightRowSet, diagnostics = inlineRowSetFilterRows(ExecutionRequest{Predicates: membership.Predicates}, rightRowSet)
		if diagnostics.BlocksNative() {
			return qsbridge.QuantaProjectedRowSet{}, diagnostics
		}
	}
	rightValues, ok := directBitmapProjectedValues(rightRowSet, membership.Right)
	if !ok {
		return qsbridge.QuantaProjectedRowSet{}, inlineRowSetDiagnostics("membership right field is not present in inline rowset")
	}
	valueSet := make(map[string]struct{}, len(rightValues))
	for _, cell := range rightValues {
		if directBitmapNullCell(cell) {
			continue
		}
		valueSet[directBitmapGroupKey(cell)] = struct{}{}
	}
	leftValues, ok := directBitmapProjectedValues(rowSet, membership.Left)
	if !ok {
		return qsbridge.QuantaProjectedRowSet{}, inlineRowSetDiagnostics("membership left field is not present in inline rowset")
	}
	keep := make([]int, 0, len(leftValues))
	for rowIndex, cell := range leftValues {
		_, matched := valueSet[directBitmapGroupKey(cell)]
		if directBitmapNullCell(cell) {
			matched = false
		}
		include := matched
		if membership.Kind == qsbridge.MembershipAnti {
			include = !matched
		}
		if include {
			keep = append(keep, rowIndex)
		}
	}
	return directBitmapProjectedRowSetRows(rowSet, keep), nil
}

func inlineRowSetPutCell(cells map[string]qsbridge.ResultCell, source qsbridge.TableInstance, field qsbridge.FieldDefinition, cell qsbridge.ResultCell) {
	if field.Name != "" {
		cells[inlineRowSetFieldKey(source.ID, field.Name)] = cell
	}
	if field.PhysicalName != "" {
		cells[inlineRowSetFieldKey(source.ID, field.PhysicalName)] = cell
	}
}

func inlineRowSetFieldLookupKeys(field qsbridge.FieldRef) []string {
	keys := make([]string, 0, 2)
	if field.Name != "" {
		keys = append(keys, inlineRowSetFieldKey(field.Table.ID, field.Name))
	}
	if field.PhysicalName != "" && !strings.EqualFold(field.PhysicalName, field.Name) {
		keys = append(keys, inlineRowSetFieldKey(field.Table.ID, field.PhysicalName))
	}
	return keys
}

func inlineRowSetFieldKey(source qsbridge.TableInstanceID, field string) string {
	return strings.ToLower(string(source)) + "\x00" + strings.ToLower(strings.TrimSpace(field))
}

func inlineRowSetFilterRows(request ExecutionRequest, rowSet qsbridge.QuantaProjectedRowSet) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet) {
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

func inlineRowSetDiagnostics(message string) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, fmt.Sprintf("inline UNION ALL rowset: %s", message)),
	}
}

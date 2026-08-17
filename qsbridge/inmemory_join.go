package qsbridge

import (
	"fmt"
	"strings"
)

func (e InMemoryNativeExecutor) executeJoinNative(request ExecutionRequest, query QueryIR) ExecutionResult {
	if diagnostic, blocked := inMemoryUnsupportedJoinSelectDiagnostic(query); blocked {
		return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
	}
	table, ok := e.tableForSource(query.Sources[0])
	if !ok {
		return request.EmptyResult().WithDispatchDiagnostic(inMemoryNativeDiagnostic("table %q is not loaded", query.Sources[0].Table))
	}
	current := inMemoryCandidatesForSource(query.Sources[0], table)
	for index, join := range query.Joins {
		rightSource := query.Sources[index+1]
		rightTable, ok := e.tableForSource(rightSource)
		if !ok {
			return request.EmptyResult().WithDispatchDiagnostic(inMemoryNativeDiagnostic("table %q is not loaded", rightSource.Table))
		}
		var diagnostic Diagnostic
		current, diagnostic, ok = inMemoryApplyJoinEdge(current, rightSource, rightTable, join, request.Bound.Parameters)
		if !ok {
			return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
		}
	}
	filteredCandidates := make([]inMemoryNativeCandidate, 0, len(current))
	for _, candidate := range current {
		matches, diagnostic, ok := inMemoryRowMatches(query.Predicates, candidate.Row, request.Bound.Parameters)
		if !ok {
			return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
		}
		if matches {
			filteredCandidates = append(filteredCandidates, candidate)
		}
	}
	matchedCandidates := len(filteredCandidates)
	result := request.EmptyResult()
	var diagnostic Diagnostic
	if inMemoryGroupedAggregateQuery(query) {
		result, diagnostic, ok = inMemoryAppendGroupedAggregateChunks(result, query, filteredCandidates, request.Options.BatchSize)
		if !ok {
			return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
		}
		return inMemoryAttachProfileCounters(result, matchedCandidates)
	}
	if inMemoryGlobalAggregateQuery(query) {
		result, diagnostic, ok = inMemoryAppendGlobalAggregateChunk(result, query, filteredCandidates)
		if !ok {
			return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
		}
		return inMemoryAttachProfileCounters(result, matchedCandidates)
	}
	diagnostic, ok = inMemorySortCandidates(query.OrderBy, filteredCandidates)
	if !ok {
		return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
	}
	filteredCandidates = inMemoryLimitCandidates(filteredCandidates, query.Result.Offset, query.Result.Limit, query.Result.HasResultLimit())
	filteredCandidates = inMemoryLimitCandidates(filteredCandidates, 0, request.Options.MaxRows, request.Options.MaxRows > 0)
	rows, diagnostic, ok := inMemoryEvaluateProjectionRows(query.Projection, filteredCandidates)
	if !ok {
		return request.EmptyResult().WithDispatchDiagnostic(diagnostic)
	}
	result = inMemoryAppendResultRowsChunks(result, rows, request.Options.BatchSize)
	return inMemoryAttachProfileCounters(result, matchedCandidates)
}

func inMemoryUnsupportedJoinSelectDiagnostic(query QueryIR) (Diagnostic, bool) {
	if len(query.Sources) < 2 || len(query.Joins) != len(query.Sources)-1 {
		return inMemoryNativeDiagnostic("join SELECT requires a linear source/join chain"), true
	}
	if len(query.Memberships) > 0 {
		return inMemoryNativeDiagnostic("membership predicates are not supported"), true
	}
	for _, join := range query.Joins {
		if join.Kind != JoinKindInner && join.Kind != JoinKindLeftOuter && join.Kind != JoinKindRightOuter {
			return inMemoryNativeDiagnostic("only INNER, LEFT, and RIGHT JOIN are supported"), true
		}
	}
	if len(query.GroupBy) > 0 && !inMemoryGroupedAggregateQuery(query) {
		return inMemoryNativeDiagnostic("only single-field GROUP BY with count(*), count(field), sum(field), avg(field), min(field), and max(field) is supported"), true
	}
	if len(query.Aggregates) > 0 && len(query.GroupBy) == 0 && !inMemoryGlobalAggregateQuery(query) {
		return inMemoryNativeDiagnostic("only global count(*), count(field), sum(field), avg(field), min(field), and max(field) aggregate projections are supported"), true
	}
	if len(query.GroupBy) > 0 {
		if diagnostic, ok := inMemoryGroupedSortSupported(query); !ok {
			return diagnostic, true
		}
	} else {
		for _, sort := range query.OrderBy {
			if _, ok := inMemoryProjectionField(sort.Expr); !ok {
				return inMemoryNativeDiagnostic("only direct field ORDER BY is supported"), true
			}
		}
	}
	for _, projection := range query.Projection {
		if !inMemorySupportedProjectionExpr(projection.Expr) {
			return inMemoryNativeDiagnostic("only direct field, arithmetic, and aggregate reference projections are supported"), true
		}
	}
	return Diagnostic{}, false
}

func inMemoryCandidatesForSource(source TableInstance, table InMemoryNativeTable) []inMemoryNativeCandidate {
	candidates := make([]inMemoryNativeCandidate, 0, len(table.Rows))
	for rowIndex, row := range table.Rows {
		candidates = append(candidates, inMemoryNativeCandidate{
			Rownum: inMemoryRownumForRow(rowIndex, row),
			Row:    inMemoryQualifiedRow(source, row),
		})
	}
	return candidates
}

func inMemoryApplyJoinEdge(current []inMemoryNativeCandidate, rightSource TableInstance, rightTable InMemoryNativeTable, join JoinEdge, parameters ParameterBindingSet) ([]inMemoryNativeCandidate, Diagnostic, bool) {
	rightRows := inMemoryCandidatesForSource(rightSource, rightTable)
	rightNull := inMemoryNullRowForSource(rightSource, rightTable)
	if join.Kind == JoinKindRightOuter {
		leftNull := inMemoryNullRowForCandidates(current)
		return inMemoryApplyRightJoinEdge(current, rightRows, leftNull, join, parameters)
	}
	rows := make([]inMemoryNativeCandidate, 0)
	nextRownum := QuantaRownum(1)
	for _, left := range current {
		matchedLeft := false
		for _, right := range rightRows {
			joined := inMemoryMergeRows(left.Row, right.Row)
			matches, diagnostic, ok := inMemoryJoinRowMatches(join, joined, parameters)
			if !ok {
				return nil, diagnostic, false
			}
			if !matches {
				continue
			}
			matchedLeft = true
			rows = append(rows, inMemoryNativeCandidate{Rownum: nextRownum, Row: joined})
			nextRownum++
		}
		if !matchedLeft && join.Kind == JoinKindLeftOuter {
			rows = append(rows, inMemoryNativeCandidate{Rownum: nextRownum, Row: inMemoryMergeRows(left.Row, rightNull)})
			nextRownum++
		}
	}
	return rows, Diagnostic{}, true
}

func inMemoryApplyRightJoinEdge(leftRows []inMemoryNativeCandidate, rightRows []inMemoryNativeCandidate, leftNull InMemoryNativeRow, join JoinEdge, parameters ParameterBindingSet) ([]inMemoryNativeCandidate, Diagnostic, bool) {
	rows := make([]inMemoryNativeCandidate, 0)
	nextRownum := QuantaRownum(1)
	for _, right := range rightRows {
		matchedRight := false
		for _, left := range leftRows {
			joined := inMemoryMergeRows(left.Row, right.Row)
			matches, diagnostic, ok := inMemoryJoinRowMatches(join, joined, parameters)
			if !ok {
				return nil, diagnostic, false
			}
			if !matches {
				continue
			}
			matchedRight = true
			rows = append(rows, inMemoryNativeCandidate{Rownum: nextRownum, Row: joined})
			nextRownum++
		}
		if !matchedRight {
			rows = append(rows, inMemoryNativeCandidate{Rownum: nextRownum, Row: inMemoryMergeRows(leftNull, right.Row)})
			nextRownum++
		}
	}
	return rows, Diagnostic{}, true
}

func inMemoryJoinRowMatches(join JoinEdge, row InMemoryNativeRow, parameters ParameterBindingSet) (bool, Diagnostic, bool) {
	left, leftOK := inMemoryFieldCell(row, join.Left)
	right, rightOK := inMemoryFieldCell(row, join.Right)
	if !leftOK || !rightOK {
		return false, inMemoryNativeDiagnostic("join fields %q and %q are not present in the in-memory row", join.Left.QualifiedName(), join.Right.QualifiedName()), false
	}
	operator := join.Operator
	if operator == "" {
		operator = BinaryOpEqual
	}
	if !inMemorySupportedComparisonOp(operator) {
		return false, inMemoryNativeDiagnostic("join operator %q is not supported by in-memory joins", operator), false
	}
	if left.Value == nil || right.Value == nil || !inMemoryCellComparesCell(operator, left, right) {
		return false, Diagnostic{}, true
	}
	return inMemoryRowMatches(join.On, row, parameters)
}

func inMemoryCellComparesCell(op BinaryOp, left ResultCell, right ResultCell) bool {
	if inMemoryIsNumericKind(left.Kind) || inMemoryIsNumericKind(right.Kind) {
		leftNumber, leftOK := inMemoryNumericValue(left.Value)
		rightNumber, rightOK := inMemoryNumericValue(right.Value)
		if !leftOK || !rightOK {
			return false
		}
		if op == BinaryOpNotEqual {
			return leftNumber != rightNumber
		}
		return inMemoryCompareFloat(op, leftNumber, rightNumber)
	}
	if op == BinaryOpNotEqual {
		return inMemoryCellCompare(left, right) != 0
	}
	return inMemoryCompareString(op, fmt.Sprint(left.Value), fmt.Sprint(right.Value))
}

func inMemoryQualifiedRow(source TableInstance, row InMemoryNativeRow) InMemoryNativeRow {
	qualified := cloneInMemoryNativeRow(row)
	if qualified == nil {
		qualified = InMemoryNativeRow{}
	}
	for name, cell := range row {
		if source.Table != "" {
			qualified[source.Table+"."+name] = cell
		}
		if ref := source.RefName(); ref != "" {
			qualified[ref+"."+name] = cell
		}
	}
	return qualified
}

func inMemoryNullRowForSource(source TableInstance, table InMemoryNativeTable) InMemoryNativeRow {
	row := InMemoryNativeRow{}
	for _, sourceRow := range table.Rows {
		for name, cell := range sourceRow {
			if strings.Contains(name, ".") {
				continue
			}
			nullCell := ResultCell{Kind: cell.Kind, Value: nil}
			row[name] = nullCell
			if source.Table != "" {
				row[source.Table+"."+name] = nullCell
			}
			if ref := source.RefName(); ref != "" {
				row[ref+"."+name] = nullCell
			}
		}
	}
	return row
}

func inMemoryNullRowForCandidates(candidates []inMemoryNativeCandidate) InMemoryNativeRow {
	row := InMemoryNativeRow{}
	for _, candidate := range candidates {
		for name, cell := range candidate.Row {
			row[name] = ResultCell{Kind: cell.Kind, Value: nil}
		}
	}
	return row
}

func inMemoryMergeRows(left InMemoryNativeRow, right InMemoryNativeRow) InMemoryNativeRow {
	merged := cloneInMemoryNativeRow(left)
	if merged == nil {
		merged = InMemoryNativeRow{}
	}
	for name, cell := range right {
		if _, exists := merged[name]; exists {
			continue
		}
		merged[name] = cell
	}
	return merged
}

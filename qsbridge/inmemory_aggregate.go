package qsbridge

import (
	"fmt"
	"sort"
	"strings"
)

func inMemoryGroupedAggregateQuery(query QueryIR) bool {
	if len(query.GroupBy) != 1 || len(query.Aggregates) == 0 {
		return false
	}
	if _, ok := inMemoryProjectionField(query.GroupBy[0]); !ok {
		return false
	}
	for _, aggregate := range query.Aggregates {
		if !inMemorySupportedAggregate(aggregate) {
			return false
		}
	}
	if _, ok := inMemoryGroupedSortSupported(query); !ok {
		return false
	}
	if _, ok := inMemoryGroupedHavingSupported(query); !ok {
		return false
	}
	if len(query.Projection) < 2 {
		return false
	}
	hasGroupProjection := false
	hasAggregateProjection := false
	for _, projection := range query.Projection {
		if _, ok := inMemoryProjectionField(projection.Expr); ok {
			hasGroupProjection = true
			continue
		}
		if ref, ok := inMemoryAggregateRef(projection.Expr); ok && ref.Index >= 0 && ref.Index < len(query.Aggregates) {
			hasAggregateProjection = true
			continue
		}
		return false
	}
	return hasGroupProjection && hasAggregateProjection
}

type inMemoryAggregateAccumulator struct {
	Count int64
	Sum   float64
	Seen  int
	Value ResultCell
}

type inMemoryGroupedAggregateRow struct {
	Key   string
	Group ResultCell
	Aggs  []ResultCell
}

func inMemoryAppendGroupedAggregateChunks(result ExecutionResult, query QueryIR, candidates []inMemoryNativeCandidate, batchSize int) (ExecutionResult, Diagnostic, bool) {
	groupExpr, ok := inMemoryProjectionField(query.GroupBy[0])
	if !ok {
		return result, inMemoryNativeDiagnostic("GROUP BY expression must be a field"), false
	}
	groups := make(map[string][]*inMemoryAggregateAccumulator)
	groupCells := make(map[string]ResultCell)
	for _, candidate := range candidates {
		groupCell, ok := inMemoryFieldCell(candidate.Row, groupExpr)
		if !ok {
			return result, inMemoryNativeDiagnostic("field %q is not present in the in-memory row", groupExpr.Name), false
		}
		key := inMemoryGroupKey(groupCell)
		group, ok := groups[key]
		if !ok {
			group = newInMemoryAggregateAccumulators(len(query.Aggregates))
			groups[key] = group
			groupCells[key] = groupCell
		}
		for index, aggregate := range query.Aggregates {
			diagnostic, ok := group[index].add(aggregate, candidate)
			if !ok {
				return result, diagnostic, false
			}
		}
	}
	ordered := make([]inMemoryGroupedAggregateRow, 0, len(groups))
	for key, group := range groups {
		cells := make([]ResultCell, 0, len(query.Aggregates))
		for index, aggregate := range query.Aggregates {
			cell, diagnostic, ok := group[index].resultCell(aggregate)
			if !ok {
				return result, diagnostic, false
			}
			cells = append(cells, cell)
		}
		ordered = append(ordered, inMemoryGroupedAggregateRow{
			Key:   key,
			Group: groupCells[key],
			Aggs:  cells,
		})
	}
	var diagnostic Diagnostic
	ordered, diagnostic, ok = inMemoryFilterGroupedAggregateRows(query.Having, ordered)
	if !ok {
		return result, diagnostic, false
	}
	inMemorySortGroupedAggregateRows(query.OrderBy, ordered)
	rows := make([]ResultRow, 0, len(ordered))
	for _, group := range ordered {
		row, diagnostic, ok := inMemoryGroupedAggregateResultRow(query.Projection, group)
		if !ok {
			return result, diagnostic, false
		}
		rows = append(rows, row)
	}
	rows = inMemoryLimitResultRows(rows, query.Result.Offset, query.Result.Limit, query.Result.HasResultLimit())
	return inMemoryAppendResultRowsChunks(result, rows, batchSize), Diagnostic{}, true
}

func newInMemoryAggregateAccumulators(count int) []*inMemoryAggregateAccumulator {
	accumulators := make([]*inMemoryAggregateAccumulator, count)
	for index := range accumulators {
		accumulators[index] = &inMemoryAggregateAccumulator{}
	}
	return accumulators
}

func inMemoryGroupedSortSupported(query QueryIR) (Diagnostic, bool) {
	if len(query.OrderBy) == 0 {
		return Diagnostic{}, true
	}
	if len(query.OrderBy) > 1 {
		return inMemoryNativeDiagnostic("only one grouped ORDER BY expression is supported"), false
	}
	sort := query.OrderBy[0]
	if field, ok := inMemoryProjectionField(sort.Expr); ok {
		groupField, groupOK := inMemoryProjectionField(query.GroupBy[0])
		if groupOK && quantaIntermediateFieldKey(field) == quantaIntermediateFieldKey(groupField) {
			return Diagnostic{}, true
		}
		return inMemoryNativeDiagnostic("grouped direct-field ORDER BY must reference the group field"), false
	}
	ref, ok := inMemoryAggregateRef(sort.Expr)
	if ok && ref.Index >= 0 && ref.Index < len(query.Aggregates) {
		return Diagnostic{}, true
	}
	return inMemoryNativeDiagnostic("grouped ORDER BY must reference the group field or an aggregate projection"), false
}

func inMemoryGroupedHavingSupported(query QueryIR) (Diagnostic, bool) {
	for _, predicate := range query.Having {
		binary, ok := inMemoryBinaryExpr(predicate.Expr)
		if !ok || !inMemorySupportedComparisonOp(binary.Op) {
			return inMemoryNativeDiagnostic("grouped HAVING must compare aggregate alias to literal"), false
		}
		if ref, ok := inMemoryAggregateRef(binary.Left); ok && ref.Index >= 0 && ref.Index < len(query.Aggregates) {
			if _, literalOK := inMemoryLiteralExpr(binary.Right); literalOK {
				continue
			}
		}
		if ref, ok := inMemoryAggregateRef(binary.Right); ok && ref.Index >= 0 && ref.Index < len(query.Aggregates) {
			if _, literalOK := inMemoryLiteralExpr(binary.Left); literalOK {
				continue
			}
		}
		return inMemoryNativeDiagnostic("grouped HAVING must compare aggregate alias to literal"), false
	}
	return Diagnostic{}, true
}

func inMemoryFilterGroupedAggregateRows(predicates []Predicate, rows []inMemoryGroupedAggregateRow) ([]inMemoryGroupedAggregateRow, Diagnostic, bool) {
	if len(predicates) == 0 {
		return rows, Diagnostic{}, true
	}
	filtered := make([]inMemoryGroupedAggregateRow, 0, len(rows))
	for _, row := range rows {
		matched, diagnostic, ok := inMemoryGroupedAggregateRowMatches(predicates, row)
		if !ok {
			return nil, diagnostic, false
		}
		if matched {
			filtered = append(filtered, row)
		}
	}
	return filtered, Diagnostic{}, true
}

func inMemoryGroupedAggregateRowMatches(predicates []Predicate, row inMemoryGroupedAggregateRow) (bool, Diagnostic, bool) {
	for _, predicate := range predicates {
		op, cell, literal, diagnostic, ok := inMemoryGroupedHavingComparison(predicate, row)
		if !ok {
			return false, diagnostic, false
		}
		if !inMemoryCellComparesLiteral(op, cell, literal) {
			return false, Diagnostic{}, true
		}
	}
	return true, Diagnostic{}, true
}

func inMemoryGroupedHavingComparison(predicate Predicate, row inMemoryGroupedAggregateRow) (BinaryOp, ResultCell, LiteralExpr, Diagnostic, bool) {
	binary, ok := inMemoryBinaryExpr(predicate.Expr)
	if !ok || !inMemorySupportedComparisonOp(binary.Op) {
		return "", ResultCell{}, LiteralExpr{}, inMemoryNativeDiagnostic("grouped HAVING must compare aggregate alias to literal"), false
	}
	if ref, ok := inMemoryAggregateRef(binary.Left); ok {
		if literal, literalOK := inMemoryLiteralExpr(binary.Right); literalOK && ref.Index >= 0 && ref.Index < len(row.Aggs) {
			return binary.Op, row.Aggs[ref.Index], literal, Diagnostic{}, true
		}
	}
	if ref, ok := inMemoryAggregateRef(binary.Right); ok {
		if literal, literalOK := inMemoryLiteralExpr(binary.Left); literalOK && ref.Index >= 0 && ref.Index < len(row.Aggs) {
			op, ok := inMemoryReverseComparisonOp(binary.Op)
			if !ok {
				return "", ResultCell{}, LiteralExpr{}, inMemoryNativeDiagnostic("grouped HAVING comparison operator %q is not supported", binary.Op), false
			}
			return op, row.Aggs[ref.Index], literal, Diagnostic{}, true
		}
	}
	return "", ResultCell{}, LiteralExpr{}, inMemoryNativeDiagnostic("grouped HAVING must compare aggregate alias to literal"), false
}

func inMemorySortGroupedAggregateRows(sortSpecs []SortSpec, rows []inMemoryGroupedAggregateRow) {
	sort.SliceStable(rows, func(i, j int) bool {
		left, right := rows[i].Group, rows[j].Group
		descending := false
		if len(sortSpecs) > 0 {
			descending = sortSpecs[0].Direction == SortDescending
			if ref, ok := inMemoryAggregateRef(sortSpecs[0].Expr); ok && ref.Index >= 0 && ref.Index < len(rows[i].Aggs) && ref.Index < len(rows[j].Aggs) {
				left, right = rows[i].Aggs[ref.Index], rows[j].Aggs[ref.Index]
			}
		}
		if inMemoryCellEqual(left, right) {
			return rows[i].Key < rows[j].Key
		}
		less := inMemoryCellLess(left, right)
		if descending {
			return !less
		}
		return less
	})
}

func inMemoryLimitResultRows(rows []ResultRow, offset int, limit int, hasLimit bool) []ResultRow {
	if !hasLimit && offset <= 0 {
		return rows
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(rows) {
		return nil
	}
	rows = rows[offset:]
	if hasLimit && limit <= 0 {
		return nil
	}
	if hasLimit && limit < len(rows) {
		return rows[:limit]
	}
	return rows
}

func (a *inMemoryAggregateAccumulator) add(aggregate Aggregate, candidate inMemoryNativeCandidate) (Diagnostic, bool) {
	switch {
	case strings.EqualFold(aggregate.Function, "count"):
		a.Count++
		return Diagnostic{}, true
	case strings.EqualFold(aggregate.Function, "sum"), strings.EqualFold(aggregate.Function, "avg"):
		value, diagnostic, ok := inMemoryAggregateInputCell(aggregate, candidate)
		if !ok {
			return diagnostic, false
		}
		if value.Value == nil {
			return Diagnostic{}, true
		}
		number, ok := inMemoryNumericValue(value.Value)
		if !ok {
			return inMemoryNativeDiagnostic("%s aggregate input is not numeric", aggregate.Function), false
		}
		a.Sum += number
		a.Seen++
		return Diagnostic{}, true
	case strings.EqualFold(aggregate.Function, "min"), strings.EqualFold(aggregate.Function, "max"):
		value, diagnostic, ok := inMemoryAggregateInputCell(aggregate, candidate)
		if !ok {
			return diagnostic, false
		}
		if value.Value == nil {
			return Diagnostic{}, true
		}
		if a.Seen == 0 {
			a.Value = value
			a.Seen = 1
			return Diagnostic{}, true
		}
		less := inMemoryCellLess(value, a.Value)
		if (strings.EqualFold(aggregate.Function, "min") && less) || (strings.EqualFold(aggregate.Function, "max") && !less && !inMemoryCellEqual(value, a.Value)) {
			a.Value = value
		}
		return Diagnostic{}, true
	default:
		return inMemoryNativeDiagnostic("aggregate function %q is not supported", aggregate.Function), false
	}
}

func (a inMemoryAggregateAccumulator) resultCell(aggregate Aggregate) (ResultCell, Diagnostic, bool) {
	switch {
	case strings.EqualFold(aggregate.Function, "count"):
		return ResultCell{Kind: ValueInt, Value: a.Count}, Diagnostic{}, true
	case strings.EqualFold(aggregate.Function, "sum"):
		if a.Seen == 0 {
			return ResultCell{Kind: ValueNull, Value: nil}, Diagnostic{}, true
		}
		return ResultCell{Kind: ValueFloat, Value: a.Sum}, Diagnostic{}, true
	case strings.EqualFold(aggregate.Function, "avg"):
		if a.Seen == 0 {
			return ResultCell{Kind: ValueNull, Value: nil}, Diagnostic{}, true
		}
		return ResultCell{Kind: ValueFloat, Value: a.Sum / float64(a.Seen)}, Diagnostic{}, true
	case strings.EqualFold(aggregate.Function, "min"), strings.EqualFold(aggregate.Function, "max"):
		if a.Seen == 0 {
			return ResultCell{Kind: ValueNull, Value: nil}, Diagnostic{}, true
		}
		return a.Value, Diagnostic{}, true
	default:
		return ResultCell{}, inMemoryNativeDiagnostic("aggregate function %q is not supported", aggregate.Function), false
	}
}

func inMemoryAggregateInputCell(aggregate Aggregate, candidate inMemoryNativeCandidate) (ResultCell, Diagnostic, bool) {
	if aggregate.Input == nil {
		return ResultCell{}, inMemoryNativeDiagnostic("%s aggregate input is required", aggregate.Function), false
	}
	return inMemoryEvalExpr(aggregate.Input, candidate.Row)
}

func inMemoryGroupedAggregateResultRow(projections []ProjectionColumn, group inMemoryGroupedAggregateRow) (ResultRow, Diagnostic, bool) {
	row := make(ResultRow, 0, len(projections))
	for _, projection := range projections {
		if _, ok := inMemoryProjectionField(projection.Expr); ok {
			row = append(row, group.Group)
			continue
		}
		if ref, ok := inMemoryAggregateRef(projection.Expr); ok && ref.Index >= 0 && ref.Index < len(group.Aggs) {
			row = append(row, group.Aggs[ref.Index])
			continue
		}
		return nil, inMemoryNativeDiagnostic("grouped aggregate projection %T is not supported", projection.Expr), false
	}
	return row, Diagnostic{}, true
}

func inMemoryGroupKey(cell ResultCell) string {
	return string(cell.Kind) + ":" + fmt.Sprint(cell.Value)
}

func inMemoryGlobalAggregateQuery(query QueryIR) bool {
	if len(query.Aggregates) == 0 || len(query.GroupBy) != 0 || len(query.Having) != 0 {
		return false
	}
	if len(query.Projection) == 0 {
		return false
	}
	for _, aggregate := range query.Aggregates {
		if !inMemorySupportedAggregate(aggregate) {
			return false
		}
	}
	for _, projection := range query.Projection {
		ref, ok := inMemoryAggregateRef(projection.Expr)
		if !ok || ref.Index < 0 || ref.Index >= len(query.Aggregates) {
			return false
		}
	}
	return true
}

func inMemorySupportedAggregate(aggregate Aggregate) bool {
	if aggregate.Filter != nil {
		return false
	}
	switch {
	case strings.EqualFold(aggregate.Function, "count"):
		return aggregate.Input == nil
	case strings.EqualFold(aggregate.Function, "sum"), strings.EqualFold(aggregate.Function, "avg"), strings.EqualFold(aggregate.Function, "min"), strings.EqualFold(aggregate.Function, "max"):
		return inMemorySupportedEvaluatedExpr(aggregate.Input)
	default:
		return false
	}
}

func inMemoryAppendGlobalAggregateChunk(result ExecutionResult, query QueryIR, candidates []inMemoryNativeCandidate) (ExecutionResult, Diagnostic, bool) {
	cells := make(map[int]ResultCell, len(query.Aggregates))
	row := make(ResultRow, 0, len(query.Projection))
	for _, projection := range query.Projection {
		ref, ok := inMemoryAggregateRef(projection.Expr)
		if !ok || ref.Index < 0 || ref.Index >= len(query.Aggregates) {
			return result, inMemoryNativeDiagnostic("global aggregate projection %T is not supported", projection.Expr), false
		}
		cell, ok := cells[ref.Index]
		if !ok {
			var diagnostic Diagnostic
			cell, diagnostic, ok = inMemoryGlobalAggregateCell(query.Aggregates[ref.Index], candidates)
			if !ok {
				return result, diagnostic, false
			}
			cells[ref.Index] = cell
		}
		row = append(row, cell)
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{row},
		Final: true,
	}), Diagnostic{}, true
}

func inMemoryGlobalAggregateCell(aggregate Aggregate, candidates []inMemoryNativeCandidate) (ResultCell, Diagnostic, bool) {
	switch {
	case strings.EqualFold(aggregate.Function, "count"):
		return ResultCell{Kind: ValueInt, Value: int64(len(candidates))}, Diagnostic{}, true
	case strings.EqualFold(aggregate.Function, "sum"):
		sum, count, diagnostic, ok := inMemoryNumericAggregateInput(aggregate, candidates)
		if !ok {
			return ResultCell{}, diagnostic, false
		}
		if count == 0 {
			return ResultCell{Kind: ValueNull, Value: nil}, Diagnostic{}, true
		}
		return ResultCell{Kind: ValueFloat, Value: sum}, Diagnostic{}, true
	case strings.EqualFold(aggregate.Function, "avg"):
		sum, count, diagnostic, ok := inMemoryNumericAggregateInput(aggregate, candidates)
		if !ok {
			return ResultCell{}, diagnostic, false
		}
		if count == 0 {
			return ResultCell{Kind: ValueNull, Value: nil}, Diagnostic{}, true
		}
		return ResultCell{Kind: ValueFloat, Value: sum / float64(count)}, Diagnostic{}, true
	case strings.EqualFold(aggregate.Function, "min"):
		return inMemoryOrderedAggregateCell(aggregate, candidates, false)
	case strings.EqualFold(aggregate.Function, "max"):
		return inMemoryOrderedAggregateCell(aggregate, candidates, true)
	default:
		return ResultCell{}, inMemoryNativeDiagnostic("aggregate function %q is not supported", aggregate.Function), false
	}
}

func inMemoryOrderedAggregateCell(aggregate Aggregate, candidates []inMemoryNativeCandidate, max bool) (ResultCell, Diagnostic, bool) {
	var selected ResultCell
	hasValue := false
	for _, candidate := range candidates {
		cell, diagnostic, ok := inMemoryEvalExpr(aggregate.Input, candidate.Row)
		if !ok {
			return ResultCell{}, diagnostic, false
		}
		if cell.Value == nil {
			continue
		}
		if !hasValue {
			selected = cell
			hasValue = true
			continue
		}
		less := inMemoryCellLess(cell, selected)
		if (!max && less) || (max && !less && !inMemoryCellEqual(cell, selected)) {
			selected = cell
		}
	}
	if !hasValue {
		return ResultCell{Kind: ValueNull, Value: nil}, Diagnostic{}, true
	}
	return selected, Diagnostic{}, true
}

func inMemoryNumericAggregateInput(aggregate Aggregate, candidates []inMemoryNativeCandidate) (float64, int, Diagnostic, bool) {
	sum := float64(0)
	count := 0
	for _, candidate := range candidates {
		cell, diagnostic, ok := inMemoryEvalExpr(aggregate.Input, candidate.Row)
		if !ok {
			return 0, 0, diagnostic, false
		}
		if cell.Value == nil {
			continue
		}
		number, ok := inMemoryNumericValue(cell.Value)
		if !ok {
			return 0, 0, inMemoryNativeDiagnostic("%s aggregate input is not numeric", aggregate.Function), false
		}
		sum += number
		count++
	}
	return sum, count, Diagnostic{}, true
}

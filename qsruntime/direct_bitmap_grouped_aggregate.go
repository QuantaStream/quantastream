package qsruntime

import (
	"container/heap"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type directBitmapGroupedAggregateRow struct {
	Key    string
	Groups []qsbridge.ResultCell
	Aggs   []qsbridge.ResultCell
}

type directBitmapGroupedAggregateProbe struct {
	CandidateRows                int
	GroupExpressionCount         int
	ComputedGroupExpressionCount int
	GroupExpressionShapes        string
	GroupExpressionFields        string
	GroupCount                   int
	PostHavingGroups             int
	SortInputGroups              int
	Limit                        int
	FinalRows                    int
	TopNCandidate                bool
	OrderStrategy                string
	GroupStrategy                string
	GroupValuesTime              time.Duration
	AggregateInputTime           time.Duration
	GroupTime                    time.Duration
	HavingTime                   time.Duration
	OrderTime                    time.Duration
	OutputTime                   time.Duration
	LimitTime                    time.Duration
}

type directBitmapGroupExpression struct {
	Expr  qsbridge.Expr
	Field qsbridge.FieldRef
}

const directBitmapStreamingGroupedAggregateMinRows = 250000

func directBitmapMaterializedGroupedAggregateResult(request ExecutionRequest, materialized qsbridge.QuantaProjectedRowSet, result ExecutionResult) ExecutionResult {
	probe := directBitmapGroupedAggregateProbe{
		CandidateRows: materialized.CandidateCount(),
		Limit:         request.Result.Limit,
		TopNCandidate: executionGroupedAggregateTopNCandidate(request),
	}
	if len(request.GroupBy) == 0 {
		result.Diagnostics = append(result.Diagnostics, directBitmapAggregateDiagnostics("direct bitmap runtime requires at least one GROUP BY field")...)
		return result
	}
	groupExpressions, diagnostics := directBitmapGroupExpressions(request.GroupBy)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	probe.GroupExpressionCount = len(groupExpressions)
	probe.ComputedGroupExpressionCount = directBitmapComputedGroupExpressionCount(groupExpressions)
	probe.GroupExpressionShapes = directBitmapGroupExpressionShapes(groupExpressions)
	probe.GroupExpressionFields = directBitmapGroupExpressionFields(groupExpressions)
	groupValuesStart := time.Now()
	groupValues, diagnostics := directBitmapGroupValueColumns(materialized, groupExpressions)
	probe.GroupValuesTime = time.Since(groupValuesStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	aggregateInputStart := time.Now()
	aggregateInputs, diagnostics := directBitmapGroupedAggregateInputs(request.SQLAggregates, materialized)
	probe.AggregateInputTime = time.Since(aggregateInputStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	groupStart := time.Now()
	rows, groupStrategy, diagnostics := directBitmapGroupedAggregateRows(request, groupValues, aggregateInputs)
	probe.GroupTime = time.Since(groupStart)
	probe.GroupStrategy = groupStrategy
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	probe.GroupCount = len(rows)
	havingStart := time.Now()
	rows, diagnostics = directBitmapFilterGroupedAggregateRows(request.Having, rows, groupExpressions)
	probe.HavingTime = time.Since(havingStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	probe.PostHavingGroups = len(rows)
	probe.SortInputGroups = len(rows)
	probe.OrderStrategy = directBitmapGroupedAggregateOrderStrategy(request, len(rows))
	orderStart := time.Now()
	rows, diagnostics = directBitmapOrderGroupedAggregateRows(request, rows, groupExpressions)
	probe.OrderTime = time.Since(orderStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	outputStart := time.Now()
	rowSet, diagnostics := directBitmapGroupedAggregateRowSet(request, rows, groupExpressions)
	probe.OutputTime = time.Since(outputStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	limitStart := time.Now()
	rowSet = directBitmapLimitProjectedRowSet(rowSet, request.Result.Offset, request.Result.Limit, request.Result.HasResultLimit())
	probe.LimitTime = time.Since(limitStart)
	probe.FinalRows = rowSet.CandidateCount()
	result.Probes = append(result.Probes, directBitmapGroupedAggregateProbes(probe)...)
	result.RowSet = rowSet
	result.Count = uint64(rowSet.CandidateCount())
	return result
}

func directBitmapGroupedAggregateProbes(probe directBitmapGroupedAggregateProbe) []ExecutionProbe {
	return []ExecutionProbe{
		{Section: "grouped_aggregate", Name: "candidate_rows", Value: strconv.Itoa(probe.CandidateRows)},
		{Section: "grouped_aggregate", Name: "group_expression_count", Value: strconv.Itoa(probe.GroupExpressionCount)},
		{Section: "grouped_aggregate", Name: "group_expression_computed_count", Value: strconv.Itoa(probe.ComputedGroupExpressionCount)},
		{Section: "grouped_aggregate", Name: "group_expression_shapes", Value: probe.GroupExpressionShapes},
		{Section: "grouped_aggregate", Name: "group_expression_fields", Value: probe.GroupExpressionFields},
		{Section: "grouped_aggregate", Name: "groups", Value: strconv.Itoa(probe.GroupCount)},
		{Section: "grouped_aggregate", Name: "post_having_groups", Value: strconv.Itoa(probe.PostHavingGroups)},
		{Section: "grouped_aggregate", Name: "sort_input_groups", Value: strconv.Itoa(probe.SortInputGroups)},
		{Section: "grouped_aggregate", Name: "limit", Value: strconv.Itoa(probe.Limit)},
		{Section: "grouped_aggregate", Name: "final_rows", Value: strconv.Itoa(probe.FinalRows)},
		{Section: "grouped_aggregate", Name: "topn_candidate", Value: strconv.FormatBool(probe.TopNCandidate)},
		{Section: "grouped_aggregate", Name: "order_strategy", Value: probe.OrderStrategy},
		{Section: "grouped_aggregate", Name: "group_strategy", Value: probe.GroupStrategy},
		{Section: "grouped_aggregate", Name: "phase_group_values_elapsed", Value: probe.GroupValuesTime.String()},
		{Section: "grouped_aggregate", Name: "phase_aggregate_inputs_elapsed", Value: probe.AggregateInputTime.String()},
		{Section: "grouped_aggregate", Name: "phase_group_elapsed", Value: probe.GroupTime.String()},
		{Section: "grouped_aggregate", Name: "phase_having_elapsed", Value: probe.HavingTime.String()},
		{Section: "grouped_aggregate", Name: "phase_order_elapsed", Value: probe.OrderTime.String()},
		{Section: "grouped_aggregate", Name: "phase_output_elapsed", Value: probe.OutputTime.String()},
		{Section: "grouped_aggregate", Name: "phase_limit_elapsed", Value: probe.LimitTime.String()},
	}
}

func directBitmapGroupedAggregateInputs(aggregates []qsbridge.Aggregate, materialized qsbridge.QuantaProjectedRowSet) ([][]qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	inputs := make([][]qsbridge.ResultCell, len(aggregates))
	for i, aggregate := range aggregates {
		if aggregate.Filter != nil {
			return nil, directBitmapAggregateDiagnostics("direct bitmap runtime only supports unfiltered grouped aggregates in this slice")
		}
		if directBitmapCountAllAggregate(aggregate) {
			continue
		}
		values, diagnostics := directBitmapAggregateInputValues(aggregate, materialized)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		inputs[i] = values
	}
	return inputs, nil
}

func directBitmapGroupedAggregateRows(request ExecutionRequest, groupValues [][]qsbridge.ResultCell, aggregateInputs [][]qsbridge.ResultCell) ([]directBitmapGroupedAggregateRow, string, qsbridge.DiagnosticSet) {
	if directBitmapStreamingGroupedAggregateCandidate(request, groupValues, aggregateInputs) {
		rows, diagnostics := directBitmapStreamingGroupedAggregateRows(request, groupValues, aggregateInputs)
		return rows, "streaming", diagnostics
	}
	rows, diagnostics := directBitmapGroupedAggregateRowsByIndexReplay(request, groupValues, aggregateInputs)
	return rows, "index_replay", diagnostics
}

func directBitmapStreamingGroupedAggregateCandidate(request ExecutionRequest, groupValues [][]qsbridge.ResultCell, aggregateInputs [][]qsbridge.ResultCell) bool {
	if len(groupValues) == 0 || len(groupValues[0]) < directBitmapStreamingGroupedAggregateMinRows {
		return false
	}
	if len(request.SQLAggregates) != len(aggregateInputs) {
		return false
	}
	for i, aggregate := range request.SQLAggregates {
		if aggregate.Filter != nil || aggregate.Mode == qsbridge.AggregateDistinct {
			return false
		}
		if directBitmapCountAllAggregate(aggregate) {
			continue
		}
		if aggregateInputs[i] == nil {
			return false
		}
		switch strings.ToLower(aggregate.Function) {
		case "sum", "avg", "min", "max":
		default:
			return false
		}
	}
	return true
}

type directBitmapStreamingGroupedAggregateState struct {
	Groups []qsbridge.ResultCell
	Aggs   []directBitmapStreamingGroupedAggregateAccumulator
}

type directBitmapStreamingGroupedAggregateAccumulator struct {
	Sum      float64
	Min      float64
	Max      float64
	Value    qsbridge.ResultCell
	Seen     int
	CountAll int
}

func directBitmapStreamingGroupedAggregateRows(request ExecutionRequest, groupValues [][]qsbridge.ResultCell, aggregateInputs [][]qsbridge.ResultCell) ([]directBitmapGroupedAggregateRow, qsbridge.DiagnosticSet) {
	if len(groupValues) == 0 {
		return nil, directBitmapAggregateDiagnostics("grouped aggregate requires at least one group value column")
	}
	groupCount := len(groupValues[0])
	stateByKey := make(map[string]int)
	states := make([]directBitmapStreamingGroupedAggregateState, 0)
	keys := make([]string, 0)
	for i := 0; i < groupCount; i++ {
		key, diagnostics := directBitmapGroupValuesKeyAt(groupValues, i)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		stateIndex, ok := stateByKey[key]
		if !ok {
			cells, diagnostics := directBitmapGroupValueCellsAt(groupValues, i)
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			stateIndex = len(states)
			stateByKey[key] = stateIndex
			keys = append(keys, key)
			states = append(states, directBitmapStreamingGroupedAggregateState{
				Groups: cells,
				Aggs:   make([]directBitmapStreamingGroupedAggregateAccumulator, len(request.SQLAggregates)),
			})
		}
		state := &states[stateIndex]
		for aggregateIndex, aggregate := range request.SQLAggregates {
			if directBitmapCountAllAggregate(aggregate) {
				state.Aggs[aggregateIndex].CountAll++
				continue
			}
			if aggregateIndex >= len(aggregateInputs) || i >= len(aggregateInputs[aggregateIndex]) {
				return nil, directBitmapAggregateDiagnostics("aggregate input field has fewer values than grouped candidates")
			}
			diagnostics := directBitmapStreamingGroupedAggregateAdd(&state.Aggs[aggregateIndex], aggregate, aggregateInputs[aggregateIndex][i])
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
		}
	}
	sort.Strings(keys)
	rows := make([]directBitmapGroupedAggregateRow, 0, len(keys))
	for _, key := range keys {
		state := states[stateByKey[key]]
		aggs := make([]qsbridge.ResultCell, 0, len(request.SQLAggregates))
		for i, aggregate := range request.SQLAggregates {
			cell, diagnostics := directBitmapStreamingGroupedAggregateCell(aggregate, state.Aggs[i])
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			aggs = append(aggs, cell)
		}
		rows = append(rows, directBitmapGroupedAggregateRow{
			Key:    key,
			Groups: state.Groups,
			Aggs:   aggs,
		})
	}
	return rows, nil
}

func directBitmapStreamingGroupedAggregateAdd(accumulator *directBitmapStreamingGroupedAggregateAccumulator, aggregate qsbridge.Aggregate, cell qsbridge.ResultCell) qsbridge.DiagnosticSet {
	if cell.Kind == qsbridge.ValueNull || cell.Value == nil {
		return nil
	}
	switch strings.ToLower(aggregate.Function) {
	case "min":
		if accumulator.Seen == 0 || directBitmapCellLess(cell, accumulator.Value) {
			accumulator.Value = cell
		}
		accumulator.Seen++
		return nil
	case "max":
		if accumulator.Seen == 0 || directBitmapCellLess(accumulator.Value, cell) {
			accumulator.Value = cell
		}
		accumulator.Seen++
		return nil
	}
	value, ok := directBitmapNumericCellValue(cell)
	if !ok {
		return directBitmapAggregateDiagnostics(fmt.Sprintf("%s aggregate requires numeric values", aggregate.Function))
	}
	if accumulator.Seen == 0 || value < accumulator.Min {
		accumulator.Min = value
	}
	if accumulator.Seen == 0 || value > accumulator.Max {
		accumulator.Max = value
	}
	accumulator.Sum += value
	accumulator.Seen++
	return nil
}

func directBitmapStreamingGroupedAggregateCell(aggregate qsbridge.Aggregate, accumulator directBitmapStreamingGroupedAggregateAccumulator) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if directBitmapCountAllAggregate(aggregate) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(accumulator.CountAll)}, nil
	}
	if accumulator.Seen == 0 {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	switch strings.ToLower(aggregate.Function) {
	case "sum":
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: accumulator.Sum}, nil
	case "avg":
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: accumulator.Sum / float64(accumulator.Seen)}, nil
	case "min":
		return accumulator.Value, nil
	case "max":
		return accumulator.Value, nil
	default:
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("direct bitmap runtime does not support %s aggregate in this slice", aggregate.Function))
	}
}

func directBitmapGroupedAggregateRowsByIndexReplay(request ExecutionRequest, groupValues [][]qsbridge.ResultCell, aggregateInputs [][]qsbridge.ResultCell) ([]directBitmapGroupedAggregateRow, qsbridge.DiagnosticSet) {
	groupIndexes := make(map[string][]int)
	groupCells := make(map[string][]qsbridge.ResultCell)
	for i := 0; i < len(groupValues[0]); i++ {
		key, diagnostics := directBitmapGroupValuesKeyAt(groupValues, i)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		groupIndexes[key] = append(groupIndexes[key], i)
		if _, ok := groupCells[key]; !ok {
			cells, diagnostics := directBitmapGroupValueCellsAt(groupValues, i)
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			groupCells[key] = cells
		}
	}
	keys := make([]string, 0, len(groupIndexes))
	for key := range groupIndexes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]directBitmapGroupedAggregateRow, 0, len(keys))
	for _, key := range keys {
		aggs := make([]qsbridge.ResultCell, 0, len(request.SQLAggregates))
		for i, aggregate := range request.SQLAggregates {
			cell, diagnostics := directBitmapGroupedAggregateCell(aggregate, aggregateInputs[i], groupIndexes[key])
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			aggs = append(aggs, cell)
		}
		rows = append(rows, directBitmapGroupedAggregateRow{
			Key:    key,
			Groups: groupCells[key],
			Aggs:   aggs,
		})
	}
	return rows, nil
}

func directBitmapGroupedAggregateCell(aggregate qsbridge.Aggregate, values []qsbridge.ResultCell, indexes []int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if aggregate.Filter != nil {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("direct bitmap runtime only supports unfiltered grouped aggregates in this slice")
	}
	if directBitmapCountAllAggregate(aggregate) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(len(indexes))}, nil
	}
	if values == nil {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("%s aggregate requires prepared input values", aggregate.Function))
	}
	groupValues := make([]qsbridge.ResultCell, 0, len(indexes))
	for _, index := range indexes {
		if index >= len(values) {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("aggregate input field has fewer values than grouped candidates")
		}
		groupValues = append(groupValues, values[index])
	}
	if directBitmapCountValueAggregate(aggregate) {
		return directBitmapCountNonNullCell(groupValues), nil
	}
	if aggregate.Mode == qsbridge.AggregateDistinct {
		if !strings.EqualFold(aggregate.Function, "count") {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("direct bitmap runtime only supports grouped count(distinct field) in this slice")
		}
		return directBitmapDistinctCountCell(groupValues), nil
	}
	return directBitmapNumericAggregateCell(aggregate, groupValues)
}

func directBitmapGroupedAggregateRowSet(request ExecutionRequest, rows []directBitmapGroupedAggregateRow, groupExpressions []directBitmapGroupExpression) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet) {
	index, _ := request.RootIndex()
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   index,
		Rownums: make([]qsbridge.QuantaRownum, len(rows)),
	}
	for i := range rows {
		rowSet.Rownums[i] = qsbridge.QuantaRownum(i + 1)
	}
	projections := request.Projection
	if len(projections) == 0 {
		projections = directBitmapDefaultGroupedAggregateProjections(request, groupExpressions)
	}
	for _, projection := range projections {
		vector, diagnostics := directBitmapGroupedAggregateProjectionVector(projection, rows, groupExpressions, request.SQLAggregates)
		if diagnostics.BlocksNative() {
			return qsbridge.QuantaProjectedRowSet{}, diagnostics
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
	}
	return rowSet, rowSet.ValidateShape()
}

func directBitmapDefaultGroupedAggregateProjections(request ExecutionRequest, groupExpressions []directBitmapGroupExpression) []qsbridge.ProjectionColumn {
	projections := make([]qsbridge.ProjectionColumn, 0, len(groupExpressions)+len(request.SQLAggregates))
	for _, groupExpression := range groupExpressions {
		projections = append(projections, qsbridge.ProjectionColumn{Expr: groupExpression.Expr, Type: qsbridge.ExprDataType(groupExpression.Expr)})
	}
	for i, aggregate := range request.SQLAggregates {
		projections = append(projections, qsbridge.ProjectionColumn{
			Expr:  qsbridge.AggregateRef(aggregate.Alias, i),
			Alias: aggregate.Alias,
			Type:  directBitmapAggregateResultType(aggregate),
		})
	}
	return projections
}

func directBitmapGroupedAggregateProjectionVector(projection qsbridge.ProjectionColumn, rows []directBitmapGroupedAggregateRow, groupExpressions []directBitmapGroupExpression, aggregates []qsbridge.Aggregate) (qsbridge.QuantaProjectionVector, qsbridge.DiagnosticSet) {
	if groupIndex := directBitmapGroupExpressionIndex(projection.Expr, groupExpressions); groupIndex >= 0 {
		values := make([]qsbridge.ResultCell, len(rows))
		for i, row := range rows {
			values[i] = row.Groups[groupIndex]
		}
		return qsbridge.QuantaProjectionVector{
			Field:  directBitmapGroupedAggregateField(projection, groupExpressions[groupIndex]),
			Values: values,
		}, nil
	}
	ref, ok := directBitmapExprAggregateRef(projection.Expr)
	if !ok || ref.Index < 0 || ref.Index >= len(aggregates) {
		values := make([]qsbridge.ResultCell, len(rows))
		for i, row := range rows {
			cell, diagnostics := directBitmapEvaluateGroupedProjectionExpr(projection.Expr, row, groupExpressions)
			if diagnostics.BlocksNative() {
				return qsbridge.QuantaProjectionVector{}, diagnostics
			}
			values[i] = cell
		}
		index := ""
		if len(groupExpressions) > 0 {
			index = groupExpressions[0].Field.Table.Table
		}
		return qsbridge.QuantaProjectionVector{
			Field: qsbridge.QuantaProjectionField{
				Index:   index,
				Field:   directBitmapProjectionColumnName(projection),
				Type:    directBitmapProjectionType(projection, qsbridge.DataTypeFloat),
				Visible: true,
			},
			Values: values,
		}, nil
	}
	values := make([]qsbridge.ResultCell, len(rows))
	for i, row := range rows {
		values[i] = row.Aggs[ref.Index]
	}
	alias := projection.Alias
	if alias == "" {
		alias = ref.Alias
	}
	if alias == "" {
		alias = aggregates[ref.Index].Alias
	}
	if alias == "" {
		alias = aggregates[ref.Index].Function
	}
	index := ""
	if len(groupExpressions) > 0 {
		index = groupExpressions[0].Field.Table.Table
	}
	return qsbridge.QuantaProjectionVector{
		Field: qsbridge.QuantaProjectionField{
			Index:   index,
			Field:   alias,
			Type:    directBitmapProjectionType(projection, directBitmapAggregateResultType(aggregates[ref.Index])),
			Visible: true,
		},
		Values: values,
	}, nil
}

func directBitmapEvaluateGroupedProjectionExpr(expr qsbridge.Expr, row directBitmapGroupedAggregateRow, groupExpressions []directBitmapGroupExpression) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if groupIndex := directBitmapGroupExpressionIndex(expr, groupExpressions); groupIndex >= 0 {
		if groupIndex >= len(row.Groups) {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("grouped aggregate projection group reference is out of range")
		}
		return row.Groups[groupIndex], nil
	}
	if ref, ok := directBitmapExprAggregateRef(expr); ok {
		if ref.Index < 0 || ref.Index >= len(row.Aggs) {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("grouped aggregate projection aggregate reference is out of range")
		}
		return row.Aggs[ref.Index], nil
	}
	if literal, ok := directBitmapLiteralExpr(expr); ok {
		return directBitmapLiteralCell(literal), nil
	}
	if call, ok := directBitmapCallExpr(expr); ok {
		return directBitmapEvaluateGroupedCallExpr(call, row, groupExpressions)
	}
	binary, ok := directBitmapBinaryExpr(expr)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("grouped aggregate projection must reference the GROUP BY field, an aggregate, or aggregate arithmetic")
	}
	left, diagnostics := directBitmapEvaluateGroupedProjectionExpr(binary.Left, row, groupExpressions)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	right, diagnostics := directBitmapEvaluateGroupedProjectionExpr(binary.Right, row, groupExpressions)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	leftNumber, leftOK := directBitmapNumericCellValue(left)
	rightNumber, rightOK := directBitmapNumericCellValue(right)
	if !leftOK || !rightOK {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("grouped aggregate projection arithmetic requires numeric operands")
	}
	switch binary.Op {
	case qsbridge.BinaryOpAdd:
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: leftNumber + rightNumber}, nil
	case qsbridge.BinaryOpSubtract:
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: leftNumber - rightNumber}, nil
	case qsbridge.BinaryOpMultiply:
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: leftNumber * rightNumber}, nil
	case qsbridge.BinaryOpDivide:
		if rightNumber == 0 {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("grouped aggregate projection division by zero")
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: leftNumber / rightNumber}, nil
	default:
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("grouped aggregate projection arithmetic operator %q is not supported", binary.Op))
	}
}

func directBitmapEvaluateGroupedCallExpr(call qsbridge.CallExpr, row directBitmapGroupedAggregateRow, groupExpressions []directBitmapGroupExpression) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	args := make([]qsbridge.ResultCell, len(call.Args))
	for i, arg := range call.Args {
		cell, diagnostics := directBitmapEvaluateGroupedProjectionExpr(arg, row, groupExpressions)
		if diagnostics.BlocksNative() {
			return qsbridge.ResultCell{}, diagnostics
		}
		args[i] = cell
	}
	switch strings.ToLower(call.Name) {
	case "tostring":
		if len(args) != 1 {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("grouped scalar function %q expects one argument", call.Name))
		}
		if directBitmapNullCell(args[0]) {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: fmt.Sprint(args[0].Value)}, nil
	case "toint":
		if len(args) != 1 {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("grouped scalar function %q expects one argument", call.Name))
		}
		if directBitmapNullCell(args[0]) {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		number, ok := directBitmapNumericCellValue(args[0])
		if !ok {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("grouped scalar function %q requires a numeric argument", call.Name))
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(number)}, nil
	case "tonumber":
		if len(args) != 1 {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("grouped scalar function %q expects one argument", call.Name))
		}
		if directBitmapNullCell(args[0]) {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		number, ok := directBitmapNumericCellValue(args[0])
		if !ok {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("grouped scalar function %q requires a numeric argument", call.Name))
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: number}, nil
	case "lower", "lcase":
		return directBitmapEvaluateGroupedStringCall(call.Name, args, strings.ToLower)
	case "upper", "ucase":
		return directBitmapEvaluateGroupedStringCall(call.Name, args, strings.ToUpper)
	case "length", "char_length":
		if len(args) != 1 {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("grouped scalar function %q expects one argument", call.Name))
		}
		if directBitmapNullCell(args[0]) {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(len([]rune(fmt.Sprint(args[0].Value))))}, nil
	default:
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("grouped scalar function %q is not supported", call.Name))
	}
}

func directBitmapEvaluateGroupedStringCall(name string, args []qsbridge.ResultCell, transform func(string) string) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("grouped scalar function %q expects one argument", name))
	}
	if directBitmapNullCell(args[0]) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: transform(fmt.Sprint(args[0].Value))}, nil
}

func directBitmapGroupedAggregateField(projection qsbridge.ProjectionColumn, groupExpression directBitmapGroupExpression) qsbridge.QuantaProjectionField {
	field := groupExpression.Field
	name := directBitmapFieldPhysicalName(field)
	if name == "" {
		name = directBitmapProjectionColumnName(projection)
	}
	if projection.Alias != "" {
		name = projection.Alias
	}
	return qsbridge.QuantaProjectionField{
		Index:        field.Table.Table,
		Field:        name,
		Type:         directBitmapProjectionType(projection, field.Type),
		PhysicalName: field.PhysicalName,
		Visible:      true,
	}
}

func directBitmapProjectionColumnName(projection qsbridge.ProjectionColumn) string {
	if projection.Alias != "" {
		return projection.Alias
	}
	if field, ok := directBitmapExprField(projection.Expr); ok {
		return directBitmapFieldPhysicalName(field)
	}
	if call, ok := directBitmapCallExpr(projection.Expr); ok {
		return call.Name
	}
	return "group"
}

func directBitmapProjectionType(projection qsbridge.ProjectionColumn, fallback qsbridge.DataType) qsbridge.DataType {
	if projection.Type != qsbridge.DataTypeUnknown {
		return projection.Type
	}
	return fallback
}

func directBitmapOrderGroupedAggregateRows(request ExecutionRequest, rows []directBitmapGroupedAggregateRow, groupExpressions []directBitmapGroupExpression) ([]directBitmapGroupedAggregateRow, qsbridge.DiagnosticSet) {
	if len(request.OrderBy) == 0 {
		sort.SliceStable(rows, func(i, j int) bool {
			return rows[i].Key < rows[j].Key
		})
		return rows, nil
	}
	if aggregateIndex, ok := directBitmapGroupedAggregateTopNIndex(request, len(request.SQLAggregates)); ok && request.Result.Limit < len(rows) {
		return directBitmapHeapTopNGroupedAggregateRows(request, rows, aggregateIndex)
	}
	sortCells, diagnostics := directBitmapGroupedSortCells(request.OrderBy, groupExpressions, len(request.SQLAggregates))
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return directBitmapGroupedAggregateRowLess(rows[i], rows[j], sortCells)
	})
	return rows, nil
}

func directBitmapGroupedAggregateOrderStrategy(request ExecutionRequest, rowCount int) string {
	if _, ok := directBitmapGroupedAggregateTopNIndex(request, len(request.SQLAggregates)); ok && request.Result.Limit < rowCount {
		return "heap_topn"
	}
	return "full_sort"
}

func directBitmapGroupedAggregateTopNIndex(request ExecutionRequest, aggregateCount int) (int, bool) {
	if len(request.OrderBy) != 1 || request.Result.Limit <= 0 || request.Result.Offset != 0 {
		return 0, false
	}
	order := request.OrderBy[0]
	if order.Direction != qsbridge.SortDescending {
		return 0, false
	}
	ref, ok := directBitmapExprAggregateRef(order.Expr)
	if !ok || ref.Index < 0 || ref.Index >= aggregateCount {
		return 0, false
	}
	return ref.Index, true
}

func directBitmapHeapTopNGroupedAggregateRows(request ExecutionRequest, rows []directBitmapGroupedAggregateRow, aggregateIndex int) ([]directBitmapGroupedAggregateRow, qsbridge.DiagnosticSet) {
	limit := request.Result.Limit
	if limit <= 0 {
		return rows, nil
	}
	sortCells := []directBitmapGroupedSortCell{{
		descending: true,
		left: func(left directBitmapGroupedAggregateRow, right directBitmapGroupedAggregateRow) (qsbridge.ResultCell, qsbridge.ResultCell) {
			if aggregateIndex >= len(left.Aggs) || aggregateIndex >= len(right.Aggs) {
				return qsbridge.ResultCell{}, qsbridge.ResultCell{}
			}
			return left.Aggs[aggregateIndex], right.Aggs[aggregateIndex]
		},
	}}
	top := &directBitmapGroupedAggregateTopNHeap{sortCells: sortCells}
	heap.Init(top)
	for _, row := range rows {
		if top.Len() < limit {
			heap.Push(top, row)
			continue
		}
		if directBitmapGroupedAggregateRowLess(row, top.rows[0], sortCells) {
			top.rows[0] = row
			heap.Fix(top, 0)
		}
	}
	result := append([]directBitmapGroupedAggregateRow(nil), top.rows...)
	sort.SliceStable(result, func(i, j int) bool {
		return directBitmapGroupedAggregateRowLess(result[i], result[j], sortCells)
	})
	return result, nil
}

type directBitmapGroupedAggregateTopNHeap struct {
	rows      []directBitmapGroupedAggregateRow
	sortCells []directBitmapGroupedSortCell
}

// Len reports the number of grouped aggregate rows currently in the heap.
func (h directBitmapGroupedAggregateTopNHeap) Len() int {
	return len(h.rows)
}

// Less reports whether row i is worse than row j for min-heap ordering.
func (h directBitmapGroupedAggregateTopNHeap) Less(i int, j int) bool {
	return directBitmapGroupedAggregateRowLess(h.rows[j], h.rows[i], h.sortCells)
}

// Swap exchanges two heap rows.
func (h directBitmapGroupedAggregateTopNHeap) Swap(i int, j int) {
	h.rows[i], h.rows[j] = h.rows[j], h.rows[i]
}

// Push appends one grouped aggregate row to the heap.
func (h *directBitmapGroupedAggregateTopNHeap) Push(value interface{}) {
	h.rows = append(h.rows, value.(directBitmapGroupedAggregateRow))
}

// Pop removes and returns the current worst grouped aggregate row.
func (h *directBitmapGroupedAggregateTopNHeap) Pop() interface{} {
	last := len(h.rows) - 1
	value := h.rows[last]
	h.rows = h.rows[:last]
	return value
}

type directBitmapGroupedSortCell struct {
	descending bool
	left       func(left directBitmapGroupedAggregateRow, right directBitmapGroupedAggregateRow) (qsbridge.ResultCell, qsbridge.ResultCell)
}

func directBitmapGroupedAggregateRowLess(left directBitmapGroupedAggregateRow, right directBitmapGroupedAggregateRow, sortCells []directBitmapGroupedSortCell) bool {
	for _, sortCell := range sortCells {
		leftCell, rightCell := sortCell.left(left, right)
		if directBitmapCellEqual(leftCell, rightCell) {
			continue
		}
		less := directBitmapCellLess(leftCell, rightCell)
		if sortCell.descending {
			return !less
		}
		return less
	}
	return left.Key < right.Key
}

func directBitmapGroupedSortCells(sortSpecs []qsbridge.SortSpec, groupExpressions []directBitmapGroupExpression, aggregateCount int) ([]directBitmapGroupedSortCell, qsbridge.DiagnosticSet) {
	sortCells := make([]directBitmapGroupedSortCell, 0, len(sortSpecs))
	for _, sortSpec := range sortSpecs {
		descending := sortSpec.Direction == qsbridge.SortDescending
		if ref, ok := directBitmapExprAggregateRef(sortSpec.Expr); ok {
			if ref.Index < 0 || ref.Index >= aggregateCount {
				return nil, directBitmapAggregateDiagnostics("grouped ORDER BY aggregate reference is out of range")
			}
			index := ref.Index
			sortCells = append(sortCells, directBitmapGroupedSortCell{
				descending: descending,
				left: func(left directBitmapGroupedAggregateRow, right directBitmapGroupedAggregateRow) (qsbridge.ResultCell, qsbridge.ResultCell) {
					if index >= len(left.Aggs) || index >= len(right.Aggs) {
						return qsbridge.ResultCell{}, qsbridge.ResultCell{}
					}
					return left.Aggs[index], right.Aggs[index]
				},
			})
			continue
		}
		if groupIndex := directBitmapGroupExpressionIndex(sortSpec.Expr, groupExpressions); groupIndex >= 0 {
			index := groupIndex
			sortCells = append(sortCells, directBitmapGroupedSortCell{
				descending: descending,
				left: func(left directBitmapGroupedAggregateRow, right directBitmapGroupedAggregateRow) (qsbridge.ResultCell, qsbridge.ResultCell) {
					return left.Groups[index], right.Groups[index]
				},
			})
			continue
		}
		return nil, directBitmapAggregateDiagnostics("grouped ORDER BY must reference the GROUP BY field or an aggregate")
	}
	return sortCells, nil
}

func directBitmapGroupExpressions(groupBy []qsbridge.Expr) ([]directBitmapGroupExpression, qsbridge.DiagnosticSet) {
	groupExpressions := make([]directBitmapGroupExpression, 0, len(groupBy))
	for _, expr := range groupBy {
		refs := qsbridge.FieldRefs(expr)
		if len(refs) == 0 {
			return nil, directBitmapAggregateDiagnostics("GROUP BY expression must reference at least one field")
		}
		groupExpressions = append(groupExpressions, directBitmapGroupExpression{Expr: expr, Field: refs[0]})
	}
	return groupExpressions, nil
}

func directBitmapComputedGroupExpressionCount(groupExpressions []directBitmapGroupExpression) int {
	count := 0
	for _, groupExpression := range groupExpressions {
		if !directBitmapGroupExpressionIsField(groupExpression) {
			count++
		}
	}
	return count
}

func directBitmapGroupExpressionShapes(groupExpressions []directBitmapGroupExpression) string {
	shapes := make([]string, 0, len(groupExpressions))
	for _, groupExpression := range groupExpressions {
		shapes = append(shapes, directBitmapGroupExpressionShape(groupExpression.Expr))
	}
	return strings.Join(shapes, ";")
}

func directBitmapGroupExpressionFields(groupExpressions []directBitmapGroupExpression) string {
	fields := make([]string, 0, len(groupExpressions))
	for _, groupExpression := range groupExpressions {
		fields = append(fields, directBitmapGroupExpressionFieldLabel(groupExpression.Field))
	}
	return strings.Join(fields, ";")
}

func directBitmapGroupExpressionShape(expr qsbridge.Expr) string {
	if field, ok := directBitmapExprField(expr); ok {
		return "field:" + directBitmapGroupExpressionFieldLabel(field)
	}
	if call, ok := directBitmapCallExpr(expr); ok {
		args := make([]string, 0, len(call.Args))
		for _, arg := range call.Args {
			args = append(args, directBitmapGroupExpressionShape(arg))
		}
		return "call:" + strings.ToLower(call.Name) + "(" + strings.Join(args, ",") + ")"
	}
	if _, ok := directBitmapLiteralExpr(expr); ok {
		return "literal"
	}
	if binary, ok := directBitmapBinaryExpr(expr); ok {
		return "binary:" + string(binary.Op)
	}
	return "expr"
}

func directBitmapGroupExpressionFieldLabel(field qsbridge.FieldRef) string {
	name := field.Name
	if name == "" {
		name = field.PhysicalName
	}
	if ref := field.Table.RefName(); ref != "" {
		return ref + "." + name
	}
	return name
}

func directBitmapGroupValueColumns(materialized qsbridge.QuantaProjectedRowSet, groupExpressions []directBitmapGroupExpression) ([][]qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	columns := make([][]qsbridge.ResultCell, 0, len(groupExpressions))
	for _, groupExpression := range groupExpressions {
		values := make([]qsbridge.ResultCell, 0, materialized.CandidateCount())
		for i := 0; i < materialized.CandidateCount(); i++ {
			cell, diagnostics := directBitmapEvaluateMaterializedExpr(groupExpression.Expr, materialized, i)
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			values = append(values, cell)
		}
		columns = append(columns, values)
	}
	return columns, nil
}

func directBitmapGroupExpressionIndex(expr qsbridge.Expr, groupExpressions []directBitmapGroupExpression) int {
	if field, ok := directBitmapExprField(expr); ok {
		for i, groupExpression := range groupExpressions {
			if directBitmapFieldRefsEqual(field, groupExpression.Field) && directBitmapGroupExpressionIsField(groupExpression) {
				return i
			}
		}
	}
	for i, groupExpression := range groupExpressions {
		if directBitmapGroupExpressionsEqual(expr, groupExpression.Expr) {
			return i
		}
	}
	return -1
}

func directBitmapGroupExpressionIsField(groupExpression directBitmapGroupExpression) bool {
	_, ok := directBitmapExprField(groupExpression.Expr)
	return ok
}

func directBitmapFieldRefsEqual(left qsbridge.FieldRef, right qsbridge.FieldRef) bool {
	if left.Table.Table != right.Table.Table || directBitmapFieldPhysicalName(left) != directBitmapFieldPhysicalName(right) {
		return false
	}
	return strings.EqualFold(materializationFieldRole(left.Table.Table, left), materializationFieldRole(right.Table.Table, right))
}

func directBitmapGroupExpressionsEqual(left qsbridge.Expr, right qsbridge.Expr) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	if leftField, ok := directBitmapExprField(left); ok {
		rightField, rightOK := directBitmapExprField(right)
		return rightOK && directBitmapFieldRefsEqual(leftField, rightField)
	}
	if leftCall, ok := directBitmapCallExpr(left); ok {
		rightCall, rightOK := directBitmapCallExpr(right)
		if !rightOK || !strings.EqualFold(leftCall.Name, rightCall.Name) || len(leftCall.Args) != len(rightCall.Args) {
			return false
		}
		for i := range leftCall.Args {
			if !directBitmapGroupExpressionsEqual(leftCall.Args[i], rightCall.Args[i]) {
				return false
			}
		}
		return true
	}
	if leftLiteral, ok := directBitmapLiteralExpr(left); ok {
		rightLiteral, rightOK := directBitmapLiteralExpr(right)
		return rightOK && leftLiteral.Kind == rightLiteral.Kind && directBitmapCellEqual(directBitmapLiteralCell(leftLiteral), directBitmapLiteralCell(rightLiteral))
	}
	if leftBinary, ok := directBitmapBinaryExpr(left); ok {
		rightBinary, rightOK := directBitmapBinaryExpr(right)
		return rightOK &&
			leftBinary.Op == rightBinary.Op &&
			directBitmapGroupExpressionsEqual(leftBinary.Left, rightBinary.Left) &&
			directBitmapGroupExpressionsEqual(leftBinary.Right, rightBinary.Right)
	}
	if leftCase, ok := directBitmapSearchedCaseExpr(left); ok {
		rightCase, rightOK := directBitmapSearchedCaseExpr(right)
		if !rightOK || len(leftCase.Whens) != len(rightCase.Whens) || !directBitmapGroupExpressionsEqual(leftCase.Else, rightCase.Else) {
			return false
		}
		for i := range leftCase.Whens {
			if !directBitmapGroupExpressionsEqual(leftCase.Whens[i].Condition, rightCase.Whens[i].Condition) ||
				!directBitmapGroupExpressionsEqual(leftCase.Whens[i].Result, rightCase.Whens[i].Result) {
				return false
			}
		}
		return true
	}
	if leftList, ok := directBitmapListExpr(left); ok {
		rightList, rightOK := directBitmapListExpr(right)
		if !rightOK || len(leftList.Items) != len(rightList.Items) {
			return false
		}
		for i := range leftList.Items {
			if !directBitmapGroupExpressionsEqual(leftList.Items[i], rightList.Items[i]) {
				return false
			}
		}
		return true
	}
	leftRef, leftOK := directBitmapExprAggregateRef(left)
	rightRef, rightOK := directBitmapExprAggregateRef(right)
	if leftOK || rightOK {
		return leftOK && rightOK && leftRef.Index == rightRef.Index && strings.EqualFold(leftRef.Alias, rightRef.Alias)
	}
	return false
}

func directBitmapGroupFieldIndex(field qsbridge.FieldRef, groupFields []qsbridge.FieldRef) int {
	for i, groupField := range groupFields {
		if directBitmapSameField(field, groupField) {
			return i
		}
	}
	return -1
}

func directBitmapExprField(expr qsbridge.Expr) (qsbridge.FieldRef, bool) {
	switch n := expr.(type) {
	case qsbridge.FieldExpr:
		return n.Ref, true
	case *qsbridge.FieldExpr:
		if n != nil {
			return n.Ref, true
		}
	}
	return qsbridge.FieldRef{}, false
}

func directBitmapExprAggregateRef(expr qsbridge.Expr) (qsbridge.AggregateRefExpr, bool) {
	switch n := expr.(type) {
	case qsbridge.AggregateRefExpr:
		return n, true
	case *qsbridge.AggregateRefExpr:
		if n != nil {
			return *n, true
		}
	}
	return qsbridge.AggregateRefExpr{}, false
}

func directBitmapFilterGroupedAggregateRows(predicates []qsbridge.Predicate, rows []directBitmapGroupedAggregateRow, optionalGroupExpressions ...[]directBitmapGroupExpression) ([]directBitmapGroupedAggregateRow, qsbridge.DiagnosticSet) {
	if len(predicates) == 0 {
		return rows, nil
	}
	var groupExpressions []directBitmapGroupExpression
	if len(optionalGroupExpressions) > 0 {
		groupExpressions = optionalGroupExpressions[0]
	}
	filtered := make([]directBitmapGroupedAggregateRow, 0, len(rows))
	for _, row := range rows {
		matches, diagnostics := directBitmapGroupedAggregateRowMatches(predicates, row, groupExpressions)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		if matches {
			filtered = append(filtered, row)
		}
	}
	return filtered, nil
}

func directBitmapGroupedAggregateRowMatches(predicates []qsbridge.Predicate, row directBitmapGroupedAggregateRow, groupExpressions []directBitmapGroupExpression) (bool, qsbridge.DiagnosticSet) {
	matched := true
	for i, predicate := range predicates {
		current, diagnostics := directBitmapEvaluateGroupedHavingBoolExpr(predicate.Expr, row, groupExpressions)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		if i == 0 || predicate.Combinator == qsbridge.PredicateCombinatorAnd || predicate.Combinator == "" {
			matched = matched && current
			continue
		}
		if predicate.Combinator == qsbridge.PredicateCombinatorOr {
			matched = matched || current
			continue
		}
		return false, directBitmapAggregateDiagnostics(fmt.Sprintf("unsupported grouped HAVING combinator: %s", predicate.Combinator))
	}
	return matched, nil
}

func directBitmapEvaluateGroupedHavingBoolExpr(expr qsbridge.Expr, row directBitmapGroupedAggregateRow, groupExpressions []directBitmapGroupExpression) (bool, qsbridge.DiagnosticSet) {
	binary, ok := directBitmapBinaryExpr(expr)
	if !ok {
		return false, directBitmapAggregateDiagnostics("grouped HAVING must be a binary expression")
	}
	if binary.Op == qsbridge.BinaryOpAnd || binary.Op == qsbridge.BinaryOpOr {
		left, diagnostics := directBitmapEvaluateGroupedHavingBoolExpr(binary.Left, row, groupExpressions)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		right, diagnostics := directBitmapEvaluateGroupedHavingBoolExpr(binary.Right, row, groupExpressions)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		if binary.Op == qsbridge.BinaryOpAnd {
			return left && right, nil
		}
		return left || right, nil
	}
	if !directBitmapSupportedComparisonOp(binary.Op) {
		return false, directBitmapAggregateDiagnostics("grouped HAVING must compare grouped expressions")
	}
	left, diagnostics := directBitmapEvaluateGroupedProjectionExpr(binary.Left, row, groupExpressions)
	if diagnostics.BlocksNative() {
		return false, diagnostics
	}
	right, diagnostics := directBitmapEvaluateGroupedProjectionExpr(binary.Right, row, groupExpressions)
	if diagnostics.BlocksNative() {
		return false, diagnostics
	}
	return directBitmapResidualCompareCells(binary.Op, left, right), nil
}

func directBitmapLiteralExpr(expr qsbridge.Expr) (qsbridge.LiteralExpr, bool) {
	switch n := expr.(type) {
	case qsbridge.LiteralExpr:
		return n, true
	case *qsbridge.LiteralExpr:
		if n != nil {
			return *n, true
		}
	}
	return qsbridge.LiteralExpr{}, false
}

func directBitmapBinaryExpr(expr qsbridge.Expr) (qsbridge.BinaryExpr, bool) {
	switch n := expr.(type) {
	case qsbridge.BinaryExpr:
		return n, true
	case *qsbridge.BinaryExpr:
		if n != nil {
			return *n, true
		}
	}
	return qsbridge.BinaryExpr{}, false
}

func directBitmapSupportedComparisonOp(op qsbridge.BinaryOp) bool {
	switch op {
	case qsbridge.BinaryOpEqual, qsbridge.BinaryOpNotEqual, qsbridge.BinaryOpLess, qsbridge.BinaryOpLessEqual, qsbridge.BinaryOpGreater, qsbridge.BinaryOpGreaterEqual:
		return true
	default:
		return false
	}
}

func directBitmapReverseComparisonOp(op qsbridge.BinaryOp) (qsbridge.BinaryOp, bool) {
	switch op {
	case qsbridge.BinaryOpEqual:
		return qsbridge.BinaryOpEqual, true
	case qsbridge.BinaryOpNotEqual:
		return qsbridge.BinaryOpNotEqual, true
	case qsbridge.BinaryOpLess:
		return qsbridge.BinaryOpGreater, true
	case qsbridge.BinaryOpLessEqual:
		return qsbridge.BinaryOpGreaterEqual, true
	case qsbridge.BinaryOpGreater:
		return qsbridge.BinaryOpLess, true
	case qsbridge.BinaryOpGreaterEqual:
		return qsbridge.BinaryOpLessEqual, true
	default:
		return "", false
	}
}

func directBitmapCellComparesLiteral(op qsbridge.BinaryOp, cell qsbridge.ResultCell, literal qsbridge.LiteralExpr) bool {
	right := directBitmapLiteralCell(literal)
	switch op {
	case qsbridge.BinaryOpEqual:
		return directBitmapCellEqual(cell, right)
	case qsbridge.BinaryOpNotEqual:
		return !directBitmapCellEqual(cell, right)
	case qsbridge.BinaryOpLess:
		return directBitmapCellLess(cell, right)
	case qsbridge.BinaryOpLessEqual:
		return directBitmapCellLess(cell, right) || directBitmapCellEqual(cell, right)
	case qsbridge.BinaryOpGreater:
		return directBitmapCellLess(right, cell)
	case qsbridge.BinaryOpGreaterEqual:
		return directBitmapCellLess(right, cell) || directBitmapCellEqual(cell, right)
	default:
		return false
	}
}

func directBitmapLiteralCell(literal qsbridge.LiteralExpr) qsbridge.ResultCell {
	return qsbridge.ResultCell{Kind: literal.Kind, Value: literal.Value}
}

func directBitmapSameField(left qsbridge.FieldRef, right qsbridge.FieldRef) bool {
	return strings.EqualFold(left.Table.Table, right.Table.Table) && directBitmapFieldPhysicalName(left) == directBitmapFieldPhysicalName(right)
}

func directBitmapGroupKey(cell qsbridge.ResultCell) string {
	return string(cell.Kind) + ":" + fmt.Sprint(cell.Value)
}

func directBitmapGroupCellsKey(cells []qsbridge.ResultCell) string {
	if len(cells) == 0 {
		return ""
	}
	var builder strings.Builder
	for i, cell := range cells {
		if i > 0 {
			builder.WriteByte('\x1f')
		}
		builder.WriteString(string(cell.Kind))
		builder.WriteByte(':')
		builder.WriteString(fmt.Sprint(cell.Value))
	}
	return builder.String()
}

func directBitmapGroupValuesKeyAt(groupValues [][]qsbridge.ResultCell, row int) (string, qsbridge.DiagnosticSet) {
	if len(groupValues) == 0 {
		return "", nil
	}
	var builder strings.Builder
	for i := range groupValues {
		if row >= len(groupValues[i]) {
			return "", directBitmapAggregateDiagnostics("group value field has fewer values than grouped candidates")
		}
		if i > 0 {
			builder.WriteByte('\x1f')
		}
		cell := groupValues[i][row]
		builder.WriteString(string(cell.Kind))
		builder.WriteByte(':')
		builder.WriteString(fmt.Sprint(cell.Value))
	}
	return builder.String(), nil
}

func directBitmapGroupValueCellsAt(groupValues [][]qsbridge.ResultCell, row int) ([]qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	cells := make([]qsbridge.ResultCell, len(groupValues))
	for i := range groupValues {
		if row >= len(groupValues[i]) {
			return nil, directBitmapAggregateDiagnostics("group value field has fewer values than grouped candidates")
		}
		cells[i] = groupValues[i][row]
	}
	return cells, nil
}

func directBitmapCellEqual(left qsbridge.ResultCell, right qsbridge.ResultCell) bool {
	leftTime, leftTimeOK := directBitmapTimeCellValue(left)
	rightTime, rightTimeOK := directBitmapTimeCellValue(right)
	if leftTimeOK && rightTimeOK {
		return leftTime.UTC().Equal(rightTime.UTC())
	}
	return left.Kind == right.Kind && fmt.Sprint(left.Value) == fmt.Sprint(right.Value)
}

func directBitmapCellLess(left qsbridge.ResultCell, right qsbridge.ResultCell) bool {
	leftTime, leftTimeOK := directBitmapTimeCellValue(left)
	rightTime, rightTimeOK := directBitmapTimeCellValue(right)
	if leftTimeOK && rightTimeOK {
		return leftTime.UTC().Before(rightTime.UTC())
	}
	leftNumber, leftOK := directBitmapNumericCellValue(left)
	rightNumber, rightOK := directBitmapNumericCellValue(right)
	if leftOK && rightOK {
		return leftNumber < rightNumber
	}
	return fmt.Sprint(left.Value) < fmt.Sprint(right.Value)
}

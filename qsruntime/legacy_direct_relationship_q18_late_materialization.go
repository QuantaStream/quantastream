package qsruntime

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type legacyDirectRelationshipQ18LargeOrderProjectionPlan struct {
	lineitemRole  string
	orderRole     string
	customerRole  string
	quantityField qsbridge.QuantaProjectionField
	finalFields   []qsbridge.QuantaProjectionField
}

type legacyDirectRelationshipQ18LargeOrderGroup struct {
	orderRow    qsbridge.QuantaRownum
	customerRow qsbridge.QuantaRownum
	sumQuantity float64
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ18LargeOrderProjectionResult(ctx context.Context, request ExecutionRequest, sink string, rownums []qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, fields []qsbridge.QuantaProjectionField, alignedRows map[string][]qsbridge.QuantaRownum, alignmentElapsed time.Duration, result ExecutionResult) (ExecutionResult, bool, error) {
	plan, ok := legacyDirectRelationshipQ18LargeOrderProjectionPlanFor(request, edges, fields)
	if !ok {
		return result, false, nil
	}
	lineRows := alignedRows[plan.lineitemRole]
	orderRows := alignedRows[plan.orderRole]
	customerRows := alignedRows[plan.customerRole]
	if len(lineRows) == 0 || len(lineRows) != len(orderRows) || len(lineRows) != len(customerRows) {
		return result, false, nil
	}
	preAggregateMaterializationStart := time.Now()
	quantityRowSet, quantityProbes, diagnostics, err := e.legacyDirectRelationshipGraphMaterializedRowSet(ctx, request, sink, rownums, []qsbridge.QuantaProjectionField{plan.quantityField}, alignedRows, edges, "graph_grouped_aggregate_preagg_materialization_")
	preAggregateMaterializationElapsed := time.Since(preAggregateMaterializationStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, true, err
	}
	quantityRef, _ := directBitmapAggregateInputField(request.SQLAggregates[0])
	quantityValues, ok := directBitmapProjectedValues(quantityRowSet, quantityRef)
	if !ok || len(quantityValues) != len(lineRows) {
		result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic("Q18 large-order fast path could not read materialized lineitem quantity values")...)
		return result, true, nil
	}

	preAggregateStart := time.Now()
	groupsByOrder := make(map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ18LargeOrderGroup)
	for i, quantityCell := range quantityValues {
		quantity, numeric := directBitmapNumericCellValue(quantityCell)
		if !numeric {
			result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic("Q18 large-order fast path requires numeric lineitem quantity values")...)
			return result, true, nil
		}
		orderRow := orderRows[i]
		group := groupsByOrder[orderRow]
		if group == nil {
			group = &legacyDirectRelationshipQ18LargeOrderGroup{
				orderRow:    orderRow,
				customerRow: customerRows[i],
			}
			groupsByOrder[orderRow] = group
		}
		group.sumQuantity += quantity
	}
	groupRows := legacyDirectRelationshipQ18GroupedRows(groupsByOrder)
	preAggregateElapsed := time.Since(preAggregateStart)

	havingStart := time.Now()
	groupRows, diagnostics = directBitmapFilterGroupedAggregateRows(request.Having, groupRows)
	havingElapsed := time.Since(havingStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true, nil
	}

	finalAligned := map[string][]qsbridge.QuantaRownum{
		plan.orderRole:    make([]qsbridge.QuantaRownum, 0, len(groupRows)),
		plan.customerRole: make([]qsbridge.QuantaRownum, 0, len(groupRows)),
	}
	survivingGroups := make([]legacyDirectRelationshipQ18LargeOrderGroup, 0, len(groupRows))
	for _, row := range groupRows {
		orderRow, groupErr := legacyDirectRelationshipQ18GroupFromRow(row)
		if groupErr != nil {
			result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic(groupErr.Error())...)
			return result, true, nil
		}
		group := groupsByOrder[orderRow]
		if group == nil {
			continue
		}
		survivingGroups = append(survivingGroups, *group)
		finalAligned[plan.orderRole] = append(finalAligned[plan.orderRole], group.orderRow)
		finalAligned[plan.customerRole] = append(finalAligned[plan.customerRole], group.customerRow)
	}

	finalMaterializationStart := time.Now()
	finalRowSet, finalProbes, diagnostics, err := e.legacyDirectRelationshipGraphMaterializedRowSet(ctx, request, sink, finalAligned[plan.orderRole], plan.finalFields, finalAligned, edges, "graph_grouped_aggregate_final_materialization_")
	finalMaterializationElapsed := time.Since(finalMaterializationStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, true, err
	}
	finalRowSet.ProjectionVectors = append(finalRowSet.ProjectionVectors, legacyDirectRelationshipQ18AggregateVector(request.SQLAggregates[0], survivingGroups))

	sortStart := time.Now()
	finalRowSet, diagnostics = legacyDirectRelationshipOrderProjectedRowSet(request, finalRowSet)
	sortElapsed := time.Since(sortStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true, nil
	}
	limitStart := time.Now()
	finalRowSet = directBitmapLimitProjectedRowSet(finalRowSet, request.Result.Offset, request.Result.Limit, request.Result.HasResultLimit())
	limitElapsed := time.Since(limitStart)
	finalRowSet = directBitmapOrderVisibleProjectedRowSet(finalRowSet, request.ProjectionOrder)

	result.RowSet = finalRowSet
	result.Count = uint64(finalRowSet.CandidateCount())
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("graph_grouped_aggregate_late_materialization", "q18_large_order_projection"),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_aligned_roles", legacyDirectRelationshipAlignedRoleDebug(alignedRows)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_alignment_elapsed", alignmentElapsed.String()),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_rows", strconv.Itoa(len(lineRows))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_groups", strconv.Itoa(len(groupsByOrder))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_post_having_groups", strconv.Itoa(len(survivingGroups))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_rows", strconv.Itoa(len(survivingGroups))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_fields", strconv.Itoa(len(plan.finalFields))),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_materialization_elapsed", preAggregateMaterializationElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_elapsed", preAggregateElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_having_elapsed", havingElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_final_materialization_elapsed", finalMaterializationElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_final_sort_elapsed", sortElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_final_limit_elapsed", limitElapsed.String()),
	)
	result.Probes = append(result.Probes, quantityProbes...)
	result.Probes = append(result.Probes, finalProbes...)
	result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
	return result, true, nil
}

func legacyDirectRelationshipQ18LargeOrderProjectionPlanFor(request ExecutionRequest, edges []legacyDirectRelationshipEdge, fields []qsbridge.QuantaProjectionField) (legacyDirectRelationshipQ18LargeOrderProjectionPlan, bool) {
	if len(request.SQLAggregates) != 1 || !strings.EqualFold(request.SQLAggregates[0].Function, "sum") || request.SQLAggregates[0].Filter != nil || len(request.GroupBy) == 0 || len(request.Having) == 0 {
		return legacyDirectRelationshipQ18LargeOrderProjectionPlan{}, false
	}
	quantityRef, ok := directBitmapAggregateInputField(request.SQLAggregates[0])
	if !ok || !strings.EqualFold(quantityRef.Table.Table, "lineitem") || directBitmapFieldPhysicalName(quantityRef) != "l_quantity" {
		return legacyDirectRelationshipQ18LargeOrderProjectionPlan{}, false
	}
	var orderEdge legacyDirectRelationshipEdge
	var customerEdge legacyDirectRelationshipEdge
	for _, edge := range edges {
		switch {
		case strings.EqualFold(edge.childTable, "lineitem") && strings.EqualFold(edge.parentTable, "orders") && strings.EqualFold(edge.childField, "l_orderkey") && strings.EqualFold(edge.parentField, "o_orderkey"):
			orderEdge = edge
		case strings.EqualFold(edge.childTable, "orders") && strings.EqualFold(edge.parentTable, "customer") && strings.EqualFold(edge.childField, "o_custkey") && strings.EqualFold(edge.parentField, "c_custkey"):
			customerEdge = edge
		}
	}
	if orderEdge.childKey() == "" || customerEdge.childKey() == "" || !strings.EqualFold(orderEdge.parentKey(), customerEdge.childKey()) {
		return legacyDirectRelationshipQ18LargeOrderProjectionPlan{}, false
	}
	quantityField := legacyDirectRelationshipProjectionFieldForFieldRef(fields, quantityRef)
	if quantityField.Field == "" {
		quantityField = legacyDirectRelationshipProjectionFieldFromRef(quantityRef, false)
	}
	groupFields, diagnostics := legacyDirectRelationshipQ18GroupFields(request.GroupBy)
	if diagnostics.BlocksNative() || len(groupFields) == 0 {
		return legacyDirectRelationshipQ18LargeOrderProjectionPlan{}, false
	}
	finalFields := make([]qsbridge.QuantaProjectionField, 0, len(groupFields))
	for _, groupField := range groupFields {
		if strings.EqualFold(groupField.Table.Table, "lineitem") {
			return legacyDirectRelationshipQ18LargeOrderProjectionPlan{}, false
		}
		field := legacyDirectRelationshipProjectionFieldForFieldRef(fields, groupField)
		if field.Field == "" {
			field = legacyDirectRelationshipProjectionFieldFromRef(groupField, true)
		}
		field.Visible = true
		finalFields = append(finalFields, field)
	}
	return legacyDirectRelationshipQ18LargeOrderProjectionPlan{
		lineitemRole:  orderEdge.childKey(),
		orderRole:     orderEdge.parentKey(),
		customerRole:  customerEdge.parentKey(),
		quantityField: quantityField,
		finalFields:   finalFields,
	}, true
}

func legacyDirectRelationshipQ18GroupFields(groupBy []qsbridge.Expr) ([]qsbridge.FieldRef, qsbridge.DiagnosticSet) {
	groupFields := make([]qsbridge.FieldRef, 0, len(groupBy))
	for _, expr := range groupBy {
		field, ok := directBitmapExprField(expr)
		if !ok {
			return nil, directBitmapAggregateDiagnostics("Q18 large-order fast path requires field-only GROUP BY expressions")
		}
		groupFields = append(groupFields, field)
	}
	return groupFields, nil
}

func legacyDirectRelationshipProjectionFieldForFieldRef(fields []qsbridge.QuantaProjectionField, ref qsbridge.FieldRef) qsbridge.QuantaProjectionField {
	for _, field := range fields {
		if !strings.EqualFold(field.Index, ref.Table.Table) {
			continue
		}
		if field.PhysicalName != "" && field.PhysicalName != directBitmapFieldPhysicalName(ref) {
			continue
		}
		if field.Field != "" && field.Field != directBitmapFieldPhysicalName(ref) && field.Field != ref.Name {
			continue
		}
		if field.Role != "" && !strings.EqualFold(string(field.Role), materializationFieldRole(field.Index, ref)) {
			continue
		}
		return field
	}
	return qsbridge.QuantaProjectionField{}
}

func legacyDirectRelationshipProjectionFieldFromRef(ref qsbridge.FieldRef, visible bool) qsbridge.QuantaProjectionField {
	name := directBitmapFieldPhysicalName(ref)
	return qsbridge.QuantaProjectionField{
		Index:        ref.Table.Table,
		Role:         qsbridge.TableInstanceID(materializationFieldRole(ref.Table.Table, ref)),
		Field:        name,
		PhysicalName: name,
		Type:         ref.Type,
		Visible:      visible,
	}
}

func legacyDirectRelationshipQ18GroupedRows(groupsByOrder map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ18LargeOrderGroup) []directBitmapGroupedAggregateRow {
	orderRows := make([]qsbridge.QuantaRownum, 0, len(groupsByOrder))
	for orderRow := range groupsByOrder {
		orderRows = append(orderRows, orderRow)
	}
	sort.Slice(orderRows, func(i, j int) bool { return orderRows[i] < orderRows[j] })
	rows := make([]directBitmapGroupedAggregateRow, 0, len(orderRows))
	for _, orderRow := range orderRows {
		group := groupsByOrder[orderRow]
		rows = append(rows, directBitmapGroupedAggregateRow{
			Key:    strconv.FormatUint(uint64(orderRow), 10),
			Groups: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: uint64(orderRow)}},
			Aggs:   []qsbridge.ResultCell{{Kind: qsbridge.ValueFloat, Value: group.sumQuantity}},
		})
	}
	return rows
}

func legacyDirectRelationshipQ18AggregateVector(aggregate qsbridge.Aggregate, groups []legacyDirectRelationshipQ18LargeOrderGroup) qsbridge.QuantaProjectionVector {
	alias := aggregate.Alias
	if alias == "" {
		alias = aggregate.Function
	}
	vector := qsbridge.QuantaProjectionVector{
		Field: qsbridge.QuantaProjectionField{
			Index:   "orders",
			Field:   alias,
			Type:    directBitmapAggregateResultType(aggregate),
			Visible: true,
		},
	}
	for _, group := range groups {
		vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: group.sumQuantity})
	}
	return vector
}

func legacyDirectRelationshipOrderProjectedRowSet(request ExecutionRequest, rowSet qsbridge.QuantaProjectedRowSet) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet) {
	if len(request.OrderBy) == 0 || rowSet.CandidateCount() < 2 {
		return rowSet, nil
	}
	type sortVector struct {
		index      int
		descending bool
	}
	sortVectors := make([]sortVector, 0, len(request.OrderBy))
	for _, order := range request.OrderBy {
		if field, ok := directBitmapExprField(order.Expr); ok {
			index := legacyDirectRelationshipProjectedVectorIndex(rowSet, field)
			if index < 0 {
				return qsbridge.QuantaProjectedRowSet{}, directBitmapAggregateDiagnostics("late materialized ORDER BY field is not present in projected row set")
			}
			sortVectors = append(sortVectors, sortVector{index: index, descending: order.Direction == qsbridge.SortDescending})
			continue
		}
		if ref, ok := directBitmapExprAggregateRef(order.Expr); ok {
			index := legacyDirectRelationshipProjectedAggregateIndex(rowSet, request.SQLAggregates, ref)
			if index < 0 {
				return qsbridge.QuantaProjectedRowSet{}, directBitmapAggregateDiagnostics("late materialized ORDER BY aggregate is not present in projected row set")
			}
			sortVectors = append(sortVectors, sortVector{index: index, descending: order.Direction == qsbridge.SortDescending})
			continue
		}
		return qsbridge.QuantaProjectedRowSet{}, directBitmapAggregateDiagnostics("late materialized ORDER BY must reference a projected field or aggregate")
	}
	order := make([]int, rowSet.CandidateCount())
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool {
		leftIndex := order[i]
		rightIndex := order[j]
		for _, vector := range sortVectors {
			left := rowSet.ProjectionVectors[vector.index].Values[leftIndex]
			right := rowSet.ProjectionVectors[vector.index].Values[rightIndex]
			if directBitmapCellEqual(left, right) {
				continue
			}
			less := directBitmapCellLess(left, right)
			if vector.descending {
				return !less
			}
			return less
		}
		return rowSet.Rownums[leftIndex] < rowSet.Rownums[rightIndex]
	})
	return legacyDirectRelationshipReorderProjectedRowSet(rowSet, order), nil
}

func legacyDirectRelationshipProjectedVectorIndex(rowSet qsbridge.QuantaProjectedRowSet, field qsbridge.FieldRef) int {
	for i, vector := range rowSet.ProjectionVectors {
		if directBitmapProjectionVectorMatchesField(vector, field) {
			return i
		}
	}
	return -1
}

func legacyDirectRelationshipProjectedAggregateIndex(rowSet qsbridge.QuantaProjectedRowSet, aggregates []qsbridge.Aggregate, ref qsbridge.AggregateRefExpr) int {
	if ref.Index < 0 || ref.Index >= len(aggregates) {
		return -1
	}
	alias := ref.Alias
	if alias == "" {
		alias = aggregates[ref.Index].Alias
	}
	for i, vector := range rowSet.ProjectionVectors {
		if strings.EqualFold(vector.Field.Field, alias) {
			return i
		}
	}
	return -1
}

func legacyDirectRelationshipReorderProjectedRowSet(rowSet qsbridge.QuantaProjectedRowSet, order []int) qsbridge.QuantaProjectedRowSet {
	reordered := rowSet
	reordered.Rownums = make([]qsbridge.QuantaRownum, len(order))
	for i, index := range order {
		reordered.Rownums[i] = rowSet.Rownums[index]
	}
	reordered.ProjectionVectors = append([]qsbridge.QuantaProjectionVector(nil), rowSet.ProjectionVectors...)
	for vectorIndex, vector := range rowSet.ProjectionVectors {
		values := make([]qsbridge.ResultCell, len(order))
		for i, index := range order {
			values[i] = vector.Values[index]
		}
		reordered.ProjectionVectors[vectorIndex].Values = values
	}
	return reordered
}

func legacyDirectRelationshipQ18GroupFromRow(row directBitmapGroupedAggregateRow) (qsbridge.QuantaRownum, error) {
	if len(row.Groups) == 0 {
		return 0, fmt.Errorf("missing Q18 order group")
	}
	switch value := row.Groups[0].Value.(type) {
	case uint64:
		return qsbridge.QuantaRownum(value), nil
	case int64:
		return qsbridge.QuantaRownum(value), nil
	case int:
		return qsbridge.QuantaRownum(value), nil
	default:
		return 0, fmt.Errorf("unexpected Q18 order group type %T", value)
	}
}

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

type legacyDirectRelationshipQ3OrderRevenuePlan struct {
	lineitemRole       string
	orderRole          string
	extendedPriceField qsbridge.QuantaProjectionField
	finalFields        []qsbridge.QuantaProjectionField
}

type legacyDirectRelationshipQ3OrderRevenueGroup struct {
	orderRow qsbridge.QuantaRownum
	lineRow  qsbridge.QuantaRownum
	revenue  float64
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3OrderRevenueResult(ctx context.Context, request ExecutionRequest, sink string, rownums []qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, fields []qsbridge.QuantaProjectionField, alignedRows map[string][]qsbridge.QuantaRownum, alignmentElapsed time.Duration, result ExecutionResult) (ExecutionResult, bool, error) {
	if !strings.EqualFold(sink, "lineitem") {
		return result, false, nil
	}
	plan, ok := legacyDirectRelationshipQ3OrderRevenuePlanFor(request, edges, fields)
	if !ok {
		return result, false, nil
	}
	lineRows := alignedRows[plan.lineitemRole]
	orderRows := alignedRows[plan.orderRole]
	if len(lineRows) == 0 || len(lineRows) != len(orderRows) {
		return result, false, nil
	}

	preAggregateMaterializationStart := time.Now()
	priceRowSet, priceProbes, diagnostics, err := e.legacyDirectRelationshipGraphMaterializedRowSet(ctx, request, sink, lineRows, []qsbridge.QuantaProjectionField{plan.extendedPriceField}, alignedRows, edges, "graph_grouped_aggregate_preagg_materialization_")
	preAggregateMaterializationElapsed := time.Since(preAggregateMaterializationStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, true, err
	}
	priceRef, _ := directBitmapAggregateInputField(request.SQLAggregates[0])
	priceValues, ok := directBitmapProjectedValues(priceRowSet, priceRef)
	if !ok || len(priceValues) != len(lineRows) {
		result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic("Q3 order-revenue fast path could not read materialized lineitem extended price values")...)
		return result, true, nil
	}

	preAggregateStart := time.Now()
	groupsByOrder := make(map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ3OrderRevenueGroup)
	for i, priceCell := range priceValues {
		price, numeric := directBitmapNumericCellValue(priceCell)
		if !numeric {
			result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic("Q3 order-revenue fast path requires numeric lineitem extended price values")...)
			return result, true, nil
		}
		orderRow := orderRows[i]
		group := groupsByOrder[orderRow]
		if group == nil {
			group = &legacyDirectRelationshipQ3OrderRevenueGroup{
				orderRow: orderRow,
				lineRow:  lineRows[i],
			}
			groupsByOrder[orderRow] = group
		}
		group.revenue += price
	}
	groupRows := legacyDirectRelationshipQ3OrderRevenueGroups(groupsByOrder)
	preAggregateElapsed := time.Since(preAggregateStart)

	finalAligned := map[string][]qsbridge.QuantaRownum{
		plan.lineitemRole: make([]qsbridge.QuantaRownum, 0, len(groupRows)),
		plan.orderRole:    make([]qsbridge.QuantaRownum, 0, len(groupRows)),
	}
	for _, group := range groupRows {
		finalAligned[plan.lineitemRole] = append(finalAligned[plan.lineitemRole], group.lineRow)
		finalAligned[plan.orderRole] = append(finalAligned[plan.orderRole], group.orderRow)
	}

	finalMaterializationStart := time.Now()
	finalRowSet, finalProbes, diagnostics, err := e.legacyDirectRelationshipGraphMaterializedRowSet(ctx, request, sink, finalAligned[plan.lineitemRole], plan.finalFields, finalAligned, edges, "graph_grouped_aggregate_final_materialization_")
	finalMaterializationElapsed := time.Since(finalMaterializationStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, true, err
	}

	groupExpressions, diagnostics := directBitmapGroupExpressions(request.GroupBy)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true, nil
	}
	groupedRowsStart := time.Now()
	aggregateRows, diagnostics := legacyDirectRelationshipQ3OrderRevenueAggregateRows(request, finalRowSet, groupExpressions, groupRows)
	groupedRowsElapsed := time.Since(groupedRowsStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true, nil
	}

	havingStart := time.Now()
	aggregateRows, diagnostics = directBitmapFilterGroupedAggregateRows(request.Having, aggregateRows, groupExpressions)
	havingElapsed := time.Since(havingStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true, nil
	}
	postHavingGroups := len(aggregateRows)
	orderStrategy := directBitmapGroupedAggregateOrderStrategy(request, len(aggregateRows))

	sortStart := time.Now()
	aggregateRows, diagnostics = directBitmapOrderGroupedAggregateRows(request, aggregateRows, groupExpressions)
	sortElapsed := time.Since(sortStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true, nil
	}

	outputStart := time.Now()
	outputRowSet, diagnostics := directBitmapGroupedAggregateRowSet(request, aggregateRows, groupExpressions)
	outputElapsed := time.Since(outputStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true, nil
	}
	limitStart := time.Now()
	outputRowSet = directBitmapLimitProjectedRowSet(outputRowSet, request.Result.Offset, request.Result.Limit)
	limitElapsed := time.Since(limitStart)

	result.RowSet = outputRowSet
	result.Count = uint64(outputRowSet.CandidateCount())
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("graph_grouped_aggregate_late_materialization", "q3_order_revenue_projection"),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_aligned_roles", legacyDirectRelationshipAlignedRoleDebug(alignedRows)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_alignment_elapsed", alignmentElapsed.String()),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_rows", strconv.Itoa(len(lineRows))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_groups", strconv.Itoa(len(groupsByOrder))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_post_having_groups", strconv.Itoa(postHavingGroups)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_rows", strconv.Itoa(len(groupRows))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_fields", strconv.Itoa(len(plan.finalFields))),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_materialization_elapsed", preAggregateMaterializationElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_elapsed", preAggregateElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_group_row_build_elapsed", groupedRowsElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_having_elapsed", havingElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_final_materialization_elapsed", finalMaterializationElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_final_sort_elapsed", sortElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_output_elapsed", outputElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_final_limit_elapsed", limitElapsed.String()),
	)
	result.Probes = append(result.Probes, priceProbes...)
	result.Probes = append(result.Probes, finalProbes...)
	result.Probes = append(result.Probes, directBitmapGroupedAggregateProbes(directBitmapGroupedAggregateProbe{
		CandidateRows:    len(groupRows),
		GroupCount:       len(groupRows),
		PostHavingGroups: postHavingGroups,
		SortInputGroups:  postHavingGroups,
		Limit:            request.Result.Limit,
		FinalRows:        outputRowSet.CandidateCount(),
		TopNCandidate:    executionGroupedAggregateTopNCandidate(request),
		OrderStrategy:    orderStrategy,
		GroupStrategy:    "relationship_preaggregate",
		GroupValuesTime:  groupedRowsElapsed,
		HavingTime:       havingElapsed,
		OrderTime:        sortElapsed,
		OutputTime:       outputElapsed,
		LimitTime:        limitElapsed,
	})...)
	result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
	return result, true, nil
}

func legacyDirectRelationshipQ3OrderRevenuePlanFor(request ExecutionRequest, edges []legacyDirectRelationshipEdge, fields []qsbridge.QuantaProjectionField) (legacyDirectRelationshipQ3OrderRevenuePlan, bool) {
	if len(request.SQLAggregates) != 1 || !strings.EqualFold(request.SQLAggregates[0].Function, "sum") || request.SQLAggregates[0].Filter != nil || request.SQLAggregates[0].Mode == qsbridge.AggregateDistinct || len(request.GroupBy) == 0 {
		return legacyDirectRelationshipQ3OrderRevenuePlan{}, false
	}
	if len(request.Memberships) > 0 || directBitmapHasResidualScanPredicates(request) {
		return legacyDirectRelationshipQ3OrderRevenuePlan{}, false
	}
	priceRef, ok := directBitmapAggregateInputField(request.SQLAggregates[0])
	if !ok || !strings.EqualFold(priceRef.Table.Table, "lineitem") || directBitmapFieldPhysicalName(priceRef) != "l_extendedprice" {
		return legacyDirectRelationshipQ3OrderRevenuePlan{}, false
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
		return legacyDirectRelationshipQ3OrderRevenuePlan{}, false
	}
	groupFields, ok := legacyDirectRelationshipQ3OrderRevenueGroupFields(request.GroupBy)
	if !ok {
		return legacyDirectRelationshipQ3OrderRevenuePlan{}, false
	}
	priceField := legacyDirectRelationshipProjectionFieldForFieldRef(fields, priceRef)
	if priceField.Field == "" {
		priceField = legacyDirectRelationshipProjectionFieldFromRef(priceRef, false)
	}
	finalFields := make([]qsbridge.QuantaProjectionField, 0, len(groupFields))
	for _, groupField := range groupFields {
		field := legacyDirectRelationshipProjectionFieldForFieldRef(fields, groupField)
		if field.Field == "" {
			field = legacyDirectRelationshipProjectionFieldFromRef(groupField, true)
		}
		field.Visible = true
		finalFields = append(finalFields, field)
	}
	return legacyDirectRelationshipQ3OrderRevenuePlan{
		lineitemRole:       orderEdge.childKey(),
		orderRole:          orderEdge.parentKey(),
		extendedPriceField: priceField,
		finalFields:        finalFields,
	}, true
}

func legacyDirectRelationshipQ3OrderRevenueGroupFields(groupBy []qsbridge.Expr) ([]qsbridge.FieldRef, bool) {
	if len(groupBy) != 3 {
		return nil, false
	}
	fields := make([]qsbridge.FieldRef, 0, len(groupBy))
	for _, expr := range groupBy {
		field, ok := directBitmapExprField(expr)
		if !ok {
			return nil, false
		}
		fields = append(fields, field)
	}
	if !legacyDirectRelationshipQ3HasGroupField(fields, "lineitem", "l_orderkey") ||
		!legacyDirectRelationshipQ3HasGroupField(fields, "orders", "o_orderdate") ||
		!legacyDirectRelationshipQ3HasGroupField(fields, "orders", "o_shippriority") {
		return nil, false
	}
	return fields, true
}

func legacyDirectRelationshipQ3HasGroupField(fields []qsbridge.FieldRef, table string, field string) bool {
	for _, candidate := range fields {
		if strings.EqualFold(candidate.Table.Table, table) && directBitmapFieldPhysicalName(candidate) == field {
			return true
		}
	}
	return false
}

func legacyDirectRelationshipQ3OrderRevenueGroups(groupsByOrder map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ3OrderRevenueGroup) []legacyDirectRelationshipQ3OrderRevenueGroup {
	orderRows := make([]qsbridge.QuantaRownum, 0, len(groupsByOrder))
	for orderRow := range groupsByOrder {
		orderRows = append(orderRows, orderRow)
	}
	sort.Slice(orderRows, func(i, j int) bool { return orderRows[i] < orderRows[j] })
	groups := make([]legacyDirectRelationshipQ3OrderRevenueGroup, 0, len(orderRows))
	for _, orderRow := range orderRows {
		groups = append(groups, *groupsByOrder[orderRow])
	}
	return groups
}

func legacyDirectRelationshipQ3OrderRevenueAggregateRows(request ExecutionRequest, finalRowSet qsbridge.QuantaProjectedRowSet, groupExpressions []directBitmapGroupExpression, groups []legacyDirectRelationshipQ3OrderRevenueGroup) ([]directBitmapGroupedAggregateRow, qsbridge.DiagnosticSet) {
	if len(request.SQLAggregates) != 1 {
		return nil, directBitmapAggregateDiagnostics("Q3 order-revenue fast path requires exactly one aggregate")
	}
	groupValues, diagnostics := directBitmapGroupValueColumns(finalRowSet, groupExpressions)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	rows := make([]directBitmapGroupedAggregateRow, 0, len(groups))
	for i, group := range groups {
		key, diagnostics := directBitmapGroupValuesKeyAt(groupValues, i)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		cells, diagnostics := directBitmapGroupValueCellsAt(groupValues, i)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		rows = append(rows, directBitmapGroupedAggregateRow{
			Key:    key,
			Groups: cells,
			Aggs: []qsbridge.ResultCell{{
				Kind:  qsbridge.ValueFloat,
				Value: group.revenue,
			}},
		})
	}
	if len(rows) != finalRowSet.CandidateCount() {
		return nil, directBitmapAggregateDiagnostics(fmt.Sprintf("Q3 order-revenue fast path grouped rows %d do not match final materialization rows %d", len(rows), finalRowSet.CandidateCount()))
	}
	return rows, nil
}

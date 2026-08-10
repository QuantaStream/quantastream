package qsruntime

import (
	"context"
	"fmt"
	"math"
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

type legacyDirectRelationshipQ3AttributionSnapshot struct {
	graphReductionElapsed              time.Duration
	alignmentElapsed                   time.Duration
	preAggregateMaterializationElapsed time.Duration
	preAggregateElapsed                time.Duration
	groupedRowsElapsed                 time.Duration
	havingElapsed                      time.Duration
	finalMaterializationElapsed        time.Duration
	sortElapsed                        time.Duration
	outputElapsed                      time.Duration
	limitElapsed                       time.Duration
	inputLineRows                      int
	inputOrderRows                     int
	preAggregateGroups                 int
	postHavingGroups                   int
	finalMaterializationRows           int
	finalMaterializationFields         int
	outputRows                         int
	preAggregateProbes                 []ExecutionProbe
	finalMaterializationProbes         []ExecutionProbe
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3OrderRevenueResult(ctx context.Context, request ExecutionRequest, sink string, rownums []qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, fields []qsbridge.QuantaProjectionField, alignedRows map[string][]qsbridge.QuantaRownum, graphReductionElapsed time.Duration, alignmentElapsed time.Duration, result ExecutionResult) (ExecutionResult, bool, error) {
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

	groupsByOrder, priceProbes, preAggregateMaterializationElapsed, preAggregateElapsed, diagnostics, err := e.legacyDirectRelationshipQ3OrderRevenuePreAggregate(ctx, request, sink, lineRows, orderRows, alignedRows, edges, plan)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, true, err
	}
	if len(groupsByOrder) == 0 {
		result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic("Q3 order-revenue fast path produced no pre-aggregate groups")...)
		return result, true, nil
	}
	groupRows := legacyDirectRelationshipQ3OrderRevenueGroups(groupsByOrder)

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
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_graph_reduction_elapsed", graphReductionElapsed.String()),
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
	result.Probes = append(result.Probes, legacyDirectRelationshipQ3AttributionProbes(legacyDirectRelationshipQ3AttributionSnapshot{
		graphReductionElapsed:              graphReductionElapsed,
		alignmentElapsed:                   alignmentElapsed,
		preAggregateMaterializationElapsed: preAggregateMaterializationElapsed,
		preAggregateElapsed:                preAggregateElapsed,
		groupedRowsElapsed:                 groupedRowsElapsed,
		havingElapsed:                      havingElapsed,
		finalMaterializationElapsed:        finalMaterializationElapsed,
		sortElapsed:                        sortElapsed,
		outputElapsed:                      outputElapsed,
		limitElapsed:                       limitElapsed,
		inputLineRows:                      len(lineRows),
		inputOrderRows:                     len(orderRows),
		preAggregateGroups:                 len(groupsByOrder),
		postHavingGroups:                   postHavingGroups,
		finalMaterializationRows:           len(groupRows),
		finalMaterializationFields:         len(plan.finalFields),
		outputRows:                         outputRowSet.CandidateCount(),
		preAggregateProbes:                 priceProbes,
		finalMaterializationProbes:         finalProbes,
	})...)
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

func legacyDirectRelationshipQ3AttributionProbes(snapshot legacyDirectRelationshipQ3AttributionSnapshot) []ExecutionProbe {
	storageTotal := legacyDirectRelationshipProbeDuration(snapshot.preAggregateProbes, "phase_graph_grouped_aggregate_preagg_storage_elapsed")
	preAggregateTotal := snapshot.preAggregateMaterializationElapsed + snapshot.preAggregateElapsed
	if storageTotal > 0 {
		preAggregateTotal = storageTotal
	}
	finalSortOutput := snapshot.sortElapsed + snapshot.outputElapsed + snapshot.limitElapsed
	finalStageTotal := snapshot.groupedRowsElapsed + snapshot.havingElapsed + snapshot.finalMaterializationElapsed + finalSortOutput
	knownTotal := snapshot.graphReductionElapsed + snapshot.alignmentElapsed + preAggregateTotal + finalStageTotal
	storageLookup := legacyDirectRelationshipProbeDuration(snapshot.preAggregateProbes, "phase_graph_grouped_aggregate_preagg_storage_lookup_elapsed")
	storageProjection := legacyDirectRelationshipProbeDuration(snapshot.preAggregateProbes, "phase_graph_grouped_aggregate_preagg_storage_projection_elapsed")
	storageAggregate := legacyDirectRelationshipProbeDuration(snapshot.preAggregateProbes, "phase_graph_grouped_aggregate_preagg_storage_aggregate_elapsed")
	finalFetch := legacyDirectRelationshipProbeDurationBySuffix(snapshot.finalMaterializationProbes, "_fetch_elapsed")
	finalAttach := legacyDirectRelationshipProbeDurationBySuffix(snapshot.finalMaterializationProbes, "_attach_elapsed")
	return []ExecutionProbe{
		legacyDirectRelationshipProbe("q3_attribution_scope", "graph_reduction_to_output"),
		legacyDirectRelationshipProbe("q3_attribution_input_line_rows", strconv.Itoa(snapshot.inputLineRows)),
		legacyDirectRelationshipProbe("q3_attribution_input_order_rows", strconv.Itoa(snapshot.inputOrderRows)),
		legacyDirectRelationshipProbe("q3_attribution_preagg_groups", strconv.Itoa(snapshot.preAggregateGroups)),
		legacyDirectRelationshipProbe("q3_attribution_post_having_groups", strconv.Itoa(snapshot.postHavingGroups)),
		legacyDirectRelationshipProbe("q3_attribution_final_materialization_rows", strconv.Itoa(snapshot.finalMaterializationRows)),
		legacyDirectRelationshipProbe("q3_attribution_final_materialization_fields", strconv.Itoa(snapshot.finalMaterializationFields)),
		legacyDirectRelationshipProbe("q3_attribution_output_rows", strconv.Itoa(snapshot.outputRows)),
		legacyDirectRelationshipProbe("q3_attribution_graph_reduction_elapsed", snapshot.graphReductionElapsed.String()),
		legacyDirectRelationshipProbe("q3_attribution_alignment_elapsed", snapshot.alignmentElapsed.String()),
		legacyDirectRelationshipProbe("q3_attribution_preagg_total_elapsed", preAggregateTotal.String()),
		legacyDirectRelationshipProbe("q3_attribution_preagg_materialization_elapsed", snapshot.preAggregateMaterializationElapsed.String()),
		legacyDirectRelationshipProbe("q3_attribution_preagg_accumulate_elapsed", snapshot.preAggregateElapsed.String()),
		legacyDirectRelationshipProbe("q3_attribution_preagg_storage_elapsed", storageTotal.String()),
		legacyDirectRelationshipProbe("q3_attribution_preagg_storage_lookup_elapsed", storageLookup.String()),
		legacyDirectRelationshipProbe("q3_attribution_preagg_storage_projection_elapsed", storageProjection.String()),
		legacyDirectRelationshipProbe("q3_attribution_preagg_storage_aggregate_elapsed", storageAggregate.String()),
		legacyDirectRelationshipProbe("q3_attribution_final_stage_elapsed", finalStageTotal.String()),
		legacyDirectRelationshipProbe("q3_attribution_final_materialization_elapsed", snapshot.finalMaterializationElapsed.String()),
		legacyDirectRelationshipProbe("q3_attribution_final_fetch_elapsed", finalFetch.String()),
		legacyDirectRelationshipProbe("q3_attribution_final_attach_elapsed", finalAttach.String()),
		legacyDirectRelationshipProbe("q3_attribution_group_row_build_elapsed", snapshot.groupedRowsElapsed.String()),
		legacyDirectRelationshipProbe("q3_attribution_having_elapsed", snapshot.havingElapsed.String()),
		legacyDirectRelationshipProbe("q3_attribution_sort_output_elapsed", finalSortOutput.String()),
		legacyDirectRelationshipProbe("q3_attribution_known_elapsed", knownTotal.String()),
	}
}

func legacyDirectRelationshipProbeDurationBySuffix(probes []ExecutionProbe, suffix string) time.Duration {
	var total time.Duration
	for _, probe := range probes {
		if probe.Section != "relationship_join" || !strings.HasSuffix(probe.Name, suffix) {
			continue
		}
		duration, err := time.ParseDuration(probe.Value)
		if err == nil {
			total += duration
		}
	}
	return total
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3OrderRevenuePreAggregate(ctx context.Context, request ExecutionRequest, sink string, lineRows []qsbridge.QuantaRownum, orderRows []qsbridge.QuantaRownum, alignedRows map[string][]qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, plan legacyDirectRelationshipQ3OrderRevenuePlan) (map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ3OrderRevenueGroup, []ExecutionProbe, time.Duration, time.Duration, qsbridge.DiagnosticSet, error) {
	if groups, probes, projectionElapsed, aggregateElapsed, diagnostics, err, ok := e.legacyDirectRelationshipQ3OrderRevenueStorageAggregate(ctx, sink, lineRows, orderRows, plan); ok {
		return groups, probes, projectionElapsed, aggregateElapsed, diagnostics, err
	}
	return e.legacyDirectRelationshipQ3OrderRevenueMaterializedAggregate(ctx, request, sink, lineRows, orderRows, alignedRows, edges, plan)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3OrderRevenueStorageAggregate(ctx context.Context, sink string, lineRows []qsbridge.QuantaRownum, orderRows []qsbridge.QuantaRownum, plan legacyDirectRelationshipQ3OrderRevenuePlan) (map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ3OrderRevenueGroup, []ExecutionProbe, time.Duration, time.Duration, qsbridge.DiagnosticSet, error, bool) {
	if e.RelationshipAggregateReader == nil || !strings.EqualFold(sink, "lineitem") {
		return nil, nil, 0, 0, nil, nil, false
	}
	valueField := legacyDirectRelationshipQ3ProjectionPhysicalField(plan.extendedPriceField)
	if valueField == "" {
		return nil, nil, 0, 0, nil, nil, false
	}
	start := time.Now()
	aggregate, diagnostics, ok, err := e.RelationshipAggregateReader.ReadRelationshipVectorAggregate(ctx, LegacyDirectRelationshipVectorAggregateRequest{
		VectorIndex: "lineitem",
		VectorField: "l_orderkey",
		ValueIndex:  "lineitem",
		ValueField:  valueField,
		ChildRows:   lineRows,
		ParentRows:  orderRows,
	})
	elapsed := time.Since(start)
	if err != nil || diagnostics.BlocksNative() || !ok {
		return nil, nil, 0, 0, diagnostics, err, ok
	}
	scale := e.legacyDirectRelationshipQ3FieldScale("lineitem", plan.extendedPriceField)
	groupsByOrder := make(map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ3OrderRevenueGroup, len(aggregate.Groups))
	for _, aggregateGroup := range aggregate.Groups {
		if aggregateGroup.Sum == nil {
			continue
		}
		groupsByOrder[aggregateGroup.ParentRow] = &legacyDirectRelationshipQ3OrderRevenueGroup{
			orderRow: aggregateGroup.ParentRow,
			lineRow:  aggregateGroup.RepresentativeChildRow,
			revenue:  float64(aggregateGroup.Sum.Int64()) / math.Pow10(scale),
		}
	}
	return groupsByOrder, []ExecutionProbe{
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_mode", "storage_relationship_sum"),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_mode", aggregate.Mode),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_rows", strconv.FormatUint(aggregate.Rows, 10)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_values", strconv.FormatUint(aggregate.Values, 10)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_source_values", strconv.Itoa(aggregate.SourceValues)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_target_rows", strconv.FormatUint(aggregate.TargetRows, 10)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_elapsed", elapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_lookup_elapsed", aggregate.LookupElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_projection_elapsed", aggregate.ProjectionElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_aggregate_elapsed", aggregate.AggregateElapsed.String()),
	}, aggregate.ProjectionElapsed, aggregate.AggregateElapsed, diagnostics, nil, true
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3OrderRevenueMaterializedAggregate(ctx context.Context, request ExecutionRequest, sink string, lineRows []qsbridge.QuantaRownum, orderRows []qsbridge.QuantaRownum, alignedRows map[string][]qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, plan legacyDirectRelationshipQ3OrderRevenuePlan) (map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ3OrderRevenueGroup, []ExecutionProbe, time.Duration, time.Duration, qsbridge.DiagnosticSet, error) {
	preAggregateMaterializationStart := time.Now()
	priceRowSet, priceProbes, diagnostics, err := e.legacyDirectRelationshipGraphMaterializedRowSet(ctx, request, sink, lineRows, []qsbridge.QuantaProjectionField{plan.extendedPriceField}, alignedRows, edges, "graph_grouped_aggregate_preagg_materialization_")
	preAggregateMaterializationElapsed := time.Since(preAggregateMaterializationStart)
	if err != nil || diagnostics.BlocksNative() {
		return nil, priceProbes, preAggregateMaterializationElapsed, 0, diagnostics, err
	}
	priceRef, _ := directBitmapAggregateInputField(request.SQLAggregates[0])
	priceValues, ok := directBitmapProjectedValues(priceRowSet, priceRef)
	if !ok || len(priceValues) != len(lineRows) {
		diagnostics = append(diagnostics, legacyDirectRelationshipDiagnostic("Q3 order-revenue fast path could not read materialized lineitem extended price values")...)
		return nil, priceProbes, preAggregateMaterializationElapsed, 0, diagnostics, nil
	}

	preAggregateStart := time.Now()
	groupsByOrder := make(map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ3OrderRevenueGroup)
	for i, priceCell := range priceValues {
		price, numeric := directBitmapNumericCellValue(priceCell)
		if !numeric {
			diagnostics = append(diagnostics, legacyDirectRelationshipDiagnostic("Q3 order-revenue fast path requires numeric lineitem extended price values")...)
			return nil, priceProbes, preAggregateMaterializationElapsed, 0, diagnostics, nil
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
	priceProbes = append([]ExecutionProbe{
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_mode", "materialized_value_vector"),
	}, priceProbes...)
	return groupsByOrder, priceProbes, preAggregateMaterializationElapsed, time.Since(preAggregateStart), diagnostics, nil
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

func legacyDirectRelationshipQ3ProjectionPhysicalField(field qsbridge.QuantaProjectionField) string {
	if field.PhysicalName != "" {
		return field.PhysicalName
	}
	return field.Field
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3FieldScale(tableName string, field qsbridge.QuantaProjectionField) int {
	table := e.legacyDirectCachedTable(tableName)
	if table == nil {
		return 0
	}
	fieldName := legacyDirectRelationshipQ3ProjectionPhysicalField(field)
	if fieldName == "" {
		return 0
	}
	attr, err := table.GetAttribute(fieldName)
	if err != nil || attr == nil || attr.Scale < 0 {
		return 0
	}
	return attr.Scale
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

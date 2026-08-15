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
	customerRole       string
	lineitemOrderEdge  legacyDirectRelationshipEdge
	customerOrderEdge  legacyDirectRelationshipEdge
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
	preAggregatePruneElapsed           time.Duration
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

type legacyDirectRelationshipQ3FinalMaterializationPrune struct {
	mode       string
	applied    bool
	rowsBefore int
	rowsAfter  int
	elapsed    time.Duration
}

type legacyDirectRelationshipQ3PreAggregatePrune struct {
	mode         string
	applied      bool
	rowsBefore   int
	rowsAfter    int
	groupsBefore int
	groupsAfter  int
	elapsed      time.Duration
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
	inputLineRows := len(lineRows)
	inputOrderRows := len(orderRows)
	lineRows, orderRows, preAggregatePrune := legacyDirectRelationshipQ3OrderRevenuePreAggregateRows(request, lineRows, orderRows)
	prefetchedFinalMaterialization := e.legacyDirectRelationshipQ3OrderRevenueStartFinalMaterializationPrefetch(ctx, request, plan, orderRows)

	groupsByOrder, priceProbes, preAggregateMaterializationElapsed, preAggregateElapsed, diagnostics, err := e.legacyDirectRelationshipQ3OrderRevenuePreAggregate(ctx, request, sink, lineRows, orderRows, alignedRows, edges, plan)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		prefetchedFinalMaterialization.stop()
		return result, true, err
	}
	if len(groupsByOrder) == 0 {
		prefetchedFinalMaterialization.stop()
		result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic("Q3 order-revenue fast path produced no pre-aggregate groups")...)
		return result, true, nil
	}
	return e.legacyDirectRelationshipQ3OrderRevenueResultFromGroups(ctx, request, sink, edges, plan, alignedRows, groupsByOrder, priceProbes, len(lineRows), inputLineRows, inputOrderRows, preAggregatePrune, prefetchedFinalMaterialization, graphReductionElapsed, alignmentElapsed, preAggregateMaterializationElapsed, preAggregateElapsed, result)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3OrderRevenueEarlyStorageResult(ctx context.Context, request ExecutionRequest, edges []legacyDirectRelationshipEdge, fields []qsbridge.QuantaProjectionField, rowsByRole map[string][]qsbridge.QuantaRownum, fullDomainInitialRowsByRole map[string]bool, result ExecutionResult) (ExecutionResult, bool, error) {
	if len(request.GroupBy) == 0 || e.RelationshipAggregateReader == nil {
		return result, false, nil
	}
	plan, ok := legacyDirectRelationshipQ3OrderRevenuePlanFor(request, edges, fields)
	if !ok || !e.legacyDirectRelationshipQ3OrderRevenueLineitemFiltersCoveredByStorageWindow(request, plan) {
		return result, false, nil
	}
	customerRows := rowsByRole[plan.customerRole]
	orderRows := rowsByRole[plan.orderRole]
	if len(customerRows) == 0 || len(orderRows) == 0 {
		return result, false, nil
	}

	reduceStart := time.Now()
	joinedOrders, _, reduceTiming, diagnostics, err := e.legacyDirectRelationshipReduceWithProjectionRowsOptions(ctx, request, plan.customerOrderEdge, customerRows, orderRows, orderRows, legacyDirectRelationshipReduceOptions{
		omitFullDomainTargetCandidates: fullDomainInitialRowsByRole[plan.orderRole],
	})
	reduceElapsed := time.Since(reduceStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, true, err
	}
	if len(joinedOrders) == 0 {
		return result, false, nil
	}

	prefetchedFinalMaterialization := e.legacyDirectRelationshipQ3OrderRevenueStartFinalMaterializationPrefetch(ctx, request, plan, joinedOrders)
	groupsByOrder, priceProbes, preAggregateMaterializationElapsed, preAggregateElapsed, preAggregateRows, diagnostics, err, ok := e.legacyDirectRelationshipQ3OrderRevenueSourceAggregate(ctx, request, joinedOrders, plan)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		prefetchedFinalMaterialization.stop()
		return result, true, err
	}
	if !ok {
		prefetchedFinalMaterialization.stop()
		return result, false, nil
	}
	if len(groupsByOrder) == 0 {
		prefetchedFinalMaterialization.stop()
		result.Diagnostics = append(result.Diagnostics, legacyDirectRelationshipDiagnostic("Q3 order-revenue source aggregate produced no pre-aggregate groups")...)
		return result, true, nil
	}

	preAggregatePrune := legacyDirectRelationshipQ3PreAggregatePrune{
		mode:         "source_value_reverse_artifact",
		rowsBefore:   preAggregateRows,
		rowsAfter:    preAggregateRows,
		groupsBefore: len(groupsByOrder),
		groupsAfter:  len(groupsByOrder),
	}
	alignedRows := legacyDirectRelationshipQ3OrderRevenueAlignedRowsFromGroups(plan, groupsByOrder)
	result.Probes = append(result.Probes,
		legacyDirectRelationshipProbe("graph_q3_early_storage_aggregate", "true"),
		legacyDirectRelationshipProbe("graph_q3_early_customer_rows", strconv.Itoa(len(customerRows))),
		legacyDirectRelationshipProbe("graph_q3_early_order_rows_before", strconv.Itoa(len(orderRows))),
		legacyDirectRelationshipProbe("graph_q3_early_order_rows_after", strconv.Itoa(len(joinedOrders))),
		legacyDirectRelationshipProbe("phase_graph_q3_early_customer_order_reduce_elapsed", reduceElapsed.String()),
		legacyDirectRelationshipProbe("graph_q3_early_customer_order_value_vector_elapsed", reduceTiming.valueVectorElapsed.String()),
		legacyDirectRelationshipProbe("graph_q3_early_customer_order_matched_rows", strconv.Itoa(reduceTiming.matchedRows)),
		legacyDirectRelationshipProbe("graph_iterations", "1"),
		legacyDirectRelationshipProbe("graph_single_pass_applied", "true"),
		legacyDirectRelationshipProbe("graph_single_pass_reason", "q3_source_value_aggregate"),
		legacyDirectRelationshipProbe("phase_graph_reduction_elapsed", reduceElapsed.String()),
		legacyDirectRelationshipProbe("graph_sink_role", plan.lineitemRole),
		legacyDirectRelationshipProbe("graph_sink", "lineitem"),
		legacyDirectRelationshipProbe("graph_sink_rows", strconv.Itoa(preAggregateRows)),
		legacyDirectRelationshipProbe("graph_reduced_roles", legacyDirectRelationshipAlignedRoleDebug(map[string][]qsbridge.QuantaRownum{
			plan.customerRole: customerRows,
			plan.orderRole:    joinedOrders,
		})),
	)
	return e.legacyDirectRelationshipQ3OrderRevenueResultFromGroups(ctx, request, "lineitem", edges, plan, alignedRows, groupsByOrder, priceProbes, preAggregateRows, preAggregateRows, len(joinedOrders), preAggregatePrune, prefetchedFinalMaterialization, reduceElapsed, 0, preAggregateMaterializationElapsed, preAggregateElapsed, result)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3OrderRevenueLineitemFiltersCoveredByStorageWindow(request ExecutionRequest, plan legacyDirectRelationshipQ3OrderRevenuePlan) bool {
	for _, fragment := range request.Query.Fragments {
		if !legacyDirectRelationshipFragmentTargetsTableRole(request, fragment, "lineitem", plan.lineitemRole) {
			continue
		}
		if fragment.BSIOp == qsbridge.QuantaBSIOpRange && fragment.Begin != nil && fragment.End != nil && e.legacyDirectRelationshipFragmentIsShardTimeField("lineitem", fragment) {
			continue
		}
		return false
	}
	return true
}

func legacyDirectRelationshipQ3OrderRevenueAlignedRowsFromGroups(plan legacyDirectRelationshipQ3OrderRevenuePlan, groupsByOrder map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ3OrderRevenueGroup) map[string][]qsbridge.QuantaRownum {
	groups := legacyDirectRelationshipQ3OrderRevenueGroups(groupsByOrder)
	alignedRows := map[string][]qsbridge.QuantaRownum{
		plan.lineitemRole: make([]qsbridge.QuantaRownum, 0, len(groups)),
		plan.orderRole:    make([]qsbridge.QuantaRownum, 0, len(groups)),
	}
	for _, group := range groups {
		alignedRows[plan.lineitemRole] = append(alignedRows[plan.lineitemRole], group.lineRow)
		alignedRows[plan.orderRole] = append(alignedRows[plan.orderRole], group.orderRow)
	}
	return alignedRows
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3OrderRevenueResultFromGroups(ctx context.Context, request ExecutionRequest, sink string, edges []legacyDirectRelationshipEdge, plan legacyDirectRelationshipQ3OrderRevenuePlan, alignedRows map[string][]qsbridge.QuantaRownum, groupsByOrder map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ3OrderRevenueGroup, priceProbes []ExecutionProbe, preAggregateRows int, inputLineRows int, inputOrderRows int, preAggregatePrune legacyDirectRelationshipQ3PreAggregatePrune, prefetchedFinalMaterialization *legacyDirectRelationshipQ3OrderRevenueMaterializationFuture, graphReductionElapsed time.Duration, alignmentElapsed time.Duration, preAggregateMaterializationElapsed time.Duration, preAggregateElapsed time.Duration, result ExecutionResult) (ExecutionResult, bool, error) {
	groupRows := legacyDirectRelationshipQ3OrderRevenueGroups(groupsByOrder)
	groupRows, finalPrune := legacyDirectRelationshipQ3OrderRevenueFinalMaterializationGroups(request, groupRows)

	finalAligned := map[string][]qsbridge.QuantaRownum{
		plan.lineitemRole: make([]qsbridge.QuantaRownum, 0, len(groupRows)),
		plan.orderRole:    make([]qsbridge.QuantaRownum, 0, len(groupRows)),
	}
	for _, group := range groupRows {
		finalAligned[plan.lineitemRole] = append(finalAligned[plan.lineitemRole], group.lineRow)
		finalAligned[plan.orderRole] = append(finalAligned[plan.orderRole], group.orderRow)
	}

	groupExpressions, diagnostics := directBitmapGroupExpressions(request.GroupBy)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true, nil
	}

	aggregateRows, finalProbes, finalMaterializationElapsed, groupedRowsElapsed, diagnostics, err, ok := e.legacyDirectRelationshipQ3OrderRevenueDirectAggregateRows(ctx, request, plan, groupExpressions, groupRows, prefetchedFinalMaterialization)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, true, err
	}
	if !ok {
		prefetchedFinalMaterialization.stop()
		finalMaterializationStart := time.Now()
		finalRowSet, fallbackProbes, diagnostics, err := e.legacyDirectRelationshipGraphMaterializedRowSet(ctx, request, sink, finalAligned[plan.lineitemRole], plan.finalFields, finalAligned, edges, "graph_grouped_aggregate_final_materialization_")
		finalMaterializationElapsed = time.Since(finalMaterializationStart)
		finalProbes = fallbackProbes
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if err != nil || result.Diagnostics.BlocksNative() {
			return result, true, err
		}

		groupedRowsStart := time.Now()
		aggregateRows, diagnostics = legacyDirectRelationshipQ3OrderRevenueAggregateRows(request, finalRowSet, groupExpressions, groupRows)
		groupedRowsElapsed = time.Since(groupedRowsStart)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if result.Diagnostics.BlocksNative() {
			return result, true, nil
		}
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
	skipDefaultSort := legacyDirectRelationshipQ3OrderRevenueSkipDefaultSort(request)
	if skipDefaultSort {
		orderStrategy = "q3_unordered_group_order"
	}

	var sortElapsed time.Duration
	if !skipDefaultSort {
		sortStart := time.Now()
		aggregateRows, diagnostics = directBitmapOrderGroupedAggregateRows(request, aggregateRows, groupExpressions)
		sortElapsed = time.Since(sortStart)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if result.Diagnostics.BlocksNative() {
			return result, true, nil
		}
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
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_rows", strconv.Itoa(preAggregateRows)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_prune_mode", preAggregatePrune.mode),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_prune_applied", strconv.FormatBool(preAggregatePrune.applied)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_rows_before_prune", strconv.Itoa(preAggregatePrune.rowsBefore)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_rows_after_prune", strconv.Itoa(preAggregatePrune.rowsAfter)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_groups_before_prune", strconv.Itoa(preAggregatePrune.groupsBefore)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_groups_after_prune", strconv.Itoa(preAggregatePrune.groupsAfter)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_prune_elapsed", preAggregatePrune.elapsed.String()),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_groups", strconv.Itoa(len(groupsByOrder))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_post_having_groups", strconv.Itoa(postHavingGroups)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_rows", strconv.Itoa(len(groupRows))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_fields", strconv.Itoa(len(plan.finalFields))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_prune_mode", finalPrune.mode),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_prune_applied", strconv.FormatBool(finalPrune.applied)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_rows_before_prune", strconv.Itoa(finalPrune.rowsBefore)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_rows_after_prune", strconv.Itoa(finalPrune.rowsAfter)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_final_materialization_prune_elapsed", finalPrune.elapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_materialization_elapsed", preAggregateMaterializationElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_elapsed", preAggregateElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_group_row_build_elapsed", groupedRowsElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_having_elapsed", havingElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_final_materialization_elapsed", finalMaterializationElapsed.String()),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_final_sort_skipped", strconv.FormatBool(skipDefaultSort)),
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
		preAggregatePruneElapsed:           preAggregatePrune.elapsed,
		inputLineRows:                      inputLineRows,
		inputOrderRows:                     inputOrderRows,
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
	knownTotal := snapshot.graphReductionElapsed + snapshot.alignmentElapsed + snapshot.preAggregatePruneElapsed + preAggregateTotal + finalStageTotal
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
		legacyDirectRelationshipProbe("q3_attribution_preagg_prune_elapsed", snapshot.preAggregatePruneElapsed.String()),
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
	if groups, probes, projectionElapsed, aggregateElapsed, diagnostics, err, ok := e.legacyDirectRelationshipQ3OrderRevenueStorageAggregate(ctx, request, sink, lineRows, orderRows, plan); ok {
		return groups, probes, projectionElapsed, aggregateElapsed, diagnostics, err
	}
	return e.legacyDirectRelationshipQ3OrderRevenueMaterializedAggregate(ctx, request, sink, lineRows, orderRows, alignedRows, edges, plan)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3OrderRevenueStorageAggregate(ctx context.Context, request ExecutionRequest, sink string, lineRows []qsbridge.QuantaRownum, orderRows []qsbridge.QuantaRownum, plan legacyDirectRelationshipQ3OrderRevenuePlan) (map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ3OrderRevenueGroup, []ExecutionProbe, time.Duration, time.Duration, qsbridge.DiagnosticSet, error, bool) {
	if e.RelationshipAggregateReader == nil || !strings.EqualFold(sink, "lineitem") {
		return nil, nil, 0, 0, nil, nil, false
	}
	valueField := legacyDirectRelationshipQ3ProjectionPhysicalField(plan.extendedPriceField)
	if valueField == "" {
		return nil, nil, 0, 0, nil, nil, false
	}
	aggregateLineRows, aggregateOrderRows, sortedAggregateRows := legacyDirectRelationshipQ3OrderRevenueSortedAggregateRows(lineRows, orderRows)
	fromEpochMillis, toEpochMillis := legacyDirectRelationshipQ3OrderRevenueStorageWindow(e, request, plan.lineitemRole)
	aggregateRequest := LegacyDirectRelationshipVectorAggregateRequest{
		VectorIndex:     "lineitem",
		VectorField:     "l_orderkey",
		ValueIndex:      "lineitem",
		ValueField:      valueField,
		ChildRows:       aggregateLineRows,
		ParentRows:      aggregateOrderRows,
		FromEpochMillis: fromEpochMillis,
		ToEpochMillis:   toEpochMillis,
	}
	start := time.Now()
	aggregate, diagnostics, ok, err := e.RelationshipAggregateReader.ReadRelationshipVectorAggregate(ctx, aggregateRequest)
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
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_nodes", strconv.FormatUint(aggregate.Nodes, 10)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_sorted_child_rows", strconv.FormatBool(sortedAggregateRows)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_elapsed", elapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_lookup_elapsed", aggregate.LookupElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_lookup_value_elapsed", aggregate.LookupValueElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_lookup_bitmap_elapsed", aggregate.LookupBitmapElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_lookup_to_array_elapsed", aggregate.LookupToArrayElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_lookup_row_slice_elapsed", aggregate.LookupRowSliceElapsed.String()),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_lookup_row_slice_groups", strconv.Itoa(aggregate.LookupRowSliceGroups)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_lookup_mode", aggregate.LookupMode),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_lookup_groups", strconv.Itoa(aggregate.LookupGroups)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_lookup_rows", strconv.FormatUint(aggregate.LookupRows, 10)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_projection_elapsed", aggregate.ProjectionElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_aggregate_elapsed", aggregate.AggregateElapsed.String()),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_projection_shards_visited", strconv.Itoa(aggregate.ProjectionShardsVisited)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_projection_shards_in_window", strconv.Itoa(aggregate.ProjectionShardsInWindow)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_projection_shards_local", strconv.Itoa(aggregate.ProjectionShardsLocal)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_projection_shards_retained", strconv.Itoa(aggregate.ProjectionShardsRetained)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_projection_rows_retained", strconv.FormatUint(aggregate.ProjectionRetainedRows, 10)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_projection_retain_bypass_rows", strconv.FormatUint(aggregate.ProjectionRetainBypassRows, 10)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_projection_retain_elapsed", aggregate.ProjectionRetainElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_projection_value_elapsed", aggregate.ProjectionValueElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_projection_merge_elapsed", aggregate.ProjectionMergeElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_rpc_elapsed", aggregate.ClientRPCElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_rpc_max_elapsed", aggregate.MaxClientRPCElapsed.String()),
	}, aggregate.ProjectionElapsed, aggregate.AggregateElapsed, diagnostics, nil, true
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3OrderRevenueSourceAggregate(ctx context.Context, request ExecutionRequest, sourceOrderRows []qsbridge.QuantaRownum, plan legacyDirectRelationshipQ3OrderRevenuePlan) (map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ3OrderRevenueGroup, []ExecutionProbe, time.Duration, time.Duration, int, qsbridge.DiagnosticSet, error, bool) {
	if e.RelationshipAggregateReader == nil {
		return nil, nil, 0, 0, 0, nil, nil, false
	}
	valueField := legacyDirectRelationshipQ3ProjectionPhysicalField(plan.extendedPriceField)
	if valueField == "" {
		return nil, nil, 0, 0, 0, nil, nil, false
	}
	sourceValues := legacyDirectRelationshipQ3OrderRevenueSourceValues(sourceOrderRows)
	if len(sourceValues) == 0 {
		return map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ3OrderRevenueGroup{}, nil, 0, 0, 0, nil, nil, true
	}
	fromEpochMillis, toEpochMillis := legacyDirectRelationshipQ3OrderRevenueStorageWindow(e, request, plan.lineitemRole)
	aggregateRequest := LegacyDirectRelationshipVectorAggregateRequest{
		VectorIndex:     "lineitem",
		VectorField:     "l_orderkey",
		ValueIndex:      "lineitem",
		ValueField:      valueField,
		SourceValues:    sourceValues,
		FromEpochMillis: fromEpochMillis,
		ToEpochMillis:   toEpochMillis,
	}
	start := time.Now()
	aggregate, diagnostics, ok, err := e.RelationshipAggregateReader.ReadRelationshipVectorAggregate(ctx, aggregateRequest)
	elapsed := time.Since(start)
	if err != nil || diagnostics.BlocksNative() || !ok {
		return nil, nil, 0, 0, 0, diagnostics, err, ok
	}
	scale := e.legacyDirectRelationshipQ3FieldScale("lineitem", plan.extendedPriceField)
	groupsByOrder := make(map[qsbridge.QuantaRownum]*legacyDirectRelationshipQ3OrderRevenueGroup, len(aggregate.Groups))
	for _, aggregateGroup := range aggregate.Groups {
		if aggregateGroup.Sum == nil {
			continue
		}
		orderRow := aggregateGroup.ParentRow
		groupsByOrder[orderRow] = &legacyDirectRelationshipQ3OrderRevenueGroup{
			orderRow: orderRow,
			lineRow:  aggregateGroup.RepresentativeChildRow,
			revenue:  float64(aggregateGroup.Sum.Int64()) / math.Pow10(scale),
		}
	}
	return groupsByOrder, []ExecutionProbe{
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_mode", "storage_reverse_artifact_sum"),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_mode", aggregate.Mode),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_rows", strconv.FormatUint(aggregate.Rows, 10)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_values", strconv.FormatUint(aggregate.Values, 10)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_source_values", strconv.Itoa(aggregate.SourceValues)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_target_rows", strconv.FormatUint(aggregate.TargetRows, 10)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_nodes", strconv.FormatUint(aggregate.Nodes, 10)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_sorted_child_rows", "source_value_artifact"),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_elapsed", elapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_lookup_elapsed", aggregate.LookupElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_lookup_value_elapsed", aggregate.LookupValueElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_lookup_bitmap_elapsed", aggregate.LookupBitmapElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_lookup_to_array_elapsed", aggregate.LookupToArrayElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_lookup_row_slice_elapsed", aggregate.LookupRowSliceElapsed.String()),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_lookup_row_slice_groups", strconv.Itoa(aggregate.LookupRowSliceGroups)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_lookup_mode", aggregate.LookupMode),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_lookup_groups", strconv.Itoa(aggregate.LookupGroups)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_lookup_rows", strconv.FormatUint(aggregate.LookupRows, 10)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_projection_elapsed", aggregate.ProjectionElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_aggregate_elapsed", aggregate.AggregateElapsed.String()),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_projection_shards_visited", strconv.Itoa(aggregate.ProjectionShardsVisited)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_projection_shards_in_window", strconv.Itoa(aggregate.ProjectionShardsInWindow)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_projection_shards_local", strconv.Itoa(aggregate.ProjectionShardsLocal)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_projection_shards_retained", strconv.Itoa(aggregate.ProjectionShardsRetained)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_projection_rows_retained", strconv.FormatUint(aggregate.ProjectionRetainedRows, 10)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_projection_retain_bypass_rows", strconv.FormatUint(aggregate.ProjectionRetainBypassRows, 10)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_projection_retain_elapsed", aggregate.ProjectionRetainElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_projection_value_elapsed", aggregate.ProjectionValueElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_projection_merge_elapsed", aggregate.ProjectionMergeElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_rpc_elapsed", aggregate.ClientRPCElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_rpc_max_elapsed", aggregate.MaxClientRPCElapsed.String()),
	}, aggregate.ProjectionElapsed, aggregate.AggregateElapsed, int(aggregate.TargetRows), diagnostics, nil, true
}

func legacyDirectRelationshipQ3OrderRevenueSourceValues(rows []qsbridge.QuantaRownum) []int64 {
	distinct := legacyDirectRelationshipQ3OrderRevenueSortedDistinctRows(rows)
	values := make([]int64, 0, len(distinct))
	for _, row := range distinct {
		values = append(values, int64(row))
	}
	return values
}

func legacyDirectRelationshipQ3OrderRevenueSortedAggregateRows(lineRows, orderRows []qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, []qsbridge.QuantaRownum, bool) {
	if len(lineRows) != len(orderRows) || len(lineRows) < 2 {
		return lineRows, orderRows, false
	}
	sorted := true
	for i := 1; i < len(lineRows); i++ {
		if lineRows[i-1] >= lineRows[i] {
			sorted = false
			break
		}
	}
	if sorted {
		return lineRows, orderRows, true
	}
	type aggregatePair struct {
		lineRow  qsbridge.QuantaRownum
		orderRow qsbridge.QuantaRownum
	}
	pairs := make([]aggregatePair, len(lineRows))
	for i := range lineRows {
		pairs[i] = aggregatePair{lineRow: lineRows[i], orderRow: orderRows[i]}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].lineRow == pairs[j].lineRow {
			return pairs[i].orderRow < pairs[j].orderRow
		}
		return pairs[i].lineRow < pairs[j].lineRow
	})
	sortedLineRows := make([]qsbridge.QuantaRownum, len(pairs))
	sortedOrderRows := make([]qsbridge.QuantaRownum, len(pairs))
	unique := true
	for i, pair := range pairs {
		if i > 0 && pairs[i-1].lineRow == pair.lineRow {
			unique = false
		}
		sortedLineRows[i] = pair.lineRow
		sortedOrderRows[i] = pair.orderRow
	}
	return sortedLineRows, sortedOrderRows, unique
}

func legacyDirectRelationshipQ3OrderRevenueStorageWindow(e LegacyDirectRelationshipVectorJoinExecutor, request ExecutionRequest, lineitemRole string) (int64, int64) {
	fromTime, toTime := e.legacyDirectRelationshipVectorProjectionWindowForRole(request, "lineitem", lineitemRole)
	return fromTime / int64(time.Millisecond), toTime / int64(time.Millisecond)
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
		customerRole:       customerEdge.parentKey(),
		lineitemOrderEdge:  orderEdge,
		customerOrderEdge:  customerEdge,
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

func legacyDirectRelationshipQ3OrderRevenuePreAggregateRows(request ExecutionRequest, lineRows, orderRows []qsbridge.QuantaRownum) ([]qsbridge.QuantaRownum, []qsbridge.QuantaRownum, legacyDirectRelationshipQ3PreAggregatePrune) {
	start := time.Now()
	prune := legacyDirectRelationshipQ3PreAggregatePrune{
		mode:         "none",
		rowsBefore:   len(lineRows),
		rowsAfter:    len(lineRows),
		groupsBefore: legacyDirectRelationshipQ3OrderRevenueDistinctOrderRows(orderRows),
		groupsAfter:  legacyDirectRelationshipQ3OrderRevenueDistinctOrderRows(orderRows),
	}
	window, ok := legacyDirectRelationshipQ3OrderRevenueFinalMaterializationWindow(request, prune.groupsBefore)
	if !ok || len(request.Having) > 0 || len(request.OrderBy) > 0 || len(lineRows) != len(orderRows) {
		prune.elapsed = time.Since(start)
		return lineRows, orderRows, prune
	}
	keepOrderRows := make(map[qsbridge.QuantaRownum]struct{}, window)
	for _, orderRow := range orderRows {
		if _, seen := keepOrderRows[orderRow]; !seen {
			if len(keepOrderRows) >= window {
				break
			}
			keepOrderRows[orderRow] = struct{}{}
		}
	}
	if len(keepOrderRows) == 0 || len(keepOrderRows) >= prune.groupsBefore {
		prune.elapsed = time.Since(start)
		return lineRows, orderRows, prune
	}
	prunedLineRows := make([]qsbridge.QuantaRownum, 0, len(lineRows))
	prunedOrderRows := make([]qsbridge.QuantaRownum, 0, len(orderRows))
	for i, orderRow := range orderRows {
		if _, keep := keepOrderRows[orderRow]; !keep {
			continue
		}
		prunedLineRows = append(prunedLineRows, lineRows[i])
		prunedOrderRows = append(prunedOrderRows, orderRow)
	}
	prune.mode = "unordered_limit"
	prune.applied = len(prunedLineRows) < len(lineRows)
	prune.rowsAfter = len(prunedLineRows)
	prune.groupsAfter = len(keepOrderRows)
	prune.elapsed = time.Since(start)
	return prunedLineRows, prunedOrderRows, prune
}

func legacyDirectRelationshipQ3OrderRevenueDistinctOrderRows(orderRows []qsbridge.QuantaRownum) int {
	if len(orderRows) == 0 {
		return 0
	}
	seen := make(map[qsbridge.QuantaRownum]struct{}, len(orderRows))
	for _, orderRow := range orderRows {
		seen[orderRow] = struct{}{}
	}
	return len(seen)
}

func legacyDirectRelationshipQ3OrderRevenueFinalMaterializationGroups(request ExecutionRequest, groups []legacyDirectRelationshipQ3OrderRevenueGroup) ([]legacyDirectRelationshipQ3OrderRevenueGroup, legacyDirectRelationshipQ3FinalMaterializationPrune) {
	start := time.Now()
	prune := legacyDirectRelationshipQ3FinalMaterializationPrune{
		mode:       "none",
		rowsBefore: len(groups),
		rowsAfter:  len(groups),
	}
	window, ok := legacyDirectRelationshipQ3OrderRevenueFinalMaterializationWindow(request, len(groups))
	if !ok || len(request.Having) > 0 {
		prune.elapsed = time.Since(start)
		return groups, prune
	}
	switch {
	case len(request.OrderBy) == 0:
		pruned := append([]legacyDirectRelationshipQ3OrderRevenueGroup(nil), groups[:window]...)
		prune.mode = "unordered_limit"
		prune.applied = len(pruned) < len(groups)
		prune.rowsAfter = len(pruned)
		prune.elapsed = time.Since(start)
		return pruned, prune
	case legacyDirectRelationshipQ3OrderRevenueFirstRevenueSort(request):
		pruned := legacyDirectRelationshipQ3OrderRevenueRevenueCutoffGroups(groups, window)
		prune.mode = "revenue_cutoff"
		prune.applied = len(pruned) < len(groups)
		prune.rowsAfter = len(pruned)
		prune.elapsed = time.Since(start)
		return pruned, prune
	default:
		prune.elapsed = time.Since(start)
		return groups, prune
	}
}

func legacyDirectRelationshipQ3OrderRevenueFinalMaterializationWindow(request ExecutionRequest, groupCount int) (int, bool) {
	if groupCount == 0 || request.Result.Limit <= 0 {
		return 0, false
	}
	window := request.Result.Limit + request.Result.Offset
	if window <= 0 || window >= groupCount {
		return 0, false
	}
	return window, true
}

func legacyDirectRelationshipQ3OrderRevenueFirstRevenueSort(request ExecutionRequest) bool {
	if len(request.OrderBy) == 0 || request.OrderBy[0].Direction != qsbridge.SortDescending {
		return false
	}
	ref, ok := directBitmapExprAggregateRef(request.OrderBy[0].Expr)
	return ok && ref.Index == 0
}

func legacyDirectRelationshipQ3OrderRevenueRevenueCutoffGroups(groups []legacyDirectRelationshipQ3OrderRevenueGroup, window int) []legacyDirectRelationshipQ3OrderRevenueGroup {
	ranked := append([]legacyDirectRelationshipQ3OrderRevenueGroup(nil), groups...)
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].revenue == ranked[j].revenue {
			return ranked[i].orderRow < ranked[j].orderRow
		}
		return ranked[i].revenue > ranked[j].revenue
	})
	cutoff := ranked[window-1].revenue
	if math.IsNaN(cutoff) {
		return groups
	}
	pruned := make([]legacyDirectRelationshipQ3OrderRevenueGroup, 0, window)
	for _, group := range groups {
		if group.revenue >= cutoff {
			pruned = append(pruned, group)
		}
	}
	return pruned
}

type legacyDirectRelationshipQ3OrderRevenueDirectFinalField struct {
	field         qsbridge.QuantaProjectionField
	roleKey       string
	table         string
	cells         []qsbridge.ResultCell
	values        []qsbridge.ResultCell
	valueByRow    map[qsbridge.QuantaRownum]qsbridge.ResultCell
	fetchElapsed  time.Duration
	attachElapsed time.Duration
	source        string
	sourceRole    string
}

type legacyDirectRelationshipQ3OrderRevenueMaterializationResult struct {
	rowSet      qsbridge.QuantaProjectedRowSet
	probes      []ExecutionProbe
	elapsed     time.Duration
	diagnostics qsbridge.DiagnosticSet
	err         error
	prefetched  bool
}

type legacyDirectRelationshipQ3OrderRevenueMaterializationFuture struct {
	done   <-chan legacyDirectRelationshipQ3OrderRevenueMaterializationResult
	cancel context.CancelFunc
}

func (f *legacyDirectRelationshipQ3OrderRevenueMaterializationFuture) await() (legacyDirectRelationshipQ3OrderRevenueMaterializationResult, bool) {
	if f == nil || f.done == nil {
		return legacyDirectRelationshipQ3OrderRevenueMaterializationResult{}, false
	}
	result := <-f.done
	f.stop()
	return result, true
}

func (f *legacyDirectRelationshipQ3OrderRevenueMaterializationFuture) stop() {
	if f == nil || f.cancel == nil {
		return
	}
	f.cancel()
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3OrderRevenueStartFinalMaterializationPrefetch(ctx context.Context, request ExecutionRequest, plan legacyDirectRelationshipQ3OrderRevenuePlan, orderRows []qsbridge.QuantaRownum) *legacyDirectRelationshipQ3OrderRevenueMaterializationFuture {
	if !legacyDirectRelationshipQ3OrderRevenueCanPrefetchFinalMaterialization(request) {
		return nil
	}
	orderFields := legacyDirectRelationshipQ3OrderRevenueOrderFields(plan)
	if len(orderFields) == 0 {
		return nil
	}
	finalOrderRows := legacyDirectRelationshipQ3OrderRevenueSortedDistinctRows(orderRows)
	if len(finalOrderRows) == 0 {
		return nil
	}
	prefetchCtx, cancel := context.WithCancel(ctx)
	done := make(chan legacyDirectRelationshipQ3OrderRevenueMaterializationResult, 1)
	go func() {
		materialization := e.projectionMaterializationKernel()
		start := time.Now()
		rowSet, probes, diagnostics, err := e.legacyDirectRelationshipQ3OrderRevenueMaterializedOrderRows(prefetchCtx, request, materialization, finalOrderRows, orderFields)
		done <- legacyDirectRelationshipQ3OrderRevenueMaterializationResult{
			rowSet:      rowSet,
			probes:      probes,
			elapsed:     time.Since(start),
			diagnostics: diagnostics,
			err:         err,
			prefetched:  true,
		}
	}()
	return &legacyDirectRelationshipQ3OrderRevenueMaterializationFuture{done: done, cancel: cancel}
}

func legacyDirectRelationshipQ3OrderRevenueCanPrefetchFinalMaterialization(request ExecutionRequest) bool {
	return len(request.Having) == 0 && len(request.OrderBy) == 0
}

func legacyDirectRelationshipQ3OrderRevenueOrderFields(plan legacyDirectRelationshipQ3OrderRevenuePlan) []qsbridge.QuantaProjectionField {
	orderFields := make([]qsbridge.QuantaProjectionField, 0, len(plan.finalFields))
	for _, field := range plan.finalFields {
		table := field.Index
		if table == "" {
			continue
		}
		roleKey := legacyDirectRelationshipProjectionFieldRoleKey(field, table)
		if strings.EqualFold(table, "orders") && strings.EqualFold(roleKey, plan.orderRole) {
			orderFields = append(orderFields, field)
		}
	}
	return orderFields
}

func legacyDirectRelationshipQ3OrderRevenueSortedDistinctRows(rows []qsbridge.QuantaRownum) []qsbridge.QuantaRownum {
	if len(rows) == 0 {
		return nil
	}
	seen := make(map[qsbridge.QuantaRownum]struct{}, len(rows))
	for _, row := range rows {
		seen[row] = struct{}{}
	}
	distinct := make([]qsbridge.QuantaRownum, 0, len(seen))
	for row := range seen {
		distinct = append(distinct, row)
	}
	sort.Slice(distinct, func(i, j int) bool { return distinct[i] < distinct[j] })
	return distinct
}

func legacyDirectRelationshipQ3OrderRevenueSkipDefaultSort(request ExecutionRequest) bool {
	return len(request.OrderBy) == 0
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3OrderRevenueDirectAggregateRows(ctx context.Context, request ExecutionRequest, plan legacyDirectRelationshipQ3OrderRevenuePlan, groupExpressions []directBitmapGroupExpression, groups []legacyDirectRelationshipQ3OrderRevenueGroup, prefetchedFinalMaterialization *legacyDirectRelationshipQ3OrderRevenueMaterializationFuture) ([]directBitmapGroupedAggregateRow, []ExecutionProbe, time.Duration, time.Duration, qsbridge.DiagnosticSet, error, bool) {
	if len(request.SQLAggregates) != 1 || len(groupExpressions) != len(plan.finalFields) {
		return nil, nil, 0, 0, nil, nil, false
	}
	fields := make([]legacyDirectRelationshipQ3OrderRevenueDirectFinalField, 0, len(plan.finalFields))
	orderFields := make([]qsbridge.QuantaProjectionField, 0, len(plan.finalFields))
	for _, field := range plan.finalFields {
		table := field.Index
		if table == "" {
			return nil, nil, 0, 0, nil, nil, false
		}
		roleKey := legacyDirectRelationshipProjectionFieldRoleKey(field, table)
		directField := legacyDirectRelationshipQ3OrderRevenueDirectFinalField{
			field:   field,
			roleKey: roleKey,
			table:   table,
		}
		switch {
		case legacyDirectRelationshipQ3OrderRevenueSyntheticOrderKeyField(field, plan):
			directField.source = "synthetic_relationship_endpoint"
			directField.sourceRole = plan.orderRole
		case strings.EqualFold(table, "orders") && strings.EqualFold(roleKey, plan.orderRole):
			directField.source = "direct_q3_group_rows"
			orderFields = append(orderFields, field)
		default:
			return nil, nil, 0, 0, nil, nil, false
		}
		fields = append(fields, directField)
	}
	if len(orderFields) == 0 {
		return nil, nil, 0, 0, nil, nil, false
	}

	orderRows := make([]qsbridge.QuantaRownum, 0, len(groups))
	for _, group := range groups {
		orderRows = append(orderRows, group.orderRow)
	}

	awaitStart := time.Now()
	materialized, prefetched := prefetchedFinalMaterialization.await()
	prefetchWaitElapsed := time.Since(awaitStart)
	if !prefetched {
		materialization := e.projectionMaterializationKernel()
		materializationStart := time.Now()
		orderRowSet, materializationProbes, diagnostics, err := e.legacyDirectRelationshipQ3OrderRevenueMaterializedOrderRows(ctx, request, materialization, orderRows, orderFields)
		materialized = legacyDirectRelationshipQ3OrderRevenueMaterializationResult{
			rowSet:      orderRowSet,
			probes:      materializationProbes,
			elapsed:     time.Since(materializationStart),
			diagnostics: diagnostics,
			err:         err,
		}
	}
	orderRowSet := materialized.rowSet
	materializationProbes := materialized.probes
	materializationElapsed := materialized.elapsed
	if prefetched {
		materializationElapsed = prefetchWaitElapsed
	}
	diagnostics := materialized.diagnostics
	err := materialized.err
	if err != nil || diagnostics.BlocksNative() {
		return nil, materializationProbes, materializationElapsed, 0, diagnostics, err, true
	}

	rowBuildStart := time.Now()
	probes := []ExecutionProbe{
		legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_mode", "direct_q3_group_rows"),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_prefetched", strconv.FormatBool(materialized.prefetched)),
	}
	if materialized.prefetched {
		probes = append(probes,
			legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_prefetch_elapsed", materialized.elapsed.String()),
			legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_final_materialization_wait_elapsed", prefetchWaitElapsed.String()),
		)
	}
	probes = append(probes, materializationProbes...)
	alignedVectors := legacyDirectRelationshipQ3OrderRevenueRownumsAligned(orderRowSet.Rownums, orderRows)
	orderVectorValues := make(map[string][]qsbridge.ResultCell, len(orderRowSet.ProjectionVectors))
	orderVectorMaps := make(map[string]map[qsbridge.QuantaRownum]qsbridge.ResultCell, len(orderRowSet.ProjectionVectors))
	for i, vector := range orderRowSet.ProjectionVectors {
		field := vector.Field
		if field.Field == "" && field.PhysicalName == "" && i < len(orderFields) {
			field = orderFields[i]
		}
		key := legacyDirectRelationshipQ3OrderRevenueDirectFieldKey(field)
		if key == "" {
			continue
		}
		if len(vector.Values) != len(orderRowSet.Rownums) {
			return nil, probes, materializationElapsed, time.Since(rowBuildStart), legacyDirectRelationshipDiagnostic(fmt.Sprintf("Q3 order-revenue final direct materialization field %s.%s returned %d values for %d rownums", field.Index, field.Field, len(vector.Values), len(orderRowSet.Rownums))), nil, true
		}
		if alignedVectors {
			orderVectorValues[key] = vector.Values
			continue
		}
		valuesByRow := make(map[qsbridge.QuantaRownum]qsbridge.ResultCell, len(orderRowSet.Rownums))
		for rowIndex, rownum := range orderRowSet.Rownums {
			valuesByRow[rownum] = vector.Values[rowIndex]
		}
		orderVectorMaps[key] = valuesByRow
	}
	fieldByKey := make(map[string]*legacyDirectRelationshipQ3OrderRevenueDirectFinalField, len(fields))
	fetchCharged := false
	for i := range fields {
		state := &fields[i]
		attachStart := time.Now()
		switch state.source {
		case "synthetic_relationship_endpoint":
			state.cells = nil
		default:
			key := legacyDirectRelationshipQ3OrderRevenueDirectFieldKey(state.field)
			state.values = orderVectorValues[key]
			state.valueByRow = orderVectorMaps[key]
			if len(state.values) == 0 && state.valueByRow == nil {
				return nil, probes, materializationElapsed, time.Since(rowBuildStart), legacyDirectRelationshipDiagnostic(fmt.Sprintf("Q3 order-revenue final direct materialization missing values for %s.%s", state.field.Index, state.field.Field)), nil, true
			}
			if !fetchCharged {
				state.fetchElapsed = materializationElapsed
				fetchCharged = true
			}
		}
		state.attachElapsed = time.Since(attachStart)
		fieldByKey[legacyDirectRelationshipQ3OrderRevenueDirectFieldKey(state.field)] = state
		probePrefix := legacyDirectRelationshipMaterializationFieldProbePrefix("graph_grouped_aggregate_final_materialization_", i+1, state.field)
		probes = append(probes,
			legacyDirectRelationshipProbe(probePrefix+"role", state.roleKey),
			legacyDirectRelationshipProbe(probePrefix+"table", state.table),
			legacyDirectRelationshipProbe(probePrefix+"field", state.field.Field),
			legacyDirectRelationshipProbe(probePrefix+"rows", strconv.Itoa(len(groups))),
			legacyDirectRelationshipProbe(probePrefix+"source", state.source),
			legacyDirectRelationshipProbe(probePrefix+"vector_aligned", strconv.FormatBool(alignedVectors)),
			legacyDirectRelationshipProbe(probePrefix+"fetch_elapsed", state.fetchElapsed.String()),
			legacyDirectRelationshipProbe(probePrefix+"attach_elapsed", state.attachElapsed.String()),
		)
		if state.sourceRole != "" {
			probes = append(probes, legacyDirectRelationshipProbe(probePrefix+"source_role", state.sourceRole))
		}
	}
	probes = append(probes, legacyDirectRelationshipProbe("graph_grouped_aggregate_final_materialization_vector_aligned", strconv.FormatBool(alignedVectors)))

	buildStableKey := len(request.OrderBy) > 0
	rows := make([]directBitmapGroupedAggregateRow, 0, len(groups))
	for rowIndex, group := range groups {
		cells := make([]qsbridge.ResultCell, 0, len(groupExpressions))
		for _, groupExpression := range groupExpressions {
			ref, ok := directBitmapExprField(groupExpression.Expr)
			if !ok {
				return nil, nil, 0, 0, nil, nil, false
			}
			state := fieldByKey[legacyDirectRelationshipQ3OrderRevenueDirectFieldRefKey(ref)]
			if state == nil {
				return nil, nil, 0, 0, nil, nil, false
			}
			cell, ok := legacyDirectRelationshipQ3OrderRevenueDirectFieldCell(state, group, rowIndex)
			if !ok {
				return nil, probes, materializationElapsed, time.Since(rowBuildStart), legacyDirectRelationshipDiagnostic(fmt.Sprintf("Q3 order-revenue final direct materialization missing value for %s.%s row %d", state.field.Index, state.field.Field, group.orderRow)), nil, true
			}
			cells = append(cells, cell)
		}
		row := directBitmapGroupedAggregateRow{
			Groups: cells,
			Aggs: []qsbridge.ResultCell{{
				Kind:  qsbridge.ValueFloat,
				Value: group.revenue,
			}},
		}
		if buildStableKey {
			row.Key = directBitmapGroupCellsKey(cells)
		}
		rows = append(rows, row)
	}
	return rows, probes, materializationElapsed, time.Since(rowBuildStart), nil, nil, true
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipQ3OrderRevenueMaterializedOrderRows(ctx context.Context, request ExecutionRequest, materialization ProjectionMaterializationKernel, orderRows []qsbridge.QuantaRownum, orderFields []qsbridge.QuantaProjectionField) (qsbridge.QuantaProjectedRowSet, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	if materialization == nil {
		return qsbridge.QuantaProjectedRowSet{}, nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship-vector materialization kernel is not configured"),
		}, nil
	}
	materializationRequest := e.legacyDirectRelationshipTimeMaterialization(request, "orders")
	materializationRequest.Index = "orders"
	materializationRequest.Rownums = append([]qsbridge.QuantaRownum(nil), orderRows...)
	materializationRequest.ProjectionFields = orderFields
	if materializationRequest.DependencyID == "" {
		materializationRequest.DependencyID = "relationship_materialization.orders"
	}
	kernelRequest := qsbridge.ProjectionMaterializationKernelRequest{
		ID:          "relationship_projection_materialization",
		ProbePrefix: "relationship_projection_materialization_",
		Requests:    []qsbridge.QuantaMaterializationRequest{materializationRequest},
	}
	result, err := ExecuteProjectionMaterializationKernel(ctx, materialization, kernelRequest)
	diagnostics := append(qsbridge.DiagnosticSet(nil), result.Diagnostics...)
	if err != nil || diagnostics.BlocksNative() {
		return qsbridge.QuantaProjectedRowSet{}, result.Probes, diagnostics, err
	}
	if len(result.Results) == 0 {
		return qsbridge.QuantaProjectedRowSet{}, result.Probes, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship-vector materialization kernel returned no results"),
		}, nil
	}
	rowSet := result.Results[0].RowSet
	diagnostics = append(diagnostics, result.Results[0].Diagnostics...)
	if diagnostics.BlocksNative() {
		return qsbridge.QuantaProjectedRowSet{}, result.Probes, diagnostics, nil
	}
	return rowSet, result.Probes, diagnostics, nil
}

func legacyDirectRelationshipQ3OrderRevenueRownumsAligned(actual []qsbridge.QuantaRownum, expected []qsbridge.QuantaRownum) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range actual {
		if actual[i] != expected[i] {
			return false
		}
	}
	return true
}

func legacyDirectRelationshipQ3OrderRevenueDirectFieldCell(state *legacyDirectRelationshipQ3OrderRevenueDirectFinalField, group legacyDirectRelationshipQ3OrderRevenueGroup, rowIndex int) (qsbridge.ResultCell, bool) {
	if state.source == "synthetic_relationship_endpoint" {
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(group.orderRow)}, true
	}
	if len(state.values) > 0 {
		if rowIndex >= len(state.values) {
			return qsbridge.ResultCell{}, false
		}
		return state.values[rowIndex], true
	}
	if state.valueByRow != nil {
		cell, ok := state.valueByRow[group.orderRow]
		return cell, ok
	}
	return qsbridge.ResultCell{}, false
}

func legacyDirectRelationshipQ3OrderRevenueSyntheticOrderKeyField(field qsbridge.QuantaProjectionField, plan legacyDirectRelationshipQ3OrderRevenuePlan) bool {
	return strings.EqualFold(field.Index, "lineitem") &&
		strings.EqualFold(legacyDirectRelationshipProjectionFieldRoleKey(field, field.Index), plan.lineitemRole) &&
		strings.EqualFold(legacyDirectRelationshipQ3ProjectionPhysicalField(field), "l_orderkey")
}

func legacyDirectRelationshipQ3OrderRevenueDirectFieldKey(field qsbridge.QuantaProjectionField) string {
	role := legacyDirectRelationshipProjectionFieldRoleKey(field, field.Index)
	return strings.ToLower(field.Index) + "\x00" + role + "\x00" + strings.ToLower(legacyDirectRelationshipQ3ProjectionPhysicalField(field))
}

func legacyDirectRelationshipQ3OrderRevenueDirectFieldRefKey(ref qsbridge.FieldRef) string {
	role := strings.ToLower(materializationFieldRole(ref.Table.Table, ref))
	return strings.ToLower(ref.Table.Table) + "\x00" + role + "\x00" + strings.ToLower(directBitmapFieldPhysicalName(ref))
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

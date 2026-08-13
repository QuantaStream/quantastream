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

type legacyDirectRelationshipDiscountedRevenuePlan struct {
	lineitemRole  string
	groupRole     string
	priceField    qsbridge.FieldRef
	discountField qsbridge.FieldRef
	groupFields   []qsbridge.QuantaProjectionField
	valueField    string
	scale         int
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipDiscountedRevenueResult(ctx context.Context, request ExecutionRequest, sink string, rownums []qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge, fields []qsbridge.QuantaProjectionField, alignedRows map[string][]qsbridge.QuantaRownum, tupleRows RelationshipTupleRowSet, graphReductionElapsed time.Duration, alignmentElapsed time.Duration, tupleExpansionElapsed time.Duration, sameRowElapsed time.Duration, membershipElapsed time.Duration, residualElapsed time.Duration, result ExecutionResult) (ExecutionResult, bool, error) {
	if e.RelationshipAggregateReader == nil {
		return result, false, nil
	}
	plan, ok := e.legacyDirectRelationshipDiscountedRevenuePlanFor(request, sink, edges, fields)
	if !ok {
		return result, false, nil
	}
	lineRows, groupRows, ok := legacyDirectRelationshipDiscountedRevenueAlignedRows(plan, alignedRows, tupleRows)
	if !ok || len(lineRows) == 0 || len(lineRows) != len(groupRows) {
		return result, false, nil
	}
	var residualMaterializationElapsed time.Duration
	var residualFilterElapsed time.Duration
	var residualProbes []ExecutionProbe
	residualRowsBefore := len(lineRows)
	residualRowsAfter := len(lineRows)
	var err error
	lineRows, groupRows, tupleRows, residualProbes, residualMaterializationElapsed, residualFilterElapsed, ok, err = e.legacyDirectRelationshipDiscountedRevenueResidualPrefilter(ctx, request, sink, plan, fields, alignedRows, tupleRows, lineRows, groupRows, edges)
	result.Probes = append(result.Probes, residualProbes...)
	if err != nil {
		return result, true, err
	}
	if !ok {
		return result, false, nil
	}
	residualRowsAfter = len(lineRows)
	if len(lineRows) == 0 || len(lineRows) != len(groupRows) {
		return result, false, nil
	}

	preAggregateStart := time.Now()
	aggregate, diagnostics, ok, err := e.RelationshipAggregateReader.ReadRelationshipVectorAggregate(ctx, LegacyDirectRelationshipVectorAggregateRequest{
		VectorIndex: "lineitem",
		VectorField: "l_orderkey",
		ValueIndex:  "lineitem",
		ValueField:  plan.valueField,
		ChildRows:   lineRows,
		ParentRows:  groupRows,
	})
	preAggregateElapsed := time.Since(preAggregateStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, true, err
	}
	if !ok || len(aggregate.Groups) == 0 {
		return result, false, nil
	}

	finalLineRows, groupRowsForMaterialization := legacyDirectRelationshipDiscountedRevenueFinalRows(aggregate.Groups)
	groupMaterializationStart := time.Now()
	groupRowSet, groupMaterializationProbes, diagnostics, err := e.legacyDirectRelationshipGraphMaterializedRowSet(ctx, request, sink, finalLineRows, plan.groupFields, map[string][]qsbridge.QuantaRownum{
		plan.lineitemRole: finalLineRows,
		plan.groupRole:    groupRowsForMaterialization,
	}, edges, "graph_grouped_aggregate_discounted_revenue_group_materialization_")
	groupMaterializationElapsed := time.Since(groupMaterializationStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, true, err
	}

	groupExpressions, diagnostics := directBitmapGroupExpressions(request.GroupBy)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true, nil
	}
	rowBuildStart := time.Now()
	aggregateRows, diagnostics := legacyDirectRelationshipDiscountedRevenueAggregateRows(request, groupRowSet, groupRowsForMaterialization, groupExpressions, aggregate.Groups, plan.scale)
	rowBuildElapsed := time.Since(rowBuildStart)
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
		legacyDirectRelationshipProbe("graph_grouped_aggregate_late_materialization", "discounted_revenue_storage_aggregate"),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_aligned_roles", legacyDirectRelationshipAlignedRoleDebug(alignedRows)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_graph_reduction_elapsed", graphReductionElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_alignment_elapsed", alignmentElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_tuple_expansion_elapsed", tupleExpansionElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_same_row_elapsed", sameRowElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_membership_elapsed", membershipElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_residual_filter_elapsed", (residualElapsed+residualFilterElapsed).String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_discounted_revenue_residual_materialization_elapsed", residualMaterializationElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_discounted_revenue_residual_filter_elapsed", residualFilterElapsed.String()),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_discounted_revenue_residual_rows_before", strconv.Itoa(residualRowsBefore)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_discounted_revenue_residual_rows_after", strconv.Itoa(residualRowsAfter)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_discounted_revenue_residual_rows_removed", strconv.Itoa(residualRowsBefore-residualRowsAfter)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_discounted_revenue_rows", strconv.Itoa(len(lineRows))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_discounted_revenue_groups", strconv.Itoa(len(aggregate.Groups))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_discounted_revenue_group_role", plan.groupRole),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_mode", "storage_discounted_revenue_sum"),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_mode", aggregate.Mode),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_rows", strconv.FormatUint(aggregate.Rows, 10)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_values", strconv.FormatUint(aggregate.Values, 10)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_source_values", strconv.Itoa(aggregate.SourceValues)),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_preagg_storage_target_rows", strconv.FormatUint(aggregate.TargetRows, 10)),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_elapsed", preAggregateElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_preagg_storage_lookup_elapsed", aggregate.LookupElapsed.String()),
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
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_discounted_revenue_group_materialization_elapsed", groupMaterializationElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_group_row_build_elapsed", rowBuildElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_having_elapsed", havingElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_final_sort_elapsed", sortElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_output_elapsed", outputElapsed.String()),
		legacyDirectRelationshipProbe("phase_graph_grouped_aggregate_final_limit_elapsed", limitElapsed.String()),
	)
	result.Probes = append(result.Probes, groupMaterializationProbes...)
	result.Probes = append(result.Probes, directBitmapGroupedAggregateProbes(directBitmapGroupedAggregateProbe{
		CandidateRows:         len(lineRows),
		GroupExpressionCount:  len(groupExpressions),
		GroupExpressionShapes: directBitmapGroupExpressionShapes(groupExpressions),
		GroupExpressionFields: directBitmapGroupExpressionFields(groupExpressions),
		GroupCount:            len(aggregateRows),
		PostHavingGroups:      postHavingGroups,
		SortInputGroups:       postHavingGroups,
		Limit:                 request.Result.Limit,
		FinalRows:             outputRowSet.CandidateCount(),
		TopNCandidate:         executionGroupedAggregateTopNCandidate(request),
		OrderStrategy:         orderStrategy,
		GroupStrategy:         "relationship_discounted_revenue_preaggregate",
		GroupValuesTime:       groupMaterializationElapsed,
		GroupTime:             rowBuildElapsed,
		HavingTime:            havingElapsed,
		OrderTime:             sortElapsed,
		OutputTime:            outputElapsed,
		LimitTime:             limitElapsed,
	})...)
	result.Probes = append(result.Probes, legacyDirectRelationshipNodeInteractionSummaryProbes(result.Probes)...)
	return result, true, nil
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipDiscountedRevenuePlanFor(request ExecutionRequest, sink string, edges []legacyDirectRelationshipEdge, fields []qsbridge.QuantaProjectionField) (legacyDirectRelationshipDiscountedRevenuePlan, bool) {
	if !strings.EqualFold(sink, "lineitem") || len(request.SQLAggregates) != 1 || len(request.GroupBy) == 0 {
		return legacyDirectRelationshipDiscountedRevenuePlan{}, false
	}
	if !request.NativePredicates.Empty() {
		return legacyDirectRelationshipDiscountedRevenuePlan{}, false
	}
	aggregate := request.SQLAggregates[0]
	if !strings.EqualFold(aggregate.Function, "sum") || aggregate.Filter != nil || aggregate.Mode == qsbridge.AggregateDistinct {
		return legacyDirectRelationshipDiscountedRevenuePlan{}, false
	}
	priceRef, discountRef, ok := legacyDirectRelationshipDiscountedRevenueInput(aggregate.Input)
	if !ok || !legacyDirectRelationshipDiscountedRevenueLineitemField(priceRef, "l_extendedprice") || !legacyDirectRelationshipDiscountedRevenueLineitemField(discountRef, "l_discount") {
		return legacyDirectRelationshipDiscountedRevenuePlan{}, false
	}
	lineitemRole, ok := legacyDirectRelationshipDiscountedRevenueLineitemRole(edges)
	if !ok {
		return legacyDirectRelationshipDiscountedRevenuePlan{}, false
	}
	groupRefs := make([]qsbridge.FieldRef, 0, len(request.GroupBy))
	var groupRole string
	for _, expr := range request.GroupBy {
		ref, ok := directBitmapExprField(expr)
		if !ok {
			return legacyDirectRelationshipDiscountedRevenuePlan{}, false
		}
		role := strings.ToLower(materializationFieldRole(ref.Table.Table, ref))
		if groupRole != "" && groupRole != role {
			return legacyDirectRelationshipDiscountedRevenuePlan{}, false
		}
		groupRole = role
		groupRefs = append(groupRefs, ref)
	}
	if groupRole == "" || groupRole == lineitemRole {
		return legacyDirectRelationshipDiscountedRevenuePlan{}, false
	}
	groupFields := make([]qsbridge.QuantaProjectionField, 0, len(groupRefs))
	for _, ref := range groupRefs {
		field := legacyDirectRelationshipProjectionFieldForFieldRef(fields, ref)
		if field.Field == "" {
			field = legacyDirectRelationshipProjectionFieldFromRef(ref, true)
		}
		field.Visible = true
		groupFields = append(groupFields, field)
	}
	priceField := directBitmapFieldPhysicalName(priceRef)
	discountField := directBitmapFieldPhysicalName(discountRef)
	return legacyDirectRelationshipDiscountedRevenuePlan{
		lineitemRole:  lineitemRole,
		groupRole:     groupRole,
		priceField:    priceRef,
		discountField: discountRef,
		groupFields:   groupFields,
		valueField:    qsbridge.RelationshipAlignedDiscountedRevenueField(priceField, discountField),
		scale:         e.legacyDirectRelationshipQ3FieldScale("lineitem", legacyDirectRelationshipProjectionFieldFromRef(priceRef, false)),
	}, true
}

func legacyDirectRelationshipDiscountedRevenueInput(expr qsbridge.Expr) (qsbridge.FieldRef, qsbridge.FieldRef, bool) {
	binary, ok := directBitmapBinaryExpr(expr)
	if !ok || binary.Op != qsbridge.BinaryOpMultiply {
		return qsbridge.FieldRef{}, qsbridge.FieldRef{}, false
	}
	if priceRef, ok := directBitmapExprField(binary.Left); ok {
		if discountRef, ok := legacyDirectRelationshipDiscountedRevenueOneMinusDiscount(binary.Right); ok {
			return priceRef, discountRef, true
		}
	}
	if priceRef, ok := directBitmapExprField(binary.Right); ok {
		if discountRef, ok := legacyDirectRelationshipDiscountedRevenueOneMinusDiscount(binary.Left); ok {
			return priceRef, discountRef, true
		}
	}
	return qsbridge.FieldRef{}, qsbridge.FieldRef{}, false
}

func legacyDirectRelationshipDiscountedRevenueOneMinusDiscount(expr qsbridge.Expr) (qsbridge.FieldRef, bool) {
	binary, ok := directBitmapBinaryExpr(expr)
	if !ok || binary.Op != qsbridge.BinaryOpSubtract {
		return qsbridge.FieldRef{}, false
	}
	literal, ok := directBitmapLiteralExpr(binary.Left)
	if !ok || !legacyDirectRelationshipDiscountedRevenueLiteralOne(literal) {
		return qsbridge.FieldRef{}, false
	}
	return directBitmapExprField(binary.Right)
}

func legacyDirectRelationshipDiscountedRevenueLiteralOne(literal qsbridge.LiteralExpr) bool {
	switch value := literal.Value.(type) {
	case int:
		return value == 1
	case int64:
		return value == 1
	case uint64:
		return value == 1
	case float64:
		return value == 1
	default:
		number, ok := directBitmapNumericCellValue(qsbridge.ResultCell{Kind: literal.Kind, Value: literal.Value})
		return ok && number == 1
	}
}

func legacyDirectRelationshipDiscountedRevenueLineitemField(ref qsbridge.FieldRef, field string) bool {
	return strings.EqualFold(ref.Table.Table, "lineitem") && strings.EqualFold(directBitmapFieldPhysicalName(ref), field)
}

func legacyDirectRelationshipDiscountedRevenueLineitemRole(edges []legacyDirectRelationshipEdge) (string, bool) {
	var role string
	for _, edge := range edges {
		if !strings.EqualFold(edge.childTable, "lineitem") {
			continue
		}
		next := edge.childKey()
		if role != "" && role != next {
			return "", false
		}
		role = next
	}
	return role, role != ""
}

func legacyDirectRelationshipDiscountedRevenueAlignedRows(plan legacyDirectRelationshipDiscountedRevenuePlan, alignedRows map[string][]qsbridge.QuantaRownum, tupleRows RelationshipTupleRowSet) ([]qsbridge.QuantaRownum, []qsbridge.QuantaRownum, bool) {
	if len(tupleRows.Rows) > 0 {
		lineRows := make([]qsbridge.QuantaRownum, 0, len(tupleRows.Rows))
		groupRows := make([]qsbridge.QuantaRownum, 0, len(tupleRows.Rows))
		for _, row := range tupleRows.Rows {
			lineRow, lineOK := row.Rownum(qsbridge.TableInstanceID(plan.lineitemRole))
			groupRow, groupOK := row.Rownum(qsbridge.TableInstanceID(plan.groupRole))
			if !lineOK || !groupOK {
				return nil, nil, false
			}
			lineRows = append(lineRows, lineRow)
			groupRows = append(groupRows, groupRow)
		}
		return lineRows, groupRows, true
	}
	lineRows := alignedRows[plan.lineitemRole]
	groupRows := alignedRows[plan.groupRole]
	return lineRows, groupRows, len(lineRows) > 0 && len(lineRows) == len(groupRows)
}

func (e LegacyDirectRelationshipVectorJoinExecutor) legacyDirectRelationshipDiscountedRevenueResidualPrefilter(ctx context.Context, request ExecutionRequest, sink string, plan legacyDirectRelationshipDiscountedRevenuePlan, fields []qsbridge.QuantaProjectionField, alignedRows map[string][]qsbridge.QuantaRownum, tupleRows RelationshipTupleRowSet, lineRows []qsbridge.QuantaRownum, groupRows []qsbridge.QuantaRownum, edges []legacyDirectRelationshipEdge) ([]qsbridge.QuantaRownum, []qsbridge.QuantaRownum, RelationshipTupleRowSet, []ExecutionProbe, time.Duration, time.Duration, bool, error) {
	residuals := directBitmapResidualScanPredicates(request)
	if len(residuals) == 0 {
		return lineRows, groupRows, tupleRows, nil, 0, 0, true, nil
	}
	if tupleRows.CandidateCount() == 0 {
		return nil, nil, RelationshipTupleRowSet{}, nil, 0, 0, false, nil
	}
	prefilterFields := legacyDirectRelationshipDiscountedRevenueResidualFields(request, fields)
	if len(prefilterFields) == 0 {
		return nil, nil, RelationshipTupleRowSet{}, nil, 0, 0, false, nil
	}
	materializationStart := time.Now()
	prefilterRowSet, probes, diagnostics, err := e.legacyDirectRelationshipGraphMaterializedRowSet(ctx, request, sink, lineRows, prefilterFields, alignedRows, edges, "graph_grouped_aggregate_discounted_revenue_residual_materialization_")
	materializationElapsed := time.Since(materializationStart)
	if err != nil || diagnostics.BlocksNative() {
		return nil, nil, RelationshipTupleRowSet{}, probes, materializationElapsed, 0, false, err
	}
	filterStart := time.Now()
	_, filteredTupleRows, diagnostics := FilterRelationshipTupleProjectedResiduals(tupleRows, request, prefilterRowSet)
	filterElapsed := time.Since(filterStart)
	if diagnostics.BlocksNative() {
		return nil, nil, RelationshipTupleRowSet{}, probes, materializationElapsed, filterElapsed, false, nil
	}
	filteredLineRows, filteredGroupRows, ok := legacyDirectRelationshipDiscountedRevenueAlignedRows(legacyDirectRelationshipDiscountedRevenuePlan{
		lineitemRole: plan.lineitemRole,
		groupRole:    plan.groupRole,
	}, nil, filteredTupleRows)
	if !ok {
		return nil, nil, RelationshipTupleRowSet{}, probes, materializationElapsed, filterElapsed, false, nil
	}
	probes = append(probes,
		legacyDirectRelationshipProbe("graph_grouped_aggregate_discounted_revenue_residual_predicates", strconv.Itoa(len(residuals))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_discounted_revenue_residual_materialization_rows", strconv.Itoa(prefilterRowSet.CandidateCount())),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_discounted_revenue_residual_materialization_fields", strconv.Itoa(len(prefilterFields))),
		legacyDirectRelationshipProbe("graph_grouped_aggregate_discounted_revenue_residual_materialization_field_list", legacyDirectRelationshipProjectionFieldsDebug(prefilterFields)),
	)
	return filteredLineRows, filteredGroupRows, filteredTupleRows, probes, materializationElapsed, filterElapsed, true, nil
}

func legacyDirectRelationshipDiscountedRevenueResidualFields(request ExecutionRequest, fields []qsbridge.QuantaProjectionField) []qsbridge.QuantaProjectionField {
	byKey := make(map[string]qsbridge.QuantaProjectionField)
	addRef := func(ref qsbridge.FieldRef, visible bool) {
		field := legacyDirectRelationshipProjectionFieldForFieldRef(fields, ref)
		if field.Field == "" {
			field = legacyDirectRelationshipProjectionFieldFromRef(ref, visible)
		}
		key := strings.ToLower(field.Index) + "." + strings.ToLower(field.Field) + "." + strings.ToLower(string(field.Role))
		if _, ok := byKey[key]; ok {
			return
		}
		byKey[key] = field
	}
	for _, expr := range request.GroupBy {
		for _, ref := range qsbridge.FieldRefs(expr) {
			addRef(ref, true)
		}
	}
	for _, predicate := range directBitmapResidualScanPredicates(request) {
		for _, ref := range qsbridge.FieldRefs(predicate.Expr) {
			addRef(ref, false)
		}
	}
	out := make([]qsbridge.QuantaProjectionField, 0, len(byKey))
	for _, field := range byKey {
		out = append(out, field)
	}
	sort.Slice(out, func(i, j int) bool {
		left := strings.ToLower(out[i].Index) + "." + strings.ToLower(out[i].Field) + "." + strings.ToLower(string(out[i].Role))
		right := strings.ToLower(out[j].Index) + "." + strings.ToLower(out[j].Field) + "." + strings.ToLower(string(out[j].Role))
		return left < right
	})
	return out
}

func legacyDirectRelationshipDiscountedRevenueFinalRows(groups []LegacyDirectRelationshipVectorAggregateGroup) ([]qsbridge.QuantaRownum, []qsbridge.QuantaRownum) {
	lineRows := make([]qsbridge.QuantaRownum, 0, len(groups))
	groupRows := make([]qsbridge.QuantaRownum, 0, len(groups))
	seen := make(map[qsbridge.QuantaRownum]struct{}, len(groups))
	for _, group := range groups {
		row := group.ParentRow
		if _, ok := seen[row]; ok {
			continue
		}
		seen[row] = struct{}{}
		lineRows = append(lineRows, group.RepresentativeChildRow)
		groupRows = append(groupRows, row)
	}
	return lineRows, groupRows
}

func legacyDirectRelationshipDiscountedRevenueAggregateRows(request ExecutionRequest, groupRowSet qsbridge.QuantaProjectedRowSet, parentRows []qsbridge.QuantaRownum, groupExpressions []directBitmapGroupExpression, groups []LegacyDirectRelationshipVectorAggregateGroup, scale int) ([]directBitmapGroupedAggregateRow, qsbridge.DiagnosticSet) {
	if len(request.SQLAggregates) != 1 {
		return nil, directBitmapAggregateDiagnostics("discounted revenue fast path requires exactly one aggregate")
	}
	groupValues, diagnostics := directBitmapGroupValueColumns(groupRowSet, groupExpressions)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	if len(parentRows) != groupRowSet.CandidateCount() {
		return nil, directBitmapAggregateDiagnostics(fmt.Sprintf("discounted revenue fast path parent rows %d do not match materialized rows %d", len(parentRows), groupRowSet.CandidateCount()))
	}
	positionByParent := make(map[qsbridge.QuantaRownum]int, len(parentRows))
	for i, rownum := range parentRows {
		positionByParent[rownum] = i
	}
	rows := make([]directBitmapGroupedAggregateRow, 0, len(groups))
	for _, group := range groups {
		if group.Sum == nil {
			continue
		}
		position, ok := positionByParent[group.ParentRow]
		if !ok {
			return nil, directBitmapAggregateDiagnostics(fmt.Sprintf("discounted revenue fast path missing group row %d", group.ParentRow))
		}
		key, diagnostics := directBitmapGroupValuesKeyAt(groupValues, position)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		cells, diagnostics := directBitmapGroupValueCellsAt(groupValues, position)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		rows = append(rows, directBitmapGroupedAggregateRow{
			Key:    key,
			Groups: cells,
			Aggs: []qsbridge.ResultCell{{
				Kind:  qsbridge.ValueFloat,
				Value: float64(group.Sum.Int64()) / math.Pow10(scale),
			}},
		})
	}
	return rows, nil
}

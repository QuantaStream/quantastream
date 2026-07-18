package qsruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func directBitmapSeedMembershipOnlyRequest(request ExecutionRequest) ExecutionRequest {
	if len(request.Query.Fragments) > 0 || len(request.Memberships) == 0 || len(request.Joins) > 0 {
		return request
	}
	root, ok := request.RootIndex()
	if !ok {
		return request
	}
	membership := request.Memberships[0]
	if !strings.EqualFold(membership.Left.Table.Table, root) {
		return request
	}
	field := directBitmapFieldPhysicalName(membership.Left)
	if field == "" {
		field = membership.Left.Name
	}
	request.Query.Fragments = append(request.Query.Fragments, qsbridge.QuantaQueryFragment{
		Index:     root,
		Field:     field,
		Operation: qsbridge.QuantaOperationIntersect,
		NullCheck: true,
		Negate:    true,
	})
	return request
}

// DirectBitmapRuntime executes direct requests through borrowed bitmap-query sessions.
type DirectBitmapRuntime struct {
	Sessions           DirectSessionProvider
	Adapter            BitmapQueryResultAdapter
	Materializer       ProjectionMaterializer
	Materialization    ProjectionMaterializationKernel
	SameRowComparison  SameRowComparisonKernel
	RelationshipJoins  RelationshipVectorJoinExecutor
	RelationshipReader RelationshipVectorReader
	FilterAdapter      DirectBitmapFilterAdapter
}

// ExecuteDirect borrows a direct session, runs the bitmap query, and adapts the result.
func (r DirectBitmapRuntime) ExecuteDirect(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	var diagnostics qsbridge.DiagnosticSet
	var err error
	request, diagnostics, err = r.filterAdapter().AdaptFilterExpression(ctx, request)
	if err != nil || diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, err
	}
	joinPlan := PlanRelationshipJoins(request.Joins)
	if joinPlan.NeedsRelationshipVectorExecution() {
		rootIndex, _ := request.RootIndex()
		result, err := r.relationshipVectorJoinExecutor().ExecuteRelationshipVectorJoin(ctx, request, joinPlan.VectorRequest(rootIndex))
		result.Probes = append(request.Probes, result.Probes...)
		return result, err
	}
	request = directBitmapSeedMembershipOnlyRequest(request)
	var session DirectSessionHandle
	if !request.HasCandidateSet || request.Mutation.Kind != qsbridge.MutationUnknown {
		if r.Sessions == nil {
			return ExecutionResult{
				Diagnostics: qsbridge.DiagnosticSet{
					qsbridge.ErrorDiagnostic(
						qsbridge.DiagnosticInternalInvariant,
						qsbridge.PhaseExecute,
						"direct bitmap runtime has no session provider",
					),
				},
			}, nil
		}
		session, diagnostics, err = r.Sessions.BorrowDirectSession(ctx, request)
		if err != nil || diagnostics.BlocksNative() {
			return ExecutionResult{Diagnostics: diagnostics}, err
		}
		if session == nil {
			return ExecutionResult{
				Diagnostics: qsbridge.DiagnosticSet{
					qsbridge.ErrorDiagnostic(
						qsbridge.DiagnosticInternalInvariant,
						qsbridge.PhaseExecute,
						"direct session provider returned nil session",
					),
				},
			}, nil
		}
	}
	if request.Mutation.Kind != qsbridge.MutationUnknown {
		statement, mutationDiagnostics, err := session.ExecuteMutation(ctx, request)
		releaseDiagnostics := session.Release(ctx)
		result := ExecutionResult{Statement: statement}
		result.Diagnostics = append(result.Diagnostics, mutationDiagnostics...)
		result.Diagnostics = append(result.Diagnostics, releaseDiagnostics...)
		return result, err
	}

	queryStart := time.Now()
	bitmapResult, queryDiagnostics, err := directBitmapCandidateSetResult(request)
	queryElapsed := time.Since(queryStart)
	releaseDiagnostics := qsbridge.DiagnosticSet(nil)
	if !request.HasCandidateSet {
		bitmapResult, queryDiagnostics, err = session.QueryBitmap(ctx, request)
		queryElapsed = time.Since(queryStart)
		releaseDiagnostics = session.Release(ctx)
	}
	bitmapResult, membershipDiagnostics, membershipErr := r.directBitmapApplyMemberships(ctx, request, bitmapResult)
	var sameRowProbes []ExecutionProbe
	var sameRowDiagnostics qsbridge.DiagnosticSet
	var sameRowErr error
	var sameRowApplied bool
	bitmapResult, sameRowProbes, sameRowDiagnostics, sameRowErr, sameRowApplied = r.directBitmapApplySameRowResiduals(ctx, request, bitmapResult)
	if sameRowApplied {
		request = directBitmapWithoutSameRowResidualPredicates(request)
	}
	result := r.Adapter.ToExecutionResult(bitmapResult)
	result.Probes = append(result.Probes, request.Probes...)
	result.Probes = append(result.Probes, sameRowProbes...)
	result.Probes = append(result.Probes,
		ExecutionProbe{
			Section: "direct_bitmap",
			Name:    "phase_bitmap_query_elapsed",
			Value:   queryElapsed.String(),
		},
		ExecutionProbe{
			Section: "direct_bitmap",
			Name:    "fragment_count",
			Value:   strconv.Itoa(len(request.Query.Fragments)),
			Detail:  directBitmapFragmentProbeDetail(request),
		},
		ExecutionProbe{
			Section: "direct_bitmap",
			Name:    "bitmap_count",
			Value:   strconv.FormatUint(bitmapResult.Count, 10),
			Detail:  fmt.Sprintf("rownums=%d", len(bitmapResult.Rownums)),
		},
	)
	result.Diagnostics = append(result.Diagnostics, queryDiagnostics...)
	result.Diagnostics = append(result.Diagnostics, releaseDiagnostics...)
	result.Diagnostics = append(result.Diagnostics, membershipDiagnostics...)
	result.Diagnostics = append(result.Diagnostics, sameRowDiagnostics...)
	if membershipErr != nil {
		return result, membershipErr
	}
	if sameRowErr != nil {
		return result, sameRowErr
	}
	if len(request.SQLAggregates) > 0 {
		return r.directBitmapAggregateResult(ctx, request, bitmapResult, result), err
	}
	if err != nil || result.Diagnostics.BlocksNative() || request.ProjectionCount() == 0 {
		return result, err
	}
	if r.projectionMaterializationKernel() == nil {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticInternalInvariant,
			qsbridge.PhaseExecute,
			"direct bitmap runtime has no projection materialization kernel",
		))
		return result, err
	}
	materializationRequest, diagnostics := MaterializationRequestFromExecution(request, bitmapResult)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, err
	}
	rowSet, materializationDiagnostics, materializationProbes, materializationErr := directBitmapMaterializeWithKernel(ctx, r.projectionMaterializationKernel(), materializationRequest)
	result.Probes = append(result.Probes, materializationProbes...)
	result.RowSet = rowSet
	result.Diagnostics = append(result.Diagnostics, materializationDiagnostics...)
	if materializationErr != nil {
		return result, materializationErr
	}
	rowSet, nativePredicateProbes, nativePredicateDiagnostics := filterRowSetByNativePredicates(request, rowSet)
	result.Probes = append(result.Probes, nativePredicateProbes...)
	result.Diagnostics = append(result.Diagnostics, nativePredicateDiagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, err
	}
	rowSet, residualDiagnostics := directBitmapFilterResidualScanPredicates(request, rowSet)
	result.Diagnostics = append(result.Diagnostics, residualDiagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, err
	}
	rowSet, orderDiagnostics := directBitmapOrderProjectedRows(request, rowSet)
	result.Diagnostics = append(result.Diagnostics, orderDiagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, err
	}
	rowSet, projectionDiagnostics := directBitmapEvaluateProjectionRowSet(request, rowSet)
	result.Diagnostics = append(result.Diagnostics, projectionDiagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, err
	}
	if request.Result.Distinct {
		rowSet = directBitmapDistinctProjectedRowSet(rowSet)
	}
	rowSet = directBitmapLimitProjectedRowSet(rowSet, request.Result.Offset, request.Result.Limit)
	if len(request.Projection) == 0 {
		rowSet = directBitmapOrderVisibleProjectedRowSet(rowSet, request.ProjectionOrder)
	}
	result.RowSet = rowSet
	result.Count = uint64(rowSet.CandidateCount())
	return result, err
}

func directBitmapFragmentProbeDetail(request ExecutionRequest) string {
	if len(request.Query.Fragments) == 0 {
		return ""
	}
	details := make([]string, 0, len(request.Query.Fragments))
	for _, fragment := range request.Query.Fragments {
		details = append(details, fmt.Sprintf("%s.%s op=%s bsi=%s null=%t negate=%t", fragment.Index, fragment.Field, fragment.Operation, fragment.BSIOp, fragment.NullCheck, fragment.Negate))
	}
	return strings.Join(details, "; ")
}

func directBitmapCandidateSetResult(request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	if !request.HasCandidateSet {
		return BitmapQueryResult{}, nil, nil
	}
	rownums := append([]qsbridge.QuantaRownum(nil), request.CandidateSet.Rownums...)
	return BitmapQueryResult{Success: true, Count: uint64(len(rownums)), Rownums: rownums}, nil, nil
}

func (r DirectBitmapRuntime) relationshipVectorJoinExecutor() RelationshipVectorJoinExecutor {
	if r.RelationshipJoins != nil {
		return r.RelationshipJoins
	}
	return UnsupportedRelationshipVectorJoinExecutor{}
}

func (r DirectBitmapRuntime) filterAdapter() DirectBitmapFilterAdapter {
	if r.FilterAdapter != nil {
		return r.FilterAdapter
	}
	return UnsupportedDirectBitmapFilterAdapter{}
}

func (r DirectBitmapRuntime) projectionMaterializationKernel() ProjectionMaterializationKernel {
	if r.Materialization != nil {
		return r.Materialization
	}
	if r.Materializer != nil {
		return ProjectionMaterializerKernelAdapter{Materializer: r.Materializer}
	}
	return nil
}

func (r DirectBitmapRuntime) sameRowComparisonKernel() SameRowComparisonKernel {
	if r.SameRowComparison != nil {
		return r.SameRowComparison
	}
	return UnsupportedSameRowComparisonKernel{}
}

func directBitmapRelationshipVectorJoinDiagnostics(request ExecutionRequest) qsbridge.DiagnosticSet {
	return qsbridge.RelationshipVectorJoinDiagnostics(qsbridge.PlanRelationshipJoins(request.Joins))
}

func directBitmapNeedsRelationshipVectorJoin(join qsbridge.JoinEdge) bool {
	return qsbridge.PlanRelationshipJoins([]qsbridge.JoinEdge{join}).NeedsRelationshipVectorExecution()
}

func (r DirectBitmapRuntime) directBitmapAggregateResult(ctx context.Context, request ExecutionRequest, bitmapResult BitmapQueryResult, result ExecutionResult) ExecutionResult {
	if result.Diagnostics.BlocksNative() {
		return result
	}
	if len(request.GroupBy) == 0 && directBitmapAllAggregatesUseBitmapCount(request.SQLAggregates) && !directBitmapHasResidualScanPredicates(request) && request.NativePredicates.Empty() {
		return directBitmapCountAggregateResult(request, result)
	}
	if r.projectionMaterializationKernel() == nil {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticInternalInvariant,
			qsbridge.PhaseExecute,
			"direct bitmap runtime has no aggregate materialization kernel",
		))
		return result
	}
	materializationRequest, diagnostics := MaterializationRequestFromExecution(request, bitmapResult)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	materializationStart := time.Now()
	rowSet, materializationDiagnostics, materializationProbes, materializationErr := directBitmapMaterializeWithKernel(ctx, r.projectionMaterializationKernel(), materializationRequest)
	materializationElapsed := time.Since(materializationStart)
	result.Probes = append(result.Probes, materializationProbes...)
	result.Diagnostics = append(result.Diagnostics, materializationDiagnostics...)
	if materializationErr != nil {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticInternalInvariant,
			qsbridge.PhaseExecute,
			materializationErr.Error(),
		))
		return result
	}
	residualStart := time.Now()
	var nativePredicateProbes []ExecutionProbe
	rowSet, nativePredicateProbes, diagnostics = filterRowSetByNativePredicates(request, rowSet)
	result.Probes = append(result.Probes, nativePredicateProbes...)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	rowSet, residualDiagnostics := directBitmapFilterResidualScanPredicates(request, rowSet)
	residualElapsed := time.Since(residualStart)
	result.Diagnostics = append(result.Diagnostics, residualDiagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	if len(request.GroupBy) > 0 {
		result.Probes = append(result.Probes,
			ExecutionProbe{Section: "grouped_aggregate", Name: "phase_materialization_elapsed", Value: materializationElapsed.String()},
			ExecutionProbe{Section: "grouped_aggregate", Name: "phase_residual_elapsed", Value: residualElapsed.String()},
		)
		return directBitmapMaterializedGroupedAggregateResult(request, rowSet, result)
	}
	result.Probes = append(result.Probes,
		ExecutionProbe{Section: "aggregate", Name: "phase_materialization_elapsed", Value: materializationElapsed.String()},
		ExecutionProbe{Section: "aggregate", Name: "phase_residual_elapsed", Value: residualElapsed.String()},
	)
	if directBitmapSingleTopNAggregate(request.SQLAggregates) {
		return directBitmapTopNAggregateResult(request, rowSet, result)
	}
	return directBitmapMaterializedAggregateResult(request, rowSet, result)
}

func directBitmapMaterializeWithKernel(ctx context.Context, kernel ProjectionMaterializationKernel, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, []ExecutionProbe, error) {
	kernelRequest := qsbridge.ProjectionMaterializationKernelRequest{
		ID:          "projection_materialization",
		ProbePrefix: "projection_materialization_",
		Requests:    []qsbridge.QuantaMaterializationRequest{request},
	}
	result, err := ExecuteProjectionMaterializationKernel(ctx, kernel, kernelRequest)
	probes := append([]ExecutionProbe(nil), result.Probes...)
	diagnostics := append(qsbridge.DiagnosticSet(nil), result.Diagnostics...)
	if len(result.Results) == 0 {
		if !diagnostics.BlocksNative() {
			diagnostics = append(diagnostics, qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"projection materialization kernel returned no results",
			))
		}
		return qsbridge.QuantaProjectedRowSet{}, diagnostics, probes, err
	}
	item := result.Results[0]
	probes = append(probes, item.Probes...)
	diagnostics = append(diagnostics, item.Diagnostics...)
	return item.RowSet, diagnostics, probes, err
}

func directBitmapCountAggregateResult(request ExecutionRequest, result ExecutionResult) ExecutionResult {
	index, _ := request.RootIndex()
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   index,
		Rownums: []qsbridge.QuantaRownum{1},
	}
	for _, aggregate := range request.SQLAggregates {
		if !directBitmapCountAllAggregate(aggregate) {
			result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedSQL,
				qsbridge.PhaseExecute,
				"direct bitmap runtime only supports count(*) aggregates in this slice",
			))
			return result
		}
		alias := aggregate.Alias
		if alias == "" {
			alias = aggregate.Function
		}
		aggregateType := aggregate.Type
		if aggregateType == qsbridge.DataTypeUnknown {
			aggregateType = qsbridge.DataTypeInt
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
			Field: qsbridge.QuantaProjectionField{
				Index:   index,
				Field:   alias,
				Type:    aggregateType,
				Visible: true,
			},
			Values: []qsbridge.ResultCell{{
				Kind:  qsbridge.ValueInt,
				Value: int64(result.Count),
			}},
		})
	}
	result.RowSet = rowSet
	result.Count = uint64(rowSet.CandidateCount())
	return result
}

func directBitmapMaterializedAggregateResult(request ExecutionRequest, materialized qsbridge.QuantaProjectedRowSet, result ExecutionResult) ExecutionResult {
	index, _ := request.RootIndex()
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   index,
		Rownums: []qsbridge.QuantaRownum{1},
	}
	aggregateCells := make([]qsbridge.ResultCell, 0, len(request.SQLAggregates))
	for _, aggregate := range request.SQLAggregates {
		cell, diagnostics := directBitmapMaterializedAggregateCell(aggregate, materialized)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if result.Diagnostics.BlocksNative() {
			return result
		}
		aggregateCells = append(aggregateCells, cell)
	}
	projections := request.Projection
	if len(projections) == 0 {
		projections = directBitmapDefaultGroupedAggregateProjections(request, nil)
	}
	rows := []directBitmapGroupedAggregateRow{{Aggs: aggregateCells}}
	for _, projection := range projections {
		vector, diagnostics := directBitmapGroupedAggregateProjectionVector(projection, rows, nil, request.SQLAggregates)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if result.Diagnostics.BlocksNative() {
			return result
		}
		if vector.Field.Index == "" {
			vector.Field.Index = index
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
	}
	result.RowSet = rowSet
	result.Count = uint64(rowSet.CandidateCount())
	return result
}

func directBitmapMaterializedAggregateCell(aggregate qsbridge.Aggregate, materialized qsbridge.QuantaProjectedRowSet) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if aggregate.Filter != nil {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("direct bitmap runtime only supports unfiltered aggregates in this slice")
	}
	if directBitmapCountAllAggregate(aggregate) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(materialized.CandidateCount())}, nil
	}
	values, diagnostics := directBitmapAggregateInputValues(aggregate, materialized)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if aggregate.Mode == qsbridge.AggregateDistinct {
		if !strings.EqualFold(aggregate.Function, "count") {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("direct bitmap runtime only supports count(distinct field) in this slice")
		}
		return directBitmapDistinctCountCell(values), nil
	}
	return directBitmapNumericAggregateCell(aggregate, values)
}

func directBitmapSingleTopNAggregate(aggregates []qsbridge.Aggregate) bool {
	return len(aggregates) == 1 && strings.EqualFold(aggregates[0].Function, "topn")
}

func directBitmapTopNAggregateResult(request ExecutionRequest, materialized qsbridge.QuantaProjectedRowSet, result ExecutionResult) ExecutionResult {
	aggregate := request.SQLAggregates[0]
	values, diagnostics := directBitmapAggregateInputValues(aggregate, materialized)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	entriesByKey := make(map[string]qsbridge.TopNRankEntry)
	for _, value := range values {
		if directBitmapNullCell(value) {
			continue
		}
		key := directBitmapGroupKey(value)
		entry := entriesByKey[key]
		if entry.Count == 0 {
			entry.Value = value
		}
		entry.Count++
		entriesByKey[key] = entry
	}
	entries := make([]qsbridge.TopNRankEntry, 0, len(entriesByKey))
	for _, entry := range entriesByKey {
		entries = append(entries, entry)
	}
	rankRows := qsbridge.BuildTopNRankRows(entries, 0)
	index, _ := request.RootIndex()
	valueName := "topn"
	if field, ok := directBitmapAggregateInputField(aggregate); ok {
		valueName = "topn_" + field.Name
	}
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   index,
		Rownums: make([]qsbridge.QuantaRownum, len(rankRows)),
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field:  qsbridge.QuantaProjectionField{Index: index, Field: valueName, Type: qsbridge.DataTypeString, Visible: true},
				Values: make([]qsbridge.ResultCell, len(rankRows)),
			},
			{
				Field:  qsbridge.QuantaProjectionField{Index: index, Field: "topn_count", Type: qsbridge.DataTypeInt, Visible: true},
				Values: make([]qsbridge.ResultCell, len(rankRows)),
			},
			{
				Field:  qsbridge.QuantaProjectionField{Index: index, Field: "topn_percent", Type: qsbridge.DataTypeFloat, Visible: true},
				Values: make([]qsbridge.ResultCell, len(rankRows)),
			},
		},
	}
	for i, rankRow := range rankRows {
		rowSet.Rownums[i] = qsbridge.QuantaRownum(i + 1)
		resultRow := rankRow.ResultRow()
		for columnIndex := range resultRow {
			rowSet.ProjectionVectors[columnIndex].Values[i] = resultRow[columnIndex]
		}
	}
	result.RowSet = rowSet
	result.Count = uint64(rowSet.CandidateCount())
	return result
}

func directBitmapNumericAggregateCell(aggregate qsbridge.Aggregate, values []qsbridge.ResultCell) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	var sum float64
	var min float64
	var max float64
	seen := 0
	for _, cell := range values {
		if cell.Kind == qsbridge.ValueNull || cell.Value == nil {
			continue
		}
		value, ok := directBitmapNumericCellValue(cell)
		if !ok {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("%s aggregate requires numeric values", aggregate.Function))
		}
		if seen == 0 || value < min {
			min = value
		}
		if seen == 0 || value > max {
			max = value
		}
		sum += value
		seen++
	}
	if seen == 0 {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	switch strings.ToLower(aggregate.Function) {
	case "sum":
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: sum}, nil
	case "avg":
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: sum / float64(seen)}, nil
	case "min":
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: min}, nil
	case "max":
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: max}, nil
	default:
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("direct bitmap runtime does not support %s aggregate in this slice", aggregate.Function))
	}
}

func directBitmapDistinctCountCell(values []qsbridge.ResultCell) qsbridge.ResultCell {
	distinct := make(map[string]struct{})
	for _, cell := range values {
		if cell.Kind == qsbridge.ValueNull || cell.Value == nil {
			continue
		}
		distinct[directBitmapGroupKey(cell)] = struct{}{}
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(len(distinct))}
}

func directBitmapAllAggregatesUseBitmapCount(aggregates []qsbridge.Aggregate) bool {
	for _, aggregate := range aggregates {
		if !directBitmapCountAllAggregate(aggregate) {
			return false
		}
	}
	return true
}

func directBitmapLimitProjectedRowSet(rowSet qsbridge.QuantaProjectedRowSet, offset int, limit int) qsbridge.QuantaProjectedRowSet {
	if offset <= 0 && limit <= 0 {
		return rowSet
	}
	start := offset
	if start < 0 {
		start = 0
	}
	if start > len(rowSet.Rownums) {
		start = len(rowSet.Rownums)
	}
	end := len(rowSet.Rownums)
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	rowSet.Rownums = append([]qsbridge.QuantaRownum(nil), rowSet.Rownums[start:end]...)
	for i, vector := range rowSet.ProjectionVectors {
		if len(vector.Values) < end {
			continue
		}
		rowSet.ProjectionVectors[i].Values = append([]qsbridge.ResultCell(nil), vector.Values[start:end]...)
	}
	return rowSet
}

func directBitmapDistinctProjectedRowSet(rowSet qsbridge.QuantaProjectedRowSet) qsbridge.QuantaProjectedRowSet {
	if len(rowSet.Rownums) == 0 || len(rowSet.ProjectionVectors) == 0 {
		return rowSet
	}
	seen := make(map[string]struct{}, len(rowSet.Rownums))
	keep := make([]int, 0, len(rowSet.Rownums))
	for rowIndex := range rowSet.Rownums {
		key := directBitmapDistinctProjectedRowKey(rowSet, rowIndex)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		keep = append(keep, rowIndex)
	}
	if len(keep) == len(rowSet.Rownums) {
		return rowSet
	}
	return directBitmapProjectedRowSetRows(rowSet, keep)
}

func directBitmapDistinctProjectedRowKey(rowSet qsbridge.QuantaProjectedRowSet, rowIndex int) string {
	var builder strings.Builder
	usedVisible := false
	for _, vector := range rowSet.ProjectionVectors {
		if !vector.Field.Visible {
			continue
		}
		usedVisible = true
		builder.WriteString(vector.Field.Field)
		builder.WriteByte('=')
		builder.WriteString(directBitmapDistinctProjectedCellKey(vector, rowIndex))
		builder.WriteByte('|')
	}
	if usedVisible {
		return builder.String()
	}
	for _, vector := range rowSet.ProjectionVectors {
		builder.WriteString(vector.Field.Field)
		builder.WriteByte('=')
		builder.WriteString(directBitmapDistinctProjectedCellKey(vector, rowIndex))
		builder.WriteByte('|')
	}
	return builder.String()
}

func directBitmapDistinctProjectedCellKey(vector qsbridge.QuantaProjectionVector, rowIndex int) string {
	if rowIndex < 0 || rowIndex >= len(vector.Values) {
		return "<missing>"
	}
	return directBitmapGroupKey(vector.Values[rowIndex])
}

func directBitmapOrderProjectedRows(request ExecutionRequest, rowSet qsbridge.QuantaProjectedRowSet) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet) {
	if len(request.OrderBy) == 0 || rowSet.CandidateCount() <= 1 {
		return rowSet, nil
	}
	sortValues := make([][]qsbridge.ResultCell, len(request.OrderBy))
	for sortIndex, spec := range request.OrderBy {
		values := make([]qsbridge.ResultCell, rowSet.CandidateCount())
		for rowIndex := 0; rowIndex < rowSet.CandidateCount(); rowIndex++ {
			cell, diagnostics := directBitmapEvaluateMaterializedExpr(spec.Expr, rowSet, rowIndex)
			if diagnostics.BlocksNative() {
				return qsbridge.QuantaProjectedRowSet{}, diagnostics
			}
			values[rowIndex] = cell
		}
		sortValues[sortIndex] = values
	}
	indexes := make([]int, rowSet.CandidateCount())
	for i := range indexes {
		indexes[i] = i
	}
	sort.SliceStable(indexes, func(i, j int) bool {
		leftIndex := indexes[i]
		rightIndex := indexes[j]
		for sortIndex, spec := range request.OrderBy {
			left := sortValues[sortIndex][leftIndex]
			right := sortValues[sortIndex][rightIndex]
			if directBitmapCellEqual(left, right) {
				continue
			}
			less := directBitmapCellLess(left, right)
			if spec.Direction == qsbridge.SortDescending {
				return !less
			}
			return less
		}
		return rowSet.Rownums[leftIndex] < rowSet.Rownums[rightIndex]
	})
	return directBitmapProjectedRowSetRows(rowSet, indexes), nil
}

func directBitmapEvaluateProjectionRowSet(request ExecutionRequest, materialized qsbridge.QuantaProjectedRowSet) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet) {
	if len(request.Projection) == 0 {
		return materialized, nil
	}
	index, _ := request.RootIndex()
	if index == "" {
		index = materialized.Index
	}
	rowSet := materialized
	rowSet.ProjectionVectors = make([]qsbridge.QuantaProjectionVector, 0, len(request.Projection))
	for _, projection := range request.Projection {
		values := make([]qsbridge.ResultCell, materialized.CandidateCount())
		for rowIndex := 0; rowIndex < materialized.CandidateCount(); rowIndex++ {
			cell, diagnostics := directBitmapEvaluateMaterializedExpr(projection.Expr, materialized, rowIndex)
			if diagnostics.BlocksNative() {
				return qsbridge.QuantaProjectedRowSet{}, diagnostics
			}
			values[rowIndex] = cell
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
			Field: qsbridge.QuantaProjectionField{
				Index:   index,
				Field:   directBitmapProjectionColumnName(projection),
				Type:    directBitmapProjectionType(projection, qsbridge.ExprDataType(projection.Expr)),
				Visible: true,
			},
			Values: values,
		})
	}
	return rowSet, rowSet.ValidateShape()
}

func directBitmapProjectedRowSetRows(rowSet qsbridge.QuantaProjectedRowSet, keep []int) qsbridge.QuantaProjectedRowSet {
	rownums := make([]qsbridge.QuantaRownum, 0, len(keep))
	for _, index := range keep {
		if index >= 0 && index < len(rowSet.Rownums) {
			rownums = append(rownums, rowSet.Rownums[index])
		}
	}
	rowSet.Rownums = rownums
	for vectorIndex, vector := range rowSet.ProjectionVectors {
		values := make([]qsbridge.ResultCell, 0, len(keep))
		for _, index := range keep {
			if index >= 0 && index < len(vector.Values) {
				values = append(values, vector.Values[index])
			}
		}
		rowSet.ProjectionVectors[vectorIndex].Values = values
	}
	return rowSet
}

func directBitmapOrderVisibleProjectedRowSet(rowSet qsbridge.QuantaProjectedRowSet, columns []qsbridge.FieldRef) qsbridge.QuantaProjectedRowSet {
	if len(columns) == 0 || len(rowSet.ProjectionVectors) == 0 {
		return rowSet
	}
	ordered := make([]qsbridge.QuantaProjectionVector, 0, len(rowSet.ProjectionVectors))
	used := make([]bool, len(rowSet.ProjectionVectors))
	for _, column := range columns {
		for i, vector := range rowSet.ProjectionVectors {
			if used[i] || !vector.Field.Visible || !directBitmapProjectionVectorMatchesField(vector, column) {
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

func directBitmapCountAllAggregate(aggregate qsbridge.Aggregate) bool {
	return strings.EqualFold(aggregate.Function, "count") &&
		aggregate.Mode != qsbridge.AggregateDistinct &&
		aggregate.Input == nil &&
		aggregate.Filter == nil
}

func directBitmapAggregateInputField(aggregate qsbridge.Aggregate) (qsbridge.FieldRef, bool) {
	switch input := aggregate.Input.(type) {
	case qsbridge.FieldExpr:
		return input.Ref, true
	case *qsbridge.FieldExpr:
		if input != nil {
			return input.Ref, true
		}
	}
	return qsbridge.FieldRef{}, false
}

func directBitmapAggregateInputValues(aggregate qsbridge.Aggregate, materialized qsbridge.QuantaProjectedRowSet) ([]qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if aggregate.Input == nil {
		return nil, directBitmapAggregateDiagnostics(fmt.Sprintf("%s aggregate requires an input expression", aggregate.Function))
	}
	values := make([]qsbridge.ResultCell, 0, materialized.CandidateCount())
	for i := 0; i < materialized.CandidateCount(); i++ {
		cell, diagnostics := directBitmapEvaluateMaterializedExpr(aggregate.Input, materialized, i)
		if diagnostics.BlocksNative() {
			return nil, diagnostics
		}
		values = append(values, cell)
	}
	return values, nil
}

func directBitmapEvaluateMaterializedExpr(expr qsbridge.Expr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if field, ok := directBitmapExprField(expr); ok {
		values, ok := directBitmapProjectedValues(materialized, field)
		if !ok {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("aggregate input field is not present in materialized row set")
		}
		if index >= len(values) {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("aggregate input field has fewer values than grouped candidates")
		}
		return values[index], nil
	}
	if literal, ok := directBitmapLiteralExpr(expr); ok {
		return directBitmapLiteralCell(literal), nil
	}
	if binary, ok := directBitmapBinaryExpr(expr); ok {
		return directBitmapEvaluateMaterializedBinaryExpr(binary, materialized, index)
	}
	if searchedCase, ok := directBitmapSearchedCaseExpr(expr); ok {
		return directBitmapEvaluateMaterializedSearchedCaseExpr(searchedCase, materialized, index)
	}
	if call, ok := directBitmapCallExpr(expr); ok {
		return directBitmapEvaluateMaterializedCallExpr(call, materialized, index)
	}
	return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("aggregate input expression is not supported by direct bitmap runtime")
}

func directBitmapEvaluateMaterializedCallExpr(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	switch strings.ToLower(call.Name) {
	case "year", "yy":
		return directBitmapEvaluateMaterializedTimePartCall(call, materialized, index)
	case "mm", "monthofyear", "yymm", "dayofweek", "hourofday", "hourofweek", "seconds":
		return directBitmapEvaluateMaterializedTimePartCall(call, materialized, index)
	case "substr", "substring", "mid":
		return directBitmapEvaluateMaterializedSubstringCall(call, materialized, index)
	case "lower", "lcase":
		return directBitmapEvaluateMaterializedStringTransformCall(call, materialized, index, strings.ToLower)
	case "upper", "ucase":
		return directBitmapEvaluateMaterializedStringTransformCall(call, materialized, index, strings.ToUpper)
	case "length", "char_length":
		return directBitmapEvaluateMaterializedLengthCall(call, materialized, index)
	case "tostring":
		return directBitmapEvaluateMaterializedToStringCall(call, materialized, index)
	case "toint":
		return directBitmapEvaluateMaterializedToIntCall(call, materialized, index)
	case "tonumber":
		return directBitmapEvaluateMaterializedToNumberCall(call, materialized, index)
	case "todate":
		return directBitmapEvaluateMaterializedToDateCall(call, materialized, index)
	case "timediff":
		return directBitmapEvaluateMaterializedTimeDiffCall(call, materialized, index)
	case "hash.sha256":
		return directBitmapEvaluateMaterializedSHA256Call(call, materialized, index)
	default:
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q is not supported by direct bitmap runtime", call.Name))
	}
}

func directBitmapEvaluateMaterializedTimePartCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument", call.Name))
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(cell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	value, ok := directBitmapTimeCellValue(cell)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires a time value", call.Name))
	}
	value = value.UTC()
	switch strings.ToLower(call.Name) {
	case "year", "yy":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(value.Year())}, nil
	case "mm", "monthofyear":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(value.Month())}, nil
	case "yymm":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(value.Year()*100 + int(value.Month()))}, nil
	case "dayofweek":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(value.Weekday()) + 1}, nil
	case "hourofday":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(value.Hour())}, nil
	case "hourofweek":
		weekday := int(value.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64((weekday-1)*24 + value.Hour())}, nil
	case "seconds":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: value.Unix()}, nil
	default:
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q is not supported by direct bitmap runtime", call.Name))
	}
}

func directBitmapEvaluateMaterializedSubstringCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 2 && len(call.Args) != 3 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects two or three arguments", call.Name))
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(cell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	startCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	start, ok := directBitmapMaterializedIntArgument(startCell)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires integer start", call.Name))
	}
	length := len(fmt.Sprint(cell.Value))
	if len(call.Args) == 3 {
		lengthCell, lengthDiagnostics := directBitmapEvaluateMaterializedExpr(call.Args[2], materialized, index)
		if lengthDiagnostics.BlocksNative() {
			return qsbridge.ResultCell{}, lengthDiagnostics
		}
		length, ok = directBitmapMaterializedIntArgument(lengthCell)
		if !ok {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires integer length", call.Name))
		}
	}
	value := fmt.Sprint(cell.Value)
	if start < 1 || length <= 0 || start > len(value) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: ""}, nil
	}
	begin := start - 1
	end := begin + length
	if end > len(value) {
		end = len(value)
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: value[begin:end]}, nil
}

func directBitmapEvaluateMaterializedStringTransformCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int, transform func(string) string) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument", call.Name))
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(cell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: transform(fmt.Sprint(cell.Value))}, nil
}

func directBitmapEvaluateMaterializedLengthCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument", call.Name))
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(cell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(len([]rune(fmt.Sprint(cell.Value))))}, nil
}

func directBitmapEvaluateMaterializedToStringCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument", call.Name))
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(cell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: fmt.Sprint(cell.Value)}, nil
}

func directBitmapEvaluateMaterializedToIntCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument", call.Name))
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(cell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	number, ok := directBitmapNumericCellValue(cell)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires a numeric value", call.Name))
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(number)}, nil
}

func directBitmapEvaluateMaterializedToNumberCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument", call.Name))
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(cell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	number, ok := directBitmapNumericCellValue(cell)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires a numeric value", call.Name))
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: number}, nil
}

func directBitmapEvaluateMaterializedToDateCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument", call.Name))
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(cell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	if strings.EqualFold(fmt.Sprint(cell.Value), "now") {
		return qsbridge.ResultCell{Kind: qsbridge.ValueTime, Value: time.Now().UTC()}, nil
	}
	value, ok := directBitmapTimeCellValue(cell)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires a date value", call.Name))
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueTime, Value: value.UTC()}, nil
}

func directBitmapEvaluateMaterializedTimeDiffCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) < 2 || len(call.Args) > 3 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects two or three arguments", call.Name))
	}
	leftCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	rightCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(leftCell) || directBitmapNullCell(rightCell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	left, ok := directBitmapTimeCellValue(leftCell)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires a time value", call.Name))
	}
	right, ok := directBitmapTimeCellValue(rightCell)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires a time value", call.Name))
	}
	scale := "duration"
	if len(call.Args) == 3 {
		scaleCell, scaleDiagnostics := directBitmapEvaluateMaterializedExpr(call.Args[2], materialized, index)
		if scaleDiagnostics.BlocksNative() {
			return qsbridge.ResultCell{}, scaleDiagnostics
		}
		if !directBitmapNullCell(scaleCell) {
			scale = strings.ToLower(fmt.Sprint(scaleCell.Value))
		}
	}
	diff := left.Sub(right)
	switch scale {
	case "nanoseconds":
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: fmt.Sprint(diff.Nanoseconds())}, nil
	case "microseconds":
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: fmt.Sprint(diff.Microseconds())}, nil
	case "milliseconds":
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: fmt.Sprint(diff.Milliseconds())}, nil
	case "seconds":
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: fmt.Sprint(diff.Seconds())}, nil
	case "minutes":
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: fmt.Sprint(diff.Minutes())}, nil
	case "hours":
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: fmt.Sprint(diff.Hours())}, nil
	case "duration":
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: diff.String()}, nil
	default:
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q does not support scale %q", call.Name, scale))
	}
}

func directBitmapEvaluateMaterializedSHA256Call(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument", call.Name))
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(cell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	sum := sha256.Sum256([]byte(fmt.Sprint(cell.Value)))
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: hex.EncodeToString(sum[:])}, nil
}

func directBitmapMaterializedIntArgument(cell qsbridge.ResultCell) (int, bool) {
	switch value := cell.Value.(type) {
	case int:
		return value, true
	case int8:
		return int(value), true
	case int16:
		return int(value), true
	case int32:
		return int(value), true
	case int64:
		return int(value), true
	case uint:
		return int(value), true
	case uint8:
		return int(value), true
	case uint16:
		return int(value), true
	case uint32:
		return int(value), true
	case uint64:
		return int(value), true
	case float64:
		return int(value), true
	case float32:
		return int(value), true
	default:
		return 0, false
	}
}

func directBitmapEvaluateMaterializedSearchedCaseExpr(searchedCase qsbridge.SearchedCaseExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	for _, when := range searchedCase.Whens {
		matched, diagnostics := directBitmapEvaluateResidualBoolExpr(when.Condition, materialized, index)
		if diagnostics.BlocksNative() {
			return qsbridge.ResultCell{}, diagnostics
		}
		if matched {
			return directBitmapEvaluateMaterializedExpr(when.Result, materialized, index)
		}
	}
	if searchedCase.Else != nil {
		return directBitmapEvaluateMaterializedExpr(searchedCase.Else, materialized, index)
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
}

func directBitmapSearchedCaseExpr(expr qsbridge.Expr) (qsbridge.SearchedCaseExpr, bool) {
	switch n := expr.(type) {
	case qsbridge.SearchedCaseExpr:
		return n, true
	case *qsbridge.SearchedCaseExpr:
		if n != nil {
			return *n, true
		}
	}
	return qsbridge.SearchedCaseExpr{}, false
}

func directBitmapCallExpr(expr qsbridge.Expr) (qsbridge.CallExpr, bool) {
	switch n := expr.(type) {
	case qsbridge.CallExpr:
		return n, true
	case *qsbridge.CallExpr:
		if n != nil {
			return *n, true
		}
	}
	return qsbridge.CallExpr{}, false
}

func directBitmapEvaluateMaterializedBinaryExpr(binary qsbridge.BinaryExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	left, diagnostics := directBitmapEvaluateMaterializedExpr(binary.Left, materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	right, diagnostics := directBitmapEvaluateMaterializedExpr(binary.Right, materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(left) || directBitmapNullCell(right) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	leftNumber, leftOK := directBitmapNumericCellValue(left)
	rightNumber, rightOK := directBitmapNumericCellValue(right)
	if !leftOK || !rightOK {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf(
			"arithmetic aggregate input requires numeric values: left_kind=%s left_value=%v right_kind=%s right_value=%v",
			left.Kind,
			left.Value,
			right.Kind,
			right.Value,
		))
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
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("division by zero in aggregate input expression")
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: leftNumber / rightNumber}, nil
	default:
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("aggregate input binary expression must be arithmetic")
	}
}

func directBitmapNullCell(cell qsbridge.ResultCell) bool {
	return cell.Kind == qsbridge.ValueNull || cell.Value == nil
}

func directBitmapTimeCellValue(cell qsbridge.ResultCell) (time.Time, bool) {
	switch typed := cell.Value.(type) {
	case time.Time:
		return typed, true
	case string:
		for _, layout := range []string{
			time.RFC3339Nano,
			"2006-01-02 15:04:05.999999999Z",
			"2006-01-02 15:04:05.000Z",
			"2006-01-02 15:04:05",
			"2006-01-02",
		} {
			parsed, err := time.Parse(layout, strings.TrimSpace(typed))
			if err == nil {
				return parsed, true
			}
		}
	}
	return time.Time{}, false
}

func directBitmapProjectedValues(rowSet qsbridge.QuantaProjectedRowSet, field qsbridge.FieldRef) ([]qsbridge.ResultCell, bool) {
	for _, vector := range rowSet.ProjectionVectors {
		if !directBitmapProjectionVectorMatchesField(vector, field) {
			continue
		}
		return vector.Values, true
	}
	return nil, false
}

func directBitmapProjectionVectorMatchesField(vector qsbridge.QuantaProjectionVector, field qsbridge.FieldRef) bool {
	if vector.Field.Index != field.Table.Table || vector.Field.Field != directBitmapFieldPhysicalName(field) {
		return false
	}
	if vector.Field.Role == "" {
		return true
	}
	return strings.EqualFold(string(vector.Field.Role), materializationFieldRole(vector.Field.Index, field))
}

func directBitmapFieldPhysicalName(field qsbridge.FieldRef) string {
	if field.PhysicalName != "" {
		return field.PhysicalName
	}
	return field.Name
}

func directBitmapNumericCellValue(cell qsbridge.ResultCell) (float64, bool) {
	switch value := cell.Value.(type) {
	case int:
		return float64(value), true
	case int8:
		return float64(value), true
	case int16:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case uint:
		return float64(value), true
	case uint8:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint64:
		return float64(value), true
	case float32:
		return float64(value), true
	case float64:
		return value, true
	case string:
		return directBitmapParseNumericString(value)
	case []byte:
		return directBitmapParseNumericString(string(value))
	case fmt.Stringer:
		return directBitmapParseNumericString(value.String())
	case *big.Rat:
		if value == nil {
			return 0, false
		}
		parsed, _ := value.Float64()
		return parsed, true
	case big.Rat:
		parsed, _ := value.Float64()
		return parsed, true
	case *big.Float:
		if value == nil {
			return 0, false
		}
		parsed, _ := value.Float64()
		return parsed, true
	case big.Float:
		parsed, _ := value.Float64()
		return parsed, true
	default:
		return 0, false
	}
}

func directBitmapParseNumericString(value string) (float64, bool) {
	value = strings.TrimSpace(value)
	parsed, err := strconv.ParseFloat(value, 64)
	if err == nil {
		return parsed, true
	}
	var rational big.Rat
	if _, ok := rational.SetString(value); ok {
		parsed, _ := rational.Float64()
		return parsed, true
	}
	return 0, false
}

func directBitmapAggregateResultType(aggregate qsbridge.Aggregate) qsbridge.DataType {
	if aggregate.Type != qsbridge.DataTypeUnknown {
		return aggregate.Type
	}
	if strings.EqualFold(aggregate.Function, "count") {
		return qsbridge.DataTypeInt
	}
	return qsbridge.DataTypeFloat
}

func directBitmapAggregateDiagnostics(message string) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, message),
	}
}

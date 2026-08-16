package qsruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

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
	Sessions                                DirectSessionProvider
	Adapter                                 BitmapQueryResultAdapter
	Materializer                            ProjectionMaterializer
	Materialization                         ProjectionMaterializationKernel
	ProjectionBSIReader                     NativeProjectionBSIReader
	SameRowComparison                       SameRowComparisonKernel
	RelationshipJoins                       RelationshipVectorJoinExecutor
	RelationshipReader                      RelationshipVectorReader
	FilterAdapter                           DirectBitmapFilterAdapter
	SiblingDiversity                        RelationshipSiblingDiversityReader
	BitmapGroupCounts                       BitmapGroupCountReader
	BitmapGroupAggregates                   BitmapGroupAggregateReader
	CorrelatedSiblingRightCandidateSeed     *BitmapQueryResult
	CorrelatedSiblingRightCandidateSeedMode string
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
		if directBitmapCountOnlyBitmapResultRequest(request) {
			if countOnlySession, ok := session.(DirectCountOnlyBitmapSessionHandle); ok {
				bitmapResult, queryDiagnostics, err = countOnlySession.QueryBitmapCountOnly(ctx, request)
			} else {
				bitmapResult, queryDiagnostics, err = session.QueryBitmap(ctx, request)
			}
		} else {
			bitmapResult, queryDiagnostics, err = session.QueryBitmap(ctx, request)
		}
		queryElapsed = time.Since(queryStart)
		releaseDiagnostics = session.Release(ctx)
	}
	rootSeedResult := bitmapResult.Clone()
	var sameRowProbes []ExecutionProbe
	var sameRowDiagnostics qsbridge.DiagnosticSet
	var sameRowErr error
	var sameRowApplied bool
	bitmapResult, sameRowProbes, sameRowDiagnostics, sameRowErr, sameRowApplied = r.directBitmapApplySameRowResiduals(ctx, request, bitmapResult)
	if sameRowApplied {
		request = directBitmapWithoutSameRowResidualPredicates(request)
	}
	bitmapResult, membershipProbes, membershipDiagnostics, membershipErr := r.directBitmapApplyMemberships(ctx, request, bitmapResult, rootSeedResult)
	directBitmapRecordCoreInstrumentation(ctx, request, bitmapResult, queryElapsed)
	recordExecutionProbes(ctx, bitmapResult.Probes)
	recordExecutionProbes(ctx, sameRowProbes)
	recordExecutionProbes(ctx, membershipProbes)
	result := r.Adapter.ToExecutionResult(bitmapResult)
	result.Probes = append(result.Probes, request.Probes...)
	result.Probes = append(result.Probes, sameRowProbes...)
	result.Probes = append(result.Probes, membershipProbes...)
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
	recordExecutionProbes(ctx, materializationProbes)
	result.RowSet = rowSet
	result.Diagnostics = append(result.Diagnostics, materializationDiagnostics...)
	if materializationErr != nil {
		return result, materializationErr
	}
	rowSet, nativePredicateProbes, nativePredicateDiagnostics := filterRowSetByNativePredicates(request, rowSet)
	result.Probes = append(result.Probes, nativePredicateProbes...)
	recordExecutionProbes(ctx, nativePredicateProbes)
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
	rowSet = directBitmapLimitProjectedRowSet(rowSet, request.Result.Offset, request.Result.Limit, request.Result.HasResultLimit())
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

func directBitmapRecordCoreInstrumentation(ctx context.Context, request ExecutionRequest, bitmapResult BitmapQueryResult, queryElapsed time.Duration) {
	recorder := ExecutionInstrumentationFromContext(ctx)
	if recorder == nil {
		return
	}
	recorder.ObserveDuration("direct_bitmap", "phase_bitmap_query_elapsed", queryElapsed, "")
	recorder.ObserveCount("direct_bitmap", "fragment_count", uint64(len(request.Query.Fragments)), directBitmapFragmentProbeDetail(request))
	recorder.ObserveCount("direct_bitmap", "bitmap_count", bitmapResult.Count, fmt.Sprintf("rownums=%d", len(bitmapResult.Rownums)))
}

func directBitmapCandidateSetResult(request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	if !request.HasCandidateSet {
		return BitmapQueryResult{}, nil, nil
	}
	rownums := append([]qsbridge.QuantaRownum(nil), request.CandidateSet.Rownums...)
	return BitmapQueryResult{Success: true, Count: uint64(len(rownums)), Rownums: rownums}, nil, nil
}

func directBitmapCountOnlyBitmapResultRequest(request ExecutionRequest) bool {
	return request.Mutation.Kind == qsbridge.MutationUnknown &&
		len(request.Joins) == 0 &&
		len(request.Memberships) == 0 &&
		len(request.GroupBy) == 0 &&
		len(request.Having) == 0 &&
		len(request.SQLAggregates) > 0 &&
		directBitmapAllAggregatesUseBitmapCount(request.SQLAggregates) &&
		!directBitmapHasResidualScanPredicates(request) &&
		request.NativePredicates.Empty()
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
	if bucketResult, ok := r.directBitmapYearBucketCountAggregateResult(ctx, request, result); ok {
		return bucketResult
	}
	if groupCountResult, ok := r.directBitmapBitmapGroupCountAggregateResult(ctx, request, bitmapResult, result); ok {
		return groupCountResult
	}
	if groupAggregateResult, ok := r.directBitmapBitmapGroupAggregateResult(ctx, request, bitmapResult, result); ok {
		return groupAggregateResult
	}
	materializationKernel := r.projectionMaterializationKernel()
	if materializationKernel == nil {
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
	if projectionMaterializationKernelSupportsExpressions(materializationKernel) {
		materializationRequest = materializationRequestWithPhysicalGroupExpressions(request, materializationRequest)
	}
	materializationStart := time.Now()
	rowSet, materializationDiagnostics, materializationProbes, materializationErr := directBitmapMaterializeWithKernel(ctx, materializationKernel, materializationRequest)
	materializationElapsed := time.Since(materializationStart)
	result.Probes = append(result.Probes, materializationProbes...)
	recordExecutionProbes(ctx, materializationProbes)
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
	recordExecutionProbes(ctx, nativePredicateProbes)
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
		probes := []ExecutionProbe{
			ExecutionProbe{Section: "grouped_aggregate", Name: "phase_materialization_elapsed", Value: materializationElapsed.String()},
			ExecutionProbe{Section: "grouped_aggregate", Name: "phase_residual_elapsed", Value: residualElapsed.String()},
		}
		result.Probes = append(result.Probes, probes...)
		recordExecutionProbes(ctx, probes)
		return directBitmapMaterializedGroupedAggregateResult(request, rowSet, result)
	}
	probes := []ExecutionProbe{
		ExecutionProbe{Section: "aggregate", Name: "phase_materialization_elapsed", Value: materializationElapsed.String()},
		ExecutionProbe{Section: "aggregate", Name: "phase_residual_elapsed", Value: residualElapsed.String()},
	}
	result.Probes = append(result.Probes, probes...)
	recordExecutionProbes(ctx, probes)
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
	if !executionProbeSliceContainsAll(probes, item.Probes) {
		probes = append(probes, item.Probes...)
	}
	diagnostics = append(diagnostics, item.Diagnostics...)
	return item.RowSet, diagnostics, probes, err
}

func executionProbeSliceContainsAll(haystack, needles []ExecutionProbe) bool {
	for _, needle := range needles {
		found := false
		for _, candidate := range haystack {
			if executionProbesEqual(candidate, needle) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func executionProbesEqual(left, right ExecutionProbe) bool {
	return left.Section == right.Section &&
		left.Name == right.Name &&
		left.Value == right.Value &&
		left.Detail == right.Detail
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

func directBitmapLimitProjectedRowSet(rowSet qsbridge.QuantaProjectedRowSet, offset int, limit int, hasLimit bool) qsbridge.QuantaProjectedRowSet {
	if !hasLimit && offset <= 0 {
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
	if hasLimit && limit <= 0 {
		end = start
	} else if hasLimit && start+limit < end {
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
	if values, ok := directBitmapProjectedExpressionValues(materialized, call); ok {
		if index >= len(values) {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics("derived expression projection has fewer values than materialized candidates")
		}
		return values[index], nil
	}
	switch strings.ToLower(call.Name) {
	case "year", "yy":
		return directBitmapEvaluateMaterializedTimePartCall(call, materialized, index)
	case "mm", "monthofyear", "month", "monthname", "yymm", "day", "dayofmonth", "dayname", "dayofweek", "quarter", "hourofday", "hourofweek", "seconds":
		return directBitmapEvaluateMaterializedTimePartCall(call, materialized, index)
	case "date":
		return directBitmapEvaluateMaterializedDateCall(call, materialized, index)
	case "week":
		return directBitmapEvaluateMaterializedWeekCall(call, materialized, index)
	case "substr", "substring", "mid":
		return directBitmapEvaluateMaterializedSubstringCall(call, materialized, index)
	case "lower", "lcase":
		return directBitmapEvaluateMaterializedStringTransformCall(call, materialized, index, strings.ToLower)
	case "upper", "ucase":
		return directBitmapEvaluateMaterializedStringTransformCall(call, materialized, index, strings.ToUpper)
	case "length", "char_length":
		return directBitmapEvaluateMaterializedLengthCall(call, materialized, index)
	case "concat":
		return directBitmapEvaluateMaterializedConcatCall(call, materialized, index)
	case "concat_ws":
		return directBitmapEvaluateMaterializedConcatWSCall(call, materialized, index)
	case "trim":
		return directBitmapEvaluateMaterializedTrimCall(call, materialized, index)
	case "ltrim":
		return directBitmapEvaluateMaterializedWhitespaceTrimCall(call, materialized, index, strings.TrimLeftFunc)
	case "rtrim":
		return directBitmapEvaluateMaterializedWhitespaceTrimCall(call, materialized, index, strings.TrimRightFunc)
	case "replace":
		return directBitmapEvaluateMaterializedReplaceCall(call, materialized, index)
	case "left":
		return directBitmapEvaluateMaterializedStringSliceCall(call, materialized, index, false)
	case "right":
		return directBitmapEvaluateMaterializedStringSliceCall(call, materialized, index, true)
	case "repeat":
		return directBitmapEvaluateMaterializedRepeatCall(call, materialized, index)
	case "reverse":
		return directBitmapEvaluateMaterializedReverseCall(call, materialized, index)
	case "lpad", "rpad":
		return directBitmapEvaluateMaterializedPadCall(call, materialized, index)
	case "ascii":
		return directBitmapEvaluateMaterializedASCIICall(call, materialized, index)
	case "locate", "instr":
		return directBitmapEvaluateMaterializedLocateCall(call, materialized, index)
	case "coalesce":
		return directBitmapEvaluateMaterializedCoalesceCall(call, materialized, index)
	case "ifnull":
		return directBitmapEvaluateMaterializedIfNullCall(call, materialized, index)
	case "nullif":
		return directBitmapEvaluateMaterializedNullIfCall(call, materialized, index)
	case "isnull":
		return directBitmapEvaluateMaterializedIsNullCall(call, materialized, index)
	case "if":
		return directBitmapEvaluateMaterializedIfCall(call, materialized, index)
	case "greatest", "least":
		return directBitmapEvaluateMaterializedGreatestLeastCall(call, materialized, index)
	case "abs":
		return directBitmapEvaluateMaterializedAbsCall(call, materialized, index)
	case "round":
		return directBitmapEvaluateMaterializedRoundCall(call, materialized, index)
	case "floor":
		return directBitmapEvaluateMaterializedUnaryNumericCall(call, materialized, index, math.Floor)
	case "ceil", "ceiling":
		return directBitmapEvaluateMaterializedUnaryNumericCall(call, materialized, index, math.Ceil)
	case "truncate":
		return directBitmapEvaluateMaterializedTruncateCall(call, materialized, index)
	case "mod":
		return directBitmapEvaluateMaterializedModCall(call, materialized, index)
	case "pow", "power":
		return directBitmapEvaluateMaterializedPowCall(call, materialized, index)
	case "sign":
		return directBitmapEvaluateMaterializedSignCall(call, materialized, index)
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
	case "date_format":
		return directBitmapEvaluateMaterializedDateFormatCall(call, materialized, index)
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
	case "mm", "monthofyear", "month":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(value.Month())}, nil
	case "monthname":
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: value.Month().String()}, nil
	case "yymm":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(value.Year()*100 + int(value.Month()))}, nil
	case "day", "dayofmonth":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(value.Day())}, nil
	case "dayname":
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: value.Weekday().String()}, nil
	case "dayofweek":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(value.Weekday()) + 1}, nil
	case "quarter":
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64((int(value.Month())-1)/3 + 1)}, nil
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

func directBitmapEvaluateMaterializedDateCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
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
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: value.UTC().Format("2006-01-02")}, nil
}

func directBitmapEvaluateMaterializedWeekCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 && len(call.Args) != 2 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one or two arguments", call.Name))
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(cell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	if len(call.Args) == 2 {
		modeCell, modeDiagnostics := directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
		if modeDiagnostics.BlocksNative() {
			return qsbridge.ResultCell{}, modeDiagnostics
		}
		if directBitmapNullCell(modeCell) {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		mode, ok := directBitmapMaterializedIntArgument(modeCell)
		if !ok {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires integer mode", call.Name))
		}
		if mode != 0 {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q only supports MySQL default mode 0", call.Name))
		}
	}
	value, ok := directBitmapTimeCellValue(cell)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires a time value", call.Name))
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(directBitmapMySQLWeekModeZero(value.UTC()))}, nil
}

func directBitmapMySQLWeekModeZero(value time.Time) int {
	start := time.Date(value.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	daysBeforeFirstSunday := (7 - int(start.Weekday())) % 7
	dayOfYear := value.YearDay() - 1
	if dayOfYear < daysBeforeFirstSunday {
		return 0
	}
	return (dayOfYear-daysBeforeFirstSunday)/7 + 1
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
	value := fmt.Sprint(cell.Value)
	runes := []rune(value)
	length := len(runes)
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
	if start == 0 || length <= 0 {
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: ""}, nil
	}
	begin := start - 1
	if start < 0 {
		begin = len(runes) + start
	}
	if begin < 0 {
		begin = 0
	}
	if begin >= len(runes) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: ""}, nil
	}
	end := begin + length
	if end > len(runes) {
		end = len(runes)
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: string(runes[begin:end])}, nil
}

func directBitmapEvaluateMaterializedConcatCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) == 0 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects at least one argument", call.Name))
	}
	var builder strings.Builder
	for _, arg := range call.Args {
		cell, diagnostics := directBitmapEvaluateMaterializedExpr(arg, materialized, index)
		if diagnostics.BlocksNative() {
			return qsbridge.ResultCell{}, diagnostics
		}
		if directBitmapNullCell(cell) {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		builder.WriteString(fmt.Sprint(cell.Value))
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: builder.String()}, nil
}

func directBitmapEvaluateMaterializedConcatWSCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) < 2 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects at least two arguments", call.Name))
	}
	separatorCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(separatorCell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	separator := fmt.Sprint(separatorCell.Value)
	parts := make([]string, 0, len(call.Args)-1)
	for _, arg := range call.Args[1:] {
		cell, diagnostics := directBitmapEvaluateMaterializedExpr(arg, materialized, index)
		if diagnostics.BlocksNative() {
			return qsbridge.ResultCell{}, diagnostics
		}
		if directBitmapNullCell(cell) {
			continue
		}
		parts = append(parts, fmt.Sprint(cell.Value))
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: strings.Join(parts, separator)}, nil
}

func directBitmapEvaluateMaterializedTrimCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) == 1 {
		cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
		if diagnostics.BlocksNative() {
			return qsbridge.ResultCell{}, diagnostics
		}
		if directBitmapNullCell(cell) {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: strings.TrimSpace(fmt.Sprint(cell.Value))}, nil
	}
	if len(call.Args) != 3 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument or MySQL TRIM mode/removal/value arguments", call.Name))
	}
	modeCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	removalCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	valueCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[2], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(modeCell) || directBitmapNullCell(removalCell) || directBitmapNullCell(valueCell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	mode := strings.ToLower(strings.TrimSpace(fmt.Sprint(modeCell.Value)))
	removal := fmt.Sprint(removalCell.Value)
	value := fmt.Sprint(valueCell.Value)
	if removal == "" {
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: value}, nil
	}
	switch mode {
	case "leading":
		value = trimRepeatedPrefix(value, removal)
	case "trailing":
		value = trimRepeatedSuffix(value, removal)
	case "both":
		value = trimRepeatedSuffix(trimRepeatedPrefix(value, removal), removal)
	default:
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q does not support trim mode %q", call.Name, mode))
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: value}, nil
}

func directBitmapEvaluateMaterializedWhitespaceTrimCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int, trim func(string, func(rune) bool) string) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
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
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: trim(fmt.Sprint(cell.Value), unicode.IsSpace)}, nil
}

func trimRepeatedPrefix(value, prefix string) string {
	for strings.HasPrefix(value, prefix) {
		value = strings.TrimPrefix(value, prefix)
	}
	return value
}

func trimRepeatedSuffix(value, suffix string) string {
	for strings.HasSuffix(value, suffix) {
		value = strings.TrimSuffix(value, suffix)
	}
	return value
}

func directBitmapEvaluateMaterializedReplaceCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 3 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects three arguments", call.Name))
	}
	cells := make([]qsbridge.ResultCell, 0, len(call.Args))
	for _, arg := range call.Args {
		cell, diagnostics := directBitmapEvaluateMaterializedExpr(arg, materialized, index)
		if diagnostics.BlocksNative() {
			return qsbridge.ResultCell{}, diagnostics
		}
		if directBitmapNullCell(cell) {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		cells = append(cells, cell)
	}
	return qsbridge.ResultCell{
		Kind:  qsbridge.ValueString,
		Value: strings.ReplaceAll(fmt.Sprint(cells[0].Value), fmt.Sprint(cells[1].Value), fmt.Sprint(cells[2].Value)),
	}, nil
}

func directBitmapEvaluateMaterializedStringSliceCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int, fromRight bool) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 2 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects two arguments", call.Name))
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(cell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	lengthCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	length, ok := directBitmapMaterializedIntArgument(lengthCell)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires integer length", call.Name))
	}
	if length <= 0 {
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: ""}, nil
	}
	runes := []rune(fmt.Sprint(cell.Value))
	if length > len(runes) {
		length = len(runes)
	}
	if fromRight {
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: string(runes[len(runes)-length:])}, nil
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: string(runes[:length])}, nil
}

func directBitmapEvaluateMaterializedRepeatCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 2 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects two arguments", call.Name))
	}
	valueCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	countCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(valueCell) || directBitmapNullCell(countCell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	count, ok := directBitmapMaterializedIntArgument(countCell)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires integer count", call.Name))
	}
	if count <= 0 {
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: ""}, nil
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: strings.Repeat(fmt.Sprint(valueCell.Value), count)}, nil
}

func directBitmapEvaluateMaterializedReverseCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument", call.Name))
	}
	valueCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(valueCell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	runes := []rune(fmt.Sprint(valueCell.Value))
	for left, right := 0, len(runes)-1; left < right; left, right = left+1, right-1 {
		runes[left], runes[right] = runes[right], runes[left]
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: string(runes)}, nil
}

func directBitmapEvaluateMaterializedPadCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 3 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects three arguments", call.Name))
	}
	valueCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	lengthCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	padCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[2], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(valueCell) || directBitmapNullCell(lengthCell) || directBitmapNullCell(padCell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	length, ok := directBitmapMaterializedIntArgument(lengthCell)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires integer length", call.Name))
	}
	if length < 0 {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	valueRunes := []rune(fmt.Sprint(valueCell.Value))
	if length <= len(valueRunes) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: string(valueRunes[:length])}, nil
	}
	padRunes := []rune(fmt.Sprint(padCell.Value))
	if len(padRunes) == 0 {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	padding := directBitmapPadString(padRunes, length-len(valueRunes))
	if strings.EqualFold(call.Name, "lpad") {
		return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: padding + string(valueRunes)}, nil
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: string(valueRunes) + padding}, nil
}

func directBitmapPadString(padRunes []rune, length int) string {
	if length <= 0 {
		return ""
	}
	output := make([]rune, 0, length)
	for len(output) < length {
		remaining := length - len(output)
		if remaining >= len(padRunes) {
			output = append(output, padRunes...)
			continue
		}
		output = append(output, padRunes[:remaining]...)
	}
	return string(output)
}

func directBitmapEvaluateMaterializedASCIICall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument", call.Name))
	}
	valueCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(valueCell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	value := fmt.Sprint(valueCell.Value)
	if value == "" {
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(0)}, nil
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(value[0])}, nil
}

func directBitmapEvaluateMaterializedLocateCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	name := strings.ToLower(call.Name)
	if name == "instr" && len(call.Args) != 2 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects two arguments", call.Name))
	}
	if name == "locate" && len(call.Args) != 2 && len(call.Args) != 3 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects two or three arguments", call.Name))
	}
	first, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	second, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(first) || directBitmapNullCell(second) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	needle := fmt.Sprint(first.Value)
	haystack := fmt.Sprint(second.Value)
	start := 1
	if name == "instr" {
		haystack = fmt.Sprint(first.Value)
		needle = fmt.Sprint(second.Value)
	}
	if name == "locate" && len(call.Args) == 3 {
		startCell, startDiagnostics := directBitmapEvaluateMaterializedExpr(call.Args[2], materialized, index)
		if startDiagnostics.BlocksNative() {
			return qsbridge.ResultCell{}, startDiagnostics
		}
		if directBitmapNullCell(startCell) {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		var ok bool
		start, ok = directBitmapMaterializedIntArgument(startCell)
		if !ok {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires integer start", call.Name))
		}
	}
	position := locateSubstringPosition(haystack, needle, start)
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(position)}, nil
}

func locateSubstringPosition(haystack, needle string, start int) int {
	if start <= 0 {
		return 0
	}
	haystackRunes := []rune(haystack)
	if start > len(haystackRunes)+1 {
		return 0
	}
	if needle == "" {
		return start
	}
	segment := string(haystackRunes[start-1:])
	byteIndex := strings.Index(segment, needle)
	if byteIndex < 0 {
		return 0
	}
	return start + utf8.RuneCountInString(segment[:byteIndex])
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

func directBitmapEvaluateMaterializedDateFormatCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 2 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects two arguments", call.Name))
	}
	valueCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	formatCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(valueCell) || directBitmapNullCell(formatCell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	value, ok := directBitmapTimeCellValue(valueCell)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires a time value", call.Name))
	}
	formatted := directBitmapMySQLDateFormat(value.UTC(), fmt.Sprint(formatCell.Value))
	return qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: formatted}, nil
}

func directBitmapMySQLDateFormat(value time.Time, format string) string {
	var builder strings.Builder
	for i := 0; i < len(format); i++ {
		if format[i] != '%' || i == len(format)-1 {
			builder.WriteByte(format[i])
			continue
		}
		i++
		switch format[i] {
		case '%':
			builder.WriteByte('%')
		case 'Y':
			builder.WriteString(fmt.Sprintf("%04d", value.Year()))
		case 'y':
			builder.WriteString(fmt.Sprintf("%02d", value.Year()%100))
		case 'm':
			builder.WriteString(fmt.Sprintf("%02d", int(value.Month())))
		case 'c':
			builder.WriteString(fmt.Sprint(int(value.Month())))
		case 'M':
			builder.WriteString(value.Month().String())
		case 'b':
			builder.WriteString(value.Month().String()[:3])
		case 'd':
			builder.WriteString(fmt.Sprintf("%02d", value.Day()))
		case 'e':
			builder.WriteString(fmt.Sprint(value.Day()))
		case 'D':
			builder.WriteString(fmt.Sprintf("%d%s", value.Day(), directBitmapOrdinalSuffix(value.Day())))
		case 'H':
			builder.WriteString(fmt.Sprintf("%02d", value.Hour()))
		case 'k':
			builder.WriteString(fmt.Sprint(value.Hour()))
		case 'h', 'I':
			hour := value.Hour() % 12
			if hour == 0 {
				hour = 12
			}
			builder.WriteString(fmt.Sprintf("%02d", hour))
		case 'l':
			hour := value.Hour() % 12
			if hour == 0 {
				hour = 12
			}
			builder.WriteString(fmt.Sprint(hour))
		case 'i':
			builder.WriteString(fmt.Sprintf("%02d", value.Minute()))
		case 's', 'S':
			builder.WriteString(fmt.Sprintf("%02d", value.Second()))
		case 'f':
			builder.WriteString(fmt.Sprintf("%06d", value.Nanosecond()/1000))
		case 'p':
			if value.Hour() < 12 {
				builder.WriteString("AM")
			} else {
				builder.WriteString("PM")
			}
		case 'T':
			builder.WriteString(value.Format("15:04:05"))
		case 'r':
			builder.WriteString(value.Format("03:04:05 PM"))
		case 'W':
			builder.WriteString(value.Weekday().String())
		case 'a':
			builder.WriteString(value.Weekday().String()[:3])
		case 'w':
			builder.WriteString(fmt.Sprint(int(value.Weekday())))
		case 'j':
			builder.WriteString(fmt.Sprintf("%03d", value.YearDay()))
		default:
			builder.WriteByte(format[i])
		}
	}
	return builder.String()
}

func directBitmapOrdinalSuffix(day int) string {
	if day%100 >= 11 && day%100 <= 13 {
		return "th"
	}
	switch day % 10 {
	case 1:
		return "st"
	case 2:
		return "nd"
	case 3:
		return "rd"
	default:
		return "th"
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

func directBitmapEvaluateMaterializedCoalesceCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) == 0 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects at least one argument", call.Name))
	}
	for _, arg := range call.Args {
		cell, diagnostics := directBitmapEvaluateMaterializedExpr(arg, materialized, index)
		if diagnostics.BlocksNative() {
			return qsbridge.ResultCell{}, diagnostics
		}
		if !directBitmapNullCell(cell) {
			return cell, nil
		}
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
}

func directBitmapEvaluateMaterializedIfNullCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 2 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects two arguments", call.Name))
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if !directBitmapNullCell(cell) {
		return cell, nil
	}
	return directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
}

func directBitmapEvaluateMaterializedNullIfCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 2 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects two arguments", call.Name))
	}
	left, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	right, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(left) || directBitmapNullCell(right) {
		return left, nil
	}
	if directBitmapCellEqual(left, right) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	return left, nil
}

func directBitmapEvaluateMaterializedIsNullCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument", call.Name))
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(cell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(1)}, nil
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(0)}, nil
}

func directBitmapEvaluateMaterializedIfCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 3 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects three arguments", call.Name))
	}
	matched, diagnostics := directBitmapEvaluateMaterializedConditionExpr(call.Args[0], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if matched {
		return directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
	}
	return directBitmapEvaluateMaterializedExpr(call.Args[2], materialized, index)
}

func directBitmapEvaluateMaterializedConditionExpr(expr qsbridge.Expr, materialized qsbridge.QuantaProjectedRowSet, index int) (bool, qsbridge.DiagnosticSet) {
	if binary, ok := directBitmapBinaryExpr(expr); ok && directBitmapConditionBinaryOp(binary.Op) {
		return directBitmapEvaluateResidualBoolExpr(binary, materialized, index)
	}
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(expr, materialized, index)
	if diagnostics.BlocksNative() {
		return false, diagnostics
	}
	return directBitmapCellTruthy(cell), nil
}

func directBitmapConditionBinaryOp(op qsbridge.BinaryOp) bool {
	switch op {
	case qsbridge.BinaryOpAnd, qsbridge.BinaryOpOr,
		qsbridge.BinaryOpEqual, qsbridge.BinaryOpNotEqual,
		qsbridge.BinaryOpLess, qsbridge.BinaryOpLessEqual,
		qsbridge.BinaryOpGreater, qsbridge.BinaryOpGreaterEqual,
		qsbridge.BinaryOpLike, qsbridge.BinaryOpNotLike,
		qsbridge.BinaryOpIn, qsbridge.BinaryOpNotIn,
		qsbridge.BinaryOpBetween, qsbridge.BinaryOpNotBetween:
		return true
	default:
		return false
	}
}

func directBitmapCellTruthy(cell qsbridge.ResultCell) bool {
	if directBitmapNullCell(cell) {
		return false
	}
	if cell.Kind == qsbridge.ValueBool {
		value, ok := cell.Value.(bool)
		return ok && value
	}
	number, ok := directBitmapNumericCellValue(cell)
	return ok && number != 0
}

func directBitmapEvaluateMaterializedGreatestLeastCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) == 0 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects at least one argument", call.Name))
	}
	cells := make([]qsbridge.ResultCell, 0, len(call.Args))
	numbers := make([]float64, 0, len(call.Args))
	numeric := true
	for _, arg := range call.Args {
		cell, diagnostics := directBitmapEvaluateMaterializedExpr(arg, materialized, index)
		if diagnostics.BlocksNative() {
			return qsbridge.ResultCell{}, diagnostics
		}
		if directBitmapNullCell(cell) {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		cells = append(cells, cell)
		number, ok := directBitmapNumericCellValue(cell)
		if !ok {
			numeric = false
		}
		numbers = append(numbers, number)
	}
	best := 0
	least := strings.EqualFold(call.Name, "least")
	for i := 1; i < len(cells); i++ {
		var better bool
		if numeric {
			better = numbers[i] > numbers[best]
			if least {
				better = numbers[i] < numbers[best]
			}
		} else {
			cmp := strings.Compare(fmt.Sprint(cells[i].Value), fmt.Sprint(cells[best].Value))
			better = cmp > 0
			if least {
				better = cmp < 0
			}
		}
		if better {
			best = i
		}
	}
	return cells[best], nil
}

func directBitmapEvaluateMaterializedAbsCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
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
	return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: math.Abs(number)}, nil
}

func directBitmapEvaluateMaterializedRoundCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 && len(call.Args) != 2 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one or two arguments", call.Name))
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
	scale := 0
	if len(call.Args) == 2 {
		scaleCell, scaleDiagnostics := directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
		if scaleDiagnostics.BlocksNative() {
			return qsbridge.ResultCell{}, scaleDiagnostics
		}
		scale, ok = directBitmapMaterializedIntArgument(scaleCell)
		if !ok {
			return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires integer scale", call.Name))
		}
	}
	var rounded float64
	if scale >= 0 {
		factor := math.Pow10(scale)
		rounded = math.Round(number*factor) / factor
	} else {
		factor := math.Pow10(-scale)
		rounded = math.Round(number/factor) * factor
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: rounded}, nil
}

func directBitmapEvaluateMaterializedUnaryNumericCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int, apply func(float64) float64) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument", call.Name))
	}
	number, null, diagnostics := directBitmapMaterializedNumericArgument(call, materialized, index, 0)
	if diagnostics.BlocksNative() || null {
		if null {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{}, diagnostics
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: apply(number)}, nil
}

func directBitmapEvaluateMaterializedTruncateCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 2 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects two arguments", call.Name))
	}
	number, null, diagnostics := directBitmapMaterializedNumericArgument(call, materialized, index, 0)
	if diagnostics.BlocksNative() || null {
		if null {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{}, diagnostics
	}
	scaleCell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[1], materialized, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if directBitmapNullCell(scaleCell) {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	scale, ok := directBitmapMaterializedIntArgument(scaleCell)
	if !ok {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires integer scale", call.Name))
	}
	factor := math.Pow10(absInt(scale))
	var truncated float64
	if scale >= 0 {
		truncated = math.Trunc(number*factor) / factor
	} else {
		truncated = math.Trunc(number/factor) * factor
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: truncated}, nil
}

func directBitmapEvaluateMaterializedModCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 2 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects two arguments", call.Name))
	}
	left, null, diagnostics := directBitmapMaterializedNumericArgument(call, materialized, index, 0)
	if diagnostics.BlocksNative() || null {
		if null {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{}, diagnostics
	}
	right, null, diagnostics := directBitmapMaterializedNumericArgument(call, materialized, index, 1)
	if diagnostics.BlocksNative() || null {
		if null {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{}, diagnostics
	}
	if right == 0 {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: math.Mod(left, right)}, nil
}

func directBitmapEvaluateMaterializedPowCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 2 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects two arguments", call.Name))
	}
	left, null, diagnostics := directBitmapMaterializedNumericArgument(call, materialized, index, 0)
	if diagnostics.BlocksNative() || null {
		if null {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{}, diagnostics
	}
	right, null, diagnostics := directBitmapMaterializedNumericArgument(call, materialized, index, 1)
	if diagnostics.BlocksNative() || null {
		if null {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{}, diagnostics
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: math.Pow(left, right)}, nil
}

func directBitmapEvaluateMaterializedSignCall(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if len(call.Args) != 1 {
		return qsbridge.ResultCell{}, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q expects one argument", call.Name))
	}
	number, null, diagnostics := directBitmapMaterializedNumericArgument(call, materialized, index, 0)
	if diagnostics.BlocksNative() || null {
		if null {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}, nil
		}
		return qsbridge.ResultCell{}, diagnostics
	}
	var sign int64
	switch {
	case number > 0:
		sign = 1
	case number < 0:
		sign = -1
	default:
		sign = 0
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: sign}, nil
}

func directBitmapMaterializedNumericArgument(call qsbridge.CallExpr, materialized qsbridge.QuantaProjectedRowSet, index int, argIndex int) (float64, bool, qsbridge.DiagnosticSet) {
	cell, diagnostics := directBitmapEvaluateMaterializedExpr(call.Args[argIndex], materialized, index)
	if diagnostics.BlocksNative() {
		return 0, false, diagnostics
	}
	if directBitmapNullCell(cell) {
		return 0, true, nil
	}
	number, ok := directBitmapNumericCellValue(cell)
	if !ok {
		return 0, false, directBitmapAggregateDiagnostics(fmt.Sprintf("materialized scalar function %q requires numeric argument %d", call.Name, argIndex+1))
	}
	return number, false, nil
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
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

func directBitmapProjectedExpressionValues(rowSet qsbridge.QuantaProjectedRowSet, expr qsbridge.Expr) ([]qsbridge.ResultCell, bool) {
	groupExpression, ok := physicalGroupProjectionExpressionForExpr(rowSet.Index, expr)
	if !ok {
		return nil, false
	}
	outputKey := groupExpression.OutputKey
	for _, vector := range rowSet.ProjectionVectors {
		if materializationProjectionFieldStorageKey(vector.Field) != outputKey {
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

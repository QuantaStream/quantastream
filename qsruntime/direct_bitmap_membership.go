package qsruntime

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// directBitmapMembershipMaxDynamicBatchEQValues caps dynamic sibling-domain
// narrowing so a large left domain does not turn into an oversized lookup list.
const directBitmapMembershipMaxDynamicBatchEQValues = 4096

func (r DirectBitmapRuntime) directBitmapApplyMemberships(ctx context.Context, request ExecutionRequest, result BitmapQueryResult, rootSeedResult BitmapQueryResult) (BitmapQueryResult, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	if len(request.Memberships) == 0 {
		return result, nil, nil, nil
	}
	if len(request.Joins) > 0 {
		return result, nil, directBitmapMembershipDiagnostics("direct bitmap runtime only supports membership filters for single-table execution in this slice"), nil
	}
	if r.Sessions == nil {
		return result, nil, directBitmapMembershipDiagnostics("direct bitmap runtime has no session provider for membership filters"), nil
	}
	if r.projectionMaterializationKernel() == nil {
		return result, nil, directBitmapMembershipDiagnostics("direct bitmap runtime has no materialization kernel for membership filters"), nil
	}
	rootIndex, ok := request.RootIndex()
	if !ok {
		return result, nil, directBitmapMembershipDiagnostics("direct bitmap runtime cannot apply membership filters without a root index"), nil
	}
	filtered := result.Clone()
	probes := make([]ExecutionProbe, 0, len(request.Memberships)*8)
	for _, membership := range request.Memberships {
		if !strings.EqualFold(membership.Left.Table.Table, rootIndex) {
			return result, probes, directBitmapMembershipDiagnostics("direct bitmap runtime only supports membership left side on the root table"), nil
		}
		filteredResult, membershipProbes, diagnostics, err := r.directBitmapApplyMembership(ctx, request, filtered, membership, rootSeedResult)
		probes = append(probes, membershipProbes...)
		if err != nil || diagnostics.BlocksNative() {
			return result, probes, diagnostics, err
		}
		filtered = filteredResult
	}
	return filtered, probes, nil, nil
}

func (r DirectBitmapRuntime) directBitmapApplyMembership(ctx context.Context, request ExecutionRequest, result BitmapQueryResult, membership qsbridge.MembershipEdge, rootSeedResult BitmapQueryResult) (BitmapQueryResult, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	if directBitmapMembershipHasCorrelatedPredicates(membership) {
		return r.directBitmapApplyCorrelatedSiblingMembership(ctx, request, result, membership, rootSeedResult)
	}
	if filtered, handled, diagnostics, err := r.directBitmapApplyRelationshipMembership(ctx, result, membership); handled || err != nil || diagnostics.BlocksNative() {
		return filtered, nil, diagnostics, err
	}
	rightValues, diagnostics, err := r.directBitmapMembershipRightValues(ctx, membership)
	if err != nil || diagnostics.BlocksNative() {
		return result, nil, diagnostics, err
	}
	leftRowSet, diagnostics, err := r.directBitmapMembershipLeftValues(ctx, result, membership.Left)
	if err != nil || diagnostics.BlocksNative() {
		return result, nil, diagnostics, err
	}
	leftValues, ok := directBitmapProjectedValues(leftRowSet, membership.Left)
	if !ok {
		return result, nil, directBitmapMembershipDiagnostics("membership left field is not present in materialized row set"), nil
	}
	if len(leftValues) != len(leftRowSet.Rownums) {
		return result, nil, directBitmapMembershipDiagnostics("membership left field value count does not match candidate row count"), nil
	}

	filtered := result.Clone()
	filtered.Rownums = filtered.Rownums[:0]
	for i, rownum := range leftRowSet.Rownums {
		_, matched := rightValues[directBitmapGroupKey(leftValues[i])]
		keep := matched
		if membership.Kind == qsbridge.MembershipAnti {
			keep = !matched
		}
		if keep {
			filtered.Rownums = append(filtered.Rownums, rownum)
		}
	}
	filtered.Count = uint64(len(filtered.Rownums))
	return filtered, nil, nil, nil
}

func (r DirectBitmapRuntime) directBitmapApplyCorrelatedSiblingMembership(ctx context.Context, request ExecutionRequest, result BitmapQueryResult, membership qsbridge.MembershipEdge, rootSeedResult BitmapQueryResult) (BitmapQueryResult, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	start := time.Now()
	detail := directBitmapMembershipNarrowProbeDetail(membership)
	probes := []ExecutionProbe{
		directBitmapMembershipProbe("correlated_sibling_left_candidates_before", strconv.Itoa(len(result.Rownums)), detail),
	}
	if !strings.EqualFold(membership.Left.Table.Table, membership.Right.Table.Table) {
		return result, nil, directBitmapMembershipDiagnostics("correlated sibling membership only supports repeated aliases of the same physical table in this slice"), nil
	}
	rightOnlyPredicates, correlatedPredicates := directBitmapSplitMembershipPredicates(membership)
	if len(correlatedPredicates) == 0 {
		return result, nil, directBitmapMembershipDiagnostics("correlated sibling membership requires a cross-domain predicate"), nil
	}
	if filtered, fastProbes, handled, diagnostics, err := r.directBitmapApplyCorrelatedSiblingMembershipBSIFastPath(ctx, request, result, membership, rightOnlyPredicates, correlatedPredicates, rootSeedResult); handled || err != nil || diagnostics.BlocksNative() {
		probes = append(probes, fastProbes...)
		return filtered, probes, diagnostics, err
	}
	leftFields := directBitmapCorrelatedMembershipProjectionFields(membership.Left, correlatedPredicates, membership.Left.Table)
	rightFields := directBitmapCorrelatedMembershipProjectionFields(membership.Right, correlatedPredicates, membership.Right.Table)
	leftMaterializationStart := time.Now()
	leftRowSet, diagnostics, leftMaterializationProbes, err := directBitmapMaterializeWithKernel(ctx, r.projectionMaterializationKernel(), qsbridge.QuantaMaterializationRequest{
		Index:            membership.Left.Table.Table,
		Rownums:          append([]qsbridge.QuantaRownum(nil), result.Rownums...),
		ProjectionFields: leftFields,
	})
	leftMaterializationElapsed := time.Since(leftMaterializationStart)
	probes = append(probes, leftMaterializationProbes...)
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_left_materialization_elapsed", leftMaterializationElapsed.String(), detail),
		directBitmapMembershipProbe("correlated_sibling_left_materialization_rows", strconv.Itoa(leftRowSet.CandidateCount()), detail),
		directBitmapMembershipProbe("correlated_sibling_left_materialization_fields", strconv.Itoa(len(leftFields)), detail),
	)
	if err != nil || diagnostics.BlocksNative() {
		return result, probes, diagnostics, err
	}
	leftKeys, ok := directBitmapProjectedValues(leftRowSet, membership.Left)
	if !ok {
		return result, probes, directBitmapMembershipDiagnostics("correlated sibling membership left key is not present in materialized row set"), nil
	}

	rightMembership := membership
	rightMembership.Predicates = rightOnlyPredicates
	narrowValues, narrowValuesOK := directBitmapMembershipBatchEQValues(leftKeys)
	narrowFragment, narrowPlanProbes, foldedNarrow := directBitmapCorrelatedMembershipRightKeyNarrowFragment(membership, leftKeys)
	probes = append(probes, narrowPlanProbes...)
	rightCandidateStart := time.Now()
	var rightResult BitmapQueryResult
	var candidateProbes []ExecutionProbe
	rightCandidateSeedApplied := false
	if seeded, seedProbes, ok := r.directBitmapCorrelatedSiblingRightCandidateSeedResult(detail); ok {
		rightResult = seeded
		candidateProbes = seedProbes
		rightCandidateSeedApplied = true
	} else if foldedNarrow && narrowValuesOK {
		rightResult, candidateProbes, diagnostics, err = r.directBitmapMembershipRightCandidateResultWithCacheAndProbes(ctx, rightMembership, narrowFragment, narrowValues, detail)
	} else {
		rightResult, candidateProbes, diagnostics, err = r.directBitmapMembershipRightCandidateResultWithExtraFragmentsAndProbes(ctx, rightMembership, narrowFragment)
	}
	rightCandidateElapsed := time.Since(rightCandidateStart)
	probes = append(probes, candidateProbes...)
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_right_candidate_elapsed", rightCandidateElapsed.String(), detail),
		directBitmapMembershipProbe("correlated_sibling_right_candidates", strconv.Itoa(len(rightResult.Rownums)), detail),
	)
	if foldedNarrow && !rightCandidateSeedApplied {
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_right_narrow_right_candidates_after", strconv.Itoa(len(rightResult.Rownums)), detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_applied", "true", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_mode", "folded_right_request", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_elapsed", rightCandidateElapsed.String(), detail),
		)
	} else if rightCandidateSeedApplied {
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_right_narrow_applied", "false", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_reason", "right_candidate_seed", detail),
		)
	}
	if err != nil || diagnostics.BlocksNative() {
		return result, probes, diagnostics, err
	}
	if !foldedNarrow && !rightCandidateSeedApplied {
		var narrowProbes []ExecutionProbe
		rightResult, narrowProbes, diagnostics, err = r.directBitmapNarrowCorrelatedMembershipRightCandidates(ctx, rightResult, membership, leftKeys)
		probes = append(probes, narrowProbes...)
		if err != nil || diagnostics.BlocksNative() {
			return result, probes, diagnostics, err
		}
	}
	rightMaterializationStart := time.Now()
	rightRowSet, diagnostics, rightMaterializationProbes, err := directBitmapMaterializeWithKernel(ctx, r.projectionMaterializationKernel(), qsbridge.QuantaMaterializationRequest{
		Index:            membership.Right.Table.Table,
		Rownums:          append([]qsbridge.QuantaRownum(nil), rightResult.Rownums...),
		ProjectionFields: rightFields,
	})
	rightMaterializationElapsed := time.Since(rightMaterializationStart)
	probes = append(probes, rightMaterializationProbes...)
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_right_materialization_elapsed", rightMaterializationElapsed.String(), detail),
		directBitmapMembershipProbe("correlated_sibling_right_materialization_rows", strconv.Itoa(rightRowSet.CandidateCount()), detail),
		directBitmapMembershipProbe("correlated_sibling_right_materialization_fields", strconv.Itoa(len(rightFields)), detail),
	)
	if err != nil || diagnostics.BlocksNative() {
		return result, probes, diagnostics, err
	}
	rightKeys, ok := directBitmapProjectedValues(rightRowSet, membership.Right)
	if !ok {
		return result, probes, directBitmapMembershipDiagnostics("correlated sibling membership right key is not present in materialized row set"), nil
	}
	if !rightCandidateSeedApplied && narrowValuesOK && directBitmapMembershipRightCandidateCanStoreRaw(rightOnlyPredicates) {
		probes = append(probes, directBitmapMembershipRightCandidateCellCacheStore(ctx, rightMembership, narrowValues, rightRowSet.Rownums, rightKeys, detail)...)
	}
	indexStart := time.Now()
	rightRowsByKey := make(map[string][]int, len(rightKeys))
	rightIndexedRows := 0
	rightNullKeyRows := 0
	for i, value := range rightKeys {
		if value.Kind == qsbridge.ValueNull || value.Value == nil {
			rightNullKeyRows++
			continue
		}
		key := directBitmapGroupKey(value)
		rightRowsByKey[key] = append(rightRowsByKey[key], i)
		rightIndexedRows++
	}
	rightMaxBucketSize := 0
	rightMultirowKeys := 0
	rightMultirowRows := 0
	for _, rows := range rightRowsByKey {
		if len(rows) > rightMaxBucketSize {
			rightMaxBucketSize = len(rows)
		}
		if len(rows) > 1 {
			rightMultirowKeys++
			rightMultirowRows += len(rows)
		}
	}
	indexElapsed := time.Since(indexStart)
	filtered := result.Clone()
	filtered.Rownums = filtered.Rownums[:0]
	evaluationStart := time.Now()
	comparisons := 0
	leftNullKeyRows := 0
	leftBucketHits := 0
	leftBucketMisses := 0
	leftSingleCandidateBuckets := 0
	leftMultiCandidateBuckets := 0
	matchedRows := 0
	unmatchedRows := 0
	firstCandidateMatches := 0
	laterCandidateMatches := 0
	for i, rownum := range leftRowSet.Rownums {
		key := ""
		if i < len(leftKeys) && leftKeys[i].Kind != qsbridge.ValueNull && leftKeys[i].Value != nil {
			key = directBitmapGroupKey(leftKeys[i])
		} else {
			leftNullKeyRows++
		}
		candidates := rightRowsByKey[key]
		if len(candidates) == 0 {
			leftBucketMisses++
		} else {
			leftBucketHits++
			if len(candidates) == 1 {
				leftSingleCandidateBuckets++
			} else {
				leftMultiCandidateBuckets++
			}
		}
		matched := false
		for candidateIndex, rightIndex := range candidates {
			comparisons++
			ok, diagnostics := directBitmapEvaluateCorrelatedMembershipPredicates(correlatedPredicates, leftRowSet, i, rightRowSet, rightIndex, membership)
			if diagnostics.BlocksNative() {
				return result, probes, diagnostics, nil
			}
			if ok {
				matched = true
				if candidateIndex == 0 {
					firstCandidateMatches++
				} else {
					laterCandidateMatches++
				}
				break
			}
		}
		if matched {
			matchedRows++
		} else {
			unmatchedRows++
		}
		keep := matched
		if membership.Kind == qsbridge.MembershipAnti {
			keep = !matched
		}
		if keep {
			filtered.Rownums = append(filtered.Rownums, rownum)
		}
	}
	evaluationElapsed := time.Since(evaluationStart)
	filtered.Count = uint64(len(filtered.Rownums))
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_right_index_elapsed", indexElapsed.String(), detail),
		directBitmapMembershipProbe("correlated_sibling_right_index_keys", strconv.Itoa(len(rightRowsByKey)), detail),
		directBitmapMembershipProbe("correlated_sibling_right_index_rows", strconv.Itoa(rightIndexedRows), detail),
		directBitmapMembershipProbe("correlated_sibling_right_index_null_rows", strconv.Itoa(rightNullKeyRows), detail),
		directBitmapMembershipProbe("correlated_sibling_right_index_max_bucket_size", strconv.Itoa(rightMaxBucketSize), detail),
		directBitmapMembershipProbe("correlated_sibling_right_index_multirow_keys", strconv.Itoa(rightMultirowKeys), detail),
		directBitmapMembershipProbe("correlated_sibling_right_index_multirow_rows", strconv.Itoa(rightMultirowRows), detail),
		directBitmapMembershipProbe("correlated_sibling_evaluation_elapsed", evaluationElapsed.String(), detail),
		directBitmapMembershipProbe("correlated_sibling_evaluation_comparisons", strconv.Itoa(comparisons), detail),
		directBitmapMembershipProbe("correlated_sibling_evaluation_left_null_key_rows", strconv.Itoa(leftNullKeyRows), detail),
		directBitmapMembershipProbe("correlated_sibling_evaluation_bucket_hits", strconv.Itoa(leftBucketHits), detail),
		directBitmapMembershipProbe("correlated_sibling_evaluation_bucket_misses", strconv.Itoa(leftBucketMisses), detail),
		directBitmapMembershipProbe("correlated_sibling_evaluation_single_candidate_buckets", strconv.Itoa(leftSingleCandidateBuckets), detail),
		directBitmapMembershipProbe("correlated_sibling_evaluation_multi_candidate_buckets", strconv.Itoa(leftMultiCandidateBuckets), detail),
		directBitmapMembershipProbe("correlated_sibling_evaluation_matched_rows", strconv.Itoa(matchedRows), detail),
		directBitmapMembershipProbe("correlated_sibling_evaluation_unmatched_rows", strconv.Itoa(unmatchedRows), detail),
		directBitmapMembershipProbe("correlated_sibling_evaluation_first_candidate_matches", strconv.Itoa(firstCandidateMatches), detail),
		directBitmapMembershipProbe("correlated_sibling_evaluation_later_candidate_matches", strconv.Itoa(laterCandidateMatches), detail),
		directBitmapMembershipProbe("correlated_sibling_left_candidates_after", strconv.Itoa(len(filtered.Rownums)), detail),
		directBitmapMembershipProbe("correlated_sibling_elapsed", time.Since(start).String(), detail),
	)
	return filtered, probes, nil, nil
}

func (r DirectBitmapRuntime) directBitmapCorrelatedSiblingRightCandidateSeedResult(detail string) (BitmapQueryResult, []ExecutionProbe, bool) {
	if r.CorrelatedSiblingRightCandidateSeed == nil || !r.CorrelatedSiblingRightCandidateSeed.Success {
		return BitmapQueryResult{}, nil, false
	}
	mode := r.CorrelatedSiblingRightCandidateSeedMode
	if mode == "" {
		mode = "provided"
	}
	seeded := r.CorrelatedSiblingRightCandidateSeed.Clone()
	probes := []ExecutionProbe{
		directBitmapMembershipProbe("membership_right_candidate_seed_reuse", "true", detail),
		directBitmapMembershipProbe("membership_right_candidate_seed_mode", mode, detail),
		directBitmapMembershipProbe("membership_right_candidate_fragment_count", "0", detail),
		directBitmapMembershipProbe("membership_right_candidate_extra_fragment_count", "0", detail),
		directBitmapMembershipProbe("membership_right_candidate_query_elapsed", "0s", detail),
		directBitmapMembershipProbe("membership_right_candidate_query_rows", strconv.Itoa(len(seeded.Rownums)), detail),
		directBitmapMembershipProbe("membership_right_candidate_residual_count", "0", detail),
	}
	return seeded, probes, true
}

type directBitmapMembershipBSIOperandSide string

const (
	directBitmapMembershipBSIOperandLeft  directBitmapMembershipBSIOperandSide = "left"
	directBitmapMembershipBSIOperandRight directBitmapMembershipBSIOperandSide = "right"
)

type directBitmapMembershipBSIOperand struct {
	Side  directBitmapMembershipBSIOperandSide
	Field qsbridge.FieldRef
}

type directBitmapMembershipBSIComparison struct {
	Op    qsbridge.BinaryOp
	Left  directBitmapMembershipBSIOperand
	Right directBitmapMembershipBSIOperand
}

type directBitmapMembershipBSIVector struct {
	Field   qsbridge.FieldRef
	Rownums []qsbridge.QuantaRownum
	Values  []*big.Int
}

func (r DirectBitmapRuntime) directBitmapApplyCorrelatedSiblingMembershipBSIFastPath(ctx context.Context, request ExecutionRequest, result BitmapQueryResult, membership qsbridge.MembershipEdge, rightOnlyPredicates []qsbridge.Predicate, correlatedPredicates []qsbridge.Predicate, rootSeedResult BitmapQueryResult) (BitmapQueryResult, []ExecutionProbe, bool, qsbridge.DiagnosticSet, error) {
	if r.ProjectionBSIReader == nil {
		return result, nil, false, nil, nil
	}
	comparisons, ok := directBitmapCorrelatedMembershipBSIComparisons(membership, correlatedPredicates)
	if !ok {
		return result, nil, false, nil, nil
	}

	start := time.Now()
	detail := directBitmapMembershipNarrowProbeDetail(membership)
	probes := []ExecutionProbe{
		directBitmapMembershipProbe("correlated_sibling_bsi_fast_path_applied", "true", detail),
	}
	if len(result.Rownums) == 0 {
		filtered := result.Clone()
		filtered.Count = 0
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_bsi_left_candidates_after", "0", detail),
			directBitmapMembershipProbe("correlated_sibling_bsi_elapsed", time.Since(start).String(), detail),
		)
		return filtered, probes, true, nil, nil
	}

	leftFields := directBitmapCorrelatedMembershipProjectionFields(membership.Left, correlatedPredicates, membership.Left.Table)
	rightFields := directBitmapCorrelatedMembershipProjectionFields(membership.Right, correlatedPredicates, membership.Right.Table)
	if directBitmapCorrelatedMembershipShouldReuseRightBSIVectors(result.Rownums, rightOnlyPredicates, leftFields, rightFields) {
		filtered, reuseProbes, diagnostics, err := r.directBitmapApplyCorrelatedSiblingMembershipBSIRightVectorReuse(ctx, request, start, result, membership, rightOnlyPredicates, correlatedPredicates, comparisons, leftFields, rightFields, rootSeedResult, detail)
		probes = append(probes, reuseProbes...)
		return filtered, probes, true, diagnostics, err
	}
	leftReadStart := time.Now()
	leftVectors, leftReadProbes, diagnostics, err := r.directBitmapReadMembershipBSIVectors(ctx, membership.Left.Table.Table, result.Rownums, leftFields)
	leftReadElapsed := time.Since(leftReadStart)
	probes = append(probes, leftReadProbes...)
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_bsi_left_read_elapsed", leftReadElapsed.String(), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_left_read_rows", strconv.Itoa(len(result.Rownums)), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_left_read_fields", strconv.Itoa(len(leftFields)), detail),
	)
	if err != nil || diagnostics.BlocksNative() {
		return result, probes, true, diagnostics, err
	}
	leftKey, ok := leftVectors[directBitmapMembershipBSIFieldKey(membership.Left)]
	if !ok {
		return result, probes, true, directBitmapMembershipDiagnostics("correlated sibling BSI fast path left key vector is missing"), nil
	}

	rightMembership := membership
	rightMembership.Predicates = rightOnlyPredicates
	narrowValues := directBitmapMembershipBSIUniqueValues(leftKey.Values)
	narrowFragment, narrowPlanProbes, foldedNarrow := directBitmapCorrelatedMembershipRightKeyNarrowFragmentValues(membership, narrowValues)
	probes = append(probes, narrowPlanProbes...)
	rightCandidateStart := time.Now()
	rightResult, candidateProbes, diagnostics, err := r.directBitmapMembershipRightCandidateResultWithCacheAndProbes(ctx, rightMembership, narrowFragment, narrowValues, detail)
	rightCandidateElapsed := time.Since(rightCandidateStart)
	probes = append(probes, candidateProbes...)
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_right_candidate_elapsed", rightCandidateElapsed.String(), detail),
		directBitmapMembershipProbe("correlated_sibling_right_candidates", strconv.Itoa(len(rightResult.Rownums)), detail),
	)
	if foldedNarrow {
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_right_narrow_right_candidates_after", strconv.Itoa(len(rightResult.Rownums)), detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_applied", "true", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_mode", "folded_right_request", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_elapsed", rightCandidateElapsed.String(), detail),
		)
	}
	if err != nil || diagnostics.BlocksNative() {
		return result, probes, true, diagnostics, err
	}

	rightReadStart := time.Now()
	rightVectors, rightReadProbes, diagnostics, err := r.directBitmapReadMembershipBSIVectors(ctx, membership.Right.Table.Table, rightResult.Rownums, rightFields)
	rightReadElapsed := time.Since(rightReadStart)
	probes = append(probes, rightReadProbes...)
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_bsi_right_read_elapsed", rightReadElapsed.String(), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_right_read_rows", strconv.Itoa(len(rightResult.Rownums)), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_right_read_fields", strconv.Itoa(len(rightFields)), detail),
	)
	if err != nil || diagnostics.BlocksNative() {
		return result, probes, true, diagnostics, err
	}
	rightKey, ok := rightVectors[directBitmapMembershipBSIFieldKey(membership.Right)]
	if !ok {
		return result, probes, true, directBitmapMembershipDiagnostics("correlated sibling BSI fast path right key vector is missing"), nil
	}
	if directBitmapMembershipRightCandidateCanStoreRaw(rightOnlyPredicates) {
		probes = append(probes, directBitmapMembershipRightCandidateCacheStore(ctx, rightMembership, narrowValues, rightKey, detail)...)
	}

	filtered, probes, diagnostics := directBitmapFinishCorrelatedSiblingMembershipBSI(start, result, membership, detail, probes, comparisons, leftVectors, leftKey, rightVectors, rightKey)
	return filtered, probes, true, diagnostics, nil
}

func directBitmapFinishCorrelatedSiblingMembershipBSI(start time.Time, result BitmapQueryResult, membership qsbridge.MembershipEdge, detail string, probes []ExecutionProbe, comparisons []directBitmapMembershipBSIComparison, leftVectors map[string]directBitmapMembershipBSIVector, leftKey directBitmapMembershipBSIVector, rightVectors map[string]directBitmapMembershipBSIVector, rightKey directBitmapMembershipBSIVector) (BitmapQueryResult, []ExecutionProbe, qsbridge.DiagnosticSet) {
	useInt64Key := directBitmapMembershipBSIVectorValuesFitInt64(leftKey.Values) && directBitmapMembershipBSIVectorValuesFitInt64(rightKey.Values)
	keyMode := "string"
	if useInt64Key {
		keyMode = "int64"
	}
	indexStart := time.Now()
	rightRowsByKey := make(map[directBitmapMembershipBSIJoinKey][]int, len(rightKey.Values))
	rightIndexedRows := 0
	rightNullKeyRows := 0
	for i, value := range rightKey.Values {
		key, ok := directBitmapMembershipBSIJoinKeyForValue(value, useInt64Key)
		if !ok {
			rightNullKeyRows++
			continue
		}
		rightRowsByKey[key] = append(rightRowsByKey[key], i)
		rightIndexedRows++
	}
	rightMaxBucketSize := 0
	rightMultirowKeys := 0
	rightMultirowRows := 0
	for _, rows := range rightRowsByKey {
		if len(rows) > rightMaxBucketSize {
			rightMaxBucketSize = len(rows)
		}
		if len(rows) > 1 {
			rightMultirowKeys++
			rightMultirowRows += len(rows)
		}
	}
	indexElapsed := time.Since(indexStart)

	filtered := result.Clone()
	filtered.Rownums = filtered.Rownums[:0]
	evaluationStart := time.Now()
	comparisonsEvaluated := 0
	leftNullKeyRows := 0
	leftBucketHits := 0
	leftBucketMisses := 0
	leftSingleCandidateBuckets := 0
	leftMultiCandidateBuckets := 0
	matchedRows := 0
	unmatchedRows := 0
	firstCandidateMatches := 0
	laterCandidateMatches := 0
	for i, rownum := range leftKey.Rownums {
		value := leftKey.Values[i]
		key, ok := directBitmapMembershipBSIJoinKeyForValue(value, useInt64Key)
		var candidates []int
		if !ok {
			leftNullKeyRows++
		} else {
			candidates = rightRowsByKey[key]
		}
		if len(candidates) == 0 {
			leftBucketMisses++
		} else {
			leftBucketHits++
			if len(candidates) == 1 {
				leftSingleCandidateBuckets++
			} else {
				leftMultiCandidateBuckets++
			}
		}
		matched := false
		for candidateIndex, rightIndex := range candidates {
			comparisonsEvaluated++
			ok, diagnostics := directBitmapEvaluateCorrelatedMembershipBSIComparisons(comparisons, leftVectors, i, rightVectors, rightIndex)
			if diagnostics.BlocksNative() {
				return result, probes, diagnostics
			}
			if ok {
				matched = true
				if candidateIndex == 0 {
					firstCandidateMatches++
				} else {
					laterCandidateMatches++
				}
				break
			}
		}
		if matched {
			matchedRows++
		} else {
			unmatchedRows++
		}
		keep := matched
		if membership.Kind == qsbridge.MembershipAnti {
			keep = !matched
		}
		if keep {
			filtered.Rownums = append(filtered.Rownums, rownum)
		}
	}
	evaluationElapsed := time.Since(evaluationStart)
	filtered.Count = uint64(len(filtered.Rownums))
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_bsi_key_mode", keyMode, detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_right_index_elapsed", indexElapsed.String(), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_right_index_keys", strconv.Itoa(len(rightRowsByKey)), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_right_index_rows", strconv.Itoa(rightIndexedRows), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_right_index_null_rows", strconv.Itoa(rightNullKeyRows), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_right_index_max_bucket_size", strconv.Itoa(rightMaxBucketSize), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_right_index_multirow_keys", strconv.Itoa(rightMultirowKeys), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_right_index_multirow_rows", strconv.Itoa(rightMultirowRows), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_evaluation_elapsed", evaluationElapsed.String(), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_evaluation_comparisons", strconv.Itoa(comparisonsEvaluated), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_evaluation_left_null_key_rows", strconv.Itoa(leftNullKeyRows), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_evaluation_bucket_hits", strconv.Itoa(leftBucketHits), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_evaluation_bucket_misses", strconv.Itoa(leftBucketMisses), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_evaluation_single_candidate_buckets", strconv.Itoa(leftSingleCandidateBuckets), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_evaluation_multi_candidate_buckets", strconv.Itoa(leftMultiCandidateBuckets), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_evaluation_matched_rows", strconv.Itoa(matchedRows), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_evaluation_unmatched_rows", strconv.Itoa(unmatchedRows), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_evaluation_first_candidate_matches", strconv.Itoa(firstCandidateMatches), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_evaluation_later_candidate_matches", strconv.Itoa(laterCandidateMatches), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_left_candidates_after", strconv.Itoa(len(filtered.Rownums)), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_elapsed", time.Since(start).String(), detail),
	)
	return filtered, probes, nil
}

func directBitmapMembershipBSIVectorValuesFitInt64(values []*big.Int) bool {
	for _, value := range values {
		if value != nil && !value.IsInt64() {
			return false
		}
	}
	return true
}

type directBitmapMembershipBSIJoinKey struct {
	int64Value  int64
	stringValue string
}

func directBitmapMembershipBSIJoinKeyForValue(value *big.Int, useInt64 bool) (directBitmapMembershipBSIJoinKey, bool) {
	if value == nil {
		return directBitmapMembershipBSIJoinKey{}, false
	}
	if useInt64 {
		return directBitmapMembershipBSIJoinKey{int64Value: value.Int64()}, true
	}
	return directBitmapMembershipBSIJoinKey{stringValue: value.String()}, true
}

func (r DirectBitmapRuntime) directBitmapApplyCorrelatedSiblingMembershipBSIRightVectorReuse(ctx context.Context, request ExecutionRequest, start time.Time, result BitmapQueryResult, membership qsbridge.MembershipEdge, rightOnlyPredicates []qsbridge.Predicate, correlatedPredicates []qsbridge.Predicate, comparisons []directBitmapMembershipBSIComparison, leftFields []qsbridge.QuantaProjectionField, rightFields []qsbridge.QuantaProjectionField, rootSeedResult BitmapQueryResult, detail string) (BitmapQueryResult, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	probes := []ExecutionProbe{
		directBitmapMembershipProbe("correlated_sibling_bsi_right_vector_reuse", "true", detail),
	}
	rightMembership := membership
	rightMembership.Predicates = rightOnlyPredicates
	rightCandidateStart := time.Now()
	rightResult, candidateProbes, reusedSeed, diagnostics, err := r.directBitmapMembershipRightCandidateResultWithRootSeedReuse(ctx, request, rightMembership, rootSeedResult, detail)
	rightCandidateElapsed := time.Since(rightCandidateStart)
	probes = append(probes, candidateProbes...)
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_right_candidate_elapsed", rightCandidateElapsed.String(), detail),
		directBitmapMembershipProbe("correlated_sibling_right_candidates", strconv.Itoa(len(rightResult.Rownums)), detail),
		directBitmapMembershipProbe("correlated_sibling_right_narrow_applied", "false", detail),
		directBitmapMembershipProbe("correlated_sibling_right_narrow_reason", "right_vector_reuse_large_left_domain", detail),
	)
	if reusedSeed {
		probes = append(probes, directBitmapMembershipProbe("correlated_sibling_right_candidate_source", "root_seed", detail))
	} else {
		probes = append(probes, directBitmapMembershipProbe("correlated_sibling_right_candidate_source", "query", detail))
	}
	if err != nil || diagnostics.BlocksNative() {
		return result, probes, diagnostics, err
	}

	rightReadStart := time.Now()
	rightVectors, rightReadProbes, diagnostics, err := r.directBitmapReadMembershipBSIVectors(ctx, membership.Right.Table.Table, rightResult.Rownums, rightFields)
	rightReadElapsed := time.Since(rightReadStart)
	probes = append(probes, rightReadProbes...)
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_bsi_right_read_elapsed", rightReadElapsed.String(), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_right_read_rows", strconv.Itoa(len(rightResult.Rownums)), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_right_read_fields", strconv.Itoa(len(rightFields)), detail),
	)
	if err != nil || diagnostics.BlocksNative() {
		return result, probes, diagnostics, err
	}
	rightKey, ok := rightVectors[directBitmapMembershipBSIFieldKey(membership.Right)]
	if !ok {
		return result, probes, directBitmapMembershipDiagnostics("correlated sibling BSI fast path right key vector is missing"), nil
	}

	leftDeriveStart := time.Now()
	leftVectors, ok := directBitmapDeriveLeftMembershipBSIVectorsFromRight(result.Rownums, leftFields, rightFields, rightVectors)
	leftDeriveElapsed := time.Since(leftDeriveStart)
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_bsi_left_read_elapsed", leftDeriveElapsed.String(), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_left_read_rows", strconv.Itoa(len(result.Rownums)), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_left_read_fields", strconv.Itoa(len(leftFields)), detail),
		directBitmapMembershipProbe("correlated_sibling_bsi_left_read_mode", "right_vector_reuse", detail),
	)
	if !ok {
		return result, probes, directBitmapMembershipDiagnostics("correlated sibling BSI right-vector reuse could not derive left vectors"), nil
	}
	leftKey, ok := leftVectors[directBitmapMembershipBSIFieldKey(membership.Left)]
	if !ok {
		return result, probes, directBitmapMembershipDiagnostics("correlated sibling BSI fast path left key vector is missing"), nil
	}

	filtered, probes, diagnostics := directBitmapFinishCorrelatedSiblingMembershipBSI(start, result, membership, detail, probes, comparisons, leftVectors, leftKey, rightVectors, rightKey)
	return filtered, probes, diagnostics, nil
}

func (r DirectBitmapRuntime) directBitmapMembershipRightCandidateResultWithRootSeedReuse(ctx context.Context, request ExecutionRequest, membership qsbridge.MembershipEdge, rootSeedResult BitmapQueryResult, detail string) (BitmapQueryResult, []ExecutionProbe, bool, qsbridge.DiagnosticSet, error) {
	if rightResult, ok := directBitmapMembershipRootSeedCandidateResult(request, membership, rootSeedResult); ok {
		probes := []ExecutionProbe{
			directBitmapMembershipProbe("membership_right_candidate_seed_reuse", "true", detail),
			directBitmapMembershipProbe("membership_right_candidate_fragment_count", strconv.Itoa(len(request.Query.Fragments)), detail),
			directBitmapMembershipProbe("membership_right_candidate_extra_fragment_count", "0", detail),
			directBitmapMembershipProbe("membership_right_candidate_query_elapsed", "0s", detail),
			directBitmapMembershipProbe("membership_right_candidate_query_rows", strconv.Itoa(len(rightResult.Rownums)), detail),
			directBitmapMembershipProbe("membership_right_candidate_residual_count", "0", detail),
		}
		return rightResult, probes, true, nil, nil
	}
	rightResult, probes, diagnostics, err := r.directBitmapMembershipRightCandidateResultWithExtraFragmentsAndProbes(ctx, membership, nil)
	probes = append([]ExecutionProbe{directBitmapMembershipProbe("membership_right_candidate_seed_reuse", "false", detail)}, probes...)
	return rightResult, probes, false, diagnostics, err
}

func directBitmapMembershipRootSeedCandidateResult(request ExecutionRequest, membership qsbridge.MembershipEdge, rootSeedResult BitmapQueryResult) (BitmapQueryResult, bool) {
	if request.HasCandidateSet || len(membership.Predicates) != 0 || !rootSeedResult.Success {
		return BitmapQueryResult{}, false
	}
	root, ok := request.RootIndex()
	if !ok ||
		!strings.EqualFold(root, membership.Left.Table.Table) ||
		!strings.EqualFold(root, membership.Right.Table.Table) {
		return BitmapQueryResult{}, false
	}
	if len(request.Query.Fragments) != 1 || len(request.Query.Seeds) != 0 {
		return BitmapQueryResult{}, false
	}
	fragment := request.Query.Fragments[0]
	if !strings.EqualFold(fragment.Index, root) ||
		fragment.Operation != qsbridge.QuantaOperationIntersect ||
		!fragment.NullCheck ||
		!fragment.Negate ||
		fragment.Value != nil ||
		len(fragment.Values) != 0 ||
		fragment.Begin != nil ||
		fragment.End != nil ||
		fragment.HasLiteral ||
		fragment.HasLiteralRange {
		return BitmapQueryResult{}, false
	}
	leftField := directBitmapFieldPhysicalName(membership.Left)
	if leftField == "" {
		leftField = membership.Left.Name
	}
	if !strings.EqualFold(fragment.Field, leftField) {
		return BitmapQueryResult{}, false
	}
	return rootSeedResult.Clone(), true
}

func directBitmapCorrelatedMembershipShouldReuseRightBSIVectors(leftRownums []qsbridge.QuantaRownum, rightOnlyPredicates []qsbridge.Predicate, leftFields []qsbridge.QuantaProjectionField, rightFields []qsbridge.QuantaProjectionField) bool {
	if len(rightOnlyPredicates) != 0 || len(leftRownums) <= directBitmapMembershipMaxDynamicBatchEQValues {
		return false
	}
	for _, leftField := range leftFields {
		if _, ok := directBitmapMatchingRightBSIProjectionField(leftField, rightFields); !ok {
			return false
		}
	}
	return true
}

func directBitmapDeriveLeftMembershipBSIVectorsFromRight(leftRownums []qsbridge.QuantaRownum, leftFields []qsbridge.QuantaProjectionField, rightFields []qsbridge.QuantaProjectionField, rightVectors map[string]directBitmapMembershipBSIVector) (map[string]directBitmapMembershipBSIVector, bool) {
	derived := make(map[string]directBitmapMembershipBSIVector, len(leftFields))
	var rightPositions map[qsbridge.QuantaRownum]int
	for _, leftField := range leftFields {
		rightField, ok := directBitmapMatchingRightBSIProjectionField(leftField, rightFields)
		if !ok {
			return nil, false
		}
		rightVector, ok := rightVectors[directBitmapMembershipBSIProjectionKey(rightField)]
		if !ok {
			return nil, false
		}
		if rightPositions == nil {
			rightPositions = make(map[qsbridge.QuantaRownum]int, len(rightVector.Rownums))
			for i, rownum := range rightVector.Rownums {
				rightPositions[rownum] = i
			}
		}
		values := make([]*big.Int, len(leftRownums))
		for i, rownum := range leftRownums {
			rightIndex, ok := rightPositions[rownum]
			if !ok || rightIndex >= len(rightVector.Values) || rightVector.Values[rightIndex] == nil {
				continue
			}
			values[i] = rightVector.Values[rightIndex]
		}
		derived[directBitmapMembershipBSIProjectionKey(leftField)] = directBitmapMembershipBSIVector{
			Field: qsbridge.FieldRef{
				Table:        qsbridge.TableInstance{Table: leftField.Index, Alias: string(leftField.Role)},
				Name:         leftField.Field,
				PhysicalName: leftField.PhysicalName,
				Type:         leftField.Type,
			},
			Rownums: append([]qsbridge.QuantaRownum(nil), leftRownums...),
			Values:  values,
		}
	}
	return derived, true
}

func directBitmapMatchingRightBSIProjectionField(leftField qsbridge.QuantaProjectionField, rightFields []qsbridge.QuantaProjectionField) (qsbridge.QuantaProjectionField, bool) {
	leftPhysical := leftField.PhysicalName
	if leftPhysical == "" {
		leftPhysical = leftField.Field
	}
	for _, rightField := range rightFields {
		rightPhysical := rightField.PhysicalName
		if rightPhysical == "" {
			rightPhysical = rightField.Field
		}
		if strings.EqualFold(leftPhysical, rightPhysical) && leftField.Type == rightField.Type {
			return rightField, true
		}
	}
	return qsbridge.QuantaProjectionField{}, false
}

func directBitmapCorrelatedMembershipBSIComparisons(membership qsbridge.MembershipEdge, predicates []qsbridge.Predicate) ([]directBitmapMembershipBSIComparison, bool) {
	comparisons := make([]directBitmapMembershipBSIComparison, 0, len(predicates))
	for _, predicate := range predicates {
		binary, ok := directBitmapBinaryExpr(predicate.Expr)
		if !ok || !directBitmapSupportedComparisonOp(binary.Op) {
			return nil, false
		}
		leftField, ok := directBitmapExprField(binary.Left)
		if !ok || !directBitmapMembershipFieldBSIFastPathEligible(leftField) {
			return nil, false
		}
		rightField, ok := directBitmapExprField(binary.Right)
		if !ok || !directBitmapMembershipFieldBSIFastPathEligible(rightField) {
			return nil, false
		}
		leftOperand, ok := directBitmapCorrelatedMembershipBSIOperand(membership, leftField)
		if !ok {
			return nil, false
		}
		rightOperand, ok := directBitmapCorrelatedMembershipBSIOperand(membership, rightField)
		if !ok {
			return nil, false
		}
		comparisons = append(comparisons, directBitmapMembershipBSIComparison{
			Op:    binary.Op,
			Left:  leftOperand,
			Right: rightOperand,
		})
	}
	return comparisons, len(comparisons) > 0
}

func directBitmapCorrelatedMembershipBSIOperand(membership qsbridge.MembershipEdge, field qsbridge.FieldRef) (directBitmapMembershipBSIOperand, bool) {
	switch {
	case directBitmapSameTableInstance(field.Table, membership.Left.Table):
		return directBitmapMembershipBSIOperand{Side: directBitmapMembershipBSIOperandLeft, Field: field}, true
	case directBitmapSameTableInstance(field.Table, membership.Right.Table):
		return directBitmapMembershipBSIOperand{Side: directBitmapMembershipBSIOperandRight, Field: field}, true
	default:
		return directBitmapMembershipBSIOperand{}, false
	}
}

func directBitmapMembershipFieldBSIFastPathEligible(field qsbridge.FieldRef) bool {
	switch field.Index {
	case qsbridge.IndexBSI, qsbridge.IndexDateTime:
	default:
		return false
	}
	switch field.Type {
	case qsbridge.DataTypeInt, qsbridge.DataTypeTime:
		return true
	default:
		return false
	}
}

func (r DirectBitmapRuntime) directBitmapReadMembershipBSIVectors(ctx context.Context, index string, rownums []qsbridge.QuantaRownum, fields []qsbridge.QuantaProjectionField) (map[string]directBitmapMembershipBSIVector, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	vectors := make(map[string]directBitmapMembershipBSIVector, len(fields))
	requests := make([]NativeProjectionBSIReadRequest, 0, len(fields))
	positions := make([]string, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		key := directBitmapMembershipBSIProjectionKey(field)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		requests = append(requests, NativeProjectionBSIReadRequest{
			Index:         index,
			Field:         field,
			PhysicalField: field.Field,
			Rownums:       append([]qsbridge.QuantaRownum(nil), rownums...),
		})
		positions = append(positions, key)
		vectors[key] = directBitmapMembershipBSIVector{
			Field:   qsbridge.FieldRef{Table: qsbridge.TableInstance{Table: index}, Name: field.Field, PhysicalName: field.PhysicalName, Type: field.Type},
			Rownums: append([]qsbridge.QuantaRownum(nil), rownums...),
			Values:  make([]*big.Int, len(rownums)),
		}
	}
	if len(rownums) == 0 || len(requests) == 0 {
		return vectors, nil, nil, nil
	}
	if valueReader, ok := r.ProjectionBSIReader.(NativeProjectionBSIValueBatchReader); ok {
		return directBitmapReadMembershipBSIValueVectors(ctx, valueReader, requests, positions, vectors)
	}
	var readResults []NativeProjectionBSIReadResult
	var diagnostics qsbridge.DiagnosticSet
	var err error
	if batchReader, ok := r.ProjectionBSIReader.(NativeProjectionBSIBatchReader); ok && len(requests) > 1 {
		readResults, diagnostics, err = batchReader.ReadProjectionBSIs(ctx, requests)
	} else {
		readResults = make([]NativeProjectionBSIReadResult, len(requests))
		for i, request := range requests {
			readResults[i], diagnostics, err = r.ProjectionBSIReader.ReadProjectionBSI(ctx, request)
			if err != nil || diagnostics.BlocksNative() {
				return vectors, nil, diagnostics, err
			}
		}
	}
	if err != nil || diagnostics.BlocksNative() {
		return vectors, nil, diagnostics, err
	}
	if len(readResults) != len(requests) {
		return vectors, nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "correlated sibling BSI fast path reader returned "+strconv.Itoa(len(readResults))+" field reads for "+strconv.Itoa(len(requests))+" requests"),
		}, nil
	}
	columnIDs := directBitmapRownumColumnIDs(rownums)
	probes := make([]ExecutionProbe, 0, len(readResults))
	for i, readResult := range readResults {
		probes = append(probes, readResult.Probes...)
		if readResult.BSI == nil {
			return vectors, probes, directBitmapMembershipDiagnostics("correlated sibling BSI fast path reader returned no BSI for " + requests[i].Index + "." + requests[i].PhysicalField), nil
		}
		vector := vectors[positions[i]]
		hydrationStart := time.Now()
		hydratedRows := 0
		missingRows := 0
		values := readResult.BSI.GetBigValues(columnIDs)
		for _, value := range values {
			if value == nil {
				missingRows++
				continue
			}
			hydratedRows++
		}
		vector.Values = values
		hydrationDetail := requests[i].Index + "." + requests[i].PhysicalField
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_bsi_value_hydration_elapsed", time.Since(hydrationStart).String(), hydrationDetail),
			directBitmapMembershipProbe("correlated_sibling_bsi_value_hydration_rows", strconv.Itoa(hydratedRows), hydrationDetail),
			directBitmapMembershipProbe("correlated_sibling_bsi_value_hydration_missing_rows", strconv.Itoa(missingRows), hydrationDetail),
		)
		vectors[positions[i]] = vector
	}
	return vectors, probes, nil, nil
}

func directBitmapReadMembershipBSIValueVectors(ctx context.Context, valueReader NativeProjectionBSIValueBatchReader, requests []NativeProjectionBSIReadRequest, positions []string, vectors map[string]directBitmapMembershipBSIVector) (map[string]directBitmapMembershipBSIVector, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	readResults, diagnostics, err := valueReader.ReadProjectionBSIValues(ctx, requests)
	if err != nil || diagnostics.BlocksNative() {
		return vectors, nil, diagnostics, err
	}
	if len(readResults) != len(requests) {
		return vectors, nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "correlated sibling BSI value reader returned "+strconv.Itoa(len(readResults))+" field reads for "+strconv.Itoa(len(requests))+" requests"),
		}, nil
	}
	probes := make([]ExecutionProbe, 0, len(readResults))
	for i, readResult := range readResults {
		probes = append(probes, readResult.Probes...)
		if len(readResult.Values) != len(requests[i].Rownums) {
			return vectors, probes, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "correlated sibling BSI value reader returned "+strconv.Itoa(len(readResult.Values))+" values for "+strconv.Itoa(len(requests[i].Rownums))+" rownums"),
			}, nil
		}
		vector := vectors[positions[i]]
		hydrationStart := time.Now()
		hydratedRows := 0
		missingRows := 0
		for _, value := range readResult.Values {
			if value == nil {
				missingRows++
				continue
			}
			hydratedRows++
		}
		vector.Values = readResult.Values
		hydrationDetail := requests[i].Index + "." + requests[i].PhysicalField
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_bsi_value_hydration_elapsed", time.Since(hydrationStart).String(), hydrationDetail),
			directBitmapMembershipProbe("correlated_sibling_bsi_value_hydration_rows", strconv.Itoa(hydratedRows), hydrationDetail),
			directBitmapMembershipProbe("correlated_sibling_bsi_value_hydration_missing_rows", strconv.Itoa(missingRows), hydrationDetail),
			directBitmapMembershipProbe("correlated_sibling_bsi_value_read_mode", "direct_values", hydrationDetail),
		)
		vectors[positions[i]] = vector
	}
	return vectors, probes, nil, nil
}

func directBitmapRownumColumnIDs(rownums []qsbridge.QuantaRownum) []uint64 {
	columnIDs := make([]uint64, len(rownums))
	for i, rownum := range rownums {
		columnIDs[i] = uint64(rownum)
	}
	return columnIDs
}

func directBitmapMembershipBSIFieldKey(field qsbridge.FieldRef) string {
	return directBitmapMembershipBSIProjectionKey(directBitmapMembershipProjectionField(field))
}

func directBitmapMembershipBSIProjectionKey(field qsbridge.QuantaProjectionField) string {
	return string(field.Role) + "\x00" + field.Field
}

func directBitmapMembershipBSIUniqueValues(values []*big.Int) []*big.Int {
	seen := make(map[string]struct{}, len(values))
	unique := make([]*big.Int, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		key := value.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, new(big.Int).Set(value))
	}
	return unique
}

func directBitmapEvaluateCorrelatedMembershipBSIComparisons(comparisons []directBitmapMembershipBSIComparison, leftVectors map[string]directBitmapMembershipBSIVector, leftIndex int, rightVectors map[string]directBitmapMembershipBSIVector, rightIndex int) (bool, qsbridge.DiagnosticSet) {
	for _, comparison := range comparisons {
		leftValue, ok := directBitmapCorrelatedMembershipBSIOperandValue(comparison.Left, leftVectors, leftIndex, rightVectors, rightIndex)
		if !ok {
			return false, directBitmapMembershipDiagnostics("correlated sibling BSI fast path left operand vector is missing")
		}
		rightValue, ok := directBitmapCorrelatedMembershipBSIOperandValue(comparison.Right, leftVectors, leftIndex, rightVectors, rightIndex)
		if !ok {
			return false, directBitmapMembershipDiagnostics("correlated sibling BSI fast path right operand vector is missing")
		}
		if leftValue == nil || rightValue == nil {
			return false, nil
		}
		if !directBitmapCompareBigIntValues(comparison.Op, leftValue, rightValue) {
			return false, nil
		}
	}
	return true, nil
}

func directBitmapCorrelatedMembershipBSIOperandValue(operand directBitmapMembershipBSIOperand, leftVectors map[string]directBitmapMembershipBSIVector, leftIndex int, rightVectors map[string]directBitmapMembershipBSIVector, rightIndex int) (*big.Int, bool) {
	switch operand.Side {
	case directBitmapMembershipBSIOperandLeft:
		return directBitmapMembershipBSIVectorValue(leftVectors, operand.Field, leftIndex)
	case directBitmapMembershipBSIOperandRight:
		return directBitmapMembershipBSIVectorValue(rightVectors, operand.Field, rightIndex)
	default:
		return nil, false
	}
}

func directBitmapMembershipBSIVectorValue(vectors map[string]directBitmapMembershipBSIVector, field qsbridge.FieldRef, index int) (*big.Int, bool) {
	vector, ok := vectors[directBitmapMembershipBSIFieldKey(field)]
	if !ok || index < 0 || index >= len(vector.Values) {
		return nil, false
	}
	return vector.Values[index], true
}

func directBitmapCompareBigIntValues(op qsbridge.BinaryOp, left *big.Int, right *big.Int) bool {
	cmp := left.Cmp(right)
	switch op {
	case qsbridge.BinaryOpEqual:
		return cmp == 0
	case qsbridge.BinaryOpNotEqual:
		return cmp != 0
	case qsbridge.BinaryOpLess:
		return cmp < 0
	case qsbridge.BinaryOpLessEqual:
		return cmp <= 0
	case qsbridge.BinaryOpGreater:
		return cmp > 0
	case qsbridge.BinaryOpGreaterEqual:
		return cmp >= 0
	default:
		return false
	}
}

func directBitmapCorrelatedMembershipRightKeyNarrowFragmentValues(membership qsbridge.MembershipEdge, values []*big.Int) ([]qsbridge.QuantaQueryFragment, []ExecutionProbe, bool) {
	start := time.Now()
	detail := directBitmapMembershipNarrowProbeDetail(membership)
	probes := []ExecutionProbe{
		directBitmapMembershipProbe("correlated_sibling_right_narrow_left_key_count", strconv.Itoa(len(values)), detail),
	}
	if len(values) == 0 {
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_right_narrow_applied", "true", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_reason", "empty_left_keys", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_mode", "folded_right_request", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_elapsed", time.Since(start).String(), detail),
		)
		return []qsbridge.QuantaQueryFragment{{
			Index:     membership.Right.Table.Table,
			Field:     directBitmapFieldPhysicalName(membership.Right),
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpBatchEQ,
			Values:    values,
		}}, probes, true
	}
	if len(values) > directBitmapMembershipMaxDynamicBatchEQValues {
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_right_narrow_cap_skipped", "true", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_applied", "false", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_reason", "key_count_exceeds_cap", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_elapsed", time.Since(start).String(), detail),
		)
		return nil, probes, false
	}
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_right_narrow_cap_skipped", "false", detail),
		directBitmapMembershipProbe("correlated_sibling_right_narrow_mode", "folded_right_request", detail),
	)
	return []qsbridge.QuantaQueryFragment{{
		Index:     membership.Right.Table.Table,
		Field:     directBitmapFieldPhysicalName(membership.Right),
		Operation: qsbridge.QuantaOperationIntersect,
		BSIOp:     qsbridge.QuantaBSIOpBatchEQ,
		Values:    values,
	}}, probes, true
}

func directBitmapCorrelatedMembershipRightKeyNarrowFragment(membership qsbridge.MembershipEdge, leftKeys []qsbridge.ResultCell) ([]qsbridge.QuantaQueryFragment, []ExecutionProbe, bool) {
	start := time.Now()
	detail := directBitmapMembershipNarrowProbeDetail(membership)
	values, ok := directBitmapMembershipBatchEQValues(leftKeys)
	probes := []ExecutionProbe{
		directBitmapMembershipProbe("correlated_sibling_right_narrow_left_key_count", strconv.Itoa(len(values)), detail),
	}
	if !ok {
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_right_narrow_applied", "false", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_reason", "unsupported_key_type", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_elapsed", time.Since(start).String(), detail),
		)
		return nil, probes, false
	}
	if len(values) == 0 {
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_right_narrow_applied", "true", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_reason", "empty_left_keys", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_mode", "folded_right_request", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_elapsed", time.Since(start).String(), detail),
		)
		return []qsbridge.QuantaQueryFragment{{
			Index:     membership.Right.Table.Table,
			Field:     directBitmapFieldPhysicalName(membership.Right),
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpBatchEQ,
			Values:    values,
		}}, probes, true
	}
	if len(values) > directBitmapMembershipMaxDynamicBatchEQValues {
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_right_narrow_cap_skipped", "true", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_applied", "false", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_reason", "key_count_exceeds_cap", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_elapsed", time.Since(start).String(), detail),
		)
		return nil, probes, false
	}
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_right_narrow_cap_skipped", "false", detail),
		directBitmapMembershipProbe("correlated_sibling_right_narrow_mode", "folded_right_request", detail),
	)
	return []qsbridge.QuantaQueryFragment{{
		Index:     membership.Right.Table.Table,
		Field:     directBitmapFieldPhysicalName(membership.Right),
		Operation: qsbridge.QuantaOperationIntersect,
		BSIOp:     qsbridge.QuantaBSIOpBatchEQ,
		Values:    values,
	}}, probes, true
}

// directBitmapNarrowCorrelatedMembershipRightCandidates reduces a sibling
// membership's right alias with a dynamic BATCH_EQ lookup from the current
// left-side key values before right-side correlation fields are materialized.
func (r DirectBitmapRuntime) directBitmapNarrowCorrelatedMembershipRightCandidates(ctx context.Context, result BitmapQueryResult, membership qsbridge.MembershipEdge, leftKeys []qsbridge.ResultCell) (BitmapQueryResult, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	start := time.Now()
	detail := directBitmapMembershipNarrowProbeDetail(membership)
	before := len(result.Rownums)
	values, ok := directBitmapMembershipBatchEQValues(leftKeys)
	probes := []ExecutionProbe{
		directBitmapMembershipProbe("correlated_sibling_right_narrow_right_candidates_before", strconv.Itoa(before), detail),
	}
	if !ok {
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_right_narrow_applied", "false", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_reason", "unsupported_key_type", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_elapsed", time.Since(start).String(), detail),
		)
		return result, probes, nil, nil
	}
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_right_narrow_left_key_count", strconv.Itoa(len(values)), detail),
	)
	if len(values) == 0 {
		filtered := result.Clone()
		filtered.Rownums = filtered.Rownums[:0]
		filtered.Count = 0
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_right_narrow_right_candidates_after", "0", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_cap_skipped", "false", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_applied", "true", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_reason", "empty_left_keys", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_elapsed", time.Since(start).String(), detail),
		)
		return filtered, probes, nil, nil
	}
	if len(values) > directBitmapMembershipMaxDynamicBatchEQValues {
		probes = append(probes,
			directBitmapMembershipProbe("correlated_sibling_right_narrow_right_candidates_after", strconv.Itoa(before), detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_cap_skipped", "true", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_applied", "false", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_reason", "key_count_exceeds_cap", detail),
			directBitmapMembershipProbe("correlated_sibling_right_narrow_elapsed", time.Since(start).String(), detail),
		)
		return result, probes, nil, nil
	}
	if r.Sessions == nil {
		return result, probes, directBitmapMembershipDiagnostics("correlated sibling membership right-key narrowing has no session provider"), nil
	}
	narrowRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     membership.Right.Table.Table,
			Field:     directBitmapFieldPhysicalName(membership.Right),
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpBatchEQ,
			Values:    values,
		}},
	})
	session, diagnostics, err := r.Sessions.BorrowDirectSession(ctx, narrowRequest)
	if err != nil || diagnostics.BlocksNative() {
		return result, probes, diagnostics, err
	}
	if session == nil {
		return result, probes, directBitmapMembershipDiagnostics("direct session provider returned nil correlated membership session"), nil
	}
	narrowed, queryDiagnostics, queryErr := session.QueryBitmap(ctx, narrowRequest)
	releaseDiagnostics := session.Release(ctx)
	diagnostics = append(diagnostics, queryDiagnostics...)
	diagnostics = append(diagnostics, releaseDiagnostics...)
	if queryErr != nil || diagnostics.BlocksNative() {
		return result, probes, diagnostics, queryErr
	}
	filtered := directBitmapIntersectMembershipCandidates(result, narrowed)
	probes = append(probes,
		directBitmapMembershipProbe("correlated_sibling_right_narrow_right_candidates_after", strconv.Itoa(len(filtered.Rownums)), detail),
		directBitmapMembershipProbe("correlated_sibling_right_narrow_cap_skipped", "false", detail),
		directBitmapMembershipProbe("correlated_sibling_right_narrow_applied", "true", detail),
		directBitmapMembershipProbe("correlated_sibling_right_narrow_elapsed", time.Since(start).String(), detail),
	)
	return filtered, probes, nil, nil
}

// directBitmapMembershipBatchEQValues converts materialized key cells into
// unique integer values suitable for a BSI BATCH_EQ lookup.
func directBitmapMembershipBatchEQValues(cells []qsbridge.ResultCell) ([]*big.Int, bool) {
	seen := make(map[string]struct{}, len(cells))
	values := make([]*big.Int, 0, len(cells))
	for _, cell := range cells {
		value, ok := directBitmapMembershipCellBigInt(cell)
		if !ok {
			return nil, false
		}
		if value == nil {
			continue
		}
		key := value.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		values = append(values, value)
	}
	return values, true
}

// directBitmapMembershipCellBigInt converts one SQL cell into an integer BSI
// lookup value, returning nil for SQL NULL and false for unsupported types.
func directBitmapMembershipCellBigInt(cell qsbridge.ResultCell) (*big.Int, bool) {
	if cell.Kind == qsbridge.ValueNull || cell.Value == nil {
		return nil, true
	}
	switch value := cell.Value.(type) {
	case int:
		return big.NewInt(int64(value)), true
	case int64:
		return big.NewInt(value), true
	case uint64:
		return new(big.Int).SetUint64(value), true
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, false
		}
		return big.NewInt(parsed), true
	default:
		return nil, false
	}
}

// directBitmapIntersectMembershipCandidates preserves the left candidate order
// while applying a right-side bitmap query as a narrowing filter.
func directBitmapIntersectMembershipCandidates(left BitmapQueryResult, right BitmapQueryResult) BitmapQueryResult {
	allowed := make(map[qsbridge.QuantaRownum]struct{}, len(right.Rownums))
	for _, rownum := range right.Rownums {
		allowed[rownum] = struct{}{}
	}
	filtered := left.Clone()
	filtered.Rownums = filtered.Rownums[:0]
	for _, rownum := range left.Rownums {
		if _, ok := allowed[rownum]; ok {
			filtered.Rownums = append(filtered.Rownums, rownum)
		}
	}
	filtered.Count = uint64(len(filtered.Rownums))
	return filtered
}

func directBitmapMembershipProbe(name string, value string, detail string) ExecutionProbe {
	return ExecutionProbe{
		Section: "direct_bitmap_membership",
		Name:    name,
		Value:   value,
		Detail:  detail,
	}
}

func directBitmapMembershipNarrowProbeDetail(membership qsbridge.MembershipEdge) string {
	return strings.Join([]string{
		"kind=" + string(membership.Kind),
		"left=" + membership.Left.QualifiedName(),
		"right=" + membership.Right.QualifiedName(),
		"cap=" + strconv.Itoa(directBitmapMembershipMaxDynamicBatchEQValues),
	}, " ")
}

func (r DirectBitmapRuntime) directBitmapApplyRelationshipMembership(ctx context.Context, result BitmapQueryResult, membership qsbridge.MembershipEdge) (BitmapQueryResult, bool, qsbridge.DiagnosticSet, error) {
	vectorRequest, ok := directBitmapRelationshipMembershipRequest(membership, qsbridge.QuantaCandidateSet{})
	if !ok {
		return result, false, nil, nil
	}
	if r.RelationshipReader == nil {
		return result, true, directBitmapMembershipDiagnostics("relationship-vector membership has no relationship reader"), nil
	}
	rightResult, diagnostics, err := r.directBitmapMembershipRightCandidateResult(ctx, membership)
	if err != nil || diagnostics.BlocksNative() {
		return result, true, diagnostics, err
	}
	vectorRequest, _ = directBitmapRelationshipMembershipRequest(membership, qsbridge.QuantaCandidateSet{
		Index:   membership.Right.Table.Table,
		Rownums: append([]qsbridge.QuantaRownum(nil), rightResult.Rownums...),
	})
	related, diagnostics, err := r.RelationshipReader.ReadRelatedCandidates(ctx, vectorRequest)
	if err != nil || diagnostics.BlocksNative() {
		return result, true, diagnostics, err
	}
	relatedRows := make(map[qsbridge.QuantaRownum]struct{}, len(related.Rownums))
	for _, rownum := range related.Rownums {
		relatedRows[rownum] = struct{}{}
	}
	filtered := result.Clone()
	filtered.Rownums = filtered.Rownums[:0]
	for _, rownum := range result.Rownums {
		_, matched := relatedRows[rownum]
		keep := matched
		if membership.Kind == qsbridge.MembershipAnti {
			keep = !matched
		}
		if keep {
			filtered.Rownums = append(filtered.Rownums, rownum)
		}
	}
	filtered.Count = uint64(len(filtered.Rownums))
	return filtered, true, nil, nil
}

func directBitmapRelationshipMembershipRequest(membership qsbridge.MembershipEdge, source qsbridge.QuantaCandidateSet) (qsbridge.FilterDomainRelationshipVectorRequest, bool) {
	leftRelation := directBitmapMembershipFieldIsParentRelation(membership.Left)
	rightRelation := directBitmapMembershipFieldIsParentRelation(membership.Right)
	if leftRelation == rightRelation {
		return qsbridge.FilterDomainRelationshipVectorRequest{}, false
	}
	switch {
	case rightRelation:
		return qsbridge.FilterDomainRelationshipVectorRequest{
			Operation:        qsbridge.FilterDomainNormalizeGroupedFilter,
			SourceFragment:   directBitmapRelationshipMembershipSourceFragment(membership.Right),
			SourceCandidates: source,
			SourceDomain:     membership.Right.Table.Table,
			TargetDomain:     membership.Left.Table.Table,
			Direction:        qsbridge.FilterDomainRelationshipVectorDirectionLeftToRight,
			Strategy:         qsbridge.PhysicalStrategyRelationshipVectorNormalization,
			Edge: qsbridge.RelationshipJoinPlanEdge{
				Left:          membership.Right,
				Right:         membership.Left,
				ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
				EncodingKind:  qsbridge.RelationshipEncodingVector,
			},
		}, true
	case leftRelation:
		return qsbridge.FilterDomainRelationshipVectorRequest{
			Operation:        qsbridge.FilterDomainNormalizeGroupedFilter,
			SourceFragment:   directBitmapRelationshipMembershipSourceFragment(membership.Right),
			SourceCandidates: source,
			SourceDomain:     membership.Right.Table.Table,
			TargetDomain:     membership.Left.Table.Table,
			Direction:        qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
			Strategy:         qsbridge.PhysicalStrategyRelationshipVectorNormalization,
			Edge: qsbridge.RelationshipJoinPlanEdge{
				Left:          membership.Left,
				Right:         membership.Right,
				ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
				EncodingKind:  qsbridge.RelationshipEncodingVector,
			},
		}, true
	default:
		return qsbridge.FilterDomainRelationshipVectorRequest{}, false
	}
}

func directBitmapRelationshipMembershipSourceFragment(field qsbridge.FieldRef) qsbridge.QuantaQueryFragment {
	return qsbridge.QuantaQueryFragment{
		Index: field.Table.Table,
		Field: directBitmapFieldPhysicalName(field),
	}
}

func directBitmapMembershipFieldIsParentRelation(field qsbridge.FieldRef) bool {
	return strings.EqualFold(field.Encoding.LegacyName, "ParentRelation")
}

func (r DirectBitmapRuntime) directBitmapMembershipRightCandidateResultWithCacheAndProbes(ctx context.Context, membership qsbridge.MembershipEdge, extraFragments []qsbridge.QuantaQueryFragment, narrowValues []*big.Int, detail string) (BitmapQueryResult, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	cacheKey, cacheDetail, cacheable := directBitmapMembershipRightCandidateBaseCacheKey(membership)
	requestedValues := directBitmapMembershipBSIValueKeyStrings(narrowValues)
	probes := []ExecutionProbe{}
	if cacheable {
		cacheStart := time.Now()
		if cached, mode, ok := MembershipRightCandidateCacheFromContext(ctx).Get(cacheKey, requestedValues); ok {
			recordQueryScratchpadCacheLookup(ctx, "membership_right_candidate_cache", true, mode, cacheDetail)
			rightRequest, diagnostics := directBitmapMembershipRightRequestWithExtraFragments(membership, extraFragments)
			probes = append(probes,
				directBitmapMembershipProbe("membership_right_candidate_cache_hit", "true", detail),
				directBitmapMembershipProbe("membership_right_candidate_cache_mode", mode, detail),
				directBitmapMembershipProbe("membership_right_candidate_cache_elapsed", time.Since(cacheStart).String(), detail),
				directBitmapMembershipProbe("membership_right_candidate_query_elapsed", "0s", detail),
				directBitmapMembershipProbe("membership_right_candidate_query_rows", strconv.Itoa(len(cached.Rownums)), detail),
			)
			if diagnostics.BlocksNative() {
				return BitmapQueryResult{}, probes, diagnostics, nil
			}
			cached, residualProbes, diagnostics, err := r.directBitmapApplyMembershipRightCandidateResiduals(ctx, rightRequest, cached, membership, detail)
			probes = append(probes, residualProbes...)
			return cached, probes, diagnostics, err
		}
		mode := "cache_absent"
		if MembershipRightCandidateCacheFromContext(ctx) != nil {
			mode = "miss"
		}
		recordQueryScratchpadCacheLookup(ctx, "membership_right_candidate_cache", false, mode, cacheDetail)
		probes = append(probes,
			directBitmapMembershipProbe("membership_right_candidate_cache_hit", "false", detail),
			directBitmapMembershipProbe("membership_right_candidate_cache_mode", mode, detail),
			directBitmapMembershipProbe("membership_right_candidate_cache_elapsed", time.Since(cacheStart).String(), detail),
		)
	}
	result, queryProbes, diagnostics, err := r.directBitmapMembershipRightCandidateResultWithExtraFragmentsAndProbes(ctx, membership, extraFragments)
	probes = append(probes, queryProbes...)
	return result, probes, diagnostics, err
}

func directBitmapMembershipRightCandidateCacheStore(ctx context.Context, membership qsbridge.MembershipEdge, narrowValues []*big.Int, rightKey directBitmapMembershipBSIVector, detail string) []ExecutionProbe {
	cacheKey, cacheDetail, ok := directBitmapMembershipRightCandidateBaseCacheKey(membership)
	if !ok {
		return nil
	}
	keyValues := directBitmapMembershipBSIVectorValueKeys(rightKey.Values)
	if len(keyValues) != len(rightKey.Rownums) {
		return nil
	}
	cache := MembershipRightCandidateCacheFromContext(ctx)
	if cache == nil {
		return nil
	}
	cache.Set(cacheKey, directBitmapMembershipBSIValueKeyStrings(narrowValues), rightKey.Rownums, keyValues)
	recordQueryScratchpadCacheStore(ctx, "membership_right_candidate_cache", cacheDetail)
	return []ExecutionProbe{
		directBitmapMembershipProbe("membership_right_candidate_cache_store", "true", detail),
		directBitmapMembershipProbe("membership_right_candidate_cache_store_rows", strconv.Itoa(len(rightKey.Rownums)), detail),
	}
}

func directBitmapMembershipRightCandidateCellCacheStore(ctx context.Context, membership qsbridge.MembershipEdge, narrowValues []*big.Int, rownums []qsbridge.QuantaRownum, values []qsbridge.ResultCell, detail string) []ExecutionProbe {
	cacheKey, cacheDetail, ok := directBitmapMembershipRightCandidateBaseCacheKey(membership)
	if !ok {
		return nil
	}
	keyValues, ok := directBitmapMembershipCellValueKeys(values)
	if !ok || len(keyValues) != len(rownums) {
		return nil
	}
	cache := MembershipRightCandidateCacheFromContext(ctx)
	if cache == nil {
		return nil
	}
	cache.Set(cacheKey, directBitmapMembershipBSIValueKeyStrings(narrowValues), rownums, keyValues)
	recordQueryScratchpadCacheStore(ctx, "membership_right_candidate_cache", cacheDetail)
	return []ExecutionProbe{
		directBitmapMembershipProbe("membership_right_candidate_cache_store", "true", detail),
		directBitmapMembershipProbe("membership_right_candidate_cache_store_rows", strconv.Itoa(len(rownums)), detail),
	}
}

func directBitmapMembershipRightCandidateCanStoreRaw(rightOnlyPredicates []qsbridge.Predicate) bool {
	return len(directBitmapMembershipResidualPredicates(rightOnlyPredicates)) == 0
}

func directBitmapMembershipRightCandidateBaseCacheKey(membership qsbridge.MembershipEdge) (string, string, bool) {
	baseRequest, diagnostics := directBitmapMembershipRightRequestWithExtraFragments(membership, nil)
	if diagnostics.BlocksNative() {
		return "", "", false
	}
	key := directBitmapMembershipRightCandidateRequestCacheKey(baseRequest)
	if key == "" {
		return "", "", false
	}
	return key, "key=" + key, true
}

func directBitmapMembershipRightCandidateRequestCacheKey(request ExecutionRequest) string {
	var b strings.Builder
	b.WriteString("fragments:")
	for i, fragment := range request.Query.Fragments {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(i))
		b.WriteByte('=')
		b.WriteString(fragment.CacheIdentity(qsbridge.QuantaFragmentCacheBoundary{}).Digest)
	}
	b.WriteString("|seeds:")
	for i, seed := range request.Query.Seeds {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(i))
		b.WriteByte('=')
		b.WriteString(seed.Index)
		b.WriteByte('.')
		b.WriteString(seed.Field)
		b.WriteByte(':')
		b.WriteString(string(seed.Kind))
		b.WriteByte(':')
		b.WriteString(directBitmapMembershipBigIntKey(seed.Begin))
		b.WriteByte(':')
		b.WriteString(directBitmapMembershipBigIntKey(seed.End))
		b.WriteByte(':')
		b.WriteString(strconv.FormatBool(seed.ShardWindow))
	}
	return b.String()
}

func directBitmapMembershipBSIValueKeyStrings(values []*big.Int) []string {
	keys := make([]string, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		keys = append(keys, value.String())
	}
	sort.Strings(keys)
	return keys
}

func directBitmapMembershipBSIVectorValueKeys(values []*big.Int) []string {
	keys := make([]string, len(values))
	for i, value := range values {
		keys[i] = directBitmapMembershipBigIntKey(value)
	}
	return keys
}

func directBitmapMembershipCellValueKeys(values []qsbridge.ResultCell) ([]string, bool) {
	keys := make([]string, len(values))
	for i, value := range values {
		parsed, ok := directBitmapMembershipCellBigInt(value)
		if !ok {
			return nil, false
		}
		keys[i] = directBitmapMembershipBigIntKey(parsed)
	}
	return keys, true
}

func directBitmapMembershipBigIntKey(value *big.Int) string {
	if value == nil {
		return "<nil>"
	}
	return value.String()
}

func (r DirectBitmapRuntime) directBitmapMembershipRightCandidateResult(ctx context.Context, membership qsbridge.MembershipEdge) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	return r.directBitmapMembershipRightCandidateResultWithExtraFragments(ctx, membership, nil)
}

func (r DirectBitmapRuntime) directBitmapMembershipRightCandidateResultWithExtraFragments(ctx context.Context, membership qsbridge.MembershipEdge, extraFragments []qsbridge.QuantaQueryFragment) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	result, _, diagnostics, err := r.directBitmapMembershipRightCandidateResultWithExtraFragmentsAndProbes(ctx, membership, extraFragments)
	return result, diagnostics, err
}

func (r DirectBitmapRuntime) directBitmapMembershipRightCandidateResultWithExtraFragmentsAndProbes(ctx context.Context, membership qsbridge.MembershipEdge, extraFragments []qsbridge.QuantaQueryFragment) (BitmapQueryResult, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	detail := directBitmapMembershipNarrowProbeDetail(membership)
	rightRequest, diagnostics := directBitmapMembershipRightRequestWithExtraFragments(membership, extraFragments)
	probes := []ExecutionProbe{
		directBitmapMembershipProbe("membership_right_candidate_fragment_count", strconv.Itoa(len(rightRequest.Query.Fragments)), detail),
		directBitmapMembershipProbe("membership_right_candidate_extra_fragment_count", strconv.Itoa(len(extraFragments)), detail),
	}
	if diagnostics.BlocksNative() {
		return BitmapQueryResult{}, probes, diagnostics, nil
	}
	borrowStart := time.Now()
	session, diagnostics, err := r.Sessions.BorrowDirectSession(ctx, rightRequest)
	probes = append(probes, directBitmapMembershipProbe("membership_right_candidate_borrow_elapsed", time.Since(borrowStart).String(), detail))
	if err != nil || diagnostics.BlocksNative() {
		return BitmapQueryResult{}, probes, diagnostics, err
	}
	if session == nil {
		return BitmapQueryResult{}, probes, directBitmapMembershipDiagnostics("direct session provider returned nil membership session"), nil
	}
	queryStart := time.Now()
	rightResult, queryDiagnostics, queryErr := session.QueryBitmap(ctx, rightRequest)
	queryElapsed := time.Since(queryStart)
	releaseStart := time.Now()
	releaseDiagnostics := session.Release(ctx)
	releaseElapsed := time.Since(releaseStart)
	probes = append(probes,
		directBitmapMembershipProbe("membership_right_candidate_query_elapsed", queryElapsed.String(), detail),
		directBitmapMembershipProbe("membership_right_candidate_query_rows", strconv.Itoa(len(rightResult.Rownums)), detail),
		directBitmapMembershipProbe("membership_right_candidate_release_elapsed", releaseElapsed.String(), detail),
	)
	diagnostics = append(diagnostics, queryDiagnostics...)
	diagnostics = append(diagnostics, releaseDiagnostics...)
	if queryErr != nil || diagnostics.BlocksNative() {
		return BitmapQueryResult{}, probes, diagnostics, queryErr
	}
	rightResult, residualProbes, diagnostics, err := r.directBitmapApplyMembershipRightCandidateResiduals(ctx, rightRequest, rightResult, membership, detail)
	probes = append(probes, residualProbes...)
	return rightResult, probes, diagnostics, err
}

func (r DirectBitmapRuntime) directBitmapApplyMembershipRightCandidateResiduals(ctx context.Context, rightRequest ExecutionRequest, rightResult BitmapQueryResult, membership qsbridge.MembershipEdge, detail string) (BitmapQueryResult, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	probes := []ExecutionProbe{}
	residuals := directBitmapMembershipResidualPredicates(membership.Predicates)
	probes = append(probes, directBitmapMembershipProbe("membership_right_candidate_residual_count", strconv.Itoa(len(residuals)), detail))
	if len(residuals) == 0 {
		return rightResult, probes, nil, nil
	}
	sameRowStart := time.Now()
	beforeSameRowCount := len(rightResult.Rownums)
	rightResult, residuals, diagnostics, err := r.directBitmapApplyMembershipRightSameRowResiduals(ctx, rightRequest, rightResult, residuals)
	sameRowElapsed := time.Since(sameRowStart)
	probes = append(probes,
		directBitmapMembershipProbe("membership_right_candidate_same_row_elapsed", sameRowElapsed.String(), detail),
		directBitmapMembershipProbe("membership_right_candidate_same_row_rows_before", strconv.Itoa(beforeSameRowCount), detail),
		directBitmapMembershipProbe("membership_right_candidate_same_row_rows_after", strconv.Itoa(len(rightResult.Rownums)), detail),
		directBitmapMembershipProbe("membership_right_candidate_residual_remaining", strconv.Itoa(len(residuals)), detail),
	)
	if err != nil || diagnostics.BlocksNative() {
		return BitmapQueryResult{}, probes, diagnostics, err
	}
	if len(residuals) == 0 {
		return rightResult, probes, nil, nil
	}
	materializationStart := time.Now()
	rowSet, diagnostics, err := r.directBitmapMembershipMaterialize(ctx, rightResult, membership.Right, residuals)
	probes = append(probes, directBitmapMembershipProbe("membership_right_candidate_residual_materialization_elapsed", time.Since(materializationStart).String(), detail))
	if err != nil || diagnostics.BlocksNative() {
		return BitmapQueryResult{}, probes, diagnostics, err
	}
	residualScanStart := time.Now()
	rowSet, diagnostics = directBitmapFilterResidualScanPredicates(ExecutionRequest{Predicates: residuals}, rowSet)
	probes = append(probes, directBitmapMembershipProbe("membership_right_candidate_residual_scan_elapsed", time.Since(residualScanStart).String(), detail))
	if diagnostics.BlocksNative() {
		return BitmapQueryResult{}, probes, diagnostics, nil
	}
	rightResult.Rownums = append([]qsbridge.QuantaRownum(nil), rowSet.Rownums...)
	rightResult.Count = uint64(len(rightResult.Rownums))
	return rightResult, probes, nil, nil
}

// directBitmapApplyMembershipRightSameRowResiduals applies right-only same-row
// BSI residual predicates before the membership path falls back to row
// materialization for remaining residuals.
func (r DirectBitmapRuntime) directBitmapApplyMembershipRightSameRowResiduals(ctx context.Context, request ExecutionRequest, result BitmapQueryResult, residuals []qsbridge.Predicate) (BitmapQueryResult, []qsbridge.Predicate, qsbridge.DiagnosticSet, error) {
	sameRow, remaining := directBitmapSplitSameRowResidualPredicates(residuals)
	if len(sameRow) == 0 {
		return result, residuals, nil, nil
	}
	sameRowRequest := request
	sameRowRequest.Predicates = sameRow
	filtered, _, diagnostics, err, applied := r.directBitmapApplySameRowResiduals(ctx, sameRowRequest, result)
	if err != nil || diagnostics.BlocksNative() {
		return result, residuals, diagnostics, err
	}
	if !applied {
		return result, residuals, nil, nil
	}
	return filtered, remaining, nil, nil
}

func (r DirectBitmapRuntime) directBitmapMembershipRightValues(ctx context.Context, membership qsbridge.MembershipEdge) (map[string]struct{}, qsbridge.DiagnosticSet, error) {
	rightResult, diagnostics, err := r.directBitmapMembershipRightCandidateResult(ctx, membership)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	rowSet, diagnostics, err := r.directBitmapMembershipMaterialize(ctx, rightResult, membership.Right, membership.Predicates)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	rowSet, diagnostics = directBitmapFilterResidualScanPredicates(ExecutionRequest{Predicates: directBitmapMembershipResidualPredicates(membership.Predicates)}, rowSet)
	if diagnostics.BlocksNative() {
		return nil, diagnostics, nil
	}
	values, ok := directBitmapProjectedValues(rowSet, membership.Right)
	if !ok {
		return nil, directBitmapMembershipDiagnostics("membership right field is not present in materialized row set"), nil
	}
	valueSet := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.Kind == qsbridge.ValueNull || value.Value == nil {
			continue
		}
		valueSet[directBitmapGroupKey(value)] = struct{}{}
	}
	return valueSet, nil, nil
}

func (r DirectBitmapRuntime) directBitmapMembershipLeftValues(ctx context.Context, result BitmapQueryResult, field qsbridge.FieldRef) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
	if len(result.Rownums) == 0 && result.Count > 0 {
		return qsbridge.QuantaProjectedRowSet{}, directBitmapMembershipDiagnostics("membership filters require materialized candidate rownums"), nil
	}
	rowSet, diagnostics, _, err := directBitmapMaterializeWithKernel(ctx, r.projectionMaterializationKernel(), qsbridge.QuantaMaterializationRequest{
		Index:   field.Table.Table,
		Rownums: append([]qsbridge.QuantaRownum(nil), result.Rownums...),
		ProjectionFields: []qsbridge.QuantaProjectionField{
			directBitmapMembershipProjectionField(field),
		},
	})
	return rowSet, diagnostics, err
}

func (r DirectBitmapRuntime) directBitmapMembershipMaterialize(ctx context.Context, result BitmapQueryResult, field qsbridge.FieldRef, predicates []qsbridge.Predicate) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
	fields := []qsbridge.QuantaProjectionField{directBitmapMembershipProjectionField(field)}
	for _, predicate := range directBitmapMembershipResidualPredicates(predicates) {
		for _, required := range directBitmapMembershipRequiredFields(predicate.Expr) {
			if !strings.EqualFold(required.Table.Table, field.Table.Table) {
				return qsbridge.QuantaProjectedRowSet{}, directBitmapMembershipDiagnostics("membership residual predicates must stay on the membership right table"), nil
			}
			projection := directBitmapMembershipProjectionField(required)
			if !directBitmapMembershipHasProjectionField(fields, projection) {
				fields = append(fields, projection)
			}
		}
	}
	rowSet, diagnostics, _, err := directBitmapMaterializeWithKernel(ctx, r.projectionMaterializationKernel(), qsbridge.QuantaMaterializationRequest{
		Index:            field.Table.Table,
		Rownums:          append([]qsbridge.QuantaRownum(nil), result.Rownums...),
		ProjectionFields: fields,
	})
	return rowSet, diagnostics, err
}

func directBitmapMembershipRightRequest(membership qsbridge.MembershipEdge) (ExecutionRequest, qsbridge.DiagnosticSet) {
	return directBitmapMembershipRightRequestWithExtraFragments(membership, nil)
}

func directBitmapMembershipRightRequestWithExtraFragments(membership qsbridge.MembershipEdge, extraFragments []qsbridge.QuantaQueryFragment) (ExecutionRequest, qsbridge.DiagnosticSet) {
	fragments := make([]qsbridge.QuantaQueryFragment, 0, len(membership.Predicates))
	for _, predicate := range membership.Predicates {
		if predicate.Placement == qsbridge.PredicateResidualScan {
			continue
		}
		fragment, diagnostics, ok := directBitmapMembershipPredicateFragment(predicate)
		if diagnostics.BlocksNative() {
			return ExecutionRequest{}, diagnostics
		}
		if !ok {
			continue
		}
		fragments = append(fragments, fragment)
	}
	fragments = append(fragments, extraFragments...)
	if len(fragments) == 0 {
		fragments = append(fragments, qsbridge.QuantaQueryFragment{
			Index:     membership.Right.Table.Table,
			Field:     directBitmapFieldPhysicalName(membership.Right),
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		})
	}
	return NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: fragments}), nil
}

func directBitmapMembershipPredicateFragment(predicate qsbridge.Predicate) (qsbridge.QuantaQueryFragment, qsbridge.DiagnosticSet, bool) {
	binary, ok := directBitmapBinaryExpr(predicate.Expr)
	if !ok {
		return qsbridge.QuantaQueryFragment{}, nil, false
	}
	field, ok := directBitmapExprField(binary.Left)
	if !ok {
		return qsbridge.QuantaQueryFragment{}, nil, false
	}
	switch binary.Op {
	case qsbridge.BinaryOpIn, qsbridge.BinaryOpNotIn:
		list, ok := directBitmapListExpr(binary.Right)
		if !ok {
			return qsbridge.QuantaQueryFragment{}, nil, false
		}
		values := make([]*big.Int, 0, len(list.Items))
		for _, item := range list.Items {
			value, ok := directBitmapMembershipLiteralBigInt(item)
			if !ok {
				return qsbridge.QuantaQueryFragment{}, nil, false
			}
			values = append(values, value)
		}
		return qsbridge.QuantaQueryFragment{
			Index:     field.Table.Table,
			Field:     directBitmapFieldPhysicalName(field),
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpBatchEQ,
			Values:    values,
			Negate:    binary.Op == qsbridge.BinaryOpNotIn,
		}, nil, true
	case qsbridge.BinaryOpEqual, qsbridge.BinaryOpNotEqual, qsbridge.BinaryOpGreater, qsbridge.BinaryOpGreaterEqual, qsbridge.BinaryOpLess, qsbridge.BinaryOpLessEqual:
		value, ok := directBitmapMembershipLiteralBigInt(binary.Right)
		if !ok {
			return qsbridge.QuantaQueryFragment{}, nil, false
		}
		operation := qsbridge.QuantaOperationIntersect
		bsiOp := directBitmapMembershipBSIOp(binary.Op)
		if binary.Op == qsbridge.BinaryOpNotEqual {
			operation = qsbridge.QuantaOperationDifference
			bsiOp = qsbridge.QuantaBSIOpEQ
		}
		return qsbridge.QuantaQueryFragment{
			Index:     field.Table.Table,
			Field:     directBitmapFieldPhysicalName(field),
			Operation: operation,
			BSIOp:     bsiOp,
			Value:     value,
		}, nil, true
	default:
		return qsbridge.QuantaQueryFragment{}, nil, false
	}
}

func directBitmapMembershipResidualPredicates(predicates []qsbridge.Predicate) []qsbridge.Predicate {
	residuals := make([]qsbridge.Predicate, 0, len(predicates))
	for _, predicate := range predicates {
		if predicate.Placement == qsbridge.PredicateResidualScan || !directBitmapMembershipPredicateCanLower(predicate) {
			predicate.Placement = qsbridge.PredicateResidualScan
			residuals = append(residuals, predicate)
		}
	}
	return residuals
}

// directBitmapSplitSameRowResidualPredicates separates residual predicates that
// can be evaluated by the native same-row BSI comparison kernel.
func directBitmapSplitSameRowResidualPredicates(predicates []qsbridge.Predicate) ([]qsbridge.Predicate, []qsbridge.Predicate) {
	sameRow := make([]qsbridge.Predicate, 0, len(predicates))
	remaining := make([]qsbridge.Predicate, 0, len(predicates))
	for _, predicate := range predicates {
		if predicate.Placement == qsbridge.PredicateResidualScan {
			if _, _, _, ok := qsbridge.SameRowBSIComparisonPredicate(predicate); ok {
				sameRow = append(sameRow, predicate)
				continue
			}
		}
		remaining = append(remaining, predicate)
	}
	return sameRow, remaining
}

func directBitmapMembershipHasCorrelatedPredicates(membership qsbridge.MembershipEdge) bool {
	_, correlated := directBitmapSplitMembershipPredicates(membership)
	return len(correlated) > 0
}

func directBitmapSplitMembershipPredicates(membership qsbridge.MembershipEdge) ([]qsbridge.Predicate, []qsbridge.Predicate) {
	rightOnly := make([]qsbridge.Predicate, 0, len(membership.Predicates))
	correlated := make([]qsbridge.Predicate, 0, len(membership.Predicates))
	for _, predicate := range membership.Predicates {
		fields := directBitmapMembershipRequiredFields(predicate.Expr)
		hasLeft := false
		hasRight := false
		other := false
		for _, field := range fields {
			switch {
			case directBitmapSameTableInstance(field.Table, membership.Left.Table):
				hasLeft = true
			case directBitmapSameTableInstance(field.Table, membership.Right.Table):
				hasRight = true
			default:
				other = true
			}
		}
		if other || hasLeft {
			correlated = append(correlated, predicate)
			continue
		}
		if hasRight || len(fields) == 0 {
			rightOnly = append(rightOnly, predicate)
		}
	}
	return rightOnly, correlated
}

func directBitmapCorrelatedMembershipProjectionFields(base qsbridge.FieldRef, predicates []qsbridge.Predicate, table qsbridge.TableInstance) []qsbridge.QuantaProjectionField {
	fields := []qsbridge.QuantaProjectionField{directBitmapMembershipProjectionField(base)}
	for _, predicate := range predicates {
		for _, required := range directBitmapMembershipRequiredFields(predicate.Expr) {
			if !directBitmapSameTableInstance(required.Table, table) {
				continue
			}
			projection := directBitmapMembershipProjectionField(required)
			if !directBitmapMembershipHasProjectionField(fields, projection) {
				fields = append(fields, projection)
			}
		}
	}
	return fields
}

func directBitmapEvaluateCorrelatedMembershipPredicates(predicates []qsbridge.Predicate, leftRowSet qsbridge.QuantaProjectedRowSet, leftIndex int, rightRowSet qsbridge.QuantaProjectedRowSet, rightIndex int, membership qsbridge.MembershipEdge) (bool, qsbridge.DiagnosticSet) {
	for _, predicate := range predicates {
		matched, diagnostics := directBitmapEvaluateCorrelatedMembershipBoolExpr(predicate.Expr, leftRowSet, leftIndex, rightRowSet, rightIndex, membership)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func directBitmapEvaluateCorrelatedMembershipBoolExpr(expr qsbridge.Expr, leftRowSet qsbridge.QuantaProjectedRowSet, leftIndex int, rightRowSet qsbridge.QuantaProjectedRowSet, rightIndex int, membership qsbridge.MembershipEdge) (bool, qsbridge.DiagnosticSet) {
	binary, ok := directBitmapBinaryExpr(expr)
	if !ok {
		return false, directBitmapMembershipDiagnostics("correlated sibling membership predicate must be binary")
	}
	switch binary.Op {
	case qsbridge.BinaryOpEqual, qsbridge.BinaryOpNotEqual,
		qsbridge.BinaryOpLess, qsbridge.BinaryOpLessEqual,
		qsbridge.BinaryOpGreater, qsbridge.BinaryOpGreaterEqual:
		left, diagnostics := directBitmapEvaluateCorrelatedMembershipExpr(binary.Left, leftRowSet, leftIndex, rightRowSet, rightIndex, membership)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		right, diagnostics := directBitmapEvaluateCorrelatedMembershipExpr(binary.Right, leftRowSet, leftIndex, rightRowSet, rightIndex, membership)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		return directBitmapResidualCompareCells(binary.Op, left, right), nil
	default:
		return false, directBitmapMembershipDiagnostics("correlated sibling membership predicate operator is not supported in this slice")
	}
}

func directBitmapEvaluateCorrelatedMembershipExpr(expr qsbridge.Expr, leftRowSet qsbridge.QuantaProjectedRowSet, leftIndex int, rightRowSet qsbridge.QuantaProjectedRowSet, rightIndex int, membership qsbridge.MembershipEdge) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if field, ok := directBitmapExprField(expr); ok {
		switch {
		case directBitmapSameTableInstance(field.Table, membership.Left.Table):
			values, ok := directBitmapProjectedValues(leftRowSet, field)
			if !ok || leftIndex >= len(values) {
				return qsbridge.ResultCell{}, directBitmapMembershipDiagnostics("correlated sibling membership left field is not materialized")
			}
			return values[leftIndex], nil
		case directBitmapSameTableInstance(field.Table, membership.Right.Table):
			values, ok := directBitmapProjectedValues(rightRowSet, field)
			if !ok || rightIndex >= len(values) {
				return qsbridge.ResultCell{}, directBitmapMembershipDiagnostics("correlated sibling membership right field is not materialized")
			}
			return values[rightIndex], nil
		default:
			return qsbridge.ResultCell{}, directBitmapMembershipDiagnostics("correlated sibling membership field is outside the membership domains")
		}
	}
	if literal, ok := directBitmapLiteralExpr(expr); ok {
		return directBitmapLiteralCell(literal), nil
	}
	return qsbridge.ResultCell{}, directBitmapMembershipDiagnostics("correlated sibling membership expression is not supported in this slice")
}

func directBitmapSameTableInstance(left qsbridge.TableInstance, right qsbridge.TableInstance) bool {
	if left.ID != "" && right.ID != "" {
		return left.ID == right.ID
	}
	return strings.EqualFold(left.RefName(), right.RefName()) && strings.EqualFold(left.Table, right.Table)
}

func directBitmapMembershipPredicateCanLower(predicate qsbridge.Predicate) bool {
	if predicate.Placement == qsbridge.PredicateResidualScan {
		return false
	}
	_, diagnostics, ok := directBitmapMembershipPredicateFragment(predicate)
	return ok && !diagnostics.BlocksNative()
}

func directBitmapMembershipRequiredFields(expr qsbridge.Expr) []qsbridge.FieldRef {
	fields := make([]qsbridge.FieldRef, 0)
	var walk func(qsbridge.Expr)
	walk = func(expr qsbridge.Expr) {
		switch value := expr.(type) {
		case qsbridge.FieldExpr:
			fields = append(fields, value.Ref)
		case *qsbridge.FieldExpr:
			if value != nil {
				fields = append(fields, value.Ref)
			}
		case qsbridge.BinaryExpr:
			walk(value.Left)
			walk(value.Right)
		case *qsbridge.BinaryExpr:
			if value != nil {
				walk(value.Left)
				walk(value.Right)
			}
		case qsbridge.CallExpr:
			for _, arg := range value.Args {
				walk(arg)
			}
		case *qsbridge.CallExpr:
			if value != nil {
				for _, arg := range value.Args {
					walk(arg)
				}
			}
		case qsbridge.ListExpr:
			for _, item := range value.Items {
				walk(item)
			}
		case *qsbridge.ListExpr:
			if value != nil {
				for _, item := range value.Items {
					walk(item)
				}
			}
		}
	}
	walk(expr)
	return fields
}

func directBitmapMembershipProjectionField(field qsbridge.FieldRef) qsbridge.QuantaProjectionField {
	return qsbridge.QuantaProjectionField{
		Index:        field.Table.Table,
		Role:         qsbridge.TableInstanceID(materializationFieldRole(field.Table.Table, field)),
		Field:        directBitmapFieldPhysicalName(field),
		Type:         field.Type,
		PhysicalName: field.PhysicalName,
		Roles:        field.Roles,
		Visible:      false,
	}
}

func directBitmapMembershipHasProjectionField(fields []qsbridge.QuantaProjectionField, want qsbridge.QuantaProjectionField) bool {
	for _, field := range fields {
		if field.Index == want.Index && field.Field == want.Field && field.PhysicalName == want.PhysicalName {
			return true
		}
	}
	return false
}

func directBitmapMembershipLiteralBigInt(expr qsbridge.Expr) (*big.Int, bool) {
	literal, ok := directBitmapLiteralExpr(expr)
	if !ok {
		return nil, false
	}
	switch value := literal.Value.(type) {
	case int:
		return big.NewInt(int64(value)), true
	case int64:
		return big.NewInt(value), true
	case uint64:
		return new(big.Int).SetUint64(value), true
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, false
		}
		return big.NewInt(parsed), true
	default:
		return nil, false
	}
}

func directBitmapMembershipBSIOp(op qsbridge.BinaryOp) qsbridge.QuantaBSIOp {
	switch op {
	case qsbridge.BinaryOpEqual:
		return qsbridge.QuantaBSIOpEQ
	case qsbridge.BinaryOpGreater:
		return qsbridge.QuantaBSIOpGT
	case qsbridge.BinaryOpGreaterEqual:
		return qsbridge.QuantaBSIOpGE
	case qsbridge.BinaryOpLess:
		return qsbridge.QuantaBSIOpLT
	case qsbridge.BinaryOpLessEqual:
		return qsbridge.QuantaBSIOpLE
	default:
		return qsbridge.QuantaBSIOpEQ
	}
}

func directBitmapMembershipDiagnostics(message string) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, fmt.Sprintf("membership filter execution: %s", message)),
	}
}

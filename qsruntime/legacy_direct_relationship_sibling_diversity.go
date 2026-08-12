package qsruntime

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// LegacyDirectSharedRelationshipSiblingDiversityReader provides the
// same-parent/different-value membership shortcut for shared/cluster runtimes.
// It builds a process-local whole-bucket summary through the existing direct
// bitmap and projection paths, then answers each query by intersecting that
// summary with the current candidate rows.
type LegacyDirectSharedRelationshipSiblingDiversityReader struct {
	Sessions   DirectSessionProvider
	Projection NativeProjectionBSIReader

	mu        sync.Mutex
	summaries map[string]legacyDirectRelationshipSiblingDiversitySummary
}

type legacyDirectRelationshipSiblingDiversitySummary struct {
	diverseRows       *roaring64.Bitmap
	rows              uint64
	values            uint64
	projectionRows    uint64
	groups            uint64
	diverseGroups     uint64
	buildElapsed      time.Duration
	projectionElapsed time.Duration
	evaluationElapsed time.Duration
}

type legacyDirectRelationshipSiblingDiversityGroup struct {
	rows     []qsbridge.QuantaRownum
	distinct map[int64]struct{}
}

// ReadRelationshipSiblingDiversityCandidates returns candidate rows whose
// complete parent bucket contains at least one row with a different value.
func (r *LegacyDirectSharedRelationshipSiblingDiversityReader) ReadRelationshipSiblingDiversityCandidates(ctx context.Context, read RelationshipSiblingDiversityReadRequest) (RelationshipSiblingDiversityReadResult, qsbridge.DiagnosticSet, bool, error) {
	start := time.Now()
	result := RelationshipSiblingDiversityReadResult{
		Mode:          "shared_projection_sibling_diversity_build",
		CandidateRows: uint64(len(read.CandidateRows)),
	}
	if strings.TrimSpace(read.Index) == "" || strings.TrimSpace(read.ParentField) == "" || strings.TrimSpace(read.ValueField) == "" {
		result.Reason = "missing_field"
		return result, nil, false, nil
	}
	if r == nil || r.Sessions == nil || r.Projection == nil {
		result.Reason = "no_reader"
		return result, nil, false, nil
	}
	if len(read.CandidateRows) == 0 {
		result.LookupElapsed = time.Since(start)
		result.Candidates = qsbridge.QuantaCandidateSet{Index: read.Index}
		return result, nil, true, nil
	}

	summary, cacheHit, diagnostics, ok, err := r.summary(ctx, read)
	if err != nil || diagnostics.BlocksNative() {
		return result, diagnostics, ok, err
	}
	if !ok {
		if strings.TrimSpace(result.Reason) == "" {
			result.Reason = "physical_unavailable"
		}
		return result, diagnostics, false, nil
	}

	result.Rows = summary.rows
	result.Values = summary.values
	result.ProjectionRows = summary.projectionRows
	result.Groups = summary.groups
	result.DiverseGroups = summary.diverseGroups
	result.CacheHit = cacheHit
	if cacheHit {
		result.Mode = "shared_projection_sibling_diversity_cache_hit"
	} else {
		result.BuildElapsed = summary.buildElapsed
		result.ProjectionElapsed = summary.projectionElapsed
		result.EvaluationElapsed = summary.evaluationElapsed
	}

	candidates := legacyDirectRelationshipBitmap(read.CandidateRows)
	targetRows := legacyDirectRelationshipSiblingDiversityCandidateRows(summary.diverseRows, candidates)
	result.TargetRows = uint64(len(targetRows))
	result.LookupElapsed = time.Since(start)
	result.Candidates = qsbridge.QuantaCandidateSet{
		Index:   read.Index,
		Rownums: targetRows,
	}
	return result, nil, true, nil
}

func (r *LegacyDirectSharedRelationshipSiblingDiversityReader) summary(ctx context.Context, read RelationshipSiblingDiversityReadRequest) (legacyDirectRelationshipSiblingDiversitySummary, bool, qsbridge.DiagnosticSet, bool, error) {
	key := legacyDirectRelationshipSiblingDiversitySummaryKey(read)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.summaries != nil {
		if summary, ok := r.summaries[key]; ok {
			return summary, true, nil, true, nil
		}
	}
	summary, diagnostics, ok, err := r.buildSummary(ctx, read)
	if err != nil || diagnostics.BlocksNative() || !ok {
		return summary, false, diagnostics, ok, err
	}
	if r.summaries == nil {
		r.summaries = make(map[string]legacyDirectRelationshipSiblingDiversitySummary)
	}
	r.summaries[key] = summary
	return summary, false, nil, true, nil
}

func (r *LegacyDirectSharedRelationshipSiblingDiversityReader) buildSummary(ctx context.Context, read RelationshipSiblingDiversityReadRequest) (legacyDirectRelationshipSiblingDiversitySummary, qsbridge.DiagnosticSet, bool, error) {
	buildStart := time.Now()
	rows, diagnostics, err := r.readParentRows(ctx, read)
	if err != nil || diagnostics.BlocksNative() {
		return legacyDirectRelationshipSiblingDiversitySummary{}, diagnostics, true, err
	}
	summary := legacyDirectRelationshipSiblingDiversitySummary{
		diverseRows:    roaring64.NewBitmap(),
		rows:           uint64(len(rows)),
		projectionRows: uint64(len(rows)),
	}
	if len(rows) == 0 {
		summary.buildElapsed = time.Since(buildStart)
		return summary, nil, true, nil
	}

	projectionStart := time.Now()
	parentProjection, valueProjection, diagnostics, err := r.readParentAndValueProjections(ctx, read, rows)
	summary.projectionElapsed = time.Since(projectionStart)
	if err != nil || diagnostics.BlocksNative() {
		return summary, diagnostics, true, err
	}
	if parentProjection.BSI == nil || valueProjection.BSI == nil {
		return summary, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "sibling diversity projection returned nil BSI"),
		}, true, nil
	}

	evaluationStart := time.Now()
	groups := legacyDirectRelationshipSiblingDiversityGroups(rows, parentProjection.BSI, valueProjection.BSI)
	summary.groups = uint64(len(groups))
	summary.values = summary.groups
	for _, group := range groups {
		if len(group.distinct) <= 1 {
			continue
		}
		summary.diverseGroups++
		for _, row := range group.rows {
			summary.diverseRows.Add(uint64(row))
		}
	}
	summary.evaluationElapsed = time.Since(evaluationStart)
	summary.buildElapsed = time.Since(buildStart)
	return summary, nil, true, nil
}

func (r *LegacyDirectSharedRelationshipSiblingDiversityReader) readParentRows(ctx context.Context, read RelationshipSiblingDiversityReadRequest) ([]qsbridge.QuantaRownum, qsbridge.DiagnosticSet, error) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     read.Index,
			Field:     read.ParentField,
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	session, diagnostics, err := r.Sessions.BorrowDirectSession(ctx, request)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	if session == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "sibling diversity reader received nil session"),
		}, nil
	}
	result, queryDiagnostics, queryErr := session.QueryBitmap(ctx, request)
	releaseDiagnostics := session.Release(ctx)
	diagnostics = append(diagnostics, queryDiagnostics...)
	diagnostics = append(diagnostics, releaseDiagnostics...)
	if queryErr != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, queryErr
	}
	return append([]qsbridge.QuantaRownum(nil), result.Rownums...), diagnostics, nil
}

func (r *LegacyDirectSharedRelationshipSiblingDiversityReader) readParentAndValueProjections(ctx context.Context, read RelationshipSiblingDiversityReadRequest, rows []qsbridge.QuantaRownum) (NativeProjectionBSIReadResult, NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	requests := []NativeProjectionBSIReadRequest{
		legacyDirectRelationshipSiblingDiversityProjectionRequest(read, read.ParentField, rows),
		legacyDirectRelationshipSiblingDiversityProjectionRequest(read, read.ValueField, rows),
	}
	if batch, ok := r.Projection.(NativeProjectionBSIBatchReader); ok {
		results, diagnostics, err := batch.ReadProjectionBSIs(ctx, requests)
		if err != nil || diagnostics.BlocksNative() {
			return NativeProjectionBSIReadResult{}, NativeProjectionBSIReadResult{}, diagnostics, err
		}
		if len(results) != len(requests) {
			return NativeProjectionBSIReadResult{}, NativeProjectionBSIReadResult{}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "sibling diversity batch projection returned "+strconv.Itoa(len(results))+" field reads for "+strconv.Itoa(len(requests))+" requests"),
			}, nil
		}
		return results[0], results[1], diagnostics, nil
	}
	parent, diagnostics, err := r.Projection.ReadProjectionBSI(ctx, requests[0])
	if err != nil || diagnostics.BlocksNative() {
		return parent, NativeProjectionBSIReadResult{}, diagnostics, err
	}
	value, valueDiagnostics, err := r.Projection.ReadProjectionBSI(ctx, requests[1])
	diagnostics = append(diagnostics, valueDiagnostics...)
	return parent, value, diagnostics, err
}

func legacyDirectRelationshipSiblingDiversityProjectionRequest(read RelationshipSiblingDiversityReadRequest, field string, rows []qsbridge.QuantaRownum) NativeProjectionBSIReadRequest {
	return NativeProjectionBSIReadRequest{
		Index: read.Index,
		Field: qsbridge.QuantaProjectionField{
			Index:        read.Index,
			Field:        field,
			PhysicalName: field,
		},
		PhysicalField:   field,
		Rownums:         append([]qsbridge.QuantaRownum(nil), rows...),
		FromEpochMillis: read.FromEpochMillis,
		ToEpochMillis:   read.ToEpochMillis,
	}
}

func legacyDirectRelationshipSiblingDiversityGroups(rows []qsbridge.QuantaRownum, parentBSI, valueBSI *roaring64.BSI) map[int64]legacyDirectRelationshipSiblingDiversityGroup {
	parentValues := parentBSI.GetBigValues(nativeProjectionRownumColumnIDs(rows))
	values := valueBSI.GetBigValues(nativeProjectionRownumColumnIDs(rows))
	groups := make(map[int64]legacyDirectRelationshipSiblingDiversityGroup)
	for i, row := range rows {
		if i >= len(parentValues) || i >= len(values) || parentValues[i] == nil || values[i] == nil || !parentValues[i].IsInt64() || !values[i].IsInt64() {
			continue
		}
		parent := parentValues[i].Int64()
		value := values[i].Int64()
		group := groups[parent]
		if group.distinct == nil {
			group.distinct = make(map[int64]struct{}, 2)
		}
		group.rows = append(group.rows, row)
		group.distinct[value] = struct{}{}
		groups[parent] = group
	}
	return groups
}

func legacyDirectRelationshipSiblingDiversityCandidateRows(diverseRows, candidates *roaring64.Bitmap) []qsbridge.QuantaRownum {
	if diverseRows == nil || diverseRows.IsEmpty() || candidates == nil || candidates.IsEmpty() {
		return nil
	}
	matched := roaring64.And(diverseRows, candidates)
	if matched == nil || matched.IsEmpty() {
		return nil
	}
	rows := matched.ToArray()
	sort.Slice(rows, func(i, j int) bool { return rows[i] < rows[j] })
	result := make([]qsbridge.QuantaRownum, 0, len(rows))
	for _, row := range rows {
		result = append(result, qsbridge.QuantaRownum(row))
	}
	return result
}

func legacyDirectRelationshipSiblingDiversitySummaryKey(read RelationshipSiblingDiversityReadRequest) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%d", read.Index, read.ParentField, read.ValueField, read.FromEpochMillis, read.ToEpochMillis)
}

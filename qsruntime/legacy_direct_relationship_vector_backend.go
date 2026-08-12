package qsruntime

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/source"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

const legacyDirectRelationshipVectorValueSetScanMaxRowsPerSourceValue = 512
const legacyDirectRelationshipReverseArtifactMaxSourceCoverageNumerator = 1
const legacyDirectRelationshipReverseArtifactMaxSourceCoverageDenominator = 2

// LegacyDirectBitIndexRelationshipVectorBackend reads relationship-vector BSIs through the legacy BitIndex.
type LegacyDirectBitIndexRelationshipVectorBackend struct {
	Source                             *source.QuantaSource
	Sessions                           DirectSessionProvider
	TableCache                         *core.TableCacheStruct
	ProjectionReader                   legacyDirectRelationshipVectorProjectionReader
	SourceKeyReader                    LegacyDirectRelationshipVectorSourceKeyReader
	ProjectionCache                    *LegacyDirectRelationshipVectorProjectionCache
	PreferDirectParentToChildCandidate bool
	ReverseArtifacts                   *RelationshipVectorReverseArtifactManager
	ReverseArtifactCandidateReader     LegacyDirectRelationshipVectorReverseArtifactCandidateReader
}

// LegacyDirectFilterTreeAdapter wires grouped-filter evaluation to relationship-vector normalization.
func LegacyDirectFilterTreeAdapter(sessions DirectSessionProvider, source *source.QuantaSource, tableCache *core.TableCacheStruct, materializer ProjectionMaterializer, materialization ProjectionMaterializationKernel, reverseArtifacts *RelationshipVectorReverseArtifactManager) DirectBitmapFilterTreeAdapter {
	reader := &LegacyDirectRelationshipVectorReader{
		Backend: LegacyDirectBitIndexRelationshipVectorBackend{
			Source:           source,
			Sessions:         sessions,
			TableCache:       tableCache,
			ReverseArtifacts: reverseArtifacts,
		},
	}
	return DirectBitmapFilterTreeAdapter{
		Sessions:        sessions,
		Materializer:    materializer,
		Materialization: materialization,
		Normalizer: DirectBitmapFilterDomainNormalizationExecutor{
			Sessions:        sessions,
			Reader:          reader,
			Materialization: materialization,
		},
	}
}

// ReadRelationshipVectorCandidates translates source rownums through a child FK relationship-vector BSI.
func (b LegacyDirectBitIndexRelationshipVectorBackend) ReadRelationshipVectorCandidates(ctx context.Context, read LegacyDirectRelationshipVectorReadRequest) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	result, diagnostics, err := b.ReadRelationshipVectorCandidateResult(ctx, read)
	return result.TargetCandidates, diagnostics, err
}

// ReadRelationshipVectorCandidateResult translates source rownums and records FK projection/expansion timings.
func (b LegacyDirectBitIndexRelationshipVectorBackend) ReadRelationshipVectorCandidateResult(ctx context.Context, read LegacyDirectRelationshipVectorReadRequest) (qsbridge.FilterDomainRelationshipVectorResult, qsbridge.DiagnosticSet, error) {
	sourceValueResult, diagnostics, err := b.relationshipVectorSourceValues(ctx, read)
	if err != nil || diagnostics.BlocksNative() {
		return qsbridge.FilterDomainRelationshipVectorResult{
			SourceKeyProjectionUsed:    sourceValueResult.ProjectionUsed,
			SourceKeyProjectionReason:  sourceValueResult.ProjectionReason,
			SourceKeyProjectionElapsed: sourceValueResult.ProjectionElapsed,
			SourceValueCount:           sourceValueResult.ValueCount,
		}, diagnostics, err
	}
	parentToChild := legacyDirectRelationshipVectorParentToChildRequest(read)
	projectionCacheKey := b.relationshipVectorProjectionCacheKey(read)
	candidateCacheKey := b.relationshipVectorCandidateCacheKey(read, projectionCacheKey)
	if parentToChild {
		candidateStart := time.Now()
		if candidates, cacheMode, ok := b.cachedRelationshipVectorCandidates(ctx, candidateCacheKey, read.TargetDomain, sourceValueResult.Values, read.AllowCandidateSuperset); ok {
			return qsbridge.FilterDomainRelationshipVectorResult{
				TargetCandidates:           candidates,
				VectorIndex:                read.VectorIndex,
				VectorField:                read.VectorField,
				Direction:                  read.Direction,
				SourceKeyProjectionUsed:    sourceValueResult.ProjectionUsed,
				SourceKeyProjectionReason:  sourceValueResult.ProjectionReason,
				SourceKeyProjectionElapsed: sourceValueResult.ProjectionElapsed,
				SourceValueCount:           sourceValueResult.ValueCount,
				CandidateCacheHit:          true,
				CandidateCacheMode:         cacheMode,
				CandidateMode:              "candidate_cache",
				CandidateElapsed:           time.Since(candidateStart),
			}, nil, nil
		}
		if candidates, _, artifactTiming, artifactDiagnostics, artifactErr, ok := b.readRelationshipVectorReverseArtifactCandidates(ctx, projectionCacheKey, read, sourceValueResult.Values); ok {
			return qsbridge.FilterDomainRelationshipVectorResult{
				TargetCandidates:           candidates,
				VectorIndex:                read.VectorIndex,
				VectorField:                read.VectorField,
				Direction:                  read.Direction,
				SourceKeyProjectionUsed:    sourceValueResult.ProjectionUsed,
				SourceKeyProjectionReason:  sourceValueResult.ProjectionReason,
				SourceKeyProjectionElapsed: sourceValueResult.ProjectionElapsed,
				SourceValueCount:           sourceValueResult.ValueCount,
				CandidateCacheHit:          artifactTiming.CacheHit,
				CandidateCacheMode:         "reverse_artifact",
				CandidateMode:              artifactTiming.Mode,
				CandidateElapsed:           time.Since(candidateStart),
				CandidateScanElapsed:       artifactTiming.LookupElapsed,
			}, artifactDiagnostics, artifactErr
		}
		if b.shouldReadRelationshipVectorParentToChildCandidatesDirect(sourceValueResult.Values) {
			if candidates, candidateDiagnostics, candidateErr, ok := b.readRelationshipVectorParentToChildCandidatesDirect(ctx, read, sourceValueResult.Values); ok {
				if read.AllowCandidateSuperset && candidateErr == nil && !candidateDiagnostics.BlocksNative() {
					b.storeRelationshipVectorCandidateSuperset(ctx, candidateCacheKey, sourceValueResult.Values, candidates.Rownums)
				}
				return qsbridge.FilterDomainRelationshipVectorResult{
					TargetCandidates:           candidates,
					VectorIndex:                read.VectorIndex,
					VectorField:                read.VectorField,
					Direction:                  read.Direction,
					SourceKeyProjectionUsed:    sourceValueResult.ProjectionUsed,
					SourceKeyProjectionReason:  sourceValueResult.ProjectionReason,
					SourceKeyProjectionElapsed: sourceValueResult.ProjectionElapsed,
					SourceValueCount:           sourceValueResult.ValueCount,
					CandidateCacheMode:         "direct_query",
					CandidateMode:              "direct_batch_eq",
					CandidateElapsed:           time.Since(candidateStart),
				}, candidateDiagnostics, candidateErr
			}
		}
		if candidates, artifactTiming, ok := b.ReverseArtifacts.cachedCandidates(ctx, projectionCacheKey, read, sourceValueResult.Values); ok {
			return qsbridge.FilterDomainRelationshipVectorResult{
				TargetCandidates:           candidates,
				VectorIndex:                read.VectorIndex,
				VectorField:                read.VectorField,
				Direction:                  read.Direction,
				SourceKeyProjectionUsed:    sourceValueResult.ProjectionUsed,
				SourceKeyProjectionReason:  sourceValueResult.ProjectionReason,
				SourceKeyProjectionElapsed: sourceValueResult.ProjectionElapsed,
				SourceValueCount:           sourceValueResult.ValueCount,
				CandidateCacheMode:         "reverse_artifact",
				CandidateMode:              artifactTiming.Mode,
				CandidateElapsed:           time.Since(candidateStart),
				CandidateScanElapsed:       artifactTiming.LookupElapsed,
			}, nil, nil
		}
	}
	projector := b.ProjectionReader
	if projector == nil {
		projector = legacyDirectBitIndexRelationshipVectorProjectionReader{
			Source:     b.Source,
			TableCache: b.TableCache,
		}
	}
	projectionStart := time.Now()
	fkBSI, projectionCacheHit := b.cachedRelationshipVectorProjection(ctx, projectionCacheKey)
	if fkBSI == nil {
		fkBSI, diagnostics, err = projector.ReadRelationshipVectorProjection(ctx, read)
		if err == nil && !diagnostics.BlocksNative() && fkBSI != nil {
			b.storeRelationshipVectorProjection(ctx, projectionCacheKey, fkBSI)
		}
	}
	projectionElapsed := time.Since(projectionStart)
	if err != nil || diagnostics.BlocksNative() {
		return qsbridge.FilterDomainRelationshipVectorResult{
			ProjectionElapsed:  projectionElapsed,
			ProjectionCacheHit: projectionCacheHit,
		}, diagnostics, err
	}
	if fkBSI == nil {
		return qsbridge.FilterDomainRelationshipVectorResult{
				ProjectionElapsed:  projectionElapsed,
				ProjectionCacheHit: projectionCacheHit,
			}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship-vector projection returned nil BSI"),
			}, nil
	}
	candidateStart := time.Now()
	if parentToChild {
		if candidates, _, artifactTiming, artifactDiagnostics, artifactErr, ok := b.readRelationshipVectorReverseArtifactCandidates(ctx, projectionCacheKey, read, sourceValueResult.Values); ok {
			return qsbridge.FilterDomainRelationshipVectorResult{
				TargetCandidates:           candidates,
				VectorIndex:                read.VectorIndex,
				VectorField:                read.VectorField,
				Direction:                  read.Direction,
				ProjectionElapsed:          projectionElapsed,
				ProjectionCacheHit:         projectionCacheHit,
				SourceKeyProjectionUsed:    sourceValueResult.ProjectionUsed,
				SourceKeyProjectionReason:  sourceValueResult.ProjectionReason,
				SourceKeyProjectionElapsed: sourceValueResult.ProjectionElapsed,
				SourceValueCount:           sourceValueResult.ValueCount,
				CandidateCacheHit:          artifactTiming.CacheHit,
				CandidateCacheMode:         "reverse_artifact",
				CandidateMode:              artifactTiming.Mode,
				CandidateElapsed:           time.Since(candidateStart),
				CandidateScanElapsed:       artifactTiming.LookupElapsed,
			}, artifactDiagnostics, artifactErr
		}
		if candidates, artifactTiming, ok := b.ReverseArtifacts.candidates(ctx, projectionCacheKey, read, fkBSI, sourceValueResult.Values); ok {
			return qsbridge.FilterDomainRelationshipVectorResult{
				TargetCandidates:           candidates,
				VectorIndex:                read.VectorIndex,
				VectorField:                read.VectorField,
				Direction:                  read.Direction,
				ProjectionElapsed:          projectionElapsed,
				ProjectionCacheHit:         projectionCacheHit,
				SourceKeyProjectionUsed:    sourceValueResult.ProjectionUsed,
				SourceKeyProjectionReason:  sourceValueResult.ProjectionReason,
				SourceKeyProjectionElapsed: sourceValueResult.ProjectionElapsed,
				SourceValueCount:           sourceValueResult.ValueCount,
				CandidateCacheMode:         "reverse_artifact",
				CandidateMode:              artifactTiming.Mode,
				CandidateElapsed:           time.Since(candidateStart),
				CandidateScanElapsed:       artifactTiming.LookupElapsed,
			}, nil, nil
		}
	}
	candidates, candidateTiming := legacyDirectRelationshipVectorCandidateResult(read, fkBSI, sourceValueResult.Values)
	candidateElapsed := time.Since(candidateStart)
	if parentToChild {
		if targetValues, ok := legacyDirectRelationshipVectorTargetValues(fkBSI, candidates.Rownums); ok {
			b.storeRelationshipVectorCandidates(ctx, candidateCacheKey, sourceValueResult.Values, candidates.Rownums, targetValues)
		}
	}
	return qsbridge.FilterDomainRelationshipVectorResult{
		TargetCandidates:           candidates,
		VectorIndex:                read.VectorIndex,
		VectorField:                read.VectorField,
		Direction:                  read.Direction,
		ProjectionElapsed:          projectionElapsed,
		ProjectionCacheHit:         projectionCacheHit,
		SourceKeyProjectionUsed:    sourceValueResult.ProjectionUsed,
		SourceKeyProjectionReason:  sourceValueResult.ProjectionReason,
		SourceKeyProjectionElapsed: sourceValueResult.ProjectionElapsed,
		SourceValueCount:           sourceValueResult.ValueCount,
		CandidateCacheMode:         "miss",
		CandidateMode:              candidateTiming.Mode,
		CandidateElapsed:           candidateElapsed,
		BatchEqualElapsed:          candidateTiming.BatchEqualElapsed,
		CandidateScanElapsed:       candidateTiming.ScanElapsed,
	}, nil, nil
}

func (b LegacyDirectBitIndexRelationshipVectorBackend) readRelationshipVectorReverseArtifactCandidates(ctx context.Context, projectionKey string, read LegacyDirectRelationshipVectorReadRequest, sourceValues []int64) (qsbridge.QuantaCandidateSet, map[qsbridge.QuantaRownum]int64, relationshipVectorReverseArtifactTiming, qsbridge.DiagnosticSet, error, bool) {
	if b.ReverseArtifactCandidateReader == nil || !read.Edge.Capabilities.Has(qsbridge.RelationshipCapabilityChildExpansion) {
		return qsbridge.QuantaCandidateSet{}, nil, relationshipVectorReverseArtifactTiming{}, nil, nil, false
	}
	sourceValueCount := len(legacyDirectRelationshipUniqueInt64s(sourceValues))
	if stats, ok, err := b.relationshipVectorReverseArtifactStats(ctx, read); err == nil && ok &&
		legacyDirectRelationshipReverseArtifactSourceTooBroad(sourceValueCount, stats.Values) {
		timing := relationshipVectorReverseArtifactTiming{
			Mode:         "reverse_artifact_skip_unselective_source",
			CacheHit:     true,
			Rows:         stats.Rows,
			Values:       stats.Values,
			SourceValues: sourceValueCount,
		}
		legacyDirectRecordRelationshipVectorReverseArtifact(ctx, read, projectionKey, timing)
		return qsbridge.QuantaCandidateSet{}, nil, timing, nil, nil, false
	}
	result, diagnostics, ok, err := b.ReverseArtifactCandidateReader.ReadRelationshipVectorReverseArtifactCandidates(ctx, read, sourceValues)
	if err != nil || diagnostics.BlocksNative() {
		return result.Candidates, result.ParentValueByChild, relationshipVectorReverseArtifactTiming{
			Mode:          result.Mode,
			CacheHit:      result.CacheHit,
			LookupElapsed: result.LookupElapsed,
			Rows:          result.Rows,
			Values:        result.Values,
			SourceValues:  result.SourceValues,
			TargetRows:    int(result.TargetRows),
		}, diagnostics, err, true
	}
	if !ok {
		return qsbridge.QuantaCandidateSet{}, nil, relationshipVectorReverseArtifactTiming{}, diagnostics, err, false
	}
	timing := relationshipVectorReverseArtifactTiming{
		Mode:          result.Mode,
		CacheHit:      result.CacheHit,
		LookupElapsed: result.LookupElapsed,
		Rows:          result.Rows,
		Values:        result.Values,
		SourceValues:  result.SourceValues,
		TargetRows:    int(result.TargetRows),
	}
	legacyDirectRecordRelationshipVectorReverseArtifact(ctx, read, projectionKey, timing)
	return result.Candidates, result.ParentValueByChild, timing, diagnostics, err, true
}

func (b LegacyDirectBitIndexRelationshipVectorBackend) relationshipVectorReverseArtifactStats(ctx context.Context, read LegacyDirectRelationshipVectorReadRequest) (LegacyDirectRelationshipVectorReverseArtifactStats, bool, error) {
	statsReader, ok := b.ReverseArtifactCandidateReader.(LegacyDirectRelationshipVectorReverseArtifactStatsReader)
	if !ok {
		return LegacyDirectRelationshipVectorReverseArtifactStats{}, false, nil
	}
	return statsReader.RelationshipVectorReverseArtifactStats(ctx, read)
}

func legacyDirectRelationshipReverseArtifactSourceTooBroad(sourceValueCount int, artifactValues uint64) bool {
	if sourceValueCount <= 0 || artifactValues == 0 {
		return false
	}
	return uint64(sourceValueCount)*legacyDirectRelationshipReverseArtifactMaxSourceCoverageDenominator >
		artifactValues*legacyDirectRelationshipReverseArtifactMaxSourceCoverageNumerator
}

func (b LegacyDirectBitIndexRelationshipVectorBackend) shouldReadRelationshipVectorParentToChildCandidatesDirect(sourceValues []int64) bool {
	if b.Sessions == nil || len(sourceValues) > directBitmapMembershipMaxDynamicBatchEQValues {
		return false
	}
	if b.ProjectionReader == nil {
		return true
	}
	return b.PreferDirectParentToChildCandidate
}

// cachedRelationshipVectorProjection returns a projected FK BSI when request-scoped reuse is available.
func (b LegacyDirectBitIndexRelationshipVectorBackend) cachedRelationshipVectorProjection(ctx context.Context, key string) (*roaring64.BSI, bool) {
	detail := legacyDirectRelationshipProjectionCacheDetail(key)
	if cache := RelationshipVectorProjectionCacheFromContext(ctx); cache != nil {
		bsi, ok := cache.Get(key)
		recordQueryScratchpadCacheLookup(ctx, "relationship_vector_projection_cache", ok, relationshipVectorProjectionCacheMode(ok), detail)
		return bsi, ok
	}
	bsi, ok := b.ProjectionCache.Get(key)
	recordQueryScratchpadCacheLookup(ctx, "relationship_vector_projection_cache", ok, relationshipVectorProjectionCacheMode(ok), detail)
	return bsi, ok
}

// storeRelationshipVectorProjection stores a projected FK BSI when a request-scoped cache is present.
func (b LegacyDirectBitIndexRelationshipVectorBackend) storeRelationshipVectorProjection(ctx context.Context, key string, bsi *roaring64.BSI) {
	detail := legacyDirectRelationshipProjectionCacheDetail(key)
	if cache := RelationshipVectorProjectionCacheFromContext(ctx); cache != nil {
		cache.Put(key, bsi)
		recordQueryScratchpadCacheStore(ctx, "relationship_vector_projection_cache", detail)
		return
	}
	b.ProjectionCache.Put(key, bsi)
	recordQueryScratchpadCacheStore(ctx, "relationship_vector_projection_cache", detail)
}

func relationshipVectorProjectionCacheMode(hit bool) string {
	if hit {
		return "exact"
	}
	return "miss"
}

// relationshipVectorProjectionCacheKey identifies a projection read independent of source candidate values.
func (b LegacyDirectBitIndexRelationshipVectorBackend) relationshipVectorProjectionCacheKey(read LegacyDirectRelationshipVectorReadRequest) string {
	windowReader := legacyDirectBitIndexRelationshipVectorProjectionReader{TableCache: b.TableCache}
	fromTime, toTime := windowReader.legacyDirectRelationshipVectorProjectionWindow(read)
	return strings.Join([]string{
		read.VectorIndex,
		read.VectorField,
		strconv.FormatInt(fromTime, 10),
		strconv.FormatInt(toTime, 10),
		legacyDirectRelationshipVectorFoundSetCacheKey(legacyDirectRelationshipVectorProjectionFoundSet(read)),
	}, "\x00")
}

func (b LegacyDirectBitIndexRelationshipVectorBackend) relationshipVectorCandidateCacheKey(read LegacyDirectRelationshipVectorReadRequest, projectionKey string) string {
	return strings.Join([]string{
		"parent_to_child",
		read.SourceDomain,
		read.TargetDomain,
		string(read.Direction),
		projectionKey,
	}, "\x00")
}

func (b LegacyDirectBitIndexRelationshipVectorBackend) cachedRelationshipVectorCandidates(ctx context.Context, key string, targetDomain string, sourceValues []int64, allowSuperset bool) (qsbridge.QuantaCandidateSet, string, bool) {
	cache := RelationshipVectorCandidateCacheFromContext(ctx)
	if cache == nil {
		recordQueryScratchpadCacheLookup(ctx, "relationship_vector_candidate_cache", false, "cache_absent", legacyDirectRelationshipProjectionCacheDetail(key))
		return qsbridge.QuantaCandidateSet{}, "cache_absent", false
	}
	var candidates qsbridge.QuantaCandidateSet
	var mode string
	var ok bool
	if allowSuperset {
		candidates, mode, ok = cache.GetAllowingSuperset(key, targetDomain, sourceValues)
	} else {
		candidates, mode, ok = cache.Get(key, targetDomain, sourceValues)
	}
	recordQueryScratchpadCacheLookup(ctx, "relationship_vector_candidate_cache", ok, mode, legacyDirectRelationshipProjectionCacheDetail(key))
	return candidates, mode, ok
}

func (b LegacyDirectBitIndexRelationshipVectorBackend) storeRelationshipVectorCandidates(ctx context.Context, key string, sourceValues []int64, rownums []qsbridge.QuantaRownum, targetValues []int64) {
	cache := RelationshipVectorCandidateCacheFromContext(ctx)
	if cache == nil {
		return
	}
	cache.Set(key, sourceValues, rownums, targetValues)
	recordQueryScratchpadCacheStore(ctx, "relationship_vector_candidate_cache", legacyDirectRelationshipProjectionCacheDetail(key))
}

func (b LegacyDirectBitIndexRelationshipVectorBackend) storeRelationshipVectorCandidateSuperset(ctx context.Context, key string, sourceValues []int64, rownums []qsbridge.QuantaRownum) {
	cache := RelationshipVectorCandidateCacheFromContext(ctx)
	if cache == nil {
		return
	}
	cache.SetSuperset(key, sourceValues, rownums)
	recordQueryScratchpadCacheStore(ctx, "relationship_vector_candidate_cache", legacyDirectRelationshipProjectionCacheDetail(key))
}

func (b LegacyDirectBitIndexRelationshipVectorBackend) readRelationshipVectorParentToChildCandidatesDirect(ctx context.Context, read LegacyDirectRelationshipVectorReadRequest, sourceValues []int64) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error, bool) {
	if b.Sessions == nil {
		return qsbridge.QuantaCandidateSet{}, nil, nil, false
	}
	if len(sourceValues) == 0 {
		return qsbridge.QuantaCandidateSet{Index: read.TargetDomain}, nil, nil, true
	}
	if len(sourceValues) > directBitmapMembershipMaxDynamicBatchEQValues {
		return qsbridge.QuantaCandidateSet{}, nil, nil, false
	}
	values := make([]*big.Int, 0, len(sourceValues))
	for _, sourceValue := range sourceValues {
		values = append(values, big.NewInt(sourceValue))
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     read.VectorIndex,
			Field:     read.VectorField,
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpBatchEQ,
			Values:    values,
		}},
	})
	session, diagnostics, err := b.Sessions.BorrowDirectSession(ctx, request)
	if err != nil || diagnostics.BlocksNative() {
		return qsbridge.QuantaCandidateSet{}, diagnostics, err, true
	}
	if session == nil {
		return qsbridge.QuantaCandidateSet{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship-vector candidate query received nil session"),
		}, nil, true
	}
	result, queryDiagnostics, queryErr := session.QueryBitmap(ctx, request)
	releaseDiagnostics := session.Release(ctx)
	diagnostics = append(diagnostics, queryDiagnostics...)
	diagnostics = append(diagnostics, releaseDiagnostics...)
	if queryErr != nil || diagnostics.BlocksNative() {
		return qsbridge.QuantaCandidateSet{}, diagnostics, queryErr, true
	}
	return qsbridge.QuantaCandidateSet{
		Index:   read.TargetDomain,
		Rownums: append([]qsbridge.QuantaRownum(nil), result.Rownums...),
	}, diagnostics, nil, true
}

// legacyDirectRelationshipVectorFoundSetCacheKey produces a stable key for projection narrowing foundsets.
func legacyDirectRelationshipVectorFoundSetCacheKey(foundSet *roaring64.Bitmap) string {
	if foundSet == nil {
		return "all"
	}
	it := foundSet.Iterator()
	var builder strings.Builder
	builder.WriteString(strconv.FormatUint(foundSet.GetCardinality(), 10))
	builder.WriteByte(':')
	for it.HasNext() {
		builder.WriteString(strconv.FormatUint(it.Next(), 10))
		builder.WriteByte(',')
	}
	return builder.String()
}

func legacyDirectRelationshipProjectionCacheDetail(key string) string {
	parts := strings.Split(key, "\x00")
	if len(parts) != 5 {
		return "key_hash=" + legacyDirectRelationshipProjectionCacheKeyHash(key) +
			" key_len=" + strconv.Itoa(len(key))
	}
	scope := "foundset"
	rows := "0"
	foundSetKey := parts[4]
	switch {
	case foundSetKey == "all":
		scope = "all"
		rows = "all"
	default:
		if count, _, ok := strings.Cut(foundSetKey, ":"); ok {
			rows = count
		}
	}
	return "index=" + parts[0] +
		" field=" + parts[1] +
		" from=" + parts[2] +
		" to=" + parts[3] +
		" scope=" + scope +
		" rows=" + rows +
		" key_hash=" + legacyDirectRelationshipProjectionCacheKeyHash(key)
}

func legacyDirectRelationshipProjectionCacheKeyHash(key string) string {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key))
	return strconv.FormatUint(hash.Sum64(), 16)
}

type legacyDirectRelationshipVectorProjectionReader interface {
	// ReadRelationshipVectorProjection returns the child FK BSI for a prepared vector read.
	ReadRelationshipVectorProjection(context.Context, LegacyDirectRelationshipVectorReadRequest) (*roaring64.BSI, qsbridge.DiagnosticSet, error)
}

// LegacyDirectRelationshipVectorSourceKeyReader projects relationship-vector
// source key values when source rownums are not themselves the relationship key.
type LegacyDirectRelationshipVectorSourceKeyReader interface {
	// ReadRelationshipVectorSourceKeyValues returns relationship-vector values for source candidate rownums.
	ReadRelationshipVectorSourceKeyValues(context.Context, LegacyDirectRelationshipVectorReadRequest) ([]int64, qsbridge.DiagnosticSet, error)
}

type legacyDirectRelationshipVectorSourceKeyReader = LegacyDirectRelationshipVectorSourceKeyReader

type legacyDirectRelationshipVectorSourceValuesResult struct {
	Values            []int64
	ProjectionUsed    bool
	ProjectionReason  string
	ProjectionElapsed time.Duration
	ValueCount        int
}

type legacyDirectBitIndexRelationshipVectorProjectionReader struct {
	Source     *source.QuantaSource
	TableCache *core.TableCacheStruct
}

// ReadRelationshipVectorProjection borrows a legacy session and projects the requested child FK BSI.
func (r legacyDirectBitIndexRelationshipVectorProjectionReader) ReadRelationshipVectorProjection(ctx context.Context, read LegacyDirectRelationshipVectorReadRequest) (*roaring64.BSI, qsbridge.DiagnosticSet, error) {
	if r.Source == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship-vector backend has no source"),
		}, nil
	}
	vectorRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index:     read.VectorIndex,
		Field:     read.VectorField,
		Operation: qsbridge.QuantaOperationIntersect,
		NullCheck: true,
		Negate:    true,
	}}})
	provider := LegacyQuantaSourceSessionProvider{Source: r.Source}
	session, diagnostics, err := provider.BorrowDirectSession(ctx, vectorRequest)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	if session == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship-vector backend received nil session"),
		}, nil
	}
	defer session.Release(ctx)
	foundSet := legacyDirectRelationshipVectorProjectionFoundSet(read)
	if foundSet == nil {
		result, queryDiagnostics, queryErr := session.QueryBitmap(ctx, vectorRequest)
		if queryErr != nil || queryDiagnostics.BlocksNative() {
			return nil, queryDiagnostics, queryErr
		}
		foundSet = legacyDirectRelationshipBitmap(result.Rownums)
	}
	legacySession, ok := session.(LegacyQuantaSessionHandle)
	if !ok || legacySession.Session == nil || legacySession.Session.BitIndex == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship-vector backend has no bitmap index"),
		}, nil
	}
	fromTime, toTime := r.legacyDirectRelationshipVectorProjectionWindow(read)
	bsiByField, _, err := legacySession.Session.BitIndex.Projection(read.VectorIndex, []string{read.VectorField}, fromTime, toTime, foundSet, false)
	if err != nil {
		return nil, nil, err
	}
	fkBSI := bsiByField[read.VectorField]
	if fkBSI == nil {
		executor := LegacyDirectRelationshipVectorJoinExecutor{TableCache: r.TableCache}
		return nil, legacyDirectRelationshipDiagnostic(
			fmt.Sprintf("relationship-vector projection did not return %s.%s (%s)",
				read.VectorIndex,
				read.VectorField,
				legacyDirectRelationshipProjectionDebug(vectorRequest, read.VectorIndex, executor.legacyDirectCachedTable(read.VectorIndex), foundSet, fromTime, toTime, bsiByField),
			),
		), nil
	}
	return fkBSI, nil, nil
}

func (r legacyDirectBitIndexRelationshipVectorProjectionReader) legacyDirectRelationshipVectorProjectionWindow(read LegacyDirectRelationshipVectorReadRequest) (int64, int64) {
	executor := LegacyDirectRelationshipVectorJoinExecutor{TableCache: r.TableCache}
	return executor.legacyDirectRelationshipBroadVectorProjectionWindow(read.VectorIndex)
}

func legacyDirectRelationshipVectorProjectionFoundSet(read LegacyDirectRelationshipVectorReadRequest) *roaring64.Bitmap {
	if !strings.EqualFold(read.SourceDomain, read.VectorIndex) || len(read.SourceCandidates.Rownums) == 0 {
		return nil
	}
	return legacyDirectRelationshipBitmap(read.SourceCandidates.Rownums)
}

func (b LegacyDirectBitIndexRelationshipVectorBackend) relationshipVectorSourceValues(ctx context.Context, read LegacyDirectRelationshipVectorReadRequest) (legacyDirectRelationshipVectorSourceValuesResult, qsbridge.DiagnosticSet, error) {
	needsProjection, projectionReason := legacyDirectRelationshipVectorSourceKeyProjectionRequirement(read)
	if !needsProjection {
		values := legacyDirectRelationshipUniqueInt64s(legacyDirectRelationshipSignedIDs(read.SourceCandidates.Rownums))
		return legacyDirectRelationshipVectorSourceValuesResult{
			Values:           values,
			ProjectionReason: projectionReason,
			ValueCount:       len(values),
		}, nil, nil
	}
	reader := b.SourceKeyReader
	if reader == nil && b.Source != nil {
		reader = legacyDirectBitIndexRelationshipVectorSourceKeyReader{
			Source:     b.Source,
			TableCache: b.TableCache,
		}
	}
	if reader == nil {
		values := legacyDirectRelationshipUniqueInt64s(legacyDirectRelationshipSignedIDs(read.SourceCandidates.Rownums))
		return legacyDirectRelationshipVectorSourceValuesResult{
			Values:           values,
			ProjectionReason: "no_source_key_reader",
			ValueCount:       len(values),
		}, nil, nil
	}
	projectionStart := time.Now()
	values, diagnostics, err := reader.ReadRelationshipVectorSourceKeyValues(ctx, read)
	projectionElapsed := time.Since(projectionStart)
	if err != nil || diagnostics.BlocksNative() {
		return legacyDirectRelationshipVectorSourceValuesResult{
			ProjectionUsed:    true,
			ProjectionReason:  "source_key_projection_error",
			ProjectionElapsed: projectionElapsed,
		}, diagnostics, err
	}
	values = legacyDirectRelationshipUniqueInt64s(values)
	return legacyDirectRelationshipVectorSourceValuesResult{
		Values:            values,
		ProjectionUsed:    true,
		ProjectionReason:  "projected_source_key",
		ProjectionElapsed: projectionElapsed,
		ValueCount:        len(values),
	}, nil, nil
}

func legacyDirectRelationshipUniqueInt64s(values []int64) []int64 {
	if len(values) == 0 {
		return nil
	}
	unique := append([]int64(nil), values...)
	sort.Slice(unique, func(i, j int) bool { return unique[i] < unique[j] })
	write := 1
	for read := 1; read < len(unique); read++ {
		if unique[read] == unique[write-1] {
			continue
		}
		unique[write] = unique[read]
		write++
	}
	return unique[:write]
}

func legacyDirectRelationshipVectorNeedsSourceKeyProjection(read LegacyDirectRelationshipVectorReadRequest) bool {
	needsProjection, _ := legacyDirectRelationshipVectorSourceKeyProjectionRequirement(read)
	return needsProjection
}

func legacyDirectRelationshipVectorSourceKeyProjectionRequirement(read LegacyDirectRelationshipVectorReadRequest) (bool, string) {
	if !strings.EqualFold(read.VectorIndex, read.TargetDomain) {
		return false, "vector_not_target_domain"
	}
	if strings.EqualFold(read.SourceDomain, read.VectorIndex) {
		return false, "source_matches_vector_index"
	}
	field, ok := legacyDirectRelationshipVectorSourceKeyField(read)
	if !ok {
		return false, "source_key_field_not_found"
	}
	name := field.PhysicalName
	if name == "" {
		name = field.Name
	}
	if name == "" {
		return false, "source_key_field_empty"
	}
	if strings.EqualFold(name, "@rownum") {
		return false, "source_key_is_rownum"
	}
	return true, "projection_required"
}

func legacyDirectRelationshipVectorSourceKeyField(read LegacyDirectRelationshipVectorReadRequest) (qsbridge.FieldRef, bool) {
	if strings.EqualFold(read.Edge.Left.Table.Table, read.SourceDomain) {
		return read.Edge.Left, true
	}
	if strings.EqualFold(read.Edge.Right.Table.Table, read.SourceDomain) {
		return read.Edge.Right, true
	}
	return qsbridge.FieldRef{}, false
}

type legacyDirectBitIndexRelationshipVectorSourceKeyReader struct {
	Source     *source.QuantaSource
	TableCache *core.TableCacheStruct
}

// ReadRelationshipVectorSourceKeyValues projects source key values for source-domain rownums.
func (r legacyDirectBitIndexRelationshipVectorSourceKeyReader) ReadRelationshipVectorSourceKeyValues(ctx context.Context, read LegacyDirectRelationshipVectorReadRequest) ([]int64, qsbridge.DiagnosticSet, error) {
	if len(read.SourceCandidates.Rownums) == 0 {
		return nil, nil, nil
	}
	if r.Source == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship-vector source-key reader has no source"),
		}, nil
	}
	field, ok := legacyDirectRelationshipVectorSourceKeyField(read)
	if !ok {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "relationship-vector source-key reader cannot find source key field"),
		}, nil
	}
	index := field.Table.Table
	physicalField := field.PhysicalName
	if physicalField == "" {
		physicalField = field.Name
	}
	if index == "" || physicalField == "" {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "relationship-vector source-key reader requires a source key table and field"),
		}, nil
	}
	keyRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index:     index,
		Field:     physicalField,
		Operation: qsbridge.QuantaOperationIntersect,
		NullCheck: true,
		Negate:    true,
	}}})
	provider := LegacyQuantaSourceSessionProvider{Source: r.Source}
	session, diagnostics, err := provider.BorrowDirectSession(ctx, keyRequest)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	if session == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship-vector source-key reader received nil session"),
		}, nil
	}
	defer session.Release(ctx)
	legacySession, ok := session.(LegacyQuantaSessionHandle)
	if !ok || legacySession.Session == nil || legacySession.Session.BitIndex == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "relationship-vector source-key reader has no bitmap index"),
		}, nil
	}
	fromTime, toTime := r.legacyDirectRelationshipVectorSourceKeyWindow(index)
	foundSet := legacyDirectRelationshipBitmap(read.SourceCandidates.Rownums)
	bsiByField, _, err := legacySession.Session.BitIndex.Projection(index, []string{physicalField}, fromTime, toTime, foundSet, false)
	if err != nil {
		return nil, nil, err
	}
	keyBSI := bsiByField[physicalField]
	if keyBSI == nil {
		return nil, legacyDirectRelationshipDiagnostic(fmt.Sprintf("relationship-vector source-key projection did not return %s.%s", index, physicalField)), nil
	}
	return legacyDirectRelationshipVectorSourceKeyValues(read.SourceCandidates.Rownums, keyBSI), nil, nil
}

func (r legacyDirectBitIndexRelationshipVectorSourceKeyReader) legacyDirectRelationshipVectorSourceKeyWindow(index string) (int64, int64) {
	executor := LegacyDirectRelationshipVectorJoinExecutor{TableCache: r.TableCache}
	return executor.legacyDirectRelationshipBroadVectorProjectionWindow(index)
}

func legacyDirectRelationshipVectorSourceKeyValues(rownums []qsbridge.QuantaRownum, keyBSI *roaring64.BSI) []int64 {
	values := make([]int64, 0, len(rownums))
	for _, rownum := range rownums {
		value, ok := keyBSI.GetValue(uint64(rownum))
		if !ok {
			continue
		}
		values = append(values, value)
	}
	return values
}

func legacyDirectRelationshipVectorCandidates(read LegacyDirectRelationshipVectorReadRequest, fkBSI *roaring64.BSI) qsbridge.QuantaCandidateSet {
	candidates, _ := legacyDirectRelationshipVectorCandidateResult(read, fkBSI, legacyDirectRelationshipSignedIDs(read.SourceCandidates.Rownums))
	return candidates
}

type legacyDirectRelationshipVectorCandidateTiming struct {
	BatchEqualElapsed time.Duration
	ScanElapsed       time.Duration
	Mode              string
}

func legacyDirectRelationshipVectorCandidateResult(read LegacyDirectRelationshipVectorReadRequest, fkBSI *roaring64.BSI, sourceValues []int64) (qsbridge.QuantaCandidateSet, legacyDirectRelationshipVectorCandidateTiming) {
	switch {
	case strings.EqualFold(read.VectorIndex, read.TargetDomain):
		return legacyDirectRelationshipVectorParentToChildCandidateResult(read, fkBSI, sourceValues)
	case strings.EqualFold(read.VectorIndex, read.SourceDomain):
		return legacyDirectRelationshipVectorChildToParentCandidates(read, fkBSI), legacyDirectRelationshipVectorCandidateTiming{}
	}
	switch read.Direction {
	case qsbridge.FilterDomainRelationshipVectorDirectionLeftToRight:
		return legacyDirectRelationshipVectorChildToParentCandidates(read, fkBSI), legacyDirectRelationshipVectorCandidateTiming{}
	case qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft:
		return legacyDirectRelationshipVectorParentToChildCandidateResult(read, fkBSI, sourceValues)
	default:
		return qsbridge.QuantaCandidateSet{Index: read.TargetDomain}, legacyDirectRelationshipVectorCandidateTiming{}
	}
}

func legacyDirectRelationshipVectorParentToChildRequest(read LegacyDirectRelationshipVectorReadRequest) bool {
	switch {
	case strings.EqualFold(read.VectorIndex, read.TargetDomain):
		return true
	case strings.EqualFold(read.VectorIndex, read.SourceDomain):
		return false
	}
	return read.Direction == qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft
}

func legacyDirectRelationshipVectorChildToParentCandidates(read LegacyDirectRelationshipVectorReadRequest, fkBSI *roaring64.BSI) qsbridge.QuantaCandidateSet {
	rownums := make([]qsbridge.QuantaRownum, 0, len(read.SourceCandidates.Rownums))
	seen := map[qsbridge.QuantaRownum]bool{}
	for _, child := range read.SourceCandidates.Rownums {
		parent, ok := fkBSI.GetValue(uint64(child))
		if !ok {
			continue
		}
		parentRow := qsbridge.QuantaRownum(parent)
		if seen[parentRow] {
			continue
		}
		seen[parentRow] = true
		rownums = append(rownums, parentRow)
	}
	return qsbridge.QuantaCandidateSet{Index: read.TargetDomain, Rownums: rownums}
}

func legacyDirectRelationshipVectorParentToChildCandidates(read LegacyDirectRelationshipVectorReadRequest, fkBSI *roaring64.BSI) qsbridge.QuantaCandidateSet {
	candidates, _ := legacyDirectRelationshipVectorParentToChildCandidateResult(read, fkBSI, legacyDirectRelationshipSignedIDs(read.SourceCandidates.Rownums))
	return candidates
}

func legacyDirectRelationshipVectorParentToChildCandidateResult(read LegacyDirectRelationshipVectorReadRequest, fkBSI *roaring64.BSI, sourceValues []int64) (qsbridge.QuantaCandidateSet, legacyDirectRelationshipVectorCandidateTiming) {
	sourceValues = legacyDirectRelationshipUniqueInt64s(sourceValues)
	if len(sourceValues) == 0 {
		return qsbridge.QuantaCandidateSet{Index: read.TargetDomain}, legacyDirectRelationshipVectorCandidateTiming{}
	}
	if legacyDirectRelationshipVectorShouldUseValueSetScan(fkBSI, sourceValues) {
		scanStart := time.Now()
		candidates := legacyDirectRelationshipVectorParentToChildCandidateScan(read, fkBSI, sourceValues)
		return candidates, legacyDirectRelationshipVectorCandidateTiming{
			Mode:        "value_set_scan",
			ScanElapsed: time.Since(scanStart),
		}
	}
	batchStart := time.Now()
	matched := fkBSI.BatchEqual(0, sourceValues).Clone()
	batchElapsed := time.Since(batchStart)
	return qsbridge.QuantaCandidateSet{Index: read.TargetDomain, Rownums: legacyDirectRelationshipRownums(matched)}, legacyDirectRelationshipVectorCandidateTiming{
		BatchEqualElapsed: batchElapsed,
		Mode:              "batch_equal",
	}
}

func legacyDirectRelationshipVectorShouldUseValueSetScan(fkBSI *roaring64.BSI, sourceValues []int64) bool {
	if fkBSI == nil || fkBSI.GetExistenceBitmap() == nil {
		return false
	}
	existenceRows := fkBSI.GetExistenceBitmap().GetCardinality()
	maxScanRows := uint64(len(sourceValues)) * legacyDirectRelationshipVectorValueSetScanMaxRowsPerSourceValue
	return len(sourceValues) >= 64 && existenceRows > 0 && existenceRows <= maxScanRows
}

func legacyDirectRelationshipVectorParentToChildCandidateScan(read LegacyDirectRelationshipVectorReadRequest, fkBSI *roaring64.BSI, sourceValues []int64) qsbridge.QuantaCandidateSet {
	sourceSet := make(map[int64]struct{}, len(sourceValues))
	for _, value := range sourceValues {
		sourceSet[value] = struct{}{}
	}
	rownums := make([]qsbridge.QuantaRownum, 0)
	it := fkBSI.GetExistenceBitmap().Iterator()
	for it.HasNext() {
		rownum := it.Next()
		value, ok := fkBSI.GetValue(rownum)
		if !ok {
			continue
		}
		if _, found := sourceSet[value]; found {
			rownums = append(rownums, qsbridge.QuantaRownum(rownum))
		}
	}
	return qsbridge.QuantaCandidateSet{Index: read.TargetDomain, Rownums: rownums}
}

func legacyDirectRelationshipVectorTargetValues(fkBSI *roaring64.BSI, rownums []qsbridge.QuantaRownum) ([]int64, bool) {
	if fkBSI == nil {
		return nil, false
	}
	values := make([]int64, 0, len(rownums))
	for _, rownum := range rownums {
		value, ok := fkBSI.GetValue(uint64(rownum))
		if !ok {
			return nil, false
		}
		values = append(values, value)
	}
	return values, true
}

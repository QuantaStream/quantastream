package qsruntime

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

const (
	relationshipVectorReverseArtifactEnv         = "QUANTASTREAM_RELATIONSHIP_VECTOR_REVERSE_ARTIFACT"
	relationshipVectorReverseArtifactEdgeEnv     = "QUANTASTREAM_RELATIONSHIP_VECTOR_REVERSE_ARTIFACT_EDGE"
	relationshipVectorReverseArtifactDefaultEdge = "all"
)

// RelationshipVectorReverseArtifactMode selects the reversible POC execution mode.
type RelationshipVectorReverseArtifactMode string

const (
	// RelationshipVectorReverseArtifactDisabled disables reverse-artifact reads.
	RelationshipVectorReverseArtifactDisabled RelationshipVectorReverseArtifactMode = ""
	// RelationshipVectorReverseArtifactQuery builds the reverse artifact per lookup.
	RelationshipVectorReverseArtifactQuery RelationshipVectorReverseArtifactMode = "query"
	// RelationshipVectorReverseArtifactProcess builds once and reuses within the runtime process.
	RelationshipVectorReverseArtifactProcess RelationshipVectorReverseArtifactMode = "process"
)

// RelationshipVectorReverseArtifactConfig configures reverse relationship-vector artifacts.
type RelationshipVectorReverseArtifactConfig struct {
	Mode RelationshipVectorReverseArtifactMode
	Edge string
}

// RelationshipVectorReverseArtifactConfigFromEnv returns the reversible POC
// configuration from environment variables.
func RelationshipVectorReverseArtifactConfigFromEnv() RelationshipVectorReverseArtifactConfig {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv(relationshipVectorReverseArtifactEnv)))
	edge := strings.ToLower(strings.TrimSpace(os.Getenv(relationshipVectorReverseArtifactEdgeEnv)))
	return RelationshipVectorReverseArtifactConfig{
		Mode: RelationshipVectorReverseArtifactMode(mode),
		Edge: edge,
	}.withDefaults()
}

// RelationshipVectorReverseArtifactManager owns process-local reverse artifacts.
type RelationshipVectorReverseArtifactManager struct {
	config RelationshipVectorReverseArtifactConfig
	cache  relationshipVectorReverseArtifactProcessCache
}

type relationshipVectorReverseArtifactTiming struct {
	Mode                        string
	CacheHit                    bool
	BuildElapsed                time.Duration
	LookupElapsed               time.Duration
	FanoutElapsed               time.Duration
	ClientRPCElapsed            time.Duration
	MaxClientRPCElapsed         time.Duration
	ResponseMergeElapsed        time.Duration
	RowMergeElapsed             time.Duration
	ParentMergeElapsed          time.Duration
	SortElapsed                 time.Duration
	RowConversionElapsed        time.Duration
	MapConversionElapsed        time.Duration
	Rows                        uint64
	Values                      uint64
	SourceValues                int
	TargetRows                  int
	ParentValueEntries          uint64
	DuplicateParentValueEntries uint64
}

// LegacyDirectRelationshipVectorReverseArtifactCandidateResult is the
// physical-tier result for a prebuilt parent-value to child-row artifact.
type LegacyDirectRelationshipVectorReverseArtifactCandidateResult struct {
	Candidates                  qsbridge.QuantaCandidateSet
	ParentValueByChild          map[qsbridge.QuantaRownum]int64
	RawParentValueByChild       map[uint64]int64
	RawParentValues             []int64
	Mode                        string
	CacheHit                    bool
	Rows                        uint64
	Values                      uint64
	SourceValues                int
	TargetRows                  uint64
	ParentValueEntries          uint64
	DuplicateParentValueEntries uint64
	LookupElapsed               time.Duration
	FanoutElapsed               time.Duration
	ClientRPCElapsed            time.Duration
	MaxClientRPCElapsed         time.Duration
	ResponseMergeElapsed        time.Duration
	RowMergeElapsed             time.Duration
	ParentMergeElapsed          time.Duration
	SortElapsed                 time.Duration
	RowConversionElapsed        time.Duration
	MapConversionElapsed        time.Duration
}

// LegacyDirectRelationshipVectorReverseArtifactStats describes a maintained
// parent-to-child artifact without materializing lookup candidates.
type LegacyDirectRelationshipVectorReverseArtifactStats struct {
	Rows                        uint64
	Values                      uint64
	FanoutElapsed               time.Duration
	ClientRPCElapsed            time.Duration
	MaxClientRPCElapsed         time.Duration
	ResponseMergeElapsed        time.Duration
	RowMergeElapsed             time.Duration
	ParentMergeElapsed          time.Duration
	SortElapsed                 time.Duration
	ParentValueEntries          uint64
	DuplicateParentValueEntries uint64
}

// LegacyDirectRelationshipVectorReverseArtifactCandidateReader exposes
// schema-declared reverse relationship artifacts owned by the physical tier.
type LegacyDirectRelationshipVectorReverseArtifactCandidateReader interface {
	ReadRelationshipVectorReverseArtifactCandidates(
		context.Context,
		LegacyDirectRelationshipVectorReadRequest,
		[]int64,
	) (LegacyDirectRelationshipVectorReverseArtifactCandidateResult, qsbridge.DiagnosticSet, bool, error)
}

// LegacyDirectRelationshipVectorReverseArtifactStatsReader is optionally
// implemented by physical tiers that can expose artifact cardinality cheaply.
type LegacyDirectRelationshipVectorReverseArtifactStatsReader interface {
	RelationshipVectorReverseArtifactStats(
		context.Context,
		LegacyDirectRelationshipVectorReadRequest,
	) (LegacyDirectRelationshipVectorReverseArtifactStats, bool, error)
}

type relationshipVectorReverseArtifact struct {
	ByValue map[int64]*roaring64.Bitmap
	Rows    uint64
}

type relationshipVectorReverseArtifactProcessCache struct {
	mu      sync.Mutex
	entries map[string]*relationshipVectorReverseArtifact
}

// NewRelationshipVectorReverseArtifactManager creates a runtime-local reverse-artifact manager.
func NewRelationshipVectorReverseArtifactManager(config RelationshipVectorReverseArtifactConfig) *RelationshipVectorReverseArtifactManager {
	config = config.withDefaults()
	if !config.enabled() {
		return nil
	}
	return &RelationshipVectorReverseArtifactManager{
		config: config,
		cache: relationshipVectorReverseArtifactProcessCache{
			entries: make(map[string]*relationshipVectorReverseArtifact),
		},
	}
}

func (c RelationshipVectorReverseArtifactConfig) withDefaults() RelationshipVectorReverseArtifactConfig {
	mode := RelationshipVectorReverseArtifactMode(strings.ToLower(strings.TrimSpace(string(c.Mode))))
	switch mode {
	case "", "off", "false", "0":
		mode = RelationshipVectorReverseArtifactDisabled
	case "query", "build", "process", "cache":
	default:
		mode = RelationshipVectorReverseArtifactDisabled
	}
	if mode == "cache" {
		mode = RelationshipVectorReverseArtifactProcess
	}
	if mode == "build" {
		mode = RelationshipVectorReverseArtifactQuery
	}
	edge := strings.ToLower(strings.TrimSpace(c.Edge))
	if edge == "" {
		edge = relationshipVectorReverseArtifactDefaultEdge
	}
	return RelationshipVectorReverseArtifactConfig{Mode: mode, Edge: edge}
}

func (c RelationshipVectorReverseArtifactConfig) enabled() bool {
	return c.Mode == RelationshipVectorReverseArtifactQuery || c.Mode == RelationshipVectorReverseArtifactProcess
}

func (m *RelationshipVectorReverseArtifactManager) enabledFor(read LegacyDirectRelationshipVectorReadRequest) bool {
	if m == nil || !m.config.enabled() {
		return false
	}
	if !read.Edge.Capabilities.Has(qsbridge.RelationshipCapabilityChildExpansion) {
		return false
	}
	vectorEdge := strings.ToLower(read.VectorIndex + "." + read.VectorField)
	edge := strings.ToLower(strings.TrimSpace(m.config.Edge))
	return edge == "*" || edge == "all" || edge == vectorEdge
}

func relationshipVectorReverseArtifactProcessCacheKey(projectionKey string, read LegacyDirectRelationshipVectorReadRequest) string {
	return strings.Join([]string{
		read.SourceDomain,
		read.TargetDomain,
		string(read.Direction),
		read.VectorIndex,
		read.VectorField,
		projectionKey,
	}, "\x00")
}

func (c *relationshipVectorReverseArtifactProcessCache) get(key string) (*relationshipVectorReverseArtifact, bool) {
	if c == nil || key == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	artifact, ok := c.entries[key]
	return artifact, ok
}

func (c *relationshipVectorReverseArtifactProcessCache) set(key string, artifact *relationshipVectorReverseArtifact) {
	if c == nil || key == "" || artifact == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = artifact
}

func (c *relationshipVectorReverseArtifactProcessCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*relationshipVectorReverseArtifact)
}

func (m *RelationshipVectorReverseArtifactManager) clear() {
	if m == nil {
		return
	}
	m.cache.clear()
}

func (m *RelationshipVectorReverseArtifactManager) cachedCandidates(ctx context.Context, projectionKey string, read LegacyDirectRelationshipVectorReadRequest, sourceValues []int64) (qsbridge.QuantaCandidateSet, relationshipVectorReverseArtifactTiming, bool) {
	if !m.enabledFor(read) || m.config.Mode != RelationshipVectorReverseArtifactProcess {
		return qsbridge.QuantaCandidateSet{}, relationshipVectorReverseArtifactTiming{}, false
	}
	key := relationshipVectorReverseArtifactProcessCacheKey(projectionKey, read)
	artifact, ok := m.cache.get(key)
	if !ok {
		legacyDirectRecordRelationshipVectorReverseArtifact(ctx, read, projectionKey, relationshipVectorReverseArtifactTiming{
			Mode:         "reverse_artifact_process_miss",
			CacheHit:     false,
			SourceValues: len(sourceValues),
		})
		return qsbridge.QuantaCandidateSet{}, relationshipVectorReverseArtifactTiming{}, false
	}
	candidates, timing := legacyDirectRelationshipVectorReverseArtifactLookup(read, artifact, sourceValues)
	timing.Mode = "reverse_artifact_process_hit"
	timing.CacheHit = true
	legacyDirectRecordRelationshipVectorReverseArtifact(ctx, read, projectionKey, timing)
	return candidates, timing, true
}

func (m *RelationshipVectorReverseArtifactManager) candidates(ctx context.Context, projectionKey string, read LegacyDirectRelationshipVectorReadRequest, fkBSI *roaring64.BSI, sourceValues []int64) (qsbridge.QuantaCandidateSet, relationshipVectorReverseArtifactTiming, bool) {
	if !m.enabledFor(read) || fkBSI == nil {
		return qsbridge.QuantaCandidateSet{}, relationshipVectorReverseArtifactTiming{}, false
	}
	var artifact *relationshipVectorReverseArtifact
	cacheHit := false
	buildElapsed := time.Duration(0)
	if m.config.Mode == RelationshipVectorReverseArtifactProcess {
		key := relationshipVectorReverseArtifactProcessCacheKey(projectionKey, read)
		if cached, ok := m.cache.get(key); ok {
			artifact = cached
			cacheHit = true
		} else {
			buildStart := time.Now()
			artifact = buildRelationshipVectorReverseArtifact(fkBSI)
			buildElapsed = time.Since(buildStart)
			m.cache.set(key, artifact)
		}
	} else {
		buildStart := time.Now()
		artifact = buildRelationshipVectorReverseArtifact(fkBSI)
		buildElapsed = time.Since(buildStart)
	}
	candidates, timing := legacyDirectRelationshipVectorReverseArtifactLookup(read, artifact, sourceValues)
	timing.CacheHit = cacheHit
	timing.BuildElapsed = buildElapsed
	switch m.config.Mode {
	case RelationshipVectorReverseArtifactProcess:
		if cacheHit {
			timing.Mode = "reverse_artifact_process_hit"
		} else {
			timing.Mode = "reverse_artifact_process_build"
		}
	default:
		timing.Mode = "reverse_artifact_query"
	}
	legacyDirectRecordRelationshipVectorReverseArtifact(ctx, read, projectionKey, timing)
	return candidates, timing, true
}

func buildRelationshipVectorReverseArtifact(fkBSI *roaring64.BSI) *relationshipVectorReverseArtifact {
	artifact := &relationshipVectorReverseArtifact{ByValue: make(map[int64]*roaring64.Bitmap)}
	if fkBSI == nil || fkBSI.GetExistenceBitmap() == nil {
		return artifact
	}
	it := fkBSI.GetExistenceBitmap().Iterator()
	for it.HasNext() {
		rownum := it.Next()
		value, ok := fkBSI.GetValue(rownum)
		if !ok {
			continue
		}
		bitmap := artifact.ByValue[value]
		if bitmap == nil {
			bitmap = roaring64.NewBitmap()
			artifact.ByValue[value] = bitmap
		}
		bitmap.Add(rownum)
		artifact.Rows++
	}
	return artifact
}

func legacyDirectRelationshipVectorReverseArtifactLookup(read LegacyDirectRelationshipVectorReadRequest, artifact *relationshipVectorReverseArtifact, sourceValues []int64) (qsbridge.QuantaCandidateSet, relationshipVectorReverseArtifactTiming) {
	sourceValues = legacyDirectRelationshipUniqueInt64s(sourceValues)
	if artifact == nil || len(sourceValues) == 0 {
		return qsbridge.QuantaCandidateSet{Index: read.TargetDomain}, relationshipVectorReverseArtifactTiming{
			Rows:         relationshipVectorReverseArtifactRows(artifact),
			Values:       relationshipVectorReverseArtifactValues(artifact),
			SourceValues: len(sourceValues),
		}
	}
	lookupStart := time.Now()
	matched := roaring64.NewBitmap()
	for _, value := range sourceValues {
		if bitmap := artifact.ByValue[value]; bitmap != nil {
			matched.Or(bitmap)
		}
	}
	lookupElapsed := time.Since(lookupStart)
	rownums := legacyDirectRelationshipRownums(matched)
	return qsbridge.QuantaCandidateSet{Index: read.TargetDomain, Rownums: rownums}, relationshipVectorReverseArtifactTiming{
		LookupElapsed: lookupElapsed,
		Rows:          artifact.Rows,
		Values:        uint64(len(artifact.ByValue)),
		SourceValues:  len(sourceValues),
		TargetRows:    len(rownums),
	}
}

func relationshipVectorReverseArtifactRows(artifact *relationshipVectorReverseArtifact) uint64 {
	if artifact == nil {
		return 0
	}
	return artifact.Rows
}

func relationshipVectorReverseArtifactValues(artifact *relationshipVectorReverseArtifact) uint64 {
	if artifact == nil {
		return 0
	}
	return uint64(len(artifact.ByValue))
}

func legacyDirectRecordRelationshipVectorReverseArtifact(ctx context.Context, read LegacyDirectRelationshipVectorReadRequest, projectionKey string, timing relationshipVectorReverseArtifactTiming) {
	recorder := ExecutionInstrumentationFromContext(ctx)
	if recorder == nil {
		return
	}
	detail := strings.Join([]string{
		"mode=" + timing.Mode,
		"cache_hit=" + strconv.FormatBool(timing.CacheHit),
		"source=" + read.SourceDomain,
		"target=" + read.TargetDomain,
		"vector=" + read.VectorIndex + "." + read.VectorField,
		legacyDirectRelationshipProjectionCacheDetail(projectionKey),
	}, " ")
	recorder.ObserveEvent("relationship_reverse_artifact", "mode", timing.Mode, detail)
	recorder.ObserveCount("relationship_reverse_artifact", "source_values", uint64(timing.SourceValues), detail)
	recorder.ObserveCount("relationship_reverse_artifact", "target_rows", uint64(timing.TargetRows), detail)
	recorder.ObserveCount("relationship_reverse_artifact", "artifact_rows", timing.Rows, detail)
	recorder.ObserveCount("relationship_reverse_artifact", "artifact_values", timing.Values, detail)
	recorder.ObserveCount("relationship_reverse_artifact", "parent_value_entries", timing.ParentValueEntries, detail)
	recorder.ObserveCount("relationship_reverse_artifact", "duplicate_parent_value_entries", timing.DuplicateParentValueEntries, detail)
	recorder.ObserveDuration("relationship_reverse_artifact", "build_elapsed", timing.BuildElapsed, detail)
	recorder.ObserveDuration("relationship_reverse_artifact", "lookup_elapsed", timing.LookupElapsed, detail)
	recorder.ObserveDuration("relationship_reverse_artifact", "fanout_elapsed", timing.FanoutElapsed, detail)
	recorder.ObserveDuration("relationship_reverse_artifact", "client_rpc_elapsed", timing.ClientRPCElapsed, detail)
	recorder.ObserveDuration("relationship_reverse_artifact", "client_rpc_max_elapsed", timing.MaxClientRPCElapsed, detail)
	recorder.ObserveDuration("relationship_reverse_artifact", "response_merge_elapsed", timing.ResponseMergeElapsed, detail)
}

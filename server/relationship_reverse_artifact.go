package server

import (
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

const relationshipSiblingDiversitySkipProjectionRowsExceedsLimit = "projection_rows_exceeds_limit"

var relationshipSiblingDiversityMaxProjectionRows uint64 = 100000
var relationshipSiblingDiversitySummaryCacheEnabled = true

// RelationshipReverseArtifactStats summarizes one reverse-artifact lookup.
type RelationshipReverseArtifactStats struct {
	Rows          uint64
	Values        uint64
	SourceValues  int
	TargetRows    uint64
	LookupElapsed time.Duration
}

// RelationshipSiblingDiversityStats summarizes a same-parent/different-value
// sibling lookup served by a maintained reverse relationship artifact.
type RelationshipSiblingDiversityStats struct {
	Rows              uint64
	Values            uint64
	CandidateRows     uint64
	ProjectionRows    uint64
	TargetRows        uint64
	Groups            uint64
	DiverseGroups     uint64
	CacheHit          bool
	Mode              string
	SkipReason        string
	LookupElapsed     time.Duration
	BuildElapsed      time.Duration
	ProjectionElapsed time.Duration
	EvaluationElapsed time.Duration
	ProjectionStats   ProjectBSIStats
}

// RelationshipReverseArtifactSumGroup carries one mergeable grouped aggregate
// keyed by a relationship-vector value.
type RelationshipReverseArtifactSumGroup struct {
	ParentValue       uint64
	RepresentativeRow uint64
	Count             uint64
	Sum               *big.Int
}

// RelationshipReverseArtifactSumStats summarizes storage-side grouped work.
type RelationshipReverseArtifactSumStats struct {
	Rows              uint64
	Values            uint64
	SourceValues      int
	TargetRows        uint64
	Groups            int
	LookupElapsed     time.Duration
	ProjectionElapsed time.Duration
	AggregateElapsed  time.Duration
	ProjectionStats   ProjectBSIStats
}

type relationshipReverseArtifactSnapshot struct {
	Fields uint64
	Values uint64
	Rows   uint64
}

type relationshipReverseArtifactWarmStats struct {
	Fields        uint64
	Shards        uint64
	AggregateRows uint64
	OwnedValues   uint64
	OwnedRows     uint64
}

type relationshipReverseArtifactData struct {
	byValue map[int64]*roaring64.Bitmap
	rows    uint64
}

type relationshipReverseArtifact struct {
	relationshipReverseArtifactData
	byShard       map[int64]*relationshipReverseArtifactData
	owned         *relationshipReverseArtifactData
	ownedShardKey string
}

type relationshipSiblingDiversityArtifact struct {
	diverseRows    *roaring64.Bitmap
	rows           uint64
	values         uint64
	projectionRows uint64
	groups         uint64
	diverseGroups  uint64
}

type relationshipSiblingDiversityBuildGroup struct {
	rows []uint64
}

// RelationshipReverseArtifactCandidatesStorage returns child-domain rownums for the
// supplied parent-domain values when a schema-declared reverse artifact exists.
func (m *BitmapIndex) RelationshipReverseArtifactCandidatesStorage(index, field string, sourceValues []int64) ([]uint64, RelationshipReverseArtifactStats, bool, error) {
	rownums, _, stats, ok, err := m.RelationshipReverseArtifactCandidateValues(index, field, sourceValues)
	return rownums, stats, ok, err
}

// RelationshipReverseArtifactCandidateValues returns child-domain rownums plus
// the parent-domain value encoded for each returned child row.
func (m *BitmapIndex) RelationshipReverseArtifactCandidateValues(index, field string, sourceValues []int64) ([]uint64, map[uint64]int64, RelationshipReverseArtifactStats, bool, error) {
	return m.relationshipReverseArtifactCandidateValues(index, field, sourceValues, nil, true, true, true)
}

// RelationshipReverseArtifactCandidateValuesForRows returns child-domain
// rownums plus parent-domain values, retaining only rows in candidateRows when
// a candidate set is supplied.
func (m *BitmapIndex) RelationshipReverseArtifactCandidateValuesForRows(index, field string, sourceValues []int64, candidateRows []uint64) ([]uint64, map[uint64]int64, RelationshipReverseArtifactStats, bool, error) {
	return m.relationshipReverseArtifactCandidateValues(index, field, sourceValues, candidateRows, true, true, true)
}

// RelationshipReverseArtifactCandidateValuesUnordered returns child-domain
// rownums without sorting them. It is intended for callers that reconstruct
// child-domain order from their own candidate row set.
func (m *BitmapIndex) RelationshipReverseArtifactCandidateValuesUnordered(index, field string, sourceValues []int64) ([]uint64, map[uint64]int64, RelationshipReverseArtifactStats, bool, error) {
	return m.relationshipReverseArtifactCandidateValues(index, field, sourceValues, nil, false, true, true)
}

// RelationshipReverseArtifactCandidateValuesForRowsUnordered returns
// child-domain rownums plus parent-domain values, retaining only rows in
// candidateRows when supplied and leaving rows in artifact iteration order.
func (m *BitmapIndex) RelationshipReverseArtifactCandidateValuesForRowsUnordered(index, field string, sourceValues []int64, candidateRows []uint64) ([]uint64, map[uint64]int64, RelationshipReverseArtifactStats, bool, error) {
	return m.relationshipReverseArtifactCandidateValues(index, field, sourceValues, candidateRows, false, true, true)
}

type relationshipSiblingDiversityGroup struct {
	rows          []uint64
	candidateRows []uint64
}

// RelationshipSiblingDiversityCandidates returns candidate rows whose
// relationship-vector parent bucket contains at least one sibling row with a
// different value in valueField.
func (m *BitmapIndex) RelationshipSiblingDiversityCandidates(index, parentField, valueField string, fromTime, toTime int64, candidateRows []uint64) ([]uint64, RelationshipSiblingDiversityStats, bool, error) {
	start := time.Now()
	if strings.TrimSpace(valueField) == "" {
		return nil, RelationshipSiblingDiversityStats{}, false, fmt.Errorf("relationship sibling diversity requires value field")
	}
	if !m.relationshipReverseArtifactEnabled(index, parentField) {
		return nil, RelationshipSiblingDiversityStats{}, false, nil
	}
	candidateSet := relationshipReverseArtifactBitmap(candidateRows)
	if summary, stats, ok, err := m.relationshipSiblingDiversitySummary(index, parentField, valueField, fromTime, toTime, start); err != nil || ok {
		if err != nil {
			return nil, stats, true, err
		}
		rownums := relationshipSiblingDiversityCandidateRows(summary.diverseRows, candidateSet)
		stats.CandidateRows = uint64(len(candidateRows))
		stats.TargetRows = uint64(len(rownums))
		stats.LookupElapsed = time.Since(start)
		return rownums, stats, true, nil
	}

	m.reverseArtifactLock.Lock()
	fields := m.reverseArtifactCache[index]
	if fields == nil {
		m.reverseArtifactLock.Unlock()
		return nil, RelationshipSiblingDiversityStats{}, false, nil
	}
	artifact := fields[parentField]
	if artifact == nil {
		m.reverseArtifactLock.Unlock()
		return nil, RelationshipSiblingDiversityStats{}, false, nil
	}
	readable := m.relationshipReverseArtifactReadableData(index, parentField, artifact)
	if readable == nil {
		m.reverseArtifactLock.Unlock()
		return []uint64{}, RelationshipSiblingDiversityStats{
			CandidateRows: uint64(len(candidateRows)),
			LookupElapsed: time.Since(start),
		}, true, nil
	}
	groups := make([]relationshipSiblingDiversityGroup, 0)
	allRows := make([]uint64, 0)
	for _, bitmap := range readable.byValue {
		if bitmap == nil || bitmap.IsEmpty() {
			continue
		}
		var retained *roaring64.Bitmap
		if candidateSet != nil {
			if !bitmap.Intersects(candidateSet) {
				continue
			}
			retained = roaring64.And(bitmap, candidateSet)
			if retained == nil || retained.IsEmpty() {
				continue
			}
		}
		rows := bitmap.ToArray()
		candidates := rows
		if retained != nil {
			candidates = retained.ToArray()
		}
		groups = append(groups, relationshipSiblingDiversityGroup{
			rows:          rows,
			candidateRows: candidates,
		})
		allRows = append(allRows, rows...)
	}
	stats := RelationshipSiblingDiversityStats{
		Rows:           readable.rows,
		Values:         uint64(len(readable.byValue)),
		CandidateRows:  uint64(len(candidateRows)),
		ProjectionRows: uint64(len(allRows)),
		Groups:         uint64(len(groups)),
		LookupElapsed:  time.Since(start),
	}
	m.reverseArtifactLock.Unlock()
	if len(groups) == 0 || len(allRows) == 0 {
		return []uint64{}, stats, true, nil
	}
	if relationshipSiblingDiversityMaxProjectionRows > 0 && stats.ProjectionRows > relationshipSiblingDiversityMaxProjectionRows {
		stats.SkipReason = relationshipSiblingDiversitySkipProjectionRowsExceedsLimit
		return nil, stats, false, nil
	}

	projectionStart := time.Now()
	valuesByField, statsByField, err := m.ProjectBSIInt64ValuesWithStats(index, []string{valueField}, fromTime, toTime, allRows, relationshipReverseArtifactBitmap(allRows), false)
	if err != nil {
		return nil, stats, true, err
	}
	stats.ProjectionElapsed = time.Since(projectionStart)
	stats.ProjectionStats = statsByField[valueField]
	values := valuesByField[valueField]
	valueByRow := make(map[uint64]int64, len(allRows))
	for i, rownum := range allRows {
		if i >= len(values.Exists) || !values.Exists[i] {
			continue
		}
		valueByRow[rownum] = values.Values[i]
	}

	evaluationStart := time.Now()
	rowSet := make(map[uint64]struct{}, len(candidateRows))
	result := make([]uint64, 0, len(candidateRows))
	for _, group := range groups {
		distinct := make(map[int64]struct{}, 2)
		for _, rownum := range group.rows {
			value, ok := valueByRow[rownum]
			if !ok {
				continue
			}
			distinct[value] = struct{}{}
			if len(distinct) > 1 {
				break
			}
		}
		if len(distinct) <= 1 {
			continue
		}
		stats.DiverseGroups++
		for _, rownum := range group.candidateRows {
			if _, ok := valueByRow[rownum]; !ok {
				continue
			}
			if _, seen := rowSet[rownum]; seen {
				continue
			}
			rowSet[rownum] = struct{}{}
			result = append(result, rownum)
		}
	}
	if len(result) > 1 {
		sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	}
	stats.EvaluationElapsed = time.Since(evaluationStart)
	stats.TargetRows = uint64(len(result))
	return result, stats, true, nil
}

func (m *BitmapIndex) relationshipSiblingDiversitySummary(index, parentField, valueField string, fromTime, toTime int64, start time.Time) (*relationshipSiblingDiversityArtifact, RelationshipSiblingDiversityStats, bool, error) {
	if !relationshipSiblingDiversitySummaryCacheEnabled {
		return nil, RelationshipSiblingDiversityStats{}, false, nil
	}
	key := relationshipSiblingDiversityCacheKey(valueField, fromTime, toTime)
	if m.relationshipReverseArtifactShardOwnershipFilterEnabled() {
		key += "\x00owners=" + m.relationshipReverseArtifactReadableShardKey(index, parentField)
	}
	if summary, ok := m.relationshipSiblingDiversityCachedSummary(index, parentField, key); ok {
		return summary, relationshipSiblingDiversityStatsFromSummary(summary, true, "sibling_diversity_summary_cache_hit", start), true, nil
	}
	summary, stats, generation, ok, err := m.buildRelationshipSiblingDiversitySummary(index, parentField, valueField, fromTime, toTime, start)
	if err != nil || !ok {
		return nil, stats, ok, err
	}
	m.reverseArtifactLock.Lock()
	if existing, ok := m.relationshipSiblingDiversityCachedSummaryLocked(index, parentField, key); ok {
		m.reverseArtifactLock.Unlock()
		return existing, relationshipSiblingDiversityStatsFromSummary(existing, true, "sibling_diversity_summary_cache_hit", start), true, nil
	}
	if generation == m.relationshipSiblingDiversityGenerationLocked(index) {
		m.relationshipSiblingDiversityCache(index, parentField)[key] = summary
	} else {
		stats.Mode = "sibling_diversity_summary_cache_build_uncached"
	}
	m.reverseArtifactLock.Unlock()
	return summary, stats, true, nil
}

func (m *BitmapIndex) relationshipSiblingDiversityCachedSummary(index, parentField, key string) (*relationshipSiblingDiversityArtifact, bool) {
	m.reverseArtifactLock.RLock()
	defer m.reverseArtifactLock.RUnlock()
	return m.relationshipSiblingDiversityCachedSummaryLocked(index, parentField, key)
}

func (m *BitmapIndex) relationshipSiblingDiversityCachedSummaryLocked(index, parentField, key string) (*relationshipSiblingDiversityArtifact, bool) {
	if m.siblingDiversityCache == nil {
		return nil, false
	}
	fields := m.siblingDiversityCache[index]
	if fields == nil {
		return nil, false
	}
	artifacts := fields[parentField]
	if artifacts == nil {
		return nil, false
	}
	summary := artifacts[key]
	return summary, summary != nil
}

func (m *BitmapIndex) buildRelationshipSiblingDiversitySummary(index, parentField, valueField string, fromTime, toTime int64, start time.Time) (*relationshipSiblingDiversityArtifact, RelationshipSiblingDiversityStats, uint64, bool, error) {
	buildStart := time.Now()
	m.reverseArtifactLock.Lock()
	generation := m.relationshipSiblingDiversityGenerationLocked(index)
	fields := m.reverseArtifactCache[index]
	if fields == nil {
		m.reverseArtifactLock.Unlock()
		return nil, RelationshipSiblingDiversityStats{}, generation, false, nil
	}
	artifact := fields[parentField]
	if artifact == nil {
		m.reverseArtifactLock.Unlock()
		return nil, RelationshipSiblingDiversityStats{}, generation, false, nil
	}
	readable := m.relationshipReverseArtifactReadableData(index, parentField, artifact)
	if readable == nil {
		m.reverseArtifactLock.Unlock()
		summary := &relationshipSiblingDiversityArtifact{diverseRows: roaring64.NewBitmap()}
		return summary, RelationshipSiblingDiversityStats{
			Mode:          "sibling_diversity_summary_cache_build",
			LookupElapsed: time.Since(start),
			BuildElapsed:  time.Since(buildStart),
		}, generation, true, nil
	}
	groups := make([]relationshipSiblingDiversityBuildGroup, 0, len(readable.byValue))
	allRows := make([]uint64, 0)
	for _, bitmap := range readable.byValue {
		if bitmap == nil || bitmap.IsEmpty() {
			continue
		}
		rows := bitmap.ToArray()
		groups = append(groups, relationshipSiblingDiversityBuildGroup{rows: rows})
		allRows = append(allRows, rows...)
	}
	stats := RelationshipSiblingDiversityStats{
		Rows:           readable.rows,
		Values:         uint64(len(readable.byValue)),
		ProjectionRows: uint64(len(allRows)),
		Groups:         uint64(len(groups)),
		Mode:           "sibling_diversity_summary_cache_build",
		LookupElapsed:  time.Since(start),
	}
	m.reverseArtifactLock.Unlock()
	if len(groups) == 0 || len(allRows) == 0 {
		summary := &relationshipSiblingDiversityArtifact{
			diverseRows:    roaring64.NewBitmap(),
			rows:           stats.Rows,
			values:         stats.Values,
			groups:         stats.Groups,
			projectionRows: stats.ProjectionRows,
		}
		stats.BuildElapsed = time.Since(buildStart)
		return summary, stats, generation, true, nil
	}

	projectionStart := time.Now()
	valuesByField, statsByField, err := m.ProjectBSIInt64ValuesWithStats(index, []string{valueField}, fromTime, toTime, allRows, relationshipReverseArtifactBitmap(allRows), false)
	if err != nil {
		return nil, stats, generation, true, err
	}
	stats.ProjectionElapsed = time.Since(projectionStart)
	stats.ProjectionStats = statsByField[valueField]
	values := valuesByField[valueField]

	evaluationStart := time.Now()
	diverseRows := roaring64.NewBitmap()
	offset := 0
	for _, group := range groups {
		distinct := make(map[int64]struct{}, 2)
		for i := 0; i < len(group.rows); i++ {
			position := offset + i
			if position >= len(values.Exists) || !values.Exists[position] {
				continue
			}
			distinct[values.Values[position]] = struct{}{}
			if len(distinct) > 1 {
				break
			}
		}
		if len(distinct) > 1 {
			stats.DiverseGroups++
			for i, rownum := range group.rows {
				position := offset + i
				if position < len(values.Exists) && values.Exists[position] {
					diverseRows.Add(rownum)
				}
			}
		}
		offset += len(group.rows)
	}
	stats.EvaluationElapsed = time.Since(evaluationStart)
	summary := &relationshipSiblingDiversityArtifact{
		diverseRows:    diverseRows,
		rows:           stats.Rows,
		values:         stats.Values,
		projectionRows: stats.ProjectionRows,
		groups:         stats.Groups,
		diverseGroups:  stats.DiverseGroups,
	}
	stats.BuildElapsed = time.Since(buildStart)
	return summary, stats, generation, true, nil
}

func relationshipSiblingDiversityStatsFromSummary(summary *relationshipSiblingDiversityArtifact, cacheHit bool, mode string, start time.Time) RelationshipSiblingDiversityStats {
	return RelationshipSiblingDiversityStats{
		Rows:           summary.rows,
		Values:         summary.values,
		ProjectionRows: summary.projectionRows,
		Groups:         summary.groups,
		DiverseGroups:  summary.diverseGroups,
		CacheHit:       cacheHit,
		Mode:           mode,
		LookupElapsed:  time.Since(start),
	}
}

func relationshipSiblingDiversityCandidateRows(diverseRows, candidateSet *roaring64.Bitmap) []uint64 {
	if diverseRows == nil || diverseRows.IsEmpty() {
		return []uint64{}
	}
	if candidateSet == nil {
		return diverseRows.ToArray()
	}
	matched := roaring64.And(diverseRows, candidateSet)
	if matched == nil || matched.IsEmpty() {
		return []uint64{}
	}
	return matched.ToArray()
}

func relationshipSiblingDiversityCacheKey(valueField string, fromTime, toTime int64) string {
	return valueField + "\x00" + strconv.FormatInt(fromTime, 10) + "\x00" + strconv.FormatInt(toTime, 10)
}

func (m *BitmapIndex) relationshipReverseArtifactCandidateValues(index, field string, sourceValues []int64, candidateRows []uint64, sortRows bool, includeRows bool, includeParentValues bool) ([]uint64, map[uint64]int64, RelationshipReverseArtifactStats, bool, error) {
	start := time.Now()
	if !m.relationshipReverseArtifactEnabled(index, field) {
		return nil, nil, RelationshipReverseArtifactStats{}, false, nil
	}
	m.reverseArtifactLock.Lock()
	fields := m.reverseArtifactCache[index]
	if fields == nil {
		m.reverseArtifactLock.Unlock()
		return nil, nil, RelationshipReverseArtifactStats{}, false, nil
	}
	artifact := fields[field]
	if artifact == nil {
		m.reverseArtifactLock.Unlock()
		return nil, nil, RelationshipReverseArtifactStats{}, false, nil
	}
	readableSources, readableRows, readableValues := m.relationshipReverseArtifactReadableDataSourcesLocked(index, field, artifact)
	uniqueValues := relationshipReverseArtifactUniqueInt64Values(sourceValues)
	targetCapacity := relationshipReverseArtifactCandidateCapacity(readableRows, readableValues, len(uniqueValues))
	candidateSet := relationshipReverseArtifactBitmap(candidateRows)
	if candidateSet != nil && uint64(targetCapacity) > candidateSet.GetCardinality() {
		targetCapacity = int(candidateSet.GetCardinality())
	}
	rownums := []uint64(nil)
	if includeRows {
		rownums = make([]uint64, 0, targetCapacity)
	}
	parentValueByChild := map[uint64]int64(nil)
	if includeParentValues {
		parentValueByChild = make(map[uint64]int64, targetCapacity)
	}
	seenRows := map[uint64]struct{}(nil)
	if includeRows && !includeParentValues {
		seenRows = make(map[uint64]struct{}, targetCapacity)
	}
	if len(readableSources) > 0 {
		requestedValues := make(map[int64]struct{}, len(uniqueValues))
		for _, value := range uniqueValues {
			requestedValues[value] = struct{}{}
		}
		for _, readable := range readableSources {
			if readable == nil {
				continue
			}
			for value, bitmap := range readable.byValue {
				if _, ok := requestedValues[value]; !ok {
					continue
				}
				if bitmap == nil {
					continue
				}
				it := bitmap.Iterator()
				for it.HasNext() {
					rownum := it.Next()
					if candidateSet != nil && !candidateSet.Contains(rownum) {
						continue
					}
					if includeParentValues {
						if _, ok := parentValueByChild[rownum]; ok {
							continue
						}
						parentValueByChild[rownum] = value
					} else if includeRows {
						if _, ok := seenRows[rownum]; ok {
							continue
						}
						seenRows[rownum] = struct{}{}
					}
					if includeRows {
						rownums = append(rownums, rownum)
					}
				}
			}
		}
	}
	if includeRows && sortRows && len(rownums) > 1 {
		sort.Slice(rownums, func(i, j int) bool { return rownums[i] < rownums[j] })
	}
	targetRows := len(parentValueByChild)
	if includeRows {
		targetRows = len(rownums)
	}
	stats := RelationshipReverseArtifactStats{
		Rows:          readableRows,
		Values:        readableValues,
		SourceValues:  len(uniqueValues),
		TargetRows:    uint64(targetRows),
		LookupElapsed: time.Since(start),
	}
	m.reverseArtifactLock.Unlock()
	return rownums, parentValueByChild, stats, true, nil
}

// RelationshipReverseArtifactSum groups child-domain BSI values by the
// relationship-vector value encoded for those child rows. The result is
// intentionally expressed as raw BSI integers so callers can apply catalog
// scale/type semantics at the query edge.
func (m *BitmapIndex) RelationshipReverseArtifactSum(index, vectorField, valueField string, fromTime, toTime int64, childRows []uint64, parentValues []uint64) ([]RelationshipReverseArtifactSumGroup, RelationshipReverseArtifactSumStats, bool, error) {
	start := time.Now()
	if strings.TrimSpace(valueField) == "" {
		return nil, RelationshipReverseArtifactSumStats{}, false, fmt.Errorf("relationship reverse artifact sum requires value field")
	}
	if !m.relationshipReverseArtifactEnabled(index, vectorField) {
		return nil, RelationshipReverseArtifactSumStats{}, false, nil
	}
	childSet := relationshipReverseArtifactBitmap(childRows)
	groupRows, stats, ok := m.relationshipReverseArtifactSumRows(index, vectorField, childSet, parentValues, start)
	if !ok {
		return nil, RelationshipReverseArtifactSumStats{}, false, nil
	}
	if len(groupRows) == 0 {
		return []RelationshipReverseArtifactSumGroup{}, stats, true, nil
	}

	allRows := make([]uint64, 0)
	for _, group := range groupRows {
		allRows = append(allRows, group.rows...)
	}
	foundSet := relationshipReverseArtifactBitmap(allRows)
	projectionStart := time.Now()
	valuesByField, statsByField, err := m.ProjectBSIValuesWithStats(index, []string{valueField}, fromTime, toTime, allRows, foundSet, false)
	if err != nil {
		return nil, stats, true, err
	}
	stats.ProjectionElapsed = time.Since(projectionStart)
	stats.ProjectionStats = statsByField[valueField]
	values := valuesByField[valueField]

	aggregateStart := time.Now()
	groups := make([]RelationshipReverseArtifactSumGroup, 0, len(groupRows))
	offset := 0
	for _, group := range groupRows {
		sum := big.NewInt(0)
		count := uint64(0)
		for i := 0; i < len(group.rows); i++ {
			position := offset + i
			if position >= len(values) || values[position] == nil {
				continue
			}
			sum.Add(sum, values[position])
			count++
		}
		offset += len(group.rows)
		if count == 0 {
			continue
		}
		groups = append(groups, RelationshipReverseArtifactSumGroup{
			ParentValue:       uint64(group.parentValue),
			RepresentativeRow: group.rows[0],
			Count:             count,
			Sum:               sum,
		})
		stats.TargetRows += count
	}
	stats.AggregateElapsed = time.Since(aggregateStart)
	stats.Groups = len(groups)
	return groups, stats, true, nil
}

// RelationshipAlignedValueSumStorage groups child-domain BSI values by caller-supplied
// parent rows that are already aligned with childRows. This is the storage-side
// aggregate path for graph reducers that have already performed relationship
// vector alignment and only need mergeable grouped measure state.
func (m *BitmapIndex) RelationshipAlignedValueSumStorage(index, valueField string, fromTime, toTime int64, childRows []uint64, parentRows []uint64) ([]RelationshipReverseArtifactSumGroup, RelationshipReverseArtifactSumStats, bool, error) {
	if strings.TrimSpace(valueField) == "" {
		return nil, RelationshipReverseArtifactSumStats{}, false, fmt.Errorf("relationship aligned value sum requires value field")
	}
	if len(childRows) != len(parentRows) {
		return nil, RelationshipReverseArtifactSumStats{}, false, fmt.Errorf("relationship aligned value sum requires childRows and parentRows to have matching lengths")
	}
	stats := RelationshipReverseArtifactSumStats{
		Rows:         uint64(len(childRows)),
		SourceValues: relationshipReverseArtifactUniqueUint64Count(parentRows),
	}
	if len(childRows) == 0 {
		return []RelationshipReverseArtifactSumGroup{}, stats, true, nil
	}

	projectionStart := time.Now()
	groupsByParent, projectionStats, aggregateElapsed, err := m.aggregateAlignedValuesWithStats(index, valueField, fromTime, toTime, childRows, parentRows)
	if err != nil {
		return nil, stats, true, err
	}
	stats.ProjectionElapsed = time.Since(projectionStart)
	stats.ProjectionStats = projectionStats
	stats.AggregateElapsed = aggregateElapsed
	stats.Groups = len(groupsByParent)
	stats.Values = uint64(len(groupsByParent))
	groups := relationshipReverseArtifactSortedSumGroups(groupsByParent)
	for _, group := range groups {
		stats.TargetRows += group.Count
	}
	return groups, stats, true, nil
}

func (m *BitmapIndex) aggregateAlignedValuesWithStats(index, valueField string, fromTime, toTime int64, rownums []uint64, parentRows []uint64) (map[uint64]*RelationshipReverseArtifactSumGroup, ProjectBSIStats, time.Duration, error) {
	if priceField, discountField, ok := qsbridge.ParseRelationshipAlignedDiscountedRevenueField(valueField); ok {
		return m.aggregateAlignedDiscountedRevenueWithStats(index, priceField, discountField, fromTime, toTime, rownums, parentRows)
	}
	return m.aggregateAlignedBSIValuesWithStats(index, valueField, fromTime, toTime, rownums, parentRows)
}

func (m *BitmapIndex) aggregateAlignedDiscountedRevenueWithStats(index, priceField, discountField string, fromTime, toTime int64, rownums []uint64, parentRows []uint64) (map[uint64]*RelationshipReverseArtifactSumGroup, ProjectBSIStats, time.Duration, error) {
	valuesByField, statsByField, err := m.ProjectBSIInt64ValuesWithStats(
		index,
		[]string{priceField, discountField},
		fromTime,
		toTime,
		rownums,
		relationshipReverseArtifactBitmap(rownums),
		false,
	)
	if err != nil {
		return nil, ProjectBSIStats{}, 0, err
	}
	stats := relationshipReverseArtifactCombineProjectionStats(statsByField[priceField], statsByField[discountField])
	priceValues := valuesByField[priceField]
	discountValues := valuesByField[discountField]
	aggregateStart := time.Now()
	groupsByParent := make(map[uint64]*RelationshipReverseArtifactSumGroup, relationshipReverseArtifactUniqueUint64Count(parentRows))
	for i, rownum := range rownums {
		if i >= len(parentRows) || i >= len(priceValues.Exists) || i >= len(discountValues.Exists) || !priceValues.Exists[i] || !discountValues.Exists[i] {
			continue
		}
		parentRow := parentRows[i]
		group := groupsByParent[parentRow]
		if group == nil {
			group = &RelationshipReverseArtifactSumGroup{
				ParentValue:       parentRow,
				RepresentativeRow: rownum,
				Sum:               big.NewInt(0),
			}
			groupsByParent[parentRow] = group
		}
		group.Sum.Add(group.Sum, relationshipDiscountedRevenueScaledValue(priceValues.Values[i], discountValues.Values[i]))
		group.Count++
	}
	return groupsByParent, stats, time.Since(aggregateStart), nil
}

func relationshipDiscountedRevenueScaledValue(priceScaled int64, discountScaled int64) *big.Int {
	value := big.NewInt(priceScaled)
	value.Mul(value, big.NewInt(100-discountScaled))
	return value.Quo(value, big.NewInt(100))
}

func relationshipReverseArtifactCombineProjectionStats(left ProjectBSIStats, right ProjectBSIStats) ProjectBSIStats {
	return ProjectBSIStats{
		ShardsVisited:    left.ShardsVisited + right.ShardsVisited,
		ShardsInWindow:   left.ShardsInWindow + right.ShardsInWindow,
		ShardsLocal:      left.ShardsLocal + right.ShardsLocal,
		ShardsRetained:   left.ShardsRetained + right.ShardsRetained,
		RetainedRows:     left.RetainedRows + right.RetainedRows,
		RetainBypassRows: left.RetainBypassRows + right.RetainBypassRows,
		RetainElapsed:    left.RetainElapsed + right.RetainElapsed,
		ValueElapsed:     left.ValueElapsed + right.ValueElapsed,
		MergeElapsed:     left.MergeElapsed + right.MergeElapsed,
	}
}

// RelationshipReverseArtifactStatsStorage returns maintained artifact cardinality
// without scanning source values or materializing candidates.
func (m *BitmapIndex) RelationshipReverseArtifactStatsStorage(index, field string) (RelationshipReverseArtifactStats, bool, error) {
	if !m.relationshipReverseArtifactEnabled(index, field) {
		return RelationshipReverseArtifactStats{}, false, nil
	}
	m.reverseArtifactLock.Lock()
	defer m.reverseArtifactLock.Unlock()
	fields := m.reverseArtifactCache[index]
	if fields == nil {
		return RelationshipReverseArtifactStats{}, false, nil
	}
	artifact := fields[field]
	if artifact == nil {
		return RelationshipReverseArtifactStats{}, false, nil
	}
	_, rows, values := m.relationshipReverseArtifactReadableDataSourcesLocked(index, field, artifact)
	return RelationshipReverseArtifactStats{
		Rows:   rows,
		Values: values,
	}, true, nil
}

type relationshipReverseArtifactGroupRows struct {
	parentValue int64
	rows        []uint64
}

func (m *BitmapIndex) relationshipReverseArtifactSumRows(index, field string, childSet *roaring64.Bitmap, parentValues []uint64, start time.Time) ([]relationshipReverseArtifactGroupRows, RelationshipReverseArtifactSumStats, bool) {
	m.reverseArtifactLock.Lock()
	defer m.reverseArtifactLock.Unlock()

	fields := m.reverseArtifactCache[index]
	if fields == nil {
		return nil, RelationshipReverseArtifactSumStats{}, false
	}
	artifact := fields[field]
	if artifact == nil {
		return nil, RelationshipReverseArtifactSumStats{}, false
	}
	readable := m.relationshipReverseArtifactReadableData(index, field, artifact)
	rows, valueCount := relationshipReverseArtifactDataStats(readable)
	values := relationshipReverseArtifactRequestedValues(readable, parentValues)
	groups := make([]relationshipReverseArtifactGroupRows, 0, len(values))
	for _, value := range values {
		if readable == nil {
			continue
		}
		bitmap := readable.byValue[value]
		if bitmap == nil {
			continue
		}
		var retained *roaring64.Bitmap
		if childSet == nil {
			retained = bitmap.Clone()
		} else {
			retained = roaring64.And(bitmap, childSet)
		}
		if retained == nil || retained.IsEmpty() {
			continue
		}
		groups = append(groups, relationshipReverseArtifactGroupRows{
			parentValue: value,
			rows:        retained.ToArray(),
		})
	}
	return groups, RelationshipReverseArtifactSumStats{
		Rows:          rows,
		Values:        valueCount,
		SourceValues:  len(values),
		LookupElapsed: time.Since(start),
	}, true
}

func relationshipReverseArtifactRequestedValues(readable *relationshipReverseArtifactData, parentValues []uint64) []int64 {
	if len(parentValues) == 0 {
		if readable == nil {
			return nil
		}
		values := make([]int64, 0, len(readable.byValue))
		for value := range readable.byValue {
			values = append(values, value)
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
		return values
	}
	unique := make(map[int64]struct{}, len(parentValues))
	values := make([]int64, 0, len(parentValues))
	for _, value := range parentValues {
		signed := int64(value)
		if _, ok := unique[signed]; ok {
			continue
		}
		unique[signed] = struct{}{}
		values = append(values, signed)
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

func relationshipReverseArtifactBitmap(rows []uint64) *roaring64.Bitmap {
	if len(rows) == 0 {
		return nil
	}
	bitmap := roaring64.NewBitmap()
	for _, row := range rows {
		bitmap.Add(row)
	}
	return bitmap
}

func (m *BitmapIndex) aggregateAlignedBSIValuesWithStats(index, field string, fromTime, toTime int64, rownums []uint64, parentRows []uint64) (map[uint64]*RelationshipReverseArtifactSumGroup, ProjectBSIStats, time.Duration, error) {
	if index == "" {
		return nil, ProjectBSIStats{}, 0, fmt.Errorf("index not specified for aligned BSI aggregate")
	}
	if field == "" {
		return nil, ProjectBSIStats{}, 0, fmt.Errorf("field not specified for aligned BSI aggregate")
	}
	sortedUniqueRows := relationshipReverseArtifactRowsStrictlyIncreasing(rownums)
	positions := map[uint64][]int(nil)
	if !sortedUniqueRows {
		positions = make(map[uint64][]int, len(rownums))
		for i, rownum := range rownums {
			positions[rownum] = append(positions[rownum], i)
		}
	}
	foundSet := relationshipReverseArtifactBitmap(rownums)
	groupsByParent := make(map[uint64]*RelationshipReverseArtifactSumGroup, relationshipReverseArtifactUniqueUint64Count(parentRows))
	stats := ProjectBSIStats{}
	aggregateElapsed := time.Duration(0)

	attr, err := m.getFieldConfig(index, field)
	if err != nil {
		return nil, stats, 0, err
	}
	tq := attr.TimeQuantumType
	from := truncateTime(time.Unix(0, fromTime).UTC(), tq)
	to := truncateTime(time.Unix(0, toTime).UTC(), tq)

	readShard := func(ts int64, bsi *BSIBitmap) error {
		if bsi == nil || bsi.BSI == nil {
			return nil
		}
		stats.ShardsVisited++
		if tq != "" {
			rts := truncateTime(time.Unix(0, ts).UTC(), tq).UnixNano()
			if rts < from.UnixNano() || rts > to.UnixNano() {
				return nil
			}
		}
		stats.ShardsInWindow++
		if tq != "" {
			hashKey := fmt.Sprintf("%s/%s/%s", index, field, formatShardTime(time.Unix(0, ts)))
			if !m.Member(hashKey) {
				return nil
			}
		}
		stats.ShardsLocal++

		retainStart := time.Now()
		existence := bsi.BSI.GetExistenceBitmap()
		retainSet := roaring64.And(existence, foundSet)
		stats.RetainElapsed += time.Since(retainStart)
		if retainSet == nil || retainSet.IsEmpty() {
			return nil
		}
		retainRows := retainSet.GetCardinality()
		stats.ShardsRetained++
		stats.RetainedRows += retainRows
		if retainRows == existence.GetCardinality() {
			stats.RetainBypassRows += retainRows
		}

		valueStart := time.Now()
		retainedRownums := retainSet.ToArray()
		retainedValues := bsi.BSI.GetBigValues(retainedRownums)
		stats.ValueElapsed += time.Since(valueStart)

		aggregateStart := time.Now()
		sortedPosition := 0
		if sortedUniqueRows && len(retainedRownums) > 0 {
			firstRetainedRow := retainedRownums[0]
			sortedPosition = sort.Search(len(rownums), func(i int) bool { return rownums[i] >= firstRetainedRow })
		}
		for i, rownum := range retainedRownums {
			if i >= len(retainedValues) || retainedValues[i] == nil {
				continue
			}
			if sortedUniqueRows {
				for sortedPosition < len(rownums) && rownums[sortedPosition] < rownum {
					sortedPosition++
				}
				if sortedPosition >= len(rownums) {
					break
				}
				if rownums[sortedPosition] != rownum || sortedPosition >= len(parentRows) {
					continue
				}
				relationshipReverseArtifactAccumulateAlignedValue(groupsByParent, parentRows[sortedPosition], rownums[sortedPosition], retainedValues[i])
				continue
			}
			for _, position := range positions[rownum] {
				if position >= len(parentRows) {
					continue
				}
				relationshipReverseArtifactAccumulateAlignedValue(groupsByParent, parentRows[position], rownums[position], retainedValues[i])
			}
		}
		aggregateElapsed += time.Since(aggregateStart)
		return nil
	}

	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()
	if tq == "" {
		if fields := m.bsiCache[index]; fields != nil {
			if shards := fields[field]; shards != nil {
				if bm, ok := shards[0]; ok {
					if err := readShard(0, bm); err != nil {
						return nil, stats, aggregateElapsed, err
					}
				}
			}
		}
		return groupsByParent, stats, aggregateElapsed, nil
	}
	if fields := m.bsiCache[index]; fields != nil {
		if shards := fields[field]; shards != nil {
			for ts, bsi := range shards {
				if err := readShard(ts, bsi); err != nil {
					return nil, stats, aggregateElapsed, err
				}
			}
		}
	}
	return groupsByParent, stats, aggregateElapsed, nil
}

func relationshipReverseArtifactRowsStrictlyIncreasing(rows []uint64) bool {
	if len(rows) < 2 {
		return true
	}
	for i := 1; i < len(rows); i++ {
		if rows[i-1] >= rows[i] {
			return false
		}
	}
	return true
}

func relationshipReverseArtifactAccumulateAlignedValue(groupsByParent map[uint64]*RelationshipReverseArtifactSumGroup, parentRow, representativeRow uint64, value *big.Int) {
	if value == nil {
		return
	}
	group := groupsByParent[parentRow]
	if group == nil {
		group = &RelationshipReverseArtifactSumGroup{
			ParentValue:       parentRow,
			RepresentativeRow: representativeRow,
			Sum:               big.NewInt(0),
		}
		groupsByParent[parentRow] = group
	}
	group.Sum.Add(group.Sum, value)
	group.Count++
}

func relationshipReverseArtifactSortedSumGroups(groupsByParent map[uint64]*RelationshipReverseArtifactSumGroup) []RelationshipReverseArtifactSumGroup {
	parentRows := make([]uint64, 0, len(groupsByParent))
	for parentRow := range groupsByParent {
		parentRows = append(parentRows, parentRow)
	}
	sort.Slice(parentRows, func(i, j int) bool { return parentRows[i] < parentRows[j] })
	groups := make([]RelationshipReverseArtifactSumGroup, 0, len(parentRows))
	for _, parentRow := range parentRows {
		group := groupsByParent[parentRow]
		if group == nil || group.Count == 0 {
			continue
		}
		groups = append(groups, *group)
	}
	return groups
}

func relationshipReverseArtifactUniqueUint64Count(values []uint64) int {
	if len(values) == 0 {
		return 0
	}
	unique := make(map[uint64]struct{}, len(values))
	for _, value := range values {
		unique[value] = struct{}{}
	}
	return len(unique)
}

func relationshipReverseArtifactUniqueInt64Values(values []int64) []int64 {
	if len(values) == 0 {
		return nil
	}
	unique := make([]int64, 0, len(values))
	seen := make(map[int64]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}

func relationshipReverseArtifactCandidateCapacity(rows uint64, values uint64, sourceValueCount int) int {
	if sourceValueCount <= 0 || rows == 0 || values == 0 {
		return 0
	}
	averageRowsPerValue := rows / values
	if averageRowsPerValue == 0 {
		averageRowsPerValue = 1
	}
	estimate := uint64(sourceValueCount) * averageRowsPerValue
	if estimate > rows {
		estimate = rows
	}
	return int(estimate)
}

func (m *BitmapIndex) relationshipReverseArtifactReadableData(index, field string, artifact *relationshipReverseArtifact) *relationshipReverseArtifactData {
	if artifact == nil {
		return nil
	}
	if !m.relationshipReverseArtifactShardOwnershipFilterEnabled() {
		return &artifact.relationshipReverseArtifactData
	}
	if len(artifact.byShard) == 0 {
		return nil
	}
	shardKey := m.relationshipReverseArtifactReadableShardKeyLocked(index, field, artifact)
	if artifact.owned != nil && artifact.ownedShardKey == shardKey {
		return artifact.owned
	}
	return m.relationshipReverseArtifactBuildOwnedLocked(index, field, artifact)
}

func (m *BitmapIndex) relationshipReverseArtifactReadableDataSourcesLocked(index, field string, artifact *relationshipReverseArtifact) ([]*relationshipReverseArtifactData, uint64, uint64) {
	if artifact == nil {
		return nil, 0, 0
	}
	if !m.relationshipReverseArtifactShardOwnershipFilterEnabled() || len(artifact.byShard) == 0 {
		rows, values := relationshipReverseArtifactDataStats(&artifact.relationshipReverseArtifactData)
		if rows == 0 {
			return nil, rows, values
		}
		return []*relationshipReverseArtifactData{&artifact.relationshipReverseArtifactData}, rows, values
	}

	shardTimes := make([]int64, 0, len(artifact.byShard))
	for shardTime := range artifact.byShard {
		if m.relationshipReverseArtifactOwnsShard(index, field, shardTime) {
			shardTimes = append(shardTimes, shardTime)
		}
	}
	sort.Slice(shardTimes, func(i, j int) bool { return shardTimes[i] < shardTimes[j] })

	sources := make([]*relationshipReverseArtifactData, 0, len(shardTimes))
	valueSet := make(map[int64]struct{})
	rows := uint64(0)
	for _, shardTime := range shardTimes {
		shard := artifact.byShard[shardTime]
		if shard == nil || shard.rows == 0 {
			continue
		}
		sources = append(sources, shard)
		rows += shard.rows
		for value := range shard.byValue {
			valueSet[value] = struct{}{}
		}
	}
	return sources, rows, uint64(len(valueSet))
}

func (m *BitmapIndex) relationshipReverseArtifactBuildOwnedLocked(index, field string, artifact *relationshipReverseArtifact) *relationshipReverseArtifactData {
	if artifact == nil || !m.relationshipReverseArtifactShardOwnershipFilterEnabled() || len(artifact.byShard) == 0 {
		return nil
	}
	shardKey := m.relationshipReverseArtifactReadableShardKeyLocked(index, field, artifact)
	owned := &relationshipReverseArtifactData{byValue: make(map[int64]*roaring64.Bitmap)}
	for shardTime, shard := range artifact.byShard {
		if shard == nil || !m.relationshipReverseArtifactOwnsShard(index, field, shardTime) {
			continue
		}
		relationshipReverseArtifactMergeInto(owned, shard)
	}
	artifact.ownedShardKey = shardKey
	if owned.rows == 0 {
		artifact.owned = nil
		return nil
	}
	artifact.owned = owned
	return artifact.owned
}

func (m *BitmapIndex) warmRelationshipReverseArtifactOwnedCaches(reason string) relationshipReverseArtifactWarmStats {
	var stats relationshipReverseArtifactWarmStats
	start := time.Now()
	if !m.relationshipReverseArtifactShardOwnershipFilterEnabled() {
		return stats
	}
	m.reverseArtifactLock.Lock()
	defer m.reverseArtifactLock.Unlock()

	for index, fields := range m.reverseArtifactCache {
		for field, artifact := range fields {
			if artifact == nil {
				continue
			}
			stats.Fields++
			stats.Shards += uint64(len(artifact.byShard))
			stats.AggregateRows += artifact.rows
			owned := m.relationshipReverseArtifactBuildOwnedLocked(index, field, artifact)
			ownedRows, ownedValues := relationshipReverseArtifactDataStats(owned)
			stats.OwnedRows += ownedRows
			stats.OwnedValues += ownedValues
		}
	}
	fmt.Printf("relationship reverse artifact owned cache warm reason=%s node=%s fields=%d shards=%d aggregate_rows=%d owned_values=%d owned_rows=%d elapsed=%v\n",
		reason, m.GetNodeID(), stats.Fields, stats.Shards, stats.AggregateRows, stats.OwnedValues, stats.OwnedRows, time.Since(start))
	return stats
}

func (m *BitmapIndex) relationshipReverseArtifactShardOwnershipFilterEnabled() bool {
	if m == nil || m.Node == nil || m.Conn == nil {
		return false
	}
	if m.IsLocalCluster || m.ServicePort <= 0 || m.HashTable == nil {
		return false
	}
	return strings.TrimSpace(m.GetNodeID()) != ""
}

func (m *BitmapIndex) relationshipReverseArtifactOwnsShard(index, field string, shardTime int64) bool {
	if !m.relationshipReverseArtifactShardOwnershipFilterEnabled() {
		return true
	}
	hashKey := fmt.Sprintf("%s/%s/%s", index, field, formatShardTime(time.Unix(0, shardTime)))
	if found, replica := m.relationshipReverseArtifactLocalShardReplica(hashKey); found {
		return replica == 1
	}
	found, replica := m.CheckNodeForKey(hashKey, m.GetNodeID())
	return found && replica == 1
}

func (m *BitmapIndex) relationshipReverseArtifactLocalShardReplica(hashKey string) (bool, int) {
	if m == nil || m.HashTable == nil {
		return false, 0
	}
	nodeID := strings.TrimSpace(m.GetNodeID())
	if nodeID == "" {
		return false, 0
	}
	if m.State != Active && m.State != Syncing {
		return false, 0
	}
	nodeKeys := m.HashTable.GetN(m.Replicas, hashKey)
	for i, candidate := range nodeKeys {
		if candidate == nodeID {
			return true, i + 1
		}
	}
	return false, 0
}

func (m *BitmapIndex) relationshipReverseArtifactReadableShardKey(index, field string) string {
	m.reverseArtifactLock.RLock()
	defer m.reverseArtifactLock.RUnlock()
	fields := m.reverseArtifactCache[index]
	if fields == nil {
		return ""
	}
	artifact := fields[field]
	if artifact == nil || len(artifact.byShard) == 0 {
		return ""
	}
	return m.relationshipReverseArtifactReadableShardKeyLocked(index, field, artifact)
}

func (m *BitmapIndex) relationshipReverseArtifactReadableShardKeyLocked(index, field string, artifact *relationshipReverseArtifact) string {
	if artifact == nil || len(artifact.byShard) == 0 {
		return ""
	}
	shardTimes := make([]int64, 0, len(artifact.byShard))
	for shardTime := range artifact.byShard {
		if m.relationshipReverseArtifactOwnsShard(index, field, shardTime) {
			shardTimes = append(shardTimes, shardTime)
		}
	}
	sort.Slice(shardTimes, func(i, j int) bool { return shardTimes[i] < shardTimes[j] })
	var builder strings.Builder
	for i, shardTime := range shardTimes {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(strconv.FormatInt(shardTime, 10))
	}
	return builder.String()
}

func relationshipReverseArtifactDataStats(data *relationshipReverseArtifactData) (uint64, uint64) {
	if data == nil {
		return 0, 0
	}
	return data.rows, uint64(len(data.byValue))
}

func relationshipReverseArtifactMergeInto(target *relationshipReverseArtifactData, source *relationshipReverseArtifactData) {
	if target == nil || source == nil {
		return
	}
	if target.byValue == nil {
		target.byValue = make(map[int64]*roaring64.Bitmap)
	}
	for value, bitmap := range source.byValue {
		if bitmap == nil || bitmap.IsEmpty() {
			continue
		}
		targetBitmap := target.byValue[value]
		before := uint64(0)
		if targetBitmap == nil {
			target.byValue[value] = bitmap.Clone()
			target.rows += bitmap.GetCardinality()
			continue
		}
		before = targetBitmap.GetCardinality()
		targetBitmap.Or(bitmap)
		after := targetBitmap.GetCardinality()
		if after > before {
			target.rows += after - before
		}
	}
}

func (m *BitmapIndex) relationshipReverseArtifactSnapshot() relationshipReverseArtifactSnapshot {
	m.reverseArtifactLock.RLock()
	defer m.reverseArtifactLock.RUnlock()

	var snapshot relationshipReverseArtifactSnapshot
	for _, fields := range m.reverseArtifactCache {
		for _, artifact := range fields {
			if artifact == nil {
				continue
			}
			snapshot.Fields++
			snapshot.Values += uint64(len(artifact.byValue))
			snapshot.Rows += artifact.rows
		}
	}
	return snapshot
}

func (m *BitmapIndex) updateRelationshipReverseArtifactForBSIFragment(index, field string, shardTime int64, oldBSI *roaring64.BSI, removedRows *roaring64.Bitmap, addedBSI *roaring64.BSI) {
	reverseArtifactEnabled := m.relationshipReverseArtifactEnabled(index, field)
	m.reverseArtifactLock.Lock()
	defer m.reverseArtifactLock.Unlock()

	m.clearRelationshipSiblingDiversityArtifactsForIndexLocked(index)
	if !reverseArtifactEnabled {
		return
	}

	artifact := m.relationshipReverseArtifact(index, field)
	ownershipFilterEnabled := m.relationshipReverseArtifactShardOwnershipFilterEnabled()
	ownsShard := false
	if ownershipFilterEnabled {
		ownsShard = m.relationshipReverseArtifactOwnsShard(index, field, shardTime)
		shardKey := m.relationshipReverseArtifactReadableShardKeyLocked(index, field, artifact)
		if shardKey == "" {
			artifact.owned = nil
			artifact.ownedShardKey = ""
		} else if artifact.owned == nil || artifact.ownedShardKey != shardKey {
			m.relationshipReverseArtifactBuildOwnedLocked(index, field, artifact)
		}
	}
	if removedRows != nil && oldBSI != nil {
		m.removeRelationshipReverseArtifactRows(&artifact.relationshipReverseArtifactData, oldBSI, removedRows)
		if shard := artifact.byShard[shardTime]; shard != nil {
			m.removeRelationshipReverseArtifactRows(shard, oldBSI, removedRows)
			if shard.rows == 0 {
				delete(artifact.byShard, shardTime)
			}
		}
		if ownershipFilterEnabled && ownsShard && artifact.owned != nil {
			m.removeRelationshipReverseArtifactRows(artifact.owned, oldBSI, removedRows)
		}
	}
	if addedBSI != nil {
		m.addRelationshipReverseArtifactRows(&artifact.relationshipReverseArtifactData, addedBSI)
		m.addRelationshipReverseArtifactRows(m.relationshipReverseArtifactShard(artifact, shardTime), addedBSI)
		if ownershipFilterEnabled && ownsShard {
			if artifact.owned == nil {
				artifact.owned = &relationshipReverseArtifactData{byValue: make(map[int64]*roaring64.Bitmap)}
			}
			m.addRelationshipReverseArtifactRows(artifact.owned, addedBSI)
		}
	}
	if ownershipFilterEnabled {
		artifact.ownedShardKey = m.relationshipReverseArtifactReadableShardKeyLocked(index, field, artifact)
		if artifact.owned != nil && artifact.owned.rows == 0 {
			artifact.owned = nil
		}
	}
}

func (m *BitmapIndex) rebuildRelationshipReverseArtifactsForIndex(index string) {
	enabled := make(map[string]struct{})
	m.tableCacheLock.RLock()
	table := m.tableCache[index]
	if table != nil {
		for _, attr := range table.Attributes {
			field := strings.TrimSpace(attr.FieldName)
			if field == "" {
				field = strings.TrimSpace(attr.SourceName)
			}
			if field == "" || !attr.RelationshipArtifacts.ParentToChild || strings.TrimSpace(attr.ForeignKey) == "" {
				continue
			}
			enabled[field] = struct{}{}
		}
	}
	m.tableCacheLock.RUnlock()

	rebuilt := make(map[string]*relationshipReverseArtifact)
	m.bsiCacheLock.RLock()
	if fields := m.bsiCache[index]; fields != nil {
		for field := range enabled {
			shards := fields[field]
			if len(shards) == 0 {
				continue
			}
			artifact := newRelationshipReverseArtifact()
			for ts, bsi := range shards {
				if bsi == nil || bsi.BSI == nil {
					continue
				}
				bsi.Lock.RLock()
				m.addRelationshipReverseArtifactRows(&artifact.relationshipReverseArtifactData, bsi.BSI)
				m.addRelationshipReverseArtifactRows(m.relationshipReverseArtifactShard(artifact, ts), bsi.BSI)
				bsi.Lock.RUnlock()
			}
			if artifact.rows > 0 {
				m.relationshipReverseArtifactBuildOwnedLocked(index, field, artifact)
				rebuilt[field] = artifact
			}
		}
	}
	m.bsiCacheLock.RUnlock()

	m.reverseArtifactLock.Lock()
	defer m.reverseArtifactLock.Unlock()
	if m.reverseArtifactCache == nil {
		m.reverseArtifactCache = make(map[string]map[string]*relationshipReverseArtifact)
	}
	delete(m.reverseArtifactCache, index)
	if len(rebuilt) > 0 {
		m.reverseArtifactCache[index] = rebuilt
	}
	m.clearRelationshipSiblingDiversityArtifactsForIndexLocked(index)
}

func (m *BitmapIndex) relationshipReverseArtifactEnabled(index, field string) bool {
	attr, err := m.getFieldConfig(index, field)
	if err != nil || attr == nil {
		return false
	}
	if !attr.RelationshipArtifacts.ParentToChild {
		return false
	}
	return strings.TrimSpace(attr.ForeignKey) != ""
}

func (m *BitmapIndex) relationshipReverseArtifact(index, field string) *relationshipReverseArtifact {
	if m.reverseArtifactCache == nil {
		m.reverseArtifactCache = make(map[string]map[string]*relationshipReverseArtifact)
	}
	if _, ok := m.reverseArtifactCache[index]; !ok {
		m.reverseArtifactCache[index] = make(map[string]*relationshipReverseArtifact)
	}
	artifact := m.reverseArtifactCache[index][field]
	if artifact == nil {
		artifact = newRelationshipReverseArtifact()
		m.reverseArtifactCache[index][field] = artifact
	}
	if artifact.byShard == nil {
		artifact.byShard = make(map[int64]*relationshipReverseArtifactData)
	}
	if artifact.byValue == nil {
		artifact.byValue = make(map[int64]*roaring64.Bitmap)
	}
	return artifact
}

func newRelationshipReverseArtifact() *relationshipReverseArtifact {
	return &relationshipReverseArtifact{
		relationshipReverseArtifactData: relationshipReverseArtifactData{
			byValue: make(map[int64]*roaring64.Bitmap),
		},
		byShard: make(map[int64]*relationshipReverseArtifactData),
	}
}

func (m *BitmapIndex) relationshipReverseArtifactShard(artifact *relationshipReverseArtifact, shardTime int64) *relationshipReverseArtifactData {
	if artifact.byShard == nil {
		artifact.byShard = make(map[int64]*relationshipReverseArtifactData)
	}
	shard := artifact.byShard[shardTime]
	if shard == nil {
		shard = &relationshipReverseArtifactData{byValue: make(map[int64]*roaring64.Bitmap)}
		artifact.byShard[shardTime] = shard
	}
	return shard
}

func (m *BitmapIndex) relationshipSiblingDiversityCache(index, parentField string) map[string]*relationshipSiblingDiversityArtifact {
	if m.siblingDiversityCache == nil {
		m.siblingDiversityCache = make(map[string]map[string]map[string]*relationshipSiblingDiversityArtifact)
	}
	if _, ok := m.siblingDiversityCache[index]; !ok {
		m.siblingDiversityCache[index] = make(map[string]map[string]*relationshipSiblingDiversityArtifact)
	}
	if _, ok := m.siblingDiversityCache[index][parentField]; !ok {
		m.siblingDiversityCache[index][parentField] = make(map[string]*relationshipSiblingDiversityArtifact)
	}
	return m.siblingDiversityCache[index][parentField]
}

func (m *BitmapIndex) relationshipSiblingDiversityGenerationLocked(index string) uint64 {
	if m.siblingDiversityGen == nil {
		m.siblingDiversityGen = make(map[string]uint64)
	}
	return m.siblingDiversityGen[index]
}

func (m *BitmapIndex) clearRelationshipSiblingDiversityArtifactsForIndexLocked(index string) {
	if m.siblingDiversityCache == nil {
		m.siblingDiversityCache = make(map[string]map[string]map[string]*relationshipSiblingDiversityArtifact)
	}
	delete(m.siblingDiversityCache, index)
	if m.siblingDiversityGen == nil {
		m.siblingDiversityGen = make(map[string]uint64)
	}
	m.siblingDiversityGen[index]++
}

func (m *BitmapIndex) removeRelationshipReverseArtifactRows(artifact *relationshipReverseArtifactData, oldBSI *roaring64.BSI, removedRows *roaring64.Bitmap) {
	it := removedRows.Iterator()
	for it.HasNext() {
		rownum := it.Next()
		value, exists := oldBSI.GetValue(rownum)
		if !exists {
			continue
		}
		bitmap := artifact.byValue[value]
		if bitmap == nil || !bitmap.Contains(rownum) {
			continue
		}
		bitmap.Remove(rownum)
		if artifact.rows > 0 {
			artifact.rows--
		}
		if bitmap.GetCardinality() == 0 {
			delete(artifact.byValue, value)
		}
	}
}

func (m *BitmapIndex) addRelationshipReverseArtifactRows(artifact *relationshipReverseArtifactData, addedBSI *roaring64.BSI) {
	rows := addedBSI.GetExistenceBitmap()
	if rows == nil {
		return
	}
	it := rows.Iterator()
	for it.HasNext() {
		rownum := it.Next()
		value, exists := addedBSI.GetValue(rownum)
		if !exists {
			continue
		}
		bitmap := artifact.byValue[value]
		if bitmap == nil {
			bitmap = roaring64.NewBitmap()
			artifact.byValue[value] = bitmap
		}
		if bitmap.Contains(rownum) {
			continue
		}
		bitmap.Add(rownum)
		artifact.rows++
	}
}

func (m *BitmapIndex) clearRelationshipReverseArtifactsForIndex(index string) {
	m.reverseArtifactLock.Lock()
	defer m.reverseArtifactLock.Unlock()
	delete(m.reverseArtifactCache, index)
	m.clearRelationshipSiblingDiversityArtifactsForIndexLocked(index)
}

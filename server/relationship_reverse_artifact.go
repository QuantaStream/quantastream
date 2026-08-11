package server

import (
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

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
	TargetRows        uint64
	Groups            uint64
	DiverseGroups     uint64
	LookupElapsed     time.Duration
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

type relationshipReverseArtifact struct {
	byValue map[int64]*roaring64.Bitmap
	rows    uint64
}

// RelationshipReverseArtifactCandidates returns child-domain rownums for the
// supplied parent-domain values when a schema-declared reverse artifact exists.
func (m *BitmapIndex) RelationshipReverseArtifactCandidates(index, field string, sourceValues []int64) ([]uint64, RelationshipReverseArtifactStats, bool, error) {
	rownums, _, stats, ok, err := m.RelationshipReverseArtifactCandidateValues(index, field, sourceValues)
	return rownums, stats, ok, err
}

// RelationshipReverseArtifactCandidateValues returns child-domain rownums plus
// the parent-domain value encoded for each returned child row.
func (m *BitmapIndex) RelationshipReverseArtifactCandidateValues(index, field string, sourceValues []int64) ([]uint64, map[uint64]int64, RelationshipReverseArtifactStats, bool, error) {
	return m.relationshipReverseArtifactCandidateValues(index, field, sourceValues, true)
}

// RelationshipReverseArtifactCandidateValuesUnordered returns child-domain
// rownums without sorting them. It is intended for callers that reconstruct
// child-domain order from their own candidate row set.
func (m *BitmapIndex) RelationshipReverseArtifactCandidateValuesUnordered(index, field string, sourceValues []int64) ([]uint64, map[uint64]int64, RelationshipReverseArtifactStats, bool, error) {
	return m.relationshipReverseArtifactCandidateValues(index, field, sourceValues, false)
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

	m.reverseArtifactLock.RLock()
	fields := m.reverseArtifactCache[index]
	if fields == nil {
		m.reverseArtifactLock.RUnlock()
		return nil, RelationshipSiblingDiversityStats{}, false, nil
	}
	artifact := fields[parentField]
	if artifact == nil {
		m.reverseArtifactLock.RUnlock()
		return nil, RelationshipSiblingDiversityStats{}, false, nil
	}
	groups := make([]relationshipSiblingDiversityGroup, 0)
	allRows := make([]uint64, 0)
	for _, bitmap := range artifact.byValue {
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
		Rows:          artifact.rows,
		Values:        uint64(len(artifact.byValue)),
		CandidateRows: uint64(len(candidateRows)),
		Groups:        uint64(len(groups)),
		LookupElapsed: time.Since(start),
	}
	m.reverseArtifactLock.RUnlock()
	if len(groups) == 0 || len(allRows) == 0 {
		return []uint64{}, stats, true, nil
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

func (m *BitmapIndex) relationshipReverseArtifactCandidateValues(index, field string, sourceValues []int64, sortRows bool) ([]uint64, map[uint64]int64, RelationshipReverseArtifactStats, bool, error) {
	start := time.Now()
	if !m.relationshipReverseArtifactEnabled(index, field) {
		return nil, nil, RelationshipReverseArtifactStats{}, false, nil
	}
	m.reverseArtifactLock.RLock()
	fields := m.reverseArtifactCache[index]
	if fields == nil {
		m.reverseArtifactLock.RUnlock()
		return nil, nil, RelationshipReverseArtifactStats{}, false, nil
	}
	artifact := fields[field]
	if artifact == nil {
		m.reverseArtifactLock.RUnlock()
		return nil, nil, RelationshipReverseArtifactStats{}, false, nil
	}
	uniqueValues := relationshipReverseArtifactUniqueInt64Values(sourceValues)
	targetCapacity := relationshipReverseArtifactCandidateCapacity(artifact, len(uniqueValues))
	rownums := make([]uint64, 0, targetCapacity)
	parentValueByChild := make(map[uint64]int64, targetCapacity)
	for _, value := range uniqueValues {
		if bitmap := artifact.byValue[value]; bitmap != nil {
			it := bitmap.Iterator()
			for it.HasNext() {
				rownum := it.Next()
				if _, ok := parentValueByChild[rownum]; ok {
					continue
				}
				parentValueByChild[rownum] = value
				rownums = append(rownums, rownum)
			}
		}
	}
	if sortRows && len(rownums) > 1 {
		sort.Slice(rownums, func(i, j int) bool { return rownums[i] < rownums[j] })
	}
	stats := RelationshipReverseArtifactStats{
		Rows:          artifact.rows,
		Values:        uint64(len(artifact.byValue)),
		SourceValues:  len(uniqueValues),
		TargetRows:    uint64(len(rownums)),
		LookupElapsed: time.Since(start),
	}
	m.reverseArtifactLock.RUnlock()
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

// RelationshipAlignedValueSum groups child-domain BSI values by caller-supplied
// parent rows that are already aligned with childRows. This is the storage-side
// aggregate path for graph reducers that have already performed relationship
// vector alignment and only need mergeable grouped measure state.
func (m *BitmapIndex) RelationshipAlignedValueSum(index, valueField string, fromTime, toTime int64, childRows []uint64, parentRows []uint64) ([]RelationshipReverseArtifactSumGroup, RelationshipReverseArtifactSumStats, bool, error) {
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
	groupsByParent, projectionStats, aggregateElapsed, err := m.aggregateAlignedBSIValuesWithStats(index, valueField, fromTime, toTime, childRows, parentRows)
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

// RelationshipReverseArtifactStats returns maintained artifact cardinality
// without scanning source values or materializing candidates.
func (m *BitmapIndex) RelationshipReverseArtifactStats(index, field string) (RelationshipReverseArtifactStats, bool, error) {
	if !m.relationshipReverseArtifactEnabled(index, field) {
		return RelationshipReverseArtifactStats{}, false, nil
	}
	m.reverseArtifactLock.RLock()
	defer m.reverseArtifactLock.RUnlock()
	fields := m.reverseArtifactCache[index]
	if fields == nil {
		return RelationshipReverseArtifactStats{}, false, nil
	}
	artifact := fields[field]
	if artifact == nil {
		return RelationshipReverseArtifactStats{}, false, nil
	}
	return RelationshipReverseArtifactStats{
		Rows:   artifact.rows,
		Values: uint64(len(artifact.byValue)),
	}, true, nil
}

type relationshipReverseArtifactGroupRows struct {
	parentValue int64
	rows        []uint64
}

func (m *BitmapIndex) relationshipReverseArtifactSumRows(index, field string, childSet *roaring64.Bitmap, parentValues []uint64, start time.Time) ([]relationshipReverseArtifactGroupRows, RelationshipReverseArtifactSumStats, bool) {
	m.reverseArtifactLock.RLock()
	defer m.reverseArtifactLock.RUnlock()

	fields := m.reverseArtifactCache[index]
	if fields == nil {
		return nil, RelationshipReverseArtifactSumStats{}, false
	}
	artifact := fields[field]
	if artifact == nil {
		return nil, RelationshipReverseArtifactSumStats{}, false
	}
	values := relationshipReverseArtifactRequestedValues(artifact, parentValues)
	groups := make([]relationshipReverseArtifactGroupRows, 0, len(values))
	for _, value := range values {
		bitmap := artifact.byValue[value]
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
		Rows:          artifact.rows,
		Values:        uint64(len(artifact.byValue)),
		SourceValues:  len(values),
		LookupElapsed: time.Since(start),
	}, true
}

func relationshipReverseArtifactRequestedValues(artifact *relationshipReverseArtifact, parentValues []uint64) []int64 {
	if len(parentValues) == 0 {
		values := make([]int64, 0, len(artifact.byValue))
		for value := range artifact.byValue {
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
	positions := make(map[uint64][]int, len(rownums))
	for i, rownum := range rownums {
		positions[rownum] = append(positions[rownum], i)
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
		for i, rownum := range retainedRownums {
			if i >= len(retainedValues) || retainedValues[i] == nil {
				continue
			}
			for _, position := range positions[rownum] {
				if position >= len(parentRows) {
					continue
				}
				parentRow := parentRows[position]
				group := groupsByParent[parentRow]
				if group == nil {
					group = &RelationshipReverseArtifactSumGroup{
						ParentValue:       parentRow,
						RepresentativeRow: rownums[position],
						Sum:               big.NewInt(0),
					}
					groupsByParent[parentRow] = group
				}
				group.Sum.Add(group.Sum, retainedValues[i])
				group.Count++
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

func relationshipReverseArtifactCandidateCapacity(artifact *relationshipReverseArtifact, sourceValueCount int) int {
	if artifact == nil || sourceValueCount <= 0 || artifact.rows == 0 || len(artifact.byValue) == 0 {
		return 0
	}
	averageRowsPerValue := artifact.rows / uint64(len(artifact.byValue))
	if averageRowsPerValue == 0 {
		averageRowsPerValue = 1
	}
	estimate := uint64(sourceValueCount) * averageRowsPerValue
	if estimate > artifact.rows {
		estimate = artifact.rows
	}
	return int(estimate)
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

func (m *BitmapIndex) updateRelationshipReverseArtifactForBSIFragment(index, field string, oldBSI *roaring64.BSI, removedRows *roaring64.Bitmap, addedBSI *roaring64.BSI) {
	if !m.relationshipReverseArtifactEnabled(index, field) {
		return
	}
	m.reverseArtifactLock.Lock()
	defer m.reverseArtifactLock.Unlock()

	artifact := m.relationshipReverseArtifact(index, field)
	if removedRows != nil && oldBSI != nil {
		m.removeRelationshipReverseArtifactRows(artifact, oldBSI, removedRows)
	}
	if addedBSI != nil {
		m.addRelationshipReverseArtifactRows(artifact, addedBSI)
	}
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
		artifact = &relationshipReverseArtifact{byValue: make(map[int64]*roaring64.Bitmap)}
		m.reverseArtifactCache[index][field] = artifact
	}
	return artifact
}

func (m *BitmapIndex) removeRelationshipReverseArtifactRows(artifact *relationshipReverseArtifact, oldBSI *roaring64.BSI, removedRows *roaring64.Bitmap) {
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

func (m *BitmapIndex) addRelationshipReverseArtifactRows(artifact *relationshipReverseArtifact, addedBSI *roaring64.BSI) {
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
}

package server

import (
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
	start := time.Now()
	if !m.relationshipReverseArtifactEnabled(index, field) {
		return nil, RelationshipReverseArtifactStats{}, false, nil
	}
	m.reverseArtifactLock.RLock()
	fields := m.reverseArtifactCache[index]
	if fields == nil {
		m.reverseArtifactLock.RUnlock()
		return nil, RelationshipReverseArtifactStats{}, false, nil
	}
	artifact := fields[field]
	if artifact == nil {
		m.reverseArtifactLock.RUnlock()
		return nil, RelationshipReverseArtifactStats{}, false, nil
	}
	unique := make(map[int64]struct{}, len(sourceValues))
	candidates := roaring64.NewBitmap()
	for _, value := range sourceValues {
		if _, ok := unique[value]; ok {
			continue
		}
		unique[value] = struct{}{}
		if bitmap := artifact.byValue[value]; bitmap != nil {
			candidates.Or(bitmap)
		}
	}
	stats := RelationshipReverseArtifactStats{
		Rows:          artifact.rows,
		Values:        uint64(len(artifact.byValue)),
		SourceValues:  len(unique),
		TargetRows:    candidates.GetCardinality(),
		LookupElapsed: time.Since(start),
	}
	m.reverseArtifactLock.RUnlock()

	rownums := make([]uint64, 0, candidates.GetCardinality())
	it := candidates.Iterator()
	for it.HasNext() {
		rownums = append(rownums, it.Next())
	}
	return rownums, stats, true, nil
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

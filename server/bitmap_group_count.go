package server

import (
	"fmt"
	"sort"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

const bitmapGroupCountMaxValueCombinations = 4096

// BitmapGroupCount is one grouped count over bitmap-backed field values.
type BitmapGroupCount struct {
	Values []uint64
	Count  uint64
}

// BitmapGroupCountStats summarizes bitmap grouped count work.
type BitmapGroupCountStats struct {
	CandidateRows uint64
	FieldCount    int
	ValueCount    int
	Groups        int
}

// BitmapGroupCounts counts candidate rows grouped by low-cardinality
// bitmap-backed field values. It returns ok=false when any field is not
// bitmap-backed or the value-domain product is too large for this kernel.
func (m *BitmapIndex) BitmapGroupCounts(index string, fields []string, fromTime, toTime int64, foundSet *roaring64.Bitmap) ([]BitmapGroupCount, BitmapGroupCountStats, bool, error) {
	stats := BitmapGroupCountStats{FieldCount: len(fields)}
	if foundSet != nil {
		stats.CandidateRows = foundSet.GetCardinality()
	}
	if index == "" {
		return nil, stats, false, fmt.Errorf("index not specified for bitmap group count")
	}
	if len(fields) == 0 {
		return nil, stats, false, nil
	}
	fieldValues := make([][]bitmapGroupCountValue, 0, len(fields))
	combinations := 1
	from := time.Unix(0, fromTime).UTC()
	to := time.Unix(0, toTime).UTC()
	for _, field := range fields {
		if field == "" {
			return nil, stats, false, fmt.Errorf("field not specified for bitmap group count")
		}
		if !m.hasBitmapField(index, field) {
			return nil, stats, false, nil
		}
		rowIDs := m.listAllRowIDs(index, field)
		sort.Slice(rowIDs, func(i, j int) bool { return rowIDs[i] < rowIDs[j] })
		values := make([]bitmapGroupCountValue, 0, len(rowIDs))
		for _, rowID := range rowIDs {
			bitmap, err := m.timeRange(index, field, rowID, from, to, foundSet, false)
			if err != nil {
				return nil, stats, false, err
			}
			if bitmap == nil || bitmap.GetCardinality() == 0 {
				continue
			}
			values = append(values, bitmapGroupCountValue{ID: rowID, Bitmap: bitmap})
		}
		if len(values) == 0 {
			return nil, stats, true, nil
		}
		stats.ValueCount += len(values)
		combinations *= len(values)
		if combinations > bitmapGroupCountMaxValueCombinations {
			return nil, stats, false, nil
		}
		fieldValues = append(fieldValues, values)
	}
	groups := make([]BitmapGroupCount, 0)
	bitmapGroupCountWalk(fieldValues, 0, nil, nil, &groups)
	stats.Groups = len(groups)
	return groups, stats, true, nil
}

type bitmapGroupCountValue struct {
	ID     uint64
	Bitmap *roaring64.Bitmap
}

func (m *BitmapIndex) hasBitmapField(index, field string) bool {
	m.bitmapCacheLock.RLock()
	defer m.bitmapCacheLock.RUnlock()
	fields, ok := m.bitmapCache[index]
	if !ok {
		return false
	}
	_, ok = fields[field]
	return ok
}

func bitmapGroupCountWalk(fields [][]bitmapGroupCountValue, depth int, values []uint64, current *roaring64.Bitmap, groups *[]BitmapGroupCount) {
	if depth == len(fields) {
		if current == nil {
			return
		}
		count := current.GetCardinality()
		if count == 0 {
			return
		}
		*groups = append(*groups, BitmapGroupCount{
			Values: append([]uint64(nil), values...),
			Count:  count,
		})
		return
	}
	for _, value := range fields[depth] {
		next := value.Bitmap.Clone()
		if current != nil {
			next.And(current)
		}
		if next.GetCardinality() == 0 {
			continue
		}
		bitmapGroupCountWalk(fields, depth+1, append(values, value.ID), next, groups)
	}
}

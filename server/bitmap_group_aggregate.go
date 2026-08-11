package server

import (
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// BitmapGroupAggregateSpec describes one grouped aggregate over bitmap groups.
// Field is empty for COUNT(*).
type BitmapGroupAggregateSpec struct {
	Function string
	Field    string
}

// BitmapGroupAggregateValue contains raw BSI aggregate state for one aggregate
// slot. Count is the aggregate input cardinality for BSI slots, or COUNT(*).
type BitmapGroupAggregateValue struct {
	Count uint64
	Sum   *big.Int
	Min   *big.Int
	Max   *big.Int
}

// BitmapGroupAggregate is one grouped aggregate result over raw bitmap IDs.
type BitmapGroupAggregate struct {
	Values []uint64
	Aggs   []BitmapGroupAggregateValue
}

// BitmapGroupAggregateStats summarizes bitmap grouped aggregate work.
type BitmapGroupAggregateStats struct {
	BitmapGroupCountStats
	AggregateCount    int
	BSIFieldCount     int
	BSIProjectElapsed time.Duration
	AggregateElapsed  time.Duration
}

// BitmapGroupAggregates computes grouped aggregates over low-cardinality
// bitmap-backed group fields and BSI-backed measure fields.
func (m *BitmapIndex) BitmapGroupAggregates(index string, groupFields []string, aggregates []BitmapGroupAggregateSpec, fromTime, toTime int64, foundSet *roaring64.Bitmap) ([]BitmapGroupAggregate, BitmapGroupAggregateStats, bool, error) {
	groupBitmaps, groupStats, ok, err := m.bitmapGroupBitmaps(index, groupFields, fromTime, toTime, foundSet)
	stats := BitmapGroupAggregateStats{
		BitmapGroupCountStats: groupStats,
		AggregateCount:        len(aggregates),
	}
	if err != nil || !ok {
		return nil, stats, ok, err
	}
	if len(aggregates) == 0 {
		return nil, stats, false, nil
	}
	bsis := make(map[string]*roaring64.BSI)
	for _, aggregate := range aggregates {
		function := strings.ToLower(aggregate.Function)
		if function == "count" && aggregate.Field == "" {
			continue
		}
		switch function {
		case "sum", "avg", "min", "max":
		default:
			return nil, stats, false, nil
		}
		if aggregate.Field == "" {
			return nil, stats, false, fmt.Errorf("%s aggregate requires BSI field", aggregate.Function)
		}
		if _, seen := bsis[aggregate.Field]; seen {
			continue
		}
		attr, err := m.getFieldConfig(index, aggregate.Field)
		if err != nil {
			return nil, stats, false, err
		}
		if !attr.IsBSI() {
			return nil, stats, false, nil
		}
		projectStart := time.Now()
		bsi, _, err := m.projectBSIWithStats(index, aggregate.Field, fromTime, toTime, foundSet, false, false)
		stats.BSIProjectElapsed += time.Since(projectStart)
		if err != nil {
			return nil, stats, true, err
		}
		if bsi == nil {
			bsi = roaring64.NewDefaultBSI()
		}
		bsis[aggregate.Field] = bsi
		stats.BSIFieldCount++
	}

	aggregateStart := time.Now()
	groups := make([]BitmapGroupAggregate, 0, len(groupBitmaps))
	for _, group := range groupBitmaps {
		if group.Bitmap == nil || group.Bitmap.IsEmpty() {
			continue
		}
		values := make([]BitmapGroupAggregateValue, 0, len(aggregates))
		for _, aggregate := range aggregates {
			value, ok := bitmapGroupAggregateValue(group.Bitmap, aggregate, bsis)
			if !ok {
				return nil, stats, false, nil
			}
			values = append(values, value)
		}
		groups = append(groups, BitmapGroupAggregate{
			Values: append([]uint64(nil), group.Values...),
			Aggs:   values,
		})
	}
	stats.AggregateElapsed = time.Since(aggregateStart)
	stats.Groups = len(groups)
	return groups, stats, true, nil
}

func bitmapGroupAggregateValue(groupRows *roaring64.Bitmap, aggregate BitmapGroupAggregateSpec, bsis map[string]*roaring64.BSI) (BitmapGroupAggregateValue, bool) {
	function := strings.ToLower(aggregate.Function)
	if function == "count" && aggregate.Field == "" {
		return BitmapGroupAggregateValue{Count: groupRows.GetCardinality()}, true
	}
	bsi := bsis[aggregate.Field]
	if bsi == nil {
		return BitmapGroupAggregateValue{}, false
	}
	rows := groupRows.Clone()
	rows.And(bsi.GetExistenceBitmap())
	count := rows.GetCardinality()
	if count == 0 {
		return BitmapGroupAggregateValue{}, true
	}
	switch function {
	case "sum":
		sum, count := bsi.SumBigValues(rows)
		return BitmapGroupAggregateValue{Count: count, Sum: sum}, true
	case "avg":
		sum, count := bsi.SumBigValues(rows)
		return BitmapGroupAggregateValue{Count: count, Sum: sum}, true
	case "min":
		return BitmapGroupAggregateValue{Count: count, Min: bsi.MinMaxBig(0, roaring64.MIN, rows)}, true
	case "max":
		return BitmapGroupAggregateValue{Count: count, Max: bsi.MinMaxBig(0, roaring64.MAX, rows)}, true
	default:
		return BitmapGroupAggregateValue{}, false
	}
}

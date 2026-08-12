package server

//
// This file contains the Query API and all server side query related functions.
// It is important to note that while most of the bulk processing of a query happens
// server side, all of the map reduce functions and final query result compilation
// happen client side given a masterless architecture.  All BSI related functions are
// processed on the server and reduced to roaring bitmaps.  Only roaring bitmaps are
// returned to the client.
//

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	u "github.com/araddon/gou"

	"runtime/debug"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/shared"
)

var bsiInt64ValuesCapability struct {
	once      sync.Once
	supported bool
}

// SupportsBSIInt64Values reports whether the linked Roaring BSI library exposes
// typed batch value reads.
func SupportsBSIInt64Values() bool {
	bsiInt64ValuesCapability.once.Do(func() {
		bsiInt64ValuesCapability.supported = reflect.ValueOf(roaring64.NewDefaultBSI()).MethodByName("GetValues").IsValid()
	})
	return bsiInt64ValuesCapability.supported
}

// Query API endpoint for client wrapper functions.
func (m *BitmapIndex) Query(ctx context.Context, query *pb.BitmapQuery) (*pb.QueryResult, error) {
	return m.queryWithPriorIntersect(ctx, query, queryPriorIntersectCandidates{})
}

// QueryWithFoundSet evaluates query after seeding same-index INTERSECT fragments
// with a prior candidate bitmap.
func (m *BitmapIndex) QueryWithFoundSet(ctx context.Context, query *pb.BitmapQuery, index string, rownums []uint64) (*pb.QueryResult, error) {
	return m.queryWithPriorIntersect(ctx, query, queryPriorIntersectCandidates{
		index:  index,
		rows:   roaring64.BitmapOf(rownums...),
		seeded: true,
	})
}

func (m *BitmapIndex) queryWithPriorIntersect(ctx context.Context, query *pb.BitmapQuery, priorIntersect queryPriorIntersectCandidates) (*pb.QueryResult, error) {

	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("Panic recover: \n%s", debug.Stack())
			u.Error(err)
		}
	}()

	if query == nil {
		return nil, fmt.Errorf("query must not be nil")
	}

	d, errx := json.Marshal(&query)
	if errx != nil {
		u.Errorf("error: %v", errx)
		return nil, errx
	}
	u.Debugf("Query dump:\n%s\n\n", string(d))

	if query.Query == nil {
		return nil, fmt.Errorf("query fragment array must not be nil")
	}
	if len(query.Query) == 0 {
		return nil, fmt.Errorf("query fragment array must not be empty")
	}
	fromTime := time.Unix(0, query.FromTime).UTC()
	toTime := time.Unix(0, query.ToTime).UTC()

	dataMap := make(map[string]*roaring64.Bitmap)
	samples := make([]*shared.RowBitmap, 0)
	bsiQueryCache := make(map[string]*BSIBitmap)

	/*
	 *  Iterate over query predicates to see if there are any null checks or situations where there is no
	 *  union.  This can happen if there are only negated conditions in the query.
	 *  If so, gather existence for referenced table.
	 */
	foundUnion := false
	for _, v := range query.Query {
		if v.Operation == pb.QueryFragment_UNION {
			foundUnion = true
			break
		}

	}
	globalExistence := make(map[string]*roaring64.Bitmap)
	queryStats := bitmapQueryStats{startedAt: time.Now()}
	for _, v := range query.Query {
		if v.Index == "" {
			return nil, fmt.Errorf("Index not specified for query fragment %#v", v)
		}
		if v.Field == "" {
			return nil, fmt.Errorf("Field not specified for query fragment %#v", v)
		}
		if (v.NullCheck && !isExistenceSeedFragment(v)) || !foundUnion {
			ei, found := globalExistence[v.Index]
			if found {
				continue
			}
			table, ok := m.tableCache[v.Index]
			if !ok {
				return nil, fmt.Errorf("Cannot locate configuration for %s", v.Index)
			}
			pka, err := table.GetPrimaryKeyInfo()
			if err != nil {
				return nil, fmt.Errorf("timeRangeExistence GetPK info failed for %s - %v", v.Index, err)
			}
			var errx error
			existenceStart := time.Now()
			ei, _, errx = m.timeRangeExistenceWithCount(v.Index, pka[0].FieldName, fromTime, toTime)
			queryStats.existenceElapsed += time.Since(existenceStart)
			if errx != nil {
				return nil, fmt.Errorf("timeRangeExistence failed for %s - %v", v.Index, errx)
			}
			globalExistence[v.Index] = ei
		}
	}

	countsByFragment := make(map[string]uint64)
	// Main query flow loop
	for _, v := range query.Query {
		fragmentStart := time.Now()
		var bm *roaring64.Bitmap
		var err error
		if v.NullCheck {
			if isExistenceSeedFragment(v) {
				v.Operation = pb.QueryFragment_UNION
			} else if v.Negate {
				v.Operation = pb.QueryFragment_INTERSECT
			} else {
				v.Operation = pb.QueryFragment_DIFFERENCE
			}
		}
		if v.NullCheck && m.isBSI(v.Index, v.Field) {
			var count uint64
			bsiLoadStart := time.Now()
			bm, count, err = m.timeRangeExistenceWithCount(v.Index, v.Field, fromTime, toTime)
			queryStats.bsiLoadElapsed += time.Since(bsiLoadStart)
			if err != nil {
				return nil, fmt.Errorf("timeRangeExistence failed for %s - %v", v.Index, err)
			}
			countsByFragment[v.Id] = count
		} else if v.BsiOp > 0 {
			queryStats.bsiFragments++
			foundSet := priorIntersect.FoundSetFor(v)
			if foundSet != nil {
				queryStats.bsiFoundSetUses++
				queryStats.bsiFoundSetRows += foundSet.GetCardinality()
			}
			values := make([]*big.Int, len(v.Values))
			for i, v := range v.Values {
				values[i] = new(big.Int).SetBytes(v)
			}
			value := new(big.Int).SetBytes(v.Value)
			begin := new(big.Int).SetBytes(v.Begin)
			end := new(big.Int).SetBytes(v.End)

			cacheKey := fmt.Sprintf("%s/%s/%d/%d", v.Index, v.Field, fromTime.UnixNano(), toTime.UnixNano())
			bsi, bsiCacheHit := bsiQueryCache[cacheKey]
			if !bsiCacheHit {
				bsiLoadStart := time.Now()
				bsi, err = m.timeRangeBSI(v.Index, v.Field, fromTime, toTime, nil, false, true)
				queryStats.bsiLoadElapsed += time.Since(bsiLoadStart)
				if err != nil {
					return nil, err
				}
				bsiQueryCache[cacheKey] = bsi
			}
			// Evaluate BSI operation resulting in roaring bitmap
			bsiCompareStart := time.Now()
			switch v.BsiOp {
			case pb.QueryFragment_LT:
				bm = bsi.CompareBigValue(0, roaring64.LT, value, nil, foundSet)
			case pb.QueryFragment_LE:
				bm = bsi.CompareBigValue(0, roaring64.LE, value, nil, foundSet)
			case pb.QueryFragment_EQ:
				bm = bsi.CompareBigValue(0, roaring64.EQ, value, nil, foundSet)
			case pb.QueryFragment_GE:
				bm = bsi.CompareBigValue(0, roaring64.GE, value, nil, foundSet)
			case pb.QueryFragment_GT:
				bm = bsi.CompareBigValue(0, roaring64.GT, value, nil, foundSet)
			case pb.QueryFragment_RANGE:
				bm = bsi.CompareBigValue(0, roaring64.RANGE, begin, end, foundSet)
			case pb.QueryFragment_BATCH_EQ:
				bm = bsi.BatchEqualBig(0, values)
			}
			queryStats.bsiCompareElapsed += time.Since(bsiCompareStart)
		} else if len(v.Values) > 0 {
			queryStats.standardFragments++
			foundSet := priorIntersect.FoundSetFor(v)
			if foundSet != nil {
				queryStats.standardFoundSetUses++
				queryStats.standardFoundSetRows += foundSet.GetCardinality()
			}
			bitmaps := make([]*roaring64.Bitmap, 0, len(v.Values))
			for _, raw := range v.Values {
				rowID := new(big.Int).SetBytes(raw).Uint64()
				standardStart := time.Now()
				x, err := m.timeRange(v.Index, v.Field, rowID, fromTime, toTime, foundSet, false)
				queryStats.standardElapsed += time.Since(standardStart)
				if err != nil {
					return nil, err
				}
				if x.GetCardinality() > 0 {
					bitmaps = append(bitmaps, x)
				}
			}
			if len(bitmaps) == 0 {
				bm = roaring64.New()
			} else {
				bm = roaring64.ParOr(0, bitmaps...)
			}
		} else {
			if v.SamplePct > 0 || v.NullCheck {
				queryStats.scanFragments++
				var x *roaring64.Bitmap
				exist := make([]*roaring64.Bitmap, 0)
				for _, row := range m.listAllRowIDs(v.Index, v.Field) {
					scanStart := time.Now()
					if x, err = m.timeRange(v.Index, v.Field, row, fromTime, toTime, nil, false); err != nil {
						queryStats.scanElapsed += time.Since(scanStart)
						return nil, err
					}
					queryStats.scanElapsed += time.Since(scanStart)
					if x.GetCardinality() == 0 {
						continue
					}
					if v.NullCheck {
						exist = append(exist, x)
					} else {
						samples = append(samples, shared.NewRowBitmap(v.Field, row, x))
					}
				}
				if len(exist) > 0 {
					bm = roaring64.ParOr(0, exist...)
				}
			} else {
				queryStats.standardFragments++
				foundSet := priorIntersect.FoundSetFor(v)
				if foundSet != nil {
					queryStats.standardFoundSetUses++
					queryStats.standardFoundSetRows += foundSet.GetCardinality()
				}
				standardStart := time.Now()
				if bm, err = m.timeRange(v.Index, v.Field, v.RowID, fromTime, toTime, foundSet, false); err != nil {
					queryStats.standardElapsed += time.Since(standardStart)
					return nil, err
				}
				queryStats.standardElapsed += time.Since(standardStart)
			}
		}

		if bm != nil {
			dataMap[v.Id] = bm
		} else {
			dataMap[v.Id] = roaring64.NewBitmap()
		}
		priorIntersect.Observe(v, dataMap[v.Id])
		queryStats.fragmentElapsed += time.Since(fragmentStart)
	}

	reduceStart := time.Now()
	ir := shared.FromProto(query, dataMap).Reduce()
	queryStats.reduceElapsed = time.Since(reduceStart)
	if len(countsByFragment) == 1 && len(query.Query) == 1 {
		if count, ok := countsByFragment[query.Query[0].Id]; ok {
			ir.SetCount(count)
		}
	}
	if len(samples) > 0 {
		ir.AddSamples(samples)
	}
	if ge, ok := globalExistence[ir.Index]; ok {
		ir.AddExistence(ge)
	}
	marshalStart := time.Now()
	result, err := ir.MarshalQueryResult()
	queryStats.marshalElapsed = time.Since(marshalStart)
	if err != nil {
		return nil, err
	}
	result.Samples = append(result.Samples, queryStats.probeSamples()...)
	return result, nil
}

type bitmapQueryStats struct {
	startedAt            time.Time
	existenceElapsed     time.Duration
	standardElapsed      time.Duration
	bsiLoadElapsed       time.Duration
	bsiCompareElapsed    time.Duration
	scanElapsed          time.Duration
	fragmentElapsed      time.Duration
	reduceElapsed        time.Duration
	marshalElapsed       time.Duration
	standardFragments    int
	bsiFragments         int
	scanFragments        int
	bsiFoundSetUses      int
	bsiFoundSetRows      uint64
	standardFoundSetUses int
	standardFoundSetRows uint64
}

func (s bitmapQueryStats) probeSamples() []*pb.BitmapResult {
	total := time.Since(s.startedAt)
	const section = "direct_bitmap_server"
	return []*pb.BitmapResult{
		shared.NewQueryResultProbeSample(section, "query_elapsed", total.String(), ""),
		shared.NewQueryResultProbeSample(section, "existence_elapsed", s.existenceElapsed.String(), ""),
		shared.NewQueryResultProbeSample(section, "standard_bitmap_elapsed", s.standardElapsed.String(), ""),
		shared.NewQueryResultProbeSample(section, "bsi_load_elapsed", s.bsiLoadElapsed.String(), ""),
		shared.NewQueryResultProbeSample(section, "bsi_compare_elapsed", s.bsiCompareElapsed.String(), ""),
		shared.NewQueryResultProbeSample(section, "scan_bitmap_elapsed", s.scanElapsed.String(), ""),
		shared.NewQueryResultProbeSample(section, "fragment_loop_elapsed", s.fragmentElapsed.String(), ""),
		shared.NewQueryResultProbeSample(section, "reduce_elapsed", s.reduceElapsed.String(), ""),
		shared.NewQueryResultProbeSample(section, "marshal_elapsed", s.marshalElapsed.String(), ""),
		shared.NewQueryResultProbeSample(section, "standard_fragment_count", strconv.Itoa(s.standardFragments), ""),
		shared.NewQueryResultProbeSample(section, "bsi_fragment_count", strconv.Itoa(s.bsiFragments), ""),
		shared.NewQueryResultProbeSample(section, "scan_fragment_count", strconv.Itoa(s.scanFragments), ""),
		shared.NewQueryResultProbeSample(section, "bsi_found_set_count", strconv.Itoa(s.bsiFoundSetUses), ""),
		shared.NewQueryResultProbeSample(section, "bsi_found_set_rows", strconv.FormatUint(s.bsiFoundSetRows, 10), ""),
		shared.NewQueryResultProbeSample(section, "standard_found_set_count", strconv.Itoa(s.standardFoundSetUses), ""),
		shared.NewQueryResultProbeSample(section, "standard_found_set_rows", strconv.FormatUint(s.standardFoundSetRows, 10), ""),
	}
}

type queryPriorIntersectCandidates struct {
	index  string
	rows   *roaring64.Bitmap
	seeded bool
}

func (s *queryPriorIntersectCandidates) FoundSetFor(fragment *pb.QueryFragment) *roaring64.Bitmap {
	if s == nil || s.rows == nil || fragment == nil {
		return nil
	}
	if s.index != fragment.Index || s.rows.GetCardinality() == 0 {
		return nil
	}
	if s.seeded {
		if !queryFragmentCanUseSeededFoundSet(fragment) {
			return nil
		}
		return s.rows
	}
	if !queryFragmentCanUsePriorIntersect(fragment) {
		return nil
	}
	return s.rows
}

func (s *queryPriorIntersectCandidates) Observe(fragment *pb.QueryFragment, bitmap *roaring64.Bitmap) {
	if s == nil || fragment == nil || bitmap == nil || !queryFragmentCanUsePriorIntersect(fragment) {
		return
	}
	if s.rows == nil {
		s.index = fragment.Index
		s.rows = bitmap.Clone()
		return
	}
	if s.index != fragment.Index {
		s.index = ""
		s.rows = nil
		return
	}
	s.rows.And(bitmap)
}

func queryFragmentCanUsePriorIntersect(fragment *pb.QueryFragment) bool {
	return fragment != nil &&
		fragment.Operation == pb.QueryFragment_INTERSECT &&
		!fragment.OrContext &&
		!fragment.Negate &&
		!fragment.NullCheck &&
		fragment.SamplePct == 0
}

func queryFragmentCanUseSeededFoundSet(fragment *pb.QueryFragment) bool {
	return fragment != nil &&
		(fragment.Operation == pb.QueryFragment_INTERSECT || fragment.Operation == pb.QueryFragment_UNION) &&
		!fragment.Negate &&
		!fragment.NullCheck &&
		fragment.SamplePct == 0
}

func isExistenceSeedFragment(fragment *pb.QueryFragment) bool {
	return fragment != nil &&
		fragment.NullCheck &&
		fragment.Negate &&
		fragment.Operation == pb.QueryFragment_UNION
}

func truncateTime(tr time.Time, tq string) time.Time {
	var rts int64
	if tq == "YMD" {
		rts = time.Date(tr.Year(), tr.Month(), tr.Day(), 0, 0, 0, 0, tr.Location()).UnixNano()
	} else { // YMDH
		rts = time.Date(tr.Year(), tr.Month(), tr.Day(), tr.Hour(), 0, 0, 0,
			tr.Location()).UnixNano()
	}
	return time.Unix(0, rts)
}

// Walk the time range and assemble a union of all bitmap fields.
func (m *BitmapIndex) timeRange(index, field string, rowID uint64, fromTime,
	toTime time.Time, foundSet *roaring64.Bitmap, negate bool) (*roaring64.Bitmap, error) {

	m.bitmapCacheLock.RLock()
	defer m.bitmapCacheLock.RUnlock()

	attr, err := m.getFieldConfig(index, field)
	if err != nil {
		return nil, err
	}
	tq := attr.TimeQuantumType
	fromTime = truncateTime(fromTime, tq)
	toTime = truncateTime(toTime, tq)
	result := roaring64.NewBitmap()
	a := make([]*roaring64.Bitmap, 0)

	if tq == "" { // No time quantum
		if rm, ok := m.bitmapCache[index][field][rowID]; ok {
			for ts, bm := range rm {
				hashKey := fmt.Sprintf("%s/%s/%d/%s", index, field, rowID, formatShardTime(time.Unix(0, ts)))
				if !m.Member(hashKey) {
					continue
				}
				if foundSet != nil {
					b := bm.Bits.Clone()
					if negate {
						b.AndNot(foundSet)
					} else {
						b.And(foundSet)
					}
					if b.GetCardinality() == 0 {
						continue
					}
					a = append(a, b)
				} else {
					a = append(a, bm.Bits)
				}
				u.Debugf("timeRange No Quantum selecting %s", hashKey)
			}
			if len(a) > 0 {
				result = roaring64.ParOr(0, a...)
			}
		}
	} else {
		if rm, ok := m.bitmapCache[index][field][rowID]; ok {
			for ts, bitmap := range rm {
				rts := truncateTime(time.Unix(0, ts).UTC(), tq).UnixNano()
				if rts < fromTime.UnixNano() || rts > toTime.UnixNano() {
					continue
				}
				hashKey := fmt.Sprintf("%s/%s/%d/%s", index, field, rowID, formatShardTime(time.Unix(0, ts)))
				if !m.Member(hashKey) {
					continue
				}
				if foundSet != nil {
					b := bitmap.Bits.Clone()
					if negate {
						b.AndNot(foundSet)
					} else {
						b.And(foundSet)
					}
					if b.GetCardinality() == 0 {
						continue
					}
					a = append(a, b)
					u.Debugf("timeRange %s selecting %s", tq, hashKey)
				} else {
					a = append(a, bitmap.Bits)
					u.Debugf("timeRange %s selecting %s", tq, hashKey)
				}
			}
		}
		result = roaring64.ParOr(0, a...)
	}
	return result, nil
}

func (m *BitmapIndex) listAllRowIDs(index, field string) []uint64 {

	m.bitmapCacheLock.RLock()
	defer m.bitmapCacheLock.RUnlock()
	rowIDs := make([]uint64, 0)
	for k := range m.bitmapCache[index][field] {
		rowIDs = append(rowIDs, k)
	}
	return rowIDs
}

// ProjectBSIStats captures in-process BSI projection work for runtime probes.
type ProjectBSIStats struct {
	ShardsVisited    int
	ShardsInWindow   int
	ShardsLocal      int
	ShardsRetained   int
	RetainedRows     uint64
	RetainBypassRows uint64
	RetainElapsed    time.Duration
	ValueElapsed     time.Duration
	MergeElapsed     time.Duration
}

// CompareBSIFieldsStats captures local same-row BSI comparison work.
type CompareBSIFieldsStats struct {
	Left           ProjectBSIStats
	Right          ProjectBSIStats
	CompareElapsed time.Duration
	OutputRows     uint64
}

// Walk the time range and assemble a union of all BSI fields.
func (m *BitmapIndex) timeRangeBSI(index, field string, fromTime, toTime time.Time,
	foundSet *roaring64.Bitmap, negate bool, ownedOnly bool, stats ...*ProjectBSIStats) (*BSIBitmap, error) {

	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()

	return m.timeRangeBSILocked(index, field, fromTime, toTime, foundSet, negate, ownedOnly, stats...)
}

func (m *BitmapIndex) timeRangeBSILocked(index, field string, fromTime, toTime time.Time,
	foundSet *roaring64.Bitmap, negate bool, ownedOnly bool, stats ...*ProjectBSIStats) (*BSIBitmap, error) {

	var stat *ProjectBSIStats
	if len(stats) > 0 {
		stat = stats[0]
	}

	attr, err := m.getFieldConfig(index, field)
	if err != nil {
		return nil, err
	}
	tq := attr.TimeQuantumType
	fromTime = truncateTime(fromTime, tq)
	toTime = truncateTime(toTime, tq)
	result := m.newBSIBitmap(index, field)
	yr, mn, da := fromTime.Date()
	lookupTime := time.Date(yr, mn, da, 0, 0, 0, 0, time.UTC)
	a := make([]*roaring64.BSI, 0)

	if tq == "" { // No time quantum
		if bm, ok := m.bsiCache[index][field][0]; ok {
			if stat != nil {
				stat.ShardsVisited++
				stat.ShardsInWindow++
				stat.ShardsLocal++
			}
			if foundSet != nil {
				retainStart := time.Now()
				x, retainedRows, bypassedRows := retainedProjectionBSI(bm.BSI, foundSet, negate)
				if stat != nil {
					stat.RetainElapsed += time.Since(retainStart)
				}
				if x != nil {
					a = append(a, x)
					if stat != nil {
						stat.ShardsRetained++
						stat.RetainedRows += retainedRows
						stat.RetainBypassRows += bypassedRows
					}
				}
			} else {
				a = append(a, bm.BSI)
				if stat != nil {
					stat.ShardsRetained++
					stat.RetainedRows += bm.BSI.GetCardinality()
				}
			}
			hashKey := fmt.Sprintf("%s/%s/%s", index, field, formatShardTime(lookupTime))
			u.Debugf("timeRangeBSI No Quantum selecting %s", hashKey)
			mergeStart := time.Now()
			if len(a) > 0 {
				result.BSI.ParOr(0, a...)
			}
			if stat != nil {
				stat.MergeElapsed += time.Since(mergeStart)
			}
		}
	} else {
		if tm, ok := m.bsiCache[index][field]; ok {
			for ts, bsi := range tm {
				if stat != nil {
					stat.ShardsVisited++
				}
				rts := truncateTime(time.Unix(0, ts).UTC(), tq).UnixNano()
				if rts < fromTime.UnixNano() || rts > toTime.UnixNano() {
					continue
				}
				if stat != nil {
					stat.ShardsInWindow++
				}
				hashKey := fmt.Sprintf("%s/%s/%s", index, field, formatShardTime(time.Unix(0, ts)))
				if ownedOnly && !m.Member(hashKey) {
					continue
				}
				if stat != nil {
					stat.ShardsLocal++
				}
				if foundSet != nil {
					retainStart := time.Now()
					x, retainedRows, bypassedRows := retainedProjectionBSI(bsi.BSI, foundSet, negate)
					if stat != nil {
						stat.RetainElapsed += time.Since(retainStart)
					}
					if x == nil {
						continue
					}
					a = append(a, x)
					if stat != nil {
						stat.ShardsRetained++
						stat.RetainedRows += retainedRows
						stat.RetainBypassRows += bypassedRows
					}
					u.Debugf("timeRangeBSI %s selecting %s with foundSet = %d", tq, hashKey, foundSet.GetCardinality())
				} else {
					if bsi.BSI.GetCardinality() == 0 {
						continue
					}
					a = append(a, bsi.BSI)
					if stat != nil {
						stat.ShardsRetained++
						stat.RetainedRows += bsi.BSI.GetCardinality()
					}
					u.Debugf("timeRangeBSI %s selecting %s", tq, hashKey)
				}
			}
		}
		mergeStart := time.Now()
		if len(a) > 0 {
			result.BSI.ParOr(0, a...)
		}
		if stat != nil {
			stat.MergeElapsed += time.Since(mergeStart)
		}
	}
	return result, nil
}

func retainedProjectionBSI(source *roaring64.BSI, foundSet *roaring64.Bitmap, negate bool) (*roaring64.BSI, uint64, uint64) {
	if source == nil {
		return nil, 0, 0
	}
	if foundSet == nil {
		cardinality := source.GetCardinality()
		if cardinality == 0 {
			return nil, 0, 0
		}
		return source, cardinality, 0
	}
	existence := source.GetExistenceBitmap()
	if negate {
		retainSet := roaring64.AndNot(existence, foundSet)
		retainedRows := retainSet.GetCardinality()
		if retainedRows == 0 {
			return nil, 0, 0
		}
		retained := source.NewBSIRetainSet(retainSet)
		retainedRows = retained.GetCardinality()
		if retainedRows == 0 {
			return nil, 0, 0
		}
		return retained, retainedRows, 0
	}

	if !existence.Intersects(foundSet) {
		return nil, 0, 0
	}
	existenceRows := existence.GetCardinality()
	if foundSet.GetCardinality() >= existenceRows {
		if existence.AndCardinality(foundSet) == existenceRows {
			return source, existenceRows, existenceRows
		}
	}

	retained := source.NewBSIRetainSet(foundSet)
	retainedRows := retained.GetCardinality()
	if retainedRows == 0 {
		return nil, 0, 0
	}
	return retained, retainedRows, 0
}

// Walk the time range and assemble a union of all BSI esistence
func (m *BitmapIndex) timeRangeExistence(index, field string, fromTime, toTime time.Time) (*roaring64.Bitmap, error) {
	result, _, err := m.timeRangeExistenceWithCount(index, field, fromTime, toTime)
	return result, err
}

func (m *BitmapIndex) timeRangeExistenceWithCount(index, field string, fromTime, toTime time.Time) (*roaring64.Bitmap, uint64, error) {
	attr, err := m.getFieldConfig(index, field)
	if err != nil {
		return nil, 0, err
	}
	tq := attr.TimeQuantumType
	fromTime = truncateTime(fromTime, tq)
	toTime = truncateTime(toTime, tq)
	if cached, count, ok := m.cachedSeedBitmap(index, field, fromTime, toTime); ok {
		return cached, count, nil
	}

	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()

	results := make([]*roaring64.Bitmap, 0)
	var count uint64
	yr, mn, da := fromTime.Date()
	lookupTime := time.Date(yr, mn, da, 0, 0, 0, 0, time.UTC)
	if tq == "" { // No time quantum
		hashKey := fmt.Sprintf("%s/%s/%s", index, field, formatShardTime(lookupTime))
		if fm, ok := m.bsiCache[index]; ok {
			if tm, ok := fm[field]; ok {
				for _, bm := range tm {
					existence := bm.BSI.GetExistenceBitmap()
					count += existence.GetCardinality()
					results = append(results, existence)
				}
			}
		}
		u.Debugf("timeRangeExistence No Quantum selecting %s", hashKey)
	} else {
		if fm, ok := m.bsiCache[index]; ok {
			tm := fm[field]
			for ts, bm := range tm {
				rts := truncateTime(time.Unix(0, ts).UTC(), tq).UnixNano()
				if rts < fromTime.UnixNano() || rts > toTime.UnixNano() {
					continue
				}
				hashKey := fmt.Sprintf("%s/%s/%s", index, field, formatShardTime(time.Unix(0, ts)))
				u.Debugf("timeRangeExistence %s selecting %s", tq, hashKey)
				existence := bm.BSI.GetExistenceBitmap()
				count += existence.GetCardinality()
				results = append(results, existence)
			}
		}
	}
	result := roaring64.ParOr(0, results...)
	m.storeSeedBitmap(index, field, fromTime, toTime, result, count)
	return result.Clone(), count, nil
}

// Join - Once the client has mapreduced the initial query fragment results, A followup call is made to
// the Join API.   This API is responsible for mapping the column ID spaces for the child index
// to the column ID space of the parent (driver) index.  It does this by using the values contained
// in a foreign key BSI as a vector to the parent column ID values.
//
// Once these values are transposed they are returned as a roaring bitmap and intersected with
// the parent index results to formulate the final results.
func (m *BitmapIndex) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {

	fromTime := time.Unix(0, req.FromTime).UTC()
	toTime := time.Unix(0, req.ToTime).UTC()

	if req.DriverIndex == "" {
		return nil, fmt.Errorf("Index not specified for join criteria")
	}
	if req.FkFields == nil || len(req.FkFields) == 0 {
		return nil, fmt.Errorf("FK Field(s) not specified for join criteria")
	}

	foundSet := roaring64.NewBitmap()
	if err := foundSet.UnmarshalBinary(req.FoundSet); err != nil {
		return nil, err
	}

	filterSets := make([]*roaring64.Bitmap, len(req.FilterSets))
	for i, fsData := range req.FilterSets {
		filterSet := roaring64.NewBitmap()
		if err := filterSet.UnmarshalBinary(fsData); err != nil {
			return nil, err
		}
		filterSets[i] = filterSet
	}

	bsiArray := make([]*BSIBitmap, len(req.FkFields))
	minCardValue := uint64(1<<64 - 1)
	minCardIndex := 0
	for i, v := range req.FkFields {
		start := time.Now()
		//bsi, err := m.timeRangeBSI(req.DriverIndex, v, fromTime, toTime, foundSet, req.Negate)
		bsi, err := m.timeRangeBSI(req.DriverIndex, v, fromTime, toTime, foundSet, false, true)
		if err != nil {
			err2 := fmt.Errorf("cannot find FK BSI for %s %s - %v", req.DriverIndex, v, err)
			return nil, err2
		}
		c := bsi.GetCardinality()
		if c < minCardValue {
			minCardValue = c
			minCardIndex = i
		}
		bsiArray[i] = bsi
		elapsed := time.Since(start)
		u.Debugf("inner join timeRange BSI elapsed time %v for %s %s", elapsed, req.DriverIndex, v)
	}

	// Process the final FK relation with TransposeWithCounts
	start := time.Now()
	transposeBsi := bsiArray[minCardIndex]
	jr := transposeBsi.TransposeWithCounts(0, transposeBsi.GetExistenceBitmap(), filterSets[minCardIndex])
	elapsed := time.Since(start)
	u.Debugf("inner join transpose elapsed time %v", elapsed)

	data, err := jr.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return &pb.JoinResponse{Results: data}, nil
}

// Projection - Retrieve bitmaps to be included in a result set projection.
func (m *BitmapIndex) Projection(ctx context.Context, req *pb.ProjectionRequest) (*pb.ProjectionResponse, error) {

	u.Debugf("Projection retrieval started for %v - %v", req.Index, req.Fields)

	fromTime := time.Unix(0, req.FromTime).UTC()
	toTime := time.Unix(0, req.ToTime).UTC()

	if req.Index == "" {
		return nil, fmt.Errorf("index not specified for projection criteria")
	}
	if req.Fields == nil || len(req.Fields) == 0 {
		return nil, fmt.Errorf("one or more fields not specified for projection criteria")
	}

	var foundSet *roaring64.Bitmap
	if len(req.FoundSet) > 0 {
		foundSet = roaring64.NewBitmap()
		if err := foundSet.UnmarshalBinary(req.FoundSet); err != nil {
			return nil, err
		}
	}

	bitmapResults := make([]*pb.BitmapResult, 0)
	bsiResults := make([]*pb.BSIResult, 0)

	start := time.Now()
	var err2 error
	for _, v := range req.Fields {
		attr, err := m.getFieldConfig(req.Index, v)
		if err != nil {
			return nil, err
		}
		if _, ok := m.bitmapCache[req.Index][v]; ok {
			var x *roaring64.Bitmap
			for _, row := range m.listAllRowIDs(req.Index, v) {
				if x, err2 = m.timeRange(req.Index, v, row, fromTime, toTime, foundSet, req.Negate); err2 != nil {
					return nil, err2
				}
				if x.GetCardinality() == 0 {
					continue
				}
				bmr := &pb.BitmapResult{Field: v, RowId: row}
				if bmr.Bitmap, err2 = x.MarshalBinary(); err2 != nil {
					return nil, fmt.Errorf("Error marshalling bitmap for field %s, rowId %d, [%v]", v, row, err2)
				}
				bitmapResults = append(bitmapResults, bmr)
			}
		}
		if _, ok := m.bsiCache[req.Index][v]; ok {
			var bsi *BSIBitmap
			//if bsi, err2 = m.timeRangeBSI(req.Index, v, fromTime, toTime, foundSet, req.Negate); err2 != nil {
			if bsi, err2 = m.timeRangeBSI(req.Index, v, fromTime, toTime, foundSet, false, true); err2 != nil {
				return nil, fmt.Errorf("Error ranging projection BSI for %s %s - %v", req.Index, v, err2)
			}
			if bsi.GetCardinality() == 0 {
				continue
			}
			bsir := &pb.BSIResult{Field: v}
			if bsir.Bitmaps, err2 = bsi.BSI.MarshalBinary(); err2 != nil {
				return nil, fmt.Errorf("Error marshalling BSI for field %s, [%v]", v, err2)
			}
			bsiResults = append(bsiResults, bsir)
		} else {
			if attr.IsBSI() || attr.MappingStrategy == "ParentRelation" {
				bsir := &pb.BSIResult{Field: v}
				bsir.Bitmaps, _ = m.newBSIBitmap(req.Index, v).MarshalBinary()
				bsiResults = append(bsiResults, bsir)
			}
		}
	}
	elapsed := time.Since(start)
	u.Debugf("Projection retrieval elapsed time %v", elapsed)
	return &pb.ProjectionResponse{BitmapResults: bitmapResults, BsiResults: bsiResults}, nil
}

// CompareBSIFields applies a same-row BSI comparison on this node and returns
// matching local rownums as a roaring bitmap.
func (m *BitmapIndex) CompareBSIFields(ctx context.Context, req *pb.CompareBSIFieldsRequest) (*pb.CompareBSIFieldsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var foundSet *roaring64.Bitmap
	if len(req.GetFoundSet()) > 0 {
		foundSet = roaring64.NewBitmap()
		if err := foundSet.UnmarshalBinary(req.GetFoundSet()); err != nil {
			return nil, err
		}
	}
	matches, stats, err := m.CompareBSIFieldsWithStats(
		req.GetIndex(),
		req.GetLeftField(),
		req.GetRightField(),
		req.GetFromTime(),
		req.GetToTime(),
		foundSet,
		roaring64.Operation(req.GetOperation()),
		req.GetInvert(),
	)
	if err != nil {
		return nil, err
	}
	if matches == nil {
		matches = roaring64.NewBitmap()
	}
	data, err := matches.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return &pb.CompareBSIFieldsResponse{
		Rownums: data,
		Stats:   compareBSIFieldsStatsToProto(stats),
	}, nil
}

// BitmapGroupAggregates computes node-local grouped aggregate partials over
// bitmap-backed group fields and BSI-backed measure fields. Cluster callers
// merge partial aggregate state by raw group value IDs.
func (m *BitmapIndex) BitmapGroupAggregates(ctx context.Context, req *pb.BitmapGroupAggregatesRequest) (*pb.BitmapGroupAggregatesResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var foundSet *roaring64.Bitmap
	if len(req.GetFoundSet()) > 0 {
		foundSet = roaring64.NewBitmap()
		if err := foundSet.UnmarshalBinary(req.GetFoundSet()); err != nil {
			return nil, err
		}
	}
	aggregates := make([]BitmapGroupAggregateSpec, 0, len(req.GetAggregates()))
	for _, aggregate := range req.GetAggregates() {
		if aggregate == nil {
			continue
		}
		aggregates = append(aggregates, BitmapGroupAggregateSpec{
			Function: aggregate.GetFunction(),
			Field:    aggregate.GetField(),
		})
	}
	groups, stats, ok, err := m.BitmapGroupAggregatesStorage(
		req.GetIndex(),
		req.GetGroupFields(),
		aggregates,
		req.GetFromTime(),
		req.GetToTime(),
		foundSet,
	)
	if err != nil {
		return nil, err
	}
	return &pb.BitmapGroupAggregatesResponse{
		Groups: bitmapGroupAggregatesToProto(groups),
		Stats:  bitmapGroupAggregateStatsToProto(stats),
		Ok:     ok,
	}, nil
}

func bitmapGroupAggregatesToProto(groups []BitmapGroupAggregate) []*pb.BitmapGroupAggregateGroup {
	protoGroups := make([]*pb.BitmapGroupAggregateGroup, 0, len(groups))
	for _, group := range groups {
		protoAggs := make([]*pb.BitmapGroupAggregateValue, 0, len(group.Aggs))
		for _, aggregate := range group.Aggs {
			protoAggs = append(protoAggs, bitmapGroupAggregateValueToProto(aggregate))
		}
		protoGroups = append(protoGroups, &pb.BitmapGroupAggregateGroup{
			Values: append([]uint64(nil), group.Values...),
			Aggs:   protoAggs,
		})
	}
	return protoGroups
}

func bitmapGroupAggregateValueToProto(value BitmapGroupAggregateValue) *pb.BitmapGroupAggregateValue {
	result := &pb.BitmapGroupAggregateValue{Count: value.Count}
	if value.Sum != nil {
		result.Sum = value.Sum.String()
	}
	if value.Min != nil {
		result.Min = value.Min.String()
	}
	if value.Max != nil {
		result.Max = value.Max.String()
	}
	return result
}

func bitmapGroupAggregateStatsToProto(stats BitmapGroupAggregateStats) *pb.BitmapGroupAggregateStats {
	return &pb.BitmapGroupAggregateStats{
		CandidateRows:          stats.CandidateRows,
		FieldCount:             uint64(stats.FieldCount),
		ValueCount:             uint64(stats.ValueCount),
		Groups:                 uint64(stats.Groups),
		AggregateCount:         uint64(stats.AggregateCount),
		BsiFieldCount:          uint64(stats.BSIFieldCount),
		BsiProjectElapsedNanos: stats.BSIProjectElapsed.Nanoseconds(),
		AggregateElapsedNanos:  stats.AggregateElapsed.Nanoseconds(),
		ValueSetElapsedNanos:   stats.ValueSetElapsed.Nanoseconds(),
		SumElapsedNanos:        stats.SumElapsed.Nanoseconds(),
		MinMaxElapsedNanos:     stats.MinMaxElapsed.Nanoseconds(),
	}
}

// RelationshipReverseArtifactCandidates returns node-local child rows for a
// maintained parent-to-child reverse relationship artifact.
func (m *BitmapIndex) RelationshipReverseArtifactCandidates(ctx context.Context, req *pb.RelationshipReverseArtifactCandidatesRequest) (*pb.RelationshipReverseArtifactCandidatesResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rownums, parentValues, stats, ok, err := m.RelationshipReverseArtifactCandidateValues(
		req.GetIndex(),
		req.GetField(),
		req.GetSourceValues(),
	)
	if err != nil {
		return nil, err
	}
	values := make([]*pb.RelationshipReverseArtifactParentValue, 0, len(parentValues))
	for rownum, parentValue := range parentValues {
		values = append(values, &pb.RelationshipReverseArtifactParentValue{
			Rownum:      rownum,
			ParentValue: parentValue,
		})
	}
	return &pb.RelationshipReverseArtifactCandidatesResponse{
		Rownums:      append([]uint64(nil), rownums...),
		ParentValues: values,
		Stats:        relationshipReverseArtifactStatsToProto(stats),
		Ok:           ok,
	}, nil
}

// RelationshipReverseArtifactStats returns node-local cardinality for a
// maintained parent-to-child reverse relationship artifact.
func (m *BitmapIndex) RelationshipReverseArtifactStats(ctx context.Context, req *pb.RelationshipReverseArtifactStatsRequest) (*pb.RelationshipReverseArtifactStatsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stats, ok, err := m.RelationshipReverseArtifactStatsStorage(req.GetIndex(), req.GetField())
	if err != nil {
		return nil, err
	}
	return &pb.RelationshipReverseArtifactStatsResponse{
		Stats: relationshipReverseArtifactStatsToProto(stats),
		Ok:    ok,
	}, nil
}

func relationshipReverseArtifactStatsToProto(stats RelationshipReverseArtifactStats) *pb.RelationshipReverseArtifactStats {
	return &pb.RelationshipReverseArtifactStats{
		Rows:               stats.Rows,
		Values:             stats.Values,
		SourceValues:       uint64(stats.SourceValues),
		TargetRows:         stats.TargetRows,
		LookupElapsedNanos: stats.LookupElapsed.Nanoseconds(),
	}
}

// RelationshipAlignedValueSum computes a node-local partial aggregate for an
// already-aligned relationship vector. Cluster callers merge partial groups by
// parent value.
func (m *BitmapIndex) RelationshipAlignedValueSum(ctx context.Context, req *pb.RelationshipAlignedValueSumRequest) (*pb.RelationshipAlignedValueSumResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	groups, stats, ok, err := m.RelationshipAlignedValueSumStorage(
		req.GetIndex(),
		req.GetValueField(),
		req.GetFromTime(),
		req.GetToTime(),
		req.GetChildRows(),
		req.GetParentRows(),
	)
	if err != nil {
		return nil, err
	}
	protoGroups := make([]*pb.RelationshipAlignedValueSumGroup, 0, len(groups))
	for _, group := range groups {
		sum := ""
		if group.Sum != nil {
			sum = group.Sum.String()
		}
		protoGroups = append(protoGroups, &pb.RelationshipAlignedValueSumGroup{
			ParentValue:       group.ParentValue,
			RepresentativeRow: group.RepresentativeRow,
			Count:             group.Count,
			Sum:               sum,
		})
	}
	return &pb.RelationshipAlignedValueSumResponse{
		Groups: protoGroups,
		Stats:  relationshipAlignedValueSumStatsToProto(stats),
		Ok:     ok,
	}, nil
}

func relationshipAlignedValueSumStatsToProto(stats RelationshipReverseArtifactSumStats) *pb.RelationshipAlignedValueSumStats {
	return &pb.RelationshipAlignedValueSumStats{
		Rows:                   stats.Rows,
		Values:                 stats.Values,
		SourceValues:           uint64(stats.SourceValues),
		TargetRows:             stats.TargetRows,
		Groups:                 uint64(stats.Groups),
		LookupElapsedNanos:     stats.LookupElapsed.Nanoseconds(),
		ProjectionElapsedNanos: stats.ProjectionElapsed.Nanoseconds(),
		AggregateElapsedNanos:  stats.AggregateElapsed.Nanoseconds(),
		Projection:             projectBSIStatsToProto(stats.ProjectionStats),
	}
}

func compareBSIFieldsStatsToProto(stats CompareBSIFieldsStats) *pb.CompareBSIFieldsStats {
	return &pb.CompareBSIFieldsStats{
		Left:                projectBSIStatsToProto(stats.Left),
		Right:               projectBSIStatsToProto(stats.Right),
		CompareElapsedNanos: stats.CompareElapsed.Nanoseconds(),
		OutputRows:          stats.OutputRows,
	}
}

func projectBSIStatsToProto(stats ProjectBSIStats) *pb.BSIProjectionStats {
	return &pb.BSIProjectionStats{
		ShardsVisited:      uint64(stats.ShardsVisited),
		ShardsInWindow:     uint64(stats.ShardsInWindow),
		ShardsLocal:        uint64(stats.ShardsLocal),
		ShardsRetained:     uint64(stats.ShardsRetained),
		RetainedRows:       stats.RetainedRows,
		RetainElapsedNanos: stats.RetainElapsed.Nanoseconds(),
		RetainBypassRows:   stats.RetainBypassRows,
		ValueElapsedNanos:  stats.ValueElapsed.Nanoseconds(),
		MergeElapsedNanos:  stats.MergeElapsed.Nanoseconds(),
	}
}

// ProjectBSI returns a projected BSI directly for in-process callers.
//
// The gRPC-shaped Projection API must marshal the BSI into protobuf bytes and
// client wrappers immediately unmarshal it again. Inabox-standard can avoid that
// local serialization tax while preserving the same time-range/foundset rules.
func (m *BitmapIndex) ProjectBSI(index, field string, fromTime, toTime int64, foundSet *roaring64.Bitmap, negate bool) (*roaring64.BSI, error) {
	bsi, _, err := m.ProjectBSIWithStats(index, field, fromTime, toTime, foundSet, negate)
	return bsi, err
}

// ProjectBSIWithStats returns a projected BSI and local execution stats for
// in-process callers that need to diagnose projection cost.
func (m *BitmapIndex) ProjectBSIWithStats(index, field string, fromTime, toTime int64, foundSet *roaring64.Bitmap, negate bool) (*roaring64.BSI, ProjectBSIStats, error) {
	return m.projectBSIWithStats(index, field, fromTime, toTime, foundSet, negate, true)
}

// BSIShardYearRange returns the observed year range for loaded BSI shards.
func (m *BitmapIndex) BSIShardYearRange(index, field string) (int, int, bool) {
	if index == "" || field == "" {
		return 0, 0, false
	}
	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()
	fields, ok := m.bsiCache[index]
	if !ok {
		for candidateIndex, candidateFields := range m.bsiCache {
			if strings.EqualFold(candidateIndex, index) {
				fields = candidateFields
				ok = true
				break
			}
		}
	}
	if !ok {
		return 0, 0, false
	}
	shards, ok := fields[field]
	if !ok {
		for candidateField, candidateShards := range fields {
			if strings.EqualFold(candidateField, field) {
				shards = candidateShards
				ok = true
				break
			}
		}
	}
	if !ok || len(shards) == 0 {
		return 0, 0, false
	}
	var minYear, maxYear int
	found := false
	for shardNanos := range shards {
		year := time.Unix(0, shardNanos).UTC().Year()
		if !found || year < minYear {
			minYear = year
		}
		if !found || year > maxYear {
			maxYear = year
		}
		found = true
	}
	return minYear, maxYear, found
}

// ProjectBSIsWithStats returns several projected BSIs under one in-process
// cache read. This is the multi-field companion to ProjectBSIWithStats for
// projection batches that share index, time window, foundset, and negate mode.
func (m *BitmapIndex) ProjectBSIsWithStats(index string, fields []string, fromTime, toTime int64, foundSet *roaring64.Bitmap, negate bool) (map[string]*roaring64.BSI, map[string]ProjectBSIStats, error) {
	results := make(map[string]*roaring64.BSI, len(fields))
	statsByField := make(map[string]ProjectBSIStats, len(fields))
	if index == "" {
		return nil, nil, fmt.Errorf("index not specified for projection criteria")
	}
	if len(fields) == 0 {
		return results, statsByField, nil
	}
	from := time.Unix(0, fromTime).UTC()
	to := time.Unix(0, toTime).UTC()
	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()
	for _, field := range fields {
		if field == "" {
			return nil, nil, fmt.Errorf("field not specified for projection criteria")
		}
		if _, seen := results[field]; seen {
			continue
		}
		stats := ProjectBSIStats{}
		bsi, err := m.timeRangeBSILocked(index, field, from, to, foundSet, negate, true, &stats)
		if err != nil {
			return nil, nil, err
		}
		if bsi == nil || bsi.BSI == nil {
			results[field] = roaring64.NewDefaultBSI()
		} else {
			results[field] = bsi.BSI
		}
		statsByField[field] = stats
	}
	return results, statsByField, nil
}

// ProjectBSIValuesWithStats returns BSI values aligned to rownums without
// constructing retained BSIs. In-process standard mode uses this for late
// materialization because the executor needs value vectors, not transport BSIs.
func (m *BitmapIndex) ProjectBSIValuesWithStats(index string, fields []string, fromTime, toTime int64, rownums []uint64, foundSet *roaring64.Bitmap, negate bool) (map[string][]*big.Int, map[string]ProjectBSIStats, error) {
	results := make(map[string][]*big.Int, len(fields))
	statsByField := make(map[string]ProjectBSIStats, len(fields))
	if index == "" {
		return nil, nil, fmt.Errorf("index not specified for projection value criteria")
	}
	if len(fields) == 0 {
		return results, statsByField, nil
	}
	if foundSet == nil {
		foundSet = roaring64.BitmapOf(rownums...)
	}
	from := time.Unix(0, fromTime).UTC()
	to := time.Unix(0, toTime).UTC()
	positions := make(map[uint64][]int, len(rownums))
	for i, rownum := range rownums {
		positions[rownum] = append(positions[rownum], i)
	}

	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()
	for _, field := range fields {
		if field == "" {
			return nil, nil, fmt.Errorf("field not specified for projection value criteria")
		}
		if _, seen := results[field]; seen {
			continue
		}
		stats := ProjectBSIStats{}
		values, err := m.projectBSIValuesLocked(index, field, from, to, rownums, positions, foundSet, negate, true, &stats)
		if err != nil {
			return nil, nil, err
		}
		results[field] = values
		statsByField[field] = stats
	}
	return results, statsByField, nil
}

// ProjectBSIInt64Values contains BSI values aligned to requested rownums.
type ProjectBSIInt64Values struct {
	Values []int64
	Exists []bool
	Fast   bool
}

// ProjectBSIInt64ValuesWithStats returns BSI int64 values aligned to rownums.
// It uses Roaring's typed value-vector API when available and falls back to
// GetBigValues conversion otherwise, preserving correctness on released
// Roaring modules that do not yet expose typed batch reads.
func (m *BitmapIndex) ProjectBSIInt64ValuesWithStats(index string, fields []string, fromTime, toTime int64, rownums []uint64, foundSet *roaring64.Bitmap, negate bool) (map[string]ProjectBSIInt64Values, map[string]ProjectBSIStats, error) {
	results := make(map[string]ProjectBSIInt64Values, len(fields))
	statsByField := make(map[string]ProjectBSIStats, len(fields))
	if index == "" {
		return nil, nil, fmt.Errorf("index not specified for projection int64 value criteria")
	}
	if len(fields) == 0 {
		return results, statsByField, nil
	}
	if foundSet == nil {
		foundSet = roaring64.BitmapOf(rownums...)
	}
	from := time.Unix(0, fromTime).UTC()
	to := time.Unix(0, toTime).UTC()
	positions := make(map[uint64][]int, len(rownums))
	for i, rownum := range rownums {
		positions[rownum] = append(positions[rownum], i)
	}

	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()
	for _, field := range fields {
		if field == "" {
			return nil, nil, fmt.Errorf("field not specified for projection int64 value criteria")
		}
		if _, seen := results[field]; seen {
			continue
		}
		stats := ProjectBSIStats{}
		values, err := m.projectBSIInt64ValuesLocked(index, field, from, to, rownums, positions, foundSet, negate, true, &stats)
		if err != nil {
			return nil, nil, err
		}
		results[field] = values
		statsByField[field] = stats
	}
	return results, statsByField, nil
}

func (m *BitmapIndex) projectBSIValuesLocked(index, field string, fromTime, toTime time.Time, rownums []uint64, positions map[uint64][]int, foundSet *roaring64.Bitmap, negate bool, ownedOnly bool, stat *ProjectBSIStats) ([]*big.Int, error) {
	attr, err := m.getFieldConfig(index, field)
	if err != nil {
		return nil, err
	}
	values := make([]*big.Int, len(rownums))
	tq := attr.TimeQuantumType
	fromTime = truncateTime(fromTime, tq)
	toTime = truncateTime(toTime, tq)

	readShard := func(ts int64, bsi *BSIBitmap) error {
		if bsi == nil || bsi.BSI == nil {
			return nil
		}
		if stat != nil {
			stat.ShardsVisited++
		}
		if tq != "" {
			rts := truncateTime(time.Unix(0, ts).UTC(), tq).UnixNano()
			if rts < fromTime.UnixNano() || rts > toTime.UnixNano() {
				return nil
			}
		}
		if stat != nil {
			stat.ShardsInWindow++
		}
		if ownedOnly && tq != "" {
			hashKey := fmt.Sprintf("%s/%s/%s", index, field, formatShardTime(time.Unix(0, ts)))
			if !m.Member(hashKey) {
				return nil
			}
		}
		if stat != nil {
			stat.ShardsLocal++
		}
		retainStart := time.Now()
		existence := bsi.BSI.GetExistenceBitmap()
		var retainSet *roaring64.Bitmap
		if foundSet == nil {
			retainSet = existence.Clone()
		} else if negate {
			retainSet = roaring64.AndNot(existence, foundSet)
		} else {
			retainSet = roaring64.And(existence, foundSet)
		}
		if stat != nil {
			stat.RetainElapsed += time.Since(retainStart)
		}
		if retainSet == nil || retainSet.IsEmpty() {
			return nil
		}
		retainRows := retainSet.GetCardinality()
		if stat != nil {
			stat.ShardsRetained++
			stat.RetainedRows += retainRows
			if retainRows == existence.GetCardinality() {
				stat.RetainBypassRows += retainRows
			}
		}

		valueStart := time.Now()
		retainedRownums := retainSet.ToArray()
		retainedValues := bsi.BSI.GetBigValues(retainedRownums)
		if stat != nil {
			stat.ValueElapsed += time.Since(valueStart)
		}
		for i, rownum := range retainedRownums {
			if i >= len(retainedValues) || retainedValues[i] == nil {
				continue
			}
			for _, position := range positions[rownum] {
				values[position] = retainedValues[i]
			}
		}
		return nil
	}

	if tq == "" {
		if bm, ok := m.bsiCache[index][field][0]; ok {
			if err := readShard(0, bm); err != nil {
				return nil, err
			}
		}
		return values, nil
	}
	if tm, ok := m.bsiCache[index][field]; ok {
		for ts, bsi := range tm {
			if err := readShard(ts, bsi); err != nil {
				return nil, err
			}
		}
	}
	return values, nil
}

func (m *BitmapIndex) projectBSIInt64ValuesLocked(index, field string, fromTime, toTime time.Time, rownums []uint64, positions map[uint64][]int, foundSet *roaring64.Bitmap, negate bool, ownedOnly bool, stat *ProjectBSIStats) (ProjectBSIInt64Values, error) {
	attr, err := m.getFieldConfig(index, field)
	if err != nil {
		return ProjectBSIInt64Values{}, err
	}
	result := ProjectBSIInt64Values{
		Values: make([]int64, len(rownums)),
		Exists: make([]bool, len(rownums)),
		Fast:   true,
	}
	tq := attr.TimeQuantumType
	fromTime = truncateTime(fromTime, tq)
	toTime = truncateTime(toTime, tq)

	readShard := func(ts int64, bsi *BSIBitmap) error {
		if bsi == nil || bsi.BSI == nil {
			return nil
		}
		if stat != nil {
			stat.ShardsVisited++
		}
		if tq != "" {
			rts := truncateTime(time.Unix(0, ts).UTC(), tq).UnixNano()
			if rts < fromTime.UnixNano() || rts > toTime.UnixNano() {
				return nil
			}
		}
		if stat != nil {
			stat.ShardsInWindow++
		}
		if ownedOnly && tq != "" {
			hashKey := fmt.Sprintf("%s/%s/%s", index, field, formatShardTime(time.Unix(0, ts)))
			if !m.Member(hashKey) {
				return nil
			}
		}
		if stat != nil {
			stat.ShardsLocal++
		}
		retainStart := time.Now()
		existence := bsi.BSI.GetExistenceBitmap()
		var retainSet *roaring64.Bitmap
		if foundSet == nil {
			retainSet = existence.Clone()
		} else if negate {
			retainSet = roaring64.AndNot(existence, foundSet)
		} else {
			retainSet = roaring64.And(existence, foundSet)
		}
		if stat != nil {
			stat.RetainElapsed += time.Since(retainStart)
		}
		if retainSet == nil || retainSet.IsEmpty() {
			return nil
		}
		retainRows := retainSet.GetCardinality()
		if stat != nil {
			stat.ShardsRetained++
			stat.RetainedRows += retainRows
			if retainRows == existence.GetCardinality() {
				stat.RetainBypassRows += retainRows
			}
		}

		valueStart := time.Now()
		retainedRownums := retainSet.ToArray()
		retainedValues, retainedExists, fast, err := readBSIInt64Values(bsi.BSI, retainedRownums)
		if stat != nil {
			stat.ValueElapsed += time.Since(valueStart)
		}
		if err != nil {
			return err
		}
		if !fast {
			result.Fast = false
		}
		for i, rownum := range retainedRownums {
			if i >= len(retainedExists) || !retainedExists[i] {
				continue
			}
			for _, position := range positions[rownum] {
				result.Values[position] = retainedValues[i]
				result.Exists[position] = true
			}
		}
		return nil
	}

	if tq == "" {
		if bm, ok := m.bsiCache[index][field][0]; ok {
			if err := readShard(0, bm); err != nil {
				return ProjectBSIInt64Values{}, err
			}
		}
		return result, nil
	}
	if tm, ok := m.bsiCache[index][field]; ok {
		for ts, bsi := range tm {
			if err := readShard(ts, bsi); err != nil {
				return ProjectBSIInt64Values{}, err
			}
		}
	}
	return result, nil
}

func readBSIInt64Values(bsi *roaring64.BSI, rownums []uint64) (values []int64, exists []bool, fast bool, err error) {
	values = make([]int64, len(rownums))
	exists = make([]bool, len(rownums))
	if bsi == nil || len(rownums) == 0 {
		return values, exists, false, nil
	}
	if method := reflect.ValueOf(bsi).MethodByName("GetValues"); method.IsValid() {
		defer func() {
			if recovered := recover(); recovered != nil {
				values = nil
				exists = nil
				err = fmt.Errorf("roaring BSI GetValues failed: %v", recovered)
			}
		}()
		out := method.Call([]reflect.Value{reflect.ValueOf(rownums)})
		if len(out) == 2 {
			if typedValues, ok := out[0].Interface().([]int64); ok {
				if typedExists, ok := out[1].Interface().([]bool); ok && len(typedValues) == len(rownums) && len(typedExists) == len(rownums) {
					return typedValues, typedExists, true, nil
				}
			}
		}
	}
	bigValues := bsi.GetBigValues(rownums)
	if len(bigValues) != len(rownums) {
		return nil, nil, false, fmt.Errorf("roaring BSI GetBigValues returned %d values for %d rownums", len(bigValues), len(rownums))
	}
	for i, value := range bigValues {
		if value == nil {
			continue
		}
		if !value.IsInt64() {
			return nil, nil, false, fmt.Errorf("roaring BSI value for rownum %d cannot be represented as int64", rownums[i])
		}
		values[i] = value.Int64()
		exists[i] = true
	}
	return values, exists, false, nil
}

func (m *BitmapIndex) projectBSIWithStats(index, field string, fromTime, toTime int64, foundSet *roaring64.Bitmap, negate bool, ownedOnly bool) (*roaring64.BSI, ProjectBSIStats, error) {
	if index == "" {
		return nil, ProjectBSIStats{}, fmt.Errorf("index not specified for projection criteria")
	}
	if field == "" {
		return nil, ProjectBSIStats{}, fmt.Errorf("field not specified for projection criteria")
	}
	stats := ProjectBSIStats{}
	bsi, err := m.timeRangeBSI(index, field, time.Unix(0, fromTime).UTC(), time.Unix(0, toTime).UTC(), foundSet, negate, ownedOnly, &stats)
	if err != nil {
		return nil, stats, err
	}
	if bsi == nil || bsi.BSI == nil {
		return roaring64.NewDefaultBSI(), stats, nil
	}
	return bsi.BSI, stats, nil
}

// CompareBSIFieldsWithStats compares two projected BSI fields inside the local
// bitmap index and returns only rownums that satisfy the row-local predicate.
func (m *BitmapIndex) CompareBSIFieldsWithStats(index, leftField, rightField string, fromTime, toTime int64, foundSet *roaring64.Bitmap, op roaring64.Operation, invert bool) (*roaring64.Bitmap, CompareBSIFieldsStats, error) {
	if index == "" {
		return nil, CompareBSIFieldsStats{}, fmt.Errorf("index not specified for BSI comparison")
	}
	if leftField == "" || rightField == "" {
		return nil, CompareBSIFieldsStats{}, fmt.Errorf("both fields must be specified for BSI comparison")
	}
	if matches, stats, ok, err := m.compareBSIFieldsShardWiseWithStats(index, leftField, rightField, fromTime, toTime, foundSet, op, invert); ok || err != nil {
		return matches, stats, err
	}
	stats := CompareBSIFieldsStats{}
	left, leftStats, err := m.projectBSIWithStats(index, leftField, fromTime, toTime, foundSet, false, false)
	if err != nil {
		return nil, stats, err
	}
	stats.Left = leftStats
	right, rightStats, err := m.projectBSIWithStats(index, rightField, fromTime, toTime, foundSet, false, false)
	if err != nil {
		return nil, stats, err
	}
	stats.Right = rightStats
	if left == nil {
		left = roaring64.NewDefaultBSI()
	}
	if right == nil {
		right = roaring64.NewDefaultBSI()
	}
	var universe *roaring64.Bitmap
	if foundSet != nil {
		universe = foundSet.Clone()
	} else {
		universe = left.GetExistenceBitmap().Clone()
	}
	universe.And(left.GetExistenceBitmap())
	universe.And(right.GetExistenceBitmap())
	compareStart := time.Now()
	matches := left.CompareBSI(op, right, universe)
	if invert {
		universe.AndNot(matches)
		matches = universe
	}
	stats.CompareElapsed = time.Since(compareStart)
	if matches != nil {
		stats.OutputRows = matches.GetCardinality()
	}
	return matches, stats, nil
}

func (m *BitmapIndex) compareBSIFieldsShardWiseWithStats(index, leftField, rightField string, fromTime, toTime int64, foundSet *roaring64.Bitmap, op roaring64.Operation, invert bool) (*roaring64.Bitmap, CompareBSIFieldsStats, bool, error) {
	leftAttr, err := m.getFieldConfig(index, leftField)
	if err != nil {
		return nil, CompareBSIFieldsStats{}, true, err
	}
	rightAttr, err := m.getFieldConfig(index, rightField)
	if err != nil {
		return nil, CompareBSIFieldsStats{}, true, err
	}
	if leftAttr.TimeQuantumType != rightAttr.TimeQuantumType {
		return nil, CompareBSIFieldsStats{}, false, nil
	}
	tq := leftAttr.TimeQuantumType
	from := truncateTime(time.Unix(0, fromTime).UTC(), tq)
	to := truncateTime(time.Unix(0, toTime).UTC(), tq)
	stats := CompareBSIFieldsStats{}
	matches := roaring64.NewBitmap()

	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()

	fields := m.bsiCache[index]
	if fields == nil {
		return matches, stats, true, nil
	}
	leftShards := fields[leftField]
	rightShards := fields[rightField]
	if tq == "" {
		left := leftShards[0]
		right := rightShards[0]
		if left != nil {
			stats.Left.ShardsVisited++
			stats.Left.ShardsInWindow++
			stats.Left.ShardsLocal++
		}
		if right != nil {
			stats.Right.ShardsVisited++
			stats.Right.ShardsInWindow++
			stats.Right.ShardsLocal++
		}
		compareBSIShardPair(left, right, foundSet, op, invert, matches, &stats)
		return matches, stats, true, nil
	}

	for ts := range leftShards {
		stats.Left.ShardsVisited++
		if !compareBSIShardTimestampInWindow(ts, tq, from, to) {
			continue
		}
		stats.Left.ShardsInWindow++
		stats.Left.ShardsLocal++
	}
	for ts := range rightShards {
		stats.Right.ShardsVisited++
		if !compareBSIShardTimestampInWindow(ts, tq, from, to) {
			continue
		}
		stats.Right.ShardsInWindow++
		stats.Right.ShardsLocal++
	}
	for ts, left := range leftShards {
		if !compareBSIShardTimestampInWindow(ts, tq, from, to) {
			continue
		}
		right := rightShards[ts]
		compareBSIShardPair(left, right, foundSet, op, invert, matches, &stats)
	}
	return matches, stats, true, nil
}

func compareBSIShardTimestampInWindow(ts int64, tq string, from, to time.Time) bool {
	rts := truncateTime(time.Unix(0, ts).UTC(), tq).UnixNano()
	return rts >= from.UnixNano() && rts <= to.UnixNano()
}

func compareBSIShardPair(left, right *BSIBitmap, foundSet *roaring64.Bitmap, op roaring64.Operation, invert bool, output *roaring64.Bitmap, stats *CompareBSIFieldsStats) {
	if left == nil || left.BSI == nil || right == nil || right.BSI == nil || output == nil || stats == nil {
		return
	}
	leftRetainStart := time.Now()
	leftRows := compareBSIShardRetainSet(left.BSI, foundSet)
	stats.Left.RetainElapsed += time.Since(leftRetainStart)
	if leftRows.IsEmpty() {
		return
	}
	stats.Left.ShardsRetained++
	stats.Left.RetainedRows += leftRows.GetCardinality()

	rightRetainStart := time.Now()
	rightRows := compareBSIShardRetainSet(right.BSI, foundSet)
	stats.Right.RetainElapsed += time.Since(rightRetainStart)
	if rightRows.IsEmpty() {
		return
	}
	stats.Right.ShardsRetained++
	stats.Right.RetainedRows += rightRows.GetCardinality()

	universe := leftRows
	universe.And(rightRows)
	if universe.IsEmpty() {
		return
	}
	compareStart := time.Now()
	matches := left.BSI.CompareBSI(op, right.BSI, universe)
	if invert {
		universe.AndNot(matches)
		matches = universe
	}
	stats.CompareElapsed += time.Since(compareStart)
	if matches == nil || matches.IsEmpty() {
		return
	}
	mergeStart := time.Now()
	output.Or(matches)
	mergeElapsed := time.Since(mergeStart)
	stats.Left.MergeElapsed += mergeElapsed
	stats.Right.MergeElapsed += mergeElapsed
	stats.OutputRows = output.GetCardinality()
}

func compareBSIShardRetainSet(bsi *roaring64.BSI, foundSet *roaring64.Bitmap) *roaring64.Bitmap {
	rows := bsi.GetExistenceBitmap().Clone()
	if foundSet != nil {
		rows.And(foundSet)
	}
	return rows
}

package qsinabox

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/server"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

const (
	standardProjectionFullTimeRangeBeginMillis int64 = -2208988800000 // 1900-01-01T00:00:00Z
	standardProjectionFullTimeRangeEndMillis   int64 = 4102444800000  // 2100-01-01T00:00:00Z
)

// StandardProjectionBSIReader reads projected BSI vectors through the
// in-process inabox-standard session pool.
type StandardProjectionBSIReader struct {
	Pool       *core.SessionPool
	TableCache *core.TableCacheStruct
	Direct     *server.BitmapIndex
}

// ReadProjectionBSI projects one BSI-backed field through the local BitmapIndex.
func (r StandardProjectionBSIReader) ReadProjectionBSI(ctx context.Context, request qsruntime.NativeProjectionBSIReadRequest) (qsruntime.NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	foundSet := standardProjectionBitmap(request.Rownums)
	fromTime, toTime := standardProjectionWindowNanos(r.TableCache, request.Index, request.FromEpochMillis, request.ToEpochMillis)
	cacheKey := qsruntime.ProjectionBSICacheKeyFor(request, fromTime, toTime)
	cache := qsruntime.ProjectionBSICacheFromContext(ctx)
	detail := standardProjectionBSICacheInstrumentationDetail(cacheKey, foundSet)
	cachedBSI, mode, ok := cache.Get(cacheKey, foundSet)
	if ok {
		qsruntime.RecordQueryScratchpadCacheLookup(ctx, "projection_bsi_cache", true, mode, detail)
		return qsruntime.NativeProjectionBSIReadResult{
			BSI: cachedBSI,
			Probes: []qsruntime.ExecutionProbe{
				standardProjectionBSIRowsProbe(request.Index, request.PhysicalField, len(request.Rownums)),
				standardProjectionBSICacheProbe(request.Index, request.PhysicalField, true),
				standardProjectionBSICacheModeProbe(request.Index, request.PhysicalField, mode),
			},
		}, nil, nil
	}
	qsruntime.RecordQueryScratchpadCacheLookup(ctx, "projection_bsi_cache", false, mode, detail)
	fetchSet := foundSet
	fetchRownums := request.Rownums
	partial := qsruntime.ProjectionBSICachePartial{}
	partialOK := false
	if mode == "coverage_miss" {
		partialResult, partialMode, ok := cache.GetPartial(cacheKey, foundSet)
		if ok {
			partial = partialResult
			partialOK = true
			qsruntime.RecordQueryScratchpadCacheLookup(ctx, "projection_bsi_cache_partial", true, partialMode, standardProjectionBSIPartialCacheDetail(detail, partial))
			if partial.MissingCardinality() == 0 {
				return qsruntime.NativeProjectionBSIReadResult{
					BSI: partial.BSI,
					Probes: []qsruntime.ExecutionProbe{
						standardProjectionBSIRowsProbe(request.Index, request.PhysicalField, len(request.Rownums)),
						standardProjectionBSICacheProbe(request.Index, request.PhysicalField, true),
						standardProjectionBSICacheModeProbe(request.Index, request.PhysicalField, partialMode),
					},
				}, nil, nil
			}
			fetchSet = partial.MissingRownumSet
			fetchRownums = partial.MissingRownums()
		}
	}
	if r.Direct != nil {
		bsi, stats, err := r.Direct.ProjectBSIWithStats(request.Index, request.PhysicalField, fromTime, toTime, fetchSet, false)
		if err != nil {
			return qsruntime.NativeProjectionBSIReadResult{}, nil, err
		}
		if bsi == nil {
			bsi = roaring64.NewDefaultBSI()
		}
		if partialOK {
			bsi = partial.MergeFetchedMissing(bsi)
		}
		probes := []qsruntime.ExecutionProbe{
			standardProjectionBSIRowsProbe(request.Index, request.PhysicalField, len(request.Rownums)),
			standardProjectionBSIFetchRowsProbe(request.Index, request.PhysicalField, len(fetchRownums)),
			standardProjectionBSICacheProbe(request.Index, request.PhysicalField, false),
			standardProjectionBSICacheModeProbe(request.Index, request.PhysicalField, mode),
			{
				Section: "native_projection_materialization",
				Name:    "standard_bsi_projection_transport",
				Value:   "local_direct",
				Detail:  request.Index + "." + request.PhysicalField,
			},
		}
		probes = append(probes, standardProjectionBSIStatsProbes(request.Index, request.PhysicalField, stats)...)
		cache.Set(cacheKey, foundSet, bsi)
		qsruntime.RecordQueryScratchpadCacheStore(ctx, "projection_bsi_cache", detail)
		return qsruntime.NativeProjectionBSIReadResult{
			BSI:    bsi,
			Probes: probes,
		}, nil, nil
	}
	session, diagnostics, err := r.borrow(ctx, request.Index)
	if err != nil || diagnostics.BlocksNative() {
		return qsruntime.NativeProjectionBSIReadResult{}, diagnostics, err
	}
	defer r.release(request.Index, session)
	bsiByField, _, err := session.BitIndex.Projection(request.Index, []string{request.PhysicalField}, fromTime, toTime, fetchSet, false)
	if err != nil {
		return qsruntime.NativeProjectionBSIReadResult{}, nil, err
	}
	bsi := bsiByField[request.PhysicalField]
	if bsi == nil {
		bsi = roaring64.NewDefaultBSI()
	}
	if partialOK {
		bsi = partial.MergeFetchedMissing(bsi)
	}
	cache.Set(cacheKey, foundSet, bsi)
	qsruntime.RecordQueryScratchpadCacheStore(ctx, "projection_bsi_cache", detail)
	return qsruntime.NativeProjectionBSIReadResult{
		BSI: bsi,
		Probes: []qsruntime.ExecutionProbe{
			standardProjectionBSIRowsProbe(request.Index, request.PhysicalField, len(request.Rownums)),
			standardProjectionBSIFetchRowsProbe(request.Index, request.PhysicalField, len(fetchRownums)),
			standardProjectionBSICacheProbe(request.Index, request.PhysicalField, false),
			standardProjectionBSICacheModeProbe(request.Index, request.PhysicalField, mode),
		},
	}, nil, nil
}

func standardProjectionBSICacheInstrumentationDetail(key qsruntime.ProjectionBSICacheKey, rownumSet *roaring64.Bitmap) string {
	rows := uint64(0)
	if rownumSet != nil {
		rows = rownumSet.GetCardinality()
	}
	return "index=" + key.Index +
		" field=" + key.Field +
		" rows=" + strconv.FormatUint(rows, 10) +
		" from=" + strconv.FormatInt(key.FromTimeNanos, 10) +
		" to=" + strconv.FormatInt(key.ToTimeNanos, 10)
}

func standardProjectionBSIPartialCacheDetail(detail string, partial qsruntime.ProjectionBSICachePartial) string {
	return detail +
		" covered_rows=" + strconv.FormatUint(partial.CoveredCardinality(), 10) +
		" missing_rows=" + strconv.FormatUint(partial.MissingCardinality(), 10)
}

func standardProjectionBSIRowsProbe(index, field string, rows int) qsruntime.ExecutionProbe {
	return qsruntime.ExecutionProbe{
		Section: "native_projection_materialization",
		Name:    "standard_bsi_projection_rows",
		Value:   strconv.Itoa(rows),
		Detail:  index + "." + field,
	}
}

func standardProjectionBSIFetchRowsProbe(index, field string, rows int) qsruntime.ExecutionProbe {
	return qsruntime.ExecutionProbe{
		Section: "native_projection_materialization",
		Name:    "standard_bsi_projection_fetch_rows",
		Value:   strconv.Itoa(rows),
		Detail:  index + "." + field,
	}
}

func standardProjectionBSICacheProbe(index, field string, hit bool) qsruntime.ExecutionProbe {
	value := "false"
	if hit {
		value = "true"
	}
	return qsruntime.ExecutionProbe{
		Section: "native_projection_materialization",
		Name:    "standard_bsi_projection_cache_hit",
		Value:   value,
		Detail:  index + "." + field,
	}
}

func standardProjectionBSICacheModeProbe(index, field string, mode string) qsruntime.ExecutionProbe {
	return qsruntime.ExecutionProbe{
		Section: "native_projection_materialization",
		Name:    "standard_bsi_projection_cache_mode",
		Value:   mode,
		Detail:  index + "." + field,
	}
}

func standardProjectionBSIStatsProbes(index, field string, stats server.ProjectBSIStats) []qsruntime.ExecutionProbe {
	detail := index + "." + field
	return []qsruntime.ExecutionProbe{
		{
			Section: "native_projection_materialization",
			Name:    "standard_bsi_projection_shards_visited",
			Value:   strconv.Itoa(stats.ShardsVisited),
			Detail:  detail,
		},
		{
			Section: "native_projection_materialization",
			Name:    "standard_bsi_projection_shards_in_window",
			Value:   strconv.Itoa(stats.ShardsInWindow),
			Detail:  detail,
		},
		{
			Section: "native_projection_materialization",
			Name:    "standard_bsi_projection_shards_local",
			Value:   strconv.Itoa(stats.ShardsLocal),
			Detail:  detail,
		},
		{
			Section: "native_projection_materialization",
			Name:    "standard_bsi_projection_shards_retained",
			Value:   strconv.Itoa(stats.ShardsRetained),
			Detail:  detail,
		},
		{
			Section: "native_projection_materialization",
			Name:    "standard_bsi_projection_rows_retained",
			Value:   strconv.FormatUint(stats.RetainedRows, 10),
			Detail:  detail,
		},
		{
			Section: "native_projection_materialization",
			Name:    "standard_bsi_projection_retain_elapsed",
			Value:   stats.RetainElapsed.String(),
			Detail:  detail,
		},
		{
			Section: "native_projection_materialization",
			Name:    "standard_bsi_projection_merge_elapsed",
			Value:   stats.MergeElapsed.String(),
			Detail:  detail,
		},
	}
}

func (r StandardProjectionBSIReader) borrow(ctx context.Context, table string) (*core.Session, qsbridge.DiagnosticSet, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if r.Pool == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-standard projection reader has no session pool"),
		}, nil
	}
	session, err := r.Pool.Borrow(table)
	if err != nil {
		return nil, nil, err
	}
	if session == nil || session.BitIndex == nil {
		if session != nil {
			r.Pool.Return(table, session)
		}
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-standard projection reader has no bitmap index"),
		}, nil
	}
	return session, nil, nil
}

func (r StandardProjectionBSIReader) release(table string, session *core.Session) {
	if r.Pool != nil && session != nil {
		r.Pool.Return(table, session)
	}
}

// StandardProjectionDictionaryIDReader reads StringEnum/direct-bitmap encoded
// ids through the in-process BitmapIndex projection path.
type StandardProjectionDictionaryIDReader struct {
	Pool       *core.SessionPool
	TableCache *core.TableCacheStruct
}

// ReadProjectionDictionaryIDs projects standard bitmap rows and aligns the
// lowest matching bitmap id to each requested rownum.
func (r StandardProjectionDictionaryIDReader) ReadProjectionDictionaryIDs(ctx context.Context, request qsruntime.NativeProjectionDictionaryIDReadRequest) (qsruntime.NativeProjectionDictionaryIDReadResult, qsbridge.DiagnosticSet, error) {
	if len(request.Rownums) == 0 {
		return qsruntime.NativeProjectionDictionaryIDReadResult{}, nil, nil
	}
	bsiReader := StandardProjectionBSIReader{Pool: r.Pool, TableCache: r.TableCache}
	session, diagnostics, err := bsiReader.borrow(ctx, request.Index)
	if err != nil || diagnostics.BlocksNative() {
		return qsruntime.NativeProjectionDictionaryIDReadResult{}, diagnostics, err
	}
	defer bsiReader.release(request.Index, session)
	foundSet := standardProjectionBitmap(request.Rownums)
	fromTime, toTime := standardProjectionWindowNanos(r.TableCache, request.Index, 0, 0)
	_, bitmapByField, err := session.BitIndex.Projection(request.Index, []string{request.PhysicalField}, fromTime, toTime, foundSet, false)
	if err != nil {
		return qsruntime.NativeProjectionDictionaryIDReadResult{}, nil, err
	}
	values := standardProjectionDictionaryIDCells(request.Rownums, bitmapByField[request.PhysicalField])
	return qsruntime.NativeProjectionDictionaryIDReadResult{
		Values: values,
		Probes: []qsruntime.ExecutionProbe{{
			Section: "native_projection_materialization",
			Name:    "standard_dictionary_id_projection_rows",
			Value:   strconv.Itoa(len(request.Rownums)),
			Detail:  request.Index + "." + request.PhysicalField,
		}},
	}, nil, nil
}

// StandardBackingStringLookupReader resolves StringHashBSI backing strings via
// local KVStore unary lookups.
type StandardBackingStringLookupReader struct {
	Pool       *core.SessionPool
	TableCache *core.TableCacheStruct
}

// LookupProjectionBackingStrings resolves rownum-keyed backing strings.
func (r StandardBackingStringLookupReader) LookupProjectionBackingStrings(ctx context.Context, request qsruntime.NativeProjectionBackingStringLookupRequest) (qsruntime.NativeProjectionBackingStringLookupResult, qsbridge.DiagnosticSet, error) {
	index, field := standardBackingStringLookupTarget(request)
	if index == "" || field == "" {
		return qsruntime.NativeProjectionBackingStringLookupResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "inabox-standard backing-string lookup requires index and field"),
		}, nil
	}
	if r.Pool == nil {
		return qsruntime.NativeProjectionBackingStringLookupResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-standard backing-string lookup has no session pool"),
		}, nil
	}
	table := standardCachedTable(r.TableCache, index)
	if table == nil {
		return qsruntime.NativeProjectionBackingStringLookupResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "inabox-standard backing-string lookup has no table metadata for "+index),
		}, nil
	}
	session, err := r.Pool.Borrow(index)
	if err != nil {
		return qsruntime.NativeProjectionBackingStringLookupResult{}, nil, err
	}
	defer r.Pool.Return(index, session)
	if session == nil || session.KVStore == nil {
		return qsruntime.NativeProjectionBackingStringLookupResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-standard backing-string lookup has no KVStore"),
		}, nil
	}
	result := qsruntime.NativeProjectionBackingStringLookupResult{
		Values: make([]qsbridge.ResultCell, len(request.Values)),
		Probes: []qsruntime.ExecutionProbe{{
			Section: "native_projection_materialization",
			Name:    "standard_backing_string_batch_lookup_values",
			Value:   strconv.Itoa(len(request.Values)),
			Detail:  index + "." + field,
		}},
	}
	batches := make(map[string]map[interface{}]interface{})
	positions := make(map[string]map[uint64][]int)
	for i, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return result, nil, err
		}
		result.Values[i] = qsbridge.ResultCell{Kind: qsbridge.ValueNull}
		rownum, ok := standardProjectionRowKey(value)
		if !ok {
			continue
		}
		path := standardBackingStringPath(table, field, time.Unix(0, int64(rownum)))
		if batches[path] == nil {
			batches[path] = make(map[interface{}]interface{})
		}
		batches[path][rownum] = ""
		if positions[path] == nil {
			positions[path] = make(map[uint64][]int)
		}
		positions[path][rownum] = append(positions[path][rownum], i)
	}

	paths := make([]string, 0, len(batches))
	for path := range batches {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		lookup, err := session.KVStore.BatchLookup(path, batches[path], true)
		if err != nil {
			return result, nil, fmt.Errorf("backing-string BatchLookup error for [%s] - %w", path, err)
		}
		for rownum, outputPositions := range positions[path] {
			visible, ok := standardBackingStringLookupValue(lookup, rownum)
			if !ok || visible == "" {
				continue
			}
			for _, outputPosition := range outputPositions {
				result.Values[outputPosition] = qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: visible}
			}
		}
	}
	return result, nil, nil
}

func standardProjectionBitmap(rownums []qsbridge.QuantaRownum) *roaring64.Bitmap {
	bitmap := roaring64.NewBitmap()
	for _, rownum := range rownums {
		bitmap.Add(uint64(rownum))
	}
	return bitmap
}

func standardProjectionWindowNanos(cache *core.TableCacheStruct, index string, fromEpochMillis int64, toEpochMillis int64) (int64, int64) {
	if fromEpochMillis != 0 || toEpochMillis != 0 {
		return fromEpochMillis * int64(time.Millisecond), toEpochMillis * int64(time.Millisecond)
	}
	table := standardCachedTable(cache, index)
	if table == nil || table.TimeQuantumType == "" || standardProjectionTimeQuantumField(table) == "" {
		return 0, 0
	}
	return standardProjectionFullTimeRangeBeginMillis * int64(time.Millisecond),
		standardProjectionFullTimeRangeEndMillis * int64(time.Millisecond)
}

func standardProjectionTimeQuantumField(table *core.Table) string {
	if table == nil {
		return ""
	}
	if table.TimeQuantumField != "" {
		return table.TimeQuantumField
	}
	for _, attribute := range table.Attributes {
		fieldName := attribute.FieldName
		if fieldName == "" {
			fieldName = attribute.SourceName
		}
		if fieldName != "" && strings.EqualFold(attribute.Type, "time") && strings.HasPrefix(attribute.MappingStrategy, "Sys") {
			return fieldName
		}
	}
	return ""
}

func standardProjectionDictionaryIDCells(rownums []qsbridge.QuantaRownum, bitmaps map[uint64]*roaring64.Bitmap) []qsbridge.ResultCell {
	values := make([]qsbridge.ResultCell, 0, len(rownums))
	ids := make([]uint64, 0, len(bitmaps))
	for id := range bitmaps {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, rownum := range rownums {
		var matched uint64
		found := false
		for _, id := range ids {
			if bitmap := bitmaps[id]; bitmap != nil && bitmap.Contains(uint64(rownum)) {
				matched = id
				found = true
				break
			}
		}
		if !found {
			values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueNull})
			continue
		}
		values = append(values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(matched)})
	}
	return values
}

func standardProjectionRowKey(value qsbridge.ResultCell) (uint64, bool) {
	if value.Kind == qsbridge.ValueNull || value.Value == nil {
		return 0, false
	}
	switch typed := value.Value.(type) {
	case uint64:
		return typed, true
	case int64:
		return uint64(typed), typed >= 0
	case int:
		return uint64(typed), typed >= 0
	default:
		return 0, false
	}
}

func standardBackingStringLookupValue(values map[interface{}]interface{}, rownum uint64) (string, bool) {
	for _, key := range []interface{}{rownum, int64(rownum), int(rownum)} {
		if value, ok := values[key]; ok {
			text, ok := value.(string)
			if !ok || text == "" || text == "<nil>" {
				return "", false
			}
			return text, true
		}
	}
	return "", false
}

func standardBackingStringLookupTarget(request qsruntime.NativeProjectionBackingStringLookupRequest) (string, string) {
	index := strings.TrimSpace(request.Index)
	if index == "" {
		index = strings.TrimSpace(request.Field.Index)
	}
	fieldName := request.Field.PhysicalName
	if fieldName == "" {
		fieldName = request.Field.Field
	}
	if fieldName == "" {
		parts := strings.Split(request.LookupRef, ".")
		if len(parts) >= 2 {
			if index == "" {
				index = parts[len(parts)-2]
			}
			fieldName = parts[len(parts)-1]
		}
	}
	return index, fieldName
}

func standardBackingStringPath(table *core.Table, field string, ts time.Time) string {
	lookupPath := fmt.Sprintf("%s/%s/strings,%s", table.Name, field, ts.UTC().Format("2006-01-02T15"))
	if table.TimeQuantumType == "YMDH" {
		utcTime := ts.UTC()
		key := fmt.Sprintf("%s/%s/%s", table.Name, field, ts.UTC().Format("2006-01-02T15"))
		fpath := fmt.Sprintf("/%s/%s/strings/%s/%s",
			table.Name,
			field,
			fmt.Sprintf("%d%02d%02d", utcTime.Year(), utcTime.Month(), utcTime.Day()),
			ts.UTC().Format("2006-01-02T15"))
		lookupPath = key + "," + fpath
	}
	return lookupPath
}

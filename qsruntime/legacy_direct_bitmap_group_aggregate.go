package qsruntime

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

const legacyDirectBitmapGroupAggregateMaxCombinations = 4096

// LegacyDirectBitmapGroupAggregateReader computes low-cardinality grouped BSI
// aggregates for the direct/cluster path without row-wise projection
// materialization.
type LegacyDirectBitmapGroupAggregateReader struct {
	Sessions   DirectSessionProvider
	TableCache *core.TableCacheStruct
	Projection NativeProjectionBSIReader
	Remote     BitmapGroupAggregateReader
}

type legacyDirectBitmapGroupAggregateValue struct {
	Count uint64
	Sum   *big.Int
	Min   *big.Int
	Max   *big.Int
}

type legacyDirectBitmapGroupAggregateRemoteSpec struct {
	Function string
	Field    string
}

type legacyDirectBitmapGroupAggregateRemoteValue struct {
	Count uint64
	Sum   *big.Int
	Min   *big.Int
	Max   *big.Int
}

type legacyDirectBitmapGroupAggregateRemoteGroup struct {
	Values []uint64
	Aggs   []legacyDirectBitmapGroupAggregateRemoteValue
}

type legacyDirectBitmapGroupAggregateRemoteStats struct {
	Nodes             uint64
	CandidateRows     uint64
	FieldCount        int
	ValueCount        int
	Groups            int
	AggregateCount    int
	BSIFieldCount     int
	BSIProjectElapsed time.Duration
	AggregateElapsed  time.Duration
	ValueSetElapsed   time.Duration
	SumElapsed        time.Duration
	MinMaxElapsed     time.Duration
}

type legacyDirectBitmapGroupAggregateEnum struct {
	Field qsbridge.FieldRef
	Name  string
	Attr  *core.Attribute
	Items []legacyDirectBitmapGroupAggregateEnumValue
}

type legacyDirectBitmapGroupAggregateEnumValue struct {
	ID    uint64
	Label string
	Cell  qsbridge.ResultCell
}

// ReadBitmapGroupAggregates evaluates the grouped aggregate by projecting each
// distinct BSI measure once over the candidate set and intersecting it with
// small StringEnum group bitmaps.
func (r LegacyDirectBitmapGroupAggregateReader) ReadBitmapGroupAggregates(ctx context.Context, read BitmapGroupAggregateReadRequest) (BitmapGroupAggregateReadResult, qsbridge.DiagnosticSet, bool, error) {
	if err := ctx.Err(); err != nil {
		return BitmapGroupAggregateReadResult{}, nil, false, err
	}
	if r.Sessions == nil || r.Projection == nil || r.TableCache == nil {
		return BitmapGroupAggregateReadResult{}, nil, false, nil
	}
	table := legacyDirectRelationshipCachedTable(r.TableCache, read.Index)
	if table == nil {
		return BitmapGroupAggregateReadResult{}, nil, false, nil
	}
	groups, groupFields, valueCount, ok := legacyDirectBitmapGroupEnums(table, read.GroupFields)
	if !ok {
		return BitmapGroupAggregateReadResult{}, nil, false, nil
	}
	measureFields, measureNames, ok := legacyDirectBitmapGroupAggregateMeasureFields(table, read)
	if !ok {
		return BitmapGroupAggregateReadResult{}, nil, false, nil
	}
	if len(measureFields) == 0 {
		return BitmapGroupAggregateReadResult{}, nil, false, nil
	}
	if r.Remote != nil {
		result, diagnostics, ok, err := r.Remote.ReadBitmapGroupAggregates(ctx, read)
		if ok && err == nil && !diagnostics.BlocksNative() && len(read.CandidateRows) > 0 && len(result.Groups) == 0 {
			ok = false
		}
		if err != nil || diagnostics.BlocksNative() || ok {
			return result, diagnostics, ok, err
		}
	}

	start := time.Now()
	projectionStart := time.Now()
	bsis, projectionProbes, diagnostics, err := r.projectMeasureBSIs(ctx, read, measureFields)
	projectionElapsed := time.Since(projectionStart)
	if err != nil || diagnostics.BlocksNative() {
		return BitmapGroupAggregateReadResult{Probes: projectionProbes}, diagnostics, true, err
	}

	groupQueryStart := time.Now()
	resultGroups, groupQueries, diagnostics, err := r.groupResults(ctx, read, groups, bsis)
	groupQueryElapsed := time.Since(groupQueryStart)
	if err != nil || diagnostics.BlocksNative() {
		return BitmapGroupAggregateReadResult{Probes: projectionProbes}, diagnostics, true, err
	}
	elapsed := time.Since(start)
	probes := append([]ExecutionProbe{}, projectionProbes...)
	probes = append(probes,
		ExecutionProbe{Section: "grouped_aggregate", Name: "legacy_direct_bitmap_group_aggregate_elapsed", Value: elapsed.String(), Detail: read.Index + "." + strings.Join(groupFields, ",")},
		ExecutionProbe{Section: "grouped_aggregate", Name: "legacy_direct_bitmap_group_aggregate_projection_elapsed", Value: projectionElapsed.String(), Detail: strings.Join(measureNames, ",")},
		ExecutionProbe{Section: "grouped_aggregate", Name: "legacy_direct_bitmap_group_aggregate_group_query_elapsed", Value: groupQueryElapsed.String(), Detail: read.Index},
		ExecutionProbe{Section: "grouped_aggregate", Name: "legacy_direct_bitmap_group_aggregate_group_queries", Value: strconv.Itoa(groupQueries), Detail: read.Index},
		ExecutionProbe{Section: "grouped_aggregate", Name: "legacy_direct_bitmap_group_aggregate_groups", Value: strconv.Itoa(len(resultGroups)), Detail: read.Index},
	)
	return BitmapGroupAggregateReadResult{
		Groups:         resultGroups,
		Mode:           "legacy_direct_string_enum_group_bsi_projection",
		CandidateRows:  uint64(len(read.CandidateRows)),
		FieldCount:     len(read.GroupFields),
		ValueCount:     valueCount,
		AggregateCount: len(read.Aggregates),
		Elapsed:        elapsed,
		Probes:         probes,
	}, nil, true, nil
}

func (r LegacyDirectBitmapGroupAggregateReader) projectMeasureBSIs(ctx context.Context, read BitmapGroupAggregateReadRequest, fields []qsbridge.FieldRef) (map[string]*roaring64.BSI, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	if len(fields) == 0 {
		return nil, nil, nil, nil
	}
	requests := make([]NativeProjectionBSIReadRequest, 0, len(fields))
	for _, field := range fields {
		name := legacyDirectBitmapGroupAggregateFieldName(field)
		requests = append(requests, NativeProjectionBSIReadRequest{
			Index: read.Index,
			Field: qsbridge.QuantaProjectionField{
				Index:        read.Index,
				Field:        field.Name,
				Type:         field.Type,
				PhysicalName: name,
				Roles:        field.Roles,
			},
			PhysicalField:   name,
			Rownums:         append([]qsbridge.QuantaRownum(nil), read.CandidateRows...),
			FromEpochMillis: read.FromEpochMillis,
			ToEpochMillis:   read.ToEpochMillis,
		})
	}
	var results []NativeProjectionBSIReadResult
	var diagnostics qsbridge.DiagnosticSet
	var err error
	if batchReader, ok := r.Projection.(NativeProjectionBSIBatchReader); ok {
		results, diagnostics, err = batchReader.ReadProjectionBSIs(ctx, requests)
	} else {
		results = make([]NativeProjectionBSIReadResult, len(requests))
		for i, request := range requests {
			results[i], diagnostics, err = r.Projection.ReadProjectionBSI(ctx, request)
			if err != nil || diagnostics.BlocksNative() {
				break
			}
		}
	}
	probes := make([]ExecutionProbe, 0)
	bsis := make(map[string]*roaring64.BSI, len(fields))
	for i, result := range results {
		if i >= len(requests) {
			break
		}
		probes = append(probes, result.Probes...)
		bsi := result.BSI
		if bsi == nil {
			bsi = roaring64.NewDefaultBSI()
		}
		bsis[requests[i].PhysicalField] = bsi
	}
	return bsis, probes, diagnostics, err
}

func (r LegacyDirectBitmapGroupAggregateReader) groupResults(ctx context.Context, read BitmapGroupAggregateReadRequest, groups []legacyDirectBitmapGroupAggregateEnum, bsis map[string]*roaring64.BSI) ([]BitmapGroupAggregateReadGroup, int, qsbridge.DiagnosticSet, error) {
	if len(groups) == 0 {
		return nil, 0, nil, nil
	}
	combinations := legacyDirectBitmapGroupAggregateCombinations(groups)
	resultGroups := make([]BitmapGroupAggregateReadGroup, 0, len(combinations))
	groupQueries := 0
	for _, combination := range combinations {
		rows, diagnostics, err := r.queryGroupRows(ctx, read, groups, combination)
		groupQueries++
		if err != nil || diagnostics.BlocksNative() {
			return resultGroups, groupQueries, diagnostics, err
		}
		if rows == nil || rows.IsEmpty() {
			continue
		}
		values := make([]qsbridge.ResultCell, 0, len(combination))
		keyParts := make([]string, 0, len(combination))
		for _, item := range combination {
			values = append(values, item.Cell)
			keyParts = append(keyParts, item.Label)
		}
		aggs, ok := legacyDirectBitmapGroupAggregateCells(read, rows, bsis)
		if !ok {
			return resultGroups, groupQueries, nil, nil
		}
		resultGroups = append(resultGroups, BitmapGroupAggregateReadGroup{
			Key:    strings.Join(keyParts, "\x00"),
			Values: values,
			Aggs:   aggs,
		})
	}
	return resultGroups, groupQueries, nil, nil
}

func (r LegacyDirectBitmapGroupAggregateReader) queryGroupRows(ctx context.Context, read BitmapGroupAggregateReadRequest, groups []legacyDirectBitmapGroupAggregateEnum, combination []legacyDirectBitmapGroupAggregateEnumValue) (*roaring64.Bitmap, qsbridge.DiagnosticSet, error) {
	fragments := legacyDirectBitmapGroupFragments(read.Index, groups, combination)
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: fragments})
	session, diagnostics, err := r.Sessions.BorrowDirectSession(ctx, request)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	if session == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "bitmap group aggregate received nil session"),
		}, nil
	}
	var bitmapResult BitmapQueryResult
	if candidateSession, ok := session.(DirectCandidateBitmapSessionHandle); ok && len(read.CandidateRows) > 0 {
		candidateSet := qsbridge.QuantaCandidateSet{Index: read.Index, Rownums: append([]qsbridge.QuantaRownum(nil), read.CandidateRows...)}
		var handled bool
		bitmapResult, diagnostics, handled, err = candidateSession.QueryBitmapWithCandidateSet(ctx, request, candidateSet)
		if err == nil && !diagnostics.BlocksNative() && !handled {
			bitmapResult, diagnostics, err = session.QueryBitmap(ctx, request)
		}
	} else {
		bitmapResult, diagnostics, err = session.QueryBitmap(ctx, request)
	}
	releaseDiagnostics := session.Release(ctx)
	diagnostics = append(diagnostics, releaseDiagnostics...)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	rows := legacyDirectRelationshipBitmap(bitmapResult.Rownums)
	if len(read.CandidateRows) > 0 && rows != nil {
		rows.And(legacyDirectRelationshipBitmap(read.CandidateRows))
	}
	return rows, diagnostics, nil
}

func legacyDirectBitmapGroupEnums(table *core.Table, fields []qsbridge.FieldRef) ([]legacyDirectBitmapGroupAggregateEnum, []string, int, bool) {
	groups := make([]legacyDirectBitmapGroupAggregateEnum, 0, len(fields))
	fieldNames := make([]string, 0, len(fields))
	valueCount := 1
	for _, field := range fields {
		name := legacyDirectBitmapGroupAggregateFieldName(field)
		if name == "" || field.Index != qsbridge.IndexStringEnum {
			return nil, nil, 0, false
		}
		attr, ok := legacyDirectBitmapGroupAggregateAttribute(table, name)
		if !ok || !nativeProjectionAttributeIsStringEnum(attr) || len(attr.Values) == 0 {
			return nil, nil, 0, false
		}
		items := make([]legacyDirectBitmapGroupAggregateEnumValue, 0, len(attr.Values))
		for _, value := range attr.Values {
			label := strings.TrimSpace(fmt.Sprint(value.Value))
			items = append(items, legacyDirectBitmapGroupAggregateEnumValue{
				ID:    value.RowID,
				Label: label,
				Cell:  qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: label},
			})
		}
		if len(items) == 0 || valueCount > legacyDirectBitmapGroupAggregateMaxCombinations/len(items) {
			return nil, nil, 0, false
		}
		valueCount *= len(items)
		groups = append(groups, legacyDirectBitmapGroupAggregateEnum{Field: field, Name: name, Attr: attr, Items: items})
		fieldNames = append(fieldNames, name)
	}
	return groups, fieldNames, valueCount, true
}

func legacyDirectBitmapGroupFragments(index string, groups []legacyDirectBitmapGroupAggregateEnum, combination []legacyDirectBitmapGroupAggregateEnumValue) []qsbridge.QuantaQueryFragment {
	fragments := make([]qsbridge.QuantaQueryFragment, 0, len(groups))
	for i, group := range groups {
		fragments = append(fragments, qsbridge.QuantaQueryFragment{
			Index:     index,
			Field:     group.Name,
			Operation: qsbridge.QuantaOperationIntersect,
			Values:    []*big.Int{new(big.Int).SetUint64(combination[i].ID)},
		})
	}
	return fragments
}

func legacyDirectBitmapGroupAggregateMeasureFields(table *core.Table, read BitmapGroupAggregateReadRequest) ([]qsbridge.FieldRef, []string, bool) {
	seen := make(map[string]struct{})
	fields := make([]qsbridge.FieldRef, 0)
	names := make([]string, 0)
	for _, aggregate := range read.Aggregates {
		function := strings.ToLower(aggregate.Function)
		if function == "count" && legacyDirectBitmapGroupAggregateFieldName(aggregate.Field) == "" {
			continue
		}
		switch function {
		case "sum", "avg", "min", "max":
		default:
			return nil, nil, false
		}
		name := legacyDirectBitmapGroupAggregateFieldName(aggregate.Field)
		if name == "" || aggregate.Field.Index != qsbridge.IndexBSI {
			return nil, nil, false
		}
		attr, ok := legacyDirectBitmapGroupAggregateAttribute(table, name)
		if !ok || attr == nil || !attr.IsBSI() {
			return nil, nil, false
		}
		key := strings.ToLower(name)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		fields = append(fields, aggregate.Field)
		names = append(names, name)
	}
	return fields, names, true
}

func legacyDirectBitmapGroupAggregateAttribute(table *core.Table, field string) (*core.Attribute, bool) {
	attr, diagnostics := nativeProjectionAttribute(table, field)
	if diagnostics.BlocksNative() || attr == nil {
		return nil, false
	}
	return attr, true
}

func legacyDirectBitmapGroupAggregateCombinations(groups []legacyDirectBitmapGroupAggregateEnum) [][]legacyDirectBitmapGroupAggregateEnumValue {
	var combinations [][]legacyDirectBitmapGroupAggregateEnumValue
	var walk func(int, []legacyDirectBitmapGroupAggregateEnumValue)
	walk = func(position int, prefix []legacyDirectBitmapGroupAggregateEnumValue) {
		if position == len(groups) {
			combinations = append(combinations, append([]legacyDirectBitmapGroupAggregateEnumValue(nil), prefix...))
			return
		}
		for _, item := range groups[position].Items {
			walk(position+1, append(prefix, item))
		}
	}
	walk(0, nil)
	return combinations
}

func legacyDirectBitmapGroupAggregateCells(read BitmapGroupAggregateReadRequest, rows *roaring64.Bitmap, bsis map[string]*roaring64.BSI) ([]qsbridge.ResultCell, bool) {
	cache := make(map[string]*legacyDirectBitmapGroupAggregateState)
	cells := make([]qsbridge.ResultCell, 0, len(read.Aggregates))
	for _, aggregate := range read.Aggregates {
		value, ok := legacyDirectBitmapGroupAggregateValueForRows(rows, aggregate, bsis, cache)
		if !ok {
			return nil, false
		}
		cells = append(cells, legacyDirectBitmapGroupAggregateCell(aggregate, value))
	}
	return cells, true
}

type legacyDirectBitmapGroupAggregateState struct {
	Rows     *roaring64.Bitmap
	Count    uint64
	Sum      *big.Int
	SumCount uint64
	Min      *big.Int
	Max      *big.Int
	HaveRows bool
	HaveSum  bool
	HaveMin  bool
	HaveMax  bool
}

func legacyDirectBitmapGroupAggregateValueForRows(groupRows *roaring64.Bitmap, aggregate BitmapGroupAggregateReadSpec, bsis map[string]*roaring64.BSI, cache map[string]*legacyDirectBitmapGroupAggregateState) (legacyDirectBitmapGroupAggregateValue, bool) {
	function := strings.ToLower(aggregate.Function)
	fieldName := legacyDirectBitmapGroupAggregateFieldName(aggregate.Field)
	if function == "count" && fieldName == "" {
		return legacyDirectBitmapGroupAggregateValue{Count: groupRows.GetCardinality()}, true
	}
	bsi := bsis[fieldName]
	if bsi == nil {
		return legacyDirectBitmapGroupAggregateValue{}, false
	}
	state := cache[fieldName]
	if state == nil {
		state = &legacyDirectBitmapGroupAggregateState{}
		cache[fieldName] = state
	}
	if !state.HaveRows {
		state.Rows = groupRows.Clone()
		state.Rows.And(bsi.GetExistenceBitmap())
		state.Count = state.Rows.GetCardinality()
		state.HaveRows = true
	}
	if state.Count == 0 {
		return legacyDirectBitmapGroupAggregateValue{}, true
	}
	switch function {
	case "sum", "avg":
		if !state.HaveSum {
			state.Sum, state.SumCount = bsi.SumBigValues(state.Rows)
			state.HaveSum = true
		}
		return legacyDirectBitmapGroupAggregateValue{Count: state.SumCount, Sum: cloneBigInt(state.Sum)}, true
	case "min":
		if !state.HaveMin {
			state.Min = bsi.MinMaxBig(0, roaring64.MIN, state.Rows)
			state.HaveMin = true
		}
		return legacyDirectBitmapGroupAggregateValue{Count: state.Count, Min: cloneBigInt(state.Min)}, true
	case "max":
		if !state.HaveMax {
			state.Max = bsi.MinMaxBig(0, roaring64.MAX, state.Rows)
			state.HaveMax = true
		}
		return legacyDirectBitmapGroupAggregateValue{Count: state.Count, Max: cloneBigInt(state.Max)}, true
	default:
		return legacyDirectBitmapGroupAggregateValue{}, false
	}
}

func legacyDirectBitmapGroupAggregateCell(aggregate BitmapGroupAggregateReadSpec, value legacyDirectBitmapGroupAggregateValue) qsbridge.ResultCell {
	function := strings.ToLower(aggregate.Function)
	if function == "count" && legacyDirectBitmapGroupAggregateFieldName(aggregate.Field) == "" {
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(value.Count)}
	}
	if value.Count == 0 {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
	}
	scale := legacyDirectBitmapGroupAggregateScale(aggregate.Field)
	switch function {
	case "sum":
		return legacyDirectBitmapGroupAggregateNumberCell(aggregate.Type, value.Sum, scale)
	case "avg":
		if value.Sum == nil {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: legacyDirectBitmapGroupAggregateFloat(value.Sum, scale) / float64(value.Count)}
	case "min":
		return legacyDirectBitmapGroupAggregateNumberCell(aggregate.Type, value.Min, scale)
	case "max":
		return legacyDirectBitmapGroupAggregateNumberCell(aggregate.Type, value.Max, scale)
	default:
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
	}
}

func legacyDirectBitmapGroupAggregateNumberCell(dataType qsbridge.DataType, value *big.Int, scale int) qsbridge.ResultCell {
	if value == nil {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
	}
	if dataType == qsbridge.DataTypeInt && scale == 0 && value.IsInt64() {
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: value.Int64()}
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: legacyDirectBitmapGroupAggregateFloat(value, scale)}
}

func legacyDirectBitmapGroupAggregateFloat(value *big.Int, scale int) float64 {
	if value == nil {
		return 0
	}
	if scale <= 0 {
		result, _ := new(big.Rat).SetInt(value).Float64()
		return result
	}
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	result, _ := new(big.Rat).SetFrac(value, divisor).Float64()
	if math.IsInf(result, 0) || math.IsNaN(result) {
		return 0
	}
	return result
}

func legacyDirectBitmapGroupAggregateScale(field qsbridge.FieldRef) int {
	scale := field.Encoding.Scale
	if scale < 0 {
		return 0
	}
	return scale
}

func legacyDirectBitmapGroupAggregateFieldName(field qsbridge.FieldRef) string {
	if field.PhysicalName != "" {
		return field.PhysicalName
	}
	return field.Name
}

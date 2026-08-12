package qsinabox

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
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/server"
)

// StandardBitmapGroupAggregateReader executes grouped BSI aggregates inside
// the local standard BitmapIndex.
type StandardBitmapGroupAggregateReader struct {
	TableCache *core.TableCacheStruct
	Direct     *server.BitmapIndex
	Database   string
}

// ReadBitmapGroupAggregates returns grouped BSI aggregates without materializing
// row-aligned group or measure vectors.
func (r StandardBitmapGroupAggregateReader) ReadBitmapGroupAggregates(ctx context.Context, read qsruntime.BitmapGroupAggregateReadRequest) (qsruntime.BitmapGroupAggregateReadResult, qsbridge.DiagnosticSet, bool, error) {
	if err := ctx.Err(); err != nil {
		return qsruntime.BitmapGroupAggregateReadResult{}, nil, false, err
	}
	if r.Direct == nil {
		return qsruntime.BitmapGroupAggregateReadResult{}, nil, false, nil
	}
	groupFields := make([]string, 0, len(read.GroupFields))
	for _, field := range read.GroupFields {
		name := standardBitmapGroupCountFieldName(field)
		if name == "" {
			return qsruntime.BitmapGroupAggregateReadResult{}, nil, false, nil
		}
		groupFields = append(groupFields, name)
	}
	aggregates := make([]server.BitmapGroupAggregateSpec, 0, len(read.Aggregates))
	for _, aggregate := range read.Aggregates {
		aggregates = append(aggregates, server.BitmapGroupAggregateSpec{
			Function: aggregate.Function,
			Field:    standardBitmapGroupCountFieldName(aggregate.Field),
		})
	}
	fromTime, toTime := standardProjectionWindowNanos(r.TableCache, read.Index, read.FromEpochMillis, read.ToEpochMillis)
	foundSet := standardProjectionBitmap(read.CandidateRows)
	start := time.Now()
	groups, stats, ok, err := r.Direct.BitmapGroupAggregatesStorage(read.Index, groupFields, aggregates, fromTime, toTime, foundSet)
	elapsed := time.Since(start)
	if err != nil || !ok {
		return qsruntime.BitmapGroupAggregateReadResult{}, nil, ok, err
	}
	resultGroups, diagnostics := r.resultGroups(read, groups)
	if diagnostics.BlocksNative() {
		return qsruntime.BitmapGroupAggregateReadResult{}, diagnostics, true, nil
	}
	return qsruntime.BitmapGroupAggregateReadResult{
		Groups:         resultGroups,
		Mode:           "standard_bitmap_bsi_aggregate",
		CandidateRows:  stats.CandidateRows,
		FieldCount:     stats.FieldCount,
		ValueCount:     stats.ValueCount,
		AggregateCount: stats.AggregateCount,
		Elapsed:        elapsed,
		Probes: []qsruntime.ExecutionProbe{{
			Section: "grouped_aggregate",
			Name:    "standard_bitmap_group_aggregate_elapsed",
			Value:   elapsed.String(),
			Detail:  read.Index + "." + strings.Join(groupFields, ","),
		}, {
			Section: "grouped_aggregate",
			Name:    "standard_bitmap_group_aggregate_groups",
			Value:   strconv.Itoa(len(resultGroups)),
			Detail:  read.Index,
		}, {
			Section: "grouped_aggregate",
			Name:    "standard_bitmap_group_aggregate_bsi_project_elapsed",
			Value:   stats.BSIProjectElapsed.String(),
			Detail:  read.Index,
		}, {
			Section: "grouped_aggregate",
			Name:    "standard_bitmap_group_aggregate_compute_elapsed",
			Value:   stats.AggregateElapsed.String(),
			Detail:  read.Index,
		}, {
			Section: "grouped_aggregate",
			Name:    "standard_bitmap_group_aggregate_value_set_elapsed",
			Value:   stats.ValueSetElapsed.String(),
			Detail:  read.Index,
		}, {
			Section: "grouped_aggregate",
			Name:    "standard_bitmap_group_aggregate_sum_elapsed",
			Value:   stats.SumElapsed.String(),
			Detail:  read.Index,
		}, {
			Section: "grouped_aggregate",
			Name:    "standard_bitmap_group_aggregate_minmax_elapsed",
			Value:   stats.MinMaxElapsed.String(),
			Detail:  read.Index,
		}},
	}, nil, true, nil
}

func (r StandardBitmapGroupAggregateReader) resultGroups(read qsruntime.BitmapGroupAggregateReadRequest, groups []server.BitmapGroupAggregate) ([]qsruntime.BitmapGroupAggregateReadGroup, qsbridge.DiagnosticSet) {
	result := make([]qsruntime.BitmapGroupAggregateReadGroup, 0, len(groups))
	labelCache := make(map[string]map[uint64]qsbridge.ResultCell)
	countReader := StandardBitmapGroupCountReader{TableCache: r.TableCache, Direct: r.Direct, Database: r.Database}
	for _, group := range groups {
		if len(group.Values) != len(read.GroupFields) {
			return nil, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "bitmap group aggregate returned mismatched group value width"),
			}
		}
		if len(group.Aggs) != len(read.Aggregates) {
			return nil, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "bitmap group aggregate returned mismatched aggregate width"),
			}
		}
		cells := make([]qsbridge.ResultCell, 0, len(group.Values))
		keyParts := make([]string, 0, len(group.Values))
		for i, id := range group.Values {
			cell, diagnostics := countReader.groupValueCell(read.Index, read.GroupFields[i], id, labelCache)
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			cells = append(cells, cell)
			keyParts = append(keyParts, fmt.Sprint(cell.Value))
		}
		aggs := make([]qsbridge.ResultCell, 0, len(group.Aggs))
		for i, aggregate := range read.Aggregates {
			aggs = append(aggs, r.aggregateCell(read.Index, aggregate, group.Aggs[i]))
		}
		result = append(result, qsruntime.BitmapGroupAggregateReadGroup{
			Key:    strings.Join(keyParts, "\x00"),
			Values: cells,
			Aggs:   aggs,
		})
	}
	return result, nil
}

func (r StandardBitmapGroupAggregateReader) aggregateCell(index string, aggregate qsruntime.BitmapGroupAggregateReadSpec, value server.BitmapGroupAggregateValue) qsbridge.ResultCell {
	function := strings.ToLower(aggregate.Function)
	if function == "count" && aggregate.Field.Name == "" && aggregate.Field.PhysicalName == "" {
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(value.Count)}
	}
	if value.Count == 0 {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
	}
	scale := r.aggregateScale(index, aggregate.Field)
	switch function {
	case "sum":
		return standardBitmapGroupAggregateNumberCell(aggregate.Type, value.Sum, scale)
	case "avg":
		if value.Sum == nil {
			return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
		}
		return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: standardBitmapGroupAggregateFloat(value.Sum, scale) / float64(value.Count)}
	case "min":
		return standardBitmapGroupAggregateNumberCell(aggregate.Type, value.Min, scale)
	case "max":
		return standardBitmapGroupAggregateNumberCell(aggregate.Type, value.Max, scale)
	default:
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
	}
}

func (r StandardBitmapGroupAggregateReader) aggregateScale(index string, field qsbridge.FieldRef) int {
	name := standardBitmapGroupCountFieldName(field)
	if r.TableCache == nil || name == "" {
		return 0
	}
	r.TableCache.TableCacheLock.RLock()
	table := r.TableCache.TableCache[index]
	r.TableCache.TableCacheLock.RUnlock()
	if table == nil {
		return 0
	}
	attr, err := table.GetAttribute(name)
	if err != nil || attr == nil || attr.Scale < 0 {
		return 0
	}
	return attr.Scale
}

func standardBitmapGroupAggregateNumberCell(dataType qsbridge.DataType, value *big.Int, scale int) qsbridge.ResultCell {
	if value == nil {
		return qsbridge.ResultCell{Kind: qsbridge.ValueNull, Value: nil}
	}
	if dataType == qsbridge.DataTypeInt && scale == 0 && value.IsInt64() {
		return qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: value.Int64()}
	}
	return qsbridge.ResultCell{Kind: qsbridge.ValueFloat, Value: standardBitmapGroupAggregateFloat(value, scale)}
}

func standardBitmapGroupAggregateFloat(value *big.Int, scale int) float64 {
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

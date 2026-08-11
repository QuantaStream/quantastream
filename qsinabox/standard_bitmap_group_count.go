package qsinabox

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/server"
)

// StandardBitmapGroupCountReader executes grouped COUNT(*) over low-cardinality
// bitmap fields inside the local standard BitmapIndex.
type StandardBitmapGroupCountReader struct {
	TableCache *core.TableCacheStruct
	Direct     *server.BitmapIndex
	Database   string
}

// ReadBitmapGroupCounts returns grouped counts without materializing row-aligned
// StringEnum vectors when the physical tier can serve the bitmap shape.
func (r StandardBitmapGroupCountReader) ReadBitmapGroupCounts(ctx context.Context, read qsruntime.BitmapGroupCountReadRequest) (qsruntime.BitmapGroupCountReadResult, qsbridge.DiagnosticSet, bool, error) {
	if err := ctx.Err(); err != nil {
		return qsruntime.BitmapGroupCountReadResult{}, nil, false, err
	}
	if r.Direct == nil {
		return qsruntime.BitmapGroupCountReadResult{}, nil, false, nil
	}
	fields := make([]string, 0, len(read.Fields))
	for _, field := range read.Fields {
		name := standardBitmapGroupCountFieldName(field)
		if name == "" {
			return qsruntime.BitmapGroupCountReadResult{}, nil, false, nil
		}
		fields = append(fields, name)
	}
	fromTime, toTime := standardProjectionWindowNanos(r.TableCache, read.Index, read.FromEpochMillis, read.ToEpochMillis)
	foundSet := standardProjectionBitmap(read.CandidateRows)
	start := time.Now()
	groups, stats, ok, err := r.Direct.BitmapGroupCounts(read.Index, fields, fromTime, toTime, foundSet)
	elapsed := time.Since(start)
	if err != nil || !ok {
		return qsruntime.BitmapGroupCountReadResult{}, nil, ok, err
	}
	resultGroups, diagnostics := r.resultGroups(read, groups)
	if diagnostics.BlocksNative() {
		return qsruntime.BitmapGroupCountReadResult{}, diagnostics, true, nil
	}
	return qsruntime.BitmapGroupCountReadResult{
		Groups:        resultGroups,
		Mode:          "standard_bitmap_intersection",
		CandidateRows: stats.CandidateRows,
		FieldCount:    stats.FieldCount,
		ValueCount:    stats.ValueCount,
		Elapsed:       elapsed,
		Probes: []qsruntime.ExecutionProbe{{
			Section: "grouped_aggregate",
			Name:    "standard_bitmap_group_count_elapsed",
			Value:   elapsed.String(),
			Detail:  read.Index + "." + strings.Join(fields, ","),
		}, {
			Section: "grouped_aggregate",
			Name:    "standard_bitmap_group_count_groups",
			Value:   strconv.Itoa(len(resultGroups)),
			Detail:  read.Index,
		}},
	}, nil, true, nil
}

func (r StandardBitmapGroupCountReader) resultGroups(read qsruntime.BitmapGroupCountReadRequest, groups []server.BitmapGroupCount) ([]qsruntime.BitmapGroupCountReadGroup, qsbridge.DiagnosticSet) {
	result := make([]qsruntime.BitmapGroupCountReadGroup, 0, len(groups))
	labelCache := make(map[string]map[uint64]qsbridge.ResultCell)
	for _, group := range groups {
		if len(group.Values) != len(read.Fields) {
			return nil, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "bitmap group count returned mismatched group value width"),
			}
		}
		cells := make([]qsbridge.ResultCell, 0, len(group.Values))
		keyParts := make([]string, 0, len(group.Values))
		for i, id := range group.Values {
			cell, diagnostics := r.groupValueCell(read.Index, read.Fields[i], id, labelCache)
			if diagnostics.BlocksNative() {
				return nil, diagnostics
			}
			cells = append(cells, cell)
			keyParts = append(keyParts, fmt.Sprint(cell.Value))
		}
		result = append(result, qsruntime.BitmapGroupCountReadGroup{
			Key:    strings.Join(keyParts, "\x00"),
			Values: cells,
			Count:  group.Count,
		})
	}
	return result, nil
}

func (r StandardBitmapGroupCountReader) groupValueCell(index string, field qsbridge.FieldRef, id uint64, cache map[string]map[uint64]qsbridge.ResultCell) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	name := standardBitmapGroupCountFieldName(field)
	cacheKey := index + "." + name
	if cache[cacheKey] == nil {
		cache[cacheKey] = make(map[uint64]qsbridge.ResultCell)
	}
	if cell, ok := cache[cacheKey][id]; ok {
		return cell, nil
	}
	if field.Index != qsbridge.IndexStringEnum {
		cell := qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(id)}
		cache[cacheKey][id] = cell
		return cell, nil
	}
	ref := field.Dictionary.Ref
	if !ref.Valid() {
		ref = qsbridge.DictionaryRef{Schema: r.Database, Table: index, Field: name}
	}
	entry, diagnostics := (qsruntime.LegacyTableCacheDictionaryResolver{
		TableCache: r.TableCache,
		Schema:     r.Database,
	}).LookupID(ref, qsbridge.StringEnumID(id))
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	cell := qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: entry.Label}
	cache[cacheKey][id] = cell
	return cell, nil
}

func standardBitmapGroupCountFieldName(field qsbridge.FieldRef) string {
	if field.PhysicalName != "" {
		return field.PhysicalName
	}
	return field.Name
}

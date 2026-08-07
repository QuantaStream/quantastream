package qsruntime

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/source"
)

// LegacyDirectBackingStringLookupReader resolves legacy StringLexBSI backing
// strings through KVStore without going through core.Projector.
type LegacyDirectBackingStringLookupReader struct {
	Source     *source.QuantaSource
	TableCache *core.TableCacheStruct
}

// LookupProjectionBackingStrings resolves rownum-keyed backing strings.
func (r LegacyDirectBackingStringLookupReader) LookupProjectionBackingStrings(_ context.Context, request NativeProjectionBackingStringLookupRequest) (NativeProjectionBackingStringLookupResult, qsbridge.DiagnosticSet, error) {
	index, fieldName := legacyDirectBackingStringLookupTarget(request)
	if index == "" || fieldName == "" {
		return NativeProjectionBackingStringLookupResult{}, nativeProjectionUnsupported("backing-string lookup requires index and field"), nil
	}
	if r.Source == nil || r.Source.GetSessionPool() == nil {
		return NativeProjectionBackingStringLookupResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "backing-string reader has no source session pool"),
		}, nil
	}
	table := legacyDirectRelationshipCachedTable(r.TableCache, index)
	if table == nil {
		return NativeProjectionBackingStringLookupResult{}, nativeProjectionUnsupported("backing-string lookup has no table metadata for " + index), nil
	}
	pool := r.Source.GetSessionPool()
	session, err := pool.Borrow(index)
	if err != nil {
		return NativeProjectionBackingStringLookupResult{}, nil, err
	}
	defer pool.Return(index, session)
	if session == nil || session.KVStore == nil {
		return NativeProjectionBackingStringLookupResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "backing-string reader has no KVStore"),
		}, nil
	}

	result := NativeProjectionBackingStringLookupResult{
		Values: make([]qsbridge.ResultCell, len(request.Values)),
		Probes: []ExecutionProbe{{
			Section: "native_projection_materialization",
			Name:    "backing_string_lookup_values",
			Value:   strconv.Itoa(len(request.Values)),
			Detail:  index + "." + fieldName,
		}},
	}
	batches := make(map[string]map[interface{}]interface{})
	positions := make(map[string]map[uint64][]int)
	for i, value := range request.Values {
		result.Values[i] = qsbridge.ResultCell{Kind: qsbridge.ValueNull}
		if value.Kind == qsbridge.ValueNull {
			continue
		}
		rownum, ok := nativeProjectionBackingStringRowKey(value)
		if !ok {
			return NativeProjectionBackingStringLookupResult{Probes: result.Probes}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "backing-string lookup received non-integer rownum key"),
			}, nil
		}
		path := legacyDirectBackingStringPath(table, fieldName, time.Unix(0, int64(rownum)))
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
			return NativeProjectionBackingStringLookupResult{Probes: result.Probes}, nil, fmt.Errorf("backing-string BatchLookup error for [%s] - %w", path, err)
		}
		for rownum, outputPositions := range positions[path] {
			visible, ok := nativeProjectionBackingStringLookupValue(lookup, rownum)
			if !ok {
				continue
			}
			for _, outputPosition := range outputPositions {
				result.Values[outputPosition] = qsbridge.ResultCell{Kind: qsbridge.ValueString, Value: visible}
			}
		}
	}
	return result, nil, nil
}

func legacyDirectBackingStringLookupTarget(request NativeProjectionBackingStringLookupRequest) (string, string) {
	index := strings.TrimSpace(request.Index)
	if index == "" {
		index = strings.TrimSpace(request.Field.Index)
	}
	fieldName := nativeProjectionPhysicalField(request.Field)
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

func legacyDirectBackingStringPath(table *core.Table, field string, ts time.Time) string {
	store := legacyDirectBackingStringStore(table, field)
	return core.PartitionedStringIndexPath(table.Name, field, store, ts)
}

func legacyDirectBackingStringStore(table *core.Table, field string) string {
	if table != nil {
		if attr, err := table.GetAttribute(field); err == nil && strings.EqualFold(strings.TrimSpace(attr.MappingStrategy), "StringLexBSI") {
			return "lex_remainders"
		}
	}
	return "strings"
}

func nativeProjectionBackingStringRowKey(value qsbridge.ResultCell) (uint64, bool) {
	if value.Kind == qsbridge.ValueNull {
		return 0, false
	}
	switch typed := value.Value.(type) {
	case NativeProjectionStringRemainderKey:
		return typed.RowNum, true
	case *NativeProjectionStringRemainderKey:
		if typed == nil {
			return 0, false
		}
		return typed.RowNum, true
	case int:
		if typed < 0 {
			return 0, false
		}
		return uint64(typed), true
	case int64:
		if typed < 0 {
			return 0, false
		}
		return uint64(typed), true
	case uint64:
		if typed > uint64(^uint64(0)>>1) {
			return 0, false
		}
		return typed, true
	default:
		return 0, false
	}
}

func nativeProjectionBackingStringLookupValue(values map[interface{}]interface{}, rownum uint64) (string, bool) {
	for _, key := range []interface{}{rownum, int64(rownum), int(rownum)} {
		if value, ok := values[key]; ok {
			text := fmt.Sprint(value)
			return text, text != "<nil>"
		}
	}
	return "", false
}

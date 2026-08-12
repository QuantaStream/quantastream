package qsruntime

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
)

// LegacyDirectBitmapGroupCountReader computes low-cardinality grouped counts
// through direct count-only bitmap queries.
type LegacyDirectBitmapGroupCountReader struct {
	Sessions   DirectSessionProvider
	TableCache *core.TableCacheStruct
}

// ReadBitmapGroupCounts evaluates each low-cardinality group as an associative
// bitmap cardinality query: count(base query AND group-value bitmaps).
func (r LegacyDirectBitmapGroupCountReader) ReadBitmapGroupCounts(ctx context.Context, read BitmapGroupCountReadRequest) (BitmapGroupCountReadResult, qsbridge.DiagnosticSet, bool, error) {
	if err := ctx.Err(); err != nil {
		return BitmapGroupCountReadResult{}, nil, false, err
	}
	if r.Sessions == nil || r.TableCache == nil {
		return BitmapGroupCountReadResult{}, nil, false, nil
	}
	table := legacyDirectRelationshipCachedTable(r.TableCache, read.Index)
	if table == nil {
		return BitmapGroupCountReadResult{}, nil, false, nil
	}
	if !legacyDirectBitmapGroupCountFullCandidateQuery(read.BaseQuery) {
		return BitmapGroupCountReadResult{}, nil, false, nil
	}
	groups, groupFields, valueCount, ok := legacyDirectBitmapGroupEnums(table, read.Fields)
	if !ok {
		return BitmapGroupCountReadResult{}, nil, false, nil
	}

	start := time.Now()
	combinations := legacyDirectBitmapGroupAggregateCombinations(groups)
	resultGroups := make([]BitmapGroupCountReadGroup, 0, len(combinations))
	for _, combination := range combinations {
		count, diagnostics, handled, err := r.queryGroupCount(ctx, read, groups, combination)
		if err != nil || diagnostics.BlocksNative() || !handled {
			return BitmapGroupCountReadResult{}, diagnostics, handled, err
		}
		if count == 0 {
			continue
		}
		values := make([]qsbridge.ResultCell, 0, len(combination))
		keyParts := make([]string, 0, len(combination))
		for _, item := range combination {
			values = append(values, item.Cell)
			keyParts = append(keyParts, item.Label)
		}
		resultGroups = append(resultGroups, BitmapGroupCountReadGroup{
			Key:    strings.Join(keyParts, "\x00"),
			Values: values,
			Count:  count,
		})
	}
	elapsed := time.Since(start)
	return BitmapGroupCountReadResult{
		Groups:        resultGroups,
		Mode:          "legacy_direct_bitmap_count_only",
		CandidateRows: uint64(len(read.CandidateRows)),
		FieldCount:    len(read.Fields),
		ValueCount:    valueCount,
		Elapsed:       elapsed,
		Probes: []ExecutionProbe{{
			Section: "grouped_aggregate",
			Name:    "legacy_direct_bitmap_group_count_elapsed",
			Value:   elapsed.String(),
			Detail:  read.Index + "." + strings.Join(groupFields, ","),
		}, {
			Section: "grouped_aggregate",
			Name:    "legacy_direct_bitmap_group_count_queries",
			Value:   strconv.Itoa(len(combinations)),
			Detail:  read.Index,
		}, {
			Section: "grouped_aggregate",
			Name:    "legacy_direct_bitmap_group_count_groups",
			Value:   strconv.Itoa(len(resultGroups)),
			Detail:  read.Index,
		}},
	}, nil, true, nil
}

func (r LegacyDirectBitmapGroupCountReader) queryGroupCount(ctx context.Context, read BitmapGroupCountReadRequest, groups []legacyDirectBitmapGroupAggregateEnum, combination []legacyDirectBitmapGroupAggregateEnumValue) (uint64, qsbridge.DiagnosticSet, bool, error) {
	query := cloneIntermediateQuery(read.BaseQuery)
	query.Fragments = append(query.Fragments, legacyDirectBitmapGroupFragments(read.Index, groups, combination)...)
	request := NewExecutionRequest(query)
	session, diagnostics, err := r.Sessions.BorrowDirectSession(ctx, request)
	if err != nil || diagnostics.BlocksNative() {
		return 0, diagnostics, true, err
	}
	if session == nil {
		return 0, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "bitmap group count received nil session"),
		}, true, nil
	}
	countSession, ok := session.(DirectCountOnlyBitmapSessionHandle)
	if !ok {
		releaseDiagnostics := session.Release(ctx)
		return 0, releaseDiagnostics, false, nil
	}
	result, queryDiagnostics, err := countSession.QueryBitmapCountOnly(ctx, request)
	releaseDiagnostics := session.Release(ctx)
	diagnostics = append(diagnostics, queryDiagnostics...)
	diagnostics = append(diagnostics, releaseDiagnostics...)
	if err != nil || diagnostics.BlocksNative() {
		return 0, diagnostics, true, err
	}
	count := result.Count
	if count == 0 && len(result.Rownums) > 0 {
		count = uint64(len(result.Rownums))
	}
	return count, diagnostics, true, nil
}

func legacyDirectBitmapGroupCountFullCandidateQuery(query qsbridge.QuantaIntermediateQuery) bool {
	if !query.Filter.Empty() {
		return false
	}
	for _, fragment := range query.Fragments {
		if fragment.Operation != qsbridge.QuantaOperationIntersect ||
			!fragment.NullCheck ||
			!fragment.Negate ||
			fragment.Value != nil ||
			len(fragment.Values) != 0 ||
			fragment.Begin != nil ||
			fragment.End != nil ||
			fragment.HasLiteral ||
			fragment.HasLiteralRange ||
			len(fragment.Literals) != 0 ||
			fragment.Literal.Value != nil ||
			fragment.BeginLiteral.Value != nil ||
			fragment.EndLiteral.Value != nil {
			return false
		}
	}
	return true
}

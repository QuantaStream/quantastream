package qsruntime

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// BitmapGroupCountReadRequest asks the physical tier to count candidate rows by
// low-cardinality bitmap-backed group fields without row-wise materialization.
type BitmapGroupCountReadRequest struct {
	Index           string
	Fields          []qsbridge.FieldRef
	BaseQuery       qsbridge.QuantaIntermediateQuery
	FromEpochMillis int64
	ToEpochMillis   int64
	CandidateRows   []qsbridge.QuantaRownum
}

// BitmapGroupCountReadGroup is one grouped COUNT(*) result.
type BitmapGroupCountReadGroup struct {
	Key    string
	Values []qsbridge.ResultCell
	Count  uint64
}

// BitmapGroupCountReadResult is the physical-tier response for bitmap group counts.
type BitmapGroupCountReadResult struct {
	Groups        []BitmapGroupCountReadGroup
	Mode          string
	CandidateRows uint64
	FieldCount    int
	ValueCount    int
	Elapsed       time.Duration
	Probes        []ExecutionProbe
}

// BitmapGroupCountReader is implemented by physical tiers that can aggregate
// grouped counts directly from bitmap fields.
type BitmapGroupCountReader interface {
	ReadBitmapGroupCounts(context.Context, BitmapGroupCountReadRequest) (BitmapGroupCountReadResult, qsbridge.DiagnosticSet, bool, error)
}

func (r DirectBitmapRuntime) directBitmapBitmapGroupCountAggregateResult(ctx context.Context, request ExecutionRequest, bitmapResult BitmapQueryResult, result ExecutionResult) (ExecutionResult, bool) {
	reader := r.BitmapGroupCounts
	if reader == nil || !directBitmapBitmapGroupCountCandidate(request) {
		return result, false
	}
	groupExpressions, diagnostics := directBitmapGroupExpressions(request.GroupBy)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() || !directBitmapBitmapGroupCountExpressions(groupExpressions) {
		return result, false
	}
	rootIndex, ok := request.RootIndex()
	if !ok {
		return result, false
	}
	fields := make([]qsbridge.FieldRef, 0, len(groupExpressions))
	for _, expression := range groupExpressions {
		field := expression.Field
		if !strings.EqualFold(field.Table.Table, rootIndex) {
			return result, false
		}
		fields = append(fields, field)
	}
	read := BitmapGroupCountReadRequest{
		Index:           rootIndex,
		Fields:          fields,
		BaseQuery:       cloneIntermediateQuery(request.Query),
		FromEpochMillis: request.Materialization.FromEpochMillis,
		ToEpochMillis:   request.Materialization.ToEpochMillis,
		CandidateRows:   append([]qsbridge.QuantaRownum(nil), bitmapResult.Rownums...),
	}
	readStart := time.Now()
	groupCounts, readDiagnostics, ok, err := reader.ReadBitmapGroupCounts(ctx, read)
	readElapsed := time.Since(readStart)
	result.Diagnostics = append(result.Diagnostics, readDiagnostics...)
	if err != nil {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticInternalInvariant,
			qsbridge.PhaseExecute,
			err.Error(),
		))
		return result, true
	}
	if result.Diagnostics.BlocksNative() || !ok {
		return result, false
	}
	rows := directBitmapRowsFromBitmapGroupCounts(request, groupCounts.Groups)
	probe := directBitmapGroupedAggregateProbe{
		CandidateRows:                len(bitmapResult.Rownums),
		GroupExpressionCount:         len(groupExpressions),
		ComputedGroupExpressionCount: directBitmapComputedGroupExpressionCount(groupExpressions),
		GroupExpressionShapes:        directBitmapGroupExpressionShapes(groupExpressions),
		GroupExpressionFields:        directBitmapGroupExpressionFields(groupExpressions),
		GroupCount:                   len(rows),
		Limit:                        request.Result.Limit,
		TopNCandidate:                executionGroupedAggregateTopNCandidate(request),
		GroupStrategy:                "bitmap_group_count",
		GroupTime:                    readElapsed,
	}
	havingStart := time.Now()
	rows, diagnostics = directBitmapFilterGroupedAggregateRows(request.Having, rows, groupExpressions)
	probe.HavingTime = time.Since(havingStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true
	}
	probe.PostHavingGroups = len(rows)
	probe.SortInputGroups = len(rows)
	probe.OrderStrategy = directBitmapGroupedAggregateOrderStrategy(request, len(rows))
	orderStart := time.Now()
	rows, diagnostics = directBitmapOrderGroupedAggregateRows(request, rows, groupExpressions)
	probe.OrderTime = time.Since(orderStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true
	}
	outputStart := time.Now()
	rowSet, diagnostics := directBitmapGroupedAggregateRowSet(request, rows, groupExpressions)
	probe.OutputTime = time.Since(outputStart)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true
	}
	limitStart := time.Now()
	rowSet = directBitmapLimitProjectedRowSet(rowSet, request.Result.Offset, request.Result.Limit, request.Result.HasResultLimit())
	probe.LimitTime = time.Since(limitStart)
	probe.FinalRows = rowSet.CandidateCount()
	newProbes := append([]ExecutionProbe{}, groupCounts.Probes...)
	newProbes = append(newProbes,
		ExecutionProbe{Section: "grouped_aggregate", Name: "bitmap_group_count_mode", Value: groupCounts.Mode},
		ExecutionProbe{Section: "grouped_aggregate", Name: "bitmap_group_count_candidate_rows", Value: strconv.FormatUint(groupCounts.CandidateRows, 10)},
		ExecutionProbe{Section: "grouped_aggregate", Name: "bitmap_group_count_fields", Value: strconv.Itoa(groupCounts.FieldCount)},
		ExecutionProbe{Section: "grouped_aggregate", Name: "bitmap_group_count_values", Value: strconv.Itoa(groupCounts.ValueCount)},
		ExecutionProbe{Section: "grouped_aggregate", Name: "phase_bitmap_group_count_elapsed", Value: readElapsed.String()},
	)
	newProbes = append(newProbes, directBitmapGroupedAggregateProbes(probe)...)
	result.Probes = append(result.Probes, newProbes...)
	recordExecutionProbes(ctx, newProbes)
	result.RowSet = rowSet
	result.Count = uint64(rowSet.CandidateCount())
	return result, true
}

func directBitmapBitmapGroupCountCandidate(request ExecutionRequest) bool {
	if len(request.GroupBy) == 0 || len(request.SQLAggregates) == 0 {
		return false
	}
	if request.HasCandidateSet {
		return false
	}
	if directBitmapHasResidualScanPredicates(request) || !request.NativePredicates.Empty() {
		return false
	}
	for _, aggregate := range request.SQLAggregates {
		if !directBitmapCountAllAggregate(aggregate) {
			return false
		}
	}
	return true
}

func directBitmapBitmapGroupCountExpressions(groupExpressions []directBitmapGroupExpression) bool {
	if len(groupExpressions) == 0 {
		return false
	}
	for _, expression := range groupExpressions {
		if expression.Expr == nil || expression.Field.Name == "" {
			return false
		}
		if !directBitmapGroupExpressionIsField(expression) {
			return false
		}
		if expression.Field.Index != qsbridge.IndexStringEnum && expression.Field.Index != qsbridge.IndexBitmap {
			return false
		}
	}
	return true
}

func directBitmapRowsFromBitmapGroupCounts(request ExecutionRequest, groups []BitmapGroupCountReadGroup) []directBitmapGroupedAggregateRow {
	rows := make([]directBitmapGroupedAggregateRow, 0, len(groups))
	for _, group := range groups {
		aggs := make([]qsbridge.ResultCell, 0, len(request.SQLAggregates))
		for range request.SQLAggregates {
			aggs = append(aggs, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: int64(group.Count)})
		}
		rows = append(rows, directBitmapGroupedAggregateRow{
			Key:    group.Key,
			Groups: append([]qsbridge.ResultCell(nil), group.Values...),
			Aggs:   aggs,
		})
	}
	return rows
}

package qsruntime

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// BitmapGroupAggregateReadRequest asks the physical tier to compute grouped
// aggregates directly from bitmap-backed group fields and BSI-backed measures.
type BitmapGroupAggregateReadRequest struct {
	Index           string
	GroupFields     []qsbridge.FieldRef
	Aggregates      []BitmapGroupAggregateReadSpec
	FromEpochMillis int64
	ToEpochMillis   int64
	CandidateRows   []qsbridge.QuantaRownum
}

// BitmapGroupAggregateReadSpec describes one aggregate slot requested from the
// physical tier. Field is empty for COUNT(*).
type BitmapGroupAggregateReadSpec struct {
	Function string
	Field    qsbridge.FieldRef
	Type     qsbridge.DataType
}

// BitmapGroupAggregateReadGroup is one grouped aggregate result.
type BitmapGroupAggregateReadGroup struct {
	Key    string
	Values []qsbridge.ResultCell
	Aggs   []qsbridge.ResultCell
}

// BitmapGroupAggregateReadResult is the physical-tier response for grouped BSI
// aggregates.
type BitmapGroupAggregateReadResult struct {
	Groups         []BitmapGroupAggregateReadGroup
	Mode           string
	CandidateRows  uint64
	FieldCount     int
	ValueCount     int
	AggregateCount int
	Elapsed        time.Duration
	Probes         []ExecutionProbe
}

// BitmapGroupAggregateReader is implemented by physical tiers that can compute
// grouped BSI aggregates without row-wise materialization.
type BitmapGroupAggregateReader interface {
	ReadBitmapGroupAggregates(context.Context, BitmapGroupAggregateReadRequest) (BitmapGroupAggregateReadResult, qsbridge.DiagnosticSet, bool, error)
}

func (r DirectBitmapRuntime) directBitmapBitmapGroupAggregateResult(ctx context.Context, request ExecutionRequest, bitmapResult BitmapQueryResult, result ExecutionResult) (ExecutionResult, bool) {
	reader := r.BitmapGroupAggregates
	if reader == nil {
		return result, false
	}
	groupExpressions, aggregateSpecs, ok, diagnostics := directBitmapBitmapGroupAggregatePlan(request)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() || !ok {
		return result, false
	}
	rootIndex, _ := request.RootIndex()
	fields := make([]qsbridge.FieldRef, 0, len(groupExpressions))
	for _, expression := range groupExpressions {
		fields = append(fields, expression.Field)
	}
	read := BitmapGroupAggregateReadRequest{
		Index:           rootIndex,
		GroupFields:     fields,
		Aggregates:      aggregateSpecs,
		FromEpochMillis: request.Materialization.FromEpochMillis,
		ToEpochMillis:   request.Materialization.ToEpochMillis,
		CandidateRows:   append([]qsbridge.QuantaRownum(nil), bitmapResult.Rownums...),
	}
	readStart := time.Now()
	groupAggregates, readDiagnostics, ok, err := reader.ReadBitmapGroupAggregates(ctx, read)
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
	rows := directBitmapRowsFromBitmapGroupAggregates(groupAggregates.Groups)
	probe := directBitmapGroupedAggregateProbe{
		CandidateRows:                len(bitmapResult.Rownums),
		GroupExpressionCount:         len(groupExpressions),
		ComputedGroupExpressionCount: directBitmapComputedGroupExpressionCount(groupExpressions),
		GroupExpressionShapes:        directBitmapGroupExpressionShapes(groupExpressions),
		GroupExpressionFields:        directBitmapGroupExpressionFields(groupExpressions),
		GroupCount:                   len(rows),
		Limit:                        request.Result.Limit,
		TopNCandidate:                executionGroupedAggregateTopNCandidate(request),
		GroupStrategy:                "bitmap_group_aggregate",
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
	newProbes := append([]ExecutionProbe{}, groupAggregates.Probes...)
	newProbes = append(newProbes,
		ExecutionProbe{Section: "grouped_aggregate", Name: "bitmap_group_aggregate_mode", Value: groupAggregates.Mode},
		ExecutionProbe{Section: "grouped_aggregate", Name: "bitmap_group_aggregate_candidate_rows", Value: strconv.FormatUint(groupAggregates.CandidateRows, 10)},
		ExecutionProbe{Section: "grouped_aggregate", Name: "bitmap_group_aggregate_fields", Value: strconv.Itoa(groupAggregates.FieldCount)},
		ExecutionProbe{Section: "grouped_aggregate", Name: "bitmap_group_aggregate_values", Value: strconv.Itoa(groupAggregates.ValueCount)},
		ExecutionProbe{Section: "grouped_aggregate", Name: "bitmap_group_aggregate_slots", Value: strconv.Itoa(groupAggregates.AggregateCount)},
		ExecutionProbe{Section: "grouped_aggregate", Name: "phase_bitmap_group_aggregate_elapsed", Value: readElapsed.String()},
	)
	newProbes = append(newProbes, directBitmapGroupedAggregateProbes(probe)...)
	result.Probes = append(result.Probes, newProbes...)
	recordExecutionProbes(ctx, newProbes)
	result.RowSet = rowSet
	result.Count = uint64(rowSet.CandidateCount())
	return result, true
}

func directBitmapBitmapGroupAggregatePlan(request ExecutionRequest) ([]directBitmapGroupExpression, []BitmapGroupAggregateReadSpec, bool, qsbridge.DiagnosticSet) {
	if len(request.GroupBy) == 0 || len(request.SQLAggregates) == 0 {
		return nil, nil, false, nil
	}
	if directBitmapHasResidualScanPredicates(request) || !request.NativePredicates.Empty() {
		return nil, nil, false, nil
	}
	rootIndex, ok := request.RootIndex()
	if !ok {
		return nil, nil, false, nil
	}
	groupExpressions, diagnostics := directBitmapGroupExpressions(request.GroupBy)
	if diagnostics.BlocksNative() || !directBitmapBitmapGroupCountExpressions(groupExpressions) {
		return nil, nil, false, diagnostics
	}
	for _, expression := range groupExpressions {
		if !strings.EqualFold(expression.Field.Table.Table, rootIndex) {
			return nil, nil, false, nil
		}
	}
	specs := make([]BitmapGroupAggregateReadSpec, 0, len(request.SQLAggregates))
	for _, aggregate := range request.SQLAggregates {
		spec, ok := directBitmapBitmapGroupAggregateSpec(rootIndex, aggregate)
		if !ok {
			return nil, nil, false, nil
		}
		specs = append(specs, spec)
	}
	return groupExpressions, specs, true, diagnostics
}

func directBitmapBitmapGroupAggregateSpec(rootIndex string, aggregate qsbridge.Aggregate) (BitmapGroupAggregateReadSpec, bool) {
	if aggregate.Filter != nil || aggregate.Mode == qsbridge.AggregateDistinct {
		return BitmapGroupAggregateReadSpec{}, false
	}
	if directBitmapCountAllAggregate(aggregate) {
		return BitmapGroupAggregateReadSpec{
			Function: "count",
			Type:     directBitmapAggregateResultType(aggregate),
		}, true
	}
	function := strings.ToLower(aggregate.Function)
	switch function {
	case "sum", "avg", "min", "max":
	default:
		return BitmapGroupAggregateReadSpec{}, false
	}
	field, ok := directBitmapAggregateInputField(aggregate)
	if !ok || !strings.EqualFold(field.Table.Table, rootIndex) || field.Index != qsbridge.IndexBSI {
		return BitmapGroupAggregateReadSpec{}, false
	}
	if field.Type != qsbridge.DataTypeInt && field.Type != qsbridge.DataTypeFloat {
		return BitmapGroupAggregateReadSpec{}, false
	}
	return BitmapGroupAggregateReadSpec{
		Function: function,
		Field:    field,
		Type:     directBitmapAggregateResultType(aggregate),
	}, true
}

func directBitmapRowsFromBitmapGroupAggregates(groups []BitmapGroupAggregateReadGroup) []directBitmapGroupedAggregateRow {
	rows := make([]directBitmapGroupedAggregateRow, 0, len(groups))
	for _, group := range groups {
		rows = append(rows, directBitmapGroupedAggregateRow{
			Key:    group.Key,
			Groups: append([]qsbridge.ResultCell(nil), group.Values...),
			Aggs:   append([]qsbridge.ResultCell(nil), group.Aggs...),
		})
	}
	return rows
}

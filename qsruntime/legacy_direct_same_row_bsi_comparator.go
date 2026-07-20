package qsruntime

import (
	"context"
	"strconv"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/source"
)

// LegacyDirectSharedSameRowBSIComparator compares same-row BSI fields through
// the shared BitmapIndex node contract and returns only matching rownums.
type LegacyDirectSharedSameRowBSIComparator struct {
	Source     *source.QuantaSource
	TableCache *core.TableCacheStruct
}

// CompareSameRowBSI asks the node layer to apply a row-local BSI comparison.
func (c LegacyDirectSharedSameRowBSIComparator) CompareSameRowBSI(ctx context.Context, request NativeSameRowBSICompareRequest) (NativeSameRowBSICompareResult, qsbridge.DiagnosticSet, error) {
	if c.Source == nil {
		return NativeSameRowBSICompareResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-direct same-row BSI comparator has no source"),
		}, nil
	}
	if err := ctx.Err(); err != nil {
		return NativeSameRowBSICompareResult{}, nil, err
	}
	fromTime, toTime := nativeProjectionWindowNanos(c.TableCache, NativeProjectionBSIReadRequest{
		Index:           request.Index,
		PhysicalField:   request.LeftField,
		FromEpochMillis: request.FromEpochMillis,
		ToEpochMillis:   request.ToEpochMillis,
	})
	foundSet := legacyDirectRelationshipBitmap(request.Rownums)
	executionRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{
		Index:     request.Index,
		Field:     request.LeftField,
		Operation: qsbridge.QuantaOperationIntersect,
		NullCheck: true,
		Negate:    true,
	}}})
	provider := LegacyQuantaSourceSessionProvider{Source: c.Source}
	session, diagnostics, err := provider.BorrowDirectSession(ctx, executionRequest)
	if err != nil || diagnostics.BlocksNative() {
		return NativeSameRowBSICompareResult{}, diagnostics, err
	}
	if session == nil {
		return NativeSameRowBSICompareResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-direct same-row BSI comparator received nil session"),
		}, nil
	}
	defer session.Release(ctx)
	legacySession, ok := session.(LegacyQuantaSessionHandle)
	if !ok || legacySession.Session == nil || legacySession.Session.BitIndex == nil {
		return NativeSameRowBSICompareResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-direct same-row BSI comparator has no bitmap index"),
		}, nil
	}
	start := time.Now()
	matches, stats, err := legacySession.Session.BitIndex.CompareBSIFieldsWithStats(
		request.Index,
		request.LeftField,
		request.RightField,
		fromTime,
		toTime,
		foundSet,
		request.Operation,
		request.Invert,
	)
	elapsed := time.Since(start)
	if err != nil {
		return NativeSameRowBSICompareResult{}, nil, err
	}
	rownums := make([]qsbridge.QuantaRownum, 0, len(request.Rownums))
	for _, rownum := range request.Rownums {
		if err := ctx.Err(); err != nil {
			return NativeSameRowBSICompareResult{}, nil, err
		}
		if matches != nil && matches.Contains(uint64(rownum)) {
			rownums = append(rownums, rownum)
		}
	}
	return NativeSameRowBSICompareResult{
		Rownums: rownums,
		Probes: append([]ExecutionProbe{
			{
				Section: "same_row_comparison",
				Name:    request.ProbePrefix + "shared_compare_transport",
				Value:   "grpc_nodes",
				Detail:  request.Index,
			},
			{
				Section: "same_row_comparison",
				Name:    request.ProbePrefix + "shared_compare_elapsed",
				Value:   elapsed.String(),
				Detail:  request.Index,
			},
			{
				Section: "same_row_comparison",
				Name:    request.ProbePrefix + "shared_compare_input_rows",
				Value:   strconv.Itoa(len(request.Rownums)),
				Detail:  request.Index,
			},
			{
				Section: "same_row_comparison",
				Name:    request.ProbePrefix + "shared_compare_output_rows",
				Value:   strconv.Itoa(len(rownums)),
				Detail:  request.Index,
			},
		}, sameRowSharedCompareStatsProbes(request, sameRowSharedCompareProbeStats{
			Nodes:          stats.Nodes,
			CompareElapsed: stats.CompareElapsed,
			OutputRows:     stats.OutputRows,
			Left: sameRowSharedCompareProjectionProbeStats{
				ShardsVisited:  stats.Left.ShardsVisited,
				ShardsInWindow: stats.Left.ShardsInWindow,
				ShardsLocal:    stats.Left.ShardsLocal,
				ShardsRetained: stats.Left.ShardsRetained,
				RetainedRows:   stats.Left.RetainedRows,
				RetainElapsed:  stats.Left.RetainElapsed,
				MergeElapsed:   stats.Left.MergeElapsed,
			},
			Right: sameRowSharedCompareProjectionProbeStats{
				ShardsVisited:  stats.Right.ShardsVisited,
				ShardsInWindow: stats.Right.ShardsInWindow,
				ShardsLocal:    stats.Right.ShardsLocal,
				ShardsRetained: stats.Right.ShardsRetained,
				RetainedRows:   stats.Right.RetainedRows,
				RetainElapsed:  stats.Right.RetainElapsed,
				MergeElapsed:   stats.Right.MergeElapsed,
			},
		})...),
	}, nil, nil
}

type sameRowSharedCompareProbeStats struct {
	Nodes          uint64
	Left           sameRowSharedCompareProjectionProbeStats
	Right          sameRowSharedCompareProjectionProbeStats
	CompareElapsed time.Duration
	OutputRows     uint64
}

type sameRowSharedCompareProjectionProbeStats struct {
	ShardsVisited  uint64
	ShardsInWindow uint64
	ShardsLocal    uint64
	ShardsRetained uint64
	RetainedRows   uint64
	RetainElapsed  time.Duration
	MergeElapsed   time.Duration
}

func sameRowSharedCompareStatsProbes(request NativeSameRowBSICompareRequest, stats sameRowSharedCompareProbeStats) []ExecutionProbe {
	prefix := request.ProbePrefix + "shared_compare_"
	probes := []ExecutionProbe{
		{Section: "same_row_comparison", Name: prefix + "stats_available", Value: strconv.FormatBool(stats.Nodes > 0), Detail: request.Index},
	}
	if stats.Nodes == 0 {
		return probes
	}
	probes = append(probes,
		ExecutionProbe{Section: "same_row_comparison", Name: prefix + "nodes", Value: strconv.FormatUint(stats.Nodes, 10), Detail: request.Index},
		ExecutionProbe{Section: "same_row_comparison", Name: prefix + "node_compare_elapsed", Value: stats.CompareElapsed.String(), Detail: request.Index},
		ExecutionProbe{Section: "same_row_comparison", Name: prefix + "aggregate_output_rows", Value: strconv.FormatUint(stats.OutputRows, 10), Detail: request.Index},
	)
	probes = append(probes, sameRowSharedCompareProjectionStatsProbes(prefix+"left_", request.Index, stats.Left)...)
	probes = append(probes, sameRowSharedCompareProjectionStatsProbes(prefix+"right_", request.Index, stats.Right)...)
	return probes
}

func sameRowSharedCompareProjectionStatsProbes(prefix string, detail string, stats sameRowSharedCompareProjectionProbeStats) []ExecutionProbe {
	return []ExecutionProbe{
		{Section: "same_row_comparison", Name: prefix + "shards_visited", Value: strconv.FormatUint(stats.ShardsVisited, 10), Detail: detail},
		{Section: "same_row_comparison", Name: prefix + "shards_in_window", Value: strconv.FormatUint(stats.ShardsInWindow, 10), Detail: detail},
		{Section: "same_row_comparison", Name: prefix + "shards_local", Value: strconv.FormatUint(stats.ShardsLocal, 10), Detail: detail},
		{Section: "same_row_comparison", Name: prefix + "shards_retained", Value: strconv.FormatUint(stats.ShardsRetained, 10), Detail: detail},
		{Section: "same_row_comparison", Name: prefix + "retained_rows", Value: strconv.FormatUint(stats.RetainedRows, 10), Detail: detail},
		{Section: "same_row_comparison", Name: prefix + "retain_elapsed", Value: stats.RetainElapsed.String(), Detail: detail},
		{Section: "same_row_comparison", Name: prefix + "merge_elapsed", Value: stats.MergeElapsed.String(), Detail: detail},
	}
}

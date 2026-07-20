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
	matches, err := legacySession.Session.BitIndex.CompareBSIFields(
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
		Probes: []ExecutionProbe{
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
		},
	}, nil, nil
}

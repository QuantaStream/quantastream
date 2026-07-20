package qsruntime

import (
	"context"
	"fmt"
	"strconv"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// SameRowComparisonKernel is the qsruntime-facing same-row comparison contract.
type SameRowComparisonKernel = qsbridge.SameRowComparisonKernel

// SameRowComparisonRequest is the runtime-neutral same-row comparison request.
type SameRowComparisonRequest = qsbridge.SameRowComparisonRequest

// SameRowComparisonResult is the runtime-neutral same-row comparison result.
type SameRowComparisonResult = qsbridge.SameRowComparisonResult

// NativeSameRowBSICompareRequest asks a storage-local primitive to compare two
// BSI fields without returning the compared vectors to the executor.
type NativeSameRowBSICompareRequest struct {
	Index           string
	ProbePrefix     string
	LeftField       string
	RightField      string
	Rownums         []qsbridge.QuantaRownum
	Operation       roaring64.Operation
	Invert          bool
	FromEpochMillis int64
	ToEpochMillis   int64
}

// NativeSameRowBSICompareResult returns rownums that passed a storage-local BSI comparison.
type NativeSameRowBSICompareResult struct {
	Rownums []qsbridge.QuantaRownum
	Probes  []ExecutionProbe
}

// NativeSameRowBSIComparator compares same-row BSI fields at the storage boundary.
type NativeSameRowBSIComparator interface {
	CompareSameRowBSI(context.Context, NativeSameRowBSICompareRequest) (NativeSameRowBSICompareResult, qsbridge.DiagnosticSet, error)
}

// NativeSameRowBSIComparatorFunc adapts a function to NativeSameRowBSIComparator.
type NativeSameRowBSIComparatorFunc func(context.Context, NativeSameRowBSICompareRequest) (NativeSameRowBSICompareResult, qsbridge.DiagnosticSet, error)

// CompareSameRowBSI calls f(ctx, request).
func (f NativeSameRowBSIComparatorFunc) CompareSameRowBSI(ctx context.Context, request NativeSameRowBSICompareRequest) (NativeSameRowBSICompareResult, qsbridge.DiagnosticSet, error) {
	return f(ctx, request)
}

// SameRowComparisonKernelFunc adapts a function to SameRowComparisonKernel.
type SameRowComparisonKernelFunc func(context.Context, qsbridge.SameRowComparisonRequest) (qsbridge.SameRowComparisonResult, error)

// CompareSameRowFields calls f(ctx, request).
func (f SameRowComparisonKernelFunc) CompareSameRowFields(ctx context.Context, request qsbridge.SameRowComparisonRequest) (qsbridge.SameRowComparisonResult, error) {
	return f(ctx, request)
}

// UnsupportedSameRowComparisonKernel preserves the explicit same-row boundary.
type UnsupportedSameRowComparisonKernel struct{}

// CompareSameRowFields reports that no native same-row comparison kernel is wired.
func (UnsupportedSameRowComparisonKernel) CompareSameRowFields(_ context.Context, request qsbridge.SameRowComparisonRequest) (qsbridge.SameRowComparisonResult, error) {
	return qsbridge.SameRowComparisonResult{
		ID: request.ID,
		Domain: qsbridge.RownumDomainSet{
			Domain: request.Domain.Domain,
		},
		Probes: []qsbridge.ProjectionProbe{
			sameRowComparisonProbe(request, "input_count", strconv.Itoa(request.CandidateCount()), ""),
			sameRowComparisonProbe(request, "kernel", "unsupported", ""),
		},
		Diagnostics: qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedSQL,
				qsbridge.PhaseExecute,
				fmt.Sprintf("same-row comparison kernel is not wired for kind %q", request.Kind),
			),
		},
	}, nil
}

func sameRowComparisonProbe(request qsbridge.SameRowComparisonRequest, name string, value string, detail string) qsbridge.ProjectionProbe {
	return qsbridge.ProjectionProbe{
		Section: "same_row_comparison",
		Name:    request.ProbePrefix + name,
		Value:   value,
		Detail:  detail,
	}
}

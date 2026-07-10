package qsruntime

import (
	"context"
	"fmt"
	"strconv"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// SameRowComparisonKernel is the qsruntime-facing same-row comparison contract.
type SameRowComparisonKernel = qsbridge.SameRowComparisonKernel

// SameRowComparisonRequest is the runtime-neutral same-row comparison request.
type SameRowComparisonRequest = qsbridge.SameRowComparisonRequest

// SameRowComparisonResult is the runtime-neutral same-row comparison result.
type SameRowComparisonResult = qsbridge.SameRowComparisonResult

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

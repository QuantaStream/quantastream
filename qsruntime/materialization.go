package qsruntime

import (
	"context"
	"strconv"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// ProjectionMaterializer is qsbridge's native projection/materialization contract.
type ProjectionMaterializer = qsbridge.ProjectionMaterializer

// ProjectionMaterializerWithProbes is qsbridge's optional materialization instrumentation contract.
type ProjectionMaterializerWithProbes = qsbridge.ProjectionMaterializerWithProbes

// ProjectionMaterializerFunc adapts a function to ProjectionMaterializer.
type ProjectionMaterializerFunc = qsbridge.ProjectionMaterializerFunc

// ProjectionMaterializationKernelRequest aliases qsbridge's grouped materialization request.
type ProjectionMaterializationKernelRequest = qsbridge.ProjectionMaterializationKernelRequest

// ProjectionMaterializationResult aliases one qsbridge materialized batch response.
type ProjectionMaterializationResult = qsbridge.ProjectionMaterializationResult

// ProjectionMaterializationKernelResult aliases qsbridge's grouped materialization result.
type ProjectionMaterializationKernelResult = qsbridge.ProjectionMaterializationKernelResult

// ProjectionMaterializationKernel aliases qsbridge's native materialization-kernel boundary.
type ProjectionMaterializationKernel = qsbridge.ProjectionMaterializationKernel

// CandidateSetFromBitmapResult aliases qsbridge's bitmap-result candidate builder during the package split.
var CandidateSetFromBitmapResult = qsbridge.CandidateSetFromBitmapResult

// ProjectionMaterializationKernelAdapter delegates grouped materialization to a configured kernel.
type ProjectionMaterializationKernelAdapter struct {
	Kernel ProjectionMaterializationKernel
}

// MaterializeProjectionBatches delegates to the configured kernel or the unsupported boundary.
func (a ProjectionMaterializationKernelAdapter) MaterializeProjectionBatches(ctx context.Context, request ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error) {
	kernel := a.Kernel
	if kernel == nil {
		kernel = UnsupportedProjectionMaterializationKernel{}
	}
	return kernel.MaterializeProjectionBatches(ctx, request)
}

// ExecuteProjectionMaterializationKernel dispatches one grouped materialization request.
func ExecuteProjectionMaterializationKernel(ctx context.Context, kernel ProjectionMaterializationKernel, request ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error) {
	return ProjectionMaterializationKernelAdapter{Kernel: kernel}.MaterializeProjectionBatches(ctx, request)
}

// ProjectionMaterializerKernelAdapter adapts the older one-request materializer contract to the grouped kernel contract.
type ProjectionMaterializerKernelAdapter struct {
	Materializer ProjectionMaterializer
}

// MaterializeProjectionBatches executes each grouped request through the configured materializer.
func (a ProjectionMaterializerKernelAdapter) MaterializeProjectionBatches(ctx context.Context, request ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error) {
	if a.Materializer == nil {
		return UnsupportedProjectionMaterializationKernel{}.MaterializeProjectionBatches(ctx, request)
	}
	result := ProjectionMaterializationKernelResult{
		ID: request.ID,
		Probes: []qsbridge.ProjectionProbe{{
			Section: "projection_materialization",
			Name:    request.ProbePrefix + "request_count",
			Value:   strconv.Itoa(request.RequestCount()),
		}},
	}
	for _, materializationRequest := range request.Requests {
		rowSet, diagnostics, probes, err := materializeWithProbes(ctx, a.Materializer, materializationRequest)
		item := ProjectionMaterializationResult{
			ID:          materializationRequest.DependencyID,
			Request:     materializationRequest,
			RowSet:      rowSet,
			Probes:      probes,
			Diagnostics: diagnostics,
		}
		result.Results = append(result.Results, item)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func materializeWithProbes(ctx context.Context, materializer ProjectionMaterializer, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, []ExecutionProbe, error) {
	if instrumented, ok := materializer.(ProjectionMaterializerWithProbes); ok {
		return instrumented.MaterializeWithProbes(ctx, request)
	}
	rowSet, diagnostics, err := materializer.Materialize(ctx, request)
	return rowSet, diagnostics, nil, err
}

// UnsupportedProjectionMaterializationKernel preserves the current explicit materialization boundary.
type UnsupportedProjectionMaterializationKernel struct{}

// MaterializeProjectionBatches reports that native materialization is not wired yet.
func (UnsupportedProjectionMaterializationKernel) MaterializeProjectionBatches(_ context.Context, request ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error) {
	return ProjectionMaterializationKernelResult{
		ID:          request.ID,
		Diagnostics: unsupportedProjectionMaterializationDiagnostics(request),
	}, nil
}

func unsupportedProjectionMaterializationDiagnostics(request ProjectionMaterializationKernelRequest) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			"projection materialization kernel is not wired yet: requests="+strconv.Itoa(len(request.Requests)),
		),
	}
}

// MaterializationRequestFromExecution builds a materialization request from result candidates.
func MaterializationRequestFromExecution(request ExecutionRequest, result BitmapQueryResult) (qsbridge.QuantaMaterializationRequest, qsbridge.DiagnosticSet) {
	if request.Materialization.ProjectionCount() > 0 || request.Materialization.CandidateCount() > 0 {
		materialization := request.Materialization
		if len(materialization.Rownums) == 0 {
			materialization.Rownums = append([]qsbridge.QuantaRownum(nil), result.Rownums...)
		}
		if rootIndex, ok := request.RootIndex(); ok {
			index := materialization.Index
			if index == "" {
				index = rootIndex
			}
			if strings.EqualFold(index, rootIndex) {
				materialization.ProjectionFields = materializationRootProjectionFields(rootIndex, materialization.ProjectionFields)
			}
		}
		return materialization, nil
	}
	rootIndex, ok := request.RootIndex()
	if !ok {
		return qsbridge.QuantaMaterializationRequest{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInvalidExecutionOption,
				qsbridge.PhaseExecute,
				"cannot materialize projection without a root index",
			),
		}
	}
	candidates := qsbridge.CandidateSetFromBitmapResult(rootIndex, result)
	return candidates.MaterializationRequest(materializationRootProjectionFields(rootIndex, request.Query.ProjectionFields)), nil
}

func materializationRootProjectionFields(rootIndex string, fields []qsbridge.QuantaProjectionField) []qsbridge.QuantaProjectionField {
	rootFields := make([]qsbridge.QuantaProjectionField, 0, len(fields))
	for _, field := range fields {
		if field.Index == "" || strings.EqualFold(field.Index, rootIndex) {
			rootFields = append(rootFields, field)
		}
	}
	return rootFields
}

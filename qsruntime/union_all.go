package qsruntime

import (
	"context"
	"fmt"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func (r SQLRuntime) unionAllRuntimeResult(ctx context.Context, request qsbridge.ExecutionRequest) (ExecutionResult, qsbridge.DiagnosticSet, error, bool) {
	query := request.Bound.Prepared.Query
	if query.Kind != qsbridge.QueryKindUnionAll {
		return ExecutionResult{}, nil, nil, false
	}
	if len(query.UnionAll) < 2 {
		diagnostics := qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticParserBoundary, qsbridge.PhaseExecute, "UNION ALL requires at least two SELECT branches"),
		}
		return ExecutionResult{Diagnostics: diagnostics}, diagnostics, nil, true
	}

	var combined qsbridge.QuantaProjectedRowSet
	var diagnostics qsbridge.DiagnosticSet
	for branchIndex, branch := range query.UnionAll {
		branchRequest := unionAllBranchExecutionRequest(request, branch)
		branchResult, branchDiagnostics, err := r.executeUnionAllBranch(ctx, branchRequest)
		if err != nil || branchDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, branchDiagnostics...)
			return ExecutionResult{Diagnostics: diagnostics}, diagnostics, err, true
		}
		next, appendDiagnostics := appendUnionAllRowSet(combined, branchResult.RowSet, branchIndex)
		if appendDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, appendDiagnostics...)
			return ExecutionResult{Diagnostics: diagnostics}, diagnostics, nil, true
		}
		combined = next
	}
	if validation := combined.ValidateShape(); validation.BlocksNative() {
		diagnostics = append(diagnostics, validation...)
		return ExecutionResult{Diagnostics: diagnostics}, diagnostics, nil, true
	}
	return ExecutionResult{
		RowSet: combined,
		Count:  uint64(combined.CandidateCount()),
	}, diagnostics, nil, true
}

func unionAllBranchExecutionRequest(request qsbridge.ExecutionRequest, branch qsbridge.QueryIR) qsbridge.ExecutionRequest {
	branchPrepared := request.Bound.Prepared
	branchPrepared.Kind = branch.Kind
	branchPrepared.Query = branch
	branchPrepared.Logical = qsbridge.BuildLogicalPlan(branch)
	branchPrepared.Physical = qsbridge.BuildPhysicalPlan(branchPrepared.Logical, branchPrepared.Scope)
	branchPrepared.Inspection = qsbridge.InspectOptimizedQuery(branch, qsbridge.OptimizationTrace{}, branchPrepared.Scope)
	branchPrepared.Access = branch.RequiredAccess()
	branchPrepared.Parameters = branch.RequiredParameters()
	branchPrepared.ResultColumns = branch.ResultColumns()
	branchPrepared.Result = branch.Result
	branchPrepared.Diagnostics = branch.Diagnostics()
	branchPrepared.Supported = branch.Supported() && branchPrepared.Inspection.Supported && !branchPrepared.Diagnostics.BlocksNative()

	branchRequest := request
	branchRequest.Bound.Prepared = branchPrepared
	branchRequest.Bound.Diagnostics = branchPrepared.Diagnostics
	branchRequest.Bound.Supported = branchPrepared.Supported
	branchRequest.Diagnostics = branchPrepared.Diagnostics
	branchRequest.Result = branchPrepared.Result
	branchRequest.ResultColumns = append([]qsbridge.ResultColumn(nil), branchPrepared.ResultColumns...)
	branchRequest.Access = append([]qsbridge.AccessRequirement(nil), branchPrepared.Access...)
	branchRequest.Supported = branchRequest.Supported && branchPrepared.Supported
	return branchRequest
}

func (r SQLRuntime) executeUnionAllBranch(ctx context.Context, request qsbridge.ExecutionRequest) (ExecutionResult, qsbridge.DiagnosticSet, error) {
	var diagnostics qsbridge.DiagnosticSet
	if runtimeResult, branchDiagnostics, ok := r.constantProjectionExecutionResult(request); ok {
		diagnostics = append(diagnostics, branchDiagnostics...)
		diagnostics = append(diagnostics, runtimeResult.Diagnostics...)
		return runtimeResult, diagnostics, nil
	}
	if runtimeResult, branchDiagnostics, ok := r.informationSchemaExecutionResult(request); ok {
		diagnostics = append(diagnostics, branchDiagnostics...)
		diagnostics = append(diagnostics, runtimeResult.Diagnostics...)
		return runtimeResult, diagnostics, nil
	}
	if runtimeResult, branchDiagnostics, ok := r.inlineRowSetRuntimeResult(request); ok {
		diagnostics = append(diagnostics, branchDiagnostics...)
		diagnostics = append(diagnostics, runtimeResult.Diagnostics...)
		return runtimeResult, diagnostics, nil
	}
	if runtimeResult, branchDiagnostics, ok := r.selectTemporaryTableRuntimeResult(request); ok {
		diagnostics = append(diagnostics, branchDiagnostics...)
		diagnostics = append(diagnostics, runtimeResult.Diagnostics...)
		return runtimeResult, diagnostics, nil
	}

	intermediate, lowerDiagnostics := r.Lowerer.LowerExecutionRequest(request)
	diagnostics = append(diagnostics, lowerDiagnostics...)
	if diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, diagnostics, nil
	}
	runtimeResult, err := r.ExecutePrepared(ctx, NewSQLExecutionRequest(intermediate, request))
	diagnostics = append(diagnostics, runtimeResult.Diagnostics...)
	return runtimeResult, diagnostics, err
}

func appendUnionAllRowSet(left, right qsbridge.QuantaProjectedRowSet, branchIndex int) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet) {
	if right.CandidateCount() == 0 && len(right.ProjectionVectors) == 0 {
		return cloneUnionAllRowSet(left), nil
	}
	if left.CandidateCount() == 0 && len(left.ProjectionVectors) == 0 {
		return cloneUnionAllRowSet(right), nil
	}
	if len(left.ProjectionVectors) != len(right.ProjectionVectors) {
		return qsbridge.QuantaProjectedRowSet{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, fmt.Sprintf("UNION ALL branch %d returned %d projection vectors, expected %d", branchIndex+1, len(right.ProjectionVectors), len(left.ProjectionVectors))),
		}
	}
	combined := cloneUnionAllRowSet(left)
	combined.Rownums = append(combined.Rownums, right.Rownums...)
	for i, vector := range right.ProjectionVectors {
		combined.ProjectionVectors[i].Values = append(combined.ProjectionVectors[i].Values, vector.Values...)
	}
	return combined, nil
}

func cloneUnionAllRowSet(rowSet qsbridge.QuantaProjectedRowSet) qsbridge.QuantaProjectedRowSet {
	cloned := rowSet
	cloned.Rownums = append([]qsbridge.QuantaRownum(nil), rowSet.Rownums...)
	cloned.ProjectionVectors = make([]qsbridge.QuantaProjectionVector, 0, len(rowSet.ProjectionVectors))
	for _, vector := range rowSet.ProjectionVectors {
		cloned.ProjectionVectors = append(cloned.ProjectionVectors, qsbridge.QuantaProjectionVector{
			Field:  vector.Field,
			Values: append([]qsbridge.ResultCell(nil), vector.Values...),
		})
	}
	return cloned
}

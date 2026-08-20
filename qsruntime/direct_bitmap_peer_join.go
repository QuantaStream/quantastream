package qsruntime

import (
	"context"
	"fmt"

	"github.com/QuantaStream/quantastream/qsbridge"
)

const (
	directBitmapPeerJoinMaxInputRows   = 10000
	directBitmapPeerJoinMaxCompareRows = 1000000
	directBitmapPeerJoinMaxOutputRows  = 100000
)

func (r DirectBitmapRuntime) directBitmapPeerJoinResult(ctx context.Context, request ExecutionRequest) (ExecutionResult, bool, error) {
	if len(request.Joins) == 0 {
		return ExecutionResult{}, false, nil
	}
	if diagnostics := directBitmapPeerJoinShapeDiagnostics(request); diagnostics.BlocksNative() {
		return ExecutionResult{Diagnostics: diagnostics}, true, nil
	} else if diagnostics != nil {
		return ExecutionResult{}, false, nil
	}
	if r.Sessions == nil {
		return ExecutionResult{Diagnostics: directBitmapAggregateDiagnostics("direct bitmap peer join fallback has no session provider")}, true, nil
	}
	materializationKernel := r.projectionMaterializationKernel()
	if materializationKernel == nil {
		return ExecutionResult{Diagnostics: directBitmapAggregateDiagnostics("direct bitmap peer join fallback has no materialization kernel")}, true, nil
	}

	refs := directBitmapPeerJoinRequiredFieldRefs(request)
	leftRows, leftProbes, diagnostics, err := r.directBitmapPeerJoinSourceRowSet(ctx, request, request.Sources[0], refs, materializationKernel)
	result := ExecutionResult{Probes: append([]ExecutionProbe(nil), leftProbes...)}
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, true, err
	}
	rightRows, rightProbes, diagnostics, err := r.directBitmapPeerJoinSourceRowSet(ctx, request, request.Sources[1], refs, materializationKernel)
	result.Probes = append(result.Probes, rightProbes...)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil || result.Diagnostics.BlocksNative() {
		return result, true, err
	}

	joined, diagnostics := directBitmapPeerJoinMaterializedRows(request, leftRows, rightRows)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, true, nil
	}
	return directBitmapFinishMaterializedRowSet(request, joined, result), true, nil
}

func directBitmapPeerJoinShapeDiagnostics(request ExecutionRequest) qsbridge.DiagnosticSet {
	if len(request.Joins) == 0 {
		return nil
	}
	if len(request.Sources) != 2 || len(request.Joins) != 1 {
		return directBitmapPeerJoinNotCandidate()
	}
	if _, ok := directBitmapSelfJoinCountCandidate(request); ok {
		return directBitmapPeerJoinNotCandidate()
	}
	if len(request.Memberships) > 0 {
		return directBitmapPeerJoinNotCandidate()
	}
	if len(request.Query.Fragments) > 0 || len(request.Query.Seeds) > 0 || !request.Query.Filter.Empty() || !request.NativePredicates.Empty() {
		return directBitmapPeerJoinNotCandidate()
	}
	join := request.Joins[0]
	if join.Direction != qsbridge.JoinPeerEquality {
		return directBitmapPeerJoinNotCandidate()
	}
	if join.Kind != "" && join.Kind != qsbridge.JoinKindInner {
		return directBitmapPeerJoinNotCandidate()
	}
	switch join.Operator {
	case "", qsbridge.BinaryOpEqual, qsbridge.BinaryOpNotEqual,
		qsbridge.BinaryOpLess, qsbridge.BinaryOpLessEqual,
		qsbridge.BinaryOpGreater, qsbridge.BinaryOpGreaterEqual:
	default:
		return directBitmapAggregateDiagnostics(fmt.Sprintf("direct bitmap peer join fallback does not support join operator %s", join.Operator))
	}
	return nil
}

func directBitmapPeerJoinNotCandidate() qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{}
}

func (r DirectBitmapRuntime) directBitmapPeerJoinSourceRowSet(ctx context.Context, request ExecutionRequest, source qsbridge.TableInstance, refs []qsbridge.FieldRef, kernel ProjectionMaterializationKernel) (qsbridge.QuantaProjectedRowSet, []ExecutionProbe, qsbridge.DiagnosticSet, error) {
	fields := directBitmapPeerJoinMaterializationFields(request, source, refs)
	if len(fields) == 0 {
		return qsbridge.QuantaProjectedRowSet{}, nil, directBitmapAggregateDiagnostics("direct bitmap peer join fallback has no fields to materialize for " + source.DisplayName()), nil
	}
	seedField := fields[0].Field
	tableRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Seeds: []qsbridge.QuantaSeed{{
			Index: source.Table,
			Role:  source.ID,
			Field: seedField,
			Kind:  qsbridge.QuantaSeedTableExistence,
		}},
	})
	tableRequest.SourceIndexes = []string{source.Table}
	tableRequest.Sources = []qsbridge.TableInstance{source}
	tableRequest.Route = request.Route
	session, diagnostics, err := r.Sessions.BorrowDirectSession(ctx, tableRequest)
	if err != nil || diagnostics.BlocksNative() {
		return qsbridge.QuantaProjectedRowSet{}, nil, diagnostics, err
	}
	if session == nil {
		return qsbridge.QuantaProjectedRowSet{}, nil, directBitmapAggregateDiagnostics("direct bitmap peer join fallback received nil session for " + source.DisplayName()), nil
	}
	bitmapResult, queryDiagnostics, queryErr := session.QueryBitmap(ctx, tableRequest)
	releaseDiagnostics := session.Release(ctx)
	diagnostics = append(diagnostics, queryDiagnostics...)
	diagnostics = append(diagnostics, releaseDiagnostics...)
	if queryErr != nil || diagnostics.BlocksNative() {
		return qsbridge.QuantaProjectedRowSet{}, nil, diagnostics, queryErr
	}
	if len(bitmapResult.Rownums) > directBitmapPeerJoinMaxInputRows {
		return qsbridge.QuantaProjectedRowSet{}, nil, directBitmapAggregateDiagnostics(fmt.Sprintf("direct bitmap peer join fallback input %s has %d rows, max %d", source.DisplayName(), len(bitmapResult.Rownums), directBitmapPeerJoinMaxInputRows)), nil
	}
	materialization := qsbridge.CandidateSetFromBitmapResult(source.Table, bitmapResult).MaterializationRequest(fields)
	rowSet, materializationDiagnostics, probes, materializationErr := directBitmapMaterializeWithKernel(ctx, kernel, materialization)
	diagnostics = append(diagnostics, materializationDiagnostics...)
	return rowSet, probes, diagnostics, materializationErr
}

func directBitmapPeerJoinMaterializationFields(request ExecutionRequest, source qsbridge.TableInstance, refs []qsbridge.FieldRef) []qsbridge.QuantaProjectionField {
	sourceRefs := make([]qsbridge.FieldRef, 0, len(refs))
	for _, ref := range refs {
		if directBitmapPeerJoinFieldBelongsToSource(ref, source) {
			sourceRefs = append(sourceRefs, ref)
		}
	}
	return materializationFieldsFromRequiredFields(source.Table, sourceRefs, materializationVisibleFieldKeys(request.Projection))
}

func directBitmapPeerJoinFieldBelongsToSource(ref qsbridge.FieldRef, source qsbridge.TableInstance) bool {
	if ref.Table.ID != "" && source.ID != "" {
		return ref.Table.ID == source.ID
	}
	if ref.Table.Alias != "" && source.Alias != "" {
		return ref.Table.Alias == source.Alias
	}
	return ref.Table.Table == source.Table
}

func directBitmapPeerJoinRequiredFieldRefs(request ExecutionRequest) []qsbridge.FieldRef {
	refs := materializationFieldRefsFromExecutionRequest(request)
	for _, join := range request.Joins {
		refs = append(refs, join.Left, join.Right)
		for _, predicate := range join.On {
			refs = append(refs, qsbridge.FieldRefs(predicate.Expr)...)
		}
	}
	return refs
}

func directBitmapPeerJoinMaterializedRows(request ExecutionRequest, leftRows qsbridge.QuantaProjectedRowSet, rightRows qsbridge.QuantaProjectedRowSet) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet) {
	join := request.Joins[0]
	if leftRows.CandidateCount()*rightRows.CandidateCount() > directBitmapPeerJoinMaxCompareRows {
		return qsbridge.QuantaProjectedRowSet{}, directBitmapAggregateDiagnostics(fmt.Sprintf("direct bitmap peer join fallback would compare %d rows, max %d", leftRows.CandidateCount()*rightRows.CandidateCount(), directBitmapPeerJoinMaxCompareRows))
	}
	joined := qsbridge.QuantaProjectedRowSet{
		Index:             leftRows.Index,
		Rownums:           make([]qsbridge.QuantaRownum, 0),
		ProjectionVectors: directBitmapPeerJoinProjectionVectors(leftRows, rightRows),
	}
	for leftIndex := 0; leftIndex < leftRows.CandidateCount(); leftIndex++ {
		for rightIndex := 0; rightIndex < rightRows.CandidateCount(); rightIndex++ {
			matched, diagnostics := directBitmapPeerJoinRowsMatch(join, leftRows, rightRows, leftIndex, rightIndex)
			if diagnostics.BlocksNative() {
				return qsbridge.QuantaProjectedRowSet{}, diagnostics
			}
			if !matched {
				continue
			}
			if len(joined.Rownums) >= directBitmapPeerJoinMaxOutputRows {
				return qsbridge.QuantaProjectedRowSet{}, directBitmapAggregateDiagnostics(fmt.Sprintf("direct bitmap peer join fallback produced more than %d rows", directBitmapPeerJoinMaxOutputRows))
			}
			directBitmapPeerJoinAppendRow(&joined, leftRows, rightRows, leftIndex, rightIndex)
		}
	}
	return joined, joined.ValidateShape()
}

func directBitmapPeerJoinRowsMatch(join qsbridge.JoinEdge, leftRows qsbridge.QuantaProjectedRowSet, rightRows qsbridge.QuantaProjectedRowSet, leftIndex int, rightIndex int) (bool, qsbridge.DiagnosticSet) {
	leftCell, ok := directBitmapPeerJoinCell(join.Left, leftRows, rightRows, leftIndex, rightIndex)
	if !ok {
		return false, directBitmapAggregateDiagnostics("direct bitmap peer join fallback could not find left join field " + join.Left.QualifiedName())
	}
	rightCell, ok := directBitmapPeerJoinCell(join.Right, leftRows, rightRows, leftIndex, rightIndex)
	if !ok {
		return false, directBitmapAggregateDiagnostics("direct bitmap peer join fallback could not find right join field " + join.Right.QualifiedName())
	}
	if directBitmapNullCell(leftCell) || directBitmapNullCell(rightCell) {
		return false, nil
	}
	op := join.Operator
	if op == "" {
		op = qsbridge.BinaryOpEqual
	}
	return directBitmapResidualCompareCells(op, leftCell, rightCell), nil
}

func directBitmapPeerJoinCell(field qsbridge.FieldRef, leftRows qsbridge.QuantaProjectedRowSet, rightRows qsbridge.QuantaProjectedRowSet, leftIndex int, rightIndex int) (qsbridge.ResultCell, bool) {
	if values, ok := directBitmapProjectedValues(leftRows, field); ok && leftIndex < len(values) {
		return values[leftIndex], true
	}
	if values, ok := directBitmapProjectedValues(rightRows, field); ok && rightIndex < len(values) {
		return values[rightIndex], true
	}
	return qsbridge.ResultCell{}, false
}

func directBitmapPeerJoinProjectionVectors(leftRows qsbridge.QuantaProjectedRowSet, rightRows qsbridge.QuantaProjectedRowSet) []qsbridge.QuantaProjectionVector {
	vectors := make([]qsbridge.QuantaProjectionVector, 0, len(leftRows.ProjectionVectors)+len(rightRows.ProjectionVectors))
	for _, vector := range leftRows.ProjectionVectors {
		vector.Values = make([]qsbridge.ResultCell, 0)
		vectors = append(vectors, vector)
	}
	for _, vector := range rightRows.ProjectionVectors {
		vector.Values = make([]qsbridge.ResultCell, 0)
		vectors = append(vectors, vector)
	}
	return vectors
}

func directBitmapPeerJoinAppendRow(joined *qsbridge.QuantaProjectedRowSet, leftRows qsbridge.QuantaProjectedRowSet, rightRows qsbridge.QuantaProjectedRowSet, leftIndex int, rightIndex int) {
	joined.Rownums = append(joined.Rownums, qsbridge.QuantaRownum(len(joined.Rownums)+1))
	vectorIndex := 0
	for _, vector := range leftRows.ProjectionVectors {
		joined.ProjectionVectors[vectorIndex].Values = append(joined.ProjectionVectors[vectorIndex].Values, vector.Values[leftIndex])
		vectorIndex++
	}
	for _, vector := range rightRows.ProjectionVectors {
		joined.ProjectionVectors[vectorIndex].Values = append(joined.ProjectionVectors[vectorIndex].Values, vector.Values[rightIndex])
		vectorIndex++
	}
}

func directBitmapFinishMaterializedRowSet(request ExecutionRequest, rowSet qsbridge.QuantaProjectedRowSet, result ExecutionResult) ExecutionResult {
	var diagnostics qsbridge.DiagnosticSet
	rowSet, diagnostics = directBitmapFilterResidualScanPredicates(request, rowSet)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	if len(request.SQLAggregates) > 0 {
		if len(request.GroupBy) > 0 {
			return directBitmapMaterializedGroupedAggregateResult(request, rowSet, result)
		}
		return directBitmapMaterializedAggregateResult(request, rowSet, result)
	}
	rowSet, diagnostics = directBitmapOrderProjectedRows(request, rowSet)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	rowSet, diagnostics = directBitmapEvaluateProjectionRowSet(request, rowSet)
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}
	if request.Result.Distinct {
		rowSet = directBitmapDistinctProjectedRowSet(rowSet)
	}
	rowSet = directBitmapLimitProjectedRowSet(rowSet, request.Result.Offset, request.Result.Limit, request.Result.HasResultLimit())
	if len(request.Projection) == 0 {
		rowSet = directBitmapOrderVisibleProjectedRowSet(rowSet, request.ProjectionOrder)
	}
	result.RowSet = rowSet
	result.Count = uint64(rowSet.CandidateCount())
	return result
}

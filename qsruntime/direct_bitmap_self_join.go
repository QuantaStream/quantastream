package qsruntime

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func (r DirectBitmapRuntime) directBitmapSelfJoinCountAggregateResult(ctx context.Context, request ExecutionRequest, bitmapResult BitmapQueryResult, result ExecutionResult) (ExecutionResult, bool) {
	join, ok := directBitmapSelfJoinCountCandidate(request)
	if !ok {
		return result, false
	}
	if bitmapResult.Count == 0 {
		result.Count = 0
		return directBitmapCountAggregateResult(request, result), true
	}
	if len(bitmapResult.Rownums) == 0 {
		result.Diagnostics = append(result.Diagnostics, directBitmapAggregateDiagnostics("self-join count requires materialized root rownums")...)
		return result, true
	}
	materialization := qsbridge.QuantaMaterializationRequest{
		Index:            join.Left.Table.Table,
		Rownums:          append([]qsbridge.QuantaRownum(nil), bitmapResult.Rownums...),
		ProjectionFields: []qsbridge.QuantaProjectionField{directBitmapMembershipProjectionField(join.Left)},
		DependencyID:     "self_join_count_key",
	}
	start := time.Now()
	rowSet, diagnostics, probes, err := directBitmapMaterializeWithKernel(ctx, r.projectionMaterializationKernel(), materialization)
	elapsed := time.Since(start)
	result.Probes = append(result.Probes, probes...)
	keyProbe := ExecutionProbe{
		Section: "self_join_count",
		Name:    "phase_key_materialization_elapsed",
		Value:   elapsed.String(),
		Detail:  fmt.Sprintf("join=%s=%s rows=%d", join.Left.QualifiedName(), join.Right.QualifiedName(), len(bitmapResult.Rownums)),
	}
	result.Probes = append(result.Probes, keyProbe)
	recordExecutionProbes(ctx, probes)
	recordExecutionProbes(ctx, []ExecutionProbe{keyProbe})
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if err != nil {
		result.Diagnostics = append(result.Diagnostics, qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, err.Error()))
		return result, true
	}
	if result.Diagnostics.BlocksNative() {
		return result, true
	}
	values, found := directBitmapProjectedValues(rowSet, join.Left)
	if !found {
		result.Diagnostics = append(result.Diagnostics, directBitmapAggregateDiagnostics("self-join count materialization did not return join key "+directBitmapFieldRefDebug(join.Left))...)
		return result, true
	}
	if len(values) != len(bitmapResult.Rownums) {
		result.Diagnostics = append(result.Diagnostics, directBitmapAggregateDiagnostics(fmt.Sprintf("self-join count key values %d do not match candidates %d", len(values), len(bitmapResult.Rownums)))...)
		return result, true
	}
	var joined uint64
	counts := make(map[string]uint64)
	for _, value := range values {
		if directBitmapNullCell(value) {
			continue
		}
		counts[directBitmapGroupKey(value)]++
	}
	for _, count := range counts {
		joined += count * count
	}
	countProbe := ExecutionProbe{
		Section: "self_join_count",
		Name:    "matched_rows",
		Value:   strconv.FormatUint(joined, 10),
		Detail:  fmt.Sprintf("groups=%d", len(counts)),
	}
	result.Probes = append(result.Probes, countProbe)
	recordExecutionProbes(ctx, []ExecutionProbe{countProbe})
	result.Count = joined
	return directBitmapCountAggregateResult(request, result), true
}

func directBitmapSelfJoinCountCandidate(request ExecutionRequest) (qsbridge.JoinEdge, bool) {
	if len(request.Joins) != 1 ||
		len(request.Memberships) != 0 ||
		len(request.GroupBy) != 0 ||
		len(request.Having) != 0 ||
		len(request.SQLAggregates) == 0 ||
		!directBitmapAllAggregatesUseBitmapCount(request.SQLAggregates) ||
		directBitmapHasResidualScanPredicates(request) ||
		!request.NativePredicates.Empty() {
		return qsbridge.JoinEdge{}, false
	}
	join := request.Joins[0]
	if join.Kind != qsbridge.JoinKindInner ||
		(join.Operator != "" && join.Operator != qsbridge.BinaryOpEqual) ||
		join.Direction != qsbridge.JoinPeerEquality ||
		!join.Supported() ||
		len(join.On) != 0 ||
		join.Encoding.Kind != qsbridge.RelationshipEncodingUnknown ||
		!directBitmapSameField(join.Left, join.Right) {
		return qsbridge.JoinEdge{}, false
	}
	leftRole := join.Left.Table.RefName()
	rightRole := join.Right.Table.RefName()
	if leftRole == "" || rightRole == "" || strings.EqualFold(leftRole, rightRole) {
		return qsbridge.JoinEdge{}, false
	}
	rootIndex, ok := request.RootIndex()
	if !ok || !strings.EqualFold(rootIndex, join.Left.Table.Table) {
		return qsbridge.JoinEdge{}, false
	}
	return join, true
}

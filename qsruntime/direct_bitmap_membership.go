package qsruntime

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func (r DirectBitmapRuntime) directBitmapApplyMemberships(ctx context.Context, request ExecutionRequest, result BitmapQueryResult) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	if len(request.Memberships) == 0 {
		return result, nil, nil
	}
	if len(request.Joins) > 0 {
		return result, directBitmapMembershipDiagnostics("direct bitmap runtime only supports membership filters for single-table execution in this slice"), nil
	}
	if r.Sessions == nil {
		return result, directBitmapMembershipDiagnostics("direct bitmap runtime has no session provider for membership filters"), nil
	}
	if r.projectionMaterializationKernel() == nil {
		return result, directBitmapMembershipDiagnostics("direct bitmap runtime has no materialization kernel for membership filters"), nil
	}
	rootIndex, ok := request.RootIndex()
	if !ok {
		return result, directBitmapMembershipDiagnostics("direct bitmap runtime cannot apply membership filters without a root index"), nil
	}
	filtered := result.Clone()
	for _, membership := range request.Memberships {
		if !strings.EqualFold(membership.Left.Table.Table, rootIndex) {
			return result, directBitmapMembershipDiagnostics("direct bitmap runtime only supports membership left side on the root table"), nil
		}
		filteredResult, diagnostics, err := r.directBitmapApplyMembership(ctx, request, filtered, membership)
		if err != nil || diagnostics.BlocksNative() {
			return result, diagnostics, err
		}
		filtered = filteredResult
	}
	return filtered, nil, nil
}

func (r DirectBitmapRuntime) directBitmapApplyMembership(ctx context.Context, request ExecutionRequest, result BitmapQueryResult, membership qsbridge.MembershipEdge) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	if directBitmapMembershipHasCorrelatedPredicates(membership) {
		return r.directBitmapApplyCorrelatedSiblingMembership(ctx, result, membership)
	}
	if filtered, handled, diagnostics, err := r.directBitmapApplyRelationshipMembership(ctx, result, membership); handled || err != nil || diagnostics.BlocksNative() {
		return filtered, diagnostics, err
	}
	rightValues, diagnostics, err := r.directBitmapMembershipRightValues(ctx, membership)
	if err != nil || diagnostics.BlocksNative() {
		return result, diagnostics, err
	}
	leftRowSet, diagnostics, err := r.directBitmapMembershipLeftValues(ctx, result, membership.Left)
	if err != nil || diagnostics.BlocksNative() {
		return result, diagnostics, err
	}
	leftValues, ok := directBitmapProjectedValues(leftRowSet, membership.Left)
	if !ok {
		return result, directBitmapMembershipDiagnostics("membership left field is not present in materialized row set"), nil
	}
	if len(leftValues) != len(leftRowSet.Rownums) {
		return result, directBitmapMembershipDiagnostics("membership left field value count does not match candidate row count"), nil
	}

	filtered := result.Clone()
	filtered.Rownums = filtered.Rownums[:0]
	for i, rownum := range leftRowSet.Rownums {
		_, matched := rightValues[directBitmapGroupKey(leftValues[i])]
		keep := matched
		if membership.Kind == qsbridge.MembershipAnti {
			keep = !matched
		}
		if keep {
			filtered.Rownums = append(filtered.Rownums, rownum)
		}
	}
	filtered.Count = uint64(len(filtered.Rownums))
	return filtered, nil, nil
}

func (r DirectBitmapRuntime) directBitmapApplyCorrelatedSiblingMembership(ctx context.Context, result BitmapQueryResult, membership qsbridge.MembershipEdge) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	if !strings.EqualFold(membership.Left.Table.Table, membership.Right.Table.Table) {
		return result, directBitmapMembershipDiagnostics("correlated sibling membership only supports repeated aliases of the same physical table in this slice"), nil
	}
	rightOnlyPredicates, correlatedPredicates := directBitmapSplitMembershipPredicates(membership)
	if len(correlatedPredicates) == 0 {
		return result, directBitmapMembershipDiagnostics("correlated sibling membership requires a cross-domain predicate"), nil
	}
	rightMembership := membership
	rightMembership.Predicates = rightOnlyPredicates
	rightResult, diagnostics, err := r.directBitmapMembershipRightCandidateResult(ctx, rightMembership)
	if err != nil || diagnostics.BlocksNative() {
		return result, diagnostics, err
	}
	leftFields := directBitmapCorrelatedMembershipProjectionFields(membership.Left, correlatedPredicates, membership.Left.Table)
	rightFields := directBitmapCorrelatedMembershipProjectionFields(membership.Right, correlatedPredicates, membership.Right.Table)
	leftRowSet, diagnostics, _, err := directBitmapMaterializeWithKernel(ctx, r.projectionMaterializationKernel(), qsbridge.QuantaMaterializationRequest{
		Index:            membership.Left.Table.Table,
		Rownums:          append([]qsbridge.QuantaRownum(nil), result.Rownums...),
		ProjectionFields: leftFields,
	})
	if err != nil || diagnostics.BlocksNative() {
		return result, diagnostics, err
	}
	rightRowSet, diagnostics, _, err := directBitmapMaterializeWithKernel(ctx, r.projectionMaterializationKernel(), qsbridge.QuantaMaterializationRequest{
		Index:            membership.Right.Table.Table,
		Rownums:          append([]qsbridge.QuantaRownum(nil), rightResult.Rownums...),
		ProjectionFields: rightFields,
	})
	if err != nil || diagnostics.BlocksNative() {
		return result, diagnostics, err
	}
	leftKeys, ok := directBitmapProjectedValues(leftRowSet, membership.Left)
	if !ok {
		return result, directBitmapMembershipDiagnostics("correlated sibling membership left key is not present in materialized row set"), nil
	}
	rightKeys, ok := directBitmapProjectedValues(rightRowSet, membership.Right)
	if !ok {
		return result, directBitmapMembershipDiagnostics("correlated sibling membership right key is not present in materialized row set"), nil
	}
	rightRowsByKey := make(map[string][]int, len(rightKeys))
	for i, value := range rightKeys {
		if value.Kind == qsbridge.ValueNull || value.Value == nil {
			continue
		}
		key := directBitmapGroupKey(value)
		rightRowsByKey[key] = append(rightRowsByKey[key], i)
	}
	filtered := result.Clone()
	filtered.Rownums = filtered.Rownums[:0]
	for i, rownum := range leftRowSet.Rownums {
		key := ""
		if i < len(leftKeys) && leftKeys[i].Kind != qsbridge.ValueNull && leftKeys[i].Value != nil {
			key = directBitmapGroupKey(leftKeys[i])
		}
		matched := false
		for _, rightIndex := range rightRowsByKey[key] {
			ok, diagnostics := directBitmapEvaluateCorrelatedMembershipPredicates(correlatedPredicates, leftRowSet, i, rightRowSet, rightIndex, membership)
			if diagnostics.BlocksNative() {
				return result, diagnostics, nil
			}
			if ok {
				matched = true
				break
			}
		}
		keep := matched
		if membership.Kind == qsbridge.MembershipAnti {
			keep = !matched
		}
		if keep {
			filtered.Rownums = append(filtered.Rownums, rownum)
		}
	}
	filtered.Count = uint64(len(filtered.Rownums))
	return filtered, nil, nil
}

func (r DirectBitmapRuntime) directBitmapApplyRelationshipMembership(ctx context.Context, result BitmapQueryResult, membership qsbridge.MembershipEdge) (BitmapQueryResult, bool, qsbridge.DiagnosticSet, error) {
	vectorRequest, ok := directBitmapRelationshipMembershipRequest(membership, qsbridge.QuantaCandidateSet{})
	if !ok {
		return result, false, nil, nil
	}
	if r.RelationshipReader == nil {
		return result, true, directBitmapMembershipDiagnostics("relationship-vector membership has no relationship reader"), nil
	}
	rightResult, diagnostics, err := r.directBitmapMembershipRightCandidateResult(ctx, membership)
	if err != nil || diagnostics.BlocksNative() {
		return result, true, diagnostics, err
	}
	vectorRequest, _ = directBitmapRelationshipMembershipRequest(membership, qsbridge.QuantaCandidateSet{
		Index:   membership.Right.Table.Table,
		Rownums: append([]qsbridge.QuantaRownum(nil), rightResult.Rownums...),
	})
	related, diagnostics, err := r.RelationshipReader.ReadRelatedCandidates(ctx, vectorRequest)
	if err != nil || diagnostics.BlocksNative() {
		return result, true, diagnostics, err
	}
	relatedRows := make(map[qsbridge.QuantaRownum]struct{}, len(related.Rownums))
	for _, rownum := range related.Rownums {
		relatedRows[rownum] = struct{}{}
	}
	filtered := result.Clone()
	filtered.Rownums = filtered.Rownums[:0]
	for _, rownum := range result.Rownums {
		_, matched := relatedRows[rownum]
		keep := matched
		if membership.Kind == qsbridge.MembershipAnti {
			keep = !matched
		}
		if keep {
			filtered.Rownums = append(filtered.Rownums, rownum)
		}
	}
	filtered.Count = uint64(len(filtered.Rownums))
	return filtered, true, nil, nil
}

func directBitmapRelationshipMembershipRequest(membership qsbridge.MembershipEdge, source qsbridge.QuantaCandidateSet) (qsbridge.FilterDomainRelationshipVectorRequest, bool) {
	leftRelation := directBitmapMembershipFieldIsParentRelation(membership.Left)
	rightRelation := directBitmapMembershipFieldIsParentRelation(membership.Right)
	if leftRelation == rightRelation {
		return qsbridge.FilterDomainRelationshipVectorRequest{}, false
	}
	switch {
	case rightRelation:
		return qsbridge.FilterDomainRelationshipVectorRequest{
			Operation:        qsbridge.FilterDomainNormalizeGroupedFilter,
			SourceFragment:   directBitmapRelationshipMembershipSourceFragment(membership.Right),
			SourceCandidates: source,
			SourceDomain:     membership.Right.Table.Table,
			TargetDomain:     membership.Left.Table.Table,
			Direction:        qsbridge.FilterDomainRelationshipVectorDirectionLeftToRight,
			Strategy:         qsbridge.PhysicalStrategyRelationshipVectorNormalization,
			Edge: qsbridge.RelationshipJoinPlanEdge{
				Left:          membership.Right,
				Right:         membership.Left,
				ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
				EncodingKind:  qsbridge.RelationshipEncodingVector,
			},
		}, true
	case leftRelation:
		return qsbridge.FilterDomainRelationshipVectorRequest{
			Operation:        qsbridge.FilterDomainNormalizeGroupedFilter,
			SourceFragment:   directBitmapRelationshipMembershipSourceFragment(membership.Right),
			SourceCandidates: source,
			SourceDomain:     membership.Right.Table.Table,
			TargetDomain:     membership.Left.Table.Table,
			Direction:        qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
			Strategy:         qsbridge.PhysicalStrategyRelationshipVectorNormalization,
			Edge: qsbridge.RelationshipJoinPlanEdge{
				Left:          membership.Left,
				Right:         membership.Right,
				ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
				EncodingKind:  qsbridge.RelationshipEncodingVector,
			},
		}, true
	default:
		return qsbridge.FilterDomainRelationshipVectorRequest{}, false
	}
}

func directBitmapRelationshipMembershipSourceFragment(field qsbridge.FieldRef) qsbridge.QuantaQueryFragment {
	return qsbridge.QuantaQueryFragment{
		Index: field.Table.Table,
		Field: directBitmapFieldPhysicalName(field),
	}
}

func directBitmapMembershipFieldIsParentRelation(field qsbridge.FieldRef) bool {
	return strings.EqualFold(field.Encoding.LegacyName, "ParentRelation")
}

func (r DirectBitmapRuntime) directBitmapMembershipRightCandidateResult(ctx context.Context, membership qsbridge.MembershipEdge) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	rightRequest, diagnostics := directBitmapMembershipRightRequest(membership)
	if diagnostics.BlocksNative() {
		return BitmapQueryResult{}, diagnostics, nil
	}
	session, diagnostics, err := r.Sessions.BorrowDirectSession(ctx, rightRequest)
	if err != nil || diagnostics.BlocksNative() {
		return BitmapQueryResult{}, diagnostics, err
	}
	if session == nil {
		return BitmapQueryResult{}, directBitmapMembershipDiagnostics("direct session provider returned nil membership session"), nil
	}
	rightResult, queryDiagnostics, queryErr := session.QueryBitmap(ctx, rightRequest)
	releaseDiagnostics := session.Release(ctx)
	diagnostics = append(diagnostics, queryDiagnostics...)
	diagnostics = append(diagnostics, releaseDiagnostics...)
	if queryErr != nil || diagnostics.BlocksNative() {
		return BitmapQueryResult{}, diagnostics, queryErr
	}
	residuals := directBitmapMembershipResidualPredicates(membership.Predicates)
	if len(residuals) == 0 {
		return rightResult, nil, nil
	}
	rowSet, diagnostics, err := r.directBitmapMembershipMaterialize(ctx, rightResult, membership.Right, membership.Predicates)
	if err != nil || diagnostics.BlocksNative() {
		return BitmapQueryResult{}, diagnostics, err
	}
	rowSet, diagnostics = directBitmapFilterResidualScanPredicates(ExecutionRequest{Predicates: residuals}, rowSet)
	if diagnostics.BlocksNative() {
		return BitmapQueryResult{}, diagnostics, nil
	}
	rightResult.Rownums = append([]qsbridge.QuantaRownum(nil), rowSet.Rownums...)
	rightResult.Count = uint64(len(rightResult.Rownums))
	return rightResult, nil, nil
}

func (r DirectBitmapRuntime) directBitmapMembershipRightValues(ctx context.Context, membership qsbridge.MembershipEdge) (map[string]struct{}, qsbridge.DiagnosticSet, error) {
	rightResult, diagnostics, err := r.directBitmapMembershipRightCandidateResult(ctx, membership)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	rowSet, diagnostics, err := r.directBitmapMembershipMaterialize(ctx, rightResult, membership.Right, membership.Predicates)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	rowSet, diagnostics = directBitmapFilterResidualScanPredicates(ExecutionRequest{Predicates: directBitmapMembershipResidualPredicates(membership.Predicates)}, rowSet)
	if diagnostics.BlocksNative() {
		return nil, diagnostics, nil
	}
	values, ok := directBitmapProjectedValues(rowSet, membership.Right)
	if !ok {
		return nil, directBitmapMembershipDiagnostics("membership right field is not present in materialized row set"), nil
	}
	valueSet := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.Kind == qsbridge.ValueNull || value.Value == nil {
			continue
		}
		valueSet[directBitmapGroupKey(value)] = struct{}{}
	}
	return valueSet, nil, nil
}

func (r DirectBitmapRuntime) directBitmapMembershipLeftValues(ctx context.Context, result BitmapQueryResult, field qsbridge.FieldRef) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
	if len(result.Rownums) == 0 && result.Count > 0 {
		return qsbridge.QuantaProjectedRowSet{}, directBitmapMembershipDiagnostics("membership filters require materialized candidate rownums"), nil
	}
	rowSet, diagnostics, _, err := directBitmapMaterializeWithKernel(ctx, r.projectionMaterializationKernel(), qsbridge.QuantaMaterializationRequest{
		Index:   field.Table.Table,
		Rownums: append([]qsbridge.QuantaRownum(nil), result.Rownums...),
		ProjectionFields: []qsbridge.QuantaProjectionField{
			directBitmapMembershipProjectionField(field),
		},
	})
	return rowSet, diagnostics, err
}

func (r DirectBitmapRuntime) directBitmapMembershipMaterialize(ctx context.Context, result BitmapQueryResult, field qsbridge.FieldRef, predicates []qsbridge.Predicate) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
	fields := []qsbridge.QuantaProjectionField{directBitmapMembershipProjectionField(field)}
	for _, predicate := range directBitmapMembershipResidualPredicates(predicates) {
		for _, required := range directBitmapMembershipRequiredFields(predicate.Expr) {
			if !strings.EqualFold(required.Table.Table, field.Table.Table) {
				return qsbridge.QuantaProjectedRowSet{}, directBitmapMembershipDiagnostics("membership residual predicates must stay on the membership right table"), nil
			}
			projection := directBitmapMembershipProjectionField(required)
			if !directBitmapMembershipHasProjectionField(fields, projection) {
				fields = append(fields, projection)
			}
		}
	}
	rowSet, diagnostics, _, err := directBitmapMaterializeWithKernel(ctx, r.projectionMaterializationKernel(), qsbridge.QuantaMaterializationRequest{
		Index:            field.Table.Table,
		Rownums:          append([]qsbridge.QuantaRownum(nil), result.Rownums...),
		ProjectionFields: fields,
	})
	return rowSet, diagnostics, err
}

func directBitmapMembershipRightRequest(membership qsbridge.MembershipEdge) (ExecutionRequest, qsbridge.DiagnosticSet) {
	fragments := make([]qsbridge.QuantaQueryFragment, 0, len(membership.Predicates))
	for _, predicate := range membership.Predicates {
		if predicate.Placement == qsbridge.PredicateResidualScan {
			continue
		}
		fragment, diagnostics, ok := directBitmapMembershipPredicateFragment(predicate)
		if diagnostics.BlocksNative() {
			return ExecutionRequest{}, diagnostics
		}
		if !ok {
			continue
		}
		fragments = append(fragments, fragment)
	}
	if len(fragments) == 0 {
		fragments = append(fragments, qsbridge.QuantaQueryFragment{
			Index:     membership.Right.Table.Table,
			Field:     directBitmapFieldPhysicalName(membership.Right),
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		})
	}
	return NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: fragments}), nil
}

func directBitmapMembershipPredicateFragment(predicate qsbridge.Predicate) (qsbridge.QuantaQueryFragment, qsbridge.DiagnosticSet, bool) {
	binary, ok := directBitmapBinaryExpr(predicate.Expr)
	if !ok {
		return qsbridge.QuantaQueryFragment{}, nil, false
	}
	field, ok := directBitmapExprField(binary.Left)
	if !ok {
		return qsbridge.QuantaQueryFragment{}, nil, false
	}
	switch binary.Op {
	case qsbridge.BinaryOpIn, qsbridge.BinaryOpNotIn:
		list, ok := directBitmapListExpr(binary.Right)
		if !ok {
			return qsbridge.QuantaQueryFragment{}, nil, false
		}
		values := make([]*big.Int, 0, len(list.Items))
		for _, item := range list.Items {
			value, ok := directBitmapMembershipLiteralBigInt(item)
			if !ok {
				return qsbridge.QuantaQueryFragment{}, nil, false
			}
			values = append(values, value)
		}
		return qsbridge.QuantaQueryFragment{
			Index:     field.Table.Table,
			Field:     directBitmapFieldPhysicalName(field),
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpBatchEQ,
			Values:    values,
			Negate:    binary.Op == qsbridge.BinaryOpNotIn,
		}, nil, true
	case qsbridge.BinaryOpEqual, qsbridge.BinaryOpNotEqual, qsbridge.BinaryOpGreater, qsbridge.BinaryOpGreaterEqual, qsbridge.BinaryOpLess, qsbridge.BinaryOpLessEqual:
		value, ok := directBitmapMembershipLiteralBigInt(binary.Right)
		if !ok {
			return qsbridge.QuantaQueryFragment{}, nil, false
		}
		operation := qsbridge.QuantaOperationIntersect
		bsiOp := directBitmapMembershipBSIOp(binary.Op)
		if binary.Op == qsbridge.BinaryOpNotEqual {
			operation = qsbridge.QuantaOperationDifference
			bsiOp = qsbridge.QuantaBSIOpEQ
		}
		return qsbridge.QuantaQueryFragment{
			Index:     field.Table.Table,
			Field:     directBitmapFieldPhysicalName(field),
			Operation: operation,
			BSIOp:     bsiOp,
			Value:     value,
		}, nil, true
	default:
		return qsbridge.QuantaQueryFragment{}, nil, false
	}
}

func directBitmapMembershipResidualPredicates(predicates []qsbridge.Predicate) []qsbridge.Predicate {
	residuals := make([]qsbridge.Predicate, 0, len(predicates))
	for _, predicate := range predicates {
		if predicate.Placement == qsbridge.PredicateResidualScan || !directBitmapMembershipPredicateCanLower(predicate) {
			predicate.Placement = qsbridge.PredicateResidualScan
			residuals = append(residuals, predicate)
		}
	}
	return residuals
}

func directBitmapMembershipHasCorrelatedPredicates(membership qsbridge.MembershipEdge) bool {
	_, correlated := directBitmapSplitMembershipPredicates(membership)
	return len(correlated) > 0
}

func directBitmapSplitMembershipPredicates(membership qsbridge.MembershipEdge) ([]qsbridge.Predicate, []qsbridge.Predicate) {
	rightOnly := make([]qsbridge.Predicate, 0, len(membership.Predicates))
	correlated := make([]qsbridge.Predicate, 0, len(membership.Predicates))
	for _, predicate := range membership.Predicates {
		fields := directBitmapMembershipRequiredFields(predicate.Expr)
		hasLeft := false
		hasRight := false
		other := false
		for _, field := range fields {
			switch {
			case directBitmapSameTableInstance(field.Table, membership.Left.Table):
				hasLeft = true
			case directBitmapSameTableInstance(field.Table, membership.Right.Table):
				hasRight = true
			default:
				other = true
			}
		}
		if other || hasLeft {
			correlated = append(correlated, predicate)
			continue
		}
		if hasRight || len(fields) == 0 {
			rightOnly = append(rightOnly, predicate)
		}
	}
	return rightOnly, correlated
}

func directBitmapCorrelatedMembershipProjectionFields(base qsbridge.FieldRef, predicates []qsbridge.Predicate, table qsbridge.TableInstance) []qsbridge.QuantaProjectionField {
	fields := []qsbridge.QuantaProjectionField{directBitmapMembershipProjectionField(base)}
	for _, predicate := range predicates {
		for _, required := range directBitmapMembershipRequiredFields(predicate.Expr) {
			if !directBitmapSameTableInstance(required.Table, table) {
				continue
			}
			projection := directBitmapMembershipProjectionField(required)
			if !directBitmapMembershipHasProjectionField(fields, projection) {
				fields = append(fields, projection)
			}
		}
	}
	return fields
}

func directBitmapEvaluateCorrelatedMembershipPredicates(predicates []qsbridge.Predicate, leftRowSet qsbridge.QuantaProjectedRowSet, leftIndex int, rightRowSet qsbridge.QuantaProjectedRowSet, rightIndex int, membership qsbridge.MembershipEdge) (bool, qsbridge.DiagnosticSet) {
	for _, predicate := range predicates {
		matched, diagnostics := directBitmapEvaluateCorrelatedMembershipBoolExpr(predicate.Expr, leftRowSet, leftIndex, rightRowSet, rightIndex, membership)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func directBitmapEvaluateCorrelatedMembershipBoolExpr(expr qsbridge.Expr, leftRowSet qsbridge.QuantaProjectedRowSet, leftIndex int, rightRowSet qsbridge.QuantaProjectedRowSet, rightIndex int, membership qsbridge.MembershipEdge) (bool, qsbridge.DiagnosticSet) {
	binary, ok := directBitmapBinaryExpr(expr)
	if !ok {
		return false, directBitmapMembershipDiagnostics("correlated sibling membership predicate must be binary")
	}
	switch binary.Op {
	case qsbridge.BinaryOpEqual, qsbridge.BinaryOpNotEqual,
		qsbridge.BinaryOpLess, qsbridge.BinaryOpLessEqual,
		qsbridge.BinaryOpGreater, qsbridge.BinaryOpGreaterEqual:
		left, diagnostics := directBitmapEvaluateCorrelatedMembershipExpr(binary.Left, leftRowSet, leftIndex, rightRowSet, rightIndex, membership)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		right, diagnostics := directBitmapEvaluateCorrelatedMembershipExpr(binary.Right, leftRowSet, leftIndex, rightRowSet, rightIndex, membership)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		return directBitmapResidualCompareCells(binary.Op, left, right), nil
	default:
		return false, directBitmapMembershipDiagnostics("correlated sibling membership predicate operator is not supported in this slice")
	}
}

func directBitmapEvaluateCorrelatedMembershipExpr(expr qsbridge.Expr, leftRowSet qsbridge.QuantaProjectedRowSet, leftIndex int, rightRowSet qsbridge.QuantaProjectedRowSet, rightIndex int, membership qsbridge.MembershipEdge) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if field, ok := directBitmapExprField(expr); ok {
		switch {
		case directBitmapSameTableInstance(field.Table, membership.Left.Table):
			values, ok := directBitmapProjectedValues(leftRowSet, field)
			if !ok || leftIndex >= len(values) {
				return qsbridge.ResultCell{}, directBitmapMembershipDiagnostics("correlated sibling membership left field is not materialized")
			}
			return values[leftIndex], nil
		case directBitmapSameTableInstance(field.Table, membership.Right.Table):
			values, ok := directBitmapProjectedValues(rightRowSet, field)
			if !ok || rightIndex >= len(values) {
				return qsbridge.ResultCell{}, directBitmapMembershipDiagnostics("correlated sibling membership right field is not materialized")
			}
			return values[rightIndex], nil
		default:
			return qsbridge.ResultCell{}, directBitmapMembershipDiagnostics("correlated sibling membership field is outside the membership domains")
		}
	}
	if literal, ok := directBitmapLiteralExpr(expr); ok {
		return directBitmapLiteralCell(literal), nil
	}
	return qsbridge.ResultCell{}, directBitmapMembershipDiagnostics("correlated sibling membership expression is not supported in this slice")
}

func directBitmapSameTableInstance(left qsbridge.TableInstance, right qsbridge.TableInstance) bool {
	if left.ID != "" && right.ID != "" {
		return left.ID == right.ID
	}
	return strings.EqualFold(left.RefName(), right.RefName()) && strings.EqualFold(left.Table, right.Table)
}

func directBitmapMembershipPredicateCanLower(predicate qsbridge.Predicate) bool {
	if predicate.Placement == qsbridge.PredicateResidualScan {
		return false
	}
	_, diagnostics, ok := directBitmapMembershipPredicateFragment(predicate)
	return ok && !diagnostics.BlocksNative()
}

func directBitmapMembershipRequiredFields(expr qsbridge.Expr) []qsbridge.FieldRef {
	fields := make([]qsbridge.FieldRef, 0)
	var walk func(qsbridge.Expr)
	walk = func(expr qsbridge.Expr) {
		switch value := expr.(type) {
		case qsbridge.FieldExpr:
			fields = append(fields, value.Ref)
		case *qsbridge.FieldExpr:
			if value != nil {
				fields = append(fields, value.Ref)
			}
		case qsbridge.BinaryExpr:
			walk(value.Left)
			walk(value.Right)
		case *qsbridge.BinaryExpr:
			if value != nil {
				walk(value.Left)
				walk(value.Right)
			}
		case qsbridge.CallExpr:
			for _, arg := range value.Args {
				walk(arg)
			}
		case *qsbridge.CallExpr:
			if value != nil {
				for _, arg := range value.Args {
					walk(arg)
				}
			}
		case qsbridge.ListExpr:
			for _, item := range value.Items {
				walk(item)
			}
		case *qsbridge.ListExpr:
			if value != nil {
				for _, item := range value.Items {
					walk(item)
				}
			}
		}
	}
	walk(expr)
	return fields
}

func directBitmapMembershipProjectionField(field qsbridge.FieldRef) qsbridge.QuantaProjectionField {
	return qsbridge.QuantaProjectionField{
		Index:        field.Table.Table,
		Role:         qsbridge.TableInstanceID(materializationFieldRole(field.Table.Table, field)),
		Field:        directBitmapFieldPhysicalName(field),
		Type:         field.Type,
		PhysicalName: field.PhysicalName,
		Roles:        field.Roles,
		Visible:      false,
	}
}

func directBitmapMembershipHasProjectionField(fields []qsbridge.QuantaProjectionField, want qsbridge.QuantaProjectionField) bool {
	for _, field := range fields {
		if field.Index == want.Index && field.Field == want.Field && field.PhysicalName == want.PhysicalName {
			return true
		}
	}
	return false
}

func directBitmapMembershipLiteralBigInt(expr qsbridge.Expr) (*big.Int, bool) {
	literal, ok := directBitmapLiteralExpr(expr)
	if !ok {
		return nil, false
	}
	switch value := literal.Value.(type) {
	case int:
		return big.NewInt(int64(value)), true
	case int64:
		return big.NewInt(value), true
	case uint64:
		return new(big.Int).SetUint64(value), true
	case string:
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, false
		}
		return big.NewInt(parsed), true
	default:
		return nil, false
	}
}

func directBitmapMembershipBSIOp(op qsbridge.BinaryOp) qsbridge.QuantaBSIOp {
	switch op {
	case qsbridge.BinaryOpEqual:
		return qsbridge.QuantaBSIOpEQ
	case qsbridge.BinaryOpGreater:
		return qsbridge.QuantaBSIOpGT
	case qsbridge.BinaryOpGreaterEqual:
		return qsbridge.QuantaBSIOpGE
	case qsbridge.BinaryOpLess:
		return qsbridge.QuantaBSIOpLT
	case qsbridge.BinaryOpLessEqual:
		return qsbridge.QuantaBSIOpLE
	default:
		return qsbridge.QuantaBSIOpEQ
	}
}

func directBitmapMembershipDiagnostics(message string) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, fmt.Sprintf("membership filter execution: %s", message)),
	}
}

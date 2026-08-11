package qsruntime

import (
	"context"
	"fmt"
	"math/big"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// QuantaFilterLeafEvaluator evaluates one physical bitmap fragment into rownums.
type QuantaFilterLeafEvaluator interface {
	EvaluateFilterLeaf(context.Context, qsbridge.QuantaQueryFragment) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error)
}

// QuantaConstrainedFilterLeafEvaluator evaluates one physical bitmap fragment inside an existing candidate set.
type QuantaConstrainedFilterLeafEvaluator interface {
	EvaluateFilterLeafWithinCandidateSet(context.Context, qsbridge.QuantaQueryFragment, qsbridge.QuantaCandidateSet) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error)
}

// QuantaFilterTreeEvaluator evaluates grouped bitmap filter trees.
type QuantaFilterTreeEvaluator struct {
	Leaves QuantaFilterLeafEvaluator
}

// EvaluateFilter applies grouped UNION and INTERSECT semantics over leaf fragments.
func (e QuantaFilterTreeEvaluator) EvaluateFilter(ctx context.Context, filter qsbridge.QuantaFilterExpression) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	if err := ctx.Err(); err != nil {
		return qsbridge.QuantaCandidateSet{}, nil, err
	}
	if filter.Empty() {
		return qsbridge.QuantaCandidateSet{}, nil, nil
	}
	switch filter.Operation {
	case qsbridge.QuantaFilterLeaf:
		if e.Leaves == nil {
			return qsbridge.QuantaCandidateSet{}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "filter tree evaluator has no leaf evaluator"),
			}, nil
		}
		return e.Leaves.EvaluateFilterLeaf(ctx, filter.Fragment)
	case qsbridge.QuantaFilterCandidateSet:
		return filter.CandidateSet, nil, nil
	case qsbridge.QuantaFilterIntersect:
		return e.evaluateFilterIntersectChildren(ctx, filter.Children)
	case qsbridge.QuantaFilterUnion:
		return e.evaluateFilterChildren(ctx, filter.Children, quantaFilterUnionCandidateSets)
	default:
		return qsbridge.QuantaCandidateSet{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, fmt.Sprintf("unsupported filter operation: %s", filter.Operation)),
		}, nil
	}
}

func (e QuantaFilterTreeEvaluator) evaluateFilterChildren(
	ctx context.Context,
	children []qsbridge.QuantaFilterExpression,
	merge func(qsbridge.QuantaCandidateSet, qsbridge.QuantaCandidateSet) qsbridge.QuantaCandidateSet,
) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	if len(children) == 0 {
		return qsbridge.QuantaCandidateSet{}, nil, nil
	}
	current, diagnostics, err := e.EvaluateFilter(ctx, children[0])
	if err != nil || diagnostics.BlocksNative() {
		return current, diagnostics, err
	}
	for _, child := range children[1:] {
		if err := ctx.Err(); err != nil {
			return qsbridge.QuantaCandidateSet{}, nil, err
		}
		next, childDiagnostics, err := e.EvaluateFilter(ctx, child)
		diagnostics = append(diagnostics, childDiagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return current, diagnostics, err
		}
		current = merge(current, next)
	}
	return current, diagnostics, nil
}

func (e QuantaFilterTreeEvaluator) evaluateFilterIntersectChildren(ctx context.Context, children []qsbridge.QuantaFilterExpression) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	if len(children) == 0 {
		return qsbridge.QuantaCandidateSet{}, nil, nil
	}
	ordered := quantaFilterIntersectEvaluationOrder(children)
	current, diagnostics, err := e.EvaluateFilter(ctx, ordered[0])
	if err != nil || diagnostics.BlocksNative() {
		return current, diagnostics, err
	}
	if len(current.Rownums) == 0 {
		return current, diagnostics, nil
	}
	for _, child := range ordered[1:] {
		if err := ctx.Err(); err != nil {
			return qsbridge.QuantaCandidateSet{}, nil, err
		}
		next, childDiagnostics, err := e.evaluateFilterIntersectChild(ctx, child, current)
		diagnostics = append(diagnostics, childDiagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return current, diagnostics, err
		}
		current = quantaFilterIntersectCandidateSets(current, next)
		if len(current.Rownums) == 0 {
			return current, diagnostics, nil
		}
	}
	return current, diagnostics, nil
}

func quantaFilterIntersectEvaluationOrder(children []qsbridge.QuantaFilterExpression) []qsbridge.QuantaFilterExpression {
	if len(children) < 2 {
		return children
	}
	flattened := quantaFilterFlattenIntersectChildren(children)
	flattened = quantaFilterCoalesceIntersectRanges(flattened)
	ordered := make([]qsbridge.QuantaFilterExpression, 0, len(flattened))
	for _, child := range flattened {
		if child.CandidateSetLeaf() {
			ordered = append(ordered, child)
		}
	}
	for _, child := range flattened {
		if child.CandidateSetLeaf() || !quantaFilterExpressionContainsCandidateSetLeaf(child) {
			continue
		}
		ordered = append(ordered, child)
	}
	if len(ordered) == 0 {
		return children
	}
	for _, child := range flattened {
		if !child.CandidateSetLeaf() && !quantaFilterExpressionContainsCandidateSetLeaf(child) {
			ordered = append(ordered, child)
		}
	}
	return ordered
}

func quantaFilterExpressionContainsCandidateSetLeaf(filter qsbridge.QuantaFilterExpression) bool {
	if filter.CandidateSetLeaf() {
		return true
	}
	for _, child := range filter.Children {
		if quantaFilterExpressionContainsCandidateSetLeaf(child) {
			return true
		}
	}
	return false
}

func quantaFilterFlattenIntersectChildren(children []qsbridge.QuantaFilterExpression) []qsbridge.QuantaFilterExpression {
	flattened := make([]qsbridge.QuantaFilterExpression, 0, len(children))
	for _, child := range children {
		if child.Operation == qsbridge.QuantaFilterIntersect {
			flattened = append(flattened, quantaFilterFlattenIntersectChildren(child.Children)...)
			continue
		}
		flattened = append(flattened, child)
	}
	return flattened
}

func quantaFilterCoalesceIntersectRanges(children []qsbridge.QuantaFilterExpression) []qsbridge.QuantaFilterExpression {
	if len(children) < 2 {
		return children
	}
	used := make([]bool, len(children))
	coalesced := make([]qsbridge.QuantaFilterExpression, 0, len(children))
	for i, child := range children {
		if used[i] {
			continue
		}
		match := -1
		for j := i + 1; j < len(children); j++ {
			if used[j] || !quantaFilterRangePair(child, children[j]) {
				continue
			}
			match = j
			break
		}
		if match == -1 {
			coalesced = append(coalesced, child)
			continue
		}
		used[i] = true
		used[match] = true
		coalesced = append(coalesced, quantaFilterRangeExpression(child, children[match]))
	}
	return coalesced
}

func quantaFilterRangePair(left qsbridge.QuantaFilterExpression, right qsbridge.QuantaFilterExpression) bool {
	if left.Operation != qsbridge.QuantaFilterLeaf || right.Operation != qsbridge.QuantaFilterLeaf {
		return false
	}
	leftFragment := left.Fragment
	rightFragment := right.Fragment
	if !quantaFilterSameRangeField(leftFragment, rightFragment) {
		return false
	}
	return (quantaFilterLowerBoundOp(leftFragment.BSIOp) && quantaFilterUpperBoundOp(rightFragment.BSIOp)) ||
		(quantaFilterUpperBoundOp(leftFragment.BSIOp) && quantaFilterLowerBoundOp(rightFragment.BSIOp))
}

func quantaFilterSameRangeField(left qsbridge.QuantaQueryFragment, right qsbridge.QuantaQueryFragment) bool {
	return left.Index == right.Index &&
		left.Role == right.Role &&
		left.Field == right.Field &&
		left.Operation == qsbridge.QuantaOperationIntersect &&
		right.Operation == qsbridge.QuantaOperationIntersect &&
		!left.Negate &&
		!right.Negate &&
		!left.NullCheck &&
		!right.NullCheck &&
		left.Value != nil &&
		right.Value != nil &&
		left.RangeCoalesceAllowed &&
		right.RangeCoalesceAllowed
}

func quantaFilterRangeExpression(left qsbridge.QuantaFilterExpression, right qsbridge.QuantaFilterExpression) qsbridge.QuantaFilterExpression {
	rangeFragment := left.Fragment
	rangeFragment.BSIOp = qsbridge.QuantaBSIOpRange
	rangeFragment.Value = nil
	if quantaFilterLowerBoundOp(left.Fragment.BSIOp) {
		rangeFragment.Begin = quantaFilterInclusiveLowerBound(left.Fragment.BSIOp, left.Fragment.Value)
		rangeFragment.End = quantaFilterInclusiveUpperBound(right.Fragment.BSIOp, right.Fragment.Value)
	} else {
		rangeFragment.Begin = quantaFilterInclusiveLowerBound(right.Fragment.BSIOp, right.Fragment.Value)
		rangeFragment.End = quantaFilterInclusiveUpperBound(left.Fragment.BSIOp, left.Fragment.Value)
	}
	rangeFragment.HasLiteral = false
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment:  rangeFragment,
	}
}

func quantaFilterLowerBoundOp(op qsbridge.QuantaBSIOp) bool {
	return op == qsbridge.QuantaBSIOpGE || op == qsbridge.QuantaBSIOpGT
}

func quantaFilterUpperBoundOp(op qsbridge.QuantaBSIOp) bool {
	return op == qsbridge.QuantaBSIOpLE || op == qsbridge.QuantaBSIOpLT
}

func quantaFilterInclusiveLowerBound(op qsbridge.QuantaBSIOp, value *big.Int) *big.Int {
	bound := new(big.Int).Set(value)
	if op == qsbridge.QuantaBSIOpGT {
		bound.Add(bound, big.NewInt(1))
	}
	return bound
}

func quantaFilterInclusiveUpperBound(op qsbridge.QuantaBSIOp, value *big.Int) *big.Int {
	bound := new(big.Int).Set(value)
	if op == qsbridge.QuantaBSIOpLT {
		bound.Sub(bound, big.NewInt(1))
	}
	return bound
}

func (e QuantaFilterTreeEvaluator) evaluateFilterIntersectChild(ctx context.Context, child qsbridge.QuantaFilterExpression, current qsbridge.QuantaCandidateSet) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	if child.Operation == qsbridge.QuantaFilterLeaf && len(current.Rownums) > 0 {
		if constrained, ok := e.Leaves.(QuantaConstrainedFilterLeafEvaluator); ok {
			return constrained.EvaluateFilterLeafWithinCandidateSet(ctx, child.Fragment, current)
		}
	}
	return e.EvaluateFilter(ctx, child)
}

func quantaFilterIntersectCandidateSets(left qsbridge.QuantaCandidateSet, right qsbridge.QuantaCandidateSet) qsbridge.QuantaCandidateSet {
	if len(left.Rownums) <= len(right.Rownums) {
		leftPositions := make(map[qsbridge.QuantaRownum][]int, len(left.Rownums))
		for i, rownum := range left.Rownums {
			leftPositions[rownum] = append(leftPositions[rownum], i)
		}
		matched := make([]bool, len(left.Rownums))
		for _, rownum := range right.Rownums {
			for _, position := range leftPositions[rownum] {
				matched[position] = true
			}
		}
		rownums := make([]qsbridge.QuantaRownum, 0, len(left.Rownums))
		for i, rownum := range left.Rownums {
			if matched[i] {
				rownums = append(rownums, rownum)
			}
		}
		left.Rownums = rownums
		if left.Index == "" {
			left.Index = right.Index
		}
		return left
	}

	rightRows := quantaFilterRownumSet(right.Rownums)
	rownums := make([]qsbridge.QuantaRownum, 0, len(right.Rownums))
	for _, rownum := range left.Rownums {
		if rightRows[rownum] {
			rownums = append(rownums, rownum)
		}
	}
	left.Rownums = rownums
	if left.Index == "" {
		left.Index = right.Index
	}
	return left
}

func quantaFilterUnionCandidateSets(left qsbridge.QuantaCandidateSet, right qsbridge.QuantaCandidateSet) qsbridge.QuantaCandidateSet {
	seen := quantaFilterRownumSet(left.Rownums)
	rownums := append([]qsbridge.QuantaRownum(nil), left.Rownums...)
	for _, rownum := range right.Rownums {
		if seen[rownum] {
			continue
		}
		seen[rownum] = true
		rownums = append(rownums, rownum)
	}
	left.Rownums = rownums
	if left.Index == "" {
		left.Index = right.Index
	}
	return left
}

func quantaFilterRownumSet(rownums []qsbridge.QuantaRownum) map[qsbridge.QuantaRownum]bool {
	set := make(map[qsbridge.QuantaRownum]bool, len(rownums))
	for _, rownum := range rownums {
		set[rownum] = true
	}
	return set
}

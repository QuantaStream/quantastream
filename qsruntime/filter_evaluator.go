package qsruntime

import (
	"context"
	"fmt"

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
	ordered := make([]qsbridge.QuantaFilterExpression, 0, len(flattened))
	for _, child := range flattened {
		if child.CandidateSetLeaf() {
			ordered = append(ordered, child)
		}
	}
	if len(ordered) == 0 {
		return children
	}
	for _, child := range flattened {
		if !child.CandidateSetLeaf() {
			ordered = append(ordered, child)
		}
	}
	return ordered
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

func (e QuantaFilterTreeEvaluator) evaluateFilterIntersectChild(ctx context.Context, child qsbridge.QuantaFilterExpression, current qsbridge.QuantaCandidateSet) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	if child.Operation == qsbridge.QuantaFilterLeaf && len(current.Rownums) > 0 {
		if constrained, ok := e.Leaves.(QuantaConstrainedFilterLeafEvaluator); ok {
			return constrained.EvaluateFilterLeafWithinCandidateSet(ctx, child.Fragment, current)
		}
	}
	return e.EvaluateFilter(ctx, child)
}

func quantaFilterIntersectCandidateSets(left qsbridge.QuantaCandidateSet, right qsbridge.QuantaCandidateSet) qsbridge.QuantaCandidateSet {
	rightRows := quantaFilterRownumSet(right.Rownums)
	rownums := make([]qsbridge.QuantaRownum, 0, len(left.Rownums))
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

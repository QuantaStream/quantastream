package qsruntime

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func directBitmapHasResidualScanPredicates(request ExecutionRequest) bool {
	return len(directBitmapResidualScanPredicates(request)) > 0
}

func directBitmapFilterResidualScanPredicates(request ExecutionRequest, rowSet qsbridge.QuantaProjectedRowSet) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet) {
	residuals := directBitmapResidualScanPredicates(request)
	if len(residuals) == 0 {
		return rowSet, nil
	}
	keep := make([]int, 0, rowSet.CandidateCount())
	for i := 0; i < rowSet.CandidateCount(); i++ {
		matched, diagnostics := directBitmapEvaluateResidualPredicates(residuals, rowSet, i)
		if diagnostics.BlocksNative() {
			return qsbridge.QuantaProjectedRowSet{}, diagnostics
		}
		if matched {
			keep = append(keep, i)
		}
	}
	return directBitmapFilterRowSetByIndexes(rowSet, keep), nil
}

func directBitmapResidualScanPredicates(request ExecutionRequest) []qsbridge.Predicate {
	residuals := make([]qsbridge.Predicate, 0, len(request.Predicates))
	for _, predicate := range request.Predicates {
		if predicate.Placement == qsbridge.PredicateResidualScan {
			residuals = append(residuals, predicate)
		}
	}
	for _, join := range request.Joins {
		for _, predicate := range join.On {
			if predicate.Placement == qsbridge.PredicateResidualScan || predicate.Placement == qsbridge.PredicateResidualJoin {
				residuals = append(residuals, predicate)
			}
		}
	}
	return residuals
}

func directBitmapEvaluateResidualPredicates(predicates []qsbridge.Predicate, rowSet qsbridge.QuantaProjectedRowSet, index int) (bool, qsbridge.DiagnosticSet) {
	matched := true
	for i, predicate := range predicates {
		current, diagnostics := directBitmapEvaluateResidualBoolExpr(predicate.Expr, rowSet, index)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		if i == 0 || predicate.Combinator == qsbridge.PredicateCombinatorAnd || predicate.Combinator == "" {
			matched = matched && current
			continue
		}
		if predicate.Combinator == qsbridge.PredicateCombinatorOr {
			matched = matched || current
			continue
		}
		return false, directBitmapResidualDiagnostics(fmt.Sprintf("unsupported residual predicate combinator: %s", predicate.Combinator))
	}
	return matched, nil
}

func directBitmapEvaluateResidualBoolExpr(expr qsbridge.Expr, rowSet qsbridge.QuantaProjectedRowSet, index int) (bool, qsbridge.DiagnosticSet) {
	binary, ok := directBitmapBinaryExpr(expr)
	if !ok {
		return false, directBitmapResidualDiagnostics("residual predicate must be a binary expression")
	}
	switch binary.Op {
	case qsbridge.BinaryOpAnd, qsbridge.BinaryOpOr:
		left, diagnostics := directBitmapEvaluateResidualBoolExpr(binary.Left, rowSet, index)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		right, diagnostics := directBitmapEvaluateResidualBoolExpr(binary.Right, rowSet, index)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		if binary.Op == qsbridge.BinaryOpAnd {
			return left && right, nil
		}
		return left || right, nil
	case qsbridge.BinaryOpEqual, qsbridge.BinaryOpNotEqual,
		qsbridge.BinaryOpLess, qsbridge.BinaryOpLessEqual,
		qsbridge.BinaryOpGreater, qsbridge.BinaryOpGreaterEqual:
		left, right, diagnostics := directBitmapEvaluateResidualPair(binary, rowSet, index)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		return directBitmapResidualCompareCells(binary.Op, left, right), nil
	case qsbridge.BinaryOpLike, qsbridge.BinaryOpNotLike:
		left, right, diagnostics := directBitmapEvaluateResidualPair(binary, rowSet, index)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		matched := directBitmapResidualLikeCells(left, right)
		if binary.Op == qsbridge.BinaryOpNotLike {
			return !matched, nil
		}
		return matched, nil
	case qsbridge.BinaryOpIn, qsbridge.BinaryOpNotIn:
		matched, diagnostics := directBitmapEvaluateResidualIn(binary, rowSet, index)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		if binary.Op == qsbridge.BinaryOpNotIn {
			return !matched, nil
		}
		return matched, nil
	case qsbridge.BinaryOpBetween, qsbridge.BinaryOpNotBetween:
		matched, diagnostics := directBitmapEvaluateResidualBetween(binary, rowSet, index)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		if binary.Op == qsbridge.BinaryOpNotBetween {
			return !matched, nil
		}
		return matched, nil
	default:
		return false, directBitmapResidualDiagnostics(fmt.Sprintf("unsupported residual predicate operator: %s", binary.Op))
	}
}

func directBitmapEvaluateResidualPair(binary qsbridge.BinaryExpr, rowSet qsbridge.QuantaProjectedRowSet, index int) (qsbridge.ResultCell, qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	left, diagnostics := directBitmapEvaluateMaterializedExpr(binary.Left, rowSet, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, qsbridge.ResultCell{}, diagnostics
	}
	right, diagnostics := directBitmapEvaluateMaterializedExpr(binary.Right, rowSet, index)
	if diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, qsbridge.ResultCell{}, diagnostics
	}
	return left, right, nil
}

func directBitmapEvaluateResidualIn(binary qsbridge.BinaryExpr, rowSet qsbridge.QuantaProjectedRowSet, index int) (bool, qsbridge.DiagnosticSet) {
	left, diagnostics := directBitmapEvaluateMaterializedExpr(binary.Left, rowSet, index)
	if diagnostics.BlocksNative() {
		return false, diagnostics
	}
	list, ok := directBitmapListExpr(binary.Right)
	if !ok {
		return false, directBitmapResidualDiagnostics("residual IN predicate requires a literal list")
	}
	for _, item := range list.Items {
		right, diagnostics := directBitmapEvaluateMaterializedExpr(item, rowSet, index)
		if diagnostics.BlocksNative() {
			return false, diagnostics
		}
		if directBitmapResidualCompareCells(qsbridge.BinaryOpEqual, left, right) {
			return true, nil
		}
	}
	return false, nil
}

func directBitmapEvaluateResidualBetween(binary qsbridge.BinaryExpr, rowSet qsbridge.QuantaProjectedRowSet, index int) (bool, qsbridge.DiagnosticSet) {
	left, diagnostics := directBitmapEvaluateMaterializedExpr(binary.Left, rowSet, index)
	if diagnostics.BlocksNative() {
		return false, diagnostics
	}
	list, ok := directBitmapListExpr(binary.Right)
	if !ok || len(list.Items) != 2 {
		return false, directBitmapResidualDiagnostics("residual BETWEEN predicate requires two bounds")
	}
	lower, diagnostics := directBitmapEvaluateMaterializedExpr(list.Items[0], rowSet, index)
	if diagnostics.BlocksNative() {
		return false, diagnostics
	}
	upper, diagnostics := directBitmapEvaluateMaterializedExpr(list.Items[1], rowSet, index)
	if diagnostics.BlocksNative() {
		return false, diagnostics
	}
	return directBitmapResidualCompareCells(qsbridge.BinaryOpGreaterEqual, left, lower) &&
		directBitmapResidualCompareCells(qsbridge.BinaryOpLessEqual, left, upper), nil
}

func directBitmapResidualCompareCells(op qsbridge.BinaryOp, left qsbridge.ResultCell, right qsbridge.ResultCell) bool {
	if left.Kind == qsbridge.ValueNull || right.Kind == qsbridge.ValueNull || left.Value == nil || right.Value == nil {
		return false
	}
	if leftNumber, leftOK := directBitmapNumericCellValue(left); leftOK {
		if rightNumber, rightOK := directBitmapNumericCellValue(right); rightOK {
			return directBitmapResidualCompareFloat(op, leftNumber, rightNumber)
		}
	}
	return directBitmapResidualCompareString(op, fmt.Sprint(left.Value), fmt.Sprint(right.Value))
}

func directBitmapResidualCompareFloat(op qsbridge.BinaryOp, left float64, right float64) bool {
	switch op {
	case qsbridge.BinaryOpEqual:
		return left == right
	case qsbridge.BinaryOpNotEqual:
		return left != right
	case qsbridge.BinaryOpLess:
		return left < right
	case qsbridge.BinaryOpLessEqual:
		return left <= right
	case qsbridge.BinaryOpGreater:
		return left > right
	case qsbridge.BinaryOpGreaterEqual:
		return left >= right
	default:
		return false
	}
}

func directBitmapResidualCompareString(op qsbridge.BinaryOp, left string, right string) bool {
	switch op {
	case qsbridge.BinaryOpEqual:
		return left == right
	case qsbridge.BinaryOpNotEqual:
		return left != right
	case qsbridge.BinaryOpLess:
		return left < right
	case qsbridge.BinaryOpLessEqual:
		return left <= right
	case qsbridge.BinaryOpGreater:
		return left > right
	case qsbridge.BinaryOpGreaterEqual:
		return left >= right
	default:
		return false
	}
}

func directBitmapResidualLikeCells(left qsbridge.ResultCell, right qsbridge.ResultCell) bool {
	if left.Kind == qsbridge.ValueNull || right.Kind == qsbridge.ValueNull || left.Value == nil || right.Value == nil {
		return false
	}
	return directBitmapResidualLike(fmt.Sprint(left.Value), fmt.Sprint(right.Value))
}

func directBitmapResidualLike(value string, pattern string) bool {
	var b strings.Builder
	b.WriteString("^")
	for _, r := range pattern {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	matched, err := regexp.MatchString(b.String(), value)
	return err == nil && matched
}

func directBitmapFilterRowSetByIndexes(rowSet qsbridge.QuantaProjectedRowSet, indexes []int) qsbridge.QuantaProjectedRowSet {
	filtered := rowSet
	filtered.Rownums = make([]qsbridge.QuantaRownum, 0, len(indexes))
	for _, index := range indexes {
		if index < len(rowSet.Rownums) {
			filtered.Rownums = append(filtered.Rownums, rowSet.Rownums[index])
		}
	}
	filtered.ProjectionVectors = append([]qsbridge.QuantaProjectionVector(nil), rowSet.ProjectionVectors...)
	for vectorIndex, vector := range rowSet.ProjectionVectors {
		values := make([]qsbridge.ResultCell, 0, len(indexes))
		for _, index := range indexes {
			if index < len(vector.Values) {
				values = append(values, vector.Values[index])
			}
		}
		filtered.ProjectionVectors[vectorIndex].Values = values
	}
	return filtered
}

func directBitmapListExpr(expr qsbridge.Expr) (qsbridge.ListExpr, bool) {
	switch typed := expr.(type) {
	case qsbridge.ListExpr:
		return typed, true
	case *qsbridge.ListExpr:
		if typed != nil {
			return *typed, true
		}
	}
	return qsbridge.ListExpr{}, false
}

func directBitmapResidualDiagnostics(message string) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, message),
	}
}

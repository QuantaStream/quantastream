package qsbridge

import "fmt"

func inMemoryNeedsEvaluatedProjection(query QueryIR) bool {
	for _, projection := range query.Projection {
		if _, ok := inMemoryProjectionField(projection.Expr); !ok {
			return true
		}
	}
	return false
}

func inMemorySupportedProjectionExpr(expr Expr) bool {
	if _, ok := inMemoryAggregateRef(expr); ok {
		return true
	}
	return inMemorySupportedEvaluatedExpr(expr)
}

func inMemorySupportedEvaluatedExpr(expr Expr) bool {
	switch typed := expr.(type) {
	case FieldExpr, *FieldExpr, LiteralExpr, *LiteralExpr:
		return true
	case BinaryExpr:
		return inMemoryArithmeticOp(typed.Op) && inMemorySupportedEvaluatedExpr(typed.Left) && inMemorySupportedEvaluatedExpr(typed.Right)
	case *BinaryExpr:
		return typed != nil && inMemoryArithmeticOp(typed.Op) && inMemorySupportedEvaluatedExpr(typed.Left) && inMemorySupportedEvaluatedExpr(typed.Right)
	default:
		return false
	}
}

func inMemorySupportedProjectionOnlyExpr(expr Expr) bool {
	switch typed := expr.(type) {
	case LiteralExpr, *LiteralExpr:
		return true
	case BinaryExpr:
		return inMemoryArithmeticOp(typed.Op) && inMemorySupportedProjectionOnlyExpr(typed.Left) && inMemorySupportedProjectionOnlyExpr(typed.Right)
	case *BinaryExpr:
		return typed != nil && inMemoryArithmeticOp(typed.Op) && inMemorySupportedProjectionOnlyExpr(typed.Left) && inMemorySupportedProjectionOnlyExpr(typed.Right)
	default:
		return false
	}
}

func inMemorySupportedProjectionOnlyBooleanExpr(expr Expr) bool {
	binary, ok := inMemoryBinaryExpr(expr)
	if !ok {
		return inMemorySupportedProjectionOnlyValueExpr(expr)
	}
	switch binary.Op {
	case BinaryOpAnd, BinaryOpOr:
		return inMemorySupportedProjectionOnlyBooleanExpr(binary.Left) && inMemorySupportedProjectionOnlyBooleanExpr(binary.Right)
	case BinaryOpEqual, BinaryOpNotEqual, BinaryOpLess, BinaryOpLessEqual, BinaryOpGreater, BinaryOpGreaterEqual:
		return inMemorySupportedProjectionOnlyValueExpr(binary.Left) && inMemorySupportedProjectionOnlyValueExpr(binary.Right)
	default:
		return false
	}
}

func inMemorySupportedProjectionOnlyValueExpr(expr Expr) bool {
	if _, ok := inMemoryLiteralExpr(expr); ok {
		return true
	}
	if _, ok := inMemoryParameterExpr(expr); ok {
		return true
	}
	binary, ok := inMemoryBinaryExpr(expr)
	return ok && inMemoryArithmeticOp(binary.Op) &&
		inMemorySupportedProjectionOnlyValueExpr(binary.Left) &&
		inMemorySupportedProjectionOnlyValueExpr(binary.Right)
}

func inMemoryProjectionOnlyMatches(query QueryIR, parameters ParameterBindingSet) (bool, Diagnostic, bool) {
	for _, predicate := range query.Predicates {
		matched, diagnostic, ok := inMemoryEvalProjectionOnlyBooleanExpr(predicate.Expr, parameters)
		if !ok {
			return false, diagnostic, false
		}
		if !matched {
			return false, Diagnostic{}, true
		}
	}
	if query.WhereExpr != nil {
		return inMemoryEvalProjectionOnlyBooleanExpr(query.WhereExpr, parameters)
	}
	return true, Diagnostic{}, true
}

// ProjectionOnlySelectRows evaluates a source-free SELECT list with optional scalar WHERE predicates.
func ProjectionOnlySelectRows(projections []ProjectionColumn, predicates []Predicate, whereExpr Expr, parameters ParameterBindingSet) ([]ResultRow, Diagnostic, bool) {
	for _, projection := range projections {
		if !inMemorySupportedProjectionOnlyExpr(projection.Expr) {
			return nil, inMemoryNativeDiagnostic("projection-only SELECT supports only scalar literal and arithmetic projections"), false
		}
	}
	matched, diagnostic, ok := ProjectionOnlySelectMatches(predicates, whereExpr, parameters)
	if !ok {
		return nil, diagnostic, false
	}
	candidates := []inMemoryNativeCandidate(nil)
	if matched {
		candidates = append(candidates, inMemoryNativeCandidate{
			Rownum: 1,
			Row:    InMemoryNativeRow{},
		})
	}
	return inMemoryEvaluateProjectionRows(projections, candidates)
}

// ProjectionOnlySelectMatches evaluates scalar WHERE predicates for source-free SELECT statements.
func ProjectionOnlySelectMatches(predicates []Predicate, whereExpr Expr, parameters ParameterBindingSet) (bool, Diagnostic, bool) {
	for _, predicate := range predicates {
		if !inMemorySupportedProjectionOnlyBooleanExpr(predicate.Expr) {
			return false, inMemoryNativeDiagnostic("projection-only SELECT WHERE supports only scalar boolean predicates"), false
		}
	}
	if whereExpr != nil && !inMemorySupportedProjectionOnlyBooleanExpr(whereExpr) {
		return false, inMemoryNativeDiagnostic("projection-only SELECT WHERE supports only scalar boolean predicates"), false
	}
	return inMemoryProjectionOnlyMatches(QueryIR{
		Predicates: predicates,
		WhereExpr:  whereExpr,
	}, parameters)
}

func inMemoryEvaluateProjectionRows(projections []ProjectionColumn, candidates []inMemoryNativeCandidate) ([]ResultRow, Diagnostic, bool) {
	rows := make([]ResultRow, 0, len(candidates))
	for _, candidate := range candidates {
		row := make(ResultRow, 0, len(projections))
		for _, projection := range projections {
			cell, diagnostic, ok := inMemoryEvalExpr(projection.Expr, candidate.Row)
			if !ok {
				return nil, diagnostic, false
			}
			row = append(row, cell)
		}
		rows = append(rows, row)
	}
	return rows, Diagnostic{}, true
}

func inMemoryEvalProjectionOnlyBooleanExpr(expr Expr, parameters ParameterBindingSet) (bool, Diagnostic, bool) {
	if binary, ok := inMemoryBinaryExpr(expr); ok {
		switch binary.Op {
		case BinaryOpAnd:
			left, diagnostic, ok := inMemoryEvalProjectionOnlyBooleanExpr(binary.Left, parameters)
			if !ok || !left {
				return left, diagnostic, ok
			}
			return inMemoryEvalProjectionOnlyBooleanExpr(binary.Right, parameters)
		case BinaryOpOr:
			left, diagnostic, ok := inMemoryEvalProjectionOnlyBooleanExpr(binary.Left, parameters)
			if !ok || left {
				return left, diagnostic, ok
			}
			return inMemoryEvalProjectionOnlyBooleanExpr(binary.Right, parameters)
		case BinaryOpEqual, BinaryOpNotEqual, BinaryOpLess, BinaryOpLessEqual, BinaryOpGreater, BinaryOpGreaterEqual:
			left, diagnostic, ok := inMemoryEvalProjectionOnlyValueExpr(binary.Left, parameters)
			if !ok {
				return false, diagnostic, false
			}
			right, diagnostic, ok := inMemoryEvalProjectionOnlyValueExpr(binary.Right, parameters)
			if !ok {
				return false, diagnostic, false
			}
			return inMemoryCompareProjectionOnlyCells(binary.Op, left, right), Diagnostic{}, true
		default:
			return false, inMemoryNativeDiagnostic("projection-only WHERE operator %q is not supported", binary.Op), false
		}
	}
	cell, diagnostic, ok := inMemoryEvalProjectionOnlyValueExpr(expr, parameters)
	if !ok {
		return false, diagnostic, false
	}
	return inMemoryProjectionOnlyTruthy(cell), Diagnostic{}, true
}

func inMemoryEvalProjectionOnlyValueExpr(expr Expr, parameters ParameterBindingSet) (ResultCell, Diagnostic, bool) {
	switch typed := expr.(type) {
	case LiteralExpr:
		return ResultCell{Kind: typed.Kind, Value: typed.Value}, Diagnostic{}, true
	case *LiteralExpr:
		if typed == nil {
			return ResultCell{}, inMemoryNativeDiagnostic("literal expression is nil"), false
		}
		return inMemoryEvalProjectionOnlyValueExpr(*typed, parameters)
	case ParameterExpr:
		return inMemoryProjectionOnlyParameterValue(typed, parameters)
	case *ParameterExpr:
		if typed == nil {
			return ResultCell{}, inMemoryNativeDiagnostic("parameter expression is nil"), false
		}
		return inMemoryProjectionOnlyParameterValue(*typed, parameters)
	case BinaryExpr:
		return inMemoryEvalProjectionOnlyArithmeticExpr(typed, parameters)
	case *BinaryExpr:
		if typed == nil {
			return ResultCell{}, inMemoryNativeDiagnostic("binary expression is nil"), false
		}
		return inMemoryEvalProjectionOnlyArithmeticExpr(*typed, parameters)
	default:
		return ResultCell{}, inMemoryNativeDiagnostic("projection-only expression %T is not supported", expr), false
	}
}

func inMemoryProjectionOnlyParameterValue(parameter ParameterExpr, parameters ParameterBindingSet) (ResultCell, Diagnostic, bool) {
	for _, binding := range parameters.Bindings {
		if parameterRefKey(binding.Ref) != parameterRefKey(parameter.Ref) {
			continue
		}
		return ResultCell{Kind: binding.Value.Kind, Value: binding.Value.Value}, Diagnostic{}, true
	}
	return ResultCell{}, inMemoryNativeDiagnostic("prepared-statement parameter %d is not bound", parameter.Ref.Index), false
}

func inMemoryEvalProjectionOnlyArithmeticExpr(expr BinaryExpr, parameters ParameterBindingSet) (ResultCell, Diagnostic, bool) {
	if !inMemoryArithmeticOp(expr.Op) {
		return ResultCell{}, inMemoryNativeDiagnostic("projection-only arithmetic operator %q is not supported", expr.Op), false
	}
	left, diagnostic, ok := inMemoryEvalProjectionOnlyValueExpr(expr.Left, parameters)
	if !ok {
		return ResultCell{}, diagnostic, false
	}
	right, diagnostic, ok := inMemoryEvalProjectionOnlyValueExpr(expr.Right, parameters)
	if !ok {
		return ResultCell{}, diagnostic, false
	}
	if left.Value == nil || right.Value == nil {
		return ResultCell{Kind: ValueNull, Value: nil}, Diagnostic{}, true
	}
	leftNumber, leftOK := inMemoryNumericValue(left.Value)
	rightNumber, rightOK := inMemoryNumericValue(right.Value)
	if !leftOK || !rightOK {
		return ResultCell{}, inMemoryNativeDiagnostic("projection-only arithmetic operands must be numeric"), false
	}
	switch expr.Op {
	case BinaryOpAdd:
		return ResultCell{Kind: ValueFloat, Value: leftNumber + rightNumber}, Diagnostic{}, true
	case BinaryOpSubtract:
		return ResultCell{Kind: ValueFloat, Value: leftNumber - rightNumber}, Diagnostic{}, true
	case BinaryOpMultiply:
		return ResultCell{Kind: ValueFloat, Value: leftNumber * rightNumber}, Diagnostic{}, true
	case BinaryOpDivide:
		if rightNumber == 0 {
			return ResultCell{Kind: ValueNull, Value: nil}, Diagnostic{}, true
		}
		return ResultCell{Kind: ValueFloat, Value: leftNumber / rightNumber}, Diagnostic{}, true
	default:
		return ResultCell{}, inMemoryNativeDiagnostic("projection-only arithmetic operator %q is not supported", expr.Op), false
	}
}

func inMemoryAppendResultRowsChunks(result ExecutionResult, rows []ResultRow, batchSize int) ExecutionResult {
	if batchSize <= 0 || batchSize >= len(rows) {
		return result.WithChunk(ResultChunk{Rows: rows, Final: true})
	}
	sequence := 0
	for start := 0; start < len(rows); start += batchSize {
		end := start + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		result = result.WithChunk(ResultChunk{
			Rows:     append([]ResultRow(nil), rows[start:end]...),
			Sequence: sequence,
			Final:    end == len(rows),
		})
		sequence++
	}
	return result
}

func inMemoryCompareProjectionOnlyCells(op BinaryOp, left ResultCell, right ResultCell) bool {
	if left.Value == nil || right.Value == nil {
		switch op {
		case BinaryOpEqual:
			return left.Value == nil && right.Value == nil
		case BinaryOpNotEqual:
			return !(left.Value == nil && right.Value == nil)
		default:
			return false
		}
	}
	if inMemoryIsNumericKind(left.Kind) || inMemoryIsNumericKind(right.Kind) {
		leftNumber, leftOK := inMemoryNumericValue(left.Value)
		rightNumber, rightOK := inMemoryNumericValue(right.Value)
		if !leftOK || !rightOK {
			return false
		}
		switch op {
		case BinaryOpNotEqual:
			return leftNumber != rightNumber
		default:
			return inMemoryCompareFloat(op, leftNumber, rightNumber)
		}
	}
	leftString := fmt.Sprint(left.Value)
	rightString := fmt.Sprint(right.Value)
	switch op {
	case BinaryOpNotEqual:
		return leftString != rightString
	default:
		return inMemoryCompareString(op, leftString, rightString)
	}
}

func inMemoryProjectionOnlyTruthy(cell ResultCell) bool {
	if cell.Value == nil {
		return false
	}
	if value, ok := cell.Value.(bool); ok {
		return value
	}
	if number, ok := inMemoryNumericValue(cell.Value); ok {
		return number != 0
	}
	return fmt.Sprint(cell.Value) != ""
}

func inMemoryEvalExpr(expr Expr, row InMemoryNativeRow) (ResultCell, Diagnostic, bool) {
	switch typed := expr.(type) {
	case FieldExpr:
		cell, ok := inMemoryFieldCell(row, typed.Ref)
		if !ok {
			return ResultCell{}, inMemoryNativeDiagnostic("field %q is not present in the in-memory row", typed.Ref.Name), false
		}
		return cell, Diagnostic{}, true
	case *FieldExpr:
		if typed == nil {
			return ResultCell{}, inMemoryNativeDiagnostic("field expression is nil"), false
		}
		return inMemoryEvalExpr(*typed, row)
	case LiteralExpr:
		return ResultCell{Kind: typed.Kind, Value: typed.Value}, Diagnostic{}, true
	case *LiteralExpr:
		if typed == nil {
			return ResultCell{}, inMemoryNativeDiagnostic("literal expression is nil"), false
		}
		return inMemoryEvalExpr(*typed, row)
	case BinaryExpr:
		return inMemoryEvalBinaryExpr(typed, row)
	case *BinaryExpr:
		if typed == nil {
			return ResultCell{}, inMemoryNativeDiagnostic("binary expression is nil"), false
		}
		return inMemoryEvalBinaryExpr(*typed, row)
	default:
		return ResultCell{}, inMemoryNativeDiagnostic("projection expression %T is not supported", expr), false
	}
}

func inMemoryEvalBinaryExpr(expr BinaryExpr, row InMemoryNativeRow) (ResultCell, Diagnostic, bool) {
	if !inMemoryArithmeticOp(expr.Op) {
		return ResultCell{}, inMemoryNativeDiagnostic("binary projection operator %q is not supported", expr.Op), false
	}
	left, diagnostic, ok := inMemoryEvalExpr(expr.Left, row)
	if !ok {
		return ResultCell{}, diagnostic, false
	}
	right, diagnostic, ok := inMemoryEvalExpr(expr.Right, row)
	if !ok {
		return ResultCell{}, diagnostic, false
	}
	if left.Value == nil || right.Value == nil {
		return ResultCell{Kind: ValueNull, Value: nil}, Diagnostic{}, true
	}
	leftNumber, leftOK := inMemoryNumericValue(left.Value)
	rightNumber, rightOK := inMemoryNumericValue(right.Value)
	if !leftOK || !rightOK {
		return ResultCell{}, inMemoryNativeDiagnostic("arithmetic projection operands must be numeric"), false
	}
	switch expr.Op {
	case BinaryOpAdd:
		return ResultCell{Kind: ValueFloat, Value: leftNumber + rightNumber}, Diagnostic{}, true
	case BinaryOpSubtract:
		return ResultCell{Kind: ValueFloat, Value: leftNumber - rightNumber}, Diagnostic{}, true
	case BinaryOpMultiply:
		return ResultCell{Kind: ValueFloat, Value: leftNumber * rightNumber}, Diagnostic{}, true
	case BinaryOpDivide:
		if rightNumber == 0 {
			return ResultCell{Kind: ValueNull, Value: nil}, Diagnostic{}, true
		}
		return ResultCell{Kind: ValueFloat, Value: leftNumber / rightNumber}, Diagnostic{}, true
	default:
		return ResultCell{}, inMemoryNativeDiagnostic("arithmetic operator %q is not supported", expr.Op), false
	}
}

func inMemoryArithmeticOp(op BinaryOp) bool {
	switch op {
	case BinaryOpAdd, BinaryOpSubtract, BinaryOpMultiply, BinaryOpDivide:
		return true
	default:
		return false
	}
}

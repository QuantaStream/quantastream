package qsbridge

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

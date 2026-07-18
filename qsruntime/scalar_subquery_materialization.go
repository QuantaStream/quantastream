package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// materializeScalarSubqueries replaces typed scalar-subquery expressions in a
// prepared request with literal values before Quanta intermediate lowering.
func (r SQLRuntime) materializeScalarSubqueries(ctx context.Context, request qsbridge.ExecutionRequest) (qsbridge.ExecutionRequest, qsbridge.DiagnosticSet, error) {
	query, diagnostics, changed, err := r.materializeScalarSubqueriesInQuery(ctx, request.Bound.Prepared.Query, request.Options)
	if err != nil || diagnostics.BlocksNative() || !changed {
		return request, diagnostics, err
	}
	request.Bound.Prepared.Query = query
	request.Bound.Prepared.Parameters = query.RequiredParameters()
	request.Bound.Prepared.ResultColumns = query.ResultColumns()
	request.Bound.Prepared.Result = query.Result
	request.Bound.Prepared.Access = query.RequiredAccess()
	request.Bound.Prepared.Supported = request.Bound.Prepared.Supported && !diagnostics.BlocksNative()
	request.Bound.Diagnostics = append(request.Bound.Diagnostics, diagnostics...)
	request.Bound.Supported = request.Bound.Supported && !request.Bound.Diagnostics.BlocksNative()
	request.Diagnostics = append(request.Diagnostics, diagnostics...)
	request.Supported = request.Bound.SupportedForExecution() && !request.Diagnostics.BlocksNative()
	request.Result = request.Bound.Prepared.Result
	request.ResultColumns = append([]qsbridge.ResultColumn(nil), request.Bound.Prepared.ResultColumns...)
	return request, diagnostics, nil
}

func (r SQLRuntime) materializeScalarSubqueriesInQuery(ctx context.Context, query qsbridge.QueryIR, options qsbridge.ExecutionOptions) (qsbridge.QueryIR, qsbridge.DiagnosticSet, bool, error) {
	diagnostics := qsbridge.DiagnosticSet(nil)
	changed := false
	for i := range query.Predicates {
		expr, exprDiagnostics, exprChanged, err := r.materializeScalarSubqueriesInExpr(ctx, query.Predicates[i].Expr, options)
		diagnostics = append(diagnostics, exprDiagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return query, diagnostics, changed, err
		}
		if exprChanged {
			query.Predicates[i].Expr = expr
			changed = true
		}
	}
	if query.WhereExpr != nil {
		expr, exprDiagnostics, exprChanged, err := r.materializeScalarSubqueriesInExpr(ctx, query.WhereExpr, options)
		diagnostics = append(diagnostics, exprDiagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return query, diagnostics, changed, err
		}
		if exprChanged {
			query.WhereExpr = expr
			changed = true
		}
	}
	for i := range query.Having {
		expr, exprDiagnostics, exprChanged, err := r.materializeScalarSubqueriesInExpr(ctx, query.Having[i].Expr, options)
		diagnostics = append(diagnostics, exprDiagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return query, diagnostics, changed, err
		}
		if exprChanged {
			query.Having[i].Expr = expr
			changed = true
		}
	}
	return query, diagnostics, changed, nil
}

func (r SQLRuntime) materializeScalarSubqueriesInExpr(ctx context.Context, expr qsbridge.Expr, options qsbridge.ExecutionOptions) (qsbridge.Expr, qsbridge.DiagnosticSet, bool, error) {
	switch typed := expr.(type) {
	case nil:
		return nil, nil, false, nil
	case qsbridge.ScalarSubqueryExpr:
		literal, diagnostics, err := r.materializeScalarSubqueryLiteral(ctx, typed, options)
		return literal, diagnostics, true, err
	case *qsbridge.ScalarSubqueryExpr:
		if typed == nil {
			return expr, nil, false, nil
		}
		literal, diagnostics, err := r.materializeScalarSubqueryLiteral(ctx, *typed, options)
		return literal, diagnostics, true, err
	case qsbridge.BinaryExpr:
		left, leftDiagnostics, leftChanged, err := r.materializeScalarSubqueriesInExpr(ctx, typed.Left, options)
		if err != nil || leftDiagnostics.BlocksNative() {
			return expr, leftDiagnostics, leftChanged, err
		}
		right, rightDiagnostics, rightChanged, err := r.materializeScalarSubqueriesInExpr(ctx, typed.Right, options)
		diagnostics := append(leftDiagnostics, rightDiagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return expr, diagnostics, leftChanged || rightChanged, err
		}
		if leftChanged || rightChanged {
			typed.Left = left
			typed.Right = right
			return typed, diagnostics, true, nil
		}
	case *qsbridge.BinaryExpr:
		if typed == nil {
			return expr, nil, false, nil
		}
		replaced, diagnostics, changed, err := r.materializeScalarSubqueriesInExpr(ctx, qsbridge.BinaryExpr(*typed), options)
		if err != nil || diagnostics.BlocksNative() || !changed {
			return expr, diagnostics, changed, err
		}
		binary := replaced.(qsbridge.BinaryExpr)
		return &binary, diagnostics, true, nil
	case qsbridge.ListExpr:
		changed := false
		diagnostics := qsbridge.DiagnosticSet(nil)
		items := make([]qsbridge.Expr, 0, len(typed.Items))
		for _, item := range typed.Items {
			replaced, itemDiagnostics, itemChanged, err := r.materializeScalarSubqueriesInExpr(ctx, item, options)
			diagnostics = append(diagnostics, itemDiagnostics...)
			if err != nil || diagnostics.BlocksNative() {
				return expr, diagnostics, changed || itemChanged, err
			}
			items = append(items, replaced)
			changed = changed || itemChanged
		}
		if changed {
			typed.Items = items
			return typed, diagnostics, true, nil
		}
	case qsbridge.CallExpr:
		changed := false
		diagnostics := qsbridge.DiagnosticSet(nil)
		args := make([]qsbridge.Expr, 0, len(typed.Args))
		for _, arg := range typed.Args {
			replaced, argDiagnostics, argChanged, err := r.materializeScalarSubqueriesInExpr(ctx, arg, options)
			diagnostics = append(diagnostics, argDiagnostics...)
			if err != nil || diagnostics.BlocksNative() {
				return expr, diagnostics, changed || argChanged, err
			}
			args = append(args, replaced)
			changed = changed || argChanged
		}
		if changed {
			typed.Args = args
			return typed, diagnostics, true, nil
		}
	case qsbridge.SearchedCaseExpr:
		changed := false
		diagnostics := qsbridge.DiagnosticSet(nil)
		whens := make([]qsbridge.SearchedCaseWhen, 0, len(typed.Whens))
		for _, when := range typed.Whens {
			condition, conditionDiagnostics, conditionChanged, err := r.materializeScalarSubqueriesInExpr(ctx, when.Condition, options)
			diagnostics = append(diagnostics, conditionDiagnostics...)
			if err != nil || diagnostics.BlocksNative() {
				return expr, diagnostics, changed || conditionChanged, err
			}
			result, resultDiagnostics, resultChanged, err := r.materializeScalarSubqueriesInExpr(ctx, when.Result, options)
			diagnostics = append(diagnostics, resultDiagnostics...)
			if err != nil || diagnostics.BlocksNative() {
				return expr, diagnostics, changed || conditionChanged || resultChanged, err
			}
			whens = append(whens, qsbridge.SearchedCaseWhen{Condition: condition, Result: result})
			changed = changed || conditionChanged || resultChanged
		}
		elseExpr, elseDiagnostics, elseChanged, err := r.materializeScalarSubqueriesInExpr(ctx, typed.Else, options)
		diagnostics = append(diagnostics, elseDiagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return expr, diagnostics, changed || elseChanged, err
		}
		if changed || elseChanged {
			typed.Whens = whens
			typed.Else = elseExpr
			return typed, diagnostics, true, nil
		}
	}
	return expr, nil, false, nil
}

func (r SQLRuntime) materializeScalarSubqueryLiteral(ctx context.Context, expr qsbridge.ScalarSubqueryExpr, options qsbridge.ExecutionOptions) (qsbridge.LiteralExpr, qsbridge.DiagnosticSet, error) {
	if expr.SQL == "" {
		return qsbridge.Literal(qsbridge.ValueUnknown, nil), helperExecutionDiagnostic(PreflightHelperPlanScalarSubquery, "scalar subquery SQL is empty"), nil
	}
	result, err := r.ExecuteSQL(ctx, expr.SQL, options)
	diagnostics := append(qsbridge.DiagnosticSet(nil), result.Diagnostics...)
	diagnostics = append(diagnostics, result.Runtime.Diagnostics...)
	if err != nil || diagnostics.BlocksNative() {
		return qsbridge.Literal(qsbridge.ValueUnknown, nil), diagnostics, err
	}
	cell, cellDiagnostics := scalarSubqueryResultCell(result.Runtime.RowSet)
	diagnostics = append(diagnostics, cellDiagnostics...)
	if diagnostics.BlocksNative() {
		return qsbridge.Literal(qsbridge.ValueUnknown, nil), diagnostics, nil
	}
	return qsbridge.Literal(cell.Kind, cell.Value), diagnostics, nil
}

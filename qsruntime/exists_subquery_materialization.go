package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type existsSubqueryGateState struct {
	EmptyCandidateSet bool
}

// materializeExistsSubqueryGates replaces non-correlated EXISTS gates with
// boolean literals before bitmap lowering. A false WHERE gate seeds an empty
// candidate set so normal SQL result shaping, including count(*), still runs.
func (r SQLRuntime) materializeExistsSubqueryGates(ctx context.Context, request qsbridge.ExecutionRequest) (qsbridge.ExecutionRequest, existsSubqueryGateState, qsbridge.DiagnosticSet, error) {
	query, state, diagnostics, changed, err := r.materializeExistsSubqueriesInQuery(ctx, request.Bound.Prepared.Query, request.Options)
	if err != nil || diagnostics.BlocksNative() || !changed {
		return request, state, diagnostics, err
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
	return request, state, diagnostics, nil
}

func (r SQLRuntime) materializeExistsSubqueriesInQuery(ctx context.Context, query qsbridge.QueryIR, options qsbridge.ExecutionOptions) (qsbridge.QueryIR, existsSubqueryGateState, qsbridge.DiagnosticSet, bool, error) {
	state := existsSubqueryGateState{}
	diagnostics := qsbridge.DiagnosticSet(nil)
	changed := false
	for i := range query.Predicates {
		expr, exprDiagnostics, exprChanged, err := r.materializeExistsSubqueriesInExpr(ctx, query.Predicates[i].Expr, options)
		diagnostics = append(diagnostics, exprDiagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return query, state, diagnostics, changed, err
		}
		if exprChanged {
			query.Predicates[i].Expr = expr
			changed = true
		}
	}
	if query.WhereExpr != nil {
		expr, exprDiagnostics, exprChanged, err := r.materializeExistsSubqueriesInExpr(ctx, query.WhereExpr, options)
		diagnostics = append(diagnostics, exprDiagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return query, state, diagnostics, changed, err
		}
		if exprChanged {
			query.WhereExpr = expr
			changed = true
		}
	}
	if changed {
		query, state, changed = applyExistsGateLiterals(query, state, changed)
	}
	return query, state, diagnostics, changed, nil
}

func (r SQLRuntime) materializeExistsSubqueriesInExpr(ctx context.Context, expr qsbridge.Expr, options qsbridge.ExecutionOptions) (qsbridge.Expr, qsbridge.DiagnosticSet, bool, error) {
	switch typed := expr.(type) {
	case nil:
		return nil, nil, false, nil
	case qsbridge.ExistsSubqueryExpr:
		literal, diagnostics, err := r.materializeExistsSubqueryLiteral(ctx, typed, options)
		return literal, diagnostics, true, err
	case *qsbridge.ExistsSubqueryExpr:
		if typed == nil {
			return expr, nil, false, nil
		}
		literal, diagnostics, err := r.materializeExistsSubqueryLiteral(ctx, *typed, options)
		return literal, diagnostics, true, err
	case qsbridge.BinaryExpr:
		left, leftDiagnostics, leftChanged, err := r.materializeExistsSubqueriesInExpr(ctx, typed.Left, options)
		if err != nil || leftDiagnostics.BlocksNative() {
			return expr, leftDiagnostics, leftChanged, err
		}
		right, rightDiagnostics, rightChanged, err := r.materializeExistsSubqueriesInExpr(ctx, typed.Right, options)
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
		replaced, diagnostics, changed, err := r.materializeExistsSubqueriesInExpr(ctx, qsbridge.BinaryExpr(*typed), options)
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
			replaced, itemDiagnostics, itemChanged, err := r.materializeExistsSubqueriesInExpr(ctx, item, options)
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
			replaced, argDiagnostics, argChanged, err := r.materializeExistsSubqueriesInExpr(ctx, arg, options)
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
			condition, conditionDiagnostics, conditionChanged, err := r.materializeExistsSubqueriesInExpr(ctx, when.Condition, options)
			diagnostics = append(diagnostics, conditionDiagnostics...)
			if err != nil || diagnostics.BlocksNative() {
				return expr, diagnostics, changed || conditionChanged, err
			}
			result, resultDiagnostics, resultChanged, err := r.materializeExistsSubqueriesInExpr(ctx, when.Result, options)
			diagnostics = append(diagnostics, resultDiagnostics...)
			if err != nil || diagnostics.BlocksNative() {
				return expr, diagnostics, changed || conditionChanged || resultChanged, err
			}
			whens = append(whens, qsbridge.SearchedCaseWhen{Condition: condition, Result: result})
			changed = changed || conditionChanged || resultChanged
		}
		elseExpr, elseDiagnostics, elseChanged, err := r.materializeExistsSubqueriesInExpr(ctx, typed.Else, options)
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

func (r SQLRuntime) materializeExistsSubqueryLiteral(ctx context.Context, expr qsbridge.ExistsSubqueryExpr, options qsbridge.ExecutionOptions) (qsbridge.LiteralExpr, qsbridge.DiagnosticSet, error) {
	if expr.SQL == "" {
		return qsbridge.Literal(qsbridge.ValueUnknown, nil), helperExecutionDiagnostic(PreflightHelperPlanScalarSubquery, "EXISTS subquery SQL is empty"), nil
	}
	result, err := r.ExecuteSQL(ctx, expr.SQL, options)
	diagnostics := append(qsbridge.DiagnosticSet(nil), result.Diagnostics...)
	diagnostics = append(diagnostics, result.Runtime.Diagnostics...)
	if err != nil || diagnostics.BlocksNative() {
		return qsbridge.Literal(qsbridge.ValueUnknown, nil), diagnostics, err
	}
	exists := result.Runtime.Count > 0 || result.Runtime.RowSet.CandidateCount() > 0
	if expr.Negated {
		exists = !exists
	}
	return qsbridge.Literal(qsbridge.ValueBool, exists), diagnostics, nil
}

func applyExistsGateLiterals(query qsbridge.QueryIR, state existsSubqueryGateState, changed bool) (qsbridge.QueryIR, existsSubqueryGateState, bool) {
	predicates := make([]qsbridge.Predicate, 0, len(query.Predicates))
	for _, predicate := range query.Predicates {
		value, ok := literalBoolValue(predicate.Expr)
		if !ok {
			predicates = append(predicates, predicate)
			continue
		}
		changed = true
		if !value && predicate.Scope == qsbridge.PredicateScopeWhere {
			state.EmptyCandidateSet = true
		}
	}
	query.Predicates = predicates
	if query.WhereExpr != nil {
		if value, ok := literalBoolValue(query.WhereExpr); ok {
			changed = true
			query.WhereExpr = nil
			if !value {
				state.EmptyCandidateSet = true
			}
		}
	}
	return query, state, changed
}

func literalBoolValue(expr qsbridge.Expr) (bool, bool) {
	switch typed := expr.(type) {
	case qsbridge.LiteralExpr:
		value, ok := typed.Value.(bool)
		return value, ok && typed.Kind == qsbridge.ValueBool
	case *qsbridge.LiteralExpr:
		if typed == nil {
			return false, false
		}
		value, ok := typed.Value.(bool)
		return value, ok && typed.Kind == qsbridge.ValueBool
	default:
		return false, false
	}
}

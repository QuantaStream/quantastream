package qsruntime

import (
	"context"
	"fmt"
	"math/big"
	"sort"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type textSearchMaterializationState struct {
	EmptyCandidateSet bool
}

// materializeTextSearchPredicates resolves MATCH ... AGAINST terms to searchable
// string hashes before Quanta intermediate lowering.
func (r SQLRuntime) materializeTextSearchPredicates(ctx context.Context, request qsbridge.ExecutionRequest) (qsbridge.ExecutionRequest, textSearchMaterializationState, qsbridge.DiagnosticSet, error) {
	query, state, diagnostics, changed, err := r.materializeTextSearchInQuery(ctx, request.Bound.Prepared.Query, request.Bound.Parameters)
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

func (r SQLRuntime) materializeTextSearchInQuery(ctx context.Context, query qsbridge.QueryIR, parameters qsbridge.ParameterBindingSet) (qsbridge.QueryIR, textSearchMaterializationState, qsbridge.DiagnosticSet, bool, error) {
	state := textSearchMaterializationState{}
	diagnostics := qsbridge.DiagnosticSet(nil)
	changed := false
	if containsTextSearchExpr(query.WhereExpr) {
		return query, state, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedSQL,
				qsbridge.PhaseExecute,
				"MATCH ... AGAINST inside mixed boolean expressions is not supported yet",
			),
		}, false, nil
	}
	predicates := make([]qsbridge.Predicate, 0, len(query.Predicates))
	for _, predicate := range query.Predicates {
		textExpr, ok := qsbridge.AsTextSearchExpr(predicate.Expr)
		if !ok {
			predicates = append(predicates, predicate)
			continue
		}
		materialized, empty, predicateDiagnostics, err := r.materializeTextSearchPredicate(ctx, textExpr, parameters)
		diagnostics = append(diagnostics, predicateDiagnostics...)
		if err != nil || diagnostics.BlocksNative() {
			return query, state, diagnostics, changed, err
		}
		changed = true
		if empty {
			if predicate.Combinator == qsbridge.PredicateCombinatorOr {
				continue
			}
			state.EmptyCandidateSet = true
			continue
		}
		predicate.Expr = materialized
		predicates = append(predicates, predicate)
	}
	if changed {
		query.Predicates = predicates
	}
	return query, state, diagnostics, changed, nil
}

func (r SQLRuntime) materializeTextSearchPredicate(ctx context.Context, expr qsbridge.TextSearchExpr, parameters qsbridge.ParameterBindingSet) (qsbridge.TextSearchExpr, bool, qsbridge.DiagnosticSet, error) {
	terms, diagnostics := textSearchQueryTerms(expr.Query, parameters)
	if diagnostics.BlocksNative() {
		return expr, false, diagnostics, nil
	}
	matches, diagnostics, err := r.executeTextSearch(ctx, expr, terms)
	if err != nil || diagnostics.BlocksNative() {
		return expr, false, diagnostics, err
	}
	if len(matches) == 0 {
		return expr, true, nil, nil
	}
	hashes := textSearchHashValues(matches)
	return expr.WithHashes(hashes), false, nil, nil
}

func textSearchQueryTerms(expr qsbridge.Expr, parameters qsbridge.ParameterBindingSet) (string, qsbridge.DiagnosticSet) {
	switch typed := expr.(type) {
	case qsbridge.LiteralExpr:
		if typed.Kind == qsbridge.ValueString {
			if value, ok := typed.Value.(string); ok {
				return value, nil
			}
		}
	case *qsbridge.LiteralExpr:
		if typed != nil && typed.Kind == qsbridge.ValueString {
			if value, ok := typed.Value.(string); ok {
				return value, nil
			}
		}
	case qsbridge.ParameterExpr:
		return textSearchParameterTerms(typed.Ref, parameters)
	case *qsbridge.ParameterExpr:
		if typed != nil {
			return textSearchParameterTerms(typed.Ref, parameters)
		}
	}
	return "", qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			"MATCH ... AGAINST currently requires a string literal or bound string parameter",
		),
	}
}

func textSearchParameterTerms(ref qsbridge.ParameterRef, parameters qsbridge.ParameterBindingSet) (string, qsbridge.DiagnosticSet) {
	for _, binding := range parameters.Bindings {
		if !textSearchParameterRefMatches(binding.Ref, ref) {
			continue
		}
		if binding.Value.Kind == qsbridge.ValueString {
			if value, ok := binding.Value.Value.(string); ok {
				return value, nil
			}
		}
		return "", qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedSQL,
				qsbridge.PhaseExecute,
				"MATCH ... AGAINST parameter must be a string",
			),
		}
	}
	return "", qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			fmt.Sprintf("MATCH ... AGAINST parameter %d is not bound", ref.Index),
		),
	}
}

func textSearchParameterRefMatches(left qsbridge.ParameterRef, right qsbridge.ParameterRef) bool {
	if left.Name != "" || right.Name != "" {
		return left.Name != "" && left.Name == right.Name
	}
	return left.Index == right.Index
}

func (r SQLRuntime) executeTextSearch(ctx context.Context, expr qsbridge.TextSearchExpr, terms string) (map[uint64]struct{}, qsbridge.DiagnosticSet, error) {
	provider, ok := r.directTextSearchSessionProvider()
	if !ok {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedSQL,
				qsbridge.PhaseExecute,
				"MATCH ... AGAINST requires a direct StringSearch-capable runtime",
			),
		}, nil
	}
	request := ExecutionRequest{
		SourceIndexes: []string{expr.Field.Table.Table},
		Sources:       []qsbridge.TableInstance{expr.Field.Table},
		Route:         DirectQIABRoute(),
	}
	handle, diagnostics, err := provider.BorrowDirectSession(ctx, request)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	defer func() {
		_ = handle.Release(ctx)
	}()
	searchHandle, ok := handle.(DirectStringSearchSessionHandle)
	if !ok {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedSQL,
				qsbridge.PhaseExecute,
				"direct session handle does not expose StringSearch",
			),
		}, nil
	}
	matches, diagnostics, err := searchHandle.SearchStringIndex(ctx, terms)
	if err != nil || diagnostics.BlocksNative() {
		return nil, diagnostics, err
	}
	return matches, nil, nil
}

func (r SQLRuntime) directTextSearchSessionProvider() (DirectSessionProvider, bool) {
	switch executor := r.Environment.Execution.Selector.Direct.(type) {
	case DirectExecutor:
		return directTextSearchSessionProviderFromRuntime(executor.Runtime)
	case *DirectExecutor:
		if executor != nil {
			return directTextSearchSessionProviderFromRuntime(executor.Runtime)
		}
	}
	return nil, false
}

func directTextSearchSessionProviderFromRuntime(runtime DirectRuntime) (DirectSessionProvider, bool) {
	switch typed := runtime.(type) {
	case DirectBitmapRuntime:
		return typed.Sessions, typed.Sessions != nil
	case *DirectBitmapRuntime:
		if typed != nil {
			return typed.Sessions, typed.Sessions != nil
		}
	}
	return nil, false
}

func textSearchHashValues(matches map[uint64]struct{}) []*big.Int {
	values := make([]uint64, 0, len(matches))
	for value := range matches {
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i] < values[j]
	})
	hashes := make([]*big.Int, 0, len(values))
	for _, value := range values {
		hashes = append(hashes, new(big.Int).SetUint64(value))
	}
	return hashes
}

func containsTextSearchExpr(expr qsbridge.Expr) bool {
	if _, ok := qsbridge.AsTextSearchExpr(expr); ok {
		return true
	}
	switch typed := expr.(type) {
	case nil:
		return false
	case qsbridge.BinaryExpr:
		return containsTextSearchExpr(typed.Left) || containsTextSearchExpr(typed.Right)
	case *qsbridge.BinaryExpr:
		return typed != nil && (containsTextSearchExpr(typed.Left) || containsTextSearchExpr(typed.Right))
	case qsbridge.ListExpr:
		for _, item := range typed.Items {
			if containsTextSearchExpr(item) {
				return true
			}
		}
	case *qsbridge.ListExpr:
		if typed == nil {
			return false
		}
		for _, item := range typed.Items {
			if containsTextSearchExpr(item) {
				return true
			}
		}
	case qsbridge.CallExpr:
		for _, arg := range typed.Args {
			if containsTextSearchExpr(arg) {
				return true
			}
		}
	case *qsbridge.CallExpr:
		if typed == nil {
			return false
		}
		for _, arg := range typed.Args {
			if containsTextSearchExpr(arg) {
				return true
			}
		}
	case qsbridge.SearchedCaseExpr:
		for _, when := range typed.Whens {
			if containsTextSearchExpr(when.Condition) || containsTextSearchExpr(when.Result) {
				return true
			}
		}
		return containsTextSearchExpr(typed.Else)
	case *qsbridge.SearchedCaseExpr:
		if typed == nil {
			return false
		}
		for _, when := range typed.Whens {
			if containsTextSearchExpr(when.Condition) || containsTextSearchExpr(when.Result) {
				return true
			}
		}
		return containsTextSearchExpr(typed.Else)
	}
	return false
}

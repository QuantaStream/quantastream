package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// SQLRuntime is the SQL-facing runtime facade before protocol-specific plumbing.
type SQLRuntime struct {
	Environment         RuntimeEnvironment
	Parser              qsbridge.ParserBridge
	Lowerer             qsbridge.QuantaIntermediateLowerer
	DefaultSchema       string
	CatalogVersion      qsbridge.CatalogVersion
	Session             qsbridge.SessionContext
	Scope               qsbridge.PhysicalScope
	PreflightHelpers    PreflightHelperExecutor
	NativeSubquerySteps qsbridge.NativeSubqueryStepExecutor
	// EnableFilterExpressions allows a runtime to execute grouped boolean filter trees.
	EnableFilterExpressions bool
}

// SQLExecutionResult captures each stage from SQL planning through runtime execution.
type SQLExecutionResult struct {
	Prepared     qsbridge.PreparedPlan
	Request      qsbridge.ExecutionRequest
	Intermediate qsbridge.QuantaIntermediateQuery
	Runtime      ExecutionResult
	Diagnostics  qsbridge.DiagnosticSet
	Preflight    PreflightRewriteSummary
}

// SQLInspectionResult captures SQL planning, lowering, and runtime inspection without execution.
type SQLInspectionResult struct {
	Prepared               qsbridge.PreparedPlan
	Request                qsbridge.ExecutionRequest
	Intermediate           qsbridge.QuantaIntermediateQuery
	Runtime                ExecutionInspection
	Diagnostics            qsbridge.DiagnosticSet
	FilterExecutionEnabled bool
}

// Supported reports whether SQL planning, lowering, and execution completed without blockers.
func (r SQLExecutionResult) Supported() bool {
	return !r.Diagnostics.BlocksNative() && !r.Runtime.Diagnostics.BlocksNative()
}

// Supported reports whether SQL planning, lowering, and runtime inspection completed without blockers.
func (r SQLInspectionResult) Supported() bool {
	return !r.Diagnostics.BlocksNative() && !r.Runtime.Diagnostics.BlocksNative()
}

// Planner returns a qsbridge planner bound to the runtime environment catalog.
func (r SQLRuntime) Planner() qsbridge.Planner {
	return qsbridge.Planner{
		Parser:         r.Parser,
		Catalog:        r.Environment.Catalog,
		DefaultSchema:  r.DefaultSchema,
		CatalogVersion: r.CatalogVersion,
		Session:        r.Session.Clone(),
		Scope:          r.Scope,
	}
}

// Plan parses and plans SQL through qsbridge without executing it.
func (r SQLRuntime) Plan(sql string) qsbridge.PlanResult {
	return r.Planner().Plan(sql)
}

// ExecuteSQL prepares, lowers, and executes SQL through the runtime environment.
func (r SQLRuntime) ExecuteSQL(ctx context.Context, sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) (SQLExecutionResult, error) {
	preflight, err := r.applyPreflightRewrites(ctx, sql, options, values...)
	if err != nil || preflight.Diagnostics.BlocksNative() {
		return SQLExecutionResult{Diagnostics: preflight.Diagnostics, Preflight: preflight.Preflight}, err
	}
	service := qsbridge.NewPlanningService(r.Planner(), nil)
	prepared, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{SQL: preflight.SQL, Optimization: preflight.Optimization}, options, values...)
	request = applyPreflightPlanningState(request, preflight)
	prepared = request.Bound.Prepared
	request, scalarDiagnostics, err := r.materializeScalarSubqueries(ctx, request)
	if err != nil || scalarDiagnostics.BlocksNative() {
		return SQLExecutionResult{
			Prepared:    request.Bound.Prepared,
			Request:     request,
			Diagnostics: scalarDiagnostics,
			Preflight:   preflight.Preflight,
		}, err
	}
	var existsGate existsSubqueryGateState
	request, existsGate, existsDiagnostics, err := r.materializeExistsSubqueryGates(ctx, request)
	if err != nil || existsDiagnostics.BlocksNative() {
		return SQLExecutionResult{
			Prepared:    request.Bound.Prepared,
			Request:     request,
			Diagnostics: existsDiagnostics,
			Preflight:   preflight.Preflight,
		}, err
	}
	prepared = request.Bound.Prepared
	result := SQLExecutionResult{
		Prepared:    prepared,
		Request:     request,
		Diagnostics: append(qsbridge.DiagnosticSet(nil), request.Diagnostics...),
		Preflight:   preflight.Preflight,
	}
	if result.Diagnostics.BlocksNative() {
		if r.EnableFilterExpressions && prepared.Kind == qsbridge.QueryKindSelect && request.Bound.Prepared.Query.Kind == qsbridge.QueryKindSelect {
			intermediate, diagnostics := r.Lowerer.LowerQuery(request.Bound.Prepared.Query, request.Bound.Parameters)
			result.Intermediate = intermediate
			result.Diagnostics = append(result.Diagnostics, diagnostics...)
			if diagnostics.BlocksNative() || intermediate.Filter.Empty() {
				return result, nil
			}
			result.Diagnostics = runtimeDiagnosticsWithoutCode(result.Diagnostics, qsbridge.DiagnosticMixedBooleanPredicate)
			if result.Diagnostics.BlocksNative() {
				return result, nil
			}
			runtimeRequest := applyPreflightRuntimeState(NewSQLExecutionRequest(intermediate, request), preflight)
			if existsGate.EmptyCandidateSet {
				runtimeRequest = withEmptyCandidateSet(runtimeRequest)
			}
			runtimeResult, err := r.ExecutePrepared(ctx, runtimeRequest)
			result.Runtime = runtimeResult
			result.Diagnostics = append(result.Diagnostics, runtimeResult.Diagnostics...)
			return result, err
		}
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindSession {
		result.Runtime = ExecutionResult{Statement: cloneStatementResult(request.Statement)}
		return result, nil
	}
	if prepared.Kind != qsbridge.QueryKindSelect {
		intermediate, diagnostics := r.Lowerer.LowerExecutionRequest(request)
		result.Intermediate = intermediate
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if result.Diagnostics.BlocksNative() {
			return result, nil
		}
		runtimeResult, err := r.ExecutePrepared(ctx, applyPreflightRuntimeState(NewSQLExecutionRequest(intermediate, request), preflight))
		result.Runtime = runtimeResult
		result.Diagnostics = append(result.Diagnostics, runtimeResult.Diagnostics...)
		return result, err
	}

	if runtimeResult, diagnostics, ok := constantProjectionExecutionResult(request); ok {
		result.Runtime = runtimeResult
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		return result, nil
	}

	intermediate, diagnostics := r.Lowerer.LowerExecutionRequest(request)
	result.Intermediate = intermediate
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}

	runtimeRequest := applyPreflightRuntimeState(NewSQLExecutionRequest(intermediate, request), preflight)
	if existsGate.EmptyCandidateSet {
		runtimeRequest = withEmptyCandidateSet(runtimeRequest)
	}
	runtimeResult, err := r.ExecutePrepared(ctx, runtimeRequest)
	result.Runtime = runtimeResult
	result.Diagnostics = append(result.Diagnostics, runtimeResult.Diagnostics...)
	return result, err
}

func applyPreflightPlanningState(request qsbridge.ExecutionRequest, preflight PreflightRewriteResult) qsbridge.ExecutionRequest {
	if preflight.ReplacementExpr == nil && len(preflight.NativePredicates.CorrelatedAggregate) == 0 {
		return request
	}
	query := request.Bound.Prepared.Query
	if preflight.ReplacementExpr != nil {
		query.Predicates = append(append([]qsbridge.Predicate(nil), query.Predicates...), qsbridge.Predicate{
			Expr:      preflight.ReplacementExpr,
			Placement: qsbridge.PredicateResidualScan,
			Scope:     qsbridge.PredicateScopeWhere,
		})
	}
	query.Subqueries = removeAppliedPreflightSubqueries(query.Subqueries)
	logical := qsbridge.BuildLogicalPlan(query)
	physical := qsbridge.BuildPhysicalPlan(logical, request.Bound.Prepared.Scope)
	inspection := qsbridge.InspectOptimizedQuery(query, preflight.Optimization, request.Bound.Prepared.Scope)
	request.Bound.Prepared.Query = query
	request.Bound.Prepared.Logical = logical
	request.Bound.Prepared.Physical = physical
	request.Bound.Prepared.Inspection = inspection
	request.Bound.Prepared.Parameters = query.RequiredParameters()
	request.Bound.Prepared.ResultColumns = query.ResultColumns()
	request.Bound.Prepared.Result = query.Result
	request.Bound.Prepared.Access = query.RequiredAccess()
	request.Bound.Prepared.Diagnostics = runtimeDiagnosticsWithoutCode(request.Bound.Prepared.Diagnostics, qsbridge.DiagnosticCorrelatedAggregateSubquery)
	request.Bound.Prepared.Diagnostics = runtimeDiagnosticsWithoutUnknownLogicalNode(request.Bound.Prepared.Diagnostics)
	request.Bound.Prepared.Supported = query.Supported() && inspection.Supported && !request.Bound.Prepared.Diagnostics.BlocksNative()
	request.Bound.Diagnostics = runtimeDiagnosticsWithoutCode(request.Bound.Diagnostics, qsbridge.DiagnosticCorrelatedAggregateSubquery)
	request.Bound.Diagnostics = runtimeDiagnosticsWithoutUnknownLogicalNode(request.Bound.Diagnostics)
	request.Bound.Supported = request.Bound.Prepared.Supported && !request.Bound.Diagnostics.BlocksNative()
	request.Diagnostics = runtimeDiagnosticsWithoutCode(request.Diagnostics, qsbridge.DiagnosticCorrelatedAggregateSubquery)
	request.Diagnostics = runtimeDiagnosticsWithoutUnknownLogicalNode(request.Diagnostics)
	request.Supported = request.Bound.SupportedForExecution() && !request.Diagnostics.BlocksNative()
	request.Result = request.Bound.Prepared.Result
	request.ResultColumns = append([]qsbridge.ResultColumn(nil), request.Bound.Prepared.ResultColumns...)
	request.Access = append([]qsbridge.AccessRequirement(nil), request.Bound.Prepared.Access...)
	return request
}

func applyPreflightRuntimeState(request ExecutionRequest, preflight PreflightRewriteResult) ExecutionRequest {
	if len(preflight.NativePredicates.CorrelatedAggregate) == 0 {
		return request
	}
	request.NativePredicates.CorrelatedAggregate = append(request.NativePredicates.CorrelatedAggregate, preflight.NativePredicates.CorrelatedAggregate...)
	request.Materialization.ProjectionFields = appendNativePredicateProjectionFields(request.Materialization.ProjectionFields, request.NativePredicates)
	return request
}

func removeAppliedPreflightSubqueries(subqueries []qsbridge.SubqueryPlanIntent) []qsbridge.SubqueryPlanIntent {
	if len(subqueries) == 0 {
		return nil
	}
	filtered := make([]qsbridge.SubqueryPlanIntent, 0, len(subqueries))
	for _, intent := range subqueries {
		if intent.Kind == qsbridge.SubqueryIntentCorrelatedAggregate {
			continue
		}
		filtered = append(filtered, intent)
	}
	if len(filtered) == len(subqueries) {
		return subqueries
	}
	return filtered
}

func withEmptyCandidateSet(request ExecutionRequest) ExecutionRequest {
	index, _ := request.RootIndex()
	return request.WithCandidateSet(qsbridge.QuantaCandidateSet{Index: index})
}

func constantProjectionExecutionResult(request qsbridge.ExecutionRequest) (ExecutionResult, qsbridge.DiagnosticSet, bool) {
	query := request.Bound.Prepared.Query
	if query.Kind != qsbridge.QueryKindSelect ||
		len(query.Sources) != 0 ||
		len(query.Joins) != 0 ||
		len(query.Memberships) != 0 ||
		len(query.Predicates) != 0 ||
		query.WhereExpr != nil ||
		len(query.GroupBy) != 0 ||
		len(query.Having) != 0 ||
		len(query.Aggregates) != 0 ||
		len(query.Projection) == 0 {
		return ExecutionResult{}, nil, false
	}

	rowSet := qsbridge.QuantaProjectedRowSet{
		Rownums:           []qsbridge.QuantaRownum{1},
		ProjectionVectors: make([]qsbridge.QuantaProjectionVector, 0, len(query.Projection)),
	}
	for _, projection := range query.Projection {
		literal, ok := projection.Expr.(qsbridge.LiteralExpr)
		if !ok {
			return ExecutionResult{}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(
					qsbridge.DiagnosticUnsupportedSQL,
					qsbridge.PhaseExecute,
					"projection-only SELECT requires literal projections after scalar materialization",
				),
			}, true
		}
		column := projection.ResultColumn()
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
			Field: qsbridge.QuantaProjectionField{
				Field:   column.Name,
				Type:    column.Type,
				Visible: true,
			},
			Values: []qsbridge.ResultCell{{Kind: literal.Kind, Value: literal.Value}},
		})
	}
	return ExecutionResult{
		RowSet: rowSet,
		Count:  1,
	}, nil, true
}

// InspectSQL prepares, lowers, and inspects runtime routing without executing SQL.
func (r SQLRuntime) InspectSQL(sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) SQLInspectionResult {
	service := qsbridge.NewPlanningService(r.Planner(), nil)
	prepared, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{SQL: sql}, options, values...)
	result := SQLInspectionResult{
		Prepared:    prepared,
		Request:     request,
		Diagnostics: append(qsbridge.DiagnosticSet(nil), request.Diagnostics...),
	}
	if result.Diagnostics.BlocksNative() {
		if prepared.Kind == qsbridge.QueryKindSelect && request.Bound.Prepared.Query.Kind == qsbridge.QueryKindSelect {
			intermediate, diagnostics := r.Lowerer.LowerQuery(request.Bound.Prepared.Query, request.Bound.Parameters)
			result.Intermediate = intermediate
			result.Diagnostics = append(result.Diagnostics, diagnostics...)
			result.FilterExecutionEnabled = r.EnableFilterExpressions && !intermediate.Filter.Empty()
			if result.FilterExecutionEnabled && !diagnostics.BlocksNative() {
				result.Diagnostics = runtimeDiagnosticsWithoutCode(result.Diagnostics, qsbridge.DiagnosticMixedBooleanPredicate)
				if !result.Diagnostics.BlocksNative() {
					runtimeRequest := NewSQLExecutionRequest(intermediate, request)
					result.Runtime = r.InspectPrepared(runtimeRequest)
					result.Diagnostics = append(result.Diagnostics, result.Runtime.Diagnostics...)
				}
			}
		}
		return result
	}

	intermediate, diagnostics := r.Lowerer.LowerExecutionRequest(request)
	result.Intermediate = intermediate
	result.FilterExecutionEnabled = r.EnableFilterExpressions && !intermediate.Filter.Empty()
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result
	}

	runtimeRequest := NewSQLExecutionRequest(intermediate, request)
	result.Runtime = r.InspectPrepared(runtimeRequest)
	result.Diagnostics = append(result.Diagnostics, result.Runtime.Diagnostics...)
	return result
}

// ExecutePrepared executes an already-lowered runtime request.
func (r SQLRuntime) ExecutePrepared(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
	return r.Environment.Execute(ctx, request)
}

// InspectPrepared inspects an already-lowered runtime request without executing it.
func (r SQLRuntime) InspectPrepared(request ExecutionRequest) ExecutionInspection {
	return r.Environment.Inspect(request)
}

// SQLRuntimeBuilder composes a SQL-facing facade over a runtime environment.
type SQLRuntimeBuilder struct {
	EnvironmentBuilder      RuntimeEnvironmentBuilder
	Parser                  qsbridge.ParserBridge
	Lowerer                 qsbridge.QuantaIntermediateLowerer
	DefaultSchema           string
	CatalogVersion          qsbridge.CatalogVersion
	Session                 qsbridge.SessionContext
	Scope                   qsbridge.PhysicalScope
	PreflightHelpers        PreflightHelperExecutor
	NativeSubquerySteps     qsbridge.NativeSubqueryStepExecutor
	EnableFilterExpressions bool
}

// Build constructs the SQL-facing runtime facade.
func (b SQLRuntimeBuilder) Build(ctx context.Context) (SQLRuntime, qsbridge.DiagnosticSet, error) {
	if b.Parser == nil {
		return SQLRuntime{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseParse,
				"sql runtime builder has no parser bridge",
			),
		}, nil
	}
	environment, diagnostics, err := b.EnvironmentBuilder.Build(ctx)
	if err != nil || diagnostics.BlocksNative() {
		return SQLRuntime{}, diagnostics, err
	}
	return SQLRuntime{
		Environment:             environment,
		Parser:                  b.Parser,
		Lowerer:                 b.Lowerer,
		DefaultSchema:           b.DefaultSchema,
		CatalogVersion:          b.CatalogVersion,
		Session:                 b.Session.Clone(),
		Scope:                   b.Scope,
		PreflightHelpers:        b.PreflightHelpers,
		NativeSubquerySteps:     b.NativeSubquerySteps,
		EnableFilterExpressions: b.EnableFilterExpressions,
	}, nil, nil
}

func runtimeDiagnosticsWithoutCode(diagnostics qsbridge.DiagnosticSet, code qsbridge.DiagnosticCode) qsbridge.DiagnosticSet {
	filtered := make(qsbridge.DiagnosticSet, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			continue
		}
		filtered = append(filtered, diagnostic)
	}
	return filtered
}

func runtimeDiagnosticsWithoutUnknownLogicalNode(diagnostics qsbridge.DiagnosticSet) qsbridge.DiagnosticSet {
	filtered := make(qsbridge.DiagnosticSet, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == qsbridge.DiagnosticInternalInvariant && diagnostic.Message == "unknown logical node type" {
			continue
		}
		filtered = append(filtered, diagnostic)
	}
	return filtered
}

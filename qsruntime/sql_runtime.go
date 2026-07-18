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
	request, scalarDiagnostics, err := r.materializeScalarSubqueries(ctx, request)
	if err != nil || scalarDiagnostics.BlocksNative() {
		return SQLExecutionResult{
			Prepared:    request.Bound.Prepared,
			Request:     request,
			Diagnostics: scalarDiagnostics,
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
			runtimeResult, err := r.ExecutePrepared(ctx, NewSQLExecutionRequest(intermediate, request))
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
		runtimeResult, err := r.ExecutePrepared(ctx, NewSQLExecutionRequest(intermediate, request))
		result.Runtime = runtimeResult
		result.Diagnostics = append(result.Diagnostics, runtimeResult.Diagnostics...)
		return result, err
	}

	intermediate, diagnostics := r.Lowerer.LowerExecutionRequest(request)
	result.Intermediate = intermediate
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}

	runtimeResult, err := r.ExecutePrepared(ctx, NewSQLExecutionRequest(intermediate, request))
	result.Runtime = runtimeResult
	result.Diagnostics = append(result.Diagnostics, runtimeResult.Diagnostics...)
	return result, err
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

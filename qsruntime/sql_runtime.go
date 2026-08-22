package qsruntime

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/version"
)

// SQLRuntime is the SQL-facing runtime facade before protocol-specific plumbing.
type SQLRuntime struct {
	Environment          RuntimeEnvironment
	Parser               qsbridge.ParserBridge
	Lowerer              qsbridge.QuantaIntermediateLowerer
	DefaultSchema        string
	CatalogVersion       qsbridge.CatalogVersion
	Session              qsbridge.SessionContext
	Scope                qsbridge.PhysicalScope
	Authorizer           qsbridge.AccessAuthorizer
	PreflightHelpers     PreflightHelperExecutor
	NativeSubquerySteps  qsbridge.NativeSubqueryStepExecutor
	ContextWrapper       func(context.Context) context.Context
	StorageMutationGuard StorageMutationGuard
	// EnableFilterExpressions allows a runtime to execute grouped boolean filter trees.
	EnableFilterExpressions bool
}

// StorageMutationGuard lets product runtimes reject durable writes before
// helper paths perform read-side prework such as CTAS source materialization.
type StorageMutationGuard func(context.Context, string) qsbridge.DiagnosticSet

// SQLExecutionResult captures each stage from SQL planning through runtime execution.
type SQLExecutionResult struct {
	Prepared         qsbridge.PreparedPlan
	Request          qsbridge.ExecutionRequest
	Intermediate     qsbridge.QuantaIntermediateQuery
	Runtime          ExecutionResult
	Instrumentation  ExecutionInstrumentationSnapshot
	Diagnostics      qsbridge.DiagnosticSet
	NativeSubqueries NativeSubqueryPreparationSummary
	Preflight        PreflightRewriteSummary
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
		Catalog:        r.planningCatalog(),
		DefaultSchema:  r.DefaultSchema,
		CatalogVersion: r.CatalogVersion,
		Session:        r.Session.Clone(),
		Scope:          r.Scope,
	}
}

func (r SQLRuntime) planningCatalog() qsbridge.Catalog {
	return qsbridge.NewSessionCatalog(r.Environment.Catalog, r.Session, r.DefaultSchema)
}

// Plan parses and plans SQL through qsbridge without executing it.
func (r SQLRuntime) Plan(sql string) qsbridge.PlanResult {
	return r.Planner().Plan(sql)
}

// ExecuteSQL prepares, lowers, and executes SQL through the runtime environment.
func (r SQLRuntime) ExecuteSQL(ctx context.Context, sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) (result SQLExecutionResult, err error) {
	if r.ContextWrapper != nil {
		ctx = r.ContextWrapper(ctx)
	}
	totalStart := time.Now()
	defer func() {
		observeSQLRuntimeElapsed(ctx, "phase_total_elapsed", totalStart, "")
		result.Instrumentation = executionInstrumentationSnapshotWithMissingProbes(
			ExecutionInstrumentationSnapshotFromContext(ctx),
			result.Runtime.Probes,
		)
	}()
	service := r.planningService()
	prepareStart := time.Now()
	prepared, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{SQL: sql}, options, values...)
	observeSQLRuntimeElapsed(ctx, "phase_prepare_elapsed", prepareStart, "")
	if authorizationDiagnostics := r.authorizationDiagnostics(request); authorizationDiagnostics.BlocksNative() {
		return SQLExecutionResult{
			Prepared:    request.Bound.Prepared,
			Request:     request,
			Diagnostics: append(append(qsbridge.DiagnosticSet(nil), request.Diagnostics...), authorizationDiagnostics...),
		}, nil
	}
	preflightStart := time.Now()
	request, nativeSubqueries, nativeSubqueryDiagnostics, err := r.materializeCorrelatedAggregatePredicates(ctx, request, values...)
	observeSQLRuntimeElapsed(ctx, "phase_correlated_aggregate_preflight_elapsed", preflightStart, "")
	if err != nil || nativeSubqueryDiagnostics.BlocksNative() {
		return SQLExecutionResult{
			Prepared:         request.Bound.Prepared,
			Request:          request,
			Diagnostics:      nativeSubqueryDiagnostics,
			NativeSubqueries: nativeSubqueries,
		}, err
	}
	prepared = request.Bound.Prepared
	preflightStart = time.Now()
	request, scalarDiagnostics, err := r.materializeScalarSubqueries(ctx, request)
	observeSQLRuntimeElapsed(ctx, "phase_scalar_subquery_preflight_elapsed", preflightStart, "")
	if err != nil || scalarDiagnostics.BlocksNative() {
		return SQLExecutionResult{
			Prepared:         request.Bound.Prepared,
			Request:          request,
			Diagnostics:      scalarDiagnostics,
			NativeSubqueries: nativeSubqueries,
		}, err
	}
	var existsGate existsSubqueryGateState
	preflightStart = time.Now()
	request, existsGate, existsDiagnostics, err := r.materializeExistsSubqueryGates(ctx, request)
	observeSQLRuntimeElapsed(ctx, "phase_exists_subquery_preflight_elapsed", preflightStart, "")
	if err != nil || existsDiagnostics.BlocksNative() {
		return SQLExecutionResult{
			Prepared:         request.Bound.Prepared,
			Request:          request,
			Diagnostics:      existsDiagnostics,
			NativeSubqueries: nativeSubqueries,
		}, err
	}
	prepared = request.Bound.Prepared
	var textSearchState textSearchMaterializationState
	preflightStart = time.Now()
	request, textSearchState, textSearchDiagnostics, err := r.materializeTextSearchPredicates(ctx, request)
	observeSQLRuntimeElapsed(ctx, "phase_text_search_preflight_elapsed", preflightStart, "")
	if err != nil || textSearchDiagnostics.BlocksNative() {
		return SQLExecutionResult{
			Prepared:         request.Bound.Prepared,
			Request:          request,
			Diagnostics:      textSearchDiagnostics,
			NativeSubqueries: nativeSubqueries,
		}, err
	}
	prepared = request.Bound.Prepared
	result = SQLExecutionResult{
		Prepared:         prepared,
		Request:          request,
		Diagnostics:      append(qsbridge.DiagnosticSet(nil), request.Diagnostics...),
		NativeSubqueries: nativeSubqueries,
	}
	result.Diagnostics = append(result.Diagnostics, r.authorizationDiagnostics(request)...)
	if result.Diagnostics.BlocksNative() {
		if r.EnableFilterExpressions && prepared.Kind == qsbridge.QueryKindSelect && request.Bound.Prepared.Query.Kind == qsbridge.QueryKindSelect {
			lowerStart := time.Now()
			intermediate, diagnostics := r.Lowerer.LowerQuery(request.Bound.Prepared.Query, request.Bound.Parameters)
			observeSQLRuntimeElapsed(ctx, "phase_filter_lower_elapsed", lowerStart, "")
			result.Intermediate = intermediate
			result.Diagnostics = append(result.Diagnostics, diagnostics...)
			if diagnostics.BlocksNative() || intermediate.Filter.Empty() {
				return result, nil
			}
			result.Diagnostics = runtimeDiagnosticsWithoutCode(result.Diagnostics, qsbridge.DiagnosticMixedBooleanPredicate)
			if result.Diagnostics.BlocksNative() {
				return result, nil
			}
			runtimeRequest := applyNativeSubqueryRuntimeState(NewSQLExecutionRequest(intermediate, request), nativeSubqueries.NativePredicates)
			if existsGate.EmptyCandidateSet || textSearchState.EmptyCandidateSet {
				runtimeRequest = withEmptyCandidateSet(runtimeRequest)
			}
			executeStart := time.Now()
			runtimeResult, err := r.ExecutePrepared(ctx, runtimeRequest)
			observeSQLRuntimeElapsed(ctx, "phase_execute_prepared_elapsed", executeStart, "filter_expression")
			result.Runtime = runtimeResult
			result.Diagnostics = append(result.Diagnostics, runtimeResult.Diagnostics...)
			return result, err
		}
		return result, nil
	}
	if runtimeResult, diagnostics, err, ok := r.unionAllRuntimeResult(ctx, request); ok {
		result.Runtime = runtimeResult
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		result.Diagnostics = append(result.Diagnostics, result.Runtime.Diagnostics...)
		return result, err
	}
	if prepared.Kind == qsbridge.QueryKindSession {
		result.Runtime = ExecutionResult{Statement: cloneStatementResult(request.Statement)}
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindCreateTable && prepared.Query.Mutation.Temporary {
		result.Runtime = r.createTemporaryTableRuntimeResult(ctx, request)
		result.Diagnostics = append(result.Diagnostics, result.Runtime.Diagnostics...)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindCreateTable && strings.TrimSpace(prepared.Query.Mutation.SourceSQL) != "" {
		result.Runtime = r.createDurableTableAsSelectRuntimeResult(ctx, request)
		result.Diagnostics = append(result.Diagnostics, result.Runtime.Diagnostics...)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindDropTable && prepared.Query.Mutation.Temporary {
		result.Runtime = r.dropTemporaryTableRuntimeResult(request)
		result.Diagnostics = append(result.Diagnostics, result.Runtime.Diagnostics...)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindInsert {
		if runtimeResult, diagnostics, ok := r.insertTemporaryTableRuntimeResult(ctx, request); ok {
			result.Runtime = runtimeResult
			result.Diagnostics = append(result.Diagnostics, diagnostics...)
			result.Diagnostics = append(result.Diagnostics, result.Runtime.Diagnostics...)
			return result, nil
		}
		if strings.TrimSpace(prepared.Query.Mutation.SourceSQL) != "" {
			result.Runtime = r.insertSelectRuntimeResult(ctx, request)
			result.Diagnostics = append(result.Diagnostics, result.Runtime.Diagnostics...)
			return result, nil
		}
	}
	if prepared.Kind == qsbridge.QueryKindShowCreateView {
		result.Runtime = showCreateViewRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowCreateTable {
		result.Runtime = showCreateTableRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowCreateDatabase {
		result.Runtime = showCreateDatabaseRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowDatabases {
		result.Runtime = showDatabasesRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowIndex {
		result.Runtime = showIndexRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowTableStatus {
		result.Runtime = showTableStatusRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowTables {
		result.Runtime = showTablesRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowOpenTables {
		result.Runtime = showOpenTablesRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowTableTypes {
		result.Runtime = showTableTypesRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowFunctionStatus {
		result.Runtime = showFunctionStatusRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowProcedureStatus {
		result.Runtime = showProcedureStatusRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowTriggers {
		result.Runtime = showTriggersRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowEvents {
		result.Runtime = showEventsRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowVariables {
		result.Runtime = r.showVariablesRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowStatus {
		result.Runtime = showStatusRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowWarnings {
		result.Runtime = showWarningsRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowErrors {
		result.Runtime = showErrorsRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowWarningCount || prepared.Kind == qsbridge.QueryKindShowErrorCount {
		result.Runtime = showDiagnosticCountRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowCharacterSet {
		result.Runtime = showCharacterSetRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowCollation {
		result.Runtime = showCollationRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowProcesslist {
		result.Runtime = showProcesslistRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowEngines {
		result.Runtime = showEnginesRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowPlugins {
		result.Runtime = showPluginsRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowPrivileges {
		result.Runtime = showPrivilegesRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindShowGrants {
		result.Runtime = showGrantsRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindExplain {
		result.Runtime = explainRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind == qsbridge.QueryKindDescribe {
		result.Runtime = describeRuntimeResult(request)
		return result, nil
	}
	if prepared.Kind != qsbridge.QueryKindSelect {
		lowerStart := time.Now()
		intermediate, diagnostics := r.Lowerer.LowerExecutionRequest(request)
		observeSQLRuntimeElapsed(ctx, "phase_mutation_lower_elapsed", lowerStart, "")
		result.Intermediate = intermediate
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if result.Diagnostics.BlocksNative() {
			return result, nil
		}
		executeStart := time.Now()
		runtimeResult, err := r.ExecutePrepared(ctx, applyNativeSubqueryRuntimeState(NewSQLExecutionRequest(intermediate, request), nativeSubqueries.NativePredicates))
		observeSQLRuntimeElapsed(ctx, "phase_execute_prepared_elapsed", executeStart, "statement")
		result.Runtime = runtimeResult
		result.Diagnostics = append(result.Diagnostics, runtimeResult.Diagnostics...)
		return result, err
	}

	if runtimeResult, diagnostics, ok := r.constantProjectionExecutionResult(request); ok {
		result.Runtime = runtimeResult
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		return result, nil
	}
	if runtimeResult, diagnostics, ok := r.informationSchemaExecutionResult(request); ok {
		result.Runtime = runtimeResult
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		return result, nil
	}
	if runtimeResult, diagnostics, ok := r.inlineRowSetRuntimeResult(request); ok {
		result.Runtime = runtimeResult
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		result.Diagnostics = append(result.Diagnostics, result.Runtime.Diagnostics...)
		return result, nil
	}
	if runtimeResult, diagnostics, ok := r.selectTemporaryTableRuntimeResult(request); ok {
		result.Runtime = runtimeResult
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		result.Diagnostics = append(result.Diagnostics, result.Runtime.Diagnostics...)
		return result, nil
	}

	lowerStart := time.Now()
	intermediate, diagnostics := r.Lowerer.LowerExecutionRequest(request)
	observeSQLRuntimeElapsed(ctx, "phase_select_lower_elapsed", lowerStart, "")
	result.Intermediate = intermediate
	result.Diagnostics = append(result.Diagnostics, diagnostics...)
	if result.Diagnostics.BlocksNative() {
		return result, nil
	}

	runtimeRequest := applyNativeSubqueryRuntimeState(NewSQLExecutionRequest(intermediate, request), nativeSubqueries.NativePredicates)
	if existsGate.EmptyCandidateSet || textSearchState.EmptyCandidateSet {
		runtimeRequest = withEmptyCandidateSet(runtimeRequest)
	}
	executeStart := time.Now()
	runtimeResult, err := r.ExecutePrepared(ctx, runtimeRequest)
	observeSQLRuntimeElapsed(ctx, "phase_execute_prepared_elapsed", executeStart, "select")
	result.Runtime = runtimeResult
	result.Diagnostics = append(result.Diagnostics, runtimeResult.Diagnostics...)
	return result, err
}

func observeSQLRuntimeElapsed(ctx context.Context, name string, start time.Time, detail string) {
	recorder := ExecutionInstrumentationFromContext(ctx)
	if recorder == nil {
		return
	}
	recorder.ObserveDuration("sql_runtime", name, time.Since(start), detail)
}

func applyNativeSubqueryPlanningState(request qsbridge.ExecutionRequest, query qsbridge.QueryIR, optimization qsbridge.OptimizationTrace) qsbridge.ExecutionRequest {
	if len(query.Subqueries) == len(request.Bound.Prepared.Query.Subqueries) && len(optimization.Rewrites) == 0 && len(optimization.Diagnostics) == 0 {
		return request
	}
	logical := qsbridge.BuildLogicalPlan(query)
	physical := qsbridge.BuildPhysicalPlan(logical, request.Bound.Prepared.Scope)
	inspection := qsbridge.InspectOptimizedQuery(query, optimization, request.Bound.Prepared.Scope)
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

func applyNativeSubqueryRuntimeState(request ExecutionRequest, predicates NativePredicateSet) ExecutionRequest {
	if len(predicates.CorrelatedAggregate) == 0 {
		return request
	}
	request.NativePredicates.CorrelatedAggregate = append(request.NativePredicates.CorrelatedAggregate, predicates.CorrelatedAggregate...)
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

func (r SQLRuntime) constantProjectionExecutionResult(request qsbridge.ExecutionRequest) (ExecutionResult, qsbridge.DiagnosticSet, bool) {
	query := request.Bound.Prepared.Query
	if query.Kind != qsbridge.QueryKindSelect ||
		len(query.Sources) != 0 ||
		len(query.Joins) != 0 ||
		len(query.Memberships) != 0 ||
		len(query.GroupBy) != 0 ||
		len(query.Having) != 0 ||
		len(query.Aggregates) != 0 ||
		len(query.Projection) == 0 {
		return ExecutionResult{}, nil, false
	}
	matched, diagnostic, ok := qsbridge.ProjectionOnlySelectMatches(query.Predicates, query.WhereExpr, request.Bound.Parameters)
	if !ok {
		return ExecutionResult{}, qsbridge.DiagnosticSet{diagnostic}, true
	}
	if !matched {
		return ExecutionResult{Count: 0}, nil, true
	}

	rowSet := qsbridge.QuantaProjectedRowSet{
		Rownums:           []qsbridge.QuantaRownum{1},
		ProjectionVectors: make([]qsbridge.QuantaProjectionVector, 0, len(query.Projection)),
	}
	for _, projection := range query.Projection {
		expr := r.materializeProjectionMetadataExpr(projection.Expr)
		cell, diagnostics := directBitmapEvaluateMaterializedExpr(expr, rowSet, 0)
		if diagnostics.BlocksNative() {
			constantDiagnostics := qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(
					qsbridge.DiagnosticUnsupportedSQL,
					qsbridge.PhaseExecute,
					"projection-only SELECT requires constant projections after scalar materialization",
				),
			}
			return ExecutionResult{}, append(constantDiagnostics, diagnostics...), true
		}
		column := projection.ResultColumn()
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, qsbridge.QuantaProjectionVector{
			Field: qsbridge.QuantaProjectionField{
				Field:   column.Name,
				Type:    column.Type,
				Visible: true,
			},
			Values: []qsbridge.ResultCell{cell},
		})
	}
	return ExecutionResult{
		RowSet: rowSet,
		Count:  1,
	}, nil, true
}

func (r SQLRuntime) materializeProjectionMetadataExpr(expr qsbridge.Expr) qsbridge.Expr {
	switch typed := expr.(type) {
	case qsbridge.CallExpr:
		return r.materializeProjectionMetadataCall(typed)
	case *qsbridge.CallExpr:
		if typed == nil {
			return expr
		}
		call := r.materializeProjectionMetadataCall(*typed)
		return call
	case qsbridge.BinaryExpr:
		typed.Left = r.materializeProjectionMetadataExpr(typed.Left)
		typed.Right = r.materializeProjectionMetadataExpr(typed.Right)
		return typed
	case *qsbridge.BinaryExpr:
		if typed == nil {
			return expr
		}
		copied := *typed
		copied.Left = r.materializeProjectionMetadataExpr(copied.Left)
		copied.Right = r.materializeProjectionMetadataExpr(copied.Right)
		return copied
	case qsbridge.SearchedCaseExpr:
		return r.materializeProjectionMetadataSearchedCase(typed)
	case *qsbridge.SearchedCaseExpr:
		if typed == nil {
			return expr
		}
		return r.materializeProjectionMetadataSearchedCase(*typed)
	default:
		return expr
	}
}

func (r SQLRuntime) materializeProjectionMetadataCall(call qsbridge.CallExpr) qsbridge.Expr {
	if literal, ok := r.runtimeMetadataVariableCallLiteral(call); ok {
		return literal
	}
	if literal, ok := r.runtimeMetadataFunctionLiteral(call.Name, len(call.Args)); ok {
		return literal
	}
	for i, arg := range call.Args {
		call.Args[i] = r.materializeProjectionMetadataExpr(arg)
	}
	return call
}

func (r SQLRuntime) materializeProjectionMetadataSearchedCase(expr qsbridge.SearchedCaseExpr) qsbridge.Expr {
	for i, when := range expr.Whens {
		expr.Whens[i] = qsbridge.SearchedCaseWhen{
			Condition: r.materializeProjectionMetadataExpr(when.Condition),
			Result:    r.materializeProjectionMetadataExpr(when.Result),
		}
	}
	if expr.Else != nil {
		expr.Else = r.materializeProjectionMetadataExpr(expr.Else)
	}
	return expr
}

func (r SQLRuntime) runtimeMetadataVariableCallLiteral(call qsbridge.CallExpr) (qsbridge.LiteralExpr, bool) {
	if !strings.EqualFold(strings.TrimSpace(call.Name), "qs_session_variable") || len(call.Args) != 1 {
		return qsbridge.LiteralExpr{}, false
	}
	literal, ok := call.Args[0].(qsbridge.LiteralExpr)
	if !ok {
		if pointer, pointerOK := call.Args[0].(*qsbridge.LiteralExpr); pointerOK && pointer != nil {
			literal = *pointer
			ok = true
		}
	}
	if !ok || literal.Kind != qsbridge.ValueString {
		return qsbridge.LiteralExpr{}, false
	}
	name, _ := literal.Value.(string)
	return r.runtimeMetadataVariableLiteral(name)
}

func (r SQLRuntime) runtimeMetadataVariableLiteral(name string) (qsbridge.LiteralExpr, bool) {
	normalized := strings.ToLower(strings.TrimSpace(name))
	if normalized == "" {
		return qsbridge.LiteralExpr{}, false
	}
	if value, ok := r.Session.Variables[normalized]; ok {
		return metadataVariableLiteral(normalized, value), true
	}
	switch normalized {
	case "version":
		return qsbridge.Literal(qsbridge.ValueString, version.MySQLVersion()), true
	case "version_comment":
		return qsbridge.Literal(qsbridge.ValueString, version.MySQLVersionComment()), true
	case "autocommit":
		return qsbridge.Literal(qsbridge.ValueInt, int64(1)), true
	case "character_set_client", "character_set_connection", "character_set_results":
		return qsbridge.Literal(qsbridge.ValueString, "utf8mb4"), true
	case "collation_connection":
		return qsbridge.Literal(qsbridge.ValueString, "utf8mb4_0900_ai_ci"), true
	case "sql_mode":
		return qsbridge.Literal(qsbridge.ValueString, strings.Join(sqlModeStrings(r.Session.SQLModes), ",")), true
	case "time_zone":
		return qsbridge.Literal(qsbridge.ValueString, runtimeSessionTimeZone(r.Session)), true
	case "max_allowed_packet":
		return qsbridge.Literal(qsbridge.ValueInt, int64(67108864)), true
	default:
		return qsbridge.Literal(qsbridge.ValueString, ""), true
	}
}

func runtimeSessionTimeZone(session qsbridge.SessionContext) string {
	if value := strings.TrimSpace(session.TimeZone); value != "" {
		return value
	}
	return "SYSTEM"
}

func metadataVariableLiteral(name string, value string) qsbridge.LiteralExpr {
	value = strings.TrimSpace(value)
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "autocommit":
		switch strings.ToLower(value) {
		case "on", "true":
			return qsbridge.Literal(qsbridge.ValueInt, int64(1))
		case "off", "false":
			return qsbridge.Literal(qsbridge.ValueInt, int64(0))
		}
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			return qsbridge.Literal(qsbridge.ValueInt, parsed)
		}
	case "max_allowed_packet":
		if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
			return qsbridge.Literal(qsbridge.ValueInt, parsed)
		}
	}
	return qsbridge.Literal(qsbridge.ValueString, value)
}

func sqlModeStrings(modes []qsbridge.SQLMode) []string {
	out := make([]string, 0, len(modes))
	for _, mode := range modes {
		value := strings.TrimSpace(string(mode))
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func (r SQLRuntime) runtimeMetadataFunctionLiteral(name string, argCount int) (qsbridge.LiteralExpr, bool) {
	if argCount != 0 {
		return qsbridge.LiteralExpr{}, false
	}
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "database", "schema":
		schema := strings.TrimSpace(r.Session.EffectiveSchema(r.DefaultSchema))
		if schema == "" {
			schema = "quanta"
		}
		return qsbridge.Literal(qsbridge.ValueString, schema), true
	case "version":
		return qsbridge.Literal(qsbridge.ValueString, version.MySQLVersion()), true
	case "user", "current_user":
		user := strings.TrimSpace(string(r.Session.User))
		if user == "" {
			user = "MOLIG004@localhost"
		}
		return qsbridge.Literal(qsbridge.ValueString, user), true
	case "connection_id":
		return qsbridge.Literal(qsbridge.ValueInt, int64(1)), true
	default:
		return qsbridge.LiteralExpr{}, false
	}
}

// InspectSQL prepares, lowers, and inspects runtime routing without executing SQL.
func (r SQLRuntime) InspectSQL(sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) SQLInspectionResult {
	service := r.planningService()
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

func (r SQLRuntime) planningService() qsbridge.PlanningService {
	service := qsbridge.NewPlanningService(r.Planner(), nil)
	service.Authorizer = r.Authorizer
	return service
}

func (r SQLRuntime) authorizationDiagnostics(request qsbridge.ExecutionRequest) qsbridge.DiagnosticSet {
	if r.Authorizer == nil {
		return nil
	}
	decision := request.AuthorizationRequest().Authorize(r.Authorizer)
	if decision.Supported() {
		return nil
	}
	return append(qsbridge.DiagnosticSet(nil), decision.Diagnostics...)
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
	Authorizer              qsbridge.AccessAuthorizer
	PreflightHelpers        PreflightHelperExecutor
	NativeSubquerySteps     qsbridge.NativeSubqueryStepExecutor
	ContextWrapper          func(context.Context) context.Context
	StorageMutationGuard    StorageMutationGuard
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
		Authorizer:              b.Authorizer,
		PreflightHelpers:        b.PreflightHelpers,
		NativeSubquerySteps:     b.NativeSubquerySteps,
		ContextWrapper:          b.ContextWrapper,
		StorageMutationGuard:    b.StorageMutationGuard,
		EnableFilterExpressions: b.EnableFilterExpressions,
	}, nil, nil
}

func (r SQLRuntime) rejectStorageMutation(ctx context.Context, operation string, status string) (ExecutionResult, bool) {
	if r.StorageMutationGuard == nil {
		return ExecutionResult{}, false
	}
	diagnostics := r.StorageMutationGuard(ctx, operation)
	if !diagnostics.BlocksNative() {
		return ExecutionResult{}, false
	}
	return ExecutionResult{
		Diagnostics: diagnostics,
		Statement:   qsbridge.StatementResult{Status: status},
	}, true
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

package qsbridge

import (
	"sort"
	"strings"
)

// ParserBridge converts SQL text into parser-neutral unbound statements.
//
// The interface is deliberately small so the native engine can be fed by the
// existing qlbridge parser, a future parser, or tests without importing parser
// internals into qsbridge.
type ParserBridge interface {
	Parse(sql string) (UnboundStatement, DiagnosticSet)
}

// UnboundStatement is a parser-neutral statement before catalog binding.
type UnboundStatement struct {
	SQL             string
	Kind            QueryKind
	Select          UnboundSelect
	Insert          UnboundInsert
	Update          UnboundUpdate
	Delete          UnboundDelete
	Truncate        UnboundTruncate
	Create          UnboundCreateTable
	Drop            UnboundDropTable
	CreateView      UnboundCreateView
	DropView        UnboundDropView
	ShowView        UnboundShowCreateView
	ShowTable       UnboundShowCreateTable
	ShowCreateDB    UnboundShowCreateDatabase
	ShowDBs         UnboundShowDatabases
	ShowIndex       UnboundShowIndex
	ShowTableStatus UnboundShowTableStatus
	ShowTables      UnboundShowTables
	ShowVars        UnboundShowVariables
	ShowStatus      UnboundShowStatus
	ShowWarnings    UnboundShowWarnings
	ShowCharset     UnboundShowCharacterSet
	ShowCollation   UnboundShowCollation
	ShowProcesslist UnboundShowProcesslist
	Describe        UnboundDescribe
	Session         UnboundSession
}

// Bind binds the statement using context and returns a QueryIR when possible.
func (s UnboundStatement) Bind(context *BindContext) (QueryIR, DiagnosticSet) {
	switch s.Kind {
	case QueryKindSelect:
		return BindSelect(context, s.Select)
	case QueryKindInsert:
		return BindInsert(context, s.Insert)
	case QueryKindUpdate:
		return BindUpdate(context, s.Update)
	case QueryKindDelete:
		return BindDelete(context, s.Delete)
	case QueryKindTruncate:
		return BindTruncate(context, s.Truncate)
	case QueryKindCreateTable:
		return BindCreateTable(context, s.Create)
	case QueryKindDropTable:
		return BindDropTable(context, s.Drop)
	case QueryKindCreateView:
		return BindCreateView(context, s.CreateView)
	case QueryKindDropView:
		return BindDropView(context, s.DropView)
	case QueryKindShowCreateView:
		return BindShowCreateView(context, s.ShowView)
	case QueryKindShowCreateTable:
		return BindShowCreateTable(context, s.ShowTable)
	case QueryKindShowCreateDatabase:
		return BindShowCreateDatabase(context, s.ShowCreateDB)
	case QueryKindShowDatabases:
		return BindShowDatabases(context, s.ShowDBs)
	case QueryKindShowIndex:
		return BindShowIndex(context, s.ShowIndex)
	case QueryKindShowTableStatus:
		return BindShowTableStatus(context, s.ShowTableStatus)
	case QueryKindShowTables:
		return BindShowTables(context, s.ShowTables)
	case QueryKindShowVariables:
		return BindShowVariables(context, s.ShowVars)
	case QueryKindShowStatus:
		return BindShowStatus(context, s.ShowStatus)
	case QueryKindShowWarnings:
		return BindShowWarnings(context, s.ShowWarnings)
	case QueryKindShowCharacterSet:
		return BindShowCharacterSet(context, s.ShowCharset)
	case QueryKindShowCollation:
		return BindShowCollation(context, s.ShowCollation)
	case QueryKindShowProcesslist:
		return BindShowProcesslist(context, s.ShowProcesslist)
	case QueryKindDescribe:
		return BindDescribe(context, s.Describe)
	case QueryKindSession:
		return BindSession(context, s.Session)
	default:
		return QueryIR{Kind: s.Kind}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "statement kind cannot be bound yet: "+string(s.Kind)),
		}
	}
}

// UnboundSelect describes a SELECT shape before catalog binding.
type UnboundSelect struct {
	Tables      []UnboundTable
	Projection  []UnboundProjection
	Joins       []UnboundJoin
	Memberships []UnboundMembership
	Predicates  []UnboundPredicate
	WhereExpr   UnboundExpr
	Subqueries  []UnboundSubqueryPlanIntent
	GroupBy     []UnboundExpr
	Aggregates  []UnboundAggregate
	Having      []UnboundPredicate
	OrderBy     []UnboundSort
	Result      ResultShape
	Blockers    []NativeBlocker
}

// UnboundInsert describes an INSERT shape before catalog binding.
type UnboundInsert struct {
	Table    UnboundTable
	Columns  []string
	Rows     [][]UnboundExpr
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundAssignment describes one UPDATE assignment before catalog binding.
type UnboundAssignment struct {
	Column string
	Value  UnboundExpr
}

// UnboundUpdate describes an UPDATE shape before catalog binding.
type UnboundUpdate struct {
	Table       UnboundTable
	Assignments []UnboundAssignment
	Predicates  []UnboundPredicate
	Result      ResultShape
	Blockers    []NativeBlocker
}

// UnboundDelete describes a DELETE shape before catalog binding.
type UnboundDelete struct {
	Table      UnboundTable
	Predicates []UnboundPredicate
	Result     ResultShape
	Blockers   []NativeBlocker
}

// UnboundTruncate describes a TRUNCATE shape before catalog binding.
type UnboundTruncate struct {
	Table    UnboundTable
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundCreateTable describes a YAML-backed CREATE TABLE activation before binding.
type UnboundCreateTable struct {
	Table    UnboundTable
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundCreateView describes a CREATE VIEW operation before binding.
type UnboundCreateView struct {
	View     UnboundTable
	SQL      string
	Replace  bool
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundDropTable describes a DROP TABLE operation before binding.
type UnboundDropTable struct {
	Table    UnboundTable
	IfExists bool
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundDropView describes a DROP VIEW operation before binding.
type UnboundDropView struct {
	View     UnboundTable
	IfExists bool
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundShowCreateView describes a SHOW CREATE VIEW operation before binding.
type UnboundShowCreateView struct {
	View     UnboundTable
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundShowCreateTable describes a SHOW CREATE TABLE operation before binding.
type UnboundShowCreateTable struct {
	Table    UnboundTable
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundShowCreateDatabase describes a SHOW CREATE DATABASE operation before binding.
type UnboundShowCreateDatabase struct {
	Schema   string
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundShowDatabases describes a SHOW DATABASES metadata read before binding.
type UnboundShowDatabases struct {
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundShowIndex describes a SHOW INDEX operation before binding.
type UnboundShowIndex struct {
	Table    UnboundTable
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundShowTableStatus describes a SHOW TABLE STATUS metadata read before binding.
type UnboundShowTableStatus struct {
	Schema   string
	Pattern  string
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundShowTables describes a SHOW TABLES metadata read before binding.
type UnboundShowTables struct {
	Schema   string
	Full     bool
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundShowVariables describes a SHOW VARIABLES metadata read before binding.
type UnboundShowVariables struct {
	Pattern  string
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundShowStatus describes a SHOW STATUS metadata read before binding.
type UnboundShowStatus struct {
	Pattern  string
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundShowWarnings describes a SHOW WARNINGS metadata read before binding.
type UnboundShowWarnings struct {
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundShowCharacterSet describes a SHOW CHARACTER SET metadata read before binding.
type UnboundShowCharacterSet struct {
	Pattern  string
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundShowCollation describes a SHOW COLLATION metadata read before binding.
type UnboundShowCollation struct {
	Pattern  string
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundShowProcesslist describes a SHOW PROCESSLIST metadata read before binding.
type UnboundShowProcesslist struct {
	Full     bool
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundDescribe describes a DESCRIBE/SHOW COLUMNS metadata read before binding.
type UnboundDescribe struct {
	Target   UnboundTable
	Full     bool
	Result   ResultShape
	Blockers []NativeBlocker
}

// UnboundSession describes a session-affecting statement before binding.
type UnboundSession struct {
	Actions         []SessionAction
	ValidateCatalog bool
	Result          ResultShape
	Blockers        []NativeBlocker
}

// UnboundProjection describes one unbound SELECT-list expression.
type UnboundProjection struct {
	Expr  UnboundExpr
	Alias string
	Type  DataType
}

// UnboundJoin describes one equality join edge before catalog binding.
type UnboundJoin struct {
	LeftQualifier  string
	LeftField      string
	RightQualifier string
	RightField     string
	Predicates     []UnboundPredicate
	Relationship   string
	Kind           JoinKind
	Nulls          NullExtension
	Unsupported    string
}

// UnboundMembership describes one semi/anti membership edge before binding.
type UnboundMembership struct {
	LeftQualifier  string
	LeftField      string
	RightTable     UnboundTable
	RightQualifier string
	RightField     string
	Predicates     []UnboundPredicate
	Kind           MembershipKind
	Relationship   string
	Unsupported    string
}

// UnboundPredicate describes one unbound boolean expression plus parser hints.
type UnboundPredicate struct {
	Expr         UnboundExpr
	Placement    PredicatePlacement
	Scope        PredicateScope
	Combinator   PredicateCombinator
	Capabilities []PlanCapability
	Unsupported  string
}

// UnboundSubqueryPlanIntent describes parser-recognized subquery intent before catalog binding.
type UnboundSubqueryPlanIntent struct {
	Kind                SubqueryIntentKind
	Capability          PlanCapability
	CorrelatedAggregate *UnboundCorrelatedAggregateSubqueryIntent
	HelperIntents       []SubqueryHelperIntent
}

// UnboundCorrelatedAggregateSubqueryIntent describes a correlated aggregate subquery before field binding.
type UnboundCorrelatedAggregateSubqueryIntent struct {
	AggregateFunction string
	Factor            float64
	SourcePredicate   string
	OuterValue        UnboundFieldExpr
	InnerValue        UnboundFieldExpr
	InnerTable        UnboundTable
	InnerKey          UnboundFieldExpr
	OuterKey          UnboundFieldExpr
	RequiredFilters   []UnboundFieldExpr
	Scope             PredicateScope
}

// UnboundSort describes one unbound ORDER BY expression.
type UnboundSort struct {
	Expr      UnboundExpr
	Direction SortDirection
}

// UnboundAggregate describes one aggregate slot before catalog binding.
type UnboundAggregate struct {
	Function string
	Mode     AggregateMode
	Input    UnboundExpr
	Filter   UnboundExpr
	Alias    string
	Type     DataType
	CountAll bool
}

// BindSelect binds a parser-neutral SELECT shape into QueryIR.
func BindSelect(context *BindContext, selectStmt UnboundSelect) (QueryIR, DiagnosticSet) {
	query := QueryIR{
		Kind:     QueryKindSelect,
		Result:   selectStmt.Result,
		Blockers: append([]NativeBlocker(nil), selectStmt.Blockers...),
	}
	diagnostics := make(DiagnosticSet, 0)

	if context == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}

	for _, table := range selectStmt.Tables {
		if _, tableDiagnostics := context.AddTable(table); tableDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, tableDiagnostics...)
		}
	}
	query.Sources = context.Sources()

	for _, join := range selectStmt.Joins {
		edge, joinDiagnostics := BindJoin(context, join)
		if joinDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, joinDiagnostics...)
			continue
		}
		query.Joins = append(query.Joins, edge)
	}

	for _, membership := range selectStmt.Memberships {
		edge, membershipDiagnostics := BindMembership(context, membership)
		if membershipDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, membershipDiagnostics...)
			continue
		}
		query.Memberships = append(query.Memberships, edge)
	}

	for _, projection := range selectStmt.Projection {
		if projections, expanded := bindWildcardProjection(context, projection, &diagnostics); expanded {
			query.Projection = append(query.Projection, projections...)
			continue
		}
		expr, exprDiagnostics := BindExpression(context, projection.Expr, FieldRoleVisible)
		if exprDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, exprDiagnostics...)
			continue
		}
		query.Projection = append(query.Projection, ProjectionColumn{
			Expr:  expr,
			Alias: projection.Alias,
			Type:  projectionType(projection.Type, expr),
		})
	}

	for _, predicate := range selectStmt.Predicates {
		if predicate.Scope == PredicateScopeUnknown {
			predicate.Scope = PredicateScopeWhere
		}
		predicate.Expr = resolveUnboundProjectionAliases(predicate.Expr, selectStmt.Projection)
		bound, predicateDiagnostics := BindPredicate(context, predicate, FieldRoleResidualInput)
		if predicateDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, predicateDiagnostics...)
			continue
		}
		query.Predicates = append(query.Predicates, bound)
	}

	for _, subquery := range selectStmt.Subqueries {
		intent, subqueryDiagnostics := BindSubqueryPlanIntent(context, subquery)
		if subqueryDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, subqueryDiagnostics...)
			continue
		}
		query.Subqueries = append(query.Subqueries, intent)
	}

	if selectStmt.WhereExpr != nil {
		whereExpr := resolveUnboundProjectionAliases(selectStmt.WhereExpr, selectStmt.Projection)
		bound, whereDiagnostics := BindExpression(context, whereExpr, FieldRoleResidualInput)
		if whereDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, whereDiagnostics...)
		} else {
			query.WhereExpr = bound
		}
	}

	for _, groupExpr := range selectStmt.GroupBy {
		expr, exprDiagnostics := BindExpression(context, groupExpr, FieldRoleGroupKey)
		if exprDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, exprDiagnostics...)
			continue
		}
		query.GroupBy = append(query.GroupBy, expr)
	}

	for _, aggregate := range selectStmt.Aggregates {
		bound, aggregateDiagnostics := BindAggregate(context, aggregate)
		if aggregateDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, aggregateDiagnostics...)
			continue
		}
		query.Aggregates = append(query.Aggregates, bound)
	}

	having := make([]UnboundPredicate, 0, len(selectStmt.Having))
	for _, predicate := range selectStmt.Having {
		predicate.Expr = resolveUnboundProjectionAliases(predicate.Expr, selectStmt.Projection)
		having = append(having, predicate)
	}
	query.Having = append(query.Having, bindUnboundPredicates(context, having, &diagnostics)...)

	for _, sort := range selectStmt.OrderBy {
		expr, exprDiagnostics := BindExpression(context, sort.Expr, FieldRoleSortKey)
		if exprDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, exprDiagnostics...)
			continue
		}
		query.OrderBy = append(query.OrderBy, SortSpec{
			Expr:      expr,
			Direction: sort.Direction,
		})
	}

	return query, diagnostics
}

func resolveUnboundProjectionAliases(expr UnboundExpr, projections []UnboundProjection) UnboundExpr {
	switch typed := expr.(type) {
	case UnboundFieldExpr:
		if typed.Qualifier != "" {
			return expr
		}
		for _, projection := range projections {
			if projection.Alias == "" || !strings.EqualFold(projection.Alias, typed.Name) {
				continue
			}
			return projection.Expr
		}
		return expr
	case UnboundBinaryExpr:
		typed.Left = resolveUnboundProjectionAliases(typed.Left, projections)
		typed.Right = resolveUnboundProjectionAliases(typed.Right, projections)
		return typed
	case UnboundCallExpr:
		args := make([]UnboundExpr, 0, len(typed.Args))
		for _, arg := range typed.Args {
			args = append(args, resolveUnboundProjectionAliases(arg, projections))
		}
		typed.Args = args
		return typed
	case UnboundListExpr:
		items := make([]UnboundExpr, 0, len(typed.Items))
		for _, item := range typed.Items {
			items = append(items, resolveUnboundProjectionAliases(item, projections))
		}
		typed.Items = items
		return typed
	default:
		return expr
	}
}

func bindWildcardProjection(context *BindContext, projection UnboundProjection, diagnostics *DiagnosticSet) ([]ProjectionColumn, bool) {
	field, ok := projection.Expr.(UnboundFieldExpr)
	if !ok || field.Name != "*" {
		return nil, false
	}
	if projection.Alias != "" {
		*diagnostics = append(*diagnostics, ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "wildcard projection cannot be aliased"))
		return nil, true
	}
	tables := context.Tables()
	if field.Qualifier != "" {
		table, tableDiagnostics := context.ResolveTable(field.Qualifier)
		if tableDiagnostics.BlocksNative() {
			*diagnostics = append(*diagnostics, tableDiagnostics...)
			return nil, true
		}
		tables = []BoundTable{table}
	}
	projections := make([]ProjectionColumn, 0)
	for _, table := range tables {
		for _, fieldDefinition := range table.Definition.Fields {
			ref := fieldDefinition.Ref(table.Instance, FieldRoleVisible)
			projections = append(projections, ProjectionColumn{
				Expr: Field(ref),
				Type: fieldDefinition.Type,
			})
		}
	}
	return projections, true
}

// BindInsert binds parser-neutral INSERT metadata into QueryIR.
func BindInsert(context *BindContext, insertStmt UnboundInsert) (QueryIR, DiagnosticSet) {
	query, target, diagnostics := bindMutationTarget(context, QueryKindInsert, insertStmt.Table, insertStmt.Result, insertStmt.Blockers)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}

	columns := make([]FieldRef, 0, len(insertStmt.Columns))
	for _, column := range insertStmt.Columns {
		ref, columnDiagnostics := context.ResolveField(target.Instance.RefName(), column, FieldRoleMutationTarget)
		if columnDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, columnDiagnostics...)
			continue
		}
		columns = append(columns, ref)
	}

	rows := make([]MutationRow, 0, len(insertStmt.Rows))
	for _, row := range insertStmt.Rows {
		if len(columns) > 0 && len(row) != len(columns) {
			diagnostics = append(diagnostics, ErrorDiagnostic(
				DiagnosticParserBoundary,
				PhaseBind,
				"insert row value count does not match target column count",
			))
			continue
		}
		values := make([]Expr, 0, len(row))
		for _, value := range row {
			bound, valueDiagnostics := BindExpression(context, value, FieldRoleMutationValue)
			if valueDiagnostics.BlocksNative() {
				diagnostics = append(diagnostics, valueDiagnostics...)
				continue
			}
			values = append(values, bound)
		}
		rows = append(rows, MutationRow{Values: values})
	}
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}

	query.Mutation = MutationShape{
		Kind:    MutationInsert,
		Target:  target.Instance,
		Columns: columns,
		Rows:    rows,
	}
	return query, diagnostics
}

// BindUpdate binds parser-neutral UPDATE metadata into QueryIR.
func BindUpdate(context *BindContext, updateStmt UnboundUpdate) (QueryIR, DiagnosticSet) {
	query, target, diagnostics := bindMutationTarget(context, QueryKindUpdate, updateStmt.Table, updateStmt.Result, updateStmt.Blockers)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}

	assignments := make([]MutationAssignment, 0, len(updateStmt.Assignments))
	columns := make([]FieldRef, 0, len(updateStmt.Assignments))
	for _, assignment := range updateStmt.Assignments {
		field, fieldDiagnostics := context.ResolveField(target.Instance.RefName(), assignment.Column, FieldRoleMutationTarget)
		if fieldDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, fieldDiagnostics...)
			continue
		}
		value, valueDiagnostics := BindExpression(context, assignment.Value, FieldRoleMutationValue)
		if valueDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, valueDiagnostics...)
			continue
		}
		columns = append(columns, field)
		assignments = append(assignments, MutationAssignment{Field: field, Value: value})
	}

	predicates := bindMutationPredicates(context, updateStmt.Predicates, &diagnostics)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}

	query.Mutation = MutationShape{
		Kind:        MutationUpdate,
		Target:      target.Instance,
		Columns:     columns,
		Assignments: assignments,
		Predicates:  predicates,
	}
	return query, diagnostics
}

// BindDelete binds parser-neutral DELETE metadata into QueryIR.
func BindDelete(context *BindContext, deleteStmt UnboundDelete) (QueryIR, DiagnosticSet) {
	query, target, diagnostics := bindMutationTarget(context, QueryKindDelete, deleteStmt.Table, deleteStmt.Result, deleteStmt.Blockers)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}

	predicates := bindMutationPredicates(context, deleteStmt.Predicates, &diagnostics)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}

	query.Mutation = MutationShape{
		Kind:       MutationDelete,
		Target:     target.Instance,
		Predicates: predicates,
	}
	return query, diagnostics
}

// BindTruncate binds parser-neutral TRUNCATE metadata into QueryIR.
func BindTruncate(context *BindContext, truncateStmt UnboundTruncate) (QueryIR, DiagnosticSet) {
	query, target, diagnostics := bindMutationTarget(context, QueryKindTruncate, truncateStmt.Table, truncateStmt.Result, truncateStmt.Blockers)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}
	dependencies, dependencyDiagnostics := bindTruncateDependentRelationships(context, target.Instance)
	if dependencyDiagnostics.BlocksNative() {
		diagnostics = append(diagnostics, dependencyDiagnostics...)
		return query, diagnostics
	}

	query.Mutation = MutationShape{
		Kind:                   MutationTruncate,
		Target:                 target.Instance,
		DependentRelationships: dependencies,
	}
	return query, diagnostics
}

// BindCreateTable binds parser-neutral CREATE TABLE metadata into QueryIR.
func BindCreateTable(context *BindContext, createStmt UnboundCreateTable) (QueryIR, DiagnosticSet) {
	query, target, diagnostics := bindSchemaOperationTarget(context, QueryKindCreateTable, createStmt.Table, createStmt.Result, createStmt.Blockers)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}
	query.Mutation = MutationShape{
		Kind:   MutationCreateTable,
		Target: target,
	}
	return query, diagnostics
}

// BindCreateView binds parser-neutral CREATE VIEW metadata into QueryIR.
func BindCreateView(context *BindContext, createStmt UnboundCreateView) (QueryIR, DiagnosticSet) {
	query, target, diagnostics := bindSchemaOperationTarget(context, QueryKindCreateView, createStmt.View, createStmt.Result, createStmt.Blockers)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}
	viewStatement, viewDiagnostics := SimpleParserBridge{}.Parse(createStmt.SQL)
	if viewDiagnostics.BlocksNative() {
		diagnostics = append(diagnostics, viewDiagnostics...)
		return query, diagnostics
	}
	if viewStatement.Kind != QueryKindSelect {
		diagnostics = append(diagnostics, ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "CREATE VIEW must use a SELECT statement"))
		return query, diagnostics
	}
	viewQuery, selectDiagnostics := BindSelect(context, viewStatement.Select)
	if selectDiagnostics.BlocksNative() {
		diagnostics = append(diagnostics, selectDiagnostics...)
		return query, diagnostics
	}
	query.Mutation = MutationShape{
		Kind:             MutationCreateView,
		Target:           target,
		ViewSQL:          strings.TrimSpace(createStmt.SQL),
		Replace:          createStmt.Replace,
		ViewDependencies: viewDependenciesFromQuery(viewQuery),
	}
	return query, diagnostics
}

// BindDropTable binds parser-neutral DROP TABLE metadata into QueryIR.
func BindDropTable(context *BindContext, dropStmt UnboundDropTable) (QueryIR, DiagnosticSet) {
	query, target, diagnostics := bindSchemaOperationTarget(context, QueryKindDropTable, dropStmt.Table, dropStmt.Result, dropStmt.Blockers)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}
	dependencies, dependencyDiagnostics := bindTruncateDependentRelationships(context, target)
	if dependencyDiagnostics.BlocksNative() {
		diagnostics = append(diagnostics, dependencyDiagnostics...)
		return query, diagnostics
	}
	query.Mutation = MutationShape{
		Kind:                   MutationDropTable,
		Target:                 target,
		IfExists:               dropStmt.IfExists,
		DependentRelationships: dependencies,
	}
	return query, diagnostics
}

// BindDropView binds parser-neutral DROP VIEW metadata into QueryIR.
func BindDropView(context *BindContext, dropStmt UnboundDropView) (QueryIR, DiagnosticSet) {
	query, target, diagnostics := bindSchemaOperationTarget(context, QueryKindDropView, dropStmt.View, dropStmt.Result, dropStmt.Blockers)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}
	query.Mutation = MutationShape{
		Kind:     MutationDropView,
		Target:   target,
		IfExists: dropStmt.IfExists,
	}
	return query, diagnostics
}

// BindShowCreateView binds parser-neutral SHOW CREATE VIEW metadata into QueryIR.
func BindShowCreateView(context *BindContext, showStmt UnboundShowCreateView) (QueryIR, DiagnosticSet) {
	query := QueryIR{
		Kind:     QueryKindShowCreateView,
		Result:   showStmt.Result,
		Blockers: append([]NativeBlocker(nil), showStmt.Blockers...),
	}
	if context == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	viewName := strings.TrimSpace(showStmt.View.Name)
	if viewName == "" {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "SHOW CREATE VIEW target is empty"),
		}
	}
	schemaName := strings.TrimSpace(showStmt.View.Schema)
	if schemaName == "" {
		schemaName = context.DefaultSchema
	}
	target := TableInstance{
		ID:     TableInstanceID(viewName),
		Schema: schemaName,
		Table:  viewName,
		Alias:  showStmt.View.Alias,
		Role:   viewName,
	}
	query.Mutation.Target = target
	viewCatalog, ok := context.Catalog.(ViewCatalog)
	if !ok {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogViewNotFound, PhaseBind, "view not found: "+qualifiedCatalogName(schemaName, viewName)),
		}
	}
	view, diagnostics := viewCatalog.View(schemaName, viewName)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}
	if strings.TrimSpace(view.Schema) != "" {
		query.Mutation.Target.Schema = strings.TrimSpace(view.Schema)
	}
	if strings.TrimSpace(view.Name) != "" {
		query.Mutation.Target.Table = strings.TrimSpace(view.Name)
	}
	query.Mutation.ViewSQL = strings.TrimSpace(view.SQL)
	if query.Mutation.ViewSQL == "" {
		query.Mutation.ViewSQL = strings.TrimSpace(view.CanonicalSQL)
	}
	return query, nil
}

// BindShowCreateTable binds parser-neutral SHOW CREATE TABLE metadata into QueryIR.
func BindShowCreateTable(context *BindContext, showStmt UnboundShowCreateTable) (QueryIR, DiagnosticSet) {
	query := QueryIR{
		Kind:     QueryKindShowCreateTable,
		Result:   showStmt.Result,
		Blockers: append([]NativeBlocker(nil), showStmt.Blockers...),
	}
	if context == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	if context.Catalog == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "catalog is nil"),
		}
	}
	tableName := strings.TrimSpace(showStmt.Table.Name)
	if tableName == "" {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "SHOW CREATE TABLE target is empty"),
		}
	}
	schemaName := strings.TrimSpace(showStmt.Table.Schema)
	if schemaName == "" {
		schemaName = context.DefaultSchema
	}
	target := TableInstance{
		ID:     TableInstanceID(tableName),
		Schema: schemaName,
		Table:  tableName,
		Alias:  showStmt.Table.Alias,
		Role:   tableName,
	}
	query.Mutation.Target = target
	table, diagnostics := context.Catalog.Table(schemaName, tableName)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}
	if strings.TrimSpace(table.Schema) != "" {
		query.Mutation.Target.Schema = strings.TrimSpace(table.Schema)
	}
	if strings.TrimSpace(table.Name) != "" {
		query.Mutation.Target.Table = strings.TrimSpace(table.Name)
	}
	query.Mutation.Columns = describeTableFieldRefs(table, query.Mutation.Target)
	return query, nil
}

// BindShowCreateDatabase binds parser-neutral SHOW CREATE DATABASE metadata into QueryIR.
func BindShowCreateDatabase(context *BindContext, showStmt UnboundShowCreateDatabase) (QueryIR, DiagnosticSet) {
	query := QueryIR{
		Kind:     QueryKindShowCreateDatabase,
		Result:   showStmt.Result,
		Blockers: append([]NativeBlocker(nil), showStmt.Blockers...),
	}
	if context == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	if context.Catalog == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "catalog is nil"),
		}
	}
	schemaName := strings.TrimSpace(showStmt.Schema)
	if schemaName == "" {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "SHOW CREATE DATABASE target is empty"),
		}
	}
	if diagnostics := validateCatalogSchema(context, schemaName); diagnostics.BlocksNative() {
		return query, diagnostics
	}
	query.Catalog.Schema = schemaName
	return query, nil
}

// BindShowDatabases binds parser-neutral SHOW DATABASES metadata into QueryIR.
func BindShowDatabases(context *BindContext, showStmt UnboundShowDatabases) (QueryIR, DiagnosticSet) {
	query := QueryIR{
		Kind:     QueryKindShowDatabases,
		Result:   showStmt.Result,
		Blockers: append([]NativeBlocker(nil), showStmt.Blockers...),
	}
	if context == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	if context.Catalog == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "catalog is nil"),
		}
	}
	metadata, ok := context.Catalog.(CatalogMetadata)
	if !ok {
		return query, catalogMetadataUnsupportedDiagnostics()
	}
	schemas, diagnostics := metadata.ListSchemas()
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}
	query.Catalog.Schemas = make([]string, 0, len(schemas))
	for _, schema := range schemas {
		name := strings.TrimSpace(schema.Name)
		if name == "" {
			continue
		}
		query.Catalog.Schemas = append(query.Catalog.Schemas, name)
	}
	return query, nil
}

func validateCatalogSchema(context *BindContext, schemaName string) DiagnosticSet {
	schemaName = strings.TrimSpace(schemaName)
	if schemaName == "" || context == nil || context.Catalog == nil {
		return nil
	}
	metadata, ok := context.Catalog.(CatalogMetadata)
	if !ok {
		return catalogMetadataUnsupportedDiagnostics()
	}
	schemas, diagnostics := metadata.ListSchemas()
	if diagnostics.BlocksNative() {
		return diagnostics
	}
	for _, schema := range schemas {
		if strings.EqualFold(strings.TrimSpace(schema.Name), schemaName) {
			return nil
		}
	}
	return DiagnosticSet{
		ErrorDiagnostic(DiagnosticCatalogSchemaNotFound, PhaseBind, "schema not found: "+schemaName),
	}
}

// BindShowIndex binds parser-neutral SHOW INDEX metadata into QueryIR.
func BindShowIndex(context *BindContext, showStmt UnboundShowIndex) (QueryIR, DiagnosticSet) {
	query := QueryIR{
		Kind:     QueryKindShowIndex,
		Result:   showStmt.Result,
		Blockers: append([]NativeBlocker(nil), showStmt.Blockers...),
	}
	if context == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	if context.Catalog == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "catalog is nil"),
		}
	}
	tableName := strings.TrimSpace(showStmt.Table.Name)
	if tableName == "" {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "SHOW INDEX target is empty"),
		}
	}
	schemaName := strings.TrimSpace(showStmt.Table.Schema)
	if schemaName == "" {
		schemaName = context.DefaultSchema
	}
	target := TableInstance{
		ID:     TableInstanceID(tableName),
		Schema: schemaName,
		Table:  tableName,
		Alias:  showStmt.Table.Alias,
		Role:   tableName,
	}
	query.Mutation.Target = target
	table, diagnostics := context.Catalog.Table(schemaName, tableName)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}
	if strings.TrimSpace(table.Schema) != "" {
		query.Mutation.Target.Schema = strings.TrimSpace(table.Schema)
	}
	if strings.TrimSpace(table.Name) != "" {
		query.Mutation.Target.Table = strings.TrimSpace(table.Name)
	}
	query.Mutation.Columns = describeTableFieldRefs(table, query.Mutation.Target)
	return query, nil
}

type catalogTableObject struct {
	instance   TableInstance
	objectType string
}

func collectCatalogTableObjects(context *BindContext, schemaName string, includeViews bool) ([]catalogTableObject, DiagnosticSet) {
	metadata, ok := context.Catalog.(CatalogMetadata)
	if !ok {
		return nil, catalogMetadataUnsupportedDiagnostics()
	}
	tables, diagnostics := metadata.ListTables(schemaName)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	objects := make([]catalogTableObject, 0, len(tables))
	for _, table := range tables {
		tableName := strings.TrimSpace(table.Name)
		if tableName == "" {
			continue
		}
		tableSchema := strings.TrimSpace(table.Schema)
		if tableSchema == "" {
			tableSchema = schemaName
		}
		objects = append(objects, catalogTableObject{
			instance: TableInstance{
				ID:     TableInstanceID(tableName),
				Schema: tableSchema,
				Table:  tableName,
				Role:   tableName,
			},
			objectType: "BASE TABLE",
		})
	}
	if includeViews {
		if viewMetadata, ok := context.Catalog.(CatalogViewMetadata); ok {
			views, viewDiagnostics := viewMetadata.ListViews(schemaName)
			if viewDiagnostics.BlocksNative() {
				return nil, viewDiagnostics
			}
			for _, view := range views {
				viewName := strings.TrimSpace(view.Name)
				if viewName == "" {
					continue
				}
				viewSchema := strings.TrimSpace(view.Schema)
				if viewSchema == "" {
					viewSchema = schemaName
				}
				objects = append(objects, catalogTableObject{
					instance: TableInstance{
						ID:     TableInstanceID(viewName),
						Schema: viewSchema,
						Table:  viewName,
						Role:   viewName,
					},
					objectType: "VIEW",
				})
			}
		}
	}
	sort.SliceStable(objects, func(i, j int) bool {
		if !strings.EqualFold(objects[i].instance.Schema, objects[j].instance.Schema) {
			return strings.ToLower(objects[i].instance.Schema) < strings.ToLower(objects[j].instance.Schema)
		}
		if !strings.EqualFold(objects[i].instance.Table, objects[j].instance.Table) {
			return strings.ToLower(objects[i].instance.Table) < strings.ToLower(objects[j].instance.Table)
		}
		return objects[i].objectType < objects[j].objectType
	})
	return objects, nil
}

// BindShowTableStatus binds parser-neutral SHOW TABLE STATUS metadata into QueryIR.
func BindShowTableStatus(context *BindContext, showStmt UnboundShowTableStatus) (QueryIR, DiagnosticSet) {
	schemaName := strings.TrimSpace(showStmt.Schema)
	query := QueryIR{
		Kind:     QueryKindShowTableStatus,
		Result:   showTableStatusResultShape(),
		Blockers: append([]NativeBlocker(nil), showStmt.Blockers...),
	}
	if context == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	if context.Catalog == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "catalog is nil"),
		}
	}
	if schemaName == "" {
		schemaName = strings.TrimSpace(context.DefaultSchema)
	}
	query.Catalog.Schema = schemaName
	query.Catalog.Pattern = strings.TrimSpace(showStmt.Pattern)
	objects, diagnostics := collectCatalogTableObjects(context, schemaName, true)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}
	query.Catalog.Objects = make([]TableInstance, 0, len(objects))
	query.Catalog.ObjectTypes = make([]string, 0, len(objects))
	for _, object := range objects {
		if query.Catalog.Pattern != "" && !sqlCatalogLikeMatch(object.instance.Table, query.Catalog.Pattern) {
			continue
		}
		query.Catalog.Objects = append(query.Catalog.Objects, object.instance)
		query.Catalog.ObjectTypes = append(query.Catalog.ObjectTypes, object.objectType)
	}
	return query, nil
}

func sqlCatalogLikeMatch(value string, pattern string) bool {
	value = strings.ToLower(value)
	pattern = strings.ToLower(pattern)
	if pattern == "%" {
		return true
	}
	if !strings.Contains(pattern, "%") {
		return value == pattern
	}
	parts := strings.Split(pattern, "%")
	position := 0
	for i, part := range parts {
		if part == "" {
			continue
		}
		found := strings.Index(value[position:], part)
		if found < 0 {
			return false
		}
		if i == 0 && !strings.HasPrefix(pattern, "%") && found != 0 {
			return false
		}
		position += found + len(part)
	}
	last := parts[len(parts)-1]
	if last != "" && !strings.HasSuffix(pattern, "%") && !strings.HasSuffix(value, last) {
		return false
	}
	return true
}

// BindShowTables binds parser-neutral SHOW TABLES metadata into QueryIR.
func BindShowTables(context *BindContext, showStmt UnboundShowTables) (QueryIR, DiagnosticSet) {
	schemaName := strings.TrimSpace(showStmt.Schema)
	query := QueryIR{
		Kind:     QueryKindShowTables,
		Result:   showTablesResultShape(schemaName, showStmt.Full),
		Blockers: append([]NativeBlocker(nil), showStmt.Blockers...),
	}
	if context == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	if context.Catalog == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "catalog is nil"),
		}
	}
	if schemaName == "" {
		schemaName = strings.TrimSpace(context.DefaultSchema)
	}
	query.Result = showTablesResultShape(schemaName, showStmt.Full)
	query.Catalog.Schema = schemaName
	query.Catalog.Full = showStmt.Full

	objects, diagnostics := collectCatalogTableObjects(context, schemaName, showStmt.Full)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}
	query.Catalog.Objects = make([]TableInstance, 0, len(objects))
	query.Catalog.ObjectTypes = make([]string, 0, len(objects))
	for _, object := range objects {
		query.Catalog.Objects = append(query.Catalog.Objects, object.instance)
		query.Catalog.ObjectTypes = append(query.Catalog.ObjectTypes, object.objectType)
	}
	return query, nil
}

// BindShowVariables binds parser-neutral SHOW VARIABLES metadata into QueryIR.
func BindShowVariables(context *BindContext, showStmt UnboundShowVariables) (QueryIR, DiagnosticSet) {
	query := QueryIR{
		Kind:     QueryKindShowVariables,
		Result:   showVariablesResultShape(),
		Blockers: append([]NativeBlocker(nil), showStmt.Blockers...),
	}
	if context == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	query.Catalog.Pattern = strings.TrimSpace(showStmt.Pattern)
	return query, nil
}

// BindShowStatus binds parser-neutral SHOW STATUS metadata into QueryIR.
func BindShowStatus(context *BindContext, showStmt UnboundShowStatus) (QueryIR, DiagnosticSet) {
	_ = context
	query := QueryIR{
		Kind:     QueryKindShowStatus,
		Result:   showStatusResultShape(),
		Blockers: append([]NativeBlocker(nil), showStmt.Blockers...),
	}
	query.Catalog.Pattern = strings.TrimSpace(showStmt.Pattern)
	return query, nil
}

// BindShowWarnings binds parser-neutral SHOW WARNINGS metadata into QueryIR.
func BindShowWarnings(context *BindContext, showStmt UnboundShowWarnings) (QueryIR, DiagnosticSet) {
	_ = context
	return QueryIR{
		Kind:     QueryKindShowWarnings,
		Result:   showWarningsResultShape(),
		Blockers: append([]NativeBlocker(nil), showStmt.Blockers...),
	}, nil
}

// BindShowCharacterSet binds parser-neutral SHOW CHARACTER SET metadata into QueryIR.
func BindShowCharacterSet(context *BindContext, showStmt UnboundShowCharacterSet) (QueryIR, DiagnosticSet) {
	_ = context
	query := QueryIR{
		Kind:     QueryKindShowCharacterSet,
		Result:   showCharacterSetResultShape(),
		Blockers: append([]NativeBlocker(nil), showStmt.Blockers...),
	}
	query.Catalog.Pattern = strings.TrimSpace(showStmt.Pattern)
	return query, nil
}

// BindShowCollation binds parser-neutral SHOW COLLATION metadata into QueryIR.
func BindShowCollation(context *BindContext, showStmt UnboundShowCollation) (QueryIR, DiagnosticSet) {
	_ = context
	query := QueryIR{
		Kind:     QueryKindShowCollation,
		Result:   showCollationResultShape(),
		Blockers: append([]NativeBlocker(nil), showStmt.Blockers...),
	}
	query.Catalog.Pattern = strings.TrimSpace(showStmt.Pattern)
	return query, nil
}

// BindShowProcesslist binds parser-neutral SHOW PROCESSLIST metadata into QueryIR.
func BindShowProcesslist(context *BindContext, showStmt UnboundShowProcesslist) (QueryIR, DiagnosticSet) {
	_ = context
	query := QueryIR{
		Kind:     QueryKindShowProcesslist,
		Result:   showProcesslistResultShape(showStmt.Full),
		Blockers: append([]NativeBlocker(nil), showStmt.Blockers...),
	}
	query.Catalog.Full = showStmt.Full
	return query, nil
}

// BindDescribe binds parser-neutral DESCRIBE/SHOW COLUMNS metadata into QueryIR.
func BindDescribe(context *BindContext, describeStmt UnboundDescribe) (QueryIR, DiagnosticSet) {
	query := QueryIR{
		Kind:     QueryKindDescribe,
		Result:   describeResultShape(describeStmt.Full),
		Blockers: append([]NativeBlocker(nil), describeStmt.Blockers...),
	}
	if context == nil {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	targetName := strings.TrimSpace(describeStmt.Target.Name)
	if targetName == "" {
		return query, DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "DESCRIBE target is empty"),
		}
	}
	schemaName := strings.TrimSpace(describeStmt.Target.Schema)
	if schemaName == "" {
		schemaName = context.DefaultSchema
	}
	target := TableInstance{
		ID:     TableInstanceID(targetName),
		Schema: schemaName,
		Table:  targetName,
		Alias:  describeStmt.Target.Alias,
		Role:   targetName,
	}
	query.Mutation.Target = target
	query.Catalog.Full = describeStmt.Full

	table, tableDiagnostics := context.Catalog.Table(schemaName, targetName)
	if !tableDiagnostics.BlocksNative() {
		query.Mutation.Columns = describeTableFieldRefs(table, target)
		return query, nil
	}

	viewCatalog, ok := context.Catalog.(ViewCatalog)
	if !ok {
		return query, tableDiagnostics
	}
	view, viewDiagnostics := viewCatalog.View(schemaName, targetName)
	if viewDiagnostics.BlocksNative() {
		return query, tableDiagnostics
	}
	if strings.TrimSpace(view.Schema) != "" {
		query.Mutation.Target.Schema = strings.TrimSpace(view.Schema)
	}
	if strings.TrimSpace(view.Name) != "" {
		query.Mutation.Target.Table = strings.TrimSpace(view.Name)
	}
	viewColumns, diagnostics := describeViewFieldRefs(context, view)
	if diagnostics.BlocksNative() {
		return query, diagnostics
	}
	query.Mutation.Columns = viewColumns
	return query, nil
}

func describeTableFieldRefs(table TableDefinition, target TableInstance) []FieldRef {
	fields := make([]FieldRef, 0, len(table.Fields))
	for _, field := range table.Fields {
		fields = append(fields, field.Ref(target, FieldRoleVisible))
	}
	return fields
}

func describeViewFieldRefs(context *BindContext, view SQLViewDefinition) ([]FieldRef, DiagnosticSet) {
	sql := strings.TrimSpace(view.SQL)
	if sql == "" {
		sql = strings.TrimSpace(view.CanonicalSQL)
	}
	if sql == "" {
		return nil, DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogViewNotFound, PhaseBind, "view SQL is empty: "+qualifiedCatalogName(view.Schema, view.Name)),
		}
	}
	viewStatement, parseDiagnostics := SimpleParserBridge{}.Parse(sql)
	if parseDiagnostics.BlocksNative() {
		return nil, parseDiagnostics
	}
	expanded, expansionDiagnostics := ExpandStatementViews(context.Catalog, SimpleParserBridge{}, context.DefaultSchema, viewStatement)
	if expansionDiagnostics.BlocksNative() {
		return nil, expansionDiagnostics
	}
	if expanded.Kind != QueryKindSelect {
		return nil, DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "DESCRIBE view target must use a SELECT statement"),
		}
	}
	bindContext := NewBindContext(context.Catalog, context.DefaultSchema)
	viewQuery, selectDiagnostics := BindSelect(bindContext, expanded.Select)
	if selectDiagnostics.BlocksNative() {
		return nil, selectDiagnostics
	}
	columns := viewQuery.ResultColumns()
	fields := make([]FieldRef, 0, len(columns))
	target := TableInstance{
		ID:     TableInstanceID(view.Name),
		Schema: view.Schema,
		Table:  view.Name,
		Role:   view.Name,
	}
	for _, column := range columns {
		fields = append(fields, FieldRef{
			Table:    target,
			Name:     column.Name,
			Type:     column.Type,
			Nullable: column.Nullable,
		})
	}
	return fields, nil
}

func viewDependenciesFromQuery(query QueryIR) []TableInstance {
	dependencies := make([]TableInstance, 0, len(query.Sources))
	seen := make(map[string]struct{}, len(query.Sources))
	for _, source := range query.Sources {
		key := strings.ToLower(strings.TrimSpace(source.Schema)) + "." + strings.ToLower(strings.TrimSpace(source.Table))
		if key == "." {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		dependencies = append(dependencies, TableInstance{
			ID:     TableInstanceID(source.Table),
			Schema: source.Schema,
			Table:  source.Table,
			Role:   source.Table,
		})
	}
	return dependencies
}

func bindSchemaOperationTarget(context *BindContext, kind QueryKind, table UnboundTable, result ResultShape, blockers []NativeBlocker) (QueryIR, TableInstance, DiagnosticSet) {
	query := QueryIR{
		Kind:     kind,
		Result:   result,
		Blockers: append([]NativeBlocker(nil), blockers...),
	}
	if context == nil {
		return query, TableInstance{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	table.Name = strings.TrimSpace(table.Name)
	if table.Name == "" {
		return query, TableInstance{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "schema operation target table is empty"),
		}
	}
	schema := strings.TrimSpace(table.Schema)
	if schema == "" {
		schema = context.DefaultSchema
	}
	target := TableInstance{
		ID:     TableInstanceID(table.Name),
		Schema: schema,
		Table:  table.Name,
		Alias:  table.Alias,
		Role:   table.Name,
	}
	query.Sources = []TableInstance{target}
	return query, target, nil
}

func bindTruncateDependentRelationships(context *BindContext, target TableInstance) ([]RelationshipDefinition, DiagnosticSet) {
	if context == nil || context.Catalog == nil || target.Table == "" {
		return nil, nil
	}
	catalog, ok := context.Catalog.(DependentRelationshipCatalog)
	if !ok {
		return nil, nil
	}
	return catalog.DependentRelationships(target.Schema, target.Table)
}

// BindJoin binds one parser-neutral equality join edge.
func BindJoin(context *BindContext, join UnboundJoin) (JoinEdge, DiagnosticSet) {
	if context == nil {
		return JoinEdge{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	left, leftDiagnostics := context.ResolveField(join.LeftQualifier, join.LeftField, FieldRoleJoinInput)
	right, rightDiagnostics := context.ResolveField(join.RightQualifier, join.RightField, FieldRoleJoinInput)
	diagnostics := make(DiagnosticSet, 0, len(leftDiagnostics)+len(rightDiagnostics))
	diagnostics = append(diagnostics, leftDiagnostics...)
	diagnostics = append(diagnostics, rightDiagnostics...)
	if diagnostics.BlocksNative() {
		return JoinEdge{}, diagnostics
	}
	onPredicates := make([]Predicate, 0, len(join.Predicates))
	for _, predicate := range join.Predicates {
		if predicate.Scope == PredicateScopeUnknown {
			predicate.Scope = PredicateScopeOn
		}
		bound, predicateDiagnostics := BindPredicate(context, predicate, FieldRoleResidualInput)
		if predicateDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, predicateDiagnostics...)
			continue
		}
		onPredicates = append(onPredicates, bound)
	}
	if diagnostics.BlocksNative() {
		return JoinEdge{}, diagnostics
	}

	edge := JoinEdge{
		Left:        left,
		Right:       right,
		Kind:        joinKindOrInner(join.Kind),
		Nulls:       nullExtensionForJoin(join.Kind, join.Nulls),
		On:          onPredicates,
		Direction:   JoinPeerEquality,
		Legal:       true,
		Unsupported: join.Unsupported,
	}
	if join.Relationship == "" {
		if relationshipEdge, ok := catalogRelationshipEdgeForFields(context, left, right); ok {
			relationshipEdge.Kind = edge.Kind
			relationshipEdge.Nulls = edge.Nulls
			relationshipEdge.On = onPredicates
			relationshipEdge.Unsupported = join.Unsupported
			return relationshipEdge, nil
		}
		return edge, nil
	}

	relationship, relationshipDiagnostics := context.Catalog.Relationship(join.Relationship)
	if relationshipDiagnostics.BlocksNative() {
		return JoinEdge{}, relationshipDiagnostics
	}
	edge = relationship.Edge(left, right)
	edge.Kind = joinKindOrInner(join.Kind)
	edge.Nulls = nullExtensionForJoin(edge.Kind, join.Nulls)
	edge.On = onPredicates
	edge.Unsupported = join.Unsupported
	return edge, nil
}

func catalogRelationshipEdgeForFields(context *BindContext, left FieldRef, right FieldRef) (JoinEdge, bool) {
	if context == nil {
		return JoinEdge{}, false
	}
	for _, table := range context.Tables() {
		for _, relationship := range table.Definition.Relationships {
			if relationshipMatchesFields(relationship, left, right) {
				return relationship.Edge(left, right), true
			}
			if relationshipMatchesFields(relationship, right, left) {
				return relationship.Edge(left, right), true
			}
		}
	}
	return JoinEdge{}, false
}

func relationshipMatchesFields(relationship RelationshipDefinition, from FieldRef, to FieldRef) bool {
	if !strings.EqualFold(relationship.FromTable, from.Table.Table) || !strings.EqualFold(relationship.FromField, from.Name) {
		return false
	}
	if !strings.EqualFold(relationship.ToTable, to.Table.Table) {
		return false
	}
	return relationship.ToField == "" || strings.EqualFold(relationship.ToField, to.Name)
}

// BindMembership binds one parser-neutral semi/anti membership edge.
func BindMembership(context *BindContext, membership UnboundMembership) (MembershipEdge, DiagnosticSet) {
	if context == nil {
		return MembershipEdge{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	left, leftDiagnostics := context.ResolveField(membership.LeftQualifier, membership.LeftField, FieldRoleResidualInput)
	right, rightDiagnostics := bindMembershipRightField(context, membership)
	diagnostics := make(DiagnosticSet, 0, len(leftDiagnostics)+len(rightDiagnostics))
	diagnostics = append(diagnostics, leftDiagnostics...)
	diagnostics = append(diagnostics, rightDiagnostics...)
	if diagnostics.BlocksNative() {
		return MembershipEdge{}, diagnostics
	}
	predicates := bindMembershipPredicates(context, membership, right, &diagnostics)
	if diagnostics.BlocksNative() {
		return MembershipEdge{}, diagnostics
	}

	kind := membership.Kind
	if kind == MembershipKindUnknown {
		kind = MembershipSemi
	}
	edge := MembershipEdge{
		Left:        left,
		Right:       right,
		Kind:        kind,
		Direction:   JoinPeerEquality,
		Legal:       true,
		Unsupported: membership.Unsupported,
		Predicates:  predicates,
	}
	if membership.Relationship == "" {
		return edge, nil
	}

	relationship, relationshipDiagnostics := context.Catalog.Relationship(membership.Relationship)
	if relationshipDiagnostics.BlocksNative() {
		return MembershipEdge{}, relationshipDiagnostics
	}
	edge.Direction = relationship.Direction
	edge.Cardinality = relationship.Cardinality
	edge.Encoding = cloneRelationshipEncodingProfile(relationship.Encoding)
	return edge, nil
}

func bindMembershipPredicates(context *BindContext, membership UnboundMembership, right FieldRef, diagnostics *DiagnosticSet) []Predicate {
	if len(membership.Predicates) == 0 {
		return nil
	}
	bindContext := context
	if membership.RightTable.Name != "" {
		var tableDiagnostics DiagnosticSet
		bindContext, tableDiagnostics = bindMembershipPredicateContext(context, membership, right)
		if tableDiagnostics.BlocksNative() {
			*diagnostics = append(*diagnostics, tableDiagnostics...)
			return nil
		}
	}
	return bindUnboundPredicates(bindContext, membership.Predicates, diagnostics)
}

func bindMembershipPredicateContext(context *BindContext, membership UnboundMembership, right FieldRef) (*BindContext, DiagnosticSet) {
	if context == nil {
		return nil, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	schema := membership.RightTable.Schema
	if schema == "" {
		schema = context.DefaultSchema
	}
	definition, diagnostics := context.Catalog.Table(schema, membership.RightTable.Name)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	bindContext := NewBindContext(context.Catalog, context.DefaultSchema)
	bindContext.tables = append(bindContext.tables, context.tables...)
	bindContext.nextTableID = context.nextTableID
	bindContext.tables = append(bindContext.tables, BoundTable{
		Instance:   right.Table,
		Definition: definition,
	})
	return bindContext, nil
}

func bindMembershipRightField(context *BindContext, membership UnboundMembership) (FieldRef, DiagnosticSet) {
	if membership.RightTable.Name == "" {
		return context.ResolveField(membership.RightQualifier, membership.RightField, FieldRoleResidualInput)
	}
	schema := membership.RightTable.Schema
	if schema == "" {
		schema = context.DefaultSchema
	}
	definition, diagnostics := context.Catalog.Table(schema, membership.RightTable.Name)
	if diagnostics.BlocksNative() {
		return FieldRef{}, diagnostics
	}
	instance := definition.Instance(TableInstanceID(membership.RightTable.Name+"_membership"), membership.RightTable.Alias)
	field, ok := definition.Field(membership.RightField)
	if !ok {
		return FieldRef{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogFieldNotFound, PhaseBind, "field not found: "+instance.RefName()+"."+membership.RightField),
		}
	}
	return field.Ref(instance, FieldRoleResidualInput), nil
}

// BindSubqueryPlanIntent binds parser-recognized subquery intent into catalog-backed planner intent.
func BindSubqueryPlanIntent(context *BindContext, unbound UnboundSubqueryPlanIntent) (SubqueryPlanIntent, DiagnosticSet) {
	intent := SubqueryPlanIntent{
		Kind:          unbound.Kind,
		Capability:    unbound.Capability,
		HelperIntents: append([]SubqueryHelperIntent(nil), unbound.HelperIntents...),
	}
	switch unbound.Kind {
	case SubqueryIntentCorrelatedAggregate:
		if unbound.CorrelatedAggregate == nil {
			return intent, DiagnosticSet{
				ErrorDiagnostic(DiagnosticCorrelatedAggregateSubquery, PhaseBind, "correlated aggregate subquery intent is empty"),
			}
		}
		correlated, diagnostics := bindCorrelatedAggregateSubqueryIntent(context, *unbound.CorrelatedAggregate)
		if diagnostics.BlocksNative() {
			return intent, diagnostics
		}
		intent.CorrelatedAggregate = &correlated
		return intent, nil
	default:
		return intent, DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "unsupported subquery intent kind: "+string(unbound.Kind)),
		}
	}
}

func bindCorrelatedAggregateSubqueryIntent(context *BindContext, unbound UnboundCorrelatedAggregateSubqueryIntent) (CorrelatedAggregateSubqueryIntent, DiagnosticSet) {
	outerValue, outerValueDiagnostics := bindOuterSubqueryField(context, unbound.OuterValue)
	innerValue, innerValueDiagnostics := bindInnerSubqueryField(context, unbound.InnerTable, unbound.InnerValue)
	innerKey, innerKeyDiagnostics := bindInnerSubqueryField(context, unbound.InnerTable, unbound.InnerKey)
	outerKey, outerKeyDiagnostics := bindOuterSubqueryField(context, unbound.OuterKey)
	diagnostics := make(DiagnosticSet, 0, len(outerValueDiagnostics)+len(innerValueDiagnostics)+len(innerKeyDiagnostics)+len(outerKeyDiagnostics))
	diagnostics = append(diagnostics, outerValueDiagnostics...)
	diagnostics = append(diagnostics, innerValueDiagnostics...)
	diagnostics = append(diagnostics, innerKeyDiagnostics...)
	diagnostics = append(diagnostics, outerKeyDiagnostics...)

	requiredFilters := make([]FieldRef, 0, len(unbound.RequiredFilters))
	for _, filter := range unbound.RequiredFilters {
		ref, filterDiagnostics := bindOuterSubqueryField(context, filter)
		if filterDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, filterDiagnostics...)
			continue
		}
		requiredFilters = append(requiredFilters, ref)
	}
	if diagnostics.BlocksNative() {
		return CorrelatedAggregateSubqueryIntent{}, diagnostics
	}
	return CorrelatedAggregateSubqueryIntent{
		AggregateFunction:    unbound.AggregateFunction,
		Factor:               unbound.Factor,
		SourcePredicate:      unbound.SourcePredicate,
		OuterValue:           outerValue,
		InnerValue:           innerValue,
		InnerKey:             innerKey,
		OuterKey:             outerKey,
		RequiredFilterFields: requiredFilters,
		Scope:                unbound.Scope,
	}, nil
}

func bindOuterSubqueryField(context *BindContext, field UnboundFieldExpr) (FieldRef, DiagnosticSet) {
	return context.ResolveField(field.Qualifier, field.Name, FieldRoleResidualInput)
}

func bindInnerSubqueryField(context *BindContext, table UnboundTable, field UnboundFieldExpr) (FieldRef, DiagnosticSet) {
	if context == nil {
		return FieldRef{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	if context.Catalog == nil {
		return FieldRef{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "catalog is nil"),
		}
	}
	schema := table.Schema
	if schema == "" {
		schema = context.DefaultSchema
	}
	definition, diagnostics := context.Catalog.Table(schema, table.Name)
	if diagnostics.BlocksNative() {
		return FieldRef{}, diagnostics
	}
	instance := definition.Instance(TableInstanceID(table.Name+"_subquery"), table.Alias)
	if field.Qualifier != "" && !strings.EqualFold(field.Qualifier, instance.RefName()) {
		return FieldRef{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticTableAliasNotFound, PhaseBind, "table reference not found in subquery: "+field.Qualifier),
		}
	}
	definitionField, ok := definition.Field(field.Name)
	if !ok {
		return FieldRef{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticCatalogFieldNotFound, PhaseBind, "field not found: "+instance.RefName()+"."+field.Name),
		}
	}
	return definitionField.Ref(instance, FieldRoleResidualInput), nil
}

// BindAggregate binds one parser-neutral aggregate slot.
func BindAggregate(context *BindContext, aggregate UnboundAggregate) (Aggregate, DiagnosticSet) {
	if context == nil {
		return Aggregate{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	function, diagnostics := context.ResolveFunction(aggregate.Function)
	if diagnostics.BlocksNative() {
		return Aggregate{}, diagnostics
	}
	if function.Kind != FunctionAggregate {
		return Aggregate{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedFunction, PhaseBind, "function is not an aggregate: "+function.Name),
		}
	}

	var input Expr
	if !aggregate.CountAll {
		boundInput, inputDiagnostics := BindExpression(context, aggregate.Input, FieldRoleResidualInput)
		if inputDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, inputDiagnostics...)
		} else {
			input = boundInput
		}
	}

	var filter Expr
	if aggregate.Filter != nil {
		boundFilter, filterDiagnostics := BindExpression(context, aggregate.Filter, FieldRoleResidualInput)
		if filterDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, filterDiagnostics...)
		} else {
			filter = boundFilter
		}
	}
	if diagnostics.BlocksNative() {
		return Aggregate{}, diagnostics
	}
	return Aggregate{
		Function:      function.Name,
		Mode:          aggregate.Mode,
		Input:         input,
		Filter:        filter,
		Alias:         aggregate.Alias,
		Type:          aggregateReturnType(aggregate.Type, function.ReturnType, input),
		Origin:        function.Origin,
		Placement:     function.EffectivePlacement(),
		Deterministic: function.Deterministic,
	}, nil
}

func aggregateReturnType(explicit DataType, functionType DataType, input Expr) DataType {
	if explicit != DataTypeUnknown {
		return explicit
	}
	if functionType != DataTypeUnknown {
		return functionType
	}
	return ExprDataType(input)
}

// UnboundExpr is a parser-neutral scalar expression before catalog binding.
type UnboundExpr interface {
	BindExpr(context *BindContext, roles FieldRole) (Expr, DiagnosticSet)
}

// UnboundLiteralExpr is a literal before expression binding.
type UnboundLiteralExpr struct {
	Kind  ValueKind
	Value any
}

// BindExpr converts the unbound literal to a bound literal expression.
func (e UnboundLiteralExpr) BindExpr(context *BindContext, roles FieldRole) (Expr, DiagnosticSet) {
	return Literal(e.Kind, e.Value), nil
}

// UnboundLiteral creates an unbound literal expression.
func UnboundLiteral(kind ValueKind, value any) UnboundLiteralExpr {
	return UnboundLiteralExpr{Kind: kind, Value: value}
}

// UnboundFieldExpr is a field reference before catalog binding.
type UnboundFieldExpr struct {
	Qualifier string
	Name      string
}

// BindExpr resolves the field reference through the binding context.
func (e UnboundFieldExpr) BindExpr(context *BindContext, roles FieldRole) (Expr, DiagnosticSet) {
	if context == nil {
		return nil, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	ref, diagnostics := context.ResolveField(e.Qualifier, e.Name, roles)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	return Field(ref), nil
}

// UnboundField creates an unbound field reference expression.
func UnboundField(qualifier string, name string) UnboundFieldExpr {
	return UnboundFieldExpr{Qualifier: qualifier, Name: name}
}

// UnboundParameterExpr is a prepared-statement placeholder before binding.
type UnboundParameterExpr struct {
	Index    int
	Name     string
	Type     DataType
	Nullable bool
}

// BindExpr converts the placeholder into a bound parameter expression.
func (e UnboundParameterExpr) BindExpr(context *BindContext, roles FieldRole) (Expr, DiagnosticSet) {
	return ParameterExpr{Ref: ParameterRef{Index: e.Index, Name: e.Name, Type: e.Type, Nullable: e.Nullable}}, nil
}

// UnboundParameter creates an unbound prepared-statement parameter expression.
func UnboundParameter(index int, dataType DataType) UnboundParameterExpr {
	return UnboundParameterExpr{Index: index, Type: dataType, Nullable: true}
}

// UnboundListExpr is a scalar expression list before catalog binding.
type UnboundListExpr struct {
	Items []UnboundExpr
}

// BindExpr binds every item in the expression list.
func (e UnboundListExpr) BindExpr(context *BindContext, roles FieldRole) (Expr, DiagnosticSet) {
	items := make([]Expr, 0, len(e.Items))
	diagnostics := make(DiagnosticSet, 0)
	for _, item := range e.Items {
		bound, itemDiagnostics := BindExpression(context, item, roles)
		if itemDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, itemDiagnostics...)
			continue
		}
		items = append(items, bound)
	}
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	return List(items...), nil
}

// UnboundList creates an unbound scalar expression list and copies the supplied item slice.
func UnboundList(items ...UnboundExpr) UnboundListExpr {
	return UnboundListExpr{Items: append([]UnboundExpr(nil), items...)}
}

// UnboundCallExpr is a function call before catalog binding.
type UnboundCallExpr struct {
	Name string
	Args []UnboundExpr
}

// BindExpr resolves the function and binds each argument.
func (e UnboundCallExpr) BindExpr(context *BindContext, roles FieldRole) (Expr, DiagnosticSet) {
	if context == nil {
		return nil, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	function, diagnostics := context.ResolveFunction(e.Name)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}

	args := make([]Expr, 0, len(e.Args))
	for _, arg := range e.Args {
		bound, argDiagnostics := BindExpression(context, arg, roles)
		if argDiagnostics.BlocksNative() {
			diagnostics = append(diagnostics, argDiagnostics...)
			continue
		}
		args = append(args, bound)
	}
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	return FunctionCall(function, args...), nil
}

// UnboundCall creates an unbound function-call expression.
func UnboundCall(name string, args ...UnboundExpr) UnboundCallExpr {
	return UnboundCallExpr{Name: name, Args: append([]UnboundExpr(nil), args...)}
}

// UnboundBinaryExpr is a binary operator expression before catalog binding.
type UnboundBinaryExpr struct {
	Op    BinaryOp
	Left  UnboundExpr
	Right UnboundExpr
}

// BindExpr binds both sides of the binary expression.
func (e UnboundBinaryExpr) BindExpr(context *BindContext, roles FieldRole) (Expr, DiagnosticSet) {
	left, leftDiagnostics := BindExpression(context, e.Left, roles)
	right, rightDiagnostics := BindExpression(context, e.Right, roles)
	diagnostics := make(DiagnosticSet, 0, len(leftDiagnostics)+len(rightDiagnostics))
	diagnostics = append(diagnostics, leftDiagnostics...)
	diagnostics = append(diagnostics, rightDiagnostics...)
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	return Binary(e.Op, left, right), nil
}

// UnboundBinary creates an unbound binary operator expression.
func UnboundBinary(op BinaryOp, left UnboundExpr, right UnboundExpr) UnboundBinaryExpr {
	return UnboundBinaryExpr{Op: op, Left: left, Right: right}
}

// UnboundSearchedCaseWhen stores one unbound searched CASE arm.
type UnboundSearchedCaseWhen struct {
	Condition UnboundExpr
	Result    UnboundExpr
}

// UnboundSearchedCaseExpr is a searched CASE expression before catalog binding.
type UnboundSearchedCaseExpr struct {
	Whens []UnboundSearchedCaseWhen
	Else  UnboundExpr
}

// BindExpr binds each searched CASE condition and result expression.
func (e UnboundSearchedCaseExpr) BindExpr(context *BindContext, roles FieldRole) (Expr, DiagnosticSet) {
	whens := make([]SearchedCaseWhen, 0, len(e.Whens))
	diagnostics := make(DiagnosticSet, 0)
	for _, when := range e.Whens {
		condition, conditionDiagnostics := BindExpression(context, when.Condition, roles)
		diagnostics = append(diagnostics, conditionDiagnostics...)
		result, resultDiagnostics := BindExpression(context, when.Result, roles)
		diagnostics = append(diagnostics, resultDiagnostics...)
		if !conditionDiagnostics.BlocksNative() && !resultDiagnostics.BlocksNative() {
			whens = append(whens, SearchedCaseWhen{Condition: condition, Result: result})
		}
	}
	var elseExpr Expr
	if e.Else != nil {
		var elseDiagnostics DiagnosticSet
		elseExpr, elseDiagnostics = BindExpression(context, e.Else, roles)
		diagnostics = append(diagnostics, elseDiagnostics...)
	}
	if diagnostics.BlocksNative() {
		return nil, diagnostics
	}
	return SearchedCase(whens, elseExpr), nil
}

// UnboundSearchedCase creates an unbound searched CASE expression.
func UnboundSearchedCase(whens []UnboundSearchedCaseWhen, elseExpr UnboundExpr) UnboundSearchedCaseExpr {
	return UnboundSearchedCaseExpr{Whens: append([]UnboundSearchedCaseWhen(nil), whens...), Else: elseExpr}
}

// UnboundAggregateRefExpr references an aggregate slot before binding.
type UnboundAggregateRefExpr struct {
	Alias string
	Index int
}

// BindExpr converts the aggregate reference into a bound aggregate reference.
func (e UnboundAggregateRefExpr) BindExpr(context *BindContext, roles FieldRole) (Expr, DiagnosticSet) {
	return AggregateRef(e.Alias, e.Index), nil
}

// UnboundAggregateRef creates an unbound aggregate-slot reference.
func UnboundAggregateRef(alias string, index int) UnboundAggregateRefExpr {
	return UnboundAggregateRefExpr{Alias: alias, Index: index}
}

// UnboundScalarSubqueryExpr is a scalar subquery before runtime materialization.
type UnboundScalarSubqueryExpr struct {
	SQL   string
	Scope PredicateScope
}

// BindExpr preserves scalar subquery intent as a typed expression. Runtime
// materialization replaces it with a literal before bitmap lowering.
func (e UnboundScalarSubqueryExpr) BindExpr(context *BindContext, roles FieldRole) (Expr, DiagnosticSet) {
	return ScalarSubquery(e.SQL, e.Scope), nil
}

// UnboundScalarSubquery creates an unbound scalar subquery expression.
func UnboundScalarSubquery(sql string, scope PredicateScope) UnboundScalarSubqueryExpr {
	return UnboundScalarSubqueryExpr{SQL: sql, Scope: scope}
}

// UnboundExistsSubqueryExpr is a non-correlated EXISTS gate before runtime
// materialization.
type UnboundExistsSubqueryExpr struct {
	SQL     string
	Negated bool
	Scope   PredicateScope
}

// BindExpr preserves EXISTS gate intent as a typed expression. Runtime
// materialization replaces it with a boolean literal before bitmap lowering.
func (e UnboundExistsSubqueryExpr) BindExpr(context *BindContext, roles FieldRole) (Expr, DiagnosticSet) {
	return ExistsSubquery(e.SQL, e.Negated, e.Scope), nil
}

// UnboundExistsSubquery creates an unbound EXISTS gate expression.
func UnboundExistsSubquery(sql string, negated bool, scope PredicateScope) UnboundExistsSubqueryExpr {
	return UnboundExistsSubqueryExpr{SQL: sql, Negated: negated, Scope: scope}
}

// BindExpression binds one parser-neutral expression.
func BindExpression(context *BindContext, expr UnboundExpr, roles FieldRole) (Expr, DiagnosticSet) {
	if expr == nil {
		return nil, DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "expression is nil"),
		}
	}
	return expr.BindExpr(context, roles)
}

// projectionType returns explicit metadata or expression-inferred metadata.
func projectionType(explicit DataType, expr Expr) DataType {
	if explicit != DataTypeUnknown {
		return explicit
	}
	return ExprDataType(expr)
}

// BindPredicate binds one parser-neutral predicate.
func BindPredicate(context *BindContext, predicate UnboundPredicate, roles FieldRole) (Predicate, DiagnosticSet) {
	if predicate.Scope == PredicateScopeUnknown {
		predicate.Scope = PredicateScopeWhere
	}
	expr, exprDiagnostics := BindExpression(context, predicate.Expr, roles)
	if exprDiagnostics.BlocksNative() {
		return Predicate{}, exprDiagnostics
	}
	capabilities := append([]PlanCapability(nil), predicate.Capabilities...)
	if _, _, _, ok := SameRowBSIComparisonPredicate(Predicate{Expr: expr}); ok {
		capabilities = append(capabilities, CapabilityNativeSameRowBSIComparison)
	}
	placement := boundPredicatePlacement(predicate.Placement, expr)
	if placement == PredicatePushdown {
		if capability, ok := StringEnumPredicateCapability(Predicate{Expr: expr, Placement: placement}); ok {
			capabilities = appendPlanCapabilityOnce(capabilities, capability)
			if stringEnumPredicateUsesBitmapDifference(expr) {
				capabilities = appendPlanCapabilityOnce(capabilities, CapabilityBitmapDifference)
			}
		}
	}
	return Predicate{
		Expr:         expr,
		Placement:    placement,
		Scope:        predicate.Scope,
		Combinator:   predicate.Combinator,
		Capabilities: capabilities,
		Unsupported:  predicate.Unsupported,
	}, nil
}

func boundPredicatePlacement(placement PredicatePlacement, expr Expr) PredicatePlacement {
	if placement == PredicateResidualScan && stringEnumPredicateCanUseBitmapPushdown(expr) {
		return PredicatePushdown
	}
	if placement != PredicatePushdown {
		return placement
	}
	binary, ok := asBinaryExpr(expr)
	if !ok {
		return placement
	}
	if !boundPredicateRequiresResidualRange(binary.Op) {
		return placement
	}
	field, ok := predicateField(binary)
	if !ok || field.Index != IndexBackingString {
		return placement
	}
	return PredicateResidualScan
}

func stringEnumPredicateCanUseBitmapPushdown(expr Expr) bool {
	capability, ok := StringEnumPredicateCapability(Predicate{Expr: expr, Placement: PredicatePushdown})
	return ok && capability != CapabilityStringEnumContainsLike
}

func stringEnumPredicateUsesBitmapDifference(expr Expr) bool {
	binary, ok := asBinaryExpr(expr)
	if !ok {
		return false
	}
	switch binary.Op {
	case BinaryOpNotEqual, BinaryOpNotIn, BinaryOpNotLike:
		return stringEnumPredicateCanUseBitmapPushdown(expr)
	default:
		return false
	}
}

func appendPlanCapabilityOnce(capabilities []PlanCapability, capability PlanCapability) []PlanCapability {
	if capability == "" {
		return capabilities
	}
	for _, existing := range capabilities {
		if existing == capability {
			return capabilities
		}
	}
	return append(capabilities, capability)
}

func boundPredicateRequiresResidualRange(op BinaryOp) bool {
	switch op {
	case BinaryOpBetween, BinaryOpNotBetween,
		BinaryOpLess, BinaryOpLessEqual,
		BinaryOpGreater, BinaryOpGreaterEqual:
		return true
	default:
		return false
	}
}

func bindUnboundPredicates(context *BindContext, predicates []UnboundPredicate, diagnostics *DiagnosticSet) []Predicate {
	boundPredicates := make([]Predicate, 0, len(predicates))
	for _, predicate := range predicates {
		if predicate.Scope == PredicateScopeUnknown {
			predicate.Scope = PredicateScopeHaving
		}
		bound, predicateDiagnostics := BindPredicate(context, predicate, FieldRoleResidualInput)
		if predicateDiagnostics.BlocksNative() {
			*diagnostics = append(*diagnostics, predicateDiagnostics...)
			continue
		}
		boundPredicates = append(boundPredicates, bound)
	}
	return boundPredicates
}

func bindMutationTarget(context *BindContext, kind QueryKind, table UnboundTable, result ResultShape, blockers []NativeBlocker) (QueryIR, BoundTable, DiagnosticSet) {
	if result.Kind == "" {
		result.Kind = ResultStatement
	}
	query := QueryIR{
		Kind:     kind,
		Result:   result,
		Blockers: append([]NativeBlocker(nil), blockers...),
	}
	if context == nil {
		return query, BoundTable{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseBind, "bind context is nil"),
		}
	}
	target, diagnostics := context.AddTable(table)
	if diagnostics.BlocksNative() {
		query.Sources = context.Sources()
		return query, BoundTable{}, diagnostics
	}
	query.Sources = context.Sources()
	return query, target, nil
}

func bindMutationPredicates(context *BindContext, predicates []UnboundPredicate, diagnostics *DiagnosticSet) []Predicate {
	boundPredicates := make([]Predicate, 0, len(predicates))
	for _, predicate := range predicates {
		if predicate.Scope == PredicateScopeUnknown {
			predicate.Scope = PredicateScopeWhere
		}
		bound, predicateDiagnostics := BindPredicate(context, predicate, FieldRoleResidualInput)
		if predicateDiagnostics.BlocksNative() {
			*diagnostics = append(*diagnostics, predicateDiagnostics...)
			continue
		}
		boundPredicates = append(boundPredicates, bound)
	}
	return boundPredicates
}

func joinKindOrInner(kind JoinKind) JoinKind {
	if kind == JoinKindUnknown {
		return JoinKindInner
	}
	return kind
}

func nullExtensionForJoin(kind JoinKind, explicit NullExtension) NullExtension {
	if explicit != NullExtensionNone {
		return explicit
	}
	switch kind {
	case JoinKindLeftOuter:
		return NullExtensionRight
	case JoinKindRightOuter:
		return NullExtensionLeft
	case JoinKindFullOuter:
		return NullExtensionBoth
	default:
		return NullExtensionNone
	}
}

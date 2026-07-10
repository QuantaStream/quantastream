package qsbridge

import "strings"

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
	SQL     string
	Kind    QueryKind
	Select  UnboundSelect
	Insert  UnboundInsert
	Update  UnboundUpdate
	Delete  UnboundDelete
	Session UnboundSession
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

// UnboundSession describes a session-affecting statement before binding.
type UnboundSession struct {
	Actions  []SessionAction
	Result   ResultShape
	Blockers []NativeBlocker
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

const (
	// UnsupportedJoinAntiDifference marks a narrow anti-join-difference edge.
	//
	// The simple SQLRunner parser uses this for fixture anti-join probes that
	// spell the edge as ON a != b. It is not a general SQL non-equality join.
	UnsupportedJoinAntiDifference = "anti_join_difference"
)

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
	predicates := bindMembershipPredicates(context, membership, &diagnostics)
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

func bindMembershipPredicates(context *BindContext, membership UnboundMembership, diagnostics *DiagnosticSet) []Predicate {
	if len(membership.Predicates) == 0 {
		return nil
	}
	bindContext := context
	if membership.RightTable.Name != "" {
		bindContext = NewBindContext(context.Catalog, context.DefaultSchema)
		if _, tableDiagnostics := bindContext.AddTable(membership.RightTable); tableDiagnostics.BlocksNative() {
			*diagnostics = append(*diagnostics, tableDiagnostics...)
			return nil
		}
	}
	return bindUnboundPredicates(bindContext, membership.Predicates, diagnostics)
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

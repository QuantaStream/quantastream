package qsbridge

// QueryKind classifies the top-level SQL operation represented by QueryIR.
type QueryKind string

const (
	// QueryKindSelect identifies a SELECT query.
	QueryKindSelect QueryKind = "select"
	// QueryKindUnionAll identifies a compound SELECT ... UNION ALL ... query.
	QueryKindUnionAll QueryKind = "union_all"
	// QueryKindInsert identifies an INSERT statement.
	QueryKindInsert QueryKind = "insert"
	// QueryKindUpdate identifies an UPDATE statement.
	QueryKindUpdate QueryKind = "update"
	// QueryKindDelete identifies a DELETE statement.
	QueryKindDelete QueryKind = "delete"
	// QueryKindTruncate identifies a TRUNCATE statement.
	QueryKindTruncate QueryKind = "truncate"
	// QueryKindCreateTable identifies a YAML-backed CREATE TABLE activation statement.
	QueryKindCreateTable QueryKind = "create_table"
	// QueryKindDropTable identifies a DROP TABLE deactivation statement.
	QueryKindDropTable QueryKind = "drop_table"
	// QueryKindAlterTable identifies an ALTER TABLE catalog mutation statement.
	QueryKindAlterTable QueryKind = "alter_table"
	// QueryKindCreateView identifies a CREATE VIEW catalog statement.
	QueryKindCreateView QueryKind = "create_view"
	// QueryKindDropView identifies a DROP VIEW catalog statement.
	QueryKindDropView QueryKind = "drop_view"
	// QueryKindShowCreateView identifies a SHOW CREATE VIEW catalog read statement.
	QueryKindShowCreateView QueryKind = "show_create_view"
	// QueryKindShowCreateTable identifies a SHOW CREATE TABLE catalog read statement.
	QueryKindShowCreateTable QueryKind = "show_create_table"
	// QueryKindShowCreateDatabase identifies a SHOW CREATE DATABASE catalog read statement.
	QueryKindShowCreateDatabase QueryKind = "show_create_database"
	// QueryKindShowDatabases identifies a SHOW DATABASES catalog read statement.
	QueryKindShowDatabases QueryKind = "show_databases"
	// QueryKindShowIndex identifies a SHOW INDEX catalog read statement.
	QueryKindShowIndex QueryKind = "show_index"
	// QueryKindShowTableStatus identifies a SHOW TABLE STATUS catalog read statement.
	QueryKindShowTableStatus QueryKind = "show_table_status"
	// QueryKindShowTables identifies a SHOW TABLES catalog read statement.
	QueryKindShowTables QueryKind = "show_tables"
	// QueryKindShowOpenTables identifies a SHOW OPEN TABLES catalog read statement.
	QueryKindShowOpenTables QueryKind = "show_open_tables"
	// QueryKindShowTableTypes identifies a SHOW TABLE TYPES metadata read statement.
	QueryKindShowTableTypes QueryKind = "show_table_types"
	// QueryKindShowFunctionStatus identifies a SHOW FUNCTION STATUS metadata read statement.
	QueryKindShowFunctionStatus QueryKind = "show_function_status"
	// QueryKindShowProcedureStatus identifies a SHOW PROCEDURE STATUS metadata read statement.
	QueryKindShowProcedureStatus QueryKind = "show_procedure_status"
	// QueryKindShowTriggers identifies a SHOW TRIGGERS metadata read statement.
	QueryKindShowTriggers QueryKind = "show_triggers"
	// QueryKindShowEvents identifies a SHOW EVENTS metadata read statement.
	QueryKindShowEvents QueryKind = "show_events"
	// QueryKindShowVariables identifies a SHOW VARIABLES catalog read statement.
	QueryKindShowVariables QueryKind = "show_variables"
	// QueryKindShowStatus identifies a SHOW STATUS metadata read statement.
	QueryKindShowStatus QueryKind = "show_status"
	// QueryKindShowWarnings identifies a SHOW WARNINGS metadata read statement.
	QueryKindShowWarnings QueryKind = "show_warnings"
	// QueryKindShowErrors identifies a SHOW ERRORS metadata read statement.
	QueryKindShowErrors QueryKind = "show_errors"
	// QueryKindShowWarningCount identifies a SHOW COUNT(*) WARNINGS metadata read statement.
	QueryKindShowWarningCount QueryKind = "show_warning_count"
	// QueryKindShowErrorCount identifies a SHOW COUNT(*) ERRORS metadata read statement.
	QueryKindShowErrorCount QueryKind = "show_error_count"
	// QueryKindShowCharacterSet identifies a SHOW CHARACTER SET metadata read statement.
	QueryKindShowCharacterSet QueryKind = "show_character_set"
	// QueryKindShowCollation identifies a SHOW COLLATION metadata read statement.
	QueryKindShowCollation QueryKind = "show_collation"
	// QueryKindShowProcesslist identifies a SHOW PROCESSLIST metadata read statement.
	QueryKindShowProcesslist QueryKind = "show_processlist"
	// QueryKindShowEngines identifies a SHOW ENGINES metadata read statement.
	QueryKindShowEngines QueryKind = "show_engines"
	// QueryKindShowPlugins identifies a SHOW PLUGINS metadata read statement.
	QueryKindShowPlugins QueryKind = "show_plugins"
	// QueryKindShowPrivileges identifies a SHOW PRIVILEGES metadata read statement.
	QueryKindShowPrivileges QueryKind = "show_privileges"
	// QueryKindShowGrants identifies a SHOW GRANTS metadata read statement.
	QueryKindShowGrants QueryKind = "show_grants"
	// QueryKindExplain identifies an EXPLAIN metadata read statement.
	QueryKindExplain QueryKind = "explain"
	// QueryKindDescribe identifies a DESCRIBE/SHOW COLUMNS catalog read statement.
	QueryKindDescribe QueryKind = "describe"
	// QueryKindSession identifies a session-affecting statement such as USE or SET.
	QueryKindSession QueryKind = "session"
	// QueryKindDDL identifies schema-changing statements.
	QueryKindDDL QueryKind = "ddl"
)

// SortDirection describes a client-visible ORDER BY direction.
type SortDirection string

const (
	// SortDefault leaves direction to SQL-default ascending semantics.
	SortDefault SortDirection = ""
	// SortAscending sorts values from low to high.
	SortAscending SortDirection = "asc"
	// SortDescending sorts values from high to low.
	SortDescending SortDirection = "desc"
)

// ProjectionColumn describes one client-visible expression in a query result.
type ProjectionColumn struct {
	Expr  Expr
	Alias string
	Type  DataType
}

// RequiredFields returns the physical fields needed to compute the column.
func (c ProjectionColumn) RequiredFields() []FieldRef {
	return FieldRefs(c.Expr)
}

// ResultColumn returns client-visible metadata for this projection column.
func (c ProjectionColumn) ResultColumn() ResultColumn {
	return ResultColumn{
		Name:     projectionColumnName(c),
		Type:     projectionColumnType(c),
		Nullable: ExprNullable(c.Expr),
		Source:   projectionColumnSource(c.Expr),
	}
}

// ResultColumn describes one client-visible result column.
type ResultColumn struct {
	Name     string
	Type     DataType
	Nullable bool
	Source   string
}

// CatalogReadShape records bound metadata-read payloads such as SHOW TABLES.
type CatalogReadShape struct {
	Schema      string
	Schemas     []string
	Objects     []TableInstance
	ObjectTypes []string
	Functions   []FunctionDefinition
	Full        bool
	Format      string
	Pattern     string
	Patterns    []string
}

// SortSpec describes one ORDER BY expression.
type SortSpec struct {
	Expr      Expr
	Direction SortDirection
}

// RequiredFields returns the physical fields needed to compute the sort key.
func (s SortSpec) RequiredFields() []FieldRef {
	return FieldRefs(s.Expr)
}

// QueryIR is the native SQL engine's bound representation of one statement.
//
// QueryIR is intentionally descriptive. It records the resolved query shape,
// physical field dependencies, and native blockers before any executor-specific
// plan is chosen.
type QueryIR struct {
	Kind        QueryKind
	Sources     []TableInstance
	InlineRows  []InlineRowSet
	Joins       []JoinEdge
	Memberships []MembershipEdge
	Predicates  []Predicate
	WhereExpr   Expr
	Subqueries  []SubqueryPlanIntent
	Projection  []ProjectionColumn
	GroupBy     []Expr
	Aggregates  []Aggregate
	Having      []Predicate
	OrderBy     []SortSpec
	Result      ResultShape
	Mutation    MutationShape
	Catalog     CatalogReadShape
	UnionAll    []QueryIR
	Blockers    []NativeBlocker
}

// Supported reports whether the query is currently executable by the native path.
func (q QueryIR) Supported() bool {
	for _, branch := range q.UnionAll {
		if !branch.Supported() {
			return false
		}
	}
	if len(q.Blockers) > 0 {
		return false
	}
	for _, predicate := range q.Predicates {
		if !predicate.Supported() {
			return false
		}
	}
	for _, predicate := range q.Having {
		if !predicate.Supported() {
			return false
		}
	}
	for _, edge := range q.Joins {
		if !edge.Supported() {
			return false
		}
		for _, predicate := range edge.On {
			if !predicate.Supported() {
				return false
			}
		}
	}
	for _, edge := range q.Memberships {
		if !edge.Supported() {
			return false
		}
	}
	for _, predicate := range q.Mutation.Predicates {
		if !predicate.Supported() {
			return false
		}
	}
	if q.Mutation.DiagnosticsForResult(q.Result).BlocksNative() {
		return false
	}
	return true
}

// Diagnostics returns query-level blockers and unsupported native features.
func (q QueryIR) Diagnostics() DiagnosticSet {
	diagnostics := make(DiagnosticSet, 0, len(q.Blockers))
	for _, branch := range q.UnionAll {
		diagnostics = append(diagnostics, branch.Diagnostics()...)
	}
	for _, blocker := range q.Blockers {
		diagnostics = append(diagnostics, blocker.Diagnostic())
	}
	for _, predicate := range q.Predicates {
		if !predicate.Supported() {
			diagnostics = append(diagnostics, PredicateDiagnostic(predicate))
		}
	}
	for _, predicate := range q.Having {
		if !predicate.Supported() {
			diagnostics = append(diagnostics, PredicateDiagnostic(predicate))
		}
	}
	for _, edge := range q.Joins {
		if !edge.Supported() {
			diagnostics = append(diagnostics, JoinDiagnostic(edge))
		}
		for _, predicate := range edge.On {
			if !predicate.Supported() {
				diagnostics = append(diagnostics, PredicateDiagnostic(predicate))
			}
		}
	}
	for _, edge := range q.Memberships {
		if !edge.Supported() {
			diagnostics = append(diagnostics, MembershipDiagnostic(edge))
		}
		for _, predicate := range edge.Predicates {
			if !predicate.Supported() {
				diagnostics = append(diagnostics, PredicateDiagnostic(predicate))
			}
		}
	}
	for _, predicate := range q.Mutation.Predicates {
		if !predicate.Supported() {
			diagnostics = append(diagnostics, PredicateDiagnostic(predicate))
		}
	}
	diagnostics = append(diagnostics, q.Mutation.DiagnosticsForResult(q.Result)...)
	return diagnostics
}

// RequiredFields returns all physical fields needed by the query in first-seen order.
func (q QueryIR) RequiredFields() []FieldRef {
	return q.RequiredFieldsForScope("")
}

// RequiredParameters returns prepared-statement parameters needed by the query.
func (q QueryIR) RequiredParameters() []ParameterRef {
	collector := newParameterCollector()
	for _, branch := range q.UnionAll {
		for _, ref := range branch.RequiredParameters() {
			collector.addParameter(ref)
		}
	}
	for _, predicate := range q.Predicates {
		collector.addExpr(predicate.Expr)
	}
	collector.addExpr(q.WhereExpr)
	for _, edge := range q.Joins {
		for _, predicate := range edge.On {
			collector.addExpr(predicate.Expr)
		}
	}
	for _, edge := range q.Memberships {
		for _, expr := range edge.LeftTuple {
			collector.addExpr(expr)
		}
		for _, expr := range edge.RightTuple {
			collector.addExpr(expr)
		}
		for _, predicate := range edge.Predicates {
			collector.addExpr(predicate.Expr)
		}
	}
	for _, projection := range q.Projection {
		collector.addExpr(projection.Expr)
	}
	for _, expr := range q.GroupBy {
		collector.addExpr(expr)
	}
	for _, aggregate := range q.Aggregates {
		collector.addExpr(aggregate.Input)
		collector.addExpr(aggregate.Filter)
	}
	for _, predicate := range q.Having {
		collector.addExpr(predicate.Expr)
	}
	for _, sort := range q.OrderBy {
		collector.addExpr(sort.Expr)
	}
	collector.addMutation(q.Mutation)
	return collector.refs
}

// ResultColumns returns client-visible result metadata in projection order.
func (q QueryIR) ResultColumns() []ResultColumn {
	if q.Result.Kind == ResultStatement {
		return nil
	}
	if len(q.UnionAll) > 0 {
		return q.UnionAll[0].ResultColumns()
	}
	if len(q.Projection) > 0 {
		columns := make([]ResultColumn, 0, len(q.Projection))
		for _, projection := range q.Projection {
			if topNColumns, ok := q.topNResultColumns(projection); ok {
				columns = append(columns, topNColumns...)
				continue
			}
			columns = append(columns, projection.ResultColumn())
		}
		return columns
	}

	columns := make([]ResultColumn, 0, len(q.Result.Columns))
	for _, field := range q.Result.Columns {
		columns = append(columns, ResultColumn{
			Name:     field.Name,
			Type:     field.Type,
			Nullable: field.Nullable,
			Source:   field.QualifiedName(),
		})
	}
	return columns
}

func (q QueryIR) topNResultColumns(projection ProjectionColumn) ([]ResultColumn, bool) {
	ref, ok := projection.Expr.(AggregateRefExpr)
	if !ok {
		if pointer, pointerOK := projection.Expr.(*AggregateRefExpr); pointerOK && pointer != nil {
			ref = *pointer
			ok = true
		}
	}
	if !ok || ref.Index < 0 || ref.Index >= len(q.Aggregates) {
		return nil, false
	}
	aggregate := q.Aggregates[ref.Index]
	if aggregate.Function != "topn" {
		return nil, false
	}
	field, ok := aggregate.Input.(FieldExpr)
	if !ok {
		if pointer, pointerOK := aggregate.Input.(*FieldExpr); pointerOK && pointer != nil {
			field = *pointer
			ok = true
		}
	}
	if !ok {
		return nil, false
	}
	return []ResultColumn{
		{Name: "topn_" + field.Ref.Name, Type: DataTypeString, Nullable: true, Source: field.Ref.QualifiedName()},
		{Name: "topn_count", Type: DataTypeInt, Nullable: false, Source: field.Ref.QualifiedName()},
		{Name: "topn_percent", Type: DataTypeFloat, Nullable: false, Source: field.Ref.QualifiedName()},
	}, true
}

// StatementResult returns client-visible OK metadata for non-row statements.
func (q QueryIR) StatementResult() StatementResult {
	return q.Result.Statement
}

// RequiredFieldsForScope returns physical fields for predicates in scope plus all non-predicate query inputs.
func (q QueryIR) RequiredFieldsForScope(scope PredicateScope) []FieldRef {
	collector := newFieldCollector()
	for _, branch := range q.UnionAll {
		for _, field := range branch.RequiredFieldsForScope(scope) {
			collector.addField(field)
		}
	}
	for _, predicate := range q.Predicates {
		if scope == "" || predicate.Scope == scope {
			collector.addExpr(predicate.Expr)
		}
	}
	if scope == "" || scope == PredicateScopeWhere {
		collector.addExpr(q.WhereExpr)
	}
	for _, edge := range q.Joins {
		collector.addField(edge.Left)
		collector.addField(edge.Right)
		for _, predicate := range edge.On {
			collector.addScopedPredicate(predicate, scope)
		}
	}
	for _, edge := range q.Memberships {
		collector.addField(edge.Left)
		collector.addField(edge.Right)
		for _, expr := range edge.LeftTuple {
			collector.addExpr(expr)
		}
		for _, expr := range edge.RightTuple {
			collector.addExpr(expr)
		}
		for _, predicate := range edge.Predicates {
			collector.addScopedPredicate(predicate, scope)
		}
	}
	for _, projection := range q.Projection {
		collector.addExpr(projection.Expr)
	}
	for _, expr := range q.GroupBy {
		collector.addExpr(expr)
	}
	for _, aggregate := range q.Aggregates {
		collector.addExpr(aggregate.Input)
		collector.addExpr(aggregate.Filter)
	}
	for _, predicate := range q.Having {
		if scope == "" || predicate.Scope == scope {
			collector.addExpr(predicate.Expr)
		}
	}
	for _, sort := range q.OrderBy {
		collector.addExpr(sort.Expr)
	}
	for _, hidden := range q.Result.Hidden {
		collector.addField(hidden)
	}
	collector.addMutation(q.Mutation)
	return collector.refs
}

// projectionColumnName chooses the MySQL-visible column name for a projection.
func projectionColumnName(column ProjectionColumn) string {
	if column.Alias != "" {
		return column.Alias
	}
	switch expr := column.Expr.(type) {
	case FieldExpr:
		return expr.Ref.Name
	case *FieldExpr:
		if expr != nil {
			return expr.Ref.Name
		}
	case CallExpr:
		if name, ok := systemVariableProjectionName(expr); ok {
			return name
		}
		return expr.Name
	case *CallExpr:
		if expr != nil {
			if name, ok := systemVariableProjectionName(*expr); ok {
				return name
			}
			return expr.Name
		}
	case AggregateRefExpr:
		return expr.Alias
	case *AggregateRefExpr:
		if expr != nil {
			return expr.Alias
		}
	}
	return ""
}

func systemVariableProjectionName(expr CallExpr) (string, bool) {
	if expr.Name != "qs_session_variable" || len(expr.Args) != 1 {
		return "", false
	}
	literal, ok := expr.Args[0].(LiteralExpr)
	if !ok {
		if pointer, pointerOK := expr.Args[0].(*LiteralExpr); pointerOK && pointer != nil {
			literal = *pointer
			ok = true
		}
	}
	if !ok || literal.Kind != ValueString {
		return "", false
	}
	name, ok := literal.Value.(string)
	if !ok || name == "" {
		return "", false
	}
	return "@@" + name, true
}

// projectionColumnType returns explicit projection metadata or infers it from the expression.
func projectionColumnType(column ProjectionColumn) DataType {
	if column.Type != DataTypeUnknown {
		return column.Type
	}
	return ExprDataType(column.Expr)
}

// projectionColumnSource returns the source field when a projection maps directly to one field.
func projectionColumnSource(expr Expr) string {
	switch n := expr.(type) {
	case FieldExpr:
		return n.Ref.QualifiedName()
	case *FieldExpr:
		if n != nil {
			return n.Ref.QualifiedName()
		}
	}
	return ""
}

type fieldCollector struct {
	refs []FieldRef
	seen map[string]struct{}
}

type parameterCollector struct {
	refs []ParameterRef
	seen map[string]struct{}
}

func newFieldCollector() fieldCollector {
	return fieldCollector{
		refs: make([]FieldRef, 0),
		seen: make(map[string]struct{}),
	}
}

func (c *fieldCollector) addExpr(expr Expr) {
	for _, ref := range FieldRefs(expr) {
		c.addField(ref)
	}
}

func (c *fieldCollector) addScopedPredicate(predicate Predicate, scope PredicateScope) {
	if scope == "" || predicate.Scope == scope {
		c.addExpr(predicate.Expr)
	}
}

func (c *fieldCollector) addField(ref FieldRef) {
	key := string(ref.Table.ID) + "\x00" + ref.Name + "\x00" + ref.PhysicalName
	if _, ok := c.seen[key]; ok {
		return
	}
	c.seen[key] = struct{}{}
	c.refs = append(c.refs, ref)
}

func (c *fieldCollector) addMutation(mutation MutationShape) {
	for _, field := range mutation.Columns {
		c.addField(field)
	}
	for _, row := range mutation.Rows {
		for _, value := range row.Values {
			c.addExpr(value)
		}
	}
	for _, assignment := range mutation.Assignments {
		c.addField(assignment.Field)
		c.addExpr(assignment.Value)
	}
	for _, predicate := range mutation.Predicates {
		c.addExpr(predicate.Expr)
	}
	for _, step := range mutation.ValidationSteps {
		for _, field := range step.Columns {
			c.addField(field)
		}
		for _, field := range step.ReferencedColumns {
			c.addField(field)
		}
	}
}

func newParameterCollector() parameterCollector {
	return parameterCollector{
		refs: make([]ParameterRef, 0),
		seen: make(map[string]struct{}),
	}
}

func (c *parameterCollector) addExpr(expr Expr) {
	for _, ref := range ParameterRefs(expr) {
		c.addParameter(ref)
	}
}

func (c *parameterCollector) addParameter(ref ParameterRef) {
	key := parameterRefKey(ref)
	if _, ok := c.seen[key]; ok {
		return
	}
	c.seen[key] = struct{}{}
	c.refs = append(c.refs, ref)
}

func (c *parameterCollector) addMutation(mutation MutationShape) {
	for _, row := range mutation.Rows {
		for _, value := range row.Values {
			c.addExpr(value)
		}
	}
	for _, assignment := range mutation.Assignments {
		c.addExpr(assignment.Value)
	}
	for _, predicate := range mutation.Predicates {
		c.addExpr(predicate.Expr)
	}
}

// Supported reports whether the join edge can be used by the native path.
func (e JoinEdge) Supported() bool {
	return e.Legal && e.Unsupported == ""
}

package qsbridge

import (
	"strings"
	"testing"
)

func TestBindSelectBuildsQueryIR(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	selectStmt := UnboundSelect{
		Tables: []UnboundTable{{Name: "orders", Alias: "o"}},
		Projection: []UnboundProjection{{
			Expr:  UnboundField("o", "o_orderkey"),
			Alias: "order_id",
			Type:  DataTypeInt,
		}},
		Predicates: []UnboundPredicate{{
			Expr: UnboundBinary(
				BinaryOpGreaterEqual,
				UnboundField("o", "o_orderdate"),
				UnboundLiteral(ValueString, "1995-01-01"),
			),
			Placement:    PredicateResidualScan,
			Scope:        PredicateScopeWhere,
			Capabilities: []PlanCapability{CapabilityResidualScan},
		}},
		GroupBy: []UnboundExpr{UnboundField("o", "o_orderkey")},
		OrderBy: []UnboundSort{{
			Expr:      UnboundField("o", "o_orderkey"),
			Direction: SortDescending,
		}},
	}

	query, diagnostics := BindSelect(context, selectStmt)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Kind != QueryKindSelect {
		t.Fatalf("Kind = %q, want select", query.Kind)
	}
	if len(query.Sources) != 1 || query.Sources[0].RefName() != "o" {
		t.Fatalf("Sources = %#v, want alias o", query.Sources)
	}
	if len(query.Projection) != 1 || query.Projection[0].Alias != "order_id" {
		t.Fatalf("Projection = %#v, want order_id projection", query.Projection)
	}
	projectionField, ok := query.Projection[0].Expr.(FieldExpr)
	if !ok {
		t.Fatalf("projection expr = %T, want FieldExpr", query.Projection[0].Expr)
	}
	if got, want := projectionField.Ref.QualifiedName(), "o.o_orderkey"; got != want {
		t.Fatalf("projection field = %q, want %q", got, want)
	}
	if projectionField.Ref.Type != DataTypeInt {
		t.Fatalf("projection field type = %q, want %q", projectionField.Ref.Type, DataTypeInt)
	}
	if !projectionField.Ref.Roles.Has(FieldRoleVisible) {
		t.Fatalf("expected projection field to be visible")
	}
	resultColumns := query.ResultColumns()
	if len(resultColumns) != 1 || resultColumns[0].Name != "order_id" || resultColumns[0].Type != DataTypeInt || resultColumns[0].Source != "o.o_orderkey" {
		t.Fatalf("result columns = %#v, want typed order_id source column", resultColumns)
	}
	if len(query.Predicates) != 1 || query.Predicates[0].Placement != PredicateResidualScan {
		t.Fatalf("Predicates = %#v, want residual predicate", query.Predicates)
	}
	if query.Predicates[0].Scope != PredicateScopeWhere {
		t.Fatalf("Predicate scope = %q, want where", query.Predicates[0].Scope)
	}
	if len(query.GroupBy) != 1 || len(query.OrderBy) != 1 {
		t.Fatalf("GroupBy/OrderBy = %#v/%#v, want one each", query.GroupBy, query.OrderBy)
	}
}

func TestBindSelectResolvesProjectionAliasesInWherePredicates(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	selectStmt := UnboundSelect{
		Tables: []UnboundTable{{Name: "orders", Alias: "o"}},
		Projection: []UnboundProjection{{
			Expr:  UnboundField("o", "o_totalprice"),
			Alias: "total",
			Type:  DataTypeFloat,
		}},
		Predicates: []UnboundPredicate{{
			Expr: UnboundBinary(
				BinaryOpGreater,
				UnboundField("", "total"),
				UnboundLiteral(ValueFloat, 100.0),
			),
			Placement: PredicatePushdown,
			Scope:     PredicateScopeWhere,
		}},
	}

	query, diagnostics := BindSelect(context, selectStmt)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if len(query.Predicates) != 1 {
		t.Fatalf("Predicates length = %d, want 1", len(query.Predicates))
	}
	predicateExpr, ok := query.Predicates[0].Expr.(BinaryExpr)
	if !ok {
		t.Fatalf("predicate expr = %T, want BinaryExpr", query.Predicates[0].Expr)
	}
	predicateField, ok := predicateExpr.Left.(FieldExpr)
	if !ok {
		t.Fatalf("predicate left = %T, want FieldExpr", predicateExpr.Left)
	}
	if got, want := predicateField.Ref.QualifiedName(), "o.o_totalprice"; got != want {
		t.Fatalf("predicate field = %q, want %q", got, want)
	}
}

func TestBindSelectBindsQ17CorrelatedAggregateIntent(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
select count(*)
from lineitem as l
inner join part as p on p.p_partkey = l.l_partkey
where p.p_brand = 'Brand#45'
  and p.p_container = 'MED JAR'
  and l.l_quantity < (
    select 0.2 * avg(l2.l_quantity)
    from lineitem as l2
    where l2.l_partkey = p.p_partkey
  )`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	query, diagnostics := statement.Bind(NewBindContext(testCorrelatedAggregateCatalog(), "quanta"))
	if diagnostics.BlocksNative() {
		t.Fatalf("bind diagnostics: %#v", diagnostics)
	}
	if got, want := len(query.Subqueries), 1; got != want {
		t.Fatalf("subqueries = %d, want %d: %#v", got, want, query.Subqueries)
	}
	if got, want := len(query.Predicates), 2; got != want {
		t.Fatalf("predicates = %d, want parent filters only: %#v", got, query.Predicates)
	}
	for _, predicate := range query.Predicates {
		if predicateReferencesField(predicate, "l", "l_quantity") {
			t.Fatalf("correlated aggregate predicate leaked into bound predicates: %#v", predicate)
		}
	}
	intent := query.Subqueries[0]
	if !intent.Valid() || intent.CorrelatedAggregate == nil {
		t.Fatalf("subquery intent = %#v, want valid correlated aggregate", intent)
	}
	correlated := intent.CorrelatedAggregate
	if correlated.OuterValue.QualifiedName() != "l.l_quantity" ||
		correlated.InnerValue.QualifiedName() != "l2.l_quantity" ||
		correlated.InnerKey.QualifiedName() != "l2.l_partkey" ||
		correlated.OuterKey.QualifiedName() != "p.p_partkey" {
		t.Fatalf("correlated refs = %#v", correlated)
	}
	if correlated.OuterValue.Type != DataTypeInt || correlated.InnerValue.Type != DataTypeInt || correlated.OuterKey.Type != DataTypeInt {
		t.Fatalf("correlated ref types = outer=%q inner=%q key=%q", correlated.OuterValue.Type, correlated.InnerValue.Type, correlated.OuterKey.Type)
	}
	if !strings.Contains(correlated.SourcePredicate, "l.l_quantity <") {
		t.Fatalf("source predicate = %q, want bound correlated predicate text", correlated.SourcePredicate)
	}
	if got, want := len(correlated.RequiredFilterFields), 2; got != want {
		t.Fatalf("required filter fields = %#v, want %d", correlated.RequiredFilterFields, want)
	}
	plan := BuildLogicalPlan(query)
	found := false
	WalkLogicalPlan(plan.Root, func(node LogicalNode) bool {
		if node.NodeKind() == PlanNodeCorrelatedAggregateSubquery {
			found = true
			return false
		}
		return true
	})
	if !found {
		t.Fatalf("plan root = %#v, want correlated aggregate placeholder", plan.Root)
	}
}

func TestBindSelectPreservesStringEnumDictionaryCapabilities(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	selectStmt := UnboundSelect{
		Tables: []UnboundTable{{Name: "lineitem", Alias: "l"}},
		Projection: []UnboundProjection{{
			Expr:  UnboundField("l", "l_shipmode"),
			Alias: "ship_mode",
			Type:  DataTypeString,
		}},
		Predicates: []UnboundPredicate{{
			Expr: UnboundBinary(
				BinaryOpLike,
				UnboundField("l", "l_shipmode"),
				UnboundLiteral(ValueString, "AIR%"),
			),
			Placement: PredicatePushdown,
			Scope:     PredicateScopeWhere,
		}},
		Result: ResultShape{Kind: ResultQuery},
	}

	query, diagnostics := BindSelect(context, selectStmt)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if len(query.Projection) != 1 {
		t.Fatalf("Projection length = %d, want 1", len(query.Projection))
	}
	projectionField, ok := query.Projection[0].Expr.(FieldExpr)
	if !ok {
		t.Fatalf("projection expr = %T, want FieldExpr", query.Projection[0].Expr)
	}
	if projectionField.Ref.Dictionary.Ref.Field != "l_shipmode" {
		t.Fatalf("projection dictionary = %#v, want l_shipmode dictionary", projectionField.Ref.Dictionary)
	}
	if !projectionField.Ref.SupportsDictionaryCapability(DictionaryCapabilityPrefixMatch) {
		t.Fatalf("expected projection field to retain prefix dictionary capability")
	}
	if projectionField.Ref.Encoding.Kind != EncodingStringEnum {
		t.Fatalf("projection encoding = %q, want %q", projectionField.Ref.Encoding.Kind, EncodingStringEnum)
	}
	if !projectionField.Ref.SupportsPredicateCapability(PredicateCapabilityPrefix) {
		t.Fatalf("expected projection field to retain prefix encoding capability")
	}
	if len(query.Predicates) != 1 {
		t.Fatalf("Predicates length = %d, want 1", len(query.Predicates))
	}
	predicateExpr, ok := query.Predicates[0].Expr.(BinaryExpr)
	if !ok {
		t.Fatalf("predicate expr = %T, want BinaryExpr", query.Predicates[0].Expr)
	}
	predicateField, ok := predicateExpr.Left.(FieldExpr)
	if !ok {
		t.Fatalf("predicate left = %T, want FieldExpr", predicateExpr.Left)
	}
	if !predicateField.Ref.SupportsDictionaryCapability(DictionaryCapabilityPrefixMatch) {
		t.Fatalf("expected predicate field to retain prefix dictionary capability")
	}

	classification := ClassifyNative(query)
	if !classification.HasCapability(CapabilityStringEnumPrefixLike) {
		t.Fatalf("classification capabilities = %#v, want StringEnum prefix LIKE", classification.Capabilities)
	}
	explanation := ExplainLogicalPlan(BuildLogicalPlan(query))
	if !explanation.HasCapability(CapabilityStringEnumPrefixLike) {
		t.Fatalf("explanation capabilities = %#v, want StringEnum prefix LIKE", explanation.Capabilities)
	}
	for _, node := range explanation.Nodes {
		if node.Kind != PlanNodeFilter {
			continue
		}
		if !predicateSummaryHasCapability(node.Predicates, CapabilityStringEnumPrefixLike) {
			t.Fatalf("predicate capabilities = %#v, want StringEnum prefix LIKE", node.Predicates.Capabilities)
		}
		return
	}
	t.Fatalf("expected filter node in explanation")
}

func TestUnboundStatementBindSelect(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement := UnboundStatement{
		SQL:  "select o_orderkey from orders",
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables:     []UnboundTable{{Name: "orders"}},
			Projection: []UnboundProjection{{Expr: UnboundField("", "o_orderkey")}},
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if len(query.Projection) != 1 {
		t.Fatalf("Projection length = %d, want 1", len(query.Projection))
	}
}

func TestUnboundStatementBindInsert(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement := UnboundStatement{
		SQL:  "insert into orders (o_orderkey, o_totalprice) values (?, ?), (?, ?)",
		Kind: QueryKindInsert,
		Insert: UnboundInsert{
			Table:   UnboundTable{Name: "orders"},
			Columns: []string{"o_orderkey", "o_totalprice"},
			Rows: [][]UnboundExpr{
				{UnboundParameter(1, DataTypeInt), UnboundParameter(2, DataTypeFloat)},
				{UnboundParameter(3, DataTypeInt), UnboundParameter(4, DataTypeFloat)},
			},
			Result: ResultShape{
				Statement: StatementResult{AffectedRows: 2, Status: "Records: 2"},
			},
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Kind != QueryKindInsert {
		t.Fatalf("Kind = %q, want insert", query.Kind)
	}
	if query.Result.Kind != ResultStatement {
		t.Fatalf("Result.Kind = %q, want statement", query.Result.Kind)
	}
	if query.Mutation.Kind != MutationInsert {
		t.Fatalf("Mutation.Kind = %q, want insert", query.Mutation.Kind)
	}
	if query.Mutation.Target.Table != "orders" {
		t.Fatalf("Mutation target = %#v, want orders", query.Mutation.Target)
	}
	if len(query.Mutation.Columns) != 2 || query.Mutation.Columns[0].Name != "o_orderkey" || query.Mutation.Columns[1].Name != "o_totalprice" {
		t.Fatalf("mutation columns = %#v, want orderkey and totalprice", query.Mutation.Columns)
	}
	for _, column := range query.Mutation.Columns {
		if !column.Roles.Has(FieldRoleMutationTarget) {
			t.Fatalf("column %s roles = %#v, want mutation target", column.Name, column.Roles)
		}
	}
	if len(query.Mutation.Rows) != 2 || len(query.Mutation.Rows[0].Values) != 2 || len(query.Mutation.Rows[1].Values) != 2 {
		t.Fatalf("mutation rows = %#v, want two rows with two values", query.Mutation.Rows)
	}
	parameters := query.RequiredParameters()
	if len(parameters) != 4 {
		t.Fatalf("RequiredParameters() returned %d refs, want 4", len(parameters))
	}
	if parameters[0].Index != 1 || parameters[1].Index != 2 || parameters[2].Index != 3 || parameters[3].Index != 4 {
		t.Fatalf("parameters = %#v, want parameters 1..4", parameters)
	}
}

func TestUnboundStatementBindUpdate(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement := UnboundStatement{
		SQL:  "update orders set o_totalprice = ? where o_orderkey = ?",
		Kind: QueryKindUpdate,
		Update: UnboundUpdate{
			Table: UnboundTable{Name: "orders", Alias: "o"},
			Assignments: []UnboundAssignment{{
				Column: "o_totalprice",
				Value:  UnboundParameter(1, DataTypeFloat),
			}},
			Predicates: []UnboundPredicate{{
				Expr: UnboundBinary(
					BinaryOpEqual,
					UnboundField("o", "o_orderkey"),
					UnboundParameter(2, DataTypeInt),
				),
				Placement: PredicatePushdown,
			}},
			Result: ResultShape{
				Statement: StatementResult{AffectedRows: 1, Status: "Rows matched: 1"},
			},
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Kind != QueryKindUpdate {
		t.Fatalf("Kind = %q, want update", query.Kind)
	}
	if query.Result.Kind != ResultStatement {
		t.Fatalf("Result.Kind = %q, want statement", query.Result.Kind)
	}
	if query.Mutation.Kind != MutationUpdate {
		t.Fatalf("Mutation.Kind = %q, want update", query.Mutation.Kind)
	}
	if len(query.Mutation.Assignments) != 1 {
		t.Fatalf("assignments = %#v, want one assignment", query.Mutation.Assignments)
	}
	if query.Mutation.Assignments[0].Field.QualifiedName() != "o.o_totalprice" {
		t.Fatalf("assignment field = %q, want o.o_totalprice", query.Mutation.Assignments[0].Field.QualifiedName())
	}
	if !query.Mutation.Assignments[0].Field.Roles.Has(FieldRoleMutationTarget) {
		t.Fatalf("assignment field roles = %#v, want mutation target", query.Mutation.Assignments[0].Field.Roles)
	}
	if len(query.Mutation.Predicates) != 1 || query.Mutation.Predicates[0].Scope != PredicateScopeWhere {
		t.Fatalf("mutation predicates = %#v, want one where predicate", query.Mutation.Predicates)
	}
	parameters := query.RequiredParameters()
	if len(parameters) != 2 || parameters[0].Index != 1 || parameters[1].Index != 2 {
		t.Fatalf("parameters = %#v, want assignment parameter before predicate parameter", parameters)
	}
}

func TestUnboundStatementBindDelete(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement := UnboundStatement{
		SQL:  "delete from orders where o_orderkey = ?",
		Kind: QueryKindDelete,
		Delete: UnboundDelete{
			Table: UnboundTable{Name: "orders"},
			Predicates: []UnboundPredicate{{
				Expr: UnboundBinary(
					BinaryOpEqual,
					UnboundField("", "o_orderkey"),
					UnboundParameter(1, DataTypeInt),
				),
				Placement: PredicatePushdown,
			}},
			Result: ResultShape{
				Statement: StatementResult{AffectedRows: 1},
			},
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Kind != QueryKindDelete {
		t.Fatalf("Kind = %q, want delete", query.Kind)
	}
	if query.Result.Kind != ResultStatement {
		t.Fatalf("Result.Kind = %q, want statement", query.Result.Kind)
	}
	if query.Mutation.Kind != MutationDelete {
		t.Fatalf("Mutation.Kind = %q, want delete", query.Mutation.Kind)
	}
	if query.Mutation.Target.Table != "orders" {
		t.Fatalf("Mutation target = %#v, want orders", query.Mutation.Target)
	}
	if len(query.Mutation.Predicates) != 1 {
		t.Fatalf("mutation predicates = %#v, want one predicate", query.Mutation.Predicates)
	}
	parameters := query.RequiredParameters()
	if len(parameters) != 1 || parameters[0].Index != 1 || parameters[0].Type != DataTypeInt {
		t.Fatalf("parameters = %#v, want int parameter 1", parameters)
	}
}

func TestUnboundStatementBindTruncate(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement := UnboundStatement{
		SQL:  "truncate table orders",
		Kind: QueryKindTruncate,
		Truncate: UnboundTruncate{
			Table:  UnboundTable{Name: "orders"},
			Result: ResultShape{Kind: ResultStatement},
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Kind != QueryKindTruncate {
		t.Fatalf("Kind = %q, want truncate", query.Kind)
	}
	if query.Result.Kind != ResultStatement {
		t.Fatalf("Result.Kind = %q, want statement", query.Result.Kind)
	}
	if query.Mutation.Kind != MutationTruncate {
		t.Fatalf("Mutation.Kind = %q, want truncate", query.Mutation.Kind)
	}
	if query.Mutation.Target.Table != "orders" {
		t.Fatalf("Mutation target = %#v, want orders", query.Mutation.Target)
	}
	access := query.RequiredAccess()
	if !hasAccessRequirement(access, AccessTruncate, "orders") {
		t.Fatalf("RequiredAccess = %#v, want truncate on orders", access)
	}
}

func TestUnboundStatementBindCreateTable(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement := UnboundStatement{
		SQL:  "create table orders",
		Kind: QueryKindCreateTable,
		Create: UnboundCreateTable{
			Table:  UnboundTable{Name: "orders"},
			Result: ResultShape{Kind: ResultStatement},
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Kind != QueryKindCreateTable {
		t.Fatalf("Kind = %q, want create_table", query.Kind)
	}
	if query.Mutation.Kind != MutationCreateTable {
		t.Fatalf("Mutation.Kind = %q, want create_table", query.Mutation.Kind)
	}
	if query.Mutation.Target.Table != "orders" || query.Mutation.Target.Schema != "quanta" {
		t.Fatalf("Mutation target = %#v, want quanta.orders", query.Mutation.Target)
	}
	access := query.RequiredAccess()
	if !hasAccessRequirement(access, AccessCreate, "orders") {
		t.Fatalf("RequiredAccess = %#v, want create on orders", access)
	}
}

func TestUnboundStatementBindCreateView(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement := UnboundStatement{
		SQL:  "create or replace view customer_names as select c_custkey, c_name from customer",
		Kind: QueryKindCreateView,
		CreateView: UnboundCreateView{
			View:    UnboundTable{Name: "customer_names"},
			SQL:     "select c_custkey, c_name from customer",
			Replace: true,
			Result:  ResultShape{Kind: ResultStatement},
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Kind != QueryKindCreateView {
		t.Fatalf("Kind = %q, want create_view", query.Kind)
	}
	if query.Mutation.Kind != MutationCreateView {
		t.Fatalf("Mutation.Kind = %q, want create_view", query.Mutation.Kind)
	}
	if query.Mutation.Target.Table != "customer_names" || query.Mutation.Target.Schema != "quanta" {
		t.Fatalf("Mutation target = %#v, want quanta.customer_names", query.Mutation.Target)
	}
	if !query.Mutation.Replace {
		t.Fatalf("Replace = false, want true")
	}
	if query.Mutation.ViewSQL != "select c_custkey, c_name from customer" {
		t.Fatalf("ViewSQL = %q", query.Mutation.ViewSQL)
	}
	if len(query.Mutation.ViewDependencies) != 1 || query.Mutation.ViewDependencies[0].Table != "customer" {
		t.Fatalf("ViewDependencies = %#v, want customer", query.Mutation.ViewDependencies)
	}
	access := query.RequiredAccess()
	if !hasAccessRequirement(access, AccessCreate, "customer_names") {
		t.Fatalf("RequiredAccess = %#v, want create on customer_names", access)
	}
}

func TestUnboundStatementBindCreateViewInlineColumnList(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement, parseDiagnostics := SimpleParserBridge{}.Parse("create or replace view customer_names (customer_key, customer_name) as select c_custkey, c_name from customer")
	if parseDiagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", parseDiagnostics)
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	wantSQL := "select c_custkey as customer_key, c_name as customer_name from customer"
	if query.Mutation.ViewSQL != wantSQL {
		t.Fatalf("ViewSQL = %q, want %q", query.Mutation.ViewSQL, wantSQL)
	}
	if len(query.Mutation.ViewDependencies) != 1 || query.Mutation.ViewDependencies[0].Table != "customer" {
		t.Fatalf("ViewDependencies = %#v, want customer", query.Mutation.ViewDependencies)
	}
}

func TestUnboundStatementBindDropTableTracksChildDependencies(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement := UnboundStatement{
		SQL:  "drop table customer",
		Kind: QueryKindDropTable,
		Drop: UnboundDropTable{
			Table:  UnboundTable{Name: "customer"},
			Result: ResultShape{Kind: ResultStatement},
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Kind != QueryKindDropTable {
		t.Fatalf("Kind = %q, want drop_table", query.Kind)
	}
	if query.Mutation.Kind != MutationDropTable {
		t.Fatalf("Mutation.Kind = %q, want drop_table", query.Mutation.Kind)
	}
	access := query.RequiredAccess()
	if !hasAccessRequirement(access, AccessDrop, "customer") {
		t.Fatalf("RequiredAccess = %#v, want drop on customer", access)
	}
	if len(query.Mutation.DependentRelationships) != 1 {
		t.Fatalf("dependent relationships = %#v, want orders_customer", query.Mutation.DependentRelationships)
	}
}

func TestUnboundStatementBindDropTableIfExists(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement := UnboundStatement{
		SQL:  "drop table if exists customer",
		Kind: QueryKindDropTable,
		Drop: UnboundDropTable{
			Table:    UnboundTable{Name: "customer"},
			IfExists: true,
			Result:   ResultShape{Kind: ResultStatement},
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Mutation.Kind != MutationDropTable {
		t.Fatalf("Mutation.Kind = %q, want drop_table", query.Mutation.Kind)
	}
	if !query.Mutation.IfExists {
		t.Fatalf("Mutation.IfExists = false, want true")
	}
}

func TestUnboundStatementBindShowCreateView(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{{
		Schema: "quanta",
		Name:   "customer_names",
		SQL:    "select c_custkey, c_name from customer",
	}}
	context := NewBindContext(catalog, "quanta")
	statement := UnboundStatement{
		SQL:  "show create view customer_names",
		Kind: QueryKindShowCreateView,
		ShowView: UnboundShowCreateView{
			View: UnboundTable{Name: "customer_names"},
			Result: ResultShape{
				Kind: ResultQuery,
				Columns: []FieldRef{
					{Name: "View", Type: DataTypeString},
					{Name: "Create View", Type: DataTypeString},
				},
			},
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Kind != QueryKindShowCreateView {
		t.Fatalf("Kind = %q, want show_create_view", query.Kind)
	}
	if query.Mutation.Target.Table != "customer_names" || query.Mutation.Target.Schema != "quanta" {
		t.Fatalf("target = %#v, want quanta.customer_names", query.Mutation.Target)
	}
	if query.Mutation.ViewSQL != "select c_custkey, c_name from customer" {
		t.Fatalf("ViewSQL = %q", query.Mutation.ViewSQL)
	}
	if len(query.Result.Columns) != 2 || query.Result.Columns[0].Name != "View" {
		t.Fatalf("result columns = %#v", query.Result.Columns)
	}
}

func TestUnboundStatementBindDescribeTable(t *testing.T) {
	catalog := MemoryCatalog{
		Tables: []TableDefinition{{
			Schema: "quanta",
			Name:   "customer",
			Fields: []FieldDefinition{
				{Name: "c_custkey", Type: DataTypeInt, PrimaryKey: true},
				{Name: "c_name", Type: DataTypeString, Nullable: true},
			},
		}},
	}
	context := NewBindContext(catalog, "quanta")
	statement := UnboundStatement{
		SQL:  "describe customer",
		Kind: QueryKindDescribe,
		Describe: UnboundDescribe{
			Target: UnboundTable{Name: "customer"},
			Result: describeResultShape(),
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Kind != QueryKindDescribe {
		t.Fatalf("Kind = %q, want describe", query.Kind)
	}
	if query.Mutation.Target.Table != "customer" || query.Mutation.Target.Schema != "quanta" {
		t.Fatalf("target = %#v, want quanta.customer", query.Mutation.Target)
	}
	if got, want := len(query.Mutation.Columns), 2; got != want {
		t.Fatalf("columns = %d, want %d", got, want)
	}
	if !query.Mutation.Columns[0].PrimaryKey || query.Mutation.Columns[0].Name != "c_custkey" {
		t.Fatalf("first column = %#v, want primary c_custkey", query.Mutation.Columns[0])
	}
	if !query.Mutation.Columns[1].Nullable {
		t.Fatalf("second column = %#v, want nullable", query.Mutation.Columns[1])
	}
}

func TestUnboundStatementBindDescribeViewDerivesColumns(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{{
		Schema: "quanta",
		Name:   "customer_names",
		SQL:    "select c_custkey as customer_key, c_name from customer",
	}}
	context := NewBindContext(catalog, "quanta")
	statement := UnboundStatement{
		SQL:  "describe customer_names",
		Kind: QueryKindDescribe,
		Describe: UnboundDescribe{
			Target: UnboundTable{Name: "customer_names"},
			Result: describeResultShape(),
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Mutation.Target.Table != "customer_names" {
		t.Fatalf("target = %#v, want customer_names", query.Mutation.Target)
	}
	if got, want := len(query.Mutation.Columns), 2; got != want {
		t.Fatalf("columns = %d, want %d: %#v", got, want, query.Mutation.Columns)
	}
	if query.Mutation.Columns[0].Name != "customer_key" || query.Mutation.Columns[0].Type != DataTypeInt {
		t.Fatalf("first view column = %#v, want typed customer_key", query.Mutation.Columns[0])
	}
	if query.Mutation.Columns[1].Name != "c_name" || query.Mutation.Columns[1].Type != DataTypeString {
		t.Fatalf("second view column = %#v, want typed c_name", query.Mutation.Columns[1])
	}
}

func TestUnboundStatementBindDropView(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement := UnboundStatement{
		SQL:  "drop view customer_names",
		Kind: QueryKindDropView,
		DropView: UnboundDropView{
			View:   UnboundTable{Name: "customer_names"},
			Result: ResultShape{Kind: ResultStatement},
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Kind != QueryKindDropView {
		t.Fatalf("Kind = %q, want drop_view", query.Kind)
	}
	if query.Mutation.Kind != MutationDropView {
		t.Fatalf("Mutation.Kind = %q, want drop_view", query.Mutation.Kind)
	}
	if query.Mutation.Target.Table != "customer_names" {
		t.Fatalf("Mutation target = %#v, want customer_names", query.Mutation.Target)
	}
	access := query.RequiredAccess()
	if !hasAccessRequirement(access, AccessDrop, "customer_names") {
		t.Fatalf("RequiredAccess = %#v, want drop on customer_names", access)
	}
}

func TestUnboundStatementBindDropViewIfExists(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement := UnboundStatement{
		SQL:  "drop view if exists customer_names",
		Kind: QueryKindDropView,
		DropView: UnboundDropView{
			View:     UnboundTable{Name: "customer_names"},
			IfExists: true,
			Result:   ResultShape{Kind: ResultStatement},
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if query.Mutation.Kind != MutationDropView {
		t.Fatalf("Mutation.Kind = %q, want drop_view", query.Mutation.Kind)
	}
	if !query.Mutation.IfExists {
		t.Fatalf("Mutation.IfExists = false, want true")
	}
}

func TestUnboundStatementBindTruncateTracksChildDependencies(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement := UnboundStatement{
		SQL:  "truncate table customer",
		Kind: QueryKindTruncate,
		Truncate: UnboundTruncate{
			Table:  UnboundTable{Name: "customer"},
			Result: ResultShape{Kind: ResultStatement},
		},
	}

	query, diagnostics := statement.Bind(context)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if len(query.Mutation.DependentRelationships) != 1 {
		t.Fatalf("dependent relationships = %#v, want orders_customer", query.Mutation.DependentRelationships)
	}
	relationship := query.Mutation.DependentRelationships[0]
	if relationship.Name != "orders_customer" || relationship.ChildTable() != "orders" || relationship.ParentTable() != "customer" {
		t.Fatalf("relationship = %#v, want orders child of customer", relationship)
	}
}

func hasAccessRequirement(requirements []AccessRequirement, privilege AccessPrivilege, table string) bool {
	for _, requirement := range requirements {
		if requirement.Privilege == privilege && requirement.Table.Table == table {
			return true
		}
	}
	return false
}

func TestUnboundStatementBindUnsupportedKind(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	statement := UnboundStatement{Kind: QueryKindDDL}

	query, diagnostics := statement.Bind(context)
	if query.Kind != QueryKindDDL {
		t.Fatalf("Kind = %q, want ddl", query.Kind)
	}
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticParserBoundary)
}

func TestBindExpressionResolvesFunctionAlias(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	if _, diagnostics := context.AddTable(UnboundTable{Name: "customer", Alias: "c"}); diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}

	expr, diagnostics := BindExpression(context, UnboundCall("LCASE", UnboundField("c", "c_name")), FieldRoleVisible)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	call, ok := expr.(CallExpr)
	if !ok {
		t.Fatalf("expr = %T, want CallExpr", expr)
	}
	if call.Name != "lower" {
		t.Fatalf("call.Name = %q, want lower", call.Name)
	}
	if call.Origin != FunctionOriginMySQLCompatible || call.Placement != FunctionPlacementExpression || !call.Deterministic {
		t.Fatalf("call metadata = %#v, want MySQL-compatible deterministic function", call)
	}
	if len(call.Args) != 1 {
		t.Fatalf("args length = %d, want 1", len(call.Args))
	}
	field, ok := call.Args[0].(FieldExpr)
	if !ok {
		t.Fatalf("arg = %T, want FieldExpr", call.Args[0])
	}
	if got, want := field.Ref.QualifiedName(), "c.c_name"; got != want {
		t.Fatalf("arg field = %q, want %q", got, want)
	}
}

func TestBindSelectBindsPreparedStatementParameters(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	selectStmt := UnboundSelect{
		Tables: []UnboundTable{{Name: "orders", Alias: "o"}},
		Predicates: []UnboundPredicate{{
			Expr: UnboundBinary(
				BinaryOpGreater,
				UnboundField("o", "o_totalprice"),
				UnboundParameter(1, DataTypeFloat),
			),
			Placement: PredicatePushdown,
		}},
		Projection: []UnboundProjection{{Expr: UnboundField("o", "o_orderkey")}},
	}

	query, diagnostics := BindSelect(context, selectStmt)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	parameters := query.RequiredParameters()
	if len(parameters) != 1 {
		t.Fatalf("RequiredParameters() returned %d refs, want 1", len(parameters))
	}
	if parameters[0].Index != 1 || parameters[0].Type != DataTypeFloat || !parameters[0].Nullable {
		t.Fatalf("parameter = %#v, want nullable float parameter 1", parameters[0])
	}
}

func TestBindSelectReportsBindingDiagnostics(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	selectStmt := UnboundSelect{
		Tables:     []UnboundTable{{Name: "orders", Alias: "o"}},
		Projection: []UnboundProjection{{Expr: UnboundField("o", "missing_field")}},
	}

	query, diagnostics := BindSelect(context, selectStmt)
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticCatalogFieldNotFound)
	if len(query.Projection) != 0 {
		t.Fatalf("Projection length = %d, want 0 after failed bind", len(query.Projection))
	}
}

func TestBindSelectPreservesNativeBlockers(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	selectStmt := UnboundSelect{
		Tables: []UnboundTable{{Name: "orders"}},
		Blockers: []NativeBlocker{{
			Code:   DiagnosticDerivedTable,
			Reason: "derived table placeholder",
			Phase:  PhaseBind,
		}},
	}

	query, diagnostics := BindSelect(context, selectStmt)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	queryDiagnostics := query.Diagnostics()
	assertSingleDiagnosticCode(t, queryDiagnostics, DiagnosticDerivedTable)
}

func TestBindSelectBindsAggregatesHavingAndAggregateRefs(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	selectStmt := UnboundSelect{
		Tables:  []UnboundTable{{Name: "orders", Alias: "o"}},
		GroupBy: []UnboundExpr{UnboundField("o", "o_orderkey")},
		Aggregates: []UnboundAggregate{
			{
				Function: "sum",
				Input:    UnboundField("o", "o_totalprice"),
				Alias:    "revenue",
				Type:     DataTypeFloat,
			},
			{
				Function: "count",
				Alias:    "order_count",
				Type:     DataTypeInt,
				CountAll: true,
			},
		},
		Projection: []UnboundProjection{
			{Expr: UnboundField("o", "o_orderkey")},
			{Expr: UnboundAggregateRef("revenue", 0), Alias: "revenue"},
			{Expr: UnboundAggregateRef("order_count", 1), Alias: "order_count"},
		},
		Having: []UnboundPredicate{{
			Expr:      UnboundBinary(BinaryOpGreater, UnboundAggregateRef("revenue", 0), UnboundLiteral(ValueInt, 100)),
			Placement: PredicateResidualScan,
		}},
		OrderBy: []UnboundSort{{
			Expr:      UnboundAggregateRef("revenue", 0),
			Direction: SortDescending,
		}},
	}

	query, diagnostics := BindSelect(context, selectStmt)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if len(query.Aggregates) != 2 {
		t.Fatalf("Aggregates length = %d, want 2", len(query.Aggregates))
	}
	if query.Aggregates[0].Function != "sum" || query.Aggregates[0].Alias != "revenue" {
		t.Fatalf("first aggregate = %#v, want sum revenue", query.Aggregates[0])
	}
	input, ok := query.Aggregates[0].Input.(FieldExpr)
	if !ok {
		t.Fatalf("aggregate input = %T, want FieldExpr", query.Aggregates[0].Input)
	}
	if got, want := input.Ref.QualifiedName(), "o.o_totalprice"; got != want {
		t.Fatalf("aggregate input = %q, want %q", got, want)
	}
	if !input.Ref.Roles.Has(FieldRoleResidualInput) {
		t.Fatalf("expected aggregate input residual role")
	}
	if query.Aggregates[1].Function != "count" || query.Aggregates[1].Input != nil {
		t.Fatalf("count aggregate = %#v, want count all with nil input", query.Aggregates[1])
	}
	if len(query.Having) != 1 {
		t.Fatalf("Having length = %d, want 1", len(query.Having))
	}
	if len(query.OrderBy) != 1 {
		t.Fatalf("OrderBy length = %d, want 1", len(query.OrderBy))
	}
	sortRef, ok := query.OrderBy[0].Expr.(AggregateRefExpr)
	if !ok {
		t.Fatalf("order expr = %T, want AggregateRefExpr", query.OrderBy[0].Expr)
	}
	if sortRef.Alias != "revenue" || sortRef.Index != 0 {
		t.Fatalf("sort aggregate ref = %#v, want revenue[0]", sortRef)
	}
}

func TestBindAggregateRejectsScalarFunction(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	_, diagnostics := BindAggregate(context, UnboundAggregate{Function: "lower", Input: UnboundLiteral(ValueString, "x")})
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticUnsupportedFunction)
}

func TestBindAggregateReportsMissingInput(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	if _, diagnostics := context.AddTable(UnboundTable{Name: "orders", Alias: "o"}); diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}

	_, diagnostics := BindAggregate(context, UnboundAggregate{Function: "sum"})
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticParserBoundary)
}

func TestBindAggregateSupportsDistinctMode(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	if _, diagnostics := context.AddTable(UnboundTable{Name: "orders", Alias: "o"}); diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}

	aggregate, diagnostics := BindAggregate(context, UnboundAggregate{
		Function: "count",
		Mode:     AggregateDistinct,
		Input:    UnboundField("o", "o_orderkey"),
		Alias:    "distinct_orders",
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if aggregate.Mode != AggregateDistinct || aggregate.Alias != "distinct_orders" {
		t.Fatalf("aggregate = %#v, want distinct_orders distinct mode", aggregate)
	}
	if aggregate.Type != DataTypeInt {
		t.Fatalf("aggregate type = %q, want catalog count return type", aggregate.Type)
	}
	if aggregate.Placement != FunctionPlacementAggregate {
		t.Fatalf("aggregate placement = %q, want aggregate placement", aggregate.Placement)
	}
}

func TestBindSelectBindsRelationshipJoin(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	selectStmt := UnboundSelect{
		Tables: []UnboundTable{
			{Name: "customer", Alias: "c"},
			{Name: "orders", Alias: "o"},
		},
		Joins: []UnboundJoin{{
			LeftQualifier:  "o",
			LeftField:      "o_custkey",
			RightQualifier: "c",
			RightField:     "c_custkey",
			Relationship:   "orders_customer",
			Predicates: []UnboundPredicate{{
				Expr:      UnboundBinary(BinaryOpGreater, UnboundField("o", "o_totalprice"), UnboundLiteral(ValueInt, 100)),
				Placement: PredicateResidualJoin,
			}},
		}},
		Projection: []UnboundProjection{{Expr: UnboundField("c", "c_name")}},
	}

	query, diagnostics := BindSelect(context, selectStmt)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if len(query.Joins) != 1 {
		t.Fatalf("Joins length = %d, want 1", len(query.Joins))
	}
	edge := query.Joins[0]
	if !edge.Supported() {
		t.Fatalf("expected supported join edge: %#v", edge)
	}
	if edge.Direction != JoinChildToParent || edge.Cardinality != "many_to_one" {
		t.Fatalf("edge = %#v, want child-to-parent many-to-one", edge)
	}
	if !edge.Encoding.Supports(RelationshipCapabilityParentLookup) {
		t.Fatalf("edge encoding = %#v, want parent lookup capability", edge.Encoding)
	}
	if !edge.Encoding.Supports(RelationshipCapabilityJoinReduction) {
		t.Fatalf("edge encoding = %#v, want join reduction capability", edge.Encoding)
	}
	if got, want := edge.Left.QualifiedName(), "o.o_custkey"; got != want {
		t.Fatalf("left join field = %q, want %q", got, want)
	}
	if got, want := edge.Right.QualifiedName(), "c.c_custkey"; got != want {
		t.Fatalf("right join field = %q, want %q", got, want)
	}
	if len(edge.On) != 1 {
		t.Fatalf("ON predicates length = %d, want 1", len(edge.On))
	}
	if edge.On[0].Scope != PredicateScopeOn {
		t.Fatalf("ON predicate scope = %q, want on", edge.On[0].Scope)
	}
	if edge.On[0].Placement != PredicateResidualJoin {
		t.Fatalf("ON predicate placement = %q, want residual_join", edge.On[0].Placement)
	}
}

func TestBindJoinDefaultsToPeerEquality(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	for _, table := range []UnboundTable{{Name: "customer", Alias: "c"}, {Name: "orders", Alias: "o"}} {
		if _, diagnostics := context.AddTable(table); diagnostics.BlocksNative() {
			t.Fatalf("unexpected diagnostics for %s: %#v", table.Name, diagnostics)
		}
	}

	edge, diagnostics := BindJoin(context, UnboundJoin{
		LeftQualifier:  "o",
		LeftField:      "o_custkey",
		RightQualifier: "c",
		RightField:     "c_custkey",
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if edge.Direction != JoinPeerEquality || !edge.Legal {
		t.Fatalf("edge = %#v, want legal peer equality", edge)
	}
}

func TestBindJoinDiscoversTableRelationshipForUnnamedEquality(t *testing.T) {
	context := NewBindContext(MemoryCatalog{
		Tables: []TableDefinition{
			{
				Schema: "quanta",
				Name:   "orders",
				Fields: []FieldDefinition{
					{Name: "o_custkey", Type: DataTypeInt, Index: IndexBSI},
				},
				Relationships: []RelationshipDefinition{{
					Name:        "orders_customer",
					FromTable:   "orders",
					FromField:   "o_custkey",
					ToTable:     "customer",
					ToField:     "c_custkey",
					Direction:   JoinChildToParent,
					Cardinality: "many_to_one",
					Encoding: RelationshipEncodingProfile{
						Kind: RelationshipEncodingVector,
						Capabilities: RelationshipCapabilities{
							RelationshipCapabilityParentLookup,
							RelationshipCapabilityJoinReduction,
						},
					},
				}},
			},
			{
				Schema: "quanta",
				Name:   "customer",
				Fields: []FieldDefinition{
					{Name: "c_custkey", Type: DataTypeInt, Index: IndexBSI},
				},
			},
		},
	}, "quanta")
	for _, table := range []UnboundTable{{Name: "customer", Alias: "c"}, {Name: "orders", Alias: "o"}} {
		if _, diagnostics := context.AddTable(table); diagnostics.BlocksNative() {
			t.Fatalf("unexpected diagnostics for %s: %#v", table.Name, diagnostics)
		}
	}

	edge, diagnostics := BindJoin(context, UnboundJoin{
		LeftQualifier:  "c",
		LeftField:      "c_custkey",
		RightQualifier: "o",
		RightField:     "o_custkey",
	})

	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if edge.Direction != JoinChildToParent {
		t.Fatalf("edge direction = %q, want child-to-parent", edge.Direction)
	}
	if !edge.Encoding.Supports(RelationshipCapabilityJoinReduction) {
		t.Fatalf("edge encoding = %#v, want join reduction", edge.Encoding)
	}
	if got, want := edge.Left.QualifiedName(), "c.c_custkey"; got != want {
		t.Fatalf("left edge field = %q, want %q", got, want)
	}
	if got, want := edge.Right.QualifiedName(), "o.o_custkey"; got != want {
		t.Fatalf("right edge field = %q, want %q", got, want)
	}
}

func TestBindJoinReportsMissingRelationship(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	for _, table := range []UnboundTable{{Name: "customer", Alias: "c"}, {Name: "orders", Alias: "o"}} {
		if _, diagnostics := context.AddTable(table); diagnostics.BlocksNative() {
			t.Fatalf("unexpected diagnostics for %s: %#v", table.Name, diagnostics)
		}
	}

	_, diagnostics := BindJoin(context, UnboundJoin{
		LeftQualifier:  "o",
		LeftField:      "o_custkey",
		RightQualifier: "c",
		RightField:     "c_custkey",
		Relationship:   "missing_relationship",
	})
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticCatalogRelationshipNotFound)
}

func TestBindJoinBindsLeftOuterJoinNullExtension(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	for _, table := range []UnboundTable{{Name: "customer", Alias: "c"}, {Name: "orders", Alias: "o"}} {
		if _, diagnostics := context.AddTable(table); diagnostics.BlocksNative() {
			t.Fatalf("unexpected diagnostics for %s: %#v", table.Name, diagnostics)
		}
	}

	edge, diagnostics := BindJoin(context, UnboundJoin{
		LeftQualifier:  "c",
		LeftField:      "c_custkey",
		RightQualifier: "o",
		RightField:     "o_custkey",
		Kind:           JoinKindLeftOuter,
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if edge.Kind != JoinKindLeftOuter {
		t.Fatalf("Kind = %q, want left_outer", edge.Kind)
	}
	if edge.Nulls != NullExtensionRight {
		t.Fatalf("Nulls = %q, want right", edge.Nulls)
	}
	capabilities := edge.Capabilities()
	if len(capabilities) != 3 || capabilities[0] != CapabilityOuterJoin || capabilities[1] != CapabilityNullExtension || capabilities[2] != CapabilityBitmapDifference {
		t.Fatalf("Capabilities() = %#v, want outer + null extension + bitmap difference", capabilities)
	}
}

func TestBindJoinPreservesExplicitNullExtension(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	for _, table := range []UnboundTable{{Name: "customer", Alias: "c"}, {Name: "orders", Alias: "o"}} {
		if _, diagnostics := context.AddTable(table); diagnostics.BlocksNative() {
			t.Fatalf("unexpected diagnostics for %s: %#v", table.Name, diagnostics)
		}
	}

	edge, diagnostics := BindJoin(context, UnboundJoin{
		LeftQualifier:  "c",
		LeftField:      "c_custkey",
		RightQualifier: "o",
		RightField:     "o_custkey",
		Kind:           JoinKindFullOuter,
		Nulls:          NullExtensionBoth,
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if edge.Kind != JoinKindFullOuter || edge.Nulls != NullExtensionBoth {
		t.Fatalf("edge = %#v, want full outer with both null extension", edge)
	}
}

func TestBindSelectBindsAntiMembership(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	selectStmt := UnboundSelect{
		Tables: []UnboundTable{
			{Name: "orders", Alias: "o"},
			{Name: "customer", Alias: "c"},
		},
		Memberships: []UnboundMembership{{
			LeftQualifier:  "o",
			LeftField:      "o_custkey",
			RightQualifier: "c",
			RightField:     "c_custkey",
			Kind:           MembershipAnti,
			Relationship:   "orders_customer",
		}},
		Projection: []UnboundProjection{{Expr: UnboundField("o", "o_orderkey")}},
	}

	query, diagnostics := BindSelect(context, selectStmt)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if len(query.Memberships) != 1 {
		t.Fatalf("Memberships length = %d, want 1", len(query.Memberships))
	}
	edge := query.Memberships[0]
	if edge.Kind != MembershipAnti {
		t.Fatalf("Kind = %q, want anti", edge.Kind)
	}
	if got, want := edge.Left.QualifiedName(), "o.o_custkey"; got != want {
		t.Fatalf("left membership field = %q, want %q", got, want)
	}
	if got, want := edge.Right.QualifiedName(), "c.c_custkey"; got != want {
		t.Fatalf("right membership field = %q, want %q", got, want)
	}
	if !edge.Encoding.Supports(RelationshipCapabilityParentLookup) {
		t.Fatalf("edge encoding = %#v, want parent lookup capability", edge.Encoding)
	}
	if !edge.Encoding.Supports(RelationshipCapabilityAntiJoinDifference) {
		t.Fatalf("edge encoding = %#v, want anti-join difference capability", edge.Encoding)
	}
	capabilities := edge.Capabilities()
	if len(capabilities) != 2 || capabilities[0] != CapabilityAntiMembership || capabilities[1] != CapabilityBitmapDifference {
		t.Fatalf("Capabilities() = %#v, want anti + bitmap difference", capabilities)
	}
}

func TestBindMembershipDefaultsToSemiMembership(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	for _, table := range []UnboundTable{{Name: "orders", Alias: "o"}, {Name: "customer", Alias: "c"}} {
		if _, diagnostics := context.AddTable(table); diagnostics.BlocksNative() {
			t.Fatalf("unexpected diagnostics for %s: %#v", table.Name, diagnostics)
		}
	}

	edge, diagnostics := BindMembership(context, UnboundMembership{
		LeftQualifier:  "o",
		LeftField:      "o_custkey",
		RightQualifier: "c",
		RightField:     "c_custkey",
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if edge.Kind != MembershipSemi || !edge.Supported() {
		t.Fatalf("edge = %#v, want supported semi membership", edge)
	}
}

func TestBindMembershipReportsMissingRelationship(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	for _, table := range []UnboundTable{{Name: "orders", Alias: "o"}, {Name: "customer", Alias: "c"}} {
		if _, diagnostics := context.AddTable(table); diagnostics.BlocksNative() {
			t.Fatalf("unexpected diagnostics for %s: %#v", table.Name, diagnostics)
		}
	}

	_, diagnostics := BindMembership(context, UnboundMembership{
		LeftQualifier:  "o",
		LeftField:      "o_custkey",
		RightQualifier: "c",
		RightField:     "c_custkey",
		Relationship:   "missing_relationship",
	})
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticCatalogRelationshipNotFound)
}

func TestBindPredicatePreservesScope(t *testing.T) {
	context := NewBindContext(testBindCatalog(), "quanta")
	if _, diagnostics := context.AddTable(UnboundTable{Name: "orders", Alias: "o"}); diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}

	predicate, diagnostics := BindPredicate(context, UnboundPredicate{
		Expr:      UnboundBinary(BinaryOpGreater, UnboundField("o", "o_totalprice"), UnboundLiteral(ValueInt, 100)),
		Placement: PredicateResidualJoin,
		Scope:     PredicateScopeOn,
	}, FieldRoleResidualInput)
	if diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", diagnostics)
	}
	if predicate.Scope != PredicateScopeOn {
		t.Fatalf("Scope = %q, want on", predicate.Scope)
	}
	if predicate.Placement != PredicateResidualJoin {
		t.Fatalf("Placement = %q, want residual_join", predicate.Placement)
	}
}

func testCorrelatedAggregateCatalog() MemoryCatalog {
	return MemoryCatalog{
		Tables: []TableDefinition{
			{
				Schema: "quanta",
				Name:   "lineitem",
				Fields: []FieldDefinition{
					{Name: "l_partkey", Type: DataTypeInt, Index: IndexBSI},
					{Name: "l_quantity", Type: DataTypeInt, Index: IndexBSI},
				},
			},
			{
				Schema: "quanta",
				Name:   "part",
				Fields: []FieldDefinition{
					{Name: "p_partkey", Type: DataTypeInt, Index: IndexBSI},
					{Name: "p_brand", Type: DataTypeString, Index: IndexBackingString},
					{Name: "p_container", Type: DataTypeString, Index: IndexStringEnum},
				},
			},
		},
		Functions: []FunctionDefinition{
			{Name: "count", Kind: FunctionAggregate, ReturnType: DataTypeInt, Native: true},
			{Name: "avg", Kind: FunctionAggregate, Arguments: []DataType{DataTypeInt}, ReturnType: DataTypeFloat, Native: true},
		},
	}
}

func predicateReferencesField(predicate Predicate, qualifier string, name string) bool {
	return exprReferencesField(predicate.Expr, qualifier, name)
}

func exprReferencesField(expr Expr, qualifier string, name string) bool {
	switch typed := expr.(type) {
	case FieldExpr:
		return typed.Ref.Table.RefName() == qualifier && typed.Ref.Name == name
	case BinaryExpr:
		return exprReferencesField(typed.Left, qualifier, name) || exprReferencesField(typed.Right, qualifier, name)
	default:
		return false
	}
}

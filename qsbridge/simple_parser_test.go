package qsbridge

import (
	"strings"
	"testing"
)

func TestSimpleParserBridgeParsesUpdateStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("update customers_qa set age = 99, phoneType = 'cell;home', last_name = 'Madden, Jr' where state = 'ID'")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindUpdate {
		t.Fatalf("kind = %q, want update", statement.Kind)
	}
	if statement.Update.Table.Name != "customers_qa" {
		t.Fatalf("table = %#v, want customers_qa", statement.Update.Table)
	}
	if len(statement.Update.Assignments) != 3 {
		t.Fatalf("assignments = %d, want 3", len(statement.Update.Assignments))
	}
	if statement.Update.Assignments[0].Column != "age" {
		t.Fatalf("first assignment = %#v, want age", statement.Update.Assignments[0])
	}
	if literal, ok := statement.Update.Assignments[0].Value.(UnboundLiteralExpr); !ok || literal.Kind != ValueInt || literal.Value != int64(99) {
		t.Fatalf("first assignment value = %#v, want int 99", statement.Update.Assignments[0].Value)
	}
	if statement.Update.Assignments[1].Column != "phoneType" {
		t.Fatalf("second assignment = %#v, want phoneType", statement.Update.Assignments[1])
	}
	if literal, ok := statement.Update.Assignments[1].Value.(UnboundLiteralExpr); !ok || literal.Kind != ValueString || literal.Value != "cell;home" {
		t.Fatalf("second assignment value = %#v, want cell;home", statement.Update.Assignments[1].Value)
	}
	if statement.Update.Assignments[2].Column != "last_name" {
		t.Fatalf("third assignment = %#v, want last_name", statement.Update.Assignments[2])
	}
	if literal, ok := statement.Update.Assignments[2].Value.(UnboundLiteralExpr); !ok || literal.Kind != ValueString || literal.Value != "Madden, Jr" {
		t.Fatalf("third assignment value = %#v, want Madden, Jr", statement.Update.Assignments[2].Value)
	}
	if len(statement.Update.Predicates) != 1 {
		t.Fatalf("predicates = %d, want 1", len(statement.Update.Predicates))
	}
}

func TestSimpleParserBridgeParsesUpdateWithoutWhereForValidation(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("update customers_qa set phoneType = null")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindUpdate {
		t.Fatalf("kind = %q, want update", statement.Kind)
	}
	if len(statement.Update.Predicates) != 0 {
		t.Fatalf("predicates = %d, want validation to handle missing predicate", len(statement.Update.Predicates))
	}
}

func TestSimpleParserBridgeParsesDeleteStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("delete from customers_qa where state = 'ID'")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindDelete {
		t.Fatalf("kind = %q, want delete", statement.Kind)
	}
	if statement.Delete.Table.Name != "customers_qa" {
		t.Fatalf("table = %#v, want customers_qa", statement.Delete.Table)
	}
	if len(statement.Delete.Predicates) != 1 {
		t.Fatalf("predicates = %d, want 1", len(statement.Delete.Predicates))
	}
}

func TestSimpleParserBridgeParsesDeleteWithoutWhereForValidation(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("delete from customers_qa")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindDelete {
		t.Fatalf("kind = %q, want delete", statement.Kind)
	}
	if len(statement.Delete.Predicates) != 0 {
		t.Fatalf("predicates = %d, want validation to handle missing predicate", len(statement.Delete.Predicates))
	}
}

func TestSimpleParserBridgeParsesTruncateStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("truncate table customers_qa;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindTruncate {
		t.Fatalf("kind = %q, want truncate", statement.Kind)
	}
	if statement.Truncate.Table.Name != "customers_qa" {
		t.Fatalf("table = %#v, want customers_qa", statement.Truncate.Table)
	}
	if statement.Truncate.Result.Kind != ResultStatement {
		t.Fatalf("result kind = %q, want statement", statement.Truncate.Result.Kind)
	}
}

func TestSimpleParserBridgeParsesCreateTableStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("create table customers_qa;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindCreateTable {
		t.Fatalf("kind = %q, want create table", statement.Kind)
	}
	if statement.Create.Table.Name != "customers_qa" {
		t.Fatalf("table = %#v, want customers_qa", statement.Create.Table)
	}
	if statement.Create.Result.Kind != ResultStatement {
		t.Fatalf("result kind = %q, want statement", statement.Create.Result.Kind)
	}
}

func TestSimpleParserBridgeParsesDropTableStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("drop table customers_qa;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindDropTable {
		t.Fatalf("kind = %q, want drop table", statement.Kind)
	}
	if statement.Drop.Table.Name != "customers_qa" {
		t.Fatalf("table = %#v, want customers_qa", statement.Drop.Table)
	}
	if statement.Drop.Result.Kind != ResultStatement {
		t.Fatalf("result kind = %q, want statement", statement.Drop.Result.Kind)
	}
}

func TestSimpleParserBridgeParsesDropTableIfExistsStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("drop table if exists customers_qa;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindDropTable {
		t.Fatalf("kind = %q, want drop table", statement.Kind)
	}
	if statement.Drop.Table.Name != "customers_qa" {
		t.Fatalf("table = %#v, want customers_qa", statement.Drop.Table)
	}
	if !statement.Drop.IfExists {
		t.Fatalf("IfExists = false, want true")
	}
}

func TestSimpleParserBridgeParsesCreateViewStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("create view building_customers as select c_custkey, c_name from customer where c_mktsegment = 'BUILDING';")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindCreateView {
		t.Fatalf("kind = %q, want create view", statement.Kind)
	}
	if statement.CreateView.View.Name != "building_customers" {
		t.Fatalf("view = %#v, want building_customers", statement.CreateView.View)
	}
	if statement.CreateView.Replace {
		t.Fatalf("replace should be false")
	}
	if statement.CreateView.SQL != "select c_custkey, c_name from customer where c_mktsegment = 'BUILDING'" {
		t.Fatalf("view sql = %q", statement.CreateView.SQL)
	}
	if statement.CreateView.Result.Kind != ResultStatement {
		t.Fatalf("result kind = %q, want statement", statement.CreateView.Result.Kind)
	}
}

func TestSimpleParserBridgeParsesCreateOrReplaceViewStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("create or replace view building_customers as select c_custkey from customer")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindCreateView {
		t.Fatalf("kind = %q, want create view", statement.Kind)
	}
	if !statement.CreateView.Replace {
		t.Fatalf("replace should be true")
	}
}

func TestSimpleParserBridgeParsesShowCreateViewStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show create view building_customers;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowCreateView {
		t.Fatalf("kind = %q, want show_create_view", statement.Kind)
	}
	if statement.ShowView.View.Name != "building_customers" {
		t.Fatalf("view = %#v, want building_customers", statement.ShowView.View)
	}
	if statement.ShowView.Result.Kind != ResultQuery || len(statement.ShowView.Result.Columns) != 2 {
		t.Fatalf("result = %#v, want two-column query result", statement.ShowView.Result)
	}
}

func TestSimpleParserBridgeParsesShowCreateTableStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show create table customer;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowCreateTable {
		t.Fatalf("kind = %q, want show_create_table", statement.Kind)
	}
	if statement.ShowTable.Table.Name != "customer" {
		t.Fatalf("table = %#v, want customer", statement.ShowTable.Table)
	}
	if statement.ShowTable.Result.Kind != ResultQuery || len(statement.ShowTable.Result.Columns) != 2 {
		t.Fatalf("result = %#v, want two-column query result", statement.ShowTable.Result)
	}
}

func TestSimpleParserBridgeParsesShowDatabasesStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show databases;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowDatabases {
		t.Fatalf("kind = %q, want show_databases", statement.Kind)
	}
	if statement.ShowDBs.Result.Kind != ResultQuery || len(statement.ShowDBs.Result.Columns) != 1 {
		t.Fatalf("result = %#v, want one-column query result", statement.ShowDBs.Result)
	}
	if got, want := statement.ShowDBs.Result.Columns[0].Name, "Database"; got != want {
		t.Fatalf("result column = %q, want %q", got, want)
	}
}

func TestSimpleParserBridgeParsesShowTablesStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show tables;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowTables {
		t.Fatalf("kind = %q, want show_tables", statement.Kind)
	}
	if statement.ShowTables.Schema != "" {
		t.Fatalf("schema = %q, want empty default schema", statement.ShowTables.Schema)
	}
	if statement.ShowTables.Result.Kind != ResultQuery || len(statement.ShowTables.Result.Columns) != 1 {
		t.Fatalf("result = %#v, want one-column query result", statement.ShowTables.Result)
	}
}

func TestSimpleParserBridgeParsesShowTablesFromSchemaStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show tables from quanta;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowTables {
		t.Fatalf("kind = %q, want show_tables", statement.Kind)
	}
	if statement.ShowTables.Schema != "quanta" {
		t.Fatalf("schema = %q, want quanta", statement.ShowTables.Schema)
	}
	if got, want := statement.ShowTables.Result.Columns[0].Name, "Tables_in_quanta"; got != want {
		t.Fatalf("result column = %q, want %q", got, want)
	}
}

func TestSimpleParserBridgeParsesDescribeStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("describe customer;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindDescribe {
		t.Fatalf("kind = %q, want describe", statement.Kind)
	}
	if statement.Describe.Target.Name != "customer" {
		t.Fatalf("target = %#v, want customer", statement.Describe.Target)
	}
	if got := len(statement.Describe.Result.Columns); got != 6 {
		t.Fatalf("describe result columns = %d, want 6", got)
	}
}

func TestSimpleParserBridgeParsesShowColumnsStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show columns from quanta.customer;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindDescribe {
		t.Fatalf("kind = %q, want describe", statement.Kind)
	}
	if statement.Describe.Target.Schema != "quanta" || statement.Describe.Target.Name != "customer" {
		t.Fatalf("target = %#v, want quanta.customer", statement.Describe.Target)
	}
	if statement.Describe.Result.Columns[0].Name != "Field" || statement.Describe.Result.Columns[4].Name != "Default" {
		t.Fatalf("result columns = %#v", statement.Describe.Result.Columns)
	}
}

func TestSimpleParserBridgeParsesDescAlias(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("desc customer")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindDescribe || statement.Describe.Target.Name != "customer" {
		t.Fatalf("statement = %#v, want describe customer", statement)
	}
}

func TestSimpleParserBridgeParsesDropViewStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("drop view building_customers;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindDropView {
		t.Fatalf("kind = %q, want drop view", statement.Kind)
	}
	if statement.DropView.View.Name != "building_customers" {
		t.Fatalf("view = %#v, want building_customers", statement.DropView.View)
	}
	if statement.DropView.Result.Kind != ResultStatement {
		t.Fatalf("result kind = %q, want statement", statement.DropView.Result.Kind)
	}
}

func TestSimpleParserBridgeParsesDropViewIfExistsStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("drop view if exists building_customers;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindDropView {
		t.Fatalf("kind = %q, want drop view", statement.Kind)
	}
	if statement.DropView.View.Name != "building_customers" {
		t.Fatalf("view = %#v, want building_customers", statement.DropView.View)
	}
	if !statement.DropView.IfExists {
		t.Fatalf("IfExists = false, want true")
	}
}

func TestSimpleParserBridgeParsesCreateViewInlineColumnList(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("create or replace view customer_names (customer_key, customer_name) as select c_custkey, c_name from customer;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindCreateView {
		t.Fatalf("kind = %q, want create view", statement.Kind)
	}
	if statement.CreateView.View.Name != "customer_names" {
		t.Fatalf("view = %#v, want customer_names", statement.CreateView.View)
	}
	if !statement.CreateView.Replace {
		t.Fatalf("Replace = false, want true")
	}
	wantSQL := "select c_custkey as customer_key, c_name as customer_name from customer"
	if statement.CreateView.SQL != wantSQL {
		t.Fatalf("CreateView.SQL = %q, want %q", statement.CreateView.SQL, wantSQL)
	}
}

func TestSimpleParserBridgeRejectsInlineCreateTableDefinition(t *testing.T) {
	_, diagnostics := SimpleParserBridge{}.Parse("create table customers_qa (id int)")
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected parser diagnostic for inline CREATE TABLE definition")
	}
}

func TestSimpleParserBridgeRejectsCreateTableAsSelect(t *testing.T) {
	_, diagnostics := SimpleParserBridge{}.Parse("create table customers_copy as select customer_id from customers_qa")
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected parser diagnostic for CREATE TABLE AS SELECT")
	}
}

func TestSimpleParserBridgeParsesOneTableProjectionSelect(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o.o_orderkey as order_id, o.o_totalprice total_price from orders as o where o.o_totalprice >= 101 and o.o_orderkey <= 8 order by o.o_totalprice desc limit 2")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindSelect {
		t.Fatalf("kind = %q, want select", statement.Kind)
	}
	if len(statement.Select.Tables) != 1 {
		t.Fatalf("tables = %d, want 1", len(statement.Select.Tables))
	}
	table := statement.Select.Tables[0]
	if table.Name != "orders" || table.Alias != "o" {
		t.Fatalf("table = %#v, want orders as o", table)
	}
	if len(statement.Select.Projection) != 2 {
		t.Fatalf("projections = %d, want 2", len(statement.Select.Projection))
	}
	first, ok := statement.Select.Projection[0].Expr.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("first projection expression = %T, want UnboundFieldExpr", statement.Select.Projection[0].Expr)
	}
	if first.Qualifier != "o" || first.Name != "o_orderkey" || statement.Select.Projection[0].Alias != "order_id" {
		t.Fatalf("first projection = %#v alias=%q", first, statement.Select.Projection[0].Alias)
	}
	second, ok := statement.Select.Projection[1].Expr.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("second projection expression = %T, want UnboundFieldExpr", statement.Select.Projection[1].Expr)
	}
	if second.Qualifier != "o" || second.Name != "o_totalprice" || statement.Select.Projection[1].Alias != "total_price" {
		t.Fatalf("second projection = %#v alias=%q", second, statement.Select.Projection[1].Alias)
	}
	if len(statement.Select.Predicates) != 2 {
		t.Fatalf("predicates = %d, want 2", len(statement.Select.Predicates))
	}
	predicate, ok := statement.Select.Predicates[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("predicate expression = %T, want UnboundBinaryExpr", statement.Select.Predicates[0].Expr)
	}
	if predicate.Op != BinaryOpGreaterEqual {
		t.Fatalf("predicate op = %q, want %q", predicate.Op, BinaryOpGreaterEqual)
	}
	secondPredicate, ok := statement.Select.Predicates[1].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("second predicate expression = %T, want UnboundBinaryExpr", statement.Select.Predicates[1].Expr)
	}
	if secondPredicate.Op != BinaryOpLessEqual {
		t.Fatalf("second predicate op = %q, want %q", secondPredicate.Op, BinaryOpLessEqual)
	}
	if len(statement.Select.OrderBy) != 1 {
		t.Fatalf("order by = %d, want 1", len(statement.Select.OrderBy))
	}
	if statement.Select.OrderBy[0].Direction != SortDescending {
		t.Fatalf("order by direction = %q, want %q", statement.Select.OrderBy[0].Direction, SortDescending)
	}
	if statement.Select.Result.Limit != 2 {
		t.Fatalf("limit = %d, want 2", statement.Select.Result.Limit)
	}
}

func TestSimpleParserBridgeParsesSelectListScalarSubqueryWithoutOuterFrom(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select (select avg(age) from customers_qa) as average_age")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindSelect {
		t.Fatalf("kind = %q, want select", statement.Kind)
	}
	if len(statement.Select.Tables) != 0 {
		t.Fatalf("tables = %#v, want projection-only select", statement.Select.Tables)
	}
	if got, want := len(statement.Select.Projection), 1; got != want {
		t.Fatalf("projection count = %d, want %d", got, want)
	}
	projection := statement.Select.Projection[0]
	if projection.Alias != "average_age" {
		t.Fatalf("alias = %q, want average_age", projection.Alias)
	}
	scalar, ok := projection.Expr.(UnboundScalarSubqueryExpr)
	if !ok {
		t.Fatalf("projection expr = %T, want scalar subquery", projection.Expr)
	}
	if scalar.Scope != PredicateScopeProjection || scalar.SQL != "select avg(age) from customers_qa" {
		t.Fatalf("scalar subquery = %#v", scalar)
	}
}

func TestSimpleParserBridgeParsesProjectionOnlyLiterals(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select 1 as one, 'alpha' as label, null as missing_value")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindSelect {
		t.Fatalf("kind = %q, want select", statement.Kind)
	}
	if len(statement.Select.Tables) != 0 {
		t.Fatalf("tables = %#v, want projection-only select", statement.Select.Tables)
	}
	if got, want := len(statement.Select.Projection), 3; got != want {
		t.Fatalf("projection count = %d, want %d", got, want)
	}
	first, ok := statement.Select.Projection[0].Expr.(UnboundLiteralExpr)
	if !ok || first.Kind != ValueInt || first.Value != int64(1) {
		t.Fatalf("first projection = %#v, want int literal 1", statement.Select.Projection[0].Expr)
	}
	second, ok := statement.Select.Projection[1].Expr.(UnboundLiteralExpr)
	if !ok || second.Kind != ValueString || second.Value != "alpha" {
		t.Fatalf("second projection = %#v, want string literal alpha", statement.Select.Projection[1].Expr)
	}
	third, ok := statement.Select.Projection[2].Expr.(UnboundLiteralExpr)
	if !ok || third.Kind != ValueNull || third.Value != nil {
		t.Fatalf("third projection = %#v, want null literal", statement.Select.Projection[2].Expr)
	}
}

func TestSimpleParserBridgeParsesQ17CorrelatedAggregateIntent(t *testing.T) {
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
	if got, want := len(statement.Select.Subqueries), 1; got != want {
		t.Fatalf("subqueries = %d, want %d: %#v", got, want, statement.Select.Subqueries)
	}
	if got, want := len(statement.Select.Predicates), 2; got != want {
		t.Fatalf("predicates = %d, want parent filters only: %#v", got, statement.Select.Predicates)
	}
	for _, predicate := range statement.Select.Predicates {
		if unboundPredicateReferencesField(predicate, "l", "l_quantity") {
			t.Fatalf("correlated aggregate predicate leaked into normal predicates: %#v", predicate)
		}
	}
	intent := statement.Select.Subqueries[0]
	if intent.Kind != SubqueryIntentCorrelatedAggregate || intent.CorrelatedAggregate == nil {
		t.Fatalf("subquery intent = %#v, want correlated aggregate", intent)
	}
	correlated := intent.CorrelatedAggregate
	if correlated.AggregateFunction != "avg" || correlated.Factor != 0.2 {
		t.Fatalf("correlated aggregate = %#v, want avg factor 0.2", correlated)
	}
	if !strings.Contains(correlated.SourcePredicate, "l.l_quantity <") || !strings.Contains(correlated.SourcePredicate, "select 0.2 * avg(l2.l_quantity)") {
		t.Fatalf("source predicate = %q, want original correlated predicate text", correlated.SourcePredicate)
	}
	if correlated.OuterValue.Qualifier != "l" || correlated.OuterValue.Name != "l_quantity" ||
		correlated.InnerValue.Qualifier != "l2" || correlated.InnerValue.Name != "l_quantity" ||
		correlated.InnerKey.Qualifier != "l2" || correlated.InnerKey.Name != "l_partkey" ||
		correlated.OuterKey.Qualifier != "p" || correlated.OuterKey.Name != "p_partkey" {
		t.Fatalf("correlated refs = %#v", correlated)
	}
	if got, want := len(correlated.RequiredFilters), 2; got != want {
		t.Fatalf("required filters = %#v, want %d", correlated.RequiredFilters, want)
	}
	if correlated.RequiredFilters[0].Name != "p_brand" || correlated.RequiredFilters[1].Name != "p_container" {
		t.Fatalf("required filters = %#v", correlated.RequiredFilters)
	}
}

func TestSimpleParserBridgeParsesLimitOffset(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o.o_orderkey as order_id from orders as o order by o.o_orderkey limit 1 offset 2")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if !statement.Select.Result.HasResultLimit() {
		t.Fatalf("has limit = false, want true")
	}
	if statement.Select.Result.Limit != 1 {
		t.Fatalf("limit = %d, want 1", statement.Select.Result.Limit)
	}
	if statement.Select.Result.Offset != 2 {
		t.Fatalf("offset = %d, want 2", statement.Select.Result.Offset)
	}
}

func TestSimpleParserBridgeParsesCommaLimitOffset(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o.o_orderkey as order_id from orders as o order by o.o_orderkey limit 2, 3")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if !statement.Select.Result.HasResultLimit() {
		t.Fatalf("has limit = false, want true")
	}
	if statement.Select.Result.Limit != 3 {
		t.Fatalf("limit = %d, want 3", statement.Select.Result.Limit)
	}
	if statement.Select.Result.Offset != 2 {
		t.Fatalf("offset = %d, want 2", statement.Select.Result.Offset)
	}
}

func TestSimpleParserBridgeParsesLimitZero(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o.o_orderkey as order_id from orders as o order by o.o_orderkey limit 0")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if !statement.Select.Result.HasResultLimit() {
		t.Fatalf("has limit = false, want true")
	}
	if statement.Select.Result.Limit != 0 {
		t.Fatalf("limit = %d, want 0", statement.Select.Result.Limit)
	}
	if !statement.Select.Result.AppliesResultWindow() {
		t.Fatalf("applies result window = false, want true")
	}
}

func TestSimpleParserBridgeParsesInequalityPredicates(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select cust_id from customers_qa where cust_id != '101' and city <> 'Seattle'")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Predicates) != 2 {
		t.Fatalf("predicates = %d, want 2", len(statement.Select.Predicates))
	}
	for index, predicate := range statement.Select.Predicates {
		binary, ok := predicate.Expr.(UnboundBinaryExpr)
		if !ok {
			t.Fatalf("predicate %d expression = %T, want UnboundBinaryExpr", index, predicate.Expr)
		}
		if binary.Op != BinaryOpNotEqual {
			t.Fatalf("predicate %d op = %q, want %q", index, binary.Op, BinaryOpNotEqual)
		}
	}
}

func TestSimpleParserBridgeParsesBooleanPredicateLiterals(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select count(*) from customers_qa where isActive = true and isLegalAge != false")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Predicates) != 2 {
		t.Fatalf("predicates = %d, want 2", len(statement.Select.Predicates))
	}
	for index, predicate := range statement.Select.Predicates {
		binary, ok := predicate.Expr.(UnboundBinaryExpr)
		if !ok {
			t.Fatalf("predicate %d expression = %T, want UnboundBinaryExpr", index, predicate.Expr)
		}
		literal, ok := binary.Right.(UnboundLiteralExpr)
		if !ok {
			t.Fatalf("predicate %d right = %T, want UnboundLiteralExpr", index, binary.Right)
		}
		if literal.Kind != ValueBool {
			t.Fatalf("predicate %d literal kind = %q, want bool", index, literal.Kind)
		}
	}
}

func TestSimpleParserBridgeParsesFieldComparisonPredicate(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select count(*) from lineitem_tpch_qa as l where l.l_commitdate < l.l_receiptdate")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Predicates) != 1 {
		t.Fatalf("predicates = %d, want 1", len(statement.Select.Predicates))
	}
	if statement.Select.Predicates[0].Placement != PredicateResidualScan {
		t.Fatalf("predicate placement = %q, want %q", statement.Select.Predicates[0].Placement, PredicateResidualScan)
	}
	predicate, ok := statement.Select.Predicates[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("predicate expression = %T, want UnboundBinaryExpr", statement.Select.Predicates[0].Expr)
	}
	if predicate.Op != BinaryOpLess {
		t.Fatalf("predicate op = %q, want %q", predicate.Op, BinaryOpLess)
	}
	right, ok := predicate.Right.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("predicate right = %T, want UnboundFieldExpr", predicate.Right)
	}
	if right.Qualifier != "l" || right.Name != "l_receiptdate" {
		t.Fatalf("predicate right = %#v, want l.l_receiptdate", right)
	}
}

func TestSimpleParserBridgeDoesNotTreatKeywordInsideStringLiteralAsPredicate(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select count(*) from lineitem_tpch_qa as l where l.l_shipinstruct = 'DELIVER IN PERSON'")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Predicates) != 1 {
		t.Fatalf("predicates = %d, want 1", len(statement.Select.Predicates))
	}
	predicate, ok := statement.Select.Predicates[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("predicate expression = %T, want UnboundBinaryExpr", statement.Select.Predicates[0].Expr)
	}
	if predicate.Op != BinaryOpEqual {
		t.Fatalf("predicate op = %q, want %q", predicate.Op, BinaryOpEqual)
	}
	literal, ok := predicate.Right.(UnboundLiteralExpr)
	if !ok {
		t.Fatalf("predicate right = %T, want UnboundLiteralExpr", predicate.Right)
	}
	if literal.Kind != ValueString || literal.Value != "DELIVER IN PERSON" {
		t.Fatalf("predicate literal = %#v, want DELIVER IN PERSON string", literal)
	}
}

func TestSimpleParserBridgeParsesWildcardProjection(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select * from orders")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Projection) != 1 {
		t.Fatalf("projections = %d, want 1 wildcard", len(statement.Select.Projection))
	}
	field, ok := statement.Select.Projection[0].Expr.(UnboundFieldExpr)
	if !ok || field.Name != "*" {
		t.Fatalf("projection = %#v, want wildcard field", statement.Select.Projection[0])
	}
}

func TestSimpleParserBridgeParsesDistinctProjection(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select distinct first_name from customers_qa")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if !statement.Select.Result.Distinct {
		t.Fatalf("distinct = false, want true")
	}
	if len(statement.Select.Projection) != 1 {
		t.Fatalf("projections = %d, want 1", len(statement.Select.Projection))
	}
	field, ok := statement.Select.Projection[0].Expr.(UnboundFieldExpr)
	if !ok || field.Name != "first_name" {
		t.Fatalf("projection = %#v, want first_name field", statement.Select.Projection[0])
	}
}

func TestSimpleParserBridgeParsesInnerJoinEdge(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select c.first_name, o.order_id from customers_qa as c inner join orders_qa as o on c.cust_id == o.cust_id where o.cust_id = '1'")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Tables) != 2 {
		t.Fatalf("tables = %d, want 2", len(statement.Select.Tables))
	}
	if len(statement.Select.Joins) != 1 {
		t.Fatalf("joins = %d, want 1", len(statement.Select.Joins))
	}
	join := statement.Select.Joins[0]
	if join.Kind != JoinKindInner {
		t.Fatalf("join kind = %q, want inner", join.Kind)
	}
	if join.LeftQualifier != "c" || join.LeftField != "cust_id" || join.RightQualifier != "o" || join.RightField != "cust_id" {
		t.Fatalf("join = %#v, want c.cust_id = o.cust_id", join)
	}
}

func TestSimpleParserBridgeParsesJoinOnResidualConjuncts(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select count(*) from supplier as s inner join lineitem as l on s.s_suppkey = l.l_suppkey and s.s_nationkey = l.l_suppkey")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Joins) != 1 {
		t.Fatalf("joins = %d, want 1", len(statement.Select.Joins))
	}
	join := statement.Select.Joins[0]
	if join.LeftQualifier != "s" || join.LeftField != "s_suppkey" || join.RightQualifier != "l" || join.RightField != "l_suppkey" {
		t.Fatalf("join = %#v, want first ON conjunct as relationship edge", join)
	}
	if len(join.Predicates) != 1 {
		t.Fatalf("join predicates = %d, want 1", len(join.Predicates))
	}
	predicate := join.Predicates[0]
	if predicate.Scope != PredicateScopeOn {
		t.Fatalf("predicate scope = %q, want %q", predicate.Scope, PredicateScopeOn)
	}
	if predicate.Placement != PredicateResidualJoin {
		t.Fatalf("predicate placement = %q, want %q", predicate.Placement, PredicateResidualJoin)
	}
	binary, ok := predicate.Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("predicate expression = %T, want UnboundBinaryExpr", predicate.Expr)
	}
	left, ok := binary.Left.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("left expression = %T, want UnboundFieldExpr", binary.Left)
	}
	right, ok := binary.Right.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("right expression = %T, want UnboundFieldExpr", binary.Right)
	}
	if left.Qualifier != "s" || left.Name != "s_nationkey" || right.Qualifier != "l" || right.Name != "l_suppkey" {
		t.Fatalf("predicate fields = %#v %#v, want s.s_nationkey = l.l_suppkey", left, right)
	}
}

func TestSimpleParserBridgeRejectsNonEqualityJoinEdge(t *testing.T) {
	_, diagnostics := SimpleParserBridge{}.Parse(`
		select c.first_name, o.order_id
		from customers_qa as c
		inner join orders_qa as o on o.cust_id = c.cust_id
		inner join lineitems_qa as l on l.order_id != o.order_id
	`)
	if !diagnostics.BlocksNative() {
		t.Fatalf("expected parser diagnostic for non-equality join edge")
	}
}

func TestSimpleParserBridgeParsesSubqueryMembership(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select c.first_name, c.cust_id
		from customers_qa as c
		where c.cust_id not in (select cust_id from orders_qa)
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Tables) != 1 {
		t.Fatalf("tables = %d, want only outer source", len(statement.Select.Tables))
	}
	if len(statement.Select.Predicates) != 0 {
		t.Fatalf("predicates = %d, want 0", len(statement.Select.Predicates))
	}
	if len(statement.Select.Memberships) != 1 {
		t.Fatalf("memberships = %d, want 1", len(statement.Select.Memberships))
	}
	membership := statement.Select.Memberships[0]
	if membership.Kind != MembershipAnti {
		t.Fatalf("membership kind = %q, want anti", membership.Kind)
	}
	if membership.LeftQualifier != "c" || membership.LeftField != "cust_id" {
		t.Fatalf("left membership = %#v, want c.cust_id", membership)
	}
	if membership.RightQualifier != "orders_qa" || membership.RightField != "cust_id" {
		t.Fatalf("right membership = %#v, want orders_qa.cust_id", membership)
	}
	if membership.RightTable.Name != "orders_qa" {
		t.Fatalf("membership right table = %#v, want orders_qa", membership.RightTable)
	}
}

func TestSimpleParserBridgeParsesFilteredSubqueryMembership(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select c.first_name
		from customers_qa as c
		where c.cust_id in (select cust_id from orders_qa where ship_via = 'UPS')
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Memberships) != 1 {
		t.Fatalf("memberships = %d, want 1", len(statement.Select.Memberships))
	}
	membership := statement.Select.Memberships[0]
	if membership.Kind != MembershipSemi {
		t.Fatalf("membership kind = %q, want semi", membership.Kind)
	}
	if len(membership.Predicates) != 1 {
		t.Fatalf("membership predicates = %d, want 1", len(membership.Predicates))
	}
	binary, ok := membership.Predicates[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("membership predicate = %T, want binary", membership.Predicates[0].Expr)
	}
	if binary.Op != BinaryOpEqual {
		t.Fatalf("membership predicate op = %q, want equal", binary.Op)
	}
	left, ok := binary.Left.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("membership predicate left = %T, want field", binary.Left)
	}
	if left.Qualifier != "orders_qa" || left.Name != "ship_via" {
		t.Fatalf("membership predicate left = %#v, want orders_qa.ship_via", left)
	}
}

func TestSimpleParserBridgeParsesFilteredSubqueryMembershipWithInnerAnd(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select count(*) as forest_partsupp_count
		from partsupp
		where ps_partkey in (
			select p_partkey
			from part
			where p_partkey >= 1
			  and p_name like 'forest%'
		)
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Memberships) != 1 {
		t.Fatalf("memberships = %d, want 1", len(statement.Select.Memberships))
	}
	membership := statement.Select.Memberships[0]
	if membership.LeftField != "ps_partkey" || membership.RightField != "p_partkey" {
		t.Fatalf("membership = %#v, want ps_partkey in p_partkey", membership)
	}
	if len(membership.Predicates) != 2 {
		t.Fatalf("membership predicates = %d, want 2", len(membership.Predicates))
	}
}

func TestSimpleParserBridgeParsesCorrelatedExistsAsMembership(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select c.first_name, c.cust_id
		from customers_qa as c
		where exists (
			select o.cust_id
			from orders_qa as o
			where o.cust_id = c.cust_id
		)
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Memberships) != 1 {
		t.Fatalf("memberships = %d, want 1", len(statement.Select.Memberships))
	}
	membership := statement.Select.Memberships[0]
	if membership.Kind != MembershipSemi {
		t.Fatalf("membership kind = %q, want semi", membership.Kind)
	}
	if membership.LeftQualifier != "c" || membership.LeftField != "cust_id" {
		t.Fatalf("left membership = %#v, want c.cust_id", membership)
	}
	if membership.RightTable.Name != "orders_qa" || membership.RightTable.Alias != "o" {
		t.Fatalf("right table = %#v, want orders_qa as o", membership.RightTable)
	}
	if membership.RightQualifier != "o" || membership.RightField != "cust_id" {
		t.Fatalf("right membership = %#v, want o.cust_id", membership)
	}
}

func TestSimpleParserBridgeParsesCorrelatedExistsWithSiblingPredicate(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select count(*) as same_order_other_supplier_count
		from lineitem as l1
		where exists (
			select *
			from lineitem as l2
			where l2.l_orderkey = l1.l_orderkey
			  and l2.l_suppkey <> l1.l_suppkey
		)
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Memberships) != 1 {
		t.Fatalf("memberships = %d, want 1", len(statement.Select.Memberships))
	}
	membership := statement.Select.Memberships[0]
	if membership.LeftQualifier != "l1" || membership.LeftField != "l_orderkey" {
		t.Fatalf("left membership = %#v, want l1.l_orderkey", membership)
	}
	if membership.RightTable.Name != "lineitem" || membership.RightTable.Alias != "l2" {
		t.Fatalf("right table = %#v, want lineitem as l2", membership.RightTable)
	}
	if membership.RightQualifier != "l2" || membership.RightField != "l_orderkey" {
		t.Fatalf("right membership = %#v, want l2.l_orderkey", membership)
	}
	if len(membership.Predicates) != 1 {
		t.Fatalf("membership predicates = %d, want 1", len(membership.Predicates))
	}
	binary, ok := membership.Predicates[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("membership predicate = %T, want binary", membership.Predicates[0].Expr)
	}
	if binary.Op != BinaryOpNotEqual {
		t.Fatalf("membership predicate op = %q, want <>", binary.Op)
	}
}

func TestSimpleParserBridgeParsesCorrelatedNotExistsAsAntiMembership(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select count(*)
		from customers_qa as c
		where not exists (
			select o.cust_id
			from orders_qa as o
			where o.cust_id = c.cust_id
		)
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Memberships) != 1 {
		t.Fatalf("memberships = %d, want 1", len(statement.Select.Memberships))
	}
	if got := statement.Select.Memberships[0].Kind; got != MembershipAnti {
		t.Fatalf("membership kind = %q, want anti", got)
	}
}

func TestSimpleParserBridgeParsesNonCorrelatedExistsAsGatePredicate(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select count(*)
		from customers_qa
		where exists (
			select cust_id
			from orders_qa
			where cust_id = '10'
		)
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Memberships) != 0 {
		t.Fatalf("memberships = %d, want 0 for non-correlated EXISTS", len(statement.Select.Memberships))
	}
	if len(statement.Select.Predicates) != 1 {
		t.Fatalf("predicates = %d, want 1", len(statement.Select.Predicates))
	}
	exists, ok := statement.Select.Predicates[0].Expr.(UnboundExistsSubqueryExpr)
	if !ok {
		t.Fatalf("predicate expr = %T, want UnboundExistsSubqueryExpr", statement.Select.Predicates[0].Expr)
	}
	if exists.Negated {
		t.Fatalf("exists negated = true, want false")
	}
	if !strings.HasPrefix(exists.SQL, "select cust_id") ||
		!strings.Contains(exists.SQL, "from orders_qa") ||
		!strings.Contains(exists.SQL, "where cust_id = '10'") {
		t.Fatalf("exists SQL = %q, want child select text", exists.SQL)
	}
}

func TestSimpleParserBridgeParsesScalarInPredicateAsResidual(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select substr(c_phone, 1, 2) as cntrycode, count(*) as numcust
		from customer_tpch_qa
		where substr(c_phone, 1, 2) in ('13', '31')
		group by substr(c_phone, 1, 2)
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Predicates) != 1 {
		t.Fatalf("predicates = %d, want 1", len(statement.Select.Predicates))
	}
	if statement.Select.Predicates[0].Placement != PredicateResidualScan {
		t.Fatalf("placement = %q, want residual", statement.Select.Predicates[0].Placement)
	}
	binary, ok := statement.Select.Predicates[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("predicate expression = %T, want UnboundBinaryExpr", statement.Select.Predicates[0].Expr)
	}
	if binary.Op != BinaryOpIn {
		t.Fatalf("predicate op = %q, want %q", binary.Op, BinaryOpIn)
	}
	if _, ok := binary.Left.(UnboundCallExpr); !ok {
		t.Fatalf("left expression = %T, want UnboundCallExpr", binary.Left)
	}
}

func TestSimpleParserBridgeNormalizesOuterJoinPreservedSide(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select c.first_name, o.order_id from customers_qa as c outer join orders_qa as o on o.cust_id == c.cust_id")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Joins) != 1 {
		t.Fatalf("joins = %d, want 1", len(statement.Select.Joins))
	}
	join := statement.Select.Joins[0]
	if join.Kind != JoinKindLeftOuter {
		t.Fatalf("join kind = %q, want left outer", join.Kind)
	}
	if join.LeftQualifier != "c" || join.LeftField != "cust_id" || join.RightQualifier != "o" || join.RightField != "cust_id" {
		t.Fatalf("join = %#v, want preserved SQL-left side c.cust_id", join)
	}
}

func TestSimpleParserBridgeParsesInsertValues(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("insert into customers_qa (cust_id, first_name, age) values ('9001', 'Ada', 42)")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindInsert {
		t.Fatalf("kind = %q, want insert", statement.Kind)
	}
	if statement.Insert.Table.Name != "customers_qa" {
		t.Fatalf("insert table = %#v, want customers_qa", statement.Insert.Table)
	}
	if len(statement.Insert.Columns) != 3 || statement.Insert.Columns[0] != "cust_id" || statement.Insert.Columns[2] != "age" {
		t.Fatalf("insert columns = %#v, want cust_id, first_name, age", statement.Insert.Columns)
	}
	if len(statement.Insert.Rows) != 1 || len(statement.Insert.Rows[0]) != 3 {
		t.Fatalf("insert rows = %#v, want one three-value row", statement.Insert.Rows)
	}
	age, ok := statement.Insert.Rows[0][2].(UnboundLiteralExpr)
	if !ok || age.Kind != ValueInt || age.Value != int64(42) {
		t.Fatalf("age literal = %#v, want int 42", statement.Insert.Rows[0][2])
	}
}

func TestSimpleParserBridgeParsesInsertValuesWithoutWhitespace(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("insert into customers_qa (cust_id, first_name, age) values('9001', 'Ada', 42)")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindInsert {
		t.Fatalf("kind = %q, want insert", statement.Kind)
	}
	if len(statement.Insert.Rows) != 1 || len(statement.Insert.Rows[0]) != 3 {
		t.Fatalf("insert rows = %#v, want one three-value row", statement.Insert.Rows)
	}
}

func TestSimpleParserBridgeParsesInsertMultipleRowsAndBooleanNull(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("insert into customers_qa (cust_id, isActive, city) values ('9001', true, null), ('9002', false, 'Seattle')")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Insert.Rows) != 2 {
		t.Fatalf("insert rows = %d, want 2", len(statement.Insert.Rows))
	}
	active, ok := statement.Insert.Rows[0][1].(UnboundLiteralExpr)
	if !ok || active.Kind != ValueBool || active.Value != true {
		t.Fatalf("active literal = %#v, want true", statement.Insert.Rows[0][1])
	}
	city, ok := statement.Insert.Rows[0][2].(UnboundLiteralExpr)
	if !ok || city.Kind != ValueNull || city.Value != nil {
		t.Fatalf("city literal = %#v, want null", statement.Insert.Rows[0][2])
	}
}

func TestSimpleParserBridgeParsesEmptyInsertValueAsNull(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("insert into customers_qa (cust_id, createdAtTimestamp, first_name) values ('200',, 'Bob')")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Insert.Rows) != 1 || len(statement.Insert.Rows[0]) != 3 {
		t.Fatalf("insert rows = %#v, want one three-value row", statement.Insert.Rows)
	}
	value, ok := statement.Insert.Rows[0][1].(UnboundLiteralExpr)
	if !ok || value.Kind != ValueNull || value.Value != nil {
		t.Fatalf("empty value = %#v, want NULL literal", statement.Insert.Rows[0][1])
	}
}

func TestSimpleParserBridgeParsesCommit(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("commit")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindSession {
		t.Fatalf("kind = %q, want session", statement.Kind)
	}
	if statement.Session.Result.Kind != ResultStatement {
		t.Fatalf("result kind = %q, want statement", statement.Session.Result.Kind)
	}
	if len(statement.Session.Actions) != 1 || statement.Session.Actions[0].Kind != SessionActionCommitTransaction {
		t.Fatalf("session actions = %#v, want commit", statement.Session.Actions)
	}
}

func TestSimpleParserBridgeParsesCommitWork(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("commit work;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Session.Actions) != 1 || statement.Session.Actions[0].Kind != SessionActionCommitTransaction {
		t.Fatalf("session actions = %#v, want commit", statement.Session.Actions)
	}
}

func TestSimpleParserBridgeParsesCountStar(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select count(*) as order_count from orders as o where o.o_totalprice >= 101")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Aggregates) != 1 {
		t.Fatalf("aggregates = %d, want 1", len(statement.Select.Aggregates))
	}
	aggregate := statement.Select.Aggregates[0]
	if aggregate.Function != "count" || !aggregate.CountAll || aggregate.Alias != "order_count" {
		t.Fatalf("aggregate = %#v, want count(*) as order_count", aggregate)
	}
	if len(statement.Select.Projection) != 1 {
		t.Fatalf("projections = %d, want 1", len(statement.Select.Projection))
	}
	ref, ok := statement.Select.Projection[0].Expr.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("projection expression = %T, want UnboundAggregateRefExpr", statement.Select.Projection[0].Expr)
	}
	if ref.Alias != "order_count" || ref.Index != 0 {
		t.Fatalf("aggregate ref = %#v, want order_count[0]", ref)
	}
}

func TestSimpleParserBridgeParsesSumAggregate(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select sum(o.o_totalprice) as revenue from orders as o where o.o_totalprice >= 101")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Aggregates) != 1 {
		t.Fatalf("aggregates = %d, want 1", len(statement.Select.Aggregates))
	}
	aggregate := statement.Select.Aggregates[0]
	if aggregate.Function != "sum" || aggregate.Alias != "revenue" {
		t.Fatalf("aggregate = %#v, want sum revenue", aggregate)
	}
	input, ok := aggregate.Input.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("aggregate input = %T, want UnboundFieldExpr", aggregate.Input)
	}
	if input.Qualifier != "o" || input.Name != "o_totalprice" {
		t.Fatalf("aggregate input = %#v, want o.o_totalprice", input)
	}
	ref, ok := statement.Select.Projection[0].Expr.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("projection expression = %T, want UnboundAggregateRefExpr", statement.Select.Projection[0].Expr)
	}
	if ref.Alias != "revenue" || ref.Index != 0 {
		t.Fatalf("aggregate ref = %#v, want revenue[0]", ref)
	}
}

func TestSimpleParserBridgeParsesCountDistinctAggregate(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select count(distinct o.o_custkey) as distinct_customers from orders as o")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Aggregates) != 1 {
		t.Fatalf("aggregates = %d, want 1", len(statement.Select.Aggregates))
	}
	aggregate := statement.Select.Aggregates[0]
	if aggregate.Function != "count" || aggregate.Mode != AggregateDistinct || aggregate.Alias != "distinct_customers" {
		t.Fatalf("aggregate = %#v, want count distinct distinct_customers", aggregate)
	}
	if _, ok := aggregate.Input.(UnboundFieldExpr); !ok {
		t.Fatalf("aggregate input = %T, want UnboundFieldExpr", aggregate.Input)
	}
	ref, ok := statement.Select.Projection[0].Expr.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("projection expression = %T, want UnboundAggregateRefExpr", statement.Select.Projection[0].Expr)
	}
	if ref.Alias != "distinct_customers" || ref.Index != 0 {
		t.Fatalf("aggregate ref = %#v, want distinct_customers[0]", ref)
	}
}

func TestSimpleParserBridgeParsesTopNAggregate(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select topn(l.l_shipmode) as shipmode_topn from lineitem as l")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Aggregates) != 1 {
		t.Fatalf("aggregates = %d, want 1", len(statement.Select.Aggregates))
	}
	aggregate := statement.Select.Aggregates[0]
	if aggregate.Function != "topn" || aggregate.Alias != "shipmode_topn" {
		t.Fatalf("aggregate = %#v, want topn shipmode_topn", aggregate)
	}
	input, ok := aggregate.Input.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("aggregate input = %T, want UnboundFieldExpr", aggregate.Input)
	}
	if input.Qualifier != "l" || input.Name != "l_shipmode" {
		t.Fatalf("aggregate input = %#v, want l.l_shipmode", input)
	}
	ref, ok := statement.Select.Projection[0].Expr.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("projection expression = %T, want UnboundAggregateRefExpr", statement.Select.Projection[0].Expr)
	}
	if ref.Alias != "shipmode_topn" || ref.Index != 0 {
		t.Fatalf("aggregate ref = %#v, want shipmode_topn[0]", ref)
	}
}

func TestSimpleParserBridgeParsesAggregateAliasOrderBy(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o.o_custkey as customer_id, count(*) as order_count from orders as o group by o.o_custkey order by order_count desc")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.OrderBy) != 1 {
		t.Fatalf("order by = %d, want 1", len(statement.Select.OrderBy))
	}
	ref, ok := statement.Select.OrderBy[0].Expr.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("order by expression = %T, want UnboundAggregateRefExpr", statement.Select.OrderBy[0].Expr)
	}
	if ref.Alias != "order_count" || ref.Index != 0 {
		t.Fatalf("order by ref = %#v, want order_count[0]", ref)
	}
	if statement.Select.OrderBy[0].Direction != SortDescending {
		t.Fatalf("order by direction = %q, want desc", statement.Select.OrderBy[0].Direction)
	}
}

func TestSimpleParserBridgeParsesOrderByOrdinal(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select n_name as nation_name, n_nationkey as nation_key from nation order by 2 desc")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.OrderBy) != 1 {
		t.Fatalf("order by = %d, want 1", len(statement.Select.OrderBy))
	}
	field, ok := statement.Select.OrderBy[0].Expr.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("order by expression = %T, want UnboundFieldExpr", statement.Select.OrderBy[0].Expr)
	}
	if field.Name != "n_nationkey" {
		t.Fatalf("order by field = %#v, want n_nationkey", field)
	}
	if statement.Select.OrderBy[0].Direction != SortDescending {
		t.Fatalf("order by direction = %q, want desc", statement.Select.OrderBy[0].Direction)
	}
}

func TestSimpleParserBridgeParsesComputedProjectionAliasOrderBy(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select year(l_shipdate) as l_year, count(*) as line_count from lineitem group by year(l_shipdate) order by l_year")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.OrderBy) != 1 {
		t.Fatalf("order by = %d, want 1", len(statement.Select.OrderBy))
	}
	call, ok := statement.Select.OrderBy[0].Expr.(UnboundCallExpr)
	if !ok {
		t.Fatalf("order by expression = %T, want UnboundCallExpr", statement.Select.OrderBy[0].Expr)
	}
	if call.Name != "year" || len(call.Args) != 1 {
		t.Fatalf("order by call = %#v, want year(field)", call)
	}
	if field, ok := call.Args[0].(UnboundFieldExpr); !ok || field.Name != "l_shipdate" {
		t.Fatalf("order by call arg = %#v, want l_shipdate field", call.Args[0])
	}
}

func TestSimpleParserBridgeParsesMultiKeyOrderBy(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o.o_custkey as customer_id, count(*) as order_count from orders as o group by o.o_custkey order by order_count desc, o.o_custkey asc")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.OrderBy) != 2 {
		t.Fatalf("order by = %d, want 2", len(statement.Select.OrderBy))
	}
	ref, ok := statement.Select.OrderBy[0].Expr.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("first order by expression = %T, want UnboundAggregateRefExpr", statement.Select.OrderBy[0].Expr)
	}
	if ref.Alias != "order_count" || ref.Index != 0 {
		t.Fatalf("first order by ref = %#v, want order_count[0]", ref)
	}
	if statement.Select.OrderBy[0].Direction != SortDescending {
		t.Fatalf("first order by direction = %q, want desc", statement.Select.OrderBy[0].Direction)
	}
	field, ok := statement.Select.OrderBy[1].Expr.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("second order by expression = %T, want UnboundFieldExpr", statement.Select.OrderBy[1].Expr)
	}
	if field.Qualifier != "o" || field.Name != "o_custkey" {
		t.Fatalf("second order by field = %#v, want o.o_custkey", field)
	}
	if statement.Select.OrderBy[1].Direction != SortAscending {
		t.Fatalf("second order by direction = %q, want asc", statement.Select.OrderBy[1].Direction)
	}
}

func TestSimpleParserBridgeParsesAggregateCallOrderBy(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o.o_custkey as customer_id, count(*) as order_count from orders as o group by o.o_custkey order by count(*) desc")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.OrderBy) != 1 {
		t.Fatalf("order by = %d, want 1", len(statement.Select.OrderBy))
	}
	ref, ok := statement.Select.OrderBy[0].Expr.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("order by expression = %T, want UnboundAggregateRefExpr", statement.Select.OrderBy[0].Expr)
	}
	if ref.Alias != "order_count" || ref.Index != 0 {
		t.Fatalf("order by ref = %#v, want order_count[0]", ref)
	}
	if statement.Select.OrderBy[0].Direction != SortDescending {
		t.Fatalf("order by direction = %q, want desc", statement.Select.OrderBy[0].Direction)
	}
}

func TestSimpleParserBridgeParsesAggregateInputCallOrderBy(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o.o_custkey as customer_id, sum(o.o_totalprice) as total_revenue from orders as o group by o.o_custkey order by sum(o.o_totalprice) desc")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	ref, ok := statement.Select.OrderBy[0].Expr.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("order by expression = %T, want UnboundAggregateRefExpr", statement.Select.OrderBy[0].Expr)
	}
	if ref.Alias != "total_revenue" || ref.Index != 0 {
		t.Fatalf("order by ref = %#v, want total_revenue[0]", ref)
	}
}

func TestSimpleParserBridgeParsesAggregateSearchedCaseWithInPredicate(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select l.l_shipmode, sum(case when o.o_orderpriority in ('1-URGENT', '2-HIGH') then 1 else 0 end) as high_line_count from orders_tpch_qa as o inner join lineitem_tpch_qa as l on l.l_orderkey = o.o_orderkey group by l.l_shipmode")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Aggregates) != 1 {
		t.Fatalf("aggregates = %d, want 1", len(statement.Select.Aggregates))
	}
	searchedCase, ok := statement.Select.Aggregates[0].Input.(UnboundSearchedCaseExpr)
	if !ok {
		t.Fatalf("aggregate input = %T, want UnboundSearchedCaseExpr", statement.Select.Aggregates[0].Input)
	}
	if len(searchedCase.Whens) != 1 {
		t.Fatalf("case whens = %d, want 1", len(searchedCase.Whens))
	}
	condition, ok := searchedCase.Whens[0].Condition.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("case condition = %T, want UnboundBinaryExpr", searchedCase.Whens[0].Condition)
	}
	if condition.Op != BinaryOpIn {
		t.Fatalf("case condition op = %q, want %q", condition.Op, BinaryOpIn)
	}
	list, ok := condition.Right.(UnboundListExpr)
	if !ok {
		t.Fatalf("case condition right = %T, want UnboundListExpr", condition.Right)
	}
	if len(list.Items) != 2 {
		t.Fatalf("case IN list = %d, want 2", len(list.Items))
	}
}

func TestSimpleParserBridgeParsesAggregateAliasHaving(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o.o_custkey as customer_id, sum(o.o_totalprice) as total_revenue from orders as o group by o.o_custkey having total_revenue > 150 order by total_revenue desc")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Having) != 1 {
		t.Fatalf("having = %d, want 1", len(statement.Select.Having))
	}
	predicate, ok := statement.Select.Having[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("having expression = %T, want UnboundBinaryExpr", statement.Select.Having[0].Expr)
	}
	if predicate.Op != BinaryOpGreater {
		t.Fatalf("having op = %q, want %q", predicate.Op, BinaryOpGreater)
	}
	ref, ok := predicate.Left.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("having left = %T, want UnboundAggregateRefExpr", predicate.Left)
	}
	if ref.Alias != "total_revenue" || ref.Index != 0 {
		t.Fatalf("having ref = %#v, want total_revenue[0]", ref)
	}
	literal, ok := predicate.Right.(UnboundLiteralExpr)
	if !ok {
		t.Fatalf("having right = %T, want UnboundLiteralExpr", predicate.Right)
	}
	if literal.Kind != ValueInt || literal.Value != int64(150) {
		t.Fatalf("having literal = %#v, want int 150", literal)
	}
}

func TestSimpleParserBridgeParsesAggregateCallHaving(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o.o_custkey as customer_id, sum(o.o_totalprice) as total_revenue from orders as o group by o.o_custkey having sum(o.o_totalprice) > 150 order by sum(o.o_totalprice) desc")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Having) != 1 {
		t.Fatalf("having = %d, want 1", len(statement.Select.Having))
	}
	predicate, ok := statement.Select.Having[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("having expression = %T, want UnboundBinaryExpr", statement.Select.Having[0].Expr)
	}
	ref, ok := predicate.Left.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("having left = %T, want UnboundAggregateRefExpr", predicate.Left)
	}
	if ref.Alias != "total_revenue" || ref.Index != 0 {
		t.Fatalf("having ref = %#v, want total_revenue[0]", ref)
	}
}

func TestSimpleParserBridgeParsesHiddenAggregateCallHaving(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select c_mktsegment as market_segment from customer group by c_mktsegment having count(*) > 300 order by market_segment")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Aggregates) != 1 {
		t.Fatalf("aggregates = %d, want 1", len(statement.Select.Aggregates))
	}
	aggregate := statement.Select.Aggregates[0]
	if aggregate.Function != "count" || !aggregate.CountAll || aggregate.Alias != "__having_agg_0" {
		t.Fatalf("aggregate = %#v, want hidden count(*)", aggregate)
	}
	predicate, ok := statement.Select.Having[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("having expression = %T, want UnboundBinaryExpr", statement.Select.Having[0].Expr)
	}
	ref, ok := predicate.Left.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("having left = %T, want UnboundAggregateRefExpr", predicate.Left)
	}
	if ref.Alias != "__having_agg_0" || ref.Index != 0 {
		t.Fatalf("having ref = %#v, want hidden count ref", ref)
	}
}

func TestSimpleParserBridgeParsesAvgAggregate(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select avg(o.o_totalprice) as avg_total from orders as o where o.o_totalprice >= 101")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Aggregates) != 1 {
		t.Fatalf("aggregates = %d, want 1", len(statement.Select.Aggregates))
	}
	aggregate := statement.Select.Aggregates[0]
	if aggregate.Function != "avg" || aggregate.Alias != "avg_total" {
		t.Fatalf("aggregate = %#v, want avg avg_total", aggregate)
	}
	input, ok := aggregate.Input.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("aggregate input = %T, want UnboundFieldExpr", aggregate.Input)
	}
	if input.Qualifier != "o" || input.Name != "o_totalprice" {
		t.Fatalf("aggregate input = %#v, want o.o_totalprice", input)
	}
	ref, ok := statement.Select.Projection[0].Expr.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("projection expression = %T, want UnboundAggregateRefExpr", statement.Select.Projection[0].Expr)
	}
	if ref.Alias != "avg_total" || ref.Index != 0 {
		t.Fatalf("aggregate ref = %#v, want avg_total[0]", ref)
	}
}

func TestSimpleParserBridgeParsesMinMaxAggregates(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select min(o.o_totalprice) as min_total, max(o.o_totalprice) as max_total from orders as o where o.o_totalprice >= 101")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Aggregates) != 2 {
		t.Fatalf("aggregates = %d, want 2", len(statement.Select.Aggregates))
	}
	if statement.Select.Aggregates[0].Function != "min" || statement.Select.Aggregates[0].Alias != "min_total" {
		t.Fatalf("first aggregate = %#v, want min_total", statement.Select.Aggregates[0])
	}
	if statement.Select.Aggregates[1].Function != "max" || statement.Select.Aggregates[1].Alias != "max_total" {
		t.Fatalf("second aggregate = %#v, want max_total", statement.Select.Aggregates[1])
	}
	firstRef, ok := statement.Select.Projection[0].Expr.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("first projection expression = %T, want UnboundAggregateRefExpr", statement.Select.Projection[0].Expr)
	}
	secondRef, ok := statement.Select.Projection[1].Expr.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("second projection expression = %T, want UnboundAggregateRefExpr", statement.Select.Projection[1].Expr)
	}
	if firstRef.Index != 0 || secondRef.Index != 1 {
		t.Fatalf("aggregate refs = %#v/%#v, want indexes 0/1", firstRef, secondRef)
	}
}

func TestSimpleParserBridgeParsesArithmeticProjection(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o.o_totalprice * 2 as doubled_total from orders as o where o.o_orderkey = 8")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Projection) != 1 {
		t.Fatalf("projections = %d, want 1", len(statement.Select.Projection))
	}
	expr, ok := statement.Select.Projection[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("projection expression = %T, want UnboundBinaryExpr", statement.Select.Projection[0].Expr)
	}
	if expr.Op != BinaryOpMultiply {
		t.Fatalf("projection op = %q, want %q", expr.Op, BinaryOpMultiply)
	}
	left, ok := expr.Left.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("left expression = %T, want UnboundFieldExpr", expr.Left)
	}
	if left.Qualifier != "o" || left.Name != "o_totalprice" {
		t.Fatalf("left expression = %#v, want o.o_totalprice", left)
	}
	right, ok := expr.Right.(UnboundLiteralExpr)
	if !ok {
		t.Fatalf("right expression = %T, want UnboundLiteralExpr", expr.Right)
	}
	if right.Kind != ValueInt || right.Value != int64(2) {
		t.Fatalf("right expression = %#v, want int literal 2", right)
	}
}

func TestSimpleParserBridgeParsesAggregateExpressionInput(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select sum(o.o_totalprice * 2) as doubled_revenue from orders as o where o.o_totalprice >= 101")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Aggregates) != 1 {
		t.Fatalf("aggregates = %d, want 1", len(statement.Select.Aggregates))
	}
	aggregate := statement.Select.Aggregates[0]
	if aggregate.Function != "sum" || aggregate.Alias != "doubled_revenue" {
		t.Fatalf("aggregate = %#v, want sum doubled_revenue", aggregate)
	}
	input, ok := aggregate.Input.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("aggregate input = %T, want UnboundBinaryExpr", aggregate.Input)
	}
	if input.Op != BinaryOpMultiply {
		t.Fatalf("aggregate input op = %q, want %q", input.Op, BinaryOpMultiply)
	}
}

func TestSimpleParserBridgeParsesNestedAggregateExpressionInput(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select sum(o.o_totalprice * (1 - o.o_discount)) as discounted_revenue from orders as o")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Aggregates) != 1 {
		t.Fatalf("aggregates = %d, want 1", len(statement.Select.Aggregates))
	}
	input, ok := statement.Select.Aggregates[0].Input.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("aggregate input = %T, want UnboundBinaryExpr", statement.Select.Aggregates[0].Input)
	}
	if input.Op != BinaryOpMultiply {
		t.Fatalf("aggregate input op = %q, want %q", input.Op, BinaryOpMultiply)
	}
	right, ok := input.Right.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("right expression = %T, want UnboundBinaryExpr", input.Right)
	}
	if right.Op != BinaryOpSubtract {
		t.Fatalf("right expression op = %q, want %q", right.Op, BinaryOpSubtract)
	}
	leftLiteral, ok := right.Left.(UnboundLiteralExpr)
	if !ok {
		t.Fatalf("right left expression = %T, want UnboundLiteralExpr", right.Left)
	}
	if leftLiteral.Kind != ValueInt || leftLiteral.Value != int64(1) {
		t.Fatalf("right left literal = %#v, want int 1", leftLiteral)
	}
	discount, ok := right.Right.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("right right expression = %T, want UnboundFieldExpr", right.Right)
	}
	if discount.Qualifier != "o" || discount.Name != "o_discount" {
		t.Fatalf("discount expression = %#v, want o.o_discount", discount)
	}
}

func TestSimpleParserBridgeParsesAggregateArithmeticProjection(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select year(o.o_orderdate) as o_year, sum(o.o_totalprice) / sum(o.o_discount) as ratio from orders as o group by year(o.o_orderdate)")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Aggregates) != 2 {
		t.Fatalf("aggregates = %d, want 2", len(statement.Select.Aggregates))
	}
	if statement.Select.Aggregates[0].Alias != "__agg_0" || statement.Select.Aggregates[1].Alias != "__agg_1" {
		t.Fatalf("aggregate aliases = %q/%q, want hidden aliases", statement.Select.Aggregates[0].Alias, statement.Select.Aggregates[1].Alias)
	}
	if len(statement.Select.Projection) != 2 {
		t.Fatalf("projections = %d, want 2", len(statement.Select.Projection))
	}
	ratio := statement.Select.Projection[1]
	if ratio.Alias != "ratio" {
		t.Fatalf("ratio alias = %q, want ratio", ratio.Alias)
	}
	binary, ok := ratio.Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("ratio expression = %T, want UnboundBinaryExpr", ratio.Expr)
	}
	if binary.Op != BinaryOpDivide {
		t.Fatalf("ratio op = %q, want %q", binary.Op, BinaryOpDivide)
	}
	left, ok := binary.Left.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("ratio left = %T, want UnboundAggregateRefExpr", binary.Left)
	}
	right, ok := binary.Right.(UnboundAggregateRefExpr)
	if !ok {
		t.Fatalf("ratio right = %T, want UnboundAggregateRefExpr", binary.Right)
	}
	if left.Alias != "__agg_0" || left.Index != 0 || right.Alias != "__agg_1" || right.Index != 1 {
		t.Fatalf("aggregate refs = %#v/%#v, want hidden refs 0/1", left, right)
	}
}

func TestSimpleParserBridgeParsesGroupedCount(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o.o_custkey as customer_id, count(*) as order_count from orders as o group by o.o_custkey")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.GroupBy) != 1 {
		t.Fatalf("group by = %d, want 1", len(statement.Select.GroupBy))
	}
	group, ok := statement.Select.GroupBy[0].(UnboundFieldExpr)
	if !ok {
		t.Fatalf("group expression = %T, want UnboundFieldExpr", statement.Select.GroupBy[0])
	}
	if group.Qualifier != "o" || group.Name != "o_custkey" {
		t.Fatalf("group expression = %#v, want o.o_custkey", group)
	}
	if len(statement.Select.Projection) != 2 || len(statement.Select.Aggregates) != 1 {
		t.Fatalf("projection/aggregates = %d/%d, want 2/1", len(statement.Select.Projection), len(statement.Select.Aggregates))
	}
	if statement.Select.Aggregates[0].Function != "count" || !statement.Select.Aggregates[0].CountAll {
		t.Fatalf("aggregate = %#v, want count(*)", statement.Select.Aggregates[0])
	}
}

func TestSimpleParserBridgeParsesGroupByProjectionAlias(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select c_mktsegment as market_segment, count(*) as customer_count from customer group by market_segment order by market_segment")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.GroupBy) != 1 {
		t.Fatalf("group by = %d, want 1", len(statement.Select.GroupBy))
	}
	group, ok := statement.Select.GroupBy[0].(UnboundFieldExpr)
	if !ok {
		t.Fatalf("group expression = %T, want UnboundFieldExpr", statement.Select.GroupBy[0])
	}
	if group.Name != "c_mktsegment" {
		t.Fatalf("group expression = %#v, want c_mktsegment", group)
	}
}

func TestSimpleParserBridgeParsesSeedAndGroupedResidualOrPredicate(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select count(*) as phone_prefix_count
		from customer
		where c_custkey >= 1
		  and (
		    substr(c_phone, 1, 2) = '13'
		    or substr(c_phone, 1, 2) = '31'
		  )
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Blockers) != 0 {
		t.Fatalf("blockers = %#v, want none", statement.Select.Blockers)
	}
	if len(statement.Select.Predicates) != 2 {
		t.Fatalf("predicates = %d, want 2", len(statement.Select.Predicates))
	}
	if statement.Select.Predicates[0].Placement != PredicatePushdown {
		t.Fatalf("seed placement = %s, want %s", statement.Select.Predicates[0].Placement, PredicatePushdown)
	}
	if statement.Select.Predicates[1].Placement != PredicateResidualScan {
		t.Fatalf("residual placement = %s, want %s", statement.Select.Predicates[1].Placement, PredicateResidualScan)
	}
	residual, ok := statement.Select.Predicates[1].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("residual expr = %T, want UnboundBinaryExpr", statement.Select.Predicates[1].Expr)
	}
	if residual.Op != BinaryOpOr {
		t.Fatalf("residual op = %s, want %s", residual.Op, BinaryOpOr)
	}
}

func TestSimpleParserBridgeRejectsUnsupportedSQL(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o_orderkey from orders where o_orderkey = 7 or o_orderkey = 8 and o_custkey = 501")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Blockers) != 1 {
		t.Fatalf("blockers = %d, want 1", len(statement.Select.Blockers))
	}
	if got := statement.Select.Blockers[0].Code; got != DiagnosticMixedBooleanPredicate {
		t.Fatalf("blocker code = %q, want %q", got, DiagnosticMixedBooleanPredicate)
	}
	where, ok := statement.Select.WhereExpr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("where expression = %T, want UnboundBinaryExpr", statement.Select.WhereExpr)
	}
	if where.Op != BinaryOpOr {
		t.Fatalf("where op = %q, want %q", where.Op, BinaryOpOr)
	}
	right, ok := where.Right.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("where right = %T, want UnboundBinaryExpr", where.Right)
	}
	if right.Op != BinaryOpAnd {
		t.Fatalf("where right op = %q, want %q", right.Op, BinaryOpAnd)
	}
}

func TestSimpleParserBridgeBoundMixedBooleanPredicateReportsBlocker(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select o_orderkey from orders where o_orderkey = 7 or o_orderkey = 8 and o_custkey = 501")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	query, bindDiagnostics := statement.Bind(NewBindContext(testBindCatalog(), "quanta"))
	if bindDiagnostics.BlocksNative() {
		t.Fatalf("bind diagnostics: %#v", bindDiagnostics)
	}
	if query.Supported() {
		t.Fatalf("mixed boolean query should remain unsupported until grouped predicate lowering exists")
	}
	queryDiagnostics := query.Diagnostics()
	if !queryDiagnostics.BlocksNative() {
		t.Fatalf("expected query diagnostics to block native execution")
	}
	if got := queryDiagnostics.Codes()[0]; got != DiagnosticMixedBooleanPredicate {
		t.Fatalf("diagnostic code = %q, want %q", got, DiagnosticMixedBooleanPredicate)
	}
}

func TestSimpleParserBridgeParsesGroupedMixedBooleanPredicateWithBlocker(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select sum(l.l_extendedprice * (1 - l.l_discount)) as revenue
		from part_qa as p
		inner join lineitem_tpch_qa as l on l.l_partkey = p.p_partkey
		where (p.p_brand = 'Brand#12'
		    and p.p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		    and p.p_size <= 5
		    and l.l_quantity >= 1
		    and l.l_quantity <= 11
		    and l.l_shipmode in ('AIR', 'AIR REG')
		    and l.l_shipinstruct = 'DELIVER IN PERSON')
		   or (p.p_brand = 'Brand#23'
		    and p.p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		    and p.p_size <= 10
		    and l.l_quantity >= 10
		    and l.l_quantity <= 20
		    and l.l_shipmode in ('AIR', 'AIR REG')
		    and l.l_shipinstruct = 'DELIVER IN PERSON')
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Blockers) != 1 {
		t.Fatalf("blockers = %d, want 1", len(statement.Select.Blockers))
	}
	if got := statement.Select.Blockers[0].Code; got != DiagnosticMixedBooleanPredicate {
		t.Fatalf("blocker code = %q, want %q", got, DiagnosticMixedBooleanPredicate)
	}
	root, ok := statement.Select.WhereExpr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("where expression = %T, want UnboundBinaryExpr", statement.Select.WhereExpr)
	}
	if root.Op != BinaryOpOr {
		t.Fatalf("where op = %q, want %q", root.Op, BinaryOpOr)
	}
	left, ok := root.Left.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("where left = %T, want UnboundBinaryExpr", root.Left)
	}
	if left.Op != BinaryOpAnd {
		t.Fatalf("where left op = %q, want %q", left.Op, BinaryOpAnd)
	}
	right, ok := root.Right.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("where right = %T, want UnboundBinaryExpr", root.Right)
	}
	if right.Op != BinaryOpAnd {
		t.Fatalf("where right op = %q, want %q", right.Op, BinaryOpAnd)
	}
}

func unboundPredicateReferencesField(predicate UnboundPredicate, qualifier string, name string) bool {
	return unboundExprReferencesField(predicate.Expr, qualifier, name)
}

func unboundExprReferencesField(expr UnboundExpr, qualifier string, name string) bool {
	switch typed := expr.(type) {
	case UnboundFieldExpr:
		return typed.Qualifier == qualifier && typed.Name == name
	case UnboundBinaryExpr:
		return unboundExprReferencesField(typed.Left, qualifier, name) || unboundExprReferencesField(typed.Right, qualifier, name)
	default:
		return false
	}
}

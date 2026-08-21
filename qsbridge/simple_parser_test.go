package qsbridge

import (
	"reflect"
	"strings"
	"testing"
)

func assertSimpleParserRejects(t *testing.T, sql string, message string) {
	t.Helper()
	_, diagnostics := SimpleParserBridge{}.Parse(sql)
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want parser blocker", diagnostics)
	}
	if len(diagnostics) != 1 || !strings.Contains(diagnostics[0].Error(), message) {
		t.Fatalf("diagnostics = %#v, want message containing %q", diagnostics, message)
	}
}

func assertSimpleParserOrderByField(t *testing.T, sorts []UnboundSort, fieldName string) {
	t.Helper()
	if len(sorts) != 1 {
		t.Fatalf("order by = %#v, want one sort", sorts)
	}
	field, ok := sorts[0].Expr.(UnboundFieldExpr)
	if !ok {
		t.Fatalf("order by expr = %#v, want field", sorts[0].Expr)
	}
	if field.Name != fieldName {
		t.Fatalf("order by field = %q, want %q", field.Name, fieldName)
	}
}

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

func TestSimpleParserBridgeParsesUpdateAssignmentExpressions(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantType string
	}{
		{
			name:     "arithmetic expression",
			sql:      "update customers_qa set age = age + 1 where state = 'ID'",
			wantType: "binary",
		},
		{
			name:     "scalar function",
			sql:      "update customers_qa set last_name = lower(last_name) where state = 'ID'",
			wantType: "call",
		},
		{
			name:     "field reference",
			sql:      "update customers_qa set last_name = first_name where state = 'ID'",
			wantType: "field",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, diagnostics := SimpleParserBridge{}.Parse(test.sql)
			if diagnostics.BlocksNative() {
				t.Fatalf("parse diagnostics: %#v", diagnostics)
			}
			if statement.Kind != QueryKindUpdate || len(statement.Update.Assignments) != 1 {
				t.Fatalf("statement = %#v, want one UPDATE assignment", statement)
			}
			value := statement.Update.Assignments[0].Value
			switch test.wantType {
			case "binary":
				if _, ok := value.(UnboundBinaryExpr); !ok {
					t.Fatalf("assignment value = %T, want UnboundBinaryExpr", value)
				}
			case "call":
				if _, ok := value.(UnboundCallExpr); !ok {
					t.Fatalf("assignment value = %T, want UnboundCallExpr", value)
				}
			case "field":
				if _, ok := value.(UnboundFieldExpr); !ok {
					t.Fatalf("assignment value = %T, want UnboundFieldExpr", value)
				}
			}
		})
	}
}

func TestSimpleParserBridgeParsesUpdateAssignmentScalarSubquery(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("update customers_qa set age = (select max(age) from customers_qa where score > 0) where state = 'ID'")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindUpdate {
		t.Fatalf("kind = %q, want update", statement.Kind)
	}
	if got, want := len(statement.Update.Assignments), 1; got != want {
		t.Fatalf("assignments = %d, want %d", got, want)
	}
	if _, ok := statement.Update.Assignments[0].Value.(UnboundScalarSubqueryExpr); !ok {
		t.Fatalf("assignment value = %#v, want scalar subquery", statement.Update.Assignments[0].Value)
	}
	if got, want := len(statement.Update.Predicates), 1; got != want {
		t.Fatalf("predicates = %d, want %d", got, want)
	}
}

func TestSimpleParserBridgeParsesMutationInListSubquery(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`update customers_qa
set state = 'WA'
where cust_id in (
  select cust_id
  from customers_qa
  where state = 'ID'
)`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindUpdate {
		t.Fatalf("kind = %q, want update", statement.Kind)
	}
	if got, want := len(statement.Update.Predicates), 1; got != want {
		t.Fatalf("predicates = %d, want %d", got, want)
	}
	binary, ok := statement.Update.Predicates[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("predicate expression = %T, want UnboundBinaryExpr", statement.Update.Predicates[0].Expr)
	}
	if binary.Op != BinaryOpIn {
		t.Fatalf("predicate op = %q, want %q", binary.Op, BinaryOpIn)
	}
	subquery, ok := binary.Right.(UnboundListSubqueryExpr)
	if !ok {
		t.Fatalf("predicate right = %T, want UnboundListSubqueryExpr", binary.Right)
	}
	if subquery.SQL != "select cust_id\n  from customers_qa\n  where state = 'ID'" {
		t.Fatalf("subquery SQL = %q", subquery.SQL)
	}
}

func TestSimpleParserBridgeParsesUpdateOrderLimitBoundary(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("update customers_qa set phoneType = 'cell' where state = 'ID' order by cust_id limit 1")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindUpdate {
		t.Fatalf("kind = %q, want update", statement.Kind)
	}
	assertSimpleParserOrderByField(t, statement.Update.OrderBy, "cust_id")
	if !statement.Update.Result.HasLimit || statement.Update.Result.Limit != 1 || statement.Update.Result.Offset != 0 {
		t.Fatalf("result = %#v, want LIMIT 1", statement.Update.Result)
	}
}

func TestSimpleParserBridgeParsesUpdateJoinBoundary(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`update qs_mysql_compat_mutations_ctas as t
inner join customer as c on t.id = c.c_custkey
set t.name = c.c_name
where t.id = 2`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindUpdate {
		t.Fatalf("kind = %q, want update", statement.Kind)
	}
	if statement.Update.Table.Name != "qs_mysql_compat_mutations_ctas" || statement.Update.Table.Alias != "t" {
		t.Fatalf("target table = %#v, want qs_mysql_compat_mutations_ctas as t", statement.Update.Table)
	}
	if got, want := len(statement.Update.Assignments), 1; got != want {
		t.Fatalf("assignments = %d, want %d", got, want)
	}
	if statement.Update.Assignments[0].Column != "name" {
		t.Fatalf("assignment column = %q, want name", statement.Update.Assignments[0].Column)
	}
	scalar, ok := statement.Update.Assignments[0].Value.(UnboundScalarSubqueryExpr)
	if !ok {
		t.Fatalf("assignment value = %T, want UnboundScalarSubqueryExpr", statement.Update.Assignments[0].Value)
	}
	if scalar.SQL != "select c.c_name from customer as c where c.c_custkey = 2" {
		t.Fatalf("assignment scalar SQL = %q", scalar.SQL)
	}
	if got, want := len(statement.Update.Predicates), 1; got != want {
		t.Fatalf("predicates = %d, want %d", got, want)
	}
	binary, ok := statement.Update.Predicates[0].Expr.(UnboundBinaryExpr)
	if !ok || binary.Op != BinaryOpIn {
		t.Fatalf("predicate = %#v, want IN binary", statement.Update.Predicates[0].Expr)
	}
	subquery, ok := binary.Right.(UnboundListSubqueryExpr)
	if !ok {
		t.Fatalf("predicate right = %T, want UnboundListSubqueryExpr", binary.Right)
	}
	if subquery.SQL != "select c.c_custkey from customer as c where c.c_custkey = 2" {
		t.Fatalf("predicate subquery SQL = %q", subquery.SQL)
	}
}

func TestSimpleParserBridgeRejectsDeleteJoinWithoutTargetAlias(t *testing.T) {
	assertSimpleParserRejects(t,
		"delete from customer c inner join orders o on c.c_custkey = o.o_custkey where o.o_orderkey = 1",
		"DELETE JOIN is not supported yet",
	)
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

func TestSimpleParserBridgeParsesDeleteTargetAliasJoin(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("delete c from customer c inner join orders o on c.c_custkey = o.o_custkey where o.o_orderkey = 1")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindDelete {
		t.Fatalf("kind = %q, want delete", statement.Kind)
	}
	if statement.Delete.Table.Name != "customer" || statement.Delete.Table.Alias != "c" {
		t.Fatalf("table = %#v, want customer c", statement.Delete.Table)
	}
	if got, want := len(statement.Delete.Predicates), 1; got != want {
		t.Fatalf("predicates = %d, want %d", got, want)
	}
	binary, ok := statement.Delete.Predicates[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("predicate = %T, want UnboundBinaryExpr", statement.Delete.Predicates[0].Expr)
	}
	if binary.Op != BinaryOpIn {
		t.Fatalf("predicate op = %q, want %q", binary.Op, BinaryOpIn)
	}
	subquery, ok := binary.Right.(UnboundListSubqueryExpr)
	if !ok {
		t.Fatalf("predicate right = %T, want UnboundListSubqueryExpr", binary.Right)
	}
	if want := "select o.o_custkey from orders as o where o.o_orderkey = 1"; subquery.SQL != want {
		t.Fatalf("subquery SQL = %q, want %q", subquery.SQL, want)
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

func TestSimpleParserBridgeParsesDeleteOrderLimitBoundary(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("delete from customers_qa where state = 'ID' order by cust_id limit 1")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindDelete {
		t.Fatalf("kind = %q, want delete", statement.Kind)
	}
	assertSimpleParserOrderByField(t, statement.Delete.OrderBy, "cust_id")
	if !statement.Delete.Result.HasLimit || statement.Delete.Result.Limit != 1 || statement.Delete.Result.Offset != 0 {
		t.Fatalf("result = %#v, want LIMIT 1", statement.Delete.Result)
	}
}

func TestSimpleParserBridgeRejectsAlterTableBoundary(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		message string
	}{
		{
			name:    "add column",
			sql:     "alter table customers_qa add column nickname varchar(40)",
			message: "ALTER TABLE only supports ADD PRIMARY KEY or ADD FOREIGN KEY",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertSimpleParserRejects(t, test.sql, test.message)
		})
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

func TestSimpleParserBridgeParsesCreateTemporaryTableStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`create temporary table if not exists qs_tmp_customer_keys (
		customer_key bigint not null,
		market_segment varchar(16),
		revenue decimal(12,2),
		created_at timestamp,
		primary key (customer_key)
	);`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindCreateTable {
		t.Fatalf("kind = %q, want create table", statement.Kind)
	}
	if !statement.Create.Temporary || !statement.Create.IfNotExists {
		t.Fatalf("create flags temporary/if_not_exists = %t/%t, want true/true", statement.Create.Temporary, statement.Create.IfNotExists)
	}
	if statement.Create.Table.Name != "qs_tmp_customer_keys" {
		t.Fatalf("table = %#v, want qs_tmp_customer_keys", statement.Create.Table)
	}
	fields := statement.Create.Columns
	if len(fields) != 4 {
		t.Fatalf("columns = %#v, want four columns", fields)
	}
	if fields[0].Name != "customer_key" || fields[0].Type != DataTypeInt || !fields[0].PrimaryKey || fields[0].Nullable {
		t.Fatalf("customer_key column = %#v, want non-null int primary key", fields[0])
	}
	if fields[1].Name != "market_segment" || fields[1].Type != DataTypeString || fields[1].Encoding.MaxLength != 16 {
		t.Fatalf("market_segment column = %#v, want varchar(16)", fields[1])
	}
	if fields[2].Name != "revenue" || fields[2].Type != DataTypeFloat || fields[2].Encoding.Scale != 2 {
		t.Fatalf("revenue column = %#v, want decimal scale 2", fields[2])
	}
	if fields[3].Name != "created_at" || fields[3].Type != DataTypeTime {
		t.Fatalf("created_at column = %#v, want timestamp/time", fields[3])
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

func TestSimpleParserBridgeParsesDropTemporaryTableIfExistsStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("drop temporary table if exists qs_tmp_customer_keys;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindDropTable {
		t.Fatalf("kind = %q, want drop table", statement.Kind)
	}
	if statement.Drop.Table.Name != "qs_tmp_customer_keys" {
		t.Fatalf("table = %#v, want qs_tmp_customer_keys", statement.Drop.Table)
	}
	if !statement.Drop.Temporary || !statement.Drop.IfExists {
		t.Fatalf("drop flags temporary/if_exists = %t/%t, want true/true", statement.Drop.Temporary, statement.Drop.IfExists)
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

func TestSimpleParserBridgeParsesAlterTableAddPrimaryKeyStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("alter table scratch_orders add primary key (order_key, customer_key);")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindAlterTable {
		t.Fatalf("kind = %q, want alter table", statement.Kind)
	}
	if statement.Alter.Table.Name != "scratch_orders" {
		t.Fatalf("table = %#v, want scratch_orders", statement.Alter.Table)
	}
	if got := statement.Alter.AddPrimaryKeyColumns; len(got) != 2 || got[0] != "order_key" || got[1] != "customer_key" {
		t.Fatalf("primary key columns = %#v, want order_key/customer_key", got)
	}
	if statement.Alter.Result.Kind != ResultStatement {
		t.Fatalf("result kind = %q, want statement", statement.Alter.Result.Kind)
	}
}

func TestSimpleParserBridgeParsesAlterTableAddForeignKeyStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("alter table orders add constraint fk_orders_customer foreign key (o_custkey) references customer(c_custkey);")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindAlterTable {
		t.Fatalf("kind = %q, want alter table", statement.Kind)
	}
	if statement.Alter.Table.Name != "orders" {
		t.Fatalf("table = %#v, want orders", statement.Alter.Table)
	}
	if statement.Alter.AddForeignKey == nil {
		t.Fatalf("AddForeignKey = nil, want parsed foreign key")
	}
	foreignKey := statement.Alter.AddForeignKey
	if foreignKey.Name != "fk_orders_customer" {
		t.Fatalf("foreign key name = %q, want fk_orders_customer", foreignKey.Name)
	}
	if len(foreignKey.Columns) != 1 || foreignKey.Columns[0] != "o_custkey" {
		t.Fatalf("foreign key columns = %#v, want o_custkey", foreignKey.Columns)
	}
	if foreignKey.ReferencedTable.Name != "customer" {
		t.Fatalf("referenced table = %#v, want customer", foreignKey.ReferencedTable)
	}
	if len(foreignKey.ReferencedColumns) != 1 || foreignKey.ReferencedColumns[0] != "c_custkey" {
		t.Fatalf("referenced columns = %#v, want c_custkey", foreignKey.ReferencedColumns)
	}
	if statement.Alter.Result.Kind != ResultStatement {
		t.Fatalf("result kind = %q, want statement", statement.Alter.Result.Kind)
	}
}

func TestSimpleParserBridgeRejectsAlterTableAddForeignKeyOptions(t *testing.T) {
	assertSimpleParserRejects(t,
		"alter table lineitem add constraint fk_lineitem_orders_reverse foreign key (l_orderkey) references orders(o_orderkey) with reverse artifact",
		"ALTER TABLE ADD FOREIGN KEY options are not supported yet",
	)
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
	if statement.ShowView.Result.Kind != ResultQuery || len(statement.ShowView.Result.Columns) != 4 {
		t.Fatalf("result = %#v, want four-column query result", statement.ShowView.Result)
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

func TestSimpleParserBridgeParsesShowFullTablesStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show full tables from quanta;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowTables {
		t.Fatalf("kind = %q, want show_tables", statement.Kind)
	}
	if !statement.ShowTables.Full {
		t.Fatalf("show full flag = false, want true")
	}
	if statement.ShowTables.Schema != "quanta" {
		t.Fatalf("schema = %q, want quanta", statement.ShowTables.Schema)
	}
	if got, want := len(statement.ShowTables.Result.Columns), 2; got != want {
		t.Fatalf("columns = %d, want %d", got, want)
	}
	if got, want := statement.ShowTables.Result.Columns[1].Name, "Table_type"; got != want {
		t.Fatalf("second result column = %q, want %q", got, want)
	}
}

func TestSimpleParserBridgeParsesShowFullTablesWhereTableTypeStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show full tables where Table_type = 'VIEW';")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowTables {
		t.Fatalf("kind = %q, want show_tables", statement.Kind)
	}
	if !statement.ShowTables.Full {
		t.Fatalf("show full flag = false, want true")
	}
	if got, want := statement.ShowTables.Pattern, "VIEW"; got != want {
		t.Fatalf("pattern = %q, want %q", got, want)
	}
	if got, want := len(statement.ShowTables.Result.Columns), 2; got != want {
		t.Fatalf("columns = %d, want %d", got, want)
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

func TestSimpleParserBridgeParsesShowOpenTablesStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show open tables from quanta like 'ord%';")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowOpenTables {
		t.Fatalf("kind = %q, want show_open_tables", statement.Kind)
	}
	if statement.ShowOpenTables.Schema != "quanta" || statement.ShowOpenTables.Pattern != "ord%" {
		t.Fatalf("show open tables = %#v, want schema quanta pattern ord%%", statement.ShowOpenTables)
	}
	if got, want := len(statement.ShowOpenTables.Result.Columns), 4; got != want {
		t.Fatalf("columns = %d, want %d", got, want)
	}
	if got, want := statement.ShowOpenTables.Result.Columns[0].Name, "Database"; got != want {
		t.Fatalf("first column = %q, want %q", got, want)
	}
}

func TestSimpleParserBridgeParsesShowTableTypesStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show table types;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowTableTypes {
		t.Fatalf("kind = %q, want show_table_types", statement.Kind)
	}
	if got, want := len(statement.ShowTableTypes.Result.Columns), 6; got != want {
		t.Fatalf("columns = %d, want %d", got, want)
	}
}

func TestSimpleParserBridgeParsesShowRoutineStatusStatements(t *testing.T) {
	functions, diagnostics := SimpleParserBridge{}.Parse("show function status like 'lo%';")
	if diagnostics.BlocksNative() {
		t.Fatalf("function status parse diagnostics: %#v", diagnostics)
	}
	if functions.Kind != QueryKindShowFunctionStatus || functions.ShowFuncStatus.Pattern != "lo%" {
		t.Fatalf("function status = %#v, want function status pattern lo%%", functions)
	}
	if got, want := len(functions.ShowFuncStatus.Result.Columns), 11; got != want {
		t.Fatalf("function status columns = %d, want %d", got, want)
	}

	procedures, diagnostics := SimpleParserBridge{}.Parse("show procedure status;")
	if diagnostics.BlocksNative() {
		t.Fatalf("procedure status parse diagnostics: %#v", diagnostics)
	}
	if procedures.Kind != QueryKindShowProcedureStatus {
		t.Fatalf("kind = %q, want show_procedure_status", procedures.Kind)
	}
	if got, want := len(procedures.ShowProcStatus.Result.Columns), 11; got != want {
		t.Fatalf("procedure status columns = %d, want %d", got, want)
	}
}

func TestSimpleParserBridgeParsesShowTriggerAndEventStatements(t *testing.T) {
	triggers, diagnostics := SimpleParserBridge{}.Parse("show triggers from quanta like 'order%';")
	if diagnostics.BlocksNative() {
		t.Fatalf("triggers parse diagnostics: %#v", diagnostics)
	}
	if triggers.Kind != QueryKindShowTriggers || triggers.ShowTriggers.Schema != "quanta" || triggers.ShowTriggers.Pattern != "order%" {
		t.Fatalf("triggers = %#v, want schema quanta pattern order%%", triggers.ShowTriggers)
	}
	if got, want := len(triggers.ShowTriggers.Result.Columns), 11; got != want {
		t.Fatalf("trigger columns = %d, want %d", got, want)
	}

	events, diagnostics := SimpleParserBridge{}.Parse("show events in quanta;")
	if diagnostics.BlocksNative() {
		t.Fatalf("events parse diagnostics: %#v", diagnostics)
	}
	if events.Kind != QueryKindShowEvents || events.ShowEvents.Schema != "quanta" {
		t.Fatalf("events = %#v, want schema quanta", events.ShowEvents)
	}
	if got, want := len(events.ShowEvents.Result.Columns), 15; got != want {
		t.Fatalf("event columns = %d, want %d", got, want)
	}
}

func TestSimpleParserBridgeParsesShowVariablesLikeStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show variables like 'character_set_%';")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowVariables {
		t.Fatalf("kind = %q, want show_variables", statement.Kind)
	}
	if got, want := statement.ShowVars.Pattern, "character_set_%"; got != want {
		t.Fatalf("pattern = %q, want %q", got, want)
	}
	if got, want := len(statement.ShowVars.Result.Columns), 2; got != want {
		t.Fatalf("columns = %d, want %d", got, want)
	}

	whereStatement, diagnostics := SimpleParserBridge{}.Parse("show variables where Variable_name = 'version';")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse where diagnostics: %#v", diagnostics)
	}
	if whereStatement.Kind != QueryKindShowVariables {
		t.Fatalf("where kind = %q, want show_variables", whereStatement.Kind)
	}
	if got, want := whereStatement.ShowVars.Patterns, []string{"version"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("where patterns = %#v, want %#v", got, want)
	}

	inStatement, diagnostics := SimpleParserBridge{}.Parse("show variables where Variable_name in ('version', 'version_comment');")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse in diagnostics: %#v", diagnostics)
	}
	if inStatement.Kind != QueryKindShowVariables {
		t.Fatalf("in kind = %q, want show_variables", inStatement.Kind)
	}
	if got, want := inStatement.ShowVars.Patterns, []string{"version", "version_comment"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("in patterns = %#v, want %#v", got, want)
	}

	orStatement, diagnostics := SimpleParserBridge{}.Parse("show variables where (Variable_name like 'character_set_%') or Variable_name = 'collation_connection';")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse or diagnostics: %#v", diagnostics)
	}
	if orStatement.Kind != QueryKindShowVariables {
		t.Fatalf("or kind = %q, want show_variables", orStatement.Kind)
	}
	if got, want := orStatement.ShowVars.Patterns, []string{"character_set_%", "collation_connection"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("or patterns = %#v, want %#v", got, want)
	}
}

func TestSimpleParserBridgeParsesShowDiagnosticsStatements(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show warnings limit 5 offset 2;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowWarnings {
		t.Fatalf("kind = %q, want show_warnings", statement.Kind)
	}
	if !statement.ShowWarnings.Result.HasLimit || statement.ShowWarnings.Result.Limit != 5 || statement.ShowWarnings.Result.Offset != 2 {
		t.Fatalf("result window = %#v, want limit 5 offset 2", statement.ShowWarnings.Result)
	}
	if got, want := len(statement.ShowWarnings.Result.Columns), 3; got != want {
		t.Fatalf("columns = %d, want %d", got, want)
	}

	errors, diagnostics := SimpleParserBridge{}.Parse("show errors limit 3;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse errors diagnostics: %#v", diagnostics)
	}
	if errors.Kind != QueryKindShowErrors {
		t.Fatalf("errors kind = %q, want show_errors", errors.Kind)
	}
	if !errors.ShowErrors.Result.HasLimit || errors.ShowErrors.Result.Limit != 3 {
		t.Fatalf("errors result window = %#v, want limit 3", errors.ShowErrors.Result)
	}
	if got, want := len(errors.ShowErrors.Result.Columns), 3; got != want {
		t.Fatalf("errors columns = %d, want %d", got, want)
	}

	warningCount, diagnostics := SimpleParserBridge{}.Parse("show count(*) warnings;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse warning count diagnostics: %#v", diagnostics)
	}
	if warningCount.Kind != QueryKindShowWarningCount {
		t.Fatalf("warning count kind = %q, want show_warning_count", warningCount.Kind)
	}
	if got, want := warningCount.ShowWarnCount.Result.Columns[0].Name, "@@session.warning_count"; got != want {
		t.Fatalf("warning count column = %q, want %q", got, want)
	}

	errorCount, diagnostics := SimpleParserBridge{}.Parse("show count(*) errors;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse error count diagnostics: %#v", diagnostics)
	}
	if errorCount.Kind != QueryKindShowErrorCount {
		t.Fatalf("error count kind = %q, want show_error_count", errorCount.Kind)
	}
	if got, want := errorCount.ShowErrorCount.Result.Columns[0].Name, "@@session.error_count"; got != want {
		t.Fatalf("error count column = %q, want %q", got, want)
	}
}

func TestSimpleParserBridgeParsesExplainStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("explain select count(*) from lineitem;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindExplain {
		t.Fatalf("kind = %q, want explain", statement.Kind)
	}
	if got, want := statement.Explain.SQL, "select count(*) from lineitem"; got != want {
		t.Fatalf("explain SQL = %q, want %q", got, want)
	}
	if got, want := len(statement.Explain.Result.Columns), 12; got != want {
		t.Fatalf("columns = %d, want %d", got, want)
	}
}

func TestSimpleParserBridgeParsesShowCharacterMetadataWhereStatement(t *testing.T) {
	charsets, diagnostics := SimpleParserBridge{}.Parse("show character set where Charset = 'utf8mb4';")
	if diagnostics.BlocksNative() {
		t.Fatalf("charset parse diagnostics: %#v", diagnostics)
	}
	if charsets.Kind != QueryKindShowCharacterSet || !reflect.DeepEqual(charsets.ShowCharset.Patterns, []string{"utf8mb4"}) {
		t.Fatalf("charsets = %#v, want character set pattern utf8mb4", charsets.ShowCharset)
	}

	collations, diagnostics := SimpleParserBridge{}.Parse("show collation where Charset = 'utf8mb4';")
	if diagnostics.BlocksNative() {
		t.Fatalf("collation parse diagnostics: %#v", diagnostics)
	}
	if collations.Kind != QueryKindShowCollation || !reflect.DeepEqual(collations.ShowCollation.Patterns, []string{"utf8mb4"}) {
		t.Fatalf("collations = %#v, want collation pattern utf8mb4", collations.ShowCollation)
	}
}

func TestSimpleParserBridgeParsesShowProcesslistStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show full processlist;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowProcesslist {
		t.Fatalf("kind = %q, want show_processlist", statement.Kind)
	}
	if !statement.ShowProcesslist.Full {
		t.Fatalf("full = false, want true")
	}
	if got, want := len(statement.ShowProcesslist.Result.Columns), 8; got != want {
		t.Fatalf("columns = %d, want %d", got, want)
	}
	if got, want := statement.ShowProcesslist.Result.Columns[7].Name, "Info"; got != want {
		t.Fatalf("info column = %q, want %q", got, want)
	}
}

func TestSimpleParserBridgeParsesShowEnginesStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show engines;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowEngines {
		t.Fatalf("kind = %q, want show_engines", statement.Kind)
	}
	if got, want := len(statement.ShowEngines.Result.Columns), 6; got != want {
		t.Fatalf("columns = %d, want %d", got, want)
	}
	if got, want := statement.ShowEngines.Result.Columns[0].Name, "Engine"; got != want {
		t.Fatalf("first column = %q, want %q", got, want)
	}
}

func TestSimpleParserBridgeParsesShowPluginsStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show plugins;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowPlugins {
		t.Fatalf("kind = %q, want show_plugins", statement.Kind)
	}
	if got, want := len(statement.ShowPlugins.Result.Columns), 5; got != want {
		t.Fatalf("columns = %d, want %d", got, want)
	}
	if got, want := statement.ShowPlugins.Result.Columns[0].Name, "Name"; got != want {
		t.Fatalf("first column = %q, want %q", got, want)
	}
}

func TestSimpleParserBridgeParsesShowPrivilegesStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show privileges;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowPrivileges {
		t.Fatalf("kind = %q, want show_privileges", statement.Kind)
	}
	if got, want := len(statement.ShowPrivileges.Result.Columns), 3; got != want {
		t.Fatalf("columns = %d, want %d", got, want)
	}
	if got, want := statement.ShowPrivileges.Result.Columns[0].Name, "Privilege"; got != want {
		t.Fatalf("first column = %q, want %q", got, want)
	}
}

func TestSimpleParserBridgeParsesShowGrantsStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show grants;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowGrants {
		t.Fatalf("kind = %q, want show_grants", statement.Kind)
	}
	if got, want := len(statement.ShowGrants.Result.Columns), 1; got != want {
		t.Fatalf("columns = %d, want %d", got, want)
	}
	if got, want := statement.ShowGrants.Result.Columns[0].Name, "Grants for User"; got != want {
		t.Fatalf("first column = %q, want %q", got, want)
	}
}

func TestSimpleParserBridgeParsesShowIndexStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show index from quanta.customer;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowIndex {
		t.Fatalf("kind = %q, want show_index", statement.Kind)
	}
	if statement.ShowIndex.Table.Schema != "quanta" || statement.ShowIndex.Table.Name != "customer" {
		t.Fatalf("target = %#v, want quanta.customer", statement.ShowIndex.Table)
	}
	if statement.ShowIndex.Result.Kind != ResultQuery || len(statement.ShowIndex.Result.Columns) != 15 {
		t.Fatalf("result = %#v, want fifteen-column query result", statement.ShowIndex.Result)
	}
}

func TestSimpleParserBridgeParsesShowKeysStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("show keys in customer;")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindShowIndex || statement.ShowIndex.Table.Name != "customer" {
		t.Fatalf("statement = %#v, want show index customer", statement)
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

func TestSimpleParserBridgeParsesDropViewCascadeStatement(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("drop view if exists building_customers cascade;")
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
	if !statement.DropView.Cascade {
		t.Fatalf("Cascade = false, want true")
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

func TestSimpleParserBridgeParsesCreateTableAsSelect(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("create table customers_copy as select customer_id from customers_qa")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics = %#v", diagnostics)
	}
	if statement.Kind != QueryKindCreateTable || statement.Create.Temporary {
		t.Fatalf("statement = %#v, want durable create table", statement)
	}
	if statement.Create.Table.Name != "customers_copy" {
		t.Fatalf("table = %#v, want customers_copy", statement.Create.Table)
	}
	if statement.Create.AsSQL != "select customer_id from customers_qa" {
		t.Fatalf("AsSQL = %q", statement.Create.AsSQL)
	}
}

func TestSimpleParserBridgeParsesCreateTableSelectWithoutAs(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("create table customers_copy select customer_id from customers_qa")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics = %#v", diagnostics)
	}
	if statement.Kind != QueryKindCreateTable || statement.Create.Temporary {
		t.Fatalf("statement = %#v, want durable create table", statement)
	}
	if statement.Create.Table.Name != "customers_copy" {
		t.Fatalf("table = %#v, want customers_copy", statement.Create.Table)
	}
	if statement.Create.AsSQL != "select customer_id from customers_qa" {
		t.Fatalf("AsSQL = %q", statement.Create.AsSQL)
	}
}

func TestSimpleParserBridgeParsesCreateTemporaryTableAsSelect(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("create temporary table customers_copy as select c_custkey from customer")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics = %#v", diagnostics)
	}
	if statement.Kind != QueryKindCreateTable || !statement.Create.Temporary {
		t.Fatalf("statement = %#v, want temporary create table", statement)
	}
	if statement.Create.Table.Name != "customers_copy" {
		t.Fatalf("table = %#v, want customers_copy", statement.Create.Table)
	}
	if statement.Create.AsSQL != "select c_custkey from customer" {
		t.Fatalf("AsSQL = %q", statement.Create.AsSQL)
	}
}

func TestSimpleParserBridgeParsesCreateTemporaryTableSelectWithoutAs(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("create temporary table customers_copy select c_custkey from customer")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics = %#v", diagnostics)
	}
	if statement.Kind != QueryKindCreateTable || !statement.Create.Temporary {
		t.Fatalf("statement = %#v, want temporary create table", statement)
	}
	if statement.Create.Table.Name != "customers_copy" {
		t.Fatalf("table = %#v, want customers_copy", statement.Create.Table)
	}
	if statement.Create.AsSQL != "select c_custkey from customer" {
		t.Fatalf("AsSQL = %q", statement.Create.AsSQL)
	}
}

func TestSimpleParserBridgeParsesCreateTemporaryTableLike(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("create temporary table customers_copy like customer")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics = %#v", diagnostics)
	}
	if statement.Kind != QueryKindCreateTable || !statement.Create.Temporary {
		t.Fatalf("statement = %#v, want temporary create table", statement)
	}
	if statement.Create.Table.Name != "customers_copy" {
		t.Fatalf("table = %#v, want customers_copy", statement.Create.Table)
	}
	if statement.Create.LikeTable.Name != "customer" {
		t.Fatalf("LikeTable = %#v, want customer", statement.Create.LikeTable)
	}
	if statement.Create.AsSQL != "" || len(statement.Create.Columns) != 0 {
		t.Fatalf("Create = %#v, want LIKE without CTAS SQL or inline fields", statement.Create)
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

func TestSimpleParserBridgeParsesDerivedTableSource(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select c.customer_key, customer_name
		from (
			select c_custkey as customer_key, c_name as customer_name
			from customer
			where c_mktsegment = 'BUILDING'
		) as c
		where customer_key = 1
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Tables) != 1 {
		t.Fatalf("tables = %#v, want one derived source", statement.Select.Tables)
	}
	table := statement.Select.Tables[0]
	if table.Name != "c" || table.Alias != "c" {
		t.Fatalf("table = %#v, want derived alias c", table)
	}
	if table.DerivedSelect == nil {
		t.Fatalf("derived select is nil")
	}
	if got := table.DerivedSQL; !strings.HasPrefix(strings.ToLower(got), "select c_custkey") {
		t.Fatalf("derived sql = %q, want inner SELECT", got)
	}
	if len(table.DerivedSelect.Tables) != 1 || table.DerivedSelect.Tables[0].Name != "customer" {
		t.Fatalf("derived tables = %#v, want customer", table.DerivedSelect.Tables)
	}
	if len(table.DerivedSelect.Predicates) != 1 {
		t.Fatalf("derived predicates = %#v, want one predicate", table.DerivedSelect.Predicates)
	}
}

func TestSimpleParserBridgeParsesInlineConstantUnionDerivedTableSource(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select c.name, c.cust_id
		from (
			select 1 as cust_id, 'Abe' as name
			union all select 2, 'Abby'
		) as c
		where c.cust_id = 1
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Tables) != 1 {
		t.Fatalf("tables = %#v, want one inline rowset source", statement.Select.Tables)
	}
	table := statement.Select.Tables[0]
	if table.Name != "c" || table.Alias != "c" {
		t.Fatalf("table = %#v, want inline alias c", table)
	}
	if table.DerivedSelect != nil {
		t.Fatalf("derived select = %#v, want nil for inline rowset", table.DerivedSelect)
	}
	if table.InlineRows == nil {
		t.Fatalf("inline rows is nil")
	}
	if len(table.InlineRows.Columns) != 2 || len(table.InlineRows.Rows) != 2 {
		t.Fatalf("inline rows = %#v, want two columns and two rows", table.InlineRows)
	}
	if table.InlineRows.Columns[0].Alias != "cust_id" || table.InlineRows.Columns[1].Alias != "name" {
		t.Fatalf("inline columns = %#v, want cust_id/name", table.InlineRows.Columns)
	}
}

func TestSimpleParserBridgeParsesDerivedTableJoinSource(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select q.order_key, q.customer_name
		from (
			select o.o_orderkey as order_key, c.c_name as customer_name
			from orders as o
			inner join customer as c on o.o_custkey = c.c_custkey
		) as q
		where q.order_key = 1
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Tables) != 1 {
		t.Fatalf("tables = %#v, want one derived source", statement.Select.Tables)
	}
	table := statement.Select.Tables[0]
	if table.Name != "q" || table.DerivedSelect == nil {
		t.Fatalf("table = %#v, want derived q", table)
	}
	if len(table.DerivedSelect.Tables) != 2 {
		t.Fatalf("derived tables = %#v, want two joined tables", table.DerivedSelect.Tables)
	}
	if len(table.DerivedSelect.Joins) != 1 {
		t.Fatalf("derived joins = %#v, want one join", table.DerivedSelect.Joins)
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

func TestSimpleParserBridgeParsesProjectionOnlyWhere(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select 1 as matched where 3 > 2 and 2 <= 2")
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
	if got, want := len(statement.Select.Predicates), 2; got != want {
		t.Fatalf("predicate count = %d, want %d", got, want)
	}
	first, ok := statement.Select.Predicates[0].Expr.(UnboundBinaryExpr)
	if !ok || first.Op != BinaryOpGreater {
		t.Fatalf("first predicate = %#v, want greater-than binary", statement.Select.Predicates[0].Expr)
	}
	second, ok := statement.Select.Predicates[1].Expr.(UnboundBinaryExpr)
	if !ok || second.Op != BinaryOpLessEqual {
		t.Fatalf("second predicate = %#v, want less-than-or-equal binary", statement.Select.Predicates[1].Expr)
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

func TestSimpleParserBridgeParsesRightJoinEdge(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select c.first_name, o.order_id from orders_qa as o right join customers_qa as c on c.cust_id = o.cust_id")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Joins) != 1 {
		t.Fatalf("joins = %d, want 1", len(statement.Select.Joins))
	}
	join := statement.Select.Joins[0]
	if join.Kind != JoinKindRightOuter {
		t.Fatalf("join kind = %q, want right outer", join.Kind)
	}
	if join.LeftQualifier != "o" || join.LeftField != "cust_id" || join.RightQualifier != "c" || join.RightField != "cust_id" {
		t.Fatalf("join = %#v, want o.cust_id = c.cust_id", join)
	}
}

func TestSimpleParserBridgeParsesNonEquiJoinEdge(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select r.r_name, n.n_name from region as r inner join nation as n on n.n_regionkey >= r.r_regionkey")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Joins) != 1 {
		t.Fatalf("joins = %d, want 1", len(statement.Select.Joins))
	}
	join := statement.Select.Joins[0]
	if join.Operator != BinaryOpGreaterEqual {
		t.Fatalf("join operator = %q, want %q", join.Operator, BinaryOpGreaterEqual)
	}
	if join.LeftQualifier != "n" || join.LeftField != "n_regionkey" || join.RightQualifier != "r" || join.RightField != "r_regionkey" {
		t.Fatalf("join = %#v, want n.n_regionkey >= r.r_regionkey", join)
	}
}

func TestSimpleParserBridgeParsesJoinUsingEdge(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select count(*) from orders_qa as o1 inner join orders_qa as o2 using (cust_id)")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Joins) != 1 {
		t.Fatalf("joins = %d, want 1", len(statement.Select.Joins))
	}
	join := statement.Select.Joins[0]
	if join.Kind != JoinKindInner {
		t.Fatalf("join kind = %q, want inner", join.Kind)
	}
	if join.LeftQualifier != "o1" || join.LeftField != "cust_id" || join.RightQualifier != "o2" || join.RightField != "cust_id" {
		t.Fatalf("join = %#v, want o1.cust_id = o2.cust_id", join)
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

func TestSimpleParserBridgeParsesNotEqualJoinEdge(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select c.first_name, o.order_id
		from customers_qa as c
		inner join orders_qa as o on o.cust_id = c.cust_id
		inner join lineitems_qa as l on l.order_id != o.order_id
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Joins) != 2 {
		t.Fatalf("joins = %d, want 2", len(statement.Select.Joins))
	}
	if statement.Select.Joins[1].Operator != BinaryOpNotEqual {
		t.Fatalf("second join operator = %q, want %q", statement.Select.Joins[1].Operator, BinaryOpNotEqual)
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

func TestSimpleParserBridgeParsesRowValueSubqueryMembership(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select n_name as nation_name
		from nation
		where (n_regionkey, n_nationkey) in (
			select r_regionkey, 1
			from region
			where r_name = 'AMERICA'
		)
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Memberships) != 1 {
		t.Fatalf("memberships = %d, want 1", len(statement.Select.Memberships))
	}
	membership := statement.Select.Memberships[0]
	if len(membership.LeftTuple) != 2 || len(membership.RightTuple) != 2 {
		t.Fatalf("membership tuple arity = %d/%d, want 2/2", len(membership.LeftTuple), len(membership.RightTuple))
	}
	left, ok := membership.LeftTuple[0].(UnboundFieldExpr)
	if !ok || left.Name != "n_regionkey" {
		t.Fatalf("left tuple[0] = %#v, want n_regionkey", membership.LeftTuple[0])
	}
	rightField, ok := membership.RightTuple[0].(UnboundFieldExpr)
	if !ok || rightField.Qualifier != "region" || rightField.Name != "r_regionkey" {
		t.Fatalf("right tuple[0] = %#v, want region.r_regionkey", membership.RightTuple[0])
	}
	rightLiteral, ok := membership.RightTuple[1].(UnboundLiteralExpr)
	if !ok || rightLiteral.Kind != ValueInt || rightLiteral.Value != int64(1) {
		t.Fatalf("right tuple[1] = %#v, want int literal 1", membership.RightTuple[1])
	}
	if len(membership.Predicates) != 1 {
		t.Fatalf("membership predicates = %d, want 1", len(membership.Predicates))
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

func TestSimpleParserBridgeParsesReplaceAsInsertEquivalent(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("replace into customers_qa (cust_id, first_name) values (1, 'Ada')")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindInsert {
		t.Fatalf("kind = %q, want insert", statement.Kind)
	}
	if statement.SQL != "replace into customers_qa (cust_id, first_name) values (1, 'Ada')" {
		t.Fatalf("SQL = %q, want original REPLACE SQL", statement.SQL)
	}
	if statement.Insert.Table.Name != "customers_qa" {
		t.Fatalf("insert table = %#v, want customers_qa", statement.Insert.Table)
	}
	if got := statement.Insert.Columns; len(got) != 2 || got[0] != "cust_id" || got[1] != "first_name" {
		t.Fatalf("insert columns = %#v, want cust_id, first_name", got)
	}
	if len(statement.Insert.Rows) != 1 || len(statement.Insert.Rows[0]) != 2 {
		t.Fatalf("insert rows = %#v, want one two-value row", statement.Insert.Rows)
	}
}

func TestSimpleParserBridgeParsesInsertOnDuplicateKeyUpdateAsInsertEquivalent(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("insert into customers_qa (cust_id, first_name) values (1, 'Ada') on duplicate key update first_name = values(first_name)")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindInsert {
		t.Fatalf("kind = %q, want insert", statement.Kind)
	}
	if statement.Insert.Table.Name != "customers_qa" {
		t.Fatalf("insert table = %#v, want customers_qa", statement.Insert.Table)
	}
	if got := statement.Insert.Columns; len(got) != 2 || got[0] != "cust_id" || got[1] != "first_name" {
		t.Fatalf("insert columns = %#v, want cust_id, first_name", got)
	}
	if len(statement.Insert.Rows) != 1 || len(statement.Insert.Rows[0]) != 2 {
		t.Fatalf("insert rows = %#v, want one two-value row", statement.Insert.Rows)
	}
}

func TestSimpleParserBridgeParsesInsertValuesWithoutColumnList(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("insert into customers_qa values ('9001', 'Ada', 42)")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindInsert {
		t.Fatalf("kind = %q, want insert", statement.Kind)
	}
	if statement.Insert.Table.Name != "customers_qa" {
		t.Fatalf("insert table = %#v, want customers_qa", statement.Insert.Table)
	}
	if len(statement.Insert.Columns) != 0 {
		t.Fatalf("insert columns = %#v, want omitted column list", statement.Insert.Columns)
	}
	if len(statement.Insert.Rows) != 1 || len(statement.Insert.Rows[0]) != 3 {
		t.Fatalf("insert rows = %#v, want one three-value row", statement.Insert.Rows)
	}
}

func TestSimpleParserBridgeParsesInsertSelect(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("insert into customers_qa (cust_id, first_name) select c_custkey, c_name from customer where c_custkey in (1, 2) order by c_custkey")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindInsert {
		t.Fatalf("kind = %q, want insert", statement.Kind)
	}
	if statement.Insert.Table.Name != "customers_qa" {
		t.Fatalf("insert table = %#v, want customers_qa", statement.Insert.Table)
	}
	if got := statement.Insert.Columns; len(got) != 2 || got[0] != "cust_id" || got[1] != "first_name" {
		t.Fatalf("insert columns = %#v, want cust_id, first_name", got)
	}
	if len(statement.Insert.Rows) != 0 {
		t.Fatalf("insert rows = %#v, want none for INSERT SELECT", statement.Insert.Rows)
	}
	wantSQL := "select c_custkey, c_name from customer where c_custkey in (1, 2) order by c_custkey"
	if statement.Insert.SourceSQL != wantSQL {
		t.Fatalf("SourceSQL = %q, want %q", statement.Insert.SourceSQL, wantSQL)
	}
}

func TestSimpleParserBridgeParsesInsertSelectWithoutColumnList(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("insert into customers_qa select c_custkey, c_name from customer order by c_custkey limit 2")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindInsert {
		t.Fatalf("kind = %q, want insert", statement.Kind)
	}
	if statement.Insert.Table.Name != "customers_qa" {
		t.Fatalf("insert table = %#v, want customers_qa", statement.Insert.Table)
	}
	if len(statement.Insert.Columns) != 0 {
		t.Fatalf("insert columns = %#v, want omitted column list", statement.Insert.Columns)
	}
	wantSQL := "select c_custkey, c_name from customer order by c_custkey limit 2"
	if statement.Insert.SourceSQL != wantSQL {
		t.Fatalf("SourceSQL = %q, want %q", statement.Insert.SourceSQL, wantSQL)
	}
}

func TestSimpleParserBridgeParsesFormattedInsertValues(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		insert into customer (
			c_custkey,
			c_name,
			c_acctbal
		) values (
			900000002,
			'Formatted Customer',
			42.50
		)
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindInsert {
		t.Fatalf("kind = %q, want insert", statement.Kind)
	}
	if statement.Insert.Table.Name != "customer" {
		t.Fatalf("insert table = %#v, want customer", statement.Insert.Table)
	}
	if got := statement.Insert.Columns; len(got) != 3 || got[0] != "c_custkey" || got[2] != "c_acctbal" {
		t.Fatalf("insert columns = %#v, want c_custkey, c_name, c_acctbal", got)
	}
	if len(statement.Insert.Rows) != 1 || len(statement.Insert.Rows[0]) != 3 {
		t.Fatalf("insert rows = %#v, want one three-value row", statement.Insert.Rows)
	}
	key, ok := statement.Insert.Rows[0][0].(UnboundLiteralExpr)
	if !ok || key.Kind != ValueInt || key.Value != int64(900000002) {
		t.Fatalf("key literal = %#v, want int 900000002", statement.Insert.Rows[0][0])
	}
	balance, ok := statement.Insert.Rows[0][2].(UnboundLiteralExpr)
	if !ok || balance.Kind != ValueFloat || balance.Value != float64(42.5) {
		t.Fatalf("balance literal = %#v, want float 42.5", statement.Insert.Rows[0][2])
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

func TestSimpleParserBridgeParsesInsertValueParameters(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("insert into orders (o_orderkey, o_orderpriority) values (?, ?), (?, ?)")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Insert.Rows) != 2 || len(statement.Insert.Rows[0]) != 2 || len(statement.Insert.Rows[1]) != 2 {
		t.Fatalf("insert rows = %#v, want two two-value rows", statement.Insert.Rows)
	}
	parameters := []UnboundExpr{
		statement.Insert.Rows[0][0],
		statement.Insert.Rows[0][1],
		statement.Insert.Rows[1][0],
		statement.Insert.Rows[1][1],
	}
	for i, expr := range parameters {
		parameter, ok := expr.(UnboundParameterExpr)
		if !ok || parameter.Index != i+1 {
			t.Fatalf("parameter %d = %#v, want index %d", i, expr, i+1)
		}
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

func TestSimpleParserBridgeParsesTransactionStatements(t *testing.T) {
	tests := []struct {
		sql      string
		wantKind SessionActionKind
	}{
		{sql: "begin", wantKind: SessionActionBeginTransaction},
		{sql: "begin work", wantKind: SessionActionBeginTransaction},
		{sql: "start transaction", wantKind: SessionActionBeginTransaction},
		{sql: "start transaction read only", wantKind: SessionActionBeginTransaction},
		{sql: "rollback", wantKind: SessionActionRollbackTransaction},
		{sql: "rollback work", wantKind: SessionActionRollbackTransaction},
	}

	for _, tt := range tests {
		statement, diagnostics := SimpleParserBridge{}.Parse(tt.sql)
		if diagnostics.BlocksNative() {
			t.Fatalf("%s parse diagnostics: %#v", tt.sql, diagnostics)
		}
		if statement.Kind != QueryKindSession {
			t.Fatalf("%s kind = %q, want session", tt.sql, statement.Kind)
		}
		if len(statement.Session.Actions) != 1 || statement.Session.Actions[0].Kind != tt.wantKind {
			t.Fatalf("%s session actions = %#v, want %s", tt.sql, statement.Session.Actions, tt.wantKind)
		}
		if statement.Session.Result.Kind != ResultStatement {
			t.Fatalf("%s result kind = %q, want statement", tt.sql, statement.Session.Result.Kind)
		}
	}
}

func TestSimpleParserBridgeParsesSetTransaction(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("set transaction isolation level read committed")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindSession {
		t.Fatalf("kind = %q, want session", statement.Kind)
	}
	if len(statement.Session.Actions) != 0 {
		t.Fatalf("session actions = %#v, want no-op", statement.Session.Actions)
	}
	if statement.Session.Result.Kind != ResultStatement {
		t.Fatalf("result kind = %q, want statement", statement.Session.Result.Kind)
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

func TestSimpleParserBridgeParsesOrderByUnprojectedExpression(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select n_name as nation_name from nation order by n_nationkey + 1 desc limit 3")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.OrderBy) != 1 {
		t.Fatalf("order by = %d, want 1", len(statement.Select.OrderBy))
	}
	expr, ok := statement.Select.OrderBy[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("order by expression = %T, want UnboundBinaryExpr", statement.Select.OrderBy[0].Expr)
	}
	if expr.Op != BinaryOpAdd {
		t.Fatalf("order by op = %q, want %q", expr.Op, BinaryOpAdd)
	}
	if field, ok := expr.Left.(UnboundFieldExpr); !ok || field.Name != "n_nationkey" {
		t.Fatalf("order by left = %#v, want n_nationkey field", expr.Left)
	}
	if literal, ok := expr.Right.(UnboundLiteralExpr); !ok || literal.Kind != ValueInt || literal.Value != int64(1) {
		t.Fatalf("order by right = %#v, want int literal 1", expr.Right)
	}
	if statement.Select.OrderBy[0].Direction != SortDescending {
		t.Fatalf("order by direction = %q, want desc", statement.Select.OrderBy[0].Direction)
	}
	if statement.Select.Result.Limit != 3 {
		t.Fatalf("limit = %d, want 3", statement.Select.Result.Limit)
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

func TestSimpleParserBridgeParsesSearchedCaseProjection(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select c_custkey, case when c_mktsegment = 'BUILDING' then 'build' else lower(c_mktsegment) end as segment_label from customer order by c_custkey limit 5")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Projection) != 2 {
		t.Fatalf("projections = %d, want 2", len(statement.Select.Projection))
	}
	searchedCase, ok := statement.Select.Projection[1].Expr.(UnboundSearchedCaseExpr)
	if !ok {
		t.Fatalf("projection expression = %T, want UnboundSearchedCaseExpr", statement.Select.Projection[1].Expr)
	}
	if statement.Select.Projection[1].Alias != "segment_label" {
		t.Fatalf("projection alias = %q, want segment_label", statement.Select.Projection[1].Alias)
	}
	if len(searchedCase.Whens) != 1 || searchedCase.Else == nil {
		t.Fatalf("searched case = %#v, want one WHEN and ELSE", searchedCase)
	}
	if _, ok := searchedCase.Else.(UnboundCallExpr); !ok {
		t.Fatalf("searched case ELSE = %T, want UnboundCallExpr", searchedCase.Else)
	}
}

func TestSimpleParserBridgeParsesSearchedCasePredicateComparison(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select c_custkey from customer where case when c_mktsegment = 'BUILDING' then 1 else 0 end = 1")
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
	if _, ok := predicate.Left.(UnboundSearchedCaseExpr); !ok {
		t.Fatalf("predicate left = %T, want UnboundSearchedCaseExpr", predicate.Left)
	}
}

func TestSimpleParserBridgeParsesSimpleCaseProjection(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select c_custkey, case c_mktsegment when 'BUILDING' then 'build' else lower(c_mktsegment) end as segment_label from customer order by c_custkey limit 5")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Projection) != 2 {
		t.Fatalf("projections = %d, want 2", len(statement.Select.Projection))
	}
	searchedCase, ok := statement.Select.Projection[1].Expr.(UnboundSearchedCaseExpr)
	if !ok {
		t.Fatalf("projection expression = %T, want UnboundSearchedCaseExpr", statement.Select.Projection[1].Expr)
	}
	if len(searchedCase.Whens) != 1 || searchedCase.Else == nil {
		t.Fatalf("simple case lowering = %#v, want one WHEN and ELSE", searchedCase)
	}
	condition, ok := searchedCase.Whens[0].Condition.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("simple case condition = %T, want UnboundBinaryExpr", searchedCase.Whens[0].Condition)
	}
	if condition.Op != BinaryOpEqual {
		t.Fatalf("simple case condition op = %q, want %q", condition.Op, BinaryOpEqual)
	}
	left, ok := condition.Left.(UnboundFieldExpr)
	if !ok || left.Name != "c_mktsegment" {
		t.Fatalf("simple case condition left = %#v, want c_mktsegment field", condition.Left)
	}
	right, ok := condition.Right.(UnboundLiteralExpr)
	if !ok || right.Value != "BUILDING" {
		t.Fatalf("simple case condition right = %#v, want BUILDING literal", condition.Right)
	}
}

func TestSimpleParserBridgeParsesSimpleCasePredicateComparison(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select c_custkey from customer where case c_mktsegment when 'BUILDING' then 1 else 0 end = 1")
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
	if _, ok := predicate.Left.(UnboundSearchedCaseExpr); !ok {
		t.Fatalf("predicate left = %T, want UnboundSearchedCaseExpr", predicate.Left)
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

func TestSimpleParserBridgeParsesMySQLScalarFunctionBoundarySyntax(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select substring('alphabet' from 2 for 3) as sub_value, trim(leading 'x' from 'xxxstream') as trim_value, cast('42' as signed) as cast_value")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Projection) != 3 {
		t.Fatalf("projection count = %d, want 3", len(statement.Select.Projection))
	}
	substringCall, ok := statement.Select.Projection[0].Expr.(UnboundCallExpr)
	if !ok || substringCall.Name != "substring" || len(substringCall.Args) != 3 {
		t.Fatalf("substring projection = %#v, want three-arg substring call", statement.Select.Projection[0].Expr)
	}
	trimCall, ok := statement.Select.Projection[1].Expr.(UnboundCallExpr)
	if !ok || trimCall.Name != "trim" || len(trimCall.Args) != 3 {
		t.Fatalf("trim projection = %#v, want normalized trim mode/removal/value call", statement.Select.Projection[1].Expr)
	}
	mode, ok := trimCall.Args[0].(UnboundLiteralExpr)
	if !ok || mode.Kind != ValueString || mode.Value != "leading" {
		t.Fatalf("trim mode = %#v, want leading literal", trimCall.Args[0])
	}
	castCall, ok := statement.Select.Projection[2].Expr.(UnboundCallExpr)
	if !ok || castCall.Name != "toint" || len(castCall.Args) != 1 {
		t.Fatalf("cast projection = %#v, want normalized toint call", statement.Select.Projection[2].Expr)
	}
}

func TestSimpleParserBridgeParsesIfPredicateCondition(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select if(l_quantity > 10, 'big', 'small') as bucket from lineitem")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Projection) != 1 {
		t.Fatalf("projection count = %d, want 1", len(statement.Select.Projection))
	}
	call, ok := statement.Select.Projection[0].Expr.(UnboundCallExpr)
	if !ok || call.Name != "if" || len(call.Args) != 3 {
		t.Fatalf("projection = %#v, want if call", statement.Select.Projection[0].Expr)
	}
	condition, ok := call.Args[0].(UnboundBinaryExpr)
	if !ok || condition.Op != BinaryOpGreater {
		t.Fatalf("if condition = %#v, want greater-than binary expression", call.Args[0])
	}
}

func TestSimpleParserBridgeParsesScalarSubqueryHaving(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`select o_orderpriority, count(*) as c
from orders
group by o_orderpriority
having c > (
  select o_orderkey
  from orders
  where o_orderkey >= 1
)`)
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
	if ref.Alias != "c" || ref.Index != 0 {
		t.Fatalf("having ref = %#v, want c[0]", ref)
	}
	subquery, ok := predicate.Right.(UnboundScalarSubqueryExpr)
	if !ok {
		t.Fatalf("having right = %T, want UnboundScalarSubqueryExpr", predicate.Right)
	}
	if subquery.Scope != PredicateScopeHaving {
		t.Fatalf("subquery scope = %q, want %q", subquery.Scope, PredicateScopeHaving)
	}
	if !strings.Contains(subquery.SQL, "select o_orderkey") {
		t.Fatalf("subquery SQL = %q, want preserved SELECT body", subquery.SQL)
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

func TestSimpleParserBridgeParsesCompoundAggregateHaving(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select c_mktsegment as market_segment, count(*) as customer_count from customer group by c_mktsegment having count(*) > 280 and count(*) < 320 order by market_segment")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Aggregates) != 1 {
		t.Fatalf("aggregates = %d, want 1", len(statement.Select.Aggregates))
	}
	aggregate := statement.Select.Aggregates[0]
	if aggregate.Function != "count" || !aggregate.CountAll || aggregate.Alias != "customer_count" {
		t.Fatalf("aggregate = %#v, want projected count(*)", aggregate)
	}
	if len(statement.Select.Having) != 1 {
		t.Fatalf("having = %d, want 1", len(statement.Select.Having))
	}
	root, ok := statement.Select.Having[0].Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("having expression = %T, want UnboundBinaryExpr", statement.Select.Having[0].Expr)
	}
	if root.Op != BinaryOpAnd {
		t.Fatalf("having root op = %q, want %q", root.Op, BinaryOpAnd)
	}
	left, ok := root.Left.(UnboundBinaryExpr)
	if !ok || left.Op != BinaryOpGreater {
		t.Fatalf("having left = %#v, want count(*) > 280", root.Left)
	}
	right, ok := root.Right.(UnboundBinaryExpr)
	if !ok || right.Op != BinaryOpLess {
		t.Fatalf("having right = %#v, want count(*) < 320", root.Right)
	}
	for _, expr := range []UnboundExpr{left.Left, right.Left} {
		ref, ok := expr.(UnboundAggregateRefExpr)
		if !ok {
			t.Fatalf("having comparison left = %T, want UnboundAggregateRefExpr", expr)
		}
		if ref.Alias != "customer_count" || ref.Index != 0 {
			t.Fatalf("having ref = %#v, want projected count ref", ref)
		}
	}
}

func TestSimpleParserBridgeParsesAggregateArithmeticHaving(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select n_regionkey as region_key, sum(n_nationkey) as nation_key_sum from nation group by n_regionkey having sum(n_nationkey) / count(*) > 10 order by region_key")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Aggregates) != 2 {
		t.Fatalf("aggregates = %d, want projected sum plus hidden count", len(statement.Select.Aggregates))
	}
	if aggregate := statement.Select.Aggregates[0]; aggregate.Function != "sum" || aggregate.Alias != "nation_key_sum" {
		t.Fatalf("first aggregate = %#v, want projected sum", aggregate)
	}
	if aggregate := statement.Select.Aggregates[1]; aggregate.Function != "count" || !aggregate.CountAll || aggregate.Alias != "__having_agg_1" {
		t.Fatalf("second aggregate = %#v, want hidden count(*)", aggregate)
	}
	if len(statement.Select.Having) != 1 {
		t.Fatalf("having = %d, want 1", len(statement.Select.Having))
	}
	comparison, ok := statement.Select.Having[0].Expr.(UnboundBinaryExpr)
	if !ok || comparison.Op != BinaryOpGreater {
		t.Fatalf("having expression = %#v, want arithmetic comparison", statement.Select.Having[0].Expr)
	}
	ratio, ok := comparison.Left.(UnboundBinaryExpr)
	if !ok || ratio.Op != BinaryOpDivide {
		t.Fatalf("having left = %#v, want aggregate ratio", comparison.Left)
	}
	leftRef, ok := ratio.Left.(UnboundAggregateRefExpr)
	if !ok || leftRef.Alias != "nation_key_sum" || leftRef.Index != 0 {
		t.Fatalf("ratio left = %#v, want projected sum ref", ratio.Left)
	}
	rightRef, ok := ratio.Right.(UnboundAggregateRefExpr)
	if !ok || rightRef.Alias != "__having_agg_1" || rightRef.Index != 1 {
		t.Fatalf("ratio right = %#v, want hidden count ref", ratio.Right)
	}
	literal, ok := comparison.Right.(UnboundLiteralExpr)
	if !ok || literal.Kind != ValueInt || literal.Value != int64(10) {
		t.Fatalf("comparison right = %#v, want int literal 10", comparison.Right)
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

func TestSimpleParserBridgeParsesGroupByOrdinalPosition(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select c_mktsegment as market_segment, count(*) as customer_count from customer group by 1 order by market_segment")
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

func TestSimpleParserBridgeParsesRegexpPredicateAsResidual(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse("select r_name from region where r_name regexp '^A'")
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Predicates) != 1 {
		t.Fatalf("predicates = %d, want 1", len(statement.Select.Predicates))
	}
	predicate := statement.Select.Predicates[0]
	if predicate.Placement != PredicateResidualScan {
		t.Fatalf("placement = %s, want %s", predicate.Placement, PredicateResidualScan)
	}
	binary, ok := predicate.Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("predicate expr = %T, want UnboundBinaryExpr", predicate.Expr)
	}
	if binary.Op != BinaryOpRegexp {
		t.Fatalf("predicate op = %s, want %s", binary.Op, BinaryOpRegexp)
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

func TestSimpleParserBridgeParsesConstantUnionAllOrderByLimitOne(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select 2 as n
		union all
		select 1 as n
		order by n
		limit 1
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Projection) != 1 {
		t.Fatalf("projections = %d, want 1", len(statement.Select.Projection))
	}
	if statement.Select.Projection[0].Alias != "n" {
		t.Fatalf("alias = %q, want n", statement.Select.Projection[0].Alias)
	}
	literal, ok := statement.Select.Projection[0].Expr.(UnboundLiteralExpr)
	if !ok {
		t.Fatalf("projection expr = %T, want UnboundLiteralExpr", statement.Select.Projection[0].Expr)
	}
	if literal.Kind != ValueInt || literal.Value != int64(1) {
		t.Fatalf("literal = %#v, want int 1", literal)
	}
	if !statement.Select.Result.HasLimit || statement.Select.Result.Limit != 1 {
		t.Fatalf("result limit = %#v, want LIMIT 1", statement.Select.Result)
	}
}

func TestSimpleParserBridgeParsesTableBackedUnionAll(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select 'orders' as source_table, count(*) as row_count
		from orders
		union all
		select 'lineitem' as source_table, count(*) as row_count
		from lineitem
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if statement.Kind != QueryKindUnionAll {
		t.Fatalf("kind = %q, want union_all", statement.Kind)
	}
	if len(statement.UnionAll.Branches) != 2 {
		t.Fatalf("branches = %d, want 2", len(statement.UnionAll.Branches))
	}
	if got := statement.UnionAll.Branches[0].Tables[0].Name; got != "orders" {
		t.Fatalf("branch 1 table = %q, want orders", got)
	}
	if got := statement.UnionAll.Branches[1].Tables[0].Name; got != "lineitem" {
		t.Fatalf("branch 2 table = %q, want lineitem", got)
	}
}

func TestSimpleParserBridgeParsesRowValueInAsResidualPredicate(t *testing.T) {
	statement, diagnostics := SimpleParserBridge{}.Parse(`
		select n_name as nation_name
		from nation
		where (n_regionkey, n_nationkey) in ((1, 1), (1, 2))
		order by nation_name
	`)
	if diagnostics.BlocksNative() {
		t.Fatalf("parse diagnostics: %#v", diagnostics)
	}
	if len(statement.Select.Predicates) != 1 {
		t.Fatalf("predicates = %d, want 1", len(statement.Select.Predicates))
	}
	predicate := statement.Select.Predicates[0]
	if predicate.Placement != PredicateResidualScan {
		t.Fatalf("placement = %s, want %s", predicate.Placement, PredicateResidualScan)
	}
	root, ok := predicate.Expr.(UnboundBinaryExpr)
	if !ok {
		t.Fatalf("predicate expr = %T, want UnboundBinaryExpr", predicate.Expr)
	}
	if root.Op != BinaryOpOr {
		t.Fatalf("root op = %s, want %s", root.Op, BinaryOpOr)
	}
	left, ok := root.Left.(UnboundBinaryExpr)
	if !ok || left.Op != BinaryOpAnd {
		t.Fatalf("left branch = %#v, want AND branch", root.Left)
	}
	right, ok := root.Right.(UnboundBinaryExpr)
	if !ok || right.Op != BinaryOpAnd {
		t.Fatalf("right branch = %#v, want AND branch", root.Right)
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

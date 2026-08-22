package qsbridge

import "testing"

func TestPlannerExpandsSimpleLogicalView(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{{
		Schema: "quanta",
		Name:   "expensive_orders",
		SQL:    "select o_orderkey as order_key, o_totalprice from orders where o_totalprice > 100",
	}}
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       catalog,
		DefaultSchema: "quanta",
	}

	result := planner.Plan("select order_key from expensive_orders where order_key = 7")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Sources) != 1 || result.Query.Sources[0].Table != "orders" {
		t.Fatalf("sources = %#v, want orders", result.Query.Sources)
	}
	if got := result.Query.Sources[0].RefName(); got != "expensive_orders" {
		t.Fatalf("source ref = %q, want expensive_orders", got)
	}
	if len(result.Query.Predicates) != 2 {
		t.Fatalf("predicates = %d, want view + outer predicates", len(result.Query.Predicates))
	}
	if !predicateReferencesField(result.Query.Predicates[0], "expensive_orders", "o_totalprice") {
		t.Fatalf("view predicate = %#v, want expensive_orders.o_totalprice", result.Query.Predicates[0])
	}
	if !predicateReferencesField(result.Query.Predicates[1], "expensive_orders", "o_orderkey") {
		t.Fatalf("outer predicate = %#v, want expensive_orders.o_orderkey", result.Query.Predicates[1])
	}
	if len(result.Query.Projection) != 1 || result.Query.Projection[0].Alias != "order_key" {
		t.Fatalf("projection = %#v, want alias order_key", result.Query.Projection)
	}
}

func TestPlannerCarriesLogicalViewOrderByWhenOuterQueryHasNoSort(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{{
		Schema: "quanta",
		Name:   "ordered_customers",
		SQL:    "select c_custkey as customer_key, c_name as customer_name from customer order by c_custkey",
	}}
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       catalog,
		DefaultSchema: "quanta",
	}

	result := planner.Plan("select customer_key, customer_name from ordered_customers limit 1")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Sources) != 1 || result.Query.Sources[0].Table != "customer" {
		t.Fatalf("sources = %#v, want customer", result.Query.Sources)
	}
	if got := result.Query.Sources[0].RefName(); got != "ordered_customers" {
		t.Fatalf("source ref = %q, want ordered_customers", got)
	}
	if result.Query.Result.Limit != 1 {
		t.Fatalf("limit = %d, want 1", result.Query.Result.Limit)
	}
	if len(result.Query.OrderBy) != 1 || !exprReferencesField(result.Query.OrderBy[0].Expr, "ordered_customers", "c_custkey") {
		t.Fatalf("order by = %#v, want ordered_customers.c_custkey", result.Query.OrderBy)
	}
	if len(result.Query.Projection) != 2 {
		t.Fatalf("projection = %#v, want customer key/name", result.Query.Projection)
	}
	if result.Query.Projection[0].Alias != "customer_key" || !exprReferencesField(result.Query.Projection[0].Expr, "ordered_customers", "c_custkey") {
		t.Fatalf("projection[0] = %#v, want ordered_customers.c_custkey", result.Query.Projection[0])
	}
}

func TestPlannerCarriesLogicalViewLimitWhenOuterQueryHasNoWindow(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{{
		Schema: "quanta",
		Name:   "limited_customers",
		SQL:    "select c_custkey as customer_key, c_name as customer_name from customer limit 1",
	}}
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       catalog,
		DefaultSchema: "quanta",
	}

	result := planner.Plan("select customer_key, customer_name from limited_customers")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Sources) != 1 || result.Query.Sources[0].Table != "customer" {
		t.Fatalf("sources = %#v, want customer", result.Query.Sources)
	}
	if got := result.Query.Sources[0].RefName(); got != "limited_customers" {
		t.Fatalf("source ref = %q, want limited_customers", got)
	}
	if !result.Query.Result.HasResultLimit() || result.Query.Result.Limit != 1 {
		t.Fatalf("result window = %#v, want limit 1", result.Query.Result)
	}
	if len(result.Query.Projection) != 2 {
		t.Fatalf("projection = %#v, want customer key/name", result.Query.Projection)
	}
	if result.Query.Projection[0].Alias != "customer_key" || !exprReferencesField(result.Query.Projection[0].Expr, "limited_customers", "c_custkey") {
		t.Fatalf("projection[0] = %#v, want limited_customers.c_custkey", result.Query.Projection[0])
	}
}

func TestPlannerExpandsSimpleDerivedTable(t *testing.T) {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}

	result := planner.Plan(`
		select customer_key, customer_name
		from (
			select c_custkey as customer_key, c_name as customer_name
			from customer
			where c_name = 'Customer#000000001'
		) as c
		where customer_key = 1
		order by customer_key
	`)
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Sources) != 1 || result.Query.Sources[0].Table != "customer" {
		t.Fatalf("sources = %#v, want customer", result.Query.Sources)
	}
	if got := result.Query.Sources[0].RefName(); got != "c" {
		t.Fatalf("source ref = %q, want c", got)
	}
	if len(result.Query.Predicates) != 2 {
		t.Fatalf("predicates = %d, want derived + outer predicates", len(result.Query.Predicates))
	}
	if !predicateReferencesField(result.Query.Predicates[0], "c", "c_name") {
		t.Fatalf("derived predicate = %#v, want c.c_name", result.Query.Predicates[0])
	}
	if !predicateReferencesField(result.Query.Predicates[1], "c", "c_custkey") {
		t.Fatalf("outer predicate = %#v, want c.c_custkey", result.Query.Predicates[1])
	}
	if len(result.Query.Projection) != 2 {
		t.Fatalf("projection = %#v, want two columns", result.Query.Projection)
	}
	if result.Query.Projection[0].Alias != "customer_key" || result.Query.Projection[1].Alias != "customer_name" {
		t.Fatalf("projection aliases = %#v, want customer_key/customer_name", result.Query.Projection)
	}
	if !exprReferencesField(result.Query.Projection[0].Expr, "c", "c_custkey") {
		t.Fatalf("projection[0] = %#v, want c.c_custkey", result.Query.Projection[0].Expr)
	}
	if len(result.Query.OrderBy) != 1 || !exprReferencesField(result.Query.OrderBy[0].Expr, "c", "c_custkey") {
		t.Fatalf("order by = %#v, want c.c_custkey", result.Query.OrderBy)
	}
}

func TestPlannerExpandsDerivedTableWithInnerJoin(t *testing.T) {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}

	result := planner.Plan(`
		select order_key, customer_name
		from (
			select o.o_orderkey as order_key, c.c_name as customer_name
			from orders as o
			inner join customer as c on o.o_custkey = c.c_custkey
			where c.c_name = 'Customer#000000001'
		) as q
		where order_key = 7
	`)
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Sources) != 2 {
		t.Fatalf("sources = %#v, want orders + customer", result.Query.Sources)
	}
	if result.Query.Sources[0].Table != "orders" || result.Query.Sources[0].RefName() != "o" {
		t.Fatalf("source[0] = %#v, want orders as o", result.Query.Sources[0])
	}
	if result.Query.Sources[1].Table != "customer" || result.Query.Sources[1].RefName() != "c" {
		t.Fatalf("source[1] = %#v, want customer as c", result.Query.Sources[1])
	}
	if len(result.Query.Joins) != 1 {
		t.Fatalf("joins = %#v, want one join", result.Query.Joins)
	}
	if got, want := result.Query.Joins[0].Left.QualifiedName(), "o.o_custkey"; got != want {
		t.Fatalf("join left = %q, want %q", got, want)
	}
	if got, want := result.Query.Joins[0].Right.QualifiedName(), "c.c_custkey"; got != want {
		t.Fatalf("join right = %q, want %q", got, want)
	}
	if len(result.Query.Predicates) != 2 {
		t.Fatalf("predicates = %d, want derived + outer predicates", len(result.Query.Predicates))
	}
	if !predicateReferencesField(result.Query.Predicates[0], "c", "c_name") {
		t.Fatalf("derived predicate = %#v, want c.c_name", result.Query.Predicates[0])
	}
	if !predicateReferencesField(result.Query.Predicates[1], "o", "o_orderkey") {
		t.Fatalf("outer predicate = %#v, want o.o_orderkey", result.Query.Predicates[1])
	}
}

func TestPlannerExpandsDerivedTableSourceInOuterJoin(t *testing.T) {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}

	result := planner.Plan(`
		select c.c_name, o.order_key
		from customer as c
		inner join (
			select o_orderkey as order_key, o_custkey as customer_key
			from orders
		) as o on o.customer_key = c.c_custkey
		where c.c_custkey = 1
	`)
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Sources) != 2 {
		t.Fatalf("sources = %#v, want customer + orders", result.Query.Sources)
	}
	if result.Query.Sources[0].Table != "customer" || result.Query.Sources[0].RefName() != "c" {
		t.Fatalf("source[0] = %#v, want customer as c", result.Query.Sources[0])
	}
	if result.Query.Sources[1].Table != "orders" || result.Query.Sources[1].RefName() != "o" {
		t.Fatalf("source[1] = %#v, want orders as o", result.Query.Sources[1])
	}
	if len(result.Query.Joins) != 1 {
		t.Fatalf("joins = %#v, want one join", result.Query.Joins)
	}
	if got, want := result.Query.Joins[0].Left.QualifiedName(), "o.o_custkey"; got != want {
		t.Fatalf("join left = %q, want %q", got, want)
	}
	if got, want := result.Query.Joins[0].Right.QualifiedName(), "c.c_custkey"; got != want {
		t.Fatalf("join right = %q, want %q", got, want)
	}
	if !exprReferencesField(result.Query.Projection[1].Expr, "o", "o_orderkey") {
		t.Fatalf("projection = %#v, want o.o_orderkey", result.Query.Projection[1])
	}
}

func TestPlannerExpandsGroupedAggregateDerivedTable(t *testing.T) {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}

	result := planner.Plan(`
		select customer_key, order_count
		from (
			select o_custkey as customer_key, count(*) as order_count
			from orders
			group by o_custkey
		) as s
		where customer_key = 1
		order by customer_key
	`)
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Sources) != 1 || result.Query.Sources[0].Table != "orders" || result.Query.Sources[0].RefName() != "s" {
		t.Fatalf("sources = %#v, want orders as s", result.Query.Sources)
	}
	if len(result.Query.GroupBy) != 1 || !exprReferencesField(result.Query.GroupBy[0], "s", "o_custkey") {
		t.Fatalf("group by = %#v, want s.o_custkey", result.Query.GroupBy)
	}
	if len(result.Query.Aggregates) != 1 || result.Query.Aggregates[0].Function != "count" || result.Query.Aggregates[0].Input != nil {
		t.Fatalf("aggregates = %#v, want count(*)", result.Query.Aggregates)
	}
	ref, ok := result.Query.Projection[1].Expr.(AggregateRefExpr)
	if !ok || ref.Alias != "order_count" || ref.Index != 0 {
		t.Fatalf("projection[1] = %#v, want order_count aggregate ref", result.Query.Projection[1].Expr)
	}
	if len(result.Query.Predicates) != 1 || !predicateReferencesField(result.Query.Predicates[0], "s", "o_custkey") {
		t.Fatalf("predicates = %#v, want s.o_custkey", result.Query.Predicates)
	}
}

func TestPlannerExpandsGroupedAggregateDerivedTableJoinedOnUniqueKey(t *testing.T) {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}

	result := planner.Plan(`
		select c.c_custkey as customer_key, s.order_count
		from customer as c
		inner join (
			select o_custkey, count(*) as order_count
			from orders
			group by o_custkey
		) as s on s.o_custkey = c.c_custkey
		where c.c_custkey in (1, 2, 3)
		order by c.c_custkey
	`)
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Sources) != 2 {
		t.Fatalf("sources = %#v, want customer + orders", result.Query.Sources)
	}
	if result.Query.Sources[0].Table != "customer" || result.Query.Sources[0].RefName() != "c" {
		t.Fatalf("source[0] = %#v, want customer as c", result.Query.Sources[0])
	}
	if result.Query.Sources[1].Table != "orders" || result.Query.Sources[1].RefName() != "s" {
		t.Fatalf("source[1] = %#v, want orders as s", result.Query.Sources[1])
	}
	if len(result.Query.Joins) != 1 {
		t.Fatalf("joins = %#v, want one join", result.Query.Joins)
	}
	if got, want := result.Query.Joins[0].Left.QualifiedName(), "s.o_custkey"; got != want {
		t.Fatalf("join left = %q, want %q", got, want)
	}
	if got, want := result.Query.Joins[0].Right.QualifiedName(), "c.c_custkey"; got != want {
		t.Fatalf("join right = %q, want %q", got, want)
	}
	if len(result.Query.GroupBy) != 2 {
		t.Fatalf("group by = %#v, want derived and outer join keys", result.Query.GroupBy)
	}
	if !exprReferencesField(result.Query.GroupBy[0], "s", "o_custkey") || !exprReferencesField(result.Query.GroupBy[1], "c", "c_custkey") {
		t.Fatalf("group by = %#v, want s.o_custkey then c.c_custkey", result.Query.GroupBy)
	}
	ref, ok := result.Query.Projection[1].Expr.(AggregateRefExpr)
	if !ok || ref.Alias != "order_count" || ref.Index != 0 {
		t.Fatalf("projection[1] = %#v, want order_count aggregate ref", result.Query.Projection[1].Expr)
	}
	if len(result.Query.OrderBy) != 1 || !exprReferencesField(result.Query.OrderBy[0].Expr, "c", "c_custkey") {
		t.Fatalf("order by = %#v, want c.c_custkey", result.Query.OrderBy)
	}
}

func TestPlannerExpandsLogicalViewExpressionProjection(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{{
		Schema: "quanta",
		Name:   "customer_next_keys",
		SQL:    "select c_custkey + 1 as next_customer_key from customer",
	}}
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       catalog,
		DefaultSchema: "quanta",
	}

	result := planner.Plan("select next_customer_key from customer_next_keys where next_customer_key = 2")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Sources) != 1 || result.Query.Sources[0].Table != "customer" {
		t.Fatalf("sources = %#v, want customer", result.Query.Sources)
	}
	if got := result.Query.Sources[0].RefName(); got != "customer_next_keys" {
		t.Fatalf("source ref = %q, want customer_next_keys", got)
	}
	if len(result.Query.Projection) != 1 || result.Query.Projection[0].Alias != "next_customer_key" {
		t.Fatalf("projection = %#v, want alias next_customer_key", result.Query.Projection)
	}
	if _, ok := result.Query.Projection[0].Expr.(BinaryExpr); !ok {
		t.Fatalf("projection expr = %T, want BinaryExpr", result.Query.Projection[0].Expr)
	}
	if !exprReferencesField(result.Query.Projection[0].Expr, "customer_next_keys", "c_custkey") {
		t.Fatalf("projection expr = %#v, want customer_next_keys.c_custkey", result.Query.Projection[0].Expr)
	}
	if len(result.Query.Predicates) != 1 {
		t.Fatalf("predicates = %d, want one outer predicate", len(result.Query.Predicates))
	}
	if !predicateReferencesField(result.Query.Predicates[0], "customer_next_keys", "c_custkey") {
		t.Fatalf("predicate = %#v, want expression over customer_next_keys.c_custkey", result.Query.Predicates[0])
	}
	predicate, ok := result.Query.Predicates[0].Expr.(BinaryExpr)
	if !ok {
		t.Fatalf("predicate expr = %T, want BinaryExpr", result.Query.Predicates[0].Expr)
	}
	if _, ok := predicate.Left.(FieldExpr); !ok {
		t.Fatalf("predicate left = %T, want rewritten FieldExpr", predicate.Left)
	}
}

func TestPlannerExpandsScalarAggregateLogicalView(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{{
		Schema: "quanta",
		Name:   "lineitem_count_view",
		SQL:    "select count(*) as line_count from lineitem",
	}}
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       catalog,
		DefaultSchema: "quanta",
	}

	result := planner.Plan("select line_count from lineitem_count_view")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Sources) != 1 || result.Query.Sources[0].Table != "lineitem" {
		t.Fatalf("sources = %#v, want lineitem", result.Query.Sources)
	}
	if got := result.Query.Sources[0].RefName(); got != "lineitem_count_view" {
		t.Fatalf("source ref = %q, want lineitem_count_view", got)
	}
	if len(result.Query.Aggregates) != 1 {
		t.Fatalf("aggregates = %#v, want one count aggregate", result.Query.Aggregates)
	}
	if result.Query.Aggregates[0].Function != "count" || result.Query.Aggregates[0].Alias != "line_count" {
		t.Fatalf("aggregate = %#v, want count line_count", result.Query.Aggregates[0])
	}
	if len(result.Query.Projection) != 1 || result.Query.Projection[0].Alias != "line_count" {
		t.Fatalf("projection = %#v, want line_count", result.Query.Projection)
	}
	ref, ok := result.Query.Projection[0].Expr.(AggregateRefExpr)
	if !ok || ref.Index != 0 || ref.Alias != "line_count" {
		t.Fatalf("projection expr = %#v, want line_count aggregate ref", result.Query.Projection[0].Expr)
	}
}

func TestPlannerExpandsDistinctLogicalView(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{{
		Schema: "quanta",
		Name:   "customer_segments",
		SQL:    "select distinct c_name as customer_name from customer",
	}}
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       catalog,
		DefaultSchema: "quanta",
	}

	result := planner.Plan("select customer_name from customer_segments order by customer_name limit 1")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if !result.Query.Result.Distinct {
		t.Fatalf("distinct = false, want true")
	}
	if result.Query.Result.Limit != 1 {
		t.Fatalf("limit = %d, want 1", result.Query.Result.Limit)
	}
	if len(result.Query.OrderBy) != 1 {
		t.Fatalf("order by = %#v, want one sort", result.Query.OrderBy)
	}
	if len(result.Query.Sources) != 1 || result.Query.Sources[0].Table != "customer" {
		t.Fatalf("sources = %#v, want customer", result.Query.Sources)
	}
	if got := result.Query.Sources[0].RefName(); got != "customer_segments" {
		t.Fatalf("source ref = %q, want customer_segments", got)
	}
	if len(result.Query.Projection) != 1 || result.Query.Projection[0].Alias != "customer_name" {
		t.Fatalf("projection = %#v, want customer_name", result.Query.Projection)
	}
	if !exprReferencesField(result.Query.Projection[0].Expr, "customer_segments", "c_name") {
		t.Fatalf("projection expr = %#v, want customer_segments.c_name", result.Query.Projection[0].Expr)
	}
}

func TestPlannerExpandsLogicalViewWithInnerJoin(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{{
		Schema: "quanta",
		Name:   "customer_orders",
		SQL:    "select o.o_orderkey as order_key, c.c_name as customer_name from orders as o inner join customer as c on o.o_custkey = c.c_custkey where c.c_name = 'Customer#000000001'",
	}}
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       catalog,
		DefaultSchema: "quanta",
	}

	result := planner.Plan("select order_key, customer_name from customer_orders where order_key = 7")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Sources) != 2 {
		t.Fatalf("sources = %#v, want orders + customer", result.Query.Sources)
	}
	if result.Query.Sources[0].Table != "orders" || result.Query.Sources[0].RefName() != "o" {
		t.Fatalf("source[0] = %#v, want orders as o", result.Query.Sources[0])
	}
	if result.Query.Sources[1].Table != "customer" || result.Query.Sources[1].RefName() != "c" {
		t.Fatalf("source[1] = %#v, want customer as c", result.Query.Sources[1])
	}
	if len(result.Query.Joins) != 1 {
		t.Fatalf("joins = %#v, want one inner join", result.Query.Joins)
	}
	if got, want := result.Query.Joins[0].Left.QualifiedName(), "o.o_custkey"; got != want {
		t.Fatalf("join left = %q, want %q", got, want)
	}
	if got, want := result.Query.Joins[0].Right.QualifiedName(), "c.c_custkey"; got != want {
		t.Fatalf("join right = %q, want %q", got, want)
	}
	if len(result.Query.Predicates) != 2 {
		t.Fatalf("predicates = %d, want view + outer predicates", len(result.Query.Predicates))
	}
	if !predicateReferencesField(result.Query.Predicates[0], "c", "c_name") {
		t.Fatalf("view predicate = %#v, want c.c_name", result.Query.Predicates[0])
	}
	if !predicateReferencesField(result.Query.Predicates[1], "o", "o_orderkey") {
		t.Fatalf("outer predicate = %#v, want o.o_orderkey", result.Query.Predicates[1])
	}
	if len(result.Query.Projection) != 2 {
		t.Fatalf("projection = %#v, want two columns", result.Query.Projection)
	}
	if result.Query.Projection[0].Alias != "order_key" || result.Query.Projection[1].Alias != "customer_name" {
		t.Fatalf("projection aliases = %#v, want order_key/customer_name", result.Query.Projection)
	}
}

func TestPlannerExpandsLogicalViewSourceInOuterJoin(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{{
		Schema: "quanta",
		Name:   "customer_source",
		SQL:    "select c_custkey as customer_key, c_name as customer_name from customer",
	}}
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       catalog,
		DefaultSchema: "quanta",
	}

	result := planner.Plan("select v.customer_key, o.o_orderkey from customer_source as v inner join orders as o on o.o_custkey = v.customer_key where o.o_orderkey = 1")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Sources) != 2 {
		t.Fatalf("sources = %#v, want customer + orders", result.Query.Sources)
	}
	if result.Query.Sources[0].Table != "customer" || result.Query.Sources[0].RefName() != "v" {
		t.Fatalf("source[0] = %#v, want customer as v", result.Query.Sources[0])
	}
	if result.Query.Sources[1].Table != "orders" || result.Query.Sources[1].RefName() != "o" {
		t.Fatalf("source[1] = %#v, want orders as o", result.Query.Sources[1])
	}
	if len(result.Query.Joins) != 1 {
		t.Fatalf("joins = %#v, want one outer join", result.Query.Joins)
	}
	if got, want := result.Query.Joins[0].Left.QualifiedName(), "o.o_custkey"; got != want {
		t.Fatalf("join left = %q, want %q", got, want)
	}
	if got, want := result.Query.Joins[0].Right.QualifiedName(), "v.c_custkey"; got != want {
		t.Fatalf("join right = %q, want %q", got, want)
	}
	if len(result.Query.Projection) != 2 {
		t.Fatalf("projection = %#v, want two columns", result.Query.Projection)
	}
	if result.Query.Projection[0].Alias != "customer_key" {
		t.Fatalf("projection[0] alias = %q, want customer_key", result.Query.Projection[0].Alias)
	}
	if len(result.Query.Predicates) != 1 {
		t.Fatalf("predicates = %#v, want one orders filter", result.Query.Predicates)
	}
	if !predicateReferencesField(result.Query.Predicates[0], "o", "o_orderkey") {
		t.Fatalf("predicate = %#v, want orders filter", result.Query.Predicates[0])
	}
}

func TestPlannerExpandsJoinBetweenTwoLogicalViewSources(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{
		{
			Schema: "quanta",
			Name:   "customer_keys",
			SQL:    "select c_custkey as customer_key from customer",
		},
		{
			Schema: "quanta",
			Name:   "order_keys",
			SQL:    "select o_orderkey as order_key, o_custkey as customer_key from orders",
		},
	}
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       catalog,
		DefaultSchema: "quanta",
	}

	result := planner.Plan("select l.customer_key, r.order_key from customer_keys as l inner join order_keys as r on r.customer_key = l.customer_key where r.order_key = 1")
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Sources) != 2 {
		t.Fatalf("sources = %#v, want customer + orders", result.Query.Sources)
	}
	if result.Query.Sources[0].Table != "customer" || result.Query.Sources[0].RefName() != "l" {
		t.Fatalf("source[0] = %#v, want customer as l", result.Query.Sources[0])
	}
	if result.Query.Sources[1].Table != "orders" || result.Query.Sources[1].RefName() != "r" {
		t.Fatalf("source[1] = %#v, want orders as r", result.Query.Sources[1])
	}
	if len(result.Query.Joins) != 1 {
		t.Fatalf("joins = %#v, want one logical-view join", result.Query.Joins)
	}
	if got, want := result.Query.Joins[0].Left.QualifiedName(), "r.o_custkey"; got != want {
		t.Fatalf("join left = %q, want %q", got, want)
	}
	if got, want := result.Query.Joins[0].Right.QualifiedName(), "l.c_custkey"; got != want {
		t.Fatalf("join right = %q, want %q", got, want)
	}
	if len(result.Query.Projection) != 2 {
		t.Fatalf("projection = %#v, want two columns", result.Query.Projection)
	}
	if result.Query.Projection[0].Alias != "customer_key" || !exprReferencesField(result.Query.Projection[0].Expr, "l", "c_custkey") {
		t.Fatalf("projection[0] = %#v, want l.c_custkey as customer_key", result.Query.Projection[0])
	}
	if result.Query.Projection[1].Alias != "order_key" || !exprReferencesField(result.Query.Projection[1].Expr, "r", "o_orderkey") {
		t.Fatalf("projection[1] = %#v, want r.o_orderkey as order_key", result.Query.Projection[1])
	}
	if len(result.Query.Predicates) != 1 {
		t.Fatalf("predicates = %#v, want one orders filter", result.Query.Predicates)
	}
	if !predicateReferencesField(result.Query.Predicates[0], "r", "o_orderkey") {
		t.Fatalf("predicate = %#v, want r.o_orderkey filter", result.Query.Predicates[0])
	}
}

func TestPlannerExpandsLogicalViewSourceInMembershipSubquery(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{{
		Schema: "quanta",
		Name:   "customer_key_view",
		SQL:    "select c_custkey as customer_key from customer",
	}}
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       catalog,
		DefaultSchema: "quanta",
	}

	result := planner.Plan(`
		select count(*) as customer_count
		from customer
		where c_custkey in (
			select customer_key
			from customer_key_view
			where customer_key = 1
		)
	`)
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Memberships) != 1 {
		t.Fatalf("memberships = %#v, want one membership edge", result.Query.Memberships)
	}
	membership := result.Query.Memberships[0]
	if got, want := membership.Left.QualifiedName(), "customer.c_custkey"; got != want {
		t.Fatalf("membership left = %q, want %q", got, want)
	}
	if got, want := membership.Right.QualifiedName(), "customer_key_view.c_custkey"; got != want {
		t.Fatalf("membership right = %q, want %q", got, want)
	}
	if len(membership.Predicates) != 1 {
		t.Fatalf("membership predicates = %#v, want one subquery predicate", membership.Predicates)
	}
	if !predicateReferencesField(membership.Predicates[0], "customer_key_view", "c_custkey") {
		t.Fatalf("membership predicate = %#v, want customer_key_view.c_custkey", membership.Predicates[0])
	}
}

func TestPlannerRejectsUnprojectedLogicalViewColumn(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{{
		Schema: "quanta",
		Name:   "order_keys",
		SQL:    "select o_orderkey from orders",
	}}
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       catalog,
		DefaultSchema: "quanta",
	}

	result := planner.Plan("select o_totalprice from order_keys")
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics should block native planning")
	}
	if got := result.Diagnostics.Codes()[0]; got != DiagnosticCatalogFieldNotFound {
		t.Fatalf("diagnostic = %q, want %q", got, DiagnosticCatalogFieldNotFound)
	}
}

func TestPlannerExpandsLogicalViewTextSearchPredicate(t *testing.T) {
	catalog := testBindCatalog()
	catalog.Views = []SQLViewDefinition{{
		Schema: "quanta",
		Name:   "customer_text_search",
		SQL:    "select c_custkey as customer_key, c_name as customer_name from customer",
	}}
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       catalog,
		DefaultSchema: "quanta",
	}

	result := planner.Plan(`
		select customer_key, customer_name
		from customer_text_search
		where match(customer_name) against ('Customer#000000001')
	`)
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Predicates) != 1 {
		t.Fatalf("predicates = %#v, want one text-search predicate", result.Query.Predicates)
	}
	search := assertTextSearchPredicate(t, result.Query.Predicates[0], "customer_text_search")
	if search.Field.Name != "c_name" {
		t.Fatalf("search field = %#v, want base c_name", search.Field)
	}
}

func TestPlannerBindsDerivedTableTextSearchPredicate(t *testing.T) {
	planner := Planner{
		Parser:        SimpleParserBridge{},
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
	}

	result := planner.Plan(`
		select customer_key, customer_name
		from (
			select
				c_custkey as customer_key,
				c_name as customer_name
			from customer
		) as c
		where match(customer_name) against ('Customer#000000001')
	`)
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v", result.Diagnostics)
	}
	if len(result.Query.Predicates) != 1 {
		t.Fatalf("predicates = %#v, want one text-search predicate", result.Query.Predicates)
	}
	search := assertTextSearchPredicate(t, result.Query.Predicates[0], "c")
	if search.Field.Name != "c_name" {
		t.Fatalf("search field = %#v, want base c_name", search.Field)
	}
}

func assertTextSearchPredicate(t *testing.T, predicate Predicate, tableRef string) TextSearchExpr {
	t.Helper()
	search, ok := AsTextSearchExpr(predicate.Expr)
	if !ok {
		t.Fatalf("predicate expression = %T, want TextSearchExpr", predicate.Expr)
	}
	if predicate.Placement != PredicatePushdown {
		t.Fatalf("predicate placement = %q, want pushdown", predicate.Placement)
	}
	if search.Field.Table.RefName() != tableRef {
		t.Fatalf("search table ref = %q, want %q", search.Field.Table.RefName(), tableRef)
	}
	if search.Field.Type != DataTypeString || !search.Field.Encoding.Searchable() {
		t.Fatalf("search field metadata = %#v, want searchable string", search.Field)
	}
	return search
}

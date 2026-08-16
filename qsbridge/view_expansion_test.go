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

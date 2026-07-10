package qsbridge

import "testing"

func TestExpressionKinds(t *testing.T) {
	table := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	field := Field(FieldRef{Table: table, Name: "c_acctbal", Index: IndexBSI})

	tests := []struct {
		name string
		expr Expr
		want ExprKind
	}{
		{name: "literal", expr: Literal(ValueInt, 10), want: ExprLiteral},
		{name: "field", expr: field, want: ExprField},
		{name: "parameter", expr: Parameter(1, DataTypeInt), want: ExprParameter},
		{name: "list", expr: List(Literal(ValueInt, 10), Parameter(1, DataTypeInt)), want: ExprList},
		{name: "call", expr: Call("abs", field), want: ExprCall},
		{name: "binary", expr: Binary(BinaryOpGreater, field, Literal(ValueInt, 0)), want: ExprBinary},
		{name: "aggregate ref", expr: AggregateRef("total", 0), want: ExprAggregateRef},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.expr.ExpressionKind(); got != tt.want {
				t.Fatalf("ExpressionKind() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParameterRefsReturnsUniqueParametersInFirstSeenOrder(t *testing.T) {
	first := Parameter(1, DataTypeInt)
	second := ParameterExpr{Ref: ParameterRef{Index: 2, Name: "city", Type: DataTypeString, Nullable: true}}
	expr := Binary(
		BinaryOpAnd,
		Binary(BinaryOpGreater, first, Literal(ValueInt, 10)),
		Binary(BinaryOpIn, Call("lower", second, second), List(Literal(ValueString, "seattle"), first)),
	)

	refs := ParameterRefs(expr)
	if len(refs) != 2 {
		t.Fatalf("ParameterRefs() returned %d refs, want 2", len(refs))
	}
	if refs[0].Index != 1 || refs[0].Type != DataTypeInt {
		t.Fatalf("refs[0] = %#v, want int parameter 1", refs[0])
	}
	if refs[1].Name != "city" || refs[1].Type != DataTypeString {
		t.Fatalf("refs[1] = %#v, want named city string parameter", refs[1])
	}
}

func TestCallCopiesArguments(t *testing.T) {
	table := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderDate := Field(FieldRef{Table: table, Name: "o_orderdate", Index: IndexDateTime})
	args := []Expr{orderDate}

	call := Call("year", args...)
	args[0] = Literal(ValueString, "changed")

	refs := FieldRefs(call)
	if len(refs) != 1 {
		t.Fatalf("expected copied call args to keep one field ref, got %d", len(refs))
	}
	if got, want := refs[0].QualifiedName(), "o.o_orderdate"; got != want {
		t.Fatalf("field ref = %q, want %q", got, want)
	}
}

func TestFunctionCallCarriesCatalogMetadata(t *testing.T) {
	table := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipmode := Field(FieldRef{Table: table, Name: "l_shipmode", Type: DataTypeString})
	function := FunctionDefinition{
		Name:          "topn",
		Kind:          FunctionAggregate,
		Origin:        FunctionOriginQuantaCustom,
		ReturnType:    DataTypeString,
		Deterministic: true,
	}

	call := FunctionCall(function, shipmode)
	if call.Name != "topn" || call.Type != DataTypeString || call.Origin != FunctionOriginQuantaCustom || call.Placement != FunctionPlacementAggregate || !call.Deterministic {
		t.Fatalf("call = %#v, want resolved function metadata", call)
	}
}

func TestFieldRefsReturnsUniqueFieldsInFirstSeenOrder(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Index: IndexBSI}
	lineOrderKey := FieldRef{Table: lineitem, Name: "l_orderkey", Index: IndexBSI}
	quantity := FieldRef{Table: lineitem, Name: "l_quantity", Index: IndexBSI}

	expr := Binary(
		BinaryOpAnd,
		Binary(BinaryOpEqual, Field(orderKey), Field(lineOrderKey)),
		Binary(BinaryOpGreater, Call("abs", Field(quantity), Field(quantity)), Literal(ValueInt, 10)),
	)

	refs := FieldRefs(expr)
	if len(refs) != 3 {
		t.Fatalf("FieldRefs() returned %d refs, want 3", len(refs))
	}
	if got, want := refs[0].QualifiedName(), "o.o_orderkey"; got != want {
		t.Fatalf("refs[0] = %q, want %q", got, want)
	}
	if got, want := refs[1].QualifiedName(), "l.l_orderkey"; got != want {
		t.Fatalf("refs[1] = %q, want %q", got, want)
	}
	if got, want := refs[2].QualifiedName(), "l.l_quantity"; got != want {
		t.Fatalf("refs[2] = %q, want %q", got, want)
	}
}

func TestFieldRefsIgnoresAggregateRefInputs(t *testing.T) {
	expr := Binary(BinaryOpGreater, AggregateRef("revenue", 0), Literal(ValueFloat, 100.0))

	if refs := FieldRefs(expr); len(refs) != 0 {
		t.Fatalf("aggregate refs should not expose original input fields, got %d refs", len(refs))
	}
}

package qsruntime

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestRelationshipTupleRowSetFiltersSiblingResidualEquality(t *testing.T) {
	rowSet := NewRelationshipTupleRowSet("p", []qsbridge.QuantaRownum{1, 2})
	rowSet = rowSet.Expand(RelationshipTupleExpansion{
		ParentRole: "p",
		ChildRole:  "l",
		ChildRowsByParent: map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
			1: {11, 12},
			2: {21},
		},
	})
	rowSet = rowSet.Expand(RelationshipTupleExpansion{
		ParentRole: "p",
		ChildRole:  "ps",
		ChildRowsByParent: map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
			1: {101, 102},
			2: {201},
		},
	})

	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	lSuppkey := qsbridge.FieldRef{Table: lineitem, Name: "l_suppkey", Type: qsbridge.DataTypeInt}
	psSuppkey := qsbridge.FieldRef{Table: partsupp, Name: "ps_suppkey", Type: qsbridge.DataTypeInt}
	fields := []qsbridge.QuantaProjectionField{
		{Index: "lineitem", Role: "l", Field: "l_suppkey", Type: qsbridge.DataTypeInt},
		{Index: "partsupp", Role: "ps", Field: "ps_suppkey", Type: qsbridge.DataTypeInt},
	}
	values := RelationshipTupleValueStore{
		RelationshipTupleValueKeyForField(fields[0]): {
			11: {Kind: qsbridge.ValueInt, Value: int64(7)},
			12: {Kind: qsbridge.ValueInt, Value: int64(8)},
			21: {Kind: qsbridge.ValueInt, Value: int64(9)},
		},
		RelationshipTupleValueKeyForField(fields[1]): {
			101: {Kind: qsbridge.ValueInt, Value: int64(7)},
			102: {Kind: qsbridge.ValueInt, Value: int64(99)},
			201: {Kind: qsbridge.ValueInt, Value: int64(9)},
		},
	}
	filtered, diagnostics := FilterRelationshipTupleResidualPredicates(rowSet,
		"lineitem",
		fields,
		values,
		[]qsbridge.Predicate{{
			Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(psSuppkey), qsbridge.Field(lSuppkey)),
			Placement: qsbridge.PredicateResidualJoin,
			Scope:     qsbridge.PredicateScopeOn,
		}},
	)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	assertRelationshipTupleRows(t, filtered, []map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{
		{"p": 1, "l": 11, "ps": 101},
		{"p": 2, "l": 21, "ps": 201},
	})
}

func TestRelationshipTupleRowSetProbesQ9TupleInspection(t *testing.T) {
	expanded := NewRelationshipTupleRowSet("p", []qsbridge.QuantaRownum{1, 2})
	expanded = expanded.Expand(RelationshipTupleExpansion{
		ParentRole: "p",
		ChildRole:  "l",
		ChildRowsByParent: map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
			1: {11, 12},
			2: {21},
		},
	})
	expanded = expanded.Expand(RelationshipTupleExpansion{
		ParentRole: "p",
		ChildRole:  "ps",
		ChildRowsByParent: map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
			1: {101, 102},
			2: {201},
		},
	})
	filtered := RelationshipTupleRowSet{Rows: []RelationshipTupleRow{expanded.Rows[0], expanded.Rows[4]}}
	materializedFields := []qsbridge.QuantaProjectionField{
		{Index: "lineitem", Role: "l", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat},
		{Index: "lineitem", Role: "l", Field: "l_discount", Type: qsbridge.DataTypeFloat},
		{Index: "lineitem", Role: "l", Field: "l_quantity", Type: qsbridge.DataTypeInt},
		{Index: "partsupp", Role: "ps", Field: "ps_supplycost", Type: qsbridge.DataTypeFloat},
	}
	probes := RelationshipTupleProbes(RelationshipTupleProbeSnapshot{
		Expanded:            expanded,
		Filtered:            filtered,
		MaterializedFields:  materializedFields,
		AggregateExpression: "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity",
		AggregateAlias:      "profit",
	})

	assertExecutionProbe(t, probes, "relationship_tuple", "roles", "l,p,ps")
	assertExecutionProbe(t, probes, "relationship_tuple", "expanded_rows", "5")
	assertExecutionProbe(t, probes, "relationship_tuple", "filtered_rows", "2")
	assertExecutionProbe(t, probes, "relationship_tuple", "materialized_fields", "l.l_discount,l.l_extendedprice,l.l_quantity,ps.ps_supplycost")
	assertExecutionProbe(t, probes, "relationship_tuple", "aggregate_expression", "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity")
	assertExecutionProbe(t, probes, "relationship_tuple", "aggregate_alias", "profit")
}

func TestRelationshipTupleRowSetFiltersQ9SiblingResidualAndAggregatesProfit(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	lSuppkey := qsbridge.FieldRef{Table: lineitem, Name: "l_suppkey", Type: qsbridge.DataTypeInt}
	psSuppkey := qsbridge.FieldRef{Table: partsupp, Name: "ps_suppkey", Type: qsbridge.DataTypeInt}
	extendedPrice := qsbridge.FieldRef{Table: lineitem, Name: "l_extendedprice", Type: qsbridge.DataTypeFloat}
	discount := qsbridge.FieldRef{Table: lineitem, Name: "l_discount", Type: qsbridge.DataTypeFloat}
	quantity := qsbridge.FieldRef{Table: lineitem, Name: "l_quantity", Type: qsbridge.DataTypeInt}
	supplyCost := qsbridge.FieldRef{Table: partsupp, Name: "ps_supplycost", Type: qsbridge.DataTypeFloat}
	_ = part
	rowSet := NewRelationshipTupleRowSetFromRootExpansions("p", []qsbridge.QuantaRownum{1, 2}, []RelationshipTupleExpansion{
		{
			ParentRole: "p",
			ChildRole:  "l",
			ChildRowsByParent: map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
				1: {11, 12},
				2: {21},
			},
		},
		{
			ParentRole: "p",
			ChildRole:  "ps",
			ChildRowsByParent: map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
				1: {101, 102},
				2: {201},
			},
		},
	})
	fields := []qsbridge.QuantaProjectionField{
		{Index: "lineitem", Role: "l", Field: "l_suppkey", Type: qsbridge.DataTypeInt},
		{Index: "partsupp", Role: "ps", Field: "ps_suppkey", Type: qsbridge.DataTypeInt},
		{Index: "lineitem", Role: "l", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat},
		{Index: "lineitem", Role: "l", Field: "l_discount", Type: qsbridge.DataTypeFloat},
		{Index: "lineitem", Role: "l", Field: "l_quantity", Type: qsbridge.DataTypeInt},
		{Index: "partsupp", Role: "ps", Field: "ps_supplycost", Type: qsbridge.DataTypeFloat},
	}
	values := RelationshipTupleValueStore{
		RelationshipTupleValueKeyForField(fields[0]): {
			11: {Kind: qsbridge.ValueInt, Value: int64(7)},
			12: {Kind: qsbridge.ValueInt, Value: int64(8)},
			21: {Kind: qsbridge.ValueInt, Value: int64(9)},
		},
		RelationshipTupleValueKeyForField(fields[1]): {
			101: {Kind: qsbridge.ValueInt, Value: int64(7)},
			102: {Kind: qsbridge.ValueInt, Value: int64(99)},
			201: {Kind: qsbridge.ValueInt, Value: int64(9)},
		},
		RelationshipTupleValueKeyForField(fields[2]): {
			11: {Kind: qsbridge.ValueFloat, Value: float64(1000)},
			12: {Kind: qsbridge.ValueFloat, Value: float64(300)},
			21: {Kind: qsbridge.ValueFloat, Value: float64(500)},
		},
		RelationshipTupleValueKeyForField(fields[3]): {
			11: {Kind: qsbridge.ValueFloat, Value: float64(0.10)},
			12: {Kind: qsbridge.ValueFloat, Value: float64(0.05)},
			21: {Kind: qsbridge.ValueFloat, Value: float64(0.20)},
		},
		RelationshipTupleValueKeyForField(fields[4]): {
			11: {Kind: qsbridge.ValueInt, Value: int64(5)},
			12: {Kind: qsbridge.ValueInt, Value: int64(3)},
			21: {Kind: qsbridge.ValueInt, Value: int64(10)},
		},
		RelationshipTupleValueKeyForField(fields[5]): {
			101: {Kind: qsbridge.ValueFloat, Value: float64(20)},
			102: {Kind: qsbridge.ValueFloat, Value: float64(40)},
			201: {Kind: qsbridge.ValueFloat, Value: float64(15)},
		},
	}
	projected, diagnostics := rowSet.ToProjectedRowSet("lineitem", fields, values)
	if diagnostics.BlocksNative() {
		t.Fatalf("project diagnostics = %#v, want none", diagnostics)
	}
	residualRequest := ExecutionRequest{Joins: []qsbridge.JoinEdge{{On: []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(psSuppkey), qsbridge.Field(lSuppkey)),
		Placement: qsbridge.PredicateResidualJoin,
		Scope:     qsbridge.PredicateScopeOn,
	}}}}}
	filteredProjected, filteredRows, diagnostics := FilterRelationshipTupleProjectedResiduals(rowSet, residualRequest, projected)
	if diagnostics.BlocksNative() {
		t.Fatalf("residual diagnostics = %#v, want none", diagnostics)
	}
	assertRelationshipTupleRows(t, filteredRows, []map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{
		{"p": 1, "l": 11, "ps": 101},
		{"p": 2, "l": 21, "ps": 201},
	})
	profitExpr := qsbridge.Binary(
		qsbridge.BinaryOpSubtract,
		qsbridge.Binary(
			qsbridge.BinaryOpMultiply,
			qsbridge.Field(extendedPrice),
			qsbridge.Binary(qsbridge.BinaryOpSubtract, qsbridge.Literal(qsbridge.ValueInt, int64(1)), qsbridge.Field(discount)),
		),
		qsbridge.Binary(qsbridge.BinaryOpMultiply, qsbridge.Field(supplyCost), qsbridge.Field(quantity)),
	)
	aggregateRequest := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Fragments: []qsbridge.QuantaQueryFragment{{Index: "lineitem"}}})
	aggregateRequest.SQLAggregates = []qsbridge.Aggregate{{
		Function: "sum",
		Input:    profitExpr,
		Alias:    "profit",
		Type:     qsbridge.DataTypeFloat,
	}}
	result := AggregateRelationshipTupleProjected(filteredRows, aggregateRequest, filteredProjected, ExecutionResult{})
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("aggregate diagnostics = %#v, want none", result.Diagnostics)
	}
	chunk, diagnostics := result.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		t.Fatalf("chunk diagnostics = %#v, want none", diagnostics)
	}
	if got := chunk.Rows[0][0].Value; got != float64(1050) {
		t.Fatalf("profit = %#v, want 1050", got)
	}
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "roles", "l,p,ps")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "expanded_rows", "2")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "filtered_rows", "2")
	assertExecutionProbe(t, result.Probes, "relationship_tuple", "aggregate_alias", "profit")
}

func TestRelationshipTupleRowSetEvaluatesQ9ProfitAggregate(t *testing.T) {
	rowSet := RelationshipTupleRowSet{Rows: []RelationshipTupleRow{
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"l": 11, "ps": 101}},
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"l": 21, "ps": 201}},
	}}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	partsupp := qsbridge.TableInstance{Table: "partsupp", Alias: "ps"}
	extendedPrice := qsbridge.FieldRef{Table: lineitem, Name: "l_extendedprice", Type: qsbridge.DataTypeFloat}
	discount := qsbridge.FieldRef{Table: lineitem, Name: "l_discount", Type: qsbridge.DataTypeFloat}
	quantity := qsbridge.FieldRef{Table: lineitem, Name: "l_quantity", Type: qsbridge.DataTypeInt}
	supplyCost := qsbridge.FieldRef{Table: partsupp, Name: "ps_supplycost", Type: qsbridge.DataTypeFloat}
	fields := []qsbridge.QuantaProjectionField{
		{Index: "lineitem", Role: "l", Field: "l_extendedprice", Type: qsbridge.DataTypeFloat},
		{Index: "lineitem", Role: "l", Field: "l_discount", Type: qsbridge.DataTypeFloat},
		{Index: "lineitem", Role: "l", Field: "l_quantity", Type: qsbridge.DataTypeInt},
		{Index: "partsupp", Role: "ps", Field: "ps_supplycost", Type: qsbridge.DataTypeFloat},
	}
	values := RelationshipTupleValueStore{
		RelationshipTupleValueKeyForField(fields[0]): {
			11: {Kind: qsbridge.ValueFloat, Value: float64(1000)},
			21: {Kind: qsbridge.ValueFloat, Value: float64(500)},
		},
		RelationshipTupleValueKeyForField(fields[1]): {
			11: {Kind: qsbridge.ValueFloat, Value: float64(0.10)},
			21: {Kind: qsbridge.ValueFloat, Value: float64(0.20)},
		},
		RelationshipTupleValueKeyForField(fields[2]): {
			11: {Kind: qsbridge.ValueInt, Value: int64(5)},
			21: {Kind: qsbridge.ValueInt, Value: int64(10)},
		},
		RelationshipTupleValueKeyForField(fields[3]): {
			101: {Kind: qsbridge.ValueFloat, Value: float64(20)},
			201: {Kind: qsbridge.ValueFloat, Value: float64(15)},
		},
	}
	projected, diagnostics := rowSet.ToProjectedRowSet("lineitem", fields, values)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	profitExpr := qsbridge.Binary(
		qsbridge.BinaryOpSubtract,
		qsbridge.Binary(
			qsbridge.BinaryOpMultiply,
			qsbridge.Field(extendedPrice),
			qsbridge.Binary(qsbridge.BinaryOpSubtract, qsbridge.Literal(qsbridge.ValueInt, int64(1)), qsbridge.Field(discount)),
		),
		qsbridge.Binary(qsbridge.BinaryOpMultiply, qsbridge.Field(supplyCost), qsbridge.Field(quantity)),
	)
	firstProfit, diagnostics := directBitmapEvaluateMaterializedExpr(profitExpr, projected, 0)
	if diagnostics.BlocksNative() {
		t.Fatalf("first profit diagnostics = %#v, want none", diagnostics)
	}
	if got := firstProfit.Value; got != float64(800) {
		t.Fatalf("first profit = %#v, want 800", got)
	}
	sum, diagnostics := directBitmapMaterializedAggregateCell(qsbridge.Aggregate{
		Function: "sum",
		Input:    profitExpr,
		Alias:    "profit",
		Type:     qsbridge.DataTypeFloat,
	}, projected)
	if diagnostics.BlocksNative() {
		t.Fatalf("sum diagnostics = %#v, want none", diagnostics)
	}
	if got := sum.Value; got != float64(1050) {
		t.Fatalf("profit sum = %#v, want 1050", got)
	}
}

func TestRelationshipTupleRowSetFiltersProjectedResidualsWithAlignedTupleRows(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	linenumber := qsbridge.FieldRef{Table: lineitem, Name: "l_linenumber", Type: qsbridge.DataTypeInt}
	rowSet := RelationshipTupleRowSet{Rows: []RelationshipTupleRow{
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"l": 11}},
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"l": 12}},
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"l": 13}},
	}}
	projected := qsbridge.QuantaProjectedRowSet{
		Index:   "lineitem",
		Rownums: []qsbridge.QuantaRownum{11, 12, 13},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{{
			Field: qsbridge.QuantaProjectionField{Index: "lineitem", Role: "l", Field: "l_linenumber", Type: qsbridge.DataTypeInt},
			Values: []qsbridge.ResultCell{
				{Kind: qsbridge.ValueInt, Value: int64(1)},
				{Kind: qsbridge.ValueInt, Value: int64(0)},
				{Kind: qsbridge.ValueInt, Value: int64(1)},
			},
		}},
	}
	request := ExecutionRequest{Predicates: []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(linenumber), qsbridge.Literal(qsbridge.ValueInt, int64(1))),
		Placement: qsbridge.PredicateResidualScan,
		Scope:     qsbridge.PredicateScopeWhere,
	}}}
	filteredProjected, filteredRows, diagnostics := FilterRelationshipTupleProjectedResiduals(rowSet, request, projected)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if filteredProjected.CandidateCount() != 2 {
		t.Fatalf("projected rows = %d, want 2", filteredProjected.CandidateCount())
	}
	assertRelationshipTupleRows(t, filteredRows, []map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{
		{"l": 11},
		{"l": 13},
	})
}

func TestRelationshipTupleRowSetFiltersNativeCorrelatedAggregatePredicates(t *testing.T) {
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	partKey := qsbridge.FieldRef{Table: part, Name: "p_partkey", Type: qsbridge.DataTypeInt}
	quantity := qsbridge.FieldRef{Table: lineitem, Name: "l_quantity", Type: qsbridge.DataTypeInt}
	rowSet := RelationshipTupleRowSet{Rows: []RelationshipTupleRow{
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"p": 101, "l": 11}},
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"p": 202, "l": 21}},
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"p": 101, "l": 12}},
	}}
	projected := qsbridge.QuantaProjectedRowSet{
		Index:   "lineitem",
		Rownums: []qsbridge.QuantaRownum{11, 21, 12},
		ProjectionVectors: []qsbridge.QuantaProjectionVector{
			{
				Field: qsbridge.QuantaProjectionField{Index: "part", Role: "p", Field: "p_partkey", Type: qsbridge.DataTypeInt},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(101)},
					{Kind: qsbridge.ValueInt, Value: int64(202)},
					{Kind: qsbridge.ValueInt, Value: int64(101)},
				},
			},
			{
				Field: qsbridge.QuantaProjectionField{Index: "lineitem", Role: "l", Field: "l_quantity", Type: qsbridge.DataTypeInt},
				Values: []qsbridge.ResultCell{
					{Kind: qsbridge.ValueInt, Value: int64(9)},
					{Kind: qsbridge.ValueInt, Value: int64(15)},
					{Kind: qsbridge.ValueInt, Value: int64(11)},
				},
			},
		},
	}
	request := ExecutionRequest{NativePredicates: NativePredicateSet{CorrelatedAggregate: []CorrelatedAggregatePredicate{{
		KeyField:   partKey,
		ValueField: quantity,
		Operator:   qsbridge.BinaryOpLess,
		Thresholds: []CorrelatedAggregateThreshold{
			{Key: 101, Threshold: 10},
			{Key: 202, Threshold: 20},
		},
	}}}}
	filteredProjected, filteredRows, diagnostics := FilterRelationshipTupleProjectedResiduals(rowSet, request, projected)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if filteredProjected.CandidateCount() != 2 {
		t.Fatalf("projected rows = %d, want 2", filteredProjected.CandidateCount())
	}
	assertRelationshipTupleRows(t, filteredRows, []map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{
		{"p": 101, "l": 11},
		{"p": 202, "l": 21},
	})
}

func assertRelationshipTupleRows(t *testing.T, rowSet RelationshipTupleRowSet, want []map[qsbridge.TableInstanceID]qsbridge.QuantaRownum) {
	t.Helper()
	if len(rowSet.Rows) != len(want) {
		t.Fatalf("rows = %#v, want %#v", rowSet.Rows, want)
	}
	for i, expected := range want {
		for role, expectedRownum := range expected {
			got, ok := rowSet.Rows[i].Rownum(role)
			if !ok || got != expectedRownum {
				t.Fatalf("row %d role %s = %d/%t, want %d", i, role, got, ok, expectedRownum)
			}
		}
		if len(rowSet.Rows[i].Rownums) != len(expected) {
			t.Fatalf("row %d = %#v, want %#v", i, rowSet.Rows[i].Rownums, expected)
		}
	}
}

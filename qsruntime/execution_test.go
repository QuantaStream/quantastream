package qsruntime

import (
	"math/big"
	"reflect"
	"testing"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestNewExecutionRequestClonesNeutralQuery(t *testing.T) {
	value := big.NewInt(8)
	query := qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "orders",
			Field:     "o_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpGE,
			Value:     value,
		}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterLeaf,
			Fragment: qsbridge.QuantaQueryFragment{
				Index:     "orders",
				Field:     "o_custkey",
				Operation: qsbridge.QuantaOperationIntersect,
				BSIOp:     qsbridge.QuantaBSIOpEQ,
				Value:     big.NewInt(501),
			},
		},
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index:   "orders",
			Field:   "o_orderkey",
			Type:    qsbridge.DataTypeInt,
			Visible: true,
		}},
	}

	request := NewExecutionRequest(query)
	value.SetInt64(9)
	query.Filter.Fragment.Value.SetInt64(502)
	query.ProjectionFields[0].Field = "mutated"

	if got := request.FragmentCount(); got != 1 {
		t.Fatalf("fragment count = %d, want 1", got)
	}
	if got := request.ProjectionCount(); got != 1 {
		t.Fatalf("projection count = %d, want 1", got)
	}
	if got := request.Query.Fragments[0].Value.Int64(); got != 8 {
		t.Fatalf("request value = %d after source mutation, want 8", got)
	}
	if got := request.Query.Filter.Fragment.Value.Int64(); got != 501 {
		t.Fatalf("request filter value = %d after source mutation, want 501", got)
	}
	if got := request.Query.ProjectionFields[0].Field; got != "o_orderkey" {
		t.Fatalf("request projection field = %q after source mutation, want o_orderkey", got)
	}
}

func TestNewExecutionRequestCarriesFilterDomainTranslation(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "lineitem", Field: "l_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterUnion,
			Children: []qsbridge.QuantaFilterExpression{
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"}},
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"}},
			},
		},
	})

	translation := request.FilterDomain
	if !translation.Required {
		t.Fatalf("translation = %#v, want required", translation)
	}
	if translation.TargetDomain != "lineitem" {
		t.Fatalf("target domain = %q, want lineitem", translation.TargetDomain)
	}
	if len(translation.SourceDomains) != 2 || translation.SourceDomains[0] != "lineitem" || translation.SourceDomains[1] != "part" {
		t.Fatalf("source domains = %#v, want lineitem/part", translation.SourceDomains)
	}
	if len(translation.Strategies) != 1 || translation.Strategies[0] != qsbridge.PhysicalStrategyRelationshipVectorNormalization {
		t.Fatalf("strategies = %#v, want relationship vector normalization", translation.Strategies)
	}
}

func TestExecutionRequestWithCandidateSetClearsFilterDomainTranslation(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "lineitem", Field: "l_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterUnion,
			Children: []qsbridge.QuantaFilterExpression{
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"}},
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"}},
			},
		},
	})
	if !request.FilterDomain.Required {
		t.Fatalf("expected initial filter-domain translation requirement")
	}

	request = request.WithCandidateSet(qsbridge.QuantaCandidateSet{Index: "lineitem", Rownums: []qsbridge.QuantaRownum{7}})
	if request.FilterDomain.Required || len(request.FilterDomain.SourceDomains) != 0 || len(request.FilterDomain.Strategies) != 0 {
		t.Fatalf("filter-domain translation = %#v, want cleared", request.FilterDomain)
	}
}

func TestLegacyBitmapQueryAdapterConvertsExecutionRequestToProto(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "orders",
			Field:     "o_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpGE,
			Value:     big.NewInt(8),
		}},
	})

	proto := LegacyBitmapQueryAdapter{}.ToProtoFromRequest(request)
	if len(proto.Query) != 1 {
		t.Fatalf("fragments = %d, want 1", len(proto.Query))
	}
	fragment := proto.Query[0]
	if fragment.Index != "orders" || fragment.Field != "o_orderkey" {
		t.Fatalf("fragment = %#v, want orders.o_orderkey", fragment)
	}
	if fragment.Operation != pb.QueryFragment_INTERSECT || fragment.BsiOp != pb.QueryFragment_GE {
		t.Fatalf("operation/bsi = %v/%v, want INTERSECT/GE", fragment.Operation, fragment.BsiOp)
	}
	if got := new(big.Int).SetBytes(fragment.Value).Int64(); got != 8 {
		t.Fatalf("value = %d, want 8", got)
	}
}

func TestNewSQLExecutionRequestAddsJoinedWhereExprResidual(t *testing.T) {
	orders := qsbridge.TableInstance{Table: "orders", Alias: "o"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	where := qsbridge.Binary(qsbridge.BinaryOpOr,
		qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(qsbridge.FieldRef{Table: orders, Name: "o_orderstatus"}), qsbridge.Literal(qsbridge.ValueString, "F")),
		qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(qsbridge.FieldRef{Table: lineitem, Name: "l_shipmode"}), qsbridge.Literal(qsbridge.ValueString, "AIR")),
	)
	prepared := qsbridge.ExecutionRequest{
		Bound: qsbridge.BoundPlan{Prepared: qsbridge.PreparedPlan{Query: qsbridge.QueryIR{
			Kind:      qsbridge.QueryKindSelect,
			Sources:   []qsbridge.TableInstance{orders, lineitem},
			WhereExpr: where,
			Joins: []qsbridge.JoinEdge{{
				Left:  qsbridge.FieldRef{Table: orders, Name: "o_orderkey"},
				Right: qsbridge.FieldRef{Table: lineitem, Name: "l_orderkey"},
			}},
		}}},
	}

	runtime := NewSQLExecutionRequest(qsbridge.QuantaIntermediateQuery{}, prepared)
	if len(runtime.Predicates) != 1 {
		t.Fatalf("predicates = %#v, want one residual where predicate", runtime.Predicates)
	}
	if !reflect.DeepEqual(runtime.Predicates[0].Expr, where) || runtime.Predicates[0].Placement != qsbridge.PredicateResidualScan {
		t.Fatalf("predicate = %#v, want residual where expression", runtime.Predicates[0])
	}
}

func TestNewSQLExecutionRequestDerivesFilterTranslationTargetFromSource(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	prepared := qsbridge.ExecutionRequest{
		Bound: qsbridge.BoundPlan{Prepared: qsbridge.PreparedPlan{Query: qsbridge.QueryIR{
			Kind:    qsbridge.QueryKindSelect,
			Sources: []qsbridge.TableInstance{lineitem, part},
		}}},
	}

	runtime := NewSQLExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterUnion,
			Children: []qsbridge.QuantaFilterExpression{
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "part", Field: "p_brand"}},
				{Operation: qsbridge.QuantaFilterLeaf, Fragment: qsbridge.QuantaQueryFragment{Index: "lineitem", Field: "l_quantity"}},
			},
		},
	}, prepared)

	if !runtime.FilterDomain.Required {
		t.Fatalf("translation = %#v, want required", runtime.FilterDomain)
	}
	if runtime.FilterDomain.TargetDomain != "lineitem" {
		t.Fatalf("target domain = %q, want first source lineitem", runtime.FilterDomain.TargetDomain)
	}
}

func TestExecutionResultAliasesQSBridgeQuantaExecutionResult(t *testing.T) {
	result := ExecutionResult{
		RowSet: qsbridge.QuantaProjectedRowSet{
			Rownums: []qsbridge.QuantaRownum{1001, 1002},
		},
		Count: 2,
	}
	var bridged qsbridge.QuantaExecutionResult = result

	if got := bridged.CandidateCount(); got != 2 {
		t.Fatalf("candidate count = %d, want 2", got)
	}
}

func TestExecutionRequestReportsRootIndex(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index: "orders",
		}},
	})

	index, ok := request.RootIndex()
	if !ok {
		t.Fatalf("root index not found")
	}
	if index != "orders" {
		t.Fatalf("root index = %q, want orders", index)
	}
}

func TestExecutionRequestFallsBackToProjectionRootIndex(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index: "lineitem",
			Field: "l_orderkey",
		}},
	})

	index, ok := request.RootIndex()
	if !ok {
		t.Fatalf("root index not found")
	}
	if index != "lineitem" {
		t.Fatalf("root index = %q, want lineitem", index)
	}
}

func TestExecutionRequestFallsBackToSourceRootIndex(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"orders"}

	index, ok := request.RootIndex()
	if !ok {
		t.Fatalf("root index not found")
	}
	if index != "orders" {
		t.Fatalf("root index = %q, want orders", index)
	}
}

func TestNewSQLExecutionRequestMaterializesAggregateInputFields(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	extendedPrice := qsbridge.FieldRef{
		Table:        lineitem,
		Name:         "l_extendedprice",
		PhysicalName: "l_extendedprice",
		Type:         qsbridge.DataTypeFloat,
	}
	discount := qsbridge.FieldRef{
		Table:        lineitem,
		Name:         "l_discount",
		PhysicalName: "l_discount",
		Type:         qsbridge.DataTypeFloat,
	}
	preparedQuery := qsbridge.QueryIR{
		Kind:    qsbridge.QueryKindSelect,
		Sources: []qsbridge.TableInstance{lineitem},
		Aggregates: []qsbridge.Aggregate{{
			Function: "sum",
			Input: qsbridge.BinaryExpr{
				Left: qsbridge.FieldExpr{Ref: extendedPrice},
				Op:   qsbridge.BinaryOpMultiply,
				Right: qsbridge.BinaryExpr{
					Left:  qsbridge.Literal(qsbridge.ValueInt, int64(1)),
					Op:    qsbridge.BinaryOpSubtract,
					Right: qsbridge.FieldExpr{Ref: discount},
				},
			},
			Alias: "total_revenue",
			Type:  qsbridge.DataTypeFloat,
		}},
	}
	executionRequest := qsbridge.ExecutionRequest{
		Bound: qsbridge.BoundPlan{
			Prepared: qsbridge.PreparedPlan{Query: preparedQuery},
		},
	}

	request := NewSQLExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{Index: "lineitem"}},
	}, executionRequest)

	fields := request.Materialization.ProjectionFields
	if len(fields) != 2 {
		t.Fatalf("materialization fields = %#v, want l_extendedprice and l_discount", fields)
	}
	if fields[0].Field != "l_extendedprice" || fields[0].Type != qsbridge.DataTypeFloat || fields[0].Visible {
		t.Fatalf("first materialization field = %#v, want hidden l_extendedprice float", fields[0])
	}
	if fields[1].Field != "l_discount" || fields[1].Type != qsbridge.DataTypeFloat || fields[1].Visible {
		t.Fatalf("second materialization field = %#v, want hidden l_discount float", fields[1])
	}
}

func TestNewSQLExecutionRequestKeepsProjectedFieldsVisible(t *testing.T) {
	customers := qsbridge.TableInstance{Table: "customers_qa"}
	firstName := qsbridge.FieldRef{
		Table:        customers,
		Name:         "first_name",
		PhysicalName: "first_name",
		Type:         qsbridge.DataTypeString,
	}
	city := qsbridge.FieldRef{
		Table:        customers,
		Name:         "city",
		PhysicalName: "city",
		Type:         qsbridge.DataTypeString,
	}
	preparedQuery := qsbridge.QueryIR{
		Kind:    qsbridge.QueryKindSelect,
		Sources: []qsbridge.TableInstance{customers},
		Projection: []qsbridge.ProjectionColumn{{
			Expr:  qsbridge.FieldExpr{Ref: firstName},
			Alias: "first_name",
			Type:  qsbridge.DataTypeString,
		}},
		Predicates: []qsbridge.Predicate{{
			Expr: qsbridge.BinaryExpr{
				Left:  qsbridge.FieldExpr{Ref: city},
				Op:    qsbridge.BinaryOpEqual,
				Right: qsbridge.Literal(qsbridge.ValueString, "Seattle"),
			},
		}},
	}
	executionRequest := qsbridge.ExecutionRequest{
		Bound: qsbridge.BoundPlan{
			Prepared: qsbridge.PreparedPlan{Query: preparedQuery},
		},
	}

	request := NewSQLExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index: "customers_qa",
			Field: "city",
			BSIOp: qsbridge.QuantaBSIOpEQ,
			Value: big.NewInt(1),
		}},
	}, executionRequest)

	fields := request.Materialization.ProjectionFields
	if len(fields) != 2 {
		t.Fatalf("materialization fields = %#v, want first_name and city", fields)
	}
	visible := make(map[string]bool, len(fields))
	for _, field := range fields {
		visible[field.Field] = field.Visible
	}
	if !visible["first_name"] {
		t.Fatalf("first_name should remain visible in materialization fields: %#v", fields)
	}
	if visible["city"] {
		t.Fatalf("predicate-only city field should stay hidden: %#v", fields)
	}
}

func TestNewSQLExecutionRequestCarriesDatetimeRangeForMaterialization(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	shipDate := qsbridge.FieldRef{
		Table:        lineitem,
		Name:         "l_shipdate",
		PhysicalName: "l_shipdate",
		Type:         qsbridge.DataTypeTime,
	}
	preparedQuery := qsbridge.QueryIR{
		Kind:    qsbridge.QueryKindSelect,
		Sources: []qsbridge.TableInstance{lineitem},
		Predicates: []qsbridge.Predicate{{
			Expr: qsbridge.FieldExpr{Ref: shipDate},
		}},
	}
	executionRequest := qsbridge.ExecutionRequest{
		Bound: qsbridge.BoundPlan{
			Prepared: qsbridge.PreparedPlan{Query: preparedQuery},
		},
	}

	request := NewSQLExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:       "lineitem",
			Field:       "l_shipdate",
			BSIOp:       qsbridge.QuantaBSIOpRange,
			Begin:       big.NewInt(820454400000),
			End:         big.NewInt(828316799999),
			ShardWindow: true,
		}},
	}, executionRequest)

	if request.Materialization.FromEpochMillis != 820454400000 {
		t.Fatalf("from epoch millis = %d, want 820454400000", request.Materialization.FromEpochMillis)
	}
	if request.Materialization.ToEpochMillis != 828316799999 {
		t.Fatalf("to epoch millis = %d, want 828316799999", request.Materialization.ToEpochMillis)
	}
}

package qsruntime

import (
	"context"
	"math/big"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestMaterializationRequestFromExecutionBuildsCandidateRequest(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "orders",
			Field:     "o_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			BSIOp:     qsbridge.QuantaBSIOpGE,
			Value:     big.NewInt(8),
		}},
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index:   "orders",
			Field:   "o_orderkey",
			Visible: true,
		}},
	})
	materialization, diagnostics := MaterializationRequestFromExecution(request, BitmapQueryResult{
		Rownums: []qsbridge.QuantaRownum{8, 9},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics: %#v", diagnostics)
	}
	if materialization.Index != "orders" {
		t.Fatalf("index = %q, want orders", materialization.Index)
	}
	if materialization.CandidateCount() != 2 {
		t.Fatalf("candidate count = %d, want 2", materialization.CandidateCount())
	}
	if materialization.ProjectionCount() != 1 {
		t.Fatalf("projection count = %d, want 1", materialization.ProjectionCount())
	}
}

func TestMaterializationRequestFromExecutionKeepsOnlyRootProjectionFields(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "customers_qa",
			Field:     "cust_id",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{Index: "customers_qa", Field: "first_name", Visible: true},
			{Index: "orders_qa", Field: "ship_via", Visible: false},
		},
	})
	materialization, diagnostics := MaterializationRequestFromExecution(request, BitmapQueryResult{
		Rownums: []qsbridge.QuantaRownum{1},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics: %#v", diagnostics)
	}
	if materialization.Index != "customers_qa" {
		t.Fatalf("index = %q, want customers_qa", materialization.Index)
	}
	if materialization.ProjectionCount() != 1 {
		t.Fatalf("projection count = %d, want 1", materialization.ProjectionCount())
	}
	if got := materialization.ProjectionFields[0].Field; got != "first_name" {
		t.Fatalf("projection field = %q, want first_name", got)
	}
}

func TestMaterializationRequestFromExecutionFiltersExplicitRootMaterialization(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "customers_qa",
			Field:     "cust_id",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	request.Materialization = qsbridge.QuantaMaterializationRequest{
		Index: "customers_qa",
		ProjectionFields: []qsbridge.QuantaProjectionField{
			{Index: "customers_qa", Field: "first_name", Visible: true},
			{Index: "orders_qa", Field: "ship_via", Visible: false},
		},
	}
	materialization, diagnostics := MaterializationRequestFromExecution(request, BitmapQueryResult{
		Rownums: []qsbridge.QuantaRownum{1},
	})
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics: %#v", diagnostics)
	}
	if materialization.ProjectionCount() != 1 {
		t.Fatalf("projection count = %d, want 1", materialization.ProjectionCount())
	}
	if got := materialization.ProjectionFields[0].Field; got != "first_name" {
		t.Fatalf("projection field = %q, want first_name", got)
	}
	if got := materialization.Rownums; len(got) != 1 || got[0] != 1 {
		t.Fatalf("rownums = %#v, want [1]", got)
	}
}

func TestMaterializationRequestWithPhysicalGroupExpressionsElidesTimestampSource(t *testing.T) {
	table := qsbridge.TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipDate := qsbridge.FieldRef{Table: table, Name: "l_shipdate", Type: qsbridge.DataTypeTime}
	yearExpr := qsbridge.Call("year", qsbridge.Field(shipDate))
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"lineitem"}
	request.GroupBy = []qsbridge.Expr{yearExpr}
	request.Projection = []qsbridge.ProjectionColumn{{Expr: yearExpr, Alias: "l_year", Type: qsbridge.DataTypeInt}}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "line_count", Type: qsbridge.DataTypeInt}}
	materialization := qsbridge.QuantaMaterializationRequest{
		Index:   "lineitem",
		Rownums: []qsbridge.QuantaRownum{7, 9},
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index: "lineitem",
			Role:  "l",
			Field: "l_shipdate",
			Type:  qsbridge.DataTypeTime,
		}},
	}

	got := materializationRequestWithPhysicalGroupExpressions(request, materialization)

	if len(got.ProjectionFields) != 0 {
		t.Fatalf("projection fields = %#v, want source timestamp elided", got.ProjectionFields)
	}
	if len(got.ProjectionExpressions) != 1 {
		t.Fatalf("projection expressions = %#v, want one year expression", got.ProjectionExpressions)
	}
	if got.ProjectionExpressions[0].Output.Field != "year_l_shipdate" {
		t.Fatalf("expression output = %#v, want year_l_shipdate", got.ProjectionExpressions[0].Output)
	}
	if got.ProjectionCount() != 1 {
		t.Fatalf("projection count = %d, want one derived projection", got.ProjectionCount())
	}
}

func TestMaterializationRequestWithPhysicalGroupExpressionsKeepsTimestampWhenOtherwiseRequired(t *testing.T) {
	table := qsbridge.TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipDate := qsbridge.FieldRef{Table: table, Name: "l_shipdate", Type: qsbridge.DataTypeTime}
	yearExpr := qsbridge.Call("year", qsbridge.Field(shipDate))
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"lineitem"}
	request.GroupBy = []qsbridge.Expr{yearExpr}
	request.Projection = []qsbridge.ProjectionColumn{
		{Expr: yearExpr, Alias: "l_year", Type: qsbridge.DataTypeInt},
		{Expr: qsbridge.Field(shipDate), Alias: "shipdate", Type: qsbridge.DataTypeTime},
	}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "line_count", Type: qsbridge.DataTypeInt}}
	materialization := qsbridge.QuantaMaterializationRequest{
		Index:   "lineitem",
		Rownums: []qsbridge.QuantaRownum{7, 9},
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index: "lineitem",
			Role:  "l",
			Field: "l_shipdate",
			Type:  qsbridge.DataTypeTime,
		}},
	}

	got := materializationRequestWithPhysicalGroupExpressions(request, materialization)

	if len(got.ProjectionFields) != 1 {
		t.Fatalf("projection fields = %#v, want source timestamp retained", got.ProjectionFields)
	}
	if len(got.ProjectionExpressions) != 1 {
		t.Fatalf("projection expressions = %#v, want one year expression", got.ProjectionExpressions)
	}
	if got.ProjectionCount() != 2 {
		t.Fatalf("projection count = %d, want source plus derived projection", got.ProjectionCount())
	}
}

func TestMaterializationRequestWithPhysicalGroupExpressionsIgnoresPushdownPredicateSource(t *testing.T) {
	table := qsbridge.TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	shipDate := qsbridge.FieldRef{Table: table, Name: "l_shipdate", Type: qsbridge.DataTypeTime}
	yearExpr := qsbridge.Call("year", qsbridge.Field(shipDate))
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{"lineitem"}
	request.GroupBy = []qsbridge.Expr{yearExpr}
	request.Projection = []qsbridge.ProjectionColumn{{Expr: yearExpr, Alias: "l_year", Type: qsbridge.DataTypeInt}}
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpGreaterEqual, qsbridge.Field(shipDate), qsbridge.Literal(qsbridge.ValueString, "1992-01-01")),
		Placement: qsbridge.PredicatePushdown,
	}}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "line_count", Type: qsbridge.DataTypeInt}}
	materialization := qsbridge.QuantaMaterializationRequest{
		Index:   "lineitem",
		Rownums: []qsbridge.QuantaRownum{7, 9},
		ProjectionFields: []qsbridge.QuantaProjectionField{{
			Index: "lineitem",
			Role:  "l",
			Field: "l_shipdate",
			Type:  qsbridge.DataTypeTime,
		}},
	}

	got := materializationRequestWithPhysicalGroupExpressions(request, materialization)

	if len(got.ProjectionFields) != 0 {
		t.Fatalf("projection fields = %#v, want pushdown predicate source elided", got.ProjectionFields)
	}
	if len(got.ProjectionExpressions) != 1 {
		t.Fatalf("projection expressions = %#v, want one year expression", got.ProjectionExpressions)
	}

	request.Predicates[0].Placement = qsbridge.PredicateResidualScan
	got = materializationRequestWithPhysicalGroupExpressions(request, materialization)
	if len(got.ProjectionFields) != 1 {
		t.Fatalf("projection fields = %#v, want residual predicate source retained", got.ProjectionFields)
	}
}

func TestProjectionMaterializationKernelSupportsExpressions(t *testing.T) {
	reader := &recordingNativeProjectionExpressionReader{}
	if !projectionMaterializationKernelSupportsExpressions(FallbackProjectionMaterializationKernel{
		Preferred: NativeProjectionMaterializationKernel{Reader: reader},
	}) {
		t.Fatalf("fallback native kernel with expression reader should support expressions")
	}
	if projectionMaterializationKernelSupportsExpressions(ProjectionMaterializerKernelAdapter{
		Materializer: ProjectionMaterializerFunc(func(_ context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			return qsbridge.QuantaProjectedRowSet{Index: request.Index, Rownums: request.Rownums}, nil, nil
		}),
	}) {
		t.Fatalf("compat materializer adapter should not advertise expression support")
	}
}

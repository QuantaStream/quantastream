package qsruntime

import (
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

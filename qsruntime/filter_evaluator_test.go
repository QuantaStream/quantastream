package qsruntime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type testFilterLeafEvaluator struct {
	sets map[string]qsbridge.QuantaCandidateSet
}

func (e testFilterLeafEvaluator) EvaluateFilterLeaf(_ context.Context, fragment qsbridge.QuantaQueryFragment) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	set, ok := e.sets[fragment.Field]
	if !ok {
		return qsbridge.QuantaCandidateSet{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticCatalogFieldNotFound, qsbridge.PhaseExecute, "missing test fragment: "+fragment.Field),
		}, nil
	}
	return set, nil, nil
}

func TestQuantaFilterTreeEvaluatorMergesNestedFilters(t *testing.T) {
	evaluator := QuantaFilterTreeEvaluator{
		Leaves: testFilterLeafEvaluator{sets: map[string]qsbridge.QuantaCandidateSet{
			"a": {Index: "orders", Rownums: []qsbridge.QuantaRownum{1, 2, 3}},
			"b": {Index: "orders", Rownums: []qsbridge.QuantaRownum{2, 3}},
			"c": {Index: "orders", Rownums: []qsbridge.QuantaRownum{5, 2}},
		}},
	}
	filter := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterUnion,
		Children: []qsbridge.QuantaFilterExpression{
			{
				Operation: qsbridge.QuantaFilterIntersect,
				Children: []qsbridge.QuantaFilterExpression{
					testFilterLeaf("a"),
					testFilterLeaf("b"),
				},
			},
			testFilterLeaf("c"),
		},
	}

	set, diagnostics, err := evaluator.EvaluateFilter(context.Background(), filter)
	if err != nil {
		t.Fatalf("EvaluateFilter error: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("EvaluateFilter diagnostics: %v", diagnostics)
	}
	want := []qsbridge.QuantaRownum{2, 3, 5}
	if !reflect.DeepEqual(set.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", set.Rownums, want)
	}
	if set.Index != "orders" {
		t.Fatalf("index = %q, want orders", set.Index)
	}
}

func TestQuantaFilterTreeEvaluatorMergesCandidateSetLeaves(t *testing.T) {
	evaluator := QuantaFilterTreeEvaluator{
		Leaves: testFilterLeafEvaluator{sets: map[string]qsbridge.QuantaCandidateSet{
			"b": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{2, 3, 4}},
		}},
	}
	filter := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterIntersect,
		Children: []qsbridge.QuantaFilterExpression{
			{
				Operation:    qsbridge.QuantaFilterCandidateSet,
				CandidateSet: qsbridge.QuantaCandidateSet{Index: "lineitem", Rownums: []qsbridge.QuantaRownum{4, 7}},
			},
			testFilterLeaf("b"),
		},
	}

	set, diagnostics, err := evaluator.EvaluateFilter(context.Background(), filter)
	if err != nil {
		t.Fatalf("EvaluateFilter error: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("EvaluateFilter diagnostics: %v", diagnostics)
	}
	want := []qsbridge.QuantaRownum{4}
	if !reflect.DeepEqual(set.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", set.Rownums, want)
	}
	if set.Index != "lineitem" {
		t.Fatalf("index = %q, want lineitem", set.Index)
	}
}

func TestQuantaFilterTreeEvaluatorReportsMissingLeafEvaluator(t *testing.T) {
	_, diagnostics, err := (QuantaFilterTreeEvaluator{}).EvaluateFilter(context.Background(), testFilterLeaf("a"))
	if err != nil {
		t.Fatalf("EvaluateFilter error: %v", err)
	}
	if !diagnostics.BlocksNative() || diagnostics[0].Code != qsbridge.DiagnosticInternalInvariant {
		t.Fatalf("diagnostics = %#v, want internal invariant blocker", diagnostics)
	}
}

func TestQuantaFilterTreeEvaluatorReportsUnsupportedOperation(t *testing.T) {
	_, diagnostics, err := (QuantaFilterTreeEvaluator{}).EvaluateFilter(context.Background(), qsbridge.QuantaFilterExpression{Operation: "XOR"})
	if err != nil {
		t.Fatalf("EvaluateFilter error: %v", err)
	}
	if !diagnostics.BlocksNative() || diagnostics[0].Code != qsbridge.DiagnosticUnsupportedSQL {
		t.Fatalf("diagnostics = %#v, want unsupported SQL blocker", diagnostics)
	}
}

func TestQuantaFilterTreeEvaluatorHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := (QuantaFilterTreeEvaluator{}).EvaluateFilter(ctx, testFilterLeaf("a"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func testFilterLeaf(field string) qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment:  qsbridge.QuantaQueryFragment{Field: field},
	}
}

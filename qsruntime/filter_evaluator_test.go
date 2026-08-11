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

type testConstrainedFilterLeafCall struct {
	field   string
	rownums []qsbridge.QuantaRownum
}

type testConstrainedFilterLeafEvaluator struct {
	broadSets        map[string]qsbridge.QuantaCandidateSet
	constrainedSets  map[string]qsbridge.QuantaCandidateSet
	broadCalls       []string
	constrainedCalls []testConstrainedFilterLeafCall
}

func (e *testConstrainedFilterLeafEvaluator) EvaluateFilterLeaf(_ context.Context, fragment qsbridge.QuantaQueryFragment) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	e.broadCalls = append(e.broadCalls, fragment.Field)
	set, ok := e.broadSets[fragment.Field]
	if !ok {
		return qsbridge.QuantaCandidateSet{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticCatalogFieldNotFound, qsbridge.PhaseExecute, "missing broad test fragment: "+fragment.Field),
		}, nil
	}
	return set, nil, nil
}

func (e *testConstrainedFilterLeafEvaluator) EvaluateFilterLeafWithinCandidateSet(_ context.Context, fragment qsbridge.QuantaQueryFragment, candidates qsbridge.QuantaCandidateSet) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	e.constrainedCalls = append(e.constrainedCalls, testConstrainedFilterLeafCall{
		field:   fragment.Field,
		rownums: append([]qsbridge.QuantaRownum(nil), candidates.Rownums...),
	})
	set, ok := e.constrainedSets[fragment.Field]
	if !ok {
		return qsbridge.QuantaCandidateSet{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticCatalogFieldNotFound, qsbridge.PhaseExecute, "missing constrained test fragment: "+fragment.Field),
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

func TestQuantaFilterIntersectCandidateSetsPreservesLeftOrderWhenLeftIsSmaller(t *testing.T) {
	left := qsbridge.QuantaCandidateSet{Index: "lineitem", Rownums: []qsbridge.QuantaRownum{5, 2, 9}}
	right := qsbridge.QuantaCandidateSet{Index: "lineitem", Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4, 5, 6, 7, 8, 9}}

	set := quantaFilterIntersectCandidateSets(left, right)
	want := []qsbridge.QuantaRownum{5, 2, 9}
	if !reflect.DeepEqual(set.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", set.Rownums, want)
	}
}

func TestQuantaFilterIntersectCandidateSetsPreservesLeftOrderWhenRightIsSmaller(t *testing.T) {
	left := qsbridge.QuantaCandidateSet{Index: "lineitem", Rownums: []qsbridge.QuantaRownum{5, 2, 9, 10}}
	right := qsbridge.QuantaCandidateSet{Index: "lineitem", Rownums: []qsbridge.QuantaRownum{9, 5}}

	set := quantaFilterIntersectCandidateSets(left, right)
	want := []qsbridge.QuantaRownum{5, 9}
	if !reflect.DeepEqual(set.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", set.Rownums, want)
	}
}

func TestQuantaFilterTreeEvaluatorEvaluatesCandidateSetFirstForIntersect(t *testing.T) {
	leaves := &testConstrainedFilterLeafEvaluator{
		broadSets: map[string]qsbridge.QuantaCandidateSet{
			"a": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{1, 2, 4, 8}},
			"b": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{4, 9}},
		},
		constrainedSets: map[string]qsbridge.QuantaCandidateSet{
			"a": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{2, 4}},
			"b": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{4}},
		},
	}
	evaluator := QuantaFilterTreeEvaluator{Leaves: leaves}
	filter := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterIntersect,
		Children: []qsbridge.QuantaFilterExpression{
			testFilterLeaf("a"),
			testFilterLeaf("b"),
			testFilterCandidateSet("lineitem", 2, 4, 8),
		},
	}

	set, diagnostics, err := evaluator.EvaluateFilter(context.Background(), filter)
	if err != nil {
		t.Fatalf("EvaluateFilter error: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("EvaluateFilter diagnostics: %v", diagnostics)
	}
	if len(leaves.broadCalls) != 0 {
		t.Fatalf("broadCalls = %#v, want none", leaves.broadCalls)
	}
	wantCalls := []testConstrainedFilterLeafCall{
		{field: "a", rownums: []qsbridge.QuantaRownum{2, 4, 8}},
		{field: "b", rownums: []qsbridge.QuantaRownum{2, 4}},
	}
	if !reflect.DeepEqual(leaves.constrainedCalls, wantCalls) {
		t.Fatalf("constrainedCalls = %#v, want %#v", leaves.constrainedCalls, wantCalls)
	}
	want := []qsbridge.QuantaRownum{4}
	if !reflect.DeepEqual(set.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", set.Rownums, want)
	}
}

func TestQuantaFilterTreeEvaluatorFlattensNestedIntersectBeforeCandidateSetOrdering(t *testing.T) {
	leaves := &testConstrainedFilterLeafEvaluator{
		broadSets: map[string]qsbridge.QuantaCandidateSet{
			"a": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{1, 2, 4, 8}},
			"b": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{4, 9}},
		},
		constrainedSets: map[string]qsbridge.QuantaCandidateSet{
			"a": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{2, 4}},
			"b": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{4}},
		},
	}
	evaluator := QuantaFilterTreeEvaluator{Leaves: leaves}
	filter := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterIntersect,
		Children: []qsbridge.QuantaFilterExpression{
			testFilterLeaf("a"),
			{
				Operation: qsbridge.QuantaFilterIntersect,
				Children: []qsbridge.QuantaFilterExpression{
					testFilterLeaf("b"),
					testFilterCandidateSet("lineitem", 2, 4, 8),
				},
			},
		},
	}

	set, diagnostics, err := evaluator.EvaluateFilter(context.Background(), filter)
	if err != nil {
		t.Fatalf("EvaluateFilter error: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("EvaluateFilter diagnostics: %v", diagnostics)
	}
	if len(leaves.broadCalls) != 0 {
		t.Fatalf("broadCalls = %#v, want none", leaves.broadCalls)
	}
	wantCalls := []testConstrainedFilterLeafCall{
		{field: "a", rownums: []qsbridge.QuantaRownum{2, 4, 8}},
		{field: "b", rownums: []qsbridge.QuantaRownum{2, 4}},
	}
	if !reflect.DeepEqual(leaves.constrainedCalls, wantCalls) {
		t.Fatalf("constrainedCalls = %#v, want %#v", leaves.constrainedCalls, wantCalls)
	}
	want := []qsbridge.QuantaRownum{4}
	if !reflect.DeepEqual(set.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", set.Rownums, want)
	}
}

func TestQuantaFilterTreeEvaluatorEvaluatesCandidateSetSubtreeFirstForIntersect(t *testing.T) {
	leaves := &testConstrainedFilterLeafEvaluator{
		broadSets: map[string]qsbridge.QuantaCandidateSet{
			"common": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{1, 2, 4, 8}},
			"branch": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{2, 8}},
		},
		constrainedSets: map[string]qsbridge.QuantaCandidateSet{
			"common": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{2}},
			"branch": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{2, 4}},
		},
	}
	evaluator := QuantaFilterTreeEvaluator{Leaves: leaves}
	filter := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterIntersect,
		Children: []qsbridge.QuantaFilterExpression{
			testFilterLeaf("common"),
			{
				Operation: qsbridge.QuantaFilterUnion,
				Children: []qsbridge.QuantaFilterExpression{
					{
						Operation: qsbridge.QuantaFilterIntersect,
						Children: []qsbridge.QuantaFilterExpression{
							testFilterCandidateSet("lineitem", 2, 4),
							testFilterLeaf("branch"),
						},
					},
				},
			},
		},
	}

	set, diagnostics, err := evaluator.EvaluateFilter(context.Background(), filter)
	if err != nil {
		t.Fatalf("EvaluateFilter error: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("EvaluateFilter diagnostics: %v", diagnostics)
	}
	if len(leaves.broadCalls) != 0 {
		t.Fatalf("broadCalls = %#v, want none", leaves.broadCalls)
	}
	wantCalls := []testConstrainedFilterLeafCall{
		{field: "branch", rownums: []qsbridge.QuantaRownum{2, 4}},
		{field: "common", rownums: []qsbridge.QuantaRownum{2, 4}},
	}
	if !reflect.DeepEqual(leaves.constrainedCalls, wantCalls) {
		t.Fatalf("constrainedCalls = %#v, want %#v", leaves.constrainedCalls, wantCalls)
	}
	want := []qsbridge.QuantaRownum{2}
	if !reflect.DeepEqual(set.Rownums, want) {
		t.Fatalf("rownums = %#v, want %#v", set.Rownums, want)
	}
}

func TestQuantaFilterTreeEvaluatorShortCircuitsEmptyCandidateSetIntersect(t *testing.T) {
	leaves := &testConstrainedFilterLeafEvaluator{
		broadSets: map[string]qsbridge.QuantaCandidateSet{
			"a": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{1, 2, 4, 8}},
		},
		constrainedSets: map[string]qsbridge.QuantaCandidateSet{
			"a": {Index: "lineitem", Rownums: []qsbridge.QuantaRownum{}},
		},
	}
	evaluator := QuantaFilterTreeEvaluator{Leaves: leaves}
	filter := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterIntersect,
		Children: []qsbridge.QuantaFilterExpression{
			testFilterLeaf("a"),
			testFilterCandidateSet("lineitem"),
		},
	}

	set, diagnostics, err := evaluator.EvaluateFilter(context.Background(), filter)
	if err != nil {
		t.Fatalf("EvaluateFilter error: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("EvaluateFilter diagnostics: %v", diagnostics)
	}
	if len(leaves.broadCalls) != 0 {
		t.Fatalf("broadCalls = %#v, want none", leaves.broadCalls)
	}
	if len(leaves.constrainedCalls) != 0 {
		t.Fatalf("constrainedCalls = %#v, want none", leaves.constrainedCalls)
	}
	if set.Index != "lineitem" {
		t.Fatalf("index = %q, want lineitem", set.Index)
	}
	if len(set.Rownums) != 0 {
		t.Fatalf("rownums = %#v, want empty", set.Rownums)
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

func testFilterCandidateSet(index string, rownums ...qsbridge.QuantaRownum) qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation:    qsbridge.QuantaFilterCandidateSet,
		CandidateSet: qsbridge.QuantaCandidateSet{Index: index, Rownums: append([]qsbridge.QuantaRownum(nil), rownums...)},
	}
}

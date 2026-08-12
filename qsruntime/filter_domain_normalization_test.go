package qsruntime

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestFilterDomainBranchesForSourceKeepsQ19PartPredicatesTogether(t *testing.T) {
	filter := q19FormalFilterTreeForTest()

	branches := filterDomainBranchesForSource(filter, "part")
	if len(branches) != 3 {
		t.Fatalf("part branches = %d, want 3: %#v", len(branches), branches)
	}
	for i, branch := range branches {
		if branch.Operation != qsbridge.QuantaFilterIntersect {
			t.Fatalf("branch[%d] operation = %q, want INTERSECT", i, branch.Operation)
		}
		if got := filterDomainExpressionLeafCount(branch); got != 4 {
			t.Fatalf("branch[%d] part leaf count = %d, want brand/container/size lower/size upper", i, got)
		}
		if !filterDomainExpressionOnlySource(branch, "part") {
			t.Fatalf("branch[%d] includes a non-part predicate: %#v", i, branch)
		}
	}

	escaped := filterDomainFragmentsForSourceExcludingBranches(filter, "part", branches)
	if len(escaped) != 0 {
		t.Fatalf("escaped part leaves = %d, want none: %#v", len(escaped), escaped)
	}
}

func TestKernelFilterDomainNormalizationExecutorParallelizesSourceBranchesWhenEnabled(t *testing.T) {
	filter := qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterUnion,
		Children: []qsbridge.QuantaFilterExpression{
			q19MixedFilterBranchForParallelTest("p_branch_1", "p_size_1", "l_quantity_1"),
			q19MixedFilterBranchForParallelTest("p_branch_2", "p_size_2", "l_quantity_2"),
			q19MixedFilterBranchForParallelTest("p_branch_3", "p_size_3", "l_quantity_3"),
		},
	}
	kernel := newParallelBranchNormalizationKernelForTest(3)
	executor := KernelFilterDomainNormalizationExecutor{
		Kernel:           kernel,
		ParallelBranches: true,
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{Filter: filter})
	plan := FilterDomainNormalizationPlan{
		Translation: qsbridge.QuantaFilterDomainTranslation{
			Required:      true,
			SourceDomains: []string{"lineitem", "part"},
			TargetDomain:  "lineitem",
			Strategies:    []qsbridge.PhysicalStrategy{qsbridge.PhysicalStrategyRelationshipVectorNormalization},
		},
		Requests: []FilterDomainNormalizationRequest{{
			Operation:    FilterDomainNormalizeGroupedFilter,
			SourceDomain: "part",
			TargetDomain: "lineitem",
			Strategy:     qsbridge.PhysicalStrategyRelationshipVectorNormalization,
		}},
	}

	type normalizationResult struct {
		rewrite     qsbridge.FilterDomainRewriteResult
		diagnostics qsbridge.DiagnosticSet
		err         error
	}
	done := make(chan normalizationResult, 1)
	go func() {
		rewrite, diagnostics, err := executor.NormalizeFilterDomains(context.Background(), request, plan)
		done <- normalizationResult{rewrite: rewrite, diagnostics: diagnostics, err: err}
	}()

	for i := 0; i < 3; i++ {
		select {
		case <-kernel.entered:
		case <-time.After(time.Second):
			t.Fatalf("branch %d did not enter parallel normalization", i+1)
		}
	}
	if got := kernel.maxActive(); got < 2 {
		t.Fatalf("max active branch normalizations = %d, want overlap", got)
	}
	close(kernel.release)

	var result normalizationResult
	select {
	case result = <-done:
	case <-time.After(time.Second):
		t.Fatalf("parallel branch normalization did not finish")
	}
	if result.err != nil {
		t.Fatalf("NormalizeFilterDomains error = %v", result.err)
	}
	if result.diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.diagnostics)
	}
	got := make([]qsbridge.QuantaRownum, 0, len(result.rewrite.Branches))
	for _, branch := range result.rewrite.Branches {
		if len(branch.CandidateSet.Rownums) != 1 {
			t.Fatalf("branch candidate set = %#v, want one row", branch.CandidateSet)
		}
		got = append(got, branch.CandidateSet.Rownums[0])
	}
	want := []qsbridge.QuantaRownum{101, 102, 103}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("branch candidate rows = %#v, want result order %#v", got, want)
	}
}

func q19FormalFilterTreeForTest() qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterUnion,
		Children: []qsbridge.QuantaFilterExpression{
			q19FormalFilterBranchForTest("Brand#12", "SM", 1, 5, 1, 11),
			q19FormalFilterBranchForTest("Brand#23", "MED", 1, 10, 10, 20),
			q19FormalFilterBranchForTest("Brand#34", "LG", 1, 15, 20, 30),
		},
	}
}

func q19FormalFilterBranchForTest(brand, containerPrefix string, minSize, maxSize, minQuantity, maxQuantity int64) qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterIntersect,
		Children: []qsbridge.QuantaFilterExpression{
			q19PartLeafForTest("p_brand", qsbridge.QuantaBSIOpEQ, brand),
			q19PartLeafForTest("p_container", "", containerPrefix),
			q19PartSizeLeafForTest(qsbridge.QuantaBSIOpGE, minSize),
			q19LineitemQuantityLeafForTest(qsbridge.QuantaBSIOpGE, minQuantity),
			q19LineitemQuantityLeafForTest(qsbridge.QuantaBSIOpLE, maxQuantity),
			q19PartSizeLeafForTest(qsbridge.QuantaBSIOpLE, maxSize),
			q19LineitemLeafForTest("l_shipmode"),
			q19LineitemLeafForTest("l_shipinstruct"),
		},
	}
}

func q19MixedFilterBranchForParallelTest(partFieldA, partFieldB, lineitemField string) qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterIntersect,
		Children: []qsbridge.QuantaFilterExpression{
			{
				Operation: qsbridge.QuantaFilterLeaf,
				Fragment:  qsbridge.QuantaQueryFragment{Index: "part", Field: partFieldA},
			},
			{
				Operation: qsbridge.QuantaFilterLeaf,
				Fragment:  qsbridge.QuantaQueryFragment{Index: "part", Field: partFieldB},
			},
			{
				Operation: qsbridge.QuantaFilterLeaf,
				Fragment:  qsbridge.QuantaQueryFragment{Index: "lineitem", Field: lineitemField},
			},
		},
	}
}

type parallelBranchNormalizationKernelForTest struct {
	entered chan struct{}
	release chan struct{}

	mu     sync.Mutex
	active int
	max    int
}

func newParallelBranchNormalizationKernelForTest(branches int) *parallelBranchNormalizationKernelForTest {
	return &parallelBranchNormalizationKernelForTest{
		entered: make(chan struct{}, branches),
		release: make(chan struct{}),
	}
}

func (k *parallelBranchNormalizationKernelForTest) NormalizeFilterLeaf(context.Context, ExecutionRequest, FilterDomainNormalizationRequest, qsbridge.QuantaQueryFragment) (qsbridge.FilterDomainNormalizedLeaf, qsbridge.DiagnosticSet, error) {
	return qsbridge.FilterDomainNormalizedLeaf{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "parallel branch test should not normalize loose leaves"),
	}, nil
}

func (k *parallelBranchNormalizationKernelForTest) NormalizeFilterExpression(_ context.Context, _ ExecutionRequest, normalization FilterDomainNormalizationRequest, filter qsbridge.QuantaFilterExpression) (qsbridge.FilterDomainNormalizedBranch, qsbridge.DiagnosticSet, error) {
	rownum := qsbridge.QuantaRownum(0)
	if len(filter.Children) > 0 {
		switch filter.Children[0].Fragment.Field {
		case "p_branch_1":
			rownum = 101
		case "p_branch_2":
			rownum = 102
		case "p_branch_3":
			rownum = 103
		}
	}
	k.mu.Lock()
	k.active++
	if k.active > k.max {
		k.max = k.active
	}
	k.mu.Unlock()

	k.entered <- struct{}{}
	<-k.release

	k.mu.Lock()
	k.active--
	k.mu.Unlock()

	return qsbridge.FilterDomainNormalizedBranch{
		OriginalFilter: filter,
		SourceDomain:   normalization.SourceDomain,
		TargetDomain:   normalization.TargetDomain,
		CandidateSet: qsbridge.QuantaCandidateSet{
			Index:   normalization.TargetDomain,
			Rownums: []qsbridge.QuantaRownum{rownum},
		},
	}, nil, nil
}

func (k *parallelBranchNormalizationKernelForTest) maxActive() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.max
}

func q19PartLeafForTest(field string, op qsbridge.QuantaBSIOp, value string) qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment: qsbridge.QuantaQueryFragment{
			Index:      "part",
			Role:       "p",
			Field:      field,
			BSIOp:      op,
			HasLiteral: true,
			Literal:    qsbridge.Literal(qsbridge.ValueString, value),
		},
	}
}

func q19PartSizeLeafForTest(op qsbridge.QuantaBSIOp, value int64) qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment: qsbridge.QuantaQueryFragment{
			Index:      "part",
			Role:       "p",
			Field:      "p_size",
			BSIOp:      op,
			HasLiteral: true,
			Literal:    qsbridge.Literal(qsbridge.ValueInt, value),
		},
	}
}

func q19LineitemQuantityLeafForTest(op qsbridge.QuantaBSIOp, value int64) qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment: qsbridge.QuantaQueryFragment{
			Index:      "lineitem",
			Role:       "l",
			Field:      "l_quantity",
			BSIOp:      op,
			HasLiteral: true,
			Literal:    qsbridge.Literal(qsbridge.ValueInt, value),
		},
	}
}

func q19LineitemLeafForTest(field string) qsbridge.QuantaFilterExpression {
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterLeaf,
		Fragment: qsbridge.QuantaQueryFragment{
			Index: "lineitem",
			Role:  "l",
			Field: field,
		},
	}
}

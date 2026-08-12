package qsruntime

import (
	"context"
	"fmt"
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

func TestKernelFilterDomainNormalizationExecutorNormalizesSourceBranchesConcurrently(t *testing.T) {
	filter := q19FormalFilterTreeForTest()
	branches := filterDomainBranchesForSource(filter, "part")
	if len(branches) < 2 {
		t.Fatalf("branches = %d, want at least two for concurrency test", len(branches))
	}

	kernel := &blockingFilterDomainExpressionKernel{
		started: make(chan struct{}, len(branches)),
		release: make(chan struct{}),
	}
	executor := KernelFilterDomainNormalizationExecutor{Kernel: kernel}
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

	type normalizeResult struct {
		rewrite     qsbridge.FilterDomainRewriteResult
		diagnostics qsbridge.DiagnosticSet
		err         error
	}
	done := make(chan normalizeResult, 1)
	go func() {
		rewrite, diagnostics, err := executor.NormalizeFilterDomains(context.Background(), request, plan)
		done <- normalizeResult{rewrite: rewrite, diagnostics: diagnostics, err: err}
	}()

	waitForFilterDomainBranchStarts(t, kernel.started, len(branches))
	close(kernel.release)
	var result normalizeResult
	select {
	case result = <-done:
	case <-time.After(time.Second):
		t.Fatal("NormalizeFilterDomains did not finish after releasing branch work")
	}
	if result.err != nil {
		t.Fatalf("NormalizeFilterDomains error = %v", result.err)
	}
	if result.diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.diagnostics)
	}
	if kernel.MaxActive() < 2 {
		t.Fatalf("max active branch normalizations = %d, want at least 2", kernel.MaxActive())
	}
	if len(result.rewrite.Branches) != len(branches) {
		t.Fatalf("normalized branches = %d, want %d", len(result.rewrite.Branches), len(branches))
	}
	for i, branch := range result.rewrite.Branches {
		got := filterDomainTestBranchSignature(branch.OriginalFilter)
		want := filterDomainTestBranchSignature(branches[i])
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("branch[%d] order changed: got %v, want %v", i, got, want)
		}
	}
}

func waitForFilterDomainBranchStarts(t *testing.T, started <-chan struct{}, count int) {
	t.Helper()
	deadline := time.After(time.Second)
	for i := 0; i < count; i++ {
		select {
		case <-started:
		case <-deadline:
			t.Fatalf("observed %d branch starts, want %d", i, count)
		}
	}
}

func filterDomainTestBranchSignature(filter qsbridge.QuantaFilterExpression) []string {
	if filter.Operation == qsbridge.QuantaFilterLeaf {
		return []string{fmt.Sprintf("%s:%s:%v", filter.Fragment.Field, filter.Fragment.BSIOp, filter.Fragment.Literal.Value)}
	}
	signature := []string{}
	for _, child := range filter.Children {
		signature = append(signature, filterDomainTestBranchSignature(child)...)
	}
	return signature
}

type blockingFilterDomainExpressionKernel struct {
	mu        sync.Mutex
	started   chan struct{}
	release   chan struct{}
	active    int
	maxActive int
}

func (k *blockingFilterDomainExpressionKernel) NormalizeFilterLeaf(context.Context, ExecutionRequest, FilterDomainNormalizationRequest, qsbridge.QuantaQueryFragment) (qsbridge.FilterDomainNormalizedLeaf, qsbridge.DiagnosticSet, error) {
	return qsbridge.FilterDomainNormalizedLeaf{}, nil, nil
}

func (k *blockingFilterDomainExpressionKernel) NormalizeFilterExpression(ctx context.Context, _ ExecutionRequest, normalization FilterDomainNormalizationRequest, filter qsbridge.QuantaFilterExpression) (qsbridge.FilterDomainNormalizedBranch, qsbridge.DiagnosticSet, error) {
	k.mu.Lock()
	k.active++
	if k.active > k.maxActive {
		k.maxActive = k.active
	}
	k.mu.Unlock()
	defer func() {
		k.mu.Lock()
		k.active--
		k.mu.Unlock()
	}()

	select {
	case k.started <- struct{}{}:
	case <-ctx.Done():
		return qsbridge.FilterDomainNormalizedBranch{}, nil, ctx.Err()
	}
	select {
	case <-k.release:
	case <-ctx.Done():
		return qsbridge.FilterDomainNormalizedBranch{}, nil, ctx.Err()
	}
	return qsbridge.FilterDomainNormalizedBranch{
		OriginalFilter: filter,
		SourceDomain:   normalization.SourceDomain,
		TargetDomain:   normalization.TargetDomain,
		CandidateSet: qsbridge.QuantaCandidateSet{
			Index:   normalization.TargetDomain,
			Rownums: []qsbridge.QuantaRownum{qsbridge.QuantaRownum(filterDomainExpressionLeafCount(filter))},
		},
	}, nil, nil
}

func (k *blockingFilterDomainExpressionKernel) MaxActive() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.maxActive
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

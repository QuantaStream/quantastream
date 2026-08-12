package qsruntime

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// FilterDomainNormalizationOperation aliases the qsbridge normalization operation.
type FilterDomainNormalizationOperation = qsbridge.FilterDomainNormalizationOperation

const (
	// FilterDomainNormalizeGroupedFilter normalizes grouped predicate leaves before bitmap boolean algebra.
	FilterDomainNormalizeGroupedFilter = qsbridge.FilterDomainNormalizeGroupedFilter
)

// FilterDomainNormalizationPlan aliases the qsbridge normalization plan.
type FilterDomainNormalizationPlan = qsbridge.FilterDomainNormalizationPlan

// FilterDomainNormalizationRequest aliases one source-to-target normalization request.
type FilterDomainNormalizationRequest = qsbridge.FilterDomainNormalizationRequest

// FilterDomainNormalizationExecutor is the adapter seam for rownum-domain normalization.
type FilterDomainNormalizationExecutor interface {
	NormalizeFilterDomains(context.Context, ExecutionRequest, FilterDomainNormalizationPlan) (qsbridge.FilterDomainRewriteResult, qsbridge.DiagnosticSet, error)
}

// DirectBitmapFilterDomainNormalizationExecutor builds request-scoped leaf evaluation for normalization.
type DirectBitmapFilterDomainNormalizationExecutor struct {
	Sessions DirectSessionProvider
	Reader   RelationshipVectorReader
}

// NormalizeFilterDomains evaluates source leaves with the current request and translates them through the reader.
func (e DirectBitmapFilterDomainNormalizationExecutor) NormalizeFilterDomains(ctx context.Context, request ExecutionRequest, plan FilterDomainNormalizationPlan) (qsbridge.FilterDomainRewriteResult, qsbridge.DiagnosticSet, error) {
	normalizer := NewReaderBackedFilterDomainNormalizer(
		directBitmapFilterTreeLeafEvaluator{Sessions: e.Sessions, Request: request},
		relationshipVectorReaderWithRequestProjectionCache(e.Reader),
	)
	if kernel, ok := normalizer.(KernelFilterDomainNormalizationExecutor); ok {
		kernel.ParallelBranches = true
		normalizer = kernel
	}
	return normalizer.NormalizeFilterDomains(ctx, request, plan)
}

// NewReaderBackedFilterDomainNormalizer composes source-leaf evaluation with relationship-vector reads.
func NewReaderBackedFilterDomainNormalizer(leaves QuantaFilterLeafEvaluator, reader RelationshipVectorReader) FilterDomainNormalizationExecutor {
	return KernelFilterDomainNormalizationExecutor{
		Kernel: RelationshipVectorFilterDomainNormalizationKernel{
			Leaves: leaves,
			Translator: ReaderBackedFilterDomainRelationshipVectorTranslator{
				Reader: reader,
			},
		},
	}
}

// FilterDomainNormalizationKernel normalizes one source-domain leaf into the target rownum domain.
type FilterDomainNormalizationKernel interface {
	NormalizeFilterLeaf(context.Context, ExecutionRequest, FilterDomainNormalizationRequest, qsbridge.QuantaQueryFragment) (qsbridge.FilterDomainNormalizedLeaf, qsbridge.DiagnosticSet, error)
}

type filterDomainExpressionNormalizationKernel interface {
	NormalizeFilterExpression(context.Context, ExecutionRequest, FilterDomainNormalizationRequest, qsbridge.QuantaFilterExpression) (qsbridge.FilterDomainNormalizedBranch, qsbridge.DiagnosticSet, error)
}

// FilterDomainRelationshipVectorTranslator translates source rownums through a relationship vector.
type FilterDomainRelationshipVectorTranslator interface {
	TranslateFilterDomain(context.Context, ExecutionRequest, qsbridge.FilterDomainRelationshipVectorRequest) (qsbridge.FilterDomainRelationshipVectorResult, qsbridge.DiagnosticSet, error)
}

// ReaderBackedFilterDomainRelationshipVectorTranslator adapts a vector reader to normalization.
type ReaderBackedFilterDomainRelationshipVectorTranslator struct {
	Reader RelationshipVectorReader
}

// TranslateFilterDomain reads target candidates through the configured relationship-vector reader.
func (t ReaderBackedFilterDomainRelationshipVectorTranslator) TranslateFilterDomain(ctx context.Context, _ ExecutionRequest, request qsbridge.FilterDomainRelationshipVectorRequest) (qsbridge.FilterDomainRelationshipVectorResult, qsbridge.DiagnosticSet, error) {
	if t.Reader == nil {
		return qsbridge.FilterDomainRelationshipVectorResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, filterDomainRelationshipTranslatorMissingMessage(request)),
		}, nil
	}
	if reader, ok := t.Reader.(RelationshipVectorResultReader); ok {
		return reader.ReadRelatedCandidateResult(ctx, request)
	}
	targetCandidates, diagnostics, err := t.Reader.ReadRelatedCandidates(ctx, request)
	return qsbridge.FilterDomainRelationshipVectorResult{
		Request:          request,
		TargetCandidates: targetCandidates,
	}, diagnostics, err
}

// RelationshipVectorFilterDomainNormalizationKernel evaluates source leaves and prepares vector translation.
type RelationshipVectorFilterDomainNormalizationKernel struct {
	Leaves     QuantaFilterLeafEvaluator
	Translator FilterDomainRelationshipVectorTranslator
}

// NormalizeFilterLeaf translates one source-domain leaf into target-domain candidates.
func (k RelationshipVectorFilterDomainNormalizationKernel) NormalizeFilterLeaf(ctx context.Context, request ExecutionRequest, normalization FilterDomainNormalizationRequest, fragment qsbridge.QuantaQueryFragment) (qsbridge.FilterDomainNormalizedLeaf, qsbridge.DiagnosticSet, error) {
	if k.Leaves == nil {
		return qsbridge.FilterDomainNormalizedLeaf{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "filter-domain normalization source leaf failed: no leaf evaluator"),
		}, nil
	}
	sourceStart := time.Now()
	sourceCandidates, diagnostics, err := k.Leaves.EvaluateFilterLeaf(ctx, fragment)
	sourceElapsed := time.Since(sourceStart)
	if err != nil || diagnostics.BlocksNative() {
		if diagnostics.BlocksNative() {
			diagnostics = append(qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "filter-domain normalization source leaf failed: "+filterDomainFragmentKey(fragment)),
			}, diagnostics...)
		}
		return qsbridge.FilterDomainNormalizedLeaf{}, diagnostics, err
	}
	vectorRequest, ok := normalization.RelationshipVectorRequest(fragment, sourceCandidates)
	if !ok {
		return qsbridge.FilterDomainNormalizedLeaf{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, filterDomainRelationshipVectorRequestMessage(normalization, fragment)),
		}, nil
	}
	if k.Translator == nil {
		return qsbridge.FilterDomainNormalizedLeaf{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, filterDomainRelationshipTranslatorMissingMessage(vectorRequest)),
		}, nil
	}
	translationStart := time.Now()
	vectorResult, diagnostics, err := k.Translator.TranslateFilterDomain(ctx, request, vectorRequest)
	translationElapsed := time.Since(translationStart)
	if err != nil || diagnostics.BlocksNative() {
		return qsbridge.FilterDomainNormalizedLeaf{}, diagnostics, err
	}
	targetCandidates := vectorResult.TargetCandidates
	if targetCandidates.Index == "" && len(targetCandidates.Rownums) == 0 {
		return qsbridge.FilterDomainNormalizedLeaf{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, filterDomainNoTargetCandidateSetMessage(vectorRequest)),
		}, nil
	}
	if targetCandidates.Index == "" {
		targetCandidates.Index = normalization.TargetDomain
	}
	return qsbridge.FilterDomainNormalizedLeaf{
		OriginalFragment:           fragment,
		SourceDomain:               normalization.SourceDomain,
		TargetDomain:               normalization.TargetDomain,
		VectorIndex:                vectorResult.VectorIndex,
		VectorField:                vectorResult.VectorField,
		Direction:                  vectorResult.Direction,
		SourceCount:                len(sourceCandidates.Rownums),
		SourceElapsed:              sourceElapsed,
		TranslationElapsed:         translationElapsed,
		ProjectionElapsed:          vectorResult.ProjectionElapsed,
		ProjectionCacheHit:         vectorResult.ProjectionCacheHit,
		SourceKeyProjectionUsed:    vectorResult.SourceKeyProjectionUsed,
		SourceKeyProjectionReason:  vectorResult.SourceKeyProjectionReason,
		SourceKeyProjectionElapsed: vectorResult.SourceKeyProjectionElapsed,
		SourceValueCount:           vectorResult.SourceValueCount,
		CandidateCacheHit:          vectorResult.CandidateCacheHit,
		CandidateCacheMode:         vectorResult.CandidateCacheMode,
		CandidateMode:              vectorResult.CandidateMode,
		CandidateElapsed:           vectorResult.CandidateElapsed,
		BatchEqualElapsed:          vectorResult.BatchEqualElapsed,
		CandidateScanElapsed:       vectorResult.CandidateScanElapsed,
		CandidateSet:               targetCandidates,
	}, diagnostics, nil
}

// NormalizeFilterExpression translates one source-domain subtree into target-domain candidates.
func (k RelationshipVectorFilterDomainNormalizationKernel) NormalizeFilterExpression(ctx context.Context, request ExecutionRequest, normalization FilterDomainNormalizationRequest, filter qsbridge.QuantaFilterExpression) (qsbridge.FilterDomainNormalizedBranch, qsbridge.DiagnosticSet, error) {
	if k.Leaves == nil {
		return qsbridge.FilterDomainNormalizedBranch{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "filter-domain normalization source branch failed: no leaf evaluator"),
		}, nil
	}
	sourceStart := time.Now()
	sourceCandidates, diagnostics, err := (QuantaFilterTreeEvaluator{Leaves: k.Leaves}).EvaluateFilter(ctx, filter)
	sourceElapsed := time.Since(sourceStart)
	if err != nil || diagnostics.BlocksNative() {
		if diagnostics.BlocksNative() {
			diagnostics = append(qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "filter-domain normalization source branch failed: "+filterDomainExpressionKey(filter)),
			}, diagnostics...)
		}
		return qsbridge.FilterDomainNormalizedBranch{}, diagnostics, err
	}
	fragment, ok := firstFilterDomainFragment(filter)
	if !ok {
		return qsbridge.FilterDomainNormalizedBranch{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, "filter-domain normalization source branch has no relationship-vector anchor"),
		}, nil
	}
	vectorRequest, ok := normalization.RelationshipVectorRequest(fragment, sourceCandidates)
	if !ok {
		return qsbridge.FilterDomainNormalizedBranch{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, filterDomainRelationshipVectorRequestMessage(normalization, fragment)),
		}, nil
	}
	if k.Translator == nil {
		return qsbridge.FilterDomainNormalizedBranch{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, filterDomainRelationshipTranslatorMissingMessage(vectorRequest)),
		}, nil
	}
	translationStart := time.Now()
	vectorResult, diagnostics, err := k.Translator.TranslateFilterDomain(ctx, request, vectorRequest)
	translationElapsed := time.Since(translationStart)
	if err != nil || diagnostics.BlocksNative() {
		return qsbridge.FilterDomainNormalizedBranch{}, diagnostics, err
	}
	targetCandidates := vectorResult.TargetCandidates
	if targetCandidates.Index == "" && len(targetCandidates.Rownums) == 0 {
		return qsbridge.FilterDomainNormalizedBranch{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, filterDomainNoTargetCandidateSetMessage(vectorRequest)),
		}, nil
	}
	if targetCandidates.Index == "" {
		targetCandidates.Index = normalization.TargetDomain
	}
	return qsbridge.FilterDomainNormalizedBranch{
		OriginalFilter:             filter,
		SourceDomain:               normalization.SourceDomain,
		TargetDomain:               normalization.TargetDomain,
		VectorIndex:                vectorResult.VectorIndex,
		VectorField:                vectorResult.VectorField,
		Direction:                  vectorResult.Direction,
		SourceCount:                len(sourceCandidates.Rownums),
		SourceElapsed:              sourceElapsed,
		TranslationElapsed:         translationElapsed,
		ProjectionElapsed:          vectorResult.ProjectionElapsed,
		ProjectionCacheHit:         vectorResult.ProjectionCacheHit,
		SourceKeyProjectionUsed:    vectorResult.SourceKeyProjectionUsed,
		SourceKeyProjectionReason:  vectorResult.SourceKeyProjectionReason,
		SourceKeyProjectionElapsed: vectorResult.SourceKeyProjectionElapsed,
		SourceValueCount:           vectorResult.SourceValueCount,
		CandidateCacheHit:          vectorResult.CandidateCacheHit,
		CandidateCacheMode:         vectorResult.CandidateCacheMode,
		CandidateMode:              vectorResult.CandidateMode,
		CandidateElapsed:           vectorResult.CandidateElapsed,
		BatchEqualElapsed:          vectorResult.BatchEqualElapsed,
		CandidateScanElapsed:       vectorResult.CandidateScanElapsed,
		CandidateSet:               targetCandidates,
	}, diagnostics, nil
}

// KernelFilterDomainNormalizationExecutor expands a plan into leaf-level kernel calls.
type KernelFilterDomainNormalizationExecutor struct {
	Kernel           FilterDomainNormalizationKernel
	ParallelBranches bool
}

// NormalizeFilterDomains collects source-domain leaves and delegates each leaf to the kernel.
func (e KernelFilterDomainNormalizationExecutor) NormalizeFilterDomains(ctx context.Context, request ExecutionRequest, plan FilterDomainNormalizationPlan) (qsbridge.FilterDomainRewriteResult, qsbridge.DiagnosticSet, error) {
	kernel := e.Kernel
	if kernel == nil {
		kernel = UnsupportedFilterDomainNormalizationKernel{}
	}
	result := qsbridge.FilterDomainRewriteResult{TargetDomain: plan.Translation.TargetDomain}
	var diagnostics qsbridge.DiagnosticSet
	for _, normalization := range plan.Requests {
		branches := filterDomainBranchesForSource(request.Query.Filter, normalization.SourceDomain)
		if expressionKernel, ok := kernel.(filterDomainExpressionNormalizationKernel); ok {
			normalizedBranches, branchDiagnostics, err := e.normalizeBranches(ctx, request, normalization, branches, expressionKernel)
			diagnostics = append(diagnostics, branchDiagnostics...)
			if err != nil || branchDiagnostics.BlocksNative() {
				return result, diagnostics, err
			}
			for _, normalized := range normalizedBranches {
				if normalized.TargetDomain == "" {
					normalized.TargetDomain = normalization.TargetDomain
				}
				if normalized.SourceDomain == "" {
					normalized.SourceDomain = normalization.SourceDomain
				}
				result.Branches = append(result.Branches, normalized)
			}
		}
		fragments := filterDomainFragmentsForSourceExcludingBranches(request.Query.Filter, normalization.SourceDomain, branches)
		for _, fragment := range fragments {
			leaf, leafDiagnostics, err := kernel.NormalizeFilterLeaf(ctx, request, normalization, fragment)
			diagnostics = append(diagnostics, leafDiagnostics...)
			if err != nil || leafDiagnostics.BlocksNative() {
				return result, diagnostics, err
			}
			if leaf.TargetDomain == "" {
				leaf.TargetDomain = normalization.TargetDomain
			}
			if leaf.SourceDomain == "" {
				leaf.SourceDomain = normalization.SourceDomain
			}
			if leaf.OriginalFragment.Index == "" && leaf.OriginalFragment.Field == "" {
				leaf.OriginalFragment = fragment
			}
			result.Leaves = append(result.Leaves, leaf)
		}
	}
	return result, diagnostics, nil
}

func (e KernelFilterDomainNormalizationExecutor) normalizeBranches(
	ctx context.Context,
	request ExecutionRequest,
	normalization FilterDomainNormalizationRequest,
	branches []qsbridge.QuantaFilterExpression,
	kernel filterDomainExpressionNormalizationKernel,
) ([]qsbridge.FilterDomainNormalizedBranch, qsbridge.DiagnosticSet, error) {
	if len(branches) == 0 {
		return nil, nil, nil
	}
	if !e.ParallelBranches || len(branches) == 1 {
		normalized := make([]qsbridge.FilterDomainNormalizedBranch, 0, len(branches))
		var diagnostics qsbridge.DiagnosticSet
		for _, branch := range branches {
			current, branchDiagnostics, err := kernel.NormalizeFilterExpression(ctx, request, normalization, branch)
			diagnostics = append(diagnostics, branchDiagnostics...)
			if err != nil || branchDiagnostics.BlocksNative() {
				return normalized, diagnostics, err
			}
			normalized = append(normalized, current)
		}
		return normalized, diagnostics, nil
	}

	type branchResult struct {
		normalized  qsbridge.FilterDomainNormalizedBranch
		diagnostics qsbridge.DiagnosticSet
		err         error
	}
	results := make([]branchResult, len(branches))
	var wg sync.WaitGroup
	wg.Add(len(branches))
	for i, branch := range branches {
		i, branch := i, branch
		go func() {
			defer wg.Done()
			if err := ctx.Err(); err != nil {
				results[i].err = err
				return
			}
			results[i].normalized, results[i].diagnostics, results[i].err = kernel.NormalizeFilterExpression(ctx, request, normalization, branch)
		}()
	}
	wg.Wait()

	normalized := make([]qsbridge.FilterDomainNormalizedBranch, 0, len(branches))
	var diagnostics qsbridge.DiagnosticSet
	for _, result := range results {
		diagnostics = append(diagnostics, result.diagnostics...)
		if result.err != nil || result.diagnostics.BlocksNative() {
			return normalized, diagnostics, result.err
		}
		normalized = append(normalized, result.normalized)
	}
	return normalized, diagnostics, nil
}

// UnsupportedFilterDomainNormalizationKernel preserves the explicit normalization boundary.
type UnsupportedFilterDomainNormalizationKernel struct{}

// NormalizeFilterLeaf reports that a source-domain leaf cannot yet be translated natively.
func (UnsupportedFilterDomainNormalizationKernel) NormalizeFilterLeaf(_ context.Context, _ ExecutionRequest, normalization FilterDomainNormalizationRequest, fragment qsbridge.QuantaQueryFragment) (qsbridge.FilterDomainNormalizedLeaf, qsbridge.DiagnosticSet, error) {
	translation := qsbridge.QuantaFilterDomainTranslation{
		Required:      true,
		SourceDomains: []string{normalization.SourceDomain},
		TargetDomain:  normalization.TargetDomain,
		Strategies:    []qsbridge.PhysicalStrategy{normalization.Strategy},
	}
	return qsbridge.FilterDomainNormalizedLeaf{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			filterDomainTranslationDiagnosticMessage(translation)+": leaf="+fragment.Index+"."+fragment.Field,
		),
	}, nil
}

// UnsupportedFilterDomainNormalizationExecutor preserves the current explicit boundary.
type UnsupportedFilterDomainNormalizationExecutor struct{}

// NormalizeFilterDomains reports that filter-domain normalization is planned but not wired.
func (UnsupportedFilterDomainNormalizationExecutor) NormalizeFilterDomains(_ context.Context, _ ExecutionRequest, plan FilterDomainNormalizationPlan) (qsbridge.FilterDomainRewriteResult, qsbridge.DiagnosticSet, error) {
	return qsbridge.FilterDomainRewriteResult{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, filterDomainTranslationDiagnosticMessage(plan.Translation)),
	}, nil
}

// FixtureFilterDomainNormalizationExecutor records the adapter request and returns deterministic diagnostics.
type FixtureFilterDomainNormalizationExecutor struct {
	LastPlan      FilterDomainNormalizationPlan
	RewriteResult qsbridge.FilterDomainRewriteResult
	Diagnostics   qsbridge.DiagnosticSet
	Err           error
	Succeed       bool
}

// NormalizeFilterDomains captures the plan shape without executing bitmap work.
func (e *FixtureFilterDomainNormalizationExecutor) NormalizeFilterDomains(ctx context.Context, request ExecutionRequest, plan FilterDomainNormalizationPlan) (qsbridge.FilterDomainRewriteResult, qsbridge.DiagnosticSet, error) {
	if e != nil {
		e.LastPlan = plan
		if e.Succeed {
			return e.RewriteResult, e.Diagnostics, e.Err
		}
	}
	return UnsupportedFilterDomainNormalizationExecutor{}.NormalizeFilterDomains(ctx, request, plan)
}

// FixtureFilterDomainNormalizationKernel returns deterministic leaf replacements for tests.
type FixtureFilterDomainNormalizationKernel struct {
	Results     map[string]qsbridge.FilterDomainNormalizedLeaf
	Calls       []qsbridge.QuantaQueryFragment
	Diagnostics qsbridge.DiagnosticSet
	Err         error
}

// NormalizeFilterLeaf records the leaf call and returns a configured replacement.
func (k *FixtureFilterDomainNormalizationKernel) NormalizeFilterLeaf(ctx context.Context, _ ExecutionRequest, normalization FilterDomainNormalizationRequest, fragment qsbridge.QuantaQueryFragment) (qsbridge.FilterDomainNormalizedLeaf, qsbridge.DiagnosticSet, error) {
	if k == nil {
		return UnsupportedFilterDomainNormalizationKernel{}.NormalizeFilterLeaf(ctx, ExecutionRequest{}, normalization, fragment)
	}
	k.Calls = append(k.Calls, fragment)
	if k.Err != nil || k.Diagnostics.BlocksNative() {
		return qsbridge.FilterDomainNormalizedLeaf{}, k.Diagnostics, k.Err
	}
	if leaf, ok := k.Results[filterDomainFragmentKey(fragment)]; ok {
		return leaf, k.Diagnostics, nil
	}
	return qsbridge.FilterDomainNormalizedLeaf{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			"filter-domain normalizer fixture has no replacement for "+filterDomainFragmentKey(fragment),
		),
	}, nil
}

// FixtureFilterDomainRelationshipVectorTranslator returns deterministic target candidates for tests.
type FixtureFilterDomainRelationshipVectorTranslator struct {
	Reader      RelationshipVectorReader
	Vectors     map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum
	Calls       []qsbridge.FilterDomainRelationshipVectorRequest
	LastRequest qsbridge.FilterDomainRelationshipVectorRequest
	Diagnostics qsbridge.DiagnosticSet
	Err         error
}

// TranslateFilterDomain records the vector request and returns a configured target-domain candidate set.
func (t *FixtureFilterDomainRelationshipVectorTranslator) TranslateFilterDomain(ctx context.Context, request ExecutionRequest, vectorRequest qsbridge.FilterDomainRelationshipVectorRequest) (qsbridge.FilterDomainRelationshipVectorResult, qsbridge.DiagnosticSet, error) {
	if t == nil {
		return qsbridge.FilterDomainRelationshipVectorResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, filterDomainRelationshipTranslatorMissingMessage(vectorRequest)),
		}, nil
	}
	t.LastRequest = vectorRequest
	t.Calls = append(t.Calls, vectorRequest)
	if t.Err != nil || t.Diagnostics.BlocksNative() {
		return qsbridge.FilterDomainRelationshipVectorResult{}, t.Diagnostics, t.Err
	}
	reader := t.Reader
	if reader == nil {
		reader = InMemoryRelationshipVectorIndex{Vectors: t.Vectors}
	}
	result, diagnostics, err := (ReaderBackedFilterDomainRelationshipVectorTranslator{Reader: reader}).TranslateFilterDomain(ctx, request, vectorRequest)
	return result, append(t.Diagnostics, diagnostics...), err
}

func filterDomainFragmentsForSource(filter qsbridge.QuantaFilterExpression, sourceDomain string) []qsbridge.QuantaQueryFragment {
	if filter.Leaf() {
		if filter.Operation == qsbridge.QuantaFilterLeaf && filter.Fragment.Index == sourceDomain {
			return []qsbridge.QuantaQueryFragment{filter.Fragment}
		}
		return nil
	}
	var fragments []qsbridge.QuantaQueryFragment
	for _, child := range filter.Children {
		fragments = append(fragments, filterDomainFragmentsForSource(child, sourceDomain)...)
	}
	return fragments
}

func filterDomainBranchesForSource(filter qsbridge.QuantaFilterExpression, sourceDomain string) []qsbridge.QuantaFilterExpression {
	if filterDomainExpressionOnlySource(filter, sourceDomain) && filterDomainExpressionLeafCount(filter) > 1 {
		return []qsbridge.QuantaFilterExpression{filter}
	}
	if branch, ok := filterDomainIntersectSourceBranch(filter, sourceDomain); ok {
		return []qsbridge.QuantaFilterExpression{branch}
	}
	var branches []qsbridge.QuantaFilterExpression
	for _, child := range filter.Children {
		branches = append(branches, filterDomainBranchesForSource(child, sourceDomain)...)
	}
	return branches
}

func filterDomainIntersectSourceBranch(filter qsbridge.QuantaFilterExpression, sourceDomain string) (qsbridge.QuantaFilterExpression, bool) {
	if filter.Operation != qsbridge.QuantaFilterIntersect || len(filter.Children) == 0 {
		return qsbridge.QuantaFilterExpression{}, false
	}
	sourceChildren := filterDomainConjunctiveSourceExpressions(filter, sourceDomain)
	if filterDomainExpressionSliceLeafCount(sourceChildren) <= 1 {
		return qsbridge.QuantaFilterExpression{}, false
	}
	if len(sourceChildren) == 1 {
		return sourceChildren[0], true
	}
	return qsbridge.QuantaFilterExpression{
		Operation: qsbridge.QuantaFilterIntersect,
		Children:  sourceChildren,
	}, true
}

func filterDomainConjunctiveSourceExpressions(filter qsbridge.QuantaFilterExpression, sourceDomain string) []qsbridge.QuantaFilterExpression {
	if filterDomainExpressionOnlySource(filter, sourceDomain) {
		return []qsbridge.QuantaFilterExpression{filter}
	}
	if filter.Operation != qsbridge.QuantaFilterIntersect {
		return nil
	}
	var sourceChildren []qsbridge.QuantaFilterExpression
	for _, child := range filter.Children {
		sourceChildren = append(sourceChildren, filterDomainConjunctiveSourceExpressions(child, sourceDomain)...)
	}
	return sourceChildren
}

func filterDomainFragmentsForSourceExcludingBranches(filter qsbridge.QuantaFilterExpression, sourceDomain string, branches []qsbridge.QuantaFilterExpression) []qsbridge.QuantaQueryFragment {
	for _, branch := range branches {
		if filterDomainExpressionMatchesRuntime(filter, branch) || filterDomainExpressionContainsRuntime(branch, filter) {
			return nil
		}
	}
	if filter.Leaf() {
		if filter.Fragment.Index == sourceDomain {
			return []qsbridge.QuantaQueryFragment{filter.Fragment}
		}
		return nil
	}
	var fragments []qsbridge.QuantaQueryFragment
	for _, child := range filter.Children {
		fragments = append(fragments, filterDomainFragmentsForSourceExcludingBranches(child, sourceDomain, branches)...)
	}
	return fragments
}

func filterDomainExpressionContainsRuntime(container, candidate qsbridge.QuantaFilterExpression) bool {
	if filterDomainExpressionMatchesRuntime(container, candidate) {
		return true
	}
	for _, child := range container.Children {
		if filterDomainExpressionContainsRuntime(child, candidate) {
			return true
		}
	}
	return false
}

func filterDomainExpressionOnlySource(filter qsbridge.QuantaFilterExpression, sourceDomain string) bool {
	if filter.Leaf() {
		return filter.Fragment.Index == sourceDomain
	}
	if filter.CandidateSetLeaf() {
		return filter.CandidateSet.Index == sourceDomain
	}
	if filter.Empty() || len(filter.Children) == 0 {
		return false
	}
	for _, child := range filter.Children {
		if !filterDomainExpressionOnlySource(child, sourceDomain) {
			return false
		}
	}
	return true
}

func filterDomainExpressionLeafCount(filter qsbridge.QuantaFilterExpression) int {
	if filter.Leaf() {
		return 1
	}
	count := 0
	for _, child := range filter.Children {
		count += filterDomainExpressionLeafCount(child)
	}
	return count
}

func filterDomainExpressionSliceLeafCount(filters []qsbridge.QuantaFilterExpression) int {
	count := 0
	for _, filter := range filters {
		count += filterDomainExpressionLeafCount(filter)
	}
	return count
}

func firstFilterDomainFragment(filter qsbridge.QuantaFilterExpression) (qsbridge.QuantaQueryFragment, bool) {
	if filter.Leaf() {
		return filter.Fragment, true
	}
	for _, child := range filter.Children {
		if fragment, ok := firstFilterDomainFragment(child); ok {
			return fragment, true
		}
	}
	return qsbridge.QuantaQueryFragment{}, false
}

func filterDomainExpressionKey(filter qsbridge.QuantaFilterExpression) string {
	if filter.Leaf() {
		return filterDomainFragmentKey(filter.Fragment)
	}
	return string(filter.Operation)
}

func filterDomainExpressionMatchesRuntime(left, right qsbridge.QuantaFilterExpression) bool {
	if left.Operation != right.Operation {
		return false
	}
	if left.Leaf() || right.Leaf() {
		return left.Leaf() && right.Leaf() && reflect.DeepEqual(left.Fragment, right.Fragment)
	}
	if left.CandidateSetLeaf() || right.CandidateSetLeaf() {
		return left.CandidateSetLeaf() && right.CandidateSetLeaf() && reflect.DeepEqual(left.CandidateSet, right.CandidateSet)
	}
	if len(left.Children) != len(right.Children) {
		return false
	}
	for i := range left.Children {
		if !filterDomainExpressionMatchesRuntime(left.Children[i], right.Children[i]) {
			return false
		}
	}
	return true
}

func filterDomainFragmentKey(fragment qsbridge.QuantaQueryFragment) string {
	if fragment.Role != "" {
		return fragment.Index + "." + string(fragment.Role) + "." + fragment.Field
	}
	return fragment.Index + "." + fragment.Field
}

func filterDomainRelationshipVectorRequestMessage(normalization FilterDomainNormalizationRequest, fragment qsbridge.QuantaQueryFragment) string {
	switch {
	case len(normalization.RelationshipPath) == 0:
		return filterDomainRelationshipPathMissingMessage(normalization, fragment)
	case len(normalization.RelationshipPath) > 1:
		return "filter-domain normalization multi-hop relationship path is not supported yet: source=" + normalization.SourceDomain +
			" target=" + normalization.TargetDomain +
			" leaf=" + filterDomainFragmentKey(fragment)
	default:
		return "filter-domain normalization relationship direction mismatch: source=" + normalization.SourceDomain +
			" target=" + normalization.TargetDomain +
			" leaf=" + filterDomainFragmentKey(fragment)
	}
}

func filterDomainRelationshipPathMissingMessage(normalization FilterDomainNormalizationRequest, fragment qsbridge.QuantaQueryFragment) string {
	return "filter-domain normalization relationship path missing: source=" + normalization.SourceDomain +
		" target=" + normalization.TargetDomain +
		" leaf=" + filterDomainFragmentKey(fragment)
}

func filterDomainRelationshipTranslatorMissingMessage(request qsbridge.FilterDomainRelationshipVectorRequest) string {
	return "filter-domain relationship-vector translation is not wired yet: source=" + request.SourceDomain +
		" target=" + request.TargetDomain +
		" leaf=" + request.LeafName()
}

func filterDomainNoTargetCandidateSetMessage(request qsbridge.FilterDomainRelationshipVectorRequest) string {
	return "filter-domain relationship-vector translation produced no target candidate set: source=" + request.SourceDomain +
		" target=" + request.TargetDomain +
		" leaf=" + request.LeafName()
}

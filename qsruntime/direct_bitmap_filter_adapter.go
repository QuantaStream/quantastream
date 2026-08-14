package qsruntime

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

const directBitmapFilterDictionaryBitmapCandidateMaterializationLimit = 1024

// DirectBitmapFilterAdapter adapts grouped filter trees before direct bitmap execution.
type DirectBitmapFilterAdapter interface {
	AdaptFilterExpression(context.Context, ExecutionRequest) (ExecutionRequest, qsbridge.DiagnosticSet, error)
}

// DirectBitmapFilterAdapterFunc adapts a function into a direct bitmap filter adapter.
type DirectBitmapFilterAdapterFunc func(context.Context, ExecutionRequest) (ExecutionRequest, qsbridge.DiagnosticSet, error)

// AdaptFilterExpression adapts grouped filter trees through the wrapped function.
func (f DirectBitmapFilterAdapterFunc) AdaptFilterExpression(ctx context.Context, request ExecutionRequest) (ExecutionRequest, qsbridge.DiagnosticSet, error) {
	return f(ctx, request)
}

// DirectBitmapFilterTreeAdapter evaluates grouped filter trees into candidate rownums.
type DirectBitmapFilterTreeAdapter struct {
	Sessions             DirectSessionProvider
	Materializer         ProjectionMaterializer
	Materialization      ProjectionMaterializationKernel
	Normalizer           FilterDomainNormalizationExecutor
	QueryCatalog         qsbridge.QueryCatalogView
	QueryCatalogProvider func() qsbridge.QueryCatalogView
	DictionaryResolver   qsbridge.DictionaryResolver
}

// AdaptFilterExpression evaluates grouped filters and stores the resulting candidate set on the request.
func (a DirectBitmapFilterTreeAdapter) AdaptFilterExpression(ctx context.Context, request ExecutionRequest) (ExecutionRequest, qsbridge.DiagnosticSet, error) {
	request = a.requestWithFallbackQueryCatalog(request)
	if request.Query.Filter.Empty() {
		return request, nil, nil
	}
	if translation := directBitmapFilterDomainTranslation(request); translation.Required {
		var diagnostics qsbridge.DiagnosticSet
		var err error
		var rewrite qsbridge.FilterDomainRewriteResult
		normalizationStart := time.Now()
		rewrite, diagnostics, err = a.filterDomainNormalizer().NormalizeFilterDomains(ctx, request, translation.NormalizationPlan(qsbridge.FilterDomainNormalizeGroupedFilter, PlanRelationshipJoins(request.Joins)))
		normalizationElapsed := time.Since(normalizationStart)
		if err != nil || diagnostics.BlocksNative() || request.Query.Filter.Empty() {
			return request, diagnostics, err
		}
		request.Probes = append(request.Probes, ExecutionProbe{
			Section: "filter_domain",
			Name:    "normalization_elapsed",
			Value:   normalizationElapsed.String(),
			Detail:  "target=" + translation.TargetDomain,
		})
		request.Probes = append(request.Probes, directBitmapFilterDomainRewriteProbes(rewrite)...)
		request.Query.Filter = rewrite.Apply(request.Query.Filter)
		request.FilterDomain = filterDomainTranslationFromRequest(request)
		if request.FilterDomain.Required {
			return request, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhaseExecute, filterDomainIncompleteRewriteDiagnosticMessage(request.FilterDomain)),
			}, nil
		}
	}
	if a.Sessions == nil {
		return request, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "direct bitmap filter adapter has no session provider"),
		}, nil
	}
	recorder := &directBitmapFilterTreeEvaluationRecorder{}
	leafEvaluator := directBitmapFilterTreeLeafEvaluator{
		Sessions:           a.Sessions,
		Materialization:    a.projectionMaterializationKernel(),
		DictionaryResolver: a.DictionaryResolver,
		Request:            request,
		Recorder:           recorder,
	}
	evaluator := QuantaFilterTreeEvaluator{Leaves: timedQuantaFilterLeafEvaluator{
		Inner:    leafEvaluator,
		Recorder: recorder,
	}}
	evaluateStart := time.Now()
	candidates, diagnostics, err := evaluator.EvaluateFilter(ctx, request.Query.Filter)
	evaluateElapsed := time.Since(evaluateStart)
	if err != nil || diagnostics.BlocksNative() {
		return request, diagnostics, err
	}
	request.Probes = append(request.Probes, recorder.Probes()...)
	request.Probes = append(request.Probes, ExecutionProbe{
		Section: "filter_tree",
		Name:    "evaluation_elapsed",
		Value:   evaluateElapsed.String(),
		Detail:  "",
	})
	if candidates.Index == "" {
		if root, ok := request.RootIndex(); ok {
			candidates.Index = root
		}
	}
	request.Probes = append(request.Probes, ExecutionProbe{
		Section: "filter_tree",
		Name:    "candidate_rows",
		Value:   strconv.Itoa(len(candidates.Rownums)),
		Detail:  "index=" + candidates.Index,
	})
	return request.WithCandidateSet(candidates), nil, nil
}

func (a DirectBitmapFilterTreeAdapter) projectionMaterializationKernel() ProjectionMaterializationKernel {
	if a.Materialization != nil {
		return a.Materialization
	}
	if a.Materializer != nil {
		return ProjectionMaterializerKernelAdapter{Materializer: a.Materializer}
	}
	return nil
}

func (a DirectBitmapFilterTreeAdapter) filterDomainNormalizer() FilterDomainNormalizationExecutor {
	if a.Normalizer != nil {
		return a.Normalizer
	}
	return UnsupportedFilterDomainNormalizationExecutor{}
}

func (a DirectBitmapFilterTreeAdapter) requestWithFallbackQueryCatalog(request ExecutionRequest) ExecutionRequest {
	if len(request.QueryCatalog.Tables) != 0 {
		return request
	}
	queryCatalog := a.QueryCatalog
	if len(queryCatalog.Tables) == 0 && a.QueryCatalogProvider != nil {
		queryCatalog = a.QueryCatalogProvider()
	}
	if len(queryCatalog.Tables) > 0 {
		request.QueryCatalog = queryCatalog
	}
	return request
}

func directBitmapFilterDomainTranslation(request ExecutionRequest) qsbridge.QuantaFilterDomainTranslation {
	if target, ok := directBitmapRelationshipVectorChildTarget(request); ok {
		if request.FilterDomain.TargetDomain != "" && request.FilterDomain.TargetDomain != target {
			return request.Query.Filter.DomainSummary().TranslationRequirement(target)
		}
	}
	if request.FilterDomain.Required || request.FilterDomain.TargetDomain != "" || len(request.FilterDomain.SourceDomains) > 0 || len(request.FilterDomain.Strategies) > 0 {
		return request.FilterDomain
	}
	target, _ := directBitmapFilterDomainTarget(request)
	return request.Query.Filter.DomainSummary().TranslationRequirement(target)
}

func directBitmapFilterDomainTarget(request ExecutionRequest) (string, bool) {
	if target, ok := directBitmapRelationshipVectorChildTarget(request); ok {
		return target, true
	}
	return request.RootIndex()
}

func directBitmapRelationshipVectorChildTarget(request ExecutionRequest) (string, bool) {
	plan := PlanRelationshipJoins(request.Joins)
	if !plan.NeedsRelationshipVectorExecution() || len(plan.Edges) != 1 {
		return "", false
	}
	edge := plan.Edges[0]
	if edge.ExecutionKind != qsbridge.RelationshipJoinExecutionVector {
		return "", false
	}
	switch {
	case edge.Left.Encoding.Kind == qsbridge.EncodingRelation && edge.Left.Table.Table != "":
		return edge.Left.Table.Table, true
	case edge.Right.Encoding.Kind == qsbridge.EncodingRelation && edge.Right.Table.Table != "":
		return edge.Right.Table.Table, true
	default:
		return "", false
	}
}

func filterDomainTranslationDiagnosticMessage(translation qsbridge.QuantaFilterDomainTranslation) string {
	message := "grouped filter expression spans multiple rownum domains and requires relationship-vector normalization"
	details := make([]string, 0, 3)
	if len(translation.SourceDomains) > 0 {
		details = append(details, "sources="+strings.Join(translation.SourceDomains, ","))
	}
	if translation.TargetDomain != "" {
		details = append(details, "target="+translation.TargetDomain)
	}
	if len(translation.Strategies) > 0 {
		details = append(details, "strategies="+filterDomainTranslationStrategiesValue(translation.Strategies))
	}
	if len(details) == 0 {
		return message
	}
	return message + ": " + strings.Join(details, " ")
}

func filterDomainIncompleteRewriteDiagnosticMessage(translation qsbridge.QuantaFilterDomainTranslation) string {
	message := "filter-domain normalization incomplete after rewrite"
	details := make([]string, 0, 3)
	if len(translation.SourceDomains) > 0 {
		details = append(details, "remaining_sources="+strings.Join(filterDomainRemainingSourceDomains(translation), ","))
	}
	if translation.TargetDomain != "" {
		details = append(details, "target="+translation.TargetDomain)
	}
	if len(translation.Strategies) > 0 {
		details = append(details, "strategies="+filterDomainTranslationStrategiesValue(translation.Strategies))
	}
	if len(details) == 0 {
		return message
	}
	return message + ": " + strings.Join(details, " ")
}

func filterDomainRemainingSourceDomains(translation qsbridge.QuantaFilterDomainTranslation) []string {
	domains := make([]string, 0, len(translation.SourceDomains))
	for _, domain := range translation.SourceDomains {
		if domain == "" || domain == translation.TargetDomain {
			continue
		}
		domains = append(domains, domain)
	}
	return domains
}

func filterDomainTranslationStrategiesValue(strategies []qsbridge.PhysicalStrategy) string {
	values := make([]string, 0, len(strategies))
	for _, strategy := range strategies {
		values = append(values, string(strategy))
	}
	return strings.Join(values, ",")
}

func directBitmapFilterDomainRewriteProbes(rewrite qsbridge.FilterDomainRewriteResult) []ExecutionProbe {
	if len(rewrite.Branches) == 0 && len(rewrite.Leaves) == 0 {
		return nil
	}
	probes := make([]ExecutionProbe, 0, len(rewrite.Branches)+len(rewrite.Leaves))
	for i, branch := range rewrite.Branches {
		probes = append(probes, ExecutionProbe{
			Section: "filter_domain",
			Name:    "normalized_branch",
			Value:   strconv.Itoa(len(branch.CandidateSet.Rownums)),
			Detail:  directBitmapFilterDomainBranchProbeDetail(branch),
		})
		probes = append(probes, directBitmapFilterDomainBranchExpansionProbes(i+1, branch)...)
		probes = append(probes, directBitmapFilterDomainScopedProbes("branch_"+directBitmapFilterDomainProbeOrdinal(i+1), branch.Probes)...)
	}
	for i, leaf := range rewrite.Leaves {
		probes = append(probes, ExecutionProbe{
			Section: "filter_domain",
			Name:    "normalized_leaf",
			Value:   strconv.Itoa(len(leaf.CandidateSet.Rownums)),
			Detail:  directBitmapFilterDomainLeafProbeDetail(leaf),
		})
		probes = append(probes, directBitmapFilterDomainLeafExpansionProbes(i+1, leaf)...)
		probes = append(probes, directBitmapFilterDomainScopedProbes("leaf_"+directBitmapFilterDomainProbeOrdinal(i+1), leaf.Probes)...)
	}
	return probes
}

func directBitmapFilterDomainScopedProbes(scope string, probes []ExecutionProbe) []ExecutionProbe {
	if len(probes) == 0 {
		return nil
	}
	scoped := make([]ExecutionProbe, 0, len(probes))
	for _, probe := range probes {
		if probe.Section == "" || probe.Name == "" {
			continue
		}
		probe.Detail = directBitmapFilterProbeDetailAppend(probe.Detail, "normalization="+scope)
		scoped = append(scoped, probe)
	}
	return scoped
}

func directBitmapFilterDomainBranchExpansionProbes(index int, branch qsbridge.FilterDomainNormalizedBranch) []ExecutionProbe {
	prefix := "branch_" + directBitmapFilterDomainProbeOrdinal(index) + "_"
	detail := directBitmapFilterDomainExpansionProbeDetail(
		branch.SourceDomain,
		branch.TargetDomain,
		branch.Direction,
		branch.VectorIndex,
		branch.VectorField,
		branch.CandidateSet.Index,
	)
	return directBitmapFilterDomainExpansionProbes(prefix, detail, filterDomainExpansionProbeValues{
		sourceRows:                 branch.SourceCount,
		targetRows:                 len(branch.CandidateSet.Rownums),
		sourceElapsed:              branch.SourceElapsed,
		translationElapsed:         branch.TranslationElapsed,
		projectionElapsed:          branch.ProjectionElapsed,
		projectionCacheHit:         branch.ProjectionCacheHit,
		sourceKeyProjectionUsed:    branch.SourceKeyProjectionUsed,
		sourceKeyProjectionElapsed: branch.SourceKeyProjectionElapsed,
		sourceValues:               branch.SourceValueCount,
		candidateCacheHit:          branch.CandidateCacheHit,
		candidateCacheMode:         branch.CandidateCacheMode,
		candidateMode:              branch.CandidateMode,
		candidateElapsed:           branch.CandidateElapsed,
		batchEqualElapsed:          branch.BatchEqualElapsed,
		candidateScanElapsed:       branch.CandidateScanElapsed,
		candidateDirectBorrow:      branch.CandidateDirectBorrowElapsed,
		candidateDirectQuery:       branch.CandidateDirectQueryElapsed,
		candidateDirectRelease:     branch.CandidateDirectReleaseElapsed,
		candidateDirectFragments:   branch.CandidateDirectFragments,
		candidateDirectRows:        branch.CandidateDirectRows,
	})
}

func directBitmapFilterDomainLeafExpansionProbes(index int, leaf qsbridge.FilterDomainNormalizedLeaf) []ExecutionProbe {
	prefix := "leaf_" + directBitmapFilterDomainProbeOrdinal(index) + "_"
	detail := directBitmapFilterDomainExpansionProbeDetail(
		leaf.SourceDomain,
		leaf.TargetDomain,
		leaf.Direction,
		leaf.VectorIndex,
		leaf.VectorField,
		leaf.CandidateSet.Index,
	)
	return directBitmapFilterDomainExpansionProbes(prefix, detail, filterDomainExpansionProbeValues{
		sourceRows:                 leaf.SourceCount,
		targetRows:                 len(leaf.CandidateSet.Rownums),
		sourceElapsed:              leaf.SourceElapsed,
		translationElapsed:         leaf.TranslationElapsed,
		projectionElapsed:          leaf.ProjectionElapsed,
		projectionCacheHit:         leaf.ProjectionCacheHit,
		sourceKeyProjectionUsed:    leaf.SourceKeyProjectionUsed,
		sourceKeyProjectionElapsed: leaf.SourceKeyProjectionElapsed,
		sourceValues:               leaf.SourceValueCount,
		candidateCacheHit:          leaf.CandidateCacheHit,
		candidateCacheMode:         leaf.CandidateCacheMode,
		candidateMode:              leaf.CandidateMode,
		candidateElapsed:           leaf.CandidateElapsed,
		batchEqualElapsed:          leaf.BatchEqualElapsed,
		candidateScanElapsed:       leaf.CandidateScanElapsed,
		candidateDirectBorrow:      leaf.CandidateDirectBorrowElapsed,
		candidateDirectQuery:       leaf.CandidateDirectQueryElapsed,
		candidateDirectRelease:     leaf.CandidateDirectReleaseElapsed,
		candidateDirectFragments:   leaf.CandidateDirectFragments,
		candidateDirectRows:        leaf.CandidateDirectRows,
	})
}

type filterDomainExpansionProbeValues struct {
	sourceRows                 int
	targetRows                 int
	sourceElapsed              time.Duration
	translationElapsed         time.Duration
	projectionElapsed          time.Duration
	projectionCacheHit         bool
	sourceKeyProjectionUsed    bool
	sourceKeyProjectionElapsed time.Duration
	sourceValues               int
	candidateCacheHit          bool
	candidateCacheMode         string
	candidateMode              string
	candidateElapsed           time.Duration
	batchEqualElapsed          time.Duration
	candidateScanElapsed       time.Duration
	candidateDirectBorrow      time.Duration
	candidateDirectQuery       time.Duration
	candidateDirectRelease     time.Duration
	candidateDirectFragments   int
	candidateDirectRows        int
}

func directBitmapFilterDomainExpansionProbes(prefix, detail string, values filterDomainExpansionProbeValues) []ExecutionProbe {
	return []ExecutionProbe{
		directBitmapFilterDomainExpansionProbe(prefix+"source_rows", strconv.Itoa(values.sourceRows), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"target_rows", strconv.Itoa(values.targetRows), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"source_elapsed", values.sourceElapsed.String(), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"translation_elapsed", values.translationElapsed.String(), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"projection_elapsed", values.projectionElapsed.String(), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"projection_cache_hit", strconv.FormatBool(values.projectionCacheHit), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"source_key_projection_used", strconv.FormatBool(values.sourceKeyProjectionUsed), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"source_key_projection_elapsed", values.sourceKeyProjectionElapsed.String(), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"source_values", strconv.Itoa(values.sourceValues), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"candidate_cache_hit", strconv.FormatBool(values.candidateCacheHit), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"candidate_cache_mode", values.candidateCacheMode, detail),
		directBitmapFilterDomainExpansionProbe(prefix+"candidate_mode", values.candidateMode, detail),
		directBitmapFilterDomainExpansionProbe(prefix+"candidate_elapsed", values.candidateElapsed.String(), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"batch_equal_elapsed", values.batchEqualElapsed.String(), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"candidate_scan_elapsed", values.candidateScanElapsed.String(), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"candidate_direct_borrow_elapsed", values.candidateDirectBorrow.String(), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"candidate_direct_query_elapsed", values.candidateDirectQuery.String(), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"candidate_direct_release_elapsed", values.candidateDirectRelease.String(), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"candidate_direct_fragments", strconv.Itoa(values.candidateDirectFragments), detail),
		directBitmapFilterDomainExpansionProbe(prefix+"candidate_direct_rows", strconv.Itoa(values.candidateDirectRows), detail),
	}
}

func directBitmapFilterDomainExpansionProbe(name, value, detail string) ExecutionProbe {
	return ExecutionProbe{
		Section: "filter_domain_expansion",
		Name:    name,
		Value:   value,
		Detail:  detail,
	}
}

func directBitmapFilterDomainExpansionProbeDetail(source, target string, direction qsbridge.FilterDomainRelationshipVectorDirection, vectorIndex, vectorField, targetIndex string) string {
	details := []string{
		"source=" + source,
		"target=" + target,
		"direction=" + string(direction),
		"vector=" + vectorIndex + "." + vectorField,
		"target_index=" + targetIndex,
	}
	return strings.Join(details, " ")
}

func directBitmapFilterDomainProbeOrdinal(index int) string {
	if index < 10 {
		return "00" + strconv.Itoa(index)
	}
	if index < 100 {
		return "0" + strconv.Itoa(index)
	}
	return strconv.Itoa(index)
}

func directBitmapFilterDomainBranchProbeDetail(branch qsbridge.FilterDomainNormalizedBranch) string {
	details := []string{
		"source=" + branch.SourceDomain,
		"target=" + branch.TargetDomain,
		"direction=" + string(branch.Direction),
		"vector=" + branch.VectorIndex + "." + branch.VectorField,
		"source_rows=" + strconv.Itoa(branch.SourceCount),
		"source_elapsed=" + branch.SourceElapsed.String(),
		"translation_elapsed=" + branch.TranslationElapsed.String(),
		"projection_elapsed=" + branch.ProjectionElapsed.String(),
		"projection_cache_hit=" + strconv.FormatBool(branch.ProjectionCacheHit),
		"source_key_projection_used=" + strconv.FormatBool(branch.SourceKeyProjectionUsed),
		"source_key_projection_reason=" + branch.SourceKeyProjectionReason,
		"source_key_projection_elapsed=" + branch.SourceKeyProjectionElapsed.String(),
		"source_values=" + strconv.Itoa(branch.SourceValueCount),
		"candidate_cache_hit=" + strconv.FormatBool(branch.CandidateCacheHit),
		"candidate_cache_mode=" + branch.CandidateCacheMode,
		"candidate_mode=" + branch.CandidateMode,
		"candidate_elapsed=" + branch.CandidateElapsed.String(),
		"batch_equal_elapsed=" + branch.BatchEqualElapsed.String(),
		"candidate_scan_elapsed=" + branch.CandidateScanElapsed.String(),
		"candidate_fanout_elapsed=" + branch.CandidateFanoutElapsed.String(),
		"candidate_client_rpc_elapsed=" + branch.CandidateClientRPCElapsed.String(),
		"candidate_client_rpc_max_elapsed=" + branch.CandidateClientRPCMaxElapsed.String(),
		"candidate_response_merge_elapsed=" + branch.CandidateResponseMergeElapsed.String(),
		"candidate_direct_borrow_elapsed=" + branch.CandidateDirectBorrowElapsed.String(),
		"candidate_direct_query_elapsed=" + branch.CandidateDirectQueryElapsed.String(),
		"candidate_direct_release_elapsed=" + branch.CandidateDirectReleaseElapsed.String(),
		"candidate_direct_fragments=" + strconv.Itoa(branch.CandidateDirectFragments),
		"candidate_direct_rows=" + strconv.Itoa(branch.CandidateDirectRows),
		"target_index=" + branch.CandidateSet.Index,
	}
	return strings.Join(details, " ")
}

func directBitmapFilterDomainLeafProbeDetail(leaf qsbridge.FilterDomainNormalizedLeaf) string {
	details := []string{
		"source=" + leaf.SourceDomain,
		"target=" + leaf.TargetDomain,
		"leaf=" + filterDomainFragmentKey(leaf.OriginalFragment),
		"direction=" + string(leaf.Direction),
		"vector=" + leaf.VectorIndex + "." + leaf.VectorField,
		"source_rows=" + strconv.Itoa(leaf.SourceCount),
		"source_elapsed=" + leaf.SourceElapsed.String(),
		"translation_elapsed=" + leaf.TranslationElapsed.String(),
		"projection_elapsed=" + leaf.ProjectionElapsed.String(),
		"projection_cache_hit=" + strconv.FormatBool(leaf.ProjectionCacheHit),
		"source_key_projection_used=" + strconv.FormatBool(leaf.SourceKeyProjectionUsed),
		"source_key_projection_reason=" + leaf.SourceKeyProjectionReason,
		"source_key_projection_elapsed=" + leaf.SourceKeyProjectionElapsed.String(),
		"source_values=" + strconv.Itoa(leaf.SourceValueCount),
		"candidate_cache_hit=" + strconv.FormatBool(leaf.CandidateCacheHit),
		"candidate_cache_mode=" + leaf.CandidateCacheMode,
		"candidate_mode=" + leaf.CandidateMode,
		"candidate_elapsed=" + leaf.CandidateElapsed.String(),
		"batch_equal_elapsed=" + leaf.BatchEqualElapsed.String(),
		"candidate_scan_elapsed=" + leaf.CandidateScanElapsed.String(),
		"candidate_fanout_elapsed=" + leaf.CandidateFanoutElapsed.String(),
		"candidate_client_rpc_elapsed=" + leaf.CandidateClientRPCElapsed.String(),
		"candidate_client_rpc_max_elapsed=" + leaf.CandidateClientRPCMaxElapsed.String(),
		"candidate_response_merge_elapsed=" + leaf.CandidateResponseMergeElapsed.String(),
		"candidate_direct_borrow_elapsed=" + leaf.CandidateDirectBorrowElapsed.String(),
		"candidate_direct_query_elapsed=" + leaf.CandidateDirectQueryElapsed.String(),
		"candidate_direct_release_elapsed=" + leaf.CandidateDirectReleaseElapsed.String(),
		"candidate_direct_fragments=" + strconv.Itoa(leaf.CandidateDirectFragments),
		"candidate_direct_rows=" + strconv.Itoa(leaf.CandidateDirectRows),
		"target_index=" + leaf.CandidateSet.Index,
	}
	return strings.Join(details, " ")
}

type directBitmapFilterTreeLeafEvaluator struct {
	Sessions           DirectSessionProvider
	Materialization    ProjectionMaterializationKernel
	DictionaryResolver qsbridge.DictionaryResolver
	Request            ExecutionRequest
	Recorder           *directBitmapFilterTreeEvaluationRecorder
}

type timedQuantaFilterLeafEvaluator struct {
	Inner    QuantaFilterLeafEvaluator
	Recorder *directBitmapFilterTreeEvaluationRecorder
}

// EvaluateFilterLeaf records elapsed time around the wrapped leaf evaluator.
func (e timedQuantaFilterLeafEvaluator) EvaluateFilterLeaf(ctx context.Context, fragment qsbridge.QuantaQueryFragment) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	start := time.Now()
	candidates, diagnostics, err := e.Inner.EvaluateFilterLeaf(ctx, fragment)
	elapsed := time.Since(start)
	if e.Recorder != nil {
		e.Recorder.Record(fragment, candidates, elapsed)
	}
	return candidates, diagnostics, err
}

// EvaluateFilterLeafWithinCandidateSet records elapsed time around constrained leaf evaluation.
func (e timedQuantaFilterLeafEvaluator) EvaluateFilterLeafWithinCandidateSet(ctx context.Context, fragment qsbridge.QuantaQueryFragment, candidates qsbridge.QuantaCandidateSet) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	constrained, ok := e.Inner.(QuantaConstrainedFilterLeafEvaluator)
	if !ok {
		return e.EvaluateFilterLeaf(ctx, fragment)
	}
	start := time.Now()
	filtered, diagnostics, err := constrained.EvaluateFilterLeafWithinCandidateSet(ctx, fragment, candidates)
	elapsed := time.Since(start)
	if e.Recorder != nil {
		e.Recorder.Record(fragment, filtered, elapsed)
	}
	return filtered, diagnostics, err
}

type directBitmapFilterTreeEvaluationRecorder struct {
	leaves      []directBitmapFilterTreeLeafProbe
	innerProbes []ExecutionProbe
}

type directBitmapFilterTreeLeafProbe struct {
	Fragment qsbridge.QuantaQueryFragment
	Rows     int
	Index    string
	Duration time.Duration
}

type directBitmapFilterFragmentMaterializationDecisionResult struct {
	Materialize       bool
	Reason            string
	ProbeDetail       string
	HasBitmapFragment bool
	BitmapFragment    qsbridge.QuantaQueryFragment
}

type directBitmapFilterFragmentStringEnumDetection struct {
	usesStringEnum bool
	shouldProbe    bool
	detail         string
	table          qsbridge.QueryTableView
	definition     qsbridge.FieldDefinition
}

// Record captures timing and output cardinality for one filter-tree leaf.
func (r *directBitmapFilterTreeEvaluationRecorder) Record(fragment qsbridge.QuantaQueryFragment, candidates qsbridge.QuantaCandidateSet, elapsed time.Duration) {
	if r == nil {
		return
	}
	r.leaves = append(r.leaves, directBitmapFilterTreeLeafProbe{
		Fragment: fragment,
		Rows:     len(candidates.Rownums),
		Index:    candidates.Index,
		Duration: elapsed,
	})
}

// RecordInnerProbes keeps lower-level bitmap/materialization timings attached
// to the grouped-filter leaf that caused them.
func (r *directBitmapFilterTreeEvaluationRecorder) RecordInnerProbes(fragment qsbridge.QuantaQueryFragment, source string, probes []ExecutionProbe) {
	if r == nil || len(probes) == 0 {
		return
	}
	for _, probe := range probes {
		if probe.Section == "" || probe.Name == "" {
			continue
		}
		probe.Detail = directBitmapFilterTreeInnerProbeDetail(fragment, source, probe.Detail)
		r.innerProbes = append(r.innerProbes, probe)
	}
}

// RecordLeafMode records which execution path was selected for one filter-tree
// leaf before lower-level probes are available.
func (r *directBitmapFilterTreeEvaluationRecorder) RecordLeafMode(fragment qsbridge.QuantaQueryFragment, source string, inputRows int, reason string) {
	if r == nil || source == "" {
		return
	}
	parts := []string{"leaf=" + filterDomainFragmentKey(fragment), "source=" + source}
	if inputRows >= 0 {
		parts = append(parts, "input_rows="+strconv.Itoa(inputRows))
	}
	if reason != "" {
		parts = append(parts, "reason="+reason)
	}
	r.innerProbes = append(r.innerProbes, ExecutionProbe{
		Section: "filter_tree",
		Name:    "leaf_evaluation_mode",
		Value:   source,
		Detail:  strings.Join(parts, " "),
	})
}

// RecordCandidateBitmapQuery records whether a constrained bitmap leaf used the
// candidate-aware session hook or fell back to an unconstrained bitmap query.
func (r *directBitmapFilterTreeEvaluationRecorder) RecordCandidateBitmapQuery(fragment qsbridge.QuantaQueryFragment, candidates qsbridge.QuantaCandidateSet, supported bool, handled bool, sessionType string, reason string) {
	if r == nil {
		return
	}
	parts := []string{
		"leaf=" + filterDomainFragmentKey(fragment),
		"candidate_rows=" + strconv.Itoa(candidates.CandidateCount()),
		"candidate_index=" + candidates.Index,
	}
	if sessionType != "" {
		parts = append(parts, "session_type="+sessionType)
	}
	if reason != "" {
		parts = append(parts, "reason="+reason)
	}
	detail := strings.Join(parts, " ")
	r.innerProbes = append(r.innerProbes,
		ExecutionProbe{
			Section: "filter_tree",
			Name:    "candidate_bitmap_query_supported",
			Value:   strconv.FormatBool(supported),
			Detail:  detail,
		},
		ExecutionProbe{
			Section: "filter_tree",
			Name:    "candidate_bitmap_query_handled",
			Value:   strconv.FormatBool(handled),
			Detail:  detail,
		},
	)
}

// RecordLeafMaterializationDecision records the catalog evidence behind the
// bitmap-vs-materialization choice for string literal leaves.
func (r *directBitmapFilterTreeEvaluationRecorder) RecordLeafMaterializationDecision(fragment qsbridge.QuantaQueryFragment, decision directBitmapFilterFragmentMaterializationDecisionResult) {
	if r == nil || decision.ProbeDetail == "" {
		return
	}
	r.innerProbes = append(r.innerProbes, ExecutionProbe{
		Section: "filter_tree",
		Name:    "leaf_materialization_decision",
		Value:   decision.Reason,
		Detail:  "leaf=" + filterDomainFragmentKey(fragment) + " " + decision.ProbeDetail,
	})
}

// Probes returns execution probes for the recorded filter-tree leaves.
func (r *directBitmapFilterTreeEvaluationRecorder) Probes() []ExecutionProbe {
	if r == nil || (len(r.leaves) == 0 && len(r.innerProbes) == 0) {
		return nil
	}
	probes := make([]ExecutionProbe, 0, len(r.leaves)+len(r.innerProbes))
	for _, leaf := range r.leaves {
		probes = append(probes, ExecutionProbe{
			Section: "filter_tree",
			Name:    "leaf_rows",
			Value:   strconv.Itoa(leaf.Rows),
			Detail: "leaf=" + filterDomainFragmentKey(leaf.Fragment) +
				" index=" + leaf.Index +
				" elapsed=" + leaf.Duration.String(),
		})
	}
	probes = append(probes, r.innerProbes...)
	return probes
}

func directBitmapFilterTreeInnerProbeDetail(fragment qsbridge.QuantaQueryFragment, source string, detail string) string {
	parts := []string{"leaf=" + filterDomainFragmentKey(fragment)}
	if source != "" {
		parts = append(parts, "source="+source)
	}
	if detail != "" {
		parts = append(parts, detail)
	}
	return strings.Join(parts, " ")
}

// EvaluateFilterLeaf executes one grouped-filter leaf as a normal bitmap query fragment.
func (e directBitmapFilterTreeLeafEvaluator) EvaluateFilterLeaf(ctx context.Context, fragment qsbridge.QuantaQueryFragment) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	decision := directBitmapFilterFragmentMaterializationDecisionDetailsWithResolver(e.Request, fragment, e.DictionaryResolver)
	e.recordLeafMaterializationDecision(fragment, decision)
	if decision.HasBitmapFragment {
		return e.evaluateFilterLeafBitmap(ctx, decision.BitmapFragment, -1, decision.Reason)
	}
	return e.evaluateFilterLeafBitmap(ctx, fragment, -1, "")
}

func (e directBitmapFilterTreeLeafEvaluator) evaluateFilterLeafBitmap(ctx context.Context, fragment qsbridge.QuantaQueryFragment, inputRows int, reason string) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	return e.evaluateFilterLeafBitmapWithCandidateSet(ctx, fragment, nil, inputRows, reason)
}

func (e directBitmapFilterTreeLeafEvaluator) evaluateFilterLeafBitmapWithCandidateSet(ctx context.Context, fragment qsbridge.QuantaQueryFragment, candidates *qsbridge.QuantaCandidateSet, inputRows int, reason string) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	e.recordLeafMode(fragment, "bitmap_query", inputRows, reason)
	leafRequest := e.Request
	leafRequest.Query = cloneIntermediateQuery(e.Request.Query)
	leafRequest.Query.Filter = qsbridge.QuantaFilterExpression{}
	leafRequest.Query.Fragments = []qsbridge.QuantaQueryFragment{fragment}
	leafRequest.HasCandidateSet = false
	leafRequest.CandidateSet = qsbridge.QuantaCandidateSet{}
	session, diagnostics, err := e.Sessions.BorrowDirectSession(ctx, leafRequest)
	if err != nil || diagnostics.BlocksNative() {
		return qsbridge.QuantaCandidateSet{}, diagnostics, err
	}
	if session == nil {
		return qsbridge.QuantaCandidateSet{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "direct bitmap filter adapter received nil session"),
		}, nil
	}
	var result BitmapQueryResult
	var queryDiagnostics qsbridge.DiagnosticSet
	var queryErr error
	handled := false
	if candidates != nil {
		candidateSupported := false
		candidateReason := "unsupported_session"
		sessionType := fmt.Sprintf("%T", session)
		if candidateSession, ok := session.(DirectCandidateBitmapSessionHandle); ok {
			candidateSupported = true
			result, queryDiagnostics, handled, queryErr = candidateSession.QueryBitmapWithCandidateSet(ctx, leafRequest, *candidates)
			switch {
			case queryErr != nil:
				candidateReason = "error"
			case queryDiagnostics.BlocksNative():
				candidateReason = "diagnostic"
			case handled:
				candidateReason = "handled"
			default:
				candidateReason = "session_unhandled"
			}
		}
		e.recordCandidateBitmapQuery(fragment, *candidates, candidateSupported, handled, sessionType, candidateReason)
	}
	if !handled && queryErr == nil && !queryDiagnostics.BlocksNative() {
		result, queryDiagnostics, queryErr = session.QueryBitmap(ctx, leafRequest)
	}
	releaseDiagnostics := session.Release(ctx)
	e.recordInnerProbes(fragment, "bitmap_query", result.Probes)
	diagnostics = append(diagnostics, queryDiagnostics...)
	diagnostics = append(diagnostics, releaseDiagnostics...)
	if queryErr != nil || diagnostics.BlocksNative() {
		return qsbridge.QuantaCandidateSet{}, diagnostics, queryErr
	}
	index := fragment.Index
	if index == "" {
		index, _ = leafRequest.RootIndex()
	}
	return qsbridge.QuantaCandidateSet{Index: index, Rownums: append([]qsbridge.QuantaRownum(nil), result.Rownums...)}, diagnostics, nil
}

// EvaluateFilterLeafWithinCandidateSet evaluates one leaf against already-narrowed candidates through late materialization.
func (e directBitmapFilterTreeLeafEvaluator) EvaluateFilterLeafWithinCandidateSet(ctx context.Context, fragment qsbridge.QuantaQueryFragment, candidates qsbridge.QuantaCandidateSet) (qsbridge.QuantaCandidateSet, qsbridge.DiagnosticSet, error) {
	if e.Materialization == nil {
		return e.evaluateFilterLeafBitmap(ctx, fragment, len(candidates.Rownums), "materialization_unavailable")
	}
	if len(candidates.Rownums) == 0 {
		return e.evaluateFilterLeafBitmap(ctx, fragment, 0, "empty_candidate_set")
	}
	index := fragment.Index
	if index == "" {
		index = candidates.Index
	}
	if index == "" || (candidates.Index != "" && candidates.Index != index) {
		return e.evaluateFilterLeafBitmap(ctx, fragment, len(candidates.Rownums), "candidate_index_mismatch")
	}
	decision := directBitmapFilterFragmentMaterializationDecisionDetailsWithResolver(e.Request, fragment, e.DictionaryResolver)
	e.recordLeafMaterializationDecision(fragment, decision)
	if !decision.Materialize {
		if directBitmapFilterShouldMaterializeDictionaryBitmapWithinCandidates(decision, candidates.CandidateCount()) {
			decision.Materialize = true
			decision.Reason = "string_enum_dictionary_bitmap_candidate_materialization"
		} else {
			bitmapFragment := fragment
			if decision.HasBitmapFragment {
				bitmapFragment = decision.BitmapFragment
			}
			leafCandidates := candidates
			if leafCandidates.Index == "" {
				leafCandidates.Index = index
			}
			return e.evaluateFilterLeafBitmapWithCandidateSet(ctx, bitmapFragment, &leafCandidates, len(candidates.Rownums), decision.Reason)
		}
	}
	if decision.Materialize {
		e.recordLeafMode(fragment, "constrained_materialization", len(candidates.Rownums), decision.Reason)
		materialization := candidates.MaterializationRequest([]qsbridge.QuantaProjectionField{{
			Index: index,
			Field: fragment.Field,
			Type:  directBitmapFilterFragmentDataType(fragment),
		}})
		rowSet, diagnostics, materializationProbes, err := directBitmapMaterializeWithKernel(ctx, e.Materialization, materialization)
		e.recordInnerProbes(fragment, "constrained_materialization", materializationProbes)
		if err != nil || diagnostics.BlocksNative() {
			return qsbridge.QuantaCandidateSet{}, diagnostics, err
		}
		if len(rowSet.ProjectionVectors) != 1 {
			return qsbridge.QuantaCandidateSet{}, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "constrained filter leaf materialization returned an unexpected projection shape"),
			}, nil
		}
		values := rowSet.ProjectionVectors[0].Values
		rownums := make([]qsbridge.QuantaRownum, 0, len(rowSet.Rownums))
		for i, rownum := range rowSet.Rownums {
			if i >= len(values) {
				return qsbridge.QuantaCandidateSet{}, qsbridge.DiagnosticSet{
					qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "constrained filter leaf materialization returned fewer values than rownums"),
				}, nil
			}
			if directBitmapFilterFragmentMatchesCell(fragment, values[i]) {
				rownums = append(rownums, rownum)
			}
		}
		return qsbridge.QuantaCandidateSet{Index: index, Rownums: rownums}, nil, nil
	}
	return qsbridge.QuantaCandidateSet{}, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "filter leaf did not select a bitmap or materialized evaluation path"),
	}, nil
}

func directBitmapFilterShouldMaterializeDictionaryBitmapWithinCandidates(decision directBitmapFilterFragmentMaterializationDecisionResult, candidateRows int) bool {
	return candidateRows > 0 &&
		candidateRows <= directBitmapFilterDictionaryBitmapCandidateMaterializationLimit &&
		decision.HasBitmapFragment &&
		decision.Reason == "string_enum_dictionary_bitmap"
}

func (e directBitmapFilterTreeLeafEvaluator) recordInnerProbes(fragment qsbridge.QuantaQueryFragment, source string, probes []ExecutionProbe) {
	if e.Recorder == nil {
		return
	}
	e.Recorder.RecordInnerProbes(fragment, source, probes)
}

func (e directBitmapFilterTreeLeafEvaluator) recordLeafMode(fragment qsbridge.QuantaQueryFragment, source string, inputRows int, reason string) {
	if e.Recorder == nil {
		return
	}
	e.Recorder.RecordLeafMode(fragment, source, inputRows, reason)
}

func (e directBitmapFilterTreeLeafEvaluator) recordLeafMaterializationDecision(fragment qsbridge.QuantaQueryFragment, decision directBitmapFilterFragmentMaterializationDecisionResult) {
	if e.Recorder == nil {
		return
	}
	e.Recorder.RecordLeafMaterializationDecision(fragment, decision)
}

func (e directBitmapFilterTreeLeafEvaluator) recordCandidateBitmapQuery(fragment qsbridge.QuantaQueryFragment, candidates qsbridge.QuantaCandidateSet, supported bool, handled bool, sessionType string, reason string) {
	if e.Recorder == nil {
		return
	}
	e.Recorder.RecordCandidateBitmapQuery(fragment, candidates, supported, handled, sessionType, reason)
}

func directBitmapFilterFragmentShouldEvaluateMaterialized(request ExecutionRequest, fragment qsbridge.QuantaQueryFragment) bool {
	materialize, _ := directBitmapFilterFragmentMaterializationDecision(request, fragment)
	return materialize
}

func directBitmapFilterFragmentMaterializationDecision(request ExecutionRequest, fragment qsbridge.QuantaQueryFragment) (bool, string) {
	decision := directBitmapFilterFragmentMaterializationDecisionDetails(request, fragment)
	return decision.Materialize, decision.Reason
}

func directBitmapFilterFragmentMaterializationDecisionDetails(request ExecutionRequest, fragment qsbridge.QuantaQueryFragment) directBitmapFilterFragmentMaterializationDecisionResult {
	return directBitmapFilterFragmentMaterializationDecisionDetailsWithResolver(request, fragment, nil)
}

func directBitmapFilterFragmentMaterializationDecisionDetailsWithResolver(request ExecutionRequest, fragment qsbridge.QuantaQueryFragment, resolver qsbridge.DictionaryResolver) directBitmapFilterFragmentMaterializationDecisionResult {
	detection := directBitmapFilterFragmentStringEnumDecision(request, fragment)
	probeDetail := ""
	if detection.shouldProbe {
		probeDetail = detection.detail
	}
	if detection.usesStringEnum {
		if bitmapFragment, ok := directBitmapFilterFragmentStringEnumBitmapFragment(fragment, detection, resolver); ok {
			probeDetail = directBitmapFilterProbeDetailAppend(probeDetail, "dictionary_bitmap_values="+strconv.Itoa(len(bitmapFragment.Values)))
			return directBitmapFilterFragmentMaterializationDecisionResult{
				Materialize:       false,
				Reason:            "string_enum_dictionary_bitmap",
				ProbeDetail:       probeDetail,
				HasBitmapFragment: true,
				BitmapFragment:    bitmapFragment,
			}
		}
		// The legacy bitmap backend currently lowers these leaves into BSI compares;
		// keep the constrained path until dictionary bitmap evaluation is available.
		return directBitmapFilterFragmentMaterializationDecisionResult{
			Materialize: true,
			Reason:      "string_enum_materialization_preferred",
			ProbeDetail: probeDetail,
		}
	}
	if !directBitmapFilterFragmentCanEvaluateMaterialized(fragment) {
		return directBitmapFilterFragmentMaterializationDecisionResult{
			Materialize: false,
			Reason:      "unsupported_leaf",
			ProbeDetail: probeDetail,
		}
	}
	return directBitmapFilterFragmentMaterializationDecisionResult{
		Materialize: true,
		Reason:      "materializable_leaf",
		ProbeDetail: probeDetail,
	}
}

func directBitmapFilterFragmentFieldUsesStringEnum(request ExecutionRequest, fragment qsbridge.QuantaQueryFragment) bool {
	return directBitmapFilterFragmentStringEnumDecision(request, fragment).usesStringEnum
}

func directBitmapFilterFragmentStringEnumBitmapFragment(fragment qsbridge.QuantaQueryFragment, detection directBitmapFilterFragmentStringEnumDetection, resolver qsbridge.DictionaryResolver) (qsbridge.QuantaQueryFragment, bool) {
	if resolver == nil || !detection.usesStringEnum || !directBitmapFilterFragmentStringEnumBitmapEligible(fragment) {
		return qsbridge.QuantaQueryFragment{}, false
	}
	literals, ok := directBitmapFilterFragmentStringEnumExactLiterals(fragment)
	if !ok || len(literals) == 0 {
		return qsbridge.QuantaQueryFragment{}, false
	}
	ref := directBitmapFilterFragmentStringEnumDictionaryRef(detection)
	if !ref.Valid() {
		return qsbridge.QuantaQueryFragment{}, false
	}
	values := make([]*big.Int, 0, len(literals))
	seen := make(map[uint64]bool, len(literals))
	for _, literal := range literals {
		label, ok := literal.Value.(string)
		if literal.Kind != qsbridge.ValueString || !ok {
			return qsbridge.QuantaQueryFragment{}, false
		}
		entry, diagnostics := resolver.LookupLabel(ref, label)
		if diagnostics.BlocksNative() {
			return qsbridge.QuantaQueryFragment{}, false
		}
		id := uint64(entry.ID)
		if seen[id] {
			continue
		}
		seen[id] = true
		values = append(values, new(big.Int).SetUint64(id))
	}
	if len(values) == 0 {
		return qsbridge.QuantaQueryFragment{}, false
	}
	bitmapFragment := fragment
	bitmapFragment.BSIOp = qsbridge.QuantaBSIOpNone
	bitmapFragment.Value = nil
	bitmapFragment.Begin = nil
	bitmapFragment.End = nil
	bitmapFragment.Values = values
	bitmapFragment.HasLiteralRange = false
	if physicalField := directBitmapFilterFragmentStringEnumPhysicalField(detection); physicalField != "" {
		bitmapFragment.Field = physicalField
	}
	return bitmapFragment, true
}

func directBitmapFilterFragmentStringEnumBitmapEligible(fragment qsbridge.QuantaQueryFragment) bool {
	if fragment.NullCheck || fragment.Negate || fragment.HasLiteralRange {
		return false
	}
	if fragment.Operation == qsbridge.QuantaOperationDifference {
		return false
	}
	switch fragment.BSIOp {
	case qsbridge.QuantaBSIOpNone, qsbridge.QuantaBSIOpEQ:
		return true
	default:
		return false
	}
}

func directBitmapFilterFragmentStringEnumExactLiterals(fragment qsbridge.QuantaQueryFragment) ([]qsbridge.LiteralExpr, bool) {
	literals := make([]qsbridge.LiteralExpr, 0, 1+len(fragment.Literals))
	if fragment.HasLiteral {
		literals = append(literals, fragment.Literal)
	}
	literals = append(literals, fragment.Literals...)
	if len(literals) == 0 {
		return nil, false
	}
	for _, literal := range literals {
		if literal.Kind != qsbridge.ValueString {
			return nil, false
		}
		if _, ok := literal.Value.(string); !ok {
			return nil, false
		}
	}
	return literals, true
}

func directBitmapFilterFragmentStringEnumDictionaryRef(detection directBitmapFilterFragmentStringEnumDetection) qsbridge.DictionaryRef {
	if detection.definition.Dictionary.Ref.Valid() {
		return detection.definition.Dictionary.Ref
	}
	return qsbridge.DictionaryRef{
		Schema: detection.table.Schema,
		Table:  detection.table.Name,
		Field:  directBitmapFilterFragmentStringEnumPhysicalField(detection),
	}
}

func directBitmapFilterFragmentStringEnumPhysicalField(detection directBitmapFilterFragmentStringEnumDetection) string {
	if detection.definition.PhysicalName != "" {
		return detection.definition.PhysicalName
	}
	return detection.definition.Name
}

func directBitmapFilterProbeDetailAppend(detail string, parts ...string) string {
	for _, part := range parts {
		if part == "" {
			continue
		}
		if detail == "" {
			detail = part
			continue
		}
		detail += " " + part
	}
	return detail
}

func directBitmapFilterFragmentStringEnumDecision(request ExecutionRequest, fragment qsbridge.QuantaQueryFragment) directBitmapFilterFragmentStringEnumDetection {
	index := fragment.Index
	if index == "" {
		index, _ = request.RootIndex()
	}
	fields := directBitmapFilterFragmentFieldLookupNames(fragment.Field)
	shouldProbe := directBitmapFilterFragmentHasStringLiteral(fragment)
	baseDetail := directBitmapFilterFragmentStringEnumProbeBaseDetail(index, fields)
	for _, table := range request.QueryCatalog.Tables {
		if !strings.EqualFold(table.Name, index) {
			continue
		}
		for _, definition := range table.Fields {
			if !directBitmapFilterFragmentDefinitionMatchesAnyField(definition, fields) {
				continue
			}
			usesStringEnum := definition.Index == qsbridge.IndexStringEnum ||
				definition.Encoding.Kind == qsbridge.EncodingStringEnum ||
				definition.Dictionary.Ref.Valid()
			detail := baseDetail +
				" matched_table=" + table.Name +
				" matched_field=" + definition.Name +
				" matched_physical_field=" + definition.PhysicalName +
				" definition_type=" + string(definition.Type) +
				" definition_index=" + string(definition.Index) +
				" encoding_kind=" + string(definition.Encoding.Kind) +
				" dictionary_ref=" + strconv.FormatBool(definition.Dictionary.Ref.Valid()) +
				" uses_string_enum=" + strconv.FormatBool(usesStringEnum)
			return directBitmapFilterFragmentStringEnumDetection{
				usesStringEnum: usesStringEnum,
				shouldProbe:    shouldProbe,
				detail:         detail,
				table:          table,
				definition:     definition,
			}
		}
		detail := baseDetail + " matched_table=" + table.Name + " matched_field=none uses_string_enum=false"
		return directBitmapFilterFragmentStringEnumDetection{
			shouldProbe: shouldProbe,
			detail:      detail,
		}
	}
	return directBitmapFilterFragmentStringEnumDetection{
		shouldProbe: shouldProbe,
		detail:      baseDetail + " matched_table=none catalog_tables=" + directBitmapFilterFragmentCatalogTableNames(request.QueryCatalog) + " uses_string_enum=false",
	}
}

func directBitmapFilterFragmentFieldLookupNames(field string) []string {
	if field == "" {
		return nil
	}
	names := []string{field}
	if dot := strings.LastIndex(field, "."); dot >= 0 && dot+1 < len(field) {
		unqualified := field[dot+1:]
		if !strings.EqualFold(unqualified, field) {
			names = append(names, unqualified)
		}
	}
	return names
}

func directBitmapFilterFragmentStringEnumProbeBaseDetail(index string, fields []string) string {
	parts := []string{"index=" + index}
	if len(fields) > 0 {
		parts = append(parts, "lookup_fields="+strings.Join(fields, ","))
	}
	return strings.Join(parts, " ")
}

func directBitmapFilterFragmentDefinitionMatchesAnyField(definition qsbridge.FieldDefinition, fields []string) bool {
	for _, field := range fields {
		if strings.EqualFold(definition.Name, field) || (definition.PhysicalName != "" && strings.EqualFold(definition.PhysicalName, field)) {
			return true
		}
	}
	return false
}

func directBitmapFilterFragmentHasStringLiteral(fragment qsbridge.QuantaQueryFragment) bool {
	if fragment.HasLiteral && fragment.Literal.Kind == qsbridge.ValueString {
		return true
	}
	for _, literal := range fragment.Literals {
		if literal.Kind == qsbridge.ValueString {
			return true
		}
	}
	if fragment.HasLiteralRange && (fragment.BeginLiteral.Kind == qsbridge.ValueString || fragment.EndLiteral.Kind == qsbridge.ValueString) {
		return true
	}
	return false
}

func directBitmapFilterFragmentCatalogTableNames(catalog qsbridge.QueryCatalogView) string {
	if len(catalog.Tables) == 0 {
		return "none"
	}
	names := make([]string, 0, len(catalog.Tables))
	for _, table := range catalog.Tables {
		names = append(names, table.Name)
	}
	return strings.Join(names, ",")
}

func directBitmapFilterFragmentCanEvaluateMaterialized(fragment qsbridge.QuantaQueryFragment) bool {
	if fragment.NullCheck {
		return true
	}
	if fragment.HasLiteral || len(fragment.Literals) > 0 || fragment.HasLiteralRange {
		return true
	}
	return false
}

func directBitmapFilterFragmentDataType(fragment qsbridge.QuantaQueryFragment) qsbridge.DataType {
	if fragment.HasLiteral {
		return directBitmapFilterLiteralDataType(fragment.Literal)
	}
	if len(fragment.Literals) > 0 {
		return directBitmapFilterLiteralDataType(fragment.Literals[0])
	}
	if fragment.HasLiteralRange {
		return directBitmapFilterLiteralDataType(fragment.BeginLiteral)
	}
	return qsbridge.DataTypeInt
}

func directBitmapFilterLiteralDataType(literal qsbridge.LiteralExpr) qsbridge.DataType {
	switch literal.Kind {
	case qsbridge.ValueString:
		return qsbridge.DataTypeString
	case qsbridge.ValueFloat:
		return qsbridge.DataTypeFloat
	case qsbridge.ValueBool:
		return qsbridge.DataTypeBool
	case qsbridge.ValueTime:
		return qsbridge.DataTypeTime
	default:
		return qsbridge.DataTypeInt
	}
}

func directBitmapFilterFragmentMatchesCell(fragment qsbridge.QuantaQueryFragment, cell qsbridge.ResultCell) bool {
	matched := directBitmapFilterFragmentPositiveMatch(fragment, cell)
	if fragment.Operation == qsbridge.QuantaOperationDifference || fragment.Negate {
		return !matched
	}
	return matched
}

func directBitmapFilterFragmentPositiveMatch(fragment qsbridge.QuantaQueryFragment, cell qsbridge.ResultCell) bool {
	if fragment.NullCheck {
		return directBitmapNullCell(cell)
	}
	switch fragment.BSIOp {
	case qsbridge.QuantaBSIOpRange:
		return directBitmapFilterRangeCellMatch(fragment, cell)
	case qsbridge.QuantaBSIOpBatchEQ:
		return directBitmapFilterBatchEqualCellMatch(fragment, cell)
	case "":
		if len(fragment.Values) > 0 || len(fragment.Literals) > 0 {
			return directBitmapFilterBatchEqualCellMatch(fragment, cell)
		}
		return directBitmapFilterLiteralCellMatch(qsbridge.BinaryOpEqual, cell, directBitmapFilterFragmentLiteral(fragment))
	case qsbridge.QuantaBSIOpEQ:
		return directBitmapFilterLiteralCellMatch(qsbridge.BinaryOpEqual, cell, directBitmapFilterFragmentLiteral(fragment))
	case qsbridge.QuantaBSIOpLT:
		return directBitmapFilterLiteralCellMatch(qsbridge.BinaryOpLess, cell, directBitmapFilterFragmentLiteral(fragment))
	case qsbridge.QuantaBSIOpLE:
		return directBitmapFilterLiteralCellMatch(qsbridge.BinaryOpLessEqual, cell, directBitmapFilterFragmentLiteral(fragment))
	case qsbridge.QuantaBSIOpGE:
		return directBitmapFilterLiteralCellMatch(qsbridge.BinaryOpGreaterEqual, cell, directBitmapFilterFragmentLiteral(fragment))
	case qsbridge.QuantaBSIOpGT:
		return directBitmapFilterLiteralCellMatch(qsbridge.BinaryOpGreater, cell, directBitmapFilterFragmentLiteral(fragment))
	default:
		return false
	}
}

func directBitmapFilterRangeCellMatch(fragment qsbridge.QuantaQueryFragment, cell qsbridge.ResultCell) bool {
	begin := fragment.BeginLiteral
	end := fragment.EndLiteral
	if !fragment.HasLiteralRange {
		begin = directBitmapFilterBigIntLiteral(fragment.Begin)
		end = directBitmapFilterBigIntLiteral(fragment.End)
	}
	return directBitmapCellComparesLiteral(qsbridge.BinaryOpGreaterEqual, cell, begin) &&
		directBitmapCellComparesLiteral(qsbridge.BinaryOpLessEqual, cell, end)
}

func directBitmapFilterBatchEqualCellMatch(fragment qsbridge.QuantaQueryFragment, cell qsbridge.ResultCell) bool {
	if len(fragment.Literals) > 0 {
		for _, literal := range fragment.Literals {
			if directBitmapFilterLiteralCellMatch(qsbridge.BinaryOpEqual, cell, literal) {
				return true
			}
		}
	}
	for _, value := range fragment.Values {
		if directBitmapFilterLiteralCellMatch(qsbridge.BinaryOpEqual, cell, directBitmapFilterBigIntLiteral(value)) {
			return true
		}
	}
	return false
}

func directBitmapFilterLiteralCellMatch(op qsbridge.BinaryOp, cell qsbridge.ResultCell, literal qsbridge.LiteralExpr) bool {
	if literal.Kind == "" {
		return false
	}
	return directBitmapCellComparesLiteral(op, cell, literal)
}

func directBitmapFilterFragmentLiteral(fragment qsbridge.QuantaQueryFragment) qsbridge.LiteralExpr {
	if fragment.HasLiteral {
		return fragment.Literal
	}
	return directBitmapFilterBigIntLiteral(fragment.Value)
}

func directBitmapFilterBigIntLiteral(value *big.Int) qsbridge.LiteralExpr {
	if value == nil {
		return qsbridge.LiteralExpr{}
	}
	return qsbridge.Literal(qsbridge.ValueInt, value.Int64())
}

// UnsupportedDirectBitmapFilterAdapter preserves the grouped-filter blocker until bitmap wiring exists.
type UnsupportedDirectBitmapFilterAdapter struct{}

// AdaptFilterExpression reports unsupported grouped filters and passes flat fragment requests through.
func (UnsupportedDirectBitmapFilterAdapter) AdaptFilterExpression(_ context.Context, request ExecutionRequest) (ExecutionRequest, qsbridge.DiagnosticSet, error) {
	if request.Query.Filter.Empty() {
		return request, nil, nil
	}
	return request, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			"direct bitmap runtime does not execute grouped filter expressions yet",
		),
	}, nil
}

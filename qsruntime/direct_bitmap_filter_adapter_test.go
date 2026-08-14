package qsruntime

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestDirectBitmapFilterDomainRewriteProbesExposeExpansionMetrics(t *testing.T) {
	rewrite := qsbridge.FilterDomainRewriteResult{
		Branches: []qsbridge.FilterDomainNormalizedBranch{{
			SourceDomain:                "part",
			TargetDomain:                "lineitem",
			VectorIndex:                 "lineitem",
			VectorField:                 "l_partkey",
			Direction:                   qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
			SourceCount:                 88,
			SourceElapsed:               2 * time.Millisecond,
			TranslationElapsed:          3 * time.Second,
			ProjectionElapsed:           2800 * time.Millisecond,
			ProjectionCacheHit:          true,
			SourceKeyProjectionUsed:     false,
			SourceKeyProjectionElapsed:  0,
			SourceValueCount:            88,
			CandidateCacheHit:           false,
			CandidateCacheMode:          "coverage_miss",
			CandidateMode:               "batch_equal",
			CandidateElapsed:            200 * time.Millisecond,
			BatchEqualElapsed:           150 * time.Millisecond,
			CandidateScanElapsed:        50 * time.Millisecond,
			CandidateDirectQueryElapsed: 125 * time.Millisecond,
			CandidateDirectFragments:    3,
			CandidateDirectRows:         27,
			CandidateSet: qsbridge.QuantaCandidateSet{
				Index:   "lineitem",
				Rownums: []qsbridge.QuantaRownum{101, 102, 103},
			},
		}},
	}

	probes := directBitmapFilterDomainRewriteProbes(rewrite)
	assertFilterDomainExpansionProbe(t, probes, "branch_001_source_rows", "88", "source=part")
	assertFilterDomainExpansionProbe(t, probes, "branch_001_target_rows", "3", "vector=lineitem.l_partkey")
	assertFilterDomainExpansionProbe(t, probes, "branch_001_translation_elapsed", "3s", "target=lineitem")
	assertFilterDomainExpansionProbe(t, probes, "branch_001_projection_cache_hit", "true", "direction=right_to_left")
	assertFilterDomainExpansionProbe(t, probes, "branch_001_candidate_cache_mode", "coverage_miss", "target_index=lineitem")
	assertFilterDomainExpansionProbe(t, probes, "branch_001_candidate_mode", "batch_equal", "source=part")
	assertFilterDomainExpansionProbe(t, probes, "branch_001_candidate_direct_query_elapsed", "125ms", "source=part")
	assertFilterDomainExpansionProbe(t, probes, "branch_001_candidate_direct_fragments", "3", "target=lineitem")
	assertFilterDomainExpansionProbe(t, probes, "branch_001_candidate_direct_rows", "27", "vector=lineitem.l_partkey")
}

func TestDirectBitmapFilterFragmentShouldMaterializeStringEnumUntilBitmapKernelIsAvailable(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.QueryCatalog = qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{{
		Name: "lineitem",
		Fields: []qsbridge.FieldDefinition{
			{Name: "l_shipmode", Type: qsbridge.DataTypeString, Index: qsbridge.IndexStringEnum},
			{
				Name: "l_shipinstruct",
				Type: qsbridge.DataTypeString,
				Dictionary: qsbridge.DictionaryDefinition{
					Ref: qsbridge.DictionaryRef{Table: "lineitem", Field: "l_shipinstruct"},
				},
			},
			{Name: "l_quantity", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI},
		},
	}}, nil, nil)

	stringEnumFragment := qsbridge.QuantaQueryFragment{
		Index:      "lineitem",
		Field:      "l_shipmode",
		HasLiteral: true,
		Literal:    qsbridge.Literal(qsbridge.ValueString, "AIR"),
	}
	if !directBitmapFilterFragmentShouldEvaluateMaterialized(request, stringEnumFragment) {
		t.Fatalf("StringEnum filter leaf should stay on constrained materialization until dictionary bitmap evaluation is available")
	}

	dictionaryFragment := qsbridge.QuantaQueryFragment{
		Index:      "lineitem",
		Field:      "l_shipinstruct",
		HasLiteral: true,
		Literal:    qsbridge.Literal(qsbridge.ValueString, "DELIVER IN PERSON"),
	}
	if !directBitmapFilterFragmentShouldEvaluateMaterialized(request, dictionaryFragment) {
		t.Fatalf("dictionary-backed StringEnum filter leaf should stay on constrained materialization until dictionary bitmap evaluation is available")
	}

	qualifiedDictionaryFragment := qsbridge.QuantaQueryFragment{
		Index:      "lineitem",
		Field:      "l.l_shipmode",
		HasLiteral: true,
		Literal:    qsbridge.Literal(qsbridge.ValueString, "AIR"),
	}
	if !directBitmapFilterFragmentShouldEvaluateMaterialized(request, qualifiedDictionaryFragment) {
		t.Fatalf("qualified dictionary-backed StringEnum filter leaf should stay on constrained materialization until dictionary bitmap evaluation is available")
	}

	bsiFragment := qsbridge.QuantaQueryFragment{
		Index:      "lineitem",
		Field:      "l_quantity",
		BSIOp:      qsbridge.QuantaBSIOpGE,
		HasLiteral: true,
		Literal:    qsbridge.Literal(qsbridge.ValueInt, int64(10)),
	}
	if !directBitmapFilterFragmentShouldEvaluateMaterialized(request, bsiFragment) {
		t.Fatalf("BSI filter leaf should keep materialized constrained evaluation")
	}
}

func TestDirectBitmapFilterFragmentUsesDictionaryBitmapForResolvedStringEnum(t *testing.T) {
	ref := qsbridge.DictionaryRef{Schema: "quanta", Table: "lineitem", Field: "l_shipmode"}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.QueryCatalog = qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{{
		Schema: "quanta",
		Name:   "lineitem",
		Fields: []qsbridge.FieldDefinition{{
			Name:       "l_shipmode",
			Type:       qsbridge.DataTypeString,
			Index:      qsbridge.IndexStringEnum,
			Dictionary: qsbridge.DictionaryDefinition{Ref: ref},
		}},
	}}, nil, nil)
	resolver := qsbridge.MemoryDictionaryResolver{
		Dictionaries: []qsbridge.DictionaryDefinition{{Ref: ref}},
		Entries: []qsbridge.DictionaryEntry{
			{Ref: ref, Label: "AIR", ID: 7},
			{Ref: ref, Label: "MAIL", ID: 8},
		},
	}
	fragment := qsbridge.QuantaQueryFragment{
		Index: "lineitem",
		Field: "l.l_shipmode",
		Literals: []qsbridge.LiteralExpr{
			qsbridge.Literal(qsbridge.ValueString, "AIR"),
			qsbridge.Literal(qsbridge.ValueString, "MAIL"),
		},
	}

	decision := directBitmapFilterFragmentMaterializationDecisionDetailsWithResolver(request, fragment, resolver)

	if decision.Materialize || decision.Reason != "string_enum_dictionary_bitmap" || !decision.HasBitmapFragment {
		t.Fatalf("decision = %#v, want dictionary bitmap decision", decision)
	}
	if decision.BitmapFragment.BSIOp != qsbridge.QuantaBSIOpNone {
		t.Fatalf("bitmap BSI op = %q, want none", decision.BitmapFragment.BSIOp)
	}
	if decision.BitmapFragment.Field != "l_shipmode" {
		t.Fatalf("bitmap field = %q, want physical field", decision.BitmapFragment.Field)
	}
	if len(decision.BitmapFragment.Values) != 2 {
		t.Fatalf("bitmap values = %#v, want two ids", decision.BitmapFragment.Values)
	}
	if got := decision.BitmapFragment.Values[0].Uint64(); got != 7 {
		t.Fatalf("first dictionary id = %d, want 7", got)
	}
	if got := decision.BitmapFragment.Values[1].Uint64(); got != 8 {
		t.Fatalf("second dictionary id = %d, want 8", got)
	}
	for _, want := range []string{
		"uses_string_enum=true",
		"dictionary_bitmap_values=2",
	} {
		if !strings.Contains(decision.ProbeDetail, want) {
			t.Fatalf("probe detail = %q, want substring %q", decision.ProbeDetail, want)
		}
	}
}

func TestDirectBitmapFilterFragmentDecisionDetailsExposeStringEnumLookup(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.QueryCatalog = qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{{
		Name: "lineitem",
		Fields: []qsbridge.FieldDefinition{
			{Name: "l_shipmode", Type: qsbridge.DataTypeString},
		},
	}}, nil, nil)
	fragment := qsbridge.QuantaQueryFragment{
		Index:      "lineitem",
		Field:      "l.l_shipmode",
		HasLiteral: true,
		Literal:    qsbridge.Literal(qsbridge.ValueString, "AIR"),
	}

	decision := directBitmapFilterFragmentMaterializationDecisionDetails(request, fragment)
	if !decision.Materialize || decision.Reason != "materializable_leaf" {
		t.Fatalf("decision = %#v, want materializable string leaf", decision)
	}
	for _, want := range []string{
		"index=lineitem",
		"lookup_fields=l.l_shipmode,l_shipmode",
		"matched_table=lineitem",
		"matched_field=l_shipmode",
		"definition_type=string",
		"uses_string_enum=false",
	} {
		if !strings.Contains(decision.ProbeDetail, want) {
			t.Fatalf("probe detail = %q, want substring %q", decision.ProbeDetail, want)
		}
	}

	request.QueryCatalog = qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{{
		Name: "lineitem",
		Fields: []qsbridge.FieldDefinition{
			{Name: "l_shipmode", Type: qsbridge.DataTypeString, Index: qsbridge.IndexStringEnum},
		},
	}}, nil, nil)
	decision = directBitmapFilterFragmentMaterializationDecisionDetails(request, fragment)
	if !decision.Materialize || decision.Reason != "string_enum_materialization_preferred" {
		t.Fatalf("decision = %#v, want StringEnum constrained materialization", decision)
	}
	if !strings.Contains(decision.ProbeDetail, "uses_string_enum=true") {
		t.Fatalf("probe detail = %q, want StringEnum evidence", decision.ProbeDetail)
	}
}

func TestDirectBitmapFilterTreeAdapterFallbackQueryCatalogFeedsStringEnumDecision(t *testing.T) {
	queryCatalog := qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{{
		Name: "part",
		Fields: []qsbridge.FieldDefinition{
			{Name: "p_container", Type: qsbridge.DataTypeString, Index: qsbridge.IndexStringEnum},
		},
	}}, nil, nil)
	adapter := DirectBitmapFilterTreeAdapter{QueryCatalogProvider: func() qsbridge.QueryCatalogView {
		return queryCatalog
	}}
	request := adapter.requestWithFallbackQueryCatalog(NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}))
	fragment := qsbridge.QuantaQueryFragment{
		Index:      "part",
		Field:      "p_container",
		HasLiteral: true,
		Literal:    qsbridge.Literal(qsbridge.ValueString, "MED BOX"),
	}

	decision := directBitmapFilterFragmentMaterializationDecisionDetails(request, fragment)

	if !decision.Materialize || decision.Reason != "string_enum_materialization_preferred" {
		t.Fatalf("decision = %#v, want fallback catalog to prefer constrained materialization", decision)
	}
	if !strings.Contains(decision.ProbeDetail, "matched_table=part") ||
		!strings.Contains(decision.ProbeDetail, "uses_string_enum=true") {
		t.Fatalf("probe detail = %q, want fallback catalog evidence", decision.ProbeDetail)
	}
}

func TestDirectBitmapFilterTreeAdapterSendsResolvedStringEnumBitmapFragment(t *testing.T) {
	ref := qsbridge.DictionaryRef{Schema: "quanta", Table: "lineitem", Field: "l_shipmode"}
	catalog := qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{{
		Schema: "quanta",
		Name:   "lineitem",
		Fields: []qsbridge.FieldDefinition{{
			Name:       "l_shipmode",
			Type:       qsbridge.DataTypeString,
			Index:      qsbridge.IndexStringEnum,
			Dictionary: qsbridge.DictionaryDefinition{Ref: ref},
		}},
	}}, nil, nil)
	resolver := qsbridge.MemoryDictionaryResolver{
		Dictionaries: []qsbridge.DictionaryDefinition{{Ref: ref}},
		Entries:      []qsbridge.DictionaryEntry{{Ref: ref, Label: "AIR", ID: 7}},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "lineitem", Field: "l_orderkey", Visible: true}},
		Filter: qsbridge.QuantaFilterExpression{
			Operation: qsbridge.QuantaFilterLeaf,
			Fragment: qsbridge.QuantaQueryFragment{
				Index:      "lineitem",
				Field:      "l.l_shipmode",
				HasLiteral: true,
				Literal:    qsbridge.Literal(qsbridge.ValueString, "AIR"),
			},
		},
	})
	request.QueryCatalog = catalog
	var captured qsbridge.QuantaQueryFragment
	adapter := DirectBitmapFilterTreeAdapter{
		DictionaryResolver: resolver,
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					captured = request.Query.Fragments[0]
					return BitmapQueryResult{Success: true, Rownums: []qsbridge.QuantaRownum{11, 12}}, nil, nil
				},
			}, nil, nil
		}),
	}

	adapted, diagnostics, err := adapter.AdaptFilterExpression(context.Background(), request)

	if err != nil {
		t.Fatalf("adapt filter: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !adapted.HasCandidateSet {
		t.Fatalf("expected candidate set")
	}
	if captured.BSIOp != qsbridge.QuantaBSIOpNone {
		t.Fatalf("captured BSI op = %q, want none", captured.BSIOp)
	}
	if captured.Field != "l_shipmode" {
		t.Fatalf("captured field = %q, want physical field", captured.Field)
	}
	if len(captured.Values) != 1 || captured.Values[0].Cmp(big.NewInt(7)) != 0 {
		t.Fatalf("captured values = %#v, want dictionary id 7", captured.Values)
	}
	assertFilterAdapterExecutionProbe(t, adapted.Probes, "filter_tree", "leaf_materialization_decision", "string_enum_dictionary_bitmap", "dictionary_bitmap_values=1")
	assertFilterAdapterExecutionProbe(t, adapted.Probes, "filter_tree", "leaf_evaluation_mode", "bitmap_query", "reason=string_enum_dictionary_bitmap")
}

func TestDirectBitmapFilterTreeLeafEvaluatorMaterializesDictionaryBitmapWithinSmallCandidateSet(t *testing.T) {
	ref := qsbridge.DictionaryRef{Schema: "quanta", Table: "lineitem", Field: "l_shipmode"}
	catalog := qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{{
		Schema: "quanta",
		Name:   "lineitem",
		Fields: []qsbridge.FieldDefinition{{
			Name:       "l_shipmode",
			Type:       qsbridge.DataTypeString,
			Index:      qsbridge.IndexStringEnum,
			Dictionary: qsbridge.DictionaryDefinition{Ref: ref},
		}},
	}}, nil, nil)
	resolver := qsbridge.MemoryDictionaryResolver{
		Dictionaries: []qsbridge.DictionaryDefinition{{Ref: ref}},
		Entries:      []qsbridge.DictionaryEntry{{Ref: ref, Label: "AIR", ID: 7}},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.QueryCatalog = catalog
	var bitmapCalled bool
	var materialized qsbridge.ProjectionMaterializationKernelRequest
	recorder := &directBitmapFilterTreeEvaluationRecorder{}
	evaluator := directBitmapFilterTreeLeafEvaluator{
		Request:            request,
		DictionaryResolver: resolver,
		Recorder:           recorder,
		Sessions: DirectSessionProviderFunc(func(context.Context, ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			bitmapCalled = true
			return DirectSessionHandleFunc{
				QueryFunc: func(context.Context, ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{}, nil, nil
				},
			}, nil, nil
		}),
		Materialization: qsruntimeMaterializationKernelFunc(func(_ context.Context, request qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
			materialized = request
			rownums := []qsbridge.QuantaRownum{11, 12, 13}
			return qsbridge.ProjectionMaterializationKernelResult{
				Results: []qsbridge.ProjectionMaterializationResult{{
					RowSet: qsbridge.QuantaProjectedRowSet{
						Index:   "lineitem",
						Rownums: rownums,
						ProjectionVectors: []qsbridge.QuantaProjectionVector{{
							Values: []qsbridge.ResultCell{
								{Kind: qsbridge.ValueString, Value: "AIR"},
								{Kind: qsbridge.ValueString, Value: "RAIL"},
								{Kind: qsbridge.ValueString, Value: "AIR"},
							},
						}},
					},
				}},
			}, nil
		}),
	}
	fragment := qsbridge.QuantaQueryFragment{
		Index:      "lineitem",
		Field:      "l.l_shipmode",
		HasLiteral: true,
		Literal:    qsbridge.Literal(qsbridge.ValueString, "AIR"),
	}
	candidates := qsbridge.QuantaCandidateSet{Index: "lineitem", Rownums: []qsbridge.QuantaRownum{11, 12, 13}}

	filtered, diagnostics, err := evaluator.EvaluateFilterLeafWithinCandidateSet(context.Background(), fragment, candidates)

	if err != nil {
		t.Fatalf("evaluate leaf: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if bitmapCalled {
		t.Fatalf("dictionary bitmap leaf with small candidate set should use materialization, not bitmap session")
	}
	if len(materialized.Requests) != 1 || len(materialized.Requests[0].Rownums) != 3 {
		t.Fatalf("materialization request = %#v, want three candidate rownums", materialized)
	}
	if len(filtered.Rownums) != 2 || filtered.Rownums[0] != 11 || filtered.Rownums[1] != 13 {
		t.Fatalf("filtered rownums = %#v, want [11 13]", filtered.Rownums)
	}
	assertFilterAdapterExecutionProbe(t, recorder.Probes(), "filter_tree", "leaf_evaluation_mode", "constrained_materialization", "reason=string_enum_dictionary_bitmap_candidate_materialization")
}

func TestDirectBitmapFilterDictionaryBitmapCandidateMaterializationLimit(t *testing.T) {
	decision := directBitmapFilterFragmentMaterializationDecisionResult{
		Reason:            "string_enum_dictionary_bitmap",
		HasBitmapFragment: true,
	}
	if !directBitmapFilterShouldMaterializeDictionaryBitmapWithinCandidates(decision, 437) {
		t.Fatalf("437 candidate rows should use constrained materialization")
	}
	if directBitmapFilterShouldMaterializeDictionaryBitmapWithinCandidates(decision, 3204) {
		t.Fatalf("3204 candidate rows should stay on dictionary bitmap")
	}
}

func TestDirectBitmapFilterTreeLeafEvaluatorUsesCandidateBitmapQueryForLargeDictionaryCandidateSet(t *testing.T) {
	ref := qsbridge.DictionaryRef{Schema: "quanta", Table: "lineitem", Field: "l_shipmode"}
	catalog := qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{{
		Schema: "quanta",
		Name:   "lineitem",
		Fields: []qsbridge.FieldDefinition{{
			Name:       "l_shipmode",
			Type:       qsbridge.DataTypeString,
			Index:      qsbridge.IndexStringEnum,
			Dictionary: qsbridge.DictionaryDefinition{Ref: ref},
		}},
	}}, nil, nil)
	resolver := qsbridge.MemoryDictionaryResolver{
		Dictionaries: []qsbridge.DictionaryDefinition{{Ref: ref}},
		Entries:      []qsbridge.DictionaryEntry{{Ref: ref, Label: "AIR", ID: 7}},
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.QueryCatalog = catalog
	candidates := qsbridge.QuantaCandidateSet{Index: "lineitem", Rownums: make([]qsbridge.QuantaRownum, 1025)}
	for i := range candidates.Rownums {
		candidates.Rownums[i] = qsbridge.QuantaRownum(i + 1)
	}
	var fallbackCalled bool
	var capturedCandidates qsbridge.QuantaCandidateSet
	var capturedFragment qsbridge.QuantaQueryFragment
	recorder := &directBitmapFilterTreeEvaluationRecorder{}
	evaluator := directBitmapFilterTreeLeafEvaluator{
		Request:            request,
		DictionaryResolver: resolver,
		Recorder:           recorder,
		Sessions: DirectSessionProviderFunc(func(context.Context, ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(context.Context, ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					fallbackCalled = true
					return BitmapQueryResult{}, nil, nil
				},
				CandidateQueryFunc: func(_ context.Context, request ExecutionRequest, candidates qsbridge.QuantaCandidateSet) (BitmapQueryResult, qsbridge.DiagnosticSet, bool, error) {
					capturedCandidates = candidates
					capturedFragment = request.Query.Fragments[0]
					return BitmapQueryResult{Success: true, Rownums: []qsbridge.QuantaRownum{12, 14}}, nil, true, nil
				},
			}, nil, nil
		}),
		Materialization: qsruntimeMaterializationKernelFunc(func(context.Context, qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
			t.Fatalf("large dictionary bitmap candidate set should not materialize")
			return qsbridge.ProjectionMaterializationKernelResult{}, nil
		}),
	}
	fragment := qsbridge.QuantaQueryFragment{
		Index:      "lineitem",
		Field:      "l.l_shipmode",
		HasLiteral: true,
		Literal:    qsbridge.Literal(qsbridge.ValueString, "AIR"),
	}

	filtered, diagnostics, err := evaluator.EvaluateFilterLeafWithinCandidateSet(context.Background(), fragment, candidates)

	if err != nil {
		t.Fatalf("evaluate leaf: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if fallbackCalled {
		t.Fatalf("candidate-aware bitmap query should handle large dictionary candidate set")
	}
	if capturedCandidates.CandidateCount() != candidates.CandidateCount() {
		t.Fatalf("captured candidates = %d, want %d", capturedCandidates.CandidateCount(), candidates.CandidateCount())
	}
	if capturedFragment.Field != "l_shipmode" || len(capturedFragment.Values) != 1 || capturedFragment.Values[0].Cmp(big.NewInt(7)) != 0 {
		t.Fatalf("captured fragment = %#v, want physical dictionary bitmap fragment", capturedFragment)
	}
	if len(filtered.Rownums) != 2 || filtered.Rownums[0] != 12 || filtered.Rownums[1] != 14 {
		t.Fatalf("filtered rownums = %#v, want [12 14]", filtered.Rownums)
	}
	assertFilterAdapterExecutionProbe(t, recorder.Probes(), "filter_tree", "leaf_evaluation_mode", "bitmap_query", "reason=string_enum_dictionary_bitmap")
	assertFilterAdapterExecutionProbe(t, recorder.Probes(), "filter_tree", "candidate_bitmap_query_supported", "true", "candidate_rows=1025")
	assertFilterAdapterExecutionProbe(t, recorder.Probes(), "filter_tree", "candidate_bitmap_query_handled", "true", "reason=handled")
}

func TestDirectBitmapFilterTreeRecorderTagsInnerLeafProbes(t *testing.T) {
	recorder := &directBitmapFilterTreeEvaluationRecorder{}
	fragment := qsbridge.QuantaQueryFragment{
		Index: "lineitem",
		Role:  "l",
		Field: "l_shipmode",
	}

	recorder.RecordInnerProbes(fragment, "bitmap_query", []ExecutionProbe{{
		Section: "direct_bitmap_server",
		Name:    "standard_bitmap_elapsed",
		Value:   "12ms",
		Detail:  "standard_fragment_count=1",
	}})

	probes := recorder.Probes()
	if len(probes) != 1 {
		t.Fatalf("probes = %d, want one inner probe", len(probes))
	}
	probe := probes[0]
	if probe.Section != "direct_bitmap_server" || probe.Name != "standard_bitmap_elapsed" || probe.Value != "12ms" {
		t.Fatalf("probe = %#v, want tagged direct bitmap server timing", probe)
	}
	for _, want := range []string{
		"leaf=lineitem.l.l_shipmode",
		"source=bitmap_query",
		"standard_fragment_count=1",
	} {
		if !strings.Contains(probe.Detail, want) {
			t.Fatalf("probe detail = %q, want %q", probe.Detail, want)
		}
	}
}

func TestDirectBitmapFilterTreeRecorderRecordsLeafMode(t *testing.T) {
	recorder := &directBitmapFilterTreeEvaluationRecorder{}
	fragment := qsbridge.QuantaQueryFragment{
		Index: "lineitem",
		Role:  "l",
		Field: "l_quantity",
	}

	recorder.RecordLeafMode(fragment, "constrained_materialization", 2622, "materializable_leaf")

	probes := recorder.Probes()
	if len(probes) != 1 {
		t.Fatalf("probes = %d, want one mode probe", len(probes))
	}
	probe := probes[0]
	if probe.Section != "filter_tree" || probe.Name != "leaf_evaluation_mode" || probe.Value != "constrained_materialization" {
		t.Fatalf("probe = %#v, want leaf evaluation mode", probe)
	}
	for _, want := range []string{
		"leaf=lineitem.l.l_quantity",
		"source=constrained_materialization",
		"input_rows=2622",
		"reason=materializable_leaf",
	} {
		if !strings.Contains(probe.Detail, want) {
			t.Fatalf("probe detail = %q, want %q", probe.Detail, want)
		}
	}
}

func assertFilterDomainExpansionProbe(t *testing.T, probes []ExecutionProbe, name, value, detailPart string) {
	t.Helper()
	for _, probe := range probes {
		if probe.Section != "filter_domain_expansion" || probe.Name != name {
			continue
		}
		if probe.Value != value {
			t.Fatalf("probe %s value = %q, want %q", name, probe.Value, value)
		}
		if detailPart != "" && !strings.Contains(probe.Detail, detailPart) {
			t.Fatalf("probe %s detail = %q, want substring %q", name, probe.Detail, detailPart)
		}
		return
	}
	t.Fatalf("probe %s not found in %#v", name, probes)
}

func assertFilterAdapterExecutionProbe(t *testing.T, probes []ExecutionProbe, section string, name string, value string, detailPart string) {
	t.Helper()
	for _, probe := range probes {
		if probe.Section != section || probe.Name != name || probe.Value != value {
			continue
		}
		if detailPart != "" && !strings.Contains(probe.Detail, detailPart) {
			t.Fatalf("probe detail = %q, want substring %q", probe.Detail, detailPart)
		}
		return
	}
	t.Fatalf("probe %s/%s=%s not found in %#v", section, name, value, probes)
}

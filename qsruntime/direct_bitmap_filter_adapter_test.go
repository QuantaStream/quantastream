package qsruntime

import (
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestDirectBitmapFilterDomainRewriteProbesExposeExpansionMetrics(t *testing.T) {
	rewrite := qsbridge.FilterDomainRewriteResult{
		Branches: []qsbridge.FilterDomainNormalizedBranch{{
			SourceDomain:               "part",
			TargetDomain:               "lineitem",
			VectorIndex:                "lineitem",
			VectorField:                "l_partkey",
			Direction:                  qsbridge.FilterDomainRelationshipVectorDirectionRightToLeft,
			SourceCount:                88,
			SourceElapsed:              2 * time.Millisecond,
			TranslationElapsed:         3 * time.Second,
			ProjectionElapsed:          2800 * time.Millisecond,
			ProjectionCacheHit:         true,
			SourceKeyProjectionUsed:    false,
			SourceKeyProjectionElapsed: 0,
			SourceValueCount:           88,
			CandidateCacheHit:          false,
			CandidateCacheMode:         "coverage_miss",
			CandidateMode:              "batch_equal",
			CandidateElapsed:           200 * time.Millisecond,
			BatchEqualElapsed:          150 * time.Millisecond,
			CandidateScanElapsed:       50 * time.Millisecond,
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
}

func TestDirectBitmapFilterFragmentShouldPreferBitmapForStringEnum(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.QueryCatalog = qsbridge.NewQueryCatalogView([]qsbridge.TableDefinition{{
		Name: "lineitem",
		Fields: []qsbridge.FieldDefinition{
			{Name: "l_shipmode", Type: qsbridge.DataTypeString, Index: qsbridge.IndexStringEnum},
			{Name: "l_quantity", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI},
		},
	}}, nil, nil)

	stringEnumFragment := qsbridge.QuantaQueryFragment{
		Index:      "lineitem",
		Field:      "l_shipmode",
		HasLiteral: true,
		Literal:    qsbridge.Literal(qsbridge.ValueString, "AIR"),
	}
	if directBitmapFilterFragmentShouldEvaluateMaterialized(request, stringEnumFragment) {
		t.Fatalf("StringEnum filter leaf should prefer bitmap evaluation over candidate materialization")
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

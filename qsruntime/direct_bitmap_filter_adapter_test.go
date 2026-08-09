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

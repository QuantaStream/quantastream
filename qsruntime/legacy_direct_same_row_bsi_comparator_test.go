package qsruntime

import (
	"testing"
	"time"
)

func TestSameRowSharedCompareStatsProbesExposeNodeWork(t *testing.T) {
	request := NativeSameRowBSICompareRequest{
		Index:       "lineitem",
		ProbePrefix: "direct_bitmap_same_row_1_",
	}
	probes := sameRowSharedCompareStatsProbes(request, sameRowSharedCompareProbeStats{
		Nodes:          3,
		CompareElapsed: 7 * time.Millisecond,
		OutputRows:     42,
		Left: sameRowSharedCompareProjectionProbeStats{
			ShardsVisited:  10,
			ShardsInWindow: 8,
			ShardsLocal:    6,
			ShardsRetained: 4,
			RetainedRows:   100,
			RetainElapsed:  2 * time.Millisecond,
			MergeElapsed:   3 * time.Millisecond,
		},
		Right: sameRowSharedCompareProjectionProbeStats{
			ShardsVisited:  11,
			ShardsInWindow: 9,
			ShardsLocal:    7,
			ShardsRetained: 5,
			RetainedRows:   200,
			RetainElapsed:  4 * time.Millisecond,
			MergeElapsed:   5 * time.Millisecond,
		},
	})

	assertExecutionProbe(t, probes, "same_row_comparison", "direct_bitmap_same_row_1_shared_compare_stats_available", "true")
	assertExecutionProbe(t, probes, "same_row_comparison", "direct_bitmap_same_row_1_shared_compare_nodes", "3")
	assertExecutionProbe(t, probes, "same_row_comparison", "direct_bitmap_same_row_1_shared_compare_node_compare_elapsed", "7ms")
	assertExecutionProbe(t, probes, "same_row_comparison", "direct_bitmap_same_row_1_shared_compare_aggregate_output_rows", "42")
	assertExecutionProbe(t, probes, "same_row_comparison", "direct_bitmap_same_row_1_shared_compare_left_shards_local", "6")
	assertExecutionProbe(t, probes, "same_row_comparison", "direct_bitmap_same_row_1_shared_compare_left_retain_elapsed", "2ms")
	assertExecutionProbe(t, probes, "same_row_comparison", "direct_bitmap_same_row_1_shared_compare_right_retained_rows", "200")
	assertExecutionProbe(t, probes, "same_row_comparison", "direct_bitmap_same_row_1_shared_compare_right_merge_elapsed", "5ms")
}

func TestSameRowSharedCompareStatsProbesMarkMissingNodeStats(t *testing.T) {
	request := NativeSameRowBSICompareRequest{
		Index:       "lineitem",
		ProbePrefix: "direct_bitmap_same_row_1_",
	}
	probes := sameRowSharedCompareStatsProbes(request, sameRowSharedCompareProbeStats{})

	if len(probes) != 1 {
		t.Fatalf("probes = %#v, want only stats availability", probes)
	}
	assertExecutionProbe(t, probes, "same_row_comparison", "direct_bitmap_same_row_1_shared_compare_stats_available", "false")
}

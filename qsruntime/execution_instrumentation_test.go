package qsruntime

import (
	"context"
	"testing"
	"time"
)

func TestRecordExecutionProbesClassifiesTimingsCountersAndEvents(t *testing.T) {
	ctx := WithQueryScratchpad(context.Background())

	recordExecutionProbes(ctx, []ExecutionProbe{
		{Section: "relationship_join", Name: "phase_graph_reduction_elapsed", Value: "3ms", Detail: "edges=2"},
		{Section: "same_row_comparison", Name: "elapsed", Value: "2us"},
		{Section: "relationship_join", Name: "matched_rows", Value: "42"},
		{Section: "relationship_join", Name: "strategy", Value: "bsi_bitwise"},
	})

	snapshot := ExecutionInstrumentationSnapshotFromContext(ctx)
	if len(snapshot.Timings) != 2 {
		t.Fatalf("timings = %#v, want two timing probes", snapshot.Timings)
	}
	if snapshot.Timings[0].Duration != 3*time.Millisecond || snapshot.Timings[0].Detail != "edges=2" {
		t.Fatalf("first timing = %#v, want reduction timing", snapshot.Timings[0])
	}
	if snapshot.Timings[1].Duration != 2*time.Microsecond {
		t.Fatalf("second timing = %#v, want same-row elapsed timing", snapshot.Timings[1])
	}
	if len(snapshot.Counters) != 1 || snapshot.Counters[0].Value != 42 {
		t.Fatalf("counters = %#v, want matched_rows counter", snapshot.Counters)
	}
	if len(snapshot.Events) != 1 || snapshot.Events[0].Value != "bsi_bitwise" {
		t.Fatalf("events = %#v, want strategy event", snapshot.Events)
	}
}

func TestExecutionInstrumentationSnapshotWithMissingProbesSuppressesDuplicates(t *testing.T) {
	snapshot := ExecutionInstrumentationSnapshot{
		Timings: []ExecutionTiming{{
			Section:  "relationship_join",
			Name:     "phase_reduce_elapsed",
			Duration: 3 * time.Millisecond,
			Detail:   "edge=1",
		}},
		Counters: []ExecutionCounter{{
			Section: "relationship_join",
			Name:    "matched_rows",
			Value:   42,
			Detail:  "edge=1",
		}},
		Events: []ExecutionEvent{{
			Section: "relationship_join",
			Name:    "strategy",
			Value:   "bsi_bitwise",
			Detail:  "edge=1",
		}},
	}

	merged := executionInstrumentationSnapshotWithMissingProbes(snapshot, []ExecutionProbe{
		{Section: "relationship_join", Name: "phase_reduce_elapsed", Value: "3ms", Detail: "edge=1"},
		{Section: "relationship_join", Name: "matched_rows", Value: "42", Detail: "edge=1"},
		{Section: "relationship_join", Name: "strategy", Value: "bsi_bitwise", Detail: "edge=1"},
		{Section: "relationship_join", Name: "phase_projection_elapsed", Value: "2ms", Detail: "edge=1"},
		{Section: "relationship_join", Name: "projection_rows", Value: "10", Detail: "edge=1"},
		{Section: "relationship_join", Name: "projection_mode", Value: "partial", Detail: "edge=1"},
	})

	if len(merged.Timings) != 2 || len(merged.Counters) != 2 || len(merged.Events) != 2 {
		t.Fatalf("merged snapshot = %#v, want one new timing/counter/event without duplicates", merged)
	}
	if merged.Timings[1].Name != "phase_projection_elapsed" || merged.Counters[1].Name != "projection_rows" || merged.Events[1].Name != "projection_mode" {
		t.Fatalf("merged snapshot appended wrong probes: %#v", merged)
	}
}

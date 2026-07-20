package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

func TestRuntimeRoadmapEngineLogsExecutionInstrumentation(t *testing.T) {
	var lines []string
	engine := runtimeRoadmapEngine{
		Logf: func(format string, args ...interface{}) {
			lines = append(lines, fmt.Sprintf(format, args...))
		},
	}

	engine.logExecutionInstrumentation(qsruntime.ExecutionInstrumentationSnapshot{
		Timings: []qsruntime.ExecutionTiming{{
			Section:  "direct_bitmap",
			Name:     "phase_bitmap_query_elapsed",
			Duration: 12 * time.Millisecond,
			Detail:   "query",
		}},
		Counters: []qsruntime.ExecutionCounter{{
			Section: "direct_bitmap",
			Name:    "bitmap_count",
			Value:   42,
			Detail:  "rownums=42",
		}},
		Events: []qsruntime.ExecutionEvent{{
			Section: "optimizer",
			Name:    "strategy",
			Value:   "seed_cache",
			Detail:  "warm",
		}},
	})

	joined := strings.Join(lines, "\n")
	for _, want := range []string{
		"RUNTIME timing section=direct_bitmap name=phase_bitmap_query_elapsed value=12ms detail=query",
		"RUNTIME counter section=direct_bitmap name=bitmap_count value=42 detail=rownums=42",
		"RUNTIME event section=optimizer name=strategy value=seed_cache detail=warm",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("instrumentation log missing %q in:\n%s", want, joined)
		}
	}
}

func TestRuntimeInstrumentationProfileRows(t *testing.T) {
	rows := runtimeInstrumentationProfileRows(qsruntime.ExecutionInstrumentationSnapshot{
		Timings: []qsruntime.ExecutionTiming{{
			Section:  "sql_runtime",
			Name:     "phase_total_elapsed",
			Duration: 15 * time.Millisecond,
			Detail:   "select",
		}},
		Counters: []qsruntime.ExecutionCounter{{
			Section: "direct_bitmap",
			Name:    "bitmap_count",
			Value:   60175,
			Detail:  "rownums=60175",
		}},
		Events: []qsruntime.ExecutionEvent{{
			Section: "relationship_join",
			Name:    "projection_cache_hit",
			Value:   "true",
			Detail:  "edge=lineitem.orders",
		}},
	})

	if len(rows) != 3 {
		t.Fatalf("rows = %d, want 3", len(rows))
	}
	assertProfileRow(t, rows[0], "timing", "sql_runtime", "phase_total_elapsed", "15ms", "select")
	assertProfileRow(t, rows[1], "counter", "direct_bitmap", "bitmap_count", "60175", "rownums=60175")
	assertProfileRow(t, rows[2], "event", "relationship_join", "projection_cache_hit", "true", "edge=lineitem.orders")
}

func TestRuntimeRoadmapEngineQueryProfileReturnsLastInstrumentationCopy(t *testing.T) {
	engine := &runtimeRoadmapEngine{}
	engine.lastProfile = []roadmap.ProfileRow{{
		Kind:    "timing",
		Section: "sql_runtime",
		Name:    "phase_total_elapsed",
		Value:   "3ms",
	}}

	rows, err := engine.QueryProfile(context.Background())
	if err != nil {
		t.Fatalf("QueryProfile error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	rows[0].Value = "mutated"
	if engine.lastProfile[0].Value != "3ms" {
		t.Fatalf("QueryProfile returned alias to profile rows")
	}
}

func assertProfileRow(t *testing.T, row roadmap.ProfileRow, kind, section, name, value, detail string) {
	t.Helper()
	if row.Kind == kind && row.Section == section && row.Name == name && row.Value == value && row.Detail == detail {
		return
	}
	t.Fatalf("unexpected profile row %#v, want kind=%s section=%s name=%s value=%s detail=%s", row, kind, section, name, value, detail)
}

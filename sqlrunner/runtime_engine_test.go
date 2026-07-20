package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsruntime"
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

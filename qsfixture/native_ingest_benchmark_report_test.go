package qsfixture

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
)

func TestBuildNativeIngestBenchmarkReportCapturesProfiles(t *testing.T) {
	report := BuildNativeIngestBenchmarkReport(NativeIngestBenchmarkReportRequest{
		Profile:           "unit-profile",
		Mode:              "inabox-standard",
		OrderCount:        2,
		LineitemsPerOrder: 3,
		ShardCount:        1,
		RunCount:          4,
		PrimaryKeyMode:    "assume_new",
		Elapsed:           10 * time.Millisecond,
		EnqueueElapsed:    4 * time.Millisecond,
		DrainElapsed:      6 * time.Millisecond,
		PutRow: core.RouterPutRowProfileSummary{
			RecordCount:     8,
			ChildRowCount:   24,
			LogicalRowCount: 32,
			InsertedCount:   8,
			TotalElapsed:    5 * time.Millisecond,
		},
		Drain: core.RouterDrainProfileSummary{
			WorkerCount:  1,
			SessionCount: 2,
			TotalElapsed: 6 * time.Millisecond,
			MaxElapsed:   6 * time.Millisecond,
		},
		Flush: core.RouterFlushProfileSummary{
			FlushCount:                2,
			TotalElapsed:              3 * time.Millisecond,
			PartitionStringEntryCount: 8,
			BSIValueEntryCount:        24,
		},
		Metrics: map[string]float64{
			"orders_per_second": 800,
		},
	})
	if report.Profile != "unit-profile" || report.Config.OrderCount != 2 || report.Counts.TotalLogicalRows != 32 {
		t.Fatalf("report = %+v, want captured config/counts", report)
	}
	if report.Config.PrimaryKeyMode != "assume_new" {
		t.Fatalf("report primary key mode = %q, want assume_new", report.Config.PrimaryKeyMode)
	}
	if report.Timings.EnqueueNanos != int64(4*time.Millisecond) || report.Timings.DrainNanos != int64(6*time.Millisecond) {
		t.Fatalf("report timings = %+v, want enqueue/drain timings", report.Timings)
	}
	if report.PutRow.RecordCount != 8 || report.Flush.FlushCount != 2 {
		t.Fatalf("report profiles = %+v/%+v, want captured summaries", report.PutRow, report.Flush)
	}
	if report.Drain.WorkerCount != 1 || report.Drain.SessionCount != 2 {
		t.Fatalf("report drain profile = %+v, want captured drain summary", report.Drain)
	}
	if report.PutRow.ChildRowCount != 24 || report.PutRow.LogicalRowCount != 32 {
		t.Fatalf("report put row counts = %+v, want profile child/logical row counts", report.PutRow)
	}

	path := filepath.Join(t.TempDir(), "profiles", "ingest.json")
	if err := WriteNativeIngestBenchmarkReport(path, report); err != nil {
		t.Fatalf("write report: %v", err)
	}
	decoded, err := ReadNativeIngestBenchmarkReport(path)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	if decoded.Profile != report.Profile || decoded.Counts.TotalLineitems != report.Counts.TotalLineitems {
		t.Fatalf("decoded report = %+v, want %+v", decoded, report)
	}
}

func TestNativeIngestBenchmarkMetricsUsesLogicalRows(t *testing.T) {
	metrics := NativeIngestBenchmarkMetrics(
		2*time.Second,
		750*time.Millisecond,
		1250*time.Millisecond,
		10,
		40,
		50,
		core.RouterPutRowProfileSummary{
			TotalElapsed: 1500 * time.Microsecond,
			PrimaryKey: core.PrimaryKeyResolveProfile{
				ResolveCount:            50,
				LookupRequiredCount:     20,
				AssumeNewCount:          10,
				LocalCacheLookupCount:   30,
				LocalCacheHitCount:      10,
				SkippedKVLookupCount:    10,
				KVLookupCount:           20,
				KVHitCount:              5,
				RownumAllocationCount:   15,
				BatchCacheWriteCount:    15,
				TotalElapsed:            5000 * time.Microsecond,
				KVLookupElapsed:         2000 * time.Microsecond,
				RownumAllocationElapsed: 750 * time.Microsecond,
				BatchCacheWriteElapsed:  300 * time.Microsecond,
			},
		},
		core.RouterDrainProfileSummary{
			WorkerCount:  2,
			SessionCount: 3,
			TotalElapsed: 1500 * time.Microsecond,
			MaxElapsed:   900 * time.Microsecond,
		},
		core.RouterFlushProfileSummary{
			FlushCount:                5,
			TotalElapsed:              2500 * time.Microsecond,
			BSIValueEntryCount:        100,
			PartitionStringEntryCount: 25,
			ByShard: map[string]core.RouterFlushProfileCounter{
				"shard0": {TotalElapsed: 1000 * time.Microsecond},
				"shard1": {TotalElapsed: 1500 * time.Microsecond},
			},
		},
		2,
	)
	if got, want := metrics["logical_rows_per_second"], 25.0; got != want {
		t.Fatalf("logical rows/sec = %v, want %v", got, want)
	}
	if got, want := metrics["logical_rows_per_order"], 5.0; got != want {
		t.Fatalf("logical rows/order = %v, want %v", got, want)
	}
	if got, want := metrics["put_microseconds_per_order"], 150.0; got != want {
		t.Fatalf("put us/order = %v, want %v", got, want)
	}
	if got, want := metrics["enqueue_microseconds_per_order"], 75000.0; got != want {
		t.Fatalf("enqueue us/order = %v, want %v", got, want)
	}
	if got, want := metrics["drain_microseconds_per_order"], 125000.0; got != want {
		t.Fatalf("drain us/order = %v, want %v", got, want)
	}
	if got, want := metrics["drain_wall_percent"], 62.5; got != want {
		t.Fatalf("drain wall percent = %v, want %v", got, want)
	}
	if got, want := metrics["drain_worker_max_microseconds"], 900.0; got != want {
		t.Fatalf("drain worker max us = %v, want %v", got, want)
	}
	if got, want := metrics["drain_worker_sum_microseconds"], 1500.0; got != want {
		t.Fatalf("drain worker sum us = %v, want %v", got, want)
	}
	if got, want := metrics["drain_worker_sum_to_wall_percent"], 0.12; got != want {
		t.Fatalf("drain worker sum/wall percent = %v, want %v", got, want)
	}
	if got, want := metrics["drain_worker_max_to_avg_percent"], 120.0; got != want {
		t.Fatalf("drain worker max/avg percent = %v, want %v", got, want)
	}
	if got, want := metrics["drain_sessions_per_worker"], 1.5; got != want {
		t.Fatalf("drain sessions/worker = %v, want %v", got, want)
	}
	if got, want := metrics["flush_microseconds_per_order"], 250.0; got != want {
		t.Fatalf("flush us/order = %v, want %v", got, want)
	}
	if got, want := metrics["flush_summed_to_drain_percent"], 0.2; got != want {
		t.Fatalf("flush summed/drain percent = %v, want %v", got, want)
	}
	if got, want := metrics["flush_shard_max_to_avg_percent"], 120.0; got != want {
		t.Fatalf("flush shard max/avg percent = %v, want %v", got, want)
	}
	if got, want := metrics["flushes_per_operation"], 2.5; got != want {
		t.Fatalf("flushes/op = %v, want %v", got, want)
	}
	if got, want := metrics["primary_key_resolves_per_logical_row"], 1.0; got != want {
		t.Fatalf("primary key resolves/logical row = %v, want %v", got, want)
	}
	if got, want := metrics["primary_key_total_microseconds_per_resolve"], 100.0; got != want {
		t.Fatalf("primary key total us/resolve = %v, want %v", got, want)
	}
	if got, want := metrics["primary_key_local_cache_hit_percent"], 100.0/3.0; got != want {
		t.Fatalf("primary key local cache hit percent = %v, want %v", got, want)
	}
	if got, want := metrics["primary_key_assume_new_percent"], 50.0; got != want {
		t.Fatalf("primary key assume-new percent = %v, want %v", got, want)
	}
	if got, want := metrics["primary_key_skipped_kv_lookup_percent"], 50.0; got != want {
		t.Fatalf("primary key skipped kv lookup percent = %v, want %v", got, want)
	}
	if got, want := metrics["primary_key_kv_lookup_microseconds_per_lookup"], 100.0; got != want {
		t.Fatalf("primary key kv lookup us/lookup = %v, want %v", got, want)
	}
}

func TestCompareNativeIngestBenchmarkReportsRendersMarkdown(t *testing.T) {
	baseline := NativeIngestBenchmarkReport{
		Profile: "baseline",
		Mode:    "inabox-standard",
		Metrics: map[string]float64{
			"logical_rows_per_second":                    1000,
			"enqueue_microseconds_per_order":             12,
			"drain_microseconds_per_order":               100,
			"drain_worker_max_microseconds":              900,
			"drain_worker_sum_to_wall_percent":           70,
			"drain_worker_max_to_avg_percent":            140,
			"put_microseconds_per_order":                 50,
			"flush_microseconds_per_order":               20,
			"flush_summed_to_drain_percent":              25,
			"flush_shard_max_to_avg_percent":             160,
			"primary_key_total_microseconds_per_resolve": 10,
			"primary_key_skipped_kv_lookup_percent":      0,
			"custom_metric":                              2,
		},
	}
	target := NativeIngestBenchmarkReport{
		Profile: "target",
		Mode:    "inabox-standard",
		Metrics: map[string]float64{
			"logical_rows_per_second":                    1500,
			"enqueue_microseconds_per_order":             8,
			"drain_microseconds_per_order":               60,
			"drain_worker_max_microseconds":              600,
			"drain_worker_sum_to_wall_percent":           45,
			"drain_worker_max_to_avg_percent":            110,
			"put_microseconds_per_order":                 40,
			"flush_microseconds_per_order":               15,
			"flush_summed_to_drain_percent":              18,
			"flush_shard_max_to_avg_percent":             120,
			"primary_key_total_microseconds_per_resolve": 8,
			"primary_key_skipped_kv_lookup_percent":      100,
			"custom_metric":                              3,
		},
	}

	comparison := CompareNativeIngestBenchmarkReports(baseline, target)
	if len(comparison.Metrics) < 3 {
		t.Fatalf("comparison metrics = %+v, want known and custom metrics", comparison.Metrics)
	}
	logicalRows := findNativeIngestBenchmarkMetric(t, comparison, "logical_rows_per_second")
	if logicalRows.Ratio != 1.5 || logicalRows.Outcome != "better" {
		t.Fatalf("logical row comparison = %+v, want 1.5x better", logicalRows)
	}
	putCost := findNativeIngestBenchmarkMetric(t, comparison, "put_microseconds_per_order")
	if putCost.Ratio != 0.8 || putCost.Outcome != "better" {
		t.Fatalf("put cost comparison = %+v, want 0.8x better", putCost)
	}

	markdown := RenderNativeIngestBenchmarkComparisonMarkdown(comparison)
	for _, fragment := range []string{
		"# Native Ingest Benchmark Comparison",
		"Baseline: baseline (inabox-standard)",
		"Target: target (inabox-standard)",
		"## Load Path Summary",
		"| Signal | Baseline | Target | Delta | Ratio | Direction |",
		"| Logical rows/sec | 1000 rows/s | 1500 rows/s | 500 rows/s | 1.50x | better |",
		"| Enqueue us/order | 12 us/order | 8 us/order | -4 us/order | 0.67x | better |",
		"| Drain us/order | 100 us/order | 60 us/order | -40 us/order | 0.60x | better |",
		"| Slowest worker drain | 900 us | 600 us | -300 us | 0.67x | better |",
		"| Drain worker max/avg | 140 percent | 110 percent | -30 percent | 0.79x | better |",
		"| Flush us/order | 20 us/order | 15 us/order | -5 us/order | 0.75x | better |",
		"| Flush shard max/avg | 160 percent | 120 percent | -40 percent | 0.75x | better |",
		"| Skipped PK KV lookup | 0 percent | 100 percent | 100 percent | n/a | better |",
		"## Detailed Metrics",
		"| logical_rows_per_second | 1000 rows/s | 1500 rows/s | 500 rows/s | 1.50x | better |",
		"| put_microseconds_per_order | 50 us/order | 40 us/order | -10 us/order | 0.80x | better |",
		"| primary_key_total_microseconds_per_resolve | 10 us/resolve | 8 us/resolve | -2 us/resolve | 0.80x | better |",
	} {
		if !strings.Contains(markdown, fragment) {
			t.Fatalf("markdown missing %q:\n%s", fragment, markdown)
		}
	}
}

func findNativeIngestBenchmarkMetric(
	t *testing.T,
	comparison NativeIngestBenchmarkComparison,
	name string,
) NativeIngestBenchmarkMetricComparison {
	t.Helper()
	for _, metric := range comparison.Metrics {
		if metric.Name == name {
			return metric
		}
	}
	t.Fatalf("metric %q not found in %+v", name, comparison.Metrics)
	return NativeIngestBenchmarkMetricComparison{}
}

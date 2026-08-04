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
		Elapsed:           10 * time.Millisecond,
		PutRow: core.RouterPutRowProfileSummary{
			RecordCount:     8,
			ChildRowCount:   24,
			LogicalRowCount: 32,
			InsertedCount:   8,
			TotalElapsed:    5 * time.Millisecond,
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
	if report.PutRow.RecordCount != 8 || report.Flush.FlushCount != 2 {
		t.Fatalf("report profiles = %+v/%+v, want captured summaries", report.PutRow, report.Flush)
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
		10,
		40,
		50,
		core.RouterPutRowProfileSummary{TotalElapsed: 1500 * time.Microsecond},
		core.RouterFlushProfileSummary{
			FlushCount:                5,
			TotalElapsed:              2500 * time.Microsecond,
			BSIValueEntryCount:        100,
			PartitionStringEntryCount: 25,
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
	if got, want := metrics["flushes_per_operation"], 2.5; got != want {
		t.Fatalf("flushes/op = %v, want %v", got, want)
	}
}

func TestCompareNativeIngestBenchmarkReportsRendersMarkdown(t *testing.T) {
	baseline := NativeIngestBenchmarkReport{
		Profile: "baseline",
		Mode:    "inabox-standard",
		Metrics: map[string]float64{
			"logical_rows_per_second":    1000,
			"put_microseconds_per_order": 50,
			"custom_metric":              2,
		},
	}
	target := NativeIngestBenchmarkReport{
		Profile: "target",
		Mode:    "inabox-standard",
		Metrics: map[string]float64{
			"logical_rows_per_second":    1500,
			"put_microseconds_per_order": 40,
			"custom_metric":              3,
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
		"| logical_rows_per_second | 1000 rows/s | 1500 rows/s | 500 rows/s | 1.50x | better |",
		"| put_microseconds_per_order | 50 us/order | 40 us/order | -10 us/order | 0.80x | better |",
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

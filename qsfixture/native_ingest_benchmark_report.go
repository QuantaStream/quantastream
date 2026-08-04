package qsfixture

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
)

const nativeIngestBenchmarkReportVersion = 1

// NativeIngestBenchmarkReportRequest captures one native ingest benchmark run.
type NativeIngestBenchmarkReportRequest struct {
	Profile           string
	Mode              string
	OrderCount        int
	LineitemsPerOrder int
	ShardCount        int
	RunCount          int
	PrimaryKeyMode    string
	Elapsed           time.Duration
	EnqueueElapsed    time.Duration
	DrainElapsed      time.Duration
	PutRow            core.RouterPutRowProfileSummary
	Drain             core.RouterDrainProfileSummary
	Flush             core.RouterFlushProfileSummary
	Metrics           map[string]float64
}

// NativeIngestBenchmarkReport is the portable JSON shape emitted by native
// ingest benchmarks.
type NativeIngestBenchmarkReport struct {
	Version     int                             `json:"version"`
	Profile     string                          `json:"profile"`
	Mode        string                          `json:"mode"`
	GeneratedAt time.Time                       `json:"generated_at"`
	Config      NativeIngestBenchmarkConfig     `json:"config"`
	Counts      NativeIngestBenchmarkCounts     `json:"counts"`
	Timings     NativeIngestBenchmarkTimings    `json:"timings"`
	Metrics     map[string]float64              `json:"metrics"`
	PutRow      core.RouterPutRowProfileSummary `json:"put_row"`
	Drain       core.RouterDrainProfileSummary  `json:"drain"`
	Flush       core.RouterFlushProfileSummary  `json:"flush"`
}

// NativeIngestBenchmarkConfig records benchmark input parameters.
type NativeIngestBenchmarkConfig struct {
	OrderCount        int    `json:"order_count"`
	LineitemsPerOrder int    `json:"lineitems_per_order"`
	ShardCount        int    `json:"shard_count"`
	RunCount          int    `json:"run_count"`
	PrimaryKeyMode    string `json:"primary_key_mode,omitempty"`
}

// NativeIngestBenchmarkCounts records logical row totals produced by a run.
type NativeIngestBenchmarkCounts struct {
	TotalOrders      int `json:"total_orders"`
	TotalLineitems   int `json:"total_lineitems"`
	TotalLogicalRows int `json:"total_logical_rows"`
}

// NativeIngestBenchmarkTimings records wall-clock benchmark timings.
type NativeIngestBenchmarkTimings struct {
	Elapsed      string `json:"elapsed"`
	ElapsedNanos int64  `json:"elapsed_nanos"`
	Enqueue      string `json:"enqueue_elapsed,omitempty"`
	EnqueueNanos int64  `json:"enqueue_elapsed_nanos,omitempty"`
	Drain        string `json:"drain_elapsed,omitempty"`
	DrainNanos   int64  `json:"drain_elapsed_nanos,omitempty"`
}

// BuildNativeIngestBenchmarkReport builds the portable report shape for a run.
func BuildNativeIngestBenchmarkReport(request NativeIngestBenchmarkReportRequest) NativeIngestBenchmarkReport {
	totalOrders := request.OrderCount * request.RunCount
	totalLineitems := totalOrders * request.LineitemsPerOrder
	return NativeIngestBenchmarkReport{
		Version:     nativeIngestBenchmarkReportVersion,
		Profile:     request.Profile,
		Mode:        request.Mode,
		GeneratedAt: time.Now().UTC(),
		Config: NativeIngestBenchmarkConfig{
			OrderCount:        request.OrderCount,
			LineitemsPerOrder: request.LineitemsPerOrder,
			ShardCount:        request.ShardCount,
			RunCount:          request.RunCount,
			PrimaryKeyMode:    request.PrimaryKeyMode,
		},
		Counts: NativeIngestBenchmarkCounts{
			TotalOrders:      totalOrders,
			TotalLineitems:   totalLineitems,
			TotalLogicalRows: totalOrders + totalLineitems,
		},
		Timings: NativeIngestBenchmarkTimings{
			Elapsed:      request.Elapsed.String(),
			ElapsedNanos: request.Elapsed.Nanoseconds(),
			Enqueue:      nativeIngestBenchmarkOptionalDurationString(request.EnqueueElapsed),
			EnqueueNanos: request.EnqueueElapsed.Nanoseconds(),
			Drain:        nativeIngestBenchmarkOptionalDurationString(request.DrainElapsed),
			DrainNanos:   request.DrainElapsed.Nanoseconds(),
		},
		Metrics: copyNativeIngestBenchmarkMetrics(request.Metrics),
		PutRow:  request.PutRow,
		Drain:   request.Drain,
		Flush:   request.Flush,
	}
}

// WriteNativeIngestBenchmarkReport writes a report when path is non-empty.
func WriteNativeIngestBenchmarkReport(path string, report NativeIngestBenchmarkReport) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0644)
}

// ReadNativeIngestBenchmarkReport reads a previously written report.
func ReadNativeIngestBenchmarkReport(path string) (NativeIngestBenchmarkReport, error) {
	var report NativeIngestBenchmarkReport
	data, err := os.ReadFile(path)
	if err != nil {
		return report, err
	}
	if err := json.Unmarshal(data, &report); err != nil {
		return report, err
	}
	return report, nil
}

// NativeIngestBenchmarkMetrics derives normalized throughput and cost metrics.
func NativeIngestBenchmarkMetrics(
	elapsed time.Duration,
	enqueueElapsed time.Duration,
	drainElapsed time.Duration,
	totalOrders int,
	totalLineitems int,
	totalLogicalRows int,
	putSnapshot core.RouterPutRowProfileSummary,
	drainSnapshot core.RouterDrainProfileSummary,
	flushSnapshot core.RouterFlushProfileSummary,
	runCount int,
) map[string]float64 {
	elapsedSeconds := elapsed.Seconds()
	if elapsedSeconds <= 0 {
		return map[string]float64{}
	}
	metrics := map[string]float64{
		"orders_per_second":       float64(totalOrders) / elapsedSeconds,
		"lineitems_per_second":    float64(totalLineitems) / elapsedSeconds,
		"logical_rows_per_second": float64(totalLogicalRows) / elapsedSeconds,
		"logical_rows_per_order":  float64(totalLogicalRows) / float64(maxNativeIngestBenchmarkInt(1, totalOrders)),
		"enqueue_microseconds_per_order": float64(enqueueElapsed.Microseconds()) /
			float64(maxNativeIngestBenchmarkInt(1, totalOrders)),
		"drain_microseconds_per_order": float64(drainElapsed.Microseconds()) /
			float64(maxNativeIngestBenchmarkInt(1, totalOrders)),
		"drain_wall_percent":               percentForDurations(drainElapsed, elapsed),
		"drain_worker_max_microseconds":    float64(drainSnapshot.MaxElapsed.Microseconds()),
		"drain_worker_sum_microseconds":    float64(drainSnapshot.TotalElapsed.Microseconds()),
		"drain_worker_sum_to_wall_percent": percentForDurations(drainSnapshot.TotalElapsed, drainElapsed),
		"drain_worker_max_to_avg_percent":  maxToAverageDurationPercent(drainSnapshot.TotalElapsed, drainSnapshot.MaxElapsed, drainSnapshot.WorkerCount),
		"drain_sessions_per_worker": float64(drainSnapshot.SessionCount) /
			float64(maxNativeIngestBenchmarkInt(1, drainSnapshot.WorkerCount)),
		"put_microseconds_per_order": float64(putSnapshot.TotalElapsed.Microseconds()) / float64(maxNativeIngestBenchmarkInt(1, totalOrders)),
		"flush_microseconds_per_order": float64(flushSnapshot.TotalElapsed.Microseconds()) /
			float64(maxNativeIngestBenchmarkInt(1, totalOrders)),
		"flush_microseconds_per_flush":   float64(flushSnapshot.TotalElapsed.Microseconds()) / float64(maxNativeIngestBenchmarkInt(1, flushSnapshot.FlushCount)),
		"flush_summed_to_drain_percent":  percentForDurations(flushSnapshot.TotalElapsed, drainElapsed),
		"flush_shard_max_to_avg_percent": routerFlushShardMaxToAveragePercent(flushSnapshot),
		"flushes_per_operation":          float64(flushSnapshot.FlushCount) / float64(maxNativeIngestBenchmarkInt(1, runCount)),
		"bsi_entries_per_logical_row":    float64(flushSnapshot.BSIValueEntryCount) / float64(maxNativeIngestBenchmarkInt(1, totalLogicalRows)),
		"kv_entries_per_logical_row":     float64(flushSnapshot.PartitionStringEntryCount) / float64(maxNativeIngestBenchmarkInt(1, totalLogicalRows)),
	}
	pk := putSnapshot.PrimaryKey
	metrics["primary_key_resolves_per_logical_row"] = float64(pk.ResolveCount) / float64(maxNativeIngestBenchmarkInt(1, totalLogicalRows))
	metrics["primary_key_total_microseconds_per_resolve"] = durationMicrosPerCount(pk.TotalElapsed, pk.ResolveCount)
	metrics["primary_key_local_cache_hit_percent"] = percentForCounts(pk.LocalCacheHitCount, pk.LocalCacheLookupCount)
	metrics["primary_key_assume_new_percent"] = percentForCounts(pk.AssumeNewCount, pk.LookupRequiredCount)
	metrics["primary_key_skipped_kv_lookup_percent"] = percentForCounts(pk.SkippedKVLookupCount, pk.LookupRequiredCount)
	metrics["primary_key_kv_hit_percent"] = percentForCounts(pk.KVHitCount, pk.KVLookupCount)
	metrics["primary_key_kv_lookup_microseconds_per_lookup"] = durationMicrosPerCount(pk.KVLookupElapsed, pk.KVLookupCount)
	metrics["primary_key_allocation_microseconds_per_allocation"] = durationMicrosPerCount(pk.RownumAllocationElapsed, pk.RownumAllocationCount)
	metrics["primary_key_batch_cache_write_microseconds_per_write"] = durationMicrosPerCount(pk.BatchCacheWriteElapsed, pk.BatchCacheWriteCount)
	return metrics
}

// NativeIngestBenchmarkComparison summarizes two native ingest reports.
type NativeIngestBenchmarkComparison struct {
	Baseline NativeIngestBenchmarkComparisonReport   `json:"baseline"`
	Target   NativeIngestBenchmarkComparisonReport   `json:"target"`
	Metrics  []NativeIngestBenchmarkMetricComparison `json:"metrics"`
}

// NativeIngestBenchmarkComparisonReport identifies one side of a comparison.
type NativeIngestBenchmarkComparisonReport struct {
	Profile string                       `json:"profile"`
	Mode    string                       `json:"mode"`
	Config  NativeIngestBenchmarkConfig  `json:"config"`
	Counts  NativeIngestBenchmarkCounts  `json:"counts"`
	Timings NativeIngestBenchmarkTimings `json:"timings"`
}

// NativeIngestBenchmarkMetricComparison records one metric delta.
type NativeIngestBenchmarkMetricComparison struct {
	Name           string  `json:"name"`
	Unit           string  `json:"unit,omitempty"`
	Baseline       float64 `json:"baseline"`
	Target         float64 `json:"target"`
	Delta          float64 `json:"delta"`
	Ratio          float64 `json:"ratio"`
	HigherIsBetter bool    `json:"higher_is_better"`
	Outcome        string  `json:"outcome"`
}

type nativeIngestBenchmarkMetricDefinition struct {
	name           string
	unit           string
	higherIsBetter bool
}

type nativeIngestBenchmarkSummaryMetric struct {
	name  string
	label string
}

var nativeIngestBenchmarkMetricDefinitions = []nativeIngestBenchmarkMetricDefinition{
	{name: "logical_rows_per_second", unit: "rows/s", higherIsBetter: true},
	{name: "orders_per_second", unit: "orders/s", higherIsBetter: true},
	{name: "lineitems_per_second", unit: "lineitems/s", higherIsBetter: true},
	{name: "put_microseconds_per_order", unit: "us/order", higherIsBetter: false},
	{name: "enqueue_microseconds_per_order", unit: "us/order", higherIsBetter: false},
	{name: "drain_microseconds_per_order", unit: "us/order", higherIsBetter: false},
	{name: "drain_wall_percent", unit: "percent", higherIsBetter: false},
	{name: "drain_worker_max_microseconds", unit: "us", higherIsBetter: false},
	{name: "drain_worker_sum_microseconds", unit: "us", higherIsBetter: false},
	{name: "drain_worker_sum_to_wall_percent", unit: "percent", higherIsBetter: false},
	{name: "drain_worker_max_to_avg_percent", unit: "percent", higherIsBetter: false},
	{name: "drain_sessions_per_worker", unit: "sessions/worker", higherIsBetter: false},
	{name: "flush_microseconds_per_order", unit: "us/order", higherIsBetter: false},
	{name: "flush_microseconds_per_flush", unit: "us/flush", higherIsBetter: false},
	{name: "flush_summed_to_drain_percent", unit: "percent", higherIsBetter: false},
	{name: "flush_shard_max_to_avg_percent", unit: "percent", higherIsBetter: false},
	{name: "flushes_per_operation", unit: "flushes/op", higherIsBetter: false},
	{name: "bsi_entries_per_logical_row", unit: "entries/row", higherIsBetter: false},
	{name: "kv_entries_per_logical_row", unit: "entries/row", higherIsBetter: false},
	{name: "logical_rows_per_order", unit: "rows/order", higherIsBetter: true},
	{name: "primary_key_resolves_per_logical_row", unit: "resolves/row", higherIsBetter: false},
	{name: "primary_key_total_microseconds_per_resolve", unit: "us/resolve", higherIsBetter: false},
	{name: "primary_key_local_cache_hit_percent", unit: "percent", higherIsBetter: true},
	{name: "primary_key_assume_new_percent", unit: "percent", higherIsBetter: true},
	{name: "primary_key_skipped_kv_lookup_percent", unit: "percent", higherIsBetter: true},
	{name: "primary_key_kv_hit_percent", unit: "percent", higherIsBetter: false},
	{name: "primary_key_kv_lookup_microseconds_per_lookup", unit: "us/lookup", higherIsBetter: false},
	{name: "primary_key_allocation_microseconds_per_allocation", unit: "us/allocation", higherIsBetter: false},
	{name: "primary_key_batch_cache_write_microseconds_per_write", unit: "us/write", higherIsBetter: false},
}

var nativeIngestBenchmarkSummaryMetrics = []nativeIngestBenchmarkSummaryMetric{
	{name: "logical_rows_per_second", label: "Logical rows/sec"},
	{name: "enqueue_microseconds_per_order", label: "Enqueue us/order"},
	{name: "drain_microseconds_per_order", label: "Drain us/order"},
	{name: "drain_worker_max_microseconds", label: "Slowest worker drain"},
	{name: "drain_worker_sum_to_wall_percent", label: "Worker drain sum/wall"},
	{name: "drain_worker_max_to_avg_percent", label: "Drain worker max/avg"},
	{name: "put_microseconds_per_order", label: "PutRow us/order"},
	{name: "flush_microseconds_per_order", label: "Flush us/order"},
	{name: "flush_summed_to_drain_percent", label: "Flush sum/drain"},
	{name: "flush_shard_max_to_avg_percent", label: "Flush shard max/avg"},
	{name: "primary_key_total_microseconds_per_resolve", label: "PK resolve us"},
	{name: "primary_key_skipped_kv_lookup_percent", label: "Skipped PK KV lookup"},
}

// CompareNativeIngestBenchmarkReports compares metrics from two reports.
func CompareNativeIngestBenchmarkReports(
	baseline NativeIngestBenchmarkReport,
	target NativeIngestBenchmarkReport,
) NativeIngestBenchmarkComparison {
	comparison := NativeIngestBenchmarkComparison{
		Baseline: nativeIngestBenchmarkComparisonReport(baseline),
		Target:   nativeIngestBenchmarkComparisonReport(target),
	}
	seen := map[string]bool{}
	for _, definition := range nativeIngestBenchmarkMetricDefinitions {
		metric, ok := compareNativeIngestBenchmarkMetric(definition, baseline.Metrics, target.Metrics)
		if !ok {
			continue
		}
		comparison.Metrics = append(comparison.Metrics, metric)
		seen[definition.name] = true
	}
	var extraNames []string
	for name := range baseline.Metrics {
		if !seen[name] {
			extraNames = append(extraNames, name)
		}
	}
	for name := range target.Metrics {
		if !seen[name] {
			extraNames = append(extraNames, name)
		}
	}
	sort.Strings(extraNames)
	for _, name := range extraNames {
		if seen[name] {
			continue
		}
		metric, ok := compareNativeIngestBenchmarkMetric(nativeIngestBenchmarkMetricDefinition{name: name}, baseline.Metrics, target.Metrics)
		if ok {
			comparison.Metrics = append(comparison.Metrics, metric)
		}
		seen[name] = true
	}
	return comparison
}

// RenderNativeIngestBenchmarkComparisonMarkdown renders a compact comparison.
func RenderNativeIngestBenchmarkComparisonMarkdown(comparison NativeIngestBenchmarkComparison) string {
	var builder strings.Builder
	builder.WriteString("# Native Ingest Benchmark Comparison\n\n")
	builder.WriteString(fmt.Sprintf("Baseline: %s (%s)\n\n", fallbackReportLabel(comparison.Baseline.Profile), fallbackReportLabel(comparison.Baseline.Mode)))
	builder.WriteString(fmt.Sprintf("Target: %s (%s)\n\n", fallbackReportLabel(comparison.Target.Profile), fallbackReportLabel(comparison.Target.Mode)))
	renderNativeIngestBenchmarkSummaryMarkdown(&builder, comparison)
	builder.WriteString("## Detailed Metrics\n\n")
	builder.WriteString("| Metric | Baseline | Target | Delta | Ratio | Direction |\n")
	builder.WriteString("| --- | ---: | ---: | ---: | ---: | --- |\n")
	for _, metric := range comparison.Metrics {
		writeNativeIngestBenchmarkMetricMarkdownRow(&builder, metric.Name, metric)
	}
	return builder.String()
}

func renderNativeIngestBenchmarkSummaryMarkdown(builder *strings.Builder, comparison NativeIngestBenchmarkComparison) {
	summaryMetrics := nativeIngestBenchmarkSummaryMetricComparisons(comparison)
	if len(summaryMetrics) == 0 {
		return
	}
	builder.WriteString("## Load Path Summary\n\n")
	builder.WriteString("| Signal | Baseline | Target | Delta | Ratio | Direction |\n")
	builder.WriteString("| --- | ---: | ---: | ---: | ---: | --- |\n")
	for _, metric := range summaryMetrics {
		writeNativeIngestBenchmarkMetricMarkdownRow(builder, metric.label, metric.comparison)
	}
	builder.WriteString("\n")
}

type nativeIngestBenchmarkSummaryMetricComparison struct {
	label      string
	comparison NativeIngestBenchmarkMetricComparison
}

func nativeIngestBenchmarkSummaryMetricComparisons(
	comparison NativeIngestBenchmarkComparison,
) []nativeIngestBenchmarkSummaryMetricComparison {
	metricsByName := make(map[string]NativeIngestBenchmarkMetricComparison, len(comparison.Metrics))
	for _, metric := range comparison.Metrics {
		metricsByName[metric.Name] = metric
	}
	summaryMetrics := make([]nativeIngestBenchmarkSummaryMetricComparison, 0, len(nativeIngestBenchmarkSummaryMetrics))
	for _, summaryMetric := range nativeIngestBenchmarkSummaryMetrics {
		metric, ok := metricsByName[summaryMetric.name]
		if !ok {
			continue
		}
		summaryMetrics = append(summaryMetrics, nativeIngestBenchmarkSummaryMetricComparison{
			label:      summaryMetric.label,
			comparison: metric,
		})
	}
	return summaryMetrics
}

func writeNativeIngestBenchmarkMetricMarkdownRow(
	builder *strings.Builder,
	label string,
	metric NativeIngestBenchmarkMetricComparison,
) {
	builder.WriteString(fmt.Sprintf(
		"| %s | %s | %s | %s | %s | %s |\n",
		label,
		formatNativeIngestBenchmarkMetric(metric.Baseline, metric.Unit),
		formatNativeIngestBenchmarkMetric(metric.Target, metric.Unit),
		formatNativeIngestBenchmarkMetric(metric.Delta, metric.Unit),
		formatNativeIngestBenchmarkRatio(metric.Ratio),
		metric.Outcome,
	))
}

// WriteNativeIngestBenchmarkComparisonMarkdown writes a comparison when path is non-empty.
func WriteNativeIngestBenchmarkComparisonMarkdown(path string, comparison NativeIngestBenchmarkComparison) error {
	if strings.TrimSpace(path) == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(RenderNativeIngestBenchmarkComparisonMarkdown(comparison)), 0644)
}

func nativeIngestBenchmarkComparisonReport(report NativeIngestBenchmarkReport) NativeIngestBenchmarkComparisonReport {
	return NativeIngestBenchmarkComparisonReport{
		Profile: report.Profile,
		Mode:    report.Mode,
		Config:  report.Config,
		Counts:  report.Counts,
		Timings: report.Timings,
	}
}

func compareNativeIngestBenchmarkMetric(
	definition nativeIngestBenchmarkMetricDefinition,
	baseline map[string]float64,
	target map[string]float64,
) (NativeIngestBenchmarkMetricComparison, bool) {
	baselineValue, baselineOK := baseline[definition.name]
	targetValue, targetOK := target[definition.name]
	if !baselineOK && !targetOK {
		return NativeIngestBenchmarkMetricComparison{}, false
	}
	delta := targetValue - baselineValue
	ratio := 0.0
	if baselineValue != 0 {
		ratio = targetValue / baselineValue
	}
	return NativeIngestBenchmarkMetricComparison{
		Name:           definition.name,
		Unit:           definition.unit,
		Baseline:       baselineValue,
		Target:         targetValue,
		Delta:          delta,
		Ratio:          ratio,
		HigherIsBetter: definition.higherIsBetter,
		Outcome:        nativeIngestBenchmarkMetricOutcome(delta, definition.higherIsBetter),
	}, true
}

func nativeIngestBenchmarkMetricOutcome(delta float64, higherIsBetter bool) string {
	if math.Abs(delta) < 0.000000001 {
		return "flat"
	}
	if higherIsBetter {
		if delta > 0 {
			return "better"
		}
		return "worse"
	}
	if delta < 0 {
		return "better"
	}
	return "worse"
}

func fallbackReportLabel(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "(unknown)"
	}
	return value
}

func formatNativeIngestBenchmarkMetric(value float64, unit string) string {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fmt.Sprint(value)
	}
	if unit == "" {
		return fmt.Sprintf("%.4g", value)
	}
	return fmt.Sprintf("%.4g %s", value, unit)
}

func formatNativeIngestBenchmarkRatio(value float64) string {
	if value == 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return "n/a"
	}
	return fmt.Sprintf("%.2fx", value)
}

func durationMicrosPerCount(duration time.Duration, count int) float64 {
	return float64(duration.Microseconds()) / float64(maxNativeIngestBenchmarkInt(1, count))
}

func percentForCounts(numerator int, denominator int) float64 {
	if denominator <= 0 {
		return 0
	}
	return 100 * float64(numerator) / float64(denominator)
}

func percentForDurations(numerator time.Duration, denominator time.Duration) float64 {
	if denominator <= 0 {
		return 0
	}
	return 100 * float64(numerator) / float64(denominator)
}

func maxToAverageDurationPercent(total time.Duration, max time.Duration, count int) float64 {
	if count <= 0 || total <= 0 {
		return 0
	}
	average := float64(total) / float64(count)
	if average <= 0 {
		return 0
	}
	return 100 * float64(max) / average
}

func routerFlushShardMaxToAveragePercent(snapshot core.RouterFlushProfileSummary) float64 {
	var total time.Duration
	var max time.Duration
	var count int
	for _, counter := range snapshot.ByShard {
		if counter.TotalElapsed <= 0 {
			continue
		}
		total += counter.TotalElapsed
		if counter.TotalElapsed > max {
			max = counter.TotalElapsed
		}
		count++
	}
	return maxToAverageDurationPercent(total, max, count)
}

func nativeIngestBenchmarkOptionalDurationString(duration time.Duration) string {
	if duration == 0 {
		return ""
	}
	return duration.String()
}

func copyNativeIngestBenchmarkMetrics(src map[string]float64) map[string]float64 {
	if src == nil {
		return nil
	}
	dst := make(map[string]float64, len(src))
	for key, value := range src {
		dst[key] = value
	}
	return dst
}

func maxNativeIngestBenchmarkInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

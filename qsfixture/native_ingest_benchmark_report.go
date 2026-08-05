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
	Profile                 string
	Mode                    string
	OrderCount              int
	LineitemsPerOrder       int
	ShardCount              int
	RunCount                int
	ReplayCount             int
	PrimaryKeyMode          string
	PrimaryKeyAuthority     string
	PrimaryKeyShadow        string
	PrimaryKeyShadowProfile core.PrimaryKeyShadowProfileSummary
	Elapsed                 time.Duration
	EnqueueElapsed          time.Duration
	DrainElapsed            time.Duration
	PutRow                  core.RouterPutRowProfileSummary
	Drain                   core.RouterDrainProfileSummary
	Flush                   core.RouterFlushProfileSummary
	Metrics                 map[string]float64
}

// NativeIngestBenchmarkReport is the portable JSON shape emitted by native
// ingest benchmarks.
type NativeIngestBenchmarkReport struct {
	Version                 int                                 `json:"version"`
	Profile                 string                              `json:"profile"`
	Mode                    string                              `json:"mode"`
	GeneratedAt             time.Time                           `json:"generated_at"`
	Config                  NativeIngestBenchmarkConfig         `json:"config"`
	Counts                  NativeIngestBenchmarkCounts         `json:"counts"`
	Timings                 NativeIngestBenchmarkTimings        `json:"timings"`
	Metrics                 map[string]float64                  `json:"metrics"`
	PutRow                  core.RouterPutRowProfileSummary     `json:"put_row"`
	Drain                   core.RouterDrainProfileSummary      `json:"drain"`
	Flush                   core.RouterFlushProfileSummary      `json:"flush"`
	PrimaryKeyShadowProfile core.PrimaryKeyShadowProfileSummary `json:"primary_key_shadow_profile"`
}

// NativeIngestBenchmarkConfig records benchmark input parameters.
type NativeIngestBenchmarkConfig struct {
	OrderCount          int    `json:"order_count"`
	LineitemsPerOrder   int    `json:"lineitems_per_order"`
	ShardCount          int    `json:"shard_count"`
	RunCount            int    `json:"run_count"`
	ReplayCount         int    `json:"replay_count,omitempty"`
	PrimaryKeyMode      string `json:"primary_key_mode,omitempty"`
	PrimaryKeyAuthority string `json:"primary_key_authority,omitempty"`
	PrimaryKeyShadow    string `json:"primary_key_shadow,omitempty"`
}

// NativeIngestBenchmarkCounts records logical row totals produced by a run.
type NativeIngestBenchmarkCounts struct {
	TotalOrders         int `json:"total_orders"`
	TotalLineitems      int `json:"total_lineitems"`
	TotalLogicalRows    int `json:"total_logical_rows"`
	TotalOrderWrites    int `json:"total_order_writes,omitempty"`
	TotalLineitemWrites int `json:"total_lineitem_writes,omitempty"`
	TotalLogicalWrites  int `json:"total_logical_writes,omitempty"`
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
	replayCount := maxNativeIngestBenchmarkInt(1, request.ReplayCount)
	totalOrders := request.OrderCount * request.RunCount
	totalLineitems := totalOrders * request.LineitemsPerOrder
	totalLogicalRows := totalOrders + totalLineitems
	totalOrderWrites := totalOrders * replayCount
	totalLineitemWrites := totalLineitems * replayCount
	totalLogicalWrites := totalLogicalRows * replayCount
	return NativeIngestBenchmarkReport{
		Version:     nativeIngestBenchmarkReportVersion,
		Profile:     request.Profile,
		Mode:        request.Mode,
		GeneratedAt: time.Now().UTC(),
		Config: NativeIngestBenchmarkConfig{
			OrderCount:          request.OrderCount,
			LineitemsPerOrder:   request.LineitemsPerOrder,
			ShardCount:          request.ShardCount,
			RunCount:            request.RunCount,
			ReplayCount:         request.ReplayCount,
			PrimaryKeyMode:      request.PrimaryKeyMode,
			PrimaryKeyAuthority: request.PrimaryKeyAuthority,
			PrimaryKeyShadow:    request.PrimaryKeyShadow,
		},
		Counts: NativeIngestBenchmarkCounts{
			TotalOrders:         totalOrders,
			TotalLineitems:      totalLineitems,
			TotalLogicalRows:    totalLogicalRows,
			TotalOrderWrites:    totalOrderWrites,
			TotalLineitemWrites: totalLineitemWrites,
			TotalLogicalWrites:  totalLogicalWrites,
		},
		Timings: NativeIngestBenchmarkTimings{
			Elapsed:      request.Elapsed.String(),
			ElapsedNanos: request.Elapsed.Nanoseconds(),
			Enqueue:      nativeIngestBenchmarkOptionalDurationString(request.EnqueueElapsed),
			EnqueueNanos: request.EnqueueElapsed.Nanoseconds(),
			Drain:        nativeIngestBenchmarkOptionalDurationString(request.DrainElapsed),
			DrainNanos:   request.DrainElapsed.Nanoseconds(),
		},
		Metrics:                 copyNativeIngestBenchmarkMetrics(request.Metrics),
		PutRow:                  request.PutRow,
		Drain:                   request.Drain,
		Flush:                   request.Flush,
		PrimaryKeyShadowProfile: request.PrimaryKeyShadowProfile,
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
	metrics["put_stage_normalize_microseconds_per_order"] = durationMicrosPerCount(putSnapshot.SourceElapsed, totalOrders)
	metrics["put_stage_identity_microseconds_per_order"] = durationMicrosPerCount(putSnapshot.IdentityElapsed, totalOrders)
	metrics["put_stage_primary_key_microseconds_per_order"] = durationMicrosPerCount(pk.TotalElapsed, totalOrders)
	metrics["put_stage_alternate_keys_microseconds_per_order"] = durationMicrosPerCount(putSnapshot.AlternateKeysElapsed, totalOrders)
	metrics["put_stage_child_expansion_microseconds_per_order"] = durationMicrosPerCount(putSnapshot.ChildExpansionElapsed, totalOrders)
	metrics["put_stage_child_traversal_microseconds_per_order"] = durationMicrosPerCount(putSnapshot.ChildTraversalElapsed, totalOrders)
	metrics["put_stage_child_recursive_write_microseconds_per_order"] = durationMicrosPerCount(
		nonNegativeDuration(putSnapshot.ChildExpansionElapsed-putSnapshot.ChildTraversalElapsed), totalOrders)
	metrics["put_stage_parent_relation_microseconds_per_order"] = durationMicrosPerCount(putSnapshot.RelationElapsed, totalOrders)
	metrics["put_stage_attribute_mapping_microseconds_per_order"] = durationMicrosPerCount(putSnapshot.AttributeElapsed, totalOrders)
	metrics["primary_key_resolves_per_logical_row"] = float64(pk.ResolveCount) / float64(maxNativeIngestBenchmarkInt(1, totalLogicalRows))
	metrics["primary_key_total_microseconds_per_resolve"] = durationMicrosPerCount(pk.TotalElapsed, pk.ResolveCount)
	metrics["primary_key_local_cache_hit_percent"] = percentForCounts(pk.LocalCacheHitCount, pk.LocalCacheLookupCount)
	metrics["primary_key_assume_new_percent"] = percentForCounts(pk.AssumeNewCount, pk.LookupRequiredCount)
	metrics["primary_key_skipped_kv_lookup_percent"] = percentForCounts(pk.SkippedKVLookupCount, pk.LookupRequiredCount)
	metrics["primary_key_kv_hit_percent"] = percentForCounts(pk.KVHitCount, pk.KVLookupCount)
	metrics["primary_key_kv_lookup_microseconds_per_lookup"] = durationMicrosPerCount(pk.KVLookupElapsed, pk.KVLookupCount)
	metrics["primary_key_bsi_hit_percent"] = percentForCounts(pk.BSIHitCount, pk.BSILookupCount)
	metrics["primary_key_skipped_bsi_lookup_percent"] = percentForCounts(pk.SkippedBSILookupCount, pk.LookupRequiredCount)
	metrics["primary_key_bsi_lookup_microseconds_per_lookup"] = durationMicrosPerCount(pk.BSILookupElapsed, pk.BSILookupCount)
	metrics["primary_key_bsi_stage_write_microseconds_per_write"] = durationMicrosPerCount(pk.BSIStageWriteElapsed, pk.BSIStageWriteCount)
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
	Profile                 string                              `json:"profile"`
	Mode                    string                              `json:"mode"`
	Config                  NativeIngestBenchmarkConfig         `json:"config"`
	Counts                  NativeIngestBenchmarkCounts         `json:"counts"`
	Timings                 NativeIngestBenchmarkTimings        `json:"timings"`
	PrimaryKeyShadowProfile core.PrimaryKeyShadowProfileSummary `json:"primary_key_shadow_profile"`
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
	{name: "unique_logical_rows_per_second", unit: "rows/s", higherIsBetter: true},
	{name: "orders_per_second", unit: "orders/s", higherIsBetter: true},
	{name: "unique_orders_per_second", unit: "orders/s", higherIsBetter: true},
	{name: "lineitems_per_second", unit: "lineitems/s", higherIsBetter: true},
	{name: "unique_lineitems_per_second", unit: "lineitems/s", higherIsBetter: true},
	{name: "replay_count", unit: "replays", higherIsBetter: false},
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
	{name: "put_stage_normalize_microseconds_per_order", unit: "us/order", higherIsBetter: false},
	{name: "put_stage_identity_microseconds_per_order", unit: "us/order", higherIsBetter: false},
	{name: "put_stage_primary_key_microseconds_per_order", unit: "us/order", higherIsBetter: false},
	{name: "put_stage_alternate_keys_microseconds_per_order", unit: "us/order", higherIsBetter: false},
	{name: "put_stage_child_expansion_microseconds_per_order", unit: "us/order", higherIsBetter: false},
	{name: "put_stage_child_traversal_microseconds_per_order", unit: "us/order", higherIsBetter: false},
	{name: "put_stage_child_recursive_write_microseconds_per_order", unit: "us/order", higherIsBetter: false},
	{name: "put_stage_parent_relation_microseconds_per_order", unit: "us/order", higherIsBetter: false},
	{name: "put_stage_attribute_mapping_microseconds_per_order", unit: "us/order", higherIsBetter: false},
	{name: "primary_key_resolves_per_logical_row", unit: "resolves/row", higherIsBetter: false},
	{name: "primary_key_total_microseconds_per_resolve", unit: "us/resolve", higherIsBetter: false},
	{name: "primary_key_local_cache_hit_percent", unit: "percent", higherIsBetter: true},
	{name: "primary_key_assume_new_percent", unit: "percent", higherIsBetter: true},
	{name: "primary_key_skipped_kv_lookup_percent", unit: "percent", higherIsBetter: true},
	{name: "primary_key_kv_hit_percent", unit: "percent", higherIsBetter: false},
	{name: "primary_key_kv_lookup_microseconds_per_lookup", unit: "us/lookup", higherIsBetter: false},
	{name: "primary_key_bsi_hit_percent", unit: "percent", higherIsBetter: true},
	{name: "primary_key_skipped_bsi_lookup_percent", unit: "percent", higherIsBetter: true},
	{name: "primary_key_bsi_lookup_microseconds_per_lookup", unit: "us/lookup", higherIsBetter: false},
	{name: "primary_key_bsi_stage_write_microseconds_per_write", unit: "us/write", higherIsBetter: false},
	{name: "primary_key_allocation_microseconds_per_allocation", unit: "us/allocation", higherIsBetter: false},
	{name: "primary_key_batch_cache_write_microseconds_per_write", unit: "us/write", higherIsBetter: false},
	{name: "primary_key_shadow_comparison_count", unit: "comparisons", higherIsBetter: false},
	{name: "primary_key_shadow_match_count", unit: "matches", higherIsBetter: true},
	{name: "primary_key_shadow_mismatch_count", unit: "mismatches", higherIsBetter: false},
	{name: "primary_key_shadow_skip_count", unit: "skips", higherIsBetter: false},
	{name: "primary_key_shadow_existing_row_match_count", unit: "matches", higherIsBetter: true},
}

var nativeIngestBenchmarkSummaryMetrics = []nativeIngestBenchmarkSummaryMetric{
	{name: "logical_rows_per_second", label: "Logical rows/sec"},
	{name: "unique_logical_rows_per_second", label: "Unique logical rows/sec"},
	{name: "enqueue_microseconds_per_order", label: "Enqueue us/order"},
	{name: "drain_microseconds_per_order", label: "Drain us/order"},
	{name: "drain_worker_max_microseconds", label: "Slowest worker drain"},
	{name: "drain_worker_sum_to_wall_percent", label: "Worker drain sum/wall"},
	{name: "drain_worker_max_to_avg_percent", label: "Drain worker max/avg"},
	{name: "put_microseconds_per_order", label: "PutRow us/order"},
	{name: "flush_microseconds_per_order", label: "Flush us/order"},
	{name: "flush_summed_to_drain_percent", label: "Flush sum/drain"},
	{name: "flush_shard_max_to_avg_percent", label: "Flush shard max/avg"},
	{name: "put_stage_normalize_microseconds_per_order", label: "Normalize us/order"},
	{name: "put_stage_identity_microseconds_per_order", label: "Identity us/order"},
	{name: "put_stage_primary_key_microseconds_per_order", label: "PK stage us/order"},
	{name: "put_stage_child_expansion_microseconds_per_order", label: "Child expansion us/order"},
	{name: "put_stage_child_traversal_microseconds_per_order", label: "Child traversal us/order"},
	{name: "put_stage_child_recursive_write_microseconds_per_order", label: "Child recursive write us/order"},
	{name: "put_stage_parent_relation_microseconds_per_order", label: "Parent relation us/order"},
	{name: "put_stage_attribute_mapping_microseconds_per_order", label: "Attribute mapping us/order"},
	{name: "primary_key_total_microseconds_per_resolve", label: "PK resolve us"},
	{name: "primary_key_bsi_hit_percent", label: "PK BSI hit"},
	{name: "primary_key_kv_hit_percent", label: "PK KV hit"},
	{name: "primary_key_kv_lookup_microseconds_per_lookup", label: "PK KV lookup us"},
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
	renderNativeIngestBenchmarkConfigMarkdown(&builder, comparison)
	renderNativeIngestBenchmarkSummaryMarkdown(&builder, comparison)
	renderNativeIngestPrimaryKeyShadowMarkdown(&builder, comparison)
	builder.WriteString("## Detailed Metrics\n\n")
	builder.WriteString("| Metric | Baseline | Target | Delta | Ratio | Direction |\n")
	builder.WriteString("| --- | ---: | ---: | ---: | ---: | --- |\n")
	for _, metric := range comparison.Metrics {
		writeNativeIngestBenchmarkMetricMarkdownRow(&builder, metric.Name, metric)
	}
	return builder.String()
}

func renderNativeIngestBenchmarkConfigMarkdown(builder *strings.Builder, comparison NativeIngestBenchmarkComparison) {
	builder.WriteString("## Benchmark Config\n\n")
	builder.WriteString("| Setting | Baseline | Target |\n")
	builder.WriteString("| --- | ---: | ---: |\n")
	writeNativeIngestBenchmarkConfigRow(builder, "Orders", configIntString(comparison.Baseline.Config.OrderCount), configIntString(comparison.Target.Config.OrderCount))
	writeNativeIngestBenchmarkConfigRow(builder, "Lineitems/order", configIntString(comparison.Baseline.Config.LineitemsPerOrder), configIntString(comparison.Target.Config.LineitemsPerOrder))
	writeNativeIngestBenchmarkConfigRow(builder, "Shards", configIntString(comparison.Baseline.Config.ShardCount), configIntString(comparison.Target.Config.ShardCount))
	writeNativeIngestBenchmarkConfigRow(builder, "Runs", configIntString(comparison.Baseline.Config.RunCount), configIntString(comparison.Target.Config.RunCount))
	writeNativeIngestBenchmarkConfigRow(builder, "Replays", replayConfigString(comparison.Baseline.Config.ReplayCount), replayConfigString(comparison.Target.Config.ReplayCount))
	writeNativeIngestBenchmarkConfigRow(builder, "Primary-key mode", defaultConfigString(comparison.Baseline.Config.PrimaryKeyMode, "verify_existing"), defaultConfigString(comparison.Target.Config.PrimaryKeyMode, "verify_existing"))
	writeNativeIngestBenchmarkConfigRow(builder, "Primary-key authority", defaultConfigString(comparison.Baseline.Config.PrimaryKeyAuthority, "kv"), defaultConfigString(comparison.Target.Config.PrimaryKeyAuthority, "kv"))
	writeNativeIngestBenchmarkConfigRow(builder, "Primary-key shadow", defaultConfigString(comparison.Baseline.Config.PrimaryKeyShadow, "none"), defaultConfigString(comparison.Target.Config.PrimaryKeyShadow, "none"))
	builder.WriteString("\n")
}

func writeNativeIngestBenchmarkConfigRow(builder *strings.Builder, label string, baseline string, target string) {
	builder.WriteString(fmt.Sprintf("| %s | %s | %s |\n",
		label,
		formatNativeIngestMarkdownCell(baseline),
		formatNativeIngestMarkdownCell(target)))
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

func renderNativeIngestPrimaryKeyShadowMarkdown(builder *strings.Builder, comparison NativeIngestBenchmarkComparison) {
	baseline := comparison.Baseline.PrimaryKeyShadowProfile
	target := comparison.Target.PrimaryKeyShadowProfile
	if baseline.ComparisonCount == 0 && target.ComparisonCount == 0 {
		return
	}
	builder.WriteString("## Primary Key Shadow Profile\n\n")
	builder.WriteString("| Signal | Baseline | Target |\n")
	builder.WriteString("| --- | ---: | ---: |\n")
	writeNativeIngestBenchmarkShadowProfileRow(builder, "Comparisons", baseline.ComparisonCount, target.ComparisonCount)
	writeNativeIngestBenchmarkShadowProfileRow(builder, "Matches", baseline.MatchCount, target.MatchCount)
	writeNativeIngestBenchmarkShadowProfileRow(builder, "Mismatches", baseline.MismatchCount, target.MismatchCount)
	writeNativeIngestBenchmarkShadowProfileRow(builder, "Skips", baseline.SkipCount, target.SkipCount)
	writeNativeIngestBenchmarkShadowProfileRow(builder, "Authority errors", baseline.AuthorityErrorCount, target.AuthorityErrorCount)
	writeNativeIngestBenchmarkShadowProfileRow(builder, "Shadow errors", baseline.ShadowErrorCount, target.ShadowErrorCount)
	writeNativeIngestBenchmarkShadowProfileRow(builder, "Authority existing rows", baseline.AuthorityExistingRow, target.AuthorityExistingRow)
	writeNativeIngestBenchmarkShadowProfileRow(builder, "Shadow existing rows", baseline.ShadowExistingRow, target.ShadowExistingRow)
	writeNativeIngestBenchmarkShadowProfileRow(builder, "Existing-row matches", baseline.ExistingRowMatch, target.ExistingRowMatch)
	builder.WriteString(fmt.Sprintf("| Reason counts | %s | %s |\n",
		formatNativeIngestShadowReasonCounts(baseline.ByReason),
		formatNativeIngestShadowReasonCounts(target.ByReason)))
	if baseline.FirstIssue != "" || target.FirstIssue != "" {
		builder.WriteString(fmt.Sprintf("| First issue | %s | %s |\n",
			formatNativeIngestMarkdownCell(baseline.FirstIssue),
			formatNativeIngestMarkdownCell(target.FirstIssue)))
	}
	builder.WriteString("\n")
}

func writeNativeIngestBenchmarkShadowProfileRow(builder *strings.Builder, label string, baseline int, target int) {
	builder.WriteString(fmt.Sprintf("| %s | %d | %d |\n", label, baseline, target))
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
		Profile:                 report.Profile,
		Mode:                    report.Mode,
		Config:                  report.Config,
		Counts:                  report.Counts,
		Timings:                 report.Timings,
		PrimaryKeyShadowProfile: report.PrimaryKeyShadowProfile,
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

func configIntString(value int) string {
	if value <= 0 {
		return ""
	}
	return fmt.Sprint(value)
}

func replayConfigString(value int) string {
	if value <= 0 {
		return "1"
	}
	return fmt.Sprint(value)
}

func defaultConfigString(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func formatNativeIngestShadowReasonCounts(counts map[string]int) string {
	if len(counts) == 0 {
		return ""
	}
	reasons := make([]string, 0, len(counts))
	for reason := range counts {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	parts := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		parts = append(parts, fmt.Sprintf("%s=%d", reason, counts[reason]))
	}
	return formatNativeIngestMarkdownCell(strings.Join(parts, ", "))
}

func formatNativeIngestMarkdownCell(value string) string {
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "|", "\\|")
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

func nonNegativeDuration(duration time.Duration) time.Duration {
	if duration < 0 {
		return 0
	}
	return duration
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

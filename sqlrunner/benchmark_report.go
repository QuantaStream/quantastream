package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

type benchmarkMeasuredRun struct {
	Index    int
	Duration time.Duration
	Summary  roadmap.Summary
}

type benchmarkReport struct {
	Version      int                   `json:"version"`
	Suite        string                `json:"suite"`
	Engine       string                `json:"engine"`
	Profile      string                `json:"profile"`
	GeneratedAt  string                `json:"generated_at"`
	Metadata     map[string]string     `json:"metadata,omitempty"`
	WarmupRuns   int                   `json:"warmup_runs"`
	MeasuredRuns int                   `json:"measured_runs"`
	Runs         []benchmarkRunReport  `json:"runs"`
	Cases        []benchmarkCaseReport `json:"cases"`
}

type benchmarkRunReport struct {
	Index      int    `json:"index"`
	Status     string `json:"status"`
	DurationMS int64  `json:"duration_ms"`
}

type benchmarkCaseReport struct {
	ID          string         `json:"id"`
	Status      string         `json:"status"`
	Runs        int            `json:"runs"`
	MinMS       int64          `json:"min_ms"`
	MedianMS    int64          `json:"median_ms"`
	P95MS       int64          `json:"p95_ms"`
	MaxMS       int64          `json:"max_ms"`
	Statuses    map[string]int `json:"statuses,omitempty"`
	FirstDetail string         `json:"first_detail,omitempty"`
}

func executeBenchmarkSuite(ctx context.Context, suite *roadmap.Suite, runner roadmap.Runner, cfg runnerConfig) error {
	if cfg.BenchmarkRuns <= 0 {
		return fmt.Errorf("benchmark_runs must be greater than zero")
	}
	if cfg.BenchmarkWarmup < 0 {
		return fmt.Errorf("benchmark_warmup cannot be negative")
	}
	metadata, err := parseBenchmarkMetadata(cfg.BenchmarkMetadata)
	if err != nil {
		return err
	}

	for i := 0; i < cfg.BenchmarkWarmup; i++ {
		log.Printf("BENCHMARK warmup %d/%d", i+1, cfg.BenchmarkWarmup)
		summary := runner.Run(ctx, suite)
		if summary.HasFailures() {
			logSummaryResults(summary, cfg.Verbose, cfg.PreciseTiming)
			return fmt.Errorf("benchmark warmup %d contains FAIL or XPASS results", i+1)
		}
	}

	measured := make([]benchmarkMeasuredRun, 0, cfg.BenchmarkRuns)
	var failed bool
	for i := 0; i < cfg.BenchmarkRuns; i++ {
		log.Printf("BENCHMARK measured run %d/%d", i+1, cfg.BenchmarkRuns)
		started := time.Now()
		summary := runner.Run(ctx, suite)
		duration := time.Since(started)
		log.Printf("BENCHMARK measured run %d/%d completed in %s", i+1, cfg.BenchmarkRuns, duration.Round(time.Millisecond))
		logSummaryResults(summary, cfg.Verbose, cfg.PreciseTiming)
		if summary.HasFailures() {
			failed = true
		}
		measured = append(measured, benchmarkMeasuredRun{Index: i + 1, Duration: duration, Summary: summary})
	}

	report := buildBenchmarkReport(suite.Name, cfg.Engine, cfg.BenchmarkProfile, metadata, cfg.BenchmarkWarmup, measured, time.Now().UTC())
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if dir := filepath.Dir(cfg.BenchmarkReport); dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	if err := os.WriteFile(cfg.BenchmarkReport, data, 0644); err != nil {
		return err
	}
	log.Printf("WROTE  %s (%d measured runs)", cfg.BenchmarkReport, len(measured))
	if failed {
		return fmt.Errorf("benchmark suite contains FAIL or XPASS results")
	}
	return nil
}

func parseBenchmarkMetadata(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	metadata := make(map[string]string)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if !ok || key == "" {
			return nil, fmt.Errorf("benchmark_metadata must be comma-separated key=value pairs")
		}
		metadata[key] = value
	}
	if len(metadata) == 0 {
		return nil, nil
	}
	return metadata, nil
}

func buildBenchmarkReport(suite, engine, profile string, metadata map[string]string, warmup int, runs []benchmarkMeasuredRun, generatedAt time.Time) benchmarkReport {
	report := benchmarkReport{
		Version:      1,
		Suite:        suite,
		Engine:       engine,
		Profile:      benchmarkProfile(profile),
		GeneratedAt:  generatedAt.UTC().Format(time.RFC3339),
		Metadata:     cloneBenchmarkMetadata(metadata),
		WarmupRuns:   warmup,
		MeasuredRuns: len(runs),
		Runs:         make([]benchmarkRunReport, 0, len(runs)),
	}
	caseDurations := map[string][]time.Duration{}
	caseStatuses := map[string]map[string]int{}
	caseDetails := map[string]string{}
	caseOrder := []string{}

	for _, run := range runs {
		runStatus := roadmap.ResultPass
		if run.Summary.HasFailures() {
			runStatus = roadmap.ResultFail
		}
		report.Runs = append(report.Runs, benchmarkRunReport{
			Index:      run.Index,
			Status:     runStatus,
			DurationMS: durationMillis(run.Duration),
		})
		for _, result := range run.Summary.Results {
			if _, ok := caseDurations[result.ID]; !ok {
				caseOrder = append(caseOrder, result.ID)
			}
			caseDurations[result.ID] = append(caseDurations[result.ID], result.Duration)
			if caseStatuses[result.ID] == nil {
				caseStatuses[result.ID] = map[string]int{}
			}
			caseStatuses[result.ID][result.Status]++
			if result.Details != "" && caseDetails[result.ID] == "" {
				caseDetails[result.ID] = result.Details
			}
		}
	}

	report.Cases = make([]benchmarkCaseReport, 0, len(caseOrder))
	for _, id := range caseOrder {
		durations := caseDurations[id]
		report.Cases = append(report.Cases, benchmarkCaseReport{
			ID:          id,
			Status:      aggregateBenchmarkStatus(caseStatuses[id]),
			Runs:        len(durations),
			MinMS:       durationMillis(durationPercentile(durations, 0.0)),
			MedianMS:    durationMillis(durationPercentile(durations, 0.50)),
			P95MS:       durationMillis(durationPercentile(durations, 0.95)),
			MaxMS:       durationMillis(durationPercentile(durations, 1.0)),
			Statuses:    cloneStatusCounts(caseStatuses[id]),
			FirstDetail: caseDetails[id],
		})
	}
	return report
}

func benchmarkProfile(profile string) string {
	profile = strings.TrimSpace(profile)
	if profile == "" {
		return "developer-local"
	}
	return profile
}

func cloneBenchmarkMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	clone := make(map[string]string, len(metadata))
	for key, value := range metadata {
		clone[key] = value
	}
	return clone
}

func cloneStatusCounts(statuses map[string]int) map[string]int {
	if len(statuses) == 0 {
		return nil
	}
	clone := make(map[string]int, len(statuses))
	for status, count := range statuses {
		clone[status] = count
	}
	return clone
}

func aggregateBenchmarkStatus(statuses map[string]int) string {
	if statuses[roadmap.ResultFail] > 0 || statuses[roadmap.ResultXPass] > 0 {
		return roadmap.ResultFail
	}
	if statuses[roadmap.ResultXFail] > 0 {
		return roadmap.ResultXFail
	}
	if statuses[roadmap.ResultSkip] > 0 && len(statuses) == 1 {
		return roadmap.ResultSkip
	}
	return roadmap.ResultPass
}

func durationPercentile(durations []time.Duration, percentile float64) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	if percentile <= 0 {
		return sorted[0]
	}
	if percentile >= 1 {
		return sorted[len(sorted)-1]
	}
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func durationMillis(duration time.Duration) int64 {
	return duration.Round(time.Millisecond).Milliseconds()
}

func printBenchmarkSummary(path string) error {
	report, err := loadBenchmarkReport(path)
	if err != nil {
		return err
	}
	return renderBenchmarkSummary(os.Stdout, report)
}

func loadBenchmarkReport(path string) (benchmarkReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return benchmarkReport{}, err
	}
	var report benchmarkReport
	if err := json.Unmarshal(data, &report); err != nil {
		return benchmarkReport{}, err
	}
	if strings.TrimSpace(report.Suite) == "" {
		return benchmarkReport{}, fmt.Errorf("benchmark report %q does not include a suite name", path)
	}
	return report, nil
}

func renderBenchmarkSummary(w io.Writer, report benchmarkReport) error {
	if _, err := fmt.Fprintf(w, "Benchmark: %s\n", report.Suite); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Engine: %s\n", report.Engine); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Profile: %s\n", report.Profile); err != nil {
		return err
	}
	if report.GeneratedAt != "" {
		if _, err := fmt.Fprintf(w, "Generated: %s\n", report.GeneratedAt); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(w, "Runs: %d measured, %d warmup\n", report.MeasuredRuns, report.WarmupRuns); err != nil {
		return err
	}
	if len(report.Metadata) > 0 {
		if _, err := fmt.Fprintln(w, "\nMetadata:"); err != nil {
			return err
		}
		for _, key := range sortedBenchmarkMetadataKeys(report.Metadata) {
			if _, err := fmt.Fprintf(w, "  %s=%s\n", key, report.Metadata[key]); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintln(w); err != nil {
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "Case\tStatus\tRuns\tMin\tMedian\tP95\tMax"); err != nil {
		return err
	}
	for _, result := range report.Cases {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%d\t%s\t%s\t%s\t%s\n",
			result.ID,
			result.Status,
			result.Runs,
			formatBenchmarkMillis(result.MinMS),
			formatBenchmarkMillis(result.MedianMS),
			formatBenchmarkMillis(result.P95MS),
			formatBenchmarkMillis(result.MaxMS),
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func sortedBenchmarkMetadataKeys(metadata map[string]string) []string {
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func formatBenchmarkMillis(millis int64) string {
	return fmt.Sprintf("%dms", millis)
}

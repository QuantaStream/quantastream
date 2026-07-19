package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
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
	metadata = mergeBenchmarkMetadata(detectBenchmarkMetadata(), metadata)

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

func mergeBenchmarkMetadata(autoMetadata, explicitMetadata map[string]string) map[string]string {
	merged := make(map[string]string, len(autoMetadata)+len(explicitMetadata))
	for key, value := range autoMetadata {
		addBenchmarkMetadataValue(merged, key, value)
	}
	for key, value := range explicitMetadata {
		addBenchmarkMetadataValue(merged, key, value)
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

func addBenchmarkMetadataValue(metadata map[string]string, key, value string) {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" {
		return
	}
	metadata[key] = value
}

func detectBenchmarkMetadata() map[string]string {
	metadata := map[string]string{
		"host_os":    runtime.GOOS,
		"host_arch":  runtime.GOARCH,
		"host_cpus":  strconv.Itoa(runtime.NumCPU()),
		"go_version": strings.TrimPrefix(runtime.Version(), "go"),
	}
	if host, err := os.Hostname(); err == nil {
		addBenchmarkMetadataValue(metadata, "host_name", host)
	}
	if cwd, err := os.Getwd(); err == nil {
		addBenchmarkMetadataValue(metadata, "cwd", cwd)
		addBenchmarkStorageMetadata(metadata, cwd)
		addBenchmarkGitMetadata(metadata, cwd)
	}
	addBenchmarkEnvMetadata(metadata, "quantastream_config_dir", "QUANTASTREAM_CONFIG_DIR")
	addBenchmarkEnvMetadata(metadata, "quantastream_data_dir", "QUANTASTREAM_DATA_DIR")
	return metadata
}

func addBenchmarkEnvMetadata(metadata map[string]string, key, envName string) {
	addBenchmarkMetadataValue(metadata, key, os.Getenv(envName))
}

func addBenchmarkGitMetadata(metadata map[string]string, cwd string) {
	if commit, ok := runBenchmarkMetadataCommand(cwd, "git", "rev-parse", "--short", "HEAD"); ok {
		addBenchmarkMetadataValue(metadata, "repo_commit", commit)
	}
	if branch, ok := runBenchmarkMetadataCommand(cwd, "git", "branch", "--show-current"); ok {
		addBenchmarkMetadataValue(metadata, "repo_branch", branch)
	}
	if status, ok := runBenchmarkMetadataCommand(cwd, "git", "status", "--short"); ok {
		addBenchmarkMetadataValue(metadata, "repo_dirty", strconv.FormatBool(strings.TrimSpace(status) != ""))
	}
}

func addBenchmarkStorageMetadata(metadata map[string]string, path string) {
	storage := detectBenchmarkStorageMetadata(path)
	for key, value := range storage {
		addBenchmarkMetadataValue(metadata, key, value)
	}
}

func detectBenchmarkStorageMetadata(path string) map[string]string {
	if runtime.GOOS != "linux" {
		return nil
	}
	if output, ok := runBenchmarkMetadataCommand("", "df", "-P", "-T", path); ok {
		return parseBenchmarkDFOutput(output, true)
	}
	if output, ok := runBenchmarkMetadataCommand("", "df", "-P", path); ok {
		return parseBenchmarkDFOutput(output, false)
	}
	return nil
}

func parseBenchmarkDFOutput(output string, includesType bool) map[string]string {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) < 2 {
		return nil
	}
	fields := strings.Fields(lines[1])
	if includesType {
		if len(fields) < 7 {
			return nil
		}
		return map[string]string{
			"storage_device":       fields[0],
			"storage_type":         fields[1],
			"storage_available_kb": fields[4],
			"storage_used_percent": strings.TrimSuffix(fields[5], "%"),
			"storage_mount":        strings.Join(fields[6:], " "),
		}
	}
	if len(fields) < 6 {
		return nil
	}
	return map[string]string{
		"storage_device":       fields[0],
		"storage_available_kb": fields[3],
		"storage_used_percent": strings.TrimSuffix(fields[4], "%"),
		"storage_mount":        strings.Join(fields[5:], " "),
	}
}

func runBenchmarkMetadataCommand(dir, name string, args ...string) (string, bool) {
	cmd := exec.Command(name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	output, err := cmd.Output()
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(output)), true
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

func printBenchmarkComparison(rawPaths string, limit int) error {
	paths, err := parseBenchmarkReportPaths(rawPaths)
	if err != nil {
		return err
	}
	reports := make([]benchmarkReport, 0, len(paths))
	for _, path := range paths {
		report, err := loadBenchmarkReport(path)
		if err != nil {
			return err
		}
		reports = append(reports, report)
	}
	return renderBenchmarkComparison(os.Stdout, reports, paths, limit)
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

func parseBenchmarkReportPaths(rawPaths string) ([]string, error) {
	rawPaths = strings.TrimSpace(rawPaths)
	if rawPaths == "" {
		return nil, fmt.Errorf("benchmark_compare requires at least two benchmark report paths")
	}
	parts := strings.Split(rawPaths, ",")
	paths := make([]string, 0, len(parts))
	for _, part := range parts {
		path := strings.TrimSpace(part)
		if path == "" {
			return nil, fmt.Errorf("benchmark_compare paths cannot be empty")
		}
		paths = append(paths, path)
	}
	if len(paths) < 2 {
		return nil, fmt.Errorf("benchmark_compare requires at least two benchmark report paths")
	}
	return paths, nil
}

type benchmarkComparisonRow struct {
	ID           string
	Baseline     benchmarkCaseReport
	Target       benchmarkCaseReport
	HasBaseline  bool
	HasTarget    bool
	DeltaMS      int64
	Status       string
	SortPriority int
}

func renderBenchmarkComparison(w io.Writer, reports []benchmarkReport, paths []string, limit int) error {
	if limit < 0 {
		return fmt.Errorf("benchmark_limit cannot be negative")
	}
	if len(reports) < 2 {
		return fmt.Errorf("benchmark comparison requires at least two benchmark reports")
	}
	if len(paths) != len(reports) {
		return fmt.Errorf("benchmark comparison paths/reports length mismatch")
	}

	baseline := reports[0]
	suite := strings.TrimSpace(baseline.Suite)
	if suite == "" {
		return fmt.Errorf("baseline benchmark report does not include a suite name")
	}
	for i := range reports {
		if strings.TrimSpace(reports[i].Suite) != suite {
			return fmt.Errorf("benchmark report %q uses suite %q, expected %q", paths[i], reports[i].Suite, suite)
		}
	}

	if _, err := fmt.Fprintf(w, "Benchmark Comparison: %s\n", suite); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Baseline: %s (%s)\n", benchmarkReportLabel(baseline), paths[0]); err != nil {
		return err
	}
	if err := renderBenchmarkComparisonReportMetadata(w, "Baseline metadata", baseline); err != nil {
		return err
	}

	for i := 1; i < len(reports); i++ {
		if i > 1 {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
		if err := renderBenchmarkComparisonTarget(w, baseline, reports[i], paths[i], limit); err != nil {
			return err
		}
	}
	return nil
}

func renderBenchmarkComparisonTarget(w io.Writer, baseline benchmarkReport, target benchmarkReport, targetPath string, limit int) error {
	if _, err := fmt.Fprintf(w, "\nTarget: %s (%s)\n", benchmarkReportLabel(target), targetPath); err != nil {
		return err
	}
	if err := renderBenchmarkComparisonReportMetadata(w, "Target metadata", target); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "Runs: baseline %d measured, target %d measured\n\n", baseline.MeasuredRuns, target.MeasuredRuns); err != nil {
		return err
	}

	rows := benchmarkComparisonRows(baseline, target)
	if len(rows) == 0 {
		_, err := fmt.Fprintln(w, "No comparable cases.")
		return err
	}
	sortBenchmarkComparisonRows(rows)
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "Case\tBaseline\tTarget\tDelta\tRatio\tStatus"); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := fmt.Fprintf(
			tw,
			"%s\t%s\t%s\t%s\t%s\t%s\n",
			row.ID,
			formatBenchmarkComparisonMillis(row.Baseline.MedianMS, row.HasBaseline),
			formatBenchmarkComparisonMillis(row.Target.MedianMS, row.HasTarget),
			formatBenchmarkComparisonDelta(row),
			formatBenchmarkComparisonRatio(row),
			row.Status,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func renderBenchmarkComparisonReportMetadata(w io.Writer, title string, report benchmarkReport) error {
	if len(report.Metadata) == 0 {
		return nil
	}
	keys := benchmarkComparisonMetadataKeys(report.Metadata)
	if len(keys) == 0 {
		return nil
	}
	if _, err := fmt.Fprintf(w, "%s:\n", title); err != nil {
		return err
	}
	for _, key := range keys {
		if _, err := fmt.Fprintf(w, "  %s=%s\n", key, report.Metadata[key]); err != nil {
			return err
		}
	}
	return nil
}

func benchmarkComparisonMetadataKeys(metadata map[string]string) []string {
	priority := []string{
		"reference",
		"target",
		"cloud",
		"region",
		"instance",
		"storage_class",
		"dataset",
		"scale_factor",
		"repo_commit",
		"repo_branch",
		"repo_dirty",
		"host_os",
		"host_arch",
		"host_cpus",
		"go_version",
		"storage_type",
		"storage_mount",
	}
	keys := make([]string, 0, len(metadata))
	seen := make(map[string]struct{}, len(metadata))
	for _, key := range priority {
		if _, ok := metadata[key]; ok {
			keys = append(keys, key)
			seen[key] = struct{}{}
		}
	}
	remaining := make([]string, 0, len(metadata)-len(keys))
	for key := range metadata {
		if _, ok := seen[key]; ok {
			continue
		}
		remaining = append(remaining, key)
	}
	sort.Strings(remaining)
	keys = append(keys, remaining...)
	return keys
}

func benchmarkComparisonRows(baseline benchmarkReport, target benchmarkReport) []benchmarkComparisonRow {
	baselineCases := benchmarkCasesByID(baseline)
	targetCases := benchmarkCasesByID(target)
	ids := make(map[string]struct{}, len(baselineCases)+len(targetCases))
	for id := range baselineCases {
		ids[id] = struct{}{}
	}
	for id := range targetCases {
		ids[id] = struct{}{}
	}

	rows := make([]benchmarkComparisonRow, 0, len(ids))
	for id := range ids {
		baselineCase, hasBaseline := baselineCases[id]
		targetCase, hasTarget := targetCases[id]
		row := benchmarkComparisonRow{
			ID:          id,
			Baseline:    baselineCase,
			Target:      targetCase,
			HasBaseline: hasBaseline,
			HasTarget:   hasTarget,
			Status:      benchmarkComparisonStatus(baselineCase, hasBaseline, targetCase, hasTarget),
		}
		if hasBaseline && hasTarget {
			row.DeltaMS = targetCase.MedianMS - baselineCase.MedianMS
		}
		row.SortPriority = benchmarkComparisonSortPriority(row)
		rows = append(rows, row)
	}
	return rows
}

func benchmarkCasesByID(report benchmarkReport) map[string]benchmarkCaseReport {
	cases := make(map[string]benchmarkCaseReport, len(report.Cases))
	for _, result := range report.Cases {
		cases[result.ID] = result
	}
	return cases
}

func sortBenchmarkComparisonRows(rows []benchmarkComparisonRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].SortPriority != rows[j].SortPriority {
			return rows[i].SortPriority > rows[j].SortPriority
		}
		if rows[i].DeltaMS != rows[j].DeltaMS {
			return rows[i].DeltaMS > rows[j].DeltaMS
		}
		return rows[i].ID < rows[j].ID
	})
}

func benchmarkComparisonSortPriority(row benchmarkComparisonRow) int {
	if !row.HasBaseline || !row.HasTarget {
		return 3
	}
	if row.Baseline.Status != row.Target.Status {
		return 2
	}
	if row.DeltaMS > 0 {
		return 1
	}
	return 0
}

func benchmarkComparisonStatus(baseline benchmarkCaseReport, hasBaseline bool, target benchmarkCaseReport, hasTarget bool) string {
	switch {
	case !hasBaseline:
		return "missing-baseline"
	case !hasTarget:
		return "missing-target"
	case baseline.Status == target.Status:
		return target.Status
	default:
		return fmt.Sprintf("%s->%s", baseline.Status, target.Status)
	}
}

func benchmarkReportLabel(report benchmarkReport) string {
	engine := strings.TrimSpace(report.Engine)
	if engine == "" {
		engine = "unknown"
	}
	profile := strings.TrimSpace(report.Profile)
	if profile == "" {
		return engine
	}
	return fmt.Sprintf("%s/%s", engine, profile)
}

func formatBenchmarkComparisonMillis(millis int64, ok bool) string {
	if !ok {
		return "n/a"
	}
	return formatBenchmarkMillis(millis)
}

func formatBenchmarkComparisonDelta(row benchmarkComparisonRow) string {
	if !row.HasBaseline || !row.HasTarget {
		return "n/a"
	}
	if row.DeltaMS > 0 {
		return fmt.Sprintf("+%dms", row.DeltaMS)
	}
	return fmt.Sprintf("%dms", row.DeltaMS)
}

func formatBenchmarkComparisonRatio(row benchmarkComparisonRow) string {
	if !row.HasBaseline || !row.HasTarget || row.Baseline.MedianMS == 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.2fx", float64(row.Target.MedianMS)/float64(row.Baseline.MedianMS))
}

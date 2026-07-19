package main

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

func TestParseBenchmarkMetadata(t *testing.T) {
	metadata, err := parseBenchmarkMetadata("cloud=do, region=nyc3, instance=s-4vcpu-8gb")
	if err != nil {
		t.Fatalf("parseBenchmarkMetadata failed: %v", err)
	}
	if metadata["cloud"] != "do" || metadata["region"] != "nyc3" || metadata["instance"] != "s-4vcpu-8gb" {
		t.Fatalf("metadata = %#v", metadata)
	}
}

func TestParseBenchmarkMetadataRejectsMalformedPair(t *testing.T) {
	if _, err := parseBenchmarkMetadata("cloud=do,broken"); err == nil {
		t.Fatal("expected malformed benchmark metadata to fail")
	}
}

func TestMergeBenchmarkMetadataExplicitOverridesAuto(t *testing.T) {
	metadata := mergeBenchmarkMetadata(
		map[string]string{"host_os": "linux", "repo_commit": "auto", "empty": ""},
		map[string]string{"repo_commit": "explicit", "suite": "tpch"},
	)
	if metadata["host_os"] != "linux" {
		t.Fatalf("host_os = %q", metadata["host_os"])
	}
	if metadata["repo_commit"] != "explicit" {
		t.Fatalf("repo_commit = %q", metadata["repo_commit"])
	}
	if metadata["suite"] != "tpch" {
		t.Fatalf("suite = %q", metadata["suite"])
	}
	if _, ok := metadata["empty"]; ok {
		t.Fatalf("empty metadata should be omitted: %#v", metadata)
	}
}

func TestDetectBenchmarkMetadataIncludesRuntimeFields(t *testing.T) {
	metadata := detectBenchmarkMetadata()
	for _, key := range []string{"host_os", "host_arch", "host_cpus", "go_version"} {
		if strings.TrimSpace(metadata[key]) == "" {
			t.Fatalf("metadata missing %s: %#v", key, metadata)
		}
	}
}

func TestParseBenchmarkDFOutputWithFilesystemType(t *testing.T) {
	metadata := parseBenchmarkDFOutput(`Filesystem     Type 1024-blocks    Used Available Capacity Mounted on
/dev/sdc       ext4   1055762868 123456 987654321      12% /home
`, true)
	if metadata["storage_device"] != "/dev/sdc" {
		t.Fatalf("storage_device = %q", metadata["storage_device"])
	}
	if metadata["storage_type"] != "ext4" {
		t.Fatalf("storage_type = %q", metadata["storage_type"])
	}
	if metadata["storage_available_kb"] != "987654321" {
		t.Fatalf("storage_available_kb = %q", metadata["storage_available_kb"])
	}
	if metadata["storage_used_percent"] != "12" {
		t.Fatalf("storage_used_percent = %q", metadata["storage_used_percent"])
	}
	if metadata["storage_mount"] != "/home" {
		t.Fatalf("storage_mount = %q", metadata["storage_mount"])
	}
}

func TestParseBenchmarkDFOutputWithoutFilesystemType(t *testing.T) {
	metadata := parseBenchmarkDFOutput(`Filesystem     1024-blocks    Used Available Capacity Mounted on
/dev/sdc        1055762868 123456 987654321      12% /home
`, false)
	if metadata["storage_type"] != "" {
		t.Fatalf("storage_type = %q", metadata["storage_type"])
	}
	if metadata["storage_available_kb"] != "987654321" || metadata["storage_used_percent"] != "12" {
		t.Fatalf("metadata = %#v", metadata)
	}
}

func TestParseBenchmarkReportPaths(t *testing.T) {
	paths, err := parseBenchmarkReportPaths("direct.json, standard.json")
	if err != nil {
		t.Fatalf("parseBenchmarkReportPaths failed: %v", err)
	}
	if len(paths) != 2 || paths[0] != "direct.json" || paths[1] != "standard.json" {
		t.Fatalf("paths = %#v", paths)
	}
}

func TestParseBenchmarkReportPathsRejectsSingleReport(t *testing.T) {
	if _, err := parseBenchmarkReportPaths("direct.json"); err == nil {
		t.Fatal("expected single benchmark report path to fail")
	}
}

func TestBuildBenchmarkReportAggregatesCaseDurations(t *testing.T) {
	runs := []benchmarkMeasuredRun{
		{
			Index:    1,
			Duration: 300 * time.Millisecond,
			Summary: roadmap.Summary{Suite: "smoke", Results: []roadmap.CaseResult{
				{ID: "001.select", Status: roadmap.ResultPass, Duration: 100 * time.Millisecond},
				{ID: "002.select", Status: roadmap.ResultPass, Duration: 200 * time.Millisecond},
			}},
		},
		{
			Index:    2,
			Duration: 700 * time.Millisecond,
			Summary: roadmap.Summary{Suite: "smoke", Results: []roadmap.CaseResult{
				{ID: "001.select", Status: roadmap.ResultPass, Duration: 150 * time.Millisecond},
				{ID: "002.select", Status: roadmap.ResultFail, Duration: 550 * time.Millisecond, Details: "mismatch"},
			}},
		},
	}

	report := buildBenchmarkReport("smoke", "inabox-direct", "inabox-standard", map[string]string{"commit": "abc"}, 1, runs, time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC))
	if report.Profile != "inabox-standard" || report.WarmupRuns != 1 || report.MeasuredRuns != 2 {
		t.Fatalf("report header = %#v", report)
	}
	if len(report.Runs) != 2 || report.Runs[1].Status != roadmap.ResultFail {
		t.Fatalf("run reports = %#v", report.Runs)
	}
	if len(report.Cases) != 2 {
		t.Fatalf("case reports = %#v", report.Cases)
	}
	if report.Cases[0].ID != "001.select" || report.Cases[0].MedianMS != 100 || report.Cases[0].P95MS != 150 {
		t.Fatalf("first case = %#v", report.Cases[0])
	}
	if report.Cases[1].Status != roadmap.ResultFail || report.Cases[1].FirstDetail != "mismatch" {
		t.Fatalf("second case = %#v", report.Cases[1])
	}
}

func TestRenderBenchmarkSummary(t *testing.T) {
	report := benchmarkReport{
		Suite:        "smoke",
		Engine:       "runtime",
		Profile:      "developer-local",
		GeneratedAt:  "2026-07-11T13:20:50Z",
		Metadata:     map[string]string{"profile": "runtime-smoke", "commit": "abc"},
		WarmupRuns:   1,
		MeasuredRuns: 2,
		Cases: []benchmarkCaseReport{
			{ID: "001.select", Status: roadmap.ResultPass, Runs: 2, MinMS: 10, MedianMS: 20, P95MS: 30, MaxMS: 40},
		},
	}

	var out bytes.Buffer
	if err := renderBenchmarkSummary(&out, report); err != nil {
		t.Fatalf("renderBenchmarkSummary failed: %v", err)
	}
	text := out.String()
	for _, want := range []string{
		"Benchmark: smoke",
		"Engine: runtime",
		"Runs: 2 measured, 1 warmup",
		"commit=abc",
		"001.select",
		"20ms",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("summary text missing %q:\n%s", want, text)
		}
	}
}

func TestRenderBenchmarkComparison(t *testing.T) {
	baseline := benchmarkReport{
		Suite:        "tpch",
		Engine:       "inabox-direct",
		Profile:      "developer-local",
		MeasuredRuns: 3,
		Cases: []benchmarkCaseReport{
			{ID: "q1", Status: roadmap.ResultPass, Runs: 3, MedianMS: 100},
			{ID: "q2", Status: roadmap.ResultPass, Runs: 3, MedianMS: 200},
		},
	}
	target := benchmarkReport{
		Suite:        "tpch",
		Engine:       "inabox-standard",
		Profile:      "developer-local",
		MeasuredRuns: 3,
		Cases: []benchmarkCaseReport{
			{ID: "q1", Status: roadmap.ResultPass, Runs: 3, MedianMS: 150},
			{ID: "q2", Status: roadmap.ResultPass, Runs: 3, MedianMS: 100},
		},
	}

	var out bytes.Buffer
	if err := renderBenchmarkComparison(&out, []benchmarkReport{baseline, target}, []string{"direct.json", "standard.json"}, 20); err != nil {
		t.Fatalf("renderBenchmarkComparison failed: %v", err)
	}
	text := out.String()
	for _, want := range []string{
		"Benchmark Comparison: tpch",
		"Baseline: inabox-direct/developer-local (direct.json)",
		"Target: inabox-standard/developer-local (standard.json)",
		"q1",
		"+50ms",
		"1.50x",
		"q2",
		"-100ms",
		"0.50x",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("comparison text missing %q:\n%s", want, text)
		}
	}
}

func TestRenderBenchmarkComparisonRejectsDifferentSuites(t *testing.T) {
	baseline := benchmarkReport{Suite: "tpch", Engine: "inabox-direct"}
	target := benchmarkReport{Suite: "basic", Engine: "inabox-standard"}

	var out bytes.Buffer
	if err := renderBenchmarkComparison(&out, []benchmarkReport{baseline, target}, []string{"direct.json", "standard.json"}, 20); err == nil {
		t.Fatal("expected suite mismatch to fail")
	}
}

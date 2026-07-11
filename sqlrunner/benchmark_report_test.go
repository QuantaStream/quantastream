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

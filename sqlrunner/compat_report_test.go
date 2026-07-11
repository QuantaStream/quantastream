package main

import (
	"testing"

	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

func TestCompatibilityReportLinesIncludeTotalsAndFeatures(t *testing.T) {
	report := roadmap.CompatibilityReport{
		Suite: "mysql_compat",
		Counts: map[string]int{
			roadmap.CompatibilityResultPass:        2,
			roadmap.CompatibilityResultUnsupported: 1,
		},
		Features: []roadmap.CompatibilityFeatureReport{{
			Feature: "select_projection",
			Counts:  map[string]int{roadmap.CompatibilityResultPass: 2},
		}},
	}

	lines := compatibilityReportLines(report)

	if len(lines) != 2 || lines[0] != "TOTAL TOTAL=3 PASS=2 UNSUPPORTED=1" || lines[1] != "FEATURE select_projection TOTAL=2 PASS=2" {
		t.Fatalf("lines = %#v", lines)
	}
}

func TestCompatibilityReportCaseLinesIncludeNonPassDetails(t *testing.T) {
	report := roadmap.CompatibilityReport{
		Cases: []roadmap.CompatibilityCaseReport{
			{ID: "select.pass", Feature: "select_projection", Category: roadmap.CompatibilityResultPass},
			{ID: "join.fail", Feature: "joins_inner", Category: roadmap.CompatibilityResultFail, Details: "row mismatch"},
			{ID: "outer.unsupported", Feature: "joins_outer", Category: roadmap.CompatibilityResultUnsupported},
		},
	}

	lines := compatibilityReportCaseLines(report)

	if len(lines) != 2 {
		t.Fatalf("case lines = %#v, want two non-pass lines", lines)
	}
	if lines[0] != "CASE joins_inner join.fail FAIL: row mismatch" {
		t.Fatalf("first case line = %q", lines[0])
	}
	if lines[1] != "CASE joins_outer outer.unsupported UNSUPPORTED: -" {
		t.Fatalf("second case line = %q", lines[1])
	}
}

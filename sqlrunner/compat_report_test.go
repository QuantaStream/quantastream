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

	if len(lines) != 2 || lines[0] != "TOTAL PASS=2 UNSUPPORTED=1" || lines[1] != "FEATURE select_projection PASS=2" {
		t.Fatalf("lines = %#v", lines)
	}
}

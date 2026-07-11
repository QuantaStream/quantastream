package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

func logCompatibilityReport(suite *roadmap.Suite, summary roadmap.Summary) {
	report := roadmap.BuildCompatibilityReport(suite, summary)
	log.Printf("\n-------- MySQL Compatibility Report: %s --------", report.Suite)
	for _, line := range compatibilityReportLines(report) {
		log.Print(line)
	}
}

func compatibilityReportLines(report roadmap.CompatibilityReport) []string {
	lines := []string{compatibilityCountLine("TOTAL", report.Counts)}
	for _, feature := range report.Features {
		lines = append(lines, compatibilityCountLine("FEATURE "+feature.Feature, feature.Counts))
	}
	return lines
}

func compatibilityCountLine(label string, counts map[string]int) string {
	categories := []string{
		roadmap.CompatibilityResultPass,
		roadmap.CompatibilityResultFail,
		roadmap.CompatibilityResultUnsupported,
		roadmap.CompatibilityResultTypeWarn,
		roadmap.CompatibilityResultPerfWarn,
	}
	parts := make([]string, 0, len(categories))
	for _, category := range categories {
		if count := counts[category]; count > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", category, count))
		}
	}
	if len(parts) == 0 {
		parts = append(parts, "none")
	}
	sort.Strings(parts)
	return fmt.Sprintf("%s %s", label, strings.Join(parts, " "))
}

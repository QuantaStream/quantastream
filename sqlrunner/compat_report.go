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
	for _, line := range compatibilityReportCaseLines(report) {
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
	categories := compatibilityReportCategories()
	parts := make([]string, 0, len(categories)+1)
	total := 0
	for _, count := range counts {
		total += count
	}
	if total > 0 {
		parts = append(parts, fmt.Sprintf("TOTAL=%d", total))
	}
	for _, category := range categories {
		if count := counts[category]; count > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", category, count))
		}
	}
	if len(parts) == 0 {
		parts = append(parts, "none")
	}
	return fmt.Sprintf("%s %s", label, strings.Join(parts, " "))
}

func compatibilityReportCaseLines(report roadmap.CompatibilityReport) []string {
	var lines []string
	for _, test := range report.Cases {
		if test.Category == roadmap.CompatibilityResultPass {
			continue
		}
		detail := strings.TrimSpace(test.Details)
		if detail == "" {
			detail = "-"
		}
		lines = append(lines, fmt.Sprintf("CASE %s %s %s: %s", test.Feature, test.ID, test.Category, detail))
	}
	sort.Strings(lines)
	return lines
}

func compatibilityReportCategories() []string {
	return []string{
		roadmap.CompatibilityResultPass,
		roadmap.CompatibilityResultFail,
		roadmap.CompatibilityResultUnsupported,
		roadmap.CompatibilityResultTypeWarn,
		roadmap.CompatibilityResultPerfWarn,
	}
}

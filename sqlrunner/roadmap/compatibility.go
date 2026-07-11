package roadmap

import (
	"sort"
	"strings"
	"time"
)

const (
	CompatibilityMySQL           = "mysql"
	CompatibilityQuanta          = "quanta"
	CompatibilityQuantaExtension = "quanta_extension"

	CompatibilityResultPass        = "PASS"
	CompatibilityResultFail        = "FAIL"
	CompatibilityResultUnsupported = "UNSUPPORTED"
	CompatibilityResultPerfWarn    = "PERF_WARN"
	CompatibilityResultTypeWarn    = "TYPE_WARN"
)

// CompatibilityCaseReport summarizes one case using compatibility-level categories.
type CompatibilityCaseReport struct {
	ID            string
	Feature       string
	Compatibility string
	Requires      []string
	Category      string
	Details       string
	Duration      time.Duration
}

// CompatibilityFeatureReport summarizes compatibility categories for one feature family.
type CompatibilityFeatureReport struct {
	Feature string
	Counts  map[string]int
}

// CompatibilityReport summarizes SQLRunner results as a MySQL compatibility scorecard.
type CompatibilityReport struct {
	Suite    string
	Cases    []CompatibilityCaseReport
	Counts   map[string]int
	Features []CompatibilityFeatureReport
}

// BuildCompatibilityReport derives a compatibility scorecard from a suite and SQLRunner summary.
func BuildCompatibilityReport(suite *Suite, summary Summary) CompatibilityReport {
	report := CompatibilityReport{
		Suite:  summary.Suite,
		Cases:  make([]CompatibilityCaseReport, 0, len(summary.Results)),
		Counts: map[string]int{},
	}
	if report.Suite == "" && suite != nil {
		report.Suite = suite.Name
	}

	tests := map[string]TestCase{}
	if suite != nil {
		for _, test := range suite.Tests {
			tests[test.ID] = test
		}
	}
	featureCounts := map[string]map[string]int{}
	for _, result := range summary.Results {
		test := tests[result.ID]
		category := compatibilityCategory(result)
		feature := test.Feature
		if feature == "" {
			feature = "uncategorized"
		}
		caseReport := CompatibilityCaseReport{
			ID:            result.ID,
			Feature:       feature,
			Compatibility: test.Compatibility,
			Requires:      append([]string(nil), test.Requires...),
			Category:      category,
			Details:       result.Details,
			Duration:      result.Duration,
		}
		report.Cases = append(report.Cases, caseReport)
		report.Counts[category]++
		if featureCounts[feature] == nil {
			featureCounts[feature] = map[string]int{}
		}
		featureCounts[feature][category]++
	}

	features := make([]string, 0, len(featureCounts))
	for feature := range featureCounts {
		features = append(features, feature)
	}
	sort.Strings(features)
	for _, feature := range features {
		report.Features = append(report.Features, CompatibilityFeatureReport{
			Feature: feature,
			Counts:  featureCounts[feature],
		})
	}
	return report
}

func compatibilityCategory(result CaseResult) string {
	switch result.Status {
	case ResultPass:
		return CompatibilityResultPass
	case ResultXFail, ResultSkip:
		return CompatibilityResultUnsupported
	case ResultFail:
		if strings.HasPrefix(strings.ToLower(result.Details), "types differ:") {
			return CompatibilityResultTypeWarn
		}
		return CompatibilityResultFail
	case ResultXPass:
		return CompatibilityResultFail
	default:
		return CompatibilityResultFail
	}
}

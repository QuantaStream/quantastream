package roadmap

import (
	"context"
	"fmt"
	"time"
)

// CompatibilityCaptureOptions controls expected-result capture for compatibility suites.
type CompatibilityCaptureOptions struct {
	Canonical CanonicalOptions
}

// CompatibilityCaptureResult contains captured expected results and a capture summary.
type CompatibilityCaptureResult struct {
	Expected CompatibilityExpectedFile
	Suite    Suite
	Summary  Summary
}

// CaptureCompatibilityExpected executes a suite and captures canonical expected results.
func (r Runner) CaptureCompatibilityExpected(ctx context.Context, suite *Suite, options CompatibilityCaptureOptions) CompatibilityCaptureResult {
	canonical := options.Canonical
	if canonical == (CanonicalOptions{}) {
		canonical = DefaultCanonicalOptions()
	}

	summary := Summary{Suite: suite.Name, Results: make([]CaseResult, 0, len(suite.Tests))}
	captured := make([]CompatibilityExpectedCase, 0, len(suite.Tests))
	engine := r.engine()

	for _, test := range suite.Tests {
		if test.Status == CaseSkip {
			summary.Results = append(summary.Results, CaseResult{ID: test.ID, Status: ResultSkip})
			continue
		}

		caseCtx, cancel := context.WithTimeout(ctx, test.CaseTimeout())
		if r.Verbose {
			r.logf("CAPTURE %s (%s)", test.ID, test.Kind)
		}
		started := time.Now()
		caseEngine := engine
		if aware, ok := engine.(caseAwareEngine); ok {
			caseEngine = aware.WithTestCase(test)
		}

		var details string
		switch test.Kind {
		case "query":
			actual, err := caseEngine.Query(caseCtx, test.SQL)
			if err != nil {
				details = "unexpected error: " + err.Error()
				captured = append(captured, CaptureCompatibilityErrorCase(test, "query", err))
			} else {
				captured = append(captured, CaptureCompatibilityQueryCase(test, actual, canonical))
			}
		case "admin":
			if r.Admin == nil {
				details = "admin executor is not configured"
			} else if err := r.Admin(caseCtx, test.SQL); err != nil {
				details = "unexpected error: " + err.Error()
			}
		default:
			affected, err := caseEngine.Exec(caseCtx, test.SQL)
			if err != nil {
				details = "unexpected error: " + err.Error()
				captured = append(captured, CaptureCompatibilityErrorCase(test, "statement", err))
			} else {
				captured = append(captured, CaptureCompatibilityStatementCase(test, affected))
			}
		}

		if caseErr := caseCtx.Err(); caseErr != nil && details == "" {
			details = "case context errored: " + caseErr.Error()
		}
		result := compatibilityCaptureCaseResult(test.ID, details, time.Since(started))
		if r.Verbose {
			if result.Details == "" {
				r.logf("DONE   %s %s in %s", result.Status, result.ID, result.Duration.Round(time.Millisecond))
			} else {
				r.logf("DONE   %s %s in %s: %s", result.Status, result.ID, result.Duration.Round(time.Millisecond), result.Details)
			}
		}
		summary.Results = append(summary.Results, result)
		caseErr := caseCtx.Err()
		cancel()
		if caseErr != nil {
			break
		}
	}

	expected := NewCompatibilityExpectedFile(suite, captured)
	return CompatibilityCaptureResult{
		Expected: expected,
		Suite:    BuildCompatibilityExpectedSuite(suite, expected),
		Summary:  summary,
	}
}

func compatibilityCaptureCaseResult(id string, details string, duration time.Duration) CaseResult {
	if details == "" {
		return CaseResult{ID: id, Status: ResultPass, Duration: duration}
	}
	return CaseResult{ID: id, Status: ResultFail, Details: details, Duration: duration}
}

// CaptureCompatibilityErrorCase captures a reference error for later comparison.
func CaptureCompatibilityErrorCase(test TestCase, kind string, err error) CompatibilityExpectedCase {
	errorText := ""
	if err != nil {
		errorText = err.Error()
	}
	return CompatibilityExpectedCase{
		ID:    test.ID,
		Kind:  kind,
		Error: errorText,
		Metadata: CompatibilityCaseMetadata{
			Feature:       test.Feature,
			Compatibility: test.Compatibility,
			Requires:      append([]string(nil), test.Requires...),
		},
	}
}

// MarshalCompatibilityExpected serializes captured compatibility expected results.
func MarshalCompatibilityExpected(expected CompatibilityExpectedFile) ([]byte, error) {
	data, err := yamlMarshal(expected)
	if err != nil {
		return nil, fmt.Errorf("marshal compatibility expected results: %w", err)
	}
	return data, nil
}

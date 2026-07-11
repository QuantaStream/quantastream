package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

type engineDiffConfig struct {
	Reference string
	Target    string
}

func parseEngineDiff(value string) (engineDiffConfig, error) {
	parts := strings.Split(value, ",")
	if len(parts) != 2 {
		return engineDiffConfig{}, fmt.Errorf("engine_diff must be reference,target")
	}
	diff := engineDiffConfig{
		Reference: strings.ToLower(strings.TrimSpace(parts[0])),
		Target:    strings.ToLower(strings.TrimSpace(parts[1])),
	}
	if diff.Reference == "" || diff.Target == "" {
		return engineDiffConfig{}, fmt.Errorf("engine_diff must include both reference and target engines")
	}
	return diff, nil
}

func validateEngineForFlags(engine string, cfg runnerConfig) error {
	switch engine {
	case engineProxy, engineDistributed:
		if cfg.Host == "" || cfg.User == "" {
			return fmt.Errorf("host and user are required for %s engine", engine)
		}
	case engineInaboxLocal:
	case engineRuntime, engineRuntimeInspect, engineInaboxDirect:
	case engineMySQLReference:
		return mysqlReferenceConfig{Driver: cfg.MySQLDriver, DSN: cfg.MySQLDSN}.validate()
	default:
		return fmt.Errorf("unsupported engine %q", engine)
	}
	return nil
}

func executeCompatibilityDiff(ctx context.Context, suite *roadmap.Suite, cfg runnerConfig) error {
	diff, err := parseEngineDiff(cfg.EngineDiff)
	if err != nil {
		return err
	}

	referenceCfg := cfg
	referenceCfg.Engine = diff.Reference
	referenceHarness, err := buildHarness(suite, referenceCfg)
	if err != nil {
		return fmt.Errorf("build reference harness %q: %w", diff.Reference, err)
	}
	defer closeHarness("reference", referenceHarness)

	capture := referenceHarness.Runner.CaptureCompatibilityExpected(ctx, suite, roadmap.CompatibilityCaptureOptions{Canonical: roadmap.DefaultCanonicalOptions()})
	log.Printf("\n-------- SQL Compatibility Reference Capture: %s (%s) --------", capture.Summary.Suite, diff.Reference)
	logSummaryResults(capture.Summary, cfg.Verbose)
	if capture.Summary.HasFailures() {
		return fmt.Errorf("reference capture contains FAIL results")
	}

	targetCfg := cfg
	targetCfg.Engine = diff.Target
	targetHarness, err := buildHarness(&capture.Suite, targetCfg)
	if err != nil {
		return fmt.Errorf("build target harness %q: %w", diff.Target, err)
	}
	defer closeHarness("target", targetHarness)

	targetSummary := targetHarness.Runner.Run(ctx, &capture.Suite)
	log.Printf("\n-------- SQL Compatibility Diff: %s (%s -> %s) --------", targetSummary.Suite, diff.Reference, diff.Target)
	logSummaryResults(targetSummary, cfg.Verbose)
	logSlowCases(targetSummary, cfg.SlowThreshold, cfg.Verbose)
	if cfg.CompatReport {
		logCompatibilityReport(&capture.Suite, targetSummary)
	}
	if targetSummary.HasFailures() {
		return fmt.Errorf("differential suite contains FAIL or XPASS results")
	}
	return nil
}

func closeHarness(label string, harness runnerHarness) {
	if harness.Close == nil {
		return
	}
	if err := harness.Close(); err != nil {
		log.Printf("SQL roadmap %s harness close failed: %v", label, err)
	}
}

func logSummaryResults(summary roadmap.Summary, verbose bool) {
	for _, result := range summary.Results {
		duration := formatCaseDuration(result.Duration, verbose)
		if result.Details == "" {
			log.Printf("%-6s %s%s", result.Status, result.ID, duration)
		} else {
			log.Printf("%-6s %s%s: %s", result.Status, result.ID, duration, result.Details)
		}
	}
}

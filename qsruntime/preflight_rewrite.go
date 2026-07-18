package qsruntime

import (
	"context"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// PreflightRewrite applies one compatibility transform before native planning.
//
// The boundary is intentionally narrow and temporary: it isolates compatibility
// scaffolding from ExecuteSQL while the equivalent shapes move into first-class
// IR and planner support.
type PreflightRewrite interface {
	// RuleID returns the stable optimizer rewrite rule represented by this preflight hook.
	RuleID() qsbridge.RewriteRuleID
	// ApplyPreflightRewrite returns transformed SQL, optional typed replacement state, diagnostics, and optimizer trace metadata.
	ApplyPreflightRewrite(ctx context.Context, sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) (PreflightRewriteResult, error)
}

// PreflightRewriteInspection summarizes one preflight rule decision.
type PreflightRewriteInspection struct {
	Rule                  qsbridge.RewriteRuleID
	Applied               bool
	Descriptor            *PreflightRewriteDescriptorSummary
	ReplacementExpression *PreflightRewriteExpressionReport
	HelperReports         []PreflightHelperExecutionRequestReport
	Duration              time.Duration
	Diagnostics           int
	Blocking              int
}

// PreflightRewriteSummary summarizes the complete preflight rewrite pass.
type PreflightRewriteSummary struct {
	Total       int
	Applied     int
	Skipped     int
	Duration    time.Duration
	Diagnostics int
	Blocking    int
	Rewrites    []PreflightRewriteInspection
}

// PreflightRewriteResult captures the output of one pre-planning rewrite attempt.
type PreflightRewriteResult struct {
	SQL                   string
	Applied               bool
	Duration              time.Duration
	Diagnostics           qsbridge.DiagnosticSet
	Descriptor            *PreflightRewriteDescriptorSummary
	ReplacementExpr       qsbridge.Expr
	NativePredicates      NativePredicateSet
	ReplacementExpression *PreflightRewriteExpressionReport
	HelperReports         []PreflightHelperExecutionRequestReport
	Optimization          qsbridge.OptimizationTrace
	Preflight             PreflightRewriteSummary
}

type preflightRewriteFunc func(ctx context.Context, sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) (string, qsbridge.DiagnosticSet, qsbridge.OptimizationTrace, qsbridge.Expr, NativePredicateSet, *PreflightRewriteExpressionReport, []PreflightHelperExecutionRequestReport, error, bool)
type preflightRewriteDescriptorFunc func(sql string) (*PreflightRewriteDescriptorSummary, bool)

type preflightRewriteRule struct {
	rule       qsbridge.RewriteRuleID
	apply      preflightRewriteFunc
	descriptor preflightRewriteDescriptorFunc
}

// RuleID returns the stable optimizer rewrite rule represented by this preflight hook.
func (r preflightRewriteRule) RuleID() qsbridge.RewriteRuleID {
	return r.rule
}

// ApplyPreflightRewrite adapts a rewrite function to the PreflightRewrite interface.
func (r preflightRewriteRule) ApplyPreflightRewrite(ctx context.Context, sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) (PreflightRewriteResult, error) {
	var descriptor *PreflightRewriteDescriptorSummary
	if r.descriptor != nil {
		descriptor, _ = r.descriptor(sql)
	}
	started := time.Now()
	rewritten, diagnostics, optimization, replacementExpr, nativePredicates, replacementExpression, helperReports, err, applied := r.apply(ctx, sql, options, values...)
	duration := time.Since(started)
	if !applied {
		return PreflightRewriteResult{
			SQL:        sql,
			Duration:   duration,
			Descriptor: descriptor,
		}, err
	}
	return PreflightRewriteResult{
		SQL:                   rewritten,
		Applied:               true,
		Duration:              duration,
		Diagnostics:           diagnostics,
		Descriptor:            descriptor,
		ReplacementExpr:       replacementExpr,
		NativePredicates:      nativePredicates,
		ReplacementExpression: replacementExpression,
		HelperReports:         helperReports,
		Optimization:          optimization,
	}, err
}

func (r SQLRuntime) preflightRewriteRules() []PreflightRewrite {
	return []PreflightRewrite{
		preflightRewriteRule{
			rule:       qsbridge.RewriteCorrelatedAggregatePreflight,
			apply:      r.rewriteCorrelatedAverageQuantitySubquery,
			descriptor: r.correlatedAverageQuantityRewriteDescriptor,
		},
	}
}

func (r SQLRuntime) applyPreflightRewrites(ctx context.Context, sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) (PreflightRewriteResult, error) {
	result := PreflightRewriteResult{
		SQL:          sql,
		Optimization: qsbridge.NewOptimizationTrace(),
	}
	for _, rewrite := range r.preflightRewriteRules() {
		step, err := rewrite.ApplyPreflightRewrite(ctx, result.SQL, options, values...)
		inspection := inspectPreflightRewrite(rewrite.RuleID(), step)
		result.Preflight.add(inspection)
		if err != nil || step.Diagnostics.BlocksNative() {
			result.Diagnostics = append(result.Diagnostics, step.Diagnostics...)
			result.Optimization = mergeRuntimeOptimizationTrace(result.Optimization, step.Optimization)
			return result, err
		}
		if !step.Applied {
			continue
		}
		result.SQL = step.SQL
		result.ReplacementExpr = step.ReplacementExpr
		result.NativePredicates.CorrelatedAggregate = append(result.NativePredicates.CorrelatedAggregate, step.NativePredicates.CorrelatedAggregate...)
		result.Diagnostics = append(result.Diagnostics, step.Diagnostics...)
		result.Optimization = mergeRuntimeOptimizationTrace(result.Optimization, step.Optimization)
	}
	return result, nil
}

func inspectPreflightRewrite(rule qsbridge.RewriteRuleID, result PreflightRewriteResult) PreflightRewriteInspection {
	inspection := PreflightRewriteInspection{
		Rule:                  rule,
		Applied:               result.Applied,
		Descriptor:            result.Descriptor,
		ReplacementExpression: result.ReplacementExpression,
		HelperReports:         append([]PreflightHelperExecutionRequestReport(nil), result.HelperReports...),
		Duration:              result.Duration,
		Diagnostics:           len(result.Diagnostics),
	}
	for _, diagnostic := range result.Diagnostics {
		if diagnostic.BlocksNative() {
			inspection.Blocking++
		}
	}
	return inspection
}

func (s *PreflightRewriteSummary) add(inspection PreflightRewriteInspection) {
	if s == nil {
		return
	}
	s.Total++
	if inspection.Applied {
		s.Applied++
	} else {
		s.Skipped++
	}
	s.Duration += inspection.Duration
	s.Diagnostics += inspection.Diagnostics
	s.Blocking += inspection.Blocking
	s.Rewrites = append(s.Rewrites, inspection)
}

func mergeRuntimeOptimizationTrace(primary qsbridge.OptimizationTrace, secondary qsbridge.OptimizationTrace) qsbridge.OptimizationTrace {
	if !primary.Supported && len(primary.Rewrites) == 0 && len(primary.Diagnostics) == 0 {
		primary = qsbridge.NewOptimizationTrace()
	}
	for _, rewrite := range secondary.Rewrites {
		primary.Add(rewrite)
	}
	for _, diagnostic := range secondary.Diagnostics {
		primary.Diagnostics = append(primary.Diagnostics, diagnostic)
		if diagnostic.BlocksNative() {
			primary.Supported = false
		}
	}
	return primary
}

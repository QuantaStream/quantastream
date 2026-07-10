package qsruntime

import (
	"context"
	"fmt"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// PreflightHelperExecutionRequest describes one temporary helper execution
// invoked by a preflight rewrite.
type PreflightHelperExecutionRequest struct {
	Plan    PreflightRewriteHelperPlanDescriptor
	SQL     string
	Payload PreflightHelperPayload
	Options qsbridge.ExecutionOptions
	Values  []qsbridge.ParameterValue
}

// PreflightHelperPayload carries typed helper inputs alongside temporary SQL fallback text.
type PreflightHelperPayload struct {
	Scalar                   *PreflightScalarHelperPayload
	ParentKeyLookup          *PreflightParentKeyLookupPayload
	AggregateThresholdLookup *PreflightAggregateThresholdLookupPayload
}

// PreflightScalarHelperPayload describes a scalar helper result request.
type PreflightScalarHelperPayload struct {
	SubquerySQL  string
	OutputName   string
	Materialized qsbridge.ResultCell
}

// PreflightParentKeyLookupPayload describes a filtered parent key lookup helper.
type PreflightParentKeyLookupPayload struct {
	Table    string
	Alias    string
	KeyField string
	Filters  []PreflightHelperEqualityFilter
	Output   string
}

// PreflightHelperEqualityFilter describes one equality filter used by a helper payload.
type PreflightHelperEqualityFilter struct {
	Field string
	Value string
}

// PreflightAggregateThresholdLookupPayload describes a grouped aggregate threshold helper.
type PreflightAggregateThresholdLookupPayload struct {
	Table             string
	KeyField          string
	AggregateFunction string
	ValueField        string
	PartKeys          []int64
	ParentRownums     []qsbridge.QuantaRownum
	Factor            float64
	KeyOutput         string
	ValueOutput       string
}

// PreflightHelperExecutionRequestReport is a compact inspection view of one helper request.
type PreflightHelperExecutionRequestReport struct {
	Plan        PreflightRewriteHelperPlanReport
	NativeStep  *qsbridge.NativeSubqueryStepReport
	NativeTrace *qsbridge.NativeSubqueryStepTrace
	SQL         string
	Payload     PreflightHelperPayloadReport
}

// PreflightHelperPayloadReport summarizes typed helper inputs without dumping full execution state.
type PreflightHelperPayloadReport struct {
	Kind                     PreflightRewriteHelperPlanKind
	Scalar                   *PreflightScalarHelperPayloadReport
	ParentKeyLookup          *PreflightParentKeyLookupPayloadReport
	AggregateThresholdLookup *PreflightAggregateThresholdLookupPayloadReport
}

// PreflightScalarHelperPayloadReport summarizes a scalar helper payload.
type PreflightScalarHelperPayloadReport struct {
	OutputName   string
	SubquerySQL  string
	Materialized bool
}

// PreflightParentKeyLookupPayloadReport summarizes a parent-key lookup payload.
type PreflightParentKeyLookupPayloadReport struct {
	Table    string
	Alias    string
	KeyField string
	Filters  []string
	Output   string
}

// PreflightAggregateThresholdLookupPayloadReport summarizes an aggregate-threshold payload.
type PreflightAggregateThresholdLookupPayloadReport struct {
	Table             string
	KeyField          string
	AggregateFunction string
	ValueField        string
	PartKeyCount      int
	ParentRownumCount int
	Factor            float64
	KeyOutput         string
	ValueOutput       string
}

// PreflightHelperExecutionResult captures the SQL runtime result produced by a
// temporary helper execution.
type PreflightHelperExecutionResult struct {
	Plan        PreflightRewriteHelperPlanDescriptor
	SQL         string
	Payload     PreflightHelperPayload
	Result      SQLExecutionResult
	NativeTrace *qsbridge.NativeSubqueryStepTrace
	Diagnostics qsbridge.DiagnosticSet
}

// PreflightHelperExecutor executes temporary helper work for preflight rewrites.
type PreflightHelperExecutor interface {
	// ExecutePreflightHelper runs one helper request and returns its helper-scoped result.
	ExecutePreflightHelper(ctx context.Context, runtime SQLRuntime, request PreflightHelperExecutionRequest) (PreflightHelperExecutionResult, error)
}

type sqlBackedPreflightHelperExecutor struct{}

// ExecutePreflightHelper executes helper work by routing the helper SQL through SQLRuntime.
func (sqlBackedPreflightHelperExecutor) ExecutePreflightHelper(ctx context.Context, runtime SQLRuntime, request PreflightHelperExecutionRequest) (PreflightHelperExecutionResult, error) {
	result, err := runtime.ExecuteSQL(ctx, request.SQL, request.Options, request.Values...)
	diagnostics := append(qsbridge.DiagnosticSet(nil), result.Diagnostics...)
	diagnostics = append(diagnostics, result.Runtime.Diagnostics...)
	return PreflightHelperExecutionResult{
		Plan:        request.Plan,
		SQL:         request.SQL,
		Payload:     request.Payload,
		Result:      result,
		Diagnostics: diagnostics,
	}, err
}

func (r SQLRuntime) executePreflightHelper(ctx context.Context, request PreflightHelperExecutionRequest) (PreflightHelperExecutionResult, error) {
	if diagnostics := request.ValidatePayload(); diagnostics.BlocksNative() {
		return PreflightHelperExecutionResult{
			Plan:        request.Plan,
			SQL:         request.SQL,
			Diagnostics: diagnostics,
		}, nil
	}
	executor := r.PreflightHelpers
	if executor == nil {
		executor = sqlBackedPreflightHelperExecutor{}
	}
	result, err := executor.ExecutePreflightHelper(ctx, r, request)
	result.Diagnostics = normalizePreflightHelperDiagnostics(request.Plan.Kind, result.Diagnostics)
	return result, err
}

// ValidatePayload checks that a helper request carries the typed payload required by its plan kind.
func (r PreflightHelperExecutionRequest) ValidatePayload() qsbridge.DiagnosticSet {
	switch r.Plan.Kind {
	case PreflightHelperPlanScalarSubquery:
		if r.Payload.Scalar == nil || r.Payload.Scalar.OutputName == "" {
			return helperPayloadDiagnostic(r.Plan.Kind, "scalar helper payload is incomplete")
		}
	case PreflightHelperPlanParentKeyLookup:
		payload := r.Payload.ParentKeyLookup
		if payload == nil || payload.Table == "" || payload.Alias == "" || payload.KeyField == "" || payload.Output == "" || len(payload.Filters) == 0 {
			return helperPayloadDiagnostic(r.Plan.Kind, "parent-key lookup helper payload is incomplete")
		}
	case PreflightHelperPlanAggregateThresholdLookup:
		payload := r.Payload.AggregateThresholdLookup
		if payload == nil || payload.Table == "" || payload.KeyField == "" || payload.AggregateFunction == "" || payload.ValueField == "" || payload.KeyOutput == "" || payload.ValueOutput == "" {
			return helperPayloadDiagnostic(r.Plan.Kind, "aggregate-threshold helper payload is incomplete")
		}
	default:
		return helperPayloadDiagnostic(r.Plan.Kind, "unknown preflight helper plan kind")
	}
	return nil
}

// Report returns a compact inspection view of one helper execution request.
func (r PreflightHelperExecutionRequest) Report() PreflightHelperExecutionRequestReport {
	planReport := r.Plan.Report()
	return PreflightHelperExecutionRequestReport{
		Plan:       planReport,
		NativeStep: planReport.NativeStep,
		SQL:        r.SQL,
		Payload:    r.Payload.Report(r.Plan.Kind),
	}
}

// Report returns a compact inspection view of one helper execution result.
func (r PreflightHelperExecutionResult) Report() PreflightHelperExecutionRequestReport {
	report := PreflightHelperExecutionRequest{
		Plan:    r.Plan,
		SQL:     r.SQL,
		Payload: r.Payload,
	}.Report()
	if r.NativeTrace != nil {
		trace := *r.NativeTrace
		report.NativeTrace = &trace
	}
	return report
}

// Report returns a compact inspection view of typed helper payloads.
func (p PreflightHelperPayload) Report(kind PreflightRewriteHelperPlanKind) PreflightHelperPayloadReport {
	report := PreflightHelperPayloadReport{Kind: kind}
	if p.Scalar != nil {
		report.Scalar = &PreflightScalarHelperPayloadReport{
			OutputName:   p.Scalar.OutputName,
			SubquerySQL:  p.Scalar.SubquerySQL,
			Materialized: p.Scalar.Materialized.Kind != "" || p.Scalar.Materialized.Value != nil,
		}
	}
	if p.ParentKeyLookup != nil {
		filters := make([]string, 0, len(p.ParentKeyLookup.Filters))
		for _, filter := range p.ParentKeyLookup.Filters {
			filters = append(filters, filter.Field+"="+filter.Value)
		}
		report.ParentKeyLookup = &PreflightParentKeyLookupPayloadReport{
			Table:    p.ParentKeyLookup.Table,
			Alias:    p.ParentKeyLookup.Alias,
			KeyField: p.ParentKeyLookup.KeyField,
			Filters:  filters,
			Output:   p.ParentKeyLookup.Output,
		}
	}
	if p.AggregateThresholdLookup != nil {
		report.AggregateThresholdLookup = &PreflightAggregateThresholdLookupPayloadReport{
			Table:             p.AggregateThresholdLookup.Table,
			KeyField:          p.AggregateThresholdLookup.KeyField,
			AggregateFunction: p.AggregateThresholdLookup.AggregateFunction,
			ValueField:        p.AggregateThresholdLookup.ValueField,
			PartKeyCount:      len(p.AggregateThresholdLookup.PartKeys),
			ParentRownumCount: len(p.AggregateThresholdLookup.ParentRownums),
			Factor:            p.AggregateThresholdLookup.Factor,
			KeyOutput:         p.AggregateThresholdLookup.KeyOutput,
			ValueOutput:       p.AggregateThresholdLookup.ValueOutput,
		}
	}
	return report
}

func helperPayloadDiagnostic(kind PreflightRewriteHelperPlanKind, message string) qsbridge.DiagnosticSet {
	return helperExecutionDiagnostic(kind, message)
}

func helperExecutionDiagnostic(kind PreflightRewriteHelperPlanKind, message string) qsbridge.DiagnosticSet {
	code := qsbridge.DiagnosticInternalInvariant
	if kind == PreflightHelperPlanScalarSubquery {
		code = qsbridge.DiagnosticScalarSubquery
	}
	if kind == PreflightHelperPlanParentKeyLookup || kind == PreflightHelperPlanAggregateThresholdLookup {
		code = qsbridge.DiagnosticCorrelatedAggregateSubquery
	}
	return qsbridge.DiagnosticSet{qsbridge.ErrorDiagnostic(code, qsbridge.PhaseExecute, helperDiagnosticMessage(kind, qsbridge.PhaseExecute, message))}
}

func normalizePreflightHelperDiagnostics(kind PreflightRewriteHelperPlanKind, diagnostics qsbridge.DiagnosticSet) qsbridge.DiagnosticSet {
	normalized := append(qsbridge.DiagnosticSet(nil), diagnostics...)
	for i := range normalized {
		if strings.Contains(normalized[i].Message, "preflight helper rule=") {
			continue
		}
		phase := normalized[i].Phase
		if phase == "" {
			phase = qsbridge.PhaseExecute
			normalized[i].Phase = phase
		}
		normalized[i].Message = helperDiagnosticMessage(kind, phase, normalized[i].Message)
	}
	return normalized
}

func helperDiagnosticMessage(kind PreflightRewriteHelperPlanKind, phase qsbridge.DiagnosticPhase, message string) string {
	return fmt.Sprintf("preflight helper rule=%s helper=%s phase=%s: %s", preflightRuleForHelperKind(kind), kind, phase, message)
}

func preflightRuleForHelperKind(kind PreflightRewriteHelperPlanKind) qsbridge.RewriteRuleID {
	switch kind {
	case PreflightHelperPlanScalarSubquery:
		return qsbridge.RewriteScalarSubqueryPreflight
	case PreflightHelperPlanParentKeyLookup, PreflightHelperPlanAggregateThresholdLookup:
		return qsbridge.RewriteCorrelatedAggregatePreflight
	default:
		return qsbridge.RewriteRuleID("unknown_preflight_rewrite")
	}
}

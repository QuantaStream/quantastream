package qsbridge

// ExecutionRequestID identifies one protocol execution request.
type ExecutionRequestID string

// CursorMode describes the client-visible cursor contract requested by a protocol adapter.
type CursorMode string

const (
	// CursorNone means results can be delivered without cursor state.
	CursorNone CursorMode = ""
	// CursorForwardOnly means results may be consumed once in order.
	CursorForwardOnly CursorMode = "forward_only"
	// CursorScrollable means the protocol requested random cursor movement.
	CursorScrollable CursorMode = "scrollable"
)

// FullTableScanPolicy controls how execution handles unfiltered source scans.
type FullTableScanPolicy string

const (
	// FullTableScanAllow permits full-table scans. This is the default for MySQL compatibility.
	FullTableScanAllow FullTableScanPolicy = ""
	// FullTableScanWarn keeps executing but asks explain/inspection surfaces to expose the scan.
	FullTableScanWarn FullTableScanPolicy = "warn"
	// FullTableScanReject rejects execution requests whose inspection reports a full-table scan.
	FullTableScanReject FullTableScanPolicy = "reject"
)

// ExecutionOptions are adapter-provided preferences for a future executor.
type ExecutionOptions struct {
	RequestID           ExecutionRequestID
	MaxRows             int
	BatchSize           int
	Streaming           bool
	Cursor              CursorMode
	Cancelable          bool
	TraceExplain        bool
	IncludeProfile      bool
	FullTableScanPolicy FullTableScanPolicy
}

// ExecutionRequest is a non-executing descriptor for handing a bound plan to an executor.
type ExecutionRequest struct {
	Bound          BoundPlan
	Options        ExecutionOptions
	Diagnostics    DiagnosticSet
	Supported      bool
	Result         ResultShape
	ResultColumns  []ResultColumn
	Statement      StatementResult
	SessionActions []SessionAction
	Access         []AccessRequirement
}

// ExecutionRequest validates values and returns a future executor-facing descriptor.
func (p PreparedPlan) ExecutionRequest(options ExecutionOptions, values ...ParameterValue) ExecutionRequest {
	return p.Bind(values...).ExecutionRequest(options)
}

// ExecutionRequest returns a future executor-facing descriptor for this bound plan.
func (p BoundPlan) ExecutionRequest(options ExecutionOptions) ExecutionRequest {
	prepared := p.Prepared
	optionDiagnostics := options.Diagnostics()
	policyDiagnostics := fullTableScanPolicyDiagnostics(prepared, options)
	diagnostics := mergeDiagnosticSets(p.Diagnostics, optionDiagnostics, policyDiagnostics)
	supported := p.SupportedForExecution() && !diagnostics.BlocksNative()
	return ExecutionRequest{
		Bound:          p,
		Options:        options,
		Diagnostics:    diagnostics,
		Supported:      supported,
		Result:         prepared.Result,
		ResultColumns:  append([]ResultColumn(nil), prepared.ResultColumns...),
		Statement:      cloneStatementResult(prepared.Statement),
		SessionActions: prepared.SessionActions(),
		Access:         cloneAccessRequirements(prepared.Access),
	}
}

// SupportedForExecution reports whether the request is valid for a future executor.
func (r ExecutionRequest) SupportedForExecution() bool {
	return r.Supported && !r.Diagnostics.BlocksNative()
}

// Diagnostics validates execution options without executing the plan.
func (o ExecutionOptions) Diagnostics() DiagnosticSet {
	diagnostics := make(DiagnosticSet, 0)
	if o.MaxRows < 0 {
		diagnostics = append(diagnostics, ErrorDiagnostic(
			DiagnosticInvalidExecutionOption,
			PhaseExecute,
			"max rows cannot be negative",
		))
	}
	if o.BatchSize < 0 {
		diagnostics = append(diagnostics, ErrorDiagnostic(
			DiagnosticInvalidExecutionOption,
			PhaseExecute,
			"batch size cannot be negative",
		))
	}
	if o.Cursor == CursorScrollable {
		diagnostics = append(diagnostics, ErrorDiagnostic(
			DiagnosticInvalidExecutionOption,
			PhaseExecute,
			"scrollable cursors are not part of the native execution scaffold",
		))
	}
	switch o.FullTableScanPolicy {
	case FullTableScanAllow, FullTableScanWarn, FullTableScanReject:
	default:
		diagnostics = append(diagnostics, ErrorDiagnostic(
			DiagnosticInvalidExecutionOption,
			PhaseExecute,
			"full-table-scan policy must be allow, warn, or reject",
		))
	}
	return diagnostics
}

func fullTableScanPolicyDiagnostics(prepared PreparedPlan, options ExecutionOptions) DiagnosticSet {
	if options.FullTableScanPolicy != FullTableScanReject {
		return nil
	}
	scan := prepared.Inspection.Query.Scan
	if !scan.FullTable && len(scan.Tables) == 0 && len(prepared.Query.Sources) > 0 {
		scan = summarizeScan(prepared.Query)
	}
	if !scan.FullTable {
		return nil
	}
	return DiagnosticSet{ErrorDiagnostic(
		DiagnosticFullTableScanRejected,
		PhaseExecute,
		"full-table scan rejected by execution policy",
	)}
}

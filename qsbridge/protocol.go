package qsbridge

// ProtocolKind identifies the network or in-process adapter class.
type ProtocolKind string

const (
	// ProtocolUnknown means the adapter did not identify a protocol family.
	ProtocolUnknown ProtocolKind = ""
	// ProtocolMySQL identifies the MySQL wire-compatible adapter.
	ProtocolMySQL ProtocolKind = "mysql"
	// ProtocolGRPC identifies the future typed control-plane adapter.
	ProtocolGRPC ProtocolKind = "grpc"
	// ProtocolGo identifies an in-process Go adapter.
	ProtocolGo ProtocolKind = "go"
	// ProtocolHTTP identifies an optional HTTP gateway adapter.
	ProtocolHTTP ProtocolKind = "http"
)

// ProtocolCapability identifies one protocol-visible behavior.
type ProtocolCapability string

const (
	// ProtocolCapabilityPreparedStatements means the adapter can prepare and execute handles.
	ProtocolCapabilityPreparedStatements ProtocolCapability = "prepared_statements"
	// ProtocolCapabilityBatchExecution means the adapter can submit multiple value sets.
	ProtocolCapabilityBatchExecution ProtocolCapability = "batch_execution"
	// ProtocolCapabilityStreamingResults means the adapter can consume incremental row chunks.
	ProtocolCapabilityStreamingResults ProtocolCapability = "streaming_results"
	// ProtocolCapabilityForwardOnlyCursor means the adapter can track forward-only cursors.
	ProtocolCapabilityForwardOnlyCursor ProtocolCapability = "forward_only_cursor"
	// ProtocolCapabilityCancellation means the adapter can request cancellation by request id.
	ProtocolCapabilityCancellation ProtocolCapability = "cancellation"
	// ProtocolCapabilityExplain means the adapter can request explain metadata.
	ProtocolCapabilityExplain ProtocolCapability = "explain"
	// ProtocolCapabilityStructuredExplain means the adapter can request sectioned explain bundles.
	ProtocolCapabilityStructuredExplain ProtocolCapability = "structured_explain"
	// ProtocolCapabilityProfile means the adapter can request execution profile metadata.
	ProtocolCapabilityProfile ProtocolCapability = "profile"
	// ProtocolCapabilityPlanCachePolicy means the adapter can request plan-cache identity policy metadata.
	ProtocolCapabilityPlanCachePolicy ProtocolCapability = "plan_cache_policy"
	// ProtocolCapabilityStatementResults means the adapter can deliver OK/status metadata.
	ProtocolCapabilityStatementResults ProtocolCapability = "statement_results"
	// ProtocolCapabilitySessionActions means the adapter can apply session action metadata.
	ProtocolCapabilitySessionActions ProtocolCapability = "session_actions"
)

// ProtocolCapabilities is an ordered set-like list of protocol capabilities.
type ProtocolCapabilities []ProtocolCapability

// Has reports whether capability is present.
func (c ProtocolCapabilities) Has(capability ProtocolCapability) bool {
	for _, current := range c {
		if current == capability {
			return true
		}
	}
	return false
}

// ProtocolExecutionMode identifies the adapter path used for one request.
type ProtocolExecutionMode string

const (
	// ProtocolSimpleExecution means a direct text statement execution.
	ProtocolSimpleExecution ProtocolExecutionMode = "simple"
	// ProtocolPreparedExecution means an execution through a prepared handle.
	ProtocolPreparedExecution ProtocolExecutionMode = "prepared"
	// ProtocolBatchExecution means prepared execution with multiple value sets.
	ProtocolBatchExecution ProtocolExecutionMode = "batch"
)

// ProtocolProfile describes one adapter's visible execution contract.
//
// It is metadata only. qsbridge uses it to validate requested execution
// features without owning MySQL, gRPC, HTTP, or in-process driver behavior.
type ProtocolProfile struct {
	Kind         ProtocolKind
	Driver       string
	Capabilities ProtocolCapabilities
}

// NewProtocolProfile creates a copied protocol capability profile.
func NewProtocolProfile(kind ProtocolKind, driver string, capabilities ...ProtocolCapability) ProtocolProfile {
	return ProtocolProfile{
		Kind:         kind,
		Driver:       driver,
		Capabilities: append(ProtocolCapabilities(nil), capabilities...),
	}
}

// Clone returns a deep copy of profile.
func (p ProtocolProfile) Clone() ProtocolProfile {
	p.Capabilities = append(ProtocolCapabilities(nil), p.Capabilities...)
	return p
}

// Supports reports whether profile advertises capability.
func (p ProtocolProfile) Supports(capability ProtocolCapability) bool {
	return p.Capabilities.Has(capability)
}

// NegotiateExecution validates requested execution options for this protocol.
func (p ProtocolProfile) NegotiateExecution(mode ProtocolExecutionMode, options ExecutionOptions) ProtocolNegotiation {
	if mode == "" {
		mode = ProtocolSimpleExecution
	}
	diagnostics := options.Diagnostics()
	diagnostics = mergeDiagnosticSets(diagnostics, p.modeDiagnostics(mode))
	diagnostics = mergeDiagnosticSets(diagnostics, p.optionDiagnostics(options))
	return ProtocolNegotiation{
		Profile:     p.Clone(),
		Mode:        mode,
		Options:     options,
		Diagnostics: diagnostics,
	}
}

func (p ProtocolProfile) modeDiagnostics(mode ProtocolExecutionMode) DiagnosticSet {
	switch mode {
	case ProtocolSimpleExecution:
		return nil
	case ProtocolPreparedExecution:
		if p.Supports(ProtocolCapabilityPreparedStatements) {
			return nil
		}
		return DiagnosticSet{protocolCapabilityDiagnostic("prepared execution requires prepared-statement support")}
	case ProtocolBatchExecution:
		if p.Supports(ProtocolCapabilityBatchExecution) {
			return nil
		}
		return DiagnosticSet{protocolCapabilityDiagnostic("batch execution requires batch support")}
	default:
		return DiagnosticSet{protocolCapabilityDiagnostic("unknown protocol execution mode: " + string(mode))}
	}
}

func (p ProtocolProfile) optionDiagnostics(options ExecutionOptions) DiagnosticSet {
	diagnostics := make(DiagnosticSet, 0)
	if options.Streaming && !p.Supports(ProtocolCapabilityStreamingResults) {
		diagnostics = append(diagnostics, protocolCapabilityDiagnostic("streaming results are not supported by protocol profile"))
	}
	if options.Cursor == CursorForwardOnly && !p.Supports(ProtocolCapabilityForwardOnlyCursor) {
		diagnostics = append(diagnostics, protocolCapabilityDiagnostic("forward-only cursors are not supported by protocol profile"))
	}
	if options.Cancelable && !p.Supports(ProtocolCapabilityCancellation) {
		diagnostics = append(diagnostics, protocolCapabilityDiagnostic("cancellation is not supported by protocol profile"))
	}
	if options.TraceExplain && !p.Supports(ProtocolCapabilityExplain) {
		diagnostics = append(diagnostics, protocolCapabilityDiagnostic("explain metadata is not supported by protocol profile"))
	}
	if options.IncludeProfile && !p.Supports(ProtocolCapabilityProfile) {
		diagnostics = append(diagnostics, protocolCapabilityDiagnostic("execution profile metadata is not supported by protocol profile"))
	}
	return diagnostics
}

func protocolCapabilityDiagnostic(message string) Diagnostic {
	return ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, message)
}

// ProtocolNegotiation records protocol capability validation for one execution.
type ProtocolNegotiation struct {
	Profile     ProtocolProfile
	Mode        ProtocolExecutionMode
	Options     ExecutionOptions
	Diagnostics DiagnosticSet
}

// Supported reports whether the protocol can carry the requested execution shape.
func (n ProtocolNegotiation) Supported() bool {
	return !n.Diagnostics.BlocksNative()
}

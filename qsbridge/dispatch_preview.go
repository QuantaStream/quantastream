package qsbridge

// DispatchTarget names the executor boundary a handoff would use.
type DispatchTarget string

const (
	// DispatchTargetNative identifies the native qsbridge executor boundary.
	DispatchTargetNative DispatchTarget = "native_executor"
	// DispatchTargetLegacy identifies the legacy compatibility executor boundary.
	DispatchTargetLegacy DispatchTarget = "legacy_executor"
	// DispatchTargetNone means the handoff is rejected before execution.
	DispatchTargetNone DispatchTarget = "none"
)

// DispatchPreview describes a non-executing dispatch decision.
//
// It lets adapters and management tooling inspect the final executor boundary,
// missing-executor diagnostics, and rejection diagnostics without calling the
// native executor or the legacy compatibility runtime.
type DispatchPreview struct {
	Handoff            ExecutionHandoffKind
	Target             DispatchTarget
	Supported          bool
	ExecutorConfigured bool
	WillDispatch       bool
	Diagnostics        DiagnosticSet
	Detail             string
}

// DispatchTargetProfile describes one executor dispatch target boundary.
type DispatchTargetProfile struct {
	Target           DispatchTarget
	Handoff          ExecutionHandoffKind
	RuntimeOwned     bool
	RequiresExecutor bool
	Configurable     bool
	Terminal         bool
	Detail           string
}

// DispatchTargetSummary aggregates dispatch target boundary metadata.
type DispatchTargetSummary struct {
	TargetCount           int
	RuntimeOwnedCount     int
	RequiresExecutorCount int
	ConfigurableCount     int
	TerminalCount         int
}

// DefaultDispatchTargetProfiles returns dispatch target boundary metadata.
func DefaultDispatchTargetProfiles() []DispatchTargetProfile {
	return cloneDispatchTargetProfiles(defaultDispatchTargetProfiles)
}

// DefaultDispatchTargetSummary returns aggregate dispatch target metadata.
func DefaultDispatchTargetSummary() DispatchTargetSummary {
	return SummarizeDispatchTargetProfiles(DefaultDispatchTargetProfiles())
}

// SummarizeDispatchTargetProfiles aggregates dispatch target profiles.
func SummarizeDispatchTargetProfiles(profiles []DispatchTargetProfile) DispatchTargetSummary {
	summary := DispatchTargetSummary{TargetCount: len(profiles)}
	for _, profile := range profiles {
		if profile.RuntimeOwned {
			summary.RuntimeOwnedCount++
		}
		if profile.RequiresExecutor {
			summary.RequiresExecutorCount++
		}
		if profile.Configurable {
			summary.ConfigurableCount++
		}
		if profile.Terminal {
			summary.TerminalCount++
		}
	}
	return summary
}

// Preview returns a non-executing dispatch decision for one handoff.
func (d ExecutionDispatcher) Preview(handoff RoutedAuthorizedExecutionRequest) DispatchPreview {
	return d.preview(handoff.HandoffKind(), handoff.Supported(), handoff.Diagnostics())
}

// PreviewBatch returns a non-executing dispatch decision for one batch handoff.
func (d ExecutionDispatcher) PreviewBatch(handoff RoutedAuthorizedBatchExecutionRequest) DispatchPreview {
	return d.preview(handoff.HandoffKind(), handoff.Supported(), handoff.Diagnostics())
}

// PreviewProtocol returns a non-executing dispatch decision for one protocol-aware handoff.
func (d ExecutionDispatcher) PreviewProtocol(handoff ProtocolRoutedAuthorizedExecutionRequest) DispatchPreview {
	return d.preview(handoff.HandoffKind(), handoff.Supported(), handoff.Diagnostics())
}

// PreviewProtocolBatch returns a non-executing dispatch decision for one protocol-aware batch handoff.
func (d ExecutionDispatcher) PreviewProtocolBatch(handoff ProtocolRoutedAuthorizedBatchExecutionRequest) DispatchPreview {
	return d.preview(handoff.HandoffKind(), handoff.Supported(), handoff.Diagnostics())
}

func (d ExecutionDispatcher) preview(kind ExecutionHandoffKind, supported bool, diagnostics DiagnosticSet) DispatchPreview {
	preview := DispatchPreview{
		Handoff:     kind,
		Supported:   supported,
		Diagnostics: cloneDiagnosticSet(diagnostics),
	}
	switch kind {
	case ExecutionHandoffNative:
		preview.Target = DispatchTargetNative
		preview.ExecutorConfigured = d.Native != nil
		preview.Detail = "dispatch to native executor"
		if !preview.ExecutorConfigured {
			preview.Diagnostics = mergeDiagnosticSets(preview.Diagnostics, DiagnosticSet{
				missingExecutorDiagnostic("native executor is not configured"),
			})
			preview.Detail = "native executor is not configured"
		}
	case ExecutionHandoffLegacyFallback:
		preview.Target = DispatchTargetLegacy
		preview.ExecutorConfigured = d.Legacy != nil
		preview.Detail = "dispatch to legacy fallback executor"
		if !preview.ExecutorConfigured {
			preview.Diagnostics = mergeDiagnosticSets(preview.Diagnostics, DiagnosticSet{
				missingExecutorDiagnostic("legacy executor is not configured"),
			})
			preview.Detail = "legacy executor is not configured"
		}
	default:
		preview.Target = DispatchTargetNone
		preview.Detail = "handoff rejected before executor dispatch"
	}
	preview.WillDispatch = preview.Supported && preview.ExecutorConfigured && !preview.Diagnostics.BlocksNative()
	return preview
}

var defaultDispatchTargetProfiles = []DispatchTargetProfile{
	{
		Target:           DispatchTargetNative,
		Handoff:          ExecutionHandoffNative,
		RuntimeOwned:     true,
		RequiresExecutor: true,
		Configurable:     true,
		Detail:           "native executor boundary for accepted qsbridge-native execution requests",
	},
	{
		Target:           DispatchTargetLegacy,
		Handoff:          ExecutionHandoffLegacyFallback,
		RuntimeOwned:     true,
		RequiresExecutor: true,
		Configurable:     true,
		Detail:           "legacy compatibility executor boundary for fallback requests",
	},
	{
		Target:   DispatchTargetNone,
		Handoff:  ExecutionHandoffRejected,
		Terminal: true,
		Detail:   "terminal no-dispatch target for rejected handoffs",
	},
}

func cloneDispatchTargetProfiles(profiles []DispatchTargetProfile) []DispatchTargetProfile {
	return append([]DispatchTargetProfile(nil), profiles...)
}

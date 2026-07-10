package qsbridge

// ExecutionStatus describes the adapter-visible lifecycle of one execution request.
type ExecutionStatus string

const (
	// ExecutionPending means the request is valid but has not produced results yet.
	ExecutionPending ExecutionStatus = "pending"
	// ExecutionStreaming means the request has produced one or more non-final chunks.
	ExecutionStreaming ExecutionStatus = "streaming"
	// ExecutionComplete means the request completed normally.
	ExecutionComplete ExecutionStatus = "complete"
	// ExecutionFailed means execution or request validation produced blocking diagnostics.
	ExecutionFailed ExecutionStatus = "failed"
	// ExecutionCancelRequested means a protocol adapter requested cancellation.
	ExecutionCancelRequested ExecutionStatus = "cancel_requested"
	// ExecutionCanceled means the result envelope records a canceled execution.
	ExecutionCanceled ExecutionStatus = "canceled"
)

// CancellationReason describes why an adapter requested cancellation.
type CancellationReason string

const (
	// CancellationClientRequest means the client explicitly requested cancellation.
	CancellationClientRequest CancellationReason = "client_request"
	// CancellationTimeout means the adapter or executor exceeded a deadline.
	CancellationTimeout CancellationReason = "timeout"
	// CancellationShutdown means the process or session is shutting down.
	CancellationShutdown CancellationReason = "shutdown"
)

// CancellationProfile describes one cancellation mode an adapter can expose.
type CancellationProfile struct {
	Reason            CancellationReason
	RequiresRequestID bool
	RequiresRegistry  bool
	ClientInitiated   bool
	TimeoutDriven     bool
	ShutdownDriven    bool
	ForceAllowed      bool
	Detail            string
}

// CancellationProfileSummary describes aggregate cancellation capability metadata.
type CancellationProfileSummary struct {
	ProfileCount           int
	RequiresRequestIDCount int
	RequiresRegistryCount  int
	ClientInitiatedCount   int
	TimeoutDrivenCount     int
	ShutdownDrivenCount    int
	ForceAllowedCount      int
}

// CancellationRequest is a metadata-only request to cancel execution.
//
// qsbridge only validates and records the request. Future protocol or executor
// adapters own the actual interruption behavior.
type CancellationRequest struct {
	RequestID   ExecutionRequestID
	Reason      CancellationReason
	Message     string
	Force       bool
	Diagnostics DiagnosticSet
}

// Supported reports whether the cancellation request is well-formed.
func (r CancellationRequest) Supported() bool {
	return r.RequestID != "" && !r.Diagnostics.BlocksNative()
}

// CancelRequest creates a metadata-only cancellation request for this execution.
func (r ExecutionRequest) CancelRequest(reason CancellationReason, message string) CancellationRequest {
	return newCancellationRequest(r.Options, reason, message, false)
}

// CancelRequest creates a metadata-only cancellation request for this batch execution.
func (r BatchExecutionRequest) CancelRequest(reason CancellationReason, message string) CancellationRequest {
	return newCancellationRequest(r.Options, reason, message, false)
}

func newCancellationRequest(options ExecutionOptions, reason CancellationReason, message string, force bool) CancellationRequest {
	request := CancellationRequest{
		RequestID: options.RequestID,
		Reason:    reason,
		Message:   message,
		Force:     force,
	}
	if request.Reason == "" {
		request.Reason = CancellationClientRequest
	}
	if request.RequestID == "" {
		request.Diagnostics = append(request.Diagnostics, ErrorDiagnostic(
			DiagnosticInvalidExecutionOption,
			PhaseExecute,
			"cancellation requires an execution request id",
		))
	}
	if !options.Cancelable {
		request.Diagnostics = append(request.Diagnostics, ErrorDiagnostic(
			DiagnosticInvalidExecutionOption,
			PhaseExecute,
			"execution request is not marked cancelable",
		))
	}
	return request
}

// DefaultCancellationProfiles returns the canonical cancellation capability inventory.
func DefaultCancellationProfiles() []CancellationProfile {
	return []CancellationProfile{
		{
			Reason:            CancellationClientRequest,
			RequiresRequestID: true,
			RequiresRegistry:  true,
			ClientInitiated:   true,
			Detail:            "client requested cancellation of a registered in-flight request",
		},
		{
			Reason:            CancellationTimeout,
			RequiresRequestID: true,
			RequiresRegistry:  true,
			TimeoutDriven:     true,
			Detail:            "adapter or executor deadline exceeded for a registered request",
		},
		{
			Reason:            CancellationShutdown,
			RequiresRequestID: true,
			RequiresRegistry:  true,
			ShutdownDriven:    true,
			ForceAllowed:      true,
			Detail:            "session or process shutdown can force cancellation metadata for registered work",
		},
	}
}

// DefaultCancellationProfileSummary returns aggregate metadata for canonical cancellation profiles.
func DefaultCancellationProfileSummary() CancellationProfileSummary {
	return SummarizeCancellationProfiles(DefaultCancellationProfiles())
}

// SummarizeCancellationProfiles returns aggregate metadata for cancellation profiles.
func SummarizeCancellationProfiles(profiles []CancellationProfile) CancellationProfileSummary {
	summary := CancellationProfileSummary{ProfileCount: len(profiles)}
	for _, profile := range profiles {
		if profile.RequiresRequestID {
			summary.RequiresRequestIDCount++
		}
		if profile.RequiresRegistry {
			summary.RequiresRegistryCount++
		}
		if profile.ClientInitiated {
			summary.ClientInitiatedCount++
		}
		if profile.TimeoutDriven {
			summary.TimeoutDrivenCount++
		}
		if profile.ShutdownDriven {
			summary.ShutdownDrivenCount++
		}
		if profile.ForceAllowed {
			summary.ForceAllowedCount++
		}
	}
	return summary
}

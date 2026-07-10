package qsbridge

// NativeExecutor is the future native execution boundary.
//
// Implementations live outside qsbridge. The interface exists so protocol
// adapters and tests can depend on a stable request/result contract without
// importing bitmap, BSI, storage, or legacy runtime packages.
type NativeExecutor interface {
	ExecuteNative(request ExecutionRequest) ExecutionResult
	ExecuteNativeBatch(request BatchExecutionRequest) BatchExecutionResult
}

// LegacyExecutor is the compatibility execution boundary.
//
// Implementations own calls into the existing runtime. qsbridge only carries
// the metadata needed to choose and describe fallback.
type LegacyExecutor interface {
	ExecuteLegacy(request FallbackRequest) ExecutionResult
	ExecuteLegacyBatch(request BatchFallbackRequest) BatchExecutionResult
}

// ExecutionDispatcher groups optional native and legacy executor boundaries.
type ExecutionDispatcher struct {
	Native NativeExecutor
	Legacy LegacyExecutor
}

// Dispatch executes the final single-request handoff through the selected boundary.
func (d ExecutionDispatcher) Dispatch(handoff RoutedAuthorizedExecutionRequest) ExecutionResult {
	switch handoff.HandoffKind() {
	case ExecutionHandoffNative:
		request, ok := handoff.NativeRequest()
		if !ok || d.Native == nil {
			return handoff.Request.EmptyResult().WithDispatchDiagnostic(missingExecutorDiagnostic("native executor is not configured"))
		}
		return d.Native.ExecuteNative(request)
	case ExecutionHandoffLegacyFallback:
		request, ok := handoff.LegacyFallbackRequest()
		if !ok || d.Legacy == nil {
			return handoff.Request.EmptyResult().WithDispatchDiagnostic(missingExecutorDiagnostic("legacy executor is not configured"))
		}
		return d.Legacy.ExecuteLegacy(request)
	default:
		return handoff.Request.EmptyResult().WithDispatchDiagnostics(handoff.Diagnostics())
	}
}

// DispatchBatch executes the final batch handoff through the selected boundary.
func (d ExecutionDispatcher) DispatchBatch(handoff RoutedAuthorizedBatchExecutionRequest) BatchExecutionResult {
	switch handoff.HandoffKind() {
	case ExecutionHandoffNative:
		request, ok := handoff.NativeRequest()
		if !ok || d.Native == nil {
			return handoff.Request.EmptyResult().WithDispatchDiagnostic(missingExecutorDiagnostic("native executor is not configured"))
		}
		return d.Native.ExecuteNativeBatch(request)
	case ExecutionHandoffLegacyFallback:
		request, ok := handoff.LegacyFallbackRequest()
		if !ok || d.Legacy == nil {
			return handoff.Request.EmptyResult().WithDispatchDiagnostic(missingExecutorDiagnostic("legacy executor is not configured"))
		}
		return d.Legacy.ExecuteLegacyBatch(request)
	default:
		return handoff.Request.EmptyResult().WithDispatchDiagnostics(handoff.Diagnostics())
	}
}

// WithDispatchDiagnostic returns a copy of result failed with one dispatch diagnostic.
func (r ExecutionResult) WithDispatchDiagnostic(diagnostic Diagnostic) ExecutionResult {
	return r.WithDispatchDiagnostics(DiagnosticSet{diagnostic})
}

// WithDispatchDiagnostics returns a copy of result failed with dispatch diagnostics.
func (r ExecutionResult) WithDispatchDiagnostics(diagnostics DiagnosticSet) ExecutionResult {
	r.Diagnostics = mergeDiagnosticSets(r.Diagnostics, diagnostics)
	if r.Diagnostics.BlocksNative() {
		r.Status = ExecutionFailed
		r.Complete = false
	}
	return r
}

// WithDispatchDiagnostic returns a copy of batch result failed with one dispatch diagnostic.
func (r BatchExecutionResult) WithDispatchDiagnostic(diagnostic Diagnostic) BatchExecutionResult {
	return r.WithDispatchDiagnostics(DiagnosticSet{diagnostic})
}

// WithDispatchDiagnostics returns a copy of batch result failed with dispatch diagnostics.
func (r BatchExecutionResult) WithDispatchDiagnostics(diagnostics DiagnosticSet) BatchExecutionResult {
	r.Diagnostics = mergeDiagnosticSets(r.Diagnostics, diagnostics)
	if r.Diagnostics.BlocksNative() {
		r.Status = ExecutionFailed
		r.Complete = false
	}
	return r
}

func missingExecutorDiagnostic(message string) Diagnostic {
	return ErrorDiagnostic(DiagnosticInternalInvariant, PhaseExecute, message)
}

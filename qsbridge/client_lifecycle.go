package qsbridge

// ClientCancellationExchange is the metadata response for canceling an execution request.
type ClientCancellationExchange struct {
	Connection   ConnectionContext
	Record       ExecutionRecord
	Cancellation CancellationRequest
	Result       ExecutionResult
	BatchResult  BatchExecutionResult
	Diagnostics  DiagnosticSet
}

// CancelClientExecutionRequest records adapter-owned cancellation metadata.
func (s PlanningService) CancelClientExecutionRequest(connection ConnectionContext, registry ExecutionRegistry, id ExecutionRequestID, reason CancellationReason, message string) ClientCancellationExchange {
	_ = s
	exchange := ClientCancellationExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = errorClientExecutionResult(id, exchange.Diagnostics)
		return exchange
	}
	if registry == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "execution registry is not configured"),
		})
		exchange.Result = errorClientExecutionResult(id, exchange.Diagnostics)
		return exchange
	}
	if id == "" {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "cancellation requires an execution request id"),
		})
		exchange.Result = errorClientExecutionResult(id, exchange.Diagnostics)
		return exchange
	}
	record, ok := registry.Get(id)
	if ok {
		exchange.Record = cloneExecutionRecord(record)
	}
	cancel := registry.Cancel(id, reason, message)
	exchange.Cancellation = cloneCancellationRequest(cancel)
	exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, cancel.Diagnostics)
	if !ok {
		exchange.Result = errorClientExecutionResult(id, exchange.Diagnostics)
		return exchange
	}
	switch record.Kind {
	case ExecutionRequestBatch:
		exchange.BatchResult = record.Batch.EmptyResult().WithCancellation(cancel)
	default:
		exchange.Result = record.Request.EmptyResult().WithCancellation(cancel)
	}
	return exchange
}

// Supported reports whether cancellation metadata can proceed.
func (e ClientCancellationExchange) Supported() bool {
	return e.Connection.Supported() && e.Cancellation.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts cancellation diagnostics into protocol-facing errors.
func (e ClientCancellationExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking cancellation error, if any.
func (e ClientCancellationExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func errorClientExecutionResult(id ExecutionRequestID, diagnostics DiagnosticSet) ExecutionResult {
	return ExecutionResult{
		RequestID:   id,
		Status:      ExecutionFailed,
		Diagnostics: cloneDiagnosticSet(diagnostics),
		Complete:    true,
	}
}

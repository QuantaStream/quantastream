package qsbridge

// PlanOnlyNativeExecutor completes native execution envelopes without reading storage.
//
// It is a scaffold executor for exercising the native request/result contract
// end to end while bitmap, BSI, and storage-backed row production remain
// outside qsbridge.
type PlanOnlyNativeExecutor struct{}

// ExecuteNative returns the metadata-only result shape for one native request.
func (PlanOnlyNativeExecutor) ExecuteNative(request ExecutionRequest) ExecutionResult {
	return completePlanOnlyExecutionResult(request.EmptyResult(), request.SupportedForExecution())
}

// ExecuteNativeBatch returns one metadata-only item result per batch parameter set.
func (PlanOnlyNativeExecutor) ExecuteNativeBatch(request BatchExecutionRequest) BatchExecutionResult {
	result := request.EmptyResult()
	if !request.SupportedForExecution() {
		return result.WithComplete()
	}
	for _, parameters := range request.ParameterSets {
		item := planOnlyBatchItemRequest(request, parameters)
		result = result.WithItem(completePlanOnlyExecutionResult(item.EmptyResult(), item.SupportedForExecution()))
	}
	return result.WithComplete()
}

func planOnlyBatchItemRequest(request BatchExecutionRequest, parameters ParameterBindingSet) ExecutionRequest {
	diagnostics := mergeDiagnosticSets(request.Prepared.Diagnostics, parameters.Diagnostics)
	supported := request.Prepared.Supported && !diagnostics.BlocksNative()
	return ExecutionRequest{
		Bound: BoundPlan{
			Prepared:    clonePreparedPlan(request.Prepared),
			Parameters:  cloneParameterBindingSet(parameters),
			Diagnostics: diagnostics,
			Supported:   supported,
		},
		Options:        request.Options,
		Diagnostics:    diagnostics,
		Supported:      supported,
		Result:         request.Result,
		ResultColumns:  append([]ResultColumn(nil), request.ResultColumns...),
		Statement:      cloneStatementResult(request.Statement),
		SessionActions: cloneSessionActions(request.SessionActions),
		Access:         cloneAccessRequirements(request.Access),
	}
}

func completePlanOnlyExecutionResult(result ExecutionResult, supported bool) ExecutionResult {
	if !supported || result.Diagnostics.BlocksNative() {
		if result.Status == "" {
			result.Status = ExecutionFailed
		}
		return result
	}
	switch result.Kind {
	case ResultQuery:
		return result.WithChunk(ResultChunk{Final: true})
	case ResultStatement:
		result.Status = ExecutionComplete
		result.Complete = true
		return result
	default:
		if len(result.Columns) > 0 {
			result.Kind = ResultQuery
			return result.WithChunk(ResultChunk{Final: true})
		}
		result.Status = ExecutionComplete
		result.Complete = true
		return result
	}
}

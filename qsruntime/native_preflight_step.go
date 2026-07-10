package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func (r SQLRuntime) executeScalarNativeSubqueryStep(ctx context.Context, request PreflightHelperExecutionRequest) (PreflightHelperExecutionResult, error) {
	return r.executeNativePreflightStep(ctx, request)
}

func (r SQLRuntime) executeParentKeyNativeSubqueryStep(ctx context.Context, request PreflightHelperExecutionRequest) (PreflightHelperExecutionResult, error) {
	return r.executeNativePreflightStep(ctx, request)
}

func (r SQLRuntime) executeAggregateThresholdNativeSubqueryStep(ctx context.Context, request PreflightHelperExecutionRequest) (PreflightHelperExecutionResult, error) {
	return r.executeNativePreflightStep(ctx, request)
}

func (r SQLRuntime) executeNativePreflightStep(ctx context.Context, request PreflightHelperExecutionRequest) (PreflightHelperExecutionResult, error) {
	if diagnostics := request.ValidatePayload(); diagnostics.BlocksNative() {
		return PreflightHelperExecutionResult{
			Plan:        request.Plan,
			SQL:         request.SQL,
			Payload:     request.Payload,
			Diagnostics: diagnostics,
		}, nil
	}
	if request.Plan.NativeStep == nil {
		return r.executePreflightHelper(ctx, request)
	}
	executor := r.nativeSubqueryStepExecutor(request)
	nativeResult, err := executor.ExecuteNativeSubqueryStep(ctx, qsbridge.NativeSubqueryStepExecutionRequest{
		Step:       *request.Plan.NativeStep,
		Parameters: append([]qsbridge.ParameterValue(nil), request.Values...),
	})
	diagnostics := normalizePreflightHelperDiagnostics(request.Plan.Kind, nativeResult.Diagnostics)
	payload := request.Payload
	if payload.Scalar != nil && !diagnostics.BlocksNative() {
		outputName := payload.Scalar.OutputName
		if outputName == "" && len(request.Plan.NativeStep.Outputs) > 0 {
			outputName = request.Plan.NativeStep.Outputs[0]
		}
		cell, ok := nativeResult.Outputs[outputName]
		if !ok {
			diagnostics = append(diagnostics, helperExecutionDiagnostic(request.Plan.Kind, "native scalar subquery step did not produce output "+outputName)...)
		} else {
			payload.Scalar.Materialized = cell
		}
	}
	trace := nativeResult.Trace()
	return PreflightHelperExecutionResult{
		Plan:    request.Plan,
		SQL:     request.SQL,
		Payload: payload,
		Result: SQLExecutionResult{Runtime: ExecutionResult{
			RowSet:      nativeResult.RowSet,
			Diagnostics: nativeResult.Diagnostics,
		}},
		NativeTrace: &trace,
		Diagnostics: diagnostics,
	}, err
}

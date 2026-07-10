package qsruntime

import (
	"context"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// nativeScalarPreflightAdapter executes native subquery steps by
// delegating to the existing SQL-backed preflight helper implementation.
//
// This is intentionally an adapter, not the final bitmap-native executor. It
// proves the native step contract can drive scalar subquery materialization
// without changing externally visible behavior.
type nativeScalarPreflightAdapter struct {
	Runtime SQLRuntime
	Helper  PreflightHelperExecutor
	Request PreflightHelperExecutionRequest
}

// ExecuteNativeSubqueryStep runs a native preflight step through the
// current preflight helper boundary and returns native-step outputs.
func (a nativeScalarPreflightAdapter) ExecuteNativeSubqueryStep(ctx context.Context, request qsbridge.NativeSubqueryStepExecutionRequest) (qsbridge.NativeSubqueryStepExecutionResult, error) {
	if !nativePreflightStepKindMatchesPlan(request.Step.Kind, a.Request.Plan.Kind) {
		return qsbridge.NativeSubqueryStepExecutionResult{
			Step: request.Step,
			Diagnostics: helperExecutionDiagnostic(
				a.Request.Plan.Kind,
				"native scalar preflight adapter received unsupported step kind "+string(request.Step.Kind),
			),
		}, nil
	}
	helperRequest := a.Request
	if request.Step.Kind == qsbridge.NativeSubqueryStepScalarMaterialization && a.Runtime.Environment.Ready() {
		if result, err, ok := a.executeScalarMaterializationNativeStep(ctx, request.Step); ok {
			return result, err
		}
	}
	if request.Step.Kind == qsbridge.NativeSubqueryStepParentKeyLookup && a.Runtime.Environment.Ready() {
		if result, err, ok := a.executeParentKeyLookupNativeStep(ctx, request.Step); ok {
			return result, err
		}
	}
	if request.Step.Kind == qsbridge.NativeSubqueryStepAggregateThresholdLookup && a.Runtime.Environment.Ready() {
		if result, err, ok := a.executeAggregateThresholdLookupNativeStep(ctx, request.Step); ok {
			return result, err
		}
	}
	helper := a.Helper
	if helper == nil {
		helper = sqlBackedPreflightHelperExecutor{}
	}
	result, err := helper.ExecutePreflightHelper(ctx, a.Runtime, helperRequest)
	diagnostics := normalizePreflightHelperDiagnostics(helperRequest.Plan.Kind, result.Diagnostics)
	outputs := make(map[string]qsbridge.ResultCell)
	rowSet := result.Result.Runtime.RowSet
	if request.Step.Kind == qsbridge.NativeSubqueryStepScalarMaterialization && err == nil && !diagnostics.BlocksNative() {
		cell, cellDiagnostics := scalarSubqueryResultCell(result.Result.Runtime.RowSet)
		diagnostics = append(diagnostics, cellDiagnostics...)
		if !diagnostics.BlocksNative() {
			outputName := request.Step.Name
			if len(request.Step.Outputs) > 0 {
				outputName = request.Step.Outputs[0]
			}
			outputs[outputName] = cell
		}
	}
	step := request.Step
	if step.ExecutionMode == "" {
		step.ExecutionMode = "sql_backed_until_bitmap_native_executor_exists"
	}
	return qsbridge.NativeSubqueryStepExecutionResult{
		Step:        step,
		Outputs:     outputs,
		RowSet:      rowSet,
		Diagnostics: diagnostics,
	}, err
}

func (a nativeScalarPreflightAdapter) executeScalarMaterializationNativeStep(ctx context.Context, step qsbridge.NativeSubqueryStep) (qsbridge.NativeSubqueryStepExecutionResult, error, bool) {
	payload := a.Request.Payload.Scalar
	if payload == nil {
		return qsbridge.NativeSubqueryStepExecutionResult{
			Step:        step,
			Diagnostics: helperExecutionDiagnostic(a.Request.Plan.Kind, "scalar native step has no scalar payload"),
		}, nil, true
	}
	runtimeRequest, diagnostics, ok := a.scalarMaterializationExecutionRequest()
	if !ok || diagnostics.BlocksNative() {
		return qsbridge.NativeSubqueryStepExecutionResult{}, nil, false
	}
	result, err := a.Runtime.ExecutePrepared(ctx, runtimeRequest)
	diagnostics = append(diagnostics, normalizePreflightHelperDiagnostics(a.Request.Plan.Kind, result.Diagnostics)...)
	outputs := make(map[string]qsbridge.ResultCell)
	if err == nil && !diagnostics.BlocksNative() {
		cell, cellDiagnostics := scalarSubqueryResultCell(result.RowSet)
		diagnostics = append(diagnostics, cellDiagnostics...)
		if !diagnostics.BlocksNative() {
			outputName := payload.OutputName
			if outputName == "" && len(step.Outputs) > 0 {
				outputName = step.Outputs[0]
			}
			outputs[outputName] = cell
		}
	}
	if step.ExecutionMode == "" || step.ExecutionMode == "sql_backed_until_bitmap_native_executor_exists" {
		step.ExecutionMode = "native_runtime_scalar_materialization"
	}
	return qsbridge.NativeSubqueryStepExecutionResult{
		Step:        step,
		Outputs:     outputs,
		RowSet:      result.RowSet,
		Diagnostics: diagnostics,
	}, err, true
}

func (a nativeScalarPreflightAdapter) scalarMaterializationExecutionRequest() (ExecutionRequest, qsbridge.DiagnosticSet, bool) {
	service := qsbridge.NewPlanningService(a.Runtime.Planner(), nil)
	prepared, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{SQL: a.Request.SQL}, a.Request.Options, a.Request.Values...)
	diagnostics := append(qsbridge.DiagnosticSet(nil), request.Diagnostics...)
	if diagnostics.BlocksNative() || prepared.Kind != qsbridge.QueryKindSelect {
		return ExecutionRequest{}, diagnostics, false
	}
	intermediate, lowerDiagnostics := a.Runtime.Lowerer.LowerExecutionRequest(request)
	diagnostics = append(diagnostics, lowerDiagnostics...)
	if diagnostics.BlocksNative() {
		return ExecutionRequest{}, diagnostics, false
	}
	return NewSQLExecutionRequest(intermediate, request), diagnostics, true
}

func (a nativeScalarPreflightAdapter) executeParentKeyLookupNativeStep(ctx context.Context, step qsbridge.NativeSubqueryStep) (qsbridge.NativeSubqueryStepExecutionResult, error, bool) {
	payload := a.Request.Payload.ParentKeyLookup
	if payload == nil {
		return qsbridge.NativeSubqueryStepExecutionResult{
			Step:        step,
			Diagnostics: helperExecutionDiagnostic(a.Request.Plan.Kind, "parent-key native step has no parent-key payload"),
		}, nil, true
	}
	runtimeRequest, diagnostics, ok := a.parentKeyLookupExecutionRequest()
	if !ok || diagnostics.BlocksNative() {
		return qsbridge.NativeSubqueryStepExecutionResult{}, nil, false
	}
	result, err := a.Runtime.ExecutePrepared(ctx, runtimeRequest)
	diagnostics = append(diagnostics, normalizePreflightHelperDiagnostics(a.Request.Plan.Kind, result.Diagnostics)...)
	if step.ExecutionMode == "" || step.ExecutionMode == "sql_backed_until_bitmap_native_executor_exists" {
		step.ExecutionMode = "native_runtime_parent_key_lookup"
	}
	return qsbridge.NativeSubqueryStepExecutionResult{
		Step:        step,
		RowSet:      result.RowSet,
		Diagnostics: diagnostics,
	}, err, true
}

func (a nativeScalarPreflightAdapter) parentKeyLookupExecutionRequest() (ExecutionRequest, qsbridge.DiagnosticSet, bool) {
	service := qsbridge.NewPlanningService(a.Runtime.Planner(), nil)
	prepared, request := service.PrepareExecutionRequest(qsbridge.PlanRequest{SQL: a.Request.SQL}, a.Request.Options, a.Request.Values...)
	diagnostics := append(qsbridge.DiagnosticSet(nil), request.Diagnostics...)
	if diagnostics.BlocksNative() || prepared.Kind != qsbridge.QueryKindSelect {
		return ExecutionRequest{}, diagnostics, false
	}
	intermediate, lowerDiagnostics := a.Runtime.Lowerer.LowerExecutionRequest(request)
	diagnostics = append(diagnostics, lowerDiagnostics...)
	if diagnostics.BlocksNative() {
		return ExecutionRequest{}, diagnostics, false
	}
	return NewSQLExecutionRequest(intermediate, request), diagnostics, true
}

func (a nativeScalarPreflightAdapter) executeAggregateThresholdLookupNativeStep(ctx context.Context, step qsbridge.NativeSubqueryStep) (qsbridge.NativeSubqueryStepExecutionResult, error, bool) {
	payload := a.Request.Payload.AggregateThresholdLookup
	if payload == nil {
		return qsbridge.NativeSubqueryStepExecutionResult{
			Step:        step,
			Diagnostics: helperExecutionDiagnostic(a.Request.Plan.Kind, "aggregate-threshold native step has no aggregate-threshold payload"),
		}, nil, true
	}
	runtimeRequest, ok := aggregateThresholdLookupExecutionRequest(payload, a.Request.Options)
	if !ok {
		return qsbridge.NativeSubqueryStepExecutionResult{}, nil, false
	}
	result, err := a.Runtime.ExecutePrepared(ctx, runtimeRequest)
	diagnostics := normalizePreflightHelperDiagnostics(a.Request.Plan.Kind, result.Diagnostics)
	if step.ExecutionMode == "" || step.ExecutionMode == "sql_backed_until_bitmap_native_executor_exists" {
		step.ExecutionMode = "native_runtime_aggregate_threshold_lookup"
	}
	return qsbridge.NativeSubqueryStepExecutionResult{
		Step:        step,
		RowSet:      result.RowSet,
		Diagnostics: diagnostics,
	}, err, true
}

func aggregateThresholdLookupExecutionRequest(payload *PreflightAggregateThresholdLookupPayload, options qsbridge.ExecutionOptions) (ExecutionRequest, bool) {
	if payload == nil || len(payload.PartKeys) == 0 || len(payload.ParentRownums) == 0 || len(payload.PartKeys) != len(payload.ParentRownums) {
		return ExecutionRequest{}, false
	}
	if payload.Table != "lineitem" || payload.KeyField != "l_partkey" || payload.ValueField != "l_quantity" {
		return ExecutionRequest{}, false
	}
	parent := qsbridge.TableInstance{Table: "part", Alias: "p"}
	child := qsbridge.TableInstance{Table: payload.Table, Alias: "l"}
	childRole := qsbridge.TableInstanceID(child.Alias)
	parentKeyRef := qsbridge.FieldRef{Table: parent, Name: "p_partkey", PhysicalName: "p_partkey", Type: qsbridge.DataTypeInt}
	childKeyRef := qsbridge.FieldRef{Table: child, Name: payload.KeyField, PhysicalName: payload.KeyField, Type: qsbridge.DataTypeInt}
	valueRef := qsbridge.FieldRef{Table: child, Name: payload.ValueField, PhysicalName: payload.ValueField, Type: qsbridge.DataTypeInt}
	parentRownums := append([]qsbridge.QuantaRownum(nil), payload.ParentRownums...)
	aggregateAlias := payload.AggregateFunction + "_" + payload.ValueField
	if payload.ValueOutput != "" {
		aggregateAlias = payload.ValueOutput
	}
	request := ExecutionRequest{
		Query: qsbridge.QuantaIntermediateQuery{
			ProjectionFields: []qsbridge.QuantaProjectionField{
				{Index: child.Table, Role: childRole, Field: payload.KeyField, PhysicalName: payload.KeyField, Type: qsbridge.DataTypeInt, Visible: true},
				{Index: child.Table, Role: childRole, Field: payload.ValueField, PhysicalName: payload.ValueField, Type: qsbridge.DataTypeInt, Visible: false},
			},
		},
		SourceIndexes: []string{parent.Table, child.Table},
		Sources:       []qsbridge.TableInstance{parent, child},
		Joins: []qsbridge.JoinEdge{{
			Left:     parentKeyRef,
			Right:    childKeyRef,
			Kind:     qsbridge.JoinKindInner,
			Encoding: qsbridge.RelationshipEncodingProfile{Kind: qsbridge.RelationshipEncodingVector},
			Legal:    true,
		}},
		Projection: []qsbridge.ProjectionColumn{
			{Expr: qsbridge.Field(childKeyRef), Alias: payload.KeyOutput, Type: qsbridge.DataTypeInt},
			{Expr: qsbridge.AggregateRef(aggregateAlias, 0), Alias: aggregateAlias, Type: qsbridge.DataTypeFloat},
		},
		GroupBy:         []qsbridge.Expr{qsbridge.Field(childKeyRef)},
		ProjectionOrder: []qsbridge.FieldRef{childKeyRef, valueRef},
		Result:          qsbridge.ResultShape{Columns: []qsbridge.FieldRef{childKeyRef, valueRef}},
		Options:         options,
		SQLAggregates: []qsbridge.Aggregate{{
			Function:      payload.AggregateFunction,
			Input:         qsbridge.Field(valueRef),
			Alias:         aggregateAlias,
			Type:          qsbridge.DataTypeFloat,
			Deterministic: true,
		}},
		Route: DirectQIABRoute(),
	}
	return request.WithCandidateSet(qsbridge.QuantaCandidateSet{Index: parent.Table, Rownums: parentRownums}), true
}

func nativePreflightStepKindMatchesPlan(stepKind qsbridge.NativeSubqueryStepKind, planKind PreflightRewriteHelperPlanKind) bool {
	switch planKind {
	case PreflightHelperPlanScalarSubquery:
		return stepKind == qsbridge.NativeSubqueryStepScalarMaterialization
	case PreflightHelperPlanParentKeyLookup:
		return stepKind == qsbridge.NativeSubqueryStepParentKeyLookup
	case PreflightHelperPlanAggregateThresholdLookup:
		return stepKind == qsbridge.NativeSubqueryStepAggregateThresholdLookup
	default:
		return false
	}
}

func (r SQLRuntime) nativeSubqueryStepExecutor(request PreflightHelperExecutionRequest) qsbridge.NativeSubqueryStepExecutor {
	if r.NativeSubquerySteps != nil {
		return r.NativeSubquerySteps
	}
	return nativeScalarPreflightAdapter{
		Runtime: r,
		Helper:  r.PreflightHelpers,
		Request: request,
	}
}

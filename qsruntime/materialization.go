package qsruntime

import (
	"context"
	"strconv"
	"strings"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// ProjectionMaterializer is qsbridge's native projection/materialization contract.
type ProjectionMaterializer = qsbridge.ProjectionMaterializer

// ProjectionMaterializerWithProbes is qsbridge's optional materialization instrumentation contract.
type ProjectionMaterializerWithProbes = qsbridge.ProjectionMaterializerWithProbes

// ProjectionMaterializerFunc adapts a function to ProjectionMaterializer.
type ProjectionMaterializerFunc = qsbridge.ProjectionMaterializerFunc

// ProjectionMaterializationKernelRequest aliases qsbridge's grouped materialization request.
type ProjectionMaterializationKernelRequest = qsbridge.ProjectionMaterializationKernelRequest

// ProjectionMaterializationResult aliases one qsbridge materialized batch response.
type ProjectionMaterializationResult = qsbridge.ProjectionMaterializationResult

// ProjectionMaterializationKernelResult aliases qsbridge's grouped materialization result.
type ProjectionMaterializationKernelResult = qsbridge.ProjectionMaterializationKernelResult

// ProjectionMaterializationKernel aliases qsbridge's native materialization-kernel boundary.
type ProjectionMaterializationKernel = qsbridge.ProjectionMaterializationKernel

// CandidateSetFromBitmapResult aliases qsbridge's bitmap-result candidate builder during the package split.
var CandidateSetFromBitmapResult = qsbridge.CandidateSetFromBitmapResult

// ProjectionMaterializationKernelAdapter delegates grouped materialization to a configured kernel.
type ProjectionMaterializationKernelAdapter struct {
	Kernel ProjectionMaterializationKernel
}

// MaterializeProjectionBatches delegates to the configured kernel or the unsupported boundary.
func (a ProjectionMaterializationKernelAdapter) MaterializeProjectionBatches(ctx context.Context, request ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error) {
	kernel := a.Kernel
	if kernel == nil {
		kernel = UnsupportedProjectionMaterializationKernel{}
	}
	return kernel.MaterializeProjectionBatches(ctx, request)
}

// ExecuteProjectionMaterializationKernel dispatches one grouped materialization request.
func ExecuteProjectionMaterializationKernel(ctx context.Context, kernel ProjectionMaterializationKernel, request ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error) {
	return ProjectionMaterializationKernelAdapter{Kernel: kernel}.MaterializeProjectionBatches(ctx, request)
}

// ProjectionMaterializerKernelAdapter adapts the older one-request materializer contract to the grouped kernel contract.
type ProjectionMaterializerKernelAdapter struct {
	Materializer ProjectionMaterializer
}

// MaterializeProjectionBatches executes each grouped request through the configured materializer.
func (a ProjectionMaterializerKernelAdapter) MaterializeProjectionBatches(ctx context.Context, request ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error) {
	if a.Materializer == nil {
		return UnsupportedProjectionMaterializationKernel{}.MaterializeProjectionBatches(ctx, request)
	}
	result := ProjectionMaterializationKernelResult{
		ID: request.ID,
		Probes: []qsbridge.ProjectionProbe{{
			Section: "projection_materialization",
			Name:    request.ProbePrefix + "request_count",
			Value:   strconv.Itoa(request.RequestCount()),
		}},
	}
	for _, materializationRequest := range request.Requests {
		rowSet, diagnostics, probes, err := materializeWithProbes(ctx, a.Materializer, materializationRequest)
		item := ProjectionMaterializationResult{
			ID:          materializationRequest.DependencyID,
			Request:     materializationRequest,
			RowSet:      rowSet,
			Probes:      probes,
			Diagnostics: diagnostics,
		}
		result.Results = append(result.Results, item)
		result.Diagnostics = append(result.Diagnostics, diagnostics...)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func materializeWithProbes(ctx context.Context, materializer ProjectionMaterializer, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, []ExecutionProbe, error) {
	if instrumented, ok := materializer.(ProjectionMaterializerWithProbes); ok {
		return instrumented.MaterializeWithProbes(ctx, request)
	}
	rowSet, diagnostics, err := materializer.Materialize(ctx, request)
	return rowSet, diagnostics, nil, err
}

// UnsupportedProjectionMaterializationKernel preserves the current explicit materialization boundary.
type UnsupportedProjectionMaterializationKernel struct{}

// MaterializeProjectionBatches reports that native materialization is not wired yet.
func (UnsupportedProjectionMaterializationKernel) MaterializeProjectionBatches(_ context.Context, request ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error) {
	return ProjectionMaterializationKernelResult{
		ID:          request.ID,
		Diagnostics: unsupportedProjectionMaterializationDiagnostics(request),
	}, nil
}

func unsupportedProjectionMaterializationDiagnostics(request ProjectionMaterializationKernelRequest) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			"projection materialization kernel is not wired yet: requests="+strconv.Itoa(len(request.Requests)),
		),
	}
}

// MaterializationRequestFromExecution builds a materialization request from result candidates.
func MaterializationRequestFromExecution(request ExecutionRequest, result BitmapQueryResult) (qsbridge.QuantaMaterializationRequest, qsbridge.DiagnosticSet) {
	if request.Materialization.ProjectionCount() > 0 || request.Materialization.CandidateCount() > 0 {
		materialization := request.Materialization
		if len(materialization.Rownums) == 0 {
			materialization.Rownums = append([]qsbridge.QuantaRownum(nil), result.Rownums...)
		}
		materialization.ProjectionFields = appendNativePredicateProjectionFields(materialization.ProjectionFields, request.NativePredicates)
		if rootIndex, ok := request.RootIndex(); ok {
			index := materialization.Index
			if index == "" {
				index = rootIndex
			}
			if strings.EqualFold(index, rootIndex) {
				materialization.ProjectionFields = materializationRootProjectionFields(rootIndex, materialization.ProjectionFields)
			}
		}
		return materialization, nil
	}
	rootIndex, ok := request.RootIndex()
	if !ok {
		return qsbridge.QuantaMaterializationRequest{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInvalidExecutionOption,
				qsbridge.PhaseExecute,
				"cannot materialize projection without a root index",
			),
		}
	}
	candidates := qsbridge.CandidateSetFromBitmapResult(rootIndex, result)
	fields := appendNativePredicateProjectionFields(request.Query.ProjectionFields, request.NativePredicates)
	return candidates.MaterializationRequest(materializationRootProjectionFields(rootIndex, fields)), nil
}

type physicalGroupProjectionExpression struct {
	Expression qsbridge.QuantaProjectionExpression
	SourceKey  string
	OutputKey  string
}

func projectionMaterializationKernelSupportsExpressions(kernel ProjectionMaterializationKernel) bool {
	switch typed := kernel.(type) {
	case nil:
		return false
	case NativeProjectionMaterializationKernel:
		return nativeProjectionReaderSupportsExpressions(typed.Reader)
	case *NativeProjectionMaterializationKernel:
		return typed != nil && nativeProjectionReaderSupportsExpressions(typed.Reader)
	case FallbackProjectionMaterializationKernel:
		return projectionMaterializationKernelSupportsExpressions(typed.Preferred)
	case *FallbackProjectionMaterializationKernel:
		return typed != nil && projectionMaterializationKernelSupportsExpressions(typed.Preferred)
	case ProjectionMaterializationKernelAdapter:
		return projectionMaterializationKernelSupportsExpressions(typed.Kernel)
	case *ProjectionMaterializationKernelAdapter:
		return typed != nil && projectionMaterializationKernelSupportsExpressions(typed.Kernel)
	default:
		return false
	}
}

func nativeProjectionReaderSupportsExpressions(reader NativeProjectionFieldReader) bool {
	_, ok := reader.(NativeProjectionExpressionReader)
	return ok
}

func materializationRequestWithPhysicalGroupExpressions(request ExecutionRequest, materialization qsbridge.QuantaMaterializationRequest) qsbridge.QuantaMaterializationRequest {
	if len(request.GroupBy) == 0 {
		return materialization
	}
	rootIndex := materialization.Index
	if rootIndex == "" {
		rootIndex, _ = request.RootIndex()
	}
	expressions := make([]physicalGroupProjectionExpression, 0, len(request.GroupBy))
	seenOutputs := make(map[string]struct{})
	for _, expr := range request.GroupBy {
		groupExpression, ok := physicalGroupProjectionExpressionForExpr(rootIndex, expr)
		if !ok {
			continue
		}
		if _, seen := seenOutputs[groupExpression.OutputKey]; seen {
			continue
		}
		seenOutputs[groupExpression.OutputKey] = struct{}{}
		expressions = append(expressions, groupExpression)
	}
	if len(expressions) == 0 {
		return materialization
	}
	requiredSourceKeys := materializationRequiredSourceKeysOutsidePhysicalGroupExpressions(request, rootIndex, expressions)
	filteredFields := make([]qsbridge.QuantaProjectionField, 0, len(materialization.ProjectionFields))
	for _, field := range materialization.ProjectionFields {
		key := materializationProjectionFieldStorageKey(field)
		if _, derived := physicalGroupProjectionExpressionSourceKeys(expressions)[key]; derived {
			if _, required := requiredSourceKeys[key]; !required {
				continue
			}
		}
		filteredFields = append(filteredFields, field)
	}
	materialization.ProjectionFields = filteredFields
	for _, expression := range expressions {
		if materializationProjectionExpressionExists(materialization.ProjectionExpressions, expression.OutputKey) {
			continue
		}
		materialization.ProjectionExpressions = append(materialization.ProjectionExpressions, expression.Expression)
	}
	return materialization
}

func physicalGroupProjectionExpressionSourceKeys(expressions []physicalGroupProjectionExpression) map[string]struct{} {
	keys := make(map[string]struct{}, len(expressions))
	for _, expression := range expressions {
		keys[expression.SourceKey] = struct{}{}
	}
	return keys
}

func materializationProjectionExpressionExists(expressions []qsbridge.QuantaProjectionExpression, outputKey string) bool {
	for _, expression := range expressions {
		if materializationProjectionFieldStorageKey(expression.Output) == outputKey {
			return true
		}
	}
	return false
}

func physicalGroupProjectionExpressionForExpr(defaultIndex string, expr qsbridge.Expr) (physicalGroupProjectionExpression, bool) {
	call, ok := directBitmapCallExpr(expr)
	if !ok || !physicalGroupProjectionFunctionIsYear(call.Name) || len(call.Args) != 1 {
		return physicalGroupProjectionExpression{}, false
	}
	field, ok := directBitmapExprField(call.Args[0])
	if !ok || field.Type != qsbridge.DataTypeTime {
		return physicalGroupProjectionExpression{}, false
	}
	index := field.Table.Table
	if index == "" {
		index = defaultIndex
	}
	if defaultIndex != "" && index != "" && !strings.EqualFold(index, defaultIndex) {
		return physicalGroupProjectionExpression{}, false
	}
	physical := directBitmapFieldPhysicalName(field)
	if physical == "" {
		return physicalGroupProjectionExpression{}, false
	}
	role := materializationFieldRole(defaultIndex, field)
	output := qsbridge.QuantaProjectionField{
		Index:        index,
		Role:         qsbridge.TableInstanceID(role),
		Field:        "year_" + physical,
		Type:         qsbridge.DataTypeInt,
		PhysicalName: "year_" + physical,
		Visible:      false,
	}
	sourceKey := materializationFieldRefStorageKey(defaultIndex, field)
	return physicalGroupProjectionExpression{
		Expression: qsbridge.QuantaProjectionExpression{
			Expr:   expr,
			Output: output,
		},
		SourceKey: sourceKey,
		OutputKey: materializationProjectionFieldStorageKey(output),
	}, true
}

func physicalGroupProjectionFunctionIsYear(name string) bool {
	return strings.EqualFold(name, "year") || strings.EqualFold(name, "yy")
}

func materializationRequiredSourceKeysOutsidePhysicalGroupExpressions(request ExecutionRequest, defaultIndex string, expressions []physicalGroupProjectionExpression) map[string]struct{} {
	required := make(map[string]struct{})
	for _, predicate := range request.Predicates {
		switch predicate.Placement {
		case qsbridge.PredicateResidualScan, qsbridge.PredicateResidualJoin:
			materializationAddExprFieldKeys(required, defaultIndex, predicate.Expr, nil)
		}
	}
	for _, projection := range request.Projection {
		materializationAddExprFieldKeys(required, defaultIndex, projection.Expr, expressions)
	}
	for _, expr := range request.GroupBy {
		materializationAddExprFieldKeys(required, defaultIndex, expr, expressions)
	}
	for _, aggregate := range request.SQLAggregates {
		materializationAddExprFieldKeys(required, defaultIndex, aggregate.Input, nil)
		materializationAddExprFieldKeys(required, defaultIndex, aggregate.Filter, nil)
	}
	for _, predicate := range request.Having {
		materializationAddExprFieldKeys(required, defaultIndex, predicate.Expr, expressions)
	}
	for _, sort := range request.OrderBy {
		materializationAddExprFieldKeys(required, defaultIndex, sort.Expr, expressions)
	}
	for _, hidden := range request.Result.Hidden {
		required[materializationFieldRefStorageKey(defaultIndex, hidden)] = struct{}{}
	}
	for _, predicate := range request.NativePredicates.CorrelatedAggregate {
		required[materializationFieldRefStorageKey(defaultIndex, predicate.KeyField)] = struct{}{}
		required[materializationFieldRefStorageKey(defaultIndex, predicate.ValueField)] = struct{}{}
	}
	return required
}

func materializationAddExprFieldKeys(required map[string]struct{}, defaultIndex string, expr qsbridge.Expr, satisfiedExpressions []physicalGroupProjectionExpression) {
	if expr == nil {
		return
	}
	for _, expression := range satisfiedExpressions {
		if directBitmapGroupExpressionsEqual(expr, expression.Expression.Expr) {
			return
		}
	}
	for _, ref := range qsbridge.FieldRefs(expr) {
		required[materializationFieldRefStorageKey(defaultIndex, ref)] = struct{}{}
	}
}

func materializationFieldRefStorageKey(defaultIndex string, ref qsbridge.FieldRef) string {
	index := ref.Table.Table
	if index == "" {
		index = defaultIndex
	}
	role := materializationFieldRole(defaultIndex, ref)
	name := directBitmapFieldPhysicalName(ref)
	return materializationStorageKey(index, role, name)
}

func materializationProjectionFieldStorageKey(field qsbridge.QuantaProjectionField) string {
	role := string(field.Role)
	if role == "" {
		role = field.Index
	}
	name := field.Field
	if name == "" {
		name = field.PhysicalName
	}
	return materializationStorageKey(field.Index, role, name)
}

func materializationStorageKey(index string, role string, name string) string {
	return strings.ToLower(index) + "\x00" + strings.ToLower(role) + "\x00" + strings.ToLower(name)
}

func materializationRootProjectionFields(rootIndex string, fields []qsbridge.QuantaProjectionField) []qsbridge.QuantaProjectionField {
	rootFields := make([]qsbridge.QuantaProjectionField, 0, len(fields))
	for _, field := range fields {
		if field.Index == "" || strings.EqualFold(field.Index, rootIndex) {
			rootFields = append(rootFields, field)
		}
	}
	return rootFields
}

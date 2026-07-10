package qsruntime

import (
	"context"
	"strconv"

	"github.com/QuantaStream/quantastream/qsbridge"
)

// RelationshipJoinExecutionKind aliases the qsbridge relationship execution kind during the package split.
type RelationshipJoinExecutionKind = qsbridge.RelationshipJoinExecutionKind

const (
	// RelationshipJoinExecutionUnknown means the join has no relationship primitive yet.
	RelationshipJoinExecutionUnknown = qsbridge.RelationshipJoinExecutionUnknown
	// RelationshipJoinExecutionVector means the join needs a relationship-vector traversal.
	RelationshipJoinExecutionVector = qsbridge.RelationshipJoinExecutionVector
)

// RelationshipJoinOperationIntent aliases the qsbridge relationship-vector operation intent.
type RelationshipJoinOperationIntent = qsbridge.RelationshipJoinOperationIntent

const (
	// RelationshipJoinOperationReduce means related found sets reduce each other for an inner join.
	RelationshipJoinOperationReduce = qsbridge.RelationshipJoinOperationReduce
	// RelationshipJoinOperationExpand means parent rows expand to child rows.
	RelationshipJoinOperationExpand = qsbridge.RelationshipJoinOperationExpand
	// RelationshipJoinOperationSemi means matching related rows keep the driving side.
	RelationshipJoinOperationSemi = qsbridge.RelationshipJoinOperationSemi
	// RelationshipJoinOperationAnti means matching related rows are subtracted from the driving side.
	RelationshipJoinOperationAnti = qsbridge.RelationshipJoinOperationAnti
	// RelationshipJoinOperationNullExtend means unmatched rows must be preserved for an outer join.
	RelationshipJoinOperationNullExtend = qsbridge.RelationshipJoinOperationNullExtend
)

// RelationshipJoinPlan aliases the qsbridge relationship-aware join plan during the package split.
type RelationshipJoinPlan = qsbridge.RelationshipJoinPlan

// RelationshipJoinPlanEdge aliases one qsbridge planned relationship edge.
type RelationshipJoinPlanEdge = qsbridge.RelationshipJoinPlanEdge

// RelationshipVectorJoinRequest aliases the qsbridge vector join adapter request.
type RelationshipVectorJoinRequest = qsbridge.RelationshipVectorJoinRequest

// RelationshipVectorJoinResult aliases the qsbridge vector join adapter result.
type RelationshipVectorJoinResult = qsbridge.RelationshipVectorJoinResult

// RelationshipJoinedRow aliases one logical joined row before late materialization.
type RelationshipJoinedRow = qsbridge.RelationshipJoinedRow

// RelationshipJoinMaterializationRequest aliases joined-row materialization input.
type RelationshipJoinMaterializationRequest = qsbridge.RelationshipJoinMaterializationRequest

// RelationshipVectorKernelRequest aliases a low-level vector primitive invocation.
type RelationshipVectorKernelRequest = qsbridge.RelationshipVectorKernelRequest

// RelationshipVectorKernelResult aliases the low-level vector primitive response.
type RelationshipVectorKernelResult = qsbridge.RelationshipVectorKernelResult

// RelationshipVectorKernel aliases the qsbridge low-level relationship-vector primitive boundary.
type RelationshipVectorKernel = qsbridge.RelationshipVectorKernel

// RelationshipVectorProjectionRead aliases one qsbridge relationship-vector projection read.
type RelationshipVectorProjectionRead = qsbridge.RelationshipVectorProjectionRead

// RelationshipVectorProjectionResult aliases one qsbridge relationship-vector projection result.
type RelationshipVectorProjectionResult = qsbridge.RelationshipVectorProjectionResult

// RelationshipVectorProjectionKernelRequest aliases grouped projector vector projection input.
type RelationshipVectorProjectionKernelRequest = qsbridge.RelationshipVectorProjectionKernelRequest

// RelationshipVectorProjectionKernelResult aliases grouped projector vector projection output.
type RelationshipVectorProjectionKernelResult = qsbridge.RelationshipVectorProjectionKernelResult

// RelationshipVectorProjectionKernel aliases the qsbridge relationship-vector projection boundary.
type RelationshipVectorProjectionKernel = qsbridge.RelationshipVectorProjectionKernel

// PlanRelationshipJoins forwards to the qsbridge relationship execution contract.
func PlanRelationshipJoins(edges []qsbridge.JoinEdge) RelationshipJoinPlan {
	return qsbridge.PlanRelationshipJoins(edges)
}

// RelationshipVectorJoinExecutor is the adapter seam for future vector-backed join execution.
type RelationshipVectorJoinExecutor interface {
	ExecuteRelationshipVectorJoin(context.Context, ExecutionRequest, RelationshipVectorJoinRequest) (ExecutionResult, error)
}

// UnsupportedRelationshipVectorJoinExecutor preserves the current explicit boundary.
type UnsupportedRelationshipVectorJoinExecutor struct{}

// ExecuteRelationshipVectorJoin reports that relationship-vector execution is planned but not wired.
func (UnsupportedRelationshipVectorJoinExecutor) ExecuteRelationshipVectorJoin(_ context.Context, _ ExecutionRequest, vector RelationshipVectorJoinRequest) (ExecutionResult, error) {
	result := UnsupportedRelationshipVectorJoinResult(vector)
	return ExecutionResult{Diagnostics: result.Diagnostics}, nil
}

// UnsupportedRelationshipVectorJoinResult builds the current explicit boundary result.
func UnsupportedRelationshipVectorJoinResult(vector RelationshipVectorJoinRequest) RelationshipVectorJoinResult {
	return RelationshipVectorJoinResult{
		RootIndex:   vector.RootIndex,
		Diagnostics: relationshipVectorJoinDiagnostics(vector.Plan),
	}
}

// FixtureRelationshipVectorJoinExecutor records the adapter request and returns deterministic diagnostics.
type FixtureRelationshipVectorJoinExecutor struct {
	LastRequest RelationshipVectorJoinRequest
}

// ExecuteRelationshipVectorJoin captures the request shape without executing bitmap work.
func (e *FixtureRelationshipVectorJoinExecutor) ExecuteRelationshipVectorJoin(_ context.Context, _ ExecutionRequest, vector RelationshipVectorJoinRequest) (ExecutionResult, error) {
	if e != nil {
		e.LastRequest = vector
	}
	result := UnsupportedRelationshipVectorJoinResult(vector)
	return ExecutionResult{Diagnostics: result.Diagnostics}, nil
}

// UnsupportedRelationshipVectorKernel preserves the current explicit relationship-vector boundary.
type UnsupportedRelationshipVectorKernel struct{}

// ReduceRelatedFoundSets reports that reduction is not wired yet.
func (UnsupportedRelationshipVectorKernel) ReduceRelatedFoundSets(_ context.Context, request RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error) {
	return unsupportedRelationshipVectorKernelResult(request), nil
}

// ExpandRelatedRows reports that expansion is not wired yet.
func (UnsupportedRelationshipVectorKernel) ExpandRelatedRows(_ context.Context, request RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error) {
	return unsupportedRelationshipVectorKernelResult(request), nil
}

// SemiJoinRelatedRows reports that semi-join membership is not wired yet.
func (UnsupportedRelationshipVectorKernel) SemiJoinRelatedRows(_ context.Context, request RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error) {
	return unsupportedRelationshipVectorKernelResult(request), nil
}

// AntiJoinRelatedRows reports that anti-join difference is not wired yet.
func (UnsupportedRelationshipVectorKernel) AntiJoinRelatedRows(_ context.Context, request RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error) {
	return unsupportedRelationshipVectorKernelResult(request), nil
}

// NullExtendRelatedRows reports that null-extension is not wired yet.
func (UnsupportedRelationshipVectorKernel) NullExtendRelatedRows(_ context.Context, request RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error) {
	return unsupportedRelationshipVectorKernelResult(request), nil
}

// ExecuteRelationshipVectorKernel dispatches one adapter request to the matching kernel primitive.
func ExecuteRelationshipVectorKernel(ctx context.Context, kernel RelationshipVectorKernel, vector RelationshipVectorJoinRequest) (RelationshipVectorKernelResult, error) {
	if kernel == nil {
		kernel = UnsupportedRelationshipVectorKernel{}
	}
	return qsbridge.ExecuteRelationshipVectorKernel(ctx, kernel, vector)
}

// RelationshipVectorProjectionKernelAdapter delegates projector vector loads to a configured kernel.
type RelationshipVectorProjectionKernelAdapter struct {
	Kernel RelationshipVectorProjectionKernel
}

// LoadRelationshipVectorProjections delegates to the configured kernel or the unsupported boundary.
func (a RelationshipVectorProjectionKernelAdapter) LoadRelationshipVectorProjections(ctx context.Context, request RelationshipVectorProjectionKernelRequest) (RelationshipVectorProjectionKernelResult, error) {
	kernel := a.Kernel
	if kernel == nil {
		kernel = UnsupportedRelationshipVectorProjectionKernel{}
	}
	return kernel.LoadRelationshipVectorProjections(ctx, request)
}

// ExecuteRelationshipVectorProjectionKernel dispatches grouped projector vector reads.
func ExecuteRelationshipVectorProjectionKernel(ctx context.Context, kernel RelationshipVectorProjectionKernel, request RelationshipVectorProjectionKernelRequest) (RelationshipVectorProjectionKernelResult, error) {
	return RelationshipVectorProjectionKernelAdapter{Kernel: kernel}.LoadRelationshipVectorProjections(ctx, request)
}

// UnsupportedRelationshipVectorProjectionKernel preserves the current explicit vector projection boundary.
type UnsupportedRelationshipVectorProjectionKernel struct{}

// LoadRelationshipVectorProjections reports that relationship-vector projection loading is not wired yet.
func (UnsupportedRelationshipVectorProjectionKernel) LoadRelationshipVectorProjections(_ context.Context, request RelationshipVectorProjectionKernelRequest) (RelationshipVectorProjectionKernelResult, error) {
	return RelationshipVectorProjectionKernelResult{
		ID:          request.ID,
		Diagnostics: unsupportedRelationshipVectorProjectionDiagnostics(request),
	}, nil
}

func unsupportedRelationshipVectorProjectionDiagnostics(request RelationshipVectorProjectionKernelRequest) qsbridge.DiagnosticSet {
	return qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedJoin,
			qsbridge.PhaseExecute,
			"relationship-vector projection kernel is not wired yet: reads="+strconv.Itoa(len(request.Reads)),
		),
	}
}

func unsupportedRelationshipVectorKernelResult(request RelationshipVectorKernelRequest) RelationshipVectorKernelResult {
	vector := RelationshipVectorJoinRequest{
		RootIndex: request.RootIndex,
		Plan: RelationshipJoinPlan{Edges: []RelationshipJoinPlanEdge{
			request.Edge,
		}},
		Edges: []RelationshipJoinPlanEdge{request.Edge},
	}
	return qsbridge.UnsupportedRelationshipVectorKernelResult(vector)
}

func relationshipVectorJoinDiagnostics(plan RelationshipJoinPlan) qsbridge.DiagnosticSet {
	return qsbridge.RelationshipVectorJoinDiagnostics(plan)
}

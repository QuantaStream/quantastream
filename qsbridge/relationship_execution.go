package qsbridge

import (
	"context"
	"fmt"
)

// RelationshipJoinExecutionKind describes the primitive needed for a join edge.
type RelationshipJoinExecutionKind string

const (
	// RelationshipJoinExecutionUnknown means the join has no relationship primitive yet.
	RelationshipJoinExecutionUnknown RelationshipJoinExecutionKind = "unknown"
	// RelationshipJoinExecutionVector means the join needs a relationship-vector traversal.
	RelationshipJoinExecutionVector RelationshipJoinExecutionKind = "relationship_vector"
)

// RelationshipJoinOperationIntent describes what relationship-vector work this SQL shape needs.
type RelationshipJoinOperationIntent string

const (
	// RelationshipJoinOperationReduce means related found sets reduce each other for an inner join.
	RelationshipJoinOperationReduce RelationshipJoinOperationIntent = "reduce"
	// RelationshipJoinOperationExpand means parent rows expand to child rows.
	RelationshipJoinOperationExpand RelationshipJoinOperationIntent = "expand"
	// RelationshipJoinOperationSemi means matching related rows keep the driving side.
	RelationshipJoinOperationSemi RelationshipJoinOperationIntent = "semi"
	// RelationshipJoinOperationAnti means matching related rows are subtracted from the driving side.
	RelationshipJoinOperationAnti RelationshipJoinOperationIntent = "anti"
	// RelationshipJoinOperationNullExtend means unmatched rows must be preserved for an outer join.
	RelationshipJoinOperationNullExtend RelationshipJoinOperationIntent = "null_extend"
)

// RelationshipVectorProjectionScope describes how much relationship-vector data a read may fetch.
type RelationshipVectorProjectionScope string

const (
	// RelationshipVectorProjectionScopeUnspecified leaves projection scope to the runtime.
	RelationshipVectorProjectionScopeUnspecified RelationshipVectorProjectionScope = ""
	// RelationshipVectorProjectionScopePredicateWindow reads only the predicate/time-window scope.
	RelationshipVectorProjectionScopePredicateWindow RelationshipVectorProjectionScope = "predicate_window"
	// RelationshipVectorProjectionScopeBroadFromFoundset reads a broader FK vector and narrows from a foundset.
	RelationshipVectorProjectionScopeBroadFromFoundset RelationshipVectorProjectionScope = "broad_from_foundset"
)

// RelationshipJoinExecutionStatus describes whether an execution boundary can handle a join edge.
type RelationshipJoinExecutionStatus string

const (
	// RelationshipJoinExecutionStatusPlanned means no runtime boundary is currently known for the join edge.
	RelationshipJoinExecutionStatusPlanned RelationshipJoinExecutionStatus = "planned"
	// RelationshipJoinExecutionStatusNotWiredYet marks a known plan shape with no runtime executor wiring yet.
	RelationshipJoinExecutionStatusNotWiredYet RelationshipJoinExecutionStatus = "not_wired_yet"
)

// RelationshipJoinPlan describes relationship-aware join work before execution.
type RelationshipJoinPlan struct {
	Edges []RelationshipJoinPlanEdge
}

// RelationshipJoinPlanEdge is one planned relationship edge.
type RelationshipJoinPlanEdge struct {
	Left            FieldRef
	LeftRole        TableInstanceID
	Right           FieldRef
	RightRole       TableInstanceID
	SQLKind         JoinKind
	ExecutionKind   RelationshipJoinExecutionKind
	Intent          RelationshipJoinOperationIntent
	EncodingKind    RelationshipEncodingKind
	Capabilities    RelationshipCapabilities
	ProjectionScope RelationshipVectorProjectionScope
	CoveragePlan    RelationshipVectorProjectionCoveragePlan
	Status          RelationshipJoinExecutionStatus
}

// RelationshipVectorJoinRequest is the protocol-neutral input to a vector join adapter.
type RelationshipVectorJoinRequest struct {
	RootIndex string
	Plan      RelationshipJoinPlan
	Edges     []RelationshipJoinPlanEdge
}

// RelationshipVectorJoinResult is the protocol-neutral output from a vector join adapter.
type RelationshipVectorJoinResult struct {
	RootIndex    string
	CandidateSet []QuantaRownum
	JoinedRows   []RelationshipJoinedRow
	Diagnostics  DiagnosticSet
}

// RownumDomain identifies the table/role coordinate space for a rownum set.
type RownumDomain struct {
	Table TableInstance
	Role  TableInstanceID
}

// Name returns a stable display name for this rownum domain.
func (d RownumDomain) Name() string {
	if d.Role != "" {
		return string(d.Role)
	}
	if d.Table.Alias != "" {
		return d.Table.Alias
	}
	return d.Table.Table
}

// RownumDomainSet carries rownums with their current table/role domain.
type RownumDomainSet struct {
	Domain  RownumDomain
	Rownums []QuantaRownum
}

// CandidateCount reports how many rownums are in this domain set.
func (s RownumDomainSet) CandidateCount() int {
	return len(s.Rownums)
}

// RownumDomainTranslationDirection describes how a relationship vector changes rownum domains.
type RownumDomainTranslationDirection string

const (
	// RownumDomainTranslationChildToParent maps child/FK-side rownums to parent/PK-side rownums.
	RownumDomainTranslationChildToParent RownumDomainTranslationDirection = "child_to_parent"
	// RownumDomainTranslationParentToChild maps parent/PK-side rownums to child/FK-side rownums.
	RownumDomainTranslationParentToChild RownumDomainTranslationDirection = "parent_to_child"
	// RownumDomainTranslationSameDomain keeps the same domain after filtering or validation.
	RownumDomainTranslationSameDomain RownumDomainTranslationDirection = "same_domain"
)

// RownumDomainTranslation describes one legal rownum-domain movement.
type RownumDomainTranslation struct {
	ID        string
	From      RownumDomain
	To        RownumDomain
	Edge      RelationshipJoinPlanEdge
	Direction RownumDomainTranslationDirection
	Intent    RelationshipJoinOperationIntent
}

// ChangesDomain reports whether this translation crosses table/role domains.
func (t RownumDomainTranslation) ChangesDomain() bool {
	return t.From.Name() != t.To.Name()
}

// RelationshipJoinedRow describes one logical joined row before SQL projection materialization.
type RelationshipJoinedRow struct {
	Left       QuantaRownum
	Right      QuantaRownum
	NullLeft   bool
	NullRight  bool
	SourceEdge int
}

// RelationshipJoinMaterializationRequest carries joined rows into late materialization.
type RelationshipJoinMaterializationRequest struct {
	RootIndex        string
	JoinedRows       []RelationshipJoinedRow
	ProjectionFields []QuantaProjectionField
}

// RelationshipVectorKernelRequest is one low-level vector primitive invocation.
type RelationshipVectorKernelRequest struct {
	RootIndex string
	Edge      RelationshipJoinPlanEdge
	Intent    RelationshipJoinOperationIntent
}

// RelationshipVectorKernelResult is the low-level vector primitive response.
type RelationshipVectorKernelResult struct {
	RootIndex    string
	CandidateSet []QuantaRownum
	JoinedRows   []RelationshipJoinedRow
	Diagnostics  DiagnosticSet
}

// RelationshipVectorProjectionRead describes one FK/vector BSI read.
//
// The read is expressed in rownum-domain terms so the planner can reason about
// whether the result is a reduction, expansion, semi/anti membership, or
// null-extension input without assuming any legacy projector behavior.
type RelationshipVectorProjectionRead struct {
	ID              string
	ProbePrefix     string
	Edge            RelationshipJoinPlanEdge
	Intent          RelationshipJoinOperationIntent
	Input           RownumDomainSet
	OutputDomain    RownumDomain
	Translation     RownumDomainTranslation
	ProjectionScope RelationshipVectorProjectionScope
	CoveragePlan    RelationshipVectorProjectionCoveragePlan
	Cacheable       bool
	FromEpochMillis int64
	ToEpochMillis   int64
}

// RelationshipVectorProjectionResult is the protocol-neutral FK/vector BSI read result.
type RelationshipVectorProjectionResult struct {
	ID          string
	Input       RownumDomainSet
	Output      RownumDomainSet
	Translation RownumDomainTranslation
	Coverage    RelationshipVectorProjectionCoverage
	CacheHit    bool
	Probes      []ProjectionProbe
	Diagnostics DiagnosticSet
}

// RelationshipVectorProjectionCoverageStatus names whether an FK/vector BSI read
// covers the requested child rownum domain.
type RelationshipVectorProjectionCoverageStatus string

const (
	// RelationshipVectorProjectionCoverageComplete means every requested rownum
	// is covered by the projected FK/vector BSI.
	RelationshipVectorProjectionCoverageComplete RelationshipVectorProjectionCoverageStatus = "complete"
	// RelationshipVectorProjectionCoveragePartial means some, but not all,
	// requested rownums are covered by the projected FK/vector BSI.
	RelationshipVectorProjectionCoveragePartial RelationshipVectorProjectionCoverageStatus = "partial"
	// RelationshipVectorProjectionCoverageEmpty means the projected FK/vector
	// BSI has no overlap with the requested rownum domain.
	RelationshipVectorProjectionCoverageEmpty RelationshipVectorProjectionCoverageStatus = "empty"
)

// RelationshipVectorProjectionRecoveryPolicy names the executor policy for an
// incomplete relationship-vector projection.
type RelationshipVectorProjectionRecoveryPolicy string

const (
	// RelationshipVectorProjectionRecoveryUseFoundset keeps the requested
	// foundset-scoped vector read.
	RelationshipVectorProjectionRecoveryUseFoundset RelationshipVectorProjectionRecoveryPolicy = "use_foundset"
	// RelationshipVectorProjectionRecoveryBroadenAndIntersect retries with a
	// broader relationship-vector read and intersects locally with the requested
	// rownum domain.
	RelationshipVectorProjectionRecoveryBroadenAndIntersect RelationshipVectorProjectionRecoveryPolicy = "broaden_and_intersect"
)

// RelationshipVectorProjectionCoveragePlan is the planner-facing expectation
// for a relationship-vector read before runtime coverage is known.
type RelationshipVectorProjectionCoveragePlan struct {
	ProjectionScope RelationshipVectorProjectionScope
	ExpectStatus    RelationshipVectorProjectionCoverageStatus
	RecoveryPolicy  RelationshipVectorProjectionRecoveryPolicy
	VerifyCoverage  bool
}

// NewRelationshipVectorProjectionCoveragePlan builds the default coverage policy for a vector read.
func NewRelationshipVectorProjectionCoveragePlan(scope RelationshipVectorProjectionScope) RelationshipVectorProjectionCoveragePlan {
	if scope == RelationshipVectorProjectionScopeUnspecified {
		scope = RelationshipVectorProjectionScopePredicateWindow
	}
	return RelationshipVectorProjectionCoveragePlan{
		ProjectionScope: scope,
		ExpectStatus:    RelationshipVectorProjectionCoverageComplete,
		RecoveryPolicy:  RelationshipVectorProjectionRecoveryBroadenAndIntersect,
		VerifyCoverage:  true,
	}
}

// Effective returns the default coverage plan when this plan is unset.
func (p RelationshipVectorProjectionCoveragePlan) Effective(scope RelationshipVectorProjectionScope) RelationshipVectorProjectionCoveragePlan {
	if p.ProjectionScope == RelationshipVectorProjectionScopeUnspecified {
		p.ProjectionScope = scope
	}
	if p.ProjectionScope == RelationshipVectorProjectionScopeUnspecified {
		p.ProjectionScope = RelationshipVectorProjectionScopePredicateWindow
	}
	if p.ExpectStatus == "" {
		p.ExpectStatus = RelationshipVectorProjectionCoverageComplete
	}
	if p.RecoveryPolicy == "" {
		p.RecoveryPolicy = RelationshipVectorProjectionRecoveryBroadenAndIntersect
	}
	if !p.VerifyCoverage {
		p.VerifyCoverage = true
	}
	return p
}

// RelationshipVectorProjectionCoverage records coverage of requested child
// rownums by a projected relationship-vector BSI.
type RelationshipVectorProjectionCoverage struct {
	RequestedRows  int
	ProjectedRows  int
	OverlapRows    int
	Status         RelationshipVectorProjectionCoverageStatus
	RecoveryPolicy RelationshipVectorProjectionRecoveryPolicy
}

// NewRelationshipVectorProjectionCoverage classifies FK/vector BSI coverage.
func NewRelationshipVectorProjectionCoverage(requestedRows, projectedRows, overlapRows int) RelationshipVectorProjectionCoverage {
	coverage := RelationshipVectorProjectionCoverage{
		RequestedRows: requestedRows,
		ProjectedRows: projectedRows,
		OverlapRows:   overlapRows,
	}
	if coverage.RequestedRows < 0 {
		coverage.RequestedRows = 0
	}
	if coverage.ProjectedRows < 0 {
		coverage.ProjectedRows = 0
	}
	if coverage.OverlapRows < 0 {
		coverage.OverlapRows = 0
	}
	switch {
	case coverage.RequestedRows == 0 || coverage.OverlapRows >= coverage.RequestedRows:
		coverage.Status = RelationshipVectorProjectionCoverageComplete
		coverage.RecoveryPolicy = RelationshipVectorProjectionRecoveryUseFoundset
	case coverage.OverlapRows == 0:
		coverage.Status = RelationshipVectorProjectionCoverageEmpty
		coverage.RecoveryPolicy = RelationshipVectorProjectionRecoveryBroadenAndIntersect
	default:
		coverage.Status = RelationshipVectorProjectionCoveragePartial
		coverage.RecoveryPolicy = RelationshipVectorProjectionRecoveryBroadenAndIntersect
	}
	return coverage
}

// Complete reports whether the projected vector covers all requested rownums.
func (c RelationshipVectorProjectionCoverage) Complete() bool {
	return c.Status == RelationshipVectorProjectionCoverageComplete
}

// NeedsRecovery reports whether the executor should broaden and intersect.
func (c RelationshipVectorProjectionCoverage) NeedsRecovery() bool {
	return c.Status == RelationshipVectorProjectionCoveragePartial || c.Status == RelationshipVectorProjectionCoverageEmpty
}

// WithRecoveryPolicy returns a copy with the selected executor recovery policy.
func (c RelationshipVectorProjectionCoverage) WithRecoveryPolicy(policy RelationshipVectorProjectionRecoveryPolicy) RelationshipVectorProjectionCoverage {
	c.RecoveryPolicy = policy
	return c
}

// RelationshipVectorProjectionReader fetches relationship-vector projection data.
type RelationshipVectorProjectionReader interface {
	ReadRelationshipVectorProjection(context.Context, RelationshipVectorProjectionRead) (RelationshipVectorProjectionResult, error)
}

// RelationshipVectorProjectionKernelRequest groups relationship-vector reads for one projector step.
type RelationshipVectorProjectionKernelRequest struct {
	ID          string
	ProbePrefix string
	Reads       []RelationshipVectorProjectionRead
}

// RelationshipVectorProjectionKernelResult carries vector projection results for one projector step.
type RelationshipVectorProjectionKernelResult struct {
	ID          string
	Results     []RelationshipVectorProjectionResult
	Probes      []ProjectionProbe
	Diagnostics DiagnosticSet
}

// OutputDomainSet returns this projection result as a domain-tagged rownum set.
func (r RelationshipVectorProjectionResult) OutputDomainSet() (RownumDomainSet, bool) {
	if r.Output.Domain.Name() == "" {
		return RownumDomainSet{}, false
	}
	output := r.Output
	output.Rownums = append([]QuantaRownum(nil), r.Output.Rownums...)
	return output, true
}

// OutputDomainSets merges vector projection outputs by rownum domain.
func (r RelationshipVectorProjectionKernelResult) OutputDomainSets() map[string]RownumDomainSet {
	sets := make(map[string]RownumDomainSet)
	seen := make(map[string]map[QuantaRownum]struct{})
	for _, result := range r.Results {
		output, ok := result.OutputDomainSet()
		if !ok {
			continue
		}
		name := output.Domain.Name()
		merged := sets[name]
		if merged.Domain.Name() == "" {
			merged.Domain = output.Domain
		}
		if seen[name] == nil {
			seen[name] = make(map[QuantaRownum]struct{}, len(merged.Rownums)+len(output.Rownums))
			for _, rownum := range merged.Rownums {
				seen[name][rownum] = struct{}{}
			}
		}
		for _, rownum := range output.Rownums {
			if _, exists := seen[name][rownum]; exists {
				continue
			}
			seen[name][rownum] = struct{}{}
			merged.Rownums = append(merged.Rownums, rownum)
		}
		sets[name] = merged
	}
	return sets
}

// RelationshipVectorProjectionKernel loads relationship-vector projection data for projector work.
type RelationshipVectorProjectionKernel interface {
	LoadRelationshipVectorProjections(context.Context, RelationshipVectorProjectionKernelRequest) (RelationshipVectorProjectionKernelResult, error)
}

// RelationshipVectorKernel owns low-level relationship-vector primitives.
type RelationshipVectorKernel interface {
	ReduceRelatedFoundSets(context.Context, RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error)
	ExpandRelatedRows(context.Context, RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error)
	SemiJoinRelatedRows(context.Context, RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error)
	AntiJoinRelatedRows(context.Context, RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error)
	NullExtendRelatedRows(context.Context, RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error)
}

// NeedsRelationshipVectorExecution reports whether any planned edge needs vector traversal.
func (p RelationshipJoinPlan) NeedsRelationshipVectorExecution() bool {
	for _, edge := range p.Edges {
		if edge.ExecutionKind == RelationshipJoinExecutionVector {
			return true
		}
	}
	return false
}

// FirstRelationshipVectorEdge returns the first edge that needs vector traversal.
func (p RelationshipJoinPlan) FirstRelationshipVectorEdge() (RelationshipJoinPlanEdge, bool) {
	for _, edge := range p.Edges {
		if edge.ExecutionKind == RelationshipJoinExecutionVector {
			return edge, true
		}
	}
	return RelationshipJoinPlanEdge{}, false
}

// VectorRequest returns the adapter request shape for relationship-vector execution.
func (p RelationshipJoinPlan) VectorRequest(rootIndex string) RelationshipVectorJoinRequest {
	edges := make([]RelationshipJoinPlanEdge, 0, len(p.Edges))
	for _, edge := range p.Edges {
		if edge.ExecutionKind == RelationshipJoinExecutionVector {
			edges = append(edges, edge)
		}
	}
	return RelationshipVectorJoinRequest{
		RootIndex: rootIndex,
		Plan:      p,
		Edges:     edges,
	}
}

// EdgeCount reports how many relationship-vector edges the adapter must handle.
func (r RelationshipVectorJoinRequest) EdgeCount() int {
	return len(r.Edges)
}

// FirstEdge returns the first relationship-vector edge in adapter order.
func (r RelationshipVectorJoinRequest) FirstEdge() (RelationshipJoinPlanEdge, bool) {
	if len(r.Edges) == 0 {
		return RelationshipJoinPlanEdge{}, false
	}
	return r.Edges[0], true
}

// RelationshipVectorProjectionReads builds projection reads for vector edges.
func (r RelationshipVectorJoinRequest) RelationshipVectorProjectionReads(inputs map[string]RownumDomainSet) []RelationshipVectorProjectionRead {
	reads := make([]RelationshipVectorProjectionRead, 0, len(r.Edges))
	for index, edge := range r.Edges {
		id := relationshipVectorProjectionID(index+1, edge)
		inputDomain := relationshipDomainForEndpoint(edge.Left, edge.LeftRole)
		outputDomain := relationshipDomainForEndpoint(edge.Right, edge.RightRole)
		direction := RownumDomainTranslationChildToParent
		if edge.Intent == RelationshipJoinOperationExpand {
			inputDomain, outputDomain = outputDomain, inputDomain
			direction = RownumDomainTranslationParentToChild
		}
		input := inputs[inputDomain.Name()]
		if input.Domain.Name() == "" {
			input.Domain = inputDomain
		}
		translation := RownumDomainTranslation{
			ID:        id + ".translation",
			From:      inputDomain,
			To:        outputDomain,
			Edge:      edge,
			Direction: direction,
			Intent:    edge.Intent,
		}
		reads = append(reads, RelationshipVectorProjectionRead{
			ID:              id,
			ProbePrefix:     projectorProbeName(id) + "_",
			Edge:            edge,
			Intent:          edge.Intent,
			Input:           input,
			OutputDomain:    outputDomain,
			Translation:     translation,
			ProjectionScope: relationshipVectorProjectionScope(edge),
			CoveragePlan:    relationshipVectorCoveragePlan(edge),
			Cacheable:       true,
		})
	}
	return reads
}

// PlanRelationshipJoins records execution-visible join primitives without executing them.
func PlanRelationshipJoins(edges []JoinEdge) RelationshipJoinPlan {
	plan := RelationshipJoinPlan{Edges: make([]RelationshipJoinPlanEdge, 0, len(edges))}
	for _, edge := range edges {
		status := RelationshipJoinExecutionStatusPlanned
		executionKind := relationshipJoinExecutionKind(edge)
		intent := relationshipJoinOperationIntent(edge)
		if executionKind == RelationshipJoinExecutionVector {
			status = RelationshipJoinExecutionStatusNotWiredYet
		}
		plan.Edges = append(plan.Edges, RelationshipJoinPlanEdge{
			Left:            edge.Left,
			LeftRole:        TableInstanceID(edge.Left.Table.RefName()),
			Right:           edge.Right,
			RightRole:       TableInstanceID(edge.Right.Table.RefName()),
			SQLKind:         edge.Kind,
			ExecutionKind:   executionKind,
			Intent:          intent,
			EncodingKind:    edge.Encoding.Kind,
			Capabilities:    append(RelationshipCapabilities(nil), edge.Encoding.Capabilities...),
			ProjectionScope: RelationshipVectorProjectionScopePredicateWindow,
			CoveragePlan:    NewRelationshipVectorProjectionCoveragePlan(RelationshipVectorProjectionScopePredicateWindow),
			Status:          status,
		})
	}
	return plan
}

// ExecuteRelationshipVectorKernel dispatches one adapter request to the matching kernel primitive.
func ExecuteRelationshipVectorKernel(ctx context.Context, kernel RelationshipVectorKernel, vector RelationshipVectorJoinRequest) (RelationshipVectorKernelResult, error) {
	if kernel == nil {
		return UnsupportedRelationshipVectorKernelResult(vector), nil
	}
	edge, ok := vector.FirstEdge()
	if !ok {
		return RelationshipVectorKernelResult{RootIndex: vector.RootIndex}, nil
	}
	request := RelationshipVectorKernelRequest{
		RootIndex: vector.RootIndex,
		Edge:      edge,
		Intent:    edge.Intent,
	}
	switch edge.Intent {
	case RelationshipJoinOperationReduce:
		return kernel.ReduceRelatedFoundSets(ctx, request)
	case RelationshipJoinOperationExpand:
		return kernel.ExpandRelatedRows(ctx, request)
	case RelationshipJoinOperationSemi:
		return kernel.SemiJoinRelatedRows(ctx, request)
	case RelationshipJoinOperationAnti:
		return kernel.AntiJoinRelatedRows(ctx, request)
	case RelationshipJoinOperationNullExtend:
		return kernel.NullExtendRelatedRows(ctx, request)
	default:
		return UnsupportedRelationshipVectorKernelResult(vector), nil
	}
}

// UnsupportedRelationshipVectorKernelResult builds an explicit unsupported kernel result.
func UnsupportedRelationshipVectorKernelResult(vector RelationshipVectorJoinRequest) RelationshipVectorKernelResult {
	return RelationshipVectorKernelResult{
		RootIndex:   vector.RootIndex,
		Diagnostics: RelationshipVectorJoinDiagnostics(vector.Plan),
	}
}

// RelationshipVectorJoinDiagnostics reports the current unsupported vector-join boundary.
func RelationshipVectorJoinDiagnostics(plan RelationshipJoinPlan) DiagnosticSet {
	edge, ok := plan.FirstRelationshipVectorEdge()
	if !ok {
		return nil
	}
	return DiagnosticSet{ErrorDiagnostic(
		DiagnosticUnsupportedJoin,
		PhaseExecute,
		fmt.Sprintf(
			"relationship-vector join execution is not wired yet: %s -> %s",
			edge.Left.QualifiedName(), edge.Right.QualifiedName(),
		),
	)}
}

func relationshipDomainForField(field FieldRef) RownumDomain {
	return relationshipDomainForEndpoint(field, "")
}

func relationshipDomainForEndpoint(field FieldRef, role TableInstanceID) RownumDomain {
	if role == "" {
		role = TableInstanceID(field.Table.RefName())
	}
	return RownumDomain{
		Table: field.Table,
		Role:  role,
	}
}

func relationshipVectorProjectionID(sequence int, edge RelationshipJoinPlanEdge) string {
	left := edge.Left.QualifiedName()
	right := edge.Right.QualifiedName()
	if left == "" {
		left = "left"
	}
	if right == "" {
		right = "right"
	}
	return fmt.Sprintf("relationship_vector.%d.%s.%s", sequence, left, right)
}

func relationshipVectorProjectionScope(edge RelationshipJoinPlanEdge) RelationshipVectorProjectionScope {
	if edge.ProjectionScope != RelationshipVectorProjectionScopeUnspecified {
		return edge.ProjectionScope
	}
	return RelationshipVectorProjectionScopePredicateWindow
}

func relationshipVectorCoveragePlan(edge RelationshipJoinPlanEdge) RelationshipVectorProjectionCoveragePlan {
	return edge.CoveragePlan.Effective(relationshipVectorProjectionScope(edge))
}

func relationshipJoinExecutionKind(edge JoinEdge) RelationshipJoinExecutionKind {
	if relationshipJoinNeedsVectorExecution(edge) {
		return RelationshipJoinExecutionVector
	}
	return RelationshipJoinExecutionUnknown
}

func relationshipJoinNeedsVectorExecution(join JoinEdge) bool {
	if join.Encoding.Kind == RelationshipEncodingVector {
		return true
	}
	for _, capability := range join.Encoding.Capabilities {
		switch capability {
		case RelationshipCapabilityParentLookup,
			RelationshipCapabilityChildExpansion,
			RelationshipCapabilityJoinReduction,
			RelationshipCapabilitySemiJoin,
			RelationshipCapabilityAntiJoinDifference,
			RelationshipCapabilityNullExtension:
			return true
		}
	}
	return false
}

func relationshipJoinOperationIntent(edge JoinEdge) RelationshipJoinOperationIntent {
	switch edge.Kind {
	case JoinKindLeftOuter, JoinKindRightOuter, JoinKindFullOuter:
		return RelationshipJoinOperationNullExtend
	}
	if edge.Encoding.Supports(RelationshipCapabilityAntiJoinDifference) {
		return RelationshipJoinOperationAnti
	}
	if edge.Encoding.Supports(RelationshipCapabilitySemiJoin) {
		return RelationshipJoinOperationSemi
	}
	if edge.Encoding.Supports(RelationshipCapabilityChildExpansion) {
		return RelationshipJoinOperationExpand
	}
	if edge.Encoding.Supports(RelationshipCapabilityJoinReduction) ||
		edge.Encoding.Supports(RelationshipCapabilityParentLookup) ||
		edge.Encoding.Kind == RelationshipEncodingVector {
		return RelationshipJoinOperationReduce
	}
	return ""
}

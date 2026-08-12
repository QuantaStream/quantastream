package qsbridge

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// QuantaFragmentOperation names a logical bitmap operation in Quanta's execution dialect.
type QuantaFragmentOperation string

const (
	// QuantaOperationIntersect applies a predicate as an AND-style filter.
	QuantaOperationIntersect QuantaFragmentOperation = "INTERSECT"
	// QuantaOperationUnion applies a predicate as an OR-style candidate set.
	QuantaOperationUnion QuantaFragmentOperation = "UNION"
	// QuantaOperationDifference applies a predicate as a bitmap difference.
	QuantaOperationDifference QuantaFragmentOperation = "DIFFERENCE"
	// QuantaOperationInnerJoin applies a relationship transpose.
	QuantaOperationInnerJoin QuantaFragmentOperation = "INNER_JOIN"
	// QuantaOperationOuterJoin reserves outer-join execution semantics.
	QuantaOperationOuterJoin QuantaFragmentOperation = "OUTER_JOIN"
)

// QuantaBSIOp names a BSI comparison operation in Quanta's execution dialect.
type QuantaBSIOp string

const (
	// QuantaBSIOpNone means no BSI comparison is attached.
	QuantaBSIOpNone QuantaBSIOp = ""
	// QuantaBSIOpLT is a less-than comparison.
	QuantaBSIOpLT QuantaBSIOp = "LT"
	// QuantaBSIOpLE is a less-than-or-equal comparison.
	QuantaBSIOpLE QuantaBSIOp = "LE"
	// QuantaBSIOpEQ is an equality comparison.
	QuantaBSIOpEQ QuantaBSIOp = "EQ"
	// QuantaBSIOpGE is a greater-than-or-equal comparison.
	QuantaBSIOpGE QuantaBSIOp = "GE"
	// QuantaBSIOpGT is a greater-than comparison.
	QuantaBSIOpGT QuantaBSIOp = "GT"
	// QuantaBSIOpRange is an inclusive range comparison.
	QuantaBSIOpRange QuantaBSIOp = "RANGE"
	// QuantaBSIOpBatchEQ is a batched equality comparison.
	QuantaBSIOpBatchEQ QuantaBSIOp = "BATCH_EQ"
)

// QuantaFragmentEpoch identifies one logical version of shard or index data.
type QuantaFragmentEpoch string

// QuantaReplicaGeneration identifies one physical replica lineage.
type QuantaReplicaGeneration string

// QuantaSnapshotID identifies a read snapshot boundary when one is available.
type QuantaSnapshotID string

// QuantaFragmentCacheBoundary records the versions required to validate a cached fragment.
//
// The fields are intentionally logical versions rather than wall-clock
// timestamps. Wall-clock time is useful for observability, but cache correctness
// depends on shard/index/catalog/dictionary versions and replica lineage.
type QuantaFragmentCacheBoundary struct {
	LogicalShard      ShardID
	Replica           ReplicaID
	ReplicaGeneration QuantaReplicaGeneration
	ShardEpoch        QuantaFragmentEpoch
	IndexEpoch        QuantaFragmentEpoch
	CatalogVersion    CatalogVersion
	DictionaryVersion DictionaryVersion
	SnapshotID        QuantaSnapshotID
}

// QuantaFragmentCacheIdentity is a deterministic identity for a node-facing physical fragment.
type QuantaFragmentCacheIdentity struct {
	Digest            string
	Index             string
	Field             string
	Operation         QuantaFragmentOperation
	BSIOp             QuantaBSIOp
	Args              []string
	LogicalShard      ShardID
	Replica           ReplicaID
	ReplicaGeneration QuantaReplicaGeneration
	ShardEpoch        QuantaFragmentEpoch
	IndexEpoch        QuantaFragmentEpoch
	CatalogVersion    CatalogVersion
	DictionaryVersion DictionaryVersion
	SnapshotID        QuantaSnapshotID
}

// QuantaProjectionField describes one node-facing projection read.
type QuantaProjectionField struct {
	Index        string
	Role         TableInstanceID
	Field        string
	Type         DataType
	PhysicalName string
	Roles        FieldRole
	Visible      bool
}

// QuantaProjectionExpression describes one derived projection that storage may
// compute without materializing its source field into SQL-visible cells first.
type QuantaProjectionExpression struct {
	Expr   Expr
	Output QuantaProjectionField
}

// QuantaAggregateOperation names a Quanta-native aggregate runtime primitive.
type QuantaAggregateOperation string

const (
	// QuantaAggregateProjectorRank delegates TopN-style ranking to core.Projector.Rank().
	QuantaAggregateProjectorRank QuantaAggregateOperation = "PROJECTOR_RANK"
)

const (
	// QuantaRuntimeTargetProjectorRank names the legacy runtime method behind TopN ranking.
	QuantaRuntimeTargetProjectorRank = "core.Projector.Rank"
)

// QuantaAggregateRequest describes a Quanta-native aggregate for a future runtime adapter.
//
// The request is declarative: qsbridge can name the intended primitive without
// importing legacy core packages or calling the runtime. Runtime adapters can
// translate this payload to in-process calls, gRPC, or a future native execution API.
type QuantaAggregateRequest struct {
	Operation  QuantaAggregateOperation
	Function   string
	Alias      string
	Input      QuantaProjectionField
	Limit      int
	Strategies []PhysicalStrategy
}

// RuntimeTarget reports the legacy runtime primitive for this aggregate request.
func (r QuantaAggregateRequest) RuntimeTarget() string {
	switch r.Operation {
	case QuantaAggregateProjectorRank:
		return QuantaRuntimeTargetProjectorRank
	default:
		return ""
	}
}

// QuantaAggregateRequestsFromPhysicalNode derives native aggregate handoff requests.
func QuantaAggregateRequestsFromPhysicalNode(node PhysicalAggregateNode) ([]QuantaAggregateRequest, DiagnosticSet) {
	requests := make([]QuantaAggregateRequest, 0, len(node.Aggregates))
	var diagnostics DiagnosticSet
	for _, aggregate := range node.Aggregates {
		if !quantaIntermediateIsTopNAggregate(aggregate) {
			continue
		}
		field, ok := quantaIntermediateFieldExpr(aggregate.Input)
		if !ok {
			diagnostics = append(diagnostics, ErrorDiagnostic(
				DiagnosticUnsupportedFunction,
				PhasePlan,
				"topn aggregate requires a direct field input for Quanta projector rank execution",
			))
			continue
		}
		requests = append(requests, QuantaAggregateRequest{
			Operation:  QuantaAggregateProjectorRank,
			Function:   aggregate.Function,
			Alias:      aggregate.Alias,
			Input:      quantaIntermediateProjectionField(field, true),
			Limit:      0,
			Strategies: []PhysicalStrategy{PhysicalStrategyQuantaTopN},
		})
	}
	return requests, diagnostics
}

// QuantaRownum identifies one tuple/entity candidate in Quanta logical space.
//
// In bitmap adapter code this value is translated to the bitmap/BSI columnID
// coordinate. Keeping qsbridge on rownum avoids leaking bitmap row/value-bucket
// terminology into planner and result-assembly contracts.
type QuantaRownum uint64

// QuantaBitmapColumnID identifies the same candidate at a bitmap/BSI adapter boundary.
//
// This type should stay close to runtime adapters that call bitmap primitives.
// Planner and result-assembly contracts should prefer QuantaRownum.
type QuantaBitmapColumnID uint64

// BitmapColumnID converts a logical Quanta rownum to the bitmap adapter coordinate.
func (r QuantaRownum) BitmapColumnID() QuantaBitmapColumnID {
	return QuantaBitmapColumnID(r)
}

// QuantaRownumFromBitmapColumnID converts a bitmap adapter coordinate to a logical rownum.
func QuantaRownumFromBitmapColumnID(id QuantaBitmapColumnID) QuantaRownum {
	return QuantaRownum(id)
}

// BitmapColumnIDsFromRownums converts rownums to bitmap adapter coordinates.
func BitmapColumnIDsFromRownums(rownums []QuantaRownum) []QuantaBitmapColumnID {
	ids := make([]QuantaBitmapColumnID, len(rownums))
	for i, rownum := range rownums {
		ids[i] = rownum.BitmapColumnID()
	}
	return ids
}

// RownumsFromBitmapColumnIDs converts bitmap adapter coordinates to rownums.
func RownumsFromBitmapColumnIDs(ids []QuantaBitmapColumnID) []QuantaRownum {
	rownums := make([]QuantaRownum, len(ids))
	for i, id := range ids {
		rownums[i] = QuantaRownumFromBitmapColumnID(id)
	}
	return rownums
}

// QuantaCandidateSet carries rownum candidates before projection values are materialized.
type QuantaCandidateSet struct {
	Index        string
	LogicalShard ShardID
	Replica      ReplicaID
	Rownums      []QuantaRownum
}

// CandidateCount reports how many tuple/entity candidates are present.
func (s QuantaCandidateSet) CandidateCount() int {
	return len(s.Rownums)
}

// MaterializationRequest creates a late-materialization request for projection fields.
func (s QuantaCandidateSet) MaterializationRequest(fields []QuantaProjectionField) QuantaMaterializationRequest {
	return QuantaMaterializationRequest{
		Index:            s.Index,
		LogicalShard:     s.LogicalShard,
		Replica:          s.Replica,
		Rownums:          append([]QuantaRownum(nil), s.Rownums...),
		ProjectionFields: append([]QuantaProjectionField(nil), fields...),
	}
}

// QuantaMaterializationRequest asks a runtime adapter to fetch projection values for rownums.
type QuantaMaterializationRequest struct {
	Index                 string
	LogicalShard          ShardID
	Replica               ReplicaID
	DependencyID          string
	ProbePrefix           string
	Batch                 ProjectionBatch
	Rownums               []QuantaRownum
	ProjectionFields      []QuantaProjectionField
	ProjectionExpressions []QuantaProjectionExpression
	FromEpochMillis       int64
	ToEpochMillis         int64
}

// CandidateCount reports how many tuple/entity candidates need materialization.
func (r QuantaMaterializationRequest) CandidateCount() int {
	return len(r.Rownums)
}

// ProjectionCount reports how many fields or derived expressions need materialization.
func (r QuantaMaterializationRequest) ProjectionCount() int {
	return len(r.ProjectionFields) + len(r.ProjectionExpressions)
}

// QuantaProjectionVector stores one projected field as a columnar value vector.
//
// Nodes naturally return BSI and bitmap-backed values by field. Keeping this
// shape columnar lets runtime adapters avoid premature row materialization,
// while result assemblers can zip rownums and vectors into protocol rows later.
type QuantaProjectionVector struct {
	Field  QuantaProjectionField
	Values []ResultCell
}

// QuantaProjectedRowSet is the neutral node response shape for projected candidates.
//
// It intentionally avoids any legacy projector or transport package. In-process,
// gRPC, and future protocol adapters can all translate into this contract before
// a planner-owned assembler applies joins, grouping, ordering, and final output
// formatting.
type QuantaProjectedRowSet struct {
	Index             string
	LogicalShard      ShardID
	Replica           ReplicaID
	DependencyID      string
	Batch             ProjectionBatch
	Rownums           []QuantaRownum
	ProjectionVectors []QuantaProjectionVector
}

// CandidateCount reports how many tuple/entity candidates are represented by the row set.
func (r QuantaProjectedRowSet) CandidateCount() int {
	return len(r.Rownums)
}

// ProjectionCount reports how many projected vectors are present.
func (r QuantaProjectedRowSet) ProjectionCount() int {
	return len(r.ProjectionVectors)
}

// ValidateShape reports internal response-shape mismatches before assembly.
func (r QuantaProjectedRowSet) ValidateShape() DiagnosticSet {
	var diagnostics DiagnosticSet
	candidateCount := r.CandidateCount()
	for _, vector := range r.ProjectionVectors {
		if len(vector.Values) == candidateCount {
			continue
		}
		fieldName := vector.Field.Field
		if vector.Field.Index != "" {
			fieldName = vector.Field.Index + "." + vector.Field.Field
		}
		diagnostics = append(diagnostics, ErrorDiagnostic(
			DiagnosticInternalInvariant,
			PhaseExecute,
			fmt.Sprintf("projection vector %s has %d values for %d rownums", fieldName, len(vector.Values), candidateCount),
		))
	}
	return diagnostics
}

// ToResultChunk zips visible projection vectors into protocol-neutral rows.
func (r QuantaProjectedRowSet) ToResultChunk(sequence int, final bool) (ResultChunk, DiagnosticSet) {
	diagnostics := r.ValidateShape()
	if diagnostics.BlocksNative() {
		return ResultChunk{}, diagnostics
	}
	visibleVectors := r.visibleProjectionVectors()
	rows := make([]ResultRow, r.CandidateCount())
	for rowIndex := range rows {
		row := make(ResultRow, len(visibleVectors))
		for columnIndex, vector := range visibleVectors {
			row[columnIndex] = vector.Values[rowIndex]
		}
		rows[rowIndex] = row
	}
	return ResultChunk{Rows: rows, Sequence: sequence, Final: final}, nil
}

func (r QuantaProjectedRowSet) visibleProjectionVectors() []QuantaProjectionVector {
	vectors := make([]QuantaProjectionVector, 0, len(r.ProjectionVectors))
	for _, vector := range r.ProjectionVectors {
		if vector.Field.Visible {
			vectors = append(vectors, vector)
		}
	}
	return vectors
}

// QuantaQueryFragment is a dependency-light fragment of Quanta's bitmap query dialect.
//
// It intentionally avoids importing the legacy shared or gRPC packages. Runtime
// adapters can translate this shape into in-process, gRPC, or future transport
// payloads without making qsbridge depend on those implementations.
type QuantaQueryFragment struct {
	Index                string
	Role                 TableInstanceID
	Field                string
	Operation            QuantaFragmentOperation
	BSIOp                QuantaBSIOp
	Value                *big.Int
	Values               []*big.Int
	Begin                *big.Int
	End                  *big.Int
	Literal              LiteralExpr
	Literals             []LiteralExpr
	BeginLiteral         LiteralExpr
	EndLiteral           LiteralExpr
	HasLiteralRange      bool
	HasLiteral           bool
	RangeCoalesceAllowed bool
	// ShardWindow marks a physical time-shard window fragment. Ordinary
	// timestamp predicates compare encoded BSI values but must not move the
	// legacy global FromTime/ToTime window.
	ShardWindow bool
	Negate      bool
	NullCheck   bool
}

// QuantaSeedKind names a set-producing physical seed request.
type QuantaSeedKind string

const (
	// QuantaSeedTableExistence asks the runtime for the table rownum existence set.
	QuantaSeedTableExistence QuantaSeedKind = "table_existence"
)

// QuantaSeed is an explicit set-producing request in the physical Quanta dialect.
//
// Seeds are not user predicates. They describe planner/runtime intent such as
// "start this scan from the table existence bitmap" so adapters can choose a
// cheap backend primitive instead of manufacturing an all-range predicate.
type QuantaSeed struct {
	Index       string
	Role        TableInstanceID
	Field       string
	Kind        QuantaSeedKind
	Begin       *big.Int
	End         *big.Int
	ShardWindow bool
}

// QuantaFilterOperation names one node in a grouped bitmap filter tree.
type QuantaFilterOperation string

const (
	// QuantaFilterLeaf wraps one node-facing bitmap query fragment.
	QuantaFilterLeaf QuantaFilterOperation = "LEAF"
	// QuantaFilterCandidateSet wraps a precomputed candidate set in target-domain rownums.
	QuantaFilterCandidateSet QuantaFilterOperation = "CANDIDATE_SET"
	// QuantaFilterIntersect combines child filters with AND-style semantics.
	QuantaFilterIntersect QuantaFilterOperation = "INTERSECT"
	// QuantaFilterUnion combines child filters with OR-style semantics.
	QuantaFilterUnion QuantaFilterOperation = "UNION"
)

// QuantaFilterExpression preserves grouped boolean predicate structure for bitmap execution.
//
// The legacy flat fragment list cannot distinguish `(a OR b) AND c` from
// `a OR (b AND c)`. This tree keeps grouping explicit while the existing flat
// fragment path remains available for simple predicate chains.
type QuantaFilterExpression struct {
	Operation    QuantaFilterOperation
	Fragment     QuantaQueryFragment
	CandidateSet QuantaCandidateSet
	Children     []QuantaFilterExpression
}

// QuantaFilterDomainSummary describes the table rownum domains touched by a
// grouped filter expression before any relationship-vector normalization.
type QuantaFilterDomainSummary struct {
	Domains []string
}

// QuantaFilterDomainTranslation describes a required rownum-domain normalization.
//
// Mixed-domain grouped filters cannot be combined directly: each leaf returns
// rownums in its own table domain. A future planner/executor step must translate
// source domains through relationship-vector storage into the chosen target
// domain before bitmap UNION/INTERSECT/DIFFERENCE can be applied correctly.
type QuantaFilterDomainTranslation struct {
	Required      bool
	SourceDomains []string
	TargetDomain  string
	Strategies    []PhysicalStrategy
}

// FilterDomainNormalizationOperation describes why rownum-domain translation is needed.
type FilterDomainNormalizationOperation string

const (
	// FilterDomainNormalizeGroupedFilter normalizes grouped predicate leaves before bitmap boolean algebra.
	FilterDomainNormalizeGroupedFilter FilterDomainNormalizationOperation = "grouped_filter"
)

// FilterDomainNormalizationPlan describes the executor-facing normalization work for a filter.
type FilterDomainNormalizationPlan struct {
	Operation   FilterDomainNormalizationOperation
	Translation QuantaFilterDomainTranslation
	Requests    []FilterDomainNormalizationRequest
}

// FilterDomainNormalizationRequest is one source-to-target rownum-domain translation.
type FilterDomainNormalizationRequest struct {
	Operation        FilterDomainNormalizationOperation
	Source           QuantaCandidateSet
	SourceDomain     string
	TargetDomain     string
	RelationshipPath []RelationshipJoinPlanEdge
	Strategy         PhysicalStrategy
}

// FilterDomainRelationshipVectorDirection names which side of a relationship edge drives translation.
type FilterDomainRelationshipVectorDirection string

const (
	// FilterDomainRelationshipVectorDirectionUnknown means source/target do not match the edge endpoints.
	FilterDomainRelationshipVectorDirectionUnknown FilterDomainRelationshipVectorDirection = "unknown"
	// FilterDomainRelationshipVectorDirectionLeftToRight translates left-edge rownums to right-edge rownums.
	FilterDomainRelationshipVectorDirectionLeftToRight FilterDomainRelationshipVectorDirection = "left_to_right"
	// FilterDomainRelationshipVectorDirectionRightToLeft translates right-edge rownums to left-edge rownums.
	FilterDomainRelationshipVectorDirectionRightToLeft FilterDomainRelationshipVectorDirection = "right_to_left"
)

// FilterDomainRelationshipVectorRequest is one concrete source-to-target rownum translation.
type FilterDomainRelationshipVectorRequest struct {
	Operation        FilterDomainNormalizationOperation
	SourceFragment   QuantaQueryFragment
	SourceCandidates QuantaCandidateSet
	SourceDomain     string
	TargetDomain     string
	Edge             RelationshipJoinPlanEdge
	Direction        FilterDomainRelationshipVectorDirection
	Strategy         PhysicalStrategy
}

// FilterDomainRelationshipVectorResult carries translated target-domain candidates.
type FilterDomainRelationshipVectorResult struct {
	Request                    FilterDomainRelationshipVectorRequest
	TargetCandidates           QuantaCandidateSet
	VectorIndex                string
	VectorField                string
	Direction                  FilterDomainRelationshipVectorDirection
	ProjectionElapsed          time.Duration
	ProjectionCacheHit         bool
	SourceKeyProjectionUsed    bool
	SourceKeyProjectionReason  string
	SourceKeyProjectionElapsed time.Duration
	SourceValueCount           int
	CandidateCacheHit          bool
	CandidateCacheMode         string
	CandidateMode              string
	CandidateElapsed           time.Duration
	BatchEqualElapsed          time.Duration
	CandidateScanElapsed       time.Duration
}

// RelationshipVectorRequest derives a concrete one-hop vector translation request.
func (r FilterDomainNormalizationRequest) RelationshipVectorRequest(fragment QuantaQueryFragment, candidates QuantaCandidateSet) (FilterDomainRelationshipVectorRequest, bool) {
	if r.Strategy != PhysicalStrategyRelationshipVectorNormalization || len(r.RelationshipPath) != 1 {
		return FilterDomainRelationshipVectorRequest{}, false
	}
	direction, ok := r.RelationshipVectorDirection()
	if !ok {
		return FilterDomainRelationshipVectorRequest{}, false
	}
	if candidates.Index == "" {
		candidates.Index = r.SourceDomain
	}
	return FilterDomainRelationshipVectorRequest{
		Operation:        r.Operation,
		SourceFragment:   fragment,
		SourceCandidates: candidates,
		SourceDomain:     r.SourceDomain,
		TargetDomain:     r.TargetDomain,
		Edge:             r.RelationshipPath[0],
		Direction:        direction,
		Strategy:         r.Strategy,
	}, true
}

// RelationshipVectorDirection derives the source-to-target direction for the one-hop relationship path.
func (r FilterDomainNormalizationRequest) RelationshipVectorDirection() (FilterDomainRelationshipVectorDirection, bool) {
	if len(r.RelationshipPath) != 1 {
		return FilterDomainRelationshipVectorDirectionUnknown, false
	}
	edge := r.RelationshipPath[0]
	left := strings.ToLower(edge.Left.Table.Table)
	right := strings.ToLower(edge.Right.Table.Table)
	source := strings.ToLower(r.SourceDomain)
	target := strings.ToLower(r.TargetDomain)
	switch {
	case source == left && target == right:
		return FilterDomainRelationshipVectorDirectionLeftToRight, true
	case source == right && target == left:
		return FilterDomainRelationshipVectorDirectionRightToLeft, true
	default:
		return FilterDomainRelationshipVectorDirectionUnknown, false
	}
}

// LeafName returns a stable source leaf label for diagnostics and fixture keys.
func (r FilterDomainRelationshipVectorRequest) LeafName() string {
	return r.SourceFragment.Index + "." + r.SourceFragment.Field
}

// FilterDomainNormalizedLeaf is one translated predicate leaf in target-domain rownums.
type FilterDomainNormalizedLeaf struct {
	OriginalFragment           QuantaQueryFragment
	SourceDomain               string
	TargetDomain               string
	VectorIndex                string
	VectorField                string
	Direction                  FilterDomainRelationshipVectorDirection
	SourceCount                int
	SourceElapsed              time.Duration
	TranslationElapsed         time.Duration
	ProjectionElapsed          time.Duration
	ProjectionCacheHit         bool
	SourceKeyProjectionUsed    bool
	SourceKeyProjectionReason  string
	SourceKeyProjectionElapsed time.Duration
	SourceValueCount           int
	CandidateCacheHit          bool
	CandidateCacheMode         string
	CandidateMode              string
	CandidateElapsed           time.Duration
	BatchEqualElapsed          time.Duration
	CandidateScanElapsed       time.Duration
	CandidateSet               QuantaCandidateSet
}

// FilterDomainNormalizedBranch is one translated source-domain filter subtree.
type FilterDomainNormalizedBranch struct {
	OriginalFilter             QuantaFilterExpression
	SourceDomain               string
	TargetDomain               string
	VectorIndex                string
	VectorField                string
	Direction                  FilterDomainRelationshipVectorDirection
	SourceCount                int
	SourceElapsed              time.Duration
	TranslationElapsed         time.Duration
	ProjectionElapsed          time.Duration
	ProjectionCacheHit         bool
	SourceKeyProjectionUsed    bool
	SourceKeyProjectionReason  string
	SourceKeyProjectionElapsed time.Duration
	SourceValueCount           int
	CandidateCacheHit          bool
	CandidateCacheMode         string
	CandidateMode              string
	CandidateElapsed           time.Duration
	BatchEqualElapsed          time.Duration
	CandidateScanElapsed       time.Duration
	CandidateSet               QuantaCandidateSet
}

// FilterDomainRewriteResult describes typed replacements for normalized filter leaves.
type FilterDomainRewriteResult struct {
	TargetDomain string
	Branches     []FilterDomainNormalizedBranch
	Leaves       []FilterDomainNormalizedLeaf
}

// Apply rewrites matching source-domain leaves as target-domain candidate-set leaves.
func (r FilterDomainRewriteResult) Apply(filter QuantaFilterExpression) QuantaFilterExpression {
	for _, branch := range r.Branches {
		if filterDomainExpressionMatches(filter, branch.OriginalFilter) {
			candidateSet := branch.CandidateSet
			if candidateSet.Index == "" {
				candidateSet.Index = branch.TargetDomain
			}
			return QuantaFilterExpression{
				Operation:    QuantaFilterCandidateSet,
				CandidateSet: candidateSet,
			}
		}
	}
	if filter.Operation == QuantaFilterIntersect {
		if rewritten, ok := r.applyIntersectBranchSubset(filter); ok {
			return rewritten
		}
	}
	if filter.Leaf() {
		for _, leaf := range r.Leaves {
			if filterDomainFragmentMatches(filter.Fragment, leaf.OriginalFragment) {
				candidateSet := leaf.CandidateSet
				if candidateSet.Index == "" {
					candidateSet.Index = leaf.TargetDomain
				}
				return QuantaFilterExpression{
					Operation:    QuantaFilterCandidateSet,
					CandidateSet: candidateSet,
				}
			}
		}
		return filter
	}
	if len(filter.Children) == 0 {
		return filter
	}
	children := make([]QuantaFilterExpression, 0, len(filter.Children))
	for _, child := range filter.Children {
		children = append(children, r.Apply(child))
	}
	filter.Children = children
	return filterDomainFactorCommonUnionConjuncts(filter)
}

func filterDomainFactorCommonUnionConjuncts(filter QuantaFilterExpression) QuantaFilterExpression {
	if filter.Operation != QuantaFilterUnion || len(filter.Children) < 2 {
		return filter
	}
	branches := make([][]QuantaFilterExpression, 0, len(filter.Children))
	for _, child := range filter.Children {
		conjuncts := filterDomainConjunctsForFactoring(child)
		if len(conjuncts) == 0 {
			return filter
		}
		branches = append(branches, conjuncts)
	}
	common := filterDomainCommonConjuncts(branches)
	if len(common) == 0 {
		return filter
	}
	unionChildren := make([]QuantaFilterExpression, 0, len(branches))
	for _, branch := range branches {
		remaining := filterDomainRemoveConjuncts(branch, common)
		if len(remaining) == 0 {
			return filter
		}
		unionChildren = append(unionChildren, filterDomainConjunctExpression(remaining))
	}
	children := make([]QuantaFilterExpression, 0, len(common)+1)
	children = append(children, common...)
	children = append(children, QuantaFilterExpression{
		Operation: QuantaFilterUnion,
		Children:  unionChildren,
	})
	return QuantaFilterExpression{
		Operation: QuantaFilterIntersect,
		Children:  children,
	}
}

func filterDomainConjunctsForFactoring(filter QuantaFilterExpression) []QuantaFilterExpression {
	if filter.Empty() {
		return nil
	}
	if filter.Operation != QuantaFilterIntersect {
		return []QuantaFilterExpression{filter}
	}
	var conjuncts []QuantaFilterExpression
	for _, child := range filter.Children {
		conjuncts = append(conjuncts, filterDomainConjunctsForFactoring(child)...)
	}
	return conjuncts
}

func filterDomainCommonConjuncts(branches [][]QuantaFilterExpression) []QuantaFilterExpression {
	if len(branches) == 0 {
		return nil
	}
	common := make([]QuantaFilterExpression, 0, len(branches[0]))
	for _, candidate := range branches[0] {
		if filterDomainExpressionMatchesAny(candidate, common) {
			continue
		}
		inAllBranches := true
		for _, branch := range branches[1:] {
			if !filterDomainExpressionMatchesAny(candidate, branch) {
				inAllBranches = false
				break
			}
		}
		if inAllBranches {
			common = append(common, candidate)
		}
	}
	return common
}

func filterDomainRemoveConjuncts(branch, remove []QuantaFilterExpression) []QuantaFilterExpression {
	remaining := make([]QuantaFilterExpression, 0, len(branch))
	removed := make([]bool, len(remove))
	for _, conjunct := range branch {
		removeIndex := filterDomainFirstUnremovedConjunctMatch(conjunct, remove, removed)
		if removeIndex >= 0 {
			removed[removeIndex] = true
			continue
		}
		remaining = append(remaining, conjunct)
	}
	return remaining
}

func filterDomainFirstUnremovedConjunctMatch(conjunct QuantaFilterExpression, candidates []QuantaFilterExpression, removed []bool) int {
	for i, candidate := range candidates {
		if removed[i] {
			continue
		}
		if filterDomainExpressionMatches(conjunct, candidate) {
			return i
		}
	}
	return -1
}

func filterDomainConjunctExpression(conjuncts []QuantaFilterExpression) QuantaFilterExpression {
	if len(conjuncts) == 0 {
		return QuantaFilterExpression{}
	}
	if len(conjuncts) == 1 {
		return conjuncts[0]
	}
	return QuantaFilterExpression{
		Operation: QuantaFilterIntersect,
		Children:  conjuncts,
	}
}

func (r FilterDomainRewriteResult) applyIntersectBranchSubset(filter QuantaFilterExpression) (QuantaFilterExpression, bool) {
	for _, branch := range r.Branches {
		if branch.OriginalFilter.Operation != QuantaFilterIntersect || len(branch.OriginalFilter.Children) == 0 {
			continue
		}
		if !filterDomainConjunctiveTreeContainsAll(filter, branch.OriginalFilter.Children) {
			continue
		}
		candidateSet := branch.CandidateSet
		if candidateSet.Index == "" {
			candidateSet.Index = branch.TargetDomain
		}
		candidateLeaf := QuantaFilterExpression{
			Operation:    QuantaFilterCandidateSet,
			CandidateSet: candidateSet,
		}
		pruned := filterDomainPruneConjunctiveBranchChildren(filter, branch.OriginalFilter.Children)
		remaining := filterDomainPrunedChildren(pruned)
		children := make([]QuantaFilterExpression, 0, len(remaining)+1)
		children = append(children, candidateLeaf)
		for _, child := range remaining {
			children = append(children, r.Apply(child))
		}
		filter.Children = children
		return filter, true
	}
	return filter, false
}

func filterDomainConjunctiveTreeContainsAll(filter QuantaFilterExpression, branchChildren []QuantaFilterExpression) bool {
	matched := make([]bool, len(branchChildren))
	filterDomainMarkConjunctiveBranchMatches(filter, branchChildren, matched)
	for _, ok := range matched {
		if !ok {
			return false
		}
	}
	return true
}

func filterDomainMarkConjunctiveBranchMatches(filter QuantaFilterExpression, branchChildren []QuantaFilterExpression, matched []bool) {
	for i := range branchChildren {
		if matched[i] {
			continue
		}
		if filterDomainExpressionMatches(filter, branchChildren[i]) {
			matched[i] = true
			return
		}
	}
	if filter.Operation != QuantaFilterIntersect {
		return
	}
	for _, child := range filter.Children {
		filterDomainMarkConjunctiveBranchMatches(child, branchChildren, matched)
	}
}

func filterDomainPruneConjunctiveBranchChildren(filter QuantaFilterExpression, branchChildren []QuantaFilterExpression) QuantaFilterExpression {
	if filterDomainExpressionMatchesAny(filter, branchChildren) {
		return QuantaFilterExpression{}
	}
	if filter.Operation != QuantaFilterIntersect || len(filter.Children) == 0 {
		return filter
	}
	children := make([]QuantaFilterExpression, 0, len(filter.Children))
	for _, child := range filter.Children {
		pruned := filterDomainPruneConjunctiveBranchChildren(child, branchChildren)
		if pruned.Empty() {
			continue
		}
		children = append(children, pruned)
	}
	filter.Children = children
	if len(filter.Children) == 0 {
		return QuantaFilterExpression{}
	}
	if len(filter.Children) == 1 {
		return filter.Children[0]
	}
	return filter
}

func filterDomainPrunedChildren(filter QuantaFilterExpression) []QuantaFilterExpression {
	if filter.Empty() {
		return nil
	}
	if filter.Operation == QuantaFilterIntersect {
		return filter.Children
	}
	return []QuantaFilterExpression{filter}
}

func filterDomainExpressionMatchesAny(filter QuantaFilterExpression, candidates []QuantaFilterExpression) bool {
	for _, candidate := range candidates {
		if filterDomainExpressionMatches(filter, candidate) {
			return true
		}
	}
	return false
}

func filterDomainExpressionMatches(left, right QuantaFilterExpression) bool {
	if left.Operation != right.Operation {
		return false
	}
	if left.Leaf() || right.Leaf() {
		return left.Leaf() && right.Leaf() && filterDomainFragmentMatches(left.Fragment, right.Fragment)
	}
	if left.CandidateSetLeaf() || right.CandidateSetLeaf() {
		return left.CandidateSetLeaf() && right.CandidateSetLeaf() && reflect.DeepEqual(left.CandidateSet, right.CandidateSet)
	}
	if len(left.Children) != len(right.Children) {
		return false
	}
	for i := range left.Children {
		if !filterDomainExpressionMatches(left.Children[i], right.Children[i]) {
			return false
		}
	}
	return true
}

func filterDomainFragmentMatches(left, right QuantaQueryFragment) bool {
	return left.Index == right.Index &&
		left.Role == right.Role &&
		left.Field == right.Field &&
		left.Operation == right.Operation &&
		left.BSIOp == right.BSIOp &&
		left.Negate == right.Negate &&
		left.NullCheck == right.NullCheck &&
		left.HasLiteral == right.HasLiteral &&
		left.HasLiteralRange == right.HasLiteralRange &&
		quantaBigIntPtrEqual(left.Value, right.Value) &&
		quantaBigIntPtrSlicesEqual(left.Values, right.Values) &&
		quantaBigIntPtrEqual(left.Begin, right.Begin) &&
		quantaBigIntPtrEqual(left.End, right.End) &&
		reflect.DeepEqual(left.Literal, right.Literal) &&
		reflect.DeepEqual(left.Literals, right.Literals) &&
		reflect.DeepEqual(left.BeginLiteral, right.BeginLiteral) &&
		reflect.DeepEqual(left.EndLiteral, right.EndLiteral)
}

func quantaBigIntPtrEqual(left, right *big.Int) bool {
	switch {
	case left == nil && right == nil:
		return true
	case left == nil || right == nil:
		return false
	default:
		return left.Cmp(right) == 0
	}
}

func quantaBigIntPtrSlicesEqual(left, right []*big.Int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !quantaBigIntPtrEqual(left[i], right[i]) {
			return false
		}
	}
	return true
}

// Required reports whether any source domain must be translated.
func (p FilterDomainNormalizationPlan) Required() bool {
	return p.Translation.Required && len(p.Requests) > 0
}

// NormalizationPlan returns executor-facing source-to-target requests for this translation.
func (t QuantaFilterDomainTranslation) NormalizationPlan(operation FilterDomainNormalizationOperation, relationshipPlans ...RelationshipJoinPlan) FilterDomainNormalizationPlan {
	plan := FilterDomainNormalizationPlan{
		Operation:   operation,
		Translation: t,
	}
	if !t.Required {
		return plan
	}
	strategy := PhysicalStrategyRelationshipVectorNormalization
	if len(t.Strategies) > 0 {
		strategy = t.Strategies[0]
	}
	for _, sourceDomain := range t.SourceDomains {
		if sourceDomain == "" || sourceDomain == t.TargetDomain {
			continue
		}
		plan.Requests = append(plan.Requests, FilterDomainNormalizationRequest{
			Operation:    operation,
			Source:       QuantaCandidateSet{Index: sourceDomain},
			SourceDomain: sourceDomain,
			TargetDomain: t.TargetDomain,
			RelationshipPath: filterDomainNormalizationPath(
				sourceDomain,
				t.TargetDomain,
				relationshipPlans,
			),
			Strategy: strategy,
		})
	}
	return plan
}

func filterDomainNormalizationPath(sourceDomain, targetDomain string, relationshipPlans []RelationshipJoinPlan) []RelationshipJoinPlanEdge {
	for _, plan := range relationshipPlans {
		for _, edge := range plan.Edges {
			if edge.ExecutionKind != RelationshipJoinExecutionVector {
				continue
			}
			if filterDomainRelationshipEdgeMatches(edge, sourceDomain, targetDomain) {
				return []RelationshipJoinPlanEdge{edge}
			}
		}
	}
	return nil
}

func filterDomainRelationshipEdgeMatches(edge RelationshipJoinPlanEdge, sourceDomain, targetDomain string) bool {
	left := strings.ToLower(edge.Left.Table.Table)
	right := strings.ToLower(edge.Right.Table.Table)
	source := strings.ToLower(sourceDomain)
	target := strings.ToLower(targetDomain)
	return (left == source && right == target) || (left == target && right == source)
}

// Mixed reports whether the filter spans more than one rownum domain.
func (s QuantaFilterDomainSummary) Mixed() bool {
	return len(s.Domains) > 1
}

// Single returns the only rownum domain when the filter is single-domain.
func (s QuantaFilterDomainSummary) Single() (string, bool) {
	if len(s.Domains) != 1 {
		return "", false
	}
	return s.Domains[0], true
}

// TranslationRequirement returns the relationship-vector normalization needed for this summary.
func (s QuantaFilterDomainSummary) TranslationRequirement(targetDomain string) QuantaFilterDomainTranslation {
	requirement := QuantaFilterDomainTranslation{
		SourceDomains: append([]string(nil), s.Domains...),
		TargetDomain:  targetDomain,
	}
	if !s.Mixed() {
		return requirement
	}
	requirement.Required = true
	requirement.Strategies = []PhysicalStrategy{PhysicalStrategyRelationshipVectorNormalization}
	return requirement
}

// Empty reports whether no grouped filter expression is present.
func (e QuantaFilterExpression) Empty() bool {
	return e.Operation == ""
}

// Leaf reports whether the expression wraps one bitmap fragment.
func (e QuantaFilterExpression) Leaf() bool {
	return e.Operation == QuantaFilterLeaf
}

// CandidateSetLeaf reports whether the expression wraps precomputed target-domain rownums.
func (e QuantaFilterExpression) CandidateSetLeaf() bool {
	return e.Operation == QuantaFilterCandidateSet
}

// DomainSummary returns the rownum domains touched by the filter tree.
func (e QuantaFilterExpression) DomainSummary() QuantaFilterDomainSummary {
	domains := make(map[string]bool)
	e.collectDomains(domains)
	result := make([]string, 0, len(domains))
	for domain := range domains {
		result = append(result, domain)
	}
	sort.Strings(result)
	return QuantaFilterDomainSummary{Domains: result}
}

func (e QuantaFilterExpression) collectDomains(domains map[string]bool) {
	if e.Empty() {
		return
	}
	if e.Leaf() {
		if e.Fragment.Index != "" {
			domains[e.Fragment.Index] = true
		}
		return
	}
	if e.CandidateSetLeaf() {
		if e.CandidateSet.Index != "" {
			domains[e.CandidateSet.Index] = true
		}
		return
	}
	for _, child := range e.Children {
		child.collectDomains(domains)
	}
}

// CacheIdentity returns a deterministic identity for this physical fragment and version boundary.
func (f QuantaQueryFragment) CacheIdentity(boundary QuantaFragmentCacheBoundary) QuantaFragmentCacheIdentity {
	identity := QuantaFragmentCacheIdentity{
		Index:             f.Index,
		Field:             f.Field,
		Operation:         f.Operation,
		BSIOp:             f.BSIOp,
		Args:              f.cacheArgs(),
		LogicalShard:      boundary.LogicalShard,
		Replica:           boundary.Replica,
		ReplicaGeneration: boundary.ReplicaGeneration,
		ShardEpoch:        boundary.ShardEpoch,
		IndexEpoch:        boundary.IndexEpoch,
		CatalogVersion:    boundary.CatalogVersion,
		DictionaryVersion: boundary.DictionaryVersion,
		SnapshotID:        boundary.SnapshotID,
	}
	identity.Digest = identity.digest()
	return identity
}

func (f QuantaQueryFragment) cacheArgs() []string {
	args := []string{
		"value:" + quantaFragmentBigIntString(f.Value),
		"begin:" + quantaFragmentBigIntString(f.Begin),
		"end:" + quantaFragmentBigIntString(f.End),
		"literal:" + quantaFragmentLiteralString(f.Literal, f.HasLiteral),
		"begin_literal:" + quantaFragmentLiteralString(f.BeginLiteral, f.HasLiteralRange),
		"end_literal:" + quantaFragmentLiteralString(f.EndLiteral, f.HasLiteralRange),
		"shard_window:" + boolCacheValue(f.ShardWindow),
		"negate:" + boolCacheValue(f.Negate),
		"null_check:" + boolCacheValue(f.NullCheck),
	}
	values := make([]string, 0, len(f.Values))
	for _, value := range f.Values {
		values = append(values, quantaFragmentBigIntString(value))
	}
	sort.Strings(values)
	for _, value := range values {
		args = append(args, "values:"+value)
	}
	literals := make([]string, 0, len(f.Literals))
	for _, literal := range f.Literals {
		literals = append(literals, quantaFragmentLiteralString(literal, true))
	}
	sort.Strings(literals)
	for _, literal := range literals {
		args = append(args, "literals:"+literal)
	}
	return args
}

func (i QuantaFragmentCacheIdentity) digest() string {
	var b strings.Builder
	writeCachePart(&b, "index", i.Index)
	writeCachePart(&b, "field", i.Field)
	writeCachePart(&b, "operation", string(i.Operation))
	writeCachePart(&b, "bsi_op", string(i.BSIOp))
	for _, arg := range i.Args {
		writeCachePart(&b, "arg", arg)
	}
	writeCachePart(&b, "logical_shard", string(i.LogicalShard))
	writeCachePart(&b, "replica", string(i.Replica))
	writeCachePart(&b, "replica_generation", string(i.ReplicaGeneration))
	writeCachePart(&b, "shard_epoch", string(i.ShardEpoch))
	writeCachePart(&b, "index_epoch", string(i.IndexEpoch))
	writeCachePart(&b, "catalog_version", string(i.CatalogVersion))
	writeCachePart(&b, "dictionary_version", string(i.DictionaryVersion))
	writeCachePart(&b, "snapshot_id", string(i.SnapshotID))

	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func quantaFragmentBigIntString(value *big.Int) string {
	if value == nil {
		return "<nil>"
	}
	return value.String()
}

func quantaFragmentLiteralString(value LiteralExpr, ok bool) string {
	if !ok {
		return "<nil>"
	}
	return string(value.Kind) + ":" + fmt.Sprint(value.Value)
}

// QuantaIntermediateQuery is the physical Quanta bitmap-query dialect produced by qsbridge.
//
// It is not qsbridge's logical IR. QueryIR stays as the planner-owned
// representation; this type marks the handoff into Quanta-specific execution
// primitives while still abstracting away legacy gRPC/runtime packages.
type QuantaIntermediateQuery struct {
	Fragments        []QuantaQueryFragment
	Seeds            []QuantaSeed
	Filter           QuantaFilterExpression
	ProjectionFields []QuantaProjectionField
}

// QuantaIntermediateLowerer converts supported execution requests into Quanta predicates.
type QuantaIntermediateLowerer struct {
	Dictionaries DictionaryResolver
}

// LowerExecutionRequest converts a supported qsbridge request to Quanta fragments.
func (l QuantaIntermediateLowerer) LowerExecutionRequest(request ExecutionRequest) (QuantaIntermediateQuery, DiagnosticSet) {
	if !request.SupportedForExecution() {
		return QuantaIntermediateQuery{}, request.Diagnostics
	}
	query := request.Bound.Prepared.Query
	if query.Kind != QueryKindSelect {
		mutationQuery, ok := quantaIntermediateMutationPredicateQuery(query)
		if !ok {
			return QuantaIntermediateQuery{}, nil
		}
		query = mutationQuery
	}
	return l.LowerQuery(query, request.Bound.Parameters)
}

func quantaIntermediateMutationPredicateQuery(query QueryIR) (QueryIR, bool) {
	switch query.Mutation.Kind {
	case MutationUpdate, MutationDelete:
		mutationQuery := query
		mutationQuery.Kind = QueryKindSelect
		mutationQuery.Predicates = append([]Predicate(nil), query.Mutation.Predicates...)
		mutationQuery.WhereExpr = nil
		return mutationQuery, true
	case MutationInsert, MutationTruncate, MutationCreateTable, MutationDropTable:
		return QueryIR{}, false
	default:
		return QueryIR{}, false
	}
}

// LowerQuery converts a bound query to Quanta fragments without enforcing execution support.
//
// This is useful for inspection and planner development: unsupported blockers
// can remain on QueryIR while qsbridge still exposes the shape it would lower.
func (l QuantaIntermediateLowerer) LowerQuery(query QueryIR, parameters ParameterBindingSet) (QuantaIntermediateQuery, DiagnosticSet) {
	if query.Kind != QueryKindSelect {
		return QuantaIntermediateQuery{}, quantaIntermediateDiagnostics("only SELECT requests can be lowered")
	}
	if len(query.Sources) != 1 && len(query.Joins) == 0 && len(query.Memberships) == 0 {
		return QuantaIntermediateQuery{}, quantaIntermediateDiagnostics("only one-table queries can be lowered")
	}

	fragments := make([]QuantaQueryFragment, 0, len(query.Predicates))
	for _, predicate := range query.Predicates {
		if predicate.Placement == PredicateResidualScan {
			continue
		}
		fragment, diagnostics, ok := l.lowerPredicate(predicate, parameters)
		if !ok {
			return QuantaIntermediateQuery{}, diagnostics
		}
		fragments = append(fragments, fragment)
	}
	fragments = quantaIntermediateCoalesceRanges(fragments)
	filter, diagnostics, ok := l.lowerFilterExpression(query.WhereExpr, parameters)
	if !ok {
		return QuantaIntermediateQuery{}, diagnostics
	}
	return QuantaIntermediateQuery{
		Fragments:        fragments,
		Filter:           filter,
		ProjectionFields: quantaIntermediateProjectionFields(query),
	}, nil
}

func (l QuantaIntermediateLowerer) lowerFilterExpression(expr Expr, parameters ParameterBindingSet) (QuantaFilterExpression, DiagnosticSet, bool) {
	if expr == nil {
		return QuantaFilterExpression{}, nil, true
	}
	if binary, ok := quantaIntermediateBinaryExpr(expr); ok {
		switch binary.Op {
		case BinaryOpAnd, BinaryOpOr:
			left, leftDiagnostics, ok := l.lowerFilterExpression(binary.Left, parameters)
			if !ok {
				return QuantaFilterExpression{}, leftDiagnostics, false
			}
			right, rightDiagnostics, ok := l.lowerFilterExpression(binary.Right, parameters)
			if !ok {
				return QuantaFilterExpression{}, rightDiagnostics, false
			}
			operation := QuantaFilterIntersect
			if binary.Op == BinaryOpOr {
				operation = QuantaFilterUnion
			}
			return QuantaFilterExpression{
				Operation: operation,
				Children:  []QuantaFilterExpression{left, right},
			}, nil, true
		}
	}
	fragment, diagnostics, ok := l.lowerPredicate(Predicate{
		Expr:      expr,
		Scope:     PredicateScopeWhere,
		Placement: PredicatePushdown,
	}, parameters)
	if !ok {
		return QuantaFilterExpression{}, diagnostics, false
	}
	return QuantaFilterExpression{
		Operation: QuantaFilterLeaf,
		Fragment:  fragment,
	}, nil, true
}

func (l QuantaIntermediateLowerer) lowerPredicate(predicate Predicate, parameters ParameterBindingSet) (QuantaQueryFragment, DiagnosticSet, bool) {
	if fragment, diagnostics, ok := l.lowerBetweenPredicate(predicate, parameters); ok || diagnostics.BlocksNative() {
		return quantaIntermediateApplyCombinator(fragment, predicate), diagnostics, ok
	}
	if fragment, diagnostics, ok := l.lowerStringEnumPredicate(predicate, parameters); ok || diagnostics.BlocksNative() {
		return quantaIntermediateApplyCombinator(fragment, predicate), diagnostics, ok
	}
	if fragment, diagnostics, ok := l.lowerInPredicate(predicate, parameters); ok || diagnostics.BlocksNative() {
		return quantaIntermediateApplyCombinator(fragment, predicate), diagnostics, ok
	}
	op, field, valueExpr, ok := quantaIntermediateComparisonParts(predicate)
	if !ok {
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("only field-to-value comparison and IN predicates can be lowered"), false
	}
	value, diagnostics, ok := quantaIntermediateValue(valueExpr, parameters)
	if !ok {
		return QuantaQueryFragment{}, diagnostics, false
	}
	if value.Kind == ValueNull {
		fragment, diagnostics, ok := quantaIntermediateNullCheckFragment(op, field)
		return quantaIntermediateApplyCombinator(fragment, predicate), diagnostics, ok
	}
	if field.Encoding.Kind == EncodingStringLexBSI {
		fragment, diagnostics, ok := quantaIntermediateStringLexBSIComparisonFragment(op, field, value)
		return quantaIntermediateApplyCombinator(fragment, predicate), diagnostics, ok
	}
	if field.Type == DataTypeBool && field.Encoding.LegacyName == "BoolDirect" {
		fragment, diagnostics, ok := quantaIntermediateBoolDirectComparisonFragment(op, field, value)
		return quantaIntermediateApplyCombinator(fragment, predicate), diagnostics, ok
	}
	if field.Index != IndexBSI && field.Index != IndexDateTime {
		fragment, diagnostics, ok := quantaIntermediateLiteralComparisonFragment(op, field, value)
		return quantaIntermediateApplyCombinator(fragment, predicate), diagnostics, ok
	}
	literalValue := value
	if field.Index == IndexDateTime {
		normalized, diagnostics, ok := quantaIntermediateNormalizeTimeValue(field, value)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
		op, normalized = quantaIntermediateNormalizeDiscreteTimeComparison(op, field, value, normalized)
		value = normalized
	}
	if field.Encoding.Scale > 0 {
		normalized, diagnostics, ok := quantaIntermediateNormalizeScaledNumericValue(field, value)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
		value = normalized
	}
	if field.Type == DataTypeInt {
		normalizedOp, normalized, diagnostics, ok := quantaIntermediateNormalizeDiscreteNumericComparison(op, value)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
		op = normalizedOp
		value = normalized
	}
	if field.Type == DataTypeBool {
		normalized, diagnostics, ok := quantaIntermediateNormalizeBoolValue(value)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
		value = normalized
	}
	bigValue, ok := quantaIntermediateBigInt(value)
	if !ok {
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("only integer BSI comparison values are lowered in this slice"), false
	}

	fragment := QuantaQueryFragment{
		Index:                field.Table.Table,
		Role:                 quantaIntermediateTableRole(field.Table),
		Field:                quantaIntermediateFieldName(field),
		Operation:            QuantaOperationIntersect,
		BSIOp:                quantaIntermediateBSIOp(op),
		Value:                bigValue,
		Literal:              literalValue,
		HasLiteral:           true,
		RangeCoalesceAllowed: true,
	}
	if op == BinaryOpNotEqual {
		fragment.BSIOp = QuantaBSIOpEQ
		fragment.Operation = QuantaOperationDifference
	}
	return quantaIntermediateApplyCombinator(fragment, predicate), nil, true
}

func (l QuantaIntermediateLowerer) lowerBetweenPredicate(predicate Predicate, parameters ParameterBindingSet) (QuantaQueryFragment, DiagnosticSet, bool) {
	field, lowerExpr, upperExpr, negate, ok := quantaIntermediateBetweenParts(predicate)
	if !ok {
		return QuantaQueryFragment{}, nil, false
	}
	lower, diagnostics, ok := quantaIntermediateValue(lowerExpr, parameters)
	if !ok {
		return QuantaQueryFragment{}, diagnostics, false
	}
	upper, upperDiagnostics, ok := quantaIntermediateValue(upperExpr, parameters)
	if !ok {
		return QuantaQueryFragment{}, upperDiagnostics, false
	}
	beginLiteral := lower
	endLiteral := upper
	if field.Index != IndexBSI && field.Index != IndexDateTime {
		return QuantaQueryFragment{
			Index:           field.Table.Table,
			Role:            quantaIntermediateTableRole(field.Table),
			Field:           quantaIntermediateFieldName(field),
			Operation:       QuantaOperationIntersect,
			BeginLiteral:    lower,
			EndLiteral:      upper,
			HasLiteralRange: true,
			Negate:          negate,
		}, nil, true
	}
	if field.Index == IndexDateTime {
		lower, diagnostics, ok = quantaIntermediateNormalizeTimeValue(field, lower)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
		upper, diagnostics, ok = quantaIntermediateNormalizeTimeValue(field, upper)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
	}
	if field.Encoding.Scale > 0 {
		lower, diagnostics, ok = quantaIntermediateNormalizeScaledNumericValue(field, lower)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
		upper, diagnostics, ok = quantaIntermediateNormalizeScaledNumericValue(field, upper)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
	}
	if field.Encoding.Kind == EncodingStringLexBSI {
		if field.Encoding.NeedsStringRemainderLookup() {
			return QuantaQueryFragment{}, quantaIntermediateDiagnostics("StringLexBSI BETWEEN predicates requiring remainder lookup are not lowered in this slice"), false
		}
		lower, diagnostics, ok = quantaIntermediateNormalizeStringLexBSIValue(field, lower)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
		upper, diagnostics, ok = quantaIntermediateNormalizeStringLexBSIValue(field, upper)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
	}
	begin, ok := quantaIntermediateBigInt(lower)
	if !ok {
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("only integer BSI BETWEEN values are lowered in this slice"), false
	}
	end, ok := quantaIntermediateBigInt(upper)
	if !ok {
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("only integer BSI BETWEEN values are lowered in this slice"), false
	}
	operation := QuantaOperationIntersect
	if negate {
		operation = QuantaOperationDifference
	}
	return QuantaQueryFragment{
		Index:           field.Table.Table,
		Role:            quantaIntermediateTableRole(field.Table),
		Field:           quantaIntermediateFieldName(field),
		Operation:       operation,
		BSIOp:           QuantaBSIOpRange,
		Begin:           begin,
		End:             end,
		BeginLiteral:    beginLiteral,
		EndLiteral:      endLiteral,
		HasLiteralRange: true,
	}, nil, true
}

func quantaIntermediateNormalizeStringLexBSIValue(field FieldRef, value LiteralExpr) (LiteralExpr, DiagnosticSet, bool) {
	if value.Kind != ValueString {
		return LiteralExpr{}, quantaIntermediateDiagnostics("StringLexBSI BETWEEN predicates require string values"), false
	}
	text, ok := value.Value.(string)
	if !ok {
		return LiteralExpr{}, quantaIntermediateDiagnostics("StringLexBSI BETWEEN predicates require string values"), false
	}
	return Literal(ValueInt, quantaIntermediateStringLexBSIValue(text, field.Encoding.PrefixLength)), nil, true
}

func quantaIntermediateApplyCombinator(fragment QuantaQueryFragment, predicate Predicate) QuantaQueryFragment {
	if predicate.Combinator == PredicateCombinatorOr && fragment.Operation == QuantaOperationIntersect {
		fragment.Operation = QuantaOperationUnion
	}
	return fragment
}

func quantaIntermediateNullCheckFragment(op BinaryOp, field FieldRef) (QuantaQueryFragment, DiagnosticSet, bool) {
	switch op {
	case BinaryOpEqual, BinaryOpNotEqual:
	default:
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("only equality predicates are lowered for NULL checks"), false
	}
	return QuantaQueryFragment{
		Index:     field.Table.Table,
		Role:      quantaIntermediateTableRole(field.Table),
		Field:     quantaIntermediateFieldName(field),
		Operation: QuantaOperationIntersect,
		NullCheck: true,
		Negate:    op == BinaryOpNotEqual,
	}, nil, true
}

func quantaIntermediateLiteralComparisonFragment(op BinaryOp, field FieldRef, value LiteralExpr) (QuantaQueryFragment, DiagnosticSet, bool) {
	switch op {
	case BinaryOpEqual, BinaryOpNotEqual:
	default:
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("only equality predicates are lowered for non-BSI fields in this slice"), false
	}
	if quantaIntermediateRownumField(field) {
		rownum, ok := quantaIntermediateBigInt(value)
		if !ok {
			return QuantaQueryFragment{}, quantaIntermediateDiagnostics("rownum predicates require integer values"), false
		}
		return QuantaQueryFragment{
			Index:      field.Table.Table,
			Role:       quantaIntermediateTableRole(field.Table),
			Field:      quantaIntermediateFieldName(field),
			Operation:  quantaIntermediateEqualityOperation(op),
			Values:     []*big.Int{rownum},
			Literal:    value,
			HasLiteral: true,
		}, nil, true
	}
	return QuantaQueryFragment{
		Index:      field.Table.Table,
		Role:       quantaIntermediateTableRole(field.Table),
		Field:      quantaIntermediateFieldName(field),
		Operation:  quantaIntermediateEqualityOperation(op),
		Literal:    value,
		HasLiteral: true,
	}, nil, true
}

func quantaIntermediateRownumField(field FieldRef) bool {
	for _, name := range []string{field.PhysicalName, field.Name} {
		normalized := strings.ToLower(strings.TrimSpace(name))
		if normalized == "rownum" || normalized == "@rownum" || strings.HasSuffix(normalized, "@rownum") {
			return true
		}
	}
	return false
}

func quantaIntermediateBoolDirectComparisonFragment(op BinaryOp, field FieldRef, value LiteralExpr) (QuantaQueryFragment, DiagnosticSet, bool) {
	switch op {
	case BinaryOpEqual, BinaryOpNotEqual:
	default:
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("only equality predicates are lowered for BoolDirect fields"), false
	}
	normalized, diagnostics, ok := quantaIntermediateNormalizeBoolValue(value)
	if !ok {
		return QuantaQueryFragment{}, diagnostics, false
	}
	boolID, ok := quantaIntermediateBigInt(normalized)
	if !ok {
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("BoolDirect predicates require boolean-compatible values"), false
	}
	return QuantaQueryFragment{
		Index:     field.Table.Table,
		Role:      quantaIntermediateTableRole(field.Table),
		Field:     quantaIntermediateFieldName(field),
		Operation: quantaIntermediateEqualityOperation(op),
		Values:    []*big.Int{boolID},
	}, nil, true
}

func quantaIntermediateEqualityOperation(op BinaryOp) QuantaFragmentOperation {
	if op == BinaryOpNotEqual {
		return QuantaOperationDifference
	}
	return QuantaOperationIntersect
}

func quantaIntermediateStringLexBSIComparisonFragment(op BinaryOp, field FieldRef, value LiteralExpr) (QuantaQueryFragment, DiagnosticSet, bool) {
	switch op {
	case BinaryOpEqual, BinaryOpNotEqual:
	default:
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("only equality predicates are lowered for StringLexBSI fields"), false
	}
	if field.Encoding.NeedsStringRemainderLookup() {
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("StringLexBSI predicates requiring remainder lookup are not lowered in this slice"), false
	}
	if value.Kind != ValueString {
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("StringLexBSI predicates require string values"), false
	}
	text, ok := value.Value.(string)
	if !ok {
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("StringLexBSI predicates require string values"), false
	}
	return QuantaQueryFragment{
		Index:     field.Table.Table,
		Role:      quantaIntermediateTableRole(field.Table),
		Field:     quantaIntermediateFieldName(field),
		Operation: quantaIntermediateEqualityOperation(op),
		BSIOp:     QuantaBSIOpEQ,
		Value:     quantaIntermediateStringLexBSIValue(text, field.Encoding.PrefixLength),
	}, nil, true
}

func quantaIntermediateStringLexBSIValue(value string, prefixLength int) *big.Int {
	if prefixLength <= 0 {
		return new(big.Int).SetBytes([]byte(value))
	}
	copied := len(value)
	if copied > prefixLength {
		copied = prefixLength
	}
	if prefixLength <= 8 {
		var encoded uint64
		for i := 0; i < copied; i++ {
			encoded |= uint64(value[i]) << uint(8*(prefixLength-i-1))
		}
		return new(big.Int).SetUint64(encoded)
	}
	prefix := make([]byte, prefixLength)
	copy(prefix, value)
	return new(big.Int).SetBytes(prefix)
}

func (l QuantaIntermediateLowerer) lowerStringEnumPredicate(predicate Predicate, parameters ParameterBindingSet) (QuantaQueryFragment, DiagnosticSet, bool) {
	if fragment, diagnostics, ok := l.lowerStringEnumInPredicate(predicate, parameters); ok || diagnostics.BlocksNative() {
		return fragment, diagnostics, ok
	}
	if fragment, diagnostics, ok := l.lowerStringEnumLikePredicate(predicate, parameters); ok || diagnostics.BlocksNative() {
		return fragment, diagnostics, ok
	}
	op, field, valueExpr, ok := quantaIntermediateStringEnumComparisonParts(predicate)
	if !ok {
		return QuantaQueryFragment{}, nil, false
	}
	value, diagnostics, ok := quantaIntermediateValue(valueExpr, parameters)
	if !ok {
		return QuantaQueryFragment{}, diagnostics, false
	}
	if value.Kind == ValueNull {
		return quantaIntermediateNullCheckFragment(op, field)
	}
	switch op {
	case BinaryOpEqual, BinaryOpNotEqual:
	default:
		return QuantaQueryFragment{}, nil, false
	}
	id, diagnostics, ok := l.stringEnumID(field, value)
	if !ok {
		if quantaIntermediateDictionaryLabelNotFound(diagnostics) {
			fragment := quantaIntermediateMissingStringEnumFragment(field, value)
			if op == BinaryOpNotEqual {
				fragment.Operation = QuantaOperationDifference
			}
			return fragment, nil, true
		}
		return QuantaQueryFragment{}, diagnostics, false
	}
	fragment := quantaIntermediateStringEnumFragment(field, []*big.Int{id}, []LiteralExpr{value})
	if op == BinaryOpNotEqual {
		fragment.Operation = QuantaOperationDifference
	}
	return fragment, nil, true
}

func (l QuantaIntermediateLowerer) lowerStringEnumLikePredicate(predicate Predicate, parameters ParameterBindingSet) (QuantaQueryFragment, DiagnosticSet, bool) {
	op, field, valueExpr, ok := quantaIntermediateStringEnumLikeParts(predicate)
	if !ok {
		return QuantaQueryFragment{}, nil, false
	}
	value, diagnostics, ok := quantaIntermediateValue(valueExpr, parameters)
	if !ok {
		return QuantaQueryFragment{}, diagnostics, false
	}
	label, ok := value.Value.(string)
	if value.Kind != ValueString || !ok {
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("StringEnum LIKE predicates require string labels"), false
	}

	var values []*big.Int
	literals := []LiteralExpr{value}
	switch simpleLikePattern(label) {
	case likePatternExact:
		id, diagnostics, ok := l.stringEnumID(field, value)
		if !ok {
			if quantaIntermediateDictionaryLabelNotFound(diagnostics) {
				fragment := quantaIntermediateMissingStringEnumFragment(field, value)
				if op == BinaryOpNotLike {
					fragment.Operation = QuantaOperationDifference
				}
				return fragment, nil, true
			}
			return QuantaQueryFragment{}, diagnostics, false
		}
		values = []*big.Int{id}
	case likePatternPrefix:
		prefix := strings.TrimSuffix(label, "%")
		entries, diagnostics, ok := l.stringEnumPrefixIDs(field, prefix)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
		values = make([]*big.Int, 0, len(entries))
		literals = make([]LiteralExpr, 0, len(entries))
		for _, entry := range entries {
			values = append(values, new(big.Int).SetUint64(uint64(entry.ID)))
			literals = append(literals, Literal(ValueString, entry.Label))
		}
	default:
		return QuantaQueryFragment{}, nil, false
	}

	if len(values) == 0 {
		fragment := quantaIntermediateMissingStringEnumFragment(field, value)
		if op == BinaryOpNotLike {
			fragment.Operation = QuantaOperationDifference
		}
		return fragment, nil, true
	}
	fragment := quantaIntermediateStringEnumFragment(field, values, literals)
	if op == BinaryOpNotLike {
		fragment.Operation = QuantaOperationDifference
	}
	return fragment, nil, true
}

func (l QuantaIntermediateLowerer) lowerStringEnumInPredicate(predicate Predicate, parameters ParameterBindingSet) (QuantaQueryFragment, DiagnosticSet, bool) {
	field, list, ok := quantaIntermediateInParts(predicate)
	if !ok || field.Index != IndexStringEnum {
		return QuantaQueryFragment{}, nil, false
	}
	values := make([]*big.Int, 0, len(list.Items))
	literals := make([]LiteralExpr, 0, len(list.Items))
	for _, item := range list.Items {
		value, diagnostics, ok := quantaIntermediateValue(item, parameters)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
		id, diagnostics, ok := l.stringEnumID(field, value)
		if !ok {
			if quantaIntermediateDictionaryLabelNotFound(diagnostics) {
				continue
			}
			return QuantaQueryFragment{}, diagnostics, false
		}
		values = append(values, id)
		literals = append(literals, value)
	}
	if len(values) == 0 {
		fragment := quantaIntermediateMissingStringEnumFragment(field, LiteralExpr{})
		if quantaIntermediateInNegated(predicate) {
			fragment.Operation = QuantaOperationDifference
		}
		return fragment, nil, true
	}
	fragment := quantaIntermediateStringEnumFragment(field, values, literals)
	if quantaIntermediateInNegated(predicate) {
		fragment.Operation = QuantaOperationDifference
	}
	return fragment, nil, true
}

func (l QuantaIntermediateLowerer) stringEnumID(field FieldRef, value LiteralExpr) (*big.Int, DiagnosticSet, bool) {
	label, ok := value.Value.(string)
	if value.Kind != ValueString || !ok {
		return nil, quantaIntermediateDiagnostics("StringEnum predicates require string labels"), false
	}
	if l.Dictionaries == nil {
		return nil, nil, false
	}
	entry, diagnostics := l.Dictionaries.LookupLabel(quantaIntermediateDictionaryRef(field), label)
	if diagnostics.BlocksNative() {
		return nil, diagnostics, false
	}
	return new(big.Int).SetUint64(uint64(entry.ID)), nil, true
}

func (l QuantaIntermediateLowerer) stringEnumPrefixIDs(field FieldRef, prefix string) ([]DictionaryEntry, DiagnosticSet, bool) {
	if l.Dictionaries == nil {
		return nil, nil, false
	}
	entries, diagnostics := l.Dictionaries.LookupPrefix(quantaIntermediateDictionaryRef(field), prefix)
	if diagnostics.BlocksNative() {
		return nil, diagnostics, false
	}
	return entries, nil, true
}

func quantaIntermediateStringEnumFragment(field FieldRef, values []*big.Int, literals []LiteralExpr) QuantaQueryFragment {
	return QuantaQueryFragment{
		Index:     field.Table.Table,
		Role:      quantaIntermediateTableRole(field.Table),
		Field:     quantaIntermediateFieldName(field),
		Operation: QuantaOperationIntersect,
		Values:    values,
		Literals:  literals,
	}
}

func quantaIntermediateMissingStringEnumFragment(field FieldRef, literal LiteralExpr) QuantaQueryFragment {
	literals := []LiteralExpr(nil)
	if literal.Kind != "" {
		literals = []LiteralExpr{literal}
	}
	return quantaIntermediateStringEnumFragment(field, []*big.Int{quantaIntermediateImpossibleStringEnumID()}, literals)
}

func quantaIntermediateImpossibleStringEnumID() *big.Int {
	return new(big.Int).SetUint64(^uint64(0))
}

func quantaIntermediateDictionaryLabelNotFound(diagnostics DiagnosticSet) bool {
	if len(diagnostics) == 0 {
		return false
	}
	for _, diagnostic := range diagnostics {
		if diagnostic.Code != DiagnosticDictionaryLabelNotFound {
			return false
		}
	}
	return true
}

func (l QuantaIntermediateLowerer) lowerInPredicate(predicate Predicate, parameters ParameterBindingSet) (QuantaQueryFragment, DiagnosticSet, bool) {
	field, list, ok := quantaIntermediateInParts(predicate)
	if !ok {
		return QuantaQueryFragment{}, nil, false
	}
	if field.Encoding.Kind == EncodingStringLexBSI {
		return l.lowerStringLexBSIInPredicate(field, list, parameters, quantaIntermediateInNegated(predicate))
	}
	if field.Index != IndexBSI && field.Index != IndexDateTime {
		return l.lowerLiteralInPredicate(field, list, parameters, quantaIntermediateInNegated(predicate))
	}
	values := make([]*big.Int, 0, len(list.Items))
	literals := make([]LiteralExpr, 0, len(list.Items))
	for _, item := range list.Items {
		value, diagnostics, ok := quantaIntermediateValue(item, parameters)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
		literalValue := value
		if field.Index == IndexDateTime {
			value, diagnostics, ok = quantaIntermediateNormalizeTimeValue(field, value)
			if !ok {
				return QuantaQueryFragment{}, diagnostics, false
			}
		}
		bigValue, ok := quantaIntermediateBigInt(value)
		if !ok {
			return QuantaQueryFragment{}, quantaIntermediateDiagnostics("only integer BSI IN values are lowered in this slice"), false
		}
		values = append(values, bigValue)
		literals = append(literals, literalValue)
	}
	return QuantaQueryFragment{
		Index:     field.Table.Table,
		Role:      quantaIntermediateTableRole(field.Table),
		Field:     quantaIntermediateFieldName(field),
		Operation: QuantaOperationIntersect,
		BSIOp:     QuantaBSIOpBatchEQ,
		Values:    values,
		Literals:  literals,
		Negate:    quantaIntermediateInNegated(predicate),
	}, nil, true
}

func (l QuantaIntermediateLowerer) lowerStringLexBSIInPredicate(field FieldRef, list ListExpr, parameters ParameterBindingSet, negate bool) (QuantaQueryFragment, DiagnosticSet, bool) {
	if field.Encoding.NeedsStringRemainderLookup() {
		return QuantaQueryFragment{}, quantaIntermediateDiagnostics("StringLexBSI IN predicates requiring remainder lookup are not lowered in this slice"), false
	}
	values := make([]*big.Int, 0, len(list.Items))
	for _, item := range list.Items {
		value, diagnostics, ok := quantaIntermediateValue(item, parameters)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
		if value.Kind != ValueString {
			return QuantaQueryFragment{}, quantaIntermediateDiagnostics("StringLexBSI IN predicates require string values"), false
		}
		text, ok := value.Value.(string)
		if !ok {
			return QuantaQueryFragment{}, quantaIntermediateDiagnostics("StringLexBSI IN predicates require string values"), false
		}
		values = append(values, quantaIntermediateStringLexBSIValue(text, field.Encoding.PrefixLength))
	}
	operation := QuantaOperationIntersect
	if negate {
		operation = QuantaOperationDifference
	}
	return QuantaQueryFragment{
		Index:     field.Table.Table,
		Role:      quantaIntermediateTableRole(field.Table),
		Field:     quantaIntermediateFieldName(field),
		Operation: operation,
		BSIOp:     QuantaBSIOpBatchEQ,
		Values:    values,
	}, nil, true
}

func (l QuantaIntermediateLowerer) lowerLiteralInPredicate(field FieldRef, list ListExpr, parameters ParameterBindingSet, negate bool) (QuantaQueryFragment, DiagnosticSet, bool) {
	values := make([]LiteralExpr, 0, len(list.Items))
	for _, item := range list.Items {
		value, diagnostics, ok := quantaIntermediateValue(item, parameters)
		if !ok {
			return QuantaQueryFragment{}, diagnostics, false
		}
		values = append(values, value)
	}
	return QuantaQueryFragment{
		Index:     field.Table.Table,
		Role:      quantaIntermediateTableRole(field.Table),
		Field:     quantaIntermediateFieldName(field),
		Operation: QuantaOperationIntersect,
		Literals:  values,
		Negate:    negate,
	}, nil, true
}

func quantaIntermediateFieldName(field FieldRef) string {
	if field.PhysicalName != "" {
		return field.PhysicalName
	}
	return field.Name
}

func quantaIntermediateTableRole(table TableInstance) TableInstanceID {
	if table.Alias != "" {
		return TableInstanceID(table.Alias)
	}
	if table.ID != "" {
		return table.ID
	}
	return TableInstanceID(table.Table)
}

func quantaIntermediateProjectionFields(query QueryIR) []QuantaProjectionField {
	fields := query.RequiredFields()
	visibleFields := quantaIntermediateVisibleProjectionFields(query)
	projections := make([]QuantaProjectionField, 0, len(fields))
	for _, field := range fields {
		_, visible := visibleFields[quantaIntermediateFieldKey(field)]
		projections = append(projections, quantaIntermediateProjectionField(field, visible))
	}
	return projections
}

func quantaIntermediateProjectionField(field FieldRef, visible bool) QuantaProjectionField {
	roles := field.Roles
	if visible {
		roles |= FieldRoleVisible
	}
	return QuantaProjectionField{
		Index:        field.Table.Table,
		Role:         quantaIntermediateTableRole(field.Table),
		Field:        quantaIntermediateFieldName(field),
		Type:         field.Type,
		PhysicalName: field.PhysicalName,
		Roles:        roles,
		Visible:      visible,
	}
}

func quantaIntermediateIsTopNAggregate(aggregate Aggregate) bool {
	return strings.EqualFold(aggregate.Function, "topn") && aggregate.Origin == FunctionOriginQuantaCustom
}

func quantaIntermediateVisibleProjectionFields(query QueryIR) map[string]struct{} {
	visible := make(map[string]struct{})
	for _, projection := range query.Projection {
		for _, field := range projection.RequiredFields() {
			visible[quantaIntermediateFieldKey(field)] = struct{}{}
		}
	}
	return visible
}

func quantaIntermediateFieldKey(field FieldRef) string {
	return string(field.Table.ID) + "\x00" + field.Name + "\x00" + field.PhysicalName
}

func quantaIntermediateInParts(predicate Predicate) (FieldRef, ListExpr, bool) {
	binary, ok := quantaIntermediateBinaryExpr(predicate.Expr)
	if !ok || (binary.Op != BinaryOpIn && binary.Op != BinaryOpNotIn) {
		return FieldRef{}, ListExpr{}, false
	}
	field, ok := quantaIntermediateFieldExpr(binary.Left)
	if !ok {
		return FieldRef{}, ListExpr{}, false
	}
	list, ok := quantaIntermediateListExpr(binary.Right)
	return field, list, ok
}

func quantaIntermediateInNegated(predicate Predicate) bool {
	binary, ok := quantaIntermediateBinaryExpr(predicate.Expr)
	return ok && binary.Op == BinaryOpNotIn
}

func quantaIntermediateBetweenParts(predicate Predicate) (FieldRef, Expr, Expr, bool, bool) {
	binary, ok := quantaIntermediateBinaryExpr(predicate.Expr)
	if !ok {
		return FieldRef{}, nil, nil, false, false
	}
	if binary.Op != BinaryOpBetween && binary.Op != BinaryOpNotBetween {
		return FieldRef{}, nil, nil, false, false
	}
	field, ok := quantaIntermediateFieldExpr(binary.Left)
	if !ok {
		return FieldRef{}, nil, nil, false, false
	}
	list, ok := quantaIntermediateListExpr(binary.Right)
	if !ok || len(list.Items) != 2 {
		return FieldRef{}, nil, nil, false, false
	}
	if !quantaIntermediateValueExpr(list.Items[0]) || !quantaIntermediateValueExpr(list.Items[1]) {
		return FieldRef{}, nil, nil, false, false
	}
	return field, list.Items[0], list.Items[1], binary.Op == BinaryOpNotBetween, true
}

func quantaIntermediateStringEnumComparisonParts(predicate Predicate) (BinaryOp, FieldRef, Expr, bool) {
	op, field, valueExpr, ok := quantaIntermediateComparisonParts(predicate)
	if !ok || field.Index != IndexStringEnum {
		return "", FieldRef{}, nil, false
	}
	switch op {
	case BinaryOpEqual, BinaryOpNotEqual:
		return op, field, valueExpr, true
	default:
		return "", FieldRef{}, nil, false
	}
}

func quantaIntermediateStringEnumLikeParts(predicate Predicate) (BinaryOp, FieldRef, Expr, bool) {
	binary, ok := quantaIntermediateBinaryExpr(predicate.Expr)
	if !ok || (binary.Op != BinaryOpLike && binary.Op != BinaryOpNotLike) {
		return "", FieldRef{}, nil, false
	}
	field, ok := quantaIntermediateFieldExpr(binary.Left)
	if !ok || field.Index != IndexStringEnum || !quantaIntermediateValueExpr(binary.Right) {
		return "", FieldRef{}, nil, false
	}
	return binary.Op, field, binary.Right, true
}

func quantaIntermediateDictionaryRef(field FieldRef) DictionaryRef {
	if field.Dictionary.Ref.Valid() {
		return field.Dictionary.Ref
	}
	return DictionaryRef{
		Schema: field.Table.Schema,
		Table:  field.Table.Table,
		Field:  quantaIntermediateFieldName(field),
	}
}

func quantaIntermediateComparisonParts(predicate Predicate) (BinaryOp, FieldRef, Expr, bool) {
	binary, ok := quantaIntermediateBinaryExpr(predicate.Expr)
	if !ok {
		return "", FieldRef{}, nil, false
	}
	if !quantaIntermediateSupportedComparison(binary.Op) {
		return "", FieldRef{}, nil, false
	}
	if field, ok := quantaIntermediateFieldExpr(binary.Left); ok {
		if quantaIntermediateValueExpr(binary.Right) {
			return binary.Op, field, binary.Right, true
		}
	}
	if field, ok := quantaIntermediateFieldExpr(binary.Right); ok {
		if quantaIntermediateValueExpr(binary.Left) {
			op, ok := quantaIntermediateReverseComparison(binary.Op)
			return op, field, binary.Left, ok
		}
	}
	return "", FieldRef{}, nil, false
}

func quantaIntermediateValue(expr Expr, parameters ParameterBindingSet) (LiteralExpr, DiagnosticSet, bool) {
	if literal, ok := quantaIntermediateLiteralExpr(expr); ok {
		return literal, nil, true
	}
	if call, ok := quantaIntermediateCallExpr(expr); ok {
		return quantaIntermediateCallValue(call, parameters)
	}
	parameter, ok := quantaIntermediateParameterExpr(expr)
	if !ok {
		return LiteralExpr{}, quantaIntermediateDiagnostics("comparison value must be a literal or parameter"), false
	}
	for _, binding := range parameters.Bindings {
		if parameterRefKey(binding.Ref) == parameterRefKey(parameter.Ref) {
			return Literal(binding.Value.Kind, binding.Value.Value), nil, true
		}
	}
	return LiteralExpr{}, quantaIntermediateDiagnostics(fmt.Sprintf("prepared-statement parameter %d is not bound", parameter.Ref.Index)), false
}

func quantaIntermediateCallValue(call CallExpr, parameters ParameterBindingSet) (LiteralExpr, DiagnosticSet, bool) {
	if !strings.EqualFold(call.Name, "todate") {
		return LiteralExpr{}, quantaIntermediateDiagnostics("comparison value function is not supported: " + call.Name), false
	}
	if len(call.Args) != 1 {
		return LiteralExpr{}, quantaIntermediateDiagnostics("todate expects exactly one argument"), false
	}
	value, diagnostics, ok := quantaIntermediateValue(call.Args[0], parameters)
	if !ok {
		return LiteralExpr{}, diagnostics, false
	}
	timeValue, ok := quantaIntermediateParseTimeValue(value)
	if !ok {
		return LiteralExpr{}, quantaIntermediateDiagnostics("datetime comparison value is not supported"), false
	}
	return Literal(ValueTime, timeValue), nil, true
}

func quantaIntermediateNormalizeTimeValue(field FieldRef, value LiteralExpr) (LiteralExpr, DiagnosticSet, bool) {
	if value.Kind == ValueInt {
		return value, nil, true
	}
	timeValue, ok := quantaIntermediateParseTimeValue(value)
	if !ok {
		return LiteralExpr{}, quantaIntermediateDiagnostics("datetime comparison values must be strings, time values, or encoded epoch integers"), false
	}
	encoded, ok := quantaIntermediateEncodeTimeValue(field.Encoding.Granularity, timeValue)
	if !ok {
		return LiteralExpr{}, quantaIntermediateDiagnostics("datetime comparison value granularity is not supported: " + string(field.Encoding.Granularity)), false
	}
	return Literal(ValueInt, encoded), nil, true
}

func quantaIntermediateNormalizeDiscreteTimeComparison(op BinaryOp, field FieldRef, literal LiteralExpr, value LiteralExpr) (BinaryOp, LiteralExpr) {
	intValue, ok := value.Value.(int64)
	if !ok {
		return op, value
	}
	nextValue := quantaIntermediateNextEncodedTimeValue(field, literal, intValue)
	switch op {
	case BinaryOpLessEqual:
		return BinaryOpLess, Literal(ValueInt, nextValue)
	case BinaryOpGreater:
		return BinaryOpGreaterEqual, Literal(ValueInt, nextValue)
	default:
		return op, value
	}
}

func quantaIntermediateNextEncodedTimeValue(field FieldRef, literal LiteralExpr, encoded int64) int64 {
	if parsed, ok := quantaIntermediateDateOnlyLiteral(literal); ok {
		if next, ok := quantaIntermediateEncodeTimeValue(field.Encoding.Granularity, parsed.AddDate(0, 0, 1)); ok {
			return next
		}
	}
	return encoded + 1
}

func quantaIntermediateDateOnlyLiteral(literal LiteralExpr) (time.Time, bool) {
	text, ok := literal.Value.(string)
	if !ok {
		return time.Time{}, false
	}
	trimmed := strings.TrimSpace(text)
	if len(trimmed) != len("2006-01-02") {
		return time.Time{}, false
	}
	parsed, err := time.Parse("2006-01-02", trimmed)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func quantaIntermediateNormalizeBoolValue(value LiteralExpr) (LiteralExpr, DiagnosticSet, bool) {
	switch value.Kind {
	case ValueBool:
		boolValue, ok := value.Value.(bool)
		if !ok {
			return LiteralExpr{}, quantaIntermediateDiagnostics("boolean bitmap predicates require boolean-compatible values"), false
		}
		if boolValue {
			return LiteralExpr{Kind: ValueInt, Value: int64(1)}, nil, true
		}
		return LiteralExpr{Kind: ValueInt, Value: int64(0)}, nil, true
	case ValueInt:
		intValue, ok := value.Value.(int64)
		if !ok || (intValue != 0 && intValue != 1) {
			return LiteralExpr{}, quantaIntermediateDiagnostics("boolean bitmap predicates require boolean-compatible values"), false
		}
		return value, nil, true
	default:
		return LiteralExpr{}, quantaIntermediateDiagnostics("boolean bitmap predicates require boolean-compatible values"), false
	}
}
func quantaIntermediateNormalizeScaledNumericValue(field FieldRef, value LiteralExpr) (LiteralExpr, DiagnosticSet, bool) {
	scale := field.Encoding.Scale
	factor := math.Pow10(scale)
	var numeric float64
	switch typed := value.Value.(type) {
	case int:
		numeric = float64(typed)
	case int8:
		numeric = float64(typed)
	case int16:
		numeric = float64(typed)
	case int32:
		numeric = float64(typed)
	case int64:
		numeric = float64(typed)
	case uint:
		numeric = float64(typed)
	case uint8:
		numeric = float64(typed)
	case uint16:
		numeric = float64(typed)
	case uint32:
		numeric = float64(typed)
	case uint64:
		numeric = float64(typed)
	case float64:
		numeric = typed
	default:
		return LiteralExpr{}, quantaIntermediateDiagnostics("scaled BSI comparison values must be numeric"), false
	}
	scaled := numeric * factor
	rounded := math.Round(scaled)
	if math.Abs(scaled-rounded) > 1e-9 {
		return LiteralExpr{}, quantaIntermediateDiagnostics(
			fmt.Sprintf("this would result in rounding error for field '%s', value should have %d decimal places", field.Name, scale),
		), false
	}
	return Literal(ValueInt, int64(rounded)), nil, true
}

func quantaIntermediateNormalizeDiscreteNumericComparison(op BinaryOp, value LiteralExpr) (BinaryOp, LiteralExpr, DiagnosticSet, bool) {
	floatValue, ok := quantaIntermediateFloat64Literal(value)
	if !ok || math.Trunc(floatValue) == floatValue {
		return op, value, nil, true
	}
	switch op {
	case BinaryOpGreater:
		return BinaryOpGreater, Literal(ValueInt, int64(math.Floor(floatValue))), nil, true
	case BinaryOpGreaterEqual:
		return BinaryOpGreaterEqual, Literal(ValueInt, int64(math.Ceil(floatValue))), nil, true
	case BinaryOpLess:
		return BinaryOpLess, Literal(ValueInt, int64(math.Ceil(floatValue))), nil, true
	case BinaryOpLessEqual:
		return BinaryOpLessEqual, Literal(ValueInt, int64(math.Floor(floatValue))), nil, true
	default:
		return op, LiteralExpr{}, quantaIntermediateDiagnostics("fractional values cannot be lowered for equality against integer BSI fields"), false
	}
}

func quantaIntermediateFloat64Literal(value LiteralExpr) (float64, bool) {
	switch typed := value.Value.(type) {
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func quantaIntermediateParseTimeMillis(text string) (int64, bool) {
	timeValue, ok := quantaIntermediateParseTimeString(text)
	if !ok {
		return 0, false
	}
	return timeValue.UnixMilli(), true
}

func quantaIntermediateParseTimeValue(value LiteralExpr) (time.Time, bool) {
	if value.Kind == ValueTime {
		typed, ok := value.Value.(time.Time)
		if ok {
			return typed.UTC(), true
		}
	}
	text, ok := value.Value.(string)
	if !ok {
		return time.Time{}, false
	}
	return quantaIntermediateParseTimeString(text)
}

func quantaIntermediateParseTimeString(text string) (time.Time, bool) {
	trimmed := strings.TrimSpace(text)
	if strings.EqualFold(trimmed, "now") {
		return quantaIntermediateReferenceNow(), true
	}
	if timeValue, ok := quantaIntermediateParseRelativeNowTime(trimmed); ok {
		return timeValue, true
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.000Z",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		parsed, err := time.Parse(layout, trimmed)
		if err == nil {
			return parsed.UTC(), true
		}
	}
	return time.Time{}, false
}

func quantaIntermediateParseRelativeNowMillis(text string) (int64, bool) {
	timeValue, ok := quantaIntermediateParseRelativeNowTime(text)
	if !ok {
		return 0, false
	}
	return timeValue.UnixMilli(), true
}

func quantaIntermediateParseRelativeNowTime(text string) (time.Time, bool) {
	if !strings.HasPrefix(strings.ToLower(text), "now") || len(text) < 6 {
		return time.Time{}, false
	}
	sign := text[3]
	if sign != '+' && sign != '-' {
		return time.Time{}, false
	}
	unit := text[len(text)-1]
	amount, err := strconv.Atoi(text[4 : len(text)-1])
	if err != nil || amount < 0 {
		return time.Time{}, false
	}
	duration, ok := quantaIntermediateRelativeDuration(amount, unit)
	if !ok {
		return time.Time{}, false
	}
	base := quantaIntermediateReferenceNow()
	if sign == '-' {
		return base.Add(-duration), true
	}
	return base.Add(duration), true
}

func quantaIntermediateEncodeTimeValue(granularity TimeGranularity, value time.Time) (int64, bool) {
	normalized := value.UTC()
	switch granularity {
	case TimeGranularityUnknown, TimeGranularityMillisecond:
		return normalized.UnixMilli(), true
	case TimeGranularitySecond:
		return normalized.Unix(), true
	case TimeGranularityMicrosecond:
		return normalized.UnixMicro(), true
	case TimeGranularityNanosecond:
		return normalized.UnixNano(), true
	case TimeGranularityDay:
		return normalized.Truncate(24*time.Hour).Unix() / int64((24*time.Hour)/time.Second), true
	default:
		return 0, false
	}
}

func quantaIntermediateRelativeDuration(amount int, unit byte) (time.Duration, bool) {
	switch unit {
	case 's', 'S':
		return time.Duration(amount) * time.Second, true
	case 'm', 'M':
		return time.Duration(amount) * time.Minute, true
	case 'h', 'H':
		return time.Duration(amount) * time.Hour, true
	case 'd', 'D':
		return time.Duration(amount) * 24 * time.Hour, true
	default:
		return 0, false
	}
}

func quantaIntermediateReferenceNow() time.Time {
	return time.Now().UTC()
}

func quantaIntermediateBigInt(literal LiteralExpr) (*big.Int, bool) {
	switch typed := literal.Value.(type) {
	case *big.Int:
		if typed == nil {
			return nil, false
		}
		return new(big.Int).Set(typed), true
	case int:
		return big.NewInt(int64(typed)), true
	case int8:
		return big.NewInt(int64(typed)), true
	case int16:
		return big.NewInt(int64(typed)), true
	case int32:
		return big.NewInt(int64(typed)), true
	case int64:
		return big.NewInt(typed), true
	case uint:
		return new(big.Int).SetUint64(uint64(typed)), true
	case uint8:
		return new(big.Int).SetUint64(uint64(typed)), true
	case uint16:
		return new(big.Int).SetUint64(uint64(typed)), true
	case uint32:
		return new(big.Int).SetUint64(uint64(typed)), true
	case uint64:
		return new(big.Int).SetUint64(typed), true
	case float64:
		if math.Trunc(typed) != typed {
			return nil, false
		}
		return big.NewInt(int64(typed)), true
	case string:
		value := strings.TrimSpace(typed)
		if value == "" {
			return nil, false
		}
		parsed, ok := new(big.Int).SetString(value, 10)
		if !ok {
			return nil, false
		}
		return parsed, true
	default:
		return nil, false
	}
}

func quantaIntermediateBSIOp(op BinaryOp) QuantaBSIOp {
	switch op {
	case BinaryOpEqual:
		return QuantaBSIOpEQ
	case BinaryOpLess:
		return QuantaBSIOpLT
	case BinaryOpLessEqual:
		return QuantaBSIOpLE
	case BinaryOpGreater:
		return QuantaBSIOpGT
	case BinaryOpGreaterEqual:
		return QuantaBSIOpGE
	default:
		return QuantaBSIOpNone
	}
}

// quantaIntermediateCoalesceRanges folds lower/upper BSI bounds into Quanta's native inclusive RANGE primitive.
func quantaIntermediateCoalesceRanges(fragments []QuantaQueryFragment) []QuantaQueryFragment {
	if len(fragments) < 2 {
		return fragments
	}
	used := make([]bool, len(fragments))
	coalesced := make([]QuantaQueryFragment, 0, len(fragments))
	for i, fragment := range fragments {
		if used[i] {
			continue
		}
		match := -1
		for j := i + 1; j < len(fragments); j++ {
			if used[j] || !quantaIntermediateRangePair(fragment, fragments[j]) {
				continue
			}
			match = j
			break
		}
		if match == -1 {
			coalesced = append(coalesced, fragment)
			continue
		}
		used[i] = true
		used[match] = true
		coalesced = append(coalesced, quantaIntermediateRangeFragment(fragment, fragments[match]))
	}
	return coalesced
}

func quantaIntermediateRangePair(left QuantaQueryFragment, right QuantaQueryFragment) bool {
	if !quantaIntermediateSameRangeField(left, right) {
		return false
	}
	return (quantaIntermediateLowerBoundOp(left.BSIOp) && quantaIntermediateUpperBoundOp(right.BSIOp)) ||
		(quantaIntermediateUpperBoundOp(left.BSIOp) && quantaIntermediateLowerBoundOp(right.BSIOp))
}

func quantaIntermediateSameRangeField(left QuantaQueryFragment, right QuantaQueryFragment) bool {
	return left.Index == right.Index &&
		left.Role == right.Role &&
		left.Field == right.Field &&
		left.Operation == QuantaOperationIntersect &&
		right.Operation == QuantaOperationIntersect &&
		!left.Negate &&
		!right.Negate &&
		!left.NullCheck &&
		!right.NullCheck &&
		left.Value != nil &&
		right.Value != nil &&
		left.RangeCoalesceAllowed &&
		right.RangeCoalesceAllowed
}

func quantaIntermediateRangeFragment(left QuantaQueryFragment, right QuantaQueryFragment) QuantaQueryFragment {
	rangeFragment := left
	rangeFragment.BSIOp = QuantaBSIOpRange
	rangeFragment.Value = nil
	if quantaIntermediateLowerBoundOp(left.BSIOp) {
		rangeFragment.Begin = quantaIntermediateInclusiveLowerBound(left.BSIOp, left.Value)
		rangeFragment.End = quantaIntermediateInclusiveUpperBound(right.BSIOp, right.Value)
		return rangeFragment
	}
	rangeFragment.Begin = quantaIntermediateInclusiveLowerBound(right.BSIOp, right.Value)
	rangeFragment.End = quantaIntermediateInclusiveUpperBound(left.BSIOp, left.Value)
	return rangeFragment
}

func quantaIntermediateLowerBoundOp(op QuantaBSIOp) bool {
	return op == QuantaBSIOpGE || op == QuantaBSIOpGT
}

func quantaIntermediateUpperBoundOp(op QuantaBSIOp) bool {
	return op == QuantaBSIOpLE || op == QuantaBSIOpLT
}

func quantaIntermediateInclusiveLowerBound(op QuantaBSIOp, value *big.Int) *big.Int {
	bound := new(big.Int).Set(value)
	if op == QuantaBSIOpGT {
		bound.Add(bound, big.NewInt(1))
	}
	return bound
}

func quantaIntermediateInclusiveUpperBound(op QuantaBSIOp, value *big.Int) *big.Int {
	bound := new(big.Int).Set(value)
	if op == QuantaBSIOpLT {
		bound.Sub(bound, big.NewInt(1))
	}
	return bound
}

func quantaIntermediateSupportedComparison(op BinaryOp) bool {
	switch op {
	case BinaryOpEqual, BinaryOpNotEqual, BinaryOpLess, BinaryOpLessEqual, BinaryOpGreater, BinaryOpGreaterEqual:
		return true
	default:
		return false
	}
}

func quantaIntermediateReverseComparison(op BinaryOp) (BinaryOp, bool) {
	switch op {
	case BinaryOpEqual:
		return BinaryOpEqual, true
	case BinaryOpNotEqual:
		return BinaryOpNotEqual, true
	case BinaryOpLess:
		return BinaryOpGreater, true
	case BinaryOpLessEqual:
		return BinaryOpGreaterEqual, true
	case BinaryOpGreater:
		return BinaryOpLess, true
	case BinaryOpGreaterEqual:
		return BinaryOpLessEqual, true
	default:
		return "", false
	}
}

func quantaIntermediateValueExpr(expr Expr) bool {
	if _, ok := quantaIntermediateLiteralExpr(expr); ok {
		return true
	}
	if _, ok := quantaIntermediateParameterExpr(expr); ok {
		return true
	}
	_, ok := quantaIntermediateCallExpr(expr)
	return ok
}

func quantaIntermediateBinaryExpr(expr Expr) (BinaryExpr, bool) {
	switch typed := expr.(type) {
	case BinaryExpr:
		return typed, true
	case *BinaryExpr:
		if typed == nil {
			return BinaryExpr{}, false
		}
		return *typed, true
	default:
		return BinaryExpr{}, false
	}
}

func quantaIntermediateFieldExpr(expr Expr) (FieldRef, bool) {
	switch typed := expr.(type) {
	case FieldExpr:
		return typed.Ref, true
	case *FieldExpr:
		if typed == nil {
			return FieldRef{}, false
		}
		return typed.Ref, true
	default:
		return FieldRef{}, false
	}
}

func quantaIntermediateLiteralExpr(expr Expr) (LiteralExpr, bool) {
	switch typed := expr.(type) {
	case LiteralExpr:
		return typed, true
	case *LiteralExpr:
		if typed == nil {
			return LiteralExpr{}, false
		}
		return *typed, true
	default:
		return LiteralExpr{}, false
	}
}

func quantaIntermediateParameterExpr(expr Expr) (ParameterExpr, bool) {
	switch typed := expr.(type) {
	case ParameterExpr:
		return typed, true
	case *ParameterExpr:
		if typed == nil {
			return ParameterExpr{}, false
		}
		return *typed, true
	default:
		return ParameterExpr{}, false
	}
}

func quantaIntermediateCallExpr(expr Expr) (CallExpr, bool) {
	switch typed := expr.(type) {
	case CallExpr:
		return typed, true
	case *CallExpr:
		if typed == nil {
			return CallExpr{}, false
		}
		return *typed, true
	default:
		return CallExpr{}, false
	}
}

func quantaIntermediateListExpr(expr Expr) (ListExpr, bool) {
	switch typed := expr.(type) {
	case ListExpr:
		return typed, true
	case *ListExpr:
		if typed == nil {
			return ListExpr{}, false
		}
		return *typed, true
	default:
		return ListExpr{}, false
	}
}

func quantaIntermediateDiagnostics(message string) DiagnosticSet {
	return DiagnosticSet{
		ErrorDiagnostic(DiagnosticNativeBlocker, PhaseExecute, "quanta intermediate lowerer: "+message),
	}
}

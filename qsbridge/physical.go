package qsbridge

import "strings"

// ShardID identifies one physical shard of table data.
type ShardID string

// ReplicaID identifies one replica that can serve a shard.
type ReplicaID string

// RoutingKey identifies the logical key used for shard routing.
type RoutingKey string

// ShardSet describes the physical shards a plan node may touch.
type ShardSet struct {
	All    bool
	Shards []ShardID
}

// Contains reports whether shard is explicitly included or all shards are selected.
func (s ShardSet) Contains(shard ShardID) bool {
	if s.All {
		return true
	}
	for _, current := range s.Shards {
		if current == shard {
			return true
		}
	}
	return false
}

// PlacementPolicy describes how physical work should choose replicas.
type PlacementPolicy string

const (
	// PlacementAny allows any healthy replica.
	PlacementAny PlacementPolicy = "any"
	// PlacementPrimary prefers the primary replica.
	PlacementPrimary PlacementPolicy = "primary"
	// PlacementLocal prefers a local replica when one is available.
	PlacementLocal PlacementPolicy = "local"
	// PlacementFollower allows follower-only reads.
	PlacementFollower PlacementPolicy = "follower"
)

// CacheScope describes the validity boundary for cached physical results.
type CacheScope string

const (
	// CacheNone disables cache reuse.
	CacheNone CacheScope = "none"
	// CacheQuery scopes cache entries to one query execution.
	CacheQuery CacheScope = "query"
	// CacheSession scopes cache entries to a client session.
	CacheSession CacheScope = "session"
	// CacheCluster scopes cache entries to cluster-visible immutable inputs.
	CacheCluster CacheScope = "cluster"
)

// PhysicalScope describes shard, replica, and cache boundaries for physical work.
type PhysicalScope struct {
	Shards    ShardSet
	Replicas  []ReplicaID
	Routing   RoutingKey
	Placement PlacementPolicy
	Cache     CacheScope
}

// PhysicalAccessIntent identifies whether physical work reads or mutates shard owners.
type PhysicalAccessIntent string

const (
	// PhysicalAccessRead selects one owner that can serve a read.
	PhysicalAccessRead PhysicalAccessIntent = "read"
	// PhysicalAccessWrite selects every owner required by the replication factor.
	PhysicalAccessWrite PhysicalAccessIntent = "write"
)

// PhysicalIntentForQueryKind maps SQL statement kind to physical read/write intent.
func PhysicalIntentForQueryKind(kind QueryKind) PhysicalAccessIntent {
	switch kind {
	case QueryKindInsert, QueryKindUpdate, QueryKindDelete, QueryKindDDL:
		return PhysicalAccessWrite
	default:
		return PhysicalAccessRead
	}
}

// PhysicalNodeSelectionInput describes the physical-layer inputs to node selection.
type PhysicalNodeSelectionInput struct {
	Intent             PhysicalAccessIntent
	Nodes              []NodeID
	ShardKey           DataShardKey
	TopologyGeneration TopologyGeneration
	Topology           ClusterTopologyProfile
}

// PhysicalNodeSelectionResult records the selected nodes for physical work.
type PhysicalNodeSelectionResult struct {
	Intent    PhysicalAccessIntent
	Nodes     []NodeID
	Placement RendezvousPlacementResult
	Complete  bool
}

// SelectPhysicalNodes selects data nodes for physical read or write work.
func SelectPhysicalNodes(input PhysicalNodeSelectionInput) PhysicalNodeSelectionResult {
	intent := input.Intent
	if intent == "" {
		intent = PhysicalAccessRead
	}
	placement := ResolveRendezvousPlacementWithTopology(RendezvousPlacementInput{
		Nodes:              input.Nodes,
		ShardKey:           input.ShardKey,
		TopologyGeneration: input.TopologyGeneration,
	}, input.Topology)
	result := PhysicalNodeSelectionResult{
		Intent:    intent,
		Placement: placement,
	}
	switch intent {
	case PhysicalAccessWrite:
		result.Nodes = append([]NodeID(nil), placement.Owners...)
		result.Complete = placement.Complete
	default:
		if len(placement.Owners) > 0 {
			result.Nodes = []NodeID{placement.Owners[0]}
			result.Complete = true
		}
	}
	return result
}

// Unscoped reports whether no physical routing information is present.
func (s PhysicalScope) Unscoped() bool {
	return !s.Shards.All &&
		len(s.Shards.Shards) == 0 &&
		len(s.Replicas) == 0 &&
		s.Routing == "" &&
		s.Placement == "" &&
		s.Cache == ""
}

// PhysicalNodeKind identifies one executor-facing physical operation.
type PhysicalNodeKind string

const (
	// PhysicalNodeStatement returns OK/affected-rows style statement metadata.
	PhysicalNodeStatement PhysicalNodeKind = "physical_statement"
	// PhysicalNodeScan reads table data using a physical scope.
	PhysicalNodeScan PhysicalNodeKind = "physical_scan"
	// PhysicalNodeFilter applies a physical predicate operation.
	PhysicalNodeFilter PhysicalNodeKind = "physical_filter"
	// PhysicalNodeMembership applies physical semi/anti membership operations.
	PhysicalNodeMembership PhysicalNodeKind = "physical_membership"
	// PhysicalNodeProject shapes physical row output.
	PhysicalNodeProject PhysicalNodeKind = "physical_project"
	// PhysicalNodeJoin executes a physical join strategy.
	PhysicalNodeJoin PhysicalNodeKind = "physical_join"
	// PhysicalNodeAggregate executes physical grouping or aggregation.
	PhysicalNodeAggregate PhysicalNodeKind = "physical_aggregate"
	// PhysicalNodeSort executes physical sorting.
	PhysicalNodeSort PhysicalNodeKind = "physical_sort"
	// PhysicalNodeLimit executes physical limit and offset.
	PhysicalNodeLimit PhysicalNodeKind = "physical_limit"
	// PhysicalNodeUnsupported preserves an unplanned physical boundary.
	PhysicalNodeUnsupported PhysicalNodeKind = "physical_unsupported"
)

// PhysicalStatementNode represents an executor-facing statement result boundary.
type PhysicalStatementNode struct {
	Kind     QueryKind
	Result   StatementResult
	Mutation MutationShape
	Scope    PhysicalScope
	Diags    DiagnosticSet
}

// PhysicalKind reports that PhysicalStatementNode is a physical statement.
func (PhysicalStatementNode) PhysicalKind() PhysicalNodeKind {
	return PhysicalNodeStatement
}

// PhysicalChildren returns no children for a physical statement.
func (PhysicalStatementNode) PhysicalChildren() []PhysicalNode {
	return nil
}

// PhysicalScope returns the scope for the statement.
func (n PhysicalStatementNode) PhysicalScope() PhysicalScope {
	return n.Scope
}

// PhysicalDiagnostics returns diagnostics attached to the statement.
func (n PhysicalStatementNode) PhysicalDiagnostics() DiagnosticSet {
	return n.Diags
}

// PhysicalStrategy names executor-facing strategy families selected by planning scaffolds.
type PhysicalStrategy string

const (
	// PhysicalStrategyBitmapPushdown uses bitmap-native predicate evaluation.
	PhysicalStrategyBitmapPushdown PhysicalStrategy = "bitmap_pushdown"
	// PhysicalStrategyBSIPushdown uses BSI-native predicate evaluation.
	PhysicalStrategyBSIPushdown PhysicalStrategy = "bsi_pushdown"
	// PhysicalStrategyResidualScan evaluates predicates after single-table projection.
	PhysicalStrategyResidualScan PhysicalStrategy = "residual_scan"
	// PhysicalStrategyEncodingEquality uses equality support advertised by an encoding profile.
	PhysicalStrategyEncodingEquality PhysicalStrategy = "encoding_equality"
	// PhysicalStrategyEncodingMembership uses membership support advertised by an encoding profile.
	PhysicalStrategyEncodingMembership PhysicalStrategy = "encoding_membership"
	// PhysicalStrategyEncodingRange uses range support advertised by an encoding profile.
	PhysicalStrategyEncodingRange PhysicalStrategy = "encoding_range"
	// PhysicalStrategyEncodingPrefix uses prefix support advertised by an encoding profile.
	PhysicalStrategyEncodingPrefix PhysicalStrategy = "encoding_prefix"
	// PhysicalStrategyEncodingContains uses contains support advertised by an encoding profile.
	PhysicalStrategyEncodingContains PhysicalStrategy = "encoding_contains"
	// PhysicalStrategyRelationshipParentLookup uses relation storage for child-to-parent lookup.
	PhysicalStrategyRelationshipParentLookup PhysicalStrategy = "relationship_parent_lookup"
	// PhysicalStrategyRelationshipChildExpansion uses relation storage for parent-to-child expansion.
	PhysicalStrategyRelationshipChildExpansion PhysicalStrategy = "relationship_child_expansion"
	// PhysicalStrategyRelationshipJoinReduction uses relation storage to reduce joined found sets.
	PhysicalStrategyRelationshipJoinReduction PhysicalStrategy = "relationship_join_reduction"
	// PhysicalStrategyRelationshipSemiJoin uses relation storage for semi-join membership.
	PhysicalStrategyRelationshipSemiJoin PhysicalStrategy = "relationship_semi_join"
	// PhysicalStrategyRelationshipAntiJoinDifference uses relation storage with bitmap difference semantics.
	PhysicalStrategyRelationshipAntiJoinDifference PhysicalStrategy = "relationship_anti_join_difference"
	// PhysicalStrategyRelationshipVectorNormalization translates rownum domains through relationship-vector storage.
	PhysicalStrategyRelationshipVectorNormalization PhysicalStrategy = "relationship_vector_normalization"
	// PhysicalStrategyPeerEqualityJoin evaluates a join as a generic equality edge without relationship-vector storage.
	PhysicalStrategyPeerEqualityJoin PhysicalStrategy = "peer_equality_join"
	// PhysicalStrategyBitmapDifference uses bitmap difference for anti-match or null-extension support.
	PhysicalStrategyBitmapDifference PhysicalStrategy = "bitmap_difference"
	// PhysicalStrategyOuterNullExtension preserves outer-join rows by null-extending the non-preserved side.
	PhysicalStrategyOuterNullExtension PhysicalStrategy = "outer_null_extension"
	// PhysicalStrategyQuantaTopN marks a future handoff to Quanta's core.Projector.Rank() primitive.
	PhysicalStrategyQuantaTopN PhysicalStrategy = "quanta_topn"
)

// PhysicalNode is one node in an executor-facing physical plan tree.
type PhysicalNode interface {
	PhysicalKind() PhysicalNodeKind
	PhysicalChildren() []PhysicalNode
	PhysicalScope() PhysicalScope
	PhysicalDiagnostics() DiagnosticSet
}

// PhysicalPlan is a physical plan tree derived from a logical plan.
type PhysicalPlan struct {
	Root    PhysicalNode
	Logical LogicalPlan
	Result  ResultShape
}

// PhysicalScanNode reads one table instance from a physical scope.
type PhysicalScanNode struct {
	Source TableInstance
	Fields []FieldRef
	Scope  PhysicalScope
	Diags  DiagnosticSet
}

// PhysicalKind reports that PhysicalScanNode is a physical scan.
func (PhysicalScanNode) PhysicalKind() PhysicalNodeKind {
	return PhysicalNodeScan
}

// PhysicalChildren returns no children for a physical scan.
func (PhysicalScanNode) PhysicalChildren() []PhysicalNode {
	return nil
}

// PhysicalScope returns the physical scope for the scan.
func (n PhysicalScanNode) PhysicalScope() PhysicalScope {
	return n.Scope
}

// PhysicalDiagnostics returns diagnostics attached to the scan.
func (n PhysicalScanNode) PhysicalDiagnostics() DiagnosticSet {
	return n.Diags
}

// PhysicalUnaryNode represents a physical unary operation.
type PhysicalUnaryNode struct {
	Kind       PhysicalNodeKind
	Input      PhysicalNode
	Strategies []PhysicalStrategy
	Scope      PhysicalScope
	Diags      DiagnosticSet
}

// PhysicalKind reports the unary physical node kind.
func (n PhysicalUnaryNode) PhysicalKind() PhysicalNodeKind {
	return n.Kind
}

// PhysicalChildren returns the unary input.
func (n PhysicalUnaryNode) PhysicalChildren() []PhysicalNode {
	return singlePhysicalChild(n.Input)
}

// PhysicalScope returns the unary physical scope.
func (n PhysicalUnaryNode) PhysicalScope() PhysicalScope {
	return n.Scope
}

// PhysicalDiagnostics returns diagnostics attached to the unary operation.
func (n PhysicalUnaryNode) PhysicalDiagnostics() DiagnosticSet {
	return n.Diags
}

// PhysicalJoinNode represents an executor-facing join operation.
type PhysicalJoinNode struct {
	Left       PhysicalNode
	Right      PhysicalNode
	Edge       JoinEdge
	Strategies []PhysicalStrategy
	Scope      PhysicalScope
	Diags      DiagnosticSet
}

// PhysicalKind reports that PhysicalJoinNode is a physical join.
func (PhysicalJoinNode) PhysicalKind() PhysicalNodeKind {
	return PhysicalNodeJoin
}

// PhysicalChildren returns the physical join inputs.
func (n PhysicalJoinNode) PhysicalChildren() []PhysicalNode {
	children := make([]PhysicalNode, 0, 2)
	if n.Left != nil {
		children = append(children, n.Left)
	}
	if n.Right != nil {
		children = append(children, n.Right)
	}
	return children
}

// PhysicalScope returns the join physical scope.
func (n PhysicalJoinNode) PhysicalScope() PhysicalScope {
	return n.Scope
}

// PhysicalDiagnostics returns diagnostics attached to the join.
func (n PhysicalJoinNode) PhysicalDiagnostics() DiagnosticSet {
	return n.Diags
}

// PhysicalAggregateNode represents executor-facing grouped, global, or native aggregate work.
type PhysicalAggregateNode struct {
	Input      PhysicalNode
	GroupBy    []Expr
	Aggregates []Aggregate
	Having     []Predicate
	Strategies []PhysicalStrategy
	Scope      PhysicalScope
	Diags      DiagnosticSet
}

// PhysicalKind reports that PhysicalAggregateNode is a physical aggregate.
func (PhysicalAggregateNode) PhysicalKind() PhysicalNodeKind {
	return PhysicalNodeAggregate
}

// PhysicalChildren returns the aggregate input.
func (n PhysicalAggregateNode) PhysicalChildren() []PhysicalNode {
	return singlePhysicalChild(n.Input)
}

// PhysicalScope returns the aggregate physical scope.
func (n PhysicalAggregateNode) PhysicalScope() PhysicalScope {
	return n.Scope
}

// PhysicalDiagnostics returns diagnostics attached to the aggregate.
func (n PhysicalAggregateNode) PhysicalDiagnostics() DiagnosticSet {
	return n.Diags
}

// BuildPhysicalPlan lowers a logical plan into an executor-facing scaffold.
func BuildPhysicalPlan(logical LogicalPlan, defaultScope PhysicalScope) PhysicalPlan {
	return PhysicalPlan{
		Root:    buildPhysicalNode(logical.Root, defaultScope),
		Logical: logical,
		Result:  logical.Result,
	}
}

func buildPhysicalNode(node LogicalNode, defaultScope PhysicalScope) PhysicalNode {
	switch n := node.(type) {
	case nil:
		return nil
	case StatementNode:
		return PhysicalStatementNode{
			Kind:     n.Kind,
			Result:   n.Result,
			Mutation: n.Mutation,
			Scope:    defaultScope,
			Diags:    n.Diags,
		}
	case ScanNode:
		return PhysicalScanNode{
			Source: n.Source,
			Fields: n.Fields,
			Scope:  defaultScope,
			Diags:  n.Diags,
		}
	case FilterNode:
		return PhysicalUnaryNode{
			Kind:       PhysicalNodeFilter,
			Input:      buildPhysicalNode(n.Input, defaultScope),
			Strategies: physicalPredicateStrategies(n.Predicates),
			Scope:      defaultScope,
			Diags:      n.Diags,
		}
	case MembershipNode:
		return PhysicalUnaryNode{
			Kind:       PhysicalNodeMembership,
			Input:      buildPhysicalNode(n.Input, defaultScope),
			Strategies: physicalMembershipStrategies(n.Memberships),
			Scope:      defaultScope,
			Diags:      n.Diags,
		}
	case ProjectNode:
		return PhysicalUnaryNode{
			Kind:  PhysicalNodeProject,
			Input: buildPhysicalNode(n.Input, defaultScope),
			Scope: defaultScope,
			Diags: n.Diags,
		}
	case JoinNode:
		return PhysicalJoinNode{
			Left:       buildPhysicalNode(n.Left, defaultScope),
			Right:      buildPhysicalNode(n.Right, defaultScope),
			Edge:       n.Edge,
			Strategies: physicalJoinStrategies(n.Edge),
			Scope:      defaultScope,
			Diags:      n.Diags,
		}
	case AggregateNode:
		return PhysicalAggregateNode{
			Input:      buildPhysicalNode(n.Input, defaultScope),
			GroupBy:    append([]Expr(nil), n.GroupBy...),
			Aggregates: append([]Aggregate(nil), n.Aggregates...),
			Having:     append([]Predicate(nil), n.Having...),
			Strategies: physicalAggregateStrategies(n.Aggregates),
			Scope:      defaultScope,
			Diags:      n.Diags,
		}
	case SortNode:
		return PhysicalUnaryNode{
			Kind:  PhysicalNodeSort,
			Input: buildPhysicalNode(n.Input, defaultScope),
			Scope: defaultScope,
			Diags: n.Diags,
		}
	case LimitNode:
		return PhysicalUnaryNode{
			Kind:  PhysicalNodeLimit,
			Input: buildPhysicalNode(n.Input, defaultScope),
			Scope: defaultScope,
			Diags: n.Diags,
		}
	case UnsupportedNode:
		return PhysicalUnaryNode{
			Kind:  PhysicalNodeUnsupported,
			Input: buildPhysicalNode(n.Input, defaultScope),
			Scope: defaultScope,
			Diags: n.Diags,
		}
	default:
		return PhysicalUnaryNode{
			Kind:  PhysicalNodeUnsupported,
			Scope: defaultScope,
			Diags: DiagnosticSet{
				ErrorDiagnostic(DiagnosticInternalInvariant, PhasePlan, "unknown logical node type"),
			},
		}
	}
}

func physicalAggregateStrategies(aggregates []Aggregate) []PhysicalStrategy {
	collector := newPhysicalStrategyCollector()
	for _, aggregate := range aggregates {
		if strings.EqualFold(aggregate.Function, "topn") && aggregate.Origin == FunctionOriginQuantaCustom {
			collector.add(PhysicalStrategyQuantaTopN)
		}
	}
	return collector.values
}

func singlePhysicalChild(child PhysicalNode) []PhysicalNode {
	if child == nil {
		return nil
	}
	return []PhysicalNode{child}
}

// physicalPredicateStrategies maps predicate capabilities to executor-facing strategy families.
func physicalPredicateStrategies(predicates []Predicate) []PhysicalStrategy {
	collector := newPhysicalStrategyCollector()
	for _, predicate := range predicates {
		for _, capability := range predicate.Capabilities {
			collector.addStrategiesForCapability(capability)
		}
		for _, capability := range EncodingPredicateCapabilities(predicate) {
			collector.addStrategiesForCapability(capability)
		}
		switch predicate.Placement {
		case PredicateResidualScan:
			collector.add(PhysicalStrategyResidualScan)
		case PredicatePushdown:
			capability, ok := StringEnumPredicateCapability(predicate)
			if ok {
				collector.addStrategiesForCapability(capability)
			}
		}
	}
	return collector.values
}

// physicalStrategyCollector preserves first-seen strategy order while removing duplicates.
type physicalStrategyCollector struct {
	values []PhysicalStrategy
	seen   map[PhysicalStrategy]struct{}
}

// newPhysicalStrategyCollector creates an empty physical strategy collector.
func newPhysicalStrategyCollector() *physicalStrategyCollector {
	return &physicalStrategyCollector{
		values: make([]PhysicalStrategy, 0),
		seen:   make(map[PhysicalStrategy]struct{}),
	}
}

// add records strategy once, preserving the first-seen order.
func (c *physicalStrategyCollector) add(strategy PhysicalStrategy) {
	if strategy == "" {
		return
	}
	if _, ok := c.seen[strategy]; ok {
		return
	}
	c.seen[strategy] = struct{}{}
	c.values = append(c.values, strategy)
}

// addStrategiesForCapability expands a plan capability into one or more physical strategies.
func (c *physicalStrategyCollector) addStrategiesForCapability(capability PlanCapability) {
	for _, strategy := range physicalStrategiesForCapability(capability) {
		c.add(strategy)
	}
}

// physicalStrategiesForCapability maps plan-level evidence to physical strategy families.
func physicalStrategiesForCapability(capability PlanCapability) []PhysicalStrategy {
	switch capability {
	case CapabilityBitmapPushdown, CapabilityStringEnumEquality, CapabilityStringEnumPrefixLike, CapabilityStringEnumContainsLike, CapabilityStringEnumMembership:
		return []PhysicalStrategy{PhysicalStrategyBitmapPushdown}
	case CapabilityBSIPushdown:
		return []PhysicalStrategy{PhysicalStrategyBSIPushdown}
	case CapabilityEncodingEquality:
		return []PhysicalStrategy{PhysicalStrategyEncodingEquality}
	case CapabilityEncodingMembership:
		return []PhysicalStrategy{PhysicalStrategyEncodingMembership}
	case CapabilityEncodingRange:
		return []PhysicalStrategy{PhysicalStrategyEncodingRange}
	case CapabilityEncodingPrefix:
		return []PhysicalStrategy{PhysicalStrategyEncodingPrefix}
	case CapabilityEncodingContains:
		return []PhysicalStrategy{PhysicalStrategyEncodingContains}
	case CapabilityRelationshipParentLookup:
		return []PhysicalStrategy{PhysicalStrategyRelationshipParentLookup}
	case CapabilityRelationshipChildExpansion, CapabilityParentToChildExpansion:
		return []PhysicalStrategy{PhysicalStrategyRelationshipChildExpansion}
	case CapabilityRelationshipJoinReduction:
		return []PhysicalStrategy{PhysicalStrategyRelationshipJoinReduction}
	case CapabilityRelationshipSemiJoin:
		return []PhysicalStrategy{PhysicalStrategyRelationshipSemiJoin}
	case CapabilityRelationshipAntiJoinDifference:
		return []PhysicalStrategy{PhysicalStrategyRelationshipAntiJoinDifference}
	case CapabilityBitmapDifference:
		return []PhysicalStrategy{PhysicalStrategyBitmapDifference}
	case CapabilityOuterJoin, CapabilityNullExtension:
		return []PhysicalStrategy{PhysicalStrategyOuterNullExtension}
	default:
		return nil
	}
}

// physicalJoinStrategies maps a join edge to executor-facing strategy families.
func physicalJoinStrategies(edge JoinEdge) []PhysicalStrategy {
	collector := newPhysicalStrategyCollector()
	if edge.Direction == JoinPeerEquality && len(edge.Encoding.Capabilities) == 0 {
		collector.add(PhysicalStrategyPeerEqualityJoin)
	}
	for _, capability := range edge.Capabilities() {
		collector.addStrategiesForCapability(capability)
	}
	for _, capability := range RelationshipPlanCapabilities(edge.Encoding) {
		collector.addStrategiesForCapability(capability)
	}
	return collector.values
}

// physicalRelationshipStrategies maps relationship encoding capabilities to join strategies.
func physicalRelationshipStrategies(encoding RelationshipEncodingProfile) []PhysicalStrategy {
	collector := newPhysicalStrategyCollector()
	for _, capability := range RelationshipPlanCapabilities(encoding) {
		collector.addStrategiesForCapability(capability)
	}
	return collector.values
}

// physicalMembershipStrategies maps membership edges to semi/anti physical strategies.
func physicalMembershipStrategies(edges []MembershipEdge) []PhysicalStrategy {
	collector := newPhysicalStrategyCollector()
	for _, edge := range edges {
		for _, capability := range edge.Capabilities() {
			collector.addStrategiesForCapability(capability)
		}
		for _, capability := range RelationshipPlanCapabilities(edge.Encoding) {
			collector.addStrategiesForCapability(capability)
		}
	}
	return collector.values
}

// WalkPhysicalPlan visits each physical node in pre-order.
func WalkPhysicalPlan(root PhysicalNode, visit func(PhysicalNode) bool) {
	if root == nil || visit == nil {
		return
	}
	if !visit(root) {
		return
	}
	for _, child := range root.PhysicalChildren() {
		WalkPhysicalPlan(child, visit)
	}
}

// PhysicalPlanDiagnostics returns diagnostics from the physical plan tree.
func PhysicalPlanDiagnostics(root PhysicalNode) DiagnosticSet {
	diagnostics := make(DiagnosticSet, 0)
	WalkPhysicalPlan(root, func(node PhysicalNode) bool {
		diagnostics = append(diagnostics, node.PhysicalDiagnostics()...)
		return true
	})
	return diagnostics
}

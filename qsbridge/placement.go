package qsbridge

import (
	"sort"
	"strconv"
)

// NodeID identifies a stable data-node identity used for deterministic placement.
type NodeID string

// DataShardKey identifies the logical data shard key routed through the node set.
type DataShardKey string

// TopologyGeneration identifies one topology metadata generation used for placement.
type TopologyGeneration string

// RendezvousPlacementInput describes rendezvous hashing shard owner selection inputs.
type RendezvousPlacementInput struct {
	Nodes              []NodeID
	ReplicationFactor  int
	ShardKey           DataShardKey
	TopologyGeneration TopologyGeneration
}

// RendezvousPlacementResult records the rendezvous hashing owner set for one data shard key.
type RendezvousPlacementResult struct {
	ShardKey           DataShardKey
	ReplicationFactor  int
	TopologyGeneration TopologyGeneration
	Owners             []NodeID
	Complete           bool
}

// ResolveRendezvousPlacement returns an ordered owner set for a shard key over stable nodes.
func ResolveRendezvousPlacement(input RendezvousPlacementInput) RendezvousPlacementResult {
	return ResolveRendezvousPlacementWithTopology(input, ClusterTopologyProfile{})
}

// ResolveRendezvousPlacementWithTopology returns an owner set using topology RF when input RF is unset.
func ResolveRendezvousPlacementWithTopology(input RendezvousPlacementInput, topology ClusterTopologyProfile) RendezvousPlacementResult {
	if topology.Name == "" {
		topology = DefaultClusterTopologyProfile()
	}
	replicationFactor := input.ReplicationFactor
	if replicationFactor == 0 {
		replicationFactor = topology.ReplicationFactor
	}
	nodes := uniqueSortedNodeIDs(input.Nodes)
	result := RendezvousPlacementResult{
		ShardKey:           input.ShardKey,
		ReplicationFactor:  replicationFactor,
		TopologyGeneration: input.TopologyGeneration,
	}
	if len(nodes) == 0 || replicationFactor <= 0 || input.ShardKey == "" {
		return result
	}
	limit := replicationFactor
	if limit > len(nodes) {
		limit = len(nodes)
	}
	ranked := make([]rendezvousNodeScore, 0, len(nodes))
	for _, node := range nodes {
		ranked = append(ranked, rendezvousNodeScore{
			Node:  node,
			Score: stablePlacementHash(string(input.ShardKey) + "|" + string(node)),
		})
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].Score != ranked[j].Score {
			return ranked[i].Score > ranked[j].Score
		}
		return ranked[i].Node < ranked[j].Node
	})
	result.Owners = make([]NodeID, 0, limit)
	for i := 0; i < limit; i++ {
		result.Owners = append(result.Owners, ranked[i].Node)
	}
	result.Complete = len(result.Owners) == replicationFactor
	return result
}

// PlacementCacheKey returns a stable cache identity for this placement result.
func (r RendezvousPlacementResult) PlacementCacheKey() string {
	if r.ShardKey == "" || r.TopologyGeneration == "" || r.ReplicationFactor <= 0 {
		return ""
	}
	return string(r.TopologyGeneration) + "|" + string(r.ShardKey) + "|" + strconv.Itoa(r.ReplicationFactor)
}

// PlacementReason explains one physical placement decision.
type PlacementReason string

const (
	// PlacementReasonAllShards means the scope targets every shard.
	PlacementReasonAllShards PlacementReason = "all_shards"
	// PlacementReasonExplicitShards means the scope targets named shards.
	PlacementReasonExplicitShards PlacementReason = "explicit_shards"
	// PlacementReasonExplicitReplicas means the scope names eligible replicas.
	PlacementReasonExplicitReplicas PlacementReason = "explicit_replicas"
	// PlacementReasonPlacementPolicy means the scope carries a replica placement preference.
	PlacementReasonPlacementPolicy PlacementReason = "placement_policy"
	// PlacementReasonCacheScope means the scope carries cache validity metadata.
	PlacementReasonCacheScope PlacementReason = "cache_scope"
	// PlacementReasonSharedNodeSet means proxy-local state must be valid for shared data nodes.
	PlacementReasonSharedNodeSet PlacementReason = "shared_node_set"
	// PlacementReasonUnscoped means no physical placement information is available.
	PlacementReasonUnscoped PlacementReason = "unscoped"
)

// PlacementExplanation describes why a physical scope may touch specific shards or replicas.
type PlacementExplanation struct {
	Scope                         PhysicalScope
	Topology                      ClusterTopologyProfile
	Reasons                       []PlacementReason
	RequiresCrossProxyConsistency bool
	Details                       []string
}

// ExplainPlacement describes physical placement and cache/topology implications for a scope.
func ExplainPlacement(scope PhysicalScope, topology ClusterTopologyProfile) PlacementExplanation {
	if topology.Name == "" {
		topology = DefaultClusterTopologyProfile()
	}
	explanation := PlacementExplanation{
		Scope:                         clonePlacementPhysicalScope(scope),
		Topology:                      cloneClusterTopologyProfile(topology),
		RequiresCrossProxyConsistency: topology.RequiresCrossProxyConsistency(),
	}
	if scope.Unscoped() {
		explanation.Reasons = append(explanation.Reasons, PlacementReasonUnscoped)
		explanation.Details = append(explanation.Details, "physical scope has no shard, replica, placement, or cache metadata")
	}
	if scope.Shards.All {
		explanation.Reasons = append(explanation.Reasons, PlacementReasonAllShards)
		explanation.Details = append(explanation.Details, "scope targets all shards")
	} else if len(scope.Shards.Shards) > 0 {
		explanation.Reasons = append(explanation.Reasons, PlacementReasonExplicitShards)
		explanation.Details = append(explanation.Details, "scope targets explicit shards")
	}
	if len(scope.Replicas) > 0 {
		explanation.Reasons = append(explanation.Reasons, PlacementReasonExplicitReplicas)
		explanation.Details = append(explanation.Details, "scope names eligible replicas")
	}
	if scope.Placement != "" {
		explanation.Reasons = append(explanation.Reasons, PlacementReasonPlacementPolicy)
		explanation.Details = append(explanation.Details, "scope carries a replica placement policy")
	}
	if scope.Cache != "" && scope.Cache != CacheNone {
		explanation.Reasons = append(explanation.Reasons, PlacementReasonCacheScope)
		explanation.Details = append(explanation.Details, "scope carries a cache validity boundary")
	}
	if explanation.RequiresCrossProxyConsistency {
		explanation.Reasons = append(explanation.Reasons, PlacementReasonSharedNodeSet)
		explanation.Details = append(explanation.Details, "proxy-local cache and metadata state must remain compatible across proxies sharing the node set")
	}
	return explanation
}

// HasReason reports whether the placement explanation includes reason.
func (e PlacementExplanation) HasReason(reason PlacementReason) bool {
	for _, current := range e.Reasons {
		if current == reason {
			return true
		}
	}
	return false
}

func clonePlacementPhysicalScope(scope PhysicalScope) PhysicalScope {
	scope.Shards.Shards = append([]ShardID(nil), scope.Shards.Shards...)
	scope.Replicas = append([]ReplicaID(nil), scope.Replicas...)
	return scope
}

func uniqueSortedNodeIDs(nodes []NodeID) []NodeID {
	seen := make(map[NodeID]struct{}, len(nodes))
	unique := make([]NodeID, 0, len(nodes))
	for _, node := range nodes {
		if node == "" {
			continue
		}
		if _, ok := seen[node]; ok {
			continue
		}
		seen[node] = struct{}{}
		unique = append(unique, node)
	}
	sort.Slice(unique, func(i, j int) bool {
		return unique[i] < unique[j]
	})
	return unique
}

type rendezvousNodeScore struct {
	Node  NodeID
	Score uint64
}

func stablePlacementHash(value string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	var hash uint64 = offset64
	for i := 0; i < len(value); i++ {
		hash ^= uint64(value[i])
		hash *= prime64
	}
	return hash
}

package qsbridge

import "testing"

func TestExplainPlacementCapturesPhysicalScopeReasons(t *testing.T) {
	scope := PhysicalScope{
		Shards:    ShardSet{Shards: []ShardID{"orders-001", "orders-002"}},
		Replicas:  []ReplicaID{"replica-a"},
		Routing:   "custkey",
		Placement: PlacementLocal,
		Cache:     CacheCluster,
	}
	explanation := ExplainPlacement(scope, DefaultClusterTopologyProfile())
	for _, reason := range []PlacementReason{
		PlacementReasonExplicitShards,
		PlacementReasonExplicitReplicas,
		PlacementReasonPlacementPolicy,
		PlacementReasonCacheScope,
		PlacementReasonSharedNodeSet,
	} {
		if !explanation.HasReason(reason) {
			t.Fatalf("reasons = %#v, want %s", explanation.Reasons, reason)
		}
	}
	if !explanation.RequiresCrossProxyConsistency {
		t.Fatalf("explanation = %#v, want cross-proxy consistency", explanation)
	}
}

func TestExplainPlacementCapturesAllShardScope(t *testing.T) {
	explanation := ExplainPlacement(PhysicalScope{Shards: ShardSet{All: true}}, DefaultClusterTopologyProfile())
	if !explanation.HasReason(PlacementReasonAllShards) {
		t.Fatalf("reasons = %#v, want all-shards reason", explanation.Reasons)
	}
}

func TestExplainPlacementCapturesUnscopedScope(t *testing.T) {
	explanation := ExplainPlacement(PhysicalScope{}, ClusterTopologyProfile{Name: "single_proxy"})
	if !explanation.HasReason(PlacementReasonUnscoped) {
		t.Fatalf("reasons = %#v, want unscoped reason", explanation.Reasons)
	}
	if explanation.RequiresCrossProxyConsistency {
		t.Fatalf("explanation = %#v, want no cross-proxy consistency for single-proxy topology", explanation)
	}
}

func TestExplainPlacementCopiesMutableState(t *testing.T) {
	scope := PhysicalScope{
		Shards:   ShardSet{Shards: []ShardID{"orders-001"}},
		Replicas: []ReplicaID{"replica-a"},
	}
	explanation := ExplainPlacement(scope, DefaultClusterTopologyProfile())
	explanation.Scope.Shards.Shards[0] = "mutated"
	explanation.Scope.Replicas[0] = "mutated"
	explanation.Topology.ScaleAxes[0] = "mutated"

	again := ExplainPlacement(scope, DefaultClusterTopologyProfile())
	if again.Scope.Shards.Shards[0] != "orders-001" || again.Scope.Replicas[0] != "replica-a" {
		t.Fatalf("scope leaked mutation: %#v", again.Scope)
	}
	if again.Topology.ScaleAxes[0] != ClusterScaleAxisDataNodes {
		t.Fatalf("topology leaked mutation: %#v", again.Topology.ScaleAxes)
	}
}

func TestResolveRendezvousPlacementReturnsStableOwners(t *testing.T) {
	input := RendezvousPlacementInput{
		Nodes:              []NodeID{"node-c", "node-a", "node-b", "node-a"},
		ReplicationFactor:  2,
		ShardKey:           "orders/customer/42",
		TopologyGeneration: "topology-7",
	}
	first := ResolveRendezvousPlacement(input)
	second := ResolveRendezvousPlacement(input)
	if !first.Complete || len(first.Owners) != 2 {
		t.Fatalf("placement = %#v, want complete two-owner placement", first)
	}
	if first.Owners[0] != second.Owners[0] || first.Owners[1] != second.Owners[1] {
		t.Fatalf("placements = %#v/%#v, want stable owners", first, second)
	}
	if first.TopologyGeneration != "topology-7" || first.PlacementCacheKey() == "" {
		t.Fatalf("placement = %#v, want topology generation and cache key", first)
	}
}

func TestResolveRendezvousPlacementHandlesIncompleteReplication(t *testing.T) {
	placement := ResolveRendezvousPlacement(RendezvousPlacementInput{
		Nodes:              []NodeID{"node-a"},
		ShardKey:           "orders/customer/42",
		TopologyGeneration: "topology-7",
	})
	if placement.Complete || len(placement.Owners) != 1 || placement.Owners[0] != "node-a" {
		t.Fatalf("placement = %#v, want incomplete single-owner placement", placement)
	}
}

func TestResolveRendezvousPlacementUsesClusterReplicationFactor(t *testing.T) {
	placement := ResolveRendezvousPlacementWithTopology(RendezvousPlacementInput{
		Nodes:              []NodeID{"node-a", "node-b", "node-c"},
		ShardKey:           "orders/customer/42",
		TopologyGeneration: "topology-7",
	}, ClusterTopologyProfile{
		Name:              "test",
		ReplicationFactor: 2,
	})
	if !placement.Complete || placement.ReplicationFactor != 2 || len(placement.Owners) != 2 {
		t.Fatalf("placement = %#v, want topology RF owner set", placement)
	}
}

func TestResolveRendezvousPlacementCacheKeyIncludesTopologyGeneration(t *testing.T) {
	input := RendezvousPlacementInput{
		Nodes:              []NodeID{"node-a", "node-b"},
		ReplicationFactor:  1,
		ShardKey:           "orders/customer/42",
		TopologyGeneration: "topology-7",
	}
	first := ResolveRendezvousPlacement(input)
	input.TopologyGeneration = "topology-8"
	second := ResolveRendezvousPlacement(input)
	if first.PlacementCacheKey() == second.PlacementCacheKey() {
		t.Fatalf("cache keys = %q/%q, want topology generation to distinguish placement", first.PlacementCacheKey(), second.PlacementCacheKey())
	}
}

func TestRendezvousPlacementResultCacheKeyRequiresVersionedInputs(t *testing.T) {
	for _, result := range []RendezvousPlacementResult{
		{TopologyGeneration: "topology-7", ReplicationFactor: 1},
		{ShardKey: "orders/customer/42", ReplicationFactor: 1},
		{TopologyGeneration: "topology-7", ShardKey: "orders/customer/42"},
	} {
		if result.PlacementCacheKey() != "" {
			t.Fatalf("result = %#v, want empty cache key", result)
		}
	}
}

func TestResolveRendezvousPlacementRejectsMissingInputs(t *testing.T) {
	for _, input := range []RendezvousPlacementInput{
		{Nodes: []NodeID{"node-a"}, ReplicationFactor: 1},
		{ReplicationFactor: 1, ShardKey: "orders/customer/42"},
	} {
		placement := ResolveRendezvousPlacement(input)
		if placement.Complete || len(placement.Owners) != 0 {
			t.Fatalf("placement = %#v for input %#v, want no owners", placement, input)
		}
	}
}

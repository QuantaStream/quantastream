package qsbridge

import "testing"

func TestDefaultClusterTopologyProfileCapturesQuantaScaleAxes(t *testing.T) {
	profile := DefaultClusterTopologyProfile()
	if profile.Name != "quanta_shared_node_set" {
		t.Fatalf("profile name = %q, want quanta_shared_node_set", profile.Name)
	}
	if !clusterScaleAxesContain(profile.ScaleAxes, ClusterScaleAxisDataNodes) ||
		!clusterScaleAxesContain(profile.ScaleAxes, ClusterScaleAxisQueryProxies) ||
		!clusterScaleAxesContain(profile.ScaleAxes, ClusterScaleAxisMetadataPlane) {
		t.Fatalf("scale axes = %#v, want data nodes, query proxies, and metadata plane", profile.ScaleAxes)
	}
	if !profile.MultipleProxiesShareNodeSet || !profile.ProcessLocalProxyCaches || !profile.NodeLocalDataCopies {
		t.Fatalf("profile = %#v, want shared-node multi-proxy topology with local caches/copies", profile)
	}
	if profile.ReplicationFactor != 3 || profile.ReplicationFactorBackend != MetadataStoreBackendConsul {
		t.Fatalf("profile = %#v, want cluster-wide RF stored in Consul metadata", profile)
	}
	if profile.GenerationSource != TopologyGenerationSourceConsul || profile.GenerationInvalidation != MetadataInvalidationWatch {
		t.Fatalf("profile = %#v, want Consul/watch topology generation metadata", profile)
	}
	if !profile.RequiresCrossProxyConsistency() {
		t.Fatalf("profile = %#v, want cross-proxy consistency requirement", profile)
	}
}

func TestDefaultClusterTopologyProfileMentionsDictionaryConsistency(t *testing.T) {
	profile := DefaultClusterTopologyProfile()
	if !stringSliceContains(profile.ConsistencyBoundaries, "dictionary label/id mappings must be compatible across proxies targeting the same nodes") {
		t.Fatalf("consistency boundaries = %#v, want dictionary label/id topology boundary", profile.ConsistencyBoundaries)
	}
	if profile.DataNodeScaleUnit == "" || profile.QueryProxyScaleUnit == "" {
		t.Fatalf("scale units = %#v/%#v, want explicit node and proxy scale units", profile.DataNodeScaleUnit, profile.QueryProxyScaleUnit)
	}
}

func TestDefaultClusterTopologyProfileCopiesMutableState(t *testing.T) {
	first := DefaultClusterTopologyProfile()
	first.ScaleAxes[0] = "mutated"
	first.ConsistencyBoundaries[0] = "mutated"

	second := DefaultClusterTopologyProfile()
	if second.ScaleAxes[0] != ClusterScaleAxisDataNodes {
		t.Fatalf("scale axes leaked mutation: %#v", second.ScaleAxes)
	}
	if second.ConsistencyBoundaries[0] == "mutated" {
		t.Fatalf("consistency boundaries leaked mutation: %#v", second.ConsistencyBoundaries)
	}
}

func TestClusterTopologyProfileRequiresCrossProxyConsistencyDependsOnShape(t *testing.T) {
	profile := ClusterTopologyProfile{MultipleProxiesShareNodeSet: true, ProcessLocalProxyCaches: true}
	if !profile.RequiresCrossProxyConsistency() {
		t.Fatalf("profile = %#v, want cross-proxy consistency", profile)
	}
	profile.ProcessLocalProxyCaches = false
	if profile.RequiresCrossProxyConsistency() {
		t.Fatalf("profile = %#v, want no cross-proxy cache consistency requirement without proxy-local caches", profile)
	}
}

func TestClusterTopologyProfileBuildsGenerationState(t *testing.T) {
	state := DefaultClusterTopologyProfile().TopologyGenerationState(
		"topology-8",
		TopologyInvalidationNodeSetChanged,
		TopologyInvalidationReplicationFactorChanged,
	)
	if state.Generation != "topology-8" || state.Source != TopologyGenerationSourceConsul || state.Invalidation != MetadataInvalidationWatch {
		t.Fatalf("state = %#v, want Consul/watch topology generation", state)
	}
	if !topologyInvalidationReasonsContain(state.Reasons, TopologyInvalidationNodeSetChanged) ||
		!topologyInvalidationReasonsContain(state.Reasons, TopologyInvalidationReplicationFactorChanged) {
		t.Fatalf("reasons = %#v, want node-set and RF invalidation reasons", state.Reasons)
	}
}

func TestTopologyGenerationStateDetectsStalePlacement(t *testing.T) {
	state := DefaultClusterTopologyProfile().TopologyGenerationState("topology-8")
	cached := RendezvousPlacementResult{
		ShardKey:           "orders/customer/42",
		ReplicationFactor:  3,
		TopologyGeneration: "topology-7",
		Owners:             []NodeID{"node-a", "node-b", "node-c"},
		Complete:           true,
	}
	if !state.StalesPlacement(cached) {
		t.Fatalf("state = %#v cached = %#v, want stale placement", state, cached)
	}
	cached.TopologyGeneration = "topology-8"
	if state.StalesPlacement(cached) {
		t.Fatalf("state = %#v cached = %#v, want fresh placement", state, cached)
	}
}

func clusterScaleAxesContain(axes []ClusterScaleAxis, target ClusterScaleAxis) bool {
	for _, axis := range axes {
		if axis == target {
			return true
		}
	}
	return false
}

func stringSliceContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func topologyInvalidationReasonsContain(reasons []TopologyInvalidationReason, target TopologyInvalidationReason) bool {
	for _, reason := range reasons {
		if reason == target {
			return true
		}
	}
	return false
}

package qsbridge

// ClusterScaleAxis identifies one independent scaling axis in a Quanta deployment.
type ClusterScaleAxis string

const (
	// ClusterScaleAxisDataNodes scales storage, shard-local index state, and storage I/O.
	ClusterScaleAxisDataNodes ClusterScaleAxis = "data_nodes"
	// ClusterScaleAxisQueryProxies scales SQL planning, coordination, merge work, and client protocol handling.
	ClusterScaleAxisQueryProxies ClusterScaleAxis = "query_proxies"
	// ClusterScaleAxisMetadataPlane scales cached catalog, topology, dictionary, and policy metadata access.
	ClusterScaleAxisMetadataPlane ClusterScaleAxis = "metadata_plane"
)

// TopologyGenerationSource identifies where topology generation metadata is read from.
type TopologyGenerationSource string

const (
	// TopologyGenerationSourceConsul means topology generation is stored in Consul metadata.
	TopologyGenerationSourceConsul TopologyGenerationSource = "consul"
	// TopologyGenerationSourceAdapter means an adapter supplies topology generation metadata.
	TopologyGenerationSourceAdapter TopologyGenerationSource = "adapter"
)

// TopologyInvalidationReason identifies a change that makes placement/cache state stale.
type TopologyInvalidationReason string

const (
	// TopologyInvalidationNodeSetChanged means stable node membership changed.
	TopologyInvalidationNodeSetChanged TopologyInvalidationReason = "node_set_changed"
	// TopologyInvalidationReplicationFactorChanged means the cluster-wide replication factor changed.
	TopologyInvalidationReplicationFactorChanged TopologyInvalidationReason = "replication_factor_changed"
	// TopologyInvalidationPlacementPolicyChanged means shard placement policy metadata changed.
	TopologyInvalidationPlacementPolicyChanged TopologyInvalidationReason = "placement_policy_changed"
	// TopologyInvalidationManualRefresh means an operator or adapter forced a topology refresh.
	TopologyInvalidationManualRefresh TopologyInvalidationReason = "manual_refresh"
)

// TopologyGenerationState records the topology generation used for placement and cache decisions.
type TopologyGenerationState struct {
	Generation   TopologyGeneration
	Source       TopologyGenerationSource
	Invalidation MetadataInvalidationMode
	Reasons      []TopologyInvalidationReason
}

// ClusterTopologyProfile records topology assumptions that planning and caching must respect.
type ClusterTopologyProfile struct {
	Name                        string
	ScaleAxes                   []ClusterScaleAxis
	DataNodeScaleUnit           string
	QueryProxyScaleUnit         string
	ReplicationFactor           int
	ReplicationFactorBackend    MetadataStoreBackend
	GenerationSource            TopologyGenerationSource
	GenerationInvalidation      MetadataInvalidationMode
	MultipleProxiesShareNodeSet bool
	ProcessLocalProxyCaches     bool
	NodeLocalDataCopies         bool
	ConsistencyBoundaries       []string
}

// DefaultClusterTopologyProfile returns the near-term Quanta topology contract.
func DefaultClusterTopologyProfile() ClusterTopologyProfile {
	return cloneClusterTopologyProfile(defaultClusterTopologyProfile)
}

// RequiresCrossProxyConsistency reports whether proxy-local state must agree across proxies.
func (p ClusterTopologyProfile) RequiresCrossProxyConsistency() bool {
	return p.MultipleProxiesShareNodeSet && p.ProcessLocalProxyCaches
}

// TopologyGenerationState returns generation metadata for this topology profile.
func (p ClusterTopologyProfile) TopologyGenerationState(generation TopologyGeneration, reasons ...TopologyInvalidationReason) TopologyGenerationState {
	source := p.GenerationSource
	if source == "" {
		source = TopologyGenerationSourceConsul
	}
	invalidation := p.GenerationInvalidation
	if invalidation == "" {
		invalidation = MetadataInvalidationWatch
	}
	return TopologyGenerationState{
		Generation:   generation,
		Source:       source,
		Invalidation: invalidation,
		Reasons:      append([]TopologyInvalidationReason(nil), reasons...),
	}
}

// StalesPlacement reports whether cached placement metadata was resolved against another topology generation.
func (s TopologyGenerationState) StalesPlacement(cached RendezvousPlacementResult) bool {
	return s.Generation != "" && cached.TopologyGeneration != "" && s.Generation != cached.TopologyGeneration
}

var defaultClusterTopologyProfile = ClusterTopologyProfile{
	Name: "quanta_shared_node_set",
	ScaleAxes: []ClusterScaleAxis{
		ClusterScaleAxisDataNodes,
		ClusterScaleAxisQueryProxies,
		ClusterScaleAxisMetadataPlane,
	},
	DataNodeScaleUnit:           "add data nodes to scale shard-local storage, bitmap/BSI/index state, and storage I/O",
	QueryProxyScaleUnit:         "add query proxies to scale SQL parsing, planning, coordination, merge work, and client protocol handling",
	ReplicationFactor:           3,
	ReplicationFactorBackend:    MetadataStoreBackendConsul,
	GenerationSource:            TopologyGenerationSourceConsul,
	GenerationInvalidation:      MetadataInvalidationWatch,
	MultipleProxiesShareNodeSet: true,
	ProcessLocalProxyCaches:     true,
	NodeLocalDataCopies:         true,
	ConsistencyBoundaries: []string{
		"proxy-local caches must be valid for a shared data-node set",
		"dictionary label/id mappings must be compatible across proxies targeting the same nodes",
		"catalog, topology, dictionary, and policy metadata need explicit versions or invalidation",
		"nodes own shard-local physical state while proxies own coordination and result shaping",
	},
}

func cloneClusterTopologyProfile(profile ClusterTopologyProfile) ClusterTopologyProfile {
	profile.ScaleAxes = append([]ClusterScaleAxis(nil), profile.ScaleAxes...)
	profile.ConsistencyBoundaries = append([]string(nil), profile.ConsistencyBoundaries...)
	return profile
}

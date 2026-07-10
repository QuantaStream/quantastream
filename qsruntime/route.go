package qsruntime

// ExecutionPath names the runtime path that will execute a neutral request.
type ExecutionPath string

const (
	// ExecutionPathDirectQIAB is the preferred in-process Quanta-in-a-box path.
	ExecutionPathDirectQIAB ExecutionPath = "direct_qiab"
	// ExecutionPathLegacyGRPC preserves the legacy node gRPC path as a compatibility adapter.
	ExecutionPathLegacyGRPC ExecutionPath = "legacy_grpc"
	// ExecutionPathLegacyProxy preserves the legacy proxy path as a compatibility adapter.
	ExecutionPathLegacyProxy ExecutionPath = "legacy_proxy"
)

// ServiceDiscoveryMode names how runtime targets are discovered.
type ServiceDiscoveryMode string

const (
	// ServiceDiscoveryLocal selects the local in-process target.
	ServiceDiscoveryLocal ServiceDiscoveryMode = "local"
	// ServiceDiscoveryConsul selects targets through the existing Consul-backed topology.
	ServiceDiscoveryConsul ServiceDiscoveryMode = "consul"
)

// RuntimeTarget identifies the selected runtime target without binding qsbridge to transport details.
type RuntimeTarget struct {
	NodeID  string
	Address string
	Local   bool
}

// ExecutionRoute describes how qsruntime should satisfy an execution request.
type ExecutionRoute struct {
	Path      ExecutionPath
	Discovery ServiceDiscoveryMode
	Target    RuntimeTarget
}

// DirectQIABRoute returns the default local in-process execution route.
func DirectQIABRoute() ExecutionRoute {
	return ExecutionRoute{
		Path:      ExecutionPathDirectQIAB,
		Discovery: ServiceDiscoveryLocal,
		Target: RuntimeTarget{
			Local: true,
		},
	}
}

// ConsulDirectRoute returns a direct runtime route using Consul-discovered topology.
func ConsulDirectRoute(target RuntimeTarget) ExecutionRoute {
	return ExecutionRoute{
		Path:      ExecutionPathDirectQIAB,
		Discovery: ServiceDiscoveryConsul,
		Target:    target,
	}
}

// LegacyGRPCRoute returns a compatibility route through the legacy gRPC node boundary.
func LegacyGRPCRoute(target RuntimeTarget) ExecutionRoute {
	return ExecutionRoute{
		Path:      ExecutionPathLegacyGRPC,
		Discovery: ServiceDiscoveryConsul,
		Target:    target,
	}
}

// LegacyProxyRoute returns a compatibility route through the legacy proxy boundary.
func LegacyProxyRoute(target RuntimeTarget) ExecutionRoute {
	return ExecutionRoute{
		Path:      ExecutionPathLegacyProxy,
		Discovery: ServiceDiscoveryConsul,
		Target:    target,
	}
}

// Direct reports whether the route uses the direct execution path.
func (r ExecutionRoute) Direct() bool {
	return r.Path == ExecutionPathDirectQIAB
}

// UsesConsul reports whether the route depends on Consul-backed service discovery.
func (r ExecutionRoute) UsesConsul() bool {
	return r.Discovery == ServiceDiscoveryConsul
}

// CompatibilityPath reports whether the route preserves a legacy runtime boundary.
func (r ExecutionRoute) CompatibilityPath() bool {
	return r.Path == ExecutionPathLegacyGRPC || r.Path == ExecutionPathLegacyProxy
}

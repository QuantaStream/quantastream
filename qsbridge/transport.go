package qsbridge

// TransportRole identifies why one transport boundary exists.
type TransportRole string

const (
	// TransportRoleClientProtocol carries client-facing requests into Quanta.
	TransportRoleClientProtocol TransportRole = "client_protocol"
	// TransportRoleInternalCluster carries proxy-to-node or node-to-node work.
	TransportRoleInternalCluster TransportRole = "internal_cluster"
	// TransportRoleInProcess carries embedded QIAB calls without network hops.
	TransportRoleInProcess TransportRole = "in_process"
)

// TransportKind identifies one concrete transport family.
type TransportKind string

const (
	// TransportKindMySQLWire is the MySQL-compatible client wire protocol.
	TransportKindMySQLWire TransportKind = "mysql_wire"
	// TransportKindGRPCAPI is the future typed API and control-plane transport.
	TransportKindGRPCAPI TransportKind = "grpc_api"
	// TransportKindQuantaInternal is Quanta's internal proxy-to-node transport.
	TransportKindQuantaInternal TransportKind = "quanta_internal"
	// TransportKindInProcess is an embedded direct-call transport for QIAB.
	TransportKindInProcess TransportKind = "in_process"
)

// ExecutionPlacement identifies where a request is expected to execute.
type ExecutionPlacement string

const (
	// ExecutionPlacementProxy means work is accepted, shaped, or coordinated at the proxy.
	ExecutionPlacementProxy ExecutionPlacement = "proxy"
	// ExecutionPlacementNode means work executes on a storage/runtime node.
	ExecutionPlacementNode ExecutionPlacement = "node"
	// ExecutionPlacementEmbedded means work executes inside one embedded process.
	ExecutionPlacementEmbedded ExecutionPlacement = "embedded"
)

// TransportBoundary describes a protocol, transport, and placement responsibility.
type TransportBoundary struct {
	Role            TransportRole
	Kind            TransportKind
	Protocol        ProtocolKind
	Owner           WireAdapterOwner
	Placement       ExecutionPlacement
	Networked       bool
	PortIndependent bool
	MetadataOnly    bool
	Detail          string
}

// DefaultTransportBoundaries returns transport and placement boundaries.
func DefaultTransportBoundaries() []TransportBoundary {
	return cloneTransportBoundaries(defaultTransportBoundaries)
}

// TransportBoundariesForRole returns boundaries matching role.
func TransportBoundariesForRole(role TransportRole) []TransportBoundary {
	boundaries := DefaultTransportBoundaries()
	if role == "" {
		return boundaries
	}
	filtered := make([]TransportBoundary, 0, len(boundaries))
	for _, boundary := range boundaries {
		if boundary.Role == role {
			filtered = append(filtered, boundary)
		}
	}
	return filtered
}

var defaultTransportBoundaries = []TransportBoundary{
	{
		Role:            TransportRoleClientProtocol,
		Kind:            TransportKindMySQLWire,
		Protocol:        ProtocolMySQL,
		Owner:           WireAdapterOwnerProtocolAdapter,
		Placement:       ExecutionPlacementProxy,
		Networked:       true,
		PortIndependent: true,
		MetadataOnly:    true,
		Detail:          "MySQL clients speak the MySQL wire protocol to an adapter; shared port numbers must not imply shared internal transport semantics",
	},
	{
		Role:            TransportRoleClientProtocol,
		Kind:            TransportKindGRPCAPI,
		Protocol:        ProtocolGRPC,
		Owner:           WireAdapterOwnerProtocolAdapter,
		Placement:       ExecutionPlacementProxy,
		Networked:       true,
		PortIndependent: true,
		MetadataOnly:    true,
		Detail:          "gRPC exposes typed admin, inspection, planning, and future API surfaces; it does not tunnel MySQL packets",
	},
	{
		Role:            TransportRoleInternalCluster,
		Kind:            TransportKindQuantaInternal,
		Protocol:        ProtocolUnknown,
		Owner:           WireAdapterOwnerExecutor,
		Placement:       ExecutionPlacementNode,
		Networked:       true,
		PortIndependent: true,
		MetadataOnly:    true,
		Detail:          "proxy-to-node and node-to-node execution traffic is an internal runtime concern distinct from client protocols",
	},
	{
		Role:            TransportRoleInProcess,
		Kind:            TransportKindInProcess,
		Protocol:        ProtocolGo,
		Owner:           WireAdapterOwnerExecutor,
		Placement:       ExecutionPlacementEmbedded,
		Networked:       false,
		PortIndependent: true,
		MetadataOnly:    true,
		Detail:          "QIAB can eventually bypass network transport and call the same planning and execution contracts directly",
	},
}

func cloneTransportBoundaries(boundaries []TransportBoundary) []TransportBoundary {
	return append([]TransportBoundary(nil), boundaries...)
}

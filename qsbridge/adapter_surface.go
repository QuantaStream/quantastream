package qsbridge

// AdapterSurfaceKind identifies one public or internal adapter surface.
type AdapterSurfaceKind string

const (
	// AdapterSurfaceMySQLServer is the MySQL-compatible server adapter.
	AdapterSurfaceMySQLServer AdapterSurfaceKind = "mysql_server"
	// AdapterSurfaceGRPCAPI is the typed gRPC API and control-plane adapter.
	AdapterSurfaceGRPCAPI AdapterSurfaceKind = "grpc_api"
	// AdapterSurfaceEmbedded is the in-process QIAB adapter.
	AdapterSurfaceEmbedded AdapterSurfaceKind = "embedded_qiab"
	// AdapterSurfaceInternalExecution is the internal executor transport adapter.
	AdapterSurfaceInternalExecution AdapterSurfaceKind = "internal_execution"
)

// AdapterSurfaceAudience identifies who should call one adapter surface.
type AdapterSurfaceAudience string

const (
	// AdapterSurfaceAudienceSQLClient means ordinary SQL drivers call the surface.
	AdapterSurfaceAudienceSQLClient AdapterSurfaceAudience = "sql_client"
	// AdapterSurfaceAudienceControlPlane means management tooling calls the surface.
	AdapterSurfaceAudienceControlPlane AdapterSurfaceAudience = "control_plane"
	// AdapterSurfaceAudienceEmbedded means QIAB or tests call the surface in-process.
	AdapterSurfaceAudienceEmbedded AdapterSurfaceAudience = "embedded"
	// AdapterSurfaceAudienceRuntime means runtime internals call the surface.
	AdapterSurfaceAudienceRuntime AdapterSurfaceAudience = "runtime"
)

// AdapterSurface describes a named qsbridge-facing adapter surface.
type AdapterSurface struct {
	Kind                 AdapterSurfaceKind
	Audience             AdapterSurfaceAudience
	Protocol             ProtocolKind
	Transport            TransportKind
	Placement            ExecutionPlacement
	Owner                WireAdapterOwner
	ClientFacing         bool
	ControlPlane         bool
	Embedded             bool
	Internal             bool
	UsesQSBridgeMetadata bool
	Detail               string
}

// AdapterSurfaceSummaryRow describes aggregate adapter surface metadata.
type AdapterSurfaceSummaryRow struct {
	SurfaceCount            int
	ClientFacingCount       int
	ControlPlaneCount       int
	EmbeddedCount           int
	InternalCount           int
	UsesQSBridgeCount       int
	MySQLProtocolCount      int
	GRPCProtocolCount       int
	InProcessTransportCount int
}

// DefaultAdapterSurfaces returns the named adapter surfaces for the refactor.
func DefaultAdapterSurfaces() []AdapterSurface {
	return cloneAdapterSurfaces(defaultAdapterSurfaces)
}

// AdapterSurfacesForAudience returns adapter surfaces matching audience.
func AdapterSurfacesForAudience(audience AdapterSurfaceAudience) []AdapterSurface {
	surfaces := DefaultAdapterSurfaces()
	if audience == "" {
		return surfaces
	}
	filtered := make([]AdapterSurface, 0, len(surfaces))
	for _, surface := range surfaces {
		if surface.Audience == audience {
			filtered = append(filtered, surface)
		}
	}
	return filtered
}

var defaultAdapterSurfaces = []AdapterSurface{
	{
		Kind:                 AdapterSurfaceMySQLServer,
		Audience:             AdapterSurfaceAudienceSQLClient,
		Protocol:             ProtocolMySQL,
		Transport:            TransportKindMySQLWire,
		Placement:            ExecutionPlacementProxy,
		Owner:                WireAdapterOwnerProtocolAdapter,
		ClientFacing:         true,
		UsesQSBridgeMetadata: true,
		Detail:               "MySQL server adapter accepts standard clients and maps wire commands to qsbridge planning, handoff, and response metadata",
	},
	{
		Kind:                 AdapterSurfaceGRPCAPI,
		Audience:             AdapterSurfaceAudienceControlPlane,
		Protocol:             ProtocolGRPC,
		Transport:            TransportKindGRPCAPI,
		Placement:            ExecutionPlacementProxy,
		Owner:                WireAdapterOwnerProtocolAdapter,
		ClientFacing:         true,
		ControlPlane:         true,
		UsesQSBridgeMetadata: true,
		Detail:               "gRPC API exposes typed catalog, inspection, planning, admin, and future data APIs without tunneling MySQL packets",
	},
	{
		Kind:                 AdapterSurfaceEmbedded,
		Audience:             AdapterSurfaceAudienceEmbedded,
		Protocol:             ProtocolGo,
		Transport:            TransportKindInProcess,
		Placement:            ExecutionPlacementEmbedded,
		Owner:                WireAdapterOwnerExecutor,
		Embedded:             true,
		UsesQSBridgeMetadata: true,
		Detail:               "embedded QIAB adapter can call the same qsbridge and executor contracts directly without sockets or packet serialization",
	},
	{
		Kind:                 AdapterSurfaceInternalExecution,
		Audience:             AdapterSurfaceAudienceRuntime,
		Protocol:             ProtocolUnknown,
		Transport:            TransportKindQuantaInternal,
		Placement:            ExecutionPlacementNode,
		Owner:                WireAdapterOwnerExecutor,
		Internal:             true,
		UsesQSBridgeMetadata: true,
		Detail:               "internal execution adapter carries proxy-to-node or node-to-node work after client protocol concerns have ended",
	},
}

func cloneAdapterSurfaces(surfaces []AdapterSurface) []AdapterSurface {
	return append([]AdapterSurface(nil), surfaces...)
}

func summarizeAdapterSurfaces(surfaces []AdapterSurface) AdapterSurfaceSummaryRow {
	summary := AdapterSurfaceSummaryRow{SurfaceCount: len(surfaces)}
	for _, surface := range surfaces {
		if surface.ClientFacing {
			summary.ClientFacingCount++
		}
		if surface.ControlPlane {
			summary.ControlPlaneCount++
		}
		if surface.Embedded {
			summary.EmbeddedCount++
		}
		if surface.Internal {
			summary.InternalCount++
		}
		if surface.UsesQSBridgeMetadata {
			summary.UsesQSBridgeCount++
		}
		switch surface.Protocol {
		case ProtocolMySQL:
			summary.MySQLProtocolCount++
		case ProtocolGRPC:
			summary.GRPCProtocolCount++
		}
		if surface.Transport == TransportKindInProcess {
			summary.InProcessTransportCount++
		}
	}
	return summary
}

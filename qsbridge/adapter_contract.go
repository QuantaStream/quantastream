package qsbridge

// AdapterContractConcern identifies one contract required by an adapter surface.
type AdapterContractConcern string

const (
	// AdapterContractProtocolDecode covers protocol command or RPC decoding.
	AdapterContractProtocolDecode AdapterContractConcern = "protocol_decode"
	// AdapterContractAuthentication covers login, credential, or token exchange.
	AdapterContractAuthentication AdapterContractConcern = "authentication"
	// AdapterContractSession covers selected schema, variables, and connection state.
	AdapterContractSession AdapterContractConcern = "session"
	// AdapterContractStatementPlanning covers SQL planning and handoff metadata.
	AdapterContractStatementPlanning AdapterContractConcern = "statement_planning"
	// AdapterContractPreparedExecution covers prepared handle and parameter metadata.
	AdapterContractPreparedExecution AdapterContractConcern = "prepared_execution"
	// AdapterContractCatalogDiscovery covers schemas, tables, columns, and functions.
	AdapterContractCatalogDiscovery AdapterContractConcern = "catalog_discovery"
	// AdapterContractInspection covers explain, profile, readiness, and diagnostics.
	AdapterContractInspection AdapterContractConcern = "inspection"
	// AdapterContractResultMetadata covers protocol-neutral result schemas and responses.
	AdapterContractResultMetadata AdapterContractConcern = "result_metadata"
	// AdapterContractResultSerialization covers protocol-specific response encoding.
	AdapterContractResultSerialization AdapterContractConcern = "result_serialization"
	// AdapterContractCancellation covers cancellation transport and request lookup.
	AdapterContractCancellation AdapterContractConcern = "cancellation"
	// AdapterContractExecutionDispatch covers native, legacy, or internal executor handoff.
	AdapterContractExecutionDispatch AdapterContractConcern = "execution_dispatch"
	// AdapterContractTopology covers cluster placement, routing, and node addressing.
	AdapterContractTopology AdapterContractConcern = "topology"
)

// AdapterContract describes one contract needed by an adapter surface.
type AdapterContract struct {
	Surface        AdapterSurfaceKind
	Concern        AdapterContractConcern
	Layer          CompatibilityLayer
	Status         CompatibilityStatus
	Owner          WireAdapterOwner
	Required       bool
	AdapterOwned   bool
	RuntimeOwned   bool
	MetadataName   string
	Implementation string
	Detail         string
}

// AdapterContractSummary aggregates readiness counts for one adapter surface.
type AdapterContractSummary struct {
	Surface            AdapterSurfaceKind
	ContractCount      int
	RequiredCount      int
	MetadataOnlyCount  int
	BoundaryOnlyCount  int
	DeferredCount      int
	AdapterOwnedCount  int
	RuntimeOwnedCount  int
	QSBridgeOwnedCount int
}

// DefaultAdapterContracts returns adapter implementation contracts.
func DefaultAdapterContracts() []AdapterContract {
	return cloneAdapterContracts(defaultAdapterContracts)
}

// AdapterContractsForSurface returns contracts for a named adapter surface.
func AdapterContractsForSurface(surface AdapterSurfaceKind) []AdapterContract {
	contracts := DefaultAdapterContracts()
	if surface == "" {
		return contracts
	}
	filtered := make([]AdapterContract, 0, len(contracts))
	for _, contract := range contracts {
		if contract.Surface == surface {
			filtered = append(filtered, contract)
		}
	}
	return filtered
}

// SummarizeAdapterContracts aggregates contract readiness by adapter surface.
func SummarizeAdapterContracts(contracts []AdapterContract) []AdapterContractSummary {
	if len(contracts) == 0 {
		return nil
	}
	summaries := make([]AdapterContractSummary, 0)
	indexBySurface := make(map[AdapterSurfaceKind]int)
	for _, contract := range contracts {
		index, ok := indexBySurface[contract.Surface]
		if !ok {
			index = len(summaries)
			indexBySurface[contract.Surface] = index
			summaries = append(summaries, AdapterContractSummary{Surface: contract.Surface})
		}
		summary := &summaries[index]
		summary.ContractCount++
		if contract.Required {
			summary.RequiredCount++
		}
		switch contract.Status {
		case CompatibilityStatusMetadataOnly:
			summary.MetadataOnlyCount++
		case CompatibilityStatusBoundaryOnly:
			summary.BoundaryOnlyCount++
		case CompatibilityStatusDeferred:
			summary.DeferredCount++
		}
		if contract.AdapterOwned {
			summary.AdapterOwnedCount++
		}
		if contract.RuntimeOwned {
			summary.RuntimeOwnedCount++
		}
		if contract.Owner == WireAdapterOwnerQSBridge {
			summary.QSBridgeOwnedCount++
		}
	}
	return summaries
}

// AdapterContractSummariesForSurface returns readiness summaries for a surface.
func AdapterContractSummariesForSurface(surface AdapterSurfaceKind) []AdapterContractSummary {
	return SummarizeAdapterContracts(AdapterContractsForSurface(surface))
}

var defaultAdapterContracts = []AdapterContract{
	{
		Surface:        AdapterSurfaceMySQLServer,
		Concern:        AdapterContractProtocolDecode,
		Layer:          CompatibilityLayerAdapter,
		Status:         CompatibilityStatusBoundaryOnly,
		Owner:          WireAdapterOwnerProtocolAdapter,
		Required:       true,
		AdapterOwned:   true,
		MetadataName:   "wire_adapter",
		Implementation: "decode MySQL commands and packet state outside qsbridge",
		Detail:         "COM_QUERY, prepared commands, connection lifecycle, and packet framing stay in the MySQL adapter",
	},
	{
		Surface:        AdapterSurfaceMySQLServer,
		Concern:        AdapterContractAuthentication,
		Layer:          CompatibilityLayerAdapter,
		Status:         CompatibilityStatusBoundaryOnly,
		Owner:          WireAdapterOwnerDeployment,
		Required:       true,
		AdapterOwned:   true,
		MetadataName:   "authentication",
		Implementation: "exchange MySQL auth packets and validate credentials through deployment hooks",
		Detail:         "qsbridge supplies authentication metadata; the adapter owns password exchange and enterprise identity integration",
	},
	{
		Surface:        AdapterSurfaceMySQLServer,
		Concern:        AdapterContractStatementPlanning,
		Layer:          CompatibilityLayerClient,
		Status:         CompatibilityStatusMetadataOnly,
		Owner:          WireAdapterOwnerQSBridge,
		Required:       true,
		MetadataName:   "client_exchange",
		Implementation: "map decoded SQL text to qsbridge planning, route, handoff, and response preview metadata",
		Detail:         "the adapter calls qsbridge metadata services before choosing native execution or legacy fallback",
	},
	{
		Surface:        AdapterSurfaceMySQLServer,
		Concern:        AdapterContractPreparedExecution,
		Layer:          CompatibilityLayerClient,
		Status:         CompatibilityStatusMetadataOnly,
		Owner:          WireAdapterOwnerQSBridge,
		Required:       true,
		MetadataName:   "prepared_metadata",
		Implementation: "use qsbridge prepared-handle, parameter, long-data, and execution metadata",
		Detail:         "wire-specific binary parameter decoding remains adapter-owned",
	},
	{
		Surface:        AdapterSurfaceMySQLServer,
		Concern:        AdapterContractResultMetadata,
		Layer:          CompatibilityLayerProtocol,
		Status:         CompatibilityStatusMetadataOnly,
		Owner:          WireAdapterOwnerQSBridge,
		Required:       true,
		MetadataName:   "result_schema",
		Implementation: "consume protocol-neutral result schemas, OK descriptors, warnings, and errors",
		Detail:         "qsbridge describes results while the adapter chooses MySQL text or binary packet serialization",
	},
	{
		Surface:        AdapterSurfaceMySQLServer,
		Concern:        AdapterContractResultSerialization,
		Layer:          CompatibilityLayerAdapter,
		Status:         CompatibilityStatusBoundaryOnly,
		Owner:          WireAdapterOwnerProtocolAdapter,
		Required:       true,
		AdapterOwned:   true,
		MetadataName:   "wire_adapter",
		Implementation: "serialize rows, OK packets, ERR packets, metadata packets, and status flags",
		Detail:         "this is intentionally outside qsbridge so planning metadata stays protocol-neutral",
	},
	{
		Surface:        AdapterSurfaceMySQLServer,
		Concern:        AdapterContractCancellation,
		Layer:          CompatibilityLayerAdapter,
		Status:         CompatibilityStatusBoundaryOnly,
		Owner:          WireAdapterOwnerProtocolAdapter,
		Required:       true,
		AdapterOwned:   true,
		MetadataName:   "cancellation",
		Implementation: "map KILL QUERY or connection cancellation to request ids and executor cancellation hooks",
		Detail:         "qsbridge records cancellation metadata but does not own socket interruption or query kill routing",
	},
	{
		Surface:        AdapterSurfaceGRPCAPI,
		Concern:        AdapterContractCatalogDiscovery,
		Layer:          CompatibilityLayerClient,
		Status:         CompatibilityStatusMetadataOnly,
		Owner:          WireAdapterOwnerQSBridge,
		Required:       true,
		MetadataName:   "catalog_discovery",
		Implementation: "expose typed catalog, function, encoding, dictionary, and metadata-store APIs",
		Detail:         "gRPC should adapt qsbridge catalog rowsets and typed structs rather than create a second catalog truth",
	},
	{
		Surface:        AdapterSurfaceGRPCAPI,
		Concern:        AdapterContractInspection,
		Layer:          CompatibilityLayerClient,
		Status:         CompatibilityStatusMetadataOnly,
		Owner:          WireAdapterOwnerQSBridge,
		Required:       true,
		MetadataName:   "inspection",
		Implementation: "serve typed explain, readiness, compatibility, optimizer, profile, and diagnostic metadata",
		Detail:         "the control plane should expose structured qsbridge metadata directly",
	},
	{
		Surface:        AdapterSurfaceGRPCAPI,
		Concern:        AdapterContractResultSerialization,
		Layer:          CompatibilityLayerAdapter,
		Status:         CompatibilityStatusBoundaryOnly,
		Owner:          WireAdapterOwnerProtocolAdapter,
		Required:       true,
		AdapterOwned:   true,
		MetadataName:   "transport",
		Implementation: "serialize protocol-neutral metadata into typed RPC responses and streams",
		Detail:         "gRPC is a typed API surface, not a MySQL packet tunnel",
	},
	{
		Surface:        AdapterSurfaceEmbedded,
		Concern:        AdapterContractStatementPlanning,
		Layer:          CompatibilityLayerClient,
		Status:         CompatibilityStatusMetadataOnly,
		Owner:          WireAdapterOwnerQSBridge,
		Required:       true,
		MetadataName:   "client_exchange",
		Implementation: "call qsbridge planning and handoff services directly in-process",
		Detail:         "QIAB can avoid network and packet overhead while preserving the same planning metadata contracts",
	},
	{
		Surface:        AdapterSurfaceEmbedded,
		Concern:        AdapterContractExecutionDispatch,
		Layer:          CompatibilityLayerExecutor,
		Status:         CompatibilityStatusBoundaryOnly,
		Owner:          WireAdapterOwnerExecutor,
		Required:       true,
		RuntimeOwned:   true,
		MetadataName:   "execution_dispatch",
		Implementation: "invoke native or legacy executor contracts without network transport",
		Detail:         "embedded dispatch remains runtime-owned even when planning occurs in the same process",
	},
	{
		Surface:        AdapterSurfaceInternalExecution,
		Concern:        AdapterContractExecutionDispatch,
		Layer:          CompatibilityLayerExecutor,
		Status:         CompatibilityStatusBoundaryOnly,
		Owner:          WireAdapterOwnerExecutor,
		Required:       true,
		RuntimeOwned:   true,
		MetadataName:   "native_executor",
		Implementation: "carry accepted physical work into storage/runtime execution paths",
		Detail:         "client protocol concerns have ended before this internal lane begins",
	},
	{
		Surface:        AdapterSurfaceInternalExecution,
		Concern:        AdapterContractTopology,
		Layer:          CompatibilityLayerExecutor,
		Status:         CompatibilityStatusDeferred,
		Owner:          WireAdapterOwnerExecutor,
		Required:       true,
		RuntimeOwned:   true,
		MetadataName:   "physical_scope",
		Implementation: "resolve shard, replica, placement, and routing metadata into runtime node addresses",
		Detail:         "qsbridge records physical scope, but distributed routing belongs to runtime topology code",
	},
}

func cloneAdapterContracts(contracts []AdapterContract) []AdapterContract {
	return append([]AdapterContract(nil), contracts...)
}

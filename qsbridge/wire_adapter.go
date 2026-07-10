package qsbridge

// WireAdapterOwner identifies who owns one protocol-adapter responsibility.
type WireAdapterOwner string

const (
	// WireAdapterOwnerProtocolAdapter means the network or in-process adapter owns the concern.
	WireAdapterOwnerProtocolAdapter WireAdapterOwner = "protocol_adapter"
	// WireAdapterOwnerQSBridge means qsbridge owns the protocol-neutral contract.
	WireAdapterOwnerQSBridge WireAdapterOwner = "qsbridge"
	// WireAdapterOwnerExecutor means native or legacy executors own the concern.
	WireAdapterOwnerExecutor WireAdapterOwner = "executor"
	// WireAdapterOwnerDeployment means deployment integration owns the concern.
	WireAdapterOwnerDeployment WireAdapterOwner = "deployment"
)

// WireAdapterConcern identifies one protocol/server boundary concern.
type WireAdapterConcern string

const (
	// WireAdapterConcernPacketIO is packet framing, sequence ids, sockets, and TLS.
	WireAdapterConcernPacketIO WireAdapterConcern = "packet_io"
	// WireAdapterConcernHandshake is initial greeting and capability exchange serialization.
	WireAdapterConcernHandshake WireAdapterConcern = "handshake"
	// WireAdapterConcernAuthExchange is credential plugin packet exchange or token validation.
	WireAdapterConcernAuthExchange WireAdapterConcern = "auth_exchange"
	// WireAdapterConcernCommandDecode is protocol command decoding such as COM_QUERY.
	WireAdapterConcernCommandDecode WireAdapterConcern = "command_decode"
	// WireAdapterConcernSQLPlanning is parser bridge, binding, diagnostics, and plan metadata.
	WireAdapterConcernSQLPlanning WireAdapterConcern = "sql_planning"
	// WireAdapterConcernParameterBinding is prepared and batch parameter metadata binding.
	WireAdapterConcernParameterBinding WireAdapterConcern = "parameter_binding"
	// WireAdapterConcernHandoff is route, protocol negotiation, authorization, and dispatch metadata.
	WireAdapterConcernHandoff WireAdapterConcern = "handoff"
	// WireAdapterConcernResultMetadata is protocol-neutral result schema and response descriptors.
	WireAdapterConcernResultMetadata WireAdapterConcern = "result_metadata"
	// WireAdapterConcernResultSerialization is text/binary resultset packet or RPC message encoding.
	WireAdapterConcernResultSerialization WireAdapterConcern = "result_serialization"
	// WireAdapterConcernSessionState is live session registry and connection lifecycle integration.
	WireAdapterConcernSessionState WireAdapterConcern = "session_state"
	// WireAdapterConcernCancellation is protocol cancellation request transport and lookup.
	WireAdapterConcernCancellation WireAdapterConcern = "cancellation"
	// WireAdapterConcernExecution is native, legacy, or future executor invocation.
	WireAdapterConcernExecution WireAdapterConcern = "execution"
)

// WireAdapterBoundary describes who owns one protocol adapter responsibility.
type WireAdapterBoundary struct {
	Concern      WireAdapterConcern
	Protocol     ProtocolKind
	Owner        WireAdapterOwner
	Permanent    bool
	MetadataOnly bool
	Detail       string
}

// DefaultWireAdapterBoundaries returns protocol-adapter responsibility boundaries.
func DefaultWireAdapterBoundaries() []WireAdapterBoundary {
	return cloneWireAdapterBoundaries(defaultWireAdapterBoundaries)
}

// WireAdapterBoundariesForProtocol returns boundaries matching protocol or protocol-agnostic rows.
func WireAdapterBoundariesForProtocol(protocol ProtocolKind) []WireAdapterBoundary {
	boundaries := DefaultWireAdapterBoundaries()
	if protocol == ProtocolUnknown {
		return boundaries
	}
	filtered := make([]WireAdapterBoundary, 0, len(boundaries))
	for _, boundary := range boundaries {
		if boundary.Protocol == ProtocolUnknown || boundary.Protocol == protocol {
			filtered = append(filtered, boundary)
		}
	}
	return filtered
}

var defaultWireAdapterBoundaries = []WireAdapterBoundary{
	{
		Concern:   WireAdapterConcernPacketIO,
		Protocol:  ProtocolMySQL,
		Owner:     WireAdapterOwnerProtocolAdapter,
		Permanent: true,
		Detail:    "MySQL packet framing, sequence ids, socket lifecycle, and TLS stay outside qsbridge",
	},
	{
		Concern:   WireAdapterConcernHandshake,
		Protocol:  ProtocolMySQL,
		Owner:     WireAdapterOwnerProtocolAdapter,
		Permanent: true,
		Detail:    "adapter serializes greeting, capability, charset, status, and auth-plugin packets from qsbridge metadata",
	},
	{
		Concern:   WireAdapterConcernAuthExchange,
		Protocol:  ProtocolUnknown,
		Owner:     WireAdapterOwnerDeployment,
		Permanent: true,
		Detail:    "password exchange, JWT/OAuth validation, and enterprise identity hooks are deployment adapter concerns",
	},
	{
		Concern:   WireAdapterConcernCommandDecode,
		Protocol:  ProtocolMySQL,
		Owner:     WireAdapterOwnerProtocolAdapter,
		Permanent: true,
		Detail:    "adapter decodes COM_QUERY, COM_STMT_PREPARE, COM_STMT_EXECUTE, COM_PING, COM_QUIT, and related commands",
	},
	{
		Concern:      WireAdapterConcernSQLPlanning,
		Protocol:     ProtocolUnknown,
		Owner:        WireAdapterOwnerQSBridge,
		Permanent:    true,
		MetadataOnly: true,
		Detail:       "qsbridge owns parser-neutral planning contracts, diagnostics, native blockers, and explain metadata",
	},
	{
		Concern:      WireAdapterConcernParameterBinding,
		Protocol:     ProtocolUnknown,
		Owner:        WireAdapterOwnerQSBridge,
		Permanent:    true,
		MetadataOnly: true,
		Detail:       "qsbridge owns prepared and batch parameter metadata after adapters decode protocol values",
	},
	{
		Concern:      WireAdapterConcernHandoff,
		Protocol:     ProtocolUnknown,
		Owner:        WireAdapterOwnerQSBridge,
		Permanent:    true,
		MetadataOnly: true,
		Detail:       "qsbridge owns route, authorization, protocol negotiation, fallback, and dispatch-preview contracts",
	},
	{
		Concern:      WireAdapterConcernResultMetadata,
		Protocol:     ProtocolUnknown,
		Owner:        WireAdapterOwnerQSBridge,
		Permanent:    true,
		MetadataOnly: true,
		Detail:       "qsbridge owns result schema, OK/status descriptors, errors, warnings, and response ordering metadata",
	},
	{
		Concern:   WireAdapterConcernResultSerialization,
		Protocol:  ProtocolMySQL,
		Owner:     WireAdapterOwnerProtocolAdapter,
		Permanent: true,
		Detail:    "adapter serializes text/binary resultsets, OK packets, ERR packets, EOF/OK terminators, and metadata packets",
	},
	{
		Concern:   WireAdapterConcernSessionState,
		Protocol:  ProtocolUnknown,
		Owner:     WireAdapterOwnerProtocolAdapter,
		Permanent: true,
		Detail:    "adapter owns live sessions, connection ids, connection close, registry mutation, and selected database application",
	},
	{
		Concern:   WireAdapterConcernCancellation,
		Protocol:  ProtocolUnknown,
		Owner:     WireAdapterOwnerProtocolAdapter,
		Permanent: true,
		Detail:    "adapter transports cancellation and maps it to request ids while qsbridge records cancellation metadata",
	},
	{
		Concern:   WireAdapterConcernExecution,
		Protocol:  ProtocolUnknown,
		Owner:     WireAdapterOwnerExecutor,
		Permanent: true,
		Detail:    "native, legacy, and future executors invoke storage/runtime paths after qsbridge handoff metadata is accepted",
	},
	{
		Concern:   WireAdapterConcernPacketIO,
		Protocol:  ProtocolGRPC,
		Owner:     WireAdapterOwnerProtocolAdapter,
		Permanent: true,
		Detail:    "gRPC adapter owns service definitions, streaming RPCs, deadlines, and transport errors",
	},
	{
		Concern:   WireAdapterConcernResultSerialization,
		Protocol:  ProtocolGRPC,
		Owner:     WireAdapterOwnerProtocolAdapter,
		Permanent: true,
		Detail:    "gRPC adapter serializes protocol-neutral result metadata into typed response messages",
	},
}

func cloneWireAdapterBoundaries(boundaries []WireAdapterBoundary) []WireAdapterBoundary {
	return append([]WireAdapterBoundary(nil), boundaries...)
}

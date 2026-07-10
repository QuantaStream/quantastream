package qsbridge

import "sort"

// ClientCapabilityScope identifies where a visible capability comes from.
type ClientCapabilityScope string

const (
	// ClientCapabilityScopeProtocol describes adapter/server protocol features.
	ClientCapabilityScopeProtocol ClientCapabilityScope = "protocol"
	// ClientCapabilityScopeClient describes capabilities accepted from the client.
	ClientCapabilityScopeClient ClientCapabilityScope = "client"
)

// ClientVisibleCapability describes one protocol- or client-facing capability.
type ClientVisibleCapability struct {
	Scope      ClientCapabilityScope
	Name       string
	Protocol   ProtocolKind
	Driver     string
	Advertised bool
}

// ClientCapabilitySummaryRow describes aggregate client and protocol capability metadata.
type ClientCapabilitySummaryRow struct {
	CapabilityCount         int
	ProtocolCapabilityCount int
	ClientCapabilityCount   int
	AdvertisedCount         int
	PreparedCount           int
	BatchCount              int
	StreamingCount          int
	CancellationCount       int
	ExplainCount            int
	ProfileCount            int
	SessionActionCount      int
	TLSCount                int
	CompressionCount        int
	SessionTrackingCount    int
}

// ClientCapabilitiesExchange is adapter-facing capability metadata for a connection.
type ClientCapabilitiesExchange struct {
	Connection   ConnectionContext
	Capabilities []ClientVisibleCapability
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// ListClientConnectionCapabilities returns active protocol and client capability metadata.
func (s PlanningService) ListClientConnectionCapabilities(connection ConnectionContext) ClientCapabilitiesExchange {
	_ = s
	exchange := ClientCapabilitiesExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.capabilitiesResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Capabilities = connectionCapabilities(connection)
	exchange.Result = exchange.capabilitiesResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether capability metadata can be returned.
func (e ClientCapabilitiesExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts capability metadata diagnostics into protocol-facing errors.
func (e ClientCapabilitiesExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking capability metadata error, if any.
func (e ClientCapabilitiesExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientCapabilitiesExchange) capabilitiesResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     capabilitiesResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.capabilityRows(),
		Final: true,
	})
}

func capabilitiesResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Scope", Type: DataTypeString},
		{Name: "Capability", Type: DataTypeString},
		{Name: "Protocol", Type: DataTypeString, Nullable: true},
		{Name: "Driver", Type: DataTypeString, Nullable: true},
		{Name: "Advertised", Type: DataTypeBool},
	}
}

func (e ClientCapabilitiesExchange) capabilityRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Capabilities))
	for _, capability := range e.Capabilities {
		rows = append(rows, ResultRow{
			metadataStringCell(string(capability.Scope)),
			metadataStringCell(capability.Name),
			metadataStringCell(string(capability.Protocol)),
			metadataStringCell(capability.Driver),
			metadataBoolCell(capability.Advertised),
		})
	}
	return rows
}

func connectionCapabilities(connection ConnectionContext) []ClientVisibleCapability {
	capabilities := make([]ClientVisibleCapability, 0, len(connection.Protocol.Capabilities)+len(connection.Capabilities))
	for _, capability := range connection.Protocol.Capabilities {
		capabilities = append(capabilities, ClientVisibleCapability{
			Scope:      ClientCapabilityScopeProtocol,
			Name:       string(capability),
			Protocol:   connection.Protocol.Kind,
			Driver:     connection.Protocol.Driver,
			Advertised: true,
		})
	}
	for _, capability := range connection.Capabilities {
		capabilities = append(capabilities, ClientVisibleCapability{
			Scope:      ClientCapabilityScopeClient,
			Name:       string(capability),
			Protocol:   connection.Protocol.Kind,
			Driver:     connection.Protocol.Driver,
			Advertised: true,
		})
	}
	sort.SliceStable(capabilities, func(i, j int) bool {
		if capabilities[i].Scope != capabilities[j].Scope {
			return capabilities[i].Scope < capabilities[j].Scope
		}
		return capabilities[i].Name < capabilities[j].Name
	})
	return capabilities
}

func summarizeClientVisibleCapabilities(capabilities []ClientVisibleCapability) ClientCapabilitySummaryRow {
	summary := ClientCapabilitySummaryRow{CapabilityCount: len(capabilities)}
	for _, capability := range capabilities {
		if capability.Scope == ClientCapabilityScopeProtocol {
			summary.ProtocolCapabilityCount++
			summarizeProtocolCapabilityName(&summary, capability.Name)
		}
		if capability.Scope == ClientCapabilityScopeClient {
			summary.ClientCapabilityCount++
			summarizeClientCapabilityName(&summary, capability.Name)
		}
		if capability.Advertised {
			summary.AdvertisedCount++
		}
	}
	return summary
}

func summarizeProtocolCapabilityName(summary *ClientCapabilitySummaryRow, name string) {
	switch ProtocolCapability(name) {
	case ProtocolCapabilityPreparedStatements:
		summary.PreparedCount++
	case ProtocolCapabilityBatchExecution:
		summary.BatchCount++
	case ProtocolCapabilityStreamingResults:
		summary.StreamingCount++
	case ProtocolCapabilityCancellation:
		summary.CancellationCount++
	case ProtocolCapabilityExplain, ProtocolCapabilityStructuredExplain, ProtocolCapabilityPlanCachePolicy:
		summary.ExplainCount++
	case ProtocolCapabilityProfile:
		summary.ProfileCount++
	case ProtocolCapabilitySessionActions:
		summary.SessionActionCount++
	}
}

func summarizeClientCapabilityName(summary *ClientCapabilitySummaryRow, name string) {
	switch ClientCapability(name) {
	case ClientCapabilityPreparedStatements:
		summary.PreparedCount++
	case ClientCapabilityBatching:
		summary.BatchCount++
	case ClientCapabilityCompression:
		summary.CompressionCount++
	case ClientCapabilityTLS:
		summary.TLSCount++
	case ClientCapabilitySessionTracking:
		summary.SessionTrackingCount++
	}
}

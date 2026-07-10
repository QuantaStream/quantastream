package qsbridge

// CompatibilityLayer identifies which layer owns a scaffold capability.
type CompatibilityLayer string

const (
	// CompatibilityLayerCore belongs to parser-neutral planning vocabulary.
	CompatibilityLayerCore CompatibilityLayer = "core"
	// CompatibilityLayerClient belongs to adapter-facing client metadata surfaces.
	CompatibilityLayerClient CompatibilityLayer = "client"
	// CompatibilityLayerProtocol belongs to protocol negotiation and response metadata.
	CompatibilityLayerProtocol CompatibilityLayer = "protocol"
	// CompatibilityLayerAdapter belongs to adapter-owned integrations.
	CompatibilityLayerAdapter CompatibilityLayer = "adapter"
	// CompatibilityLayerExecutor belongs to future native or legacy execution.
	CompatibilityLayerExecutor CompatibilityLayer = "executor"
)

// CompatibilityStatus describes how ready a capability is inside qsbridge.
type CompatibilityStatus string

const (
	// CompatibilityStatusNativePlanning means qsbridge can model or plan this natively.
	CompatibilityStatusNativePlanning CompatibilityStatus = "native_planning"
	// CompatibilityStatusMetadataOnly means qsbridge exposes metadata but does not perform the action.
	CompatibilityStatusMetadataOnly CompatibilityStatus = "metadata_only"
	// CompatibilityStatusBoundaryOnly means qsbridge defines the interface while another layer owns behavior.
	CompatibilityStatusBoundaryOnly CompatibilityStatus = "boundary_only"
	// CompatibilityStatusAuditOnly means qsbridge records inspection or optimization audit metadata only.
	CompatibilityStatusAuditOnly CompatibilityStatus = "audit_only"
	// CompatibilityStatusDeferred means the capability is intentionally future work.
	CompatibilityStatusDeferred CompatibilityStatus = "deferred"
)

// CompatibilityCapability describes one stable qsbridge compatibility contract.
type CompatibilityCapability struct {
	Name         string
	Layer        CompatibilityLayer
	Status       CompatibilityStatus
	RuntimeOwned bool
	AdapterOwned bool
	Description  string
}

// CompatibilityProfile is the scaffold's compatibility manifest.
type CompatibilityProfile struct {
	Capabilities []CompatibilityCapability
	Diagnostics  DiagnosticSet
}

// DefaultCompatibilityProfile returns the current qsbridge scaffold manifest.
func DefaultCompatibilityProfile() CompatibilityProfile {
	return CompatibilityProfile{Capabilities: []CompatibilityCapability{
		{
			Name:        "catalog_binding",
			Layer:       CompatibilityLayerCore,
			Status:      CompatibilityStatusNativePlanning,
			Description: "catalog-backed table, field, function, relationship, and access requirement binding",
		},
		{
			Name:        "query_ir",
			Layer:       CompatibilityLayerCore,
			Status:      CompatibilityStatusNativePlanning,
			Description: "parser-neutral query, expression, predicate, aggregate, membership, and mutation vocabulary",
		},
		{
			Name:        "logical_plan",
			Layer:       CompatibilityLayerCore,
			Status:      CompatibilityStatusNativePlanning,
			Description: "executor-neutral logical plan nodes and explanation metadata",
		},
		{
			Name:        "physical_plan",
			Layer:       CompatibilityLayerCore,
			Status:      CompatibilityStatusNativePlanning,
			Description: "physical plan scaffold with shard, replica, placement, and cache-scope metadata",
		},
		{
			Name:        "optimizer_trace",
			Layer:       CompatibilityLayerCore,
			Status:      CompatibilityStatusAuditOnly,
			Description: "optimizer rewrite and blocker audit records without rewrite execution",
		},
		{
			Name:        "client_statement_flow",
			Layer:       CompatibilityLayerClient,
			Status:      CompatibilityStatusMetadataOnly,
			Description: "ordered client statement planning, handoff, response preview, and route decision metadata",
		},
		{
			Name:        "catalog_discovery",
			Layer:       CompatibilityLayerClient,
			Status:      CompatibilityStatusMetadataOnly,
			Description: "schema, table, column, index, constraint, relationship, function, charset, and engine rowsets",
		},
		{
			Name:        "prepared_metadata",
			Layer:       CompatibilityLayerClient,
			Status:      CompatibilityStatusMetadataOnly,
			Description: "prepared handle registry, describe, execute metadata, reset, long data, and cache inspection surfaces",
		},
		{
			Name:        "structured_explain",
			Layer:       CompatibilityLayerClient,
			Status:      CompatibilityStatusMetadataOnly,
			Description: "section-selected explain bundles for logical, physical, optimizer, diagnostic, function, and blocker metadata",
		},
		{
			Name:        "plan_cache_policy",
			Layer:       CompatibilityLayerClient,
			Status:      CompatibilityStatusMetadataOnly,
			Description: "prepared-plan cache identity policy rows for included, display-only, execute-only, and audit-only factors",
		},
		{
			Name:         "protocol_negotiation",
			Layer:        CompatibilityLayerProtocol,
			Status:       CompatibilityStatusBoundaryOnly,
			AdapterOwned: true,
			Description:  "protocol profile validation and response metadata without packet serialization",
		},
		{
			Name:         "adapter_surfaces",
			Layer:        CompatibilityLayerProtocol,
			Status:       CompatibilityStatusMetadataOnly,
			AdapterOwned: true,
			Description:  "named MySQL, gRPC, embedded, and internal adapter surfaces plus implementation contract metadata",
		},
		{
			Name:         "authentication",
			Layer:        CompatibilityLayerAdapter,
			Status:       CompatibilityStatusBoundaryOnly,
			AdapterOwned: true,
			Description:  "login/session metadata boundaries while password exchange and identity providers stay outside qsbridge",
		},
		{
			Name:         "authorization",
			Layer:        CompatibilityLayerAdapter,
			Status:       CompatibilityStatusBoundaryOnly,
			AdapterOwned: true,
			Description:  "planner-derived access requirements and delegated allow/deny decisions",
		},
		{
			Name:         "session_mutation",
			Layer:        CompatibilityLayerAdapter,
			Status:       CompatibilityStatusMetadataOnly,
			AdapterOwned: true,
			Description:  "USE, SET, reset, change-user, transaction, and session-state action metadata without live mutation",
		},
		{
			Name:         "native_executor",
			Layer:        CompatibilityLayerExecutor,
			Status:       CompatibilityStatusBoundaryOnly,
			RuntimeOwned: true,
			Description:  "native executor interface and dispatch contract without bitmap, BSI, storage, or goroutine ownership",
		},
		{
			Name:         "legacy_fallback",
			Layer:        CompatibilityLayerExecutor,
			Status:       CompatibilityStatusBoundaryOnly,
			RuntimeOwned: true,
			Description:  "legacy fallback handoff metadata without invoking the qlbridge runtime",
		},
	}}
}

// CompatibilityProfile returns the service compatibility manifest.
func (s PlanningService) CompatibilityProfile() CompatibilityProfile {
	_ = s
	return DefaultCompatibilityProfile().Clone()
}

// Clone returns a deep copy of profile.
func (p CompatibilityProfile) Clone() CompatibilityProfile {
	p.Capabilities = cloneCompatibilityCapabilities(p.Capabilities)
	p.Diagnostics = cloneDiagnosticSet(p.Diagnostics)
	return p
}

func cloneCompatibilityCapabilities(capabilities []CompatibilityCapability) []CompatibilityCapability {
	if len(capabilities) == 0 {
		return nil
	}
	return append([]CompatibilityCapability(nil), capabilities...)
}

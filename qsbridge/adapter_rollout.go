package qsbridge

// AdapterRolloutPhase identifies one rollout phase for an adapter surface.
type AdapterRolloutPhase string

const (
	// AdapterRolloutMetadataInventory records qsbridge metadata and diagnostics first.
	AdapterRolloutMetadataInventory AdapterRolloutPhase = "metadata_inventory"
	// AdapterRolloutAdapterShell introduces the protocol or embedded adapter boundary.
	AdapterRolloutAdapterShell AdapterRolloutPhase = "adapter_shell"
	// AdapterRolloutShadowValidation compares adapter behavior before routing live traffic.
	AdapterRolloutShadowValidation AdapterRolloutPhase = "shadow_validation"
	// AdapterRolloutCompatibilityRoute allows controlled compatibility routing.
	AdapterRolloutCompatibilityRoute AdapterRolloutPhase = "compatibility_route"
	// AdapterRolloutRuntimeEnablement turns on runtime dispatch for eligible work.
	AdapterRolloutRuntimeEnablement AdapterRolloutPhase = "runtime_enablement"
)

// AdapterRolloutStep describes one ordered rollout step for an adapter surface.
type AdapterRolloutStep struct {
	Surface       AdapterSurfaceKind
	Phase         AdapterRolloutPhase
	Order         int
	Status        CompatibilityStatus
	Owner         WireAdapterOwner
	Requires      []AdapterContractConcern
	BlocksRuntime bool
	Detail        string
}

// AdapterRolloutSummary aggregates rollout readiness for one adapter surface.
type AdapterRolloutSummary struct {
	Surface            AdapterSurfaceKind
	PhaseCount         int
	MetadataOnlyCount  int
	BoundaryOnlyCount  int
	DeferredCount      int
	BlocksRuntime      int
	QSBridgeOwnedCount int
	AdapterOwnedCount  int
	RuntimeOwnedCount  int
}

// DefaultAdapterRolloutSteps returns adapter rollout metadata.
func DefaultAdapterRolloutSteps() []AdapterRolloutStep {
	return cloneAdapterRolloutSteps(defaultAdapterRolloutSteps)
}

// AdapterRolloutStepsForSurface returns rollout steps for a surface.
func AdapterRolloutStepsForSurface(surface AdapterSurfaceKind) []AdapterRolloutStep {
	steps := DefaultAdapterRolloutSteps()
	if surface == "" {
		return steps
	}
	filtered := make([]AdapterRolloutStep, 0, len(steps))
	for _, step := range steps {
		if step.Surface == surface {
			filtered = append(filtered, step)
		}
	}
	return filtered
}

// SummarizeAdapterRolloutSteps aggregates rollout readiness by adapter surface.
func SummarizeAdapterRolloutSteps(steps []AdapterRolloutStep) []AdapterRolloutSummary {
	if len(steps) == 0 {
		return nil
	}
	summaries := make([]AdapterRolloutSummary, 0)
	indexBySurface := make(map[AdapterSurfaceKind]int)
	for _, step := range steps {
		index, ok := indexBySurface[step.Surface]
		if !ok {
			index = len(summaries)
			indexBySurface[step.Surface] = index
			summaries = append(summaries, AdapterRolloutSummary{Surface: step.Surface})
		}
		summary := &summaries[index]
		summary.PhaseCount++
		switch step.Status {
		case CompatibilityStatusMetadataOnly:
			summary.MetadataOnlyCount++
		case CompatibilityStatusBoundaryOnly:
			summary.BoundaryOnlyCount++
		case CompatibilityStatusDeferred:
			summary.DeferredCount++
		}
		if step.BlocksRuntime {
			summary.BlocksRuntime++
		}
		switch step.Owner {
		case WireAdapterOwnerQSBridge:
			summary.QSBridgeOwnedCount++
		case WireAdapterOwnerProtocolAdapter, WireAdapterOwnerDeployment:
			summary.AdapterOwnedCount++
		case WireAdapterOwnerExecutor:
			summary.RuntimeOwnedCount++
		}
	}
	return summaries
}

// AdapterRolloutSummariesForSurface returns rollout summaries for a surface.
func AdapterRolloutSummariesForSurface(surface AdapterSurfaceKind) []AdapterRolloutSummary {
	return SummarizeAdapterRolloutSteps(AdapterRolloutStepsForSurface(surface))
}

var defaultAdapterRolloutSteps = []AdapterRolloutStep{
	adapterRolloutMetadataStep(AdapterSurfaceMySQLServer, []AdapterContractConcern{
		AdapterContractStatementPlanning,
		AdapterContractPreparedExecution,
		AdapterContractResultMetadata,
	}, "inventory MySQL-compatible planning, prepared metadata, response descriptors, diagnostics, and fallback boundaries"),
	adapterRolloutBoundaryStep(AdapterSurfaceMySQLServer, AdapterRolloutAdapterShell, 2, WireAdapterOwnerProtocolAdapter, []AdapterContractConcern{
		AdapterContractProtocolDecode,
		AdapterContractAuthentication,
		AdapterContractResultSerialization,
	}, "build the MySQL adapter shell for packet decode, auth exchange, and packet serialization"),
	adapterRolloutDeferredStep(AdapterSurfaceMySQLServer, AdapterRolloutShadowValidation, 3, WireAdapterOwnerProtocolAdapter, []AdapterContractConcern{
		AdapterContractStatementPlanning,
		AdapterContractResultSerialization,
	}, "compare adapter-shaped responses against legacy behavior before routing live SQL through the new path"),
	adapterRolloutDeferredStep(AdapterSurfaceMySQLServer, AdapterRolloutCompatibilityRoute, 4, WireAdapterOwnerProtocolAdapter, []AdapterContractConcern{
		AdapterContractStatementPlanning,
		AdapterContractExecutionDispatch,
	}, "route selected compatible SQL through the new adapter while preserving legacy fallback"),
	adapterRolloutDeferredStep(AdapterSurfaceMySQLServer, AdapterRolloutRuntimeEnablement, 5, WireAdapterOwnerExecutor, []AdapterContractConcern{
		AdapterContractExecutionDispatch,
		AdapterContractCancellation,
	}, "enable live native or legacy runtime dispatch once cancellation and executor contracts are ready"),

	adapterRolloutMetadataStep(AdapterSurfaceGRPCAPI, []AdapterContractConcern{
		AdapterContractCatalogDiscovery,
		AdapterContractInspection,
		AdapterContractResultMetadata,
	}, "inventory typed catalog, inspection, readiness, compatibility, and diagnostic metadata for control-plane APIs"),
	adapterRolloutBoundaryStep(AdapterSurfaceGRPCAPI, AdapterRolloutAdapterShell, 2, WireAdapterOwnerProtocolAdapter, []AdapterContractConcern{
		AdapterContractResultSerialization,
	}, "build the gRPC service shell and typed response envelopes without tunneling MySQL packets"),
	adapterRolloutDeferredStep(AdapterSurfaceGRPCAPI, AdapterRolloutShadowValidation, 3, WireAdapterOwnerProtocolAdapter, []AdapterContractConcern{
		AdapterContractCatalogDiscovery,
		AdapterContractInspection,
	}, "validate typed gRPC metadata against existing rowset and CLI-style diagnostics"),
	adapterRolloutDeferredStep(AdapterSurfaceGRPCAPI, AdapterRolloutCompatibilityRoute, 4, WireAdapterOwnerProtocolAdapter, []AdapterContractConcern{
		AdapterContractStatementPlanning,
	}, "allow typed planning requests after catalog and inspection APIs stabilize"),
	adapterRolloutDeferredStep(AdapterSurfaceGRPCAPI, AdapterRolloutRuntimeEnablement, 5, WireAdapterOwnerExecutor, []AdapterContractConcern{
		AdapterContractExecutionDispatch,
	}, "enable future typed data APIs only after executor dispatch contracts are ready"),

	adapterRolloutMetadataStep(AdapterSurfaceEmbedded, []AdapterContractConcern{
		AdapterContractStatementPlanning,
	}, "inventory the in-process QIAB call path over existing planning and handoff metadata"),
	adapterRolloutBoundaryStep(AdapterSurfaceEmbedded, AdapterRolloutAdapterShell, 2, WireAdapterOwnerExecutor, []AdapterContractConcern{
		AdapterContractStatementPlanning,
	}, "build an embedded direct-call shell that avoids packet serialization and sockets"),
	adapterRolloutDeferredStep(AdapterSurfaceEmbedded, AdapterRolloutShadowValidation, 3, WireAdapterOwnerExecutor, []AdapterContractConcern{
		AdapterContractExecutionDispatch,
	}, "compare embedded execution envelopes with networked compatibility behavior"),
	adapterRolloutDeferredStep(AdapterSurfaceEmbedded, AdapterRolloutCompatibilityRoute, 4, WireAdapterOwnerExecutor, []AdapterContractConcern{
		AdapterContractExecutionDispatch,
	}, "route QIAB eligible work through direct in-process calls"),
	adapterRolloutDeferredStep(AdapterSurfaceEmbedded, AdapterRolloutRuntimeEnablement, 5, WireAdapterOwnerExecutor, []AdapterContractConcern{
		AdapterContractExecutionDispatch,
	}, "enable runtime dispatch after embedded executor boundaries are ready"),

	adapterRolloutMetadataStep(AdapterSurfaceInternalExecution, []AdapterContractConcern{
		AdapterContractExecutionDispatch,
		AdapterContractTopology,
	}, "inventory physical scope, topology, and executor handoff metadata for internal runtime traffic"),
	adapterRolloutBoundaryStep(AdapterSurfaceInternalExecution, AdapterRolloutAdapterShell, 2, WireAdapterOwnerExecutor, []AdapterContractConcern{
		AdapterContractExecutionDispatch,
	}, "build the internal executor transport boundary after client protocol concerns have ended"),
	adapterRolloutDeferredStep(AdapterSurfaceInternalExecution, AdapterRolloutShadowValidation, 3, WireAdapterOwnerExecutor, []AdapterContractConcern{
		AdapterContractTopology,
	}, "validate shard, replica, placement, and routing metadata against runtime topology behavior"),
	adapterRolloutDeferredStep(AdapterSurfaceInternalExecution, AdapterRolloutCompatibilityRoute, 4, WireAdapterOwnerExecutor, []AdapterContractConcern{
		AdapterContractExecutionDispatch,
		AdapterContractTopology,
	}, "route accepted physical work through internal execution transport"),
	adapterRolloutDeferredStep(AdapterSurfaceInternalExecution, AdapterRolloutRuntimeEnablement, 5, WireAdapterOwnerExecutor, []AdapterContractConcern{
		AdapterContractExecutionDispatch,
		AdapterContractTopology,
	}, "enable distributed runtime dispatch after topology and cancellation semantics are proven"),
}

func adapterRolloutMetadataStep(surface AdapterSurfaceKind, requires []AdapterContractConcern, detail string) AdapterRolloutStep {
	return AdapterRolloutStep{
		Surface:  surface,
		Phase:    AdapterRolloutMetadataInventory,
		Order:    1,
		Status:   CompatibilityStatusMetadataOnly,
		Owner:    WireAdapterOwnerQSBridge,
		Requires: append([]AdapterContractConcern(nil), requires...),
		Detail:   detail,
	}
}

func adapterRolloutBoundaryStep(surface AdapterSurfaceKind, phase AdapterRolloutPhase, order int, owner WireAdapterOwner, requires []AdapterContractConcern, detail string) AdapterRolloutStep {
	return AdapterRolloutStep{
		Surface:       surface,
		Phase:         phase,
		Order:         order,
		Status:        CompatibilityStatusBoundaryOnly,
		Owner:         owner,
		Requires:      append([]AdapterContractConcern(nil), requires...),
		BlocksRuntime: true,
		Detail:        detail,
	}
}

func adapterRolloutDeferredStep(surface AdapterSurfaceKind, phase AdapterRolloutPhase, order int, owner WireAdapterOwner, requires []AdapterContractConcern, detail string) AdapterRolloutStep {
	return AdapterRolloutStep{
		Surface:       surface,
		Phase:         phase,
		Order:         order,
		Status:        CompatibilityStatusDeferred,
		Owner:         owner,
		Requires:      append([]AdapterContractConcern(nil), requires...),
		BlocksRuntime: true,
		Detail:        detail,
	}
}

func cloneAdapterRolloutSteps(steps []AdapterRolloutStep) []AdapterRolloutStep {
	cloned := make([]AdapterRolloutStep, len(steps))
	for i, step := range steps {
		cloned[i] = step
		cloned[i].Requires = append([]AdapterContractConcern(nil), step.Requires...)
	}
	return cloned
}

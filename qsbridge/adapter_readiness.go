package qsbridge

// AdapterReadinessReport summarizes implementation readiness for one adapter surface.
type AdapterReadinessReport struct {
	Surface               AdapterSurfaceKind
	Audience              AdapterSurfaceAudience
	Protocol              ProtocolKind
	Transport             TransportKind
	Placement             ExecutionPlacement
	ClientFacing          bool
	ControlPlane          bool
	Embedded              bool
	Internal              bool
	MetadataReady         bool
	RuntimeReady          bool
	NextPhase             AdapterRolloutPhase
	ContractCount         int
	DeferredContracts     int
	AdapterOwnedContracts int
	RuntimeOwnedContracts int
	QSBridgeContracts     int
	PhaseCount            int
	DeferredPhases        int
	RuntimeBlockingPhases int
	Detail                string
}

// AdapterReadinessSummary aggregates readiness across adapter surfaces.
type AdapterReadinessSummary struct {
	SurfaceCount          int
	MetadataReadyCount    int
	RuntimeReadyCount     int
	ClientFacingCount     int
	ControlPlaneCount     int
	EmbeddedCount         int
	InternalCount         int
	ContractCount         int
	DeferredContracts     int
	AdapterOwnedContracts int
	RuntimeOwnedContracts int
	QSBridgeContracts     int
	PhaseCount            int
	DeferredPhases        int
	RuntimeBlockingPhases int
}

// AdapterReadinessBlockerSource identifies where a readiness blocker came from.
type AdapterReadinessBlockerSource string

const (
	// AdapterReadinessBlockerContract means a deferred contract blocks readiness.
	AdapterReadinessBlockerContract AdapterReadinessBlockerSource = "contract"
	// AdapterReadinessBlockerRollout means a rollout phase blocks runtime readiness.
	AdapterReadinessBlockerRollout AdapterReadinessBlockerSource = "rollout"
)

// AdapterReadinessBlocker explains why an adapter surface is not runtime-ready.
type AdapterReadinessBlocker struct {
	Surface       AdapterSurfaceKind
	Source        AdapterReadinessBlockerSource
	Phase         AdapterRolloutPhase
	Concern       AdapterContractConcern
	Status        CompatibilityStatus
	Owner         WireAdapterOwner
	BlocksRuntime bool
	Detail        string
}

// AdapterReadinessBlockerSummary aggregates runtime blockers for one adapter surface.
type AdapterReadinessBlockerSummary struct {
	Surface              AdapterSurfaceKind
	BlockerCount         int
	ContractBlockers     int
	RolloutBlockers      int
	DeferredCount        int
	BoundaryOnlyCount    int
	RuntimeBlockingCount int
	AdapterOwnedCount    int
	RuntimeOwnedCount    int
	QSBridgeOwnedCount   int
}

// AdapterReadinessGateKind identifies one release-readiness gate.
type AdapterReadinessGateKind string

const (
	// AdapterReadinessGateContracts verifies adapter contracts have no deferred blockers.
	AdapterReadinessGateContracts AdapterReadinessGateKind = "contracts"
	// AdapterReadinessGateMetadataInventory verifies metadata inventory is ready.
	AdapterReadinessGateMetadataInventory AdapterReadinessGateKind = "metadata_inventory"
	// AdapterReadinessGateAdapterShell verifies the adapter shell boundary is ready.
	AdapterReadinessGateAdapterShell AdapterReadinessGateKind = "adapter_shell"
	// AdapterReadinessGateShadowValidation verifies shadow validation is ready.
	AdapterReadinessGateShadowValidation AdapterReadinessGateKind = "shadow_validation"
	// AdapterReadinessGateCompatibilityRoute verifies compatibility routing is ready.
	AdapterReadinessGateCompatibilityRoute AdapterReadinessGateKind = "compatibility_route"
	// AdapterReadinessGateRuntimeEnablement verifies runtime dispatch can be enabled.
	AdapterReadinessGateRuntimeEnablement AdapterReadinessGateKind = "runtime_enablement"
)

// AdapterReadinessGate is a release-checklist row for one adapter surface.
type AdapterReadinessGate struct {
	Surface       AdapterSurfaceKind
	Gate          AdapterReadinessGateKind
	Order         int
	Status        CompatibilityStatus
	Owner         WireAdapterOwner
	Ready         bool
	BlocksRuntime bool
	BlockerCount  int
	Next          bool
	Detail        string
}

// AdapterReadinessGateSummary aggregates release gates for one adapter surface.
type AdapterReadinessGateSummary struct {
	Surface           AdapterSurfaceKind
	GateCount         int
	ReadyCount        int
	RuntimeBlockCount int
	BlockerCount      int
	NextGate          AdapterReadinessGateKind
	NextGateOrder     int
	ContractsReady    bool
	MetadataReady     bool
	RuntimeReady      bool
}

// AdapterReadinessNextAction identifies the next readiness gate to work on.
type AdapterReadinessNextAction struct {
	Surface       AdapterSurfaceKind
	Gate          AdapterReadinessGateKind
	Order         int
	Status        CompatibilityStatus
	Owner         WireAdapterOwner
	BlocksRuntime bool
	BlockerCount  int
	Detail        string
}

// DefaultAdapterReadinessReports returns readiness reports for all adapter surfaces.
func DefaultAdapterReadinessReports() []AdapterReadinessReport {
	return AdapterReadinessReportsForSurface("")
}

// AdapterReadinessReportsForSurface returns readiness reports for a surface.
func AdapterReadinessReportsForSurface(surface AdapterSurfaceKind) []AdapterReadinessReport {
	surfaces := DefaultAdapterSurfaces()
	contractSummaries := AdapterContractSummariesForSurface("")
	rolloutSummaries := AdapterRolloutSummariesForSurface("")
	rolloutSteps := DefaultAdapterRolloutSteps()
	reports := make([]AdapterReadinessReport, 0, len(surfaces))
	for _, current := range surfaces {
		if surface != "" && current.Kind != surface {
			continue
		}
		contractSummary := adapterContractSummaryForSurface(contractSummaries, current.Kind)
		rolloutSummary := adapterRolloutSummaryForSurface(rolloutSummaries, current.Kind)
		report := AdapterReadinessReport{
			Surface:               current.Kind,
			Audience:              current.Audience,
			Protocol:              current.Protocol,
			Transport:             current.Transport,
			Placement:             current.Placement,
			ClientFacing:          current.ClientFacing,
			ControlPlane:          current.ControlPlane,
			Embedded:              current.Embedded,
			Internal:              current.Internal,
			MetadataReady:         current.UsesQSBridgeMetadata && contractSummary.MetadataOnlyCount > 0 && rolloutSummary.MetadataOnlyCount > 0,
			RuntimeReady:          contractSummary.DeferredCount == 0 && rolloutSummary.DeferredCount == 0 && rolloutSummary.BlocksRuntime == 0,
			NextPhase:             nextAdapterRolloutPhase(rolloutSteps, current.Kind),
			ContractCount:         contractSummary.ContractCount,
			DeferredContracts:     contractSummary.DeferredCount,
			AdapterOwnedContracts: contractSummary.AdapterOwnedCount,
			RuntimeOwnedContracts: contractSummary.RuntimeOwnedCount,
			QSBridgeContracts:     contractSummary.QSBridgeOwnedCount,
			PhaseCount:            rolloutSummary.PhaseCount,
			DeferredPhases:        rolloutSummary.DeferredCount,
			RuntimeBlockingPhases: rolloutSummary.BlocksRuntime,
			Detail:                current.Detail,
		}
		reports = append(reports, report)
	}
	return reports
}

// SummarizeAdapterReadinessReports aggregates readiness reports into one summary.
func SummarizeAdapterReadinessReports(reports []AdapterReadinessReport) AdapterReadinessSummary {
	summary := AdapterReadinessSummary{SurfaceCount: len(reports)}
	for _, report := range reports {
		if report.MetadataReady {
			summary.MetadataReadyCount++
		}
		if report.RuntimeReady {
			summary.RuntimeReadyCount++
		}
		if report.ClientFacing {
			summary.ClientFacingCount++
		}
		if report.ControlPlane {
			summary.ControlPlaneCount++
		}
		if report.Embedded {
			summary.EmbeddedCount++
		}
		if report.Internal {
			summary.InternalCount++
		}
		summary.ContractCount += report.ContractCount
		summary.DeferredContracts += report.DeferredContracts
		summary.AdapterOwnedContracts += report.AdapterOwnedContracts
		summary.RuntimeOwnedContracts += report.RuntimeOwnedContracts
		summary.QSBridgeContracts += report.QSBridgeContracts
		summary.PhaseCount += report.PhaseCount
		summary.DeferredPhases += report.DeferredPhases
		summary.RuntimeBlockingPhases += report.RuntimeBlockingPhases
	}
	return summary
}

// DefaultAdapterReadinessSummary returns aggregate readiness for all adapter surfaces.
func DefaultAdapterReadinessSummary() AdapterReadinessSummary {
	return SummarizeAdapterReadinessReports(DefaultAdapterReadinessReports())
}

// DefaultAdapterReadinessBlockers returns blockers across all adapter surfaces.
func DefaultAdapterReadinessBlockers() []AdapterReadinessBlocker {
	return AdapterReadinessBlockersForSurface("")
}

// DefaultAdapterReadinessBlockerSummaries returns blocker summaries for all surfaces.
func DefaultAdapterReadinessBlockerSummaries() []AdapterReadinessBlockerSummary {
	return AdapterReadinessBlockerSummariesForSurface("")
}

// DefaultAdapterReadinessGates returns release-readiness gates for all surfaces.
func DefaultAdapterReadinessGates() []AdapterReadinessGate {
	return AdapterReadinessGatesForSurface("")
}

// DefaultAdapterReadinessGateSummaries returns release-readiness gate summaries.
func DefaultAdapterReadinessGateSummaries() []AdapterReadinessGateSummary {
	return AdapterReadinessGateSummariesForSurface("")
}

// DefaultAdapterReadinessNextActions returns next readiness actions for all surfaces.
func DefaultAdapterReadinessNextActions() []AdapterReadinessNextAction {
	return AdapterReadinessNextActionsForSurface("")
}

// AdapterReadinessBlockersForSurface returns runtime readiness blockers for a surface.
func AdapterReadinessBlockersForSurface(surface AdapterSurfaceKind) []AdapterReadinessBlocker {
	blockers := make([]AdapterReadinessBlocker, 0)
	for _, contract := range AdapterContractsForSurface(surface) {
		if contract.Status != CompatibilityStatusDeferred {
			continue
		}
		blockers = append(blockers, AdapterReadinessBlocker{
			Surface:       contract.Surface,
			Source:        AdapterReadinessBlockerContract,
			Concern:       contract.Concern,
			Status:        contract.Status,
			Owner:         contract.Owner,
			BlocksRuntime: true,
			Detail:        contract.Detail,
		})
	}
	for _, step := range AdapterRolloutStepsForSurface(surface) {
		if !step.BlocksRuntime {
			continue
		}
		blockers = append(blockers, AdapterReadinessBlocker{
			Surface:       step.Surface,
			Source:        AdapterReadinessBlockerRollout,
			Phase:         step.Phase,
			Status:        step.Status,
			Owner:         step.Owner,
			BlocksRuntime: step.BlocksRuntime,
			Detail:        step.Detail,
		})
	}
	return blockers
}

// AdapterReadinessBlockerSummariesForSurface returns blocker summaries for a surface.
func AdapterReadinessBlockerSummariesForSurface(surface AdapterSurfaceKind) []AdapterReadinessBlockerSummary {
	return SummarizeAdapterReadinessBlockers(AdapterReadinessBlockersForSurface(surface))
}

// SummarizeAdapterReadinessBlockers aggregates blockers by adapter surface.
func SummarizeAdapterReadinessBlockers(blockers []AdapterReadinessBlocker) []AdapterReadinessBlockerSummary {
	if len(blockers) == 0 {
		return nil
	}
	summaries := make([]AdapterReadinessBlockerSummary, 0)
	indexBySurface := make(map[AdapterSurfaceKind]int)
	for _, blocker := range blockers {
		index, ok := indexBySurface[blocker.Surface]
		if !ok {
			index = len(summaries)
			indexBySurface[blocker.Surface] = index
			summaries = append(summaries, AdapterReadinessBlockerSummary{Surface: blocker.Surface})
		}
		summary := &summaries[index]
		summary.BlockerCount++
		switch blocker.Source {
		case AdapterReadinessBlockerContract:
			summary.ContractBlockers++
		case AdapterReadinessBlockerRollout:
			summary.RolloutBlockers++
		}
		switch blocker.Status {
		case CompatibilityStatusDeferred:
			summary.DeferredCount++
		case CompatibilityStatusBoundaryOnly:
			summary.BoundaryOnlyCount++
		}
		if blocker.BlocksRuntime {
			summary.RuntimeBlockingCount++
		}
		switch blocker.Owner {
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

// AdapterReadinessGatesForSurface returns release-readiness gates for a surface.
func AdapterReadinessGatesForSurface(surface AdapterSurfaceKind) []AdapterReadinessGate {
	reports := AdapterReadinessReportsForSurface(surface)
	gates := make([]AdapterReadinessGate, 0, len(reports)*6)
	for _, report := range reports {
		blockers := AdapterReadinessBlockersForSurface(report.Surface)
		nextGate := nextAdapterReadinessGate(report)
		gates = append(gates, adapterContractReadinessGate(report, blockers, nextGate))
		for _, step := range AdapterRolloutStepsForSurface(report.Surface) {
			gates = append(gates, adapterRolloutReadinessGate(report, step, blockers, nextGate))
		}
	}
	return gates
}

// AdapterReadinessGateSummariesForSurface returns release-readiness gate summaries.
func AdapterReadinessGateSummariesForSurface(surface AdapterSurfaceKind) []AdapterReadinessGateSummary {
	return SummarizeAdapterReadinessGates(AdapterReadinessGatesForSurface(surface))
}

// AdapterReadinessNextActionsForSurface returns next readiness actions for a surface.
func AdapterReadinessNextActionsForSurface(surface AdapterSurfaceKind) []AdapterReadinessNextAction {
	return AdapterReadinessNextActionsFromGates(AdapterReadinessGatesForSurface(surface))
}

// SummarizeAdapterReadinessGates aggregates release-readiness gates by surface.
func SummarizeAdapterReadinessGates(gates []AdapterReadinessGate) []AdapterReadinessGateSummary {
	if len(gates) == 0 {
		return nil
	}
	summaries := make([]AdapterReadinessGateSummary, 0)
	indexBySurface := make(map[AdapterSurfaceKind]int)
	for _, gate := range gates {
		index, ok := indexBySurface[gate.Surface]
		if !ok {
			index = len(summaries)
			indexBySurface[gate.Surface] = index
			summaries = append(summaries, AdapterReadinessGateSummary{
				Surface:        gate.Surface,
				ContractsReady: true,
				MetadataReady:  true,
				RuntimeReady:   true,
			})
		}
		summary := &summaries[index]
		summary.GateCount++
		if gate.Ready {
			summary.ReadyCount++
		}
		if gate.BlocksRuntime {
			summary.RuntimeBlockCount++
		}
		summary.BlockerCount += gate.BlockerCount
		if gate.Next {
			summary.NextGate = gate.Gate
			summary.NextGateOrder = gate.Order
		}
		switch gate.Gate {
		case AdapterReadinessGateContracts:
			summary.ContractsReady = gate.Ready
		case AdapterReadinessGateMetadataInventory:
			summary.MetadataReady = gate.Ready
		}
		if !gate.Ready || gate.BlocksRuntime {
			summary.RuntimeReady = false
		}
	}
	return summaries
}

// AdapterReadinessNextActionsFromGates returns the gate marked as next per surface.
func AdapterReadinessNextActionsFromGates(gates []AdapterReadinessGate) []AdapterReadinessNextAction {
	if len(gates) == 0 {
		return nil
	}
	actions := make([]AdapterReadinessNextAction, 0)
	seen := make(map[AdapterSurfaceKind]struct{})
	for _, gate := range gates {
		if !gate.Next {
			continue
		}
		if _, ok := seen[gate.Surface]; ok {
			continue
		}
		seen[gate.Surface] = struct{}{}
		actions = append(actions, AdapterReadinessNextAction{
			Surface:       gate.Surface,
			Gate:          gate.Gate,
			Order:         gate.Order,
			Status:        gate.Status,
			Owner:         gate.Owner,
			BlocksRuntime: gate.BlocksRuntime,
			BlockerCount:  gate.BlockerCount,
			Detail:        gate.Detail,
		})
	}
	return actions
}

func adapterContractSummaryForSurface(summaries []AdapterContractSummary, surface AdapterSurfaceKind) AdapterContractSummary {
	for _, summary := range summaries {
		if summary.Surface == surface {
			return summary
		}
	}
	return AdapterContractSummary{Surface: surface}
}

func adapterRolloutSummaryForSurface(summaries []AdapterRolloutSummary, surface AdapterSurfaceKind) AdapterRolloutSummary {
	for _, summary := range summaries {
		if summary.Surface == surface {
			return summary
		}
	}
	return AdapterRolloutSummary{Surface: surface}
}

func nextAdapterRolloutPhase(steps []AdapterRolloutStep, surface AdapterSurfaceKind) AdapterRolloutPhase {
	nextOrder := 0
	var next AdapterRolloutPhase
	for _, step := range steps {
		if step.Surface != surface || step.Status == CompatibilityStatusMetadataOnly {
			continue
		}
		if nextOrder == 0 || step.Order < nextOrder {
			nextOrder = step.Order
			next = step.Phase
		}
	}
	return next
}

func adapterContractReadinessGate(report AdapterReadinessReport, blockers []AdapterReadinessBlocker, nextGate AdapterReadinessGateKind) AdapterReadinessGate {
	blockerCount := countAdapterReadinessBlockers(blockers, AdapterReadinessBlockerContract, "")
	ready := report.DeferredContracts == 0
	status := CompatibilityStatusMetadataOnly
	if !ready {
		status = CompatibilityStatusDeferred
	}
	return AdapterReadinessGate{
		Surface:       report.Surface,
		Gate:          AdapterReadinessGateContracts,
		Order:         0,
		Status:        status,
		Owner:         adapterReadinessBlockerOwner(blockers, AdapterReadinessBlockerContract, WireAdapterOwnerQSBridge),
		Ready:         ready,
		BlocksRuntime: !ready,
		BlockerCount:  blockerCount,
		Next:          nextGate == AdapterReadinessGateContracts,
		Detail:        "adapter contracts must be non-deferred before runtime enablement",
	}
}

func adapterRolloutReadinessGate(report AdapterReadinessReport, step AdapterRolloutStep, blockers []AdapterReadinessBlocker, nextGate AdapterReadinessGateKind) AdapterReadinessGate {
	gate := adapterReadinessGateForRolloutPhase(step.Phase)
	ready := !step.BlocksRuntime && step.Status != CompatibilityStatusDeferred
	status := step.Status
	blockerCount := countAdapterReadinessBlockers(blockers, AdapterReadinessBlockerRollout, step.Phase)
	if step.Phase == AdapterRolloutMetadataInventory {
		ready = report.MetadataReady
		if !ready {
			status = CompatibilityStatusDeferred
			blockerCount++
		}
	}
	return AdapterReadinessGate{
		Surface:       report.Surface,
		Gate:          gate,
		Order:         step.Order,
		Status:        status,
		Owner:         step.Owner,
		Ready:         ready,
		BlocksRuntime: !ready || step.BlocksRuntime,
		BlockerCount:  blockerCount,
		Next:          nextGate == gate,
		Detail:        step.Detail,
	}
}

func nextAdapterReadinessGate(report AdapterReadinessReport) AdapterReadinessGateKind {
	if report.DeferredContracts > 0 {
		return AdapterReadinessGateContracts
	}
	if !report.MetadataReady {
		return AdapterReadinessGateMetadataInventory
	}
	if report.NextPhase != "" {
		return adapterReadinessGateForRolloutPhase(report.NextPhase)
	}
	return ""
}

func adapterReadinessGateForRolloutPhase(phase AdapterRolloutPhase) AdapterReadinessGateKind {
	switch phase {
	case AdapterRolloutMetadataInventory:
		return AdapterReadinessGateMetadataInventory
	case AdapterRolloutAdapterShell:
		return AdapterReadinessGateAdapterShell
	case AdapterRolloutShadowValidation:
		return AdapterReadinessGateShadowValidation
	case AdapterRolloutCompatibilityRoute:
		return AdapterReadinessGateCompatibilityRoute
	case AdapterRolloutRuntimeEnablement:
		return AdapterReadinessGateRuntimeEnablement
	default:
		return AdapterReadinessGateKind(phase)
	}
}

func countAdapterReadinessBlockers(blockers []AdapterReadinessBlocker, source AdapterReadinessBlockerSource, phase AdapterRolloutPhase) int {
	count := 0
	for _, blocker := range blockers {
		if blocker.Source != source {
			continue
		}
		if phase != "" && blocker.Phase != phase {
			continue
		}
		count++
	}
	return count
}

func adapterReadinessBlockerOwner(blockers []AdapterReadinessBlocker, source AdapterReadinessBlockerSource, fallback WireAdapterOwner) WireAdapterOwner {
	for _, blocker := range blockers {
		if blocker.Source == source && blocker.Owner != "" {
			return blocker.Owner
		}
	}
	return fallback
}

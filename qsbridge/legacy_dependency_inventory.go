package qsbridge

import "strings"

// LegacyDependencyKind identifies the kind of legacy surface being inventoried.
type LegacyDependencyKind string

const (
	// LegacyDependencyPackage marks a package-level dependency.
	LegacyDependencyPackage LegacyDependencyKind = "package"
	// LegacyDependencyFile marks a file-level dependency or refactor target.
	LegacyDependencyFile LegacyDependencyKind = "file"
	// LegacyDependencyFeature marks a behavior that must survive outside its legacy implementation.
	LegacyDependencyFeature LegacyDependencyKind = "feature"
)

// LegacyDependencyDisposition describes how one legacy subject should be handled.
type LegacyDependencyDisposition string

const (
	// LegacyDependencyPreserveBehindInterface means behavior survives behind new qsbridge/runtime interfaces.
	LegacyDependencyPreserveBehindInterface LegacyDependencyDisposition = "preserve_behind_new_interface"
	// LegacyDependencyMoveToCompat means compatibility code may remain temporarily in qscompat.
	LegacyDependencyMoveToCompat LegacyDependencyDisposition = "move_to_qscompat"
	// LegacyDependencyDeleteAfterProxyRetirement means the subject should disappear with the old proxy.
	LegacyDependencyDeleteAfterProxyRetirement LegacyDependencyDisposition = "delete_after_proxy_retirement"
	// LegacyDependencyResearchNeeded means Guy's compatibility research spike should classify the subject.
	LegacyDependencyResearchNeeded LegacyDependencyDisposition = "research_needed"
)

// LegacyDependencyInventoryItem records one proxy-retirement dependency subject.
type LegacyDependencyInventoryItem struct {
	Subject     string
	Kind        LegacyDependencyKind
	Disposition LegacyDependencyDisposition
	Reason      string
	Owner       string
}

// DefaultLegacyDependencyInventory returns the initial proxy-retirement dependency map.
func DefaultLegacyDependencyInventory() []LegacyDependencyInventoryItem {
	return cloneLegacyDependencyInventory(defaultLegacyDependencyInventory)
}

// LegacyDependencyInventoryForSubject returns the inventory row for a subject.
func LegacyDependencyInventoryForSubject(subject string) (LegacyDependencyInventoryItem, bool) {
	normalized := normalizeLegacyDependencySubject(subject)
	if normalized == "" {
		return LegacyDependencyInventoryItem{}, false
	}
	for _, item := range defaultLegacyDependencyInventory {
		if normalizeLegacyDependencySubject(item.Subject) == normalized {
			return item.clone(), true
		}
	}
	return LegacyDependencyInventoryItem{}, false
}

// LegacyDependencyInventoryByDisposition returns all inventory rows with a disposition.
func LegacyDependencyInventoryByDisposition(disposition LegacyDependencyDisposition) []LegacyDependencyInventoryItem {
	items := []LegacyDependencyInventoryItem{}
	for _, item := range defaultLegacyDependencyInventory {
		if item.Disposition == disposition {
			items = append(items, item.clone())
		}
	}
	return items
}

// RequiresProxyRetirementResearch reports whether an inventory item still needs Guy's research spike.
func (i LegacyDependencyInventoryItem) RequiresProxyRetirementResearch() bool {
	return i.Disposition == LegacyDependencyResearchNeeded || strings.TrimSpace(i.Owner) == "Guy"
}

func normalizeLegacyDependencySubject(subject string) string {
	subject = strings.TrimSpace(subject)
	subject = strings.ReplaceAll(subject, "\\", "/")
	return subject
}

func cloneLegacyDependencyInventory(items []LegacyDependencyInventoryItem) []LegacyDependencyInventoryItem {
	cloned := make([]LegacyDependencyInventoryItem, len(items))
	for i, item := range items {
		cloned[i] = item.clone()
	}
	return cloned
}

func (i LegacyDependencyInventoryItem) clone() LegacyDependencyInventoryItem {
	return i
}

var defaultLegacyDependencyInventory = []LegacyDependencyInventoryItem{
	{
		Subject:     "github.com/QuantaStream/quantastream/core",
		Kind:        LegacyDependencyPackage,
		Disposition: LegacyDependencyPreserveBehindInterface,
		Reason:      "durable session, table, projection, and node/load machinery must be split behind new interfaces while proxy-only code retires",
		Owner:       "Guy",
	},
	{
		Subject:     "github.com/QuantaStream/quantastream/source",
		Kind:        LegacyDependencyPackage,
		Disposition: LegacyDependencyMoveToCompat,
		Reason:      "legacy QuantaSource direct execution is a compatibility bridge until native runtime storage adapters are complete",
		Owner:       "Codex",
	},
	{
		Subject:     "github.com/QuantaStream/quantastream/shared",
		Kind:        LegacyDependencyPackage,
		Disposition: LegacyDependencyPreserveBehindInterface,
		Reason:      "shared IL, metadata, and protobuf-adjacent data shapes need migration into stable client/node/runtime contracts",
		Owner:       "Guy",
	},
	{
		Subject:     "github.com/QuantaStream/quantastream/grpc",
		Kind:        LegacyDependencyPackage,
		Disposition: LegacyDependencyMoveToCompat,
		Reason:      "legacy protobuf/gRPC transport remains a short-term compatibility surface, not a native engine dependency",
		Owner:       "Guy",
	},
	{
		Subject:     "core/table.go",
		Kind:        LegacyDependencyFile,
		Disposition: LegacyDependencyPreserveBehindInterface,
		Reason:      "selector semantics must survive without carrying qlbridge/expr forward",
		Owner:       "Guy",
	},
	{
		Subject:     "core/session.go",
		Kind:        LegacyDependencyFile,
		Disposition: LegacyDependencyPreserveBehindInterface,
		Reason:      "session remains critical for batch load and future streaming ingest, but qlbridge schema/expr/vm references must be bridged or replaced",
		Owner:       "Guy",
	},
	{
		Subject:     "core/session_pool.go",
		Kind:        LegacyDependencyFile,
		Disposition: LegacyDependencyPreserveBehindInterface,
		Reason:      "session pool expression leakage should be removed with durable session machinery",
		Owner:       "Guy",
	},
	{
		Subject:     "core.Projector",
		Kind:        LegacyDependencyFeature,
		Disposition: LegacyDependencyMoveToCompat,
		Reason:      "native projector, materialization, relationship-vector, same-row comparison, and dictionary/KV kernels replace this island",
		Owner:       "Codex",
	},
	{
		Subject:     "source/quanta_join.go",
		Kind:        LegacyDependencyFile,
		Disposition: LegacyDependencyDeleteAfterProxyRetirement,
		Reason:      "join concepts are reference material; new execution should use typed relationship-vector kernels",
		Owner:       "Codex",
	},
	{
		Subject:     "qsruntime/preflight_helper_execution.go",
		Kind:        LegacyDependencyFile,
		Disposition: LegacyDependencyMoveToCompat,
		Reason:      "SQL-text helper execution is documented technical debt and does not block proxy retirement unless it requires the old proxy",
		Owner:       "Codex",
	},
}

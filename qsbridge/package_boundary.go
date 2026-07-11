package qsbridge

import "strings"

// PackageBoundaryName identifies a future qsbridge package boundary.
type PackageBoundaryName string

const (
	// PackageBoundaryCore is the root planning vocabulary and service facade.
	PackageBoundaryCore PackageBoundaryName = "qsbridge"
	// PackageBoundaryCatalog is catalog, encoding, dictionary, and metadata vocabulary.
	PackageBoundaryCatalog PackageBoundaryName = "qsbridge/catalog"
	// PackageBoundaryPlanning is binding, classification, and semantically correct plan shaping.
	PackageBoundaryPlanning PackageBoundaryName = "qsbridge/plan"
	// PackageBoundaryOptimizer is optional rewrite, costing, and plan-improvement vocabulary.
	PackageBoundaryOptimizer PackageBoundaryName = "qsbridge/opt"
	// PackageBoundaryExecution is executor-neutral execution, result, and lifecycle contracts.
	PackageBoundaryExecution PackageBoundaryName = "qsbridge/execution"
	// PackageBoundaryClient is the adapter-facing metadata rowset surface.
	PackageBoundaryClient PackageBoundaryName = "qsbridge/client"
	// PackageBoundaryProtocol is protocol compatibility and response metadata.
	PackageBoundaryProtocol PackageBoundaryName = "qsbridge/protocol"
	// PackageBoundaryCache is reusable shared-memory, cache, and cache-key infrastructure.
	PackageBoundaryCache PackageBoundaryName = "qsbridge/cache"
	// PackageBoundaryTestkit is future shared fixtures and test builders.
	PackageBoundaryTestkit PackageBoundaryName = "qsbridge/testkit"
	// PackageBoundaryRuntime is the native runtime adapter layer outside qsbridge.
	PackageBoundaryRuntime PackageBoundaryName = "qsruntime"
	// PackageBoundaryCompat is the legacy compatibility quarantine outside qsbridge.
	PackageBoundaryCompat PackageBoundaryName = "qscompat"
)

// PackageSplitPhase names one step in the future qsbridge package split.
type PackageSplitPhase string

const (
	// PackageSplitFoundation extracts dependency-light vocabulary first.
	PackageSplitFoundation PackageSplitPhase = "foundation"
	// PackageSplitMetadata extracts catalog and metadata representations.
	PackageSplitMetadata PackageSplitPhase = "metadata"
	// PackageSplitPlanning extracts binder, classifier, and semantic plan IR.
	PackageSplitPlanning PackageSplitPhase = "planning"
	// PackageSplitOptimizer extracts optional rewrite, costing, and plan-improvement policy.
	PackageSplitOptimizer PackageSplitPhase = "optimizer"
	// PackageSplitExecution extracts executor-neutral state and result contracts.
	PackageSplitExecution PackageSplitPhase = "execution"
	// PackageSplitClient extracts adapter-facing rowset surfaces.
	PackageSplitClient PackageSplitPhase = "client"
	// PackageSplitTestkit extracts shared fixtures after production boundaries settle.
	PackageSplitTestkit PackageSplitPhase = "testkit"
	// PackageSplitRuntime keeps runtime adapters outside qsbridge.
	PackageSplitRuntime PackageSplitPhase = "runtime"
	// PackageSplitCompat keeps legacy compatibility adapters outside qsbridge.
	PackageSplitCompat PackageSplitPhase = "compat"
)

// PackageBoundaryLifecycle describes whether a staged boundary is target-state or transitional.
type PackageBoundaryLifecycle string

const (
	// PackageBoundaryActive marks a boundary intended to survive the refactor.
	PackageBoundaryActive PackageBoundaryLifecycle = "active"
	// PackageBoundaryTransitional marks a boundary that quarantines code while replacement work proceeds.
	PackageBoundaryTransitional PackageBoundaryLifecycle = "transitional"
)

// PackageBoundary describes a future package split while qsbridge is still one package.
type PackageBoundary struct {
	Name             PackageBoundaryName
	SplitPhase       PackageSplitPhase
	SplitOrder       int
	Lifecycle        PackageBoundaryLifecycle
	Responsibilities []string
	FilePrefixes     []string
	FileNames        []string
	MayDependOn      []PackageBoundaryName
	DeprecationNotes []string
}

// DefaultPackageBoundaries returns the intended package split for qsbridge.
func DefaultPackageBoundaries() []PackageBoundary {
	return clonePackageBoundaries(defaultPackageBoundaries)
}

// PackageBoundaryForFile classifies a qsbridge file into its intended future package.
func PackageBoundaryForFile(fileName string) (PackageBoundaryName, bool) {
	normalizedPath := normalizeBoundaryPath(fileName)
	if normalizedPath == "" {
		return "", false
	}
	if strings.HasPrefix(normalizedPath, "qsruntime/") {
		return PackageBoundaryRuntime, true
	}
	if strings.HasPrefix(normalizedPath, "qscompat/") {
		return PackageBoundaryCompat, true
	}
	normalized := normalizeBoundaryFileName(fileName)
	if normalized == "" {
		return "", false
	}
	for _, boundary := range defaultPackageBoundaries {
		for _, prefix := range boundary.FilePrefixes {
			if strings.HasPrefix(normalized, prefix) {
				return boundary.Name, true
			}
		}
		for _, name := range boundary.FileNames {
			if normalized == name {
				return boundary.Name, true
			}
		}
	}
	return PackageBoundaryCore, true
}

// PackageBoundaryDependencies returns the package boundaries this boundary may import.
func PackageBoundaryDependencies(name PackageBoundaryName) ([]PackageBoundaryName, bool) {
	for _, boundary := range defaultPackageBoundaries {
		if boundary.Name == name {
			return append([]PackageBoundaryName(nil), boundary.MayDependOn...), true
		}
	}
	return nil, false
}

// PackageBoundaryMayDependOn reports whether one future package boundary may import another.
func PackageBoundaryMayDependOn(from, to PackageBoundaryName) bool {
	if from == to {
		return true
	}
	dependencies, ok := PackageBoundaryDependencies(from)
	if !ok {
		return false
	}
	for _, dependency := range dependencies {
		if dependency == to {
			return true
		}
	}
	return false
}

var defaultPackageBoundaries = []PackageBoundary{
	{
		Name:       PackageBoundaryClient,
		SplitPhase: PackageSplitClient,
		SplitOrder: 5,
		Responsibilities: []string{
			"stable adapter-facing metadata rowsets",
			"client command exchange summaries",
			"prepared, cursor, session, catalog, and diagnostics surfaces",
			"rarely changing logical client surfaces separated from wire protocol details",
		},
		FilePrefixes: []string{"client_"},
		MayDependOn: []PackageBoundaryName{
			PackageBoundaryCore,
			PackageBoundaryCatalog,
			PackageBoundaryPlanning,
			PackageBoundaryExecution,
			PackageBoundaryProtocol,
			PackageBoundaryCache,
		},
	},
	{
		Name:       PackageBoundaryCatalog,
		SplitPhase: PackageSplitMetadata,
		SplitOrder: 2,
		Responsibilities: []string{
			"schema, table, field, relationship, and function metadata",
			"physical encoding and rehydration vocabulary",
			"dictionary and metadata-store boundaries",
		},
		FileNames: []string{
			"catalog.go",
			"catalog_metadata.go",
			"dictionary.go",
			"encoding.go",
			"legacy_encoding.go",
			"store.go",
			"topology.go",
		},
		MayDependOn: []PackageBoundaryName{
			PackageBoundaryCore,
			PackageBoundaryCache,
		},
	},
	{
		Name:       PackageBoundaryPlanning,
		SplitPhase: PackageSplitPlanning,
		SplitOrder: 3,
		Responsibilities: []string{
			"parser-neutral binding and query IR shaping",
			"capability classification, native blockers, and semantic validation",
			"correct logical and coarse physical plan construction without requiring optimizer policy",
		},
		FileNames: []string{
			"bind.go",
			"bridge.go",
			"classifier.go",
			"explain.go",
			"function_usage.go",
			"handoff.go",
			"inspection.go",
			"parameters.go",
			"physical.go",
			"plan.go",
			"plan_invariant.go",
			"planner.go",
			"prepare_metadata.go",
			"query.go",
			"route.go",
			"simple_lexer.go",
			"simple_parser.go",
			"sql_feature.go",
			"sql_feature_coverage.go",
		},
		MayDependOn: []PackageBoundaryName{
			PackageBoundaryCore,
			PackageBoundaryCatalog,
			PackageBoundaryCache,
		},
	},
	{
		Name:       PackageBoundaryOptimizer,
		SplitPhase: PackageSplitOptimizer,
		SplitOrder: 4,
		Responsibilities: []string{
			"optional rewrite candidates and optimizer audit records",
			"cost, benefit, and plan-alternative metadata",
			"future join-ordering and predicate-placement policy that must not be required for semantic correctness",
		},
		FileNames: []string{
			"optimizer.go",
		},
		MayDependOn: []PackageBoundaryName{
			PackageBoundaryCore,
			PackageBoundaryCatalog,
			PackageBoundaryPlanning,
			PackageBoundaryCache,
		},
	},
	{
		Name:       PackageBoundaryExecution,
		SplitPhase: PackageSplitExecution,
		SplitOrder: 5,
		Responsibilities: []string{
			"executor-neutral execution and fallback contracts",
			"prepared, cursor, session, transaction, and batch lifecycle state",
			"result, profile, dispatch, and response metadata",
		},
		FileNames: []string{
			"batch.go",
			"cursor.go",
			"cursor_registry.go",
			"dispatch_preview.go",
			"execution.go",
			"execution_registry.go",
			"executor.go",
			"fallback.go",
			"inmemory_aggregate.go",
			"inmemory_native_executor.go",
			"inmemory_projection.go",
			"lifecycle.go",
			"materialization.go",
			"native_plan_executor.go",
			"prepared.go",
			"prepared_long_data.go",
			"prepared_registry.go",
			"profile.go",
			"projector_kernel.go",
			"quanta_intermediate.go",
			"relationship_execution.go",
			"relationship_vector_reader.go",
			"result.go",
			"same_row_comparison.go",
			"select_execution.go",
			"service.go",
			"session.go",
			"session_registry.go",
			"session_statement.go",
			"session_transition.go",
			"topn_rank.go",
			"transaction.go",
		},
		MayDependOn: []PackageBoundaryName{
			PackageBoundaryCore,
			PackageBoundaryCatalog,
			PackageBoundaryPlanning,
			PackageBoundaryOptimizer,
			PackageBoundaryProtocol,
			PackageBoundaryCache,
		},
	},
	{
		Name:       PackageBoundaryProtocol,
		SplitPhase: PackageSplitFoundation,
		SplitOrder: 1,
		Responsibilities: []string{
			"protocol profiles",
			"result schemas",
			"statement response descriptors",
			"protocol-facing error metadata",
			"wire and transport vocabulary separated from stable client surfaces",
		},
		FileNames: []string{
			"adapter_contract.go",
			"adapter_readiness.go",
			"adapter_rollout.go",
			"adapter_surface.go",
			"compatibility.go",
			"protocol.go",
			"protocol_error.go",
			"result_schema.go",
			"statement_response.go",
			"transport.go",
			"wire_adapter.go",
		},
		MayDependOn: []PackageBoundaryName{
			PackageBoundaryCore,
		},
	},
	{
		Name:       PackageBoundaryCache,
		SplitPhase: PackageSplitFoundation,
		SplitOrder: 1,
		Responsibilities: []string{
			"bounded lock-sharded shared-memory utilities",
			"deterministic shared-memory sizing controllers",
			"deterministic plan-cache keys",
			"process-local prepared-plan and catalog metadata caches",
		},
		FileNames: []string{
			"cachekey.go",
			"catalog_cache.go",
			"prepared_cache.go",
			"sharded_cache.go",
			"shared_memory_tuner.go",
		},
		MayDependOn: []PackageBoundaryName{
			PackageBoundaryCore,
		},
	},
	{
		Name:       PackageBoundaryTestkit,
		SplitPhase: PackageSplitTestkit,
		SplitOrder: 7,
		Responsibilities: []string{
			"shared fixtures",
			"test builders",
			"cross-package assertion helpers",
		},
		MayDependOn: []PackageBoundaryName{
			PackageBoundaryCore,
			PackageBoundaryCatalog,
			PackageBoundaryPlanning,
			PackageBoundaryExecution,
			PackageBoundaryProtocol,
			PackageBoundaryCache,
			PackageBoundaryClient,
		},
	},
	{
		Name:       PackageBoundaryRuntime,
		SplitPhase: PackageSplitRuntime,
		SplitOrder: 8,
		Lifecycle:  PackageBoundaryTransitional,
		Responsibilities: []string{
			"runtime adapters that translate qsbridge-neutral requests to Quanta execution packages",
			"native direct QIAB executor selection and in-process execution adapter implementation",
			"explicit relationship-vector kernel contracts instead of calls into legacy quanta_join",
			"temporary mixed staging until stable execution and qscompat can be split cleanly",
		},
		FileNames: []string{
			"bitmap_result.go",
			"catalog_factory.go",
			"direct_bitmap_runtime.go",
			"direct_config.go",
			"direct_executor.go",
			"direct_factory.go",
			"direct_session.go",
			"execution.go",
			"executor.go",
			"executor_selector.go",
			"legacy_bitmap_result.go",
			"legacy_catalog.go",
			"legacy_direct_runtime.go",
			"legacy_executor.go",
			"quanta_legacy_adapter.go",
			"relationship_join_plan.go",
			"relationship_vector_reader.go",
			"route.go",
			"runtime_environment.go",
			"service.go",
			"sql_runtime.go",
		},
		MayDependOn: []PackageBoundaryName{
			PackageBoundaryCore,
			PackageBoundaryExecution,
		},
		DeprecationNotes: []string{
			"legacy compatibility adapters should move to qscompat when the split is worth the churn",
			"qsruntime itself may shrink once direct QIAB execution no longer needs compatibility staging",
		},
	},
	{
		Name:       PackageBoundaryCompat,
		SplitPhase: PackageSplitCompat,
		SplitOrder: 9,
		Lifecycle:  PackageBoundaryTransitional,
		Responsibilities: []string{
			"legacy gRPC/proxy compatibility adapters",
			"legacy source, core, shared, grpc, and server bridges needed only during migration",
			"inabox-direct session and projector shims that must not leak back into qsbridge",
		},
		MayDependOn: []PackageBoundaryName{
			PackageBoundaryCore,
			PackageBoundaryExecution,
			PackageBoundaryRuntime,
		},
		DeprecationNotes: []string{
			"qscompat is the future quarantine for legacy compatibility code and should be deleted when direct execution covers the required SQL surface",
		},
	},
	{
		Name:       PackageBoundaryCore,
		SplitPhase: PackageSplitFoundation,
		SplitOrder: 1,
		Responsibilities: []string{
			"foundational SQL value and expression vocabulary",
			"diagnostic and support-status primitives",
			"authentication, authorization, connection, and protocol-neutral identity concepts",
			"package-boundary metadata for the staged refactor",
		},
		MayDependOn: nil,
	},
}

func normalizeBoundaryPath(fileName string) string {
	fileName = strings.TrimSpace(fileName)
	fileName = strings.ReplaceAll(fileName, "\\", "/")
	return strings.TrimPrefix(fileName, "./")
}

func normalizeBoundaryFileName(fileName string) string {
	fileName = strings.TrimSpace(fileName)
	fileName = strings.ReplaceAll(fileName, "\\", "/")
	if slash := strings.LastIndex(fileName, "/"); slash >= 0 {
		fileName = fileName[slash+1:]
	}
	if strings.HasSuffix(fileName, "_test.go") {
		fileName = strings.TrimSuffix(fileName, "_test.go") + ".go"
	}
	if fileName == "" {
		return ""
	}
	return fileName
}

func clonePackageBoundaries(boundaries []PackageBoundary) []PackageBoundary {
	cloned := make([]PackageBoundary, len(boundaries))
	for i, boundary := range boundaries {
		cloned[i] = PackageBoundary{
			Name:             boundary.Name,
			SplitPhase:       boundary.SplitPhase,
			SplitOrder:       boundary.SplitOrder,
			Lifecycle:        boundary.Lifecycle,
			Responsibilities: append([]string(nil), boundary.Responsibilities...),
			FilePrefixes:     append([]string(nil), boundary.FilePrefixes...),
			FileNames:        append([]string(nil), boundary.FileNames...),
			MayDependOn:      append([]PackageBoundaryName(nil), boundary.MayDependOn...),
			DeprecationNotes: append([]string(nil), boundary.DeprecationNotes...),
		}
	}
	return cloned
}

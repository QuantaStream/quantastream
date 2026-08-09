package qsruntime

import (
	"path"
	"strings"
)

// RuntimeOwnershipCategory describes why a qsruntime file belongs here today.
type RuntimeOwnershipCategory string

const (
	// RuntimeOwnershipUnknown means a file has not been assigned an ownership lane.
	RuntimeOwnershipUnknown RuntimeOwnershipCategory = ""
	// RuntimeOwnershipComposition is durable runtime assembly and routing glue.
	RuntimeOwnershipComposition RuntimeOwnershipCategory = "runtime_composition"
	// RuntimeOwnershipQSBridgeMigration is durable engine vocabulary that should move toward qsbridge.
	RuntimeOwnershipQSBridgeMigration RuntimeOwnershipCategory = "qsbridge_migration"
	// RuntimeOwnershipNativeKernelStaging is native execution logic staged here until qsbridge owns the executor boundary.
	RuntimeOwnershipNativeKernelStaging RuntimeOwnershipCategory = "native_kernel_staging"
	// RuntimeOwnershipMetadataBridge adapts metadata or dictionary invalidation into runtime caches.
	RuntimeOwnershipMetadataBridge RuntimeOwnershipCategory = "metadata_bridge"
	// RuntimeOwnershipCompatQuarantine is legacy/direct compatibility code that should move to qscompat or die.
	RuntimeOwnershipCompatQuarantine RuntimeOwnershipCategory = "compat_quarantine"
	// RuntimeOwnershipPreflightScaffold is temporary SQL rewrite/subquery-step scaffolding awaiting native planner/executor ownership.
	RuntimeOwnershipPreflightScaffold RuntimeOwnershipCategory = "preflight_scaffold"
	// RuntimeOwnershipPackageDocs is package-level documentation and architecture guardrails.
	RuntimeOwnershipPackageDocs RuntimeOwnershipCategory = "package_docs"
	// RuntimeOwnershipTests marks test files, which are excluded from production ownership burn-down.
	RuntimeOwnershipTests RuntimeOwnershipCategory = "tests"
)

// RuntimeOwnership records the current and intended home for one qsruntime file.
type RuntimeOwnership struct {
	File       string
	Category   RuntimeOwnershipCategory
	Target     string
	Retirement string
}

// RuntimeOwnershipForFile classifies a qsruntime file into its burn-down lane.
func RuntimeOwnershipForFile(fileName string) RuntimeOwnership {
	file := runtimeOwnershipFileName(fileName)
	ownership := RuntimeOwnership{File: file}
	switch {
	case file == "":
		return ownership
	case strings.HasSuffix(file, "_test.go"):
		ownership.Category = RuntimeOwnershipTests
		ownership.Target = "test coverage"
	case file == "doc.go" || file == "ownership.go" || file == "architecture_test.go" || file == "documentation_test.go":
		ownership.Category = RuntimeOwnershipPackageDocs
		ownership.Target = "qsruntime guardrails"
	case runtimeOwnershipCompatFile(file):
		ownership.Category = RuntimeOwnershipCompatQuarantine
		ownership.Target = "qscompat"
		ownership.Retirement = "delete after native execution covers the required SQL surface"
	case runtimeOwnershipPreflightScaffoldFile(file):
		ownership.Category = RuntimeOwnershipPreflightScaffold
		ownership.Target = "native planner/executor subquery steps"
		ownership.Retirement = "delete SQL-text rewrite path after typed IR and native subquery execution cover the shape"
	case runtimeOwnershipMetadataBridgeFile(file):
		ownership.Category = RuntimeOwnershipMetadataBridge
		ownership.Target = "runtime metadata cache boundary"
		ownership.Retirement = "keep only while runtime caches are outside the final catalog/cache package"
	case runtimeOwnershipNativeKernelStagingFile(file):
		ownership.Category = RuntimeOwnershipNativeKernelStaging
		ownership.Target = "qsbridge executor boundary"
		ownership.Retirement = "move semantic contracts into qsbridge, leave only storage/cluster adapters here"
	case runtimeOwnershipQSBridgeMigrationFile(file):
		ownership.Category = RuntimeOwnershipQSBridgeMigration
		ownership.Target = "qsbridge"
		ownership.Retirement = "move once import churn is lower than boundary confusion"
	case runtimeOwnershipCompositionFile(file):
		ownership.Category = RuntimeOwnershipComposition
		ownership.Target = "qsruntime"
		ownership.Retirement = "shrink after direct QIAB runtime and qscompat split stabilize"
	}
	return ownership
}

// RuntimeOwnershipQSBridgeMigrationFiles returns files still parked in the generic migration lane.
func RuntimeOwnershipQSBridgeMigrationFiles(fileNames []string) []string {
	files := []string{}
	for _, fileName := range fileNames {
		ownership := RuntimeOwnershipForFile(fileName)
		if ownership.Category == RuntimeOwnershipQSBridgeMigration {
			files = append(files, ownership.File)
		}
	}
	return files
}

func runtimeOwnershipFileName(fileName string) string {
	normalized := strings.TrimSpace(fileName)
	normalized = strings.ReplaceAll(normalized, "\\", "/")
	return path.Base(normalized)
}

func runtimeOwnershipCompatFile(file string) bool {
	if strings.HasPrefix(file, "legacy_") {
		return true
	}
	switch file {
	case "quanta_legacy_adapter.go":
		return true
	default:
		return false
	}
}

func runtimeOwnershipPreflightScaffoldFile(file string) bool {
	if strings.HasPrefix(file, "preflight_") {
		return true
	}
	switch file {
	case "correlated_subquery_rewrite.go",
		"native_preflight_step.go",
		"native_subquery_step_executor.go":
		return true
	default:
		return false
	}
}

func runtimeOwnershipMetadataBridgeFile(file string) bool {
	switch file {
	case "dictionary_invalidation.go",
		"metadata_invalidation.go",
		"schema_mutation.go":
		return true
	default:
		return false
	}
}

func runtimeOwnershipNativeKernelStagingFile(file string) bool {
	if strings.HasPrefix(file, "direct_bitmap_") {
		return true
	}
	switch file {
	case "exists_subquery_materialization.go",
		"filter_domain_normalization.go",
		"filter_evaluator.go",
		"native_predicate.go",
		"native_projection_capability.go",
		"native_projection_materialization.go",
		"native_subquery_preparation.go",
		"relationship_tuple_rowset.go",
		"relationship_vector_reader.go",
		"scalar_subquery_materialization.go":
		return true
	case "same_row_comparison.go":
		return true
	default:
		return false
	}
}

func runtimeOwnershipQSBridgeMigrationFile(file string) bool {
	return false
}

func runtimeOwnershipCompositionFile(file string) bool {
	switch file {
	case "bitmap_result_adapter.go",
		"catalog_factory.go",
		"direct_config.go",
		"direct_executor.go",
		"direct_factory.go",
		"direct_physical_tier.go",
		"direct_session.go",
		"execution.go",
		"execution_instrumentation.go",
		"execution_inspection.go",
		"executor.go",
		"executor_selector.go",
		"inspection_rows.go",
		"materialization.go",
		"native_proxy_listener.go",
		"native_proxy_mysql_command.go",
		"native_proxy_mysql_metadata.go",
		"native_proxy_mysql_profile.go",
		"native_proxy_mysql_session.go",
		"native_proxy_runtime.go",
		"native_proxy_server.go",
		"query_scratchpad.go",
		"relationship_join_plan.go",
		"route.go",
		"runtime_environment.go",
		"runtime_profile.go",
		"service.go",
		"sql_runtime.go":
		return true
	default:
		return false
	}
}

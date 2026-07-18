package qsruntime

import (
	"os"
	"strings"
	"testing"
)

func TestRuntimeOwnershipForFileClassifiesEveryProductionGoFile(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read qsruntime package: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		ownership := RuntimeOwnershipForFile(entry.Name())
		if ownership.Category == RuntimeOwnershipUnknown {
			t.Fatalf("%s has no qsruntime ownership lane", entry.Name())
		}
		if ownership.Target == "" {
			t.Fatalf("%s ownership = %#v, want target", entry.Name(), ownership)
		}
	}
}

func TestRuntimeOwnershipQSBridgeMigrationLaneIsIntentionallyEmpty(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read qsruntime package: %v", err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		files = append(files, entry.Name())
	}
	if migration := RuntimeOwnershipQSBridgeMigrationFiles(files); len(migration) != 0 {
		t.Fatalf("qsbridge migration lane = %#v, want empty; classify as composition, native staging, preflight scaffold, metadata bridge, or compat quarantine", migration)
	}
}

func TestRuntimeOwnershipForFileMarksLegacyAdaptersAsCompatQuarantine(t *testing.T) {
	for _, file := range []string{
		"legacy_direct_runtime.go",
		"legacy_projection_materializer.go",
		"quanta_legacy_adapter.go",
	} {
		ownership := RuntimeOwnershipForFile(file)
		if ownership.Category != RuntimeOwnershipCompatQuarantine || ownership.Target != "qscompat" {
			t.Fatalf("%s ownership = %#v, want compat quarantine", file, ownership)
		}
	}
}

func TestRuntimeOwnershipForFileMarksPreflightHelpersAsScaffold(t *testing.T) {
	for _, file := range []string{
		"preflight_helper_execution.go",
		"preflight_rewrite.go",
		"preflight_rewrite_descriptor.go",
		"preflight_rewrite_inventory.go",
		"native_preflight_step.go",
		"native_subquery_step_executor.go",
		"correlated_subquery_rewrite.go",
	} {
		ownership := RuntimeOwnershipForFile(file)
		if ownership.Category != RuntimeOwnershipPreflightScaffold || ownership.Target != "native planner/executor subquery steps" {
			t.Fatalf("%s ownership = %#v, want preflight scaffold", file, ownership)
		}
		if ownership.Retirement == "" {
			t.Fatalf("%s ownership = %#v, want retirement path", file, ownership)
		}
	}
}

func TestRuntimeOwnershipForFileSeparatesDurableContractsFromNativeStaging(t *testing.T) {
	tests := map[string]RuntimeOwnershipCategory{
		"bitmap_result_adapter.go":           RuntimeOwnershipComposition,
		"execution_inspection.go":            RuntimeOwnershipComposition,
		"inspection_rows.go":                 RuntimeOwnershipComposition,
		"materialization.go":                 RuntimeOwnershipComposition,
		"relationship_join_plan.go":          RuntimeOwnershipComposition,
		"relationship_tuple_rowset.go":       RuntimeOwnershipNativeKernelStaging,
		"direct_bitmap_runtime.go":           RuntimeOwnershipNativeKernelStaging,
		"exists_subquery_materialization.go": RuntimeOwnershipNativeKernelStaging,
		"relationship_vector_reader.go":      RuntimeOwnershipNativeKernelStaging,
		"scalar_subquery_materialization.go": RuntimeOwnershipNativeKernelStaging,
		"preflight_rewrite.go":               RuntimeOwnershipPreflightScaffold,
		"execution.go":                       RuntimeOwnershipComposition,
		"sql_runtime.go":                     RuntimeOwnershipComposition,
		"metadata_invalidation.go":           RuntimeOwnershipMetadataBridge,
	}
	for file, want := range tests {
		if got := RuntimeOwnershipForFile(file).Category; got != want {
			t.Fatalf("%s ownership = %q, want %q", file, got, want)
		}
	}
}

func TestRuntimeOwnershipTemporaryCategoriesDeclareRetirementPath(t *testing.T) {
	for _, file := range []string{
		"legacy_direct_runtime.go",
		"metadata_invalidation.go",
		"direct_bitmap_runtime.go",
		"materialization.go",
		"bitmap_result_adapter.go",
	} {
		ownership := RuntimeOwnershipForFile(file)
		if ownership.Category == RuntimeOwnershipUnknown {
			t.Fatalf("%s has no qsruntime ownership lane", file)
		}
		if ownership.Category == RuntimeOwnershipPackageDocs || ownership.Category == RuntimeOwnershipTests {
			continue
		}
		if ownership.Retirement == "" {
			t.Fatalf("%s ownership = %#v, want retirement path", file, ownership)
		}
	}
}

func TestRuntimeOwnershipUnknownForUnregisteredProductionFile(t *testing.T) {
	ownership := RuntimeOwnershipForFile("new_engine_vocabulary.go")
	if ownership.Category != RuntimeOwnershipUnknown {
		t.Fatalf("ownership = %#v, want unknown for unregistered production file", ownership)
	}
}

package qsbridge

import "testing"

func TestPackageBoundaryForFileClassifiesFutureSplit(t *testing.T) {
	tests := []struct {
		name string
		file string
		want PackageBoundaryName
	}{
		{name: "core", file: "diagnostic.go", want: PackageBoundaryCore},
		{name: "core test", file: "architecture_test.go", want: PackageBoundaryCore},
		{name: "catalog", file: "catalog.go", want: PackageBoundaryCatalog},
		{name: "dictionary", file: "dictionary_test.go", want: PackageBoundaryCatalog},
		{name: "planning", file: "planner.go", want: PackageBoundaryPlanning},
		{name: "optimizer", file: "optimizer.go", want: PackageBoundaryOptimizer},
		{name: "query ir", file: "query.go", want: PackageBoundaryPlanning},
		{name: "execution", file: "execution.go", want: PackageBoundaryExecution},
		{name: "projector kernel", file: "projector_kernel.go", want: PackageBoundaryExecution},
		{name: "session", file: "session_registry.go", want: PackageBoundaryExecution},
		{name: "client", file: "client_cursor.go", want: PackageBoundaryClient},
		{name: "client structured explain", file: "client_explain.go", want: PackageBoundaryClient},
		{name: "client plan cache policy", file: "client_plan_cache_policy.go", want: PackageBoundaryClient},
		{name: "client path", file: "qsbridge/client_session_test.go", want: PackageBoundaryClient},
		{name: "protocol", file: "protocol_error.go", want: PackageBoundaryProtocol},
		{name: "compatibility manifest", file: "compatibility.go", want: PackageBoundaryProtocol},
		{name: "result schema", file: "result_schema.go", want: PackageBoundaryProtocol},
		{name: "cache", file: "sharded_cache.go", want: PackageBoundaryCache},
		{name: "shared memory tuner", file: "shared_memory_tuner.go", want: PackageBoundaryCache},
		{name: "plan cache key", file: "cachekey.go", want: PackageBoundaryCache},
		{name: "catalog cache", file: "catalog_cache_test.go", want: PackageBoundaryCache},
		{name: "runtime adapter path", file: "qsruntime/quanta_legacy_adapter.go", want: PackageBoundaryRuntime},
		{name: "runtime bitmap result path", file: "qsruntime/bitmap_result.go", want: PackageBoundaryRuntime},
		{name: "runtime catalog factory path", file: "qsruntime/catalog_factory.go", want: PackageBoundaryRuntime},
		{name: "runtime direct bitmap path", file: "qsruntime/direct_bitmap_runtime.go", want: PackageBoundaryRuntime},
		{name: "runtime direct config path", file: "qsruntime/direct_config.go", want: PackageBoundaryRuntime},
		{name: "runtime direct executor path", file: "qsruntime/direct_executor.go", want: PackageBoundaryRuntime},
		{name: "runtime direct factory path", file: "qsruntime/direct_factory.go", want: PackageBoundaryRuntime},
		{name: "runtime legacy catalog path", file: "qsruntime/legacy_catalog.go", want: PackageBoundaryRuntime},
		{name: "runtime legacy direct path", file: "qsruntime/legacy_direct_runtime.go", want: PackageBoundaryRuntime},
		{name: "runtime environment path", file: "qsruntime/runtime_environment.go", want: PackageBoundaryRuntime},
		{name: "runtime service path", file: "qsruntime/service.go", want: PackageBoundaryRuntime},
		{name: "runtime sql facade path", file: "qsruntime/sql_runtime.go", want: PackageBoundaryRuntime},
		{name: "runtime windows path", file: `qsruntime\executor_selector_test.go`, want: PackageBoundaryRuntime},
		{name: "compat path", file: "qscompat/legacy_executor.go", want: PackageBoundaryCompat},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := PackageBoundaryForFile(tt.file)
			if !ok {
				t.Fatalf("PackageBoundaryForFile(%q) not classified", tt.file)
			}
			if got != tt.want {
				t.Fatalf("PackageBoundaryForFile(%q) = %s, want %s", tt.file, got, tt.want)
			}
		})
	}
}

func TestPackageBoundaryForFileRejectsEmptyNames(t *testing.T) {
	if got, ok := PackageBoundaryForFile("   "); ok {
		t.Fatalf("PackageBoundaryForFile(empty) = %s, true; want false", got)
	}
}

func TestDefaultPackageBoundariesReturnCopies(t *testing.T) {
	first := DefaultPackageBoundaries()
	if len(first) == 0 {
		t.Fatal("DefaultPackageBoundaries() returned no boundaries")
	}
	first[0].Responsibilities[0] = "mutated"
	first[0].MayDependOn[0] = PackageBoundaryTestkit

	second := DefaultPackageBoundaries()
	if second[0].Responsibilities[0] == "mutated" {
		t.Fatal("DefaultPackageBoundaries returned aliased responsibility slices")
	}
	if second[0].MayDependOn[0] == PackageBoundaryTestkit {
		t.Fatal("DefaultPackageBoundaries returned aliased dependency slices")
	}
}

func TestDefaultPackageBoundariesDescribeSplitPhases(t *testing.T) {
	boundaries := DefaultPackageBoundaries()
	tests := map[PackageBoundaryName]struct {
		phase PackageSplitPhase
		order int
	}{
		PackageBoundaryCore:      {phase: PackageSplitFoundation, order: 1},
		PackageBoundaryProtocol:  {phase: PackageSplitFoundation, order: 1},
		PackageBoundaryCache:     {phase: PackageSplitFoundation, order: 1},
		PackageBoundaryCatalog:   {phase: PackageSplitMetadata, order: 2},
		PackageBoundaryPlanning:  {phase: PackageSplitPlanning, order: 3},
		PackageBoundaryOptimizer: {phase: PackageSplitOptimizer, order: 4},
		PackageBoundaryExecution: {phase: PackageSplitExecution, order: 5},
		PackageBoundaryClient:    {phase: PackageSplitClient, order: 5},
		PackageBoundaryTestkit:   {phase: PackageSplitTestkit, order: 7},
		PackageBoundaryRuntime:   {phase: PackageSplitRuntime, order: 8},
		PackageBoundaryCompat:    {phase: PackageSplitCompat, order: 9},
	}
	for _, boundary := range boundaries {
		want, ok := tests[boundary.Name]
		if !ok {
			t.Fatalf("boundary = %#v, want known split phase", boundary)
		}
		if boundary.SplitPhase != want.phase || boundary.SplitOrder != want.order {
			t.Fatalf("boundary = %#v, want phase/order %#v", boundary, want)
		}
	}
}

func TestPackageBoundaryDependenciesDescribeAcyclicImportDirection(t *testing.T) {
	if !PackageBoundaryMayDependOn(PackageBoundaryClient, PackageBoundaryCore) {
		t.Fatal("client boundary should be allowed to depend on core")
	}
	if !PackageBoundaryMayDependOn(PackageBoundaryProtocol, PackageBoundaryCore) {
		t.Fatal("protocol boundary should be allowed to depend on core")
	}
	if !PackageBoundaryMayDependOn(PackageBoundaryCache, PackageBoundaryCore) {
		t.Fatal("cache boundary should be allowed to depend on core vocabulary")
	}
	if !PackageBoundaryMayDependOn(PackageBoundaryPlanning, PackageBoundaryCatalog) {
		t.Fatal("planning boundary should be allowed to depend on catalog metadata")
	}
	if !PackageBoundaryMayDependOn(PackageBoundaryOptimizer, PackageBoundaryPlanning) {
		t.Fatal("optimizer boundary should be allowed to depend on semantically correct planned query shapes")
	}
	if !PackageBoundaryMayDependOn(PackageBoundaryExecution, PackageBoundaryOptimizer) {
		t.Fatal("execution boundary should be allowed to depend on optimizer metadata")
	}
	if PackageBoundaryMayDependOn(PackageBoundaryCore, PackageBoundaryClient) {
		t.Fatal("core boundary must not depend on client-facing rowsets")
	}
	if PackageBoundaryMayDependOn(PackageBoundaryCatalog, PackageBoundaryPlanning) {
		t.Fatal("catalog boundary must not depend on planner internals")
	}
	if PackageBoundaryMayDependOn(PackageBoundaryProtocol, PackageBoundaryClient) {
		t.Fatal("protocol boundary must not depend on client-facing rowsets")
	}
	if PackageBoundaryMayDependOn(PackageBoundaryName("unknown"), PackageBoundaryCore) {
		t.Fatal("unknown boundary must not report dependencies")
	}
	if !PackageBoundaryMayDependOn(PackageBoundaryRuntime, PackageBoundaryExecution) {
		t.Fatal("runtime boundary should be allowed to depend on execution handoff contracts")
	}
	if !PackageBoundaryMayDependOn(PackageBoundaryCompat, PackageBoundaryRuntime) {
		t.Fatal("compat boundary should be allowed to depend on runtime staging contracts")
	}
	if PackageBoundaryMayDependOn(PackageBoundaryExecution, PackageBoundaryRuntime) {
		t.Fatal("execution boundary must not depend on runtime adapters")
	}
}

func TestRuntimeAndCompatBoundariesAreMarkedTransitional(t *testing.T) {
	var runtime PackageBoundary
	found := false
	for _, boundary := range DefaultPackageBoundaries() {
		if boundary.Name == PackageBoundaryRuntime {
			runtime = boundary
			found = true
			break
		}
	}
	if !found {
		t.Fatal("runtime boundary not found")
	}
	if runtime.Lifecycle != PackageBoundaryTransitional {
		t.Fatalf("runtime lifecycle = %q, want %q", runtime.Lifecycle, PackageBoundaryTransitional)
	}
	if len(runtime.DeprecationNotes) == 0 {
		t.Fatal("runtime boundary should describe compatibility staging notes")
	}

	var compat PackageBoundary
	found = false
	for _, boundary := range DefaultPackageBoundaries() {
		if boundary.Name == PackageBoundaryCompat {
			compat = boundary
			found = true
			break
		}
	}
	if !found {
		t.Fatal("compat boundary not found")
	}
	if compat.Lifecycle != PackageBoundaryTransitional {
		t.Fatalf("compat lifecycle = %q, want %q", compat.Lifecycle, PackageBoundaryTransitional)
	}
}

func TestPackageBoundaryDependenciesReturnCopies(t *testing.T) {
	dependencies, ok := PackageBoundaryDependencies(PackageBoundaryClient)
	if !ok || len(dependencies) == 0 {
		t.Fatalf("PackageBoundaryDependencies(client) = %#v/%v, want dependencies", dependencies, ok)
	}
	dependencies[0] = PackageBoundaryTestkit

	again, ok := PackageBoundaryDependencies(PackageBoundaryClient)
	if !ok || again[0] == PackageBoundaryTestkit {
		t.Fatalf("PackageBoundaryDependencies leaked mutation: %#v/%v", again, ok)
	}
}

func TestDefaultPackageBoundariesCopyDeprecationNotes(t *testing.T) {
	first := DefaultPackageBoundaries()
	var runtimeIndex int
	found := false
	for i, boundary := range first {
		if boundary.Name == PackageBoundaryRuntime {
			runtimeIndex = i
			found = true
			break
		}
	}
	if !found || len(first[runtimeIndex].DeprecationNotes) == 0 {
		t.Fatal("runtime boundary deprecation notes not found")
	}
	first[runtimeIndex].DeprecationNotes[0] = "mutated"

	again := DefaultPackageBoundaries()
	for _, boundary := range again {
		if boundary.Name == PackageBoundaryRuntime {
			if boundary.DeprecationNotes[0] == "mutated" {
				t.Fatal("DefaultPackageBoundaries returned aliased deprecation notes")
			}
			return
		}
	}
	t.Fatal("runtime boundary not found on second read")
}

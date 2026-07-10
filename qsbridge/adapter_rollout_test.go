package qsbridge

import "testing"

func TestAdapterRolloutStepsForSurfaceAreOrderedAndStatused(t *testing.T) {
	steps := AdapterRolloutStepsForSurface(AdapterSurfaceMySQLServer)
	if len(steps) != 5 {
		t.Fatalf("steps = %#v, want five MySQL rollout phases", steps)
	}
	if steps[0].Phase != AdapterRolloutMetadataInventory || steps[0].Order != 1 ||
		steps[0].Status != CompatibilityStatusMetadataOnly || steps[0].BlocksRuntime {
		t.Fatalf("first step = %#v, want non-blocking metadata inventory", steps[0])
	}
	if steps[1].Phase != AdapterRolloutAdapterShell || steps[1].Status != CompatibilityStatusBoundaryOnly ||
		!steps[1].BlocksRuntime {
		t.Fatalf("second step = %#v, want runtime-blocking adapter shell", steps[1])
	}
	if steps[4].Phase != AdapterRolloutRuntimeEnablement || steps[4].Status != CompatibilityStatusDeferred ||
		steps[4].Owner != WireAdapterOwnerExecutor {
		t.Fatalf("final step = %#v, want deferred executor-owned runtime enablement", steps[4])
	}
}

func TestDefaultAdapterRolloutStepsIncludeEverySurface(t *testing.T) {
	steps := DefaultAdapterRolloutSteps()
	for _, surface := range []AdapterSurfaceKind{
		AdapterSurfaceMySQLServer,
		AdapterSurfaceGRPCAPI,
		AdapterSurfaceEmbedded,
		AdapterSurfaceInternalExecution,
	} {
		if countAdapterRolloutSteps(steps, surface) != 5 {
			t.Fatalf("steps for %s = %#v, want five phases", surface, AdapterRolloutStepsForSurface(surface))
		}
	}
}

func TestDefaultAdapterRolloutStepsReturnDeepCopies(t *testing.T) {
	first := DefaultAdapterRolloutSteps()
	first[0].Detail = "mutated"
	first[0].Requires[0] = AdapterContractTopology

	second := DefaultAdapterRolloutSteps()
	if second[0].Detail == "mutated" || second[0].Requires[0] == AdapterContractTopology {
		t.Fatalf("adapter rollout steps leaked mutable state: %#v", second[0])
	}
}

func countAdapterRolloutSteps(steps []AdapterRolloutStep, surface AdapterSurfaceKind) int {
	count := 0
	for _, step := range steps {
		if step.Surface == surface {
			count++
		}
	}
	return count
}

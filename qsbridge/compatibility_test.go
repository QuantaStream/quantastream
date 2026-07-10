package qsbridge

import "testing"

func TestDefaultCompatibilityProfileDescribesKeyScaffoldCapabilities(t *testing.T) {
	profile := DefaultCompatibilityProfile()
	if len(profile.Capabilities) == 0 {
		t.Fatalf("profile = %#v, want scaffold capabilities", profile)
	}
	for _, name := range []string{
		"catalog_binding",
		"query_ir",
		"client_statement_flow",
		"structured_explain",
		"plan_cache_policy",
		"protocol_negotiation",
		"native_executor",
		"legacy_fallback",
	} {
		if !compatibilityProfileHas(profile, name) {
			t.Fatalf("profile = %#v, missing %q", profile.Capabilities, name)
		}
	}
	if capability, ok := compatibilityProfileCapability(profile, "native_executor"); !ok || !capability.RuntimeOwned || capability.Status != CompatibilityStatusBoundaryOnly {
		t.Fatalf("native executor capability = %#v/%v, want runtime-owned boundary", capability, ok)
	}
	if capability, ok := compatibilityProfileCapability(profile, "authentication"); !ok || !capability.AdapterOwned || capability.Status != CompatibilityStatusBoundaryOnly {
		t.Fatalf("authentication capability = %#v/%v, want adapter-owned boundary", capability, ok)
	}
	if capability, ok := compatibilityProfileCapability(profile, "structured_explain"); !ok || capability.Layer != CompatibilityLayerClient || capability.Status != CompatibilityStatusMetadataOnly {
		t.Fatalf("structured explain capability = %#v/%v, want client metadata", capability, ok)
	}
	if capability, ok := compatibilityProfileCapability(profile, "plan_cache_policy"); !ok || capability.Layer != CompatibilityLayerClient || capability.Status != CompatibilityStatusMetadataOnly {
		t.Fatalf("plan cache policy capability = %#v/%v, want client metadata", capability, ok)
	}
}

func TestCompatibilityProfileCloneCopiesMutableState(t *testing.T) {
	profile := DefaultCompatibilityProfile()
	profile.Diagnostics = DiagnosticSet{
		ErrorDiagnostic(DiagnosticInternalInvariant, PhasePlan, "original"),
	}

	cloned := profile.Clone()
	cloned.Capabilities[0].Name = "mutated"
	cloned.Diagnostics[0].Message = "mutated"

	if profile.Capabilities[0].Name == "mutated" {
		t.Fatalf("capabilities leaked mutation: %#v", profile.Capabilities[0])
	}
	if profile.Diagnostics[0].Message != "original" {
		t.Fatalf("diagnostics leaked mutation: %#v", profile.Diagnostics)
	}
}

func compatibilityProfileHas(profile CompatibilityProfile, name string) bool {
	_, ok := compatibilityProfileCapability(profile, name)
	return ok
}

func compatibilityProfileCapability(profile CompatibilityProfile, name string) (CompatibilityCapability, bool) {
	for _, capability := range profile.Capabilities {
		if capability.Name == name {
			return capability, true
		}
	}
	return CompatibilityCapability{}, false
}

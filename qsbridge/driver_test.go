package qsbridge

import "testing"

func TestDefaultClientDriverCompatibilityIncludesTargetEcosystems(t *testing.T) {
	profiles := DefaultClientDriverCompatibility()
	for _, ecosystem := range []ClientDriverEcosystem{
		ClientDriverEcosystemNodeJS,
		ClientDriverEcosystemPython,
		ClientDriverEcosystemJava,
		ClientDriverEcosystemGo,
		ClientDriverEcosystemNativeGo,
		ClientDriverEcosystemGRPC,
	} {
		if _, ok := clientDriverCompatibilityByEcosystem(profiles, ecosystem); !ok {
			t.Fatalf("profiles = %#v, missing ecosystem %s", profiles, ecosystem)
		}
	}
	node, _ := clientDriverCompatibilityByEcosystem(profiles, ClientDriverEcosystemNodeJS)
	if node.Protocol != ProtocolMySQL || !node.Capabilities.Has(ProtocolCapabilityPreparedStatements) {
		t.Fatalf("node profile = %#v, want MySQL prepared-statement target", node)
	}
	if !clientDriverAuthPluginsContain(node.AuthPlugins, AuthenticationPluginCachingSHA2Password) {
		t.Fatalf("node profile = %#v, want caching_sha2 auth plugin", node)
	}
	native, _ := clientDriverCompatibilityByEcosystem(profiles, ClientDriverEcosystemNativeGo)
	if native.Protocol != ProtocolGo || !native.Capabilities.Has(ProtocolCapabilityProfile) {
		t.Fatalf("native profile = %#v, want Go profile target", native)
	}
	if !native.Capabilities.Has(ProtocolCapabilityStructuredExplain) || !native.Capabilities.Has(ProtocolCapabilityPlanCachePolicy) {
		t.Fatalf("native profile = %#v, want structured explain and cache policy metadata", native)
	}
	if !clientDriverAuthPluginsContain(native.AuthPlugins, AuthenticationPluginBearerJWT) {
		t.Fatalf("native profile = %#v, want bearer JWT auth hook", native)
	}

	grpc, _ := clientDriverCompatibilityByEcosystem(profiles, ClientDriverEcosystemGRPC)
	if grpc.Protocol != ProtocolGRPC || !grpc.Capabilities.Has(ProtocolCapabilityStreamingResults) ||
		!grpc.Capabilities.Has(ProtocolCapabilityCancellation) {
		t.Fatalf("grpc profile = %#v, want typed streaming/cancelable API target", grpc)
	}
	if !grpc.Capabilities.Has(ProtocolCapabilityStructuredExplain) || !grpc.Capabilities.Has(ProtocolCapabilityPlanCachePolicy) {
		t.Fatalf("grpc profile = %#v, want structured explain and cache policy metadata", grpc)
	}
}

func TestDefaultClientDriverCompatibilityReturnsCopies(t *testing.T) {
	first := DefaultClientDriverCompatibility()
	first[0].Drivers[0] = "mutated"
	first[0].Capabilities[0] = "mutated"
	first[0].AuthPlugins[0] = "mutated"

	second := DefaultClientDriverCompatibility()
	if second[0].Drivers[0] == "mutated" || second[0].Capabilities[0] == "mutated" || second[0].AuthPlugins[0] == "mutated" {
		t.Fatalf("driver compatibility leaked mutable state: %#v", second[0])
	}
}

func clientDriverCompatibilityByEcosystem(profiles []ClientDriverCompatibility, ecosystem ClientDriverEcosystem) (ClientDriverCompatibility, bool) {
	for _, profile := range profiles {
		if profile.Ecosystem == ecosystem {
			return profile, true
		}
	}
	return ClientDriverCompatibility{}, false
}

func clientDriverAuthPluginsContain(plugins []AuthenticationPlugin, target AuthenticationPlugin) bool {
	for _, plugin := range plugins {
		if plugin == target {
			return true
		}
	}
	return false
}

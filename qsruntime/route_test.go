package qsruntime

import (
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestDirectQIABRouteUsesLocalDiscovery(t *testing.T) {
	route := DirectQIABRoute()

	if !route.Direct() {
		t.Fatalf("direct route Direct() = false, want true")
	}
	if route.UsesConsul() {
		t.Fatalf("direct route UsesConsul() = true, want false")
	}
	if route.CompatibilityPath() {
		t.Fatalf("direct route CompatibilityPath() = true, want false")
	}
	if !route.Target.Local {
		t.Fatalf("direct route target local = false, want true")
	}
}

func TestConsulDirectRoutePreservesTarget(t *testing.T) {
	target := RuntimeTarget{
		NodeID:  "node-a",
		Address: "127.0.0.1:4000",
	}
	route := ConsulDirectRoute(target)

	if !route.Direct() {
		t.Fatalf("consul direct route Direct() = false, want true")
	}
	if !route.UsesConsul() {
		t.Fatalf("consul direct route UsesConsul() = false, want true")
	}
	if route.Target.NodeID != target.NodeID || route.Target.Address != target.Address {
		t.Fatalf("route target = %#v, want %#v", route.Target, target)
	}
}

func TestLegacyRoutesAreCompatibilityPaths(t *testing.T) {
	for name, route := range map[string]ExecutionRoute{
		"grpc":  LegacyGRPCRoute(RuntimeTarget{NodeID: "node-a"}),
		"proxy": LegacyProxyRoute(RuntimeTarget{Address: "127.0.0.1:4000"}),
	} {
		if route.Direct() {
			t.Fatalf("%s route Direct() = true, want false", name)
		}
		if !route.UsesConsul() {
			t.Fatalf("%s route UsesConsul() = false, want true", name)
		}
		if !route.CompatibilityPath() {
			t.Fatalf("%s route CompatibilityPath() = false, want true", name)
		}
	}
}

func TestExecutionRequestDefaultsToDirectQIABRoute(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})

	if !request.Route.Direct() {
		t.Fatalf("request route Direct() = false, want true")
	}
	if !request.Route.Target.Local {
		t.Fatalf("request route local = false, want true")
	}
}

func TestExecutionRequestWithRouteDoesNotMutateOriginal(t *testing.T) {
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	routed := request.WithRoute(LegacyGRPCRoute(RuntimeTarget{NodeID: "node-a"}))

	if !request.Route.Direct() {
		t.Fatalf("original route Direct() = false after WithRoute, want true")
	}
	if !routed.Route.CompatibilityPath() {
		t.Fatalf("routed request CompatibilityPath() = false, want true")
	}
	if routed.Route.Target.NodeID != "node-a" {
		t.Fatalf("routed target node = %q, want node-a", routed.Route.Target.NodeID)
	}
}

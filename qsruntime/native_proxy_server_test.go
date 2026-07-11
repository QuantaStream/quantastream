package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestNativeProxyServerDefaultsToDirectQIABRoute(t *testing.T) {
	server := NewNativeProxyServer(NativeProxyRuntime{}, NativeProxyServerConfig{})

	if server.Route.Path != ExecutionPathDirectQIAB || !server.Route.Target.Local {
		t.Fatalf("route = %#v, want local direct QIAB", server.Route)
	}
}

func TestNativeProxyServerReportsNotReady(t *testing.T) {
	server := NewNativeProxyServer(NativeProxyRuntime{}, NativeProxyServerConfig{})

	if server.Ready() {
		t.Fatal("server should not be ready with an empty runtime")
	}
	result, err := server.ExecuteSQL(context.Background(), "select o_orderkey from orders", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL err = %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocking not-ready diagnostic", result.Diagnostics)
	}
	inspection := server.InspectSQL("select o_orderkey from orders", qsbridge.ExecutionOptions{})
	if !inspection.Diagnostics.BlocksNative() {
		t.Fatalf("inspection diagnostics = %#v, want blocking not-ready diagnostic", inspection.Diagnostics)
	}
}

func TestNativeProxyServerDelegatesExecutionAndInspection(t *testing.T) {
	var gotRequest ExecutionRequest
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{Count: 13}, nil
	})}
	server := NewNativeProxyServer(runtime, NativeProxyServerConfig{})

	if !server.Ready() {
		t.Fatal("server should be ready when the owned runtime is ready")
	}
	result, err := server.ExecuteSQL(context.Background(), "select o_orderkey from orders where o_orderkey >= 100", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if result.Runtime.Count != 13 {
		t.Fatalf("count = %d, want 13", result.Runtime.Count)
	}
	if gotRequest.FragmentCount() != 1 || gotRequest.ProjectionCount() != 1 {
		t.Fatalf("runtime request fragments/projections = %d/%d, want 1/1", gotRequest.FragmentCount(), gotRequest.ProjectionCount())
	}

	inspection := server.InspectSQL("select o_orderkey from orders where o_orderkey >= 100", qsbridge.ExecutionOptions{})
	if !inspection.Supported() {
		t.Fatalf("inspection diagnostics = %#v / runtime %#v, want supported", inspection.Diagnostics, inspection.Runtime.Diagnostics)
	}
	if inspection.Runtime.Route.Path != ExecutionPathDirectQIAB {
		t.Fatalf("inspection route = %#v, want direct QIAB", inspection.Runtime.Route)
	}
}

func TestNativeProxyFrontDoorDefaultsToMySQLQIABWithoutClaimingWireReadiness(t *testing.T) {
	frontDoor := NewNativeProxyFrontDoor(NativeProxyRuntime{}, NativeProxyFrontDoorConfig{})
	summary := frontDoor.Summary()

	if summary.Protocol != qsbridge.ProtocolMySQL || summary.Driver != "mysql-wire" {
		t.Fatalf("summary protocol = %#v, want MySQL wire defaults", summary)
	}
	if summary.BindAddress != "127.0.0.1" || summary.Port != 4000 {
		t.Fatalf("summary address = %#v, want 127.0.0.1:4000", summary)
	}
	if summary.Route != ExecutionPathDirectQIAB {
		t.Fatalf("summary route = %#v, want direct QIAB", summary)
	}
	if summary.Ready || summary.WireReady || summary.RuntimeReady {
		t.Fatalf("summary readiness = %#v, want scaffold not network-ready", summary)
	}
	if summary.NextStep == "" {
		t.Fatalf("summary = %#v, want next step", summary)
	}
}

func TestNativeProxyFrontDoorSeparatesRuntimeReadinessFromPacketIO(t *testing.T) {
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Count: 1}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{})
	summary := frontDoor.Summary()

	if !summary.RuntimeReady {
		t.Fatalf("summary = %#v, want runtime ready", summary)
	}
	if summary.WireReady || summary.Ready {
		t.Fatalf("summary = %#v, packet IO should still block network readiness", summary)
	}
}

func TestNativeProxyFrontDoorCanRepresentMountedWireAdapter(t *testing.T) {
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		return ExecutionResult{Count: 1}, nil
	})}
	frontDoor := NewNativeProxyFrontDoor(runtime, NativeProxyFrontDoorConfig{PacketIOReady: true, BindAddress: "0.0.0.0", Port: 4400})
	summary := frontDoor.Summary()

	if !summary.Ready || !summary.RuntimeReady || !summary.WireReady {
		t.Fatalf("summary = %#v, want fully ready front door when packet IO is mounted", summary)
	}
	if summary.BindAddress != "0.0.0.0" || summary.Port != 4400 {
		t.Fatalf("summary address = %#v, want configured address", summary)
	}
	if summary.NextStep != "" {
		t.Fatalf("summary = %#v, want no next step when ready", summary)
	}
}

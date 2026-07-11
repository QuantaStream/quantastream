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

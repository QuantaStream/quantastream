package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestNativeProxyRuntimeConfigAppliesQIABDefaults(t *testing.T) {
	config := NativeProxyRuntimeConfig{}.WithDefaults()

	if config.DefaultSchema != "quanta" {
		t.Fatalf("DefaultSchema = %q, want quanta", config.DefaultSchema)
	}
	if config.CatalogVersion != qsbridge.CatalogVersion("native-proxy-runtime") {
		t.Fatalf("CatalogVersion = %q", config.CatalogVersion)
	}
	if config.Direct.ServicePort != DefaultDirectServicePort {
		t.Fatalf("ServicePort = %d, want %d", config.Direct.ServicePort, DefaultDirectServicePort)
	}
	if config.Profile.Implementation != LegacyDirectRuntimeProfile().Implementation {
		t.Fatalf("Profile.Implementation = %q", config.Profile.Implementation)
	}
}

func TestNativeProxyRuntimeExecutesThroughSQLRuntime(t *testing.T) {
	var gotRequest ExecutionRequest
	runtime := NativeProxyRuntime{Runtime: newTestSQLRuntimeWithDirect(t, func(ctx context.Context, request ExecutionRequest) (ExecutionResult, error) {
		gotRequest = request
		return ExecutionResult{Count: 7}, nil
	})}

	if !runtime.Ready() {
		t.Fatal("native proxy runtime should report ready SQL runtime")
	}
	result, err := runtime.ExecuteSQL(context.Background(), "select o_orderkey from orders where o_orderkey >= 100", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQL failed: %v", err)
	}
	if !result.Supported() {
		t.Fatalf("result diagnostics = %#v / runtime %#v, want supported", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if gotRequest.FragmentCount() != 1 || gotRequest.ProjectionCount() != 1 {
		t.Fatalf("runtime request fragments/projections = %d/%d, want 1/1", gotRequest.FragmentCount(), gotRequest.ProjectionCount())
	}
	if result.Runtime.Count != 7 {
		t.Fatalf("count = %d, want 7", result.Runtime.Count)
	}
}

func TestNewNativeProxyRuntimeFromSourceReportsMissingSource(t *testing.T) {
	_, diagnostics, err := NewNativeProxyRuntimeFromSource(context.Background(), nil, core.NewTableCacheStruct(), NativeProxyRuntimeConfig{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocking diagnostic", diagnostics)
	}
}

package qsruntime

import (
	"context"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestUnsupportedProjectionMaterializationKernelReportsBoundary(t *testing.T) {
	result, err := ExecuteProjectionMaterializationKernel(context.Background(), nil, ProjectionMaterializationKernelRequest{
		ID: "projection_materialization",
		Requests: []qsbridge.QuantaMaterializationRequest{
			{Index: "orders"},
			{Index: "customer"},
		},
	})
	if err != nil {
		t.Fatalf("ExecuteProjectionMaterializationKernel error = %v", err)
	}
	if result.ID != "projection_materialization" {
		t.Fatalf("result id = %q, want request id", result.ID)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want unsupported boundary", result.Diagnostics)
	}
	if !strings.Contains(result.Diagnostics[0].Message, "requests=2") {
		t.Fatalf("diagnostic message = %q, want request count", result.Diagnostics[0].Message)
	}
}

func TestProjectionMaterializationKernelAdapterDelegatesConfiguredKernel(t *testing.T) {
	called := false
	adapter := ProjectionMaterializationKernelAdapter{
		Kernel: qsruntimeMaterializationKernelFunc(func(_ context.Context, request qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
			called = true
			if request.ID != "projection_materialization" || len(request.Requests) != 1 {
				t.Fatalf("request = %#v, want forwarded grouped materialization request", request)
			}
			return qsbridge.ProjectionMaterializationKernelResult{ID: "delegated"}, nil
		}),
	}

	result, err := adapter.MaterializeProjectionBatches(context.Background(), ProjectionMaterializationKernelRequest{
		ID:       "projection_materialization",
		Requests: []qsbridge.QuantaMaterializationRequest{{Index: "orders"}},
	})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if !called || result.ID != "delegated" {
		t.Fatalf("called/result = %v/%#v, want delegated kernel result", called, result)
	}
}

func TestProjectionMaterializerKernelAdapterGroupsMaterializerRequests(t *testing.T) {
	field := qsbridge.QuantaProjectionField{Index: "orders", Field: "o_orderkey", Visible: true}
	calls := 0
	materializer := ProjectionMaterializerFunc(func(_ context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
		calls++
		return qsbridge.QuantaProjectedRowSet{
			Index:   request.Index,
			Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			ProjectionVectors: []qsbridge.QuantaProjectionVector{{
				Field:  field,
				Values: []qsbridge.ResultCell{{Kind: qsbridge.ValueInt, Value: int64(1000 + request.Rownums[0])}},
			}},
		}, nil, nil
	})

	result, err := ProjectionMaterializerKernelAdapter{Materializer: materializer}.MaterializeProjectionBatches(context.Background(), ProjectionMaterializationKernelRequest{
		ID:          "projection_materialization",
		ProbePrefix: "projection_materialization_",
		Requests: []qsbridge.QuantaMaterializationRequest{
			{Index: "orders", Rownums: []qsbridge.QuantaRownum{1}, ProjectionFields: []qsbridge.QuantaProjectionField{field}, DependencyID: "one"},
			{Index: "orders", Rownums: []qsbridge.QuantaRownum{2}, ProjectionFields: []qsbridge.QuantaProjectionField{field}, DependencyID: "two"},
		},
	})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want two materializer calls", calls)
	}
	if len(result.Results) != 2 || result.Results[1].ID != "two" {
		t.Fatalf("results = %#v, want grouped materializer responses", result.Results)
	}
	if got := result.Results[1].RowSet.ProjectionVectors[0].Values[0].Value; got != int64(1002) {
		t.Fatalf("second result value = %#v, want 1002", got)
	}
	if len(result.Probes) == 0 {
		t.Fatalf("probes = %#v, want grouped materialization probe", result.Probes)
	}
}

func TestDirectBitmapRuntimePrefersProjectionMaterializationKernel(t *testing.T) {
	calledKernel := false
	runtime := DirectBitmapRuntime{
		Materializer: ProjectionMaterializerFunc(func(context.Context, qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			t.Fatalf("legacy materializer should not be selected when grouped kernel is present")
			return qsbridge.QuantaProjectedRowSet{}, nil, nil
		}),
		Materialization: qsruntimeMaterializationKernelFunc(func(_ context.Context, request qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
			calledKernel = true
			return qsbridge.ProjectionMaterializationKernelResult{ID: request.ID}, nil
		}),
	}
	kernel := runtime.projectionMaterializationKernel()
	if kernel == nil {
		t.Fatalf("projection materialization kernel is nil")
	}
	result, err := kernel.MaterializeProjectionBatches(context.Background(), ProjectionMaterializationKernelRequest{ID: "projection_materialization"})
	if err != nil {
		t.Fatalf("MaterializeProjectionBatches error = %v", err)
	}
	if !calledKernel || result.ID != "projection_materialization" {
		t.Fatalf("called/result = %v/%#v, want configured grouped kernel", calledKernel, result)
	}
}

type qsruntimeMaterializationKernelFunc func(context.Context, qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error)

func (f qsruntimeMaterializationKernelFunc) MaterializeProjectionBatches(ctx context.Context, request qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
	return f(ctx, request)
}

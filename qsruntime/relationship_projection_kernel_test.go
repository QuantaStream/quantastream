package qsruntime

import (
	"context"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestUnsupportedRelationshipVectorProjectionKernelReportsBoundary(t *testing.T) {
	kernel := UnsupportedRelationshipVectorProjectionKernel{}
	result, err := kernel.LoadRelationshipVectorProjections(context.Background(), RelationshipVectorProjectionKernelRequest{
		ID: "relationship_vector_projection",
		Reads: []RelationshipVectorProjectionRead{
			{ID: "relationship_vector.1.l.l_partkey.p.p_partkey"},
			{ID: "relationship_vector.2.o.o_custkey.c.c_custkey"},
		},
	})
	if err != nil {
		t.Fatalf("LoadRelationshipVectorProjections error = %v", err)
	}
	if result.ID != "relationship_vector_projection" {
		t.Fatalf("result id = %q, want request id", result.ID)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want unsupported boundary", result.Diagnostics)
	}
	if !strings.Contains(result.Diagnostics[0].Message, "reads=2") {
		t.Fatalf("diagnostic message = %q, want read count", result.Diagnostics[0].Message)
	}
}

func TestRelationshipVectorProjectionKernelAliasAcceptsQSBridgeKernel(t *testing.T) {
	var kernel RelationshipVectorProjectionKernel = qsruntimeProjectionKernelFunc(func(context.Context, qsbridge.RelationshipVectorProjectionKernelRequest) (qsbridge.RelationshipVectorProjectionKernelResult, error) {
		return qsbridge.RelationshipVectorProjectionKernelResult{ID: "ok"}, nil
	})

	result, err := kernel.LoadRelationshipVectorProjections(context.Background(), RelationshipVectorProjectionKernelRequest{})
	if err != nil {
		t.Fatalf("LoadRelationshipVectorProjections error = %v", err)
	}
	if result.ID != "ok" {
		t.Fatalf("result = %#v, want alias-compatible qsbridge kernel result", result)
	}
}

func TestRelationshipVectorProjectionKernelAdapterDefaultsToUnsupportedBoundary(t *testing.T) {
	result, err := ExecuteRelationshipVectorProjectionKernel(context.Background(), nil, RelationshipVectorProjectionKernelRequest{
		ID:    "projector_vectors",
		Reads: []RelationshipVectorProjectionRead{{ID: "relationship_vector.1"}},
	})
	if err != nil {
		t.Fatalf("ExecuteRelationshipVectorProjectionKernel error = %v", err)
	}
	if result.ID != "projector_vectors" || !result.Diagnostics.BlocksNative() {
		t.Fatalf("result = %#v, want unsupported boundary with request id", result)
	}
}

func TestRelationshipVectorProjectionKernelAdapterDelegatesConfiguredKernel(t *testing.T) {
	called := false
	kernel := RelationshipVectorProjectionKernelAdapter{
		Kernel: qsruntimeProjectionKernelFunc(func(_ context.Context, request qsbridge.RelationshipVectorProjectionKernelRequest) (qsbridge.RelationshipVectorProjectionKernelResult, error) {
			called = true
			if request.ID != "projector_vectors" || len(request.Reads) != 1 {
				t.Fatalf("request = %#v, want forwarded projector vector request", request)
			}
			return qsbridge.RelationshipVectorProjectionKernelResult{ID: "delegated"}, nil
		}),
	}

	result, err := kernel.LoadRelationshipVectorProjections(context.Background(), RelationshipVectorProjectionKernelRequest{
		ID:    "projector_vectors",
		Reads: []RelationshipVectorProjectionRead{{ID: "relationship_vector.1"}},
	})
	if err != nil {
		t.Fatalf("LoadRelationshipVectorProjections error = %v", err)
	}
	if !called || result.ID != "delegated" {
		t.Fatalf("called/result = %v/%#v, want delegated kernel result", called, result)
	}
}

type qsruntimeProjectionKernelFunc func(context.Context, qsbridge.RelationshipVectorProjectionKernelRequest) (qsbridge.RelationshipVectorProjectionKernelResult, error)

func (f qsruntimeProjectionKernelFunc) LoadRelationshipVectorProjections(ctx context.Context, request qsbridge.RelationshipVectorProjectionKernelRequest) (qsbridge.RelationshipVectorProjectionKernelResult, error) {
	return f(ctx, request)
}

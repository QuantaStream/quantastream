package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestPlanRelationshipJoinsMarksVectorEdgesNotWiredYet(t *testing.T) {
	plan := PlanRelationshipJoins([]qsbridge.JoinEdge{{
		Left:  qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "c"}, Name: "cust_id"},
		Right: qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "o"}, Name: "cust_id"},
		Kind:  qsbridge.JoinKindInner,
		Encoding: qsbridge.RelationshipEncodingProfile{
			Kind: qsbridge.RelationshipEncodingVector,
			Capabilities: qsbridge.RelationshipCapabilities{
				qsbridge.RelationshipCapabilityParentLookup,
				qsbridge.RelationshipCapabilityJoinReduction,
			},
		},
	}})

	if !plan.NeedsRelationshipVectorExecution() {
		t.Fatalf("NeedsRelationshipVectorExecution = false, want true")
	}
	edge, ok := plan.FirstRelationshipVectorEdge()
	if !ok {
		t.Fatalf("FirstRelationshipVectorEdge ok = false, want true")
	}
	if edge.ExecutionKind != RelationshipJoinExecutionVector {
		t.Fatalf("execution kind = %q, want %q", edge.ExecutionKind, RelationshipJoinExecutionVector)
	}
	if edge.Intent != RelationshipJoinOperationReduce {
		t.Fatalf("intent = %q, want %q", edge.Intent, RelationshipJoinOperationReduce)
	}
	if edge.Status != ExecutionJoinStatusNotWiredYet {
		t.Fatalf("status = %q, want %q", edge.Status, ExecutionJoinStatusNotWiredYet)
	}
	if edge.EncodingKind != qsbridge.RelationshipEncodingVector {
		t.Fatalf("encoding kind = %q, want %q", edge.EncodingKind, qsbridge.RelationshipEncodingVector)
	}
	if !edge.Capabilities.Has(qsbridge.RelationshipCapabilityJoinReduction) {
		t.Fatalf("capabilities = %#v, want join reduction", edge.Capabilities)
	}
}

func TestUnsupportedRelationshipVectorJoinExecutorReturnsBoundaryDiagnostic(t *testing.T) {
	plan := PlanRelationshipJoins([]qsbridge.JoinEdge{{
		Left:     qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "c"}, Name: "cust_id"},
		Right:    qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "o"}, Name: "cust_id"},
		Encoding: qsbridge.RelationshipEncodingProfile{Kind: qsbridge.RelationshipEncodingVector},
	}})

	result, err := UnsupportedRelationshipVectorJoinExecutor{}.ExecuteRelationshipVectorJoin(nil, ExecutionRequest{}, plan.VectorRequest("orders_qa"))
	if err != nil {
		t.Fatalf("ExecuteRelationshipVectorJoin error = %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want blocker", result.Diagnostics)
	}
	if result.Diagnostics[0].Code != qsbridge.DiagnosticUnsupportedJoin {
		t.Fatalf("diagnostic code = %s, want %s", result.Diagnostics[0].Code, qsbridge.DiagnosticUnsupportedJoin)
	}
}

func TestRelationshipJoinPlanBuildsVectorAdapterRequest(t *testing.T) {
	plan := PlanRelationshipJoins([]qsbridge.JoinEdge{{
		Left:  qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "c"}, Name: "cust_id"},
		Right: qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "o"}, Name: "cust_id"},
		Kind:  qsbridge.JoinKindLeftOuter,
		Encoding: qsbridge.RelationshipEncodingProfile{
			Kind: qsbridge.RelationshipEncodingVector,
			Capabilities: qsbridge.RelationshipCapabilities{
				qsbridge.RelationshipCapabilityParentLookup,
				qsbridge.RelationshipCapabilityNullExtension,
			},
		},
	}})

	request := plan.VectorRequest("customers_qa")
	if request.RootIndex != "customers_qa" {
		t.Fatalf("root index = %q, want customers_qa", request.RootIndex)
	}
	if request.EdgeCount() != 1 {
		t.Fatalf("edge count = %d, want 1", request.EdgeCount())
	}
	edge, ok := request.FirstEdge()
	if !ok {
		t.Fatalf("first edge ok = false, want true")
	}
	if edge.SQLKind != qsbridge.JoinKindLeftOuter {
		t.Fatalf("sql kind = %q, want left outer", edge.SQLKind)
	}
	if edge.Intent != RelationshipJoinOperationNullExtend {
		t.Fatalf("intent = %q, want null_extend", edge.Intent)
	}
	if !edge.Capabilities.Has(qsbridge.RelationshipCapabilityNullExtension) {
		t.Fatalf("capabilities = %#v, want null extension", edge.Capabilities)
	}
}

func TestRelationshipJoinPlanClassifiesOperationIntents(t *testing.T) {
	tests := []struct {
		name         string
		joinKind     qsbridge.JoinKind
		capabilities qsbridge.RelationshipCapabilities
		want         RelationshipJoinOperationIntent
	}{
		{
			name:         "inner reduction",
			joinKind:     qsbridge.JoinKindInner,
			capabilities: qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityJoinReduction},
			want:         RelationshipJoinOperationReduce,
		},
		{
			name:         "outer null extension",
			joinKind:     qsbridge.JoinKindLeftOuter,
			capabilities: qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityNullExtension},
			want:         RelationshipJoinOperationNullExtend,
		},
		{
			name:         "semi membership",
			joinKind:     qsbridge.JoinKindInner,
			capabilities: qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilitySemiJoin},
			want:         RelationshipJoinOperationSemi,
		},
		{
			name:         "anti membership",
			joinKind:     qsbridge.JoinKindInner,
			capabilities: qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityAntiJoinDifference},
			want:         RelationshipJoinOperationAnti,
		},
		{
			name:         "child expansion",
			joinKind:     qsbridge.JoinKindInner,
			capabilities: qsbridge.RelationshipCapabilities{qsbridge.RelationshipCapabilityChildExpansion},
			want:         RelationshipJoinOperationExpand,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := PlanRelationshipJoins([]qsbridge.JoinEdge{{
				Left:  qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "l"}, Name: "id"},
				Right: qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "r"}, Name: "id"},
				Kind:  tt.joinKind,
				Encoding: qsbridge.RelationshipEncodingProfile{
					Kind:         qsbridge.RelationshipEncodingVector,
					Capabilities: tt.capabilities,
				},
			}})
			edge, ok := plan.FirstRelationshipVectorEdge()
			if !ok {
				t.Fatalf("first edge ok = false, want true")
			}
			if edge.Intent != tt.want {
				t.Fatalf("intent = %q, want %q", edge.Intent, tt.want)
			}
		})
	}
}

func TestRelationshipVectorResultCarriesJoinedRowsForLateMaterialization(t *testing.T) {
	result := RelationshipVectorJoinResult{
		RootIndex: "orders_qa",
		JoinedRows: []RelationshipJoinedRow{{
			Left:       10,
			Right:      20,
			SourceEdge: 0,
		}, {
			Left:      11,
			NullRight: true,
		}},
	}
	request := RelationshipJoinMaterializationRequest{
		RootIndex:        result.RootIndex,
		JoinedRows:       result.JoinedRows,
		ProjectionFields: []qsbridge.QuantaProjectionField{{Index: "orders_qa", Field: "order_id"}},
	}

	if request.RootIndex != "orders_qa" {
		t.Fatalf("root index = %q, want orders_qa", request.RootIndex)
	}
	if len(request.JoinedRows) != 2 {
		t.Fatalf("joined rows = %d, want 2", len(request.JoinedRows))
	}
	if !request.JoinedRows[1].NullRight {
		t.Fatalf("second joined row should preserve null-right outer row")
	}
	if len(request.ProjectionFields) != 1 {
		t.Fatalf("projection fields = %d, want 1", len(request.ProjectionFields))
	}
}

func TestFixtureRelationshipVectorJoinExecutorRecordsRequest(t *testing.T) {
	executor := &FixtureRelationshipVectorJoinExecutor{}
	plan := PlanRelationshipJoins([]qsbridge.JoinEdge{{
		Left:     qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "c"}, Name: "cust_id"},
		Right:    qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "o"}, Name: "cust_id"},
		Encoding: qsbridge.RelationshipEncodingProfile{Kind: qsbridge.RelationshipEncodingVector},
	}})

	result, err := executor.ExecuteRelationshipVectorJoin(nil, ExecutionRequest{}, plan.VectorRequest("orders_qa"))
	if err != nil {
		t.Fatalf("ExecuteRelationshipVectorJoin error = %v", err)
	}
	if executor.LastRequest.RootIndex != "orders_qa" {
		t.Fatalf("recorded root index = %q, want orders_qa", executor.LastRequest.RootIndex)
	}
	if executor.LastRequest.EdgeCount() != 1 {
		t.Fatalf("recorded edge count = %d, want 1", executor.LastRequest.EdgeCount())
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want unsupported boundary", result.Diagnostics)
	}
}

func TestExecuteRelationshipVectorKernelDispatchesByIntent(t *testing.T) {
	tests := []struct {
		name   string
		intent RelationshipJoinOperationIntent
		want   string
	}{
		{name: "reduce", intent: RelationshipJoinOperationReduce, want: "reduce"},
		{name: "expand", intent: RelationshipJoinOperationExpand, want: "expand"},
		{name: "semi", intent: RelationshipJoinOperationSemi, want: "semi"},
		{name: "anti", intent: RelationshipJoinOperationAnti, want: "anti"},
		{name: "null extend", intent: RelationshipJoinOperationNullExtend, want: "null_extend"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kernel := &recordingRelationshipVectorKernel{}
			vector := RelationshipVectorJoinRequest{
				RootIndex: "orders_qa",
				Edges: []RelationshipJoinPlanEdge{{
					Left:          qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "l"}, Name: "id"},
					Right:         qsbridge.FieldRef{Table: qsbridge.TableInstance{Alias: "r"}, Name: "id"},
					ExecutionKind: RelationshipJoinExecutionVector,
					Intent:        tt.intent,
				}},
			}

			result, err := ExecuteRelationshipVectorKernel(nil, kernel, vector)
			if err != nil {
				t.Fatalf("ExecuteRelationshipVectorKernel error = %v", err)
			}
			if kernel.called != tt.want {
				t.Fatalf("called = %q, want %q", kernel.called, tt.want)
			}
			if result.RootIndex != "orders_qa" {
				t.Fatalf("root index = %q, want orders_qa", result.RootIndex)
			}
		})
	}
}

type recordingRelationshipVectorKernel struct {
	called string
}

func (k *recordingRelationshipVectorKernel) ReduceRelatedFoundSets(_ context.Context, request RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error) {
	k.called = "reduce"
	return RelationshipVectorKernelResult{RootIndex: request.RootIndex}, nil
}

func (k *recordingRelationshipVectorKernel) ExpandRelatedRows(_ context.Context, request RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error) {
	k.called = "expand"
	return RelationshipVectorKernelResult{RootIndex: request.RootIndex}, nil
}

func (k *recordingRelationshipVectorKernel) SemiJoinRelatedRows(_ context.Context, request RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error) {
	k.called = "semi"
	return RelationshipVectorKernelResult{RootIndex: request.RootIndex}, nil
}

func (k *recordingRelationshipVectorKernel) AntiJoinRelatedRows(_ context.Context, request RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error) {
	k.called = "anti"
	return RelationshipVectorKernelResult{RootIndex: request.RootIndex}, nil
}

func (k *recordingRelationshipVectorKernel) NullExtendRelatedRows(_ context.Context, request RelationshipVectorKernelRequest) (RelationshipVectorKernelResult, error) {
	k.called = "null_extend"
	return RelationshipVectorKernelResult{RootIndex: request.RootIndex}, nil
}

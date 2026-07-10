package qsfixture

import (
	"context"
	"reflect"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestRelationshipVectorProjectionFixtureKernelLoadsProjectorPlanReads(t *testing.T) {
	relationship := qsbridge.RelationshipJoinPlan{Edges: []qsbridge.RelationshipJoinPlanEdge{{
		Left:          qsbridge.FieldRef{Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"}, Name: "l_partkey"},
		Right:         qsbridge.FieldRef{Table: qsbridge.TableInstance{Table: "part", Alias: "p"}, Name: "p_partkey"},
		SQLKind:       qsbridge.JoinKindInner,
		ExecutionKind: qsbridge.RelationshipJoinExecutionVector,
		Intent:        qsbridge.RelationshipJoinOperationReduce,
		EncodingKind:  qsbridge.RelationshipEncodingVector,
	}}}
	plan := qsbridge.BuildProjectorKernelPlan(qsbridge.ProjectorKernelSpec{
		RelationshipPlan: relationship,
	})
	request := plan.RelationshipVectorProjectionKernelRequest(map[string]qsbridge.RownumDomainSet{
		"l": {
			Domain:  qsbridge.RownumDomain{Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"}, Role: "l"},
			Rownums: []qsbridge.QuantaRownum{10, 20, 30},
		},
	})
	kernel := NewRelationshipVectorProjectionFixtureKernel(map[string]map[qsbridge.QuantaRownum][]qsbridge.QuantaRownum{
		"relationship_vector.1.l.l_partkey.p.p_partkey": {
			10: {100},
			20: {100, 200},
			30: {300},
		},
	})

	result, err := kernel.LoadRelationshipVectorProjections(context.Background(), request)
	if err != nil {
		t.Fatalf("LoadRelationshipVectorProjections error = %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if result.ID != "relationship_vector_projection" || len(result.Results) != 1 {
		t.Fatalf("result = %#v, want one projector vector result", result)
	}
	projected := result.Results[0]
	if projected.Translation.Direction != qsbridge.RownumDomainTranslationChildToParent || projected.Output.Domain.Name() != "p" {
		t.Fatalf("projected translation/output = %#v", projected)
	}
	if !reflect.DeepEqual(projected.Output.Rownums, []qsbridge.QuantaRownum{100, 200, 300}) {
		t.Fatalf("output rownums = %#v, want deduped parent rownums", projected.Output.Rownums)
	}
	if len(result.Probes) != 2 || len(projected.Probes) != 1 {
		t.Fatalf("probes = %#v/%#v, want kernel and read probes", result.Probes, projected.Probes)
	}
}

func TestRelationshipVectorProjectionFixtureKernelReportsMissingVector(t *testing.T) {
	kernel := NewRelationshipVectorProjectionFixtureKernel(nil)
	result, err := kernel.LoadRelationshipVectorProjections(context.Background(), qsbridge.RelationshipVectorProjectionKernelRequest{
		ID: "relationship_vector_projection",
		Reads: []qsbridge.RelationshipVectorProjectionRead{{
			ID: "relationship_vector.1.l.l_partkey.p.p_partkey",
		}},
	})
	if err != nil {
		t.Fatalf("LoadRelationshipVectorProjections error = %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want missing fixture vector diagnostic", result.Diagnostics)
	}
}

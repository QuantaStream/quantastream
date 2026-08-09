package qsruntime

import "testing"

func TestDirectPhysicalExecutionTierProjectsRuntimeCapabilities(t *testing.T) {
	sessions := DirectSessionProviderFunc(nil)
	filterAdapter := DirectBitmapFilterAdapterFunc(nil)
	materialization := FallbackProjectionMaterializationKernel{}
	projectionReader := LegacyDirectProjectionBSIReader{}
	sameRowComparison := LegacyDirectSameRowBSIComparisonKernel{}
	relationshipReader := &LegacyDirectRelationshipVectorReader{}
	relationshipJoins := LegacyDirectRelationshipVectorJoinExecutor{}

	runtime := (DirectPhysicalExecutionTier{
		Sessions:            sessions,
		Adapter:             BitmapQueryResultAdapter{},
		FilterAdapter:       filterAdapter,
		Materialization:     materialization,
		ProjectionBSIReader: projectionReader,
		SameRowComparison:   sameRowComparison,
		RelationshipReader:  relationshipReader,
		RelationshipJoins:   relationshipJoins,
	}).Runtime()

	if runtime.Sessions == nil {
		t.Fatalf("runtime sessions = nil")
	}
	if runtime.FilterAdapter == nil {
		t.Fatalf("runtime filter adapter = nil")
	}
	if runtime.projectionMaterializationKernel() == nil {
		t.Fatalf("runtime materialization kernel = nil")
	}
	if runtime.ProjectionBSIReader == nil {
		t.Fatalf("runtime projection BSI reader = nil")
	}
	if runtime.SameRowComparison == nil {
		t.Fatalf("runtime same-row comparison = nil")
	}
	if runtime.RelationshipReader == nil {
		t.Fatalf("runtime relationship reader = nil")
	}
	if runtime.RelationshipJoins == nil {
		t.Fatalf("runtime relationship joins = nil")
	}
}

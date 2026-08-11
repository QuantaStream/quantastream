package qsruntime

// DirectPhysicalExecutionTier groups the physical execution capabilities that
// a deployment mode contributes to the direct bitmap runtime.
//
// Query planning and SQL execution should depend on these capabilities, not on
// deployment-mode names such as inabox-standard or inabox-direct. A mode should
// only differ in how it constructs sessions, readers, materializers, and vector
// kernels.
type DirectPhysicalExecutionTier struct {
	Sessions            DirectSessionProvider
	Adapter             BitmapQueryResultAdapter
	Materializer        ProjectionMaterializer
	Materialization     ProjectionMaterializationKernel
	ProjectionBSIReader NativeProjectionBSIReader
	SameRowComparison   SameRowComparisonKernel
	RelationshipJoins   RelationshipVectorJoinExecutor
	RelationshipReader  RelationshipVectorReader
	FilterAdapter       DirectBitmapFilterAdapter
	SiblingDiversity    RelationshipSiblingDiversityReader
	BitmapGroupCounts   BitmapGroupCountReader
}

// Runtime projects the physical capability bundle into the executable runtime.
func (t DirectPhysicalExecutionTier) Runtime() DirectBitmapRuntime {
	return DirectBitmapRuntime{
		Sessions:            t.Sessions,
		Adapter:             t.Adapter,
		Materializer:        t.Materializer,
		Materialization:     t.Materialization,
		ProjectionBSIReader: t.ProjectionBSIReader,
		SameRowComparison:   t.SameRowComparison,
		RelationshipJoins:   t.RelationshipJoins,
		RelationshipReader:  t.RelationshipReader,
		FilterAdapter:       t.FilterAdapter,
		SiblingDiversity:    t.SiblingDiversity,
		BitmapGroupCounts:   t.BitmapGroupCounts,
	}
}

package qsbridge

import (
	"context"
	"reflect"
	"testing"
)

func TestBuildProjectorKernelPlanStagesRelationshipMaterialization(t *testing.T) {
	relationship := RelationshipJoinPlan{Edges: []RelationshipJoinPlanEdge{{
		Left:          FieldRef{Table: TableInstance{Table: "lineitem", Alias: "l"}, Name: "l_suppkey"},
		Right:         FieldRef{Table: TableInstance{Table: "supplier", Alias: "s"}, Name: "s_suppkey"},
		SQLKind:       JoinKindInner,
		ExecutionKind: RelationshipJoinExecutionVector,
		Intent:        RelationshipJoinOperationReduce,
		EncodingKind:  RelationshipEncodingVector,
	}}}
	spec := ProjectorKernelSpec{
		Driver: "lineitem",
		Candidates: []QuantaCandidateSet{{
			Index:   "lineitem",
			Rownums: []QuantaRownum{10, 20, 30},
		}},
		RelationshipPlan: relationship,
		ProjectionFields: []QuantaProjectionField{
			{Index: "lineitem", Field: "l_extendedprice", Visible: true},
			{Index: "supplier", Field: "s_name", Visible: true},
		},
		RehydrationFields: map[string]bool{"supplier.s_name": true},
		BatchSize:         128,
		FromEpochMillis:   1000,
		ToEpochMillis:     2000,
	}

	plan := BuildProjectorKernelPlan(spec)

	wantStages := []ProjectorKernelStageKind{
		ProjectorKernelSeedCandidates,
		ProjectorKernelLoadRelationshipVectors,
		ProjectorKernelBatchCandidates,
		ProjectorKernelMaterializeFields,
		ProjectorKernelRehydrateValues,
		ProjectorKernelAssembleRows,
	}
	if !reflect.DeepEqual(plan.StageKinds(), wantStages) {
		t.Fatalf("stages = %#v, want %#v", plan.StageKinds(), wantStages)
	}
	if !plan.NeedsRelationshipVectors() {
		t.Fatalf("expected relationship vector dependency")
	}
	if !plan.NeedsRehydration() {
		t.Fatalf("expected rehydration dependency")
	}
	if len(plan.Relationship) != 1 || plan.Relationship[0].Child.Name != "l_suppkey" || plan.Relationship[0].Parent.Name != "s_suppkey" {
		t.Fatalf("relationship dependencies = %#v", plan.Relationship)
	}
	if plan.Relationship[0].ID != "relationship_vector.1.l.l_suppkey.s.s_suppkey" || plan.Relationship[0].ProbeName != "relationship_vector_1_l_l_suppkey_s_s_suppkey" {
		t.Fatalf("relationship identity = %#v, want stable vector dependency identity", plan.Relationship[0])
	}
	if plan.Relationship[0].Translation.Direction != RownumDomainTranslationChildToParent || plan.Relationship[0].Translation.From.Name() != "l" || plan.Relationship[0].Translation.To.Name() != "s" {
		t.Fatalf("relationship translation = %#v, want l -> s child-to-parent", plan.Relationship[0].Translation)
	}
	if !plan.Relationship[0].Coverage.VerifyCoverage ||
		plan.Relationship[0].Coverage.ExpectStatus != RelationshipVectorProjectionCoverageComplete ||
		plan.Relationship[0].Coverage.RecoveryPolicy != RelationshipVectorProjectionRecoveryBroadenAndIntersect {
		t.Fatalf("relationship coverage plan = %#v, want verified complete coverage with broaden-and-intersect recovery", plan.Relationship[0].Coverage)
	}
}

func TestProjectorKernelMaterializationRequestsPreserveFoundSetWindow(t *testing.T) {
	spec := ProjectorKernelSpec{
		Candidates: []QuantaCandidateSet{{
			Index:        "orders",
			LogicalShard: ShardID("orders/1996-01"),
			Replica:      ReplicaID("node-a"),
			Rownums:      []QuantaRownum{1, 2},
		}},
		ProjectionFields: []QuantaProjectionField{{Index: "orders", Field: "o_orderdate", Visible: true}},
		FromEpochMillis:  111,
		ToEpochMillis:    222,
		BatchSize:        512,
		BatchIntent:      ProjectionBatchIntentTimeToFirstByte,
	}

	plan := BuildProjectorKernelPlan(spec)
	requests := plan.MaterializationRequests()

	if len(requests) != 1 {
		t.Fatalf("requests = %#v, want one", requests)
	}
	request := requests[0]
	if request.Index != "orders" || request.FromEpochMillis != 111 || request.ToEpochMillis != 222 {
		t.Fatalf("request identity/window = %#v", request)
	}
	if request.DependencyID != "materialization.1.orders" || request.ProbePrefix != "materialization_1_orders_" {
		t.Fatalf("request dependency/probe identity = %#v", request)
	}
	if request.Batch.Size != 512 || request.Batch.Sequence != 0 || !request.Batch.Final || !request.Batch.TimeToFirstByte() {
		t.Fatalf("request batch = %#v, want final time-to-first-byte batch", request.Batch)
	}
	if !reflect.DeepEqual(request.Rownums, []QuantaRownum{1, 2}) {
		t.Fatalf("request rownums = %#v", request.Rownums)
	}
	if request.ProjectionCount() != 1 || request.ProjectionFields[0].Field != "o_orderdate" {
		t.Fatalf("request projection fields = %#v", request.ProjectionFields)
	}
}

func TestProjectorKernelRelationshipVectorProjectionKernelRequestUsesPlanDependencies(t *testing.T) {
	relationship := RelationshipJoinPlan{Edges: []RelationshipJoinPlanEdge{{
		Left:          FieldRef{Table: TableInstance{Table: "lineitem", Alias: "l"}, Name: "l_partkey"},
		Right:         FieldRef{Table: TableInstance{Table: "part", Alias: "p"}, Name: "p_partkey"},
		SQLKind:       JoinKindInner,
		ExecutionKind: RelationshipJoinExecutionVector,
		Intent:        RelationshipJoinOperationReduce,
		EncodingKind:  RelationshipEncodingVector,
	}}}
	plan := BuildProjectorKernelPlan(ProjectorKernelSpec{
		RelationshipPlan: relationship,
		FromEpochMillis:  111,
		ToEpochMillis:    222,
	})

	request := plan.RelationshipVectorProjectionKernelRequest(map[string]RownumDomainSet{
		"l": {
			Domain: RownumDomain{Table: TableInstance{Table: "lineitem", Alias: "l"}, Role: "l"},
			Rownums: []QuantaRownum{
				10,
				20,
			},
		},
	})

	if request.ID != "relationship_vector_projection" || request.ProbePrefix != "relationship_vector_projection_" {
		t.Fatalf("request identity = %#v", request)
	}
	if len(request.Reads) != 1 {
		t.Fatalf("reads = %#v, want one", request.Reads)
	}
	read := request.Reads[0]
	if read.ID != "relationship_vector.1.l.l_partkey.p.p_partkey" || read.ProbePrefix != "relationship_vector_1_l_l_partkey_p_p_partkey_" {
		t.Fatalf("read identity = %#v", read)
	}
	if read.Translation.Direction != RownumDomainTranslationChildToParent || read.Input.Domain.Name() != "l" || read.OutputDomain.Name() != "p" {
		t.Fatalf("read translation/input/output = %#v", read)
	}
	if !reflect.DeepEqual(read.Input.Rownums, []QuantaRownum{10, 20}) {
		t.Fatalf("input rownums = %#v, want [10 20]", read.Input.Rownums)
	}
	if read.FromEpochMillis != 111 || read.ToEpochMillis != 222 || !read.Cacheable {
		t.Fatalf("read window/cache = %#v", read)
	}
	if read.CoveragePlan.RecoveryPolicy != RelationshipVectorProjectionRecoveryBroadenAndIntersect ||
		read.CoveragePlan.ExpectStatus != RelationshipVectorProjectionCoverageComplete ||
		!read.CoveragePlan.VerifyCoverage {
		t.Fatalf("read coverage plan = %#v, want verified complete coverage policy", read.CoveragePlan)
	}
}

func TestProjectorKernelMaterializationRequestPlansExposeStableDependencies(t *testing.T) {
	spec := ProjectorKernelSpec{
		Driver: "orders",
		Candidates: []QuantaCandidateSet{
			{Index: "orders", Rownums: []QuantaRownum{1, 2}},
			{Index: "customer", Rownums: []QuantaRownum{10, 20}},
		},
		ProjectionFields: []QuantaProjectionField{
			{Index: "orders", Field: "o_totalprice", Visible: true},
			{Index: "customer", Field: "c_name", Visible: true},
		},
		RehydrationFields: map[string]bool{"customer.c_name": true},
	}

	plan := BuildProjectorKernelPlan(spec)
	requests := plan.MaterializationRequestPlans()

	if len(requests) != 2 {
		t.Fatalf("request plans = %#v, want two grouped requests", requests)
	}
	if requests[0].ID != "materialization.1.orders" || requests[0].ProbePrefix != "materialization_1_orders_" {
		t.Fatalf("first request = %#v, want stable orders identity", requests[0])
	}
	if requests[0].Dependencies[0].ID != "materialize.orders.o_totalprice" || requests[0].Dependencies[0].ProbeName != "materialize_orders_o_totalprice" {
		t.Fatalf("first dependency = %#v, want stable orders dependency", requests[0].Dependencies[0])
	}
	if requests[1].ID != "materialization.2.customer" || requests[1].ProbePrefix != "materialization_2_customer_" {
		t.Fatalf("second request = %#v, want stable customer identity", requests[1])
	}
	if !requests[1].Dependencies[0].Rehydrates {
		t.Fatalf("customer dependency = %#v, want rehydration marker", requests[1].Dependencies[0])
	}
	if !requests[1].Request.Batch.Final {
		t.Fatalf("final request batch = %#v, want final marker", requests[1].Request.Batch)
	}
}

func TestProjectorKernelPlanClonesCandidateRownums(t *testing.T) {
	candidates := []QuantaCandidateSet{{Index: "customer", Rownums: []QuantaRownum{1}}}
	plan := BuildProjectorKernelPlan(ProjectorKernelSpec{Candidates: candidates})
	candidates[0].Rownums[0] = 99

	driver, ok := plan.DriverCandidate()
	if !ok {
		t.Fatalf("driver candidate not found")
	}
	if got := driver.Rownums[0]; got != 1 {
		t.Fatalf("driver rownum = %d, want cloned value 1", got)
	}
}

func TestProjectorKernelCandidateBatchesExposeProjectorNextWindow(t *testing.T) {
	plan := BuildProjectorKernelPlan(ProjectorKernelSpec{
		Candidates: []QuantaCandidateSet{{
			Index:   "lineitem",
			Rownums: []QuantaRownum{10, 20, 30, 40, 50},
		}},
		BatchSize:   2,
		BatchIntent: ProjectionBatchIntentTimeToFirstByte,
	})

	batches := plan.CandidateBatches()

	if len(batches) != 3 {
		t.Fatalf("batches = %#v, want three", batches)
	}
	if batches[0].ID != "candidate_batch.1.lineitem" || batches[0].ProbePrefix != "candidate_batch_1_lineitem_" {
		t.Fatalf("first batch identity = %#v", batches[0])
	}
	if !reflect.DeepEqual(batches[0].Set.Rownums, []QuantaRownum{10, 20}) ||
		!reflect.DeepEqual(batches[1].Set.Rownums, []QuantaRownum{30, 40}) ||
		!reflect.DeepEqual(batches[2].Set.Rownums, []QuantaRownum{50}) {
		t.Fatalf("batch rownums = %#v", batches)
	}
	if batches[0].Batch.Sequence != 0 || batches[1].Batch.Sequence != 1 || batches[2].Batch.Sequence != 2 {
		t.Fatalf("batch sequences = %#v", batches)
	}
	if batches[0].Batch.Final || batches[1].Batch.Final || !batches[2].Batch.Final {
		t.Fatalf("final markers = %#v, want only last final", batches)
	}
	if !batches[0].Batch.TimeToFirstByte() || batches[0].Batch.Size != 2 {
		t.Fatalf("batch policy = %#v, want time-to-first-byte size 2", batches[0].Batch)
	}
}

func TestProjectorKernelCandidateBatchesCopyRownumWindows(t *testing.T) {
	plan := BuildProjectorKernelPlan(ProjectorKernelSpec{
		Candidates: []QuantaCandidateSet{{Index: "orders", Rownums: []QuantaRownum{1, 2}}},
		BatchSize:  1,
	})

	batches := plan.CandidateBatches()
	batches[0].Set.Rownums[0] = 99

	again := plan.CandidateBatches()
	if got := again[0].Set.Rownums[0]; got != 1 {
		t.Fatalf("batch rownum = %d, want copied value 1", got)
	}
}

func TestProjectorKernelMaterializationBatchRequestsUseCandidateWindows(t *testing.T) {
	plan := BuildProjectorKernelPlan(ProjectorKernelSpec{
		Candidates: []QuantaCandidateSet{{
			Index:        "orders",
			LogicalShard: ShardID("orders/1996-01"),
			Replica:      ReplicaID("node-a"),
			Rownums:      []QuantaRownum{1, 2, 3},
		}},
		ProjectionFields: []QuantaProjectionField{{Index: "orders", Field: "o_totalprice", Visible: true}},
		FromEpochMillis:  111,
		ToEpochMillis:    222,
		BatchSize:        2,
		BatchIntent:      ProjectionBatchIntentTimeToFirstByte,
	})

	requests := plan.MaterializationBatchRequestPlans()

	if len(requests) != 2 {
		t.Fatalf("batch request plans = %#v, want two", requests)
	}
	if requests[0].ID != "materialization_batch.1.orders" || requests[0].ProbePrefix != "materialization_batch_1_orders_" {
		t.Fatalf("first batch request identity = %#v", requests[0])
	}
	if !reflect.DeepEqual(requests[0].Request.Rownums, []QuantaRownum{1, 2}) ||
		!reflect.DeepEqual(requests[1].Request.Rownums, []QuantaRownum{3}) {
		t.Fatalf("request rownums = %#v", requests)
	}
	if requests[0].Request.Batch.Sequence != 0 || requests[0].Request.Batch.Final ||
		requests[1].Request.Batch.Sequence != 1 || !requests[1].Request.Batch.Final {
		t.Fatalf("request batches = %#v, want propagated candidate batch metadata", requests)
	}
	if !requests[0].Request.Batch.TimeToFirstByte() || requests[0].Request.FromEpochMillis != 111 || requests[0].Request.ToEpochMillis != 222 {
		t.Fatalf("request policy/window = %#v", requests[0].Request)
	}
	if requests[0].Dependencies[0].ID != "materialize.orders.o_totalprice" {
		t.Fatalf("dependencies = %#v, want materialization dependency copied", requests[0].Dependencies)
	}
}

func TestProjectorKernelExecutionPlanSketchesOrderedRuntimeHandoffs(t *testing.T) {
	lineitem := TableInstance{Table: "lineitem", Alias: "l"}
	orders := TableInstance{Table: "orders", Alias: "o"}
	relationship := RelationshipJoinPlan{Edges: []RelationshipJoinPlanEdge{{
		Left:          FieldRef{Table: lineitem, Name: "l_orderkey"},
		Right:         FieldRef{Table: orders, Name: "o_orderkey"},
		SQLKind:       JoinKindInner,
		ExecutionKind: RelationshipJoinExecutionVector,
		Intent:        RelationshipJoinOperationReduce,
		EncodingKind:  RelationshipEncodingVector,
	}}}
	plan := BuildProjectorKernelPlan(ProjectorKernelSpec{
		Driver: "l",
		Candidates: []QuantaCandidateSet{{
			Index:   "l",
			Rownums: []QuantaRownum{1, 2, 3},
		}},
		RelationshipPlan:    relationship,
		ProjectionFields:    []QuantaProjectionField{{Index: "l", Field: "l_quantity", Visible: true}},
		RehydrationFields:   map[string]bool{"l.l_quantity": true},
		BatchSize:           2,
		BatchIntent:         ProjectionBatchIntentTimeToFirstByte,
		RequiresAggregation: true,
		RequiresRanking:     true,
	})

	execution := plan.ExecutionPlan(map[string]RownumDomainSet{
		"l": {Domain: RownumDomain{Table: lineitem, Role: "l"}, Rownums: []QuantaRownum{1, 2, 3}},
	})

	wantStages := []ProjectorKernelStageKind{
		ProjectorKernelSeedCandidates,
		ProjectorKernelLoadRelationshipVectors,
		ProjectorKernelBatchCandidates,
		ProjectorKernelMaterializeFields,
		ProjectorKernelRehydrateValues,
		ProjectorKernelAssembleRows,
		ProjectorKernelAggregateRows,
		ProjectorKernelRankRows,
	}
	gotStages := make([]ProjectorKernelStageKind, 0, len(execution.Stages))
	for _, stage := range execution.Stages {
		gotStages = append(gotStages, stage.Kind)
	}
	if !reflect.DeepEqual(gotStages, wantStages) {
		t.Fatalf("execution stages = %#v, want %#v", gotStages, wantStages)
	}
	if execution.Driver != "l" {
		t.Fatalf("driver = %q, want l", execution.Driver)
	}
	if execution.Stages[1].RelationshipVectorProjection.ID != "relationship_vector_projection" ||
		len(execution.Stages[1].RelationshipVectorProjection.Reads) != 1 {
		t.Fatalf("relationship vector step = %#v", execution.Stages[1])
	}
	if len(execution.Stages[2].CandidateBatches) != 2 || !execution.Stages[2].CandidateBatches[1].Batch.Final {
		t.Fatalf("candidate batch step = %#v, want two batches with final marker", execution.Stages[2])
	}
	if len(execution.Stages[3].Materialization) != 2 ||
		execution.Stages[3].Materialization[0].Request.Batch.Sequence != 0 ||
		execution.Stages[3].Materialization[1].Request.Batch.Sequence != 1 {
		t.Fatalf("materialization step = %#v, want batch-aware materialization requests", execution.Stages[3])
	}
	if execution.Stages[3].MaterializationKernel.ID != "projection_materialization" ||
		execution.Stages[3].MaterializationKernel.RequestCount() != 2 {
		t.Fatalf("materialization kernel request = %#v, want grouped projector materialization handoff", execution.Stages[3].MaterializationKernel)
	}
	if execution.Stages[0].ID != "projector.seed_candidates" ||
		execution.Stages[1].ProbePrefix != "projector_load_relationship_vectors_" {
		t.Fatalf("stage identities = %#v", execution.Stages[:2])
	}
}

func TestExecuteProjectorKernelPlanRunsMaterializationAndAssembly(t *testing.T) {
	field := QuantaProjectionField{Index: "orders", Field: "o_orderkey", Visible: true}
	plan := BuildProjectorKernelPlan(ProjectorKernelSpec{
		Driver: "orders",
		Candidates: []QuantaCandidateSet{{
			Index:   "orders",
			Rownums: []QuantaRownum{7, 8},
		}},
		ProjectionFields: []QuantaProjectionField{field},
		BatchSize:        1,
		BatchIntent:      ProjectionBatchIntentTimeToFirstByte,
	})
	var requestCount int
	runtime := ProjectorKernelRuntime{
		Materialization: projectorMaterializationKernelFunc(func(_ context.Context, request ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error) {
			requestCount = request.RequestCount()
			results := make([]ProjectionMaterializationResult, 0, len(request.Requests))
			for _, materializationRequest := range request.Requests {
				value := int64(1000 + materializationRequest.Rownums[0])
				results = append(results, ProjectionMaterializationResult{
					ID:      materializationRequest.DependencyID,
					Request: materializationRequest,
					RowSet: QuantaProjectedRowSet{
						Index:   materializationRequest.Index,
						Rownums: append([]QuantaRownum(nil), materializationRequest.Rownums...),
						ProjectionVectors: []QuantaProjectionVector{{
							Field:  field,
							Values: []ResultCell{{Kind: ValueInt, Value: value}},
						}},
					},
					Probes: []ProjectionProbe{{Section: "materialization", Name: materializationRequest.ProbePrefix + "fake", Value: "1"}},
				})
			}
			return ProjectionMaterializationKernelResult{
				ID:      request.ID,
				Results: results,
				Probes:  []ProjectionProbe{{Section: "materialization", Name: request.ProbePrefix + "request_count", Value: "2"}},
			}, nil
		}),
	}

	result, err := ExecuteProjectorKernelPlan(context.Background(), plan, runtime, nil)
	if err != nil {
		t.Fatalf("ExecuteProjectorKernelPlan error = %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if requestCount != 2 {
		t.Fatalf("materialization request count = %d, want 2", requestCount)
	}
	if result.RowSet.CandidateCount() != 2 || result.RowSet.ProjectionCount() != 1 {
		t.Fatalf("rowset = %#v, want assembled two-row rowset", result.RowSet)
	}
	if got := result.RowSet.ProjectionVectors[0].Values[1].Value; got != int64(1008) {
		t.Fatalf("second assembled value = %#v, want 1008", got)
	}
	if len(result.Chunks) != 2 || !result.Chunks[1].Final {
		t.Fatalf("chunks = %#v, want two assembled chunks", result.Chunks)
	}
	if len(result.Probes) == 0 {
		t.Fatalf("probes = %#v, want materialization/assembly probes", result.Probes)
	}
}

func TestExecuteProjectorKernelPlanReportsMissingMaterializationKernel(t *testing.T) {
	plan := BuildProjectorKernelPlan(ProjectorKernelSpec{
		Candidates:          []QuantaCandidateSet{{Index: "orders", Rownums: []QuantaRownum{7}}},
		ProjectionFields:    []QuantaProjectionField{{Index: "orders", Field: "o_orderkey", Visible: true}},
		BatchSize:           1,
		RequiresRanking:     false,
		RequiresAggregation: false,
	})

	result, err := ExecuteProjectorKernelPlan(context.Background(), plan, ProjectorKernelRuntime{}, nil)
	if err != nil {
		t.Fatalf("ExecuteProjectorKernelPlan error = %v", err)
	}
	if !result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want missing materialization diagnostic", result.Diagnostics)
	}
	if got := result.Diagnostics.Codes()[0]; got != DiagnosticUnsupportedSQL {
		t.Fatalf("diagnostic code = %q, want %q", got, DiagnosticUnsupportedSQL)
	}
}

type projectorMaterializationKernelFunc func(context.Context, ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error)

func (f projectorMaterializationKernelFunc) MaterializeProjectionBatches(ctx context.Context, request ProjectionMaterializationKernelRequest) (ProjectionMaterializationKernelResult, error) {
	return f(ctx, request)
}

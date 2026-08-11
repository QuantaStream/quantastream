package qsruntime

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestDirectBitmapRuntimeAppliesCorrelatedSiblingMembershipWithRawBSIVectors(t *testing.T) {
	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}

	for _, test := range []struct {
		name string
		kind qsbridge.MembershipKind
		want int64
	}{
		{name: "semi", kind: qsbridge.MembershipSemi, want: 2},
		{name: "anti", kind: qsbridge.MembershipAnti, want: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := fakeMembershipProjectionBSIReader{
				Values: map[string]map[uint64]int64{
					"l_orderkey": {1: 10, 2: 10, 3: 20, 4: 30},
					"l_suppkey":  {1: 1, 2: 2, 3: 5, 4: 9},
				},
			}
			orderKeys := reader.Values["l_orderkey"]
			runtime := DirectBitmapRuntime{
				Adapter:             BitmapQueryResultAdapter{},
				ProjectionBSIReader: reader,
				Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
					t.Fatalf("correlated sibling BSI fast path should not materialize rows for %s", request.Index)
					return qsbridge.QuantaProjectedRowSet{}, nil, nil
				}),
				Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
					return DirectSessionHandleFunc{
						QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
							values := directBitmapTestBatchEQValues(request.Query.Fragments, "l_orderkey")
							rownums := make([]qsbridge.QuantaRownum, 0, len(orderKeys))
							for _, rownum := range []qsbridge.QuantaRownum{1, 2, 3, 4} {
								if len(values) > 0 {
									if _, ok := values[orderKeys[uint64(rownum)]]; !ok {
										continue
									}
								}
								rownums = append(rownums, rownum)
							}
							return BitmapQueryResult{Success: true, Count: uint64(len(rownums)), Rownums: rownums}, nil, nil
						},
					}, nil, nil
				}),
			}
			request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
				Fragments: []qsbridge.QuantaQueryFragment{{
					Index:     "lineitem",
					Field:     "l_orderkey",
					Operation: qsbridge.QuantaOperationIntersect,
					NullCheck: true,
					Negate:    true,
				}},
			})
			request.Memberships = []qsbridge.MembershipEdge{{
				Left:  l1OrderKey,
				Right: l2OrderKey,
				Kind:  test.kind,
				Legal: true,
				Predicates: []qsbridge.Predicate{
					{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(l2OrderKey), qsbridge.Field(l1OrderKey))},
					{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
				},
			}}
			request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "qualified_rows", Type: qsbridge.DataTypeInt}}

			result, err := runtime.ExecuteDirect(context.Background(), request)
			if err != nil {
				t.Fatalf("execute direct: %v", err)
			}
			if result.Diagnostics.BlocksNative() {
				t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
			}
			if result.RowSet.CandidateCount() != 1 || len(result.RowSet.ProjectionVectors) != 1 || len(result.RowSet.ProjectionVectors[0].Values) != 1 {
				t.Fatalf("rowset = %#v, want one count aggregate cell", result.RowSet)
			}
			if got := result.RowSet.ProjectionVectors[0].Values[0].Value; got != test.want {
				t.Fatalf("count aggregate = %#v, want %d", got, test.want)
			}
			assertExecutionProbeName(t, result.Probes, "direct_bitmap_membership", "correlated_sibling_bsi_fast_path_applied")
			assertExecutionProbeName(t, result.Probes, "direct_bitmap_membership", "correlated_sibling_bsi_value_hydration_elapsed")
		})
	}
}

func TestDirectBitmapRuntimeUsesSiblingDiversityArtifact(t *testing.T) {
	t.Setenv(directBitmapCorrelatedSiblingDiversityArtifactEnv, "1")

	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}

	for _, test := range []struct {
		name string
		kind qsbridge.MembershipKind
		want int64
	}{
		{name: "semi", kind: qsbridge.MembershipSemi, want: 2},
		{name: "anti", kind: qsbridge.MembershipAnti, want: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			reader := &fakeMembershipProjectionBSIValueReader{Values: map[string]map[uint64]int64{}}
			diversityReader := &fakeRelationshipSiblingDiversityReader{
				Result: RelationshipSiblingDiversityReadResult{
					Candidates: qsbridge.QuantaCandidateSet{
						Index:   "lineitem",
						Rownums: []qsbridge.QuantaRownum{1, 2},
					},
					Mode:          "test_diversity",
					Rows:          4,
					Values:        2,
					CandidateRows: 4,
					TargetRows:    2,
					Groups:        3,
					DiverseGroups: 1,
				},
			}
			runtime := DirectBitmapRuntime{
				Adapter:             BitmapQueryResultAdapter{},
				ProjectionBSIReader: reader,
				SiblingDiversity:    diversityReader,
				Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
					t.Fatalf("sibling diversity artifact path should not materialize rows for %s", request.Index)
					return qsbridge.QuantaProjectedRowSet{}, nil, nil
				}),
				Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
					return DirectSessionHandleFunc{
						QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
							return BitmapQueryResult{Success: true, Count: 4, Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4}}, nil, nil
						},
					}, nil, nil
				}),
			}
			request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
				Fragments: []qsbridge.QuantaQueryFragment{{
					Index:     "lineitem",
					Field:     "l_orderkey",
					Operation: qsbridge.QuantaOperationIntersect,
					NullCheck: true,
					Negate:    true,
				}},
			})
			request.Memberships = []qsbridge.MembershipEdge{{
				Left:  l1OrderKey,
				Right: l2OrderKey,
				Kind:  test.kind,
				Legal: true,
				Predicates: []qsbridge.Predicate{
					{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(l2OrderKey), qsbridge.Field(l1OrderKey))},
					{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
				},
			}}
			request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "qualified_rows", Type: qsbridge.DataTypeInt}}

			result, err := runtime.ExecuteDirect(context.Background(), request)
			if err != nil {
				t.Fatalf("execute direct: %v", err)
			}
			if result.Diagnostics.BlocksNative() {
				t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
			}
			if got := result.RowSet.ProjectionVectors[0].Values[0].Value; got != test.want {
				t.Fatalf("count aggregate = %#v, want %d", got, test.want)
			}
			if diversityReader.Calls != 1 {
				t.Fatalf("sibling diversity reader calls = %d, want 1", diversityReader.Calls)
			}
			if reader.RawReads != 0 || reader.ValueReads != 0 {
				t.Fatalf("projection BSI reads = raw %d value %d, want zero", reader.RawReads, reader.ValueReads)
			}
			assertExecutionProbeName(t, result.Probes, "direct_bitmap_membership", "correlated_sibling_bsi_diversity_artifact_applied")
			assertExecutionProbe(t, result.Probes, "direct_bitmap_membership", "correlated_sibling_bsi_diversity_artifact_mode", "test_diversity")
			assertExecutionProbe(t, result.Probes, "optimizer", "correlated_sibling_diversity_choice", "test_diversity")
			assertExecutionProbe(t, result.Probes, "optimizer", "correlated_sibling_diversity_candidate_rows", "4")
			assertExecutionProbe(t, result.Probes, "optimizer", "correlated_sibling_diversity_target_rows", "2")
		})
	}
}

func TestDirectBitmapRuntimeUsesSiblingDiversityArtifactWithQualifiedNames(t *testing.T) {
	t.Setenv(directBitmapCorrelatedSiblingDiversityArtifactEnv, "1")

	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l1.l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l2.l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l1.l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l2.l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}

	reader := &fakeMembershipProjectionBSIValueReader{Values: map[string]map[uint64]int64{}}
	diversityReader := &fakeRelationshipSiblingDiversityReader{
		Result: RelationshipSiblingDiversityReadResult{
			Candidates: qsbridge.QuantaCandidateSet{
				Index:   "lineitem",
				Rownums: []qsbridge.QuantaRownum{1, 2},
			},
			Mode:       "test_diversity",
			TargetRows: 2,
		},
	}
	runtime := DirectBitmapRuntime{
		Adapter:             BitmapQueryResultAdapter{},
		ProjectionBSIReader: reader,
		SiblingDiversity:    diversityReader,
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			t.Fatalf("sibling diversity artifact path should not materialize rows for %s", request.Index)
			return qsbridge.QuantaProjectedRowSet{}, nil, nil
		}),
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Count: 4, Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4}}, nil, nil
				},
			}, nil, nil
		}),
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	request.Memberships = []qsbridge.MembershipEdge{{
		Left:  l1OrderKey,
		Right: l2OrderKey,
		Kind:  qsbridge.MembershipSemi,
		Legal: true,
		Predicates: []qsbridge.Predicate{
			{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(l2OrderKey), qsbridge.Field(l1OrderKey))},
			{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
		},
	}}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "qualified_rows", Type: qsbridge.DataTypeInt}}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if got := result.RowSet.ProjectionVectors[0].Values[0].Value; got != int64(2) {
		t.Fatalf("count aggregate = %#v, want 2", got)
	}
	if diversityReader.Calls != 1 {
		t.Fatalf("sibling diversity reader calls = %d, want 1", diversityReader.Calls)
	}
	if diversityReader.Last.ParentField != "l_orderkey" || diversityReader.Last.ValueField != "l_suppkey" {
		t.Fatalf("diversity read fields = %s/%s, want l_orderkey/l_suppkey", diversityReader.Last.ParentField, diversityReader.Last.ValueField)
	}
	if reader.RawReads != 0 || reader.ValueReads != 0 {
		t.Fatalf("projection BSI reads = raw %d value %d, want zero", reader.RawReads, reader.ValueReads)
	}
	assertExecutionProbe(t, result.Probes, "direct_bitmap_membership", "correlated_sibling_bsi_diversity_artifact_applied", "true")
	assertExecutionProbe(t, result.Probes, "optimizer", "correlated_sibling_diversity_choice", "test_diversity")
}

func TestDirectBitmapCorrelatedSiblingDiversityArtifactDisabledByDefault(t *testing.T) {
	t.Setenv(directBitmapCorrelatedSiblingDiversityArtifactEnv, "")

	if directBitmapCorrelatedSiblingDiversityArtifactEnabled() {
		t.Fatalf("sibling diversity artifact enabled by default, want disabled")
	}
	t.Setenv(directBitmapCorrelatedSiblingDiversityArtifactEnv, "enabled")
	if !directBitmapCorrelatedSiblingDiversityArtifactEnabled() {
		t.Fatalf("sibling diversity artifact disabled with explicit opt-in")
	}
}

func TestDirectBitmapRuntimeReportsSiblingDiversityArtifactSkipReason(t *testing.T) {
	t.Setenv(directBitmapCorrelatedSiblingDiversityArtifactEnv, "1")

	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}

	diversityReader := &fakeRelationshipSiblingDiversityReader{
		Unavailable: true,
		Result: RelationshipSiblingDiversityReadResult{
			Rows:           300000,
			Values:         75000,
			CandidateRows:  190000,
			ProjectionRows: 300000,
			Groups:         68000,
			Reason:         "projection_rows_exceeds_limit",
		},
	}
	runtime := DirectBitmapRuntime{SiblingDiversity: diversityReader}
	membership := qsbridge.MembershipEdge{
		Left:  l1OrderKey,
		Right: l2OrderKey,
		Kind:  qsbridge.MembershipSemi,
		Legal: true,
	}
	comparisons := []directBitmapMembershipBSIComparison{
		{
			Op:    qsbridge.BinaryOpEqual,
			Left:  directBitmapMembershipBSIOperand{Side: directBitmapMembershipBSIOperandRight, Field: l2OrderKey},
			Right: directBitmapMembershipBSIOperand{Side: directBitmapMembershipBSIOperandLeft, Field: l1OrderKey},
		},
		{
			Op:    qsbridge.BinaryOpNotEqual,
			Left:  directBitmapMembershipBSIOperand{Side: directBitmapMembershipBSIOperandRight, Field: l2SuppKey},
			Right: directBitmapMembershipBSIOperand{Side: directBitmapMembershipBSIOperandLeft, Field: l1SuppKey},
		},
	}

	result, probes, handled, diagnostics, err := runtime.directBitmapApplyCorrelatedSiblingDiversityFastPath(
		context.Background(),
		NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}),
		time.Now(),
		BitmapQueryResult{Success: true, Count: 4, Rownums: []qsbridge.QuantaRownum{1, 2, 3, 4}},
		membership,
		nil,
		comparisons,
		"test",
	)
	if err != nil {
		t.Fatalf("diversity fast path error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if handled {
		t.Fatalf("diversity fast path handled = true, want false")
	}
	if result.Count != 4 {
		t.Fatalf("result count = %d, want unchanged fallback result", result.Count)
	}
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_bsi_diversity_artifact_applied", "false")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_bsi_diversity_artifact_reason", "projection_rows_exceeds_limit")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_bsi_diversity_artifact_projection_rows", "300000")
	assertExecutionProbe(t, probes, "optimizer", "correlated_sibling_diversity_choice", "projection_rows_exceeds_limit")
	assertExecutionProbe(t, probes, "optimizer", "correlated_sibling_diversity_projection_rows", "300000")
}

func TestDirectBitmapRuntimeReusesRightBSIVectorsForLargeSiblingDomain(t *testing.T) {
	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}

	rownums := make([]qsbridge.QuantaRownum, 0, directBitmapMembershipMaxDynamicBatchEQValues+2)
	orderKeys := make(map[uint64]int64, directBitmapMembershipMaxDynamicBatchEQValues+2)
	suppKeys := make(map[uint64]int64, directBitmapMembershipMaxDynamicBatchEQValues+2)
	for i := 1; i <= directBitmapMembershipMaxDynamicBatchEQValues+2; i++ {
		rownum := qsbridge.QuantaRownum(i)
		rownums = append(rownums, rownum)
		orderKeys[uint64(rownum)] = int64(1000 + i)
		suppKeys[uint64(rownum)] = 1
	}
	orderKeys[1] = 10
	orderKeys[2] = 10
	suppKeys[1] = 1
	suppKeys[2] = 2
	reader := fakeMembershipProjectionBSIReader{
		Values: map[string]map[uint64]int64{
			"l_orderkey": orderKeys,
			"l_suppkey":  suppKeys,
		},
	}
	runtime := DirectBitmapRuntime{
		Adapter:             BitmapQueryResultAdapter{},
		ProjectionBSIReader: reader,
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			t.Fatalf("correlated sibling BSI right-vector reuse should not materialize rows for %s", request.Index)
			return qsbridge.QuantaProjectedRowSet{}, nil, nil
		}),
	}
	queryCalls := 0
	runtime.Sessions = DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
		return DirectSessionHandleFunc{
			QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
				queryCalls++
				return BitmapQueryResult{Success: true, Count: uint64(len(rownums)), Rownums: append([]qsbridge.QuantaRownum(nil), rownums...)}, nil, nil
			},
		}, nil, nil
	})
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	request.Memberships = []qsbridge.MembershipEdge{{
		Left:  l1OrderKey,
		Right: l2OrderKey,
		Kind:  qsbridge.MembershipSemi,
		Legal: true,
		Predicates: []qsbridge.Predicate{
			{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(l2OrderKey), qsbridge.Field(l1OrderKey))},
			{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
		},
	}}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "qualified_rows", Type: qsbridge.DataTypeInt}}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if got := result.RowSet.ProjectionVectors[0].Values[0].Value; got != int64(2) {
		t.Fatalf("count aggregate = %#v, want 2", got)
	}
	if queryCalls != 1 {
		t.Fatalf("bitmap candidate queries = %d, want 1", queryCalls)
	}
	assertExecutionProbeName(t, result.Probes, "direct_bitmap_membership", "correlated_sibling_bsi_right_vector_reuse")
	assertExecutionProbeName(t, result.Probes, "direct_bitmap_membership", "correlated_sibling_bsi_value_hydration_elapsed")
	assertExecutionProbe(t, result.Probes, "direct_bitmap_membership", "correlated_sibling_bsi_key_mode", "int64")
	assertExecutionProbe(t, result.Probes, "direct_bitmap_membership", "membership_right_candidate_seed_reuse", "true")
}

func TestDirectBitmapCorrelatedSiblingSeedAppliesRightOnlySameRowResiduals(t *testing.T) {
	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2ReceiptDate := qsbridge.FieldRef{Table: l2, Name: "l_receiptdate", PhysicalName: "l_receiptdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime}
	l2CommitDate := qsbridge.FieldRef{Table: l2, Name: "l_commitdate", PhysicalName: "l_commitdate", Type: qsbridge.DataTypeTime, Index: qsbridge.IndexDateTime}

	sameRowCalled := false
	runtime := DirectBitmapRuntime{
		CorrelatedSiblingRightCandidateSeed: &BitmapQueryResult{
			Success: true,
			Count:   3,
			Rownums: []qsbridge.QuantaRownum{10, 11, 12},
		},
		CorrelatedSiblingRightCandidateSeedMode: "test_graph_parent_vector_expansion",
		SameRowComparison: SameRowComparisonKernelFunc(func(_ context.Context, comparison qsbridge.SameRowComparisonRequest) (qsbridge.SameRowComparisonResult, error) {
			sameRowCalled = true
			if got := comparison.Domain.Rownums; len(got) != 3 || got[0] != 10 || got[1] != 11 || got[2] != 12 {
				t.Fatalf("same-row candidates = %#v, want [10 11 12]", got)
			}
			return qsbridge.SameRowComparisonResult{
				ID: comparison.ID,
				Domain: qsbridge.RownumDomainSet{
					Domain:  comparison.Domain.Domain,
					Rownums: []qsbridge.QuantaRownum{10, 12},
				},
			}, nil
		}),
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			valuesByField := map[string]map[qsbridge.QuantaRownum]qsbridge.ResultCell{
				"l_orderkey": {
					1:  {Kind: qsbridge.ValueInt, Value: int64(100)},
					2:  {Kind: qsbridge.ValueInt, Value: int64(200)},
					10: {Kind: qsbridge.ValueInt, Value: int64(100)},
					11: {Kind: qsbridge.ValueInt, Value: int64(100)},
					12: {Kind: qsbridge.ValueInt, Value: int64(200)},
				},
				"l_suppkey": {
					1:  {Kind: qsbridge.ValueInt, Value: int64(7)},
					2:  {Kind: qsbridge.ValueInt, Value: int64(7)},
					10: {Kind: qsbridge.ValueInt, Value: int64(8)},
					11: {Kind: qsbridge.ValueInt, Value: int64(7)},
					12: {Kind: qsbridge.ValueInt, Value: int64(7)},
				},
			}
			rowSet := qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
			}
			for _, field := range request.ProjectionFields {
				name := field.PhysicalName
				if name == "" {
					name = field.Field
				}
				vector := qsbridge.QuantaProjectionVector{Field: field}
				for _, rownum := range request.Rownums {
					vector.Values = append(vector.Values, valuesByField[name][rownum])
				}
				rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
			}
			return rowSet, nil, nil
		}),
	}
	membership := qsbridge.MembershipEdge{
		Left:  l1OrderKey,
		Right: l2OrderKey,
		Kind:  qsbridge.MembershipSemi,
		Legal: true,
		Predicates: []qsbridge.Predicate{
			{
				Placement: qsbridge.PredicateResidualScan,
				Expr:      qsbridge.Binary(qsbridge.BinaryOpGreater, qsbridge.Field(l2ReceiptDate), qsbridge.Field(l2CommitDate)),
			},
			{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
		},
	}

	filtered, probes, diagnostics, err := runtime.directBitmapApplyCorrelatedSiblingMembership(
		context.Background(),
		NewExecutionRequest(qsbridge.QuantaIntermediateQuery{}),
		BitmapQueryResult{Success: true, Count: 2, Rownums: []qsbridge.QuantaRownum{1, 2}},
		membership,
		BitmapQueryResult{},
	)
	if err != nil {
		t.Fatalf("directBitmapApplyCorrelatedSiblingMembership error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !sameRowCalled {
		t.Fatalf("right-only same-row residual was not applied to seeded candidates")
	}
	if got := filtered.Rownums; len(got) != 1 || got[0] != 1 {
		t.Fatalf("filtered rownums = %#v, want [1]", got)
	}
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "membership_right_candidate_seed_reuse", "true")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "membership_right_candidate_residual_count", "1")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "membership_right_candidate_same_row_rows_before", "3")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "membership_right_candidate_same_row_rows_after", "2")
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_right_candidates", "2")
}

func TestDirectBitmapFinishCorrelatedSiblingMembershipBSIFallsBackToStringKeysForBigValues(t *testing.T) {
	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	bigKey := new(big.Int).Lsh(big.NewInt(1), 80)

	leftKey := directBitmapMembershipBSIVector{
		Field:   l1OrderKey,
		Rownums: []qsbridge.QuantaRownum{1},
		Values:  []*big.Int{bigKey},
	}
	rightKey := directBitmapMembershipBSIVector{
		Field:   l2OrderKey,
		Rownums: []qsbridge.QuantaRownum{2},
		Values:  []*big.Int{new(big.Int).Set(bigKey)},
	}
	leftVectors := map[string]directBitmapMembershipBSIVector{
		directBitmapMembershipBSIFieldKey(l1OrderKey): leftKey,
		directBitmapMembershipBSIFieldKey(l1SuppKey): {
			Field:   l1SuppKey,
			Rownums: []qsbridge.QuantaRownum{1},
			Values:  []*big.Int{big.NewInt(10)},
		},
	}
	rightVectors := map[string]directBitmapMembershipBSIVector{
		directBitmapMembershipBSIFieldKey(l2OrderKey): rightKey,
		directBitmapMembershipBSIFieldKey(l2SuppKey): {
			Field:   l2SuppKey,
			Rownums: []qsbridge.QuantaRownum{2},
			Values:  []*big.Int{big.NewInt(20)},
		},
	}
	comparisons := []directBitmapMembershipBSIComparison{
		{
			Op:    qsbridge.BinaryOpEqual,
			Left:  directBitmapMembershipBSIOperand{Side: directBitmapMembershipBSIOperandRight, Field: l2OrderKey},
			Right: directBitmapMembershipBSIOperand{Side: directBitmapMembershipBSIOperandLeft, Field: l1OrderKey},
		},
		{
			Op:    qsbridge.BinaryOpNotEqual,
			Left:  directBitmapMembershipBSIOperand{Side: directBitmapMembershipBSIOperandRight, Field: l2SuppKey},
			Right: directBitmapMembershipBSIOperand{Side: directBitmapMembershipBSIOperandLeft, Field: l1SuppKey},
		},
	}
	filtered, probes, diagnostics := directBitmapFinishCorrelatedSiblingMembershipBSI(
		time.Now(),
		BitmapQueryResult{Success: true, Count: 1, Rownums: []qsbridge.QuantaRownum{1}},
		qsbridge.MembershipEdge{Left: l1OrderKey, Right: l2OrderKey, Kind: qsbridge.MembershipSemi},
		"",
		nil,
		comparisons,
		leftVectors,
		leftKey,
		rightVectors,
		rightKey,
	)
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if len(filtered.Rownums) != 1 || filtered.Rownums[0] != 1 {
		t.Fatalf("filtered rownums = %#v, want [1]", filtered.Rownums)
	}
	assertExecutionProbe(t, probes, "direct_bitmap_membership", "correlated_sibling_bsi_key_mode", "string")
}

func TestDirectBitmapRuntimeReadsCorrelatedSiblingMembershipValuesDirectly(t *testing.T) {
	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}

	rownums := make([]qsbridge.QuantaRownum, 0, directBitmapMembershipMaxDynamicBatchEQValues+2)
	orderKeys := make(map[uint64]int64, directBitmapMembershipMaxDynamicBatchEQValues+2)
	suppKeys := make(map[uint64]int64, directBitmapMembershipMaxDynamicBatchEQValues+2)
	for i := 1; i <= directBitmapMembershipMaxDynamicBatchEQValues+2; i++ {
		rownum := qsbridge.QuantaRownum(i)
		rownums = append(rownums, rownum)
		orderKeys[uint64(rownum)] = int64(1000 + i)
		suppKeys[uint64(rownum)] = 1
	}
	orderKeys[1] = 10
	orderKeys[2] = 10
	suppKeys[1] = 1
	suppKeys[2] = 2
	reader := &fakeMembershipProjectionBSIValueReader{
		Values: map[string]map[uint64]int64{
			"l_orderkey": orderKeys,
			"l_suppkey":  suppKeys,
		},
	}
	runtime := DirectBitmapRuntime{
		Adapter:             BitmapQueryResultAdapter{},
		ProjectionBSIReader: reader,
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			t.Fatalf("correlated sibling BSI value fast path should not materialize rows for %s", request.Index)
			return qsbridge.QuantaProjectedRowSet{}, nil, nil
		}),
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					return BitmapQueryResult{Success: true, Count: uint64(len(rownums)), Rownums: append([]qsbridge.QuantaRownum(nil), rownums...)}, nil, nil
				},
			}, nil, nil
		}),
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	request.Memberships = []qsbridge.MembershipEdge{{
		Left:  l1OrderKey,
		Right: l2OrderKey,
		Kind:  qsbridge.MembershipSemi,
		Legal: true,
		Predicates: []qsbridge.Predicate{
			{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(l2OrderKey), qsbridge.Field(l1OrderKey))},
			{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
		},
	}}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "qualified_rows", Type: qsbridge.DataTypeInt}}

	result, err := runtime.ExecuteDirect(context.Background(), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if got := result.RowSet.ProjectionVectors[0].Values[0].Value; got != int64(2) {
		t.Fatalf("count aggregate = %#v, want 2", got)
	}
	if reader.ValueReads == 0 {
		t.Fatalf("value reader was not used")
	}
	if reader.RawReads != 0 {
		t.Fatalf("raw BSI reads = %d, want 0", reader.RawReads)
	}
	assertExecutionProbeName(t, result.Probes, "direct_bitmap_membership", "correlated_sibling_bsi_value_read_mode")
}

func TestDirectBitmapRuntimeReusesCorrelatedSiblingRightCandidateSuperset(t *testing.T) {
	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l3 := qsbridge.TableInstance{Table: "lineitem", Alias: "l3"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l3OrderKey := qsbridge.FieldRef{Table: l3, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}
	l3SuppKey := qsbridge.FieldRef{Table: l3, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt, Index: qsbridge.IndexBSI}

	reader := &fakeMembershipProjectionBSIValueReader{
		Values: map[string]map[uint64]int64{
			"l_orderkey": {1: 10, 2: 10, 3: 20, 4: 30},
			"l_suppkey":  {1: 1, 2: 2, 3: 5, 4: 9},
		},
	}
	rightCandidateQueries := 0
	runtime := DirectBitmapRuntime{
		Adapter:             BitmapQueryResultAdapter{},
		ProjectionBSIReader: reader,
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			t.Fatalf("correlated sibling BSI cache path should not materialize rows for %s", request.Index)
			return qsbridge.QuantaProjectedRowSet{}, nil, nil
		}),
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					values := directBitmapTestBatchEQValues(request.Query.Fragments, "l_orderkey")
					if len(values) > 0 {
						rightCandidateQueries++
					}
					rownums := []qsbridge.QuantaRownum{}
					for _, rownum := range []qsbridge.QuantaRownum{1, 2, 3, 4} {
						if len(values) > 0 {
							if _, ok := values[reader.Values["l_orderkey"][uint64(rownum)]]; !ok {
								continue
							}
						}
						rownums = append(rownums, rownum)
					}
					return BitmapQueryResult{Success: true, Count: uint64(len(rownums)), Rownums: rownums}, nil, nil
				},
			}, nil, nil
		}),
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	request.Memberships = []qsbridge.MembershipEdge{
		{
			Left:  l1OrderKey,
			Right: l2OrderKey,
			Kind:  qsbridge.MembershipSemi,
			Legal: true,
			Predicates: []qsbridge.Predicate{
				{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(l2OrderKey), qsbridge.Field(l1OrderKey))},
				{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
			},
		},
		{
			Left:  l1OrderKey,
			Right: l3OrderKey,
			Kind:  qsbridge.MembershipAnti,
			Legal: true,
			Predicates: []qsbridge.Predicate{
				{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(l3OrderKey), qsbridge.Field(l1OrderKey))},
				{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l3SuppKey), qsbridge.Field(l1SuppKey))},
			},
		},
	}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "qualified_rows", Type: qsbridge.DataTypeInt}}

	result, err := runtime.ExecuteDirect(WithQueryScratchpad(context.Background()), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if got := result.RowSet.ProjectionVectors[0].Values[0].Value; got != int64(0) {
		t.Fatalf("count aggregate = %#v, want 0", got)
	}
	if rightCandidateQueries != 1 {
		t.Fatalf("right candidate bitmap queries = %d, want 1", rightCandidateQueries)
	}
	assertExecutionProbe(t, result.Probes, "direct_bitmap_membership", "membership_right_candidate_cache_mode", "retained_subset")
}

func TestDirectBitmapRuntimeReusesMaterializedSiblingRightCandidateSuperset(t *testing.T) {
	l1 := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	l2 := qsbridge.TableInstance{Table: "lineitem", Alias: "l2"}
	l3 := qsbridge.TableInstance{Table: "lineitem", Alias: "l3"}
	l1OrderKey := qsbridge.FieldRef{Table: l1, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt}
	l2OrderKey := qsbridge.FieldRef{Table: l2, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt}
	l3OrderKey := qsbridge.FieldRef{Table: l3, Name: "l_orderkey", PhysicalName: "l_orderkey", Type: qsbridge.DataTypeInt}
	l1SuppKey := qsbridge.FieldRef{Table: l1, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt}
	l2SuppKey := qsbridge.FieldRef{Table: l2, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt}
	l3SuppKey := qsbridge.FieldRef{Table: l3, Name: "l_suppkey", PhysicalName: "l_suppkey", Type: qsbridge.DataTypeInt}
	values := map[string]map[uint64]int64{
		"l_orderkey": {1: 10, 2: 10, 3: 20, 4: 30},
		"l_suppkey":  {1: 1, 2: 2, 3: 5, 4: 9},
	}

	rightCandidateQueries := 0
	runtime := DirectBitmapRuntime{
		Adapter: BitmapQueryResultAdapter{},
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			return fakeMembershipProjectedRowSet(request.Index, request.Rownums, request.ProjectionFields, values), nil, nil
		}),
		Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					batchValues := directBitmapTestBatchEQValues(request.Query.Fragments, "l_orderkey")
					if len(batchValues) > 0 {
						rightCandidateQueries++
					}
					rownums := []qsbridge.QuantaRownum{}
					for _, rownum := range []qsbridge.QuantaRownum{1, 2, 3, 4} {
						if len(batchValues) > 0 {
							if _, ok := batchValues[values["l_orderkey"][uint64(rownum)]]; !ok {
								continue
							}
						}
						rownums = append(rownums, rownum)
					}
					return BitmapQueryResult{Success: true, Count: uint64(len(rownums)), Rownums: rownums}, nil, nil
				},
			}, nil, nil
		}),
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{
		Fragments: []qsbridge.QuantaQueryFragment{{
			Index:     "lineitem",
			Field:     "l_orderkey",
			Operation: qsbridge.QuantaOperationIntersect,
			NullCheck: true,
			Negate:    true,
		}},
	})
	request.Memberships = []qsbridge.MembershipEdge{
		{
			Left:  l1OrderKey,
			Right: l2OrderKey,
			Kind:  qsbridge.MembershipSemi,
			Legal: true,
			Predicates: []qsbridge.Predicate{
				{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(l2OrderKey), qsbridge.Field(l1OrderKey))},
				{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l2SuppKey), qsbridge.Field(l1SuppKey))},
			},
		},
		{
			Left:  l1OrderKey,
			Right: l3OrderKey,
			Kind:  qsbridge.MembershipAnti,
			Legal: true,
			Predicates: []qsbridge.Predicate{
				{Expr: qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(l3OrderKey), qsbridge.Field(l1OrderKey))},
				{Expr: qsbridge.Binary(qsbridge.BinaryOpNotEqual, qsbridge.Field(l3SuppKey), qsbridge.Field(l1SuppKey))},
			},
		},
	}
	request.SQLAggregates = []qsbridge.Aggregate{{Function: "count", Alias: "qualified_rows", Type: qsbridge.DataTypeInt}}

	result, err := runtime.ExecuteDirect(WithQueryScratchpad(context.Background()), request)
	if err != nil {
		t.Fatalf("execute direct: %v", err)
	}
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", result.Diagnostics)
	}
	if got := result.RowSet.ProjectionVectors[0].Values[0].Value; got != int64(0) {
		t.Fatalf("count aggregate = %#v, want 0", got)
	}
	if rightCandidateQueries != 1 {
		t.Fatalf("right candidate bitmap queries = %d, want 1", rightCandidateQueries)
	}
	assertExecutionProbe(t, result.Probes, "direct_bitmap_membership", "membership_right_candidate_cache_mode", "retained_subset")
}

type fakeMembershipProjectionBSIReader struct {
	Values map[string]map[uint64]int64
}

type fakeRelationshipSiblingDiversityReader struct {
	Result      RelationshipSiblingDiversityReadResult
	Calls       int
	Last        RelationshipSiblingDiversityReadRequest
	Unavailable bool
}

func (r *fakeRelationshipSiblingDiversityReader) ReadRelationshipSiblingDiversityCandidates(ctx context.Context, request RelationshipSiblingDiversityReadRequest) (RelationshipSiblingDiversityReadResult, qsbridge.DiagnosticSet, bool, error) {
	r.Calls++
	r.Last = request
	if r.Unavailable {
		return r.Result, nil, false, nil
	}
	return r.Result, nil, true, nil
}

func (r fakeMembershipProjectionBSIReader) ReadProjectionBSI(ctx context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	results, diagnostics, err := r.ReadProjectionBSIs(ctx, []NativeProjectionBSIReadRequest{request})
	if len(results) == 0 {
		return NativeProjectionBSIReadResult{}, diagnostics, err
	}
	return results[0], diagnostics, err
}

func (r fakeMembershipProjectionBSIReader) ReadProjectionBSIs(ctx context.Context, requests []NativeProjectionBSIReadRequest) ([]NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	results := make([]NativeProjectionBSIReadResult, 0, len(requests))
	for _, request := range requests {
		bsi := roaring64.NewDefaultBSI()
		values := r.Values[request.PhysicalField]
		for _, rownum := range request.Rownums {
			value, ok := values[uint64(rownum)]
			if !ok {
				continue
			}
			bsi.SetBigValue(uint64(rownum), big.NewInt(value))
		}
		results = append(results, NativeProjectionBSIReadResult{BSI: bsi})
	}
	return results, nil, nil
}

type fakeMembershipProjectionBSIValueReader struct {
	Values     map[string]map[uint64]int64
	RawReads   int
	ValueReads int
}

func (r *fakeMembershipProjectionBSIValueReader) ReadProjectionBSI(ctx context.Context, request NativeProjectionBSIReadRequest) (NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	r.RawReads++
	return (fakeMembershipProjectionBSIReader{Values: r.Values}).ReadProjectionBSI(ctx, request)
}

func (r *fakeMembershipProjectionBSIValueReader) ReadProjectionBSIs(ctx context.Context, requests []NativeProjectionBSIReadRequest) ([]NativeProjectionBSIReadResult, qsbridge.DiagnosticSet, error) {
	r.RawReads++
	return (fakeMembershipProjectionBSIReader{Values: r.Values}).ReadProjectionBSIs(ctx, requests)
}

func (r *fakeMembershipProjectionBSIValueReader) ReadProjectionBSIValues(ctx context.Context, requests []NativeProjectionBSIReadRequest) ([]NativeProjectionBSIValueReadResult, qsbridge.DiagnosticSet, error) {
	r.ValueReads++
	results := make([]NativeProjectionBSIValueReadResult, 0, len(requests))
	for _, request := range requests {
		values := make([]*big.Int, 0, len(request.Rownums))
		fieldValues := r.Values[request.PhysicalField]
		for _, rownum := range request.Rownums {
			value, ok := fieldValues[uint64(rownum)]
			if !ok {
				values = append(values, nil)
				continue
			}
			values = append(values, big.NewInt(value))
		}
		results = append(results, NativeProjectionBSIValueReadResult{Values: values})
	}
	return results, nil, nil
}

func directBitmapTestBatchEQValues(fragments []qsbridge.QuantaQueryFragment, field string) map[int64]struct{} {
	values := make(map[int64]struct{})
	for _, fragment := range fragments {
		if fragment.Field != field || fragment.BSIOp != qsbridge.QuantaBSIOpBatchEQ {
			continue
		}
		for _, value := range fragment.Values {
			if value == nil || !value.IsInt64() {
				continue
			}
			values[value.Int64()] = struct{}{}
		}
	}
	return values
}

func fakeMembershipProjectedRowSet(index string, rownums []qsbridge.QuantaRownum, fields []qsbridge.QuantaProjectionField, values map[string]map[uint64]int64) qsbridge.QuantaProjectedRowSet {
	rowSet := qsbridge.QuantaProjectedRowSet{
		Index:   index,
		Rownums: append([]qsbridge.QuantaRownum(nil), rownums...),
	}
	for _, field := range fields {
		vector := qsbridge.QuantaProjectionVector{
			Field:  field,
			Values: make([]qsbridge.ResultCell, 0, len(rownums)),
		}
		for _, rownum := range rownums {
			value, ok := values[field.Field][uint64(rownum)]
			if !ok {
				vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueNull})
				continue
			}
			vector.Values = append(vector.Values, qsbridge.ResultCell{Kind: qsbridge.ValueInt, Value: value})
		}
		rowSet.ProjectionVectors = append(rowSet.ProjectionVectors, vector)
	}
	return rowSet
}

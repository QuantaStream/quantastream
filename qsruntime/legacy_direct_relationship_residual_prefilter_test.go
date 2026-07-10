package qsruntime

import (
	"context"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestLegacyDirectRelationshipResidualRolePrefilterPlansSingleRoleResidual(t *testing.T) {
	partField := qsbridge.FieldRef{
		Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
		Name:  "p_name",
		Type:  qsbridge.DataTypeString,
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpLike, qsbridge.Field(partField), qsbridge.Literal(qsbridge.ValueString, "%green%")),
		Placement: qsbridge.PredicateResidualScan,
		Scope:     qsbridge.PredicateScopeWhere,
	}}

	plans := legacyDirectRelationshipResidualRolePrefilterPlans(request)

	if len(plans) != 1 {
		t.Fatalf("plans = %#v, want one", plans)
	}
	if plans[0].role != "p" || plans[0].table != "part" {
		t.Fatalf("role/table = %q/%q, want p/part", plans[0].role, plans[0].table)
	}
	if len(plans[0].predicates) != 1 || len(plans[0].fields) != 1 {
		t.Fatalf("predicates/fields = %d/%d, want 1/1", len(plans[0].predicates), len(plans[0].fields))
	}
	if plans[0].fields[0].Role != "p" || plans[0].fields[0].Field != "p_name" {
		t.Fatalf("field = %#v, want role p field p_name", plans[0].fields[0])
	}
}

func TestLegacyDirectRelationshipResidualRolePrefilterPlansSkipMixedRoleResidual(t *testing.T) {
	partField := qsbridge.FieldRef{
		Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
		Name:  "p_name",
		Type:  qsbridge.DataTypeString,
	}
	lineField := qsbridge.FieldRef{
		Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l"},
		Name:  "l_comment",
		Type:  qsbridge.DataTypeString,
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(partField), qsbridge.Field(lineField)),
		Placement: qsbridge.PredicateResidualScan,
		Scope:     qsbridge.PredicateScopeWhere,
	}}

	plans := legacyDirectRelationshipResidualRolePrefilterPlans(request)

	if len(plans) != 0 {
		t.Fatalf("plans = %#v, want none for mixed-role residual", plans)
	}
}

func TestLegacyDirectRelationshipApplyResidualRolePrefiltersShrinksRoleRows(t *testing.T) {
	partField := qsbridge.FieldRef{
		Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
		Name:  "p_name",
		Type:  qsbridge.DataTypeString,
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpLike, qsbridge.Field(partField), qsbridge.Literal(qsbridge.ValueString, "%green%")),
		Placement: qsbridge.PredicateResidualScan,
		Scope:     qsbridge.PredicateScopeWhere,
	}}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"p": []qsbridge.QuantaRownum{1, 2, 3},
		"l": []qsbridge.QuantaRownum{10, 11, 12},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			if request.Index != "part" {
				t.Fatalf("materialization index = %q, want part", request.Index)
			}
			if len(request.ProjectionFields) != 1 || request.ProjectionFields[0].Role != "p" {
				t.Fatalf("projection fields = %#v, want one p role field", request.ProjectionFields)
			}
			return qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field: request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{
						{Kind: qsbridge.ValueString, Value: "green part"},
						{Kind: qsbridge.ValueString, Value: "red part"},
						{Kind: qsbridge.ValueString, Value: "dark green part"},
					},
				}},
			}, nil, nil
		}),
	}

	probes, diagnostics, err := executor.legacyDirectRelationshipApplyResidualRolePrefilters(context.Background(), request, rowsByRole, nil)

	if err != nil {
		t.Fatalf("prefilter: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if got := rowsByRole["p"]; len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("filtered p rows = %#v, want [1 3]", got)
	}
	if got := rowsByRole["l"]; len(got) != 3 {
		t.Fatalf("lineitem rows changed = %#v, want unchanged", got)
	}
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_roles", "1")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_1_rows_before", "3")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_1_rows_after", "2")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_1_rows_removed", "1")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_1_rows_evaluated", "3")
	assertExecutionProbeName(t, probes, "relationship_join", "residual_prefilter_1_materialization_elapsed")
	assertExecutionProbeName(t, probes, "relationship_join", "residual_prefilter_1_evaluation_elapsed")
}

func TestLegacyDirectRelationshipApplyResidualRolePrefiltersDefersSameRoleFieldComparison(t *testing.T) {
	receiptField := qsbridge.FieldRef{
		Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l1"},
		Name:  "l_receiptdate",
		Type:  qsbridge.DataTypeTime,
	}
	commitField := qsbridge.FieldRef{
		Table: qsbridge.TableInstance{Table: "lineitem", Alias: "l1"},
		Name:  "l_commitdate",
		Type:  qsbridge.DataTypeTime,
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpGreater, qsbridge.Field(receiptField), qsbridge.Field(commitField)),
		Placement: qsbridge.PredicateResidualScan,
		Scope:     qsbridge.PredicateScopeWhere,
	}}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"l1": {1, 2, 3},
		"o":  {10, 11},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			t.Fatalf("field-comparison residual prefilter should not materialize before graph reduction")
			return qsbridge.QuantaProjectedRowSet{}, nil, nil
		}),
	}
	edges := []legacyDirectRelationshipEdge{{
		childRole:   "l1",
		childTable:  "lineitem",
		childField:  "l_orderkey",
		parentRole:  "o",
		parentTable: "orders",
		parentField: "o_orderkey",
	}}

	probes, diagnostics, err := executor.legacyDirectRelationshipApplyResidualRolePrefilters(context.Background(), request, rowsByRole, edges)

	if err != nil {
		t.Fatalf("prefilter: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if got := rowsByRole["l1"]; len(got) != 3 {
		t.Fatalf("filtered l1 rows = %#v, want unchanged", got)
	}
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_roles", "1")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_1_field_count", "2")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_1_rows_after", "3")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_1_rows_removed", "0")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_1_rows_evaluated", "0")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_1_placement", "deferred_after_graph_reduction")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_1_placement_reason", "same_role_field_comparison_on_relationship_role")
}

func TestLegacyDirectRelationshipClassifyTupleResidualsSplitsNativeSameRowFromMaterialized(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Predicates = []qsbridge.Predicate{
		{
			Expr: qsbridge.Binary(
				qsbridge.BinaryOpGreater,
				qsbridge.Field(qsbridge.FieldRef{Table: lineitem, Name: "l_receiptdate", Type: qsbridge.DataTypeTime}),
				qsbridge.Field(qsbridge.FieldRef{Table: lineitem, Name: "l_commitdate", Type: qsbridge.DataTypeTime}),
			),
			Placement: qsbridge.PredicateResidualScan,
			Scope:     qsbridge.PredicateScopeWhere,
		},
		{
			Expr:      qsbridge.Binary(qsbridge.BinaryOpLike, qsbridge.Field(qsbridge.FieldRef{Table: part, Name: "p_name", Type: qsbridge.DataTypeString}), qsbridge.Literal(qsbridge.ValueString, "%green%")),
			Placement: qsbridge.PredicateResidualScan,
			Scope:     qsbridge.PredicateScopeWhere,
		},
	}

	classification := legacyDirectRelationshipClassifyTupleResiduals(request)
	filtered := legacyDirectRelationshipRequestWithMaterializedTupleResiduals(request, classification)

	if len(classification.nativeSameRow) != 1 {
		t.Fatalf("native same-row plans = %#v, want one", classification.nativeSameRow)
	}
	if len(classification.materialized) != 1 {
		t.Fatalf("materialized residuals = %#v, want one", classification.materialized)
	}
	if classification.nativeSameRow[0].Domain.Name() != "l1" {
		t.Fatalf("same-row domain = %q, want l1", classification.nativeSameRow[0].Domain.Name())
	}
	filteredFields := qsbridge.FieldRefs(filtered.Predicates[0].Expr)
	if len(filtered.Predicates) != 1 || len(filteredFields) != 1 || filteredFields[0].Name != "p_name" {
		t.Fatalf("filtered predicates = %#v, want only materialized residual", filtered.Predicates)
	}
}

func TestLegacyDirectRelationshipApplyTupleSameRowResidualsUsesNativePolicyAndKeepsMaterializedResidual(t *testing.T) {
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l1"}
	part := qsbridge.TableInstance{Table: "part", Alias: "p"}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Predicates = []qsbridge.Predicate{
		{
			Expr: qsbridge.Binary(
				qsbridge.BinaryOpGreater,
				qsbridge.Field(qsbridge.FieldRef{Table: lineitem, Name: "l_receiptdate", Type: qsbridge.DataTypeTime}),
				qsbridge.Field(qsbridge.FieldRef{Table: lineitem, Name: "l_commitdate", Type: qsbridge.DataTypeTime}),
			),
			Placement: qsbridge.PredicateResidualScan,
			Scope:     qsbridge.PredicateScopeWhere,
		},
		{
			Expr:      qsbridge.Binary(qsbridge.BinaryOpLike, qsbridge.Field(qsbridge.FieldRef{Table: part, Name: "p_name", Type: qsbridge.DataTypeString}), qsbridge.Literal(qsbridge.ValueString, "%green%")),
			Placement: qsbridge.PredicateResidualScan,
			Scope:     qsbridge.PredicateScopeWhere,
		},
	}
	tupleRows := RelationshipTupleRowSet{Rows: []RelationshipTupleRow{
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"l1": 10, "p": 100}},
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"l1": 11, "p": 101}},
		{Rownums: map[qsbridge.TableInstanceID]qsbridge.QuantaRownum{"l1": 12, "p": 102}},
	}}
	alignedRows := map[string][]qsbridge.QuantaRownum{
		"l1": {10, 11, 12},
		"p":  {100, 101, 102},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		SameRowComparison: SameRowComparisonKernelFunc(func(_ context.Context, comparison qsbridge.SameRowComparisonRequest) (qsbridge.SameRowComparisonResult, error) {
			return qsbridge.SameRowComparisonResult{
				ID: comparison.ID,
				Domain: qsbridge.RownumDomainSet{
					Domain:  comparison.Domain.Domain,
					Rownums: []qsbridge.QuantaRownum{11, 12},
				},
			}, nil
		}),
	}

	filteredTuples, filteredAligned, filteredRequest, probes, diagnostics, err := executor.legacyDirectRelationshipApplyTupleSameRowResiduals(context.Background(), request, tupleRows, alignedRows)

	if err != nil {
		t.Fatalf("same-row residuals: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if filteredTuples.CandidateCount() != 2 {
		t.Fatalf("tuple count = %d, want 2", filteredTuples.CandidateCount())
	}
	if got := filteredAligned["l1"]; len(got) != 2 || got[0] != 11 || got[1] != 12 {
		t.Fatalf("filtered l1 rows = %#v, want [11 12]", got)
	}
	if len(filteredRequest.Predicates) != 1 {
		t.Fatalf("filtered request predicates = %#v, want one materialized residual", filteredRequest.Predicates)
	}
	assertExecutionProbe(t, probes, "relationship_join", "graph_tuple_residual_native_same_row", "1")
	assertExecutionProbe(t, probes, "relationship_join", "graph_tuple_residual_materialized", "1")
	assertExecutionProbe(t, probes, "relationship_join", "graph_tuple_same_row_1_policy", "native_same_row")
	assertExecutionProbe(t, probes, "relationship_join", "graph_tuple_same_row_1_policy_reason", "native_keeps_compared_values_unmaterialized")
}

func TestLegacyDirectRelationshipApplyResidualRolePrefiltersKeepsLiteralPredicateEager(t *testing.T) {
	partField := qsbridge.FieldRef{
		Table: qsbridge.TableInstance{Table: "part", Alias: "p"},
		Name:  "p_name",
		Type:  qsbridge.DataTypeString,
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpLike, qsbridge.Field(partField), qsbridge.Literal(qsbridge.ValueString, "%green%")),
		Placement: qsbridge.PredicateResidualScan,
		Scope:     qsbridge.PredicateScopeWhere,
	}}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"p": {1, 2, 3},
		"l": {10, 11, 12},
	}
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materializer: ProjectionMaterializerFunc(func(ctx context.Context, request qsbridge.QuantaMaterializationRequest) (qsbridge.QuantaProjectedRowSet, qsbridge.DiagnosticSet, error) {
			return qsbridge.QuantaProjectedRowSet{
				Index:   request.Index,
				Rownums: append([]qsbridge.QuantaRownum(nil), request.Rownums...),
				ProjectionVectors: []qsbridge.QuantaProjectionVector{{
					Field: request.ProjectionFields[0],
					Values: []qsbridge.ResultCell{
						{Kind: qsbridge.ValueString, Value: "green part"},
						{Kind: qsbridge.ValueString, Value: "red part"},
						{Kind: qsbridge.ValueString, Value: "dark green part"},
					},
				}},
			}, nil, nil
		}),
	}
	edges := []legacyDirectRelationshipEdge{{
		childRole:   "l",
		childTable:  "lineitem",
		childField:  "l_partkey",
		parentRole:  "p",
		parentTable: "part",
		parentField: "p_partkey",
	}}

	probes, diagnostics, err := executor.legacyDirectRelationshipApplyResidualRolePrefilters(context.Background(), request, rowsByRole, edges)

	if err != nil {
		t.Fatalf("prefilter: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if got := rowsByRole["p"]; len(got) != 2 {
		t.Fatalf("filtered p rows = %#v, want eager literal prefilter", got)
	}
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_1_fields", "1")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_1_rows_evaluated", "3")
}

func TestLegacyDirectRelationshipResidualPlacementPolicyRecommendsDeferralAfterLargeGraphReduction(t *testing.T) {
	prefilterProbes := []ExecutionProbe{
		legacyDirectRelationshipProbe("residual_prefilter_roles", "1"),
		legacyDirectRelationshipProbe("residual_prefilter_1_role", "l1"),
		legacyDirectRelationshipProbe("residual_prefilter_1_table", "lineitem"),
		legacyDirectRelationshipProbe("residual_prefilter_1_fields", "2"),
		legacyDirectRelationshipProbe("residual_prefilter_1_rows_before", "100"),
		legacyDirectRelationshipProbe("residual_prefilter_1_rows_after", "50"),
	}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"l1": {1, 2, 3, 4, 5},
	}

	probes := legacyDirectRelationshipResidualPlacementPolicyProbes(prefilterProbes, rowsByRole)

	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_policies", "1")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_1_observe_only", "true")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_1_role", "l1")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_1_current", "eager_prefilter")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_1_recommended", "defer_after_graph_reduction")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_1_reason", "graph_reduced_prefiltered_role_by_order_of_magnitude")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_1_rows_after_eager", "50")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_1_rows_after_graph", "5")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_1_graph_reduction_pct", "90")
}

func TestLegacyDirectRelationshipResidualPlacementPolicyReportsAppliedDeferral(t *testing.T) {
	prefilterProbes := []ExecutionProbe{
		legacyDirectRelationshipProbe("residual_prefilter_roles", "1"),
		legacyDirectRelationshipProbe("residual_prefilter_1_role", "p"),
		legacyDirectRelationshipProbe("residual_prefilter_1_table", "part"),
		legacyDirectRelationshipProbe("residual_prefilter_1_field_count", "1"),
		legacyDirectRelationshipProbe("residual_prefilter_1_rows_before", "2000"),
		legacyDirectRelationshipProbe("residual_prefilter_1_rows_after", "2000"),
		legacyDirectRelationshipProbe("residual_prefilter_1_placement", "deferred_after_graph_reduction"),
	}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"p": {1, 2, 3},
	}

	probes := legacyDirectRelationshipResidualPlacementPolicyProbes(prefilterProbes, rowsByRole)

	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_policies", "1")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_1_current", "deferred_after_graph_reduction")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_1_recommended", "defer_after_graph_reduction")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_1_reason", "internal_deferred_prefilter_applied")
	assertExecutionProbe(t, probes, "relationship_join", "residual_prefilter_placement_1_fields", "1")
}

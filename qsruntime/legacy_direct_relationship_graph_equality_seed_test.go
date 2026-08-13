package qsruntime

import (
	"context"
	"math/big"
	"reflect"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestLegacyDirectRelationshipGraphEqualityRoleSeedEnabled(t *testing.T) {
	t.Setenv("QUANTASTREAM_GRAPH_EQUALITY_ROLE_SEED", "")
	if legacyDirectRelationshipGraphEqualityRoleSeedEnabled() {
		t.Fatalf("default equality role seed enabled = true, want false")
	}
	t.Setenv("QUANTASTREAM_GRAPH_EQUALITY_ROLE_SEED", "0")
	if legacyDirectRelationshipGraphEqualityRoleSeedEnabled() {
		t.Fatalf("disabled equality role seed enabled = true, want false")
	}
	t.Setenv("QUANTASTREAM_GRAPH_EQUALITY_ROLE_SEED", "true")
	if !legacyDirectRelationshipGraphEqualityRoleSeedEnabled() {
		t.Fatalf("true equality role seed enabled = false, want true")
	}
}

func TestLegacyDirectRelationshipGraphEqualityRoleSeedCandidatesUsesReducedSide(t *testing.T) {
	supplier := qsbridge.TableInstance{Table: "supplier", Alias: "s"}
	customer := qsbridge.TableInstance{Table: "customer", Alias: "c"}
	sNation := qsbridge.FieldRef{Table: supplier, Name: "s_nationkey", Type: qsbridge.DataTypeInt}
	cNation := qsbridge.FieldRef{Table: customer, Name: "c_nationkey", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(cNation), qsbridge.Field(sNation)),
		Placement: qsbridge.PredicateResidualJoin,
		Scope:     qsbridge.PredicateScopeWhere,
	}}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"s": {10, 20},
		"c": {1, 2, 3, 4},
	}
	fullDomain := map[string]bool{
		"s": false,
		"c": true,
	}

	candidates := legacyDirectRelationshipGraphEqualityRoleSeedCandidates(request, rowsByRole, fullDomain)
	if len(candidates) != 1 {
		t.Fatalf("candidates = %#v, want one supplier-to-customer seed", candidates)
	}
	got := candidates[0]
	if got.sourceRole != "s" || got.targetRole != "c" || directBitmapFieldPhysicalName(got.sourceField) != "s_nationkey" || directBitmapFieldPhysicalName(got.targetField) != "c_nationkey" {
		t.Fatalf("candidate = %#v, want s.s_nationkey -> c.c_nationkey", got)
	}
}

func TestLegacyDirectRelationshipGraphEqualityRoleSeedCandidatesRequireReducedSource(t *testing.T) {
	supplier := qsbridge.TableInstance{Table: "supplier", Alias: "s"}
	customer := qsbridge.TableInstance{Table: "customer", Alias: "c"}
	sNation := qsbridge.FieldRef{Table: supplier, Name: "s_nationkey", Type: qsbridge.DataTypeInt}
	cNation := qsbridge.FieldRef{Table: customer, Name: "c_nationkey", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(cNation), qsbridge.Field(sNation)),
		Placement: qsbridge.PredicateResidualJoin,
		Scope:     qsbridge.PredicateScopeWhere,
	}}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"s": {10, 20},
		"c": {1, 2, 3, 4},
	}
	fullDomain := map[string]bool{
		"s": true,
		"c": true,
	}

	if candidates := legacyDirectRelationshipGraphEqualityRoleSeedCandidates(request, rowsByRole, fullDomain); len(candidates) != 0 {
		t.Fatalf("candidates = %#v, want none when both sides are full-domain", candidates)
	}
}

func TestLegacyDirectRelationshipApplyGraphEqualityRoleSeedsSkipsEmptyFields(t *testing.T) {
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"o": {10, 20},
		"l": {1, 2, 3, 4},
	}
	fullDomain := map[string]bool{
		"o": false,
		"l": true,
	}

	executor := LegacyDirectRelationshipVectorJoinExecutor{}
	probes, changed, diagnostics, err := executor.legacyDirectRelationshipApplyGraphEqualityRoleSeedsForFieldsWithPrefix(context.Background(), ExecutionRequest{}, nil, rowsByRole, fullDomain, "test_")
	if err != nil {
		t.Fatalf("ApplyGraphEqualityRoleSeedsForFieldsWithPrefix error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if changed || len(probes) != 0 {
		t.Fatalf("changed/probes = %v/%#v, want false/no probes", changed, probes)
	}
}

func TestLegacyDirectRelationshipGraphEqualityRoleSeedCandidatesIgnoreJoinEdgeEqualities(t *testing.T) {
	orders := qsbridge.TableInstance{Table: "orders", Alias: "o"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	oOrderKey := qsbridge.FieldRef{Table: orders, Name: "o_orderkey", Type: qsbridge.DataTypeInt}
	lOrderKey := qsbridge.FieldRef{Table: lineitem, Name: "l_orderkey", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Joins = []qsbridge.JoinEdge{{
		Left:  oOrderKey,
		Right: lOrderKey,
		Kind:  qsbridge.JoinKindInner,
		On: []qsbridge.Predicate{{
			Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(oOrderKey), qsbridge.Field(lOrderKey)),
			Placement: qsbridge.PredicateResidualJoin,
			Scope:     qsbridge.PredicateScopeOn,
		}},
	}}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"o": {10, 20},
		"l": {1, 2, 3, 4},
	}
	fullDomain := map[string]bool{
		"o": false,
		"l": true,
	}

	if candidates := legacyDirectRelationshipGraphEqualityRoleSeedCandidates(request, rowsByRole, fullDomain); len(candidates) != 0 {
		t.Fatalf("candidates = %#v, want none for relationship join edge equality", candidates)
	}
}

func TestLegacyDirectRelationshipGraphEqualityRoleSeedCandidatesIgnoreOnScopePredicates(t *testing.T) {
	orders := qsbridge.TableInstance{Table: "orders", Alias: "o"}
	lineitem := qsbridge.TableInstance{Table: "lineitem", Alias: "l"}
	oOrderKey := qsbridge.FieldRef{Table: orders, Name: "o_orderkey", Type: qsbridge.DataTypeInt}
	lOrderKey := qsbridge.FieldRef{Table: lineitem, Name: "l_orderkey", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(oOrderKey), qsbridge.Field(lOrderKey)),
		Placement: qsbridge.PredicateResidualJoin,
		Scope:     qsbridge.PredicateScopeOn,
	}}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"o": {10, 20},
		"l": {1, 2, 3, 4},
	}
	fullDomain := map[string]bool{
		"o": false,
		"l": true,
	}

	if candidates := legacyDirectRelationshipGraphEqualityRoleSeedCandidates(request, rowsByRole, fullDomain); len(candidates) != 0 {
		t.Fatalf("candidates = %#v, want none for ON-scope residual predicate", candidates)
	}
}

func TestLegacyDirectRelationshipApplyGraphEqualityRoleSeedsNarrowsFullDomainTarget(t *testing.T) {
	supplier := qsbridge.TableInstance{Table: "supplier", Alias: "s"}
	customer := qsbridge.TableInstance{Table: "customer", Alias: "c"}
	sNation := qsbridge.FieldRef{Table: supplier, Name: "s_nationkey", Type: qsbridge.DataTypeInt}
	cNation := qsbridge.FieldRef{Table: customer, Name: "c_nationkey", Type: qsbridge.DataTypeInt}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Sources = []qsbridge.TableInstance{supplier, customer}
	request.Predicates = []qsbridge.Predicate{{
		Expr:      qsbridge.Binary(qsbridge.BinaryOpEqual, qsbridge.Field(cNation), qsbridge.Field(sNation)),
		Placement: qsbridge.PredicateResidualJoin,
		Scope:     qsbridge.PredicateScopeWhere,
	}}
	rowsByRole := map[string][]qsbridge.QuantaRownum{
		"s": {101, 102, 103},
		"c": {1, 2, 3, 4, 5},
	}
	fullDomain := map[string]bool{
		"s": false,
		"c": true,
	}
	materialized := false
	queried := false
	executor := LegacyDirectRelationshipVectorJoinExecutor{
		Materialization: qsruntimeMaterializationKernelFunc(func(_ context.Context, kernelRequest qsbridge.ProjectionMaterializationKernelRequest) (qsbridge.ProjectionMaterializationKernelResult, error) {
			materialized = true
			if kernelRequest.RequestCount() != 1 {
				t.Fatalf("materialization request count = %d, want one", kernelRequest.RequestCount())
			}
			materialization := kernelRequest.Requests[0]
			if materialization.Index != "supplier" || !reflect.DeepEqual(materialization.Rownums, []qsbridge.QuantaRownum{101, 102, 103}) {
				t.Fatalf("materialization = %#v, want supplier source rows", materialization)
			}
			return qsbridge.ProjectionMaterializationKernelResult{
				ID: kernelRequest.ID,
				Results: []qsbridge.ProjectionMaterializationResult{{
					ID:      materialization.DependencyID,
					Request: materialization,
					RowSet: qsbridge.QuantaProjectedRowSet{
						Index:   "supplier",
						Rownums: append([]qsbridge.QuantaRownum(nil), materialization.Rownums...),
						ProjectionVectors: []qsbridge.QuantaProjectionVector{{
							Field: materialization.ProjectionFields[0],
							Values: []qsbridge.ResultCell{
								{Kind: qsbridge.ValueInt, Value: int64(1)},
								{Kind: qsbridge.ValueInt, Value: int64(2)},
								{Kind: qsbridge.ValueInt, Value: int64(2)},
							},
						}},
					},
				}},
			}, nil
		}),
		Sessions: DirectSessionProviderFunc(func(_ context.Context, queryRequest ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
			return DirectSessionHandleFunc{
				QueryFunc: func(_ context.Context, queryRequest ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
					queried = true
					if len(queryRequest.Query.Fragments) != 1 {
						t.Fatalf("fragments = %#v, want one", queryRequest.Query.Fragments)
					}
					fragment := queryRequest.Query.Fragments[0]
					if fragment.Index != "customer" || fragment.Role != "c" || fragment.Field != "c_nationkey" || fragment.BSIOp != qsbridge.QuantaBSIOpBatchEQ {
						t.Fatalf("fragment = %#v, want customer c_nationkey BATCH_EQ", fragment)
					}
					if len(fragment.Values) != 2 || fragment.Values[0].Cmp(big.NewInt(1)) != 0 || fragment.Values[1].Cmp(big.NewInt(2)) != 0 {
						t.Fatalf("fragment values = %#v, want [1,2]", fragment.Values)
					}
					return BitmapQueryResult{Success: true, Count: 3, Rownums: []qsbridge.QuantaRownum{2, 4, 8}}, nil, nil
				},
			}, nil, nil
		}),
	}

	probes, changed, diagnostics, err := executor.legacyDirectRelationshipApplyGraphEqualityRoleSeeds(context.Background(), request, rowsByRole, fullDomain, 1)
	if err != nil {
		t.Fatalf("ApplyGraphEqualityRoleSeeds error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, want none", diagnostics)
	}
	if !changed || !materialized || !queried {
		t.Fatalf("changed/materialized/queried = %v/%v/%v, want all true", changed, materialized, queried)
	}
	if got := rowsByRole["c"]; !reflect.DeepEqual(got, []qsbridge.QuantaRownum{2, 4}) {
		t.Fatalf("customer rows = %#v, want [2 4]", got)
	}
	if fullDomain["c"] {
		t.Fatalf("customer full-domain flag = true, want false")
	}
	assertExecutionProbe(t, probes, "relationship_join", "graph_iter_1_equality_role_seed_1_applied", "true")
	assertExecutionProbe(t, probes, "relationship_join", "graph_iter_1_equality_role_seed_1_target_rows_after", "2")
}

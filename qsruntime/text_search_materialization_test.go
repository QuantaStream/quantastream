package qsruntime

import (
	"context"
	"math/big"
	"testing"

	"github.com/QuantaStream/quantastream/qsbridge"
)

func TestSQLRuntimeMaterializesTextSearchPredicate(t *testing.T) {
	field := testTextSearchFieldRef()
	searchCalls := 0
	releaseCalls := 0
	runtime := SQLRuntime{
		Environment: RuntimeEnvironment{
			Execution: NewExecutionService(DirectExecutor{
				Runtime: DirectBitmapRuntime{
					Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
						if len(request.SourceIndexes) != 1 || request.SourceIndexes[0] != "customer" {
							t.Fatalf("borrow request source indexes = %#v, want customer", request.SourceIndexes)
						}
						return DirectSessionHandleFunc{
							SearchFunc: func(ctx context.Context, terms string) (map[uint64]struct{}, qsbridge.DiagnosticSet, error) {
								searchCalls++
								if terms != "Customer" {
									t.Fatalf("search terms = %q, want Customer", terms)
								}
								return map[uint64]struct{}{
									9: {},
									7: {},
								}, nil, nil
							},
							ReleaseFunc: func(ctx context.Context) qsbridge.DiagnosticSet {
								releaseCalls++
								return nil
							},
						}, nil, nil
					}),
				},
			}, nil),
		},
	}
	request := qsbridge.ExecutionRequest{
		Bound: qsbridge.BoundPlan{
			Prepared: qsbridge.PreparedPlan{
				Query:     testTextSearchQuery(field, qsbridge.TextSearch(field, qsbridge.Literal(qsbridge.ValueString, "Customer"), "")),
				Result:    qsbridge.ResultShape{Kind: qsbridge.ResultQuery},
				Supported: true,
			},
			Supported: true,
		},
		Supported: true,
	}

	materialized, state, diagnostics, err := runtime.materializeTextSearchPredicates(context.Background(), request)
	if err != nil {
		t.Fatalf("materialize err: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("materialize diagnostics: %#v", diagnostics)
	}
	if state.EmptyCandidateSet {
		t.Fatalf("empty candidate set = true, want false")
	}
	if searchCalls != 1 || releaseCalls != 1 {
		t.Fatalf("search calls = %d release calls = %d, want 1/1", searchCalls, releaseCalls)
	}
	if len(materialized.Bound.Prepared.Query.Predicates) != 1 {
		t.Fatalf("predicates = %#v, want one materialized text-search predicate", materialized.Bound.Prepared.Query.Predicates)
	}
	search, ok := qsbridge.AsTextSearchExpr(materialized.Bound.Prepared.Query.Predicates[0].Expr)
	if !ok {
		t.Fatalf("predicate expression = %T, want TextSearchExpr", materialized.Bound.Prepared.Query.Predicates[0].Expr)
	}
	wantHashes := []*big.Int{big.NewInt(7), big.NewInt(9)}
	if len(search.Hashes) != len(wantHashes) {
		t.Fatalf("hashes = %#v, want %#v", search.Hashes, wantHashes)
	}
	for i := range wantHashes {
		if search.Hashes[i].Cmp(wantHashes[i]) != 0 {
			t.Fatalf("hash[%d] = %v, want %v", i, search.Hashes[i], wantHashes[i])
		}
	}
}

func TestSQLRuntimeMaterializesTextSearchEmptyCandidateSet(t *testing.T) {
	field := testTextSearchFieldRef()
	runtime := SQLRuntime{
		Environment: RuntimeEnvironment{
			Execution: NewExecutionService(DirectExecutor{
				Runtime: DirectBitmapRuntime{
					Sessions: DirectSessionProviderFunc(func(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
						return DirectSessionHandleFunc{
							SearchFunc: func(ctx context.Context, terms string) (map[uint64]struct{}, qsbridge.DiagnosticSet, error) {
								return map[uint64]struct{}{}, nil, nil
							},
						}, nil, nil
					}),
				},
			}, nil),
		},
	}
	request := qsbridge.ExecutionRequest{
		Bound: qsbridge.BoundPlan{
			Prepared: qsbridge.PreparedPlan{
				Query:     testTextSearchQuery(field, qsbridge.TextSearch(field, qsbridge.Literal(qsbridge.ValueString, "Nope"), "")),
				Result:    qsbridge.ResultShape{Kind: qsbridge.ResultQuery},
				Supported: true,
			},
			Supported: true,
		},
		Supported: true,
	}

	materialized, state, diagnostics, err := runtime.materializeTextSearchPredicates(context.Background(), request)
	if err != nil {
		t.Fatalf("materialize err: %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("materialize diagnostics: %#v", diagnostics)
	}
	if !state.EmptyCandidateSet {
		t.Fatalf("empty candidate set = false, want true")
	}
	if len(materialized.Bound.Prepared.Query.Predicates) != 0 {
		t.Fatalf("predicates = %#v, want text-search predicate removed after empty result", materialized.Bound.Prepared.Query.Predicates)
	}
}

func testTextSearchFieldRef() qsbridge.FieldRef {
	table := qsbridge.TableInstance{Schema: "quanta", Table: "customer", Alias: "c"}
	return qsbridge.FieldRef{
		Table: table,
		Name:  "c_name",
		Type:  qsbridge.DataTypeString,
		Index: qsbridge.IndexBSI,
		Encoding: qsbridge.LegacyEncodingProfile("StringLexBSI", qsbridge.LegacyEncodingOptions{
			Searchable:   true,
			PrefixLength: 10,
			MaxLength:    10,
		}),
	}
}

func testTextSearchQuery(field qsbridge.FieldRef, expr qsbridge.Expr) qsbridge.QueryIR {
	return qsbridge.QueryIR{
		Kind:    qsbridge.QueryKindSelect,
		Sources: []qsbridge.TableInstance{field.Table},
		Predicates: []qsbridge.Predicate{{
			Expr:      expr,
			Placement: qsbridge.PredicatePushdown,
			Scope:     qsbridge.PredicateScopeWhere,
		}},
		Result: qsbridge.ResultShape{Kind: qsbridge.ResultQuery},
	}
}

package qsbridge

import "testing"

func TestQueryIRFunctionUsagesReturnsBoundCallsInQueryOrder(t *testing.T) {
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	name := Field(FieldRef{Table: customer, Name: "c_name", Type: DataTypeString})
	acctbal := Field(FieldRef{Table: customer, Name: "c_acctbal", Type: DataTypeFloat})

	query := QueryIR{
		Kind: QueryKindSelect,
		Projection: []ProjectionColumn{{
			Expr: FunctionCall(FunctionDefinition{
				Name:          "lower",
				Kind:          FunctionScalar,
				Origin:        FunctionOriginMySQLCompatible,
				ReturnType:    DataTypeString,
				Deterministic: true,
			}, name),
		}},
		Predicates: []Predicate{{
			Expr: FunctionCall(FunctionDefinition{
				Name:       "sample_stratified",
				Kind:       FunctionScalar,
				Origin:     FunctionOriginLegacyCustom,
				Placement:  FunctionPlacementPredicate,
				ReturnType: DataTypeBool,
			}, Literal(ValueString, "c_name"), Literal(ValueFloat, 1.5)),
			Placement: PredicateResidualScan,
		}},
		OrderBy: []SortSpec{{
			Expr: FunctionCall(FunctionDefinition{
				Name:       "abs",
				Kind:       FunctionScalar,
				Origin:     FunctionOriginMySQLCompatible,
				ReturnType: DataTypeFloat,
			}, acctbal),
		}},
	}

	usages := query.FunctionUsages()
	if len(usages) != 3 {
		t.Fatalf("FunctionUsages() returned %#v, want three usages", usages)
	}
	assertFunctionUsage(t, usages[0], "lower", FunctionOriginMySQLCompatible, FunctionPlacementExpression, FunctionUsageProjection)
	assertFunctionUsage(t, usages[1], "sample_stratified", FunctionOriginLegacyCustom, FunctionPlacementPredicate, FunctionUsagePredicate)
	assertFunctionUsage(t, usages[2], "abs", FunctionOriginMySQLCompatible, FunctionPlacementExpression, FunctionUsageOrderBy)
	if !usages[0].Deterministic || usages[0].ReturnType != DataTypeString {
		t.Fatalf("first usage = %#v, want deterministic string metadata", usages[0])
	}
}

func TestQueryIRFunctionUsagesIncludesNestedAndMutationContexts(t *testing.T) {
	table := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderDate := Field(FieldRef{Table: table, Name: "o_orderdate", Type: DataTypeTime})
	comment := FieldRef{Table: table, Name: "o_comment", Type: DataTypeString}

	query := QueryIR{
		Kind: QueryKindUpdate,
		Aggregates: []Aggregate{{
			Function:  "topn",
			Origin:    FunctionOriginQuantaCustom,
			Placement: FunctionPlacementAggregate,
			Type:      DataTypeString,
			Input:     FunctionCall(FunctionDefinition{Name: "year", Kind: FunctionScalar, Origin: FunctionOriginMySQLCompatible, ReturnType: DataTypeInt}, orderDate),
		}},
		Mutation: MutationShape{
			Kind: MutationUpdate,
			Assignments: []MutationAssignment{{
				Field: comment,
				Value: FunctionCall(
					FunctionDefinition{Name: "upper", Kind: FunctionScalar, Origin: FunctionOriginMySQLCompatible, ReturnType: DataTypeString},
					FunctionCall(FunctionDefinition{Name: "trim", Kind: FunctionScalar, Origin: FunctionOriginMySQLCompatible, ReturnType: DataTypeString}, Field(comment)),
				),
			}},
			Predicates: []Predicate{{
				Expr:      FunctionCall(FunctionDefinition{Name: "length", Kind: FunctionScalar, Origin: FunctionOriginMySQLCompatible, ReturnType: DataTypeInt}, Field(comment)),
				Placement: PredicateResidualScan,
			}},
		},
	}

	usages := query.FunctionUsages()
	if len(usages) != 5 {
		t.Fatalf("FunctionUsages() returned %#v, want five usages", usages)
	}
	assertFunctionUsage(t, usages[0], "topn", FunctionOriginQuantaCustom, FunctionPlacementAggregate, FunctionUsageAggregate)
	assertFunctionUsage(t, usages[1], "year", FunctionOriginMySQLCompatible, FunctionPlacementExpression, FunctionUsageAggregateInput)
	assertFunctionUsage(t, usages[2], "upper", FunctionOriginMySQLCompatible, FunctionPlacementExpression, FunctionUsageMutationValue)
	assertFunctionUsage(t, usages[3], "trim", FunctionOriginMySQLCompatible, FunctionPlacementExpression, FunctionUsageMutationValue)
	assertFunctionUsage(t, usages[4], "length", FunctionOriginMySQLCompatible, FunctionPlacementExpression, FunctionUsageMutationPredicate)
}

func assertFunctionUsage(t *testing.T, usage FunctionUsage, name string, origin FunctionOrigin, placement FunctionPlacement, context FunctionUsageContext) {
	t.Helper()
	if usage.Name != name || usage.Origin != origin || usage.Placement != placement || usage.Context != context {
		t.Fatalf("usage = %#v, want name=%s origin=%s placement=%s context=%s", usage, name, origin, placement, context)
	}
}

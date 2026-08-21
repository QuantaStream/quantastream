package qsbridge

import "testing"

func TestQueryIRRequiredAccessSummarizesSelectFieldsByTable(t *testing.T) {
	orders := TableInstance{ID: "orders", Schema: "quanta", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Schema: "quanta", Table: "lineitem", Alias: "l"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey"}
	totalPrice := FieldRef{Table: orders, Name: "o_totalprice"}
	lineOrderKey := FieldRef{Table: lineitem, Name: "l_orderkey"}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders, lineitem},
		Joins: []JoinEdge{{
			Left:  lineOrderKey,
			Right: orderKey,
			Legal: true,
		}},
		Projection: []ProjectionColumn{{Expr: Field(orderKey)}},
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpGreater, Field(totalPrice), Literal(ValueInt, 10)),
			Placement: PredicatePushdown,
		}},
	}

	access := query.RequiredAccess()
	if len(access) != 2 {
		t.Fatalf("access length = %d, want two table requirements: %#v", len(access), access)
	}
	assertAccessRequirement(t, access[0], AccessSelect, "orders", []string{"o.o_totalprice", "o.o_orderkey"})
	assertAccessRequirement(t, access[1], AccessSelect, "lineitem", []string{"l.l_orderkey"})
}

func TestQueryIRRequiredAccessIncludesMutationPrivileges(t *testing.T) {
	orders := TableInstance{ID: "orders", Schema: "quanta", Table: "orders", Alias: "o"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey"}
	totalPrice := FieldRef{Table: orders, Name: "o_totalprice"}
	query := QueryIR{
		Kind: QueryKindUpdate,
		Mutation: MutationShape{
			Kind:   MutationUpdate,
			Target: orders,
			Columns: []FieldRef{
				totalPrice,
			},
			Predicates: []Predicate{{
				Expr:      Binary(BinaryOpEqual, Field(orderKey), Literal(ValueInt, 10)),
				Placement: PredicatePushdown,
			}},
		},
	}

	access := query.RequiredAccess()
	if len(access) != 2 {
		t.Fatalf("access length = %d, want select and update requirements: %#v", len(access), access)
	}
	assertAccessRequirement(t, access[0], AccessSelect, "orders", []string{"o.o_orderkey"})
	assertAccessRequirement(t, access[1], AccessUpdate, "orders", []string{"o.o_totalprice"})
}

func TestQueryIRRequiredAccessIncludesTableLevelDelete(t *testing.T) {
	orders := TableInstance{ID: "orders", Schema: "quanta", Table: "orders"}
	query := QueryIR{
		Kind: QueryKindDelete,
		Mutation: MutationShape{
			Kind:   MutationDelete,
			Target: orders,
		},
	}

	access := query.RequiredAccess()
	if len(access) != 1 {
		t.Fatalf("access length = %d, want one delete requirement", len(access))
	}
	assertAccessRequirement(t, access[0], AccessDelete, "orders", nil)
}

func TestPlanAndPreparedExposeRequiredAccess(t *testing.T) {
	orders := TableInstance{ID: "orders", Schema: "quanta", Table: "orders"}
	field := FieldRef{Table: orders, Name: "o_orderkey"}
	result := PlanResult{Query: QueryIR{
		Kind:       QueryKindSelect,
		Sources:    []TableInstance{orders},
		Projection: []ProjectionColumn{{Expr: Field(field)}},
	}}

	if len(result.RequiredAccess()) != 1 {
		t.Fatalf("plan result access = %#v, want one requirement", result.RequiredAccess())
	}
	if len(result.PreparedPlan().RequiredAccess()) != 1 {
		t.Fatalf("prepared plan access = %#v, want one requirement", result.PreparedPlan().RequiredAccess())
	}
}

func TestQueryIRRequiredAccessIncludesSubqueryIntentAccess(t *testing.T) {
	orders := TableInstance{ID: "orders", Schema: "quanta", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Schema: "quanta", Table: "lineitem", Alias: "l"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey"}
	lineOrderKey := FieldRef{Table: lineitem, Name: "l_orderkey"}
	query := QueryIR{
		Kind:       QueryKindSelect,
		Sources:    []TableInstance{orders},
		Projection: []ProjectionColumn{{Expr: Field(orderKey)}},
		Subqueries: []SubqueryPlanIntent{{
			Kind:       SubqueryIntentScalar,
			Capability: CapabilityScalarSubquery,
			Access: []AccessRequirement{{
				Privilege: AccessSelect,
				Table:     lineitem,
				Fields:    []FieldRef{lineOrderKey},
			}},
			Scalar: &ScalarSubqueryIntent{OutputName: "scalar_subquery_value"},
		}},
	}

	access := query.RequiredAccess()
	if len(access) != 2 {
		t.Fatalf("access length = %d, want outer and subquery table requirements: %#v", len(access), access)
	}
	assertAccessRequirement(t, access[0], AccessSelect, "orders", []string{"o.o_orderkey"})
	assertAccessRequirement(t, access[1], AccessSelect, "lineitem", []string{"l.l_orderkey"})
}

func TestQueryIRRequiredAccessIncludesCorrelatedAggregateSubqueryRefs(t *testing.T) {
	outerLineitem := TableInstance{ID: "lineitem", Schema: "quanta", Table: "lineitem", Alias: "l"}
	innerLineitem := TableInstance{ID: "lineitem_subquery", Schema: "quanta", Table: "lineitem", Alias: "l2"}
	part := TableInstance{ID: "part", Schema: "quanta", Table: "part", Alias: "p"}
	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{outerLineitem, part},
		Subqueries: []SubqueryPlanIntent{{
			Kind:       SubqueryIntentCorrelatedAggregate,
			Capability: CapabilityScalarSubquery,
			CorrelatedAggregate: &CorrelatedAggregateSubqueryIntent{
				AggregateFunction: "avg",
				Factor:            0.2,
				OuterValue:        FieldRef{Table: outerLineitem, Name: "l_quantity"},
				InnerValue:        FieldRef{Table: innerLineitem, Name: "l_quantity"},
				InnerKey:          FieldRef{Table: innerLineitem, Name: "l_partkey"},
				OuterKey:          FieldRef{Table: part, Name: "p_partkey"},
				RequiredFilterFields: []FieldRef{
					{Table: part, Name: "p_brand"},
					{Table: part, Name: "p_container"},
				},
			},
		}},
	}

	access := query.RequiredAccess()
	for _, want := range []struct {
		table string
		alias string
		field string
	}{
		{"lineitem", "l", "l_quantity"},
		{"lineitem", "l2", "l_quantity"},
		{"lineitem", "l2", "l_partkey"},
		{"part", "p", "p_partkey"},
		{"part", "p", "p_brand"},
		{"part", "p", "p_container"},
	} {
		if !hasAccessField(access, AccessSelect, want.table, want.alias, want.field) {
			t.Fatalf("RequiredAccess = %#v, want select on %s.%s.%s", access, want.table, want.alias, want.field)
		}
	}
}

func assertAccessRequirement(t *testing.T, requirement AccessRequirement, privilege AccessPrivilege, table string, fields []string) {
	t.Helper()
	if requirement.Privilege != privilege {
		t.Fatalf("privilege = %q, want %q", requirement.Privilege, privilege)
	}
	if requirement.Table.Table != table {
		t.Fatalf("table = %q, want %q", requirement.Table.Table, table)
	}
	if len(requirement.Fields) != len(fields) {
		t.Fatalf("fields = %#v, want %#v", qualifiedFieldNames(requirement.Fields), fields)
	}
	got := qualifiedFieldNames(requirement.Fields)
	for i := range fields {
		if got[i] != fields[i] {
			t.Fatalf("fields = %#v, want %#v", got, fields)
		}
	}
}

func hasAccessField(requirements []AccessRequirement, privilege AccessPrivilege, table string, alias string, field string) bool {
	for _, requirement := range requirements {
		if requirement.Privilege != privilege || requirement.Table.Table != table || requirement.Table.Alias != alias {
			continue
		}
		for _, got := range requirement.Fields {
			if got.Name == field || got.PhysicalName == field {
				return true
			}
		}
	}
	return false
}

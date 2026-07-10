package qsbridge

import "testing"

func TestQueryIRSupportedRequiresPredicatesJoinsAndBlockers(t *testing.T) {
	part := TableInstance{ID: "part", Table: "part", Alias: "p"}
	partKey := FieldRef{Table: part, Name: "p_partkey", Index: IndexBSI}
	query := QueryIR{
		Kind:       QueryKindSelect,
		Sources:    []TableInstance{part},
		Predicates: []Predicate{{Expr: Binary(BinaryOpGreater, Field(partKey), Literal(ValueInt, 0)), Placement: PredicatePushdown}},
	}

	if !query.Supported() {
		t.Fatalf("expected query with supported predicate to be supported")
	}

	query.Predicates[0].Placement = PredicateUnsupported
	query.Predicates[0].Unsupported = "contains-style string predicate"
	if query.Supported() {
		t.Fatalf("expected unsupported predicate to block query")
	}

	query.Predicates = nil
	query.Blockers = []NativeBlocker{{Code: DiagnosticScalarSubquery, Capability: CapabilityScalarSubquery, Reason: "scalar subqueries are not planned yet"}}
	if query.Supported() {
		t.Fatalf("expected explicit blocker to block query")
	}
}

func TestQueryIRRequiredFieldsIncludesAllQueryInputsOnce(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Index: IndexBSI}
	orderDate := FieldRef{Table: orders, Name: "o_orderdate", Index: IndexDateTime}
	lineOrderKey := FieldRef{Table: lineitem, Name: "l_orderkey", Index: IndexBSI}
	quantity := FieldRef{Table: lineitem, Name: "l_quantity", Index: IndexBSI}
	extendedPrice := FieldRef{Table: lineitem, Name: "l_extendedprice", Index: IndexBSI}
	discount := FieldRef{Table: lineitem, Name: "l_discount", Index: IndexBSI}

	query := QueryIR{
		Kind:    QueryKindSelect,
		Sources: []TableInstance{orders, lineitem},
		Joins: []JoinEdge{{
			Left:      lineOrderKey,
			Right:     orderKey,
			Direction: JoinChildToParent,
			Legal:     true,
		}},
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpGreaterEqual, Field(orderDate), Literal(ValueString, "1995-01-01")),
			Placement: PredicatePushdown,
		}},
		Projection: []ProjectionColumn{{
			Expr:  Call("year", Field(orderDate)),
			Alias: "o_year",
		}},
		GroupBy: []Expr{Call("year", Field(orderDate))},
		Aggregates: []Aggregate{{
			Function: "sum",
			Input:    Binary(BinaryOpGreater, Field(extendedPrice), Field(discount)),
			Alias:    "revenue",
		}},
		OrderBy: []SortSpec{{Expr: AggregateRef("revenue", 0), Direction: SortDescending}},
		Result: ResultShape{
			Kind:    ResultQuery,
			Columns: []FieldRef{{Table: orders, Name: "o_year", Roles: FieldRoleVisible}},
			Hidden:  []FieldRef{quantity},
		},
	}

	refs := query.RequiredFields()
	if len(refs) != 6 {
		t.Fatalf("RequiredFields() returned %d refs, want 6", len(refs))
	}

	want := []string{
		"o.o_orderdate",
		"l.l_orderkey",
		"o.o_orderkey",
		"l.l_extendedprice",
		"l.l_discount",
		"l.l_quantity",
	}
	for i, name := range want {
		if got := refs[i].QualifiedName(); got != name {
			t.Fatalf("refs[%d] = %q, want %q", i, got, name)
		}
	}
}

func TestQueryIRResultColumnsUseProjectionAndFallbackShapeMetadata(t *testing.T) {
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	name := FieldRef{Table: customer, Name: "c_name", Type: DataTypeString, Nullable: false}
	acctbal := FieldRef{Table: customer, Name: "c_acctbal", Type: DataTypeFloat, Nullable: true}
	query := QueryIR{
		Kind: QueryKindSelect,
		Projection: []ProjectionColumn{
			{Expr: Field(name), Alias: "customer_name"},
			{Expr: Field(acctbal), Type: DataTypeFloat},
		},
	}

	columns := query.ResultColumns()
	if len(columns) != 2 {
		t.Fatalf("ResultColumns() returned %d columns, want 2", len(columns))
	}
	if columns[0].Name != "customer_name" || columns[0].Type != DataTypeString || columns[0].Nullable {
		t.Fatalf("columns[0] = %#v, want non-null string customer_name", columns[0])
	}
	if columns[1].Name != "c_acctbal" || columns[1].Type != DataTypeFloat || !columns[1].Nullable {
		t.Fatalf("columns[1] = %#v, want nullable float c_acctbal", columns[1])
	}

	fallback := QueryIR{
		Kind: QueryKindSelect,
		Result: ResultShape{
			Columns: []FieldRef{name},
		},
	}
	columns = fallback.ResultColumns()
	if len(columns) != 1 || columns[0].Name != "c_name" || columns[0].Source != "c.c_name" {
		t.Fatalf("fallback columns = %#v, want c_name source metadata", columns)
	}
}

func TestQueryIRStatementResultCarriesOKMetadata(t *testing.T) {
	query := QueryIR{
		Kind: QueryKindInsert,
		Result: ResultShape{
			Kind: ResultStatement,
			Statement: StatementResult{
				AffectedRows: 3,
				LastInsertID: 42,
				Warnings:     1,
				Status:       "Records: 3",
			},
		},
	}

	if columns := query.ResultColumns(); len(columns) != 0 {
		t.Fatalf("statement ResultColumns() = %#v, want none", columns)
	}
	result := query.StatementResult()
	if result.AffectedRows != 3 || result.LastInsertID != 42 || result.Warnings != 1 || result.Status != "Records: 3" {
		t.Fatalf("StatementResult() = %#v, want OK metadata", result)
	}
}

func TestQueryIRRequiredParametersIncludesPredicateProjectionAggregateAndSort(t *testing.T) {
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	quantity := FieldRef{Table: lineitem, Name: "l_quantity", Type: DataTypeInt}
	discount := FieldRef{Table: lineitem, Name: "l_discount", Type: DataTypeFloat}
	query := QueryIR{
		Kind: QueryKindSelect,
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpGreater, Field(quantity), Parameter(1, DataTypeInt)),
			Placement: PredicatePushdown,
		}},
		Projection: []ProjectionColumn{{Expr: Binary(BinaryOpGreater, Field(discount), Parameter(2, DataTypeFloat))}},
		Aggregates: []Aggregate{{
			Function: "sum",
			Input:    Binary(BinaryOpGreater, Field(quantity), Parameter(1, DataTypeInt)),
		}},
		OrderBy: []SortSpec{{Expr: ParameterExpr{Ref: ParameterRef{Index: 3, Name: "sort_cutoff", Type: DataTypeFloat}}}},
	}

	parameters := query.RequiredParameters()
	if len(parameters) != 3 {
		t.Fatalf("RequiredParameters() returned %d refs, want 3", len(parameters))
	}
	if parameters[0].Index != 1 || parameters[0].Type != DataTypeInt {
		t.Fatalf("parameters[0] = %#v, want int parameter 1", parameters[0])
	}
	if parameters[1].Index != 2 || parameters[1].Type != DataTypeFloat {
		t.Fatalf("parameters[1] = %#v, want float parameter 2", parameters[1])
	}
	if parameters[2].Name != "sort_cutoff" || parameters[2].Type != DataTypeFloat {
		t.Fatalf("parameters[2] = %#v, want named sort_cutoff", parameters[2])
	}
}

func TestQueryIRRequiredFieldsAndParametersIncludeMutationShape(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Type: DataTypeInt, Roles: FieldRoleMutationTarget}
	totalPrice := FieldRef{Table: orders, Name: "o_totalprice", Type: DataTypeFloat, Roles: FieldRoleMutationTarget}
	query := QueryIR{
		Kind: QueryKindInsert,
		Result: ResultShape{
			Kind: ResultStatement,
		},
		Mutation: MutationShape{
			Kind:    MutationInsert,
			Target:  orders,
			Columns: []FieldRef{orderKey, totalPrice},
			Rows: []MutationRow{{
				Values: []Expr{
					Parameter(1, DataTypeInt),
					Parameter(2, DataTypeFloat),
				},
			}},
		},
	}

	fields := query.RequiredFields()
	if len(fields) != 2 {
		t.Fatalf("RequiredFields() returned %d refs, want mutation target columns", len(fields))
	}
	if fields[0].Name != "o_orderkey" || fields[1].Name != "o_totalprice" {
		t.Fatalf("fields = %#v, want orderkey and totalprice", fields)
	}

	parameters := query.RequiredParameters()
	if len(parameters) != 2 {
		t.Fatalf("RequiredParameters() returned %d refs, want two row-value parameters", len(parameters))
	}
	if parameters[0].Index != 1 || parameters[0].Type != DataTypeInt || parameters[1].Index != 2 || parameters[1].Type != DataTypeFloat {
		t.Fatalf("parameters = %#v, want typed parameters 1 and 2", parameters)
	}

	update := QueryIR{
		Kind: QueryKindUpdate,
		Result: ResultShape{
			Kind: ResultStatement,
		},
		Mutation: MutationShape{
			Kind:    MutationUpdate,
			Target:  orders,
			Columns: []FieldRef{totalPrice},
			Assignments: []MutationAssignment{{
				Field: totalPrice,
				Value: Parameter(3, DataTypeFloat),
			}},
			Predicates: []Predicate{{
				Expr:      Binary(BinaryOpEqual, Field(orderKey), Parameter(4, DataTypeInt)),
				Placement: PredicatePushdown,
				Scope:     PredicateScopeWhere,
			}},
		},
	}
	fields = update.RequiredFields()
	if len(fields) != 2 || fields[0].Name != "o_totalprice" || fields[1].Name != "o_orderkey" {
		t.Fatalf("update fields = %#v, want target column then predicate field", fields)
	}
	parameters = update.RequiredParameters()
	if len(parameters) != 2 || parameters[0].Index != 3 || parameters[1].Index != 4 {
		t.Fatalf("update parameters = %#v, want assignment then predicate parameters", parameters)
	}
}

func TestQueryIRDiagnosticsCollectsBlockersPredicatesAndJoins(t *testing.T) {
	part := TableInstance{ID: "part", Table: "part", Alias: "p"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	partKey := FieldRef{Table: part, Name: "p_partkey"}
	linePartKey := FieldRef{Table: lineitem, Name: "l_partkey"}
	query := QueryIR{
		Blockers: []NativeBlocker{{Code: DiagnosticScalarSubquery, Reason: "scalar subquery"}},
		Predicates: []Predicate{{
			Expr:        Binary(BinaryOpEqual, Field(partKey), Literal(ValueInt, 1)),
			Placement:   PredicateUnsupported,
			Unsupported: "unsupported predicate",
		}},
		Joins: []JoinEdge{{
			Left:        partKey,
			Right:       linePartKey,
			Direction:   JoinParentToChild,
			Unsupported: "parent-to-child expansion is not planned",
		}},
	}

	codes := query.Diagnostics().Codes()
	want := []DiagnosticCode{DiagnosticScalarSubquery, DiagnosticUnsupportedPredicate, DiagnosticUnsupportedJoinDirection}
	if len(codes) != len(want) || codes[0] != want[0] || codes[1] != want[1] || codes[2] != want[2] {
		t.Fatalf("diagnostic codes = %#v, want %#v", codes, want)
	}
}

func TestQueryIRDiagnosticsIncludeMutationPredicates(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey"}
	query := QueryIR{
		Kind: QueryKindDelete,
		Mutation: MutationShape{
			Kind:   MutationDelete,
			Target: orders,
			Predicates: []Predicate{{
				Expr:        Binary(BinaryOpEqual, Field(orderKey), Literal(ValueInt, 1)),
				Placement:   PredicateUnsupported,
				Scope:       PredicateScopeWhere,
				Unsupported: "unsupported mutation predicate",
			}},
		},
	}

	if query.Supported() {
		t.Fatalf("expected unsupported mutation predicate to block query")
	}
	diagnostics := query.Diagnostics()
	assertSingleDiagnosticCode(t, diagnostics, DiagnosticUnsupportedPredicate)
}

func TestJoinEdgeSupportedRequiresLegalEdge(t *testing.T) {
	part := TableInstance{ID: "part", Table: "part", Alias: "p"}
	lineitem := TableInstance{ID: "lineitem", Table: "lineitem", Alias: "l"}
	edge := JoinEdge{
		Left:      FieldRef{Table: lineitem, Name: "l_partkey"},
		Right:     FieldRef{Table: part, Name: "p_partkey"},
		Direction: JoinChildToParent,
		Legal:     true,
	}

	if !edge.Supported() {
		t.Fatalf("expected legal edge to be supported")
	}

	edge.Unsupported = "mixed-table predicate cannot be pushed down"
	if edge.Supported() {
		t.Fatalf("expected unsupported reason to block edge")
	}
}

func TestJoinEdgeOuterJoinCapabilities(t *testing.T) {
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	edge := JoinEdge{
		Left:  FieldRef{Table: customer, Name: "c_custkey"},
		Right: FieldRef{Table: orders, Name: "o_custkey"},
		Kind:  JoinKindLeftOuter,
		Nulls: NullExtensionRight,
		Legal: true,
	}

	capabilities := edge.Capabilities()
	if len(capabilities) != 3 {
		t.Fatalf("Capabilities length = %d, want 3", len(capabilities))
	}
	want := []PlanCapability{CapabilityOuterJoin, CapabilityNullExtension, CapabilityBitmapDifference}
	for i := range want {
		if capabilities[i] != want[i] {
			t.Fatalf("capabilities[%d] = %q, want %q", i, capabilities[i], want[i])
		}
	}
}

func TestQueryIRMembershipsAffectSupportDiagnosticsAndRequiredFields(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	orderCustKey := FieldRef{Table: orders, Name: "o_custkey"}
	customerKey := FieldRef{Table: customer, Name: "c_custkey"}
	query := QueryIR{
		Kind: QueryKindSelect,
		Memberships: []MembershipEdge{{
			Left:        orderCustKey,
			Right:       customerKey,
			Kind:        MembershipAnti,
			Legal:       true,
			Unsupported: "anti membership disabled",
		}},
	}

	if query.Supported() {
		t.Fatalf("expected unsupported membership to block query")
	}
	codes := query.Diagnostics().Codes()
	if len(codes) != 1 || codes[0] != DiagnosticUnsupportedMembership {
		t.Fatalf("diagnostic codes = %#v, want unsupported membership", codes)
	}

	refs := query.RequiredFields()
	if len(refs) != 2 {
		t.Fatalf("RequiredFields length = %d, want 2", len(refs))
	}
	if got, want := refs[0].QualifiedName(), "o.o_custkey"; got != want {
		t.Fatalf("refs[0] = %q, want %q", got, want)
	}
	if got, want := refs[1].QualifiedName(), "c.c_custkey"; got != want {
		t.Fatalf("refs[1] = %q, want %q", got, want)
	}
}

func TestQueryIRRequiredFieldsForScopeFiltersPredicates(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	orderDate := FieldRef{Table: orders, Name: "o_orderdate"}
	totalPrice := FieldRef{Table: orders, Name: "o_totalprice"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey"}
	customerKey := FieldRef{Table: customer, Name: "c_custkey"}
	query := QueryIR{
		Kind: QueryKindSelect,
		Joins: []JoinEdge{{
			Left:  orderKey,
			Right: customerKey,
			On: []Predicate{{
				Expr:      Binary(BinaryOpGreater, Field(totalPrice), Literal(ValueInt, 100)),
				Placement: PredicateResidualJoin,
				Scope:     PredicateScopeOn,
			}},
			Legal: true,
		}},
		Predicates: []Predicate{{
			Expr:      Binary(BinaryOpGreaterEqual, Field(orderDate), Literal(ValueString, "1995-01-01")),
			Placement: PredicatePushdown,
			Scope:     PredicateScopeWhere,
		}},
		Projection: []ProjectionColumn{{Expr: Field(orderKey)}},
	}

	whereFields := query.RequiredFieldsForScope(PredicateScopeWhere)
	wantWhere := []string{"o.o_orderdate", "o.o_orderkey", "c.c_custkey"}
	if len(whereFields) != len(wantWhere) {
		t.Fatalf("where fields length = %d, want %d", len(whereFields), len(wantWhere))
	}
	for i, name := range wantWhere {
		if got := whereFields[i].QualifiedName(); got != name {
			t.Fatalf("whereFields[%d] = %q, want %q", i, got, name)
		}
	}

	onFields := query.RequiredFieldsForScope(PredicateScopeOn)
	wantOn := []string{"o.o_orderkey", "c.c_custkey", "o.o_totalprice"}
	if len(onFields) != len(wantOn) {
		t.Fatalf("on fields length = %d, want %d", len(onFields), len(wantOn))
	}
	for i, name := range wantOn {
		if got := onFields[i].QualifiedName(); got != name {
			t.Fatalf("onFields[%d] = %q, want %q", i, got, name)
		}
	}
}

func TestProjectionAndSortRequiredFields(t *testing.T) {
	customer := TableInstance{ID: "customer", Table: "customer", Alias: "c"}
	phone := FieldRef{Table: customer, Name: "c_phone", Index: IndexBackingString}
	acctbal := FieldRef{Table: customer, Name: "c_acctbal", Index: IndexBSI}
	projection := ProjectionColumn{
		Expr:  Call("substr", Field(phone), Literal(ValueInt, 1), Literal(ValueInt, 2)),
		Alias: "cntrycode",
	}
	sort := SortSpec{Expr: Field(acctbal), Direction: SortAscending}

	projectionFields := projection.RequiredFields()
	if len(projectionFields) != 1 || projectionFields[0].QualifiedName() != "c.c_phone" {
		t.Fatalf("unexpected projection fields: %#v", projectionFields)
	}

	sortFields := sort.RequiredFields()
	if len(sortFields) != 1 || sortFields[0].QualifiedName() != "c.c_acctbal" {
		t.Fatalf("unexpected sort fields: %#v", sortFields)
	}
}

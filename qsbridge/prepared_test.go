package qsbridge

import "testing"

func TestPlanResultPreparedPlanExposesProtocolMetadata(t *testing.T) {
	result := testPreparedSelectPlan(t)
	result.Session = SessionContext{ID: "session-2", CurrentSchema: "quanta", Roles: []RoleName{"reader"}}

	prepared := result.PreparedPlan()
	if !prepared.Supported {
		t.Fatalf("expected prepared plan to be supported: %#v", prepared.Diagnostics)
	}
	if prepared.SQL == "" || prepared.Kind != QueryKindSelect {
		t.Fatalf("prepared SQL/kind = %q/%q, want select SQL", prepared.SQL, prepared.Kind)
	}
	if prepared.AccessIntent() != PhysicalAccessRead {
		t.Fatalf("access intent = %q, want read", prepared.AccessIntent())
	}
	if len(prepared.Parameters) != 1 || prepared.Parameters[0].Index != 1 || prepared.Parameters[0].Type != DataTypeFloat {
		t.Fatalf("parameters = %#v, want one float parameter", prepared.Parameters)
	}
	if len(prepared.ResultColumns) != 1 || prepared.ResultColumns[0].Name != "order_id" || prepared.ResultColumns[0].Type != DataTypeInt {
		t.Fatalf("result columns = %#v, want typed order_id", prepared.ResultColumns)
	}
	if prepared.Logical.Root == nil || prepared.Physical.Root == nil {
		t.Fatalf("expected prepared plan to retain logical and physical roots")
	}
	if prepared.Session.ID != "session-2" || prepared.Session.CurrentSchema != "quanta" {
		t.Fatalf("prepared session = %#v, want copied session metadata", prepared.Session)
	}
	prepared.Session.Roles[0] = "mutated"
	if result.Session.Roles[0] != "reader" {
		t.Fatalf("prepared plan leaked mutable session roles")
	}
}

func TestPreparedPlanBindValidatesValuesWithoutExecution(t *testing.T) {
	prepared := testPreparedSelectPlan(t).PreparedPlan()

	bound := prepared.Bind(IndexedParameterValue(1, ValueFloat, 100.5))
	if !bound.SupportedForExecution() {
		t.Fatalf("unexpected bound diagnostics: %#v", bound.Diagnostics)
	}
	if len(bound.Parameters.Bindings) != 1 || bound.Parameters.Bindings[0].Value.Value != 100.5 {
		t.Fatalf("bindings = %#v, want supplied float value", bound.Parameters.Bindings)
	}
}

func TestPreparedPlanBindCarriesPlanDiagnostics(t *testing.T) {
	prepared := PreparedPlan{
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeInt}},
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticNativeBlocker, PhasePlan, "cannot plan natively"),
		},
		Supported: true,
	}

	bound := prepared.Bind(IndexedParameterValue(1, ValueInt, 1))
	if bound.SupportedForExecution() {
		t.Fatalf("expected plan diagnostic to block execution")
	}
	if got := bound.Diagnostics.Codes()[0]; got != DiagnosticNativeBlocker {
		t.Fatalf("diagnostic code = %q, want %q", got, DiagnosticNativeBlocker)
	}
}

func TestPreparedPlanPreservesStatementMetadata(t *testing.T) {
	query := QueryIR{
		Kind: QueryKindInsert,
		Result: ResultShape{
			Kind: ResultStatement,
			Statement: StatementResult{
				AffectedRows: 2,
				LastInsertID: 10,
				Warnings:     1,
				Status:       "ok",
			},
		},
	}
	prepared := PreparePlan(PlanResult{
		SQL:       "insert into orders values (?, ?)",
		Query:     query,
		Supported: true,
	})
	if len(prepared.ResultColumns) != 0 {
		t.Fatalf("statement result columns = %#v, want none", prepared.ResultColumns)
	}
	if prepared.Statement.AffectedRows != 2 || prepared.Statement.LastInsertID != 10 || prepared.Statement.Warnings != 1 {
		t.Fatalf("statement metadata = %#v, want OK metadata", prepared.Statement)
	}
	if prepared.AccessIntent() != PhysicalAccessWrite {
		t.Fatalf("access intent = %q, want write", prepared.AccessIntent())
	}
}

func TestPreparedPlanDescriptionCarriesProtocolPrepareMetadata(t *testing.T) {
	prepared := testPreparedSelectPlan(t).PreparedPlan().WithHandle(PreparedStatementHandle{
		ID:   42,
		Name: "stmt_orders",
	})

	description := prepared.Description()
	if !description.SupportedForPrepare() {
		t.Fatalf("unexpected description diagnostics: %#v", description.Diagnostics)
	}
	if description.Handle.ID != 42 || description.Handle.Name != "stmt_orders" {
		t.Fatalf("handle = %#v, want adapter-owned identity", description.Handle)
	}
	if description.SQL != prepared.SQL || description.Kind != QueryKindSelect {
		t.Fatalf("description SQL/kind = %q/%q, want prepared SQL/select", description.SQL, description.Kind)
	}
	if description.AccessIntent != PhysicalAccessRead {
		t.Fatalf("description access intent = %q, want read", description.AccessIntent)
	}
	if len(description.Parameters) != 1 || description.Parameters[0].Type != DataTypeFloat {
		t.Fatalf("parameters = %#v, want float parameter metadata", description.Parameters)
	}
	if len(description.ResultColumns) != 1 || description.ResultColumns[0].Name != "order_id" {
		t.Fatalf("result columns = %#v, want order_id", description.ResultColumns)
	}

	description.Parameters[0].Type = DataTypeString
	description.ResultColumns[0].Name = "mutated"
	second := prepared.Description()
	if second.Parameters[0].Type != DataTypeFloat || second.ResultColumns[0].Name != "order_id" {
		t.Fatalf("description leaked mutable metadata: %#v %#v", second.Parameters, second.ResultColumns)
	}
}

func TestPreparedPlanDescriptionReportsBlockingDiagnostics(t *testing.T) {
	prepared := PreparedPlan{
		Supported: true,
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseParse, "bad sql"),
		},
	}

	description := prepared.Description()
	if description.SupportedForPrepare() {
		t.Fatalf("expected description to be unsupported")
	}
	if got := description.Diagnostics.Codes()[0]; got != DiagnosticParserBoundary {
		t.Fatalf("diagnostic code = %q, want parser boundary", got)
	}
}

func TestPreparedPlanDescriptionCarriesWriteIntent(t *testing.T) {
	description := PreparedPlan{Kind: QueryKindDelete, Supported: true}.Description()
	if description.AccessIntent != PhysicalAccessWrite {
		t.Fatalf("description access intent = %q, want write", description.AccessIntent)
	}
}

func TestPreparedPlanCloseRequestRequiresHandle(t *testing.T) {
	unsupported := PreparedPlan{}.CloseRequest()
	if unsupported.Supported() {
		t.Fatalf("expected close request without handle to be unsupported")
	}
	if !containsDiagnosticCode(unsupported.Diagnostics.Codes(), DiagnosticInvalidExecutionOption) {
		t.Fatalf("diagnostics = %#v, want invalid execution option", unsupported.Diagnostics.Codes())
	}

	supported := PreparedPlan{}.WithHandle(PreparedStatementHandle{ID: 9}).CloseRequest()
	if !supported.Supported() || supported.Handle.ID != 9 {
		t.Fatalf("close request = %#v, want supported id 9", supported)
	}
}

func testPreparedSelectPlan(t *testing.T) PlanResult {
	t.Helper()
	parser := stubParserBridge{statement: UnboundStatement{
		SQL:  "select o_orderkey as order_id from orders where o_totalprice >= ?",
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables: []UnboundTable{{Name: "orders", Alias: "o"}},
			Projection: []UnboundProjection{{
				Expr:  UnboundField("o", "o_orderkey"),
				Alias: "order_id",
				Type:  DataTypeInt,
			}},
			Predicates: []UnboundPredicate{{
				Expr: UnboundBinary(
					BinaryOpGreaterEqual,
					UnboundField("o", "o_totalprice"),
					UnboundParameter(1, DataTypeFloat),
				),
				Placement: PredicatePushdown,
				Scope:     PredicateScopeWhere,
			}},
			Result: ResultShape{Kind: ResultQuery},
		},
	}}
	result := Planner{
		Parser:        parser,
		Catalog:       testBindCatalog(),
		DefaultSchema: "quanta",
		Scope:         PhysicalScope{Placement: PlacementLocal},
	}.Plan(parser.statement.SQL)
	if result.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", result.Diagnostics)
	}
	return result
}

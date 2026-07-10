package qsbridge

import "testing"

func TestPreparedPlanExecutionRequestBindsParametersAndCarriesMetadata(t *testing.T) {
	prepared := PreparedPlan{
		SQL:           "select o_orderkey from orders where o_orderkey = ?",
		DefaultSchema: "quanta",
		Kind:          QueryKindSelect,
		Supported:     true,
		Parameters:    []ParameterRef{{Index: 1, Type: DataTypeInt}},
		Result:        ResultShape{Kind: ResultQuery, Limit: 10},
		ResultColumns: []ResultColumn{{Name: "o_orderkey", Type: DataTypeInt}},
		Access: []AccessRequirement{{
			Privilege: AccessSelect,
			Table:     TableInstance{ID: "orders", Table: "orders"},
		}},
	}

	request := prepared.ExecutionRequest(
		ExecutionOptions{
			RequestID:  "req-1",
			MaxRows:    10,
			BatchSize:  5,
			Streaming:  true,
			Cursor:     CursorForwardOnly,
			Cancelable: true,
		},
		IndexedParameterValue(1, ValueInt, 99),
	)
	if !request.SupportedForExecution() {
		t.Fatalf("unexpected diagnostics: %#v", request.Diagnostics)
	}
	if request.Options.RequestID != "req-1" || !request.Options.Streaming || !request.Options.Cancelable {
		t.Fatalf("options = %#v, want execution options copied", request.Options)
	}
	if len(request.Bound.Parameters.Bindings) != 1 || request.Bound.Parameters.Bindings[0].Value.Value != 99 {
		t.Fatalf("bindings = %#v, want bound int value", request.Bound.Parameters.Bindings)
	}
	if len(request.ResultColumns) != 1 || request.ResultColumns[0].Name != "o_orderkey" {
		t.Fatalf("result columns = %#v, want o_orderkey", request.ResultColumns)
	}
	if len(request.Access) != 1 || request.Access[0].Privilege != AccessSelect {
		t.Fatalf("access = %#v, want select requirement", request.Access)
	}
}

func TestBoundPlanExecutionRequestCarriesStatementAndSessionActions(t *testing.T) {
	prepared := PreparedPlan{
		Kind:      QueryKindSession,
		Supported: true,
		Result:    ResultShape{Kind: ResultStatement},
		Statement: StatementResult{
			Status: "Database changed",
			SessionActions: []SessionAction{{
				Kind:  SessionActionUseSchema,
				Value: "analytics",
			}},
		},
	}
	bound := prepared.Bind()

	request := bound.ExecutionRequest(ExecutionOptions{})
	if !request.SupportedForExecution() {
		t.Fatalf("unexpected diagnostics: %#v", request.Diagnostics)
	}
	if request.Statement.Status != "Database changed" {
		t.Fatalf("statement = %#v, want status", request.Statement)
	}
	if len(request.SessionActions) != 1 || request.SessionActions[0].Value != "analytics" {
		t.Fatalf("session actions = %#v, want analytics schema action", request.SessionActions)
	}
	request.SessionActions[0].Value = "mutated"
	if prepared.SessionActions()[0].Value != "analytics" {
		t.Fatalf("execution request leaked mutable session action")
	}
}

func TestExecutionRequestRejectsFullTableScanByPolicy(t *testing.T) {
	orders := TableInstance{ID: "orders", Table: "orders", Alias: "o"}
	orderKey := FieldRef{Table: orders, Name: "o_orderkey", Type: DataTypeInt}
	query := QueryIR{
		Kind:       QueryKindSelect,
		Sources:    []TableInstance{orders},
		Projection: []ProjectionColumn{{Expr: Field(orderKey)}},
		Result:     ResultShape{Kind: ResultQuery, Columns: []FieldRef{orderKey}},
	}
	prepared := PreparedPlan{
		Kind:       QueryKindSelect,
		Query:      query,
		Inspection: InspectQuery(query, PhysicalScope{}),
		Supported:  true,
		Result:     query.Result,
	}

	allowed := prepared.ExecutionRequest(ExecutionOptions{FullTableScanPolicy: FullTableScanWarn})
	if !allowed.SupportedForExecution() {
		t.Fatalf("warn policy should not block execution: %#v", allowed.Diagnostics)
	}

	rejected := prepared.ExecutionRequest(ExecutionOptions{FullTableScanPolicy: FullTableScanReject})
	if rejected.SupportedForExecution() {
		t.Fatalf("expected reject policy to block full-table scan")
	}
	if !containsDiagnosticCode(rejected.Diagnostics.Codes(), DiagnosticFullTableScanRejected) {
		t.Fatalf("diagnostics = %#v, want full-table-scan rejection", rejected.Diagnostics)
	}
}

func TestExecutionRequestReportsParameterAndOptionDiagnostics(t *testing.T) {
	prepared := PreparedPlan{
		Supported:  true,
		Parameters: []ParameterRef{{Index: 1, Type: DataTypeInt}},
	}

	request := prepared.ExecutionRequest(
		ExecutionOptions{MaxRows: -1, BatchSize: -2, Cursor: CursorScrollable},
		IndexedParameterValue(1, ValueString, "bad"),
	)
	if request.SupportedForExecution() {
		t.Fatalf("expected execution request to be unsupported")
	}
	codes := request.Diagnostics.Codes()
	want := []DiagnosticCode{
		DiagnosticParameterTypeMismatch,
		DiagnosticInvalidExecutionOption,
	}
	if len(codes) < len(want) {
		t.Fatalf("codes = %#v, want at least %#v", codes, want)
	}
	for _, expected := range want {
		if !containsDiagnosticCode(codes, expected) {
			t.Fatalf("codes = %#v, want %q", codes, expected)
		}
	}
}

func TestExecutionRequestReturnsCopiesOfMutableMetadata(t *testing.T) {
	prepared := PreparedPlan{
		Supported:     true,
		ResultColumns: []ResultColumn{{Name: "original"}},
		Access: []AccessRequirement{{
			Privilege: AccessSelect,
			Fields:    []FieldRef{{Name: "original"}},
		}},
	}

	request := prepared.ExecutionRequest(ExecutionOptions{})
	request.ResultColumns[0].Name = "mutated"
	request.Access[0].Fields[0].Name = "mutated"

	second := prepared.ExecutionRequest(ExecutionOptions{})
	if second.ResultColumns[0].Name != "original" {
		t.Fatalf("result columns leaked mutation: %#v", second.ResultColumns)
	}
	if second.Access[0].Fields[0].Name != "original" {
		t.Fatalf("access fields leaked mutation: %#v", second.Access)
	}
}

func containsDiagnosticCode(codes []DiagnosticCode, want DiagnosticCode) bool {
	for _, code := range codes {
		if code == want {
			return true
		}
	}
	return false
}

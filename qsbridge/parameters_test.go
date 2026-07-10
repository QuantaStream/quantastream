package qsbridge

import "testing"

func TestBindParameterValuesBindsRequiredParametersInOrder(t *testing.T) {
	required := []ParameterRef{
		{Index: 1, Type: DataTypeInt, Nullable: false},
		{Index: 2, Type: DataTypeFloat, Nullable: true},
		{Name: "city", Type: DataTypeString, Nullable: false},
	}

	bindings := BindParameterValues(
		required,
		NamedParameterValue("city", ValueString, "Seattle"),
		IndexedParameterValue(2, ValueInt, 12),
		IndexedParameterValue(1, ValueInt, 42),
	)
	if bindings.Diagnostics.BlocksNative() {
		t.Fatalf("unexpected diagnostics: %#v", bindings.Diagnostics)
	}
	if len(bindings.Bindings) != len(required) {
		t.Fatalf("bindings length = %d, want %d", len(bindings.Bindings), len(required))
	}
	if bindings.Bindings[0].Value.Value != 42 || bindings.Bindings[1].Value.Value != 12 || bindings.Bindings[2].Value.Value != "Seattle" {
		t.Fatalf("bindings = %#v, want required-parameter order", bindings.Bindings)
	}
}

func TestQueryIRBindParametersUsesRequiredParameters(t *testing.T) {
	query := QueryIR{
		Predicates: []Predicate{{
			Expr: Binary(
				BinaryOpEqual,
				Parameter(1, DataTypeString),
				Literal(ValueString, "Seattle"),
			),
			Placement: PredicateResidualScan,
		}},
	}

	bindings := query.BindParameters(IndexedParameterValue(1, ValueString, "Seattle"))
	if !bindings.Supported() {
		t.Fatalf("unexpected diagnostics: %#v", bindings.Diagnostics)
	}
	if len(bindings.Bindings) != 1 || bindings.Bindings[0].Ref.Index != 1 {
		t.Fatalf("bindings = %#v, want one positional binding", bindings.Bindings)
	}
}

func TestPlanResultBindParametersUsesBoundQuery(t *testing.T) {
	result := PlanResult{Query: QueryIR{
		Projection: []ProjectionColumn{{
			Expr: Parameter(1, DataTypeInt),
		}},
	}}

	bindings := result.BindParameters(IndexedParameterValue(1, ValueInt, 7))
	if !bindings.Supported() {
		t.Fatalf("unexpected diagnostics: %#v", bindings.Diagnostics)
	}
	if len(bindings.Bindings) != 1 {
		t.Fatalf("bindings = %#v, want one binding", bindings.Bindings)
	}
}

func TestBindParameterValuesReportsMissingExtraDuplicateAndTypeDiagnostics(t *testing.T) {
	required := []ParameterRef{{Index: 1, Type: DataTypeInt, Nullable: false}}

	bindings := BindParameterValues(
		required,
		IndexedParameterValue(1, ValueString, "not-int"),
		IndexedParameterValue(1, ValueInt, 7),
		IndexedParameterValue(2, ValueInt, 8),
	)
	codes := bindings.Diagnostics.Codes()
	want := []DiagnosticCode{
		DiagnosticDuplicateParameter,
		DiagnosticParameterTypeMismatch,
		DiagnosticParameterExtra,
	}
	assertDiagnosticCodes(t, codes, want)
}

func TestBindParameterValuesReportsMissingAndNullNotAllowed(t *testing.T) {
	required := []ParameterRef{
		{Index: 1, Type: DataTypeInt, Nullable: false},
		{Index: 2, Type: DataTypeString, Nullable: false},
	}

	bindings := BindParameterValues(required, IndexedParameterValue(1, ValueNull, nil))
	codes := bindings.Diagnostics.Codes()
	want := []DiagnosticCode{
		DiagnosticParameterNullNotAllowed,
		DiagnosticParameterMissing,
	}
	assertDiagnosticCodes(t, codes, want)
}

func assertDiagnosticCodes(t *testing.T, got []DiagnosticCode, want []DiagnosticCode) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("codes = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("codes = %#v, want %#v", got, want)
		}
	}
}

package qsbridge

import "testing"

func TestDiagnosticProtocolErrorMapsCommonMySQLCompatibleCodes(t *testing.T) {
	tests := []struct {
		name       string
		diagnostic Diagnostic
		state      SQLState
		code       int
	}{
		{
			name:       "parse",
			diagnostic: ErrorDiagnostic(DiagnosticParserBoundary, PhaseParse, "bad sql"),
			state:      SQLStateSyntaxError,
			code:       mysqlErrorParse,
		},
		{
			name:       "table",
			diagnostic: ErrorDiagnostic(DiagnosticCatalogTableNotFound, PhaseBind, "missing table"),
			state:      SQLStateBaseTableNotFound,
			code:       mysqlErrorTableNotFound,
		},
		{
			name:       "schema",
			diagnostic: ErrorDiagnostic(DiagnosticCatalogSchemaNotFound, PhaseBind, "missing schema"),
			state:      SQLStateInvalidCatalogName,
			code:       mysqlErrorUnknownDatabase,
		},
		{
			name:       "field",
			diagnostic: ErrorDiagnostic(DiagnosticCatalogFieldNotFound, PhaseBind, "missing field"),
			state:      SQLStateColumnNotFound,
			code:       mysqlErrorColumnNotFound,
		},
		{
			name:       "parameter",
			diagnostic: ErrorDiagnostic(DiagnosticParameterTypeMismatch, PhaseBind, "bad parameter"),
			state:      SQLStateInvalidParameter,
			code:       mysqlErrorInvalidParameter,
		},
		{
			name:       "generic",
			diagnostic: ErrorDiagnostic(DiagnosticUnsupportedPredicate, PhaseClassify, "unsupported"),
			state:      SQLStateGeneralError,
			code:       mysqlErrorUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protocol := tt.diagnostic.ProtocolError()
			if protocol.SQLState != tt.state || protocol.VendorCode != tt.code {
				t.Fatalf("protocol = %#v, want state=%q code=%d", protocol, tt.state, tt.code)
			}
			if protocol.Message == "" {
				t.Fatalf("expected protocol message")
			}
		})
	}
}

func TestDiagnosticSetProtocolErrorsSkipsInfoAndCopiesFields(t *testing.T) {
	set := DiagnosticSet{
		{
			Code:     DiagnosticNativeBlocker,
			Severity: SeverityInfo,
			Message:  "context",
		},
		{
			Code:     DiagnosticCatalogFieldNotFound,
			Severity: SeverityError,
			Message:  "missing field",
			Fields:   []FieldRef{{Name: "original"}},
		},
	}

	errors := set.ProtocolErrors()
	if len(errors) != 1 {
		t.Fatalf("errors = %#v, want one non-info protocol error", errors)
	}
	errors[0].Diagnostic.Fields[0].Name = "mutated"
	if set[1].Fields[0].Name != "original" {
		t.Fatalf("protocol error leaked mutable diagnostic fields")
	}
}

func TestDiagnosticSetFirstProtocolErrorReturnsFirstBlocker(t *testing.T) {
	set := DiagnosticSet{
		{
			Code:     DiagnosticNativeBlocker,
			Severity: SeverityWarning,
			Message:  "warning",
		},
		ErrorDiagnostic(DiagnosticParameterMissing, PhaseBind, "missing parameter"),
	}

	protocol, ok := set.FirstProtocolError()
	if !ok {
		t.Fatalf("expected first protocol error")
	}
	if protocol.SQLState != SQLStateInvalidParameter || protocol.VendorCode != mysqlErrorInvalidParameter {
		t.Fatalf("protocol = %#v, want invalid parameter", protocol)
	}
}

func TestExecutionResultProtocolErrorsUseResultDiagnostics(t *testing.T) {
	result := ExecutionResult{
		Diagnostics: DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "bad option"),
		},
	}

	errors := result.ProtocolErrors()
	if len(errors) != 1 || errors[0].SQLState != SQLStateGeneralError {
		t.Fatalf("errors = %#v, want one general protocol error", errors)
	}
	first, ok := result.FirstProtocolError()
	if !ok || first.VendorCode != mysqlErrorUnknown {
		t.Fatalf("first = %#v ok=%v, want unknown protocol error", first, ok)
	}
}

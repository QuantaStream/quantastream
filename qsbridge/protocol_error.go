package qsbridge

// SQLState is the protocol-neutral SQLSTATE value adapters can expose.
type SQLState string

const (
	// SQLStateGeneralError is the generic SQLSTATE used when no specific class fits.
	SQLStateGeneralError SQLState = "HY000"
	// SQLStateSyntaxError marks parser and SQL shape errors.
	SQLStateSyntaxError SQLState = "42000"
	// SQLStateBaseTableNotFound marks a missing table.
	SQLStateBaseTableNotFound SQLState = "42S02"
	// SQLStateInvalidCatalogName marks a missing schema or database.
	SQLStateInvalidCatalogName SQLState = "42000"
	// SQLStateColumnNotFound marks a missing or ambiguous column reference.
	SQLStateColumnNotFound SQLState = "42S22"
	// SQLStateInvalidParameter marks prepared statement parameter binding errors.
	SQLStateInvalidParameter SQLState = "HY093"
)

const (
	mysqlErrorUnknown          = 1105
	mysqlErrorParse            = 1064
	mysqlErrorTableNotFound    = 1146
	mysqlErrorUnknownDatabase  = 1049
	mysqlErrorColumnNotFound   = 1054
	mysqlErrorColumnAmbiguous  = 1052
	mysqlErrorAccessDenied     = 1142
	mysqlErrorInvalidParameter = 1210
)

// ProtocolError is the adapter-facing form of a diagnostic.
//
// It keeps the original diagnostic for inspection while giving MySQL and future
// network adapters stable SQLSTATE/vendor-code style metadata.
type ProtocolError struct {
	SQLState   SQLState
	VendorCode int
	Message    string
	Diagnostic Diagnostic
}

// ProtocolError converts a diagnostic into protocol-facing error metadata.
func (d Diagnostic) ProtocolError() ProtocolError {
	sqlState, vendorCode := diagnosticProtocolCode(d.Code)
	return ProtocolError{
		SQLState:   sqlState,
		VendorCode: vendorCode,
		Message:    d.Error(),
		Diagnostic: cloneDiagnostic(d),
	}
}

// ProtocolErrors converts diagnostics into protocol-facing error metadata.
func (set DiagnosticSet) ProtocolErrors() []ProtocolError {
	if len(set) == 0 {
		return nil
	}
	errors := make([]ProtocolError, 0, len(set))
	for _, diagnostic := range set {
		if diagnostic.Severity == SeverityInfo {
			continue
		}
		errors = append(errors, diagnostic.ProtocolError())
	}
	return errors
}

// FirstProtocolError returns the first blocking protocol error, if any.
func (set DiagnosticSet) FirstProtocolError() (ProtocolError, bool) {
	for _, diagnostic := range set {
		if diagnostic.BlocksNative() {
			return diagnostic.ProtocolError(), true
		}
	}
	return ProtocolError{}, false
}

// ProtocolErrors converts result diagnostics into adapter-facing error metadata.
func (r ExecutionResult) ProtocolErrors() []ProtocolError {
	return r.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking result error, if any.
func (r ExecutionResult) FirstProtocolError() (ProtocolError, bool) {
	return r.Diagnostics.FirstProtocolError()
}

func diagnosticProtocolCode(code DiagnosticCode) (SQLState, int) {
	switch code {
	case DiagnosticParserBoundary,
		DiagnosticMixedBooleanPredicate:
		return SQLStateSyntaxError, mysqlErrorParse
	case DiagnosticCatalogTableNotFound:
		return SQLStateBaseTableNotFound, mysqlErrorTableNotFound
	case DiagnosticCatalogSchemaNotFound:
		return SQLStateInvalidCatalogName, mysqlErrorUnknownDatabase
	case DiagnosticCatalogFieldNotFound:
		return SQLStateColumnNotFound, mysqlErrorColumnNotFound
	case DiagnosticAmbiguousField:
		return SQLStateColumnNotFound, mysqlErrorColumnAmbiguous
	case DiagnosticAccessDenied:
		return SQLStateSyntaxError, mysqlErrorAccessDenied
	case DiagnosticParameterMissing,
		DiagnosticParameterExtra,
		DiagnosticDuplicateParameter,
		DiagnosticParameterTypeMismatch,
		DiagnosticParameterNullNotAllowed:
		return SQLStateInvalidParameter, mysqlErrorInvalidParameter
	default:
		return SQLStateGeneralError, mysqlErrorUnknown
	}
}

func cloneDiagnostic(diagnostic Diagnostic) Diagnostic {
	diagnostic.Fields = append([]FieldRef(nil), diagnostic.Fields...)
	return diagnostic
}

package qsbridge

// ClientPreparedLongDataExchange is the metadata response for storing a chunked prepared parameter.
type ClientPreparedLongDataExchange struct {
	Connection  ConnectionContext
	Fragment    PreparedLongDataFragment
	Prepared    PreparedPlan
	State       PreparedLongDataState
	Stored      bool
	Diagnostics DiagnosticSet
}

// StoreClientPreparedLongData validates and records one chunk of adapter-owned prepared parameter data.
func (s PlanningService) StoreClientPreparedLongData(connection ConnectionContext, preparedRegistry PreparedStatementRegistry, longDataRegistry PreparedLongDataRegistry, fragment PreparedLongDataFragment) ClientPreparedLongDataExchange {
	_ = s
	result := ClientPreparedLongDataExchange{
		Connection:  cloneConnectionContext(connection),
		Fragment:    clonePreparedLongDataFragment(fragment),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		return result
	}
	if preparedRegistry == nil {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "prepared statement registry is not configured"),
		})
		return result
	}
	if longDataRegistry == nil {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "prepared long-data registry is not configured"),
		})
		return result
	}
	if fragment.Handle.Empty() {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "prepared long data requires a statement id or name"),
		})
		return result
	}

	prepared, ok := preparedRegistry.Get(fragment.Handle)
	if !ok {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "prepared statement handle not found"),
		})
		return result
	}
	result.Prepared = clonePreparedPlan(prepared)
	result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, validatePreparedLongDataFragment(prepared, fragment))
	if result.Diagnostics.BlocksNative() {
		return result
	}

	state, ok := longDataRegistry.Append(fragment)
	if !ok {
		result.Diagnostics = mergeDiagnosticSets(result.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseBind, "prepared long data could not be stored"),
		})
		return result
	}
	result.State = clonePreparedLongDataState(state)
	result.Stored = true
	return result
}

// Supported reports whether the long-data fragment was accepted into adapter metadata.
func (e ClientPreparedLongDataExchange) Supported() bool {
	return e.Connection.Supported() && e.Stored && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts long-data diagnostics into protocol-facing errors.
func (e ClientPreparedLongDataExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking long-data error, if any.
func (e ClientPreparedLongDataExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func validatePreparedLongDataFragment(prepared PreparedPlan, fragment PreparedLongDataFragment) DiagnosticSet {
	diagnostics := make(DiagnosticSet, 0)
	ref, ok := lookupPreparedLongDataParameter(prepared.Parameters, fragment.Parameter)
	if !ok {
		return append(diagnostics, ErrorDiagnostic(
			DiagnosticParameterExtra,
			PhaseBind,
			"prepared long data references unknown parameter: "+parameterValueLabel(fragment.Parameter),
		))
	}
	if fragment.Parameter.Kind == ValueUnknown {
		return diagnostics
	}
	if !parameterValueMatchesType(ref.Type, fragment.Parameter.Kind) {
		diagnostics = append(diagnostics, ErrorDiagnostic(
			DiagnosticParameterTypeMismatch,
			PhaseBind,
			"prepared long data type mismatch for "+parameterRefLabel(ref)+": got "+string(fragment.Parameter.Kind)+", want "+string(ref.Type),
		))
	}
	return diagnostics
}

func lookupPreparedLongDataParameter(required []ParameterRef, parameter ParameterValue) (ParameterRef, bool) {
	for _, ref := range required {
		if ref.Name != "" && parameter.Name == ref.Name {
			return ref, true
		}
		if ref.Name == "" && parameter.Name == "" && ref.Index == parameter.Index {
			return ref, true
		}
	}
	return ParameterRef{}, false
}

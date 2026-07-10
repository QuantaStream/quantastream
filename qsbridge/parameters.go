package qsbridge

import "strconv"

// ParameterValue is one execute-time value supplied for a prepared statement.
type ParameterValue struct {
	Index int
	Name  string
	Kind  ValueKind
	Value any
}

// IndexedParameterValue creates a positional prepared-statement value.
func IndexedParameterValue(index int, kind ValueKind, value any) ParameterValue {
	return ParameterValue{Index: index, Kind: kind, Value: value}
}

// NamedParameterValue creates a named prepared-statement value.
func NamedParameterValue(name string, kind ValueKind, value any) ParameterValue {
	return ParameterValue{Name: name, Kind: kind, Value: value}
}

// ParameterBinding pairs a required placeholder with its supplied value.
type ParameterBinding struct {
	Ref   ParameterRef
	Value ParameterValue
}

// ParameterBindingSet is the validation result for execute-time parameters.
type ParameterBindingSet struct {
	Bindings    []ParameterBinding
	Diagnostics DiagnosticSet
}

// Supported reports whether all required parameters have valid values.
func (s ParameterBindingSet) Supported() bool {
	return !s.Diagnostics.BlocksNative()
}

// BindParameters validates supplied values against this query's placeholders.
func (q QueryIR) BindParameters(values ...ParameterValue) ParameterBindingSet {
	return BindParameterValues(q.RequiredParameters(), values...)
}

// BindParameters validates supplied values against this planning result's placeholders.
func (r PlanResult) BindParameters(values ...ParameterValue) ParameterBindingSet {
	return r.Query.BindParameters(values...)
}

// BindParameterValues validates supplied values against required placeholders.
func BindParameterValues(required []ParameterRef, values ...ParameterValue) ParameterBindingSet {
	indexed := make(map[int]ParameterValue)
	named := make(map[string]ParameterValue)
	used := make(map[string]struct{})
	diagnostics := make(DiagnosticSet, 0)

	for _, value := range values {
		key := parameterValueKey(value)
		if _, ok := used[key]; ok {
			diagnostics = append(diagnostics, ErrorDiagnostic(
				DiagnosticDuplicateParameter,
				PhaseBind,
				"duplicate prepared-statement parameter value: "+parameterValueLabel(value),
			))
			continue
		}
		used[key] = struct{}{}
		if value.Name != "" {
			named[value.Name] = value
			continue
		}
		indexed[value.Index] = value
	}

	bindings := make([]ParameterBinding, 0, len(required))
	for _, ref := range required {
		value, ok := lookupParameterValue(ref, indexed, named)
		if !ok {
			diagnostics = append(diagnostics, ErrorDiagnostic(
				DiagnosticParameterMissing,
				PhaseBind,
				"missing prepared-statement parameter: "+parameterRefLabel(ref),
			))
			continue
		}
		if diagnostic, ok := validateParameterValue(ref, value); ok {
			diagnostics = append(diagnostics, diagnostic)
			continue
		}
		bindings = append(bindings, ParameterBinding{Ref: ref, Value: value})
	}

	requiredKeys := make(map[string]struct{}, len(required))
	for _, ref := range required {
		requiredKeys[parameterRefKey(ref)] = struct{}{}
	}
	for _, value := range values {
		if _, ok := requiredKeys[parameterValueKey(value)]; ok {
			continue
		}
		diagnostics = append(diagnostics, ErrorDiagnostic(
			DiagnosticParameterExtra,
			PhaseBind,
			"extra prepared-statement parameter value: "+parameterValueLabel(value),
		))
	}

	return ParameterBindingSet{Bindings: bindings, Diagnostics: diagnostics}
}

func lookupParameterValue(ref ParameterRef, indexed map[int]ParameterValue, named map[string]ParameterValue) (ParameterValue, bool) {
	if ref.Name != "" {
		value, ok := named[ref.Name]
		return value, ok
	}
	value, ok := indexed[ref.Index]
	return value, ok
}

func validateParameterValue(ref ParameterRef, value ParameterValue) (Diagnostic, bool) {
	if value.Kind == ValueNull {
		if ref.Nullable {
			return Diagnostic{}, false
		}
		return ErrorDiagnostic(
			DiagnosticParameterNullNotAllowed,
			PhaseBind,
			"prepared-statement parameter cannot be null: "+parameterRefLabel(ref),
		), true
	}
	if parameterValueMatchesType(ref.Type, value.Kind) {
		return Diagnostic{}, false
	}
	return ErrorDiagnostic(
		DiagnosticParameterTypeMismatch,
		PhaseBind,
		"prepared-statement parameter type mismatch for "+parameterRefLabel(ref)+": got "+string(value.Kind)+", want "+string(ref.Type),
	), true
}

func parameterValueMatchesType(dataType DataType, kind ValueKind) bool {
	switch dataType {
	case DataTypeUnknown:
		return true
	case DataTypeBool:
		return kind == ValueBool
	case DataTypeInt:
		return kind == ValueInt
	case DataTypeFloat:
		return kind == ValueFloat || kind == ValueInt
	case DataTypeString:
		return kind == ValueString
	case DataTypeTime:
		return kind == ValueTime || kind == ValueString
	default:
		return false
	}
}

func parameterRefLabel(ref ParameterRef) string {
	if ref.Name != "" {
		return ":" + ref.Name
	}
	return "?" + strconv.Itoa(ref.Index)
}

func parameterValueLabel(value ParameterValue) string {
	if value.Name != "" {
		return ":" + value.Name
	}
	return "?" + strconv.Itoa(value.Index)
}

func parameterValueKey(value ParameterValue) string {
	if value.Name != "" {
		return "name:" + value.Name
	}
	return "index:" + strconv.Itoa(value.Index)
}

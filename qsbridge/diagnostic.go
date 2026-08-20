package qsbridge

// DiagnosticCode is a stable machine-readable reason code.
type DiagnosticCode string

const (
	// DiagnosticNativeBlocker marks a generic native planning blocker.
	DiagnosticNativeBlocker DiagnosticCode = "native_blocker"
	// DiagnosticUnsupportedSQL marks SQL outside the supported scope of a specific planning surface.
	DiagnosticUnsupportedSQL DiagnosticCode = "unsupported_sql"
	// DiagnosticUnsupportedPredicate marks a predicate the native path cannot execute.
	DiagnosticUnsupportedPredicate DiagnosticCode = "unsupported_predicate"
	// DiagnosticMixedTableResidual marks a residual expression spanning multiple tables.
	DiagnosticMixedTableResidual DiagnosticCode = "mixed_table_residual"
	// DiagnosticScalarSubquery marks a scalar subquery that cannot be planned natively.
	DiagnosticScalarSubquery DiagnosticCode = "scalar_subquery"
	// DiagnosticUnsupportedMembership marks a semi/anti membership edge the native path cannot execute.
	DiagnosticUnsupportedMembership DiagnosticCode = "unsupported_membership"
	// DiagnosticCorrelatedAggregateSubquery marks a correlated aggregate subquery.
	DiagnosticCorrelatedAggregateSubquery DiagnosticCode = "correlated_aggregate_subquery"
	// DiagnosticUnsupportedJoin marks a join edge the native path cannot traverse.
	DiagnosticUnsupportedJoin DiagnosticCode = "unsupported_join"
	// DiagnosticUnsupportedJoinDirection marks a known but unsupported relationship direction.
	DiagnosticUnsupportedJoinDirection DiagnosticCode = "unsupported_join_direction"
	// DiagnosticOuterJoin marks an outer join that cannot be planned natively yet.
	DiagnosticOuterJoin DiagnosticCode = "outer_join"
	// DiagnosticDerivedTable marks a derived-table boundary.
	DiagnosticDerivedTable DiagnosticCode = "derived_table"
	// DiagnosticParserBoundary marks SQL the parser or bridge cannot represent cleanly.
	DiagnosticParserBoundary DiagnosticCode = "parser_boundary"
	// DiagnosticMixedBooleanPredicate marks a boolean predicate shape that needs grouped expression planning.
	DiagnosticMixedBooleanPredicate DiagnosticCode = "mixed_boolean_predicate"
	// DiagnosticUnsupportedFunction marks a scalar or aggregate function blocker.
	DiagnosticUnsupportedFunction DiagnosticCode = "unsupported_function"
	// DiagnosticAmbiguousField marks a field reference that cannot be resolved uniquely.
	DiagnosticAmbiguousField DiagnosticCode = "ambiguous_field"
	// DiagnosticInternalInvariant marks an internal planning invariant violation.
	DiagnosticInternalInvariant DiagnosticCode = "internal_invariant"
	// DiagnosticCatalogTableNotFound marks a missing catalog table lookup.
	DiagnosticCatalogTableNotFound DiagnosticCode = "catalog_table_not_found"
	// DiagnosticCatalogViewNotFound marks a missing catalog view lookup.
	DiagnosticCatalogViewNotFound DiagnosticCode = "catalog_view_not_found"
	// DiagnosticCatalogSchemaNotFound marks a missing catalog schema lookup.
	DiagnosticCatalogSchemaNotFound DiagnosticCode = "catalog_schema_not_found"
	// DiagnosticCatalogFieldNotFound marks a missing catalog field lookup.
	DiagnosticCatalogFieldNotFound DiagnosticCode = "catalog_field_not_found"
	// DiagnosticCatalogExpressionInvalid marks a catalog default or selector expression that cannot be parsed or evaluated.
	DiagnosticCatalogExpressionInvalid DiagnosticCode = "catalog_expression_invalid"
	// DiagnosticCatalogExpressionUnresolved marks a catalog expression reference missing from the evaluation payload.
	DiagnosticCatalogExpressionUnresolved DiagnosticCode = "catalog_expression_unresolved"
	// DiagnosticCatalogRelationshipNotFound marks a missing catalog relationship lookup.
	DiagnosticCatalogRelationshipNotFound DiagnosticCode = "catalog_relationship_not_found"
	// DiagnosticCatalogFunctionNotFound marks a missing catalog function lookup.
	DiagnosticCatalogFunctionNotFound DiagnosticCode = "catalog_function_not_found"
	// DiagnosticDictionaryNotFound marks a missing StringEnum dictionary lookup.
	DiagnosticDictionaryNotFound DiagnosticCode = "dictionary_not_found"
	// DiagnosticDictionaryLabelNotFound marks a missing StringEnum label lookup.
	DiagnosticDictionaryLabelNotFound DiagnosticCode = "dictionary_label_not_found"
	// DiagnosticDictionaryIDNotFound marks a missing StringEnum encoded-id lookup.
	DiagnosticDictionaryIDNotFound DiagnosticCode = "dictionary_id_not_found"
	// DiagnosticTableAliasNotFound marks a missing query-local table reference.
	DiagnosticTableAliasNotFound DiagnosticCode = "table_alias_not_found"
	// DiagnosticDuplicateTableAlias marks a repeated query-local table reference.
	DiagnosticDuplicateTableAlias DiagnosticCode = "duplicate_table_alias"
	// DiagnosticParameterMissing marks a missing prepared-statement value.
	DiagnosticParameterMissing DiagnosticCode = "parameter_missing"
	// DiagnosticParameterExtra marks a supplied value with no matching placeholder.
	DiagnosticParameterExtra DiagnosticCode = "parameter_extra"
	// DiagnosticDuplicateParameter marks a repeated supplied placeholder value.
	DiagnosticDuplicateParameter DiagnosticCode = "duplicate_parameter"
	// DiagnosticParameterTypeMismatch marks a value that cannot satisfy placeholder type metadata.
	DiagnosticParameterTypeMismatch DiagnosticCode = "parameter_type_mismatch"
	// DiagnosticParameterNullNotAllowed marks a NULL value supplied for a non-null placeholder.
	DiagnosticParameterNullNotAllowed DiagnosticCode = "parameter_null_not_allowed"
	// DiagnosticInvalidExecutionOption marks an unsupported or invalid execution option.
	DiagnosticInvalidExecutionOption DiagnosticCode = "invalid_execution_option"
	// DiagnosticFullTableScanRejected marks an execution policy rejection for unfiltered scans.
	DiagnosticFullTableScanRejected DiagnosticCode = "full_table_scan_rejected"
	// DiagnosticMutationMissingPredicate marks an UPDATE/DELETE rejected because it would mutate without a predicate.
	DiagnosticMutationMissingPredicate DiagnosticCode = "mutation_missing_predicate"
	// DiagnosticMutationProtectedField marks an UPDATE assignment to row identity or another protected field.
	DiagnosticMutationProtectedField DiagnosticCode = "mutation_protected_field"
	// DiagnosticMutationPrimaryKeyNull marks existing NULL data that blocks promoting columns to a primary key.
	DiagnosticMutationPrimaryKeyNull DiagnosticCode = "mutation_primary_key_null"
	// DiagnosticMutationPrimaryKeyDuplicate marks existing duplicate data that blocks promoting columns to a primary key.
	DiagnosticMutationPrimaryKeyDuplicate DiagnosticCode = "mutation_primary_key_duplicate"
	// DiagnosticUnsupportedMutation marks mutation semantics outside the initial native legality model.
	DiagnosticUnsupportedMutation DiagnosticCode = "unsupported_mutation"
	// DiagnosticTruncateChildDataExists rejects truncating a parent before dependent child tables are empty.
	DiagnosticTruncateChildDataExists DiagnosticCode = "truncate_child_data_exists"
	// DiagnosticRouteRejected marks a native-only route rejected by policy.
	DiagnosticRouteRejected DiagnosticCode = "route_rejected"
	// DiagnosticAccessDenied marks an authorization decision that denied access.
	DiagnosticAccessDenied DiagnosticCode = "access_denied"
)

// DiagnosticSeverity classifies how a diagnostic affects planning.
type DiagnosticSeverity string

const (
	// SeverityInfo records explanatory context.
	SeverityInfo DiagnosticSeverity = "info"
	// SeverityWarning records a non-fatal concern.
	SeverityWarning DiagnosticSeverity = "warning"
	// SeverityError records a blocker.
	SeverityError DiagnosticSeverity = "error"
)

// DiagnosticPhase identifies where a diagnostic was produced.
type DiagnosticPhase string

const (
	// PhaseParse identifies parser or SQL bridge diagnostics.
	PhaseParse DiagnosticPhase = "parse"
	// PhaseBind identifies name and schema binding diagnostics.
	PhaseBind DiagnosticPhase = "bind"
	// PhaseClassify identifies native capability classification diagnostics.
	PhaseClassify DiagnosticPhase = "classify"
	// PhasePlan identifies native plan construction diagnostics.
	PhasePlan DiagnosticPhase = "plan"
	// PhaseExecute identifies executor diagnostics.
	PhaseExecute DiagnosticPhase = "execute"
)

// SourceSpan identifies an approximate location in the original SQL text.
type SourceSpan struct {
	StartLine int
	StartCol  int
	EndLine   int
	EndCol    int
}

// Empty reports whether the span has no source coordinates.
func (s SourceSpan) Empty() bool {
	return s.StartLine == 0 && s.StartCol == 0 && s.EndLine == 0 && s.EndCol == 0
}

// Diagnostic is a structured reason emitted by parsing, binding, planning, or execution.
type Diagnostic struct {
	Code       DiagnosticCode
	Severity   DiagnosticSeverity
	Phase      DiagnosticPhase
	Message    string
	Capability PlanCapability
	Span       SourceSpan
	Fields     []FieldRef
}

// Error returns a stable, human-readable diagnostic string.
func (d Diagnostic) Error() string {
	if d.Message == "" {
		return string(d.Code)
	}
	if d.Code == "" {
		return d.Message
	}
	return string(d.Code) + ": " + d.Message
}

// BlocksNative reports whether the diagnostic prevents native planning.
func (d Diagnostic) BlocksNative() bool {
	return d.Severity == SeverityError
}

// ErrorDiagnostic creates a native-planning blocker diagnostic.
func ErrorDiagnostic(code DiagnosticCode, phase DiagnosticPhase, message string) Diagnostic {
	return Diagnostic{
		Code:     code,
		Severity: SeverityError,
		Phase:    phase,
		Message:  message,
	}
}

// DiagnosticSet is an ordered collection of diagnostics.
type DiagnosticSet []Diagnostic

// BlocksNative reports whether any diagnostic prevents native planning.
func (set DiagnosticSet) BlocksNative() bool {
	for _, diagnostic := range set {
		if diagnostic.BlocksNative() {
			return true
		}
	}
	return false
}

// Codes returns diagnostic codes in first-seen order.
func (set DiagnosticSet) Codes() []DiagnosticCode {
	codes := make([]DiagnosticCode, 0, len(set))
	for _, diagnostic := range set {
		codes = append(codes, diagnostic.Code)
	}
	return codes
}

// NativeBlocker records a known reason the native path cannot execute a query.
type NativeBlocker struct {
	Code       DiagnosticCode
	Capability PlanCapability
	Reason     string
	Phase      DiagnosticPhase
	Span       SourceSpan
}

// Diagnostic converts the blocker into the common diagnostic shape.
func (b NativeBlocker) Diagnostic() Diagnostic {
	code := b.Code
	if code == "" {
		code = DiagnosticNativeBlocker
	}
	phase := b.Phase
	if phase == "" {
		phase = PhaseClassify
	}
	return Diagnostic{
		Code:       code,
		Severity:   SeverityError,
		Phase:      phase,
		Message:    b.Reason,
		Capability: b.Capability,
		Span:       b.Span,
	}
}

// PredicateDiagnostic converts an unsupported predicate into a diagnostic.
func PredicateDiagnostic(predicate Predicate) Diagnostic {
	code := DiagnosticUnsupportedPredicate
	if predicate.Placement == PredicateResidualJoin {
		code = DiagnosticMixedTableResidual
	}
	return Diagnostic{
		Code:       code,
		Severity:   SeverityError,
		Phase:      PhaseClassify,
		Message:    predicate.Unsupported,
		Capability: firstCapability(predicate.Capabilities),
		Fields:     FieldRefs(predicate.Expr),
	}
}

// JoinDiagnostic converts an unsupported join edge into a diagnostic.
func JoinDiagnostic(edge JoinEdge) Diagnostic {
	code := DiagnosticUnsupportedJoin
	if edge.Direction == JoinParentToChild {
		code = DiagnosticUnsupportedJoinDirection
	}
	return Diagnostic{
		Code:     code,
		Severity: SeverityError,
		Phase:    PhaseClassify,
		Message:  edge.Unsupported,
		Fields:   []FieldRef{edge.Left, edge.Right},
	}
}

// MembershipDiagnostic converts an unsupported membership edge into a diagnostic.
func MembershipDiagnostic(edge MembershipEdge) Diagnostic {
	return Diagnostic{
		Code:       DiagnosticUnsupportedMembership,
		Severity:   SeverityError,
		Phase:      PhaseClassify,
		Message:    edge.Unsupported,
		Capability: membershipDiagnosticCapability(edge),
		Fields:     []FieldRef{edge.Left, edge.Right},
	}
}

// membershipDiagnosticCapability chooses the most specific membership planning evidence.
func membershipDiagnosticCapability(edge MembershipEdge) PlanCapability {
	if capability := firstCapability(RelationshipPlanCapabilities(edge.Encoding)); capability != "" {
		return capability
	}
	return firstCapability(edge.Capabilities())
}

func firstCapability(capabilities []PlanCapability) PlanCapability {
	if len(capabilities) == 0 {
		return ""
	}
	return capabilities[0]
}

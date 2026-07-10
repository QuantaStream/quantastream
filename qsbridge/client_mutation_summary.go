package qsbridge

// ClientMutationSummaryRow describes the write shape of one prepared mutation plan.
type ClientMutationSummaryRow struct {
	Kind            MutationKind
	Target          TableInstance
	ColumnCount     int
	RowCount        int
	AssignmentCount int
	PredicateCount  int
	ParameterCount  int
	Columns         []FieldRef
	PredicateScopes []PredicateScope
	Supported       bool
	Diagnostics     []DiagnosticCode
}

// ClientMutationSummaryExchange is adapter-facing mutation-shape metadata.
type ClientMutationSummaryExchange struct {
	Connection   ConnectionContext
	Prepared     PreparedPlan
	Rows         []ClientMutationSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// PrepareClientMutationSummary returns protocol-neutral mutation-shape rows.
func (s PlanningService) PrepareClientMutationSummary(connection ConnectionContext, prepared PreparedPlan) ClientMutationSummaryExchange {
	_ = s
	exchange := ClientMutationSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Prepared:    clonePreparedPlan(prepared),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), prepared.Diagnostics),
	}
	if connection.Supported() && prepared.Query.Mutation.Kind != MutationUnknown {
		exchange.Rows = []ClientMutationSummaryRow{mutationSummaryRow(prepared)}
	}
	exchange.Result = exchange.mutationSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether mutation-shape metadata can be returned.
func (e ClientMutationSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts mutation summary diagnostics into protocol-facing errors.
func (e ClientMutationSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking mutation summary error, if any.
func (e ClientMutationSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientMutationSummaryExchange) mutationSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     mutationSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.mutationSummaryRows(),
		Final: true,
	})
}

func mutationSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Kind", Type: DataTypeString},
		{Name: "Target", Type: DataTypeString, Nullable: true},
		{Name: "Column_count", Type: DataTypeInt},
		{Name: "Row_count", Type: DataTypeInt},
		{Name: "Assignment_count", Type: DataTypeInt},
		{Name: "Predicate_count", Type: DataTypeInt},
		{Name: "Parameter_count", Type: DataTypeInt},
		{Name: "Columns", Type: DataTypeString, Nullable: true},
		{Name: "Predicate_scopes", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientMutationSummaryExchange) mutationSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(row.Kind)),
			metadataStringCell(row.Target.DisplayName()),
			metadataIntCell(row.ColumnCount),
			metadataIntCell(row.RowCount),
			metadataIntCell(row.AssignmentCount),
			metadataIntCell(row.PredicateCount),
			metadataIntCell(row.ParameterCount),
			metadataStringCell(joinStringValues(qualifiedFieldNames(row.Columns))),
			metadataStringCell(joinStringValues(predicateScopeStrings(row.PredicateScopes))),
			metadataBoolCell(row.Supported),
			metadataStringCell(joinDiagnosticCodes(row.Diagnostics)),
		})
	}
	return rows
}

func mutationSummaryRow(prepared PreparedPlan) ClientMutationSummaryRow {
	mutation := prepared.Query.Mutation
	return ClientMutationSummaryRow{
		Kind:            mutation.Kind,
		Target:          mutation.Target,
		ColumnCount:     len(mutation.Columns),
		RowCount:        len(mutation.Rows),
		AssignmentCount: len(mutation.Assignments),
		PredicateCount:  len(mutation.Predicates),
		ParameterCount:  len(prepared.Parameters),
		Columns:         append([]FieldRef(nil), mutation.Columns...),
		PredicateScopes: mutationPredicateScopes(mutation),
		Supported:       prepared.Supported && !prepared.Diagnostics.BlocksNative(),
		Diagnostics:     prepared.Diagnostics.Codes(),
	}
}

func mutationPredicateScopes(mutation MutationShape) []PredicateScope {
	if len(mutation.Predicates) == 0 {
		return nil
	}
	scopes := make([]PredicateScope, 0, len(mutation.Predicates))
	for _, predicate := range mutation.Predicates {
		scopes = append(scopes, predicate.Scope)
	}
	return scopes
}

func predicateScopeStrings(scopes []PredicateScope) []string {
	if len(scopes) == 0 {
		return nil
	}
	values := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		values = append(values, string(scope))
	}
	return values
}

package qsbridge

// ClientParameterBindingRow describes one supplied or required prepared value.
type ClientParameterBindingRow struct {
	Ordinal      int
	Parameter    string
	Name         string
	RequiredType DataType
	Nullable     bool
	SuppliedKind ValueKind
	Present      bool
	Bound        bool
	Diagnostic   DiagnosticCode
}

// ClientParameterBindingSummaryRow describes aggregate execute-time parameter binding metadata.
type ClientParameterBindingSummaryRow struct {
	ParameterCount      int
	RequiredCount       int
	NamedCount          int
	PositionalCount     int
	NullableCount       int
	PresentCount        int
	BoundCount          int
	MissingCount        int
	ExtraCount          int
	TypeMismatchCount   int
	NullNotAllowedCount int
}

// ClientParameterBindingExchange is adapter-facing execute-time parameter binding metadata.
type ClientParameterBindingExchange struct {
	Connection          ConnectionContext
	Prepared            PreparedPlan
	Values              []ParameterValue
	Bindings            ParameterBindingSet
	Rows                []ClientParameterBindingRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// ListClientParameterBindings validates supplied values and returns binding rows.
func (s PlanningService) ListClientParameterBindings(connection ConnectionContext, prepared PreparedPlan, values ...ParameterValue) ClientParameterBindingExchange {
	_ = s
	bindings := BindParameterValues(prepared.Parameters, values...)
	exchange := ClientParameterBindingExchange{
		Connection:          cloneConnectionContext(connection),
		Prepared:            clonePreparedPlan(prepared),
		Values:              append([]ParameterValue(nil), values...),
		Bindings:            cloneParameterBindingSet(bindings),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = parameterBindingRows(prepared.Parameters, values)
	}
	exchange.Result = exchange.parameterBindingResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether parameter binding metadata can be returned.
func (e ClientParameterBindingExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientParameterBindingExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientParameterBindingExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientParameterBindingExchange) parameterBindingResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     parameterBindingResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.parameterBindingResultRows(),
		Final: true,
	})
}

func parameterBindingResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Ordinal", Type: DataTypeInt},
		{Name: "Parameter", Type: DataTypeString},
		{Name: "Name", Type: DataTypeString, Nullable: true},
		{Name: "Required_type", Type: DataTypeString, Nullable: true},
		{Name: "Nullable", Type: DataTypeBool},
		{Name: "Supplied_kind", Type: DataTypeString, Nullable: true},
		{Name: "Present", Type: DataTypeBool},
		{Name: "Bound", Type: DataTypeBool},
		{Name: "Diagnostic", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientParameterBindingExchange) parameterBindingResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Ordinal),
			metadataStringCell(row.Parameter),
			metadataStringCell(row.Name),
			metadataStringCell(string(row.RequiredType)),
			metadataBoolCell(row.Nullable),
			metadataStringCell(string(row.SuppliedKind)),
			metadataBoolCell(row.Present),
			metadataBoolCell(row.Bound),
			metadataStringCell(string(row.Diagnostic)),
		})
	}
	return rows
}

func parameterBindingRows(required []ParameterRef, values []ParameterValue) []ClientParameterBindingRow {
	rows := make([]ClientParameterBindingRow, 0, len(required)+len(values))
	for i, ref := range required {
		value, present := firstParameterValue(ref, values)
		row := ClientParameterBindingRow{
			Ordinal:      i + 1,
			Parameter:    parameterRefLabel(ref),
			Name:         ref.Name,
			RequiredType: ref.Type,
			Nullable:     ref.Nullable,
			Present:      present,
		}
		if present {
			row.SuppliedKind = value.Kind
			if diagnostic, ok := validateParameterValue(ref, value); ok {
				row.Diagnostic = diagnostic.Code
			} else {
				row.Bound = true
			}
		} else {
			row.Diagnostic = DiagnosticParameterMissing
		}
		rows = append(rows, row)
	}
	for _, value := range values {
		if parameterValueMatchesRequired(value, required) {
			continue
		}
		rows = append(rows, ClientParameterBindingRow{
			Ordinal:      len(rows) + 1,
			Parameter:    parameterValueLabel(value),
			Name:         value.Name,
			SuppliedKind: value.Kind,
			Present:      true,
			Diagnostic:   DiagnosticParameterExtra,
		})
	}
	return rows
}

func summarizeParameterBindingRows(rows []ClientParameterBindingRow) ClientParameterBindingSummaryRow {
	summary := ClientParameterBindingSummaryRow{ParameterCount: len(rows)}
	for _, row := range rows {
		if row.RequiredType != "" {
			summary.RequiredCount++
		}
		if row.Name != "" {
			summary.NamedCount++
		} else {
			summary.PositionalCount++
		}
		if row.Nullable {
			summary.NullableCount++
		}
		if row.Present {
			summary.PresentCount++
		}
		if row.Bound {
			summary.BoundCount++
		}
		switch row.Diagnostic {
		case DiagnosticParameterMissing:
			summary.MissingCount++
		case DiagnosticParameterExtra:
			summary.ExtraCount++
		case DiagnosticParameterTypeMismatch:
			summary.TypeMismatchCount++
		case DiagnosticParameterNullNotAllowed:
			summary.NullNotAllowedCount++
		}
	}
	return summary
}

func firstParameterValue(ref ParameterRef, values []ParameterValue) (ParameterValue, bool) {
	for _, value := range values {
		if ref.Name != "" && value.Name == ref.Name {
			return value, true
		}
		if ref.Name == "" && value.Name == "" && value.Index == ref.Index {
			return value, true
		}
	}
	return ParameterValue{}, false
}

func parameterValueMatchesRequired(value ParameterValue, required []ParameterRef) bool {
	for _, ref := range required {
		if parameterValueKey(value) == parameterRefKey(ref) {
			return true
		}
	}
	return false
}

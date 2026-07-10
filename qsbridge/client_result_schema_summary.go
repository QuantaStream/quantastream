package qsbridge

// ClientResultSchemaColumnRow describes one protocol-visible result column.
type ClientResultSchemaColumnRow struct {
	StatementOrdinal int
	ColumnOrdinal    int
	StatementSQL     string
	AccessIntent     PhysicalAccessIntent
	Lifecycle        ClientPlanLifecycleKind
	LifecycleSteps   int
	Name             string
	Source           string
	Nullable         bool
	LogicalType      DataType
	TypeName         string
	WireType         string
	Flags            []ProtocolColumnFlag
}

// ClientResultSchemaSummaryExchange exposes result-preview schemas as rows.
type ClientResultSchemaSummaryExchange struct {
	Connection          ConnectionContext
	Preview             ClientResultPreviewBundle
	Rows                []ClientResultSchemaColumnRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientResultSchemas returns one row per query result column in a preview bundle.
func (s PlanningService) SummarizeClientResultSchemas(connection ConnectionContext, preview ClientResultPreviewBundle) ClientResultSchemaSummaryExchange {
	_ = s
	exchange := ClientResultSchemaSummaryExchange{
		Connection:          cloneConnectionContext(connection),
		Preview:             cloneClientResultPreviewBundle(preview),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = resultSchemaColumnRows(preview)
	}
	exchange.Result = exchange.resultSchemaSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether result-schema summary metadata can be returned.
func (e ClientResultSchemaSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientResultSchemaSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientResultSchemaSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientResultSchemaSummaryExchange) resultSchemaSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     resultSchemaSummaryColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.resultSchemaSummaryRows(),
		Final: true,
	})
}

func resultSchemaSummaryColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Statement_ordinal", Type: DataTypeInt},
		{Name: "Column_ordinal", Type: DataTypeInt},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Name", Type: DataTypeString},
		{Name: "Source", Type: DataTypeString, Nullable: true},
		{Name: "Logical_type", Type: DataTypeString, Nullable: true},
		{Name: "Type_name", Type: DataTypeString, Nullable: true},
		{Name: "Wire_type", Type: DataTypeString, Nullable: true},
		{Name: "Nullable", Type: DataTypeBool},
		{Name: "Flags", Type: DataTypeString, Nullable: true},
		{Name: "SQL", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientResultSchemaSummaryExchange) resultSchemaSummaryRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.StatementOrdinal),
			metadataIntCell(row.ColumnOrdinal),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataStringCell(row.Name),
			metadataStringCell(row.Source),
			metadataStringCell(string(row.LogicalType)),
			metadataStringCell(row.TypeName),
			metadataStringCell(row.WireType),
			metadataBoolCell(row.Nullable),
			metadataStringCell(joinProtocolColumnFlags(row.Flags)),
			metadataStringCell(row.StatementSQL),
		})
	}
	return rows
}

func resultSchemaColumnRows(preview ClientResultPreviewBundle) []ClientResultSchemaColumnRow {
	rows := make([]ClientResultSchemaColumnRow, 0)
	for _, statement := range preview.Statements {
		if !statement.HasSchema {
			continue
		}
		for i, column := range statement.Schema.Columns {
			rows = append(rows, ClientResultSchemaColumnRow{
				StatementOrdinal: statement.Statement.Ordinal,
				ColumnOrdinal:    i + 1,
				StatementSQL:     statement.Statement.SQL,
				AccessIntent:     statement.Outcome.AccessIntent,
				Lifecycle:        statement.Outcome.Lifecycle,
				LifecycleSteps:   statement.Outcome.LifecycleSteps,
				Name:             column.Name,
				Source:           column.Source,
				Nullable:         column.Nullable,
				LogicalType:      column.LogicalType,
				TypeName:         column.TypeName,
				WireType:         column.WireType,
				Flags:            append([]ProtocolColumnFlag(nil), column.Flags...),
			})
		}
	}
	return rows
}

func cloneClientResultPreviewBundle(preview ClientResultPreviewBundle) ClientResultPreviewBundle {
	preview.Connection = cloneConnectionContext(preview.Connection)
	preview.Diagnostics = cloneDiagnosticSet(preview.Diagnostics)
	preview.Statements = cloneClientStatementResultPreviews(preview.Statements)
	return preview
}

func cloneClientStatementResultPreviews(previews []ClientStatementResultPreview) []ClientStatementResultPreview {
	if len(previews) == 0 {
		return nil
	}
	cloned := make([]ClientStatementResultPreview, 0, len(previews))
	for _, preview := range previews {
		preview.Outcome.Diagnostics = cloneDiagnosticSet(preview.Outcome.Diagnostics)
		preview.Result = cloneExecutionResult(preview.Result)
		preview.Schema = cloneProtocolResultSchema(preview.Schema)
		preview.StatementResponse = cloneProtocolStatementResponse(preview.StatementResponse)
		cloned = append(cloned, preview)
	}
	return cloned
}

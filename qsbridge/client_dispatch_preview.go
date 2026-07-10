package qsbridge

// ClientDispatchPreviewRow describes one adapter-visible executor dispatch preview.
type ClientDispatchPreviewRow struct {
	Ordinal            int
	SQL                string
	Handoff            ExecutionHandoffKind
	Target             DispatchTarget
	AccessIntent       PhysicalAccessIntent
	Lifecycle          ClientPlanLifecycleKind
	LifecycleSteps     int
	Supported          bool
	ExecutorConfigured bool
	WillDispatch       bool
	Diagnostics        []DiagnosticCode
	Detail             string
}

// ClientDispatchPreviewExchange is adapter-facing non-executing dispatch metadata.
type ClientDispatchPreviewExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Rows                []ClientDispatchPreviewRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// ListClientDispatchPreviews returns executor dispatch preview rows for a handoff bundle.
func (s PlanningService) ListClientDispatchPreviews(dispatcher ExecutionDispatcher, bundle ClientHandoffBundle) ClientDispatchPreviewExchange {
	_ = s
	exchange := ClientDispatchPreviewExchange{
		Connection:          cloneConnectionContext(bundle.Connection),
		Diagnostics:         cloneDiagnosticSet(bundle.Diagnostics),
		ExchangeDiagnostics: cloneDiagnosticSet(bundle.Connection.Diagnostics),
	}
	if bundle.Connection.Supported() {
		exchange.Rows = clientDispatchPreviewRows(dispatcher, bundle)
	}
	exchange.Result = exchange.dispatchPreviewResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(bundle.Connection.Protocol)
	return exchange
}

// Supported reports whether dispatch-preview metadata can be returned.
func (e ClientDispatchPreviewExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientDispatchPreviewExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientDispatchPreviewExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientDispatchPreviewExchange) dispatchPreviewResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     dispatchPreviewResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.dispatchPreviewResultRows(),
		Final: true,
	})
}

func dispatchPreviewResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Ordinal", Type: DataTypeInt},
		{Name: "SQL", Type: DataTypeString},
		{Name: "Handoff", Type: DataTypeString},
		{Name: "Dispatch_target", Type: DataTypeString},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Executor_configured", Type: DataTypeBool},
		{Name: "Will_dispatch", Type: DataTypeBool},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
		{Name: "Detail", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientDispatchPreviewExchange) dispatchPreviewResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Ordinal),
			metadataStringCell(row.SQL),
			metadataStringCell(string(row.Handoff)),
			metadataStringCell(string(row.Target)),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataBoolCell(row.Supported),
			metadataBoolCell(row.ExecutorConfigured),
			metadataBoolCell(row.WillDispatch),
			metadataStringCell(joinDiagnosticCodes(row.Diagnostics)),
			metadataStringCell(row.Detail),
		})
	}
	return rows
}

func clientDispatchPreviewRows(dispatcher ExecutionDispatcher, bundle ClientHandoffBundle) []ClientDispatchPreviewRow {
	if len(bundle.Statements) == 0 {
		return nil
	}
	rows := make([]ClientDispatchPreviewRow, 0, len(bundle.Statements))
	for _, statement := range bundle.Statements {
		preview := dispatcher.PreviewProtocol(statement.Handoff)
		rows = append(rows, ClientDispatchPreviewRow{
			Ordinal:            statement.Statement.Ordinal,
			SQL:                statement.Statement.SQL,
			Handoff:            preview.Handoff,
			Target:             preview.Target,
			AccessIntent:       statement.Plan.Prepared.AccessIntent(),
			Lifecycle:          clientPlanLifecycleKind(statement.Plan.Prepared.Kind),
			LifecycleSteps:     clientPlanLifecycleStepCount(statement.Plan.Prepared.Kind),
			Supported:          preview.Supported,
			ExecutorConfigured: preview.ExecutorConfigured,
			WillDispatch:       preview.WillDispatch,
			Diagnostics:        append([]DiagnosticCode(nil), diagnosticCodes(preview.Diagnostics)...),
			Detail:             preview.Detail,
		})
	}
	return rows
}

package qsbridge

import "strings"

// ClientOptimizerTraceExchange is adapter-facing optimizer audit metadata.
type ClientOptimizerTraceExchange struct {
	Connection   ConnectionContext
	Trace        OptimizationTrace
	Rows         []RewriteRecord
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// PrepareClientOptimizerTrace returns protocol-neutral optimizer audit rows.
func (s PlanningService) PrepareClientOptimizerTrace(connection ConnectionContext, trace OptimizationTrace) ClientOptimizerTraceExchange {
	_ = s
	exchange := ClientOptimizerTraceExchange{
		Connection:  cloneConnectionContext(connection),
		Trace:       trace.Clone(),
		Diagnostics: mergeDiagnosticSets(cloneDiagnosticSet(connection.Diagnostics), trace.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = cloneRewriteRecords(trace.Rewrites)
	}
	exchange.Result = exchange.optimizerTraceResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether optimizer trace metadata can be returned.
func (e ClientOptimizerTraceExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts optimizer diagnostics into protocol-facing errors.
func (e ClientOptimizerTraceExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking optimizer trace error, if any.
func (e ClientOptimizerTraceExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientOptimizerTraceExchange) optimizerTraceResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     optimizerTraceResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.optimizerTraceRows(),
		Final: true,
	})
}

func optimizerTraceResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Rule", Type: DataTypeString},
		{Name: "Status", Type: DataTypeString},
		{Name: "Category", Type: DataTypeString},
		{Name: "Impact", Type: DataTypeString},
		{Name: "Reason", Type: DataTypeString, Nullable: true},
		{Name: "Before", Type: DataTypeString, Nullable: true},
		{Name: "After", Type: DataTypeString, Nullable: true},
		{Name: "Capabilities", Type: DataTypeString, Nullable: true},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
		{Name: "Fields", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientOptimizerTraceExchange) optimizerTraceRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, rewrite := range e.Rows {
		rows = append(rows, ResultRow{
			metadataStringCell(string(rewrite.Rule)),
			metadataStringCell(string(rewrite.Status)),
			metadataStringCell(string(rewrite.Category)),
			metadataStringCell(string(rewrite.Impact)),
			metadataStringCell(rewrite.Reason),
			metadataStringCell(rewrite.Before),
			metadataStringCell(rewrite.After),
			metadataStringCell(joinPlanCapabilities(rewrite.Capabilities)),
			metadataStringCell(joinDiagnosticCodes(diagnosticCodes(rewrite.Diagnostics))),
			metadataStringCell(joinStringValues(qualifiedFieldNames(rewrite.Fields))),
		})
	}
	return rows
}

func cloneRewriteRecords(records []RewriteRecord) []RewriteRecord {
	if len(records) == 0 {
		return nil
	}
	cloned := make([]RewriteRecord, 0, len(records))
	for _, record := range records {
		cloned = append(cloned, record.Clone())
	}
	return cloned
}

func joinPlanCapabilities(capabilities []PlanCapability) string {
	if len(capabilities) == 0 {
		return ""
	}
	values := make([]string, 0, len(capabilities))
	for _, capability := range capabilities {
		values = append(values, string(capability))
	}
	return joinStringValues(values)
}

func joinDiagnosticCodes(codes []DiagnosticCode) string {
	if len(codes) == 0 {
		return ""
	}
	values := make([]string, 0, len(codes))
	for _, code := range codes {
		values = append(values, string(code))
	}
	return joinStringValues(values)
}

func joinStringValues(values []string) string {
	return strings.Join(values, ",")
}

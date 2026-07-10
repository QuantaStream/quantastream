package qsbridge

import "sort"

// ClientGrantSummaryExchange is adapter-facing SHOW GRANTS summary metadata.
type ClientGrantSummaryExchange struct {
	Connection   ConnectionContext
	Grants       []AccessGrant
	Row          ClientGrantSummaryRow
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// SummarizeClientAccessGrants returns aggregate grant metadata for an access policy.
func (s PlanningService) SummarizeClientAccessGrants(connection ConnectionContext, policy AccessPolicy) ClientGrantSummaryExchange {
	_ = s
	exchange := ClientGrantSummaryExchange{
		Connection:  cloneConnectionContext(connection),
		Diagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Result = exchange.grantSummaryResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	exchange.Grants = filterAccessGrantsForSession(policy.Grants(), connection.Session)
	sort.Slice(exchange.Grants, func(i, j int) bool {
		return grantSortKey(exchange.Grants[i]) < grantSortKey(exchange.Grants[j])
	})
	exchange.Row = summarizeAccessGrants(exchange.Grants)
	exchange.Result = exchange.grantSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether grant summary metadata can be returned.
func (e ClientGrantSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts grant summary diagnostics into protocol-facing errors.
func (e ClientGrantSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking grant summary error, if any.
func (e ClientGrantSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientGrantSummaryExchange) grantSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     grantSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  []ResultRow{grantSummaryResultRow(e.Row)},
		Final: true,
	})
}

func grantSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Grant_count", Type: DataTypeInt},
		{Name: "User_grant_count", Type: DataTypeInt},
		{Name: "Role_grant_count", Type: DataTypeInt},
		{Name: "Select_count", Type: DataTypeInt},
		{Name: "Insert_count", Type: DataTypeInt},
		{Name: "Update_count", Type: DataTypeInt},
		{Name: "Delete_count", Type: DataTypeInt},
		{Name: "Table_count", Type: DataTypeInt},
		{Name: "Field_scoped_count", Type: DataTypeInt},
		{Name: "Field_mention_count", Type: DataTypeInt},
	}
}

func grantSummaryResultRow(row ClientGrantSummaryRow) ResultRow {
	return ResultRow{
		metadataIntCell(row.GrantCount),
		metadataIntCell(row.UserGrantCount),
		metadataIntCell(row.RoleGrantCount),
		metadataIntCell(row.SelectCount),
		metadataIntCell(row.InsertCount),
		metadataIntCell(row.UpdateCount),
		metadataIntCell(row.DeleteCount),
		metadataIntCell(row.TableCount),
		metadataIntCell(row.FieldScopedCount),
		metadataIntCell(row.FieldMentionCount),
	}
}

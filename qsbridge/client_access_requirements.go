package qsbridge

// ClientAccessRequirementRow describes one authorization requirement for a prepared plan.
type ClientAccessRequirementRow struct {
	Ordinal   int
	Privilege AccessPrivilege
	Schema    string
	Table     string
	Alias     string
	Fields    []FieldRef
}

// ClientAccessRequirementSummaryRow describes aggregate authorization requirement metadata.
type ClientAccessRequirementSummaryRow struct {
	RequirementCount int
	SelectCount      int
	InsertCount      int
	UpdateCount      int
	DeleteCount      int
	TableCount       int
	FieldCount       int
	HasMutation      bool
}

// ClientAccessRequirementExchange is adapter-facing access requirement metadata.
type ClientAccessRequirementExchange struct {
	Connection          ConnectionContext
	Prepared            PreparedPlan
	Diagnostics         DiagnosticSet
	Rows                []ClientAccessRequirementRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// ListClientAccessRequirements returns authorization requirements for one prepared plan.
func (s PlanningService) ListClientAccessRequirements(connection ConnectionContext, prepared PreparedPlan) ClientAccessRequirementExchange {
	_ = s
	exchange := ClientAccessRequirementExchange{
		Connection:          cloneConnectionContext(connection),
		Prepared:            clonePreparedPlan(prepared),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if connection.Supported() {
		exchange.Rows = accessRequirementRows(prepared.RequiredAccess())
	}
	exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
	exchange.Result = exchange.accessRequirementResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether access requirement metadata can be returned.
func (e ClientAccessRequirementExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientAccessRequirementExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientAccessRequirementExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientAccessRequirementExchange) accessRequirementResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     accessRequirementResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.accessRequirementResultRows(),
		Final: true,
	})
}

func accessRequirementResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Ordinal", Type: DataTypeInt},
		{Name: "Privilege", Type: DataTypeString},
		{Name: "Schema", Type: DataTypeString, Nullable: true},
		{Name: "Table", Type: DataTypeString},
		{Name: "Alias", Type: DataTypeString, Nullable: true},
		{Name: "Fields", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientAccessRequirementExchange) accessRequirementResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Ordinal),
			metadataStringCell(string(row.Privilege)),
			metadataStringCell(row.Schema),
			metadataStringCell(row.Table),
			metadataStringCell(row.Alias),
			metadataStringCell(joinStringValues(qualifiedFieldNames(row.Fields))),
		})
	}
	return rows
}

func accessRequirementRows(requirements []AccessRequirement) []ClientAccessRequirementRow {
	if len(requirements) == 0 {
		return nil
	}
	rows := make([]ClientAccessRequirementRow, 0, len(requirements))
	for index, requirement := range requirements {
		rows = append(rows, ClientAccessRequirementRow{
			Ordinal:   index + 1,
			Privilege: requirement.Privilege,
			Schema:    requirement.Table.Schema,
			Table:     requirement.Table.Table,
			Alias:     requirement.Table.Alias,
			Fields:    append([]FieldRef(nil), requirement.Fields...),
		})
	}
	return rows
}

func summarizeAccessRequirementRows(rows []ClientAccessRequirementRow) ClientAccessRequirementSummaryRow {
	summary := ClientAccessRequirementSummaryRow{RequirementCount: len(rows)}
	tables := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		switch row.Privilege {
		case AccessSelect:
			summary.SelectCount++
		case AccessInsert:
			summary.InsertCount++
			summary.HasMutation = true
		case AccessUpdate:
			summary.UpdateCount++
			summary.HasMutation = true
		case AccessDelete:
			summary.DeleteCount++
			summary.HasMutation = true
		}
		if row.Table != "" {
			tables[accessRequirementRowTableKey(row)] = struct{}{}
		}
		summary.FieldCount += len(row.Fields)
	}
	summary.TableCount = len(tables)
	return summary
}

func accessRequirementRowTableKey(row ClientAccessRequirementRow) string {
	return row.Schema + "\x00" + row.Table + "\x00" + row.Alias
}

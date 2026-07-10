package qsbridge

// ClientPreparedStatementRow describes one registered prepared statement handle.
type ClientPreparedStatementRow struct {
	StatementID    PreparedStatementID
	StatementName  string
	SQL            string
	Schema         string
	CatalogVersion CatalogVersion
	User           UserName
	Kind           QueryKind
	AccessIntent   PhysicalAccessIntent
	Lifecycle      ClientPlanLifecycleKind
	LifecycleSteps int
	Supported      bool
	Parameters     int
	ResultColumns  int
	Placement      PlacementPolicy
	CacheScope     CacheScope
	Diagnostics    []DiagnosticCode
}

// ClientPreparedStatementStatusSummaryRow describes aggregate prepared handle inventory metadata.
type ClientPreparedStatementStatusSummaryRow struct {
	StatementCount         int
	NamedStatementCount    int
	SupportedCount         int
	UnsupportedCount       int
	ReadIntentCount        int
	WriteIntentCount       int
	SelectLifecycleCount   int
	MutationLifecycleCount int
	ParameterCount         int
	ResultColumnCount      int
	DiagnosticCount        int
	PrimaryPlacementCount  int
	LocalPlacementCount    int
	FollowerPlacementCount int
	SessionCacheCount      int
}

// ClientPreparedStatementStatusExchange is adapter-facing prepared handle inventory.
type ClientPreparedStatementStatusExchange struct {
	Connection          ConnectionContext
	Diagnostics         DiagnosticSet
	Rows                []ClientPreparedStatementRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// ListClientPreparedStatements returns adapter-owned prepared handle metadata as rows.
func (s PlanningService) ListClientPreparedStatements(connection ConnectionContext, registry PreparedStatementRegistry) ClientPreparedStatementStatusExchange {
	_ = s
	exchange := ClientPreparedStatementStatusExchange{
		Connection:          cloneConnectionContext(connection),
		ExchangeDiagnostics: cloneDiagnosticSet(connection.Diagnostics),
	}
	if !connection.Supported() {
		exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
		exchange.Result = exchange.preparedStatementStatusResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
		return exchange
	}
	if registry == nil {
		exchange.ExchangeDiagnostics = mergeDiagnosticSets(exchange.ExchangeDiagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInvalidExecutionOption, PhaseExecute, "prepared statement registry is not configured"),
		})
	} else {
		exchange.Rows = preparedStatementRows(registry.List())
	}
	exchange.Diagnostics = cloneDiagnosticSet(exchange.ExchangeDiagnostics)
	exchange.Result = exchange.preparedStatementStatusResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(connection.Protocol)
	return exchange
}

// Supported reports whether prepared handle inventory metadata can be returned.
func (e ClientPreparedStatementStatusExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientPreparedStatementStatusExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientPreparedStatementStatusExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientPreparedStatementStatusExchange) preparedStatementStatusResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     preparedStatementStatusResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.preparedStatementStatusRows(),
		Final: true,
	})
}

func preparedStatementStatusResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Statement_id", Type: DataTypeInt},
		{Name: "Statement_name", Type: DataTypeString, Nullable: true},
		{Name: "Schema", Type: DataTypeString, Nullable: true},
		{Name: "Catalog_version", Type: DataTypeString, Nullable: true},
		{Name: "User", Type: DataTypeString, Nullable: true},
		{Name: "Kind", Type: DataTypeString, Nullable: true},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Parameters", Type: DataTypeInt},
		{Name: "Result_columns", Type: DataTypeInt},
		{Name: "Placement", Type: DataTypeString, Nullable: true},
		{Name: "Cache_scope", Type: DataTypeString, Nullable: true},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
		{Name: "SQL", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPreparedStatementStatusExchange) preparedStatementStatusRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(int(row.StatementID)),
			metadataStringCell(row.StatementName),
			metadataStringCell(row.Schema),
			metadataStringCell(string(row.CatalogVersion)),
			metadataStringCell(string(row.User)),
			metadataStringCell(string(row.Kind)),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataBoolCell(row.Supported),
			metadataIntCell(row.Parameters),
			metadataIntCell(row.ResultColumns),
			metadataStringCell(string(row.Placement)),
			metadataStringCell(string(row.CacheScope)),
			metadataStringCell(joinDiagnosticCodes(row.Diagnostics)),
			metadataStringCell(row.SQL),
		})
	}
	return rows
}

func preparedStatementRows(plans []PreparedPlan) []ClientPreparedStatementRow {
	if len(plans) == 0 {
		return nil
	}
	rows := make([]ClientPreparedStatementRow, 0, len(plans))
	for _, plan := range plans {
		rows = append(rows, ClientPreparedStatementRow{
			StatementID:    plan.Handle.ID,
			StatementName:  plan.Handle.Name,
			SQL:            plan.SQL,
			Schema:         plan.DefaultSchema,
			CatalogVersion: plan.CatalogVersion,
			User:           plan.Session.User,
			Kind:           plan.Kind,
			AccessIntent:   plan.AccessIntent(),
			Lifecycle:      clientPlanLifecycleKind(plan.Kind),
			LifecycleSteps: clientPlanLifecycleStepCount(plan.Kind),
			Supported:      plan.Supported && !plan.Diagnostics.BlocksNative(),
			Parameters:     len(plan.Parameters),
			ResultColumns:  len(plan.ResultColumns),
			Placement:      plan.Scope.Placement,
			CacheScope:     plan.Scope.Cache,
			Diagnostics:    append([]DiagnosticCode(nil), diagnosticCodes(plan.Diagnostics)...),
		})
	}
	return rows
}

func summarizePreparedStatementRows(rows []ClientPreparedStatementRow) ClientPreparedStatementStatusSummaryRow {
	summary := ClientPreparedStatementStatusSummaryRow{StatementCount: len(rows)}
	for _, row := range rows {
		if row.StatementName != "" {
			summary.NamedStatementCount++
		}
		if row.Supported {
			summary.SupportedCount++
		} else {
			summary.UnsupportedCount++
		}
		switch row.AccessIntent {
		case PhysicalAccessRead:
			summary.ReadIntentCount++
		case PhysicalAccessWrite:
			summary.WriteIntentCount++
		}
		switch row.Lifecycle {
		case ClientPlanLifecycleSelect:
			summary.SelectLifecycleCount++
		case ClientPlanLifecycleMutation:
			summary.MutationLifecycleCount++
		}
		summary.ParameterCount += row.Parameters
		summary.ResultColumnCount += row.ResultColumns
		if len(row.Diagnostics) > 0 {
			summary.DiagnosticCount++
		}
		switch row.Placement {
		case PlacementPrimary:
			summary.PrimaryPlacementCount++
		case PlacementLocal:
			summary.LocalPlacementCount++
		case PlacementFollower:
			summary.FollowerPlacementCount++
		}
		if row.CacheScope == CacheSession {
			summary.SessionCacheCount++
		}
	}
	return summary
}

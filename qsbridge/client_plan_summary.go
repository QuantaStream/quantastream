package qsbridge

// ClientPlanLifecycleKind identifies which planning lifecycle view applies to a statement.
type ClientPlanLifecycleKind string

const (
	// ClientPlanLifecycleUnsupported means no detailed lifecycle view is available.
	ClientPlanLifecycleUnsupported ClientPlanLifecycleKind = "unsupported"
	// ClientPlanLifecycleSelect means the SELECT lifecycle view applies.
	ClientPlanLifecycleSelect ClientPlanLifecycleKind = "select"
	// ClientPlanLifecycleMutation means the mutation lifecycle view applies.
	ClientPlanLifecycleMutation ClientPlanLifecycleKind = "mutation"
)

// ClientPlanSummaryRow describes one statement in a client planning bundle.
type ClientPlanSummaryRow struct {
	Ordinal            int
	Kind               QueryKind
	User               UserName
	Schema             string
	CatalogVersion     CatalogVersion
	Supported          bool
	AccessIntent       PhysicalAccessIntent
	Lifecycle          ClientPlanLifecycleKind
	LifecycleSteps     int
	LogicalRoot        PlanNodeKind
	PhysicalRoot       PhysicalNodeKind
	Parameters         int
	ResultColumns      int
	AccessRequirements int
	Placement          PlacementPolicy
	CacheScope         CacheScope
	SQLLength          int
	DiagnosticCodes    []DiagnosticCode
}

// ClientPlanSummaryExchange is adapter-facing client planning metadata.
type ClientPlanSummaryExchange struct {
	Connection          ConnectionContext
	Plan                ClientPlanBundle
	Rows                []ClientPlanSummaryRow
	Result              ExecutionResult
	ResultSchema        ProtocolResultSchema
	ExchangeDiagnostics DiagnosticSet
}

// SummarizeClientPlanBundle returns row metadata for a prepared client plan bundle.
func (s PlanningService) SummarizeClientPlanBundle(bundle ClientPlanBundle) ClientPlanSummaryExchange {
	_ = s
	exchange := ClientPlanSummaryExchange{
		Connection:          cloneConnectionContext(bundle.Connection),
		Plan:                cloneClientPlanBundle(bundle),
		ExchangeDiagnostics: cloneClientPlanSummaryDiagnostics(bundle),
	}
	if !exchange.ExchangeDiagnostics.BlocksNative() {
		exchange.Rows = clientPlanSummaryRows(bundle)
	}
	exchange.Result = exchange.clientPlanSummaryResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(bundle.Connection.Protocol)
	return exchange
}

// Supported reports whether plan summary metadata can be returned.
func (e ClientPlanSummaryExchange) Supported() bool {
	return e.Connection.Supported() && !e.ExchangeDiagnostics.BlocksNative()
}

// ProtocolErrors converts exchange diagnostics into protocol-facing errors.
func (e ClientPlanSummaryExchange) ProtocolErrors() []ProtocolError {
	return e.ExchangeDiagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking exchange error, if any.
func (e ClientPlanSummaryExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.ExchangeDiagnostics.FirstProtocolError()
}

func (e ClientPlanSummaryExchange) clientPlanSummaryResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     clientPlanSummaryResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.ExchangeDiagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.clientPlanSummaryResultRows(),
		Final: true,
	})
}

func clientPlanSummaryResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Ordinal", Type: DataTypeInt},
		{Name: "Kind", Type: DataTypeString, Nullable: true},
		{Name: "User", Type: DataTypeString, Nullable: true},
		{Name: "Schema", Type: DataTypeString, Nullable: true},
		{Name: "Catalog_version", Type: DataTypeString, Nullable: true},
		{Name: "Supported", Type: DataTypeBool},
		{Name: "Access_intent", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle", Type: DataTypeString, Nullable: true},
		{Name: "Lifecycle_steps", Type: DataTypeInt},
		{Name: "Logical_root", Type: DataTypeString, Nullable: true},
		{Name: "Physical_root", Type: DataTypeString, Nullable: true},
		{Name: "Parameters", Type: DataTypeInt},
		{Name: "Result_columns", Type: DataTypeInt},
		{Name: "Access_requirements", Type: DataTypeInt},
		{Name: "Placement", Type: DataTypeString, Nullable: true},
		{Name: "Cache_scope", Type: DataTypeString, Nullable: true},
		{Name: "SQL_length", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientPlanSummaryExchange) clientPlanSummaryResultRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Rows))
	for _, row := range e.Rows {
		rows = append(rows, ResultRow{
			metadataIntCell(row.Ordinal),
			metadataStringCell(string(row.Kind)),
			metadataStringCell(string(row.User)),
			metadataStringCell(row.Schema),
			metadataStringCell(string(row.CatalogVersion)),
			metadataBoolCell(row.Supported),
			metadataStringCell(string(row.AccessIntent)),
			metadataStringCell(string(row.Lifecycle)),
			metadataIntCell(row.LifecycleSteps),
			metadataStringCell(string(row.LogicalRoot)),
			metadataStringCell(string(row.PhysicalRoot)),
			metadataIntCell(row.Parameters),
			metadataIntCell(row.ResultColumns),
			metadataIntCell(row.AccessRequirements),
			metadataStringCell(string(row.Placement)),
			metadataStringCell(string(row.CacheScope)),
			metadataIntCell(row.SQLLength),
			metadataStringCell(joinDiagnosticCodes(row.DiagnosticCodes)),
		})
	}
	return rows
}

func clientPlanSummaryRows(bundle ClientPlanBundle) []ClientPlanSummaryRow {
	rows := make([]ClientPlanSummaryRow, 0, len(bundle.Statements))
	for _, statement := range bundle.Statements {
		rows = append(rows, clientPlanSummaryRow(statement))
	}
	return rows
}

func clientPlanSummaryRow(statement ClientStatementPlan) ClientPlanSummaryRow {
	prepared := statement.Prepared
	return ClientPlanSummaryRow{
		Ordinal:            statement.Statement.Ordinal,
		Kind:               prepared.Kind,
		User:               prepared.Session.User,
		Schema:             prepared.DefaultSchema,
		CatalogVersion:     prepared.CatalogVersion,
		Supported:          prepared.Supported && !prepared.Diagnostics.BlocksNative(),
		AccessIntent:       prepared.AccessIntent(),
		Lifecycle:          clientPlanLifecycleKind(prepared.Kind),
		LifecycleSteps:     clientPlanLifecycleStepCount(prepared.Kind),
		LogicalRoot:        logicalRootKind(prepared.Logical),
		PhysicalRoot:       physicalRootKind(prepared.Physical),
		Parameters:         len(prepared.Parameters),
		ResultColumns:      len(prepared.ResultColumns),
		AccessRequirements: len(prepared.Access),
		Placement:          prepared.Scope.Placement,
		CacheScope:         prepared.Scope.Cache,
		SQLLength:          len(prepared.SQL),
		DiagnosticCodes:    prepared.Diagnostics.Codes(),
	}
}

func cloneClientPlanSummaryDiagnostics(bundle ClientPlanBundle) DiagnosticSet {
	diagnostics := cloneDiagnosticSet(bundle.Connection.Diagnostics)
	if len(bundle.Statements) == 0 {
		diagnostics = mergeDiagnosticSets(diagnostics, bundle.Diagnostics)
	}
	return diagnostics
}

func cloneClientPlanBundle(bundle ClientPlanBundle) ClientPlanBundle {
	bundle.Connection = cloneConnectionContext(bundle.Connection)
	bundle.Diagnostics = cloneDiagnosticSet(bundle.Diagnostics)
	if len(bundle.Statements) > 0 {
		statements := make([]ClientStatementPlan, 0, len(bundle.Statements))
		for _, statement := range bundle.Statements {
			statements = append(statements, cloneClientStatementPlan(statement))
		}
		bundle.Statements = statements
	}
	return bundle
}

func logicalRootKind(plan LogicalPlan) PlanNodeKind {
	if plan.Root == nil {
		return ""
	}
	return plan.Root.NodeKind()
}

func physicalRootKind(plan PhysicalPlan) PhysicalNodeKind {
	if plan.Root == nil {
		return ""
	}
	return plan.Root.PhysicalKind()
}

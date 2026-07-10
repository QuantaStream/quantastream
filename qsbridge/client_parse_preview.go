package qsbridge

// ClientParseStatement is parser-only metadata for one client statement.
type ClientParseStatement struct {
	Statement   ClientStatementText
	Unbound     UnboundStatement
	Diagnostics DiagnosticSet
}

// ClientParsePreviewSummaryRow describes aggregate parser-preview metadata.
type ClientParsePreviewSummaryRow struct {
	StatementCount  int
	SelectCount     int
	InsertCount     int
	UpdateCount     int
	DeleteCount     int
	SessionCount    int
	TableCount      int
	ProjectionCount int
	JoinCount       int
	MembershipCount int
	PredicateCount  int
	GroupByCount    int
	AggregateCount  int
	HavingCount     int
	OrderByCount    int
	BlockerCount    int
	DiagnosticCount int
}

// ClientParsePreviewExchange is adapter-facing parser bridge metadata.
type ClientParsePreviewExchange struct {
	Connection   ConnectionContext
	Statements   []ClientParseStatement
	Result       ExecutionResult
	ResultSchema ProtocolResultSchema
	Diagnostics  DiagnosticSet
}

// PreviewClientParse parses client statements without binding, planning, or executing.
func (s PlanningService) PreviewClientParse(bundle ClientStatementBundle) ClientParsePreviewExchange {
	exchange := ClientParsePreviewExchange{
		Connection:  cloneConnectionContext(bundle.Connection),
		Diagnostics: cloneDiagnosticSet(bundle.Diagnostics),
	}
	if !bundle.Supported() {
		exchange.Result = exchange.parsePreviewResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(bundle.Connection.Protocol)
		return exchange
	}
	if s.Planner.Parser == nil {
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, DiagnosticSet{
			ErrorDiagnostic(DiagnosticInternalInvariant, PhaseParse, "parser bridge is nil"),
		})
		exchange.Result = exchange.parsePreviewResult()
		exchange.ResultSchema = exchange.Result.ProtocolResultSchema(bundle.Connection.Protocol)
		return exchange
	}
	exchange.Statements = make([]ClientParseStatement, 0, len(bundle.Statements))
	for _, statement := range bundle.Statements {
		unbound, diagnostics := s.Planner.Parser.Parse(statement.SQL)
		if unbound.SQL == "" {
			unbound.SQL = statement.SQL
		}
		row := ClientParseStatement{
			Statement:   statement,
			Unbound:     cloneUnboundStatement(unbound),
			Diagnostics: cloneDiagnosticSet(diagnostics),
		}
		exchange.Statements = append(exchange.Statements, row)
		exchange.Diagnostics = mergeDiagnosticSets(exchange.Diagnostics, diagnostics)
	}
	exchange.Result = exchange.parsePreviewResult()
	exchange.ResultSchema = exchange.Result.ProtocolResultSchema(bundle.Connection.Protocol)
	return exchange
}

// Supported reports whether every statement parsed without blocking diagnostics.
func (e ClientParsePreviewExchange) Supported() bool {
	return e.Connection.Supported() && !e.Diagnostics.BlocksNative()
}

// ProtocolErrors converts parse preview diagnostics into protocol-facing errors.
func (e ClientParsePreviewExchange) ProtocolErrors() []ProtocolError {
	return e.Diagnostics.ProtocolErrors()
}

// FirstProtocolError returns the first blocking parse preview error, if any.
func (e ClientParsePreviewExchange) FirstProtocolError() (ProtocolError, bool) {
	return e.Diagnostics.FirstProtocolError()
}

func (e ClientParsePreviewExchange) parsePreviewResult() ExecutionResult {
	result := ExecutionResult{
		Status:      ExecutionPending,
		Kind:        ResultQuery,
		Columns:     parsePreviewResultColumns(),
		Diagnostics: cloneDiagnosticSet(e.Diagnostics),
	}
	if result.Diagnostics.BlocksNative() {
		result.Status = ExecutionFailed
		result.Complete = true
		return result
	}
	return result.WithChunk(ResultChunk{
		Rows:  e.parsePreviewRows(),
		Final: true,
	})
}

func parsePreviewResultColumns() []ResultColumn {
	return []ResultColumn{
		{Name: "Ordinal", Type: DataTypeInt},
		{Name: "Kind", Type: DataTypeString, Nullable: true},
		{Name: "Tables", Type: DataTypeInt},
		{Name: "Projections", Type: DataTypeInt},
		{Name: "Joins", Type: DataTypeInt},
		{Name: "Memberships", Type: DataTypeInt},
		{Name: "Predicates", Type: DataTypeInt},
		{Name: "Group_by", Type: DataTypeInt},
		{Name: "Aggregates", Type: DataTypeInt},
		{Name: "Having", Type: DataTypeInt},
		{Name: "Order_by", Type: DataTypeInt},
		{Name: "Blockers", Type: DataTypeInt},
		{Name: "Diagnostics", Type: DataTypeString, Nullable: true},
		{Name: "SQL", Type: DataTypeString, Nullable: true},
	}
}

func (e ClientParsePreviewExchange) parsePreviewRows() []ResultRow {
	rows := make([]ResultRow, 0, len(e.Statements))
	for _, statement := range e.Statements {
		counts := unboundStatementCounts(statement.Unbound)
		rows = append(rows, ResultRow{
			metadataIntCell(statement.Statement.Ordinal),
			metadataStringCell(string(statement.Unbound.Kind)),
			metadataIntCell(counts.tables),
			metadataIntCell(counts.projections),
			metadataIntCell(counts.joins),
			metadataIntCell(counts.memberships),
			metadataIntCell(counts.predicates),
			metadataIntCell(counts.groupBy),
			metadataIntCell(counts.aggregates),
			metadataIntCell(counts.having),
			metadataIntCell(counts.orderBy),
			metadataIntCell(counts.blockers),
			metadataStringCell(joinDiagnosticCodes(statement.Diagnostics.Codes())),
			metadataStringCell(statement.Unbound.SQL),
		})
	}
	return rows
}

type unboundStatementCount struct {
	tables      int
	projections int
	joins       int
	memberships int
	predicates  int
	groupBy     int
	aggregates  int
	having      int
	orderBy     int
	blockers    int
}

func unboundStatementCounts(statement UnboundStatement) unboundStatementCount {
	switch statement.Kind {
	case QueryKindSelect:
		return unboundStatementCount{
			tables:      len(statement.Select.Tables),
			projections: len(statement.Select.Projection),
			joins:       len(statement.Select.Joins),
			memberships: len(statement.Select.Memberships),
			predicates:  len(statement.Select.Predicates),
			groupBy:     len(statement.Select.GroupBy),
			aggregates:  len(statement.Select.Aggregates),
			having:      len(statement.Select.Having),
			orderBy:     len(statement.Select.OrderBy),
			blockers:    len(statement.Select.Blockers),
		}
	case QueryKindInsert:
		return unboundStatementCount{
			tables:      1,
			projections: len(statement.Insert.Columns),
			blockers:    len(statement.Insert.Blockers),
		}
	case QueryKindUpdate:
		return unboundStatementCount{
			tables:      1,
			projections: len(statement.Update.Assignments),
			predicates:  len(statement.Update.Predicates),
			blockers:    len(statement.Update.Blockers),
		}
	case QueryKindDelete:
		return unboundStatementCount{
			tables:     1,
			predicates: len(statement.Delete.Predicates),
			blockers:   len(statement.Delete.Blockers),
		}
	case QueryKindSession:
		return unboundStatementCount{
			projections: len(statement.Session.Actions),
			blockers:    len(statement.Session.Blockers),
		}
	default:
		return unboundStatementCount{}
	}
}

func cloneUnboundStatement(statement UnboundStatement) UnboundStatement {
	statement.Select.Tables = append([]UnboundTable(nil), statement.Select.Tables...)
	statement.Select.Projection = append([]UnboundProjection(nil), statement.Select.Projection...)
	statement.Select.Joins = append([]UnboundJoin(nil), statement.Select.Joins...)
	statement.Select.Memberships = append([]UnboundMembership(nil), statement.Select.Memberships...)
	statement.Select.Predicates = append([]UnboundPredicate(nil), statement.Select.Predicates...)
	statement.Select.WhereExpr = cloneUnboundExpr(statement.Select.WhereExpr)
	statement.Select.GroupBy = append([]UnboundExpr(nil), statement.Select.GroupBy...)
	statement.Select.Aggregates = append([]UnboundAggregate(nil), statement.Select.Aggregates...)
	statement.Select.Having = append([]UnboundPredicate(nil), statement.Select.Having...)
	statement.Select.OrderBy = append([]UnboundSort(nil), statement.Select.OrderBy...)
	statement.Select.Blockers = append([]NativeBlocker(nil), statement.Select.Blockers...)
	statement.Insert.Columns = append([]string(nil), statement.Insert.Columns...)
	statement.Insert.Rows = cloneUnboundRows(statement.Insert.Rows)
	statement.Insert.Blockers = append([]NativeBlocker(nil), statement.Insert.Blockers...)
	statement.Update.Assignments = append([]UnboundAssignment(nil), statement.Update.Assignments...)
	statement.Update.Predicates = append([]UnboundPredicate(nil), statement.Update.Predicates...)
	statement.Update.Blockers = append([]NativeBlocker(nil), statement.Update.Blockers...)
	statement.Delete.Predicates = append([]UnboundPredicate(nil), statement.Delete.Predicates...)
	statement.Delete.Blockers = append([]NativeBlocker(nil), statement.Delete.Blockers...)
	statement.Session.Actions = append([]SessionAction(nil), statement.Session.Actions...)
	statement.Session.Blockers = append([]NativeBlocker(nil), statement.Session.Blockers...)
	return statement
}

func cloneUnboundExpr(expr UnboundExpr) UnboundExpr {
	switch typed := expr.(type) {
	case nil:
		return nil
	case UnboundListExpr:
		return UnboundListExpr{Items: cloneUnboundExprs(typed.Items)}
	case UnboundCallExpr:
		return UnboundCallExpr{Name: typed.Name, Args: cloneUnboundExprs(typed.Args)}
	case UnboundBinaryExpr:
		return UnboundBinaryExpr{Op: typed.Op, Left: cloneUnboundExpr(typed.Left), Right: cloneUnboundExpr(typed.Right)}
	case UnboundSearchedCaseExpr:
		whens := make([]UnboundSearchedCaseWhen, 0, len(typed.Whens))
		for _, when := range typed.Whens {
			whens = append(whens, UnboundSearchedCaseWhen{
				Condition: cloneUnboundExpr(when.Condition),
				Result:    cloneUnboundExpr(when.Result),
			})
		}
		return UnboundSearchedCaseExpr{Whens: whens, Else: cloneUnboundExpr(typed.Else)}
	default:
		return expr
	}
}

func cloneUnboundExprs(exprs []UnboundExpr) []UnboundExpr {
	cloned := make([]UnboundExpr, 0, len(exprs))
	for _, expr := range exprs {
		cloned = append(cloned, cloneUnboundExpr(expr))
	}
	return cloned
}

func cloneUnboundRows(rows [][]UnboundExpr) [][]UnboundExpr {
	if len(rows) == 0 {
		return nil
	}
	cloned := make([][]UnboundExpr, 0, len(rows))
	for _, row := range rows {
		cloned = append(cloned, append([]UnboundExpr(nil), row...))
	}
	return cloned
}

func cloneClientParseStatements(statements []ClientParseStatement) []ClientParseStatement {
	if len(statements) == 0 {
		return nil
	}
	cloned := make([]ClientParseStatement, 0, len(statements))
	for _, statement := range statements {
		cloned = append(cloned, ClientParseStatement{
			Statement:   statement.Statement,
			Unbound:     cloneUnboundStatement(statement.Unbound),
			Diagnostics: cloneDiagnosticSet(statement.Diagnostics),
		})
	}
	return cloned
}

func summarizeClientParsePreview(statements []ClientParseStatement) ClientParsePreviewSummaryRow {
	summary := ClientParsePreviewSummaryRow{StatementCount: len(statements)}
	for _, statement := range statements {
		switch statement.Unbound.Kind {
		case QueryKindSelect:
			summary.SelectCount++
		case QueryKindInsert:
			summary.InsertCount++
		case QueryKindUpdate:
			summary.UpdateCount++
		case QueryKindDelete:
			summary.DeleteCount++
		case QueryKindSession:
			summary.SessionCount++
		}
		counts := unboundStatementCounts(statement.Unbound)
		summary.TableCount += counts.tables
		summary.ProjectionCount += counts.projections
		summary.JoinCount += counts.joins
		summary.MembershipCount += counts.memberships
		summary.PredicateCount += counts.predicates
		summary.GroupByCount += counts.groupBy
		summary.AggregateCount += counts.aggregates
		summary.HavingCount += counts.having
		summary.OrderByCount += counts.orderBy
		summary.BlockerCount += counts.blockers
		summary.DiagnosticCount += len(statement.Diagnostics)
	}
	return summary
}

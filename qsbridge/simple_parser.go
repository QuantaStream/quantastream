package qsbridge

import (
	"fmt"
	"strconv"
	"strings"
)

// SimpleParserBridge parses the smallest qsbridge-native SELECT surface.
//
// It is not a full SQL parser. It exists to prove the parser boundary with
// real SQL text while the production parser strategy remains open. Supported
// shape: SELECT direct fields or aggregates with optional AS aliases FROM one
// or more tables with optional table aliases and equality joins, optional
// AND-combined comparisons, ORDER BY, LIMIT, and OFFSET.
type SimpleParserBridge struct{}

// Parse converts a narrow one-table SELECT into an UnboundStatement.
func (SimpleParserBridge) Parse(sql string) (UnboundStatement, DiagnosticSet) {
	statement, diagnostic, ok := parseSimpleStatement(sql)
	if !ok {
		return UnboundStatement{SQL: sql}, DiagnosticSet{diagnostic}
	}
	return statement, nil
}

func parseSimpleStatement(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	if trimmed == "" {
		return UnboundStatement{}, simpleParserDiagnostic("empty SQL"), false
	}
	if _, ok := consumeKeyword(trimmed, "select"); ok {
		return parseSimpleSelect(sql)
	}
	if _, ok := consumeKeyword(trimmed, "insert"); ok {
		return parseSimpleInsert(sql)
	}
	if _, ok := consumeKeyword(trimmed, "update"); ok {
		return parseSimpleUpdate(sql)
	}
	if _, ok := consumeKeyword(trimmed, "delete"); ok {
		return parseSimpleDelete(sql)
	}
	if _, ok := consumeKeyword(trimmed, "truncate"); ok {
		return parseSimpleTruncate(sql)
	}
	if _, ok := consumeKeyword(trimmed, "create"); ok {
		return parseSimpleCreate(sql)
	}
	if _, ok := consumeKeyword(trimmed, "drop"); ok {
		return parseSimpleDrop(sql)
	}
	if _, ok := consumeKeyword(trimmed, "show"); ok {
		return parseSimpleShow(sql)
	}
	if _, ok := consumeKeyword(trimmed, "explain"); ok {
		return parseSimpleExplain(sql)
	}
	if _, ok := consumeKeyword(trimmed, "describe"); ok {
		return parseSimpleDescribe(sql)
	}
	if _, ok := consumeKeyword(trimmed, "desc"); ok {
		return parseSimpleDescribe(sql)
	}
	if _, ok := consumeKeyword(trimmed, "use"); ok {
		return parseSimpleUse(sql)
	}
	if _, ok := consumeKeyword(trimmed, "set"); ok {
		return parseSimpleSet(sql)
	}
	if _, ok := consumeKeyword(trimmed, "begin"); ok {
		return parseSimpleBegin(sql)
	}
	if _, ok := consumeKeyword(trimmed, "start"); ok {
		return parseSimpleStartTransaction(sql)
	}
	if _, ok := consumeKeyword(trimmed, "rollback"); ok {
		return parseSimpleRollback(sql)
	}
	if _, ok := consumeKeyword(trimmed, "commit"); ok {
		return parseSimpleCommit(sql)
	}
	return UnboundStatement{}, simpleParserDiagnostic("only SELECT, INSERT, UPDATE, DELETE, TRUNCATE, CREATE TABLE, CREATE VIEW, DROP TABLE, DROP VIEW, SHOW CREATE VIEW, SHOW CREATE TABLE, SHOW CREATE DATABASE, SHOW DATABASES, SHOW TABLE STATUS, SHOW TABLES, SHOW FULL TABLES, SHOW VARIABLES, SHOW STATUS, SHOW WARNINGS, SHOW ERRORS, SHOW COUNT, SHOW CHARACTER SET, SHOW COLLATION, SHOW INDEX, SHOW COLUMNS, SHOW FULL COLUMNS, EXPLAIN, DESCRIBE, USE, SET, BEGIN, START TRANSACTION, ROLLBACK, and COMMIT statements are supported"), false
}

func parseSimpleSelect(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	if trimmed == "" {
		return UnboundStatement{}, simpleParserDiagnostic("empty SQL"), false
	}
	selectBody, ok := consumeKeyword(trimmed, "select")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only SELECT statements are supported"), false
	}
	distinct := false
	if remaining, ok := consumeKeyword(selectBody, "distinct"); ok {
		selectBody = remaining
		distinct = true
	}
	projectionText, sourceText, hasSource := splitBeforeTopLevelKeyword(selectBody, "from")
	if !hasSource {
		if _, _, found := findTopLevelSimpleKeyword(selectBody, "from"); found {
			return UnboundStatement{}, simpleParserDiagnostic("SELECT must include a projection list and FROM source"), false
		}
		projectionText = strings.TrimSpace(selectBody)
	}
	sourceText, limit, offset, hasLimit, diagnostic, ok := parseSimpleLimitClause(sourceText)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	sourceText, orderBy, hasOrderBy, diagnostic, ok := parseSimpleOrderByClause(sourceText)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	projections, aggregates, diagnostic, ok := parseSimpleProjections(projectionText)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	if !hasSource {
		if hasAnyTopLevelKeyword(projectionText, "where", "join", "group", "having", "order", "limit") {
			return UnboundStatement{}, simpleParserDiagnostic("projection-only SELECT only supports a SELECT list"), false
		}
		return UnboundStatement{
			SQL:  sql,
			Kind: QueryKindSelect,
			Select: UnboundSelect{
				Projection: projections,
				Aggregates: aggregates,
				Result:     ResultShape{Kind: ResultQuery, Limit: limit, HasLimit: hasLimit, Offset: offset, Distinct: distinct},
			},
		}, Diagnostic{}, true
	}
	sourceText, having, aggregates, hasHaving, diagnostic, ok := parseSimpleHavingClause(sourceText, projections, aggregates)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	sourceText, groupBy, hasGroupBy, diagnostic, ok := parseSimpleGroupByClause(sourceText)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	if hasGroupBy {
		groupBy = resolveSimpleGroupByProjections(groupBy, projections)
	}
	sourceOnlyText, whereText, hasWhere := splitOptionalKeyword(sourceText, "where")
	tables, joins, diagnostic, ok := parseSimpleSources(sourceOnlyText)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	if hasAnyKeyword(sourceOnlyText, "having") {
		return UnboundStatement{}, simpleParserDiagnostic("unexpected HAVING in table source"), false
	}
	if hasWhere && hasAnyTopLevelKeyword(whereText, "join", "group", "having", "order", "limit") {
		return UnboundStatement{}, simpleParserDiagnostic("only AND-combined comparison predicates are supported"), false
	}
	if hasOrderBy {
		for i, sort := range orderBy {
			orderBy[i], diagnostic, ok = resolveSimpleOrderByProjection(sort, projections, aggregates)
			if !ok {
				return UnboundStatement{}, diagnostic, false
			}
		}
	}
	predicates := []UnboundPredicate(nil)
	memberships := []UnboundMembership(nil)
	subqueries := []UnboundSubqueryPlanIntent(nil)
	whereExpr := UnboundExpr(nil)
	blockers := []NativeBlocker(nil)
	if hasWhere {
		predicates, memberships, whereExpr, subqueries, blockers, diagnostic, ok = parseSimpleWhere(whereText)
		if !ok {
			return UnboundStatement{}, diagnostic, false
		}
	}
	if !hasGroupBy {
		groupBy = nil
	}
	if !hasHaving {
		having = nil
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindSelect,
		Select: UnboundSelect{
			Tables:      tables,
			Projection:  projections,
			Aggregates:  aggregates,
			Joins:       joins,
			Memberships: memberships,
			Predicates:  predicates,
			WhereExpr:   whereExpr,
			Subqueries:  subqueries,
			GroupBy:     groupBy,
			Having:      having,
			OrderBy:     orderBy,
			Result:      ResultShape{Kind: ResultQuery, Limit: limit, HasLimit: hasLimit, Offset: offset, Distinct: distinct},
			Blockers:    blockers,
		},
	}, Diagnostic{}, true
}

func parseSimpleInsert(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	insertBody, ok := consumeKeyword(trimmed, "insert")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only INSERT statements are supported"), false
	}
	insertBody, ok = consumeKeyword(insertBody, "into")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("INSERT must include INTO"), false
	}
	targetText, valuesText, ok := splitBeforeKeyword(insertBody, "values")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("INSERT must include VALUES"), false
	}
	table, columns, diagnostic, ok := parseSimpleInsertTarget(targetText)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	rows, diagnostic, ok := parseSimpleInsertRows(valuesText, len(columns))
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindInsert,
		Insert: UnboundInsert{
			Table:   table,
			Columns: columns,
			Rows:    rows,
			Result:  ResultShape{Kind: ResultStatement},
		},
	}, Diagnostic{}, true
}

func parseSimpleUpdate(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	updateBody, ok := consumeKeyword(trimmed, "update")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only UPDATE statements are supported"), false
	}
	targetText, mutationText, ok := splitBeforeKeyword(updateBody, "set")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("UPDATE must include SET"), false
	}
	table, diagnostic, ok := parseSimpleTable(targetText)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	assignmentText, whereText, hasWhere := splitOptionalKeyword(mutationText, "where")
	assignments, parameterIndex, diagnostic, ok := parseSimpleUpdateAssignments(assignmentText)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	predicates := []UnboundPredicate(nil)
	if hasWhere {
		predicates, diagnostic, ok = parseSimpleUpdatePredicates(whereText, parameterIndex)
		if !ok {
			return UnboundStatement{}, diagnostic, false
		}
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindUpdate,
		Update: UnboundUpdate{
			Table:       table,
			Assignments: assignments,
			Predicates:  predicates,
			Result:      ResultShape{Kind: ResultStatement},
		},
	}, Diagnostic{}, true
}

func parseSimpleUpdateAssignments(text string) ([]UnboundAssignment, int, Diagnostic, bool) {
	parts := splitSimpleCommaList(text)
	assignments := make([]UnboundAssignment, 0, len(parts))
	parameterIndex := 1
	for _, part := range parts {
		op, left, right, ok := splitBeforeComparisonOperator(part)
		if !ok || op != BinaryOpEqual {
			return nil, 0, simpleParserDiagnostic("UPDATE assignments must be column = value"), false
		}
		column := strings.TrimSpace(left)
		if column == "" {
			return nil, 0, simpleParserDiagnostic("UPDATE assignment column is empty"), false
		}
		value, diagnostic, ok := parseSimpleComparisonValue(strings.TrimSpace(right), &parameterIndex)
		if !ok {
			return nil, 0, diagnostic, false
		}
		assignments = append(assignments, UnboundAssignment{Column: column, Value: value})
	}
	if len(assignments) == 0 {
		return nil, 0, simpleParserDiagnostic("UPDATE must include at least one assignment"), false
	}
	return assignments, parameterIndex, Diagnostic{}, true
}

func parseSimpleUpdatePredicates(text string, parameterIndex int) ([]UnboundPredicate, Diagnostic, bool) {
	if strings.TrimSpace(text) == "" {
		return nil, simpleParserDiagnostic("WHERE predicate is empty"), false
	}
	return parseSimplePredicatesWithParameterIndex(text, parameterIndex)
}

func parseSimpleDelete(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	deleteBody, ok := consumeKeyword(trimmed, "delete")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only DELETE statements are supported"), false
	}
	deleteBody, ok = consumeKeyword(deleteBody, "from")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("DELETE must include FROM"), false
	}
	targetText, whereText, hasWhere := splitOptionalKeyword(deleteBody, "where")
	table, diagnostic, ok := parseSimpleTable(targetText)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	predicates := []UnboundPredicate(nil)
	if hasWhere {
		predicates, diagnostic, ok = parseSimplePredicates(whereText)
		if !ok {
			return UnboundStatement{}, diagnostic, false
		}
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindDelete,
		Delete: UnboundDelete{
			Table:      table,
			Predicates: predicates,
			Result:     ResultShape{Kind: ResultStatement},
		},
	}, Diagnostic{}, true
}

func parseSimpleTruncate(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	truncateBody, ok := consumeKeyword(trimmed, "truncate")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only TRUNCATE statements are supported"), false
	}
	if remaining, ok := consumeKeyword(truncateBody, "table"); ok {
		truncateBody = remaining
	}
	if strings.TrimSpace(truncateBody) == "" {
		return UnboundStatement{}, simpleParserDiagnostic("TRUNCATE must include a table"), false
	}
	if hasAnyKeyword(truncateBody, "where", "partition", "cascade", "restrict") {
		return UnboundStatement{}, simpleParserDiagnostic("TRUNCATE only supports one table name"), false
	}
	table, diagnostic, ok := parseSimpleTable(truncateBody)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	if table.Alias != "" {
		return UnboundStatement{}, simpleParserDiagnostic("TRUNCATE table aliases are not supported"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindTruncate,
		Truncate: UnboundTruncate{
			Table:  table,
			Result: ResultShape{Kind: ResultStatement},
		},
	}, Diagnostic{}, true
}

func parseSimpleCreate(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	createBody, ok := consumeKeyword(trimmed, "create")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only CREATE statements are supported"), false
	}
	replace := false
	if remaining, ok := consumeKeyword(createBody, "or"); ok {
		remaining, ok = consumeKeyword(remaining, "replace")
		if !ok {
			return UnboundStatement{}, simpleParserDiagnostic("CREATE OR must be followed by REPLACE"), false
		}
		createBody = remaining
		replace = true
	}
	if remaining, ok := consumeKeyword(createBody, "table"); ok {
		if replace {
			return UnboundStatement{}, simpleParserDiagnostic("CREATE OR REPLACE TABLE is not supported"), false
		}
		return parseSimpleCreateTableBody(sql, remaining)
	}
	if remaining, ok := consumeKeyword(createBody, "view"); ok {
		return parseSimpleCreateViewBody(sql, remaining, replace)
	}
	return UnboundStatement{}, simpleParserDiagnostic("CREATE must include TABLE or VIEW"), false
}

func parseSimpleCreateTableBody(sql string, createBody string) (UnboundStatement, Diagnostic, bool) {
	if strings.TrimSpace(createBody) == "" {
		return UnboundStatement{}, simpleParserDiagnostic("CREATE TABLE must include a table"), false
	}
	if hasAnyKeyword(createBody, "if", "exists", "like", "as", "select", "temporary", "where", "partition") || strings.Contains(createBody, "(") {
		return UnboundStatement{}, simpleParserDiagnostic("CREATE TABLE only supports one YAML-backed table name"), false
	}
	table, diagnostic, ok := parseSimpleTable(createBody)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	if table.Alias != "" {
		return UnboundStatement{}, simpleParserDiagnostic("CREATE TABLE aliases are not supported"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindCreateTable,
		Create: UnboundCreateTable{
			Table:  table,
			Result: ResultShape{Kind: ResultStatement},
		},
	}, Diagnostic{}, true
}

func parseSimpleCreateViewBody(sql string, createBody string, replace bool) (UnboundStatement, Diagnostic, bool) {
	viewText, selectText, ok := splitBeforeTopLevelKeyword(createBody, "as")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("CREATE VIEW must include AS SELECT"), false
	}
	if strings.TrimSpace(viewText) == "" {
		return UnboundStatement{}, simpleParserDiagnostic("CREATE VIEW must include a view name"), false
	}
	viewNameText, inlineColumns, diagnostic, ok := parseSimpleCreateViewTarget(viewText)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	view, diagnostic, ok := parseSimpleTable(viewNameText)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	if view.Alias != "" {
		return UnboundStatement{}, simpleParserDiagnostic("CREATE VIEW aliases are not supported"), false
	}
	if _, ok := consumeKeyword(selectText, "select"); !ok {
		return UnboundStatement{}, simpleParserDiagnostic("CREATE VIEW must use a SELECT statement"), false
	}
	if len(inlineColumns) > 0 {
		selectText, diagnostic, ok = applySimpleCreateViewColumnAliases(selectText, inlineColumns)
		if !ok {
			return UnboundStatement{}, diagnostic, false
		}
	}
	if _, diagnostic, ok := parseSimpleSelect(selectText); !ok {
		return UnboundStatement{}, diagnostic, false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindCreateView,
		CreateView: UnboundCreateView{
			View:    view,
			SQL:     strings.TrimSpace(selectText),
			Replace: replace,
			Result:  ResultShape{Kind: ResultStatement},
		},
	}, Diagnostic{}, true
}

func parseSimpleCreateViewTarget(viewText string) (string, []string, Diagnostic, bool) {
	viewText = strings.TrimSpace(viewText)
	if !strings.Contains(viewText, "(") {
		if strings.Contains(viewText, ",") {
			return "", nil, simpleParserDiagnostic("CREATE VIEW only supports one view name"), false
		}
		return viewText, nil, Diagnostic{}, true
	}
	open := strings.Index(viewText, "(")
	close := strings.LastIndex(viewText, ")")
	if close != len(viewText)-1 || open < 0 || close < open {
		return "", nil, simpleParserDiagnostic("CREATE VIEW inline column list must be enclosed in parentheses"), false
	}
	viewName := strings.TrimSpace(viewText[:open])
	if viewName == "" {
		return "", nil, simpleParserDiagnostic("CREATE VIEW must include a view name"), false
	}
	columnText := strings.TrimSpace(viewText[open+1 : close])
	if columnText == "" {
		return "", nil, simpleParserDiagnostic("CREATE VIEW inline column list must not be empty"), false
	}
	columns := splitSimpleCommaList(columnText)
	for i, column := range columns {
		column = strings.TrimSpace(column)
		if column == "" || strings.Contains(column, ".") || len(strings.Fields(column)) != 1 {
			return "", nil, simpleParserDiagnostic("CREATE VIEW inline column names must be simple identifiers"), false
		}
		columns[i] = column
	}
	return viewName, columns, Diagnostic{}, true
}

func applySimpleCreateViewColumnAliases(selectText string, columns []string) (string, Diagnostic, bool) {
	selectBody, ok := consumeKeyword(selectText, "select")
	if !ok {
		return "", simpleParserDiagnostic("CREATE VIEW must use a SELECT statement"), false
	}
	projectionText, sourceText, hasSource := splitBeforeTopLevelKeyword(selectBody, "from")
	if !hasSource {
		projectionText = strings.TrimSpace(selectBody)
	}
	projectionParts := splitSimpleCommaList(projectionText)
	if len(projectionParts) != len(columns) {
		return "", simpleParserDiagnostic("CREATE VIEW inline column count must match SELECT projection count"), false
	}
	aliased := make([]string, 0, len(projectionParts))
	for index, part := range projectionParts {
		exprText, _, diagnostic, ok := parseSimpleProjectionAlias(part)
		if !ok {
			return "", diagnostic, false
		}
		if _, field := splitProjectionField(exprText); field == "*" {
			return "", simpleParserDiagnostic("CREATE VIEW inline column list requires explicit projections"), false
		}
		aliased = append(aliased, strings.TrimSpace(exprText)+" as "+columns[index])
	}
	result := "select " + strings.Join(aliased, ", ")
	if hasSource {
		result += " from " + strings.TrimSpace(sourceText)
	}
	return result, Diagnostic{}, true
}

func parseSimpleDrop(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	dropBody, ok := consumeKeyword(trimmed, "drop")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only DROP statements are supported"), false
	}
	if remaining, ok := consumeKeyword(dropBody, "table"); ok {
		return parseSimpleDropTableBody(sql, remaining)
	}
	if remaining, ok := consumeKeyword(dropBody, "view"); ok {
		return parseSimpleDropViewBody(sql, remaining)
	}
	return parseSimpleDropTableBody(sql, dropBody)
}

func parseSimpleDropTableBody(sql string, dropBody string) (UnboundStatement, Diagnostic, bool) {
	dropBody = strings.TrimSpace(dropBody)
	if dropBody == "" {
		return UnboundStatement{}, simpleParserDiagnostic("DROP TABLE must include a table"), false
	}
	ifExists := false
	if remaining, ok := consumeKeyword(dropBody, "if"); ok {
		existsRemaining, existsOK := consumeKeyword(remaining, "exists")
		if !existsOK {
			return UnboundStatement{}, simpleParserDiagnostic("DROP TABLE IF must be followed by EXISTS"), false
		}
		ifExists = true
		dropBody = strings.TrimSpace(existsRemaining)
	}
	if dropBody == "" {
		return UnboundStatement{}, simpleParserDiagnostic("DROP TABLE must include a table"), false
	}
	if hasAnyKeyword(dropBody, "if", "exists", "cascade", "restrict", "where", "partition") || strings.Contains(dropBody, ",") {
		return UnboundStatement{}, simpleParserDiagnostic("DROP TABLE only supports one table name"), false
	}
	table, diagnostic, ok := parseSimpleTable(dropBody)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	if table.Alias != "" {
		return UnboundStatement{}, simpleParserDiagnostic("DROP TABLE aliases are not supported"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindDropTable,
		Drop: UnboundDropTable{
			Table:    table,
			IfExists: ifExists,
			Result:   ResultShape{Kind: ResultStatement},
		},
	}, Diagnostic{}, true
}

func parseSimpleDropViewBody(sql string, dropBody string) (UnboundStatement, Diagnostic, bool) {
	dropBody = strings.TrimSpace(dropBody)
	if dropBody == "" {
		return UnboundStatement{}, simpleParserDiagnostic("DROP VIEW must include a view"), false
	}
	ifExists := false
	if remaining, ok := consumeKeyword(dropBody, "if"); ok {
		existsRemaining, existsOK := consumeKeyword(remaining, "exists")
		if !existsOK {
			return UnboundStatement{}, simpleParserDiagnostic("DROP VIEW IF must be followed by EXISTS"), false
		}
		ifExists = true
		dropBody = strings.TrimSpace(existsRemaining)
	}
	if dropBody == "" {
		return UnboundStatement{}, simpleParserDiagnostic("DROP VIEW must include a view"), false
	}
	if hasAnyKeyword(dropBody, "if", "exists", "cascade", "restrict", "where", "partition") || strings.Contains(dropBody, ",") {
		return UnboundStatement{}, simpleParserDiagnostic("DROP VIEW only supports one view name"), false
	}
	view, diagnostic, ok := parseSimpleTable(dropBody)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	if view.Alias != "" {
		return UnboundStatement{}, simpleParserDiagnostic("DROP VIEW aliases are not supported"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindDropView,
		DropView: UnboundDropView{
			View:     view,
			IfExists: ifExists,
			Result:   ResultShape{Kind: ResultStatement},
		},
	}, Diagnostic{}, true
}

func parseSimpleShow(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	showBody, ok := consumeKeyword(trimmed, "show")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only SHOW statements are supported"), false
	}
	createBody, ok := consumeKeyword(showBody, "create")
	if ok {
		databaseBody, databaseOK := consumeKeyword(createBody, "database")
		if !databaseOK {
			databaseBody, databaseOK = consumeKeyword(createBody, "schema")
		}
		if databaseOK {
			return parseSimpleShowCreateDatabase(sql, databaseBody)
		}
		viewBody, viewOK := consumeKeyword(createBody, "view")
		if viewOK {
			view, diagnostic, ok := parseSimpleTable(viewBody)
			if !ok {
				return UnboundStatement{}, diagnostic, false
			}
			if view.Alias != "" {
				return UnboundStatement{}, simpleParserDiagnostic("SHOW CREATE VIEW aliases are not supported"), false
			}
			return UnboundStatement{
				SQL:  sql,
				Kind: QueryKindShowCreateView,
				ShowView: UnboundShowCreateView{
					View:   view,
					Result: showCreateViewResultShape(),
				},
			}, Diagnostic{}, true
		}
		tableBody, tableOK := consumeKeyword(createBody, "table")
		if tableOK {
			table, diagnostic, ok := parseSimpleTable(tableBody)
			if !ok {
				return UnboundStatement{}, diagnostic, false
			}
			if table.Alias != "" {
				return UnboundStatement{}, simpleParserDiagnostic("SHOW CREATE TABLE aliases are not supported"), false
			}
			return UnboundStatement{
				SQL:  sql,
				Kind: QueryKindShowCreateTable,
				ShowTable: UnboundShowCreateTable{
					Table:  table,
					Result: showCreateTableResultShape(),
				},
			}, Diagnostic{}, true
		}
		return UnboundStatement{}, simpleParserDiagnostic("SHOW CREATE only supports DATABASE, SCHEMA, VIEW, or TABLE"), false
	}
	databasesBody, ok := consumeKeyword(showBody, "databases")
	if !ok {
		databasesBody, ok = consumeKeyword(showBody, "schemas")
	}
	if ok {
		return parseSimpleShowDatabases(sql, databasesBody)
	}
	scopedShowBody, scopedShow := stripSimpleShowScope(showBody)
	if scopedShow {
		variablesBody, ok := consumeKeyword(scopedShowBody, "variables")
		if ok {
			return parseSimpleShowVariables(sql, variablesBody)
		}
		statusBody, ok := consumeKeyword(scopedShowBody, "status")
		if ok {
			return parseSimpleShowStatus(sql, statusBody)
		}
		return UnboundStatement{}, simpleParserDiagnostic("SHOW scoped prefix only supports VARIABLES or STATUS"), false
	}
	tableBody, ok := consumeKeyword(showBody, "table")
	if ok {
		statusBody, statusOK := consumeKeyword(tableBody, "status")
		if statusOK {
			return parseSimpleShowTableStatus(sql, statusBody)
		}
		typesBody, typesOK := consumeKeyword(tableBody, "types")
		if typesOK {
			return parseSimpleShowTableTypes(sql, typesBody)
		}
		return UnboundStatement{}, simpleParserDiagnostic("SHOW TABLE only supports STATUS or TYPES"), false
	}
	tablesBody, ok := consumeKeyword(showBody, "tables")
	if ok {
		return parseSimpleShowTables(sql, tablesBody, false)
	}
	fullBody, ok := consumeKeyword(showBody, "full")
	if ok {
		tablesBody, ok := consumeKeyword(fullBody, "tables")
		if ok {
			return parseSimpleShowTables(sql, tablesBody, true)
		}
		processlistBody, ok := consumeKeyword(fullBody, "processlist")
		if ok {
			return parseSimpleShowProcesslist(sql, processlistBody, true)
		}
		columnsBody, ok := consumeKeyword(fullBody, "columns")
		if !ok {
			columnsBody, ok = consumeKeyword(fullBody, "fields")
		}
		if !ok {
			return UnboundStatement{}, simpleParserDiagnostic("SHOW FULL only supports TABLES, PROCESSLIST, or COLUMNS/FIELDS"), false
		}
		return parseSimpleShowColumns(sql, columnsBody, true)
	}
	openBody, ok := consumeKeyword(showBody, "open")
	if ok {
		tablesBody, ok := consumeKeyword(openBody, "tables")
		if !ok {
			return UnboundStatement{}, simpleParserDiagnostic("SHOW OPEN only supports TABLES"), false
		}
		return parseSimpleShowOpenTables(sql, tablesBody)
	}
	functionBody, ok := consumeKeyword(showBody, "function")
	if ok {
		statusBody, ok := consumeKeyword(functionBody, "status")
		if !ok {
			return UnboundStatement{}, simpleParserDiagnostic("SHOW FUNCTION only supports STATUS"), false
		}
		return parseSimpleShowFunctionStatus(sql, statusBody)
	}
	procedureBody, ok := consumeKeyword(showBody, "procedure")
	if ok {
		statusBody, ok := consumeKeyword(procedureBody, "status")
		if !ok {
			return UnboundStatement{}, simpleParserDiagnostic("SHOW PROCEDURE only supports STATUS"), false
		}
		return parseSimpleShowProcedureStatus(sql, statusBody)
	}
	triggersBody, ok := consumeKeyword(showBody, "triggers")
	if ok {
		return parseSimpleShowTriggers(sql, triggersBody)
	}
	eventsBody, ok := consumeKeyword(showBody, "events")
	if ok {
		return parseSimpleShowEvents(sql, eventsBody)
	}
	countBody, ok := consumeKeyword(showBody, "count")
	if ok {
		return parseSimpleShowCount(sql, countBody)
	}
	variablesBody, ok := consumeKeyword(showBody, "variables")
	if ok {
		return parseSimpleShowVariables(sql, variablesBody)
	}
	statusBody, ok := consumeKeyword(showBody, "status")
	if ok {
		return parseSimpleShowStatus(sql, statusBody)
	}
	warningsBody, ok := consumeKeyword(showBody, "warnings")
	if ok {
		return parseSimpleShowWarnings(sql, warningsBody)
	}
	errorsBody, ok := consumeKeyword(showBody, "errors")
	if ok {
		return parseSimpleShowErrors(sql, errorsBody)
	}
	characterBody, ok := consumeKeyword(showBody, "character")
	if ok {
		setBody, setOK := consumeKeyword(characterBody, "set")
		if !setOK {
			return UnboundStatement{}, simpleParserDiagnostic("SHOW CHARACTER only supports SET"), false
		}
		return parseSimpleShowCharacterSet(sql, setBody)
	}
	charsetBody, ok := consumeKeyword(showBody, "charset")
	if ok {
		return parseSimpleShowCharacterSet(sql, charsetBody)
	}
	collationBody, ok := consumeKeyword(showBody, "collation")
	if ok {
		return parseSimpleShowCollation(sql, collationBody)
	}
	processlistBody, ok := consumeKeyword(showBody, "processlist")
	if ok {
		return parseSimpleShowProcesslist(sql, processlistBody, false)
	}
	enginesBody, ok := consumeKeyword(showBody, "engines")
	if ok {
		return parseSimpleShowEngines(sql, enginesBody)
	}
	pluginsBody, ok := consumeKeyword(showBody, "plugins")
	if ok {
		return parseSimpleShowPlugins(sql, pluginsBody)
	}
	privilegesBody, ok := consumeKeyword(showBody, "privileges")
	if ok {
		return parseSimpleShowPrivileges(sql, privilegesBody)
	}
	grantsBody, ok := consumeKeyword(showBody, "grants")
	if ok {
		return parseSimpleShowGrants(sql, grantsBody)
	}
	indexBody, ok := consumeKeyword(showBody, "index")
	if !ok {
		indexBody, ok = consumeKeyword(showBody, "indexes")
	}
	if !ok {
		indexBody, ok = consumeKeyword(showBody, "keys")
	}
	if ok {
		return parseSimpleShowIndex(sql, indexBody)
	}
	columnsBody, ok := consumeKeyword(showBody, "columns")
	if !ok {
		columnsBody, ok = consumeKeyword(showBody, "fields")
	}
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("SHOW only supports CREATE VIEW, CREATE TABLE, DATABASES, TABLES, FULL TABLES, PROCESSLIST, ENGINES, VARIABLES, STATUS, WARNINGS, ERRORS, COUNT, CHARACTER SET, COLLATION, INDEX, or COLUMNS/FIELDS FROM table"), false
	}
	return parseSimpleShowColumns(sql, columnsBody, false)
}

func stripSimpleShowScope(showBody string) (string, bool) {
	trimmed := strings.TrimSpace(showBody)
	if next, ok := consumeKeyword(trimmed, "session"); ok {
		return strings.TrimSpace(next), true
	}
	if next, ok := consumeKeyword(trimmed, "global"); ok {
		return strings.TrimSpace(next), true
	}
	if next, ok := consumeKeyword(trimmed, "local"); ok {
		return strings.TrimSpace(next), true
	}
	return trimmed, false
}

func parseSimpleShowCreateDatabase(sql string, databaseBody string) (UnboundStatement, Diagnostic, bool) {
	fields := strings.Fields(strings.TrimSpace(databaseBody))
	if len(fields) != 1 {
		return UnboundStatement{}, simpleParserDiagnostic("SHOW CREATE DATABASE must include one schema name"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowCreateDatabase,
		ShowCreateDB: UnboundShowCreateDatabase{
			Schema: fields[0],
			Result: showCreateDatabaseResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowColumns(sql string, columnsBody string, full bool) (UnboundStatement, Diagnostic, bool) {
	targetBody, ok := consumeKeyword(columnsBody, "from")
	if !ok {
		targetBody, ok = consumeKeyword(columnsBody, "in")
	}
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("SHOW COLUMNS must include FROM table"), false
	}
	return parseSimpleDescribeTarget(sql, targetBody, full)
}

func parseSimpleShowIndex(sql string, indexBody string) (UnboundStatement, Diagnostic, bool) {
	targetBody, ok := consumeKeyword(indexBody, "from")
	if !ok {
		targetBody, ok = consumeKeyword(indexBody, "in")
	}
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("SHOW INDEX must include FROM table"), false
	}
	target, diagnostic, ok := parseSimpleTable(strings.TrimSpace(targetBody))
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	if target.Alias != "" {
		return UnboundStatement{}, simpleParserDiagnostic("SHOW INDEX aliases are not supported"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowIndex,
		ShowIndex: UnboundShowIndex{
			Table:  target,
			Result: showIndexResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowDatabases(sql string, databasesBody string) (UnboundStatement, Diagnostic, bool) {
	if strings.TrimSpace(databasesBody) != "" {
		return UnboundStatement{}, simpleParserDiagnostic("SHOW DATABASES does not support additional clauses yet"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowDatabases,
		ShowDBs: UnboundShowDatabases{
			Result: showDatabasesResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowTableStatus(sql string, statusBody string) (UnboundStatement, Diagnostic, bool) {
	schemaName := ""
	pattern := ""
	trimmed := strings.TrimSpace(statusBody)
	if trimmed != "" {
		if schemaBody, ok := consumeKeyword(trimmed, "from"); ok {
			schemaName, pattern, ok = parseSimpleSchemaAndOptionalLike(schemaBody)
			if !ok {
				return UnboundStatement{}, simpleParserDiagnostic("SHOW TABLE STATUS FROM schema supports optional LIKE pattern"), false
			}
		} else if schemaBody, ok := consumeKeyword(trimmed, "in"); ok {
			schemaName, pattern, ok = parseSimpleSchemaAndOptionalLike(schemaBody)
			if !ok {
				return UnboundStatement{}, simpleParserDiagnostic("SHOW TABLE STATUS IN schema supports optional LIKE pattern"), false
			}
		} else if likeBody, ok := consumeKeyword(trimmed, "like"); ok {
			likeFields := strings.Fields(likeBody)
			if len(likeFields) != 1 {
				return UnboundStatement{}, simpleParserDiagnostic("SHOW TABLE STATUS LIKE must use one literal pattern"), false
			}
			pattern = strings.Trim(likeFields[0], "'\"")
		} else {
			return UnboundStatement{}, simpleParserDiagnostic("SHOW TABLE STATUS only supports optional FROM/IN schema and LIKE pattern"), false
		}
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowTableStatus,
		ShowTableStatus: UnboundShowTableStatus{
			Schema:  schemaName,
			Pattern: pattern,
			Result:  showTableStatusResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleSchemaAndOptionalLike(text string) (string, string, bool) {
	schemaText, likeText, hasLike := splitOptionalKeyword(text, "like")
	schemaFields := strings.Fields(schemaText)
	if len(schemaFields) != 1 {
		return "", "", false
	}
	if !hasLike {
		return schemaFields[0], "", true
	}
	likeFields := strings.Fields(likeText)
	if len(likeFields) != 1 {
		return "", "", false
	}
	return schemaFields[0], strings.Trim(likeFields[0], "'\""), true
}

func parseSimpleShowTables(sql string, tablesBody string, full bool) (UnboundStatement, Diagnostic, bool) {
	schemaName := ""
	pattern := ""
	trimmed := strings.TrimSpace(tablesBody)
	if trimmed != "" {
		schemaText := trimmed
		whereText := ""
		hasWhere := false
		if body, ok := consumeKeyword(trimmed, "where"); ok {
			schemaText = ""
			whereText = body
			hasWhere = true
		} else {
			schemaText, whereText, hasWhere = splitOptionalKeyword(trimmed, "where")
		}
		if hasWhere {
			if !full {
				return UnboundStatement{}, simpleParserDiagnostic("SHOW TABLES WHERE Table_type requires SHOW FULL TABLES"), false
			}
			var diagnostic Diagnostic
			var ok bool
			pattern, diagnostic, ok = parseSimpleShowTablesWherePattern(whereText)
			if !ok {
				return UnboundStatement{}, diagnostic, false
			}
		}
		schemaText = strings.TrimSpace(schemaText)
		if schemaText != "" {
			schemaBody, ok := consumeKeyword(schemaText, "from")
			if !ok {
				schemaBody, ok = consumeKeyword(schemaText, "in")
			}
			if !ok {
				return UnboundStatement{}, simpleParserDiagnostic("SHOW TABLES only supports optional FROM/IN schema and SHOW FULL TABLES WHERE Table_type = literal"), false
			}
			fields := strings.Fields(schemaBody)
			if len(fields) != 1 {
				return UnboundStatement{}, simpleParserDiagnostic("SHOW TABLES schema must be a single name"), false
			}
			schemaName = fields[0]
		}
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowTables,
		ShowTables: UnboundShowTables{
			Schema:  schemaName,
			Full:    full,
			Pattern: pattern,
			Result:  showTablesResultShape(schemaName, full),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowTablesWherePattern(whereText string) (string, Diagnostic, bool) {
	fields := strings.Fields(strings.TrimSpace(whereText))
	if len(fields) < 3 {
		return "", simpleParserDiagnostic("SHOW FULL TABLES WHERE must use Table_type = literal or Table_type LIKE literal"), false
	}
	fieldName := strings.Trim(fields[0], "`\"")
	if !strings.EqualFold(fieldName, "Table_type") {
		return "", simpleParserDiagnostic("SHOW FULL TABLES WHERE only supports Table_type predicates"), false
	}
	op := strings.ToLower(fields[1])
	if op != "=" && op != "like" {
		return "", simpleParserDiagnostic("SHOW FULL TABLES WHERE only supports Table_type = literal or Table_type LIKE literal"), false
	}
	return strings.Trim(strings.Join(fields[2:], " "), "'\""), Diagnostic{}, true
}

func parseSimpleShowOpenTables(sql string, tablesBody string) (UnboundStatement, Diagnostic, bool) {
	schemaName := ""
	pattern := ""
	trimmed := strings.TrimSpace(tablesBody)
	if trimmed != "" {
		if schemaBody, ok := consumeKeyword(trimmed, "from"); ok {
			schemaName, pattern, ok = parseSimpleSchemaAndOptionalLike(schemaBody)
			if !ok {
				return UnboundStatement{}, simpleParserDiagnostic("SHOW OPEN TABLES FROM schema supports optional LIKE pattern"), false
			}
		} else if schemaBody, ok := consumeKeyword(trimmed, "in"); ok {
			schemaName, pattern, ok = parseSimpleSchemaAndOptionalLike(schemaBody)
			if !ok {
				return UnboundStatement{}, simpleParserDiagnostic("SHOW OPEN TABLES IN schema supports optional LIKE pattern"), false
			}
		} else if likeBody, ok := consumeKeyword(trimmed, "like"); ok {
			likeFields := strings.Fields(likeBody)
			if len(likeFields) != 1 {
				return UnboundStatement{}, simpleParserDiagnostic("SHOW OPEN TABLES LIKE must use one literal pattern"), false
			}
			pattern = strings.Trim(likeFields[0], "'\"")
		} else {
			return UnboundStatement{}, simpleParserDiagnostic("SHOW OPEN TABLES only supports optional FROM/IN schema and LIKE pattern"), false
		}
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowOpenTables,
		ShowOpenTables: UnboundShowOpenTables{
			Schema:  schemaName,
			Pattern: pattern,
			Result:  showOpenTablesResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowTableTypes(sql string, typesBody string) (UnboundStatement, Diagnostic, bool) {
	if strings.TrimSpace(typesBody) != "" {
		return UnboundStatement{}, simpleParserDiagnostic("SHOW TABLE TYPES does not support additional clauses yet"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowTableTypes,
		ShowTableTypes: UnboundShowTableTypes{
			Result: showTableTypesResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowFunctionStatus(sql string, statusBody string) (UnboundStatement, Diagnostic, bool) {
	pattern, diagnostic, ok := parseSimpleOptionalLikePattern(statusBody, "SHOW FUNCTION STATUS")
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowFunctionStatus,
		ShowFuncStatus: UnboundShowFunctionStatus{
			Pattern: pattern,
			Result:  showRoutineStatusResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowProcedureStatus(sql string, statusBody string) (UnboundStatement, Diagnostic, bool) {
	pattern, diagnostic, ok := parseSimpleOptionalLikePattern(statusBody, "SHOW PROCEDURE STATUS")
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowProcedureStatus,
		ShowProcStatus: UnboundShowProcedureStatus{
			Pattern: pattern,
			Result:  showRoutineStatusResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowTriggers(sql string, triggersBody string) (UnboundStatement, Diagnostic, bool) {
	schemaName, pattern, diagnostic, ok := parseSimpleShowSchemaAndLike(triggersBody, "SHOW TRIGGERS")
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowTriggers,
		ShowTriggers: UnboundShowTriggers{
			Schema:  schemaName,
			Pattern: pattern,
			Result:  showTriggersResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowEvents(sql string, eventsBody string) (UnboundStatement, Diagnostic, bool) {
	schemaName, pattern, diagnostic, ok := parseSimpleShowSchemaAndLike(eventsBody, "SHOW EVENTS")
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowEvents,
		ShowEvents: UnboundShowEvents{
			Schema:  schemaName,
			Pattern: pattern,
			Result:  showEventsResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowSchemaAndLike(body string, statement string) (string, string, Diagnostic, bool) {
	schemaName := ""
	pattern := ""
	trimmed := strings.TrimSpace(body)
	if trimmed == "" {
		return schemaName, pattern, Diagnostic{}, true
	}
	if schemaBody, ok := consumeKeyword(trimmed, "from"); ok {
		schemaName, pattern, ok = parseSimpleSchemaAndOptionalLike(schemaBody)
		if !ok {
			return "", "", simpleParserDiagnostic(statement + " FROM schema supports optional LIKE pattern"), false
		}
		return schemaName, pattern, Diagnostic{}, true
	}
	if schemaBody, ok := consumeKeyword(trimmed, "in"); ok {
		schemaName, pattern, ok = parseSimpleSchemaAndOptionalLike(schemaBody)
		if !ok {
			return "", "", simpleParserDiagnostic(statement + " IN schema supports optional LIKE pattern"), false
		}
		return schemaName, pattern, Diagnostic{}, true
	}
	if likeBody, ok := consumeKeyword(trimmed, "like"); ok {
		likeFields := strings.Fields(likeBody)
		if len(likeFields) != 1 {
			return "", "", simpleParserDiagnostic(statement + " LIKE must use one literal pattern"), false
		}
		pattern = strings.Trim(likeFields[0], "'\"")
		return schemaName, pattern, Diagnostic{}, true
	}
	return "", "", simpleParserDiagnostic(statement + " only supports optional FROM/IN schema and LIKE pattern"), false
}

func parseSimpleShowVariables(sql string, variablesBody string) (UnboundStatement, Diagnostic, bool) {
	pattern, diagnostic, ok := parseSimpleOptionalLikePattern(variablesBody, "SHOW VARIABLES")
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowVariables,
		ShowVars: UnboundShowVariables{
			Pattern: pattern,
			Result:  showVariablesResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowStatus(sql string, statusBody string) (UnboundStatement, Diagnostic, bool) {
	pattern, diagnostic, ok := parseSimpleOptionalLikePattern(statusBody, "SHOW STATUS")
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowStatus,
		ShowStatus: UnboundShowStatus{
			Pattern: pattern,
			Result:  showStatusResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowCount(sql string, countBody string) (UnboundStatement, Diagnostic, bool) {
	compact := strings.ToLower(strings.Join(strings.Fields(countBody), ""))
	switch compact {
	case "(*)warnings":
		return UnboundStatement{
			SQL:  sql,
			Kind: QueryKindShowWarningCount,
			ShowWarnCount: UnboundShowWarningCount{
				Result: showWarningCountResultShape(),
			},
		}, Diagnostic{}, true
	case "(*)errors":
		return UnboundStatement{
			SQL:  sql,
			Kind: QueryKindShowErrorCount,
			ShowErrorCount: UnboundShowErrorCount{
				Result: showErrorCountResultShape(),
			},
		}, Diagnostic{}, true
	default:
		return UnboundStatement{}, simpleParserDiagnostic("SHOW COUNT only supports COUNT(*) WARNINGS or COUNT(*) ERRORS"), false
	}
}

func parseSimpleShowWarnings(sql string, warningsBody string) (UnboundStatement, Diagnostic, bool) {
	limit, offset, hasLimit, diagnostic, ok := parseSimpleOptionalLimitOnlyClause(warningsBody, "SHOW WARNINGS")
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	result := showWarningsResultShape()
	result.Limit = limit
	result.Offset = offset
	result.HasLimit = hasLimit
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowWarnings,
		ShowWarnings: UnboundShowWarnings{
			Result: result,
		},
	}, Diagnostic{}, true
}

func parseSimpleShowErrors(sql string, errorsBody string) (UnboundStatement, Diagnostic, bool) {
	limit, offset, hasLimit, diagnostic, ok := parseSimpleOptionalLimitOnlyClause(errorsBody, "SHOW ERRORS")
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	result := showErrorsResultShape()
	result.Limit = limit
	result.Offset = offset
	result.HasLimit = hasLimit
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowErrors,
		ShowErrors: UnboundShowErrors{
			Result: result,
		},
	}, Diagnostic{}, true
}

func parseSimpleExplain(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	body, ok := consumeKeyword(trimmed, "explain")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only EXPLAIN statements are supported"), false
	}
	body = strings.TrimSpace(body)
	if body == "" {
		return UnboundStatement{}, simpleParserDiagnostic("EXPLAIN must include a statement"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindExplain,
		Explain: UnboundExplain{
			SQL:    body,
			Result: explainResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowCharacterSet(sql string, characterSetBody string) (UnboundStatement, Diagnostic, bool) {
	pattern, diagnostic, ok := parseSimpleOptionalLikePattern(characterSetBody, "SHOW CHARACTER SET")
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowCharacterSet,
		ShowCharset: UnboundShowCharacterSet{
			Pattern: pattern,
			Result:  showCharacterSetResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowCollation(sql string, collationBody string) (UnboundStatement, Diagnostic, bool) {
	pattern, diagnostic, ok := parseSimpleOptionalLikePattern(collationBody, "SHOW COLLATION")
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowCollation,
		ShowCollation: UnboundShowCollation{
			Pattern: pattern,
			Result:  showCollationResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowProcesslist(sql string, processlistBody string, full bool) (UnboundStatement, Diagnostic, bool) {
	if strings.TrimSpace(processlistBody) != "" {
		return UnboundStatement{}, simpleParserDiagnostic("SHOW PROCESSLIST does not support additional clauses yet"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowProcesslist,
		ShowProcesslist: UnboundShowProcesslist{
			Full:   full,
			Result: showProcesslistResultShape(full),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowEngines(sql string, enginesBody string) (UnboundStatement, Diagnostic, bool) {
	if strings.TrimSpace(enginesBody) != "" {
		return UnboundStatement{}, simpleParserDiagnostic("SHOW ENGINES does not support additional clauses yet"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowEngines,
		ShowEngines: UnboundShowEngines{
			Result: showEnginesResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowPlugins(sql string, pluginsBody string) (UnboundStatement, Diagnostic, bool) {
	if strings.TrimSpace(pluginsBody) != "" {
		return UnboundStatement{}, simpleParserDiagnostic("SHOW PLUGINS does not support additional clauses yet"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowPlugins,
		ShowPlugins: UnboundShowPlugins{
			Result: showPluginsResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowPrivileges(sql string, privilegesBody string) (UnboundStatement, Diagnostic, bool) {
	if strings.TrimSpace(privilegesBody) != "" {
		return UnboundStatement{}, simpleParserDiagnostic("SHOW PRIVILEGES does not support additional clauses yet"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowPrivileges,
		ShowPrivileges: UnboundShowPrivileges{
			Result: showPrivilegesResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleShowGrants(sql string, grantsBody string) (UnboundStatement, Diagnostic, bool) {
	if strings.TrimSpace(grantsBody) != "" {
		return UnboundStatement{}, simpleParserDiagnostic("SHOW GRANTS only supports the current session user"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindShowGrants,
		ShowGrants: UnboundShowGrants{
			Result: showGrantsResultShape(),
		},
	}, Diagnostic{}, true
}

func parseSimpleOptionalLikePattern(text string, statement string) (string, Diagnostic, bool) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return "", Diagnostic{}, true
	}
	likeBody, ok := consumeKeyword(trimmed, "like")
	if ok {
		fields := strings.Fields(likeBody)
		if len(fields) != 1 {
			return "", simpleParserDiagnostic(statement + " LIKE must use one literal pattern"), false
		}
		return strings.Trim(fields[0], "'\""), Diagnostic{}, true
	}
	whereBody, ok := consumeKeyword(trimmed, "where")
	if !ok {
		return "", simpleParserDiagnostic(statement + " only supports optional LIKE pattern or WHERE field = literal"), false
	}
	return parseSimpleShowWherePattern(whereBody, statement)
}

func parseSimpleShowWherePattern(whereBody string, statement string) (string, Diagnostic, bool) {
	fields := strings.Fields(strings.TrimSpace(whereBody))
	if len(fields) < 3 {
		return "", simpleParserDiagnostic(statement + " WHERE must use field = literal or field LIKE literal"), false
	}
	op := strings.ToLower(fields[1])
	if op != "=" && op != "like" {
		return "", simpleParserDiagnostic(statement + " WHERE only supports field = literal or field LIKE literal"), false
	}
	return strings.Trim(strings.Join(fields[2:], " "), "'\""), Diagnostic{}, true
}

func parseSimpleDescribe(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	targetBody, ok := consumeKeyword(trimmed, "describe")
	if !ok {
		targetBody, ok = consumeKeyword(trimmed, "desc")
	}
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only DESCRIBE statements are supported"), false
	}
	return parseSimpleDescribeTarget(sql, targetBody)
}

func parseSimpleDescribeTarget(sql string, targetBody string, full ...bool) (UnboundStatement, Diagnostic, bool) {
	target, diagnostic, ok := parseSimpleTable(strings.TrimSpace(targetBody))
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	if target.Alias != "" {
		return UnboundStatement{}, simpleParserDiagnostic("DESCRIBE aliases are not supported"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindDescribe,
		Describe: UnboundDescribe{
			Target: target,
			Full:   len(full) > 0 && full[0],
			Result: describeResultShape(full...),
		},
	}, Diagnostic{}, true
}

func describeResultShape(full ...bool) ResultShape {
	columns := []FieldRef{
		{Name: "Field", Type: DataTypeString},
		{Name: "Type", Type: DataTypeString},
	}
	if len(full) > 0 && full[0] {
		columns = append(columns, FieldRef{Name: "Collation", Type: DataTypeString, Nullable: true})
	}
	columns = append(columns,
		FieldRef{Name: "Null", Type: DataTypeString},
		FieldRef{Name: "Key", Type: DataTypeString},
		FieldRef{Name: "Default", Type: DataTypeString, Nullable: true},
		FieldRef{Name: "Extra", Type: DataTypeString},
	)
	if len(full) > 0 && full[0] {
		columns = append(columns,
			FieldRef{Name: "Privileges", Type: DataTypeString},
			FieldRef{Name: "Comment", Type: DataTypeString},
		)
	}
	return ResultShape{
		Kind:    ResultQuery,
		Columns: columns,
	}
}

func showCreateViewResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "View", Type: DataTypeString},
			{Name: "Create View", Type: DataTypeString},
			{Name: "character_set_client", Type: DataTypeString},
			{Name: "collation_connection", Type: DataTypeString},
		},
	}
}

func showCreateTableResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Table", Type: DataTypeString},
			{Name: "Create Table", Type: DataTypeString},
		},
	}
}

func showCreateDatabaseResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Database", Type: DataTypeString},
			{Name: "Create Database", Type: DataTypeString},
		},
	}
}

func showDatabasesResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Database", Type: DataTypeString},
		},
	}
}

func showIndexResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Table", Type: DataTypeString},
			{Name: "Non_unique", Type: DataTypeInt},
			{Name: "Key_name", Type: DataTypeString},
			{Name: "Seq_in_index", Type: DataTypeInt},
			{Name: "Column_name", Type: DataTypeString},
			{Name: "Collation", Type: DataTypeString},
			{Name: "Cardinality", Type: DataTypeInt, Nullable: true},
			{Name: "Sub_part", Type: DataTypeInt, Nullable: true},
			{Name: "Packed", Type: DataTypeString, Nullable: true},
			{Name: "Null", Type: DataTypeString},
			{Name: "Index_type", Type: DataTypeString},
			{Name: "Comment", Type: DataTypeString},
			{Name: "Index_comment", Type: DataTypeString},
			{Name: "Visible", Type: DataTypeString},
			{Name: "Expression", Type: DataTypeString, Nullable: true},
		},
	}
}

func showTableStatusResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Name", Type: DataTypeString},
			{Name: "Engine", Type: DataTypeString, Nullable: true},
			{Name: "Version", Type: DataTypeInt, Nullable: true},
			{Name: "Row_format", Type: DataTypeString, Nullable: true},
			{Name: "Rows", Type: DataTypeInt, Nullable: true},
			{Name: "Avg_row_length", Type: DataTypeInt, Nullable: true},
			{Name: "Data_length", Type: DataTypeInt, Nullable: true},
			{Name: "Max_data_length", Type: DataTypeInt, Nullable: true},
			{Name: "Index_length", Type: DataTypeInt, Nullable: true},
			{Name: "Data_free", Type: DataTypeInt, Nullable: true},
			{Name: "Auto_increment", Type: DataTypeInt, Nullable: true},
			{Name: "Create_time", Type: DataTypeTime, Nullable: true},
			{Name: "Update_time", Type: DataTypeTime, Nullable: true},
			{Name: "Check_time", Type: DataTypeTime, Nullable: true},
			{Name: "Collation", Type: DataTypeString, Nullable: true},
			{Name: "Checksum", Type: DataTypeInt, Nullable: true},
			{Name: "Create_options", Type: DataTypeString},
			{Name: "Comment", Type: DataTypeString},
		},
	}
}

func showTablesResultShape(schemaName string, full bool) ResultShape {
	columns := []FieldRef{
		{Name: showTablesColumnName(schemaName), Type: DataTypeString},
	}
	if full {
		columns = append(columns, FieldRef{Name: "Table_type", Type: DataTypeString})
	}
	return ResultShape{
		Kind:    ResultQuery,
		Columns: columns,
	}
}

func showOpenTablesResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Database", Type: DataTypeString},
			{Name: "Table", Type: DataTypeString},
			{Name: "In_use", Type: DataTypeInt},
			{Name: "Name_locked", Type: DataTypeInt},
		},
	}
}

func showTableTypesResultShape() ResultShape {
	return showEnginesResultShape()
}

func showRoutineStatusResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Db", Type: DataTypeString},
			{Name: "Name", Type: DataTypeString},
			{Name: "Type", Type: DataTypeString},
			{Name: "Definer", Type: DataTypeString},
			{Name: "Modified", Type: DataTypeTime, Nullable: true},
			{Name: "Created", Type: DataTypeTime, Nullable: true},
			{Name: "Security_type", Type: DataTypeString},
			{Name: "Comment", Type: DataTypeString},
			{Name: "character_set_client", Type: DataTypeString},
			{Name: "collation_connection", Type: DataTypeString},
			{Name: "Database Collation", Type: DataTypeString},
		},
	}
}

func showTriggersResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Trigger", Type: DataTypeString},
			{Name: "Event", Type: DataTypeString},
			{Name: "Table", Type: DataTypeString},
			{Name: "Statement", Type: DataTypeString},
			{Name: "Timing", Type: DataTypeString},
			{Name: "Created", Type: DataTypeTime, Nullable: true},
			{Name: "sql_mode", Type: DataTypeString},
			{Name: "Definer", Type: DataTypeString},
			{Name: "character_set_client", Type: DataTypeString},
			{Name: "collation_connection", Type: DataTypeString},
			{Name: "Database Collation", Type: DataTypeString},
		},
	}
}

func showEventsResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Db", Type: DataTypeString},
			{Name: "Name", Type: DataTypeString},
			{Name: "Definer", Type: DataTypeString},
			{Name: "Time zone", Type: DataTypeString},
			{Name: "Type", Type: DataTypeString},
			{Name: "Execute at", Type: DataTypeTime, Nullable: true},
			{Name: "Interval value", Type: DataTypeString, Nullable: true},
			{Name: "Interval field", Type: DataTypeString, Nullable: true},
			{Name: "Starts", Type: DataTypeTime, Nullable: true},
			{Name: "Ends", Type: DataTypeTime, Nullable: true},
			{Name: "Status", Type: DataTypeString},
			{Name: "Originator", Type: DataTypeInt},
			{Name: "character_set_client", Type: DataTypeString},
			{Name: "collation_connection", Type: DataTypeString},
			{Name: "Database Collation", Type: DataTypeString},
		},
	}
}

func showVariablesResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "variable_name", Type: DataTypeString},
			{Name: "value", Type: DataTypeString},
		},
	}
}

func showStatusResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "variable_name", Type: DataTypeString},
			{Name: "value", Type: DataTypeString},
		},
	}
}

func showWarningsResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Level", Type: DataTypeString},
			{Name: "Code", Type: DataTypeInt},
			{Name: "Message", Type: DataTypeString},
		},
	}
}

func showErrorsResultShape() ResultShape {
	return showWarningsResultShape()
}

func showWarningCountResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "@@session.warning_count", Type: DataTypeInt},
		},
	}
}

func showErrorCountResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "@@session.error_count", Type: DataTypeInt},
		},
	}
}

func explainResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "id", Type: DataTypeInt},
			{Name: "select_type", Type: DataTypeString},
			{Name: "table", Type: DataTypeString, Nullable: true},
			{Name: "partitions", Type: DataTypeString, Nullable: true},
			{Name: "type", Type: DataTypeString, Nullable: true},
			{Name: "possible_keys", Type: DataTypeString, Nullable: true},
			{Name: "key", Type: DataTypeString, Nullable: true},
			{Name: "key_len", Type: DataTypeString, Nullable: true},
			{Name: "ref", Type: DataTypeString, Nullable: true},
			{Name: "rows", Type: DataTypeInt, Nullable: true},
			{Name: "filtered", Type: DataTypeFloat, Nullable: true},
			{Name: "extra", Type: DataTypeString, Nullable: true},
		},
	}
}

func showCharacterSetResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Charset", Type: DataTypeString},
			{Name: "Description", Type: DataTypeString},
			{Name: "Default collation", Type: DataTypeString},
			{Name: "Maxlen", Type: DataTypeInt},
		},
	}
}

func showCollationResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Collation", Type: DataTypeString},
			{Name: "Charset", Type: DataTypeString},
			{Name: "Id", Type: DataTypeInt},
			{Name: "Default", Type: DataTypeString},
			{Name: "Compiled", Type: DataTypeString},
			{Name: "Sortlen", Type: DataTypeInt},
		},
	}
}

func showProcesslistResultShape(full bool) ResultShape {
	infoLength := 100
	if full {
		infoLength = 0
	}
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Id", Type: DataTypeInt},
			{Name: "User", Type: DataTypeString},
			{Name: "Host", Type: DataTypeString},
			{Name: "db", Type: DataTypeString, Nullable: true},
			{Name: "Command", Type: DataTypeString},
			{Name: "Time", Type: DataTypeInt},
			{Name: "State", Type: DataTypeString, Nullable: true},
			{Name: "Info", Type: DataTypeString, Nullable: true, Encoding: EncodingProfile{MaxLength: infoLength}},
		},
	}
}

func showEnginesResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Engine", Type: DataTypeString},
			{Name: "Support", Type: DataTypeString},
			{Name: "Comment", Type: DataTypeString},
			{Name: "Transactions", Type: DataTypeString},
			{Name: "XA", Type: DataTypeString},
			{Name: "Savepoints", Type: DataTypeString},
		},
	}
}

func showPluginsResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Name", Type: DataTypeString},
			{Name: "Status", Type: DataTypeString},
			{Name: "Type", Type: DataTypeString},
			{Name: "Library", Type: DataTypeString, Nullable: true},
			{Name: "License", Type: DataTypeString},
		},
	}
}

func showPrivilegesResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Privilege", Type: DataTypeString},
			{Name: "Context", Type: DataTypeString},
			{Name: "Comment", Type: DataTypeString},
		},
	}
}

func showGrantsResultShape() ResultShape {
	return ResultShape{
		Kind: ResultQuery,
		Columns: []FieldRef{
			{Name: "Grants for User", Type: DataTypeString},
		},
	}
}

func showTablesColumnName(schemaName string) string {
	schemaName = strings.TrimSpace(schemaName)
	if schemaName == "" {
		return "Tables_in"
	}
	return "Tables_in_" + schemaName
}

func parseSimpleBegin(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	beginBody, ok := consumeKeyword(trimmed, "begin")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only BEGIN statements are supported"), false
	}
	if beginBody != "" && !strings.EqualFold(beginBody, "work") {
		return UnboundStatement{}, simpleParserDiagnostic("BEGIN only supports optional WORK"), false
	}
	return simpleTransactionStatement(sql, BeginTransactionAction()), Diagnostic{}, true
}

func parseSimpleStartTransaction(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	startBody, ok := consumeKeyword(trimmed, "start")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only START TRANSACTION statements are supported"), false
	}
	_, ok = consumeKeyword(startBody, "transaction")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("START only supports TRANSACTION"), false
	}
	return simpleTransactionStatement(sql, BeginTransactionAction()), Diagnostic{}, true
}

func parseSimpleRollback(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	rollbackBody, ok := consumeKeyword(trimmed, "rollback")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only ROLLBACK statements are supported"), false
	}
	if rollbackBody != "" && !strings.EqualFold(rollbackBody, "work") {
		return UnboundStatement{}, simpleParserDiagnostic("ROLLBACK only supports optional WORK"), false
	}
	return simpleTransactionStatement(sql, RollbackTransactionAction()), Diagnostic{}, true
}

func parseSimpleCommit(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	commitBody, ok := consumeKeyword(trimmed, "commit")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only COMMIT statements are supported"), false
	}
	if commitBody != "" && !strings.EqualFold(commitBody, "work") {
		return UnboundStatement{}, simpleParserDiagnostic("COMMIT only supports optional WORK"), false
	}
	return simpleTransactionStatement(sql, CommitTransactionAction()), Diagnostic{}, true
}

func simpleTransactionStatement(sql string, action SessionAction) UnboundStatement {
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindSession,
		Session: UnboundSession{
			Actions: []SessionAction{action},
			Result:  ResultShape{Kind: ResultStatement},
		},
	}
}

func parseSimpleUse(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	useBody, ok := consumeKeyword(trimmed, "use")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only USE statements are supported"), false
	}
	fields := strings.Fields(useBody)
	if len(fields) != 1 {
		return UnboundStatement{}, simpleParserDiagnostic("USE must include one schema name"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindSession,
		Session: UnboundSession{
			Actions:         []SessionAction{{Kind: SessionActionUseSchema, Value: fields[0]}},
			ValidateCatalog: true,
			Result: ResultShape{
				Kind:      ResultStatement,
				Statement: StatementResult{Status: "Database changed"},
			},
		},
	}, Diagnostic{}, true
}

func parseSimpleSet(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	setBody, ok := consumeKeyword(trimmed, "set")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only SET statements are supported"), false
	}
	namesBody, ok := consumeKeyword(setBody, "names")
	if ok {
		return parseSimpleSetNames(sql, namesBody)
	}
	characterBody, ok := consumeKeyword(setBody, "character")
	if ok {
		characterSetBody, ok := consumeKeyword(characterBody, "set")
		if !ok {
			return UnboundStatement{}, simpleParserDiagnostic("SET CHARACTER only supports SET"), false
		}
		return parseSimpleSetCharacterSet(sql, characterSetBody)
	}
	charsetBody, ok := consumeKeyword(setBody, "charset")
	if ok {
		return parseSimpleSetCharacterSet(sql, charsetBody)
	}
	scopedBody := stripSimpleSetScope(setBody)
	transactionBody, ok := consumeKeyword(scopedBody, "transaction")
	if ok {
		return parseSimpleSetTransaction(sql, transactionBody)
	}
	actions, diagnostic, ok := parseSimpleSetAssignments(scopedBody)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindSession,
		Session: UnboundSession{
			Actions: actions,
			Result: ResultShape{
				Kind:      ResultStatement,
				Statement: StatementResult{Status: "Query OK"},
			},
		},
	}, Diagnostic{}, true
}

func stripSimpleSetScope(setBody string) string {
	trimmed := strings.TrimSpace(setBody)
	for {
		next, ok := consumeKeyword(trimmed, "session")
		if !ok {
			next, ok = consumeKeyword(trimmed, "global")
		}
		if !ok {
			next, ok = consumeKeyword(trimmed, "local")
		}
		if !ok {
			next, ok = consumeKeyword(trimmed, "option")
		}
		if !ok {
			return trimmed
		}
		trimmed = strings.TrimSpace(next)
	}
}

func parseSimpleSetNames(sql string, namesBody string) (UnboundStatement, Diagnostic, bool) {
	fields := strings.Fields(strings.TrimSpace(namesBody))
	if len(fields) != 1 && len(fields) != 3 {
		return UnboundStatement{}, simpleParserDiagnostic("SET NAMES must include one character set and optional COLLATE collation"), false
	}
	charset := strings.Trim(fields[0], "'\"")
	if charset == "" {
		return UnboundStatement{}, simpleParserDiagnostic("SET NAMES character set is empty"), false
	}
	actions := []SessionAction{
		{Kind: SessionActionSetVariable, Name: "character_set_client", Value: charset},
		{Kind: SessionActionSetVariable, Name: "character_set_connection", Value: charset},
		{Kind: SessionActionSetVariable, Name: "character_set_results", Value: charset},
	}
	if len(fields) == 3 {
		if !strings.EqualFold(fields[1], "collate") {
			return UnboundStatement{}, simpleParserDiagnostic("SET NAMES optional clause must be COLLATE collation"), false
		}
		collation := strings.Trim(fields[2], "'\"")
		if collation == "" {
			return UnboundStatement{}, simpleParserDiagnostic("SET NAMES collation is empty"), false
		}
		actions = append(actions, SessionAction{Kind: SessionActionSetVariable, Name: "collation_connection", Value: collation})
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindSession,
		Session: UnboundSession{
			Actions: actions,
			Result: ResultShape{
				Kind:      ResultStatement,
				Statement: StatementResult{Status: "Query OK"},
			},
		},
	}, Diagnostic{}, true
}

func parseSimpleSetCharacterSet(sql string, characterSetBody string) (UnboundStatement, Diagnostic, bool) {
	fields := strings.Fields(strings.TrimSpace(characterSetBody))
	if len(fields) != 1 {
		return UnboundStatement{}, simpleParserDiagnostic("SET CHARACTER SET must include one character set"), false
	}
	charset := strings.Trim(fields[0], "'\"")
	if charset == "" {
		return UnboundStatement{}, simpleParserDiagnostic("SET CHARACTER SET character set is empty"), false
	}
	actions := []SessionAction{
		{Kind: SessionActionSetVariable, Name: "character_set_client", Value: charset},
		{Kind: SessionActionSetVariable, Name: "character_set_results", Value: charset},
		{Kind: SessionActionSetVariable, Name: "character_set_connection", Value: charset},
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindSession,
		Session: UnboundSession{
			Actions: actions,
			Result: ResultShape{
				Kind:      ResultStatement,
				Statement: StatementResult{Status: "Query OK"},
			},
		},
	}, Diagnostic{}, true
}

func parseSimpleSetTransaction(sql string, transactionBody string) (UnboundStatement, Diagnostic, bool) {
	if strings.TrimSpace(transactionBody) == "" {
		return UnboundStatement{}, simpleParserDiagnostic("SET TRANSACTION must include transaction characteristics"), false
	}
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindSession,
		Session: UnboundSession{
			Result: ResultShape{
				Kind:      ResultStatement,
				Statement: StatementResult{Status: "Query OK"},
			},
		},
	}, Diagnostic{}, true
}

func parseSimpleSetAssignments(setBody string) ([]SessionAction, Diagnostic, bool) {
	parts := splitSimpleCommaList(setBody)
	actions := make([]SessionAction, 0, len(parts))
	for _, part := range parts {
		if _, _, hasAssignment := splitSimpleSetAssignment(part); !hasAssignment {
			continuation := strings.TrimSpace(part)
			if continuation != "" && len(actions) > 0 {
				actions[len(actions)-1].Value += "," + continuation
				continue
			}
		}
		name, value, diagnostic, ok := parseSimpleSetAssignment(part)
		if !ok {
			return nil, diagnostic, false
		}
		actions = append(actions, sessionActionForSetAssignment(name, value))
	}
	if len(actions) == 0 {
		return nil, simpleParserDiagnostic("SET must include at least one assignment"), false
	}
	return actions, Diagnostic{}, true
}

func parseSimpleSetAssignment(text string) (string, string, Diagnostic, bool) {
	nameText, valueText, ok := splitSimpleSetAssignment(text)
	if !ok {
		return "", "", simpleParserDiagnostic("SET assignment must use name = value"), false
	}
	name := normalizeSimpleSystemVariableName(nameText)
	if name == "" {
		return "", "", simpleParserDiagnostic("SET variable name is empty"), false
	}
	value := strings.TrimSpace(valueText)
	if value == "" {
		return "", "", simpleParserDiagnostic("SET assignment value is empty"), false
	}
	if literal, _, ok := parseSimpleLiteral(value); ok {
		return name, simpleLiteralSessionValue(literal), Diagnostic{}, true
	}
	return name, strings.Trim(value, "'\""), Diagnostic{}, true
}

func splitSimpleSetAssignment(text string) (string, string, bool) {
	trimmed := strings.TrimSpace(text)
	if index := strings.Index(trimmed, ":="); index >= 0 {
		return trimmed[:index], trimmed[index+2:], true
	}
	parts := strings.SplitN(trimmed, "=", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func sessionActionForSetAssignment(name string, value string) SessionAction {
	switch strings.ToLower(name) {
	case "sql_mode":
		return SessionAction{Kind: SessionActionSetSQLMode, Name: name, Value: value}
	case "time_zone":
		return SessionAction{Kind: SessionActionSetTimeZone, Name: name, Value: value}
	default:
		return SessionAction{Kind: SessionActionSetVariable, Name: name, Value: value}
	}
}

func simpleLiteralSessionValue(literal UnboundLiteralExpr) string {
	if literal.Kind == ValueNull || literal.Value == nil {
		return ""
	}
	return strings.TrimSpace(strings.Trim(fmt.Sprint(literal.Value), "'\""))
}

func parseSimpleInsertTarget(text string) (UnboundTable, []string, Diagnostic, bool) {
	trimmed := strings.TrimSpace(text)
	open := strings.Index(trimmed, "(")
	close := strings.LastIndex(trimmed, ")")
	if open <= 0 || close <= open || close != len(trimmed)-1 {
		return UnboundTable{}, nil, simpleParserDiagnostic("INSERT target must include a column list"), false
	}
	table, diagnostic, ok := parseSimpleTable(strings.TrimSpace(trimmed[:open]))
	if !ok {
		return UnboundTable{}, nil, diagnostic, false
	}
	columnParts := splitSimpleCommaList(trimmed[open+1 : close])
	columns := make([]string, 0, len(columnParts))
	for _, part := range columnParts {
		column := strings.TrimSpace(part)
		if column == "" {
			return UnboundTable{}, nil, simpleParserDiagnostic("INSERT column list contains an empty column"), false
		}
		columns = append(columns, column)
	}
	if len(columns) == 0 {
		return UnboundTable{}, nil, simpleParserDiagnostic("INSERT column list is empty"), false
	}
	return table, columns, Diagnostic{}, true
}

func parseSimpleInsertRows(text string, columnCount int) ([][]UnboundExpr, Diagnostic, bool) {
	trimmed := strings.TrimSpace(text)
	rows := make([][]UnboundExpr, 0, 1)
	for trimmed != "" {
		if trimmed[0] == ',' {
			trimmed = strings.TrimSpace(trimmed[1:])
			continue
		}
		if trimmed[0] != '(' {
			return nil, simpleParserDiagnostic("INSERT VALUES rows must be parenthesized"), false
		}
		rowText, rest, diagnostic, ok := consumeSimpleParenthesized(trimmed)
		if !ok {
			return nil, diagnostic, false
		}
		row, diagnostic, ok := parseSimpleInsertRow(rowText, columnCount)
		if !ok {
			return nil, diagnostic, false
		}
		rows = append(rows, row)
		trimmed = strings.TrimSpace(rest)
		if trimmed != "" && trimmed[0] != ',' {
			return nil, simpleParserDiagnostic("INSERT VALUES rows must be separated by commas"), false
		}
	}
	if len(rows) == 0 {
		return nil, simpleParserDiagnostic("INSERT must include at least one VALUES row"), false
	}
	return rows, Diagnostic{}, true
}

func parseSimpleInsertRow(text string, columnCount int) ([]UnboundExpr, Diagnostic, bool) {
	parts := splitSimpleCommaList(text)
	if len(parts) != columnCount {
		return nil, simpleParserDiagnostic("INSERT row value count does not match target column count"), false
	}
	values := make([]UnboundExpr, 0, len(parts))
	for _, part := range parts {
		value, diagnostic, ok := parseSimpleInsertValue(strings.TrimSpace(part))
		if !ok {
			return nil, diagnostic, false
		}
		values = append(values, value)
	}
	return values, Diagnostic{}, true
}

func parseSimpleInsertValue(text string) (UnboundExpr, Diagnostic, bool) {
	switch simpleLowerASCII(text) {
	case "":
		return UnboundLiteral(ValueNull, nil), Diagnostic{}, true
	case "true":
		return UnboundLiteral(ValueBool, true), Diagnostic{}, true
	case "false":
		return UnboundLiteral(ValueBool, false), Diagnostic{}, true
	case "null":
		return UnboundLiteral(ValueNull, nil), Diagnostic{}, true
	default:
		return parseSimpleLiteral(text)
	}
}

func consumeSimpleParenthesized(text string) (string, string, Diagnostic, bool) {
	inString := false
	for i := 0; i < len(text); i++ {
		ch := text[i]
		if ch == '\'' {
			if inString && i+1 < len(text) && text[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		if i == 0 {
			if ch != '(' {
				return "", "", simpleParserDiagnostic("expected parenthesized expression"), false
			}
			continue
		}
		if ch == ')' {
			return text[1:i], text[i+1:], Diagnostic{}, true
		}
	}
	return "", "", simpleParserDiagnostic("unterminated parenthesized expression"), false
}

func splitSimpleCommaList(text string) []string {
	parts := make([]string, 0, 1)
	start := 0
	var quote byte
	depth := 0
	for i := 0; i < len(text); i++ {
		ch := text[i]
		if ch == '\'' || ch == '"' {
			if quote == ch && i+1 < len(text) && text[i+1] == ch {
				i++
				continue
			}
			if quote == ch {
				quote = 0
				continue
			}
			if quote == 0 {
				quote = ch
			}
			continue
		}
		if quote != 0 {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth > 0 {
				continue
			}
			parts = append(parts, text[start:i])
			start = i + 1
		}
	}
	parts = append(parts, text[start:])
	return parts
}

func resolveSimpleOrderByProjection(sort UnboundSort, projections []UnboundProjection, aggregates []UnboundAggregate) (UnboundSort, Diagnostic, bool) {
	if aggregateRef, diagnostic, ok := resolveSimpleOrderByAggregateCall(sort.Expr, aggregates); ok || diagnostic.Code != "" {
		if !ok {
			return UnboundSort{}, diagnostic, false
		}
		sort.Expr = aggregateRef
		return sort, Diagnostic{}, true
	}
	if ordinal, ok := simpleUnboundIntegerOrdinal(sort.Expr); ok {
		if ordinal < 1 || ordinal > len(projections) {
			return UnboundSort{}, simpleParserDiagnostic("ORDER BY ordinal is out of range"), false
		}
		sort.Expr = projections[ordinal-1].Expr
		return sort, Diagnostic{}, true
	}
	field, ok := sort.Expr.(UnboundFieldExpr)
	if !ok || field.Qualifier != "" {
		return sort, Diagnostic{}, true
	}
	for _, projection := range projections {
		if projection.Alias == "" || !strings.EqualFold(projection.Alias, field.Name) {
			continue
		}
		sort.Expr = projection.Expr
		return sort, Diagnostic{}, true
	}
	return sort, Diagnostic{}, true
}

func simpleUnboundIntegerOrdinal(expr UnboundExpr) (int, bool) {
	literal, ok := expr.(UnboundLiteralExpr)
	if !ok || literal.Kind != ValueInt {
		return 0, false
	}
	value, ok := literal.Value.(int64)
	if !ok {
		return 0, false
	}
	return int(value), true
}

func resolveSimpleGroupByProjections(expressions []UnboundExpr, projections []UnboundProjection) []UnboundExpr {
	resolved := make([]UnboundExpr, 0, len(expressions))
	for _, expr := range expressions {
		field, ok := expr.(UnboundFieldExpr)
		if !ok || field.Qualifier != "" {
			resolved = append(resolved, expr)
			continue
		}
		replacement := expr
		for _, projection := range projections {
			if projection.Alias == "" || !strings.EqualFold(projection.Alias, field.Name) {
				continue
			}
			if _, aggregateRef := projection.Expr.(UnboundAggregateRefExpr); aggregateRef {
				break
			}
			replacement = projection.Expr
			break
		}
		resolved = append(resolved, replacement)
	}
	return resolved
}

func resolveSimpleOrderByAggregateCall(expr UnboundExpr, aggregates []UnboundAggregate) (UnboundAggregateRefExpr, Diagnostic, bool) {
	return resolveSimpleAggregateCall(expr, aggregates, "ORDER BY")
}

func resolveSimpleAggregateCall(expr UnboundExpr, aggregates []UnboundAggregate, clause string) (UnboundAggregateRefExpr, Diagnostic, bool) {
	call, ok := expr.(UnboundCallExpr)
	if !ok {
		return UnboundAggregateRefExpr{}, Diagnostic{}, false
	}
	if !simpleAggregateFunctionName(call.Name) {
		return UnboundAggregateRefExpr{}, Diagnostic{}, false
	}
	if len(call.Args) != 1 {
		return UnboundAggregateRefExpr{}, simpleParserDiagnostic(clause + " aggregate call must match a SELECT aggregate"), false
	}
	if ref, ok := findSimpleAggregateCallRef(call, aggregates); ok {
		return ref, Diagnostic{}, true
	}
	return UnboundAggregateRefExpr{}, simpleParserDiagnostic(clause + " aggregate call must match a SELECT aggregate"), false
}

func findSimpleAggregateCallRef(call UnboundCallExpr, aggregates []UnboundAggregate) (UnboundAggregateRefExpr, bool) {
	for index, aggregate := range aggregates {
		if !strings.EqualFold(aggregate.Function, call.Name) {
			continue
		}
		if strings.EqualFold(aggregate.Function, "count") && aggregate.CountAll {
			if _, ok := call.Args[0].(simpleUnboundWildcardExpr); ok {
				return UnboundAggregateRef(aggregate.Alias, index), true
			}
			continue
		}
		if simpleUnboundExprEqual(aggregate.Input, call.Args[0]) {
			return UnboundAggregateRef(aggregate.Alias, index), true
		}
	}
	return UnboundAggregateRefExpr{}, false
}

func parseSimpleSources(sourceText string) ([]UnboundTable, []UnboundJoin, Diagnostic, bool) {
	leftText, joinText, hasJoin := splitBeforeKeyword(sourceText, "join")
	if !hasJoin {
		table, diagnostic, ok := parseSimpleTable(sourceText)
		if !ok {
			return nil, nil, diagnostic, false
		}
		return []UnboundTable{table}, nil, Diagnostic{}, true
	}

	leftText, kind, diagnostic, ok := parseSimpleJoinKindPrefix(leftText)
	if !ok {
		return nil, nil, diagnostic, false
	}
	leftTable, diagnostic, ok := parseSimpleTable(leftText)
	if !ok {
		return nil, nil, diagnostic, false
	}
	tables := []UnboundTable{leftTable}
	joins := make([]UnboundJoin, 0, 1)
	previousTable := leftTable
	for {
		rightText, onText, ok := splitBeforeKeyword(joinText, "on")
		if !ok {
			return nil, nil, simpleParserDiagnostic("JOIN must include ON"), false
		}
		rightTable, diagnostic, ok := parseSimpleTable(rightText)
		if !ok {
			return nil, nil, diagnostic, false
		}
		nextJoinText, nextKind := "", JoinKindInner
		onText, nextJoinText, nextKind, hasJoin, diagnostic, ok = parseSimpleJoinOnTail(onText)
		if !ok {
			return nil, nil, diagnostic, false
		}
		join, diagnostic, ok := parseSimpleJoinOn(onText, kind)
		if !ok {
			return nil, nil, diagnostic, false
		}
		joins = append(joins, normalizeSimpleJoinEdge(join, previousTable))
		tables = append(tables, rightTable)
		previousTable = rightTable
		if !hasJoin {
			break
		}
		joinText = nextJoinText
		kind = nextKind
	}
	return tables, joins, Diagnostic{}, true
}

func parseSimpleJoinKindPrefix(text string) (string, JoinKind, Diagnostic, bool) {
	kind := JoinKindInner
	if remaining, ok := consumeTrailingKeyword(text, "inner"); ok {
		text = remaining
	} else if remaining, ok := consumeTrailingKeyword(text, "outer"); ok {
		text = remaining
		kind = JoinKindLeftOuter
		if remaining, ok := consumeTrailingKeyword(text, "left"); ok {
			text = remaining
		}
	} else if remaining, ok := consumeTrailingKeyword(text, "left"); ok {
		text = remaining
		kind = JoinKindLeftOuter
	} else if hasAnyKeyword(text, "right", "full") {
		return "", "", simpleParserDiagnostic("only INNER and LEFT OUTER JOIN are supported"), false
	}
	return text, kind, Diagnostic{}, true
}

func parseSimpleJoinOnTail(text string) (string, string, JoinKind, bool, Diagnostic, bool) {
	onText, nextJoinText, hasJoin := splitBeforeKeyword(text, "join")
	if !hasJoin {
		return text, "", "", false, Diagnostic{}, true
	}
	onText, nextKind, diagnostic, ok := parseSimpleJoinKindPrefix(onText)
	if !ok {
		return "", "", "", false, diagnostic, false
	}
	return onText, nextJoinText, nextKind, true, Diagnostic{}, true
}

func parseSimpleJoinOn(text string, kind JoinKind) (UnboundJoin, Diagnostic, bool) {
	parts := splitSimpleAndPredicates(text)
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return UnboundJoin{}, simpleParserDiagnostic("JOIN ON must be an equality between fields"), false
	}
	op, leftText, rightText, ok := splitBeforeComparisonOperator(parts[0])
	if !ok {
		return UnboundJoin{}, simpleParserDiagnostic("JOIN ON must be an equality between fields"), false
	}
	switch op {
	case BinaryOpEqual:
	default:
		return UnboundJoin{}, simpleParserDiagnostic("JOIN ON only supports equality between fields"), false
	}
	leftQualifier, leftField := splitProjectionField(leftText)
	rightQualifier, rightField := splitProjectionField(rightText)
	if leftQualifier == "" || leftField == "" || rightQualifier == "" || rightField == "" {
		return UnboundJoin{}, simpleParserDiagnostic("JOIN ON fields must be qualified"), false
	}
	predicates := make([]UnboundPredicate, 0, len(parts)-1)
	parameterIndex := 1
	for _, part := range parts[1:] {
		parsed, diagnostic, ok := parseSimplePredicate(part, &parameterIndex)
		if !ok {
			return UnboundJoin{}, diagnostic, false
		}
		for _, predicate := range parsed {
			predicate.Scope = PredicateScopeOn
			predicate.Placement = PredicateResidualJoin
			predicates = append(predicates, predicate)
		}
	}
	return UnboundJoin{
		LeftQualifier:  leftQualifier,
		LeftField:      leftField,
		RightQualifier: rightQualifier,
		RightField:     rightField,
		Kind:           kind,
		Predicates:     predicates,
	}, Diagnostic{}, true
}

func normalizeSimpleJoinEdge(join UnboundJoin, leftTable UnboundTable) UnboundJoin {
	if join.Kind != JoinKindLeftOuter {
		return join
	}
	leftRef := tableRefName(leftTable.Name, leftTable.Alias)
	if strings.EqualFold(join.LeftQualifier, leftRef) {
		return join
	}
	if strings.EqualFold(join.RightQualifier, leftRef) {
		join.LeftQualifier, join.RightQualifier = join.RightQualifier, join.LeftQualifier
		join.LeftField, join.RightField = join.RightField, join.LeftField
	}
	return join
}

func parseSimpleTable(sourceText string) (UnboundTable, Diagnostic, bool) {
	fields := strings.Fields(sourceText)
	switch len(fields) {
	case 1:
		schema, table := splitQualifiedName(fields[0])
		return UnboundTable{Schema: schema, Name: table}, Diagnostic{}, true
	case 2:
		if strings.EqualFold(fields[1], "as") {
			return UnboundTable{}, simpleParserDiagnostic("table alias is missing after AS"), false
		}
		schema, table := splitQualifiedName(fields[0])
		return UnboundTable{Schema: schema, Name: table, Alias: fields[1]}, Diagnostic{}, true
	case 3:
		if !strings.EqualFold(fields[1], "as") {
			return UnboundTable{}, simpleParserDiagnostic("unexpected table source syntax"), false
		}
		schema, table := splitQualifiedName(fields[0])
		return UnboundTable{Schema: schema, Name: table, Alias: fields[2]}, Diagnostic{}, true
	default:
		return UnboundTable{}, simpleParserDiagnostic("expected one table source"), false
	}
}

func parseSimpleProjections(projectionText string) ([]UnboundProjection, []UnboundAggregate, Diagnostic, bool) {
	parts := splitSimpleCommaList(projectionText)
	projections := make([]UnboundProjection, 0, len(parts))
	aggregates := make([]UnboundAggregate, 0)
	for _, part := range parts {
		projection, projectionAggregates, diagnostic, ok := parseSimpleProjection(part, len(aggregates))
		if !ok {
			return nil, nil, diagnostic, false
		}
		projections = append(projections, projection)
		aggregates = append(aggregates, projectionAggregates...)
	}
	if len(projections) == 0 {
		return nil, nil, simpleParserDiagnostic("SELECT list is empty"), false
	}
	return projections, aggregates, Diagnostic{}, true
}

func parseSimpleProjection(text string, aggregateIndex int) (UnboundProjection, []UnboundAggregate, Diagnostic, bool) {
	exprText, alias, diagnostic, ok := parseSimpleProjectionAlias(text)
	if !ok {
		return UnboundProjection{}, nil, diagnostic, false
	}
	if aggregate, projection, ok := parseSimpleCountProjection(exprText, alias, aggregateIndex); ok {
		return projection, []UnboundAggregate{aggregate}, Diagnostic{}, true
	}
	if aggregate, projection, ok := parseSimpleAggregateProjection(exprText, alias, aggregateIndex); ok {
		return projection, []UnboundAggregate{aggregate}, Diagnostic{}, true
	}
	if aggregates, projection, ok := parseSimpleAggregateArithmeticProjection(exprText, alias, aggregateIndex); ok {
		return projection, aggregates, Diagnostic{}, true
	}
	if expr, ok := parseSimpleSystemVariableExpression(exprText); ok {
		return UnboundProjection{
			Expr:  expr,
			Alias: alias,
		}, nil, Diagnostic{}, true
	}
	if literal, _, ok := parseSimpleLiteral(exprText); ok {
		return UnboundProjection{
			Expr:  literal,
			Alias: alias,
		}, nil, Diagnostic{}, true
	}
	if expr, ok := parseSimpleArithmeticExpression(exprText); ok {
		return UnboundProjection{
			Expr:  expr,
			Alias: alias,
			Type:  DataTypeFloat,
		}, nil, Diagnostic{}, true
	}
	if expr, ok := parseSimpleScalarCallExpression(exprText); ok {
		return UnboundProjection{
			Expr:  expr,
			Alias: alias,
		}, nil, Diagnostic{}, true
	}
	if expr, ok := parseSimpleScalarSubqueryExpression(exprText, PredicateScopeProjection); ok {
		return UnboundProjection{
			Expr:  expr,
			Alias: alias,
		}, nil, Diagnostic{}, true
	}
	qualifier, field := splitProjectionField(exprText)
	if field == "*" {
		if alias != "" {
			return UnboundProjection{}, nil, simpleParserDiagnostic("wildcard projections cannot be aliased"), false
		}
		return UnboundProjection{
			Expr: UnboundField(qualifier, field),
		}, nil, Diagnostic{}, true
	}
	if field == "" {
		return UnboundProjection{}, nil, simpleParserDiagnostic("projection field is empty"), false
	}
	return UnboundProjection{
		Expr:  UnboundField(qualifier, field),
		Alias: alias,
	}, nil, Diagnostic{}, true
}

func parseSimpleCountProjection(exprText string, alias string, aggregateIndex int) (UnboundAggregate, UnboundProjection, bool) {
	trimmed := strings.TrimSpace(exprText)
	if !strings.HasPrefix(strings.ToLower(trimmed), "count(") || !strings.HasSuffix(trimmed, ")") {
		return UnboundAggregate{}, UnboundProjection{}, false
	}
	inputText := strings.TrimSpace(trimmed[len("count(") : len(trimmed)-1])
	aggregate := UnboundAggregate{
		Function: "count",
		Alias:    alias,
		Type:     DataTypeInt,
	}
	switch {
	case inputText == "*":
		aggregate.CountAll = true
		if aggregate.Alias == "" {
			aggregate.Alias = "count"
		}
	default:
		distinctInput, ok := consumeKeyword(inputText, "distinct")
		if !ok {
			return UnboundAggregate{}, UnboundProjection{}, false
		}
		input, inputOK := parseSimpleScalarExpression(distinctInput)
		if !inputOK {
			return UnboundAggregate{}, UnboundProjection{}, false
		}
		aggregate.Mode = AggregateDistinct
		aggregate.Input = input
		if aggregate.Alias == "" {
			aggregate.Alias = "count"
		}
	}
	return aggregate, UnboundProjection{
		Expr:  UnboundAggregateRef(aggregate.Alias, aggregateIndex),
		Alias: aggregate.Alias,
		Type:  DataTypeInt,
	}, true
}

func parseSimpleProjectionAlias(text string) (string, string, Diagnostic, bool) {
	trimmed := strings.TrimSpace(text)
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return "", "", simpleParserDiagnostic("empty projection"), false
	}
	for index, field := range fields {
		if !strings.EqualFold(field, "as") {
			continue
		}
		if index == 0 || index != len(fields)-2 {
			return "", "", simpleParserDiagnostic("unexpected projection alias syntax"), false
		}
		return strings.Join(fields[:index], " "), fields[index+1], Diagnostic{}, true
	}
	if len(fields) == 2 {
		return fields[0], fields[1], Diagnostic{}, true
	}
	return trimmed, "", Diagnostic{}, true
}

func parseSimpleArithmeticExpression(text string) (UnboundExpr, bool) {
	parser := simpleArithmeticParser{text: strings.TrimSpace(text)}
	expr, ok := parser.parseExpression()
	if !ok {
		return nil, false
	}
	parser.skipSpaces()
	if parser.pos != len(parser.text) || !parser.sawOperator {
		return nil, false
	}
	return expr, true
}

func parseSimpleScalarExpression(text string) (UnboundExpr, bool) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil, false
	}
	if expr, ok := parseSimpleSystemVariableExpression(trimmed); ok {
		return expr, true
	}
	if literal, _, ok := parseSimpleLiteral(trimmed); ok {
		return literal, true
	}
	if expr, ok := parseSimpleSearchedCaseExpression(trimmed); ok {
		return expr, true
	}
	if call, ok := parseSimpleScalarCallExpression(trimmed); ok {
		return call, true
	}
	if expr, ok := parseSimpleArithmeticExpression(trimmed); ok {
		return expr, true
	}
	qualifier, field := splitProjectionField(trimmed)
	if field == "" || field == "*" {
		return nil, false
	}
	return UnboundField(qualifier, field), true
}

func parseSimpleSearchedCaseExpression(text string) (UnboundSearchedCaseExpr, bool) {
	body, ok := consumeKeyword(text, "case")
	if !ok {
		return UnboundSearchedCaseExpr{}, false
	}
	whens := make([]UnboundSearchedCaseWhen, 0, 1)
	remaining := body
	parameterIndex := 1
	for {
		next, ok := consumeKeyword(remaining, "when")
		if !ok {
			return UnboundSearchedCaseExpr{}, false
		}
		conditionText, thenBody, ok := splitBeforeKeyword(next, "then")
		if !ok {
			return UnboundSearchedCaseExpr{}, false
		}
		predicates, diagnostic, ok := parseSimplePredicate(conditionText, &parameterIndex)
		if !ok || diagnostic.Code != "" || len(predicates) != 1 {
			return UnboundSearchedCaseExpr{}, false
		}
		resultText, keyword, tail, ok := splitBeforeFirstSimpleKeyword(thenBody, "when", "else", "end")
		if !ok {
			return UnboundSearchedCaseExpr{}, false
		}
		result, ok := parseSimpleScalarExpression(resultText)
		if !ok {
			return UnboundSearchedCaseExpr{}, false
		}
		whens = append(whens, UnboundSearchedCaseWhen{Condition: predicates[0].Expr, Result: result})
		switch keyword {
		case "when":
			remaining = "when " + tail
		case "else":
			endIndex, endOffset, ok := findSimpleKeyword(tail, "end")
			if !ok {
				return UnboundSearchedCaseExpr{}, false
			}
			elseText := strings.TrimSpace(tail[:endIndex])
			endTail := strings.TrimSpace(tail[endOffset:])
			if elseText == "" || endTail != "" {
				return UnboundSearchedCaseExpr{}, false
			}
			elseExpr, ok := parseSimpleScalarExpression(elseText)
			if !ok {
				return UnboundSearchedCaseExpr{}, false
			}
			return UnboundSearchedCase(whens, elseExpr), true
		case "end":
			if strings.TrimSpace(tail) != "" {
				return UnboundSearchedCaseExpr{}, false
			}
			return UnboundSearchedCase(whens, nil), true
		default:
			return UnboundSearchedCaseExpr{}, false
		}
	}
}

func parseSimpleScalarCallExpression(text string) (UnboundCallExpr, bool) {
	trimmed := strings.TrimSpace(text)
	open := strings.Index(trimmed, "(")
	if open <= 0 || !strings.HasSuffix(trimmed, ")") {
		return UnboundCallExpr{}, false
	}
	function := strings.ToLower(strings.TrimSpace(trimmed[:open]))
	if !simpleScalarFunctionName(function) {
		return UnboundCallExpr{}, false
	}
	inputText := strings.TrimSpace(trimmed[open+1 : len(trimmed)-1])
	if inputText == "" {
		if simpleZeroArgumentScalarFunctionName(function) {
			return UnboundCall(function), true
		}
		return UnboundCallExpr{}, false
	}
	argTexts := splitSimpleCommaList(inputText)
	args := make([]UnboundExpr, 0, len(argTexts))
	for _, argText := range argTexts {
		argText = strings.TrimSpace(argText)
		if argText == "" {
			return UnboundCallExpr{}, false
		}
		if argText == "?" {
			args = append(args, UnboundParameter(len(args)+1, DataTypeUnknown))
			continue
		}
		arg, ok := parseSimpleScalarExpression(argText)
		if !ok {
			return UnboundCallExpr{}, false
		}
		args = append(args, arg)
	}
	return UnboundCall(function, args...), true
}

func simpleScalarFunctionName(function string) bool {
	return IsBuiltinSQLScalarFunction(function)
}

func simpleZeroArgumentScalarFunctionName(function string) bool {
	switch strings.ToLower(strings.TrimSpace(function)) {
	case "database", "schema", "version", "user", "current_user", "connection_id":
		return true
	default:
		return false
	}
}

func parseSimpleSystemVariableExpression(text string) (UnboundExpr, bool) {
	name := normalizeSimpleSystemVariableName(text)
	if name == "" || !strings.HasPrefix(strings.TrimSpace(text), "@@") {
		return nil, false
	}
	return UnboundCall("qs_session_variable", UnboundLiteral(ValueString, name)), true
}

func normalizeSimpleSystemVariableName(text string) string {
	trimmed := strings.TrimSpace(text)
	trimmed = strings.TrimPrefix(trimmed, "@@")
	trimmed = strings.TrimPrefix(strings.TrimSpace(trimmed), "session.")
	trimmed = strings.TrimPrefix(strings.TrimSpace(trimmed), "SESSION.")
	trimmed = strings.TrimPrefix(strings.TrimSpace(trimmed), "global.")
	trimmed = strings.TrimPrefix(strings.TrimSpace(trimmed), "GLOBAL.")
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" || strings.ContainsAny(trimmed, " \t\r\n()") {
		return ""
	}
	return strings.ToLower(trimmed)
}

func parseSimpleAggregateCallExpression(text string) (UnboundCallExpr, bool) {
	trimmed := strings.TrimSpace(text)
	open := strings.Index(trimmed, "(")
	if open <= 0 || !strings.HasSuffix(trimmed, ")") {
		return UnboundCallExpr{}, false
	}
	function := strings.ToLower(strings.TrimSpace(trimmed[:open]))
	inputText, ok := simpleStripBalancedParens(trimmed[open:])
	if !ok {
		return UnboundCallExpr{}, false
	}
	if function == "count" && inputText == "*" {
		return UnboundCall("count", simpleUnboundWildcardExpr{}), true
	}
	if !simpleAggregateFunctionName(function) {
		return UnboundCallExpr{}, false
	}
	input, ok := parseSimpleScalarExpression(inputText)
	if !ok {
		return UnboundCallExpr{}, false
	}
	return UnboundCall(function, input), true
}

type simpleArithmeticParser struct {
	text        string
	pos         int
	sawOperator bool
}

type simpleUnboundWildcardExpr struct{}

// BindExpr rejects wildcard binding because wildcard markers are parser-local.
func (simpleUnboundWildcardExpr) BindExpr(context *BindContext, roles FieldRole) (Expr, DiagnosticSet) {
	return nil, DiagnosticSet{
		ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "wildcard expression can only be used while matching aggregate calls"),
	}
}

func (p *simpleArithmeticParser) parseExpression() (UnboundExpr, bool) {
	left, ok := p.parseTerm()
	if !ok {
		return nil, false
	}
	for {
		p.skipSpaces()
		op, ok := p.consumeAddSub()
		if !ok {
			return left, true
		}
		right, ok := p.parseTerm()
		if !ok {
			return nil, false
		}
		p.sawOperator = true
		left = UnboundBinary(op, left, right)
	}
}

func (p *simpleArithmeticParser) parseTerm() (UnboundExpr, bool) {
	left, ok := p.parsePrimary()
	if !ok {
		return nil, false
	}
	for {
		p.skipSpaces()
		op, ok := p.consumeMulDiv()
		if !ok {
			return left, true
		}
		right, ok := p.parsePrimary()
		if !ok {
			return nil, false
		}
		p.sawOperator = true
		left = UnboundBinary(op, left, right)
	}
}

func (p *simpleArithmeticParser) parsePrimary() (UnboundExpr, bool) {
	p.skipSpaces()
	if p.consume("(") {
		expr, ok := p.parseExpression()
		if !ok {
			return nil, false
		}
		p.skipSpaces()
		if !p.consume(")") {
			return nil, false
		}
		return expr, true
	}
	token := p.consumeToken()
	if token == "" {
		return nil, false
	}
	if literal, _, ok := parseSimpleLiteral(token); ok {
		return literal, true
	}
	qualifier, field := splitProjectionField(token)
	if field == "" || field == "*" {
		return nil, false
	}
	return UnboundField(qualifier, field), true
}

func (p *simpleArithmeticParser) consumeAddSub() (BinaryOp, bool) {
	switch {
	case p.consume("+"):
		return BinaryOpAdd, true
	case p.consume("-"):
		return BinaryOpSubtract, true
	default:
		return "", false
	}
}

func (p *simpleArithmeticParser) consumeMulDiv() (BinaryOp, bool) {
	switch {
	case p.consume("*"):
		return BinaryOpMultiply, true
	case p.consume("/"):
		return BinaryOpDivide, true
	default:
		return "", false
	}
}

func (p *simpleArithmeticParser) consumeToken() string {
	start := p.pos
	for p.pos < len(p.text) {
		switch p.text[p.pos] {
		case ' ', '\t', '\n', '\r', '(', ')', '+', '-', '*', '/':
			if p.pos == start {
				return ""
			}
			return p.text[start:p.pos]
		default:
			p.pos++
		}
	}
	return p.text[start:p.pos]
}

func (p *simpleArithmeticParser) consume(token string) bool {
	if strings.HasPrefix(p.text[p.pos:], token) {
		p.pos += len(token)
		return true
	}
	return false
}

func (p *simpleArithmeticParser) skipSpaces() {
	for p.pos < len(p.text) {
		switch p.text[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func parseSimpleAggregateProjection(fieldToken string, alias string, aggregateIndex int) (UnboundAggregate, UnboundProjection, bool) {
	call, ok := parseSimpleAggregateCallExpression(fieldToken)
	if !ok || call.Name == "count" || len(call.Args) != 1 {
		return UnboundAggregate{}, UnboundProjection{}, false
	}
	function := call.Name
	input := call.Args[0]
	if alias == "" {
		alias = function
	}
	returnType := simpleAggregateReturnType(function)
	aggregate := UnboundAggregate{
		Function: function,
		Input:    input,
		Alias:    alias,
		Type:     returnType,
	}
	projection := UnboundProjection{
		Expr:  UnboundAggregateRef(alias, aggregateIndex),
		Alias: alias,
		Type:  returnType,
	}
	return aggregate, projection, true
}

func parseSimpleAggregateArithmeticProjection(exprText string, alias string, aggregateIndex int) ([]UnboundAggregate, UnboundProjection, bool) {
	expr, aggregates, ok := parseSimpleAggregateArithmeticExpression(exprText, aggregateIndex)
	if !ok || len(aggregates) == 0 {
		return nil, UnboundProjection{}, false
	}
	return aggregates, UnboundProjection{
		Expr:  expr,
		Alias: alias,
		Type:  DataTypeFloat,
	}, true
}

func parseSimpleAggregateArithmeticExpression(text string, aggregateIndex int) (UnboundExpr, []UnboundAggregate, bool) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil, nil, false
	}
	if strings.HasPrefix(trimmed, "(") && strings.HasSuffix(trimmed, ")") {
		if inner, ok := simpleStripBalancedParens(trimmed); ok {
			return parseSimpleAggregateArithmeticExpression(inner, aggregateIndex)
		}
	}
	leftText, op, rightText, ok := splitSimpleTopLevelArithmeticOperator(trimmed)
	if ok {
		left, leftAggregates, leftOK := parseSimpleAggregateArithmeticExpression(leftText, aggregateIndex)
		if !leftOK {
			return nil, nil, false
		}
		right, rightAggregates, rightOK := parseSimpleAggregateArithmeticExpression(rightText, aggregateIndex+len(leftAggregates))
		if !rightOK {
			return nil, nil, false
		}
		aggregates := append(leftAggregates, rightAggregates...)
		return UnboundBinary(op, left, right), aggregates, true
	}
	if call, ok := parseSimpleAggregateCallExpression(trimmed); ok && len(call.Args) == 1 && call.Name != "count" {
		alias := "__agg_" + strconv.Itoa(aggregateIndex)
		aggregate := UnboundAggregate{
			Function: call.Name,
			Input:    call.Args[0],
			Alias:    alias,
			Type:     simpleAggregateReturnType(call.Name),
		}
		return UnboundAggregateRef(alias, aggregateIndex), []UnboundAggregate{aggregate}, true
	}
	if literal, _, ok := parseSimpleLiteral(trimmed); ok {
		return literal, nil, true
	}
	return nil, nil, false
}

func splitSimpleTopLevelArithmeticOperator(text string) (string, BinaryOp, string, bool) {
	if left, op, right, ok := splitSimpleTopLevelArithmeticOperatorSet(text, "+-"); ok {
		if op == '+' {
			return left, BinaryOpAdd, right, true
		}
		return left, BinaryOpSubtract, right, true
	}
	if left, op, right, ok := splitSimpleTopLevelArithmeticOperatorSet(text, "*/"); ok {
		if op == '*' {
			return left, BinaryOpMultiply, right, true
		}
		return left, BinaryOpDivide, right, true
	}
	return "", "", "", false
}

func splitSimpleTopLevelArithmeticOperatorSet(text string, operators string) (string, byte, string, bool) {
	depth := 0
	quoted := false
	for i := len(text) - 1; i >= 0; i-- {
		ch := text[i]
		if ch == '\'' {
			quoted = !quoted
			continue
		}
		if quoted {
			continue
		}
		switch ch {
		case ')':
			depth++
			continue
		case '(':
			depth--
			continue
		}
		if depth != 0 || !strings.ContainsRune(operators, rune(ch)) {
			continue
		}
		left := strings.TrimSpace(text[:i])
		right := strings.TrimSpace(text[i+1:])
		if left == "" || right == "" {
			return "", 0, "", false
		}
		return left, ch, right, true
	}
	return "", 0, "", false
}

func simpleStripBalancedParens(text string) (string, bool) {
	depth := 0
	quoted := false
	for i := 0; i < len(text); i++ {
		ch := text[i]
		if ch == '\'' {
			quoted = !quoted
			continue
		}
		if quoted {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(text)-1 {
				return "", false
			}
			if depth < 0 {
				return "", false
			}
		}
	}
	if depth != 0 || quoted {
		return "", false
	}
	return strings.TrimSpace(text[1 : len(text)-1]), true
}

func simpleAggregateReturnType(function string) DataType {
	return BuiltinAggregateReturnType(function)
}

func simpleAggregateFunctionName(function string) bool {
	return IsBuiltinSQLAggregateFunction(function)
}

func parseSimpleAggregateInput(text string) (UnboundExpr, bool) {
	return parseSimpleScalarExpression(text)
}

func parseSimplePredicates(text string) ([]UnboundPredicate, Diagnostic, bool) {
	return parseSimplePredicatesWithParameterIndex(text, 1)
}

func parseSimplePredicatesWithParameterIndex(text string, parameterIndex int) ([]UnboundPredicate, Diagnostic, bool) {
	orParts := splitSimpleOrPredicates(text)
	if len(orParts) > 1 {
		predicates := make([]UnboundPredicate, 0, len(orParts))
		for _, part := range orParts {
			if len(splitSimpleAndPredicates(part)) > 1 {
				return nil, mixedBooleanPredicateDiagnostic("mixed AND/OR predicates require grouped boolean expression support"), false
			}
			parsed, diagnostic, ok := parseSimplePredicate(part, &parameterIndex)
			if !ok {
				return nil, diagnostic, false
			}
			for index := range parsed {
				parsed[index].Combinator = PredicateCombinatorOr
			}
			predicates = append(predicates, parsed...)
		}
		if len(predicates) == 0 {
			return nil, simpleParserDiagnostic("WHERE predicate is empty"), false
		}
		return predicates, Diagnostic{}, true
	}
	parts := splitSimpleAndPredicates(text)
	predicates := make([]UnboundPredicate, 0, len(parts))
	for _, part := range parts {
		parsed, diagnostic, ok := parseSimplePredicate(part, &parameterIndex)
		if !ok {
			return nil, diagnostic, false
		}
		predicates = append(predicates, parsed...)
	}
	if len(predicates) == 0 {
		return nil, simpleParserDiagnostic("WHERE predicate is empty"), false
	}
	return predicates, Diagnostic{}, true
}

func parseSimpleWhere(text string) ([]UnboundPredicate, []UnboundMembership, UnboundExpr, []UnboundSubqueryPlanIntent, []NativeBlocker, Diagnostic, bool) {
	if simpleWhereHasMixedBooleanPredicates(text) {
		if predicates, diagnostic, ok := parseSimpleMixedBooleanWherePredicates(text); ok || diagnostic.Code != "" {
			if !ok {
				return nil, nil, nil, nil, nil, diagnostic, false
			}
			return predicates, nil, nil, nil, nil, Diagnostic{}, true
		}
		whereExpr, diagnostic, ok := parseSimpleBooleanExpression(text)
		if !ok {
			return nil, nil, nil, nil, nil, diagnostic, false
		}
		return nil, nil, whereExpr, nil, []NativeBlocker{mixedBooleanPredicateBlocker("mixed AND/OR predicates require grouped boolean expression lowering")}, Diagnostic{}, true
	}
	if len(splitSimpleOrPredicates(text)) > 1 {
		predicates, diagnostic, ok := parseSimplePredicates(text)
		return predicates, nil, nil, nil, nil, diagnostic, ok
	}
	parts := splitSimpleAndPredicates(text)
	predicates := make([]UnboundPredicate, 0, len(parts))
	memberships := make([]UnboundMembership, 0)
	subqueries := make([]UnboundSubqueryPlanIntent, 0, 1)
	parameterIndex := 1
	for _, part := range parts {
		existsMembership, diagnostic, ok := parseSimpleExistsMembership(part)
		if diagnostic.Code != "" {
			return nil, nil, nil, nil, nil, diagnostic, false
		}
		if ok {
			memberships = append(memberships, existsMembership)
			continue
		}
		existsPredicate, diagnostic, ok := parseSimpleExistsPredicate(part)
		if diagnostic.Code != "" {
			return nil, nil, nil, nil, nil, diagnostic, false
		}
		if ok {
			predicates = append(predicates, existsPredicate)
			continue
		}
		membership, diagnostic, ok := parseSimpleSubqueryMembership(part)
		if diagnostic.Code != "" {
			return nil, nil, nil, nil, nil, diagnostic, false
		}
		if ok {
			memberships = append(memberships, membership)
			continue
		}
		if subquery, diagnostic, ok := parseSimpleCorrelatedAggregateSubqueryIntent(part, parts); diagnostic.Code != "" {
			return nil, nil, nil, nil, nil, diagnostic, false
		} else if ok {
			subqueries = append(subqueries, subquery)
			continue
		}
		parsed, diagnostic, ok := parseSimplePredicate(part, &parameterIndex)
		if !ok {
			return nil, nil, nil, nil, nil, diagnostic, false
		}
		predicates = append(predicates, parsed...)
	}
	if len(predicates) == 0 && len(memberships) == 0 {
		return nil, nil, nil, nil, nil, simpleParserDiagnostic("WHERE predicate is empty"), false
	}
	return predicates, memberships, nil, subqueries, nil, Diagnostic{}, true
}

func parseSimpleCorrelatedAggregateSubqueryIntent(text string, whereParts []string) (UnboundSubqueryPlanIntent, Diagnostic, bool) {
	op, leftText, rightText, ok := splitBeforeComparisonOperator(text)
	if !ok || !simpleCorrelatedAggregateComparisonOperator(op) {
		return UnboundSubqueryPlanIntent{}, Diagnostic{}, false
	}
	leftExpr, ok := parseSimpleScalarExpression(leftText)
	if !ok {
		return UnboundSubqueryPlanIntent{}, Diagnostic{}, false
	}
	outerValue, ok := leftExpr.(UnboundFieldExpr)
	if !ok || outerValue.Qualifier == "" {
		return UnboundSubqueryPlanIntent{}, Diagnostic{}, false
	}
	scalar, ok := parseSimpleScalarSubqueryExpression(rightText, PredicateScopeWhere)
	if !ok {
		return UnboundSubqueryPlanIntent{}, Diagnostic{}, false
	}
	correlated, diagnostic, ok := parseSimpleCorrelatedAggregateSubqueryBody(scalar.SQL, outerValue, whereParts)
	if !ok {
		return UnboundSubqueryPlanIntent{}, diagnostic, false
	}
	correlated.SourcePredicate = strings.TrimSpace(text)
	return UnboundSubqueryPlanIntent{
		Kind:                SubqueryIntentCorrelatedAggregate,
		Capability:          CapabilityScalarSubquery,
		CorrelatedAggregate: &correlated,
	}, Diagnostic{}, true
}

func simpleCorrelatedAggregateComparisonOperator(op BinaryOp) bool {
	switch op {
	case BinaryOpLess, BinaryOpLessEqual, BinaryOpGreater, BinaryOpGreaterEqual:
		return true
	default:
		return false
	}
}

func parseSimpleCorrelatedAggregateSubqueryBody(sql string, outerValue UnboundFieldExpr, whereParts []string) (UnboundCorrelatedAggregateSubqueryIntent, Diagnostic, bool) {
	selectBody, ok := consumeKeyword(sql, "select")
	if !ok {
		return UnboundCorrelatedAggregateSubqueryIntent{}, Diagnostic{}, false
	}
	projectionText, sourceText, ok := splitBeforeTopLevelKeyword(selectBody, "from")
	if !ok {
		return UnboundCorrelatedAggregateSubqueryIntent{}, Diagnostic{}, false
	}
	sourceOnlyText, predicateText, hasWhere := splitOptionalKeyword(sourceText, "where")
	if !hasWhere {
		return UnboundCorrelatedAggregateSubqueryIntent{}, Diagnostic{}, false
	}
	if hasAnyTopLevelKeyword(sourceOnlyText, "join", "group", "having", "order", "limit") ||
		hasAnyTopLevelKeyword(predicateText, "where", "join", "group", "having", "order", "limit") {
		return UnboundCorrelatedAggregateSubqueryIntent{}, Diagnostic{}, false
	}
	innerTable, diagnostic, ok := parseSimpleTable(sourceOnlyText)
	if !ok {
		return UnboundCorrelatedAggregateSubqueryIntent{}, diagnostic, false
	}
	aggregateFunction, factor, innerValue, ok := parseSimpleCorrelatedAggregateProjection(projectionText)
	if !ok {
		return UnboundCorrelatedAggregateSubqueryIntent{}, Diagnostic{}, false
	}
	innerTableRef := tableRefName(innerTable.Name, innerTable.Alias)
	if !simpleQualifierMatchesTable(innerValue.Qualifier, innerTableRef) {
		return UnboundCorrelatedAggregateSubqueryIntent{}, Diagnostic{}, false
	}
	innerKey, outerKey, ok := parseSimpleCorrelatedAggregateKeys(predicateText, innerTableRef)
	if !ok {
		return UnboundCorrelatedAggregateSubqueryIntent{}, Diagnostic{}, false
	}
	return UnboundCorrelatedAggregateSubqueryIntent{
		AggregateFunction: aggregateFunction,
		Factor:            factor,
		OuterValue:        outerValue,
		InnerValue:        innerValue,
		InnerTable:        innerTable,
		InnerKey:          innerKey,
		OuterKey:          outerKey,
		RequiredFilters:   parseSimpleCorrelatedAggregateRequiredFilters(whereParts, outerKey.Qualifier),
		Scope:             PredicateScopeWhere,
	}, Diagnostic{}, true
}

func parseSimpleCorrelatedAggregateProjection(text string) (string, float64, UnboundFieldExpr, bool) {
	if function, field, ok := parseSimpleAggregateFieldProjection(text); ok {
		return function, 1, field, true
	}
	left, right, ok := splitBeforeOperator(text, "*")
	if !ok {
		return "", 0, UnboundFieldExpr{}, false
	}
	if factor, ok := parseSimpleFloatFactor(left); ok {
		if function, field, aggregateOK := parseSimpleAggregateFieldProjection(right); aggregateOK {
			return function, factor, field, true
		}
	}
	if factor, ok := parseSimpleFloatFactor(right); ok {
		if function, field, aggregateOK := parseSimpleAggregateFieldProjection(left); aggregateOK {
			return function, factor, field, true
		}
	}
	return "", 0, UnboundFieldExpr{}, false
}

func parseSimpleAggregateFieldProjection(text string) (string, UnboundFieldExpr, bool) {
	call, ok := parseSimpleAggregateCallExpression(text)
	if !ok || len(call.Args) != 1 {
		return "", UnboundFieldExpr{}, false
	}
	field, ok := call.Args[0].(UnboundFieldExpr)
	if !ok || field.Qualifier == "" {
		return "", UnboundFieldExpr{}, false
	}
	return call.Name, field, true
}

func parseSimpleFloatFactor(text string) (float64, bool) {
	literal, diagnostic, ok := parseSimpleLiteral(strings.TrimSpace(text))
	if !ok || diagnostic.Code != "" {
		return 0, false
	}
	switch literal.Kind {
	case ValueInt:
		if value, ok := literal.Value.(int64); ok {
			return float64(value), true
		}
	case ValueFloat:
		if value, ok := literal.Value.(float64); ok {
			return value, true
		}
	}
	return 0, false
}

func parseSimpleCorrelatedAggregateKeys(text string, innerTableRef string) (UnboundFieldExpr, UnboundFieldExpr, bool) {
	for _, part := range splitSimpleAndPredicates(text) {
		op, leftText, rightText, ok := splitBeforeComparisonOperator(part)
		if !ok || op != BinaryOpEqual {
			continue
		}
		leftExpr, leftOK := parseSimpleScalarExpression(leftText)
		rightExpr, rightOK := parseSimpleScalarExpression(rightText)
		left, leftField := leftExpr.(UnboundFieldExpr)
		right, rightField := rightExpr.(UnboundFieldExpr)
		if !leftOK || !rightOK || !leftField || !rightField {
			continue
		}
		if simpleQualifierMatchesTable(left.Qualifier, innerTableRef) && !simpleQualifierMatchesTable(right.Qualifier, innerTableRef) {
			return left, right, true
		}
		if simpleQualifierMatchesTable(right.Qualifier, innerTableRef) && !simpleQualifierMatchesTable(left.Qualifier, innerTableRef) {
			return right, left, true
		}
	}
	return UnboundFieldExpr{}, UnboundFieldExpr{}, false
}

func parseSimpleCorrelatedAggregateRequiredFilters(whereParts []string, qualifier string) []UnboundFieldExpr {
	if qualifier == "" {
		return nil
	}
	filters := make([]UnboundFieldExpr, 0, 2)
	seen := make(map[string]struct{})
	for _, part := range whereParts {
		op, leftText, rightText, ok := splitBeforeComparisonOperator(part)
		if !ok || op != BinaryOpEqual {
			continue
		}
		leftExpr, leftOK := parseSimpleScalarExpression(leftText)
		rightExpr, rightOK := parseSimpleScalarExpression(rightText)
		if left, ok := leftExpr.(UnboundFieldExpr); ok && leftOK && simpleQualifierMatchesTable(left.Qualifier, qualifier) && simpleCorrelatedAggregateLiteralExpr(rightExpr, rightOK) {
			key := strings.ToLower(left.Qualifier + "." + left.Name)
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				filters = append(filters, left)
			}
		}
		if right, ok := rightExpr.(UnboundFieldExpr); ok && rightOK && simpleQualifierMatchesTable(right.Qualifier, qualifier) && simpleCorrelatedAggregateLiteralExpr(leftExpr, leftOK) {
			key := strings.ToLower(right.Qualifier + "." + right.Name)
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				filters = append(filters, right)
			}
		}
	}
	return filters
}

func simpleCorrelatedAggregateLiteralExpr(expr UnboundExpr, ok bool) bool {
	if !ok {
		return false
	}
	_, literal := expr.(UnboundLiteralExpr)
	return literal
}

func parseSimpleMixedBooleanWherePredicates(text string) ([]UnboundPredicate, Diagnostic, bool) {
	parts := splitSimpleBooleanParts(text, "and")
	if len(parts) <= 1 {
		return nil, Diagnostic{}, false
	}
	predicates := make([]UnboundPredicate, 0, len(parts))
	parameterIndex := 1
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, simpleParserDiagnostic("WHERE predicate is empty"), false
		}
		strippedPart, stripped := stripSimpleEnclosingParensWithStatus(part)
		if simpleWhereHasMixedBooleanPredicates(part) || len(splitSimpleBooleanParts(strippedPart, "or")) > 1 {
			if !stripped {
				return nil, Diagnostic{}, false
			}
			expr, diagnostic, ok := parseSimpleBooleanExpression(strippedPart)
			if !ok {
				return nil, diagnostic, false
			}
			predicates = append(predicates, UnboundPredicate{
				Expr:      expr,
				Placement: PredicateResidualScan,
				Scope:     PredicateScopeWhere,
			})
			continue
		}
		parsed, diagnostic, ok := parseSimplePredicate(part, &parameterIndex)
		if !ok {
			return nil, diagnostic, false
		}
		predicates = append(predicates, parsed...)
	}
	if len(predicates) == 0 {
		return nil, simpleParserDiagnostic("WHERE predicate is empty"), false
	}
	return predicates, Diagnostic{}, true
}

func simpleWhereHasMixedBooleanPredicates(text string) bool {
	orParts := splitSimpleBooleanParts(text, "or")
	if len(orParts) > 1 {
		for _, part := range orParts {
			if len(splitSimpleBooleanParts(stripSimpleEnclosingParens(part), "and")) > 1 {
				return true
			}
		}
	}
	andParts := splitSimpleBooleanParts(text, "and")
	if len(andParts) > 1 {
		for _, part := range andParts {
			if len(splitSimpleBooleanParts(stripSimpleEnclosingParens(part), "or")) > 1 {
				return true
			}
		}
	}
	return false
}

func parseSimpleBooleanExpression(text string) (UnboundExpr, Diagnostic, bool) {
	parameterIndex := 1
	return parseSimpleBooleanOr(text, &parameterIndex)
}

func parseSimpleBooleanOr(text string, parameterIndex *int) (UnboundExpr, Diagnostic, bool) {
	parts := splitSimpleBooleanParts(text, "or")
	if len(parts) == 1 {
		return parseSimpleBooleanAnd(parts[0], parameterIndex)
	}
	return parseSimpleBooleanBinary(parts, BinaryOpOr, parseSimpleBooleanAnd, parameterIndex)
}

func parseSimpleBooleanAnd(text string, parameterIndex *int) (UnboundExpr, Diagnostic, bool) {
	parts := splitSimpleBooleanParts(text, "and")
	if len(parts) == 1 {
		return parseSimpleBooleanLeaf(parts[0], parameterIndex)
	}
	return parseSimpleBooleanBinary(parts, BinaryOpAnd, parseSimpleBooleanLeaf, parameterIndex)
}

func parseSimpleBooleanBinary(parts []string, op BinaryOp, parsePart func(string, *int) (UnboundExpr, Diagnostic, bool), parameterIndex *int) (UnboundExpr, Diagnostic, bool) {
	left, diagnostic, ok := parsePart(parts[0], parameterIndex)
	if !ok {
		return nil, diagnostic, false
	}
	for _, part := range parts[1:] {
		right, rightDiagnostic, rightOK := parsePart(part, parameterIndex)
		if !rightOK {
			return nil, rightDiagnostic, false
		}
		left = UnboundBinary(op, left, right)
	}
	return left, Diagnostic{}, true
}

func parseSimpleBooleanLeaf(text string, parameterIndex *int) (UnboundExpr, Diagnostic, bool) {
	trimmed, stripped := stripSimpleEnclosingParensWithStatus(text)
	if trimmed == "" {
		return nil, simpleParserDiagnostic("boolean expression contains an empty predicate"), false
	}
	if stripped {
		return parseSimpleBooleanOr(trimmed, parameterIndex)
	}
	parsed, diagnostic, ok := parseSimplePredicate(trimmed, parameterIndex)
	if !ok {
		return nil, diagnostic, false
	}
	if len(parsed) != 1 {
		return nil, simpleParserDiagnostic("boolean expression leaf must produce one predicate"), false
	}
	return parsed[0].Expr, Diagnostic{}, true
}

func parseSimpleSubqueryMembership(text string) (UnboundMembership, Diagnostic, bool) {
	left, right, ok := splitBeforeKeyword(text, "in")
	if !ok {
		return UnboundMembership{}, Diagnostic{}, false
	}
	kind := MembershipSemi
	left = strings.TrimSpace(left)
	if remaining, ok := consumeTrailingKeyword(left, "not"); ok {
		left = remaining
		kind = MembershipAnti
	}
	leftQualifier, leftField := splitProjectionField(strings.TrimSpace(left))
	if leftField == "" {
		return UnboundMembership{}, simpleParserDiagnostic("subquery membership field is empty"), false
	}
	body, diagnostic, ok := parseSimpleMembershipSubqueryBody(right)
	if !ok {
		if diagnostic.Code != "" {
			return UnboundMembership{}, diagnostic, false
		}
		return UnboundMembership{}, Diagnostic{}, false
	}
	projectionText, sourceText, ok := splitBeforeKeyword(body, "from")
	if !ok {
		return UnboundMembership{}, simpleParserDiagnostic("membership subquery must include FROM"), false
	}
	sourceOnlyText, predicateText, hasWhere := splitOptionalKeyword(sourceText, "where")
	if hasAnyKeyword(sourceOnlyText, "join", "group", "having", "order", "limit") {
		return UnboundMembership{}, simpleParserDiagnostic("membership subquery only supports a single table source"), false
	}
	if hasWhere && hasAnyKeyword(predicateText, "where", "join", "group", "having", "order", "limit") {
		return UnboundMembership{}, simpleParserDiagnostic("membership subquery only supports simple WHERE predicates"), false
	}
	table, diagnostic, ok := parseSimpleTable(sourceOnlyText)
	if !ok {
		return UnboundMembership{}, diagnostic, false
	}
	rightQualifier, rightField := splitProjectionField(strings.TrimSpace(projectionText))
	if rightField == "" {
		return UnboundMembership{}, simpleParserDiagnostic("membership subquery SELECT field is empty"), false
	}
	if rightQualifier == "" {
		rightQualifier = tableRefName(table.Name, table.Alias)
	}
	predicates := []UnboundPredicate(nil)
	if hasWhere {
		var predicateDiagnostic Diagnostic
		predicates, predicateDiagnostic, ok = parseSimplePredicates(predicateText)
		if !ok {
			return UnboundMembership{}, predicateDiagnostic, false
		}
		predicates = qualifySimpleMembershipPredicates(predicates, rightQualifier)
	}
	return UnboundMembership{
		LeftQualifier:  leftQualifier,
		LeftField:      leftField,
		RightTable:     table,
		RightQualifier: rightQualifier,
		RightField:     rightField,
		Predicates:     predicates,
		Kind:           kind,
	}, Diagnostic{}, true
}

func qualifySimpleMembershipPredicates(predicates []UnboundPredicate, qualifier string) []UnboundPredicate {
	qualified := make([]UnboundPredicate, len(predicates))
	for i, predicate := range predicates {
		predicate.Expr = qualifySimpleMembershipExpr(predicate.Expr, qualifier)
		qualified[i] = predicate
	}
	return qualified
}

func qualifySimpleMembershipExpr(expr UnboundExpr, qualifier string) UnboundExpr {
	if qualifier == "" {
		return expr
	}
	switch typed := expr.(type) {
	case UnboundFieldExpr:
		if typed.Qualifier == "" {
			typed.Qualifier = qualifier
		}
		return typed
	case UnboundBinaryExpr:
		typed.Left = qualifySimpleMembershipExpr(typed.Left, qualifier)
		typed.Right = qualifySimpleMembershipExpr(typed.Right, qualifier)
		return typed
	case UnboundListExpr:
		for i := range typed.Items {
			typed.Items[i] = qualifySimpleMembershipExpr(typed.Items[i], qualifier)
		}
		return typed
	case UnboundCallExpr:
		for i := range typed.Args {
			typed.Args[i] = qualifySimpleMembershipExpr(typed.Args[i], qualifier)
		}
		return typed
	case UnboundSearchedCaseExpr:
		for i := range typed.Whens {
			typed.Whens[i].Condition = qualifySimpleMembershipExpr(typed.Whens[i].Condition, qualifier)
			typed.Whens[i].Result = qualifySimpleMembershipExpr(typed.Whens[i].Result, qualifier)
		}
		typed.Else = qualifySimpleMembershipExpr(typed.Else, qualifier)
		return typed
	default:
		return expr
	}
}

func parseSimpleExistsMembership(text string) (UnboundMembership, Diagnostic, bool) {
	trimmed := strings.TrimSpace(text)
	kind := MembershipSemi
	existsBody, ok := consumeKeyword(trimmed, "exists")
	if !ok {
		if remaining, notOK := consumeKeyword(trimmed, "not"); notOK {
			existsBody, ok = consumeKeyword(remaining, "exists")
			if ok {
				kind = MembershipAnti
			}
		}
	}
	if !ok {
		return UnboundMembership{}, Diagnostic{}, false
	}
	body, diagnostic, ok := parseSimpleMembershipSubqueryBody(existsBody)
	if !ok {
		if diagnostic.Code != "" {
			return UnboundMembership{}, diagnostic, false
		}
		return UnboundMembership{}, simpleParserDiagnostic("EXISTS subquery must be a SELECT"), false
	}
	_, sourceText, ok := splitBeforeKeyword(body, "from")
	if !ok {
		return UnboundMembership{}, simpleParserDiagnostic("EXISTS subquery must include FROM"), false
	}
	sourceOnlyText, predicateText, hasWhere := splitOptionalKeyword(sourceText, "where")
	if !hasWhere {
		return UnboundMembership{}, Diagnostic{}, false
	}
	if hasAnyKeyword(sourceOnlyText, "join", "group", "having", "order", "limit") {
		return UnboundMembership{}, simpleParserDiagnostic("EXISTS subquery only supports a single table source"), false
	}
	if hasAnyKeyword(predicateText, "where", "join", "group", "having", "order", "limit") {
		return UnboundMembership{}, simpleParserDiagnostic("EXISTS subquery only supports simple AND-combined predicates"), false
	}
	table, diagnostic, ok := parseSimpleTable(sourceOnlyText)
	if !ok {
		return UnboundMembership{}, diagnostic, false
	}
	tableRef := tableRefName(table.Name, table.Alias)
	parts := splitSimpleAndPredicates(predicateText)
	predicates := make([]UnboundPredicate, 0, len(parts)-1)
	parameterIndex := 1
	membership := UnboundMembership{}
	correlationFound := false
	for _, part := range parts {
		op, leftText, rightText, ok := splitBeforeComparisonOperator(part)
		if !correlationFound && ok && op == BinaryOpEqual {
			leftQualifier, leftField := splitProjectionField(strings.TrimSpace(leftText))
			rightQualifier, rightField := splitProjectionField(strings.TrimSpace(rightText))
			if simpleQualifierMatchesTable(leftQualifier, tableRef) && !simpleQualifierMatchesTable(rightQualifier, tableRef) {
				membership = UnboundMembership{
					LeftQualifier:  rightQualifier,
					LeftField:      rightField,
					RightTable:     table,
					RightQualifier: leftQualifier,
					RightField:     leftField,
					Kind:           kind,
				}
				correlationFound = true
				continue
			}
			if simpleQualifierMatchesTable(rightQualifier, tableRef) && !simpleQualifierMatchesTable(leftQualifier, tableRef) {
				membership = UnboundMembership{
					LeftQualifier:  leftQualifier,
					LeftField:      leftField,
					RightTable:     table,
					RightQualifier: rightQualifier,
					RightField:     rightField,
					Kind:           kind,
				}
				correlationFound = true
				continue
			}
		}
		parsed, diagnostic, ok := parseSimplePredicate(part, &parameterIndex)
		if !ok {
			return UnboundMembership{}, diagnostic, false
		}
		for i := range parsed {
			parsed[i].Placement = PredicateResidualScan
		}
		predicates = append(predicates, parsed...)
	}
	if !correlationFound {
		return UnboundMembership{}, Diagnostic{}, false
	}
	membership.Predicates = predicates
	return membership, Diagnostic{}, true
}

func parseSimpleExistsPredicate(text string) (UnboundPredicate, Diagnostic, bool) {
	trimmed := strings.TrimSpace(text)
	negated := false
	existsBody, ok := consumeKeyword(trimmed, "exists")
	if !ok {
		if remaining, notOK := consumeKeyword(trimmed, "not"); notOK {
			existsBody, ok = consumeKeyword(remaining, "exists")
			negated = ok
		}
	}
	if !ok {
		return UnboundPredicate{}, Diagnostic{}, false
	}
	body, diagnostic, ok := parseSimpleMembershipSubqueryBody(existsBody)
	if !ok {
		if diagnostic.Code != "" {
			return UnboundPredicate{}, diagnostic, false
		}
		return UnboundPredicate{}, simpleParserDiagnostic("EXISTS subquery must be a SELECT"), false
	}
	sql := "select " + strings.TrimSpace(body)
	return UnboundPredicate{
		Expr:         UnboundExistsSubquery(sql, negated, PredicateScopeWhere),
		Placement:    PredicateResidualScan,
		Scope:        PredicateScopeWhere,
		Capabilities: []PlanCapability{CapabilityExistsSubquery},
	}, Diagnostic{}, true
}

func simpleQualifierMatchesTable(qualifier string, tableRef string) bool {
	return qualifier != "" && strings.EqualFold(qualifier, tableRef)
}

func parseSimpleMembershipSubqueryBody(text string) (string, Diagnostic, bool) {
	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, "(") || !strings.HasSuffix(trimmed, ")") {
		return "", Diagnostic{}, false
	}
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "("), ")"))
	selectBody, ok := consumeKeyword(body, "select")
	if !ok {
		return "", Diagnostic{}, false
	}
	return selectBody, Diagnostic{}, true
}

func parseSimplePredicate(text string, parameterIndex *int) ([]UnboundPredicate, Diagnostic, bool) {
	if predicate, diagnostic, ok := parseSimpleNullPredicate(text); ok || diagnostic.Code != "" {
		if !ok {
			return nil, diagnostic, false
		}
		return []UnboundPredicate{predicate}, Diagnostic{}, true
	}
	if predicates, diagnostic, ok := parseSimpleBetweenPredicate(text, parameterIndex); ok || diagnostic.Code != "" {
		return predicates, diagnostic, ok
	}
	if predicate, diagnostic, ok := parseSimpleInPredicate(text, parameterIndex); ok || diagnostic.Code != "" {
		if !ok {
			return nil, diagnostic, false
		}
		return []UnboundPredicate{predicate}, Diagnostic{}, true
	}
	if predicate, diagnostic, ok := parseSimpleLikePredicate(text, parameterIndex); ok || diagnostic.Code != "" {
		if !ok {
			return nil, diagnostic, false
		}
		return []UnboundPredicate{predicate}, Diagnostic{}, true
	}
	op, left, right, ok := splitBeforeComparisonOperator(text)
	if !ok {
		return nil, simpleParserDiagnostic("WHERE must be field comparison literal, BETWEEN range, or IN list"), false
	}
	leftExpr, ok := parseSimpleScalarExpression(left)
	if !ok {
		return nil, simpleParserDiagnostic("WHERE left expression is empty"), false
	}
	comparisonValue, diagnostic, ok := parseSimpleComparisonValue(strings.TrimSpace(right), parameterIndex)
	if !ok {
		rightExpr, rightOK := parseSimpleScalarExpression(right)
		if !rightOK {
			return nil, diagnostic, false
		}
		comparisonValue = rightExpr
	}
	placement := simpleComparisonPredicatePlacement(leftExpr, comparisonValue)
	return []UnboundPredicate{{
		Expr:      UnboundBinary(op, leftExpr, comparisonValue),
		Placement: placement,
		Scope:     PredicateScopeWhere,
	}}, Diagnostic{}, true
}

func parseSimpleLikePredicate(text string, parameterIndex *int) (UnboundPredicate, Diagnostic, bool) {
	left, right, ok := splitBeforeKeyword(text, "like")
	if !ok {
		return UnboundPredicate{}, Diagnostic{}, false
	}
	op := BinaryOpLike
	left = strings.TrimSpace(left)
	if remaining, ok := consumeTrailingKeyword(left, "not"); ok {
		left = remaining
		op = BinaryOpNotLike
	}
	leftExpr, ok := parseSimpleScalarExpression(left)
	if !ok {
		return UnboundPredicate{}, simpleParserDiagnostic("LIKE left expression is empty"), false
	}
	comparisonValue, diagnostic, ok := parseSimpleComparisonValue(strings.TrimSpace(right), parameterIndex)
	if !ok {
		return UnboundPredicate{}, diagnostic, false
	}
	return UnboundPredicate{
		Expr:      UnboundBinary(op, leftExpr, comparisonValue),
		Placement: PredicateResidualScan,
		Scope:     PredicateScopeWhere,
	}, Diagnostic{}, true
}

func parseSimpleNullPredicate(text string) (UnboundPredicate, Diagnostic, bool) {
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return UnboundPredicate{}, Diagnostic{}, false
	}
	if len(fields) != 3 && len(fields) != 4 {
		for _, field := range fields {
			if strings.EqualFold(field, "is") {
				return UnboundPredicate{}, simpleParserDiagnostic("IS NULL must be field IS [NOT] NULL"), false
			}
		}
		return UnboundPredicate{}, Diagnostic{}, false
	}
	if !strings.EqualFold(fields[1], "is") {
		return UnboundPredicate{}, Diagnostic{}, false
	}
	op := BinaryOpEqual
	nullIndex := 2
	if len(fields) == 4 {
		if !strings.EqualFold(fields[2], "not") {
			return UnboundPredicate{}, simpleParserDiagnostic("IS NULL only supports optional NOT"), false
		}
		op = BinaryOpNotEqual
		nullIndex = 3
	}
	if !strings.EqualFold(fields[nullIndex], "null") {
		return UnboundPredicate{}, simpleParserDiagnostic("IS predicate only supports NULL"), false
	}
	qualifier, field := splitProjectionField(fields[0])
	if field == "" {
		return UnboundPredicate{}, simpleParserDiagnostic("IS NULL field is empty"), false
	}
	return UnboundPredicate{
		Expr:      UnboundBinary(op, UnboundField(qualifier, field), UnboundLiteral(ValueNull, nil)),
		Placement: PredicatePushdown,
		Scope:     PredicateScopeWhere,
	}, Diagnostic{}, true
}

func parseSimpleInPredicate(text string, parameterIndex *int) (UnboundPredicate, Diagnostic, bool) {
	left, right, ok := splitBeforeKeyword(text, "in")
	if !ok {
		return UnboundPredicate{}, Diagnostic{}, false
	}
	op := BinaryOpIn
	left = strings.TrimSpace(left)
	if remaining, ok := consumeTrailingKeyword(left, "not"); ok {
		left = remaining
		op = BinaryOpNotIn
	}
	leftExpr, ok := parseSimpleScalarExpression(left)
	if !ok {
		return UnboundPredicate{}, simpleParserDiagnostic("IN left expression is empty"), false
	}
	list, diagnostic, ok := parseSimpleInList(right, parameterIndex)
	if !ok {
		return UnboundPredicate{}, diagnostic, false
	}
	placement := PredicateResidualScan
	if _, ok := leftExpr.(UnboundFieldExpr); ok {
		placement = PredicatePushdown
	}
	return UnboundPredicate{
		Expr:      UnboundBinary(op, leftExpr, list),
		Placement: placement,
		Scope:     PredicateScopeWhere,
	}, Diagnostic{}, true
}

func parseSimpleInList(text string, parameterIndex *int) (UnboundListExpr, Diagnostic, bool) {
	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, "(") || !strings.HasSuffix(trimmed, ")") {
		return UnboundListExpr{}, simpleParserDiagnostic("IN must use a parenthesized literal list"), false
	}
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "("), ")"))
	if body == "" {
		return UnboundListExpr{}, simpleParserDiagnostic("IN list cannot be empty"), false
	}
	parts := strings.Split(body, ",")
	items := make([]UnboundExpr, 0, len(parts))
	for _, part := range parts {
		item, diagnostic, ok := parseSimpleComparisonValue(strings.TrimSpace(part), parameterIndex)
		if !ok {
			return UnboundListExpr{}, diagnostic, false
		}
		items = append(items, item)
	}
	return UnboundList(items...), Diagnostic{}, true
}

func simpleComparisonPredicatePlacement(left UnboundExpr, right UnboundExpr) PredicatePlacement {
	if _, ok := left.(UnboundFieldExpr); !ok {
		return PredicateResidualScan
	}
	switch right.(type) {
	case UnboundLiteralExpr, UnboundParameterExpr, UnboundCallExpr, UnboundScalarSubqueryExpr:
		return PredicatePushdown
	default:
		return PredicateResidualScan
	}
}

func parseSimpleBetweenPredicate(text string, parameterIndex *int) ([]UnboundPredicate, Diagnostic, bool) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil, Diagnostic{}, false
	}
	leftText, rightText, ok := splitBeforeKeyword(trimmed, "between")
	if !ok {
		return nil, Diagnostic{}, false
	}
	negate := false
	leftFields := strings.Fields(leftText)
	if len(leftFields) == 2 && strings.EqualFold(leftFields[1], "not") {
		negate = true
		leftText = leftFields[0]
	} else if strings.Contains(strings.ToLower(leftText), " not") {
		return nil, simpleParserDiagnostic("BETWEEN must be field [NOT] BETWEEN lower AND upper"), false
	}
	lowerText, upperText, ok := splitBeforeKeyword(rightText, "and")
	if !ok {
		return nil, simpleParserDiagnostic("BETWEEN must be field BETWEEN lower AND upper"), false
	}
	qualifier, field := splitProjectionField(leftText)
	if field == "" {
		return nil, simpleParserDiagnostic("BETWEEN field is empty"), false
	}
	lower, diagnostic, ok := parseSimpleComparisonValue(lowerText, parameterIndex)
	if !ok {
		return nil, diagnostic, false
	}
	upper, diagnostic, ok := parseSimpleComparisonValue(upperText, parameterIndex)
	if !ok {
		return nil, diagnostic, false
	}
	op := BinaryOpBetween
	if negate {
		op = BinaryOpNotBetween
	}
	ref := UnboundField(qualifier, field)
	return []UnboundPredicate{
		{
			Expr:      UnboundBinary(op, ref, UnboundList(lower, upper)),
			Placement: PredicatePushdown,
			Scope:     PredicateScopeWhere,
		},
	}, Diagnostic{}, true
}

func parseSimpleComparisonValue(text string, parameterIndex *int) (UnboundExpr, Diagnostic, bool) {
	if subquery, ok := parseSimpleScalarSubqueryExpression(text, PredicateScopeWhere); ok {
		return subquery, Diagnostic{}, true
	}
	if text == "?" {
		if parameterIndex == nil {
			return UnboundParameter(1, DataTypeUnknown), Diagnostic{}, true
		}
		index := *parameterIndex
		*parameterIndex = index + 1
		return UnboundParameter(index, DataTypeUnknown), Diagnostic{}, true
	}
	if call, ok := parseSimpleScalarCallExpression(text); ok {
		return call, Diagnostic{}, true
	}
	return parseSimpleLiteral(text)
}

func parseSimpleScalarSubqueryExpression(text string, scope PredicateScope) (UnboundScalarSubqueryExpr, bool) {
	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, "(") || !strings.HasSuffix(trimmed, ")") {
		return UnboundScalarSubqueryExpr{}, false
	}
	body, ok := simpleStripBalancedParens(trimmed)
	if !ok {
		return UnboundScalarSubqueryExpr{}, false
	}
	if _, ok := consumeKeyword(body, "select"); !ok {
		return UnboundScalarSubqueryExpr{}, false
	}
	return UnboundScalarSubquery(strings.TrimSpace(body), scope), true
}

func parseSimpleLiteral(text string) (UnboundLiteralExpr, Diagnostic, bool) {
	if strings.EqualFold(text, "null") {
		return UnboundLiteral(ValueNull, nil), Diagnostic{}, true
	}
	if strings.EqualFold(text, "true") {
		return UnboundLiteral(ValueBool, true), Diagnostic{}, true
	}
	if strings.EqualFold(text, "false") {
		return UnboundLiteral(ValueBool, false), Diagnostic{}, true
	}
	if len(text) >= 2 && strings.HasPrefix(text, "'") && strings.HasSuffix(text, "'") {
		return UnboundLiteral(ValueString, strings.Trim(text, "'")), Diagnostic{}, true
	}
	if strings.Contains(text, ".") {
		value, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return UnboundLiteralExpr{}, simpleParserDiagnostic("invalid numeric literal"), false
		}
		return UnboundLiteral(ValueFloat, value), Diagnostic{}, true
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return UnboundLiteralExpr{}, simpleParserDiagnostic("literal must be a quoted string or number"), false
	}
	return UnboundLiteral(ValueInt, value), Diagnostic{}, true
}

func splitSimpleBooleanParts(text string, keyword string) []string {
	trimmed := strings.TrimSpace(text)
	lowered := strings.ToLower(trimmed)
	loweredKeyword := strings.ToLower(keyword)
	parts := make([]string, 0, 1)
	start := 0
	depth := 0
	inString := false
	skipNextAnd := false
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] == '\'' {
			if inString && i+1 < len(trimmed) && trimmed[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch trimmed[i] {
		case '(':
			depth++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth != 0 {
			continue
		}
		if loweredKeyword == "and" && strings.HasPrefix(lowered[i:], "between") && isSimpleKeywordBoundary(trimmed, i-1) && isSimpleKeywordBoundary(trimmed, i+len("between")) {
			skipNextAnd = true
			i += len("between") - 1
			continue
		}
		end := i + len(loweredKeyword)
		if end <= len(trimmed) &&
			strings.HasPrefix(lowered[i:], loweredKeyword) &&
			isSimpleKeywordBoundary(trimmed, i-1) &&
			isSimpleKeywordBoundary(trimmed, end) {
			if loweredKeyword == "and" && skipNextAnd {
				skipNextAnd = false
				i = end - 1
				continue
			}
			parts = append(parts, strings.TrimSpace(trimmed[start:i]))
			start = end
			i = end - 1
		}
	}
	parts = append(parts, strings.TrimSpace(trimmed[start:]))
	return parts
}

func stripSimpleEnclosingParens(text string) string {
	trimmed, _ := stripSimpleEnclosingParensWithStatus(text)
	return trimmed
}

func stripSimpleEnclosingParensWithStatus(text string) (string, bool) {
	trimmed := strings.TrimSpace(text)
	stripped := false
	for len(trimmed) >= 2 && strings.HasPrefix(trimmed, "(") && strings.HasSuffix(trimmed, ")") && simpleParensWrapAll(trimmed) {
		trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
		stripped = true
	}
	return trimmed, stripped
}

func simpleParensWrapAll(text string) bool {
	depth := 0
	inString := false
	for i := 0; i < len(text); i++ {
		if text[i] == '\'' {
			if inString && i+1 < len(text) && text[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch text[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(text)-1 {
				return false
			}
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0
}

func splitSimpleAndPredicates(text string) []string {
	return splitSimpleBooleanParts(text, "and")
}

func splitSimpleOrPredicates(text string) []string {
	return splitSimpleBooleanParts(text, "or")
}

func parseSimpleOrderByClause(text string) (string, []UnboundSort, bool, Diagnostic, bool) {
	left, right, ok := splitBeforeKeyword(text, "order")
	if !ok {
		return text, nil, false, Diagnostic{}, true
	}
	orderText, ok := consumeKeyword(right, "by")
	if !ok {
		return "", nil, false, simpleParserDiagnostic("ORDER must be followed by BY"), false
	}
	terms := splitSimpleCommaList(orderText)
	if len(terms) == 0 {
		return "", nil, false, simpleParserDiagnostic("ORDER BY must contain at least one field"), false
	}
	sorts := make([]UnboundSort, 0, len(terms))
	for _, term := range terms {
		sort, diagnostic, ok := parseSimpleOrderByTerm(term)
		if !ok {
			return "", nil, false, diagnostic, false
		}
		sorts = append(sorts, sort)
	}
	return left, sorts, true, Diagnostic{}, true
}

func parseSimpleOrderByTerm(text string) (UnboundSort, Diagnostic, bool) {
	fields := strings.Fields(text)
	if len(fields) == 0 || len(fields) > 2 {
		return UnboundSort{}, simpleParserDiagnostic("ORDER BY term must contain one field and optional direction"), false
	}
	direction := SortAscending
	if len(fields) == 2 {
		switch {
		case strings.EqualFold(fields[1], "asc"):
			direction = SortAscending
		case strings.EqualFold(fields[1], "desc"):
			direction = SortDescending
		default:
			return UnboundSort{}, simpleParserDiagnostic("ORDER BY direction must be ASC or DESC"), false
		}
	}
	if expr, ok := parseSimpleOrderByAggregateExpression(fields[0]); ok {
		return UnboundSort{Expr: expr, Direction: direction}, Diagnostic{}, true
	}
	if expr, ok := parseSimpleScalarExpression(fields[0]); ok {
		return UnboundSort{Expr: expr, Direction: direction}, Diagnostic{}, true
	}
	qualifier, field := splitProjectionField(fields[0])
	if field == "" || field == "*" {
		return UnboundSort{}, simpleParserDiagnostic("ORDER BY field is invalid"), false
	}
	return UnboundSort{Expr: UnboundField(qualifier, field), Direction: direction}, Diagnostic{}, true
}

func parseSimpleOrderByAggregateExpression(text string) (UnboundExpr, bool) {
	return parseSimpleAggregateCallExpression(text)
}

func parseSimpleGroupByClause(text string) (string, []UnboundExpr, bool, Diagnostic, bool) {
	left, right, ok := splitBeforeKeyword(text, "group")
	if !ok {
		return text, nil, false, Diagnostic{}, true
	}
	groupText, ok := consumeKeyword(right, "by")
	if !ok {
		return "", nil, false, simpleParserDiagnostic("GROUP must be followed by BY"), false
	}
	fields := splitSimpleCommaList(groupText)
	if len(fields) == 0 {
		return "", nil, false, simpleParserDiagnostic("GROUP BY must contain at least one field"), false
	}
	expressions := make([]UnboundExpr, 0, len(fields))
	for _, field := range fields {
		expr, ok := parseSimpleScalarExpression(field)
		if !ok {
			return "", nil, false, simpleParserDiagnostic("GROUP BY field is invalid"), false
		}
		expressions = append(expressions, expr)
	}
	return left, expressions, true, Diagnostic{}, true
}

func parseSimpleHavingClause(text string, projections []UnboundProjection, aggregates []UnboundAggregate) (string, []UnboundPredicate, []UnboundAggregate, bool, Diagnostic, bool) {
	left, right, ok := splitBeforeKeyword(text, "having")
	if !ok {
		return text, nil, aggregates, false, Diagnostic{}, true
	}
	if hasAnyTopLevelKeyword(right, "where", "join", "group", "having", "order", "limit", "and", "or") {
		return "", nil, aggregates, false, simpleParserDiagnostic("HAVING supports one aggregate alias comparison literal"), false
	}
	op, aliasText, literalText, ok := splitBeforeComparisonOperator(right)
	if !ok {
		return "", nil, aggregates, false, simpleParserDiagnostic("HAVING must compare aggregate alias to literal"), false
	}
	refExpr, updatedAggregates, diagnostic, ok := resolveSimpleHavingAggregateRef(aliasText, projections, aggregates)
	var leftExpr UnboundExpr = refExpr
	if !ok {
		scalarExpr, scalarOK := parseSimpleScalarExpression(aliasText)
		if !scalarOK {
			return "", nil, aggregates, false, diagnostic, false
		}
		leftExpr = scalarExpr
	} else {
		aggregates = updatedAggregates
	}
	literal, diagnostic, ok := parseSimpleComparisonValueWithScope(strings.TrimSpace(literalText), nil, PredicateScopeHaving)
	if !ok {
		return "", nil, aggregates, false, diagnostic, false
	}
	return left, []UnboundPredicate{{
		Expr:      UnboundBinary(op, leftExpr, literal),
		Placement: PredicateResidualScan,
		Scope:     PredicateScopeHaving,
	}}, aggregates, true, Diagnostic{}, true
}

func parseSimpleComparisonValueWithScope(text string, parameterIndex *int, scope PredicateScope) (UnboundExpr, Diagnostic, bool) {
	if subquery, ok := parseSimpleScalarSubqueryExpression(text, scope); ok {
		return subquery, Diagnostic{}, true
	}
	return parseSimpleComparisonValue(text, parameterIndex)
}

func resolveSimpleHavingAggregateRef(text string, projections []UnboundProjection, aggregates []UnboundAggregate) (UnboundAggregateRefExpr, []UnboundAggregate, Diagnostic, bool) {
	if ref, ok := resolveSimpleAggregateAlias(text, projections); ok {
		return ref, aggregates, Diagnostic{}, true
	}
	if expr, ok := parseSimpleOrderByAggregateExpression(strings.TrimSpace(text)); ok {
		call, callOK := expr.(UnboundCallExpr)
		if !callOK {
			return UnboundAggregateRefExpr{}, aggregates, simpleParserDiagnostic("HAVING aggregate call is invalid"), false
		}
		if ref, ok := findSimpleAggregateCallRef(call, aggregates); ok {
			return ref, aggregates, Diagnostic{}, true
		}
		aggregate, ref, diagnostic, ok := simpleHiddenAggregateFromCall(call, len(aggregates), "__having_agg_")
		if !ok {
			return UnboundAggregateRefExpr{}, aggregates, diagnostic, false
		}
		return ref, append(aggregates, aggregate), Diagnostic{}, true
	}
	return UnboundAggregateRefExpr{}, aggregates, simpleParserDiagnostic("HAVING must reference an aggregate alias or matching aggregate call"), false
}

func resolveSimpleAggregateAlias(aliasText string, projections []UnboundProjection) (UnboundAggregateRefExpr, bool) {
	aliasText = strings.TrimSpace(aliasText)
	if aliasText == "" || strings.Contains(aliasText, ".") {
		return UnboundAggregateRefExpr{}, false
	}
	for _, projection := range projections {
		if projection.Alias == "" || !strings.EqualFold(projection.Alias, aliasText) {
			continue
		}
		ref, ok := projection.Expr.(UnboundAggregateRefExpr)
		return ref, ok
	}
	return UnboundAggregateRefExpr{}, false
}

func simpleHiddenAggregateFromCall(call UnboundCallExpr, aggregateIndex int, aliasPrefix string) (UnboundAggregate, UnboundAggregateRefExpr, Diagnostic, bool) {
	if len(call.Args) != 1 || !simpleAggregateFunctionName(call.Name) {
		return UnboundAggregate{}, UnboundAggregateRefExpr{}, simpleParserDiagnostic("HAVING aggregate call is invalid"), false
	}
	alias := aliasPrefix + strconv.Itoa(aggregateIndex)
	aggregate := UnboundAggregate{
		Function: call.Name,
		Alias:    alias,
		Type:     simpleAggregateReturnType(call.Name),
	}
	if strings.EqualFold(call.Name, "count") {
		aggregate.Type = DataTypeInt
		if _, ok := call.Args[0].(simpleUnboundWildcardExpr); ok {
			aggregate.CountAll = true
		} else {
			aggregate.Input = call.Args[0]
		}
	} else {
		aggregate.Input = call.Args[0]
	}
	return aggregate, UnboundAggregateRef(alias, aggregateIndex), Diagnostic{}, true
}

func simpleUnboundExprEqual(left UnboundExpr, right UnboundExpr) bool {
	switch leftTyped := left.(type) {
	case UnboundFieldExpr:
		rightTyped, ok := right.(UnboundFieldExpr)
		return ok && strings.EqualFold(leftTyped.Qualifier, rightTyped.Qualifier) && strings.EqualFold(leftTyped.Name, rightTyped.Name)
	case UnboundLiteralExpr:
		rightTyped, ok := right.(UnboundLiteralExpr)
		return ok && leftTyped.Kind == rightTyped.Kind && leftTyped.Value == rightTyped.Value
	case UnboundListExpr:
		rightTyped, ok := right.(UnboundListExpr)
		if !ok || len(leftTyped.Items) != len(rightTyped.Items) {
			return false
		}
		for index := range leftTyped.Items {
			if !simpleUnboundExprEqual(leftTyped.Items[index], rightTyped.Items[index]) {
				return false
			}
		}
		return true
	case UnboundCallExpr:
		rightTyped, ok := right.(UnboundCallExpr)
		if !ok || !strings.EqualFold(leftTyped.Name, rightTyped.Name) || len(leftTyped.Args) != len(rightTyped.Args) {
			return false
		}
		for index := range leftTyped.Args {
			if !simpleUnboundExprEqual(leftTyped.Args[index], rightTyped.Args[index]) {
				return false
			}
		}
		return true
	case UnboundBinaryExpr:
		rightTyped, ok := right.(UnboundBinaryExpr)
		return ok && leftTyped.Op == rightTyped.Op && simpleUnboundExprEqual(leftTyped.Left, rightTyped.Left) && simpleUnboundExprEqual(leftTyped.Right, rightTyped.Right)
	case UnboundSearchedCaseExpr:
		rightTyped, ok := right.(UnboundSearchedCaseExpr)
		if !ok || len(leftTyped.Whens) != len(rightTyped.Whens) {
			return false
		}
		for index := range leftTyped.Whens {
			if !simpleUnboundExprEqual(leftTyped.Whens[index].Condition, rightTyped.Whens[index].Condition) ||
				!simpleUnboundExprEqual(leftTyped.Whens[index].Result, rightTyped.Whens[index].Result) {
				return false
			}
		}
		if leftTyped.Else == nil || rightTyped.Else == nil {
			return leftTyped.Else == nil && rightTyped.Else == nil
		}
		return simpleUnboundExprEqual(leftTyped.Else, rightTyped.Else)
	default:
		return false
	}
}

func parseSimpleOptionalLimitOnlyClause(text string, statement string) (int, int, bool, Diagnostic, bool) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return 0, 0, false, Diagnostic{}, true
	}
	body, ok := consumeKeyword(trimmed, "limit")
	if !ok {
		return 0, 0, false, simpleParserDiagnostic(statement + " only supports optional LIMIT syntax"), false
	}
	commaParts := splitSimpleCommaList(body)
	if len(commaParts) == 2 {
		offset, err := strconv.Atoi(strings.TrimSpace(commaParts[0]))
		if err != nil || offset < 0 {
			return 0, 0, false, simpleParserDiagnostic("LIMIT offset must be a non-negative integer"), false
		}
		limit, err := strconv.Atoi(strings.TrimSpace(commaParts[1]))
		if err != nil || limit < 0 {
			return 0, 0, false, simpleParserDiagnostic("LIMIT must be a non-negative integer"), false
		}
		return limit, offset, true, Diagnostic{}, true
	}
	if len(commaParts) > 2 {
		return 0, 0, false, simpleParserDiagnostic("LIMIT must contain one integer and optional OFFSET integer"), false
	}
	fields := strings.Fields(body)
	if len(fields) != 1 && len(fields) != 3 {
		return 0, 0, false, simpleParserDiagnostic("LIMIT must contain one integer and optional OFFSET integer"), false
	}
	limit, err := strconv.Atoi(fields[0])
	if err != nil || limit < 0 {
		return 0, 0, false, simpleParserDiagnostic("LIMIT must be a non-negative integer"), false
	}
	offset := 0
	if len(fields) == 3 {
		if !strings.EqualFold(fields[1], "offset") {
			return 0, 0, false, simpleParserDiagnostic("LIMIT offset syntax must be LIMIT n OFFSET m"), false
		}
		offset, err = strconv.Atoi(fields[2])
		if err != nil || offset < 0 {
			return 0, 0, false, simpleParserDiagnostic("OFFSET must be a non-negative integer"), false
		}
	}
	return limit, offset, true, Diagnostic{}, true
}

func parseSimpleLimitClause(text string) (string, int, int, bool, Diagnostic, bool) {
	left, right, ok := splitBeforeKeyword(text, "limit")
	if !ok {
		return text, 0, 0, false, Diagnostic{}, true
	}
	commaParts := splitSimpleCommaList(right)
	if len(commaParts) == 2 {
		offset, err := strconv.Atoi(strings.TrimSpace(commaParts[0]))
		if err != nil || offset < 0 {
			return "", 0, 0, false, simpleParserDiagnostic("LIMIT offset must be a non-negative integer"), false
		}
		limit, err := strconv.Atoi(strings.TrimSpace(commaParts[1]))
		if err != nil || limit < 0 {
			return "", 0, 0, false, simpleParserDiagnostic("LIMIT must be a non-negative integer"), false
		}
		return left, limit, offset, true, Diagnostic{}, true
	}
	if len(commaParts) > 2 {
		return "", 0, 0, false, simpleParserDiagnostic("LIMIT must contain one integer and optional OFFSET integer"), false
	}
	fields := strings.Fields(right)
	if len(fields) != 1 && len(fields) != 3 {
		return "", 0, 0, false, simpleParserDiagnostic("LIMIT must contain one integer and optional OFFSET integer"), false
	}
	limit, err := strconv.Atoi(fields[0])
	if err != nil || limit < 0 {
		return "", 0, 0, false, simpleParserDiagnostic("LIMIT must be a non-negative integer"), false
	}
	offset := 0
	if len(fields) == 3 {
		if !strings.EqualFold(fields[1], "offset") {
			return "", 0, 0, false, simpleParserDiagnostic("LIMIT offset syntax must be LIMIT n OFFSET m"), false
		}
		offset, err = strconv.Atoi(fields[2])
		if err != nil || offset < 0 {
			return "", 0, 0, false, simpleParserDiagnostic("OFFSET must be a non-negative integer"), false
		}
	}
	return left, limit, offset, true, Diagnostic{}, true
}

func consumeKeyword(text string, keyword string) (string, bool) {
	trimmed := strings.TrimSpace(text)
	lowered := strings.ToLower(trimmed)
	loweredKeyword := strings.ToLower(keyword)
	if lowered == loweredKeyword {
		return "", true
	}
	if strings.HasPrefix(lowered, loweredKeyword) && isSimpleKeywordBoundary(trimmed, len(keyword)) {
		return strings.TrimSpace(trimmed[len(keyword):]), true
	}
	return "", false
}

func consumeTrailingKeyword(text string, keyword string) (string, bool) {
	trimmed := strings.TrimSpace(text)
	if len(trimmed) < len(keyword) {
		return "", false
	}
	start := len(trimmed) - len(keyword)
	if !strings.EqualFold(trimmed[start:], keyword) {
		return "", false
	}
	if start > 0 && !isSimpleSQLWhitespace(trimmed[start-1]) {
		return "", false
	}
	return strings.TrimSpace(trimmed[:start]), true
}

func isSimpleSQLWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func splitBeforeKeyword(text string, keyword string) (string, string, bool) {
	index, end, ok := findSimpleKeyword(text, keyword)
	if !ok {
		return "", "", false
	}
	left := strings.TrimSpace(text[:index])
	right := strings.TrimSpace(text[end:])
	return left, right, left != "" && right != ""
}

func splitBeforeTopLevelKeyword(text string, keyword string) (string, string, bool) {
	index, end, ok := findTopLevelSimpleKeyword(text, keyword)
	if !ok {
		return "", "", false
	}
	left := strings.TrimSpace(text[:index])
	right := strings.TrimSpace(text[end:])
	return left, right, left != "" && right != ""
}

func splitBeforeFirstSimpleKeyword(text string, keywords ...string) (string, string, string, bool) {
	bestIndex := -1
	bestEnd := -1
	bestKeyword := ""
	for _, keyword := range keywords {
		index, end, ok := findSimpleKeyword(text, keyword)
		if !ok {
			continue
		}
		if bestIndex == -1 || index < bestIndex {
			bestIndex = index
			bestEnd = end
			bestKeyword = strings.ToLower(keyword)
		}
	}
	if bestIndex == -1 {
		return "", "", "", false
	}
	left := strings.TrimSpace(text[:bestIndex])
	right := strings.TrimSpace(text[bestEnd:])
	return left, bestKeyword, right, left != ""
}

func findSimpleKeyword(text string, keyword string) (int, int, bool) {
	lowered := strings.ToLower(text)
	loweredKeyword := strings.ToLower(keyword)
	inString := false
	for i := 0; i < len(lowered); i++ {
		if text[i] == '\'' {
			if inString && i+1 < len(text) && text[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		end := i + len(loweredKeyword)
		if end <= len(lowered) &&
			strings.HasPrefix(lowered[i:], loweredKeyword) &&
			isSimpleKeywordBoundary(text, i-1) &&
			isSimpleKeywordBoundary(text, end) {
			return i, end, true
		}
	}
	return 0, 0, false
}

func isSimpleKeywordBoundary(text string, index int) bool {
	if index < 0 || index >= len(text) {
		return true
	}
	ch := text[index]
	return !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_')
}

func splitOptionalKeyword(text string, keyword string) (string, string, bool) {
	left, right, ok := splitBeforeKeyword(text, keyword)
	if ok {
		return left, right, true
	}
	return text, "", false
}

func splitBeforeComparisonOperator(text string) (BinaryOp, string, string, bool) {
	for _, operator := range []struct {
		text string
		op   BinaryOp
	}{
		{text: "!=", op: BinaryOpNotEqual},
		{text: "<>", op: BinaryOpNotEqual},
		{text: ">=", op: BinaryOpGreaterEqual},
		{text: "<=", op: BinaryOpLessEqual},
		{text: "==", op: BinaryOpEqual},
		{text: ">", op: BinaryOpGreater},
		{text: "<", op: BinaryOpLess},
		{text: "=", op: BinaryOpEqual},
	} {
		left, right, ok := splitBeforeOperator(text, operator.text)
		if ok {
			return operator.op, left, right, true
		}
	}
	return "", "", "", false
}

func splitBeforeOperator(text string, operator string) (string, string, bool) {
	index := topLevelSimpleOperatorIndex(text, operator)
	if index < 0 {
		return "", "", false
	}
	left := strings.TrimSpace(text[:index])
	right := strings.TrimSpace(text[index+len(operator):])
	return left, right, left != "" && right != ""
}

func topLevelSimpleOperatorIndex(text string, operator string) int {
	depth := 0
	inString := false
	for i := 0; i <= len(text)-len(operator); i++ {
		if text[i] == '\'' {
			if inString && i+1 < len(text) && text[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch text[i] {
		case '(':
			depth++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 && strings.HasPrefix(text[i:], operator) {
			return i
		}
	}
	return -1
}

func hasAnyKeyword(text string, keywords ...string) bool {
	for _, field := range strings.Fields(text) {
		for _, keyword := range keywords {
			if strings.EqualFold(field, keyword) {
				return true
			}
		}
	}
	return false
}

func hasAnyTopLevelKeyword(text string, keywords ...string) bool {
	for _, keyword := range keywords {
		if _, _, ok := findTopLevelSimpleKeyword(text, keyword); ok {
			return true
		}
	}
	return false
}

func findTopLevelSimpleKeyword(text string, keyword string) (int, int, bool) {
	lowered := strings.ToLower(text)
	loweredKeyword := strings.ToLower(keyword)
	depth := 0
	inString := false
	for i := 0; i < len(lowered); i++ {
		if text[i] == '\'' {
			if inString && i+1 < len(text) && text[i+1] == '\'' {
				i++
				continue
			}
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch text[i] {
		case '(':
			depth++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			continue
		}
		end := i + len(loweredKeyword)
		if depth == 0 &&
			end <= len(lowered) &&
			strings.HasPrefix(lowered[i:], loweredKeyword) &&
			isSimpleKeywordBoundary(text, i-1) &&
			isSimpleKeywordBoundary(text, end) {
			return i, end, true
		}
	}
	return 0, 0, false
}

func splitQualifiedName(name string) (string, string) {
	parts := strings.Split(name, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", name
}

func splitProjectionField(name string) (string, string) {
	parts := strings.Split(name, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", name
}

func simpleParserDiagnostic(message string) Diagnostic {
	return ErrorDiagnostic(DiagnosticParserBoundary, PhaseParse, "simple parser supports SELECT field[, field...] or * FROM table [WHERE field comparison literal|field BETWEEN lower AND upper|field IN (literal[, ...]) [AND ...]] [ORDER BY field ASC|DESC] [LIMIT n [OFFSET m]], INSERT ... VALUES, UPDATE, DELETE, TRUNCATE, and COMMIT: "+message)
}

func mixedBooleanPredicateDiagnostic(message string) Diagnostic {
	return ErrorDiagnostic(DiagnosticMixedBooleanPredicate, PhaseParse, message)
}

func mixedBooleanPredicateBlocker(reason string) NativeBlocker {
	return NativeBlocker{
		Code:   DiagnosticMixedBooleanPredicate,
		Reason: reason,
		Phase:  PhaseParse,
	}
}

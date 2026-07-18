package qsbridge

import (
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
		return parseSimpleCreateTable(sql)
	}
	if _, ok := consumeKeyword(trimmed, "drop"); ok {
		return parseSimpleDropTable(sql)
	}
	if _, ok := consumeKeyword(trimmed, "commit"); ok {
		return parseSimpleCommit(sql)
	}
	return UnboundStatement{}, simpleParserDiagnostic("only SELECT, INSERT, UPDATE, DELETE, TRUNCATE, CREATE TABLE, DROP TABLE, and COMMIT statements are supported"), false
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
	projectionText, sourceText, ok := splitBeforeKeyword(selectBody, "from")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("SELECT must include FROM"), false
	}
	sourceText, limit, offset, diagnostic, ok := parseSimpleLimitClause(sourceText)
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
	sourceText, having, hasHaving, diagnostic, ok := parseSimpleHavingClause(sourceText, projections, aggregates)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	sourceText, groupBy, hasGroupBy, diagnostic, ok := parseSimpleGroupByClause(sourceText)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	sourceOnlyText, whereText, hasWhere := splitOptionalKeyword(sourceText, "where")
	tables, joins, diagnostic, ok := parseSimpleSources(sourceOnlyText)
	if !ok {
		return UnboundStatement{}, diagnostic, false
	}
	if hasAnyKeyword(sourceOnlyText, "having") {
		return UnboundStatement{}, simpleParserDiagnostic("unexpected HAVING in table source"), false
	}
	if hasWhere && hasAnyKeyword(whereText, "join", "group", "having", "order", "limit") {
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
	whereExpr := UnboundExpr(nil)
	blockers := []NativeBlocker(nil)
	if hasWhere {
		predicates, memberships, whereExpr, blockers, diagnostic, ok = parseSimpleWhere(whereText)
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
			GroupBy:     groupBy,
			Having:      having,
			OrderBy:     orderBy,
			Result:      ResultShape{Kind: ResultQuery, Limit: limit, Offset: offset, Distinct: distinct},
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

func parseSimpleCreateTable(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	createBody, ok := consumeKeyword(trimmed, "create")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only CREATE statements are supported"), false
	}
	createBody, ok = consumeKeyword(createBody, "table")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("CREATE must include TABLE"), false
	}
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

func parseSimpleDropTable(sql string) (UnboundStatement, Diagnostic, bool) {
	trimmed := strings.TrimSpace(strings.TrimSuffix(sql, ";"))
	dropBody, ok := consumeKeyword(trimmed, "drop")
	if !ok {
		return UnboundStatement{}, simpleParserDiagnostic("only DROP statements are supported"), false
	}
	if remaining, ok := consumeKeyword(dropBody, "table"); ok {
		dropBody = remaining
	}
	if strings.TrimSpace(dropBody) == "" {
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
			Table:  table,
			Result: ResultShape{Kind: ResultStatement},
		},
	}, Diagnostic{}, true
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
	return UnboundStatement{
		SQL:  sql,
		Kind: QueryKindSession,
		Session: UnboundSession{
			Actions: []SessionAction{CommitTransactionAction()},
			Result:  ResultShape{Kind: ResultStatement},
		},
	}, Diagnostic{}, true
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
	inString := false
	depth := 0
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
	for index, aggregate := range aggregates {
		if !strings.EqualFold(aggregate.Function, call.Name) {
			continue
		}
		if strings.EqualFold(aggregate.Function, "count") && aggregate.CountAll {
			if _, ok := call.Args[0].(simpleUnboundWildcardExpr); ok {
				return UnboundAggregateRef(aggregate.Alias, index), Diagnostic{}, true
			}
			continue
		}
		if simpleUnboundExprEqual(aggregate.Input, call.Args[0]) {
			return UnboundAggregateRef(aggregate.Alias, index), Diagnostic{}, true
		}
	}
	return UnboundAggregateRefExpr{}, simpleParserDiagnostic(clause + " aggregate call must match a SELECT aggregate"), false
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

func parseSimpleWhere(text string) ([]UnboundPredicate, []UnboundMembership, UnboundExpr, []NativeBlocker, Diagnostic, bool) {
	if simpleWhereHasMixedBooleanPredicates(text) {
		if predicates, diagnostic, ok := parseSimpleMixedBooleanWherePredicates(text); ok || diagnostic.Code != "" {
			if !ok {
				return nil, nil, nil, nil, diagnostic, false
			}
			return predicates, nil, nil, nil, Diagnostic{}, true
		}
		whereExpr, diagnostic, ok := parseSimpleBooleanExpression(text)
		if !ok {
			return nil, nil, nil, nil, diagnostic, false
		}
		return nil, nil, whereExpr, []NativeBlocker{mixedBooleanPredicateBlocker("mixed AND/OR predicates require grouped boolean expression lowering")}, Diagnostic{}, true
	}
	if len(splitSimpleOrPredicates(text)) > 1 {
		predicates, diagnostic, ok := parseSimplePredicates(text)
		return predicates, nil, nil, nil, diagnostic, ok
	}
	parts := splitSimpleAndPredicates(text)
	predicates := make([]UnboundPredicate, 0, len(parts))
	memberships := make([]UnboundMembership, 0)
	parameterIndex := 1
	for _, part := range parts {
		existsMembership, diagnostic, ok := parseSimpleExistsMembership(part)
		if diagnostic.Code != "" {
			return nil, nil, nil, nil, diagnostic, false
		}
		if ok {
			memberships = append(memberships, existsMembership)
			continue
		}
		membership, diagnostic, ok := parseSimpleSubqueryMembership(part)
		if diagnostic.Code != "" {
			return nil, nil, nil, nil, diagnostic, false
		}
		if ok {
			memberships = append(memberships, membership)
			continue
		}
		parsed, diagnostic, ok := parseSimplePredicate(part, &parameterIndex)
		if !ok {
			return nil, nil, nil, nil, diagnostic, false
		}
		predicates = append(predicates, parsed...)
	}
	if len(predicates) == 0 && len(memberships) == 0 {
		return nil, nil, nil, nil, simpleParserDiagnostic("WHERE predicate is empty"), false
	}
	return predicates, memberships, nil, nil, Diagnostic{}, true
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
	predicates := []UnboundPredicate(nil)
	if hasWhere {
		var predicateDiagnostic Diagnostic
		predicates, predicateDiagnostic, ok = parseSimplePredicates(predicateText)
		if !ok {
			return UnboundMembership{}, predicateDiagnostic, false
		}
	}
	rightQualifier, rightField := splitProjectionField(strings.TrimSpace(projectionText))
	if rightField == "" {
		return UnboundMembership{}, simpleParserDiagnostic("membership subquery SELECT field is empty"), false
	}
	if rightQualifier == "" {
		rightQualifier = tableRefName(table.Name, table.Alias)
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
		return UnboundMembership{}, simpleParserDiagnostic("EXISTS subquery requires a correlated equality predicate"), false
	}
	if hasAnyKeyword(sourceOnlyText, "join", "group", "having", "order", "limit") {
		return UnboundMembership{}, simpleParserDiagnostic("EXISTS subquery only supports a single table source"), false
	}
	if hasAnyKeyword(predicateText, "where", "join", "group", "having", "order", "limit") {
		return UnboundMembership{}, simpleParserDiagnostic("EXISTS subquery only supports a simple correlated equality predicate"), false
	}
	table, diagnostic, ok := parseSimpleTable(sourceOnlyText)
	if !ok {
		return UnboundMembership{}, diagnostic, false
	}
	op, leftText, rightText, ok := splitBeforeComparisonOperator(predicateText)
	if !ok || op != BinaryOpEqual {
		return UnboundMembership{}, simpleParserDiagnostic("EXISTS subquery requires a correlated equality predicate"), false
	}
	leftQualifier, leftField := splitProjectionField(strings.TrimSpace(leftText))
	rightQualifier, rightField := splitProjectionField(strings.TrimSpace(rightText))
	tableRef := tableRefName(table.Name, table.Alias)
	if simpleQualifierMatchesTable(leftQualifier, tableRef) && !simpleQualifierMatchesTable(rightQualifier, tableRef) {
		return UnboundMembership{
			LeftQualifier:  rightQualifier,
			LeftField:      rightField,
			RightTable:     table,
			RightQualifier: leftQualifier,
			RightField:     leftField,
			Kind:           kind,
		}, Diagnostic{}, true
	}
	if simpleQualifierMatchesTable(rightQualifier, tableRef) && !simpleQualifierMatchesTable(leftQualifier, tableRef) {
		return UnboundMembership{
			LeftQualifier:  leftQualifier,
			LeftField:      leftField,
			RightTable:     table,
			RightQualifier: rightQualifier,
			RightField:     rightField,
			Kind:           kind,
		}, Diagnostic{}, true
	}
	return UnboundMembership{}, simpleParserDiagnostic("EXISTS subquery correlation must compare child and outer fields"), false
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
	case UnboundLiteralExpr, UnboundParameterExpr, UnboundCallExpr:
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

func parseSimpleHavingClause(text string, projections []UnboundProjection, aggregates []UnboundAggregate) (string, []UnboundPredicate, bool, Diagnostic, bool) {
	left, right, ok := splitBeforeKeyword(text, "having")
	if !ok {
		return text, nil, false, Diagnostic{}, true
	}
	if hasAnyKeyword(right, "where", "join", "group", "having", "order", "limit", "and", "or") {
		return "", nil, false, simpleParserDiagnostic("HAVING supports one aggregate alias comparison literal"), false
	}
	op, aliasText, literalText, ok := splitBeforeComparisonOperator(right)
	if !ok {
		return "", nil, false, simpleParserDiagnostic("HAVING must compare aggregate alias to literal"), false
	}
	refExpr, diagnostic, ok := resolveSimpleHavingAggregateRef(aliasText, projections, aggregates)
	var leftExpr UnboundExpr = refExpr
	if !ok {
		scalarExpr, scalarOK := parseSimpleScalarExpression(aliasText)
		if !scalarOK {
			return "", nil, false, diagnostic, false
		}
		leftExpr = scalarExpr
	}
	literal, diagnostic, ok := parseSimpleLiteral(strings.TrimSpace(literalText))
	if !ok {
		return "", nil, false, diagnostic, false
	}
	return left, []UnboundPredicate{{
		Expr:      UnboundBinary(op, leftExpr, literal),
		Placement: PredicateResidualScan,
		Scope:     PredicateScopeHaving,
	}}, true, Diagnostic{}, true
}

func resolveSimpleHavingAggregateRef(text string, projections []UnboundProjection, aggregates []UnboundAggregate) (UnboundAggregateRefExpr, Diagnostic, bool) {
	if ref, ok := resolveSimpleAggregateAlias(text, projections); ok {
		return ref, Diagnostic{}, true
	}
	if expr, ok := parseSimpleOrderByAggregateExpression(strings.TrimSpace(text)); ok {
		return resolveSimpleAggregateCall(expr, aggregates, "HAVING")
	}
	return UnboundAggregateRefExpr{}, simpleParserDiagnostic("HAVING must reference an aggregate alias or matching aggregate call"), false
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

func parseSimpleLimitClause(text string) (string, int, int, Diagnostic, bool) {
	left, right, ok := splitBeforeKeyword(text, "limit")
	if !ok {
		return text, 0, 0, Diagnostic{}, true
	}
	fields := strings.Fields(right)
	if len(fields) != 1 && len(fields) != 3 {
		return "", 0, 0, simpleParserDiagnostic("LIMIT must contain one integer and optional OFFSET integer"), false
	}
	limit, err := strconv.Atoi(fields[0])
	if err != nil || limit < 0 {
		return "", 0, 0, simpleParserDiagnostic("LIMIT must be a non-negative integer"), false
	}
	offset := 0
	if len(fields) == 3 {
		if !strings.EqualFold(fields[1], "offset") {
			return "", 0, 0, simpleParserDiagnostic("LIMIT offset syntax must be LIMIT n OFFSET m"), false
		}
		offset, err = strconv.Atoi(fields[2])
		if err != nil || offset < 0 {
			return "", 0, 0, simpleParserDiagnostic("OFFSET must be a non-negative integer"), false
		}
	}
	return left, limit, offset, Diagnostic{}, true
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
	index := strings.Index(text, operator)
	if index < 0 {
		return "", "", false
	}
	left := strings.TrimSpace(text[:index])
	right := strings.TrimSpace(text[index+len(operator):])
	return left, right, left != "" && right != ""
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

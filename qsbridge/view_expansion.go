package qsbridge

import "strings"

// ExpandStatementViews rewrites supported logical view references into their base SELECT shape.
func ExpandStatementViews(catalog Catalog, parser ParserBridge, defaultSchema string, statement UnboundStatement) (UnboundStatement, DiagnosticSet) {
	if statement.Kind != QueryKindSelect {
		return statement, nil
	}
	state := viewExpansionState{
		catalog:       catalog,
		parser:        parser,
		defaultSchema: defaultSchema,
		stack:         map[string]struct{}{},
	}
	selectStmt, diagnostics := state.expandSelect(statement.Select)
	if diagnostics.BlocksNative() {
		return statement, diagnostics
	}
	statement.Select = selectStmt
	return statement, nil
}

type viewExpansionState struct {
	catalog       Catalog
	parser        ParserBridge
	defaultSchema string
	stack         map[string]struct{}
}

type viewExpansion struct {
	tables     []UnboundTable
	joins      []UnboundJoin
	predicates []UnboundPredicate
	columns    viewProjectionMap
	viewRef    string
}

type viewProjectionMap struct {
	wildcard bool
	exprs    map[string]UnboundExpr
}

func (s viewExpansionState) expandSelect(selectStmt UnboundSelect) (UnboundSelect, DiagnosticSet) {
	if len(selectStmt.Tables) == 0 {
		return selectStmt, nil
	}
	viewCount := 0
	for _, table := range selectStmt.Tables {
		if s.tableExists(table) {
			continue
		}
		if _, diagnostics, ok := s.lookupView(table); ok {
			if diagnostics.BlocksNative() {
				return selectStmt, diagnostics
			}
			viewCount++
		}
	}
	if viewCount == 0 {
		return selectStmt, nil
	}
	if len(selectStmt.Tables) != 1 || len(selectStmt.Joins) > 0 || len(selectStmt.Memberships) > 0 || len(selectStmt.Subqueries) > 0 {
		return selectStmt, DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "logical view expansion currently supports one view source without joins, memberships, or subqueries"),
		}
	}
	expansion, diagnostics, ok := s.expandTableView(selectStmt.Tables[0])
	if diagnostics.BlocksNative() || !ok {
		return selectStmt, diagnostics
	}
	selectStmt.Tables = expansion.tables
	selectStmt.Joins = expansion.joins
	var rewriteDiagnostics DiagnosticSet
	selectStmt, rewriteDiagnostics = rewriteOuterSelectViewReferences(selectStmt, expansion)
	if rewriteDiagnostics.BlocksNative() {
		return selectStmt, rewriteDiagnostics
	}
	selectStmt.Predicates = append(append([]UnboundPredicate(nil), expansion.predicates...), selectStmt.Predicates...)
	return selectStmt, nil
}

func (s viewExpansionState) expandTableView(table UnboundTable) (viewExpansion, DiagnosticSet, bool) {
	view, diagnostics, ok := s.lookupView(table)
	if diagnostics.BlocksNative() || !ok {
		return viewExpansion{}, diagnostics, ok
	}
	schema := strings.TrimSpace(view.Schema)
	if schema == "" {
		schema = s.schemaForTable(table)
	}
	viewName := strings.TrimSpace(view.Name)
	if viewName == "" {
		viewName = strings.TrimSpace(table.Name)
	}
	key := strings.ToLower(schema) + "." + strings.ToLower(viewName)
	if _, seen := s.stack[key]; seen {
		return viewExpansion{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "recursive logical view reference: "+qualifiedCatalogName(schema, viewName)),
		}, true
	}
	s.stack[key] = struct{}{}
	defer delete(s.stack, key)

	viewSQL := strings.TrimSpace(view.SQL)
	if viewSQL == "" {
		viewSQL = strings.TrimSpace(view.CanonicalSQL)
	}
	statement, parseDiagnostics := s.parser.Parse(viewSQL)
	if parseDiagnostics.BlocksNative() {
		return viewExpansion{}, parseDiagnostics, true
	}
	if statement.Kind != QueryKindSelect {
		return viewExpansion{}, DiagnosticSet{
			ErrorDiagnostic(DiagnosticParserBoundary, PhaseBind, "view definition must be a SELECT statement: "+qualifiedCatalogName(schema, viewName)),
		}, true
	}
	viewSelect, diagnostics := s.expandSelect(statement.Select)
	if diagnostics.BlocksNative() {
		return viewExpansion{}, diagnostics, true
	}
	if diagnostics := validateExpandableViewSelect(schema, viewName, viewSelect); diagnostics.BlocksNative() {
		return viewExpansion{}, diagnostics, true
	}
	columns, diagnostics := buildViewProjectionMap(viewSelect)
	if diagnostics.BlocksNative() {
		return viewExpansion{}, diagnostics, true
	}
	outerRef := tableRefName(table.Name, table.Alias)
	tables := append([]UnboundTable(nil), viewSelect.Tables...)
	joins := append([]UnboundJoin(nil), viewSelect.Joins...)
	predicates := append([]UnboundPredicate(nil), viewSelect.Predicates...)
	if len(tables) == 1 {
		base := tables[0]
		predicates, diagnostics = rewriteViewDefinitionPredicates(predicates, base, outerRef)
		if diagnostics.BlocksNative() {
			return viewExpansion{}, diagnostics, true
		}
		columns, diagnostics = rewriteViewProjectionMapSource(columns, base, outerRef)
		if diagnostics.BlocksNative() {
			return viewExpansion{}, diagnostics, true
		}
		tables[0].Alias = outerRef
		tables[0].Role = table.Role
	}
	return viewExpansion{
		tables:     tables,
		joins:      joins,
		predicates: predicates,
		columns:    columns,
		viewRef:    outerRef,
	}, nil, true
}

func (s viewExpansionState) tableExists(table UnboundTable) bool {
	if s.catalog == nil {
		return false
	}
	_, diagnostics := s.catalog.Table(s.schemaForTable(table), table.Name)
	return !diagnostics.BlocksNative()
}

func (s viewExpansionState) lookupView(table UnboundTable) (SQLViewDefinition, DiagnosticSet, bool) {
	viewCatalog, ok := s.catalog.(ViewCatalog)
	if !ok {
		return SQLViewDefinition{}, nil, false
	}
	view, diagnostics := viewCatalog.View(s.schemaForTable(table), table.Name)
	if diagnostics.BlocksNative() {
		if diagnosticsContainCode(diagnostics, DiagnosticCatalogViewNotFound) {
			return SQLViewDefinition{}, nil, false
		}
		return SQLViewDefinition{}, diagnostics, true
	}
	return view, nil, true
}

func (s viewExpansionState) schemaForTable(table UnboundTable) string {
	if strings.TrimSpace(table.Schema) != "" {
		return strings.TrimSpace(table.Schema)
	}
	return strings.TrimSpace(s.defaultSchema)
}

func validateExpandableViewSelect(schema string, viewName string, selectStmt UnboundSelect) DiagnosticSet {
	if len(selectStmt.Tables) == 0 || len(selectStmt.Memberships) > 0 || len(selectStmt.Subqueries) > 0 {
		return DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "view "+qualifiedCatalogName(schema, viewName)+" must expand to base tables without memberships or subqueries"),
		}
	}
	if len(selectStmt.Aggregates) > 0 || len(selectStmt.GroupBy) > 0 || len(selectStmt.Having) > 0 || len(selectStmt.OrderBy) > 0 {
		return DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "view "+qualifiedCatalogName(schema, viewName)+" cannot contain grouping, aggregates, having, or order by yet"),
		}
	}
	if selectStmt.Result.Distinct || selectStmt.Result.Limit > 0 || selectStmt.Result.Offset > 0 {
		return DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "view "+qualifiedCatalogName(schema, viewName)+" cannot contain distinct, limit, or offset yet"),
		}
	}
	if selectStmt.WhereExpr != nil {
		return DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "view "+qualifiedCatalogName(schema, viewName)+" cannot contain grouped boolean predicates yet"),
		}
	}
	return nil
}

func buildViewProjectionMap(selectStmt UnboundSelect) (viewProjectionMap, DiagnosticSet) {
	columns := viewProjectionMap{exprs: map[string]UnboundExpr{}}
	sourceRefs := viewSourceRefs(selectStmt.Tables)
	requireQualified := len(selectStmt.Tables) > 1
	for _, projection := range selectStmt.Projection {
		field, ok := projection.Expr.(UnboundFieldExpr)
		if ok {
			if field.Name == "*" {
				columns.wildcard = true
				continue
			}
			qualifier := strings.TrimSpace(field.Qualifier)
			if qualifier == "" && requireQualified {
				return columns, DiagnosticSet{
					ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "joined view projections must qualify fields: "+field.Name),
				}
			}
			if qualifier != "" {
				if _, ok := sourceRefs[strings.ToLower(qualifier)]; !ok {
					return columns, DiagnosticSet{
						ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "view projection qualifier does not match a base source: "+field.Qualifier),
					}
				}
				field.Qualifier = qualifier
			}
			if qualifier == "" && len(selectStmt.Tables) == 1 {
				field.Qualifier = tableRefName(selectStmt.Tables[0].Name, selectStmt.Tables[0].Alias)
			}
			exposed := strings.TrimSpace(projection.Alias)
			if exposed == "" {
				exposed = field.Name
			}
			columns.exprs[strings.ToLower(exposed)] = field
			continue
		}
		exposed := strings.TrimSpace(projection.Alias)
		if exposed == "" {
			return columns, DiagnosticSet{
				ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "view expression projections must have aliases"),
			}
		}
		expr, diagnostics := normalizeViewProjectionExpr(projection.Expr, selectStmt, sourceRefs, requireQualified)
		if diagnostics.BlocksNative() {
			return columns, diagnostics
		}
		columns.exprs[strings.ToLower(exposed)] = expr
	}
	return columns, nil
}

func normalizeViewProjectionExpr(expr UnboundExpr, selectStmt UnboundSelect, sourceRefs map[string]struct{}, requireQualified bool) (UnboundExpr, DiagnosticSet) {
	if len(selectStmt.Tables) == 1 {
		aliases := tableAliases(selectStmt.Tables[0])
		var diagnostics DiagnosticSet
		expr = rewriteBaseQualifierExpr(expr, aliases, tableRefName(selectStmt.Tables[0].Name, selectStmt.Tables[0].Alias), &diagnostics)
		return expr, diagnostics
	}
	var diagnostics DiagnosticSet
	expr = validateViewProjectionExprQualifiers(expr, sourceRefs, requireQualified, &diagnostics)
	return expr, diagnostics
}

func validateViewProjectionExprQualifiers(expr UnboundExpr, sourceRefs map[string]struct{}, requireQualified bool, diagnostics *DiagnosticSet) UnboundExpr {
	switch typed := expr.(type) {
	case nil:
		return nil
	case UnboundFieldExpr:
		qualifier := strings.TrimSpace(typed.Qualifier)
		if qualifier == "" {
			if requireQualified {
				*diagnostics = append(*diagnostics, ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "joined view expression projections must qualify fields: "+typed.Name))
			}
			return typed
		}
		if _, ok := sourceRefs[strings.ToLower(qualifier)]; !ok {
			*diagnostics = append(*diagnostics, ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "view expression projection qualifier does not match a base source: "+typed.Qualifier))
			return typed
		}
		typed.Qualifier = qualifier
		return typed
	case UnboundBinaryExpr:
		typed.Left = validateViewProjectionExprQualifiers(typed.Left, sourceRefs, requireQualified, diagnostics)
		typed.Right = validateViewProjectionExprQualifiers(typed.Right, sourceRefs, requireQualified, diagnostics)
		return typed
	case UnboundCallExpr:
		for i, arg := range typed.Args {
			typed.Args[i] = validateViewProjectionExprQualifiers(arg, sourceRefs, requireQualified, diagnostics)
		}
		return typed
	case UnboundListExpr:
		for i, item := range typed.Items {
			typed.Items[i] = validateViewProjectionExprQualifiers(item, sourceRefs, requireQualified, diagnostics)
		}
		return typed
	case UnboundSearchedCaseExpr:
		for i, when := range typed.Whens {
			typed.Whens[i].Condition = validateViewProjectionExprQualifiers(when.Condition, sourceRefs, requireQualified, diagnostics)
			typed.Whens[i].Result = validateViewProjectionExprQualifiers(when.Result, sourceRefs, requireQualified, diagnostics)
		}
		typed.Else = validateViewProjectionExprQualifiers(typed.Else, sourceRefs, requireQualified, diagnostics)
		return typed
	default:
		return expr
	}
}

func viewSourceRefs(tables []UnboundTable) map[string]struct{} {
	refs := map[string]struct{}{}
	for _, table := range tables {
		for _, ref := range []string{table.Name, table.Alias, tableRefName(table.Name, table.Alias)} {
			ref = strings.TrimSpace(ref)
			if ref != "" {
				refs[strings.ToLower(ref)] = struct{}{}
			}
		}
	}
	return refs
}

func rewriteViewProjectionMapSource(columns viewProjectionMap, base UnboundTable, replacementRef string) (viewProjectionMap, DiagnosticSet) {
	aliases := tableAliases(base)
	var diagnostics DiagnosticSet
	for key, expr := range columns.exprs {
		columns.exprs[key] = rewriteBaseQualifierExpr(expr, aliases, replacementRef, &diagnostics)
	}
	return columns, diagnostics
}

func tableAliases(table UnboundTable) map[string]struct{} {
	aliases := map[string]struct{}{}
	for _, alias := range []string{table.Name, table.Alias, tableRefName(table.Name, table.Alias)} {
		if strings.TrimSpace(alias) != "" {
			aliases[strings.ToLower(strings.TrimSpace(alias))] = struct{}{}
		}
	}
	return aliases
}

func expansionWildcardQualifier(expansion viewExpansion) string {
	if len(expansion.tables) != 1 {
		return ""
	}
	return tableRefName(expansion.tables[0].Name, expansion.tables[0].Alias)
}

func expansionMappedExpr(expansion viewExpansion, name string) (UnboundExpr, bool) {
	expr, ok := expansion.columns.exprs[strings.ToLower(name)]
	return expr, ok
}

func rewriteMappedViewExpr(expr UnboundExpr) UnboundExpr {
	return expr
}

func rewriteOuterSelectViewReferences(selectStmt UnboundSelect, expansion viewExpansion) (UnboundSelect, DiagnosticSet) {
	var diagnostics DiagnosticSet
	for i, projection := range selectStmt.Projection {
		if projection.Alias == "" {
			projection.Alias = projectionAliasForViewColumn(projection.Expr, expansion)
		}
		selectStmt.Projection[i].Expr = rewriteViewColumnExpr(projection.Expr, expansion, &diagnostics)
		selectStmt.Projection[i].Alias = projection.Alias
	}
	for i, predicate := range selectStmt.Predicates {
		selectStmt.Predicates[i].Expr = rewriteViewColumnExpr(predicate.Expr, expansion, &diagnostics)
	}
	selectStmt.WhereExpr = rewriteViewColumnExpr(selectStmt.WhereExpr, expansion, &diagnostics)
	for i, expr := range selectStmt.GroupBy {
		selectStmt.GroupBy[i] = rewriteViewColumnExpr(expr, expansion, &diagnostics)
	}
	for i, aggregate := range selectStmt.Aggregates {
		selectStmt.Aggregates[i].Input = rewriteViewColumnExpr(aggregate.Input, expansion, &diagnostics)
		selectStmt.Aggregates[i].Filter = rewriteViewColumnExpr(aggregate.Filter, expansion, &diagnostics)
	}
	for i, predicate := range selectStmt.Having {
		selectStmt.Having[i].Expr = rewriteViewColumnExpr(predicate.Expr, expansion, &diagnostics)
	}
	for i, sort := range selectStmt.OrderBy {
		selectStmt.OrderBy[i].Expr = rewriteViewColumnExpr(sort.Expr, expansion, &diagnostics)
	}
	return selectStmt, diagnostics
}

func projectionAliasForViewColumn(expr UnboundExpr, expansion viewExpansion) string {
	field, ok := expr.(UnboundFieldExpr)
	if !ok || field.Name == "*" || expansion.columns.wildcard {
		return ""
	}
	if field.Qualifier != "" && !strings.EqualFold(field.Qualifier, expansion.viewRef) {
		return ""
	}
	mapped, ok := expansionMappedExpr(expansion, field.Name)
	if !ok {
		return ""
	}
	mappedField, ok := mapped.(UnboundFieldExpr)
	if !ok || !strings.EqualFold(mappedField.Name, field.Name) {
		return field.Name
	}
	return ""
}

func rewriteViewColumnExpr(expr UnboundExpr, expansion viewExpansion, diagnostics *DiagnosticSet) UnboundExpr {
	switch typed := expr.(type) {
	case nil:
		return nil
	case UnboundFieldExpr:
		if typed.Qualifier != "" && !strings.EqualFold(typed.Qualifier, expansion.viewRef) {
			return typed
		}
		if typed.Name == "*" {
			typed.Qualifier = expansionWildcardQualifier(expansion)
			return typed
		}
		if expansion.columns.wildcard {
			typed.Qualifier = expansionWildcardQualifier(expansion)
			return typed
		}
		expr, ok := expansionMappedExpr(expansion, typed.Name)
		if !ok {
			*diagnostics = append(*diagnostics, ErrorDiagnostic(DiagnosticCatalogFieldNotFound, PhaseBind, "view column not found: "+expansion.viewRef+"."+typed.Name))
			return typed
		}
		return rewriteMappedViewExpr(expr)
	case UnboundBinaryExpr:
		typed.Left = rewriteViewColumnExpr(typed.Left, expansion, diagnostics)
		typed.Right = rewriteViewColumnExpr(typed.Right, expansion, diagnostics)
		return typed
	case UnboundCallExpr:
		for i, arg := range typed.Args {
			typed.Args[i] = rewriteViewColumnExpr(arg, expansion, diagnostics)
		}
		return typed
	case UnboundListExpr:
		for i, item := range typed.Items {
			typed.Items[i] = rewriteViewColumnExpr(item, expansion, diagnostics)
		}
		return typed
	case UnboundSearchedCaseExpr:
		for i, when := range typed.Whens {
			typed.Whens[i].Condition = rewriteViewColumnExpr(when.Condition, expansion, diagnostics)
			typed.Whens[i].Result = rewriteViewColumnExpr(when.Result, expansion, diagnostics)
		}
		typed.Else = rewriteViewColumnExpr(typed.Else, expansion, diagnostics)
		return typed
	default:
		return expr
	}
}

func rewriteViewDefinitionPredicates(predicates []UnboundPredicate, base UnboundTable, replacementRef string) ([]UnboundPredicate, DiagnosticSet) {
	aliases := tableAliases(base)
	rewritten := make([]UnboundPredicate, 0, len(predicates))
	var diagnostics DiagnosticSet
	for _, predicate := range predicates {
		predicate.Expr = rewriteBaseQualifierExpr(predicate.Expr, aliases, replacementRef, &diagnostics)
		rewritten = append(rewritten, predicate)
	}
	return rewritten, diagnostics
}

func rewriteBaseQualifierExpr(expr UnboundExpr, aliases map[string]struct{}, replacementRef string, diagnostics *DiagnosticSet) UnboundExpr {
	switch typed := expr.(type) {
	case nil:
		return nil
	case UnboundFieldExpr:
		qualifier := strings.TrimSpace(typed.Qualifier)
		if qualifier == "" {
			typed.Qualifier = replacementRef
			return typed
		}
		if _, ok := aliases[strings.ToLower(qualifier)]; ok {
			typed.Qualifier = replacementRef
			return typed
		}
		*diagnostics = append(*diagnostics, ErrorDiagnostic(DiagnosticTableAliasNotFound, PhaseBind, "view predicate qualifier does not match its base source: "+qualifier))
		return typed
	case UnboundBinaryExpr:
		typed.Left = rewriteBaseQualifierExpr(typed.Left, aliases, replacementRef, diagnostics)
		typed.Right = rewriteBaseQualifierExpr(typed.Right, aliases, replacementRef, diagnostics)
		return typed
	case UnboundCallExpr:
		for i, arg := range typed.Args {
			typed.Args[i] = rewriteBaseQualifierExpr(arg, aliases, replacementRef, diagnostics)
		}
		return typed
	case UnboundListExpr:
		for i, item := range typed.Items {
			typed.Items[i] = rewriteBaseQualifierExpr(item, aliases, replacementRef, diagnostics)
		}
		return typed
	case UnboundSearchedCaseExpr:
		for i, when := range typed.Whens {
			typed.Whens[i].Condition = rewriteBaseQualifierExpr(when.Condition, aliases, replacementRef, diagnostics)
			typed.Whens[i].Result = rewriteBaseQualifierExpr(when.Result, aliases, replacementRef, diagnostics)
		}
		typed.Else = rewriteBaseQualifierExpr(typed.Else, aliases, replacementRef, diagnostics)
		return typed
	default:
		return expr
	}
}

func diagnosticsContainCode(diagnostics DiagnosticSet, code DiagnosticCode) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return true
		}
	}
	return false
}

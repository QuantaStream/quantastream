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
	aggregates []UnboundAggregate
	columns    viewProjectionMap
	viewRef    string
	distinct   bool
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
	if viewCount > 1 || len(selectStmt.Memberships) > 0 || len(selectStmt.Subqueries) > 0 {
		return selectStmt, DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "logical view expansion currently supports one view source without memberships or subqueries"),
		}
	}
	var expansion viewExpansion
	var foundView bool
	tables := make([]UnboundTable, 0, len(selectStmt.Tables))
	joins := make([]UnboundJoin, 0, len(selectStmt.Joins))
	predicates := make([]UnboundPredicate, 0)
	for _, table := range selectStmt.Tables {
		if s.tableExists(table) {
			tables = append(tables, table)
			continue
		}
		if _, diagnostics, ok := s.lookupView(table); diagnostics.BlocksNative() || !ok {
			if diagnostics.BlocksNative() {
				return selectStmt, diagnostics
			}
			tables = append(tables, table)
			continue
		}
		nextExpansion, diagnostics, ok := s.expandTableView(table)
		if diagnostics.BlocksNative() || !ok {
			return selectStmt, diagnostics
		}
		expansion = nextExpansion
		foundView = true
		tables = append(tables, nextExpansion.tables...)
		joins = append(joins, nextExpansion.joins...)
		predicates = append(predicates, nextExpansion.predicates...)
	}
	if !foundView {
		return selectStmt, nil
	}
	selectStmt.Tables = tables
	selectStmt.Joins = append(joins, selectStmt.Joins...)
	if len(expansion.aggregates) > 0 {
		if diagnostics := validateAggregateViewUsage(selectStmt, expansion); diagnostics.BlocksNative() {
			return selectStmt, diagnostics
		}
		selectStmt.Aggregates = append(expansion.aggregates, selectStmt.Aggregates...)
	}
	if expansion.distinct {
		if diagnostics := validateDistinctViewUsage(selectStmt, expansion); diagnostics.BlocksNative() {
			return selectStmt, diagnostics
		}
		selectStmt.Result.Distinct = true
	}
	var rewriteDiagnostics DiagnosticSet
	selectStmt, rewriteDiagnostics = rewriteOuterSelectViewReferences(selectStmt, expansion)
	if rewriteDiagnostics.BlocksNative() {
		return selectStmt, rewriteDiagnostics
	}
	selectStmt.Predicates = append(predicates, selectStmt.Predicates...)
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
	aggregates := append([]UnboundAggregate(nil), viewSelect.Aggregates...)
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
		aggregates, diagnostics = rewriteViewDefinitionAggregates(aggregates, base, outerRef)
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
		aggregates: aggregates,
		columns:    columns,
		viewRef:    outerRef,
		distinct:   viewSelect.Result.Distinct,
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
	if len(selectStmt.GroupBy) > 0 || len(selectStmt.Having) > 0 || len(selectStmt.OrderBy) > 0 {
		return DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "view "+qualifiedCatalogName(schema, viewName)+" cannot contain grouping, having, or order by yet"),
		}
	}
	if selectStmt.Result.Limit > 0 || selectStmt.Result.Offset > 0 {
		return DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "view "+qualifiedCatalogName(schema, viewName)+" cannot contain limit or offset yet"),
		}
	}
	if selectStmt.WhereExpr != nil {
		return DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "view "+qualifiedCatalogName(schema, viewName)+" cannot contain grouped boolean predicates yet"),
		}
	}
	return nil
}

func validateDistinctViewUsage(selectStmt UnboundSelect, expansion viewExpansion) DiagnosticSet {
	if len(expansion.aggregates) > 0 || len(expansion.joins) > 0 || len(selectStmt.Tables) != len(expansion.tables) || len(selectStmt.Joins) > len(expansion.joins) || len(selectStmt.Predicates) > 0 || selectStmt.WhereExpr != nil || len(selectStmt.GroupBy) > 0 || len(selectStmt.Aggregates) > 0 || len(selectStmt.Having) > 0 {
		return DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "distinct logical views currently support direct projection only"),
		}
	}
	if expansion.columns.wildcard || len(selectStmt.Projection) != len(expansion.columns.exprs) {
		return DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "distinct logical views currently require projecting every view column explicitly"),
		}
	}
	for _, projection := range selectStmt.Projection {
		field, ok := projection.Expr.(UnboundFieldExpr)
		if !ok || field.Name == "*" {
			return DiagnosticSet{
				ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "distinct logical views currently require explicit projected columns"),
			}
		}
		if field.Qualifier != "" && !strings.EqualFold(field.Qualifier, expansion.viewRef) {
			return DiagnosticSet{
				ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "distinct logical views currently support direct projection only"),
			}
		}
		if _, ok := expansionMappedExpr(expansion, field.Name); !ok {
			return DiagnosticSet{
				ErrorDiagnostic(DiagnosticCatalogFieldNotFound, PhaseBind, "view column not found: "+expansion.viewRef+"."+field.Name),
			}
		}
	}
	for _, sort := range selectStmt.OrderBy {
		field, ok := sort.Expr.(UnboundFieldExpr)
		if !ok || field.Name == "*" {
			return DiagnosticSet{
				ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "distinct logical views currently require ordering by projected columns"),
			}
		}
		if field.Qualifier != "" && !strings.EqualFold(field.Qualifier, expansion.viewRef) {
			return DiagnosticSet{
				ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "distinct logical views currently support ordering by view columns only"),
			}
		}
		if _, ok := expansionMappedExpr(expansion, field.Name); !ok {
			return DiagnosticSet{
				ErrorDiagnostic(DiagnosticCatalogFieldNotFound, PhaseBind, "view column not found: "+expansion.viewRef+"."+field.Name),
			}
		}
	}
	return nil
}

func validateAggregateViewUsage(selectStmt UnboundSelect, expansion viewExpansion) DiagnosticSet {
	if len(selectStmt.Tables) != len(expansion.tables) || len(selectStmt.Joins) > len(expansion.joins) || len(selectStmt.Predicates) > 0 || selectStmt.WhereExpr != nil || len(selectStmt.GroupBy) > 0 || len(selectStmt.Aggregates) > 0 || len(selectStmt.Having) > 0 || len(selectStmt.OrderBy) > 0 {
		return DiagnosticSet{
			ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "aggregate logical views currently support direct projection only"),
		}
	}
	for _, projection := range selectStmt.Projection {
		field, ok := projection.Expr.(UnboundFieldExpr)
		if !ok || field.Name == "*" {
			return DiagnosticSet{
				ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "aggregate logical views currently require explicit aggregate columns"),
			}
		}
		if field.Qualifier != "" && !strings.EqualFold(field.Qualifier, expansion.viewRef) {
			return DiagnosticSet{
				ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "aggregate logical views currently support direct projection only"),
			}
		}
		mapped, ok := expansionMappedExpr(expansion, field.Name)
		if !ok {
			return DiagnosticSet{
				ErrorDiagnostic(DiagnosticCatalogFieldNotFound, PhaseBind, "view column not found: "+expansion.viewRef+"."+field.Name),
			}
		}
		if !viewProjectionExprUsesOnlyAggregates(mapped) {
			return DiagnosticSet{
				ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "aggregate logical views currently expose aggregate expressions only"),
			}
		}
	}
	return nil
}

func viewProjectionExprUsesOnlyAggregates(expr UnboundExpr) bool {
	switch typed := expr.(type) {
	case UnboundAggregateRefExpr:
		return true
	case UnboundBinaryExpr:
		return viewProjectionExprUsesOnlyAggregates(typed.Left) && viewProjectionExprUsesOnlyAggregates(typed.Right)
	case UnboundCallExpr:
		for _, arg := range typed.Args {
			if !viewProjectionExprUsesOnlyAggregates(arg) {
				return false
			}
		}
		return true
	case UnboundLiteralExpr:
		return true
	default:
		return false
	}
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
	for i, join := range selectStmt.Joins {
		selectStmt.Joins[i] = rewriteViewColumnJoin(join, expansion, &diagnostics)
	}
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

func rewriteViewColumnJoin(join UnboundJoin, expansion viewExpansion, diagnostics *DiagnosticSet) UnboundJoin {
	join.LeftQualifier, join.LeftField = rewriteViewColumnJoinField(join.LeftQualifier, join.LeftField, expansion, diagnostics)
	join.RightQualifier, join.RightField = rewriteViewColumnJoinField(join.RightQualifier, join.RightField, expansion, diagnostics)
	for i, predicate := range join.Predicates {
		join.Predicates[i].Expr = rewriteViewColumnExpr(predicate.Expr, expansion, diagnostics)
	}
	return join
}

func rewriteViewColumnJoinField(qualifier string, name string, expansion viewExpansion, diagnostics *DiagnosticSet) (string, string) {
	if qualifier != "" && !strings.EqualFold(qualifier, expansion.viewRef) {
		return qualifier, name
	}
	if qualifier == "" {
		return qualifier, name
	}
	expr, ok := expansionMappedExpr(expansion, name)
	if !ok {
		*diagnostics = append(*diagnostics, ErrorDiagnostic(DiagnosticCatalogFieldNotFound, PhaseBind, "view join column not found: "+expansion.viewRef+"."+name))
		return qualifier, name
	}
	field, ok := expr.(UnboundFieldExpr)
	if !ok {
		*diagnostics = append(*diagnostics, ErrorDiagnostic(DiagnosticUnsupportedSQL, PhaseBind, "view join column must expand to a base field: "+expansion.viewRef+"."+name))
		return qualifier, name
	}
	return field.Qualifier, field.Name
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
		if rewritten, ok := rewriteViewColumnComparisonExpr(typed, expansion); ok {
			return rewritten
		}
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

func rewriteViewColumnComparisonExpr(binary UnboundBinaryExpr, expansion viewExpansion) (UnboundExpr, bool) {
	if !viewExpressionComparisonOp(binary.Op) {
		return nil, false
	}
	if field, ok := binary.Left.(UnboundFieldExpr); ok {
		if mapped, mappedOK := viewMappedExprForField(field, expansion); mappedOK {
			return rewriteViewExpressionAliasComparison(mapped, binary.Op, binary.Right)
		}
	}
	if field, ok := binary.Right.(UnboundFieldExpr); ok {
		if mapped, mappedOK := viewMappedExprForField(field, expansion); mappedOK {
			return rewriteViewExpressionAliasComparison(mapped, flipViewExpressionComparisonOp(binary.Op), binary.Left)
		}
	}
	return nil, false
}

func viewMappedExprForField(field UnboundFieldExpr, expansion viewExpansion) (UnboundExpr, bool) {
	if field.Qualifier != "" && !strings.EqualFold(field.Qualifier, expansion.viewRef) {
		return nil, false
	}
	if field.Name == "*" || expansion.columns.wildcard {
		return nil, false
	}
	return expansionMappedExpr(expansion, field.Name)
}

func rewriteViewExpressionAliasComparison(expr UnboundExpr, op BinaryOp, value UnboundExpr) (UnboundExpr, bool) {
	if _, ok := expr.(UnboundFieldExpr); ok {
		return UnboundBinary(op, expr, value), true
	}
	binary, ok := expr.(UnboundBinaryExpr)
	if !ok {
		return nil, false
	}
	switch binary.Op {
	case BinaryOpAdd:
		if field, offset, ok := viewExpressionFieldAndNumericOffset(binary.Left, binary.Right); ok {
			adjusted, ok := adjustViewExpressionNumericLiteral(value, offset, BinaryOpSubtract)
			if !ok {
				return nil, false
			}
			return UnboundBinary(op, field, adjusted), true
		}
	case BinaryOpSubtract:
		if field, offset, ok := viewExpressionLeadingFieldAndNumericOffset(binary.Left, binary.Right); ok {
			adjusted, ok := adjustViewExpressionNumericLiteral(value, offset, BinaryOpAdd)
			if !ok {
				return nil, false
			}
			return UnboundBinary(op, field, adjusted), true
		}
		if offset, field, ok := viewExpressionNumericOffsetAndField(binary.Left, binary.Right); ok {
			adjusted, ok := adjustViewExpressionNumericLiteral(offset, value, BinaryOpSubtract)
			if !ok {
				return nil, false
			}
			return UnboundBinary(flipViewExpressionComparisonOp(op), field, adjusted), true
		}
	}
	return nil, false
}

func viewExpressionLeadingFieldAndNumericOffset(left UnboundExpr, right UnboundExpr) (UnboundFieldExpr, UnboundLiteralExpr, bool) {
	field, ok := left.(UnboundFieldExpr)
	if !ok {
		return UnboundFieldExpr{}, UnboundLiteralExpr{}, false
	}
	literal, ok := right.(UnboundLiteralExpr)
	if !ok || !viewExpressionNumericLiteral(literal) {
		return UnboundFieldExpr{}, UnboundLiteralExpr{}, false
	}
	return field, literal, true
}

func viewExpressionFieldAndNumericOffset(left UnboundExpr, right UnboundExpr) (UnboundFieldExpr, UnboundLiteralExpr, bool) {
	if field, ok := left.(UnboundFieldExpr); ok {
		if literal, literalOK := right.(UnboundLiteralExpr); literalOK && viewExpressionNumericLiteral(literal) {
			return field, literal, true
		}
	}
	if field, ok := right.(UnboundFieldExpr); ok {
		if literal, literalOK := left.(UnboundLiteralExpr); literalOK && viewExpressionNumericLiteral(literal) {
			return field, literal, true
		}
	}
	return UnboundFieldExpr{}, UnboundLiteralExpr{}, false
}

func viewExpressionNumericOffsetAndField(left UnboundExpr, right UnboundExpr) (UnboundLiteralExpr, UnboundFieldExpr, bool) {
	field, ok := right.(UnboundFieldExpr)
	if !ok {
		return UnboundLiteralExpr{}, UnboundFieldExpr{}, false
	}
	literal, ok := left.(UnboundLiteralExpr)
	if !ok || !viewExpressionNumericLiteral(literal) {
		return UnboundLiteralExpr{}, UnboundFieldExpr{}, false
	}
	return literal, field, true
}

func adjustViewExpressionNumericLiteral(value UnboundExpr, offset UnboundExpr, op BinaryOp) (UnboundLiteralExpr, bool) {
	valueLiteral, ok := value.(UnboundLiteralExpr)
	if !ok || !viewExpressionNumericLiteral(valueLiteral) {
		return UnboundLiteralExpr{}, false
	}
	offsetLiteral, ok := offset.(UnboundLiteralExpr)
	if !ok || !viewExpressionNumericLiteral(offsetLiteral) {
		return UnboundLiteralExpr{}, false
	}
	valueNumber, valueInt, ok := viewExpressionLiteralNumber(valueLiteral)
	if !ok {
		return UnboundLiteralExpr{}, false
	}
	offsetNumber, offsetInt, ok := viewExpressionLiteralNumber(offsetLiteral)
	if !ok {
		return UnboundLiteralExpr{}, false
	}
	adjusted := valueNumber
	switch op {
	case BinaryOpAdd:
		adjusted += offsetNumber
	case BinaryOpSubtract:
		adjusted -= offsetNumber
	default:
		return UnboundLiteralExpr{}, false
	}
	if valueInt && offsetInt && adjusted == float64(int64(adjusted)) {
		return UnboundLiteral(ValueInt, int64(adjusted)), true
	}
	return UnboundLiteral(ValueFloat, adjusted), true
}

func viewExpressionNumericLiteral(literal UnboundLiteralExpr) bool {
	switch literal.Kind {
	case ValueInt, ValueFloat:
		return true
	default:
		return false
	}
}

func viewExpressionLiteralNumber(literal UnboundLiteralExpr) (float64, bool, bool) {
	switch typed := literal.Value.(type) {
	case int:
		return float64(typed), true, true
	case int8:
		return float64(typed), true, true
	case int16:
		return float64(typed), true, true
	case int32:
		return float64(typed), true, true
	case int64:
		return float64(typed), true, true
	case uint:
		return float64(typed), true, true
	case uint8:
		return float64(typed), true, true
	case uint16:
		return float64(typed), true, true
	case uint32:
		return float64(typed), true, true
	case uint64:
		return float64(typed), true, true
	case float32:
		return float64(typed), false, true
	case float64:
		return typed, false, true
	default:
		return 0, false, false
	}
}

func viewExpressionComparisonOp(op BinaryOp) bool {
	switch op {
	case BinaryOpEqual, BinaryOpNotEqual, BinaryOpLess, BinaryOpLessEqual, BinaryOpGreater, BinaryOpGreaterEqual:
		return true
	default:
		return false
	}
}

func flipViewExpressionComparisonOp(op BinaryOp) BinaryOp {
	switch op {
	case BinaryOpLess:
		return BinaryOpGreater
	case BinaryOpLessEqual:
		return BinaryOpGreaterEqual
	case BinaryOpGreater:
		return BinaryOpLess
	case BinaryOpGreaterEqual:
		return BinaryOpLessEqual
	default:
		return op
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

func rewriteViewDefinitionAggregates(aggregates []UnboundAggregate, base UnboundTable, replacementRef string) ([]UnboundAggregate, DiagnosticSet) {
	aliases := tableAliases(base)
	rewritten := make([]UnboundAggregate, 0, len(aggregates))
	var diagnostics DiagnosticSet
	for _, aggregate := range aggregates {
		if aggregate.Input != nil {
			aggregate.Input = rewriteBaseQualifierExpr(aggregate.Input, aliases, replacementRef, &diagnostics)
		}
		if aggregate.Filter != nil {
			aggregate.Filter = rewriteBaseQualifierExpr(aggregate.Filter, aliases, replacementRef, &diagnostics)
		}
		rewritten = append(rewritten, aggregate)
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

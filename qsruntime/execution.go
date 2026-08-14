package qsruntime

import "github.com/QuantaStream/quantastream/qsbridge"

// ExecutionRequest is qsruntime's neutral execution envelope.
//
// It is intentionally smaller than legacy shared request types. The envelope
// carries qsbridge-neutral query, aggregate, and materialization contracts
// before a direct or legacy adapter chooses how to execute them.
type ExecutionRequest struct {
	Query                       qsbridge.QuantaIntermediateQuery
	PreserveBitmapFragmentOrder bool
	HasCandidateSet             bool
	CandidateSet                qsbridge.QuantaCandidateSet
	SourceIndexes               []string
	Sources                     []qsbridge.TableInstance
	Joins                       []qsbridge.JoinEdge
	Memberships                 []qsbridge.MembershipEdge
	Result                      qsbridge.ResultShape
	Statement                   qsbridge.StatementResult
	Projection                  []qsbridge.ProjectionColumn
	Predicates                  []qsbridge.Predicate
	GroupBy                     []qsbridge.Expr
	Having                      []qsbridge.Predicate
	ProjectionOrder             []qsbridge.FieldRef
	OrderBy                     []qsbridge.SortSpec
	Options                     qsbridge.ExecutionOptions
	Mutation                    qsbridge.MutationShape
	SQLAggregates               []qsbridge.Aggregate
	Aggregates                  []qsbridge.QuantaAggregateRequest
	Materialization             qsbridge.QuantaMaterializationRequest
	NativePredicates            NativePredicateSet
	FilterDomain                qsbridge.QuantaFilterDomainTranslation
	NodeCatalog                 qsbridge.NodeCatalogView
	QueryCatalog                qsbridge.QueryCatalogView
	Route                       ExecutionRoute
	Probes                      []ExecutionProbe
}

// NewExecutionRequest copies a neutral Quanta query into a runtime envelope.
func NewExecutionRequest(query qsbridge.QuantaIntermediateQuery) ExecutionRequest {
	request := ExecutionRequest{
		Query: cloneIntermediateQuery(query),
		Route: DirectQIABRoute(),
	}
	request.FilterDomain = filterDomainTranslationFromRequest(request)
	return request
}

// WithCandidateSet returns a request that starts execution from a precomputed candidate set.
func (r ExecutionRequest) WithCandidateSet(candidates qsbridge.QuantaCandidateSet) ExecutionRequest {
	r.HasCandidateSet = true
	r.CandidateSet = cloneCandidateSet(candidates)
	r.Query = cloneIntermediateQuery(r.Query)
	r.Query.Filter = qsbridge.QuantaFilterExpression{}
	r.Query.Fragments = nil
	r.FilterDomain = qsbridge.QuantaFilterDomainTranslation{}
	return r
}

// NewSQLExecutionRequest preserves SQL-visible result controls alongside lowered Quanta fragments.
func NewSQLExecutionRequest(query qsbridge.QuantaIntermediateQuery, request qsbridge.ExecutionRequest) ExecutionRequest {
	runtimeRequest := NewExecutionRequest(query)
	preparedQuery := request.Bound.Prepared.Query
	runtimeRequest.SourceIndexes = sourceIndexes(request.Bound.Prepared.Query.Sources)
	runtimeRequest.Sources = append([]qsbridge.TableInstance(nil), preparedQuery.Sources...)
	runtimeRequest.Joins = append([]qsbridge.JoinEdge(nil), preparedQuery.Joins...)
	runtimeRequest.Memberships = append([]qsbridge.MembershipEdge(nil), preparedQuery.Memberships...)
	runtimeRequest.Result = cloneResultShape(request.Result)
	runtimeRequest.Statement = cloneStatementResult(request.Statement)
	runtimeRequest.Projection = cloneProjectionColumns(preparedQuery.Projection)
	runtimeRequest.Predicates = append([]qsbridge.Predicate(nil), preparedQuery.Predicates...)
	runtimeRequest.Predicates = append(runtimeRequest.Predicates, residualWhereExprPredicate(preparedQuery)...)
	runtimeRequest.GroupBy = append([]qsbridge.Expr(nil), preparedQuery.GroupBy...)
	runtimeRequest.Having = append([]qsbridge.Predicate(nil), preparedQuery.Having...)
	runtimeRequest.ProjectionOrder = projectionOrder(preparedQuery.Projection)
	runtimeRequest.OrderBy = append([]qsbridge.SortSpec(nil), preparedQuery.OrderBy...)
	runtimeRequest.Options = request.Options
	runtimeRequest.Mutation = cloneMutationShape(preparedQuery.Mutation)
	runtimeRequest.SQLAggregates = append([]qsbridge.Aggregate(nil), preparedQuery.Aggregates...)
	runtimeRequest.Materialization = materializationRequestFromPreparedQuery(runtimeRequest, preparedQuery)
	runtimeRequest.FilterDomain = filterDomainTranslationFromRequest(runtimeRequest)
	return runtimeRequest
}

func filterDomainTranslationFromRequest(request ExecutionRequest) qsbridge.QuantaFilterDomainTranslation {
	if request.Query.Filter.Empty() {
		return qsbridge.QuantaFilterDomainTranslation{}
	}
	summary := request.Query.Filter.DomainSummary()
	target, _ := request.RootIndex()
	if target == "" {
		target, _ = summary.Single()
	}
	return summary.TranslationRequirement(target)
}

func residualWhereExprPredicate(query qsbridge.QueryIR) []qsbridge.Predicate {
	if query.WhereExpr == nil || len(query.Joins) == 0 {
		return nil
	}
	return []qsbridge.Predicate{{
		Expr:      query.WhereExpr,
		Placement: qsbridge.PredicateResidualScan,
		Scope:     qsbridge.PredicateScopeWhere,
	}}
}

func cloneProjectionColumns(columns []qsbridge.ProjectionColumn) []qsbridge.ProjectionColumn {
	cloned := make([]qsbridge.ProjectionColumn, len(columns))
	copy(cloned, columns)
	return cloned
}

func cloneCandidateSet(candidates qsbridge.QuantaCandidateSet) qsbridge.QuantaCandidateSet {
	candidates.Rownums = append([]qsbridge.QuantaRownum(nil), candidates.Rownums...)
	return candidates
}

func projectionOrder(columns []qsbridge.ProjectionColumn) []qsbridge.FieldRef {
	fields := make([]qsbridge.FieldRef, 0, len(columns))
	for _, column := range columns {
		switch expr := column.Expr.(type) {
		case qsbridge.FieldExpr:
			fields = append(fields, expr.Ref)
		case *qsbridge.FieldExpr:
			if expr != nil {
				fields = append(fields, expr.Ref)
			}
		}
	}
	return fields
}

func materializationRequestFromPreparedQuery(runtimeRequest ExecutionRequest, preparedQuery qsbridge.QueryIR) qsbridge.QuantaMaterializationRequest {
	rootIndex, _ := runtimeRequest.RootIndex()
	if rootIndex == "" && len(preparedQuery.Sources) == 1 {
		rootIndex = preparedQuery.Sources[0].Table
	}
	fields := materializationFieldsFromRequiredFields(rootIndex, preparedQuery.RequiredFields(), materializationVisibleFieldKeys(preparedQuery.Projection))
	if len(fields) == 0 {
		return qsbridge.QuantaMaterializationRequest{}
	}
	materialization := qsbridge.QuantaMaterializationRequest{
		Index:            rootIndex,
		ProjectionFields: fields,
	}
	timeWindowRequest := runtimeRequest
	timeWindowRequest.Materialization = materialization
	for _, fragment := range runtimeRequest.Query.Fragments {
		if fragment.BSIOp != qsbridge.QuantaBSIOpRange || fragment.Begin == nil || fragment.End == nil {
			continue
		}
		if !fragment.ShardWindow {
			continue
		}
		materialization.FromEpochMillis = fragment.Begin.Int64()
		materialization.ToEpochMillis = fragment.End.Int64()
		break
	}
	return materialization
}

func materializationFieldsFromExecutionRequest(request ExecutionRequest) []qsbridge.QuantaProjectionField {
	rootIndex, _ := request.RootIndex()
	refs := materializationFieldRefsFromExecutionRequest(request)
	if len(refs) == 0 {
		return nil
	}
	return materializationFieldsFromRequiredFields(rootIndex, refs, materializationVisibleFieldKeys(request.Projection))
}

func materializationFieldRefsFromExecutionRequest(request ExecutionRequest) []qsbridge.FieldRef {
	refs := make([]qsbridge.FieldRef, 0)
	for _, predicate := range request.Predicates {
		switch predicate.Placement {
		case qsbridge.PredicateResidualScan, qsbridge.PredicateResidualJoin:
			refs = appendMaterializationExprFieldRefs(refs, predicate.Expr)
		}
	}
	for _, projection := range request.Projection {
		refs = appendMaterializationExprFieldRefs(refs, projection.Expr)
	}
	for _, expr := range request.GroupBy {
		refs = appendMaterializationExprFieldRefs(refs, expr)
	}
	for _, aggregate := range request.SQLAggregates {
		refs = appendMaterializationExprFieldRefs(refs, aggregate.Input)
		refs = appendMaterializationExprFieldRefs(refs, aggregate.Filter)
	}
	for _, predicate := range request.Having {
		refs = appendMaterializationExprFieldRefs(refs, predicate.Expr)
	}
	for _, sort := range request.OrderBy {
		refs = appendMaterializationExprFieldRefs(refs, sort.Expr)
	}
	for _, hidden := range request.Result.Hidden {
		refs = append(refs, hidden)
	}
	return refs
}

func appendMaterializationExprFieldRefs(refs []qsbridge.FieldRef, expr qsbridge.Expr) []qsbridge.FieldRef {
	return append(refs, qsbridge.FieldRefs(expr)...)
}

func materializationFieldsFromRequiredFields(defaultIndex string, refs []qsbridge.FieldRef, visible map[string]struct{}) []qsbridge.QuantaProjectionField {
	seen := make(map[string]struct{})
	fields := make([]qsbridge.QuantaProjectionField, 0, len(refs))
	for _, ref := range refs {
		index := ref.Table.Table
		if index == "" {
			index = defaultIndex
		}
		role := materializationFieldRole(defaultIndex, ref)
		name := ref.PhysicalName
		if name == "" {
			name = ref.Name
		}
		if name == "" {
			continue
		}
		key := role + "." + name
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		_, isVisible := visible[materializationFieldKey(defaultIndex, ref)]
		fields = append(fields, qsbridge.QuantaProjectionField{
			Index:        index,
			Role:         qsbridge.TableInstanceID(role),
			Field:        name,
			Type:         ref.Type,
			PhysicalName: ref.PhysicalName,
			Roles:        ref.Roles,
			Visible:      isVisible,
		})
	}
	return fields
}

func materializationVisibleFieldKeys(columns []qsbridge.ProjectionColumn) map[string]struct{} {
	visible := make(map[string]struct{})
	for _, column := range columns {
		for _, ref := range column.RequiredFields() {
			index := ref.Table.Table
			visible[materializationFieldKey(index, ref)] = struct{}{}
		}
	}
	return visible
}

func materializationFieldRole(defaultIndex string, ref qsbridge.FieldRef) string {
	switch {
	case ref.Table.Alias != "":
		return ref.Table.Alias
	case ref.Table.ID != "":
		return string(ref.Table.ID)
	case ref.Table.Table != "":
		return ref.Table.Table
	default:
		return defaultIndex
	}
}

func materializationFieldKey(defaultIndex string, ref qsbridge.FieldRef) string {
	name := ref.PhysicalName
	if name == "" {
		name = ref.Name
	}
	return materializationFieldRole(defaultIndex, ref) + "." + name
}

// WithRoute returns a copy of the request targeting the supplied execution route.
func (r ExecutionRequest) WithRoute(route ExecutionRoute) ExecutionRequest {
	r.Route = route
	return r
}

// FragmentCount reports how many bitmap-query fragments are present.
func (r ExecutionRequest) FragmentCount() int {
	return len(r.Query.Fragments)
}

// ProjectionCount reports how many projection fields are requested.
func (r ExecutionRequest) ProjectionCount() int {
	return len(r.Query.ProjectionFields)
}

// AggregateCount reports how many native aggregate handoffs are requested.
func (r ExecutionRequest) AggregateCount() int {
	return len(r.Aggregates)
}

// RootIndex returns the first table/index needed to start direct execution.
func (r ExecutionRequest) RootIndex() (string, bool) {
	for _, fragment := range r.Query.Fragments {
		if fragment.Index != "" {
			return fragment.Index, true
		}
	}
	for _, seed := range r.Query.Seeds {
		if seed.Index != "" {
			return seed.Index, true
		}
	}
	for _, projection := range r.Query.ProjectionFields {
		if projection.Index != "" {
			return projection.Index, true
		}
	}
	for _, index := range r.SourceIndexes {
		if index != "" {
			return index, true
		}
	}
	if r.Mutation.Target.Table != "" {
		return r.Mutation.Target.Table, true
	}
	return "", false
}

// ExecutionResult is qsbridge's native bitmap/runtime result envelope.
type ExecutionResult = qsbridge.QuantaExecutionResult

// ExecutionProbe is qsbridge's neutral projector/materialization observation shape.
type ExecutionProbe = qsbridge.ProjectionProbe

func cloneIntermediateQuery(query qsbridge.QuantaIntermediateQuery) qsbridge.QuantaIntermediateQuery {
	return qsbridge.QuantaIntermediateQuery{
		Fragments:        cloneQueryFragments(query.Fragments),
		Seeds:            cloneQuantaSeeds(query.Seeds),
		Filter:           cloneQuantaFilterExpression(query.Filter),
		ProjectionFields: append([]qsbridge.QuantaProjectionField(nil), query.ProjectionFields...),
	}
}

func cloneResultShape(shape qsbridge.ResultShape) qsbridge.ResultShape {
	cloned := shape
	cloned.Columns = append([]qsbridge.FieldRef(nil), shape.Columns...)
	cloned.Hidden = append([]qsbridge.FieldRef(nil), shape.Hidden...)
	cloned.OrderBy = append([]qsbridge.Expr(nil), shape.OrderBy...)
	return cloned
}

func cloneStatementResult(statement qsbridge.StatementResult) qsbridge.StatementResult {
	cloned := statement
	cloned.Notices = append([]qsbridge.StatementNotice(nil), statement.Notices...)
	cloned.SessionActions = append([]qsbridge.SessionAction(nil), statement.SessionActions...)
	return cloned
}

func cloneMutationShape(mutation qsbridge.MutationShape) qsbridge.MutationShape {
	cloned := mutation
	cloned.Columns = append([]qsbridge.FieldRef(nil), mutation.Columns...)
	cloned.Rows = append([]qsbridge.MutationRow(nil), mutation.Rows...)
	for i := range cloned.Rows {
		cloned.Rows[i].Values = append([]qsbridge.Expr(nil), mutation.Rows[i].Values...)
	}
	cloned.Assignments = append([]qsbridge.MutationAssignment(nil), mutation.Assignments...)
	cloned.Predicates = append([]qsbridge.Predicate(nil), mutation.Predicates...)
	cloned.DependentRelationships = append([]qsbridge.RelationshipDefinition(nil), mutation.DependentRelationships...)
	return cloned
}

func sourceIndexes(sources []qsbridge.TableInstance) []string {
	indexes := make([]string, 0, len(sources))
	for _, source := range sources {
		if source.Table != "" {
			indexes = append(indexes, source.Table)
		}
	}
	return indexes
}

func cloneQueryFragments(fragments []qsbridge.QuantaQueryFragment) []qsbridge.QuantaQueryFragment {
	cloned := make([]qsbridge.QuantaQueryFragment, len(fragments))
	for i, fragment := range fragments {
		cloned[i] = cloneQueryFragment(fragment)
	}
	return cloned
}

func cloneQuantaSeeds(seeds []qsbridge.QuantaSeed) []qsbridge.QuantaSeed {
	cloned := make([]qsbridge.QuantaSeed, len(seeds))
	for i, seed := range seeds {
		cloned[i] = cloneQuantaSeed(seed)
	}
	return cloned
}

func cloneQuantaFilterExpression(filter qsbridge.QuantaFilterExpression) qsbridge.QuantaFilterExpression {
	cloned := filter
	cloned.Fragment = cloneQueryFragment(filter.Fragment)
	cloned.Children = make([]qsbridge.QuantaFilterExpression, len(filter.Children))
	for i, child := range filter.Children {
		cloned.Children[i] = cloneQuantaFilterExpression(child)
	}
	return cloned
}

func cloneQueryFragment(fragment qsbridge.QuantaQueryFragment) qsbridge.QuantaQueryFragment {
	cloned := fragment
	cloned.Value = cloneBigInt(fragment.Value)
	cloned.Values = cloneBigIntSlice(fragment.Values)
	cloned.Begin = cloneBigInt(fragment.Begin)
	cloned.End = cloneBigInt(fragment.End)
	return cloned
}

func cloneQuantaSeed(seed qsbridge.QuantaSeed) qsbridge.QuantaSeed {
	cloned := seed
	cloned.Begin = cloneBigInt(seed.Begin)
	cloned.End = cloneBigInt(seed.End)
	return cloned
}

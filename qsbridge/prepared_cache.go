package qsbridge

import "sort"

// PreparedPlanCache is the process-local cache boundary for prepared plans.
//
// The interface is deliberately small and backend-neutral. Protocol adapters can
// use it to avoid reparsing/replanning without qsbridge owning session storage,
// invalidation policy, or runtime execution.
type PreparedPlanCache interface {
	Get(key PlanCacheKey) (PreparedPlan, bool)
	Put(plan PreparedPlan)
	Delete(key PlanCacheKey)
	Clear()
}

// PreparedPlanCacheInspector optionally exposes metadata about cached prepared plans.
type PreparedPlanCacheInspector interface {
	ListPreparedPlanCacheEntries() []PreparedPlanCacheEntry
}

// PreparedPlanCacheEntry describes one cached prepared plan without exposing plan internals.
type PreparedPlanCacheEntry struct {
	Key               PlanCacheKey
	Handle            PreparedStatementHandle
	SQL               string
	Schema            string
	CatalogVersion    CatalogVersion
	User              UserName
	Kind              QueryKind
	AccessIntent      PhysicalAccessIntent
	Lifecycle         ClientPlanLifecycleKind
	LifecycleSteps    int
	Supported         bool
	ParameterCount    int
	ResultColumnCount int
	Scope             PhysicalScope
}

// MemoryPreparedPlanCache stores prepared plans in a lock-sharded in-memory map.
type MemoryPreparedPlanCache struct {
	plans *shardedValueCache
}

// NewMemoryPreparedPlanCache creates an empty in-memory prepared-plan cache.
func NewMemoryPreparedPlanCache() *MemoryPreparedPlanCache {
	return &MemoryPreparedPlanCache{plans: newShardedValueCache()}
}

// Get returns a prepared plan by cache key.
func (c *MemoryPreparedPlanCache) Get(key PlanCacheKey) (PreparedPlan, bool) {
	if c == nil || c.plans == nil || key.Digest == "" {
		return PreparedPlan{}, false
	}
	value, ok := c.plans.Get(key.Digest)
	if !ok {
		return PreparedPlan{}, false
	}
	return clonePreparedPlan(value.(PreparedPlan)), true
}

// Put stores plan under its deterministic cache key.
func (c *MemoryPreparedPlanCache) Put(plan PreparedPlan) {
	if c == nil || c.plans == nil {
		return
	}
	key := plan.CacheKey()
	if key.Digest == "" {
		return
	}
	c.plans.Set(key.Digest, clonePreparedPlan(plan))
}

// Delete removes one prepared plan by cache key.
func (c *MemoryPreparedPlanCache) Delete(key PlanCacheKey) {
	if c == nil || c.plans == nil || key.Digest == "" {
		return
	}
	c.plans.Delete(key.Digest)
}

// Clear removes all prepared plans.
func (c *MemoryPreparedPlanCache) Clear() {
	if c == nil || c.plans == nil {
		return
	}
	c.plans.Clear()
}

// ListPreparedPlanCacheEntries returns metadata for cached prepared plans.
func (c *MemoryPreparedPlanCache) ListPreparedPlanCacheEntries() []PreparedPlanCacheEntry {
	if c == nil || c.plans == nil {
		return nil
	}
	entries := make([]PreparedPlanCacheEntry, 0)
	for _, entry := range c.plans.Entries() {
		plan, ok := entry.Value.(PreparedPlan)
		if !ok {
			continue
		}
		entries = append(entries, preparedPlanCacheEntry(plan))
	}
	sortPreparedPlanCacheEntries(entries)
	return entries
}

func preparedPlanCacheEntry(plan PreparedPlan) PreparedPlanCacheEntry {
	key := plan.CacheKey()
	return PreparedPlanCacheEntry{
		Key:               key,
		Handle:            plan.Handle,
		SQL:               plan.SQL,
		Schema:            key.Schema,
		CatalogVersion:    key.CatalogVersion,
		User:              key.User,
		Kind:              plan.Kind,
		AccessIntent:      plan.AccessIntent(),
		Lifecycle:         clientPlanLifecycleKind(plan.Kind),
		LifecycleSteps:    clientPlanLifecycleStepCount(plan.Kind),
		Supported:         plan.Supported && !plan.Diagnostics.BlocksNative(),
		ParameterCount:    len(plan.Parameters),
		ResultColumnCount: len(plan.ResultColumns),
		Scope:             clonePhysicalScope(plan.Scope),
	}
}

func clonePreparedPlan(plan PreparedPlan) PreparedPlan {
	cloned := plan
	cloned.Session = plan.Session.Clone()
	cloned.Scope = clonePhysicalScope(plan.Scope)
	cloned.Diagnostics = cloneDiagnosticSet(plan.Diagnostics)
	cloned.Access = cloneAccessRequirements(plan.Access)
	cloned.Parameters = append([]ParameterRef(nil), plan.Parameters...)
	cloned.ResultColumns = append([]ResultColumn(nil), plan.ResultColumns...)
	cloned.Statement = cloneStatementResult(plan.Statement)
	cloned.Result.Statement = cloneStatementResult(plan.Result.Statement)
	cloned.Query = cloneQueryIR(plan.Query)
	return cloned
}

func cloneAccessRequirements(requirements []AccessRequirement) []AccessRequirement {
	if len(requirements) == 0 {
		return nil
	}
	cloned := make([]AccessRequirement, 0, len(requirements))
	for _, requirement := range requirements {
		requirement.Fields = append([]FieldRef(nil), requirement.Fields...)
		cloned = append(cloned, requirement)
	}
	return cloned
}

func cloneQueryIR(query QueryIR) QueryIR {
	cloned := query
	cloned.Sources = append([]TableInstance(nil), query.Sources...)
	cloned.InlineRows = cloneInlineRowSets(query.InlineRows)
	cloned.Joins = append([]JoinEdge(nil), query.Joins...)
	cloned.Memberships = cloneMembershipEdges(query.Memberships)
	cloned.Predicates = append([]Predicate(nil), query.Predicates...)
	cloned.WhereExpr = query.WhereExpr
	cloned.Projection = append([]ProjectionColumn(nil), query.Projection...)
	cloned.GroupBy = append([]Expr(nil), query.GroupBy...)
	cloned.Aggregates = append([]Aggregate(nil), query.Aggregates...)
	cloned.Having = append([]Predicate(nil), query.Having...)
	cloned.OrderBy = append([]SortSpec(nil), query.OrderBy...)
	cloned.Subqueries = cloneSubqueryPlanIntents(query.Subqueries)
	cloned.Result.Columns = append([]FieldRef(nil), query.Result.Columns...)
	cloned.Result.Statement = cloneStatementResult(query.Result.Statement)
	cloned.UnionAll = cloneQueryIRs(query.UnionAll)
	cloned.Mutation.Columns = append([]FieldRef(nil), query.Mutation.Columns...)
	cloned.Mutation.Rows = append([]MutationRow(nil), query.Mutation.Rows...)
	cloned.Mutation.Assignments = append([]MutationAssignment(nil), query.Mutation.Assignments...)
	cloned.Mutation.Predicates = append([]Predicate(nil), query.Mutation.Predicates...)
	cloned.Mutation.Relationships = cloneRelationshipDefinitions(query.Mutation.Relationships)
	cloned.Mutation.DependentRelationships = cloneRelationshipDefinitions(query.Mutation.DependentRelationships)
	cloned.Mutation.ValidationSteps = cloneMutationValidationSteps(query.Mutation.ValidationSteps)
	cloned.Blockers = append([]NativeBlocker(nil), query.Blockers...)
	return cloned
}

func cloneMutationValidationSteps(steps []MutationValidationStep) []MutationValidationStep {
	if len(steps) == 0 {
		return nil
	}
	cloned := make([]MutationValidationStep, 0, len(steps))
	for _, step := range steps {
		step.Columns = append([]FieldRef(nil), step.Columns...)
		step.ReferencedColumns = append([]FieldRef(nil), step.ReferencedColumns...)
		cloned = append(cloned, step)
	}
	return cloned
}

func cloneSubqueryPlanIntents(intents []SubqueryPlanIntent) []SubqueryPlanIntent {
	if len(intents) == 0 {
		return nil
	}
	cloned := make([]SubqueryPlanIntent, 0, len(intents))
	for _, intent := range intents {
		cloned = append(cloned, cloneSubqueryPlanIntent(intent))
	}
	return cloned
}

func cloneSubqueryPlanIntent(intent SubqueryPlanIntent) SubqueryPlanIntent {
	cloned := intent
	cloned.HelperIntents = append([]SubqueryHelperIntent(nil), intent.HelperIntents...)
	cloned.Access = cloneAccessRequirements(intent.Access)
	if intent.Scalar != nil {
		scalar := *intent.Scalar
		cloned.Scalar = &scalar
	}
	if intent.CorrelatedAggregate != nil {
		correlated := *intent.CorrelatedAggregate
		correlated.RequiredFilterFields = append([]FieldRef(nil), intent.CorrelatedAggregate.RequiredFilterFields...)
		correlated.RequiredFilters = append([]string(nil), intent.CorrelatedAggregate.RequiredFilters...)
		cloned.CorrelatedAggregate = &correlated
	}
	if intent.CorrelatedMembership != nil {
		correlated := *intent.CorrelatedMembership
		correlated.CrossDomainPredicates = append([]string(nil), intent.CorrelatedMembership.CrossDomainPredicates...)
		correlated.RequiredFilters = append([]string(nil), intent.CorrelatedMembership.RequiredFilters...)
		cloned.CorrelatedMembership = &correlated
	}
	return cloned
}

func cloneQueryIRs(queries []QueryIR) []QueryIR {
	if len(queries) == 0 {
		return nil
	}
	cloned := make([]QueryIR, 0, len(queries))
	for _, query := range queries {
		cloned = append(cloned, cloneQueryIR(query))
	}
	return cloned
}

func cloneMembershipEdges(edges []MembershipEdge) []MembershipEdge {
	if len(edges) == 0 {
		return nil
	}
	cloned := make([]MembershipEdge, len(edges))
	for index, edge := range edges {
		cloned[index] = edge
		cloned[index].Predicates = append([]Predicate(nil), edge.Predicates...)
		cloned[index].RightInlineRows = cloneInlineRowSetPointer(edge.RightInlineRows)
		cloned[index].LeftTuple = append([]Expr(nil), edge.LeftTuple...)
		cloned[index].RightTuple = append([]Expr(nil), edge.RightTuple...)
	}
	return cloned
}

func cloneStatementResult(result StatementResult) StatementResult {
	result.Notices = cloneStatementNotices(result.Notices)
	result.SessionActions = cloneSessionActions(result.SessionActions)
	return result
}

func clonePreparedPlanCacheEntries(entries []PreparedPlanCacheEntry) []PreparedPlanCacheEntry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]PreparedPlanCacheEntry, 0, len(entries))
	for _, entry := range entries {
		entry.Key.Roles = append([]RoleName(nil), entry.Key.Roles...)
		entry.Key.SQLModes = append([]SQLMode(nil), entry.Key.SQLModes...)
		entry.Key.Variables = sortedVariableCopy(entry.Key.Variables)
		entry.Key.Scope = clonePhysicalScope(entry.Key.Scope)
		entry.Scope = clonePhysicalScope(entry.Scope)
		cloned = append(cloned, entry)
	}
	return cloned
}

func sortPreparedPlanCacheEntries(entries []PreparedPlanCacheEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Schema != entries[j].Schema {
			return entries[i].Schema < entries[j].Schema
		}
		if entries[i].SQL != entries[j].SQL {
			return entries[i].SQL < entries[j].SQL
		}
		return entries[i].Key.Digest < entries[j].Key.Digest
	})
}

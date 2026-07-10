package qsbridge

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

// CatalogVersion identifies one catalog metadata generation for plan-cache safety.
type CatalogVersion string

// PlanCacheKey identifies the context a plan was built under.
//
// It is deliberately conservative: user, roles, SQL modes, session variables,
// default schema, and physical scope all participate so future plan caches can
// avoid reusing a plan across compatibility or authorization boundaries.
type PlanCacheKey struct {
	Digest         string
	SQL            string
	Schema         string
	CatalogVersion CatalogVersion
	User           UserName
	Roles          []RoleName
	SQLModes       []SQLMode
	TimeZone       string
	Variables      map[string]string
	Scope          PhysicalScope
}

// PlanCacheFactor identifies one candidate input to plan-cache identity.
type PlanCacheFactor string

const (
	// PlanCacheFactorSQL is the SQL text being prepared.
	PlanCacheFactorSQL PlanCacheFactor = "sql"
	// PlanCacheFactorSchema is the effective default schema.
	PlanCacheFactorSchema PlanCacheFactor = "schema"
	// PlanCacheFactorCatalogVersion is the catalog metadata generation.
	PlanCacheFactorCatalogVersion PlanCacheFactor = "catalog_version"
	// PlanCacheFactorUser is the authenticated planning user.
	PlanCacheFactorUser PlanCacheFactor = "user"
	// PlanCacheFactorRoles is the sorted role set active while planning.
	PlanCacheFactorRoles PlanCacheFactor = "roles"
	// PlanCacheFactorSQLModes is the sorted SQL compatibility mode set.
	PlanCacheFactorSQLModes PlanCacheFactor = "sql_modes"
	// PlanCacheFactorTimeZone is the planning session time zone.
	PlanCacheFactorTimeZone PlanCacheFactor = "time_zone"
	// PlanCacheFactorVariables is the sorted planning session variable map.
	PlanCacheFactorVariables PlanCacheFactor = "variables"
	// PlanCacheFactorPhysicalScope is shard, replica, placement, routing, and cache scope.
	PlanCacheFactorPhysicalScope PlanCacheFactor = "physical_scope"
	// PlanCacheFactorOptimizationTrace is optimizer audit metadata supplied with the request.
	PlanCacheFactorOptimizationTrace PlanCacheFactor = "optimization_trace"
	// PlanCacheFactorExplainOptions is section-selection metadata for explain output.
	PlanCacheFactorExplainOptions PlanCacheFactor = "explain_options"
	// PlanCacheFactorProfileOptions is execution profile and timing metadata selection.
	PlanCacheFactorProfileOptions PlanCacheFactor = "profile_options"
	// PlanCacheFactorParameterValues is the execute-time prepared statement parameter set.
	PlanCacheFactorParameterValues PlanCacheFactor = "parameter_values"
	// PlanCacheFactorResultBatching is max-row, batch-size, and streaming preferences.
	PlanCacheFactorResultBatching PlanCacheFactor = "result_batching"
	// PlanCacheFactorCursorMode is client-visible cursor movement behavior.
	PlanCacheFactorCursorMode PlanCacheFactor = "cursor_mode"
)

// PlanCacheParticipation describes whether a candidate factor belongs in the digest.
type PlanCacheParticipation string

const (
	// PlanCacheParticipationIncluded means the factor participates in the prepared-plan cache digest.
	PlanCacheParticipationIncluded PlanCacheParticipation = "included"
	// PlanCacheParticipationDisplayOnly means the factor changes metadata output, not plan semantics.
	PlanCacheParticipationDisplayOnly PlanCacheParticipation = "excluded_display_only"
	// PlanCacheParticipationExecuteOnly means the factor belongs to bind/execute, not prepare planning.
	PlanCacheParticipationExecuteOnly PlanCacheParticipation = "excluded_execute_only"
	// PlanCacheParticipationAuditOnly means the factor is recorded for inspection but not cache identity.
	PlanCacheParticipationAuditOnly PlanCacheParticipation = "excluded_audit_only"
)

// PlanCacheKeyPolicy records how one factor affects prepared-plan cache identity.
type PlanCacheKeyPolicy struct {
	Factor        PlanCacheFactor
	Participation PlanCacheParticipation
	Reason        string
}

// DefaultPlanCacheKeyPolicy returns the conservative prepared-plan cache identity policy.
func DefaultPlanCacheKeyPolicy() []PlanCacheKeyPolicy {
	return clonePlanCacheKeyPolicies(defaultPlanCacheKeyPolicy)
}

// PlanCacheFactorParticipates reports whether factor is included in the digest.
func PlanCacheFactorParticipates(factor PlanCacheFactor) bool {
	for _, policy := range defaultPlanCacheKeyPolicy {
		if policy.Factor == factor {
			return policy.Participation == PlanCacheParticipationIncluded
		}
	}
	return false
}

// CacheKey returns a deterministic key for this planning request.
func (r PlanRequest) CacheKey() PlanCacheKey {
	return newPlanCacheKey(r.SQL, r.DefaultSchema, r.CatalogVersion, r.Session, r.Scope)
}

// CacheKey returns the key for this planning result.
func (r PlanResult) CacheKey() PlanCacheKey {
	return newPlanCacheKey(r.SQL, r.DefaultSchema, r.CatalogVersion, r.Session, r.Scope)
}

// CacheKey returns the key for this prepared plan.
func (p PreparedPlan) CacheKey() PlanCacheKey {
	return newPlanCacheKey(p.SQL, p.DefaultSchema, p.CatalogVersion, p.Session, p.Scope)
}

func newPlanCacheKey(sql string, schema string, catalogVersion CatalogVersion, session SessionContext, scope PhysicalScope) PlanCacheKey {
	session = session.Clone()
	if schema == "" {
		schema = session.CurrentSchema
	}
	key := PlanCacheKey{
		SQL:            sql,
		Schema:         schema,
		CatalogVersion: catalogVersion,
		User:           session.User,
		Roles:          sortedRoleNames(session.Roles),
		SQLModes:       sortedSQLModes(session.SQLModes),
		TimeZone:       session.TimeZone,
		Variables:      sortedVariableCopy(session.Variables),
		Scope:          clonePhysicalScope(scope),
	}
	key.Digest = key.digest()
	return key
}

func (k PlanCacheKey) digest() string {
	var b strings.Builder
	writeCachePart(&b, "sql", k.SQL)
	writeCachePart(&b, "schema", k.Schema)
	writeCachePart(&b, "catalog_version", string(k.CatalogVersion))
	writeCachePart(&b, "user", string(k.User))
	for _, role := range k.Roles {
		writeCachePart(&b, "role", string(role))
	}
	for _, mode := range k.SQLModes {
		writeCachePart(&b, "sql_mode", string(mode))
	}
	writeCachePart(&b, "time_zone", k.TimeZone)
	for _, name := range sortedVariableNames(k.Variables) {
		writeCachePart(&b, "var:"+name, k.Variables[name])
	}
	writeCachePart(&b, "shards_all", boolCacheValue(k.Scope.Shards.All))
	for _, shard := range sortedShardIDs(k.Scope.Shards.Shards) {
		writeCachePart(&b, "shard", string(shard))
	}
	for _, replica := range sortedReplicaIDs(k.Scope.Replicas) {
		writeCachePart(&b, "replica", string(replica))
	}
	writeCachePart(&b, "routing", string(k.Scope.Routing))
	writeCachePart(&b, "placement", string(k.Scope.Placement))
	writeCachePart(&b, "cache", string(k.Scope.Cache))

	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func writeCachePart(b *strings.Builder, name string, value string) {
	b.WriteString(name)
	b.WriteByte('=')
	b.WriteString(value)
	b.WriteByte('\x00')
}

func boolCacheValue(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func sortedRoleNames(roles []RoleName) []RoleName {
	sorted := append([]RoleName(nil), roles...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted
}

func sortedSQLModes(modes []SQLMode) []SQLMode {
	sorted := append([]SQLMode(nil), modes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted
}

func sortedVariableCopy(variables map[string]string) map[string]string {
	if variables == nil {
		return nil
	}
	copied := make(map[string]string, len(variables))
	for key, value := range variables {
		copied[key] = value
	}
	return copied
}

func sortedVariableNames(variables map[string]string) []string {
	names := make([]string, 0, len(variables))
	for name := range variables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedShardIDs(shards []ShardID) []ShardID {
	sorted := append([]ShardID(nil), shards...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted
}

func sortedReplicaIDs(replicas []ReplicaID) []ReplicaID {
	sorted := append([]ReplicaID(nil), replicas...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted
}

func clonePhysicalScope(scope PhysicalScope) PhysicalScope {
	return PhysicalScope{
		Shards: ShardSet{
			All:    scope.Shards.All,
			Shards: append([]ShardID(nil), scope.Shards.Shards...),
		},
		Replicas:  append([]ReplicaID(nil), scope.Replicas...),
		Routing:   scope.Routing,
		Placement: scope.Placement,
		Cache:     scope.Cache,
	}
}

var defaultPlanCacheKeyPolicy = []PlanCacheKeyPolicy{
	{Factor: PlanCacheFactorSQL, Participation: PlanCacheParticipationIncluded, Reason: "SQL text determines query shape"},
	{Factor: PlanCacheFactorSchema, Participation: PlanCacheParticipationIncluded, Reason: "default schema affects unqualified names"},
	{Factor: PlanCacheFactorCatalogVersion, Participation: PlanCacheParticipationIncluded, Reason: "catalog changes can alter binding, encodings, and relationships"},
	{Factor: PlanCacheFactorUser, Participation: PlanCacheParticipationIncluded, Reason: "user identity affects authorization and visibility"},
	{Factor: PlanCacheFactorRoles, Participation: PlanCacheParticipationIncluded, Reason: "roles affect authorization and visibility"},
	{Factor: PlanCacheFactorSQLModes, Participation: PlanCacheParticipationIncluded, Reason: "SQL modes can alter parsing and compatibility semantics"},
	{Factor: PlanCacheFactorTimeZone, Participation: PlanCacheParticipationIncluded, Reason: "time zone can alter temporal expression semantics"},
	{Factor: PlanCacheFactorVariables, Participation: PlanCacheParticipationIncluded, Reason: "session variables can alter planning semantics"},
	{Factor: PlanCacheFactorPhysicalScope, Participation: PlanCacheParticipationIncluded, Reason: "physical scope affects placement and executor selection"},
	{Factor: PlanCacheFactorOptimizationTrace, Participation: PlanCacheParticipationAuditOnly, Reason: "optimizer audit records explain decisions but should not define cache identity"},
	{Factor: PlanCacheFactorExplainOptions, Participation: PlanCacheParticipationDisplayOnly, Reason: "explain sections change metadata output, not prepared plan semantics"},
	{Factor: PlanCacheFactorProfileOptions, Participation: PlanCacheParticipationDisplayOnly, Reason: "profile flags change metadata output, not prepared plan semantics"},
	{Factor: PlanCacheFactorParameterValues, Participation: PlanCacheParticipationExecuteOnly, Reason: "parameter values bind at execute time after prepare"},
	{Factor: PlanCacheFactorResultBatching, Participation: PlanCacheParticipationExecuteOnly, Reason: "batching controls result delivery, not prepared plan identity"},
	{Factor: PlanCacheFactorCursorMode, Participation: PlanCacheParticipationExecuteOnly, Reason: "cursor behavior belongs to execution/result delivery"},
}

func clonePlanCacheKeyPolicies(policies []PlanCacheKeyPolicy) []PlanCacheKeyPolicy {
	return append([]PlanCacheKeyPolicy(nil), policies...)
}

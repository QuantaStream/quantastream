package qsbridge

// MetadataStoreDomain identifies a qsbridge-adjacent metadata persistence domain.
type MetadataStoreDomain string

const (
	// MetadataStoreDomainCatalog stores schema, table, field, relationship, and function metadata.
	MetadataStoreDomainCatalog MetadataStoreDomain = "catalog"
	// MetadataStoreDomainDictionary stores field-level dictionary labels and encoded ids.
	MetadataStoreDomainDictionary MetadataStoreDomain = "dictionary"
	// MetadataStoreDomainAccess stores users, roles, grants, and policy metadata.
	MetadataStoreDomainAccess MetadataStoreDomain = "access"
	// MetadataStoreDomainSession stores adapter-owned session metadata.
	MetadataStoreDomainSession MetadataStoreDomain = "session"
	// MetadataStoreDomainPrepared stores prepared handles, long-data fragments, and plan-cache metadata.
	MetadataStoreDomainPrepared MetadataStoreDomain = "prepared"
)

// MetadataStoreBackend identifies a persistence backend family without importing it.
type MetadataStoreBackend string

const (
	// MetadataStoreBackendUnspecified means the backend has not been chosen.
	MetadataStoreBackendUnspecified MetadataStoreBackend = ""
	// MetadataStoreBackendConsul identifies Consul or a Consul-like metadata store.
	MetadataStoreBackendConsul MetadataStoreBackend = "consul"
	// MetadataStoreBackendKVStore identifies legacy Quanta KVStore-backed metadata.
	MetadataStoreBackendKVStore MetadataStoreBackend = "kvstore"
	// MetadataStoreBackendAdapter identifies adapter-owned storage outside qsbridge.
	MetadataStoreBackendAdapter MetadataStoreBackend = "adapter"
	// MetadataStoreBackendMemory identifies process-local memory for tests and transient adapters.
	MetadataStoreBackendMemory MetadataStoreBackend = "memory"
)

// MetadataCacheScope identifies where cached metadata may live.
type MetadataCacheScope string

const (
	// MetadataCacheScopeNone means the profile does not require a cache.
	MetadataCacheScopeNone MetadataCacheScope = ""
	// MetadataCacheScopeProcess means cache entries are process-local.
	MetadataCacheScopeProcess MetadataCacheScope = "process"
	// MetadataCacheScopeSession means cache entries are scoped to one client session.
	MetadataCacheScopeSession MetadataCacheScope = "session"
	// MetadataCacheScopeCluster means cache entries may be coordinated across nodes.
	MetadataCacheScopeCluster MetadataCacheScope = "cluster"
)

// MetadataInvalidationMode identifies how stale metadata should be detected.
type MetadataInvalidationMode string

const (
	// MetadataInvalidationNone means no explicit invalidation is required.
	MetadataInvalidationNone MetadataInvalidationMode = ""
	// MetadataInvalidationVersioned means callers should compare metadata generations.
	MetadataInvalidationVersioned MetadataInvalidationMode = "versioned"
	// MetadataInvalidationWatch means adapters should subscribe to backend change notifications.
	MetadataInvalidationWatch MetadataInvalidationMode = "watch"
	// MetadataInvalidationSessionClose means metadata expires with the session lifecycle.
	MetadataInvalidationSessionClose MetadataInvalidationMode = "session_close"
	// MetadataInvalidationExplicit means callers must explicitly invalidate entries.
	MetadataInvalidationExplicit MetadataInvalidationMode = "explicit"
)

// MetadataStoreProfile describes one metadata store boundary.
type MetadataStoreProfile struct {
	Name            string
	Domain          MetadataStoreDomain
	Backend         MetadataStoreBackend
	CacheRequired   bool
	CacheScope      MetadataCacheScope
	Invalidation    MetadataInvalidationMode
	Mutable         bool
	Distributed     bool
	NodeLocalCopy   bool
	AdapterOwned    bool
	RuntimeOwned    bool
	ConsistencyNote string
}

// DefaultMetadataStoreProfiles returns the intended metadata persistence boundaries.
func DefaultMetadataStoreProfiles() []MetadataStoreProfile {
	return cloneMetadataStoreProfiles(defaultMetadataStoreProfiles)
}

var defaultMetadataStoreProfiles = []MetadataStoreProfile{
	{
		Name:            "catalog_metadata",
		Domain:          MetadataStoreDomainCatalog,
		Backend:         MetadataStoreBackendConsul,
		CacheRequired:   true,
		CacheScope:      MetadataCacheScopeProcess,
		Invalidation:    MetadataInvalidationWatch,
		Distributed:     true,
		AdapterOwned:    true,
		ConsistencyNote: "schema, relationship, function, and role metadata should be read through process-local catalog caches",
	},
	{
		Name:            "string_enum_dictionary",
		Domain:          MetadataStoreDomainDictionary,
		Backend:         MetadataStoreBackendKVStore,
		CacheRequired:   true,
		CacheScope:      MetadataCacheScopeProcess,
		Invalidation:    MetadataInvalidationVersioned,
		Mutable:         true,
		Distributed:     true,
		NodeLocalCopy:   true,
		RuntimeOwned:    true,
		ConsistencyNote: "StringEnum labels and ids stay in KVStore while every node keeps a Pogreb copy through dictionary-specific propagation; query proxy cache misses can fan new keys out to nodes, and singleflight guards concurrent proxies from creating duplicate ids for the same label, so dictionary versions and cache invalidation must be explicit",
	},
	{
		Name:            "access_policy",
		Domain:          MetadataStoreDomainAccess,
		Backend:         MetadataStoreBackendAdapter,
		CacheRequired:   true,
		CacheScope:      MetadataCacheScopeProcess,
		Invalidation:    MetadataInvalidationWatch,
		Mutable:         true,
		Distributed:     true,
		AdapterOwned:    true,
		ConsistencyNote: "users, roles, and grants should be adapter-owned and cached before policy checks",
	},
	{
		Name:            "user_role_metadata",
		Domain:          MetadataStoreDomainAccess,
		Backend:         MetadataStoreBackendConsul,
		CacheRequired:   true,
		CacheScope:      MetadataCacheScopeProcess,
		Invalidation:    MetadataInvalidationWatch,
		Mutable:         true,
		Distributed:     true,
		AdapterOwned:    true,
		ConsistencyNote: "user and role metadata changes are infrequent and should be read through cached metadata adapters",
	},
	{
		Name:            "session_registry",
		Domain:          MetadataStoreDomainSession,
		Backend:         MetadataStoreBackendAdapter,
		CacheRequired:   false,
		CacheScope:      MetadataCacheScopeSession,
		Invalidation:    MetadataInvalidationSessionClose,
		Mutable:         true,
		AdapterOwned:    true,
		ConsistencyNote: "protocol adapters own live sessions; qsbridge only previews session metadata transitions",
	},
	{
		Name:            "prepared_plan_cache",
		Domain:          MetadataStoreDomainPrepared,
		Backend:         MetadataStoreBackendMemory,
		CacheRequired:   true,
		CacheScope:      MetadataCacheScopeSession,
		Invalidation:    MetadataInvalidationExplicit,
		Mutable:         true,
		AdapterOwned:    true,
		ConsistencyNote: "prepared handles, long-data fragments, and plan cache entries are scoped by adapter/session metadata",
	},
}

func cloneMetadataStoreProfiles(profiles []MetadataStoreProfile) []MetadataStoreProfile {
	return append([]MetadataStoreProfile(nil), profiles...)
}

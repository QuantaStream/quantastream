package qsinabox

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/server"
	"github.com/QuantaStream/quantastream/shared"
)

// NewLocalConnection builds the client-side connection facade for the mounted
// in-process node services.
func (b StandardLocalBackend) NewLocalConnection() *shared.Conn {
	conn := shared.NewDefaultConnection("inabox-standard")
	conn.ServiceName = "quantastream"
	conn.ServicePort = 0
	conn.Quorum = 1
	conn.Replicas = 1
	conn.IsLocalCluster = true
	conn.LocalNodeServices = b.Services
	return conn
}

// ConfigBaseDir returns the schema catalog directory visible to sessions.
func (b StandardLocalBackend) ConfigBaseDir(config StandardConfig) string {
	config = config.WithDefaults()
	return filepath.Join(config.DataDir, "config")
}

// NewSessionPool builds a session pool over the in-process local backend.
func (b StandardLocalBackend) NewSessionPool(config StandardConfig, tableCache *core.TableCacheStruct, poolSize int) *core.SessionPool {
	if tableCache == nil {
		tableCache = core.NewTableCacheStruct()
	}
	return core.NewSessionPool(tableCache, b.NewLocalConnection(), b.ConfigBaseDir(config), poolSize)
}

// StandardDirectRuntimeMount owns a direct bitmap runtime and its local session
// pool lifecycle.
type StandardDirectRuntimeMount struct {
	Runtime qsruntime.DirectBitmapRuntime
	Pool    *core.SessionPool
}

// Close releases sessions owned by the runtime mount.
func (m *StandardDirectRuntimeMount) Close() {
	if m == nil || m.Pool == nil {
		return
	}
	m.Pool.Shutdown()
}

// NewDirectRuntime builds the first in-process runtime surface for
// inabox-standard. The mounted runtime supports local bitmap reads, native
// projection materialization, file-backed DDL, and the essential insert path.
func (b StandardLocalBackend) NewDirectRuntime(config StandardConfig, tableCache *core.TableCacheStruct, poolSize int) StandardDirectRuntimeMount {
	if tableCache == nil {
		tableCache = core.NewTableCacheStruct()
	}
	pool := b.NewSessionPool(config, tableCache, poolSize)
	sessions := StandardDirectSessionProvider{
		Pool:                                 pool,
		SchemaDir:                            b.ConfigBaseDir(config),
		Conn:                                 b.NewLocalConnection(),
		Direct:                               b.Adapter.BitmapIndex,
		PrimaryKeyResolverFactory:            standardDirectPrimaryKeyResolverFactory(config, tableCache, b.Adapter.BitmapIndex, pool),
		PrimaryKeyAuthorityManifestPublisher: StandardBSIPrimaryKeyAuthorityManifestFilePublisher{Config: config, Source: "standard-session-flush"},
	}
	bsiReader := StandardProjectionBSIReader{
		Pool:       pool,
		TableCache: tableCache,
		Direct:     b.Adapter.BitmapIndex,
	}
	dictionaryIDReader := StandardProjectionDictionaryIDReader{
		Pool:       pool,
		TableCache: tableCache,
	}
	backingStringReader := StandardBackingStringLookupReader{
		Pool:       pool,
		TableCache: tableCache,
	}
	bitmapGroupCountReader := StandardBitmapGroupCountReader{
		TableCache: tableCache,
		Direct:     b.Adapter.BitmapIndex,
		Database:   config.WithDefaults().Database,
	}
	bitmapGroupAggregateReader := StandardBitmapGroupAggregateReader{
		TableCache: tableCache,
		Direct:     b.Adapter.BitmapIndex,
		Database:   config.WithDefaults().Database,
	}
	dictionaryResolver := qsruntime.LegacyTableCacheDictionaryResolver{
		TableCache: tableCache,
		Schema:     config.WithDefaults().Database,
	}
	cachedDictionaryResolver := qsbridge.NewCachedDictionaryResolver(dictionaryResolver)
	queryCatalogProvider := func() qsbridge.QueryCatalogView {
		return standardQueryCatalogViewForCachedTables(config, tableCache)
	}
	queryCatalog := queryCatalogProvider()
	materialization := qsruntime.FallbackProjectionMaterializationKernel{
		Preferred: qsruntime.NativeProjectionMaterializationKernel{
			Reader: qsruntime.NativeProjectionBSIFieldReader{
				TableCache:       tableCache,
				Reader:           bsiReader,
				DictionaryReader: dictionaryIDReader,
			},
			Rehydrator: qsruntime.NativeProjectionCompositeRehydrator{
				Dictionary:     qsruntime.NewNativeProjectionDictionaryLabelRehydrator(queryCatalog, cachedDictionaryResolver),
				BackingStrings: backingStringReader,
			},
		},
	}
	sameRowComparison := qsruntime.LegacyDirectSameRowBSIComparisonKernel{
		TableCache: tableCache,
		Reader:     bsiReader,
		Comparator: StandardSameRowBSIComparator{
			TableCache: tableCache,
			Direct:     b.Adapter.BitmapIndex,
		},
	}
	relationshipProjectionReader := StandardRelationshipVectorProjectionReader{
		Pool:       pool,
		TableCache: tableCache,
		Direct:     b.Adapter.BitmapIndex,
	}
	relationshipSourceKeyReader := StandardRelationshipVectorSourceKeyReader{
		Reader: bsiReader,
	}
	relationshipReverseArtifactReader := StandardRelationshipReverseArtifactCandidateReader{
		TableCache: tableCache,
		Direct:     b.Adapter.BitmapIndex,
	}
	relationshipAggregateReader := StandardRelationshipVectorAggregateReader{
		TableCache: tableCache,
		Direct:     b.Adapter.BitmapIndex,
	}
	reverseArtifacts := qsruntime.NewRelationshipVectorReverseArtifactManager(qsruntime.RelationshipVectorReverseArtifactConfigFromEnv())
	relationshipReader := &qsruntime.LegacyDirectRelationshipVectorReader{
		Backend: qsruntime.LegacyDirectBitIndexRelationshipVectorBackend{
			Sessions:                       sessions,
			TableCache:                     tableCache,
			ProjectionReader:               relationshipProjectionReader,
			SourceKeyReader:                relationshipSourceKeyReader,
			ReverseArtifacts:               reverseArtifacts,
			ReverseArtifactCandidateReader: relationshipReverseArtifactReader,
		},
	}
	physicalTier := qsruntime.DirectPhysicalExecutionTier{
		Sessions:              sessions,
		Adapter:               qsruntime.BitmapQueryResultAdapter{},
		FilterAdapter:         qsruntime.DirectBitmapFilterTreeAdapter{Sessions: sessions, Materialization: materialization, Normalizer: qsruntime.DirectBitmapFilterDomainNormalizationExecutor{Sessions: sessions, Reader: relationshipReader}, QueryCatalog: queryCatalog, QueryCatalogProvider: queryCatalogProvider, DictionaryResolver: cachedDictionaryResolver},
		Materialization:       materialization,
		ProjectionBSIReader:   bsiReader,
		SameRowComparison:     sameRowComparison,
		SiblingDiversity:      relationshipReverseArtifactReader,
		BitmapGroupCounts:     bitmapGroupCountReader,
		BitmapGroupAggregates: bitmapGroupAggregateReader,
		RelationshipReader:    relationshipReader,
		RelationshipJoins: qsruntime.LegacyDirectRelationshipVectorJoinExecutor{
			Sessions:                       sessions,
			TableCache:                     tableCache,
			Materialization:                materialization,
			ProjectionBSIReader:            bsiReader,
			SameRowComparison:              sameRowComparison,
			RelationshipProjectionReader:   relationshipProjectionReader,
			RelationshipSourceKeyReader:    relationshipSourceKeyReader,
			ReverseArtifacts:               reverseArtifacts,
			ReverseArtifactCandidateReader: relationshipReverseArtifactReader,
			RelationshipAggregateReader:    relationshipAggregateReader,
			ApplyRecommendedEdgeOrder:      qsruntime.DefaultApplyRecommendedEdgeOrder,
		},
	}
	return StandardDirectRuntimeMount{
		Pool:    pool,
		Runtime: physicalTier.Runtime(),
	}
}

// StandardDirectSessionProvider borrows table-scoped direct sessions from an
// inabox-standard local session pool.
type StandardDirectSessionProvider struct {
	Pool                                 *core.SessionPool
	SchemaDir                            string
	Conn                                 *shared.Conn
	Direct                               *server.BitmapIndex
	PrimaryKeyResolverFactory            core.SessionPrimaryKeyResolverFactory
	PrimaryKeyAuthorityManifestPublisher StandardBSIPrimaryKeyAuthorityManifestPublisher
}

// BorrowDirectSession returns a direct session handle for the request root table.
func (p StandardDirectSessionProvider) BorrowDirectSession(ctx context.Context, request qsruntime.ExecutionRequest) (qsruntime.DirectSessionHandle, qsbridge.DiagnosticSet, error) {
	if p.Pool == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "inabox-standard session pool is not initialized"),
		}, nil
	}
	table, ok := request.RootIndex()
	if !ok || table == "" {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticUnsupportedSQL, qsbridge.PhasePlan, "direct request has no root table"),
		}, nil
	}
	if standardSchemaMutationNeedsSyntheticHandle(request.Mutation.Kind) && strings.TrimSpace(p.SchemaDir) != "" {
		return StandardDirectSessionHandle{
			Pool:      p.Pool,
			Table:     table,
			Session:   p.syntheticSchemaMutationSession(),
			Query:     qsruntime.LegacyBitmapQueryAdapter{},
			Result:    qsruntime.LegacyBitmapQueryResultAdapter{},
			Synthetic: true,
		}, nil, nil
	}
	session, err := p.Pool.Borrow(table)
	if err != nil {
		return nil, nil, fmt.Errorf("borrow inabox-standard session for %s: %w", table, err)
	}
	if p.PrimaryKeyResolverFactory != nil {
		session.SetPrimaryKeyResolver(p.PrimaryKeyResolverFactory(session))
	}
	return StandardDirectSessionHandle{
		Pool:                                 p.Pool,
		Table:                                table,
		Session:                              session,
		Query:                                qsruntime.LegacyBitmapQueryAdapter{},
		Result:                               qsruntime.LegacyBitmapQueryResultAdapter{},
		PrimaryKeyAuthorityManifestPublisher: p.PrimaryKeyAuthorityManifestPublisher,
	}, nil, nil
}

func standardSchemaMutationNeedsSyntheticHandle(kind qsbridge.MutationKind) bool {
	switch kind {
	case qsbridge.MutationCreateTable, qsbridge.MutationCreateView, qsbridge.MutationDropView:
		return true
	default:
		return false
	}
}

// TimeBucketYearBounds reports observed BSI shard years for local standard mode.
func (p StandardDirectSessionProvider) TimeBucketYearBounds(ctx context.Context, request qsruntime.ExecutionRequest, field qsbridge.FieldRef) (int, int, bool) {
	if p.Direct == nil {
		return 0, 0, false
	}
	table := field.Table.Table
	if table == "" {
		root, ok := request.RootIndex()
		if !ok {
			return 0, 0, false
		}
		table = root
	}
	physical := field.PhysicalName
	if physical == "" {
		physical = field.Name
	}
	return p.Direct.BSIShardYearRange(table, physical)
}

func standardDirectPrimaryKeyResolverFactory(config StandardConfig, tableCache *core.TableCacheStruct, direct *server.BitmapIndex, pool *core.SessionPool) core.SessionPrimaryKeyResolverFactory {
	policy := observeStandardBSIPrimaryKeyAuthorityPolicy(config)
	if policy.BlockMutations {
		return standardBlockedPrimaryKeyResolverFactory(policy.Observation)
	}
	reader := StandardSingleColumnBSIPrimaryKeyReader{
		Pool:       pool,
		TableCache: tableCache,
		Direct:     direct,
	}
	return NewStandardBSIPrimaryKeyResolverFactory(reader)
}

func (p StandardDirectSessionProvider) syntheticSchemaMutationSession() *core.Session {
	return &core.Session{
		BasePath:     strings.TrimSpace(p.SchemaDir),
		BitIndex:     shared.NewBitmapIndex(p.Conn),
		KVStore:      shared.NewKVStore(p.Conn),
		StringIndex:  shared.NewStringSearch(p.Conn, 1000),
		TableBuffers: map[string]*core.TableBuffer{},
		CreatedAt:    time.Now().UTC(),
	}
}

// StandardDirectSessionHandle executes direct bitmap calls through a local
// core.Session without gRPC.
type StandardDirectSessionHandle struct {
	Pool                                 *core.SessionPool
	Table                                string
	Session                              *core.Session
	Query                                qsruntime.LegacyBitmapQueryAdapter
	Result                               qsruntime.LegacyBitmapQueryResultAdapter
	PrimaryKeyAuthorityManifestPublisher StandardBSIPrimaryKeyAuthorityManifestPublisher
	// Synthetic handles schema mutations for tables that are not active yet.
	Synthetic bool
}

// QueryBitmap executes a lowered bitmap request through the local session.
func (h StandardDirectSessionHandle) QueryBitmap(ctx context.Context, request qsruntime.ExecutionRequest) (qsruntime.BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	return h.legacyHandle().QueryBitmap(ctx, request)
}

// QueryBitmapWithCandidateSet evaluates a lowered bitmap request against a
// candidate set when the local bitmap boundary supports found-set pushdown.
func (h StandardDirectSessionHandle) QueryBitmapWithCandidateSet(ctx context.Context, request qsruntime.ExecutionRequest, candidates qsbridge.QuantaCandidateSet) (qsruntime.BitmapQueryResult, qsbridge.DiagnosticSet, bool, error) {
	return h.legacyHandle().QueryBitmapWithCandidateSet(ctx, request, candidates)
}

// QueryBitmapCountOnly executes a lowered bitmap request while preserving only
// cardinality for callers that do not need row identities.
func (h StandardDirectSessionHandle) QueryBitmapCountOnly(ctx context.Context, request qsruntime.ExecutionRequest) (qsruntime.BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	return h.legacyHandle().QueryBitmapCountOnly(ctx, request)
}

// ExecuteMutation dispatches in-process SQL mutations through the local session.
func (h StandardDirectSessionHandle) ExecuteMutation(ctx context.Context, request qsruntime.ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	return h.legacyHandle().ExecuteMutation(ctx, request)
}

// InsertRows writes bound literal rows through the local session.
func (h StandardDirectSessionHandle) InsertRows(ctx context.Context, request qsruntime.ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	return h.legacyHandle().InsertRows(ctx, request)
}

// Release returns the local session to the pool.
func (h StandardDirectSessionHandle) Release(ctx context.Context) qsbridge.DiagnosticSet {
	if h.Synthetic {
		return nil
	}
	if h.Pool == nil || h.Session == nil || h.Table == "" {
		return nil
	}
	profile, err := h.Pool.ReturnWithProfile(h.Table, h.Session)
	if err != nil {
		return qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute,
				fmt.Sprintf("release inabox-standard session for %s: %v", h.Table, err)),
		}
	}
	if h.PrimaryKeyAuthorityManifestPublisher != nil {
		if _, err := h.PrimaryKeyAuthorityManifestPublisher.PublishAfterFlush(profile); err != nil {
			return qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute,
					fmt.Sprintf("publish BSI primary-key authority manifest after %s flush: %v", h.Table, err)),
			}
		}
	}
	return nil
}

func (h StandardDirectSessionHandle) legacyHandle() qsruntime.LegacyQuantaSessionHandle {
	return qsruntime.LegacyQuantaSessionHandle{
		TableName: h.Table,
		Pool:      h.Pool,
		Session:   h.Session,
		Query:     h.Query,
		Result:    h.Result,
		Synthetic: h.Synthetic,
	}
}

func standardQueryCatalogViewForCachedTables(config StandardConfig, tableCache *core.TableCacheStruct) qsbridge.QueryCatalogView {
	queryCatalog, diagnostics := qsruntime.LegacyCatalogViewAdapter{
		Catalog: qsruntime.LegacyTableCacheCatalog{
			TableCache: tableCache,
			Functions:  qsbridge.BuiltinSQLFunctionDefinitions(),
		},
	}.QueryCatalogViewForCachedTables(config.WithDefaults().Database)
	if diagnostics.BlocksNative() {
		return qsbridge.QueryCatalogView{}
	}
	return queryCatalog
}

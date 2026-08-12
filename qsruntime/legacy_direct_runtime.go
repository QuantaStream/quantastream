package qsruntime

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/source"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// LegacyQuantaSourceFactory builds a direct runtime through source.NewQuantaSource.
type LegacyQuantaSourceFactory struct {
	TableCache *core.TableCacheStruct
}

// NewDirectRuntime constructs a direct bitmap runtime backed by the legacy Quanta source.
func (f LegacyQuantaSourceFactory) NewDirectRuntime(ctx context.Context, config DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error) {
	if diagnostics := f.ensureTableCache(); diagnostics.BlocksNative() {
		return nil, diagnostics, nil
	}
	args := config.QuantaSourceArgs()
	quantaSource, err := source.NewQuantaSource(
		f.TableCache,
		args.BaseDir,
		args.ConsulAddress,
		args.ServicePort,
		args.SessionPoolSize,
	)
	if err != nil {
		return nil, nil, err
	}

	runtime, diagnostics := NewLegacyDirectBitmapRuntimeFromSource(quantaSource, f.TableCache, LegacyDirectRuntimeOptions{})
	return runtime, diagnostics, nil
}

// LegacyDirectRuntimeOptions controls compatibility adapter wiring while the direct runtime is split out.
type LegacyDirectRuntimeOptions struct {
	DefaultSchema               string
	SchemaDir                   string
	DisableRecommendedEdgeOrder bool
	DictionaryResolver          qsbridge.DictionaryResolver
	DictionaryInvalidator       RuntimeDictionaryInvalidator
	PrimaryKeyResolverFactory   core.SessionPrimaryKeyResolverFactory
}

// NativeProxyRuntimeLegacyOptions carries adapter-island dependencies needed
// while the native proxy runtime still borrows core sessions through the legacy
// QuantaSource path.
type NativeProxyRuntimeLegacyOptions struct {
	PrimaryKeyResolverFactory core.SessionPrimaryKeyResolverFactory
}

// NewLegacyDirectBitmapRuntimeFromSource builds the direct bitmap runtime around an existing Quanta source.
func NewLegacyDirectBitmapRuntimeFromSource(quantaSource *source.QuantaSource, tableCache *core.TableCacheStruct, options LegacyDirectRuntimeOptions) (DirectBitmapRuntime, qsbridge.DiagnosticSet) {
	if quantaSource == nil || quantaSource.GetSessionPool() == nil {
		return DirectBitmapRuntime{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "direct runtime source is not initialized"),
		}
	}
	if tableCache == nil {
		return DirectBitmapRuntime{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "direct runtime table cache is not initialized"),
		}
	}
	sessions := LegacyQuantaSourceSessionProvider{
		Source:                    quantaSource,
		SchemaDir:                 options.SchemaDir,
		DictionaryInvalidator:     options.DictionaryInvalidator,
		PrimaryKeyResolverFactory: options.PrimaryKeyResolverFactory,
	}
	bsiReader := LegacyDirectProjectionBSIReader{
		Source:     quantaSource,
		TableCache: tableCache,
	}
	dictionaryIDReader := LegacyDirectProjectionDictionaryIDReader{
		Source:     quantaSource,
		TableCache: tableCache,
	}
	backingStringReader := LegacyDirectBackingStringLookupReader{
		Source:     quantaSource,
		TableCache: tableCache,
	}
	dictionaryResolver := LegacyTableCacheDictionaryResolver{
		TableCache:          quantaSource.GetSessionPool().TableCache,
		FallbackTableCaches: []*core.TableCacheStruct{tableCache},
		Schema:              options.DefaultSchema,
	}
	resolver := options.DictionaryResolver
	if resolver == nil {
		resolver = dictionaryResolver
	}
	materialization := FallbackProjectionMaterializationKernel{
		Preferred: NativeProjectionMaterializationKernel{
			Reader: NativeProjectionBSIFieldReader{
				TableCache:       tableCache,
				Reader:           bsiReader,
				DictionaryReader: dictionaryIDReader,
			},
			Rehydrator: NativeProjectionCompositeRehydrator{
				Dictionary:     NewNativeProjectionDictionaryLabelRehydrator(qsbridge.QueryCatalogView{}, resolver),
				BackingStrings: backingStringReader,
			},
		},
	}
	sameRowComparison := LegacyDirectSameRowBSIComparisonKernel{
		Source:     quantaSource,
		TableCache: tableCache,
		Reader:     bsiReader,
		Comparator: LegacyDirectSharedSameRowBSIComparator{
			Source:     quantaSource,
			TableCache: tableCache,
		},
	}
	reverseArtifacts := NewRelationshipVectorReverseArtifactManager(RelationshipVectorReverseArtifactConfigFromEnv())
	relationshipReader := &LegacyDirectRelationshipVectorReader{
		Backend: LegacyDirectBitIndexRelationshipVectorBackend{
			Source:           quantaSource,
			Sessions:         sessions,
			TableCache:       tableCache,
			ReverseArtifacts: reverseArtifacts,
		},
	}
	physicalTier := DirectPhysicalExecutionTier{
		Sessions:            sessions,
		Adapter:             BitmapQueryResultAdapter{},
		FilterAdapter:       LegacyDirectFilterTreeAdapter(sessions, quantaSource, tableCache, nil, materialization, reverseArtifacts, resolver),
		Materialization:     materialization,
		ProjectionBSIReader: bsiReader,
		SameRowComparison:   sameRowComparison,
		RelationshipReader:  relationshipReader,
		RelationshipJoins: LegacyDirectRelationshipVectorJoinExecutor{
			Source:                    quantaSource,
			Sessions:                  sessions,
			TableCache:                tableCache,
			Materialization:           materialization,
			ProjectionBSIReader:       bsiReader,
			SameRowComparison:         sameRowComparison,
			ReverseArtifacts:          reverseArtifacts,
			ApplyRecommendedEdgeOrder: DefaultApplyRecommendedEdgeOrder && !options.DisableRecommendedEdgeOrder,
		},
	}
	return physicalTier.Runtime(), nil
}

func legacyDirectCatalogTableLoader(quantaSource *source.QuantaSource, baseDir string) LegacyTableLoader {
	return func(tableCache *core.TableCacheStruct, name string) (*core.Table, error) {
		if quantaSource == nil || quantaSource.GetConnection() == nil {
			return core.LoadTable(tableCache, baseDir, nil, name, nil)
		}
		session, err := core.OpenSession(tableCache, baseDir, name, false, quantaSource.GetConnection())
		if err != nil {
			return nil, err
		}
		if session != nil {
			defer session.CloseSession()
		}
		return legacyDirectCachedTable(tableCache, name), nil
	}
}

func legacyDirectCachedTable(tableCache *core.TableCacheStruct, name string) *core.Table {
	if tableCache == nil {
		return nil
	}
	tableCache.TableCacheLock.RLock()
	defer tableCache.TableCacheLock.RUnlock()
	for tableName, table := range tableCache.TableCache {
		if strings.EqualFold(tableName, name) || (table != nil && strings.EqualFold(table.Name, name)) {
			return table
		}
	}
	return nil
}

// NewNativeProxyRuntimeFromSource builds a SQL-facing runtime over an existing Quanta source.
func NewNativeProxyRuntimeFromSource(ctx context.Context, quantaSource *source.QuantaSource, tableCache *core.TableCacheStruct, config NativeProxyRuntimeConfig) (NativeProxyRuntime, qsbridge.DiagnosticSet, error) {
	return NewNativeProxyRuntimeFromSourceWithLegacyOptions(ctx, quantaSource, tableCache, config, NativeProxyRuntimeLegacyOptions{})
}

// NewNativeProxyRuntimeFromSourceWithLegacyOptions builds a SQL-facing runtime
// over an existing Quanta source with explicit adapter-island dependencies.
func NewNativeProxyRuntimeFromSourceWithLegacyOptions(ctx context.Context, quantaSource *source.QuantaSource, tableCache *core.TableCacheStruct, config NativeProxyRuntimeConfig, legacyOptions NativeProxyRuntimeLegacyOptions) (NativeProxyRuntime, qsbridge.DiagnosticSet, error) {
	config = config.WithDefaults()
	if quantaSource == nil || quantaSource.GetSessionPool() == nil {
		return NativeProxyRuntime{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "native proxy runtime source is not initialized"),
		}, nil
	}
	if tableCache == nil {
		return NativeProxyRuntime{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(qsbridge.DiagnosticInternalInvariant, qsbridge.PhaseExecute, "native proxy runtime table cache is not initialized"),
		}, nil
	}
	dictionaryResolver := LegacyTableCacheDictionaryResolver{
		TableCache:          quantaSource.GetSessionPool().TableCache,
		FallbackTableCaches: []*core.TableCacheStruct{tableCache},
		Schema:              config.DefaultSchema,
	}
	cachedDictionaryResolver := qsbridge.NewCachedDictionaryResolver(dictionaryResolver)
	runtime, diagnostics, err := SQLRuntimeBuilder{
		Parser: qsbridge.SimpleParserBridge{},
		Lowerer: qsbridge.QuantaIntermediateLowerer{
			Dictionaries: cachedDictionaryResolver,
		},
		DefaultSchema:  config.DefaultSchema,
		CatalogVersion: config.CatalogVersion,
		EnvironmentBuilder: RuntimeEnvironmentBuilder{
			Config:  config.Direct,
			Profile: config.Profile,
			CatalogFactory: LegacyTableCacheCatalogFactory{
				TableCache: tableCache,
				LoadTable:  legacyDirectCatalogTableLoader(quantaSource, config.Direct.BaseDir),
				Functions:  append([]qsbridge.FunctionDefinition(nil), config.Functions...),
			},
			DirectFactory: DirectRuntimeFactoryFunc(func(context.Context, DirectRuntimeConfig) (DirectRuntime, qsbridge.DiagnosticSet, error) {
				runtime, diagnostics := NewLegacyDirectBitmapRuntimeFromSource(quantaSource, tableCache, LegacyDirectRuntimeOptions{
					DefaultSchema:               config.DefaultSchema,
					SchemaDir:                   config.SchemaDir,
					DisableRecommendedEdgeOrder: config.DisableRecommendedEdgeOrder,
					DictionaryResolver:          cachedDictionaryResolver,
					PrimaryKeyResolverFactory:   legacyOptions.PrimaryKeyResolverFactory,
					DictionaryInvalidator: RuntimeDictionaryInvalidator{
						Dictionaries:  cachedDictionaryResolver,
						DefaultSchema: config.DefaultSchema,
					},
				})
				return runtime, diagnostics, nil
			}),
		},
		ContextWrapper:          config.ContextWrapper,
		EnableFilterExpressions: config.EnableFilterExpressions,
	}.Build(ctx)
	if err != nil || diagnostics.BlocksNative() {
		return NativeProxyRuntime{}, diagnostics, err
	}
	return NativeProxyRuntime{Runtime: runtime}, nil, nil
}

func (f LegacyQuantaSourceFactory) ensureTableCache() qsbridge.DiagnosticSet {
	if f.TableCache == nil {
		return qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"legacy quanta source factory has no table cache",
			),
		}
	}
	if f.TableCache.TableCache == nil {
		f.TableCache.TableCache = make(map[string]*core.Table)
	}
	return nil
}

// LegacyQuantaSourceSessionProvider borrows direct sessions from a legacy Quanta source.
type LegacyQuantaSourceSessionProvider struct {
	Source                    *source.QuantaSource
	SchemaDir                 string
	DictionaryInvalidator     RuntimeDictionaryInvalidator
	PrimaryKeyResolverFactory core.SessionPrimaryKeyResolverFactory
}

// BorrowDirectSession borrows a table-scoped session from the source session pool.
func (p LegacyQuantaSourceSessionProvider) BorrowDirectSession(ctx context.Context, request ExecutionRequest) (DirectSessionHandle, qsbridge.DiagnosticSet, error) {
	if p.Source == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"legacy quanta source session provider has no source",
			),
		}, nil
	}
	tableName, ok := request.RootIndex()
	if !ok {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInvalidExecutionOption,
				qsbridge.PhaseExecute,
				"direct execution request has no root index",
			),
		}, nil
	}
	pool := p.Source.GetSessionPool()
	if pool == nil {
		return nil, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"legacy quanta source has no session pool",
			),
		}, nil
	}
	if request.Mutation.Kind == qsbridge.MutationCreateTable && strings.TrimSpace(p.SchemaDir) != "" {
		handle, err := NewLegacySchemaMutationHandle(p.Source, tableName, p.SchemaDir)
		if err != nil {
			return nil, nil, err
		}
		handle.DictionaryInvalidator = p.DictionaryInvalidator
		return handle, nil, nil
	}
	session, err := pool.Borrow(tableName)
	if err != nil {
		return nil, nil, err
	}
	if p.PrimaryKeyResolverFactory != nil {
		session.SetPrimaryKeyResolver(p.PrimaryKeyResolverFactory(session))
	}
	return LegacyQuantaSessionHandle{
		TableName:             tableName,
		Pool:                  pool,
		Session:               session,
		DictionaryInvalidator: p.DictionaryInvalidator,
	}, nil, nil
}

// LegacyQuantaSessionHandle adapts a borrowed core.Session to DirectSessionHandle.
type LegacyQuantaSessionHandle struct {
	TableName             string
	Pool                  *core.SessionPool
	Session               *core.Session
	Query                 LegacyBitmapQueryAdapter
	Result                LegacyBitmapQueryResultAdapter
	Synthetic             bool
	DictionaryInvalidator RuntimeDictionaryInvalidator
}

// QueryBitmap executes a neutral request through the legacy session BitmapIndex.
func (h LegacyQuantaSessionHandle) QueryBitmap(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil || h.Session.BitIndex == nil {
		return BitmapQueryResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"legacy quanta session has no bitmap index",
			),
		}, nil
	}
	request = h.withShardWindow(request)
	if diagnostics := h.Query.ValidateExecutionRequest(request); diagnostics.BlocksNative() {
		return BitmapQueryResult{}, diagnostics, nil
	}
	response, err := h.Session.BitIndex.Query(h.Query.ToBitmapQueryFromRequest(request))
	return h.Result.ToBitmapQueryResult(response), nil, err
}

// QueryBitmapWithCandidateSet evaluates a bitmap query against a precomputed
// row set when the local bitmap boundary can honor found-set pushdown.
func (h LegacyQuantaSessionHandle) QueryBitmapWithCandidateSet(ctx context.Context, request ExecutionRequest, candidates qsbridge.QuantaCandidateSet) (BitmapQueryResult, qsbridge.DiagnosticSet, bool, error) {
	if h.Session == nil || h.Session.BitIndex == nil {
		return BitmapQueryResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"legacy quanta session has no bitmap index",
			),
		}, true, nil
	}
	request = h.withShardWindow(request)
	if diagnostics := h.Query.ValidateExecutionRequest(request); diagnostics.BlocksNative() {
		return BitmapQueryResult{}, diagnostics, true, nil
	}
	rows := make([]uint64, len(candidates.Rownums))
	for i, rownum := range candidates.Rownums {
		rows[i] = uint64(rownum)
	}
	response, handled, err := h.Session.BitIndex.QueryWithFoundSet(ctx, h.Query.ToBitmapQueryFromRequest(request), candidates.Index, rows)
	if !handled {
		return BitmapQueryResult{}, nil, false, nil
	}
	return h.Result.ToBitmapQueryResult(response), nil, true, err
}

// QueryBitmapCountOnly executes a bitmap query when the caller has already
// proven only cardinality is required.
func (h LegacyQuantaSessionHandle) QueryBitmapCountOnly(ctx context.Context, request ExecutionRequest) (BitmapQueryResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil || h.Session.BitIndex == nil {
		return BitmapQueryResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"legacy quanta session has no bitmap index",
			),
		}, nil
	}
	request = h.withShardWindow(request)
	if diagnostics := h.Query.ValidateExecutionRequest(request); diagnostics.BlocksNative() {
		return BitmapQueryResult{}, diagnostics, nil
	}
	response, err := h.Session.BitIndex.Query(h.Query.ToBitmapQueryFromRequest(request))
	return h.Result.ToCountOnlyBitmapQueryResult(response), nil, err
}

func (h LegacyQuantaSessionHandle) withShardWindow(request ExecutionRequest) ExecutionRequest {
	table := h.cachedRootTable(request)
	request = legacyDirectExecutionWithShardWindow(request, table)
	return legacyDirectExecutionWithFullTableScanSeed(request, table)
}

func (h LegacyQuantaSessionHandle) cachedRootTable(request ExecutionRequest) *core.Table {
	tableName := h.TableName
	if root, ok := request.RootIndex(); ok && root != "" {
		tableName = root
	}
	if h.Session != nil {
		if buffer, ok := h.Session.TableBuffers[tableName]; ok && buffer != nil && buffer.Table != nil {
			return buffer.Table
		}
		for name, buffer := range h.Session.TableBuffers {
			if strings.EqualFold(name, tableName) && buffer != nil && buffer.Table != nil {
				return buffer.Table
			}
			if buffer != nil && buffer.Table != nil && strings.EqualFold(buffer.Table.Name, tableName) {
				return buffer.Table
			}
		}
	}
	if h.Pool == nil || h.Pool.TableCache == nil {
		return nil
	}
	h.Pool.TableCache.TableCacheLock.RLock()
	defer h.Pool.TableCache.TableCacheLock.RUnlock()
	table, ok := h.Pool.TableCache.TableCache[tableName]
	if ok {
		return table
	}
	for name, candidate := range h.Pool.TableCache.TableCache {
		if strings.EqualFold(name, tableName) || (candidate != nil && strings.EqualFold(candidate.Name, tableName)) {
			return candidate
		}
	}
	return nil
}

func legacyDirectExecutionWithFullTableScanSeed(request ExecutionRequest, table *core.Table) ExecutionRequest {
	if table == nil || request.HasCandidateSet || len(request.Query.Seeds) > 0 || len(request.Query.Fragments) > 0 || !request.Query.Filter.Empty() {
		return request
	}
	index, _ := request.RootIndex()
	if table.Name != "" {
		index = table.Name
	}
	if index == "" {
		return request
	}
	if timeField := legacyDirectRelationshipTimeQuantumField(table); legacyDirectTableHasPhysicalShardWindow(table) && timeField != "" {
		begin, end := legacyDirectRelationshipFullTimeRangeEncoded(table, timeField)
		request.Query = cloneIntermediateQuery(request.Query)
		request.Query.Seeds = append([]qsbridge.QuantaSeed{{
			Index:       index,
			Field:       timeField,
			Kind:        qsbridge.QuantaSeedTableExistence,
			Begin:       big.NewInt(begin),
			End:         big.NewInt(end),
			ShardWindow: true,
		}}, request.Query.Seeds...)
		request.Query.ProjectionFields = legacyDirectEnsureShardProjectionField(request.Query.ProjectionFields, index, timeField)
		return request
	}
	field := legacyDirectFullTableScanSeedField(table)
	if field == "" {
		return request
	}
	request.Query = cloneIntermediateQuery(request.Query)
	request.Query.Seeds = append([]qsbridge.QuantaSeed{{
		Index: index,
		Field: field,
		Kind:  qsbridge.QuantaSeedTableExistence,
	}}, request.Query.Seeds...)
	return request
}

func legacyDirectFullTableScanSeedField(table *core.Table) string {
	if table == nil {
		return ""
	}
	for _, part := range strings.Split(table.PrimaryKey, "+") {
		if field := strings.TrimSpace(part); field != "" {
			return field
		}
	}
	for _, attribute := range table.Attributes {
		if strings.EqualFold(attribute.FieldName, "rownum") {
			return attribute.FieldName
		}
		if strings.EqualFold(attribute.SourceName, "rownum") {
			return attribute.SourceName
		}
	}
	if table.TimeQuantumField != "" {
		return table.TimeQuantumField
	}
	for _, attribute := range table.Attributes {
		if attribute.FieldName != "" {
			return attribute.FieldName
		}
		if attribute.SourceName != "" {
			return attribute.SourceName
		}
	}
	return ""
}

func legacyDirectExecutionWithShardWindow(request ExecutionRequest, table *core.Table) ExecutionRequest {
	timeField := legacyDirectRelationshipTimeQuantumField(table)
	if timeField == "" || !legacyDirectTableHasPhysicalShardWindow(table) {
		return request
	}
	index, _ := request.RootIndex()
	if table != nil && table.Name != "" {
		index = table.Name
	}
	if index == "" {
		return request
	}

	request.Query = cloneIntermediateQuery(request.Query)
	if legacyDirectRequestHasShardTimePredicate(request, timeField) {
		request.Query.Fragments = legacyDirectMarkShardTimePredicates(request.Query.Fragments, timeField)
		request.Query.ProjectionFields = legacyDirectEnsureShardProjectionField(request.Query.ProjectionFields, index, timeField)
		return request
	}
	if legacyDirectRequestHasShardWindowSeed(request, index, timeField) {
		request.Query.ProjectionFields = legacyDirectEnsureShardProjectionField(request.Query.ProjectionFields, index, timeField)
		return request
	}
	needsSyntheticShardWindow := legacyDirectRequestNeedsSyntheticShardWindow(request, timeField)
	if !needsSyntheticShardWindow && len(request.Query.Fragments) > 0 {
		needsSyntheticShardWindow = true
	}
	if !needsSyntheticShardWindow {
		return request
	}

	request.Query.Seeds = append([]qsbridge.QuantaSeed{{
		Index:       index,
		Field:       timeField,
		Kind:        qsbridge.QuantaSeedTableExistence,
		Begin:       big.NewInt(legacyDirectRelationshipFullTimeRangeBeginMillis),
		End:         big.NewInt(legacyDirectRelationshipFullTimeRangeEndMillis),
		ShardWindow: true,
	}}, request.Query.Seeds...)
	request.Query.ProjectionFields = legacyDirectEnsureShardProjectionField(request.Query.ProjectionFields, index, timeField)
	return request
}

func legacyDirectRequestHasShardWindowSeed(request ExecutionRequest, index, timeField string) bool {
	for _, seed := range request.Query.Seeds {
		if seed.Kind != qsbridge.QuantaSeedTableExistence || !seed.ShardWindow || seed.Begin == nil || seed.End == nil {
			continue
		}
		if !strings.EqualFold(seed.Field, timeField) {
			continue
		}
		if seed.Index == "" || index == "" || strings.EqualFold(seed.Index, index) {
			return true
		}
	}
	return false
}

func legacyDirectRequestNeedsSyntheticShardWindow(request ExecutionRequest, timeField string) bool {
	if request.HasCandidateSet || len(request.Joins) > 0 || len(request.Memberships) > 0 {
		return true
	}
	for _, fragment := range request.Query.Fragments {
		if legacyDirectFragmentFieldMatches(fragment, timeField) || legacyRequestFragmentIsTimeField(request, fragment) {
			return true
		}
	}
	return false
}

func legacyDirectTableHasPhysicalShardWindow(table *core.Table) bool {
	return table != nil && table.TimeQuantumType != "" && legacyDirectRelationshipTimeQuantumField(table) != ""
}

func legacyDirectRequestHasShardTimePredicate(request ExecutionRequest, timeField string) bool {
	for _, fragment := range request.Query.Fragments {
		if legacyDirectShardTimePredicate(fragment, timeField) {
			return true
		}
	}
	return false
}

func legacyDirectMarkShardTimePredicates(fragments []qsbridge.QuantaQueryFragment, timeField string) []qsbridge.QuantaQueryFragment {
	marked := make([]qsbridge.QuantaQueryFragment, len(fragments))
	copy(marked, fragments)
	for i := range marked {
		if legacyDirectShardTimePredicate(marked[i], timeField) {
			marked[i].ShardWindow = true
		}
	}
	return marked
}

func legacyDirectShardTimePredicate(fragment qsbridge.QuantaQueryFragment, timeField string) bool {
	if !legacyDirectFragmentFieldMatches(fragment, timeField) {
		return false
	}
	switch fragment.BSIOp {
	case qsbridge.QuantaBSIOpRange, qsbridge.QuantaBSIOpGE, qsbridge.QuantaBSIOpGT, qsbridge.QuantaBSIOpLE, qsbridge.QuantaBSIOpLT:
		return true
	default:
		return false
	}
}

func legacyDirectFragmentFieldMatches(fragment qsbridge.QuantaQueryFragment, field string) bool {
	if strings.EqualFold(fragment.Field, field) {
		return true
	}
	if dot := strings.LastIndex(fragment.Field, "."); dot >= 0 {
		return strings.EqualFold(fragment.Field[dot+1:], field)
	}
	return false
}

func legacyDirectEnsureShardProjectionField(fields []qsbridge.QuantaProjectionField, index, timeField string) []qsbridge.QuantaProjectionField {
	for _, field := range fields {
		if (strings.EqualFold(field.Index, index) || field.Index == "") && legacyDirectProjectionFieldMatches(field, timeField) && field.Type == qsbridge.DataTypeTime {
			return fields
		}
	}
	shardField := qsbridge.QuantaProjectionField{
		Index:        index,
		Field:        timeField,
		PhysicalName: timeField,
		Type:         qsbridge.DataTypeTime,
	}
	return append([]qsbridge.QuantaProjectionField{shardField}, fields...)
}

func legacyDirectProjectionFieldMatches(field qsbridge.QuantaProjectionField, name string) bool {
	return strings.EqualFold(field.Field, name) || strings.EqualFold(field.PhysicalName, name)
}

// ExecuteMutation dispatches bound native mutation requests through the borrowed legacy session.
func (h LegacyQuantaSessionHandle) ExecuteMutation(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	switch request.Mutation.Kind {
	case qsbridge.MutationInsert:
		return h.InsertRows(ctx, request)
	case qsbridge.MutationUpdate:
		return h.UpdateRows(ctx, request)
	case qsbridge.MutationDelete:
		return h.DeleteRows(ctx, request)
	case qsbridge.MutationTruncate:
		return h.TruncateTable(ctx, request)
	case qsbridge.MutationCreateTable:
		return h.CreateTable(ctx, request)
	case qsbridge.MutationDropTable:
		return h.DropTable(ctx, request)
	default:
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedMutation,
				qsbridge.PhaseExecute,
				"unsupported mutation kind: "+string(request.Mutation.Kind),
			),
		}, nil
	}
}

// TruncateTable clears all bitmap-backed data for the target table through the table operation API.
func (h LegacyQuantaSessionHandle) TruncateTable(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil || h.Session.BitIndex == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"legacy quanta session has no bitmap index",
			),
		}, nil
	}
	if request.Mutation.Kind != qsbridge.MutationTruncate {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedMutation,
				qsbridge.PhaseExecute,
				"TRUNCATE adapter called for non-TRUNCATE mutation",
			),
		}, nil
	}
	tableName := h.TableName
	if request.Mutation.Target.Table != "" {
		tableName = request.Mutation.Target.Table
	}
	if tableName == "" {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInvalidExecutionOption,
				qsbridge.PhaseExecute,
				"truncate mutation has no target table",
			),
		}, nil
	}
	if err := ctx.Err(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if diagnostics, err := h.ensureTruncateChildTablesEmpty(ctx, tableName, request.Mutation.DependentRelationships); err != nil || diagnostics.BlocksNative() {
		return qsbridge.StatementResult{}, diagnostics, err
	}
	if err := h.Session.BitIndex.TableOperation(tableName, "truncate"); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	return qsbridge.StatementResult{
		AffectedRows: 0,
		Status:       fmt.Sprintf("Table %s truncated", tableName),
	}, nil, nil
}

func (h LegacyQuantaSessionHandle) ensureTruncateChildTablesEmpty(ctx context.Context, parentTable string, relationships []qsbridge.RelationshipDefinition) (qsbridge.DiagnosticSet, error) {
	childTables := legacyDirectTruncateChildTables(parentTable, relationships)
	if len(childTables) == 0 {
		return nil, nil
	}
	if h.Pool == nil {
		return qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"truncate dependency check requires a session pool",
			),
		}, nil
	}
	for _, childTable := range childTables {
		count, diagnostics, err := h.truncateChildTableCount(ctx, childTable)
		if err != nil || diagnostics.BlocksNative() {
			return diagnostics, err
		}
		if count > 0 {
			return qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(
					qsbridge.DiagnosticTruncateChildDataExists,
					qsbridge.PhaseExecute,
					fmt.Sprintf("cannot truncate parent table %s while child table %s contains %d row(s); truncate child tables first", parentTable, childTable, count),
				),
			}, nil
		}
	}
	return nil, nil
}

func (h LegacyQuantaSessionHandle) truncateChildTableCount(ctx context.Context, childTable string) (uint64, qsbridge.DiagnosticSet, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	session, err := h.Pool.Borrow(childTable)
	if err != nil {
		return 0, nil, err
	}
	childHandle := LegacyQuantaSessionHandle{
		TableName: childTable,
		Pool:      h.Pool,
		Session:   session,
		Query:     h.Query,
		Result:    h.Result,
	}
	request := NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.SourceIndexes = []string{childTable}
	result, diagnostics, queryErr := childHandle.QueryBitmap(ctx, request)
	releaseDiagnostics := childHandle.Release(ctx)
	diagnostics = append(diagnostics, releaseDiagnostics...)
	if queryErr != nil || diagnostics.BlocksNative() {
		return 0, diagnostics, queryErr
	}
	return result.Count, nil, nil
}

func legacyDirectTruncateChildTables(parentTable string, relationships []qsbridge.RelationshipDefinition) []string {
	seen := make(map[string]struct{})
	tables := make([]string, 0, len(relationships))
	for _, relationship := range relationships {
		if !relationship.ReferencesParentTable(parentTable) {
			continue
		}
		childTable := strings.TrimSpace(relationship.ChildTable())
		if childTable == "" {
			continue
		}
		key := strings.ToLower(childTable)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		tables = append(tables, childTable)
	}
	return tables
}

// InsertRows writes bound literal INSERT rows through the borrowed legacy session.
func (h LegacyQuantaSessionHandle) InsertRows(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"legacy quanta session has no session",
			),
		}, nil
	}
	if request.Mutation.Kind != qsbridge.MutationInsert {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedSQL,
				qsbridge.PhaseExecute,
				"runtime only supports INSERT mutations in this slice",
			),
		}, nil
	}
	tableName := h.TableName
	if request.Mutation.Target.Table != "" {
		tableName = request.Mutation.Target.Table
	}
	if tableName == "" {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInvalidExecutionOption,
				qsbridge.PhaseExecute,
				"insert mutation has no target table",
			),
		}, nil
	}
	var affected uint64
	var lastInsertID uint64
	table := h.cachedRootTable(request)
	for _, row := range request.Mutation.Rows {
		rowMap, providedRownum, diagnostics, ok := legacyDirectInsertRowMap(table, request.Mutation.Columns, row)
		if !ok {
			return qsbridge.StatementResult{}, diagnostics, nil
		}
		if err := ctx.Err(); err != nil {
			return qsbridge.StatementResult{}, nil, err
		}
		if err := h.Session.PutRow(tableName, rowMap, providedRownum, false, false); err != nil {
			return qsbridge.StatementResult{}, nil, err
		}
		current, err := h.Session.CurrentColumnID(tableName)
		if err != nil {
			return qsbridge.StatementResult{}, nil, err
		}
		lastInsertID = current
		affected++
	}
	return qsbridge.StatementResult{
		AffectedRows: affected,
		LastInsertID: lastInsertID,
		Status:       fmt.Sprintf("Records: %d", affected),
	}, nil, nil
}

func legacyDirectInsertRowMap(table *core.Table, columns []qsbridge.FieldRef, row qsbridge.MutationRow) (map[string]interface{}, uint64, qsbridge.DiagnosticSet, bool) {
	if len(columns) != len(row.Values) {
		return nil, 0, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticParserBoundary,
				qsbridge.PhaseExecute,
				"insert row value count does not match target column count",
			),
		}, false
	}
	rowMap := make(map[string]interface{}, len(columns))
	var providedRownum uint64
	for i, column := range columns {
		value, diagnostics, ok := legacyDirectMutationLiteralValue(column, row.Values[i])
		if !ok {
			return nil, 0, diagnostics, false
		}
		columnName := directBitmapFieldPhysicalName(column)
		if columnName == "" {
			columnName = column.Name
		}
		if value == nil {
			if legacyDirectInsertUsesExplicitEmptyDefaultTime(table, columnName) {
				rowMap[columnName] = legacyDirectExplicitEmptyTimeSentinel
			}
			continue
		}
		if legacyDirectInsertUsesCatalogDefault(table, columnName, value) {
			continue
		}
		rowMap[columnName] = value
	}
	return rowMap, providedRownum, nil, true
}

const legacyDirectExplicitEmptyTimeSentinel = "1900-01-01T00:00:00Z"

func legacyDirectInsertUsesExplicitEmptyDefaultTime(table *core.Table, columnName string) bool {
	attr := legacyDirectInsertAttribute(table, columnName)
	if attr == nil || strings.TrimSpace(attr.DefaultValue) == "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(attr.Type)) {
	case "date", "datetime", "time":
		return true
	default:
		return false
	}
}

func legacyDirectInsertUsesCatalogDefault(table *core.Table, columnName string, value interface{}) bool {
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) != "" {
		return false
	}
	attr := legacyDirectInsertAttribute(table, columnName)
	return attr != nil && strings.TrimSpace(attr.DefaultValue) != ""
}

func legacyDirectInsertAttribute(table *core.Table, columnName string) *core.Attribute {
	if table == nil || columnName == "" {
		return nil
	}
	if attr, err := table.GetAttribute(columnName); err == nil && attr != nil {
		return attr
	}
	if table.AttributeNameMap != nil {
		for name, attr := range table.AttributeNameMap {
			if strings.EqualFold(name, columnName) {
				return attr
			}
		}
	}
	for i := range table.Attributes {
		attr := &table.Attributes[i]
		if strings.EqualFold(attr.FieldName, columnName) || strings.EqualFold(attr.SourceName, columnName) {
			return attr
		}
	}
	return nil
}

func legacyDirectMutationLiteralValue(field qsbridge.FieldRef, expr qsbridge.Expr) (interface{}, qsbridge.DiagnosticSet, bool) {
	literal, diagnostics, ok := legacyDirectInsertLiteralValue(expr)
	if !ok {
		return nil, diagnostics, false
	}
	return legacyDirectNormalizeMultiplicityValue(field, literal), nil, true
}

func legacyDirectNormalizeMultiplicityValue(field qsbridge.FieldRef, literal interface{}) interface{} {
	if !field.Encoding.IsSetValued() {
		return literal
	}
	text, ok := literal.(string)
	if !ok {
		return literal
	}
	if text == "" {
		return literal
	}
	parts := strings.Split(text, ";")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		values = append(values, part)
	}
	if len(values) == 0 {
		return literal
	}
	return values
}

func legacyDirectInsertLiteralValue(expr qsbridge.Expr) (interface{}, qsbridge.DiagnosticSet, bool) {
	switch value := expr.(type) {
	case qsbridge.LiteralExpr:
		return value.Value, nil, true
	case *qsbridge.LiteralExpr:
		if value == nil {
			break
		}
		return value.Value, nil, true
	}
	return nil, qsbridge.DiagnosticSet{
		qsbridge.ErrorDiagnostic(
			qsbridge.DiagnosticUnsupportedSQL,
			qsbridge.PhaseExecute,
			"INSERT only supports literal row values in this slice",
		),
	}, false
}

// UpdateRows applies literal UPDATE assignments to the rownums selected by the lowered predicate request.
func (h LegacyQuantaSessionHandle) UpdateRows(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"legacy quanta session has no session",
			),
		}, nil
	}
	if request.Mutation.Kind != qsbridge.MutationUpdate {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedMutation,
				qsbridge.PhaseExecute,
				"UPDATE adapter called for non-UPDATE mutation",
			),
		}, nil
	}
	if len(request.Mutation.Predicates) == 0 {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticMutationMissingPredicate,
				qsbridge.PhaseExecute,
				"update requires a predicate before runtime execution",
			),
		}, nil
	}
	valueMap, diagnostics, ok := legacyDirectUpdateValueMap(request.Mutation.Assignments)
	if !ok {
		return qsbridge.StatementResult{}, diagnostics, nil
	}
	bitmapResult, queryDiagnostics, err := h.QueryBitmap(ctx, request)
	if err != nil || queryDiagnostics.BlocksNative() {
		return qsbridge.StatementResult{}, queryDiagnostics, err
	}
	tableName := h.TableName
	if request.Mutation.Target.Table != "" {
		tableName = request.Mutation.Target.Table
	}
	if tableName == "" {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInvalidExecutionOption,
				qsbridge.PhaseExecute,
				"update mutation has no target table",
			),
		}, nil
	}
	for _, rownum := range bitmapResult.Rownums {
		if err := ctx.Err(); err != nil {
			return qsbridge.StatementResult{}, nil, err
		}
		if err := h.Session.UpdateRow(tableName, uint64(rownum), valueMap, h.updatePartitionTime(request, rownum)); err != nil {
			return qsbridge.StatementResult{}, nil, err
		}
	}
	return qsbridge.StatementResult{
		AffectedRows: uint64(len(bitmapResult.Rownums)),
		Status:       fmt.Sprintf("Rows matched: %d", len(bitmapResult.Rownums)),
	}, nil, nil
}

// DeleteRows clears the rownums selected by the lowered predicate request.
func (h LegacyQuantaSessionHandle) DeleteRows(ctx context.Context, request ExecutionRequest) (qsbridge.StatementResult, qsbridge.DiagnosticSet, error) {
	if h.Session == nil || h.Session.BitIndex == nil {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"legacy quanta session has no bitmap index",
			),
		}, nil
	}
	if request.Mutation.Kind != qsbridge.MutationDelete {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticUnsupportedMutation,
				qsbridge.PhaseExecute,
				"DELETE adapter called for non-DELETE mutation",
			),
		}, nil
	}
	if len(request.Mutation.Predicates) == 0 {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticMutationMissingPredicate,
				qsbridge.PhaseExecute,
				"delete requires a predicate before runtime execution",
			),
		}, nil
	}
	bitmapResult, queryDiagnostics, err := h.QueryBitmap(ctx, request)
	if err != nil || queryDiagnostics.BlocksNative() {
		return qsbridge.StatementResult{}, queryDiagnostics, err
	}
	tableName := h.TableName
	if request.Mutation.Target.Table != "" {
		tableName = request.Mutation.Target.Table
	}
	if tableName == "" {
		return qsbridge.StatementResult{}, qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInvalidExecutionOption,
				qsbridge.PhaseExecute,
				"delete mutation has no target table",
			),
		}, nil
	}
	fromTime, toTime := h.mutationTimeWindow(request)
	if err := h.Session.BitIndex.BulkClear(tableName, fromTime, toTime, legacyDirectMutationBitmap(bitmapResult.Rownums)); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	if err := h.Session.Flush(); err != nil {
		return qsbridge.StatementResult{}, nil, err
	}
	return qsbridge.StatementResult{
		AffectedRows: uint64(len(bitmapResult.Rownums)),
		Status:       fmt.Sprintf("Rows deleted: %d", len(bitmapResult.Rownums)),
	}, nil, nil
}

func (h LegacyQuantaSessionHandle) mutationTimeWindow(request ExecutionRequest) (string, string) {
	fromTime := "1970-01-01T00"
	toTime := time.Now().AddDate(0, 0, 1).Format(legacyDirectYMDHTimeFmt)
	for _, fragment := range request.Query.Fragments {
		if fragment.BSIOp != qsbridge.QuantaBSIOpRange || fragment.Begin == nil || fragment.End == nil {
			continue
		}
		if !legacyRequestFragmentIsTimeField(request, fragment) {
			continue
		}
		fromTime = time.Unix(0, fragment.Begin.Int64()*int64(time.Millisecond)).Format(legacyDirectYMDHTimeFmt)
		toTime = time.Unix(0, fragment.End.Int64()*int64(time.Millisecond)).Format(legacyDirectYMDHTimeFmt)
		break
	}
	return fromTime, toTime
}

func legacyDirectMutationBitmap(rownums []qsbridge.QuantaRownum) *roaring64.Bitmap {
	bitmap := roaring64.NewBitmap()
	for _, rownum := range rownums {
		bitmap.Add(uint64(rownum))
	}
	return bitmap
}

func legacyDirectUpdateValueMap(assignments []qsbridge.MutationAssignment) (map[string]*qsbridge.ResultCell, qsbridge.DiagnosticSet, bool) {
	valueMap := make(map[string]*qsbridge.ResultCell, len(assignments))
	for _, assignment := range assignments {
		literal, diagnostics, ok := legacyDirectMutationLiteralValue(assignment.Field, assignment.Value)
		if !ok {
			return nil, diagnostics, false
		}
		fieldName := directBitmapFieldPhysicalName(assignment.Field)
		if fieldName == "" {
			fieldName = assignment.Field.Name
		}
		if fieldName == "" {
			return nil, qsbridge.DiagnosticSet{
				qsbridge.ErrorDiagnostic(
					qsbridge.DiagnosticInvalidExecutionOption,
					qsbridge.PhaseExecute,
					"update assignment has no target field",
				),
			}, false
		}
		//valueMap[fieldName] = &rel.ValueColumn{Value: value.NewValue(literal)}
		valueMap[fieldName] = &qsbridge.ResultCell{Value: literal}
	}
	return valueMap, nil, true
}

const (
	legacyDirectYMDHTimeFmt = "2006-01-02T15"
	legacyDirectYMDTimeFmt  = "2006-01-02"
)

func (h LegacyQuantaSessionHandle) updatePartitionTime(request ExecutionRequest, rownum qsbridge.QuantaRownum) time.Time {
	table := h.cachedRootTable(request)
	if table == nil || table.TimeQuantumType == "" {
		return time.Unix(0, 0)
	}
	for _, fragment := range request.Query.Fragments {
		if !fragment.ShardWindow || fragment.Begin == nil {
			continue
		}
		if !legacyRequestFragmentIsTimeField(request, fragment) {
			continue
		}
		partition := time.UnixMilli(legacyBitmapQueryEpochValueMillis(fragment.Begin.Int64()))
		return legacyDirectTruncatePartition(partition, table.TimeQuantumType)
	}
	timeFmt := legacyDirectYMDHTimeFmt
	if table.TimeQuantumType == "YMD" {
		timeFmt = legacyDirectYMDTimeFmt
	}
	partition := time.Unix(0, int64(rownum))
	partStr := partition.Format(timeFmt)
	parsed, err := time.Parse(timeFmt, partStr)
	if err != nil {
		return partition
	}
	return parsed
}

func legacyDirectTruncatePartition(partition time.Time, quantum string) time.Time {
	partition = partition.UTC()
	switch quantum {
	case "YMD":
		return time.Date(partition.Year(), partition.Month(), partition.Day(), 0, 0, 0, 0, time.UTC)
	case "YMDH":
		return time.Date(partition.Year(), partition.Month(), partition.Day(), partition.Hour(), 0, 0, 0, time.UTC)
	default:
		return partition
	}
}

// Release returns the borrowed session to the legacy session pool.
func (h LegacyQuantaSessionHandle) Release(ctx context.Context) qsbridge.DiagnosticSet {
	if h.Synthetic {
		return nil
	}
	if h.Pool == nil || h.Session == nil {
		return qsbridge.DiagnosticSet{
			qsbridge.ErrorDiagnostic(
				qsbridge.DiagnosticInternalInvariant,
				qsbridge.PhaseExecute,
				"legacy quanta session handle cannot release without pool and session",
			),
		}
	}
	h.Pool.Return(h.TableName, h.Session)
	return nil
}

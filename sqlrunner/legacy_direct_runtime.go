package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/QuantaStream/quantastream/source"
	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
	"github.com/QuantaStream/quantastream/test"
)

type legacyDirectHarnessState struct {
	cfg     runnerConfig
	tables  []string
	config  qsruntime.DirectRuntimeConfig
	runtime *runtimeRoadmapEngine
}

func buildLegacyDirectHarness(suite *roadmap.Suite, cfg runnerConfig) (runnerHarness, error) {
	servicePort, err := legacyDirectServicePort(cfg.Port)
	if err != nil {
		return runnerHarness{}, err
	}
	tables := legacyDirectSuiteTables(suite)
	if err := legacyDirectEnsureConfigBackedTables(tables); err != nil {
		return runnerHarness{}, err
	}
	state := &legacyDirectHarnessState{
		cfg:     cfg,
		tables:  tables,
		config:  qsruntime.NewDirectRuntimeConfig("", cfg.Consul, servicePort, 1),
		runtime: &runtimeRoadmapEngine{},
	}
	if cfg.Verbose {
		state.runtime.Logf = log.Printf
	}
	if err := state.rebuild(context.Background()); err != nil {
		return runnerHarness{}, err
	}
	return runnerHarness{
		Runner: roadmap.Runner{
			Engine:     state.runtime,
			Admin:      state.admin,
			Verbose:    cfg.Verbose,
			DumpActual: cfg.DumpActual,
			Logf:       log.Printf,
		},
	}, nil
}

func (s *legacyDirectHarnessState) admin(ctx context.Context, command string) error {
	if err := legacyDirectExecuteAdmin(command); err != nil {
		return err
	}
	if !legacyDirectAdminChangesTable(command) {
		return nil
	}
	return s.rebuild(ctx)
}

func (s *legacyDirectHarnessState) rebuild(ctx context.Context) error {
	catalogTableCache := core.NewTableCacheStruct()
	runtimeTableCache := core.NewTableCacheStruct()
	quantaSource, err := source.NewQuantaSource(
		runtimeTableCache,
		s.config.BaseDir,
		s.config.ConsulAddress,
		s.config.ServicePort,
		s.config.SessionPoolSize,
	)
	if err != nil {
		return err
	}
	if err := preloadLegacyDirectTables(ctx, catalogTableCache, quantaSource, s.tables); err != nil {
		return err
	}
	runtime, diagnostics, err := legacyDirectBuildSQLRuntime(ctx, s.cfg, s.config, catalogTableCache, quantaSource)
	if err != nil {
		return err
	}
	if diagnostics.BlocksNative() {
		return diagnosticsError(diagnostics)
	}
	s.runtime.Runtime = runtime
	return nil
}

func legacyDirectBuildSQLRuntime(ctx context.Context, cfg runnerConfig, config qsruntime.DirectRuntimeConfig, catalogTableCache *core.TableCacheStruct, quantaSource *source.QuantaSource) (qsruntime.SQLRuntime, qsbridge.DiagnosticSet, error) {
	return qsruntime.SQLRuntimeBuilder{
		Parser: qsbridge.SimpleParserBridge{},
		Lowerer: qsbridge.QuantaIntermediateLowerer{Dictionaries: qsruntime.LegacyTableCacheDictionaryResolver{
			TableCache:          quantaSource.GetSessionPool().TableCache,
			FallbackTableCaches: []*core.TableCacheStruct{catalogTableCache},
			Schema:              legacyDirectDefaultSchema(cfg.Database),
		}},
		DefaultSchema:  legacyDirectDefaultSchema(cfg.Database),
		CatalogVersion: qsbridge.CatalogVersion("sqlrunner-legacy-direct"),
		EnvironmentBuilder: qsruntime.RuntimeEnvironmentBuilder{
			Config:  config,
			Profile: qsruntime.LegacyDirectRuntimeProfile(),
			CatalogFactory: qsruntime.LegacyTableCacheCatalogFactory{
				TableCache: catalogTableCache,
				Functions:  legacyDirectSQLFunctions(),
			},
			DirectFactory: qsruntime.DirectRuntimeFactoryFunc(func(context.Context, qsruntime.DirectRuntimeConfig) (qsruntime.DirectRuntime, qsbridge.DiagnosticSet, error) {
				sessions := qsruntime.LegacyQuantaSourceSessionProvider{Source: quantaSource}
				bsiReader := qsruntime.LegacyDirectProjectionBSIReader{
					Source:     quantaSource,
					TableCache: catalogTableCache,
				}
				dictionaryIDReader := qsruntime.LegacyDirectProjectionDictionaryIDReader{
					Source:     quantaSource,
					TableCache: catalogTableCache,
				}
				backingStringReader := qsruntime.LegacyDirectBackingStringLookupReader{
					Source:     quantaSource,
					TableCache: catalogTableCache,
				}
				dictionaryResolver := qsruntime.LegacyTableCacheDictionaryResolver{
					TableCache:          quantaSource.GetSessionPool().TableCache,
					FallbackTableCaches: []*core.TableCacheStruct{catalogTableCache},
					Schema:              legacyDirectDefaultSchema(cfg.Database),
				}
				materialization := qsruntime.FallbackProjectionMaterializationKernel{
					Preferred: qsruntime.NativeProjectionMaterializationKernel{
						Reader: qsruntime.NativeProjectionBSIFieldReader{
							TableCache:       catalogTableCache,
							Reader:           bsiReader,
							DictionaryReader: dictionaryIDReader,
						},
						Rehydrator: qsruntime.NativeProjectionCompositeRehydrator{
							Dictionary:     qsruntime.NativeProjectionDictionaryLabelRehydrator{Resolver: dictionaryResolver},
							BackingStrings: backingStringReader,
						},
					},
				}
				sameRowComparison := qsruntime.LegacyDirectSameRowBSIComparisonKernel{
					Source:     quantaSource,
					TableCache: catalogTableCache,
					Reader:     bsiReader,
				}
				relationshipReader := &qsruntime.LegacyDirectRelationshipVectorReader{
					Backend: qsruntime.LegacyDirectBitIndexRelationshipVectorBackend{
						Source:     quantaSource,
						TableCache: catalogTableCache,
					},
				}
				return qsruntime.DirectBitmapRuntime{
					Sessions:           sessions,
					Adapter:            qsruntime.BitmapQueryResultAdapter{},
					FilterAdapter:      qsruntime.LegacyDirectFilterTreeAdapter(sessions, quantaSource, catalogTableCache, nil, materialization),
					Materialization:    materialization,
					SameRowComparison:  sameRowComparison,
					RelationshipReader: relationshipReader,
					RelationshipJoins: qsruntime.LegacyDirectRelationshipVectorJoinExecutor{
						Source:                    quantaSource,
						TableCache:                catalogTableCache,
						Materialization:           materialization,
						SameRowComparison:         sameRowComparison,
						ApplyRecommendedEdgeOrder: os.Getenv("QUANTA_LEGACY_DIRECT_APPLY_EDGE_ORDER") == "1",
					},
				}, nil, nil
			}),
		},
		EnableFilterExpressions: true,
	}.Build(ctx)
}

func preloadLegacyDirectTables(ctx context.Context, tableCache *core.TableCacheStruct, quantaSource *source.QuantaSource, tables []string) error {
	if tableCache == nil {
		return fmt.Errorf("legacy direct table cache is not initialized")
	}
	if quantaSource == nil || quantaSource.GetConnection() == nil || quantaSource.GetSessionPool() == nil {
		return fmt.Errorf("legacy direct source is not initialized")
	}
	kvStore := shared.NewKVStore(quantaSource.GetConnection())
	for _, table := range tables {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := core.LoadTable(tableCache, "", kvStore, table, quantaSource.GetConnection().Consul); err != nil {
			if legacyDirectMissingTablePreloadError(table, err) {
				continue
			}
			return fmt.Errorf("preload legacy table %s: %w", table, err)
		}
	}
	return nil
}

func legacyDirectMissingTablePreloadError(table string, err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	table = strings.ToLower(strings.TrimSpace(table))
	return table != "" &&
		strings.Contains(message, "table "+table+" not found") &&
		strings.Contains(message, "unmarshalconsul")
}

func legacyDirectDictionaryResolver(tableCache *core.TableCacheStruct, schema string) qsbridge.MemoryDictionaryResolver {
	resolver := qsbridge.MemoryDictionaryResolver{}
	if tableCache == nil {
		return resolver
	}
	tableCache.TableCacheLock.RLock()
	defer tableCache.TableCacheLock.RUnlock()
	for _, table := range tableCache.TableCache {
		if table == nil {
			continue
		}
		for _, attribute := range table.Attributes {
			encoding := qsbridge.LegacyEncodingProfile(attribute.MappingStrategy, qsbridge.LegacyEncodingOptions{
				NonExclusive: attribute.NonExclusive,
				Searchable:   attribute.Searchable,
				Scale:        attribute.Scale,
			})
			if encoding.Kind != qsbridge.EncodingStringEnum {
				continue
			}
			fieldName := attribute.FieldName
			if fieldName == "" {
				fieldName = attribute.SourceName
			}
			if fieldName == "" {
				continue
			}
			ref := qsbridge.DictionaryRef{Schema: schema, Table: table.Name, Field: fieldName}
			resolver.Dictionaries = append(resolver.Dictionaries, qsbridge.DictionaryDefinition{
				Ref:         ref,
				Version:     qsbridge.DictionaryVersion("legacy-table-cache"),
				Cardinality: uint64(len(attribute.Values)),
				UpdateMode:  qsbridge.DictionaryUpdateAppendOnly,
				Consistency: qsbridge.DictionaryConsistencyVersionedDistributed,
				Capabilities: qsbridge.DictionaryCapabilities{
					qsbridge.DictionaryCapabilityStableIDs,
					qsbridge.DictionaryCapabilityPrefixMatch,
					qsbridge.DictionaryCapabilityMutable,
				},
			})
			for _, value := range attribute.Values {
				resolver.Entries = append(resolver.Entries, qsbridge.DictionaryEntry{
					Ref:     ref,
					Label:   fmt.Sprint(value.Value),
					ID:      qsbridge.StringEnumID(value.RowID),
					Version: qsbridge.DictionaryVersion("legacy-table-cache"),
				})
			}
		}
	}
	return resolver
}

func legacyDirectSuiteTables(suite *roadmap.Suite) []string {
	if suite == nil {
		return nil
	}
	parser := qsbridge.SimpleParserBridge{}
	seen := make(map[string]struct{})
	rememberTable := func(table qsbridge.UnboundTable) {
		if table.Name != "" {
			seen[strings.ToLower(table.Name)] = struct{}{}
		}
	}
	for _, test := range suite.Tests {
		if test.Status == roadmap.CaseSkip {
			continue
		}
		if test.Kind == "admin" {
			if table := legacyDirectAdminCreateTable(test.SQL); table != "" {
				seen[strings.ToLower(table)] = struct{}{}
			}
			continue
		}
		statement, diagnostics := parser.Parse(test.SQL)
		if diagnostics.BlocksNative() {
			for _, table := range legacyDirectRawSQLTables(test.SQL) {
				seen[strings.ToLower(table)] = struct{}{}
			}
			continue
		}
		switch statement.Kind {
		case qsbridge.QueryKindSelect:
			for _, table := range statement.Select.Tables {
				rememberTable(table)
			}
			for _, membership := range statement.Select.Memberships {
				rememberTable(membership.RightTable)
			}
		case qsbridge.QueryKindInsert:
			rememberTable(statement.Insert.Table)
		case qsbridge.QueryKindUpdate:
			rememberTable(statement.Update.Table)
		case qsbridge.QueryKindDelete:
			rememberTable(statement.Delete.Table)
		}
	}
	tables := make([]string, 0, len(seen))
	for table := range seen {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

func legacyDirectRawSQLTables(sql string) []string {
	fields := strings.Fields(sql)
	tables := make([]string, 0)
	seen := make(map[string]struct{})
	for i := 0; i+1 < len(fields); i++ {
		keyword := strings.ToLower(strings.Trim(fields[i], "`\"'(),;"))
		if keyword != "from" && keyword != "join" {
			continue
		}
		table := strings.Trim(fields[i+1], "`\"'(),;")
		if table == "" || strings.EqualFold(table, "select") {
			continue
		}
		if dot := strings.LastIndex(table, "."); dot >= 0 {
			table = table[dot+1:]
		}
		key := strings.ToLower(table)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

func legacyDirectAdminCreateTable(sql string) string {
	fields := strings.Fields(sql)
	if len(fields) < 2 {
		return ""
	}
	if strings.EqualFold(fields[0], "create") {
		return strings.Trim(fields[1], "`")
	}
	return ""
}

func legacyDirectAdminChangesTable(sql string) bool {
	fields := strings.Fields(sql)
	if len(fields) < 2 {
		return false
	}
	switch strings.ToLower(strings.Trim(fields[0], "`")) {
	case "create", "drop", "truncate":
		return true
	default:
		return false
	}
}

func legacyDirectEnsureConfigBackedTables(tables []string) error {
	ordered, err := legacyDirectConfigBackedTablesInDependencyOrder(tables)
	if err != nil {
		return err
	}
	for _, table := range ordered {
		if !legacyDirectHasConfigSchema(table) {
			continue
		}
		if err := legacyDirectExecuteAdmin("create " + table); err != nil {
			return fmt.Errorf("bootstrap legacy-direct table %s: %w", table, err)
		}
	}
	return nil
}

func legacyDirectConfigBackedTablesInDependencyOrder(tables []string) ([]string, error) {
	ordered := make([]string, 0, len(tables))
	visiting := map[string]bool{}
	visited := map[string]bool{}
	var visit func(string) error
	visit = func(table string) error {
		table = strings.TrimSpace(table)
		key := strings.ToLower(table)
		if key == "" || !legacyDirectHasConfigSchema(table) || visited[key] {
			return nil
		}
		if visiting[key] {
			return fmt.Errorf("cycle in config-backed table dependencies at %s", table)
		}
		visiting[key] = true
		dependencies, err := legacyDirectConfigSchemaForeignKeys(table)
		if err != nil {
			return err
		}
		for _, dependency := range dependencies {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		visiting[key] = false
		visited[key] = true
		ordered = append(ordered, table)
		return nil
	}
	for _, table := range tables {
		if err := visit(table); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

func legacyDirectHasConfigSchema(table string) bool {
	_, ok := legacyDirectConfigSchemaPath(table)
	return ok
}

func legacyDirectConfigSchemaPath(table string) (string, bool) {
	if table == "" {
		return "", false
	}
	for _, path := range []string{
		"config/" + table + "/schema.yaml",
		"sqlrunner/config/" + table + "/schema.yaml",
	} {
		if _, err := os.Stat(path); err == nil {
			return path, true
		}
	}
	return "", false
}

func legacyDirectConfigSchemaForeignKeys(table string) ([]string, error) {
	path, ok := legacyDirectConfigSchemaPath(table)
	if !ok {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	keys := make([]string, 0)
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "foreignKey:") {
			continue
		}
		key := strings.TrimSpace(strings.TrimPrefix(trimmed, "foreignKey:"))
		key = strings.Trim(key, `"'`)
		if key == "" || seen[strings.ToLower(key)] {
			continue
		}
		seen[strings.ToLower(key)] = true
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func legacyDirectExecuteAdmin(command string) error {
	workingDirectory, err := os.Getwd()
	if err != nil {
		return err
	}
	if _, err := os.Stat("config"); os.IsNotExist(err) {
		if _, sqlrunnerErr := os.Stat("sqlrunner/config"); sqlrunnerErr == nil {
			if err := os.Chdir("sqlrunner"); err != nil {
				return err
			}
			defer func() {
				if err := os.Chdir(workingDirectory); err != nil {
					log.Printf("legacy-direct admin restore working directory failed: %v", err)
				}
			}()
		}
	}
	return test.ExecuteAdminCommandAndWait(command)
}

func legacyDirectServicePort(port string) (int, error) {
	port = strings.TrimSpace(port)
	if port == "" {
		return qsruntime.DefaultDirectServicePort, nil
	}
	servicePort, err := strconv.Atoi(port)
	if err != nil || servicePort < 0 {
		return 0, fmt.Errorf("legacy-direct port must be a non-negative integer: %q", port)
	}
	return servicePort, nil
}

func legacyDirectDefaultSchema(schema string) string {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		return "quanta"
	}
	return schema
}

func legacyDirectSQLFunctions() []qsbridge.FunctionDefinition {
	return qsbridge.BuiltinSQLFunctionDefinitions()
}

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
	"github.com/QuantaStream/quantastream/qsinabox"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/QuantaStream/quantastream/source"
	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

type inaboxDirectHarnessState struct {
	cfg     runnerConfig
	tables  []string
	config  qsruntime.DirectRuntimeConfig
	runtime *runtimeRoadmapEngine
}

func buildInaboxDirectHarness(suite *roadmap.Suite, cfg runnerConfig) (runnerHarness, error) {
	servicePort, err := inaboxDirectServicePort(cfg.Port)
	if err != nil {
		return runnerHarness{}, err
	}
	tables := inaboxDirectSuiteTables(suite)
	config := qsruntime.NewDirectRuntimeConfig("", cfg.Consul, servicePort, 0)
	if err := inaboxDirectEnsureConfigBackedTables(context.Background(), tables, config, inaboxDirectDefaultSchema(cfg.Database)); err != nil {
		return runnerHarness{}, err
	}
	state := &inaboxDirectHarnessState{
		cfg:     cfg,
		tables:  tables,
		config:  config,
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
			Engine:         state.runtime,
			Verbose:        cfg.Verbose,
			DumpActual:     cfg.DumpActual,
			CaptureProfile: cfg.CaptureProfile,
			Logf:           log.Printf,
		},
	}, nil
}

func (s *inaboxDirectHarnessState) rebuild(ctx context.Context) error {
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
	if err := preloadInaboxDirectTables(ctx, catalogTableCache, quantaSource, s.tables); err != nil {
		return err
	}
	runtime, diagnostics, err := inaboxDirectBuildSQLRuntime(ctx, s.cfg, s.config, catalogTableCache, quantaSource)
	if err != nil {
		return err
	}
	if diagnostics.BlocksNative() {
		return diagnosticsError(diagnostics)
	}
	s.runtime.Runtime = runtime
	return nil
}

func inaboxDirectBuildSQLRuntime(ctx context.Context, cfg runnerConfig, config qsruntime.DirectRuntimeConfig, catalogTableCache *core.TableCacheStruct, quantaSource *source.QuantaSource) (qsruntime.SQLRuntime, qsbridge.DiagnosticSet, error) {
	proxyRuntime, diagnostics, err := qsruntime.NewNativeProxyRuntimeFromSourceWithLegacyOptions(ctx, quantaSource, catalogTableCache, qsruntime.NativeProxyRuntimeConfig{
		Direct:                  config,
		DefaultSchema:           inaboxDirectDefaultSchema(cfg.Database),
		SchemaDir:               inaboxDirectConfigDir(),
		CatalogVersion:          qsbridge.CatalogVersion("sqlrunner-inabox-direct"),
		Functions:               inaboxDirectSQLFunctions(),
		Profile:                 qsruntime.LegacyDirectRuntimeProfile(),
		ContextWrapper:          qsruntime.WithQueryScratchpad,
		EnableFilterExpressions: true,
	}, qsruntime.NativeProxyRuntimeLegacyOptions{
		PrimaryKeyResolverFactory: qsinabox.NewSharedStandardSessionBSIPrimaryKeyResolverFactory(
			catalogTableCache,
		),
	})
	return proxyRuntime.Runtime, diagnostics, err
}
func preloadInaboxDirectTables(ctx context.Context, tableCache *core.TableCacheStruct, quantaSource *source.QuantaSource, tables []string) error {
	if tableCache == nil {
		return fmt.Errorf("inabox-direct table cache is not initialized")
	}
	if quantaSource == nil || quantaSource.GetConnection() == nil || quantaSource.GetSessionPool() == nil {
		return fmt.Errorf("inabox-direct source is not initialized")
	}
	kvStore := shared.NewKVStore(quantaSource.GetConnection())
	for _, table := range tables {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := core.LoadTable(tableCache, "", kvStore, table, quantaSource.GetConnection().Consul); err != nil {
			if inaboxDirectMissingTablePreloadError(table, err) {
				continue
			}
			return fmt.Errorf("preload inabox-direct table %s: %w", table, err)
		}
	}
	return nil
}

func inaboxDirectMissingTablePreloadError(table string, err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	table = strings.ToLower(strings.TrimSpace(table))
	return table != "" &&
		strings.Contains(message, "table "+table+" not found") &&
		strings.Contains(message, "unmarshalconsul")
}

func inaboxDirectDictionaryResolver(tableCache *core.TableCacheStruct, schema string) qsbridge.MemoryDictionaryResolver {
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

func inaboxDirectSuiteTables(suite *roadmap.Suite) []string {
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
			test.SQL = roadmap.AdminStatementSQL(test.SQL)
		}
		statement, diagnostics := parser.Parse(test.SQL)
		if diagnostics.BlocksNative() {
			for _, table := range inaboxDirectRawSQLTables(test.SQL) {
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
		case qsbridge.QueryKindCreateTable:
			rememberTable(statement.Create.Table)
		case qsbridge.QueryKindDropTable:
			rememberTable(statement.Drop.Table)
		case qsbridge.QueryKindTruncate:
			rememberTable(statement.Truncate.Table)
		}
	}
	tables := make([]string, 0, len(seen))
	for table := range seen {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

func inaboxDirectRawSQLTables(sql string) []string {
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

func inaboxDirectEnsureConfigBackedTables(ctx context.Context, tables []string, config qsruntime.DirectRuntimeConfig, schemaName string) error {
	ordered, err := inaboxDirectConfigBackedTablesInDependencyOrder(tables)
	if err != nil {
		return err
	}
	if len(ordered) == 0 {
		return nil
	}
	config = config.WithDefaults()
	quantaSource, err := source.NewQuantaSource(core.NewTableCacheStruct(), config.BaseDir, config.ConsulAddress, config.ServicePort, config.SessionPoolSize)
	if err != nil {
		return err
	}
	for _, table := range ordered {
		if !inaboxDirectHasConfigSchema(table) {
			continue
		}
		if err := inaboxDirectCreateConfigBackedTable(ctx, quantaSource, table, schemaName); err != nil {
			return fmt.Errorf("bootstrap inabox-direct table %s: %w", table, err)
		}
	}
	return nil
}

func inaboxDirectCreateConfigBackedTable(ctx context.Context, quantaSource *source.QuantaSource, table, schemaName string) error {
	handle, err := qsruntime.NewLegacySchemaMutationHandle(quantaSource, table, inaboxDirectConfigDir())
	if err != nil {
		return err
	}
	request := qsruntime.NewExecutionRequest(qsbridge.QuantaIntermediateQuery{})
	request.Mutation = qsbridge.MutationShape{
		Kind: qsbridge.MutationCreateTable,
		Target: qsbridge.TableInstance{
			Schema: schemaName,
			Table:  table,
		},
	}
	_, diagnostics, err := handle.CreateTable(ctx, request)
	if err != nil {
		return err
	}
	if diagnostics.BlocksNative() {
		return diagnosticsError(diagnostics)
	}
	return nil
}

func inaboxDirectConfigBackedTablesInDependencyOrder(tables []string) ([]string, error) {
	ordered := make([]string, 0, len(tables))
	visiting := map[string]bool{}
	visited := map[string]bool{}
	var visit func(string) error
	visit = func(table string) error {
		table = strings.TrimSpace(table)
		key := strings.ToLower(table)
		if key == "" || !inaboxDirectHasConfigSchema(table) || visited[key] {
			return nil
		}
		if visiting[key] {
			return fmt.Errorf("cycle in config-backed table dependencies at %s", table)
		}
		visiting[key] = true
		dependencies, err := inaboxDirectConfigSchemaForeignKeys(table)
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

func inaboxDirectHasConfigSchema(table string) bool {
	_, ok := inaboxDirectConfigSchemaPath(table)
	return ok
}

func inaboxDirectConfigDir() string {
	for _, path := range []string{"config", "sqlrunner/config"} {
		if info, err := os.Stat(path); err == nil && info.IsDir() {
			return path
		}
	}
	return ""
}

func inaboxDirectConfigSchemaPath(table string) (string, bool) {
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

func inaboxDirectConfigSchemaForeignKeys(table string) ([]string, error) {
	path, ok := inaboxDirectConfigSchemaPath(table)
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

func inaboxDirectServicePort(port string) (int, error) {
	port = strings.TrimSpace(port)
	if port == "" {
		return qsruntime.DefaultDirectServicePort, nil
	}
	servicePort, err := strconv.Atoi(port)
	if err != nil || servicePort < 0 {
		return 0, fmt.Errorf("inabox-direct port must be a non-negative integer: %q", port)
	}
	return servicePort, nil
}

func inaboxDirectDefaultSchema(schema string) string {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		return "quanta"
	}
	return schema
}

func inaboxDirectSQLFunctions() []qsbridge.FunctionDefinition {
	return qsbridge.BuiltinSQLFunctionDefinitions()
}

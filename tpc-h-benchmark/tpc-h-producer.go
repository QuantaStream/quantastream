package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	_ "runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsinabox"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/QuantaStream/quantastream/version"
	"github.com/hashicorp/consul/api"
	"golang.org/x/sync/errgroup"
	"gopkg.in/alecthomas/kingpin.v2"
)

// Variables to identify the build
var (
	Version  = version.Version
	Build    = version.BuildString()
	EPOCH, _ = time.ParseInLocation(time.RFC3339, "2000-01-01T00:00:00+00:00", time.UTC)
	loc, _   = time.LoadLocation("Local")
)

// Exit Codes
const (
	Success = 0
)

const (
	directModeCluster         = "cluster"
	directModeStandard        = "standard"
	directModeStandardRemote  = "standard-remote"
	directModeStandardOffline = "standard-offline"

	defaultDirectFlushInterval = 30 * time.Second
)

// Main strct defines command line arguments variables and various global meta-data associated with record loads.
type Main struct {
	Index               string
	BatchSize           int
	Path                string
	File                string
	Prefix              string
	Pattern             string
	totalBytes          int64
	bytesLock           sync.RWMutex
	totalRecs           *Counter
	failedRecs          *Counter
	IsNested            bool
	ConsulAddr          string
	ConsulClient        *api.Client
	Table               *shared.BasicTable
	conn                *shared.Conn
	router              *core.SessionRouter
	tableCache          *core.TableCacheStruct
	lock                *api.Lock
	shardCols           []*shared.BasicAttribute
	Direct              bool
	DirectMode          string
	Workers             int
	ConfigDir           string
	DataDir             string
	Database            string
	NativeGRPCAddr      string
	DirectFlushInterval time.Duration
	BasePath            string
	stdBackend          *qsinabox.StandardLocalBackend
	putRowProfile       *core.RouterPutRowProfile
	flushProfile        *core.RouterFlushProfile
	drainProfile        *core.RouterDrainProfile

	splitElapsed          time.Duration
	recordGenerateElapsed time.Duration
	jsonMarshalElapsed    time.Duration
	directEnqueueElapsed  time.Duration
	directEnqueueCount    int64
}

type LoadSummary struct {
	Table           string
	Records         int64
	Failures        int64
	Bytes           int64
	EnqueueDuration time.Duration
	DrainDuration   time.Duration
	TotalDuration   time.Duration
	SplitElapsed    time.Duration
	RecordElapsed   time.Duration
	JSONElapsed     time.Duration
	DirectEnqueue   time.Duration
	DirectEnqueues  int64
}

// NewMain allocates a new pointer to Main struct with empty record counter
func NewMain() *Main {
	m := &Main{
		totalRecs:  &Counter{},
		failedRecs: &Counter{},
	}
	return m
}

func main() {

	app := kingpin.New(os.Args[0], "Quanta TPC-H Direct Data Loader").DefaultEnvars()
	app.Version("Version: " + Version + "\nBuild: " + Build)

	filePath := app.Arg("file-path", "Path to TPC-H data files directory.").Required().String()
	index := app.Arg("index", "Table name.").Required().String()
	batchSize := app.Flag("batch-size", "Direct-load batch size").Default("100").Int32()
	app.Flag("direct", "Deprecated no-op; TPC-H loader always writes directly into Quanta sessions.").Bool()
	directMode := app.Flag("direct-mode", "Direct load target: cluster uses Consul/gRPC nodes; standard-remote connects to a running inabox-standard native gRPC endpoint; standard-offline mounts a local in-process backend.").Default(directModeCluster).Enum(directModeCluster, directModeStandardRemote, directModeStandardOffline, directModeStandard)
	workers := app.Flag("workers", "Direct-load session worker count.").Default("3").Int()
	configDir := app.Flag("config-dir", "Schema config directory for inabox-standard direct loads.").Default("config").String()
	dataDir := app.Flag("data-dir", "Data directory for inabox-standard direct loads.").Default("local/standard-data").String()
	database := app.Flag("database", "Database/schema name for inabox-standard direct loads.").Default("quanta").String()
	nativeGRPCAddr := app.Flag("native-grpc-addr", "Native gRPC address for standard-remote direct loads.").Default("127.0.0.1:4100").String()
	directFlushInterval := app.Flag("direct-flush-interval", "Direct-load session router flush interval. Larger values improve batch-load coalescing; lower values improve streaming freshness.").Default("30s").Duration()
	environment := app.Flag("env", "Environment [DEV, QA, STG, VAL, PROD]").Default("DEV").String()
	consul := app.Flag("consul-endpoint", "Consul agent address/port").Default("127.0.0.1:8500").String()

	shared.InitLogging("WARN", *environment, "TPC-H-Producer", Version, version.ProductName)

	kingpin.MustParse(app.Parse(os.Args[1:]))

	main := NewMain()
	main.Index = *index
	main.BatchSize = int(*batchSize)
	main.ConsulAddr = *consul
	main.Direct = true
	main.DirectMode = normalizeDirectMode(*directMode)
	main.Workers = *workers
	main.ConfigDir = *configDir
	main.DataDir = *dataDir
	main.Database = *database
	main.NativeGRPCAddr = *nativeGRPCAddr
	main.DirectFlushInterval = *directFlushInterval
	if *directMode == directModeStandard {
		log.Printf("Direct load mode %s is deprecated; use %s for offline bootstrap loads or %s for a running standard server.\n",
			directModeStandard, directModeStandardOffline, directModeStandardRemote)
	}

	log.Printf("Table name %v.\n", main.Index)
	log.Printf("Batch size %d.\n", main.BatchSize)
	log.Printf("Direct load mode %s workers %d flush_interval %s.\n", main.DirectMode, main.Workers, main.directFlushInterval())
	switch main.DirectMode {
	case directModeStandardRemote:
		log.Printf("Direct standard remote config_dir=%s database=%s native_grpc_addr=%s\n", main.ConfigDir, main.Database, main.NativeGRPCAddr)
	case directModeStandardOffline:
		log.Printf("Direct standard offline config_dir=%s data_dir=%s database=%s\n", main.ConfigDir, main.DataDir, main.Database)
	}
	log.Printf("Consul agent at [%s]\n", main.ConsulAddr)

	if err := main.Init(); err != nil {
		log.Fatal(err)
	}
	defer main.Close()

	main.Path = *filePath

	var eg errgroup.Group
	var ticker *time.Ticker
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for range c {
			log.Printf("Interrupted,  Bytes processed: %s", core.Bytes(main.BytesProcessed()))
			os.Exit(0)
		}
	}()

	fullPath := fmt.Sprintf("%s/%s.tbl", main.Path, main.Index)
	readFile, err := os.Open(fullPath)
	if err != nil {
		log.Fatalf("Open error %v", err)
	}

	ticker = main.printStats()

	startedAt := time.Now()
	enqueueStartedAt := time.Now()
	main.processRowsForFile(readFile)
	enqueueFinishedAt := time.Now()

	readFile.Close()
	if main.router != nil {
		if err := main.router.Close(); err != nil {
			log.Fatalf("router close error %v", err)
		}
		commitStartedAt := time.Now()
		if err := main.commitDirectLoad(); err != nil {
			log.Fatalf("direct load commit error %v", err)
		}
		log.Printf("TPC-H direct load commit table=%s elapsed=%s", main.Index, time.Since(commitStartedAt).Round(time.Millisecond))
	}
	finishedAt := time.Now()

	if err := eg.Wait(); err != nil {
		log.Fatalf("Open error %v", err)
	}
	ticker.Stop()
	summary := LoadSummary{
		Table:           main.Index,
		Records:         main.totalRecs.Get(),
		Failures:        main.failedRecs.Get(),
		Bytes:           main.BytesProcessed(),
		EnqueueDuration: enqueueFinishedAt.Sub(enqueueStartedAt),
		DrainDuration:   finishedAt.Sub(enqueueFinishedAt),
		TotalDuration:   finishedAt.Sub(startedAt),
		SplitElapsed:    main.splitElapsed,
		RecordElapsed:   main.recordGenerateElapsed,
		JSONElapsed:     main.jsonMarshalElapsed,
		DirectEnqueue:   main.directEnqueueElapsed,
		DirectEnqueues:  main.directEnqueueCount,
	}
	main.logLoadSummary(summary)
	main.logDirectProfileSummary()
	if summary.Failures > 0 {
		log.Fatalf("TPC-H load failed table=%s failures=%d records=%d", summary.Table, summary.Failures, summary.Records)
	}

}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func (m *Main) processRowsForFile(readFile *os.File) {

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	for fileScanner.Scan() {

		// Split the pipe delimited text line
		splitStartedAt := time.Now()
		s := strings.Split(fileScanner.Text(), "|")
		m.splitElapsed += time.Since(splitStartedAt)

		recordStartedAt := time.Now()
		shardKey, record := m.generateRecord(s)
		m.recordGenerateElapsed += time.Since(recordStartedAt)
		marshalStartedAt := time.Now()
		outData, err := json.Marshal(record)
		m.jsonMarshalElapsed += time.Since(marshalStartedAt)
		if err != nil {
			m.failedRecs.Add(1)
			log.Printf("marshal error %v", err)
			continue
		}

		buildShardKey := m.directLoadBuildShardKey(record)
		enqueueStartedAt := time.Now()
		if err := m.router.Enqueue(core.IngestRecord{
			TableName:     m.Index,
			Data:          record,
			ShardKey:      shardKey,
			BuildShardKey: buildShardKey,
		}); err != nil {
			m.directEnqueueElapsed += time.Since(enqueueStartedAt)
			m.failedRecs.Add(1)
			log.Printf("direct load error %v", err)
			continue
		}
		m.directEnqueueElapsed += time.Since(enqueueStartedAt)
		m.directEnqueueCount++

		m.totalRecs.Add(1)
		m.AddBytes(len(outData))
	}
}

func (m *Main) logLoadSummary(summary LoadSummary) {
	totalSeconds := summary.TotalDuration.Seconds()
	enqueueSeconds := summary.EnqueueDuration.Seconds()
	rowsPerSecond := float64(summary.Records)
	bytesPerSecond := float64(summary.Bytes)
	enqueueRowsPerSecond := float64(summary.Records)
	if totalSeconds > 0 {
		rowsPerSecond /= totalSeconds
		bytesPerSecond /= totalSeconds
	}
	if enqueueSeconds > 0 {
		enqueueRowsPerSecond /= enqueueSeconds
	}

	log.Printf("TPC-H load summary table=%s mode=%s records=%d failures=%d bytes=%s enqueue=%s drain=%s total=%s split=%s record=%s json=%s direct_enqueue=%s direct_enqueue_count=%d rows_per_sec=%.2f enqueue_rows_per_sec=%.2f bytes_per_sec=%s",
		summary.Table,
		m.loadMode(),
		summary.Records,
		summary.Failures,
		core.Bytes(summary.Bytes),
		summary.EnqueueDuration.Round(time.Millisecond),
		summary.DrainDuration.Round(time.Millisecond),
		summary.TotalDuration.Round(time.Millisecond),
		summary.SplitElapsed.Round(time.Millisecond),
		summary.RecordElapsed.Round(time.Millisecond),
		summary.JSONElapsed.Round(time.Millisecond),
		summary.DirectEnqueue.Round(time.Millisecond),
		summary.DirectEnqueues,
		rowsPerSecond,
		enqueueRowsPerSecond,
		core.Bytes(bytesPerSecond),
	)
}

func (m *Main) logDirectProfileSummary() {
	if !m.Direct {
		return
	}
	if m.putRowProfile != nil {
		profile := m.putRowProfile.Snapshot()
		pk := profile.PrimaryKey
		log.Printf("TPC-H direct putrow profile table=%s records=%d logical_rows=%d child_rows=%d inserted=%d existing=%d duplicate=%d conflict=%d total=%s source=%s identity=%s child_expand=%s child_traverse=%s relation=%s attributes=%s attribute_read=%s attribute_map=%s pk_read=%s pk_map=%s pk_stage_map=%s attribute_by_mapper=%q pk_total=%s pk_resolves=%d pk_lookup_required=%d pk_bsi_lookup=%d pk_bsi_hit=%d pk_empty_domain_probe=%d pk_empty_domain_skip=%d pk_empty_domain_non_empty=%d pk_empty_domain_unknown=%d pk_empty_domain_probe_elapsed=%s pk_bsi_projection_cache_lookup=%d pk_bsi_projection_cache_hit=%d pk_bsi_projection=%s pk_bsi_compare=%s pk_bsi_stage_write=%s pk_rownum_alloc=%s pk_batch_cache=%s",
			m.Index,
			profile.RecordCount,
			profile.LogicalRowCount,
			profile.ChildRowCount,
			profile.InsertedCount,
			profile.ExistingCount,
			profile.DuplicateCount,
			profile.ConflictCount,
			profile.TotalElapsed.Round(time.Millisecond),
			profile.SourceElapsed.Round(time.Millisecond),
			profile.IdentityElapsed.Round(time.Millisecond),
			profile.ChildExpansionElapsed.Round(time.Millisecond),
			profile.ChildTraversalElapsed.Round(time.Millisecond),
			profile.RelationElapsed.Round(time.Millisecond),
			profile.AttributeElapsed.Round(time.Millisecond),
			profile.AttributeReadElapsed.Round(time.Millisecond),
			profile.AttributeMapElapsed.Round(time.Millisecond),
			profile.PrimaryKeyReadElapsed.Round(time.Millisecond),
			profile.PrimaryKeyMapElapsed.Round(time.Millisecond),
			profile.PrimaryKeyStageElapsed.Round(time.Millisecond),
			formatPutRowMapperProfiles(profile.AttributeByMapper),
			pk.TotalElapsed.Round(time.Millisecond),
			pk.ResolveCount,
			pk.LookupRequiredCount,
			pk.BSILookupCount,
			pk.BSIHitCount,
			pk.EmptyDomainProbeCount,
			pk.EmptyDomainSkipCount,
			pk.EmptyDomainNonEmptyCount,
			pk.EmptyDomainUnknownCount,
			pk.EmptyDomainProbeElapsed.Round(time.Millisecond),
			pk.BSIProjectionCacheLookupCount,
			pk.BSIProjectionCacheHitCount,
			pk.BSIProjectionElapsed.Round(time.Millisecond),
			pk.BSICompareElapsed.Round(time.Millisecond),
			pk.BSIStageWriteElapsed.Round(time.Millisecond),
			pk.RownumAllocationElapsed.Round(time.Millisecond),
			pk.BatchCacheWriteElapsed.Round(time.Millisecond),
		)
	}
	if m.flushProfile != nil {
		profile := m.flushProfile.Snapshot()
		log.Printf("TPC-H direct flush profile table=%s flushes=%d errors=%d total=%s partition_string=%s partition_string_route=%s partition_string_build=%s partition_string_stream=%s partition_string_stream_open=%s partition_string_stream_send=%s partition_string_stream_close=%s partition_string_stream_max=%s bitmap_set=%s bitmap_clear=%s bsi_value=%s bsi_value_route=%s bsi_value_build=%s bsi_value_marshal=%s bsi_value_stream=%s bsi_value_stream_open=%s bsi_value_stream_send=%s bsi_value_stream_close=%s bsi_value_stream_max=%s bsi_clear=%s partition_string_put_calls=%d partition_string_batches=%d partition_string_entries=%d partition_string_routed_entries=%d bitmap_set_entries=%d bitmap_clear_entries=%d bsi_value_entries=%d bsi_value_batches=%d bsi_value_routed_items=%d bsi_clear_entries=%d",
			m.Index,
			profile.FlushCount,
			profile.ErrorCount,
			profile.TotalElapsed.Round(time.Millisecond),
			profile.PartitionStringElapsed.Round(time.Millisecond),
			profile.PartitionStringRoute.Round(time.Millisecond),
			profile.PartitionStringBuild.Round(time.Millisecond),
			profile.PartitionStringStream.Round(time.Millisecond),
			profile.PartitionStringStreamOpen.Round(time.Millisecond),
			profile.PartitionStringStreamSend.Round(time.Millisecond),
			profile.PartitionStringStreamClose.Round(time.Millisecond),
			profile.PartitionStringStreamMax.Round(time.Millisecond),
			profile.BitmapSetElapsed.Round(time.Millisecond),
			profile.BitmapClearElapsed.Round(time.Millisecond),
			profile.BSIValueElapsed.Round(time.Millisecond),
			profile.BSIValueRoute.Round(time.Millisecond),
			profile.BSIValueBuild.Round(time.Millisecond),
			profile.BSIValueMarshal.Round(time.Millisecond),
			profile.BSIValueStream.Round(time.Millisecond),
			profile.BSIValueStreamOpen.Round(time.Millisecond),
			profile.BSIValueStreamSend.Round(time.Millisecond),
			profile.BSIValueStreamClose.Round(time.Millisecond),
			profile.BSIValueStreamMax.Round(time.Millisecond),
			profile.BSIClearValueElapsed.Round(time.Millisecond),
			profile.PartitionStringPutCalls,
			profile.PartitionStringBatchCount,
			profile.PartitionStringEntryCount,
			profile.PartitionStringRoutedEntryCount,
			profile.BitmapSetEntryCount,
			profile.BitmapClearEntryCount,
			profile.BSIValueEntryCount,
			profile.BSIValueBatchCount,
			profile.BSIValueRoutedItemCount,
			profile.BSIClearValueEntryCount,
		)
	}
	if m.drainProfile != nil {
		profile := m.drainProfile.Snapshot()
		log.Printf("TPC-H direct drain profile table=%s workers=%d sessions=%d errors=%d total=%s max=%s",
			m.Index,
			profile.WorkerCount,
			profile.SessionCount,
			profile.ErrorCount,
			profile.TotalElapsed.Round(time.Millisecond),
			profile.MaxElapsed.Round(time.Millisecond),
		)
	}
}

func formatPutRowMapperProfiles(profiles map[string]core.PutRowMapperProfile) string {
	if len(profiles) == 0 {
		return ""
	}
	keys := make([]string, 0, len(profiles))
	for key := range profiles {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		profile := profiles[key]
		parts = append(parts, fmt.Sprintf("%s[count=%d,read=%s,map=%s]",
			key,
			profile.ValueCount,
			profile.ReadElapsed.Round(time.Millisecond),
			profile.MapElapsed.Round(time.Millisecond),
		))
	}
	return strings.Join(parts, ";")
}

func (m *Main) loadMode() string {
	if m.DirectMode != "" {
		return "direct-" + m.DirectMode
	}
	return "direct"
}

func normalizeDirectMode(mode string) string {
	switch strings.TrimSpace(mode) {
	case "", directModeCluster:
		return directModeCluster
	case directModeStandardRemote:
		return directModeStandardRemote
	case directModeStandardOffline, directModeStandard:
		return directModeStandardOffline
	default:
		return mode
	}
}

// generateRecord builds the loader envelope consumed by the direct load path.
func (m *Main) generateRecord(fields []string) (string, map[string]interface{}) {
	env := make(map[string]interface{}, 0)
	data := make(map[string]interface{}, 0)

	var sb strings.Builder

	for _, v := range m.Table.Attributes {
		if v.SourceOrdinal > 0 {
			data[v.FieldName] = fields[v.SourceOrdinal-1]
		}
	}
	for _, v := range m.shardCols {
		if x, ok := data[v.FieldName]; ok {
			sb.WriteString(x.(string))
		}
	}
	shardKey := sb.String()
	env["data"] = data
	env["type"] = m.Index
	env["shardKey"] = shardKey
	return shardKey, env
}

func (m *Main) directLoadBuildShardKey(record map[string]interface{}) string {
	if !m.Direct || m.Table == nil {
		return ""
	}
	data, ok := record["data"].(map[string]interface{})
	if !ok {
		return ""
	}
	result, ok := core.ResolveIngestBuildShardKey(core.IngestBuildShardKeyRequest{
		Table:   m.Table,
		Payload: data,
	})
	if !ok {
		return ""
	}
	return result.BuildShardKey
}

// Init function initializes process.
func (m *Main) Init() error {

	var err error

	m.DirectMode = normalizeDirectMode(m.DirectMode)
	switch m.DirectMode {
	case directModeStandardRemote:
		return m.initStandardRemoteDirect()
	case directModeStandardOffline:
		return m.initStandardOfflineDirect()
	case directModeCluster:
	default:
		return fmt.Errorf("unsupported direct load mode %q", m.DirectMode)
	}

	m.ConsulClient, err = api.NewClient(&api.Config{Address: m.ConsulAddr})
	if err != nil {
		return err
	}

	m.Table, err = shared.LoadSchema("", m.Index, m.ConsulClient)
	if err != nil {
		return err
	}

	pkInfo, errx := m.Table.GetPrimaryKeyInfo()
	if errx != nil {
		return errx
	}
	m.shardCols = pkInfo
	return m.initDirect()
}

func (m *Main) initDirect() error {
	m.tableCache = core.NewTableCacheStruct()
	m.conn = shared.NewDefaultConnection("tpch-direct-loader")
	m.conn.Quorum = 3
	if err := m.conn.Connect(m.ConsulClient); err != nil {
		return err
	}
	if err := m.waitDirectClusterReady(2 * time.Minute); err != nil {
		return err
	}
	if m.Workers <= 0 {
		m.Workers = 3
	}
	router, err := core.NewSessionRouter(m.clusterDirectRouterConfig())
	if err != nil {
		return err
	}
	m.router = router
	return nil
}

func (m *Main) clusterDirectRouterConfig() core.SessionRouterConfig {
	return m.withDirectProfileCallbacks(core.SessionRouterConfig{
		TableCache:                m.tableCache,
		BasePath:                  m.BasePath,
		Conn:                      m.conn,
		ShardCount:                m.Workers,
		ChannelSize:               m.BatchSize * m.Workers,
		FlushInterval:             m.directFlushInterval(),
		PrimaryKeyResolverFactory: qsinabox.NewSharedStandardSessionBSIPrimaryKeyResolverFactory(m.tableCache),
		OnError: func(err error) {
			m.failedRecs.Add(1)
			log.Printf("direct load error %v", err)
		},
	})
}

func (m *Main) initStandardRemoteDirect() error {
	if strings.TrimSpace(m.NativeGRPCAddr) == "" {
		return fmt.Errorf("standard-remote direct load requires --native-grpc-addr")
	}
	m.BasePath = m.ConfigDir
	var err error
	m.Table, err = shared.LoadSchema(m.BasePath, m.Index, nil)
	if err != nil {
		return err
	}
	pkInfo, err := m.Table.GetPrimaryKeyInfo()
	if err != nil {
		return err
	}
	m.shardCols = pkInfo
	m.tableCache = core.NewTableCacheStruct()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m.conn, err = shared.NewLoaderConnection(ctx, shared.LoaderConnectionConfig{
		Mode:    shared.LoaderConnectionStandardNative,
		Owner:   "tpch-standard-remote-loader",
		Address: m.NativeGRPCAddr,
	})
	if err != nil {
		return err
	}
	if m.Workers <= 0 {
		m.Workers = 1
	}
	router, err := core.NewSessionRouter(m.standardDirectRouterConfig())
	if err != nil {
		return err
	}
	m.router = router
	return nil
}

func (m *Main) initStandardOfflineDirect() error {
	config := qsinabox.StandardConfig{
		ConfigDir: m.ConfigDir,
		DataDir:   m.DataDir,
		Database:  m.Database,
	}
	backend, err := qsinabox.MountStandardLocalBackend(config, nil)
	if err != nil {
		return err
	}
	m.stdBackend = &backend
	m.BasePath = backend.ConfigBaseDir(config)
	m.Table, err = shared.LoadSchema(m.BasePath, m.Index, nil)
	if err != nil {
		return err
	}
	pkInfo, err := m.Table.GetPrimaryKeyInfo()
	if err != nil {
		return err
	}
	m.shardCols = pkInfo
	m.tableCache = core.NewTableCacheStruct()
	m.conn = backend.NewLocalConnection()
	if m.Workers <= 0 {
		m.Workers = 3
	}
	router, err := core.NewSessionRouter(m.standardDirectRouterConfig())
	if err != nil {
		return err
	}
	m.router = router
	return nil
}

func (m *Main) standardDirectRouterConfig() core.SessionRouterConfig {
	return m.withDirectProfileCallbacks(core.SessionRouterConfig{
		TableCache:                m.tableCache,
		BasePath:                  m.BasePath,
		Conn:                      m.conn,
		ShardCount:                m.Workers,
		ChannelSize:               m.BatchSize * m.Workers,
		FlushInterval:             m.directFlushInterval(),
		PrimaryKeyResolverFactory: qsinabox.NewSharedStandardSessionBSIPrimaryKeyResolverFactory(m.tableCache),
		OnError: func(err error) {
			m.failedRecs.Add(1)
			log.Printf("direct load error %v", err)
		},
	})
}

func (m *Main) directFlushInterval() time.Duration {
	if m.DirectFlushInterval > 0 {
		return m.DirectFlushInterval
	}
	return defaultDirectFlushInterval
}

func (m *Main) withDirectProfileCallbacks(cfg core.SessionRouterConfig) core.SessionRouterConfig {
	m.ensureDirectProfiles()
	cfg.OnPutRowResult = m.putRowProfile.Callback()
	cfg.OnFlushProfile = m.flushProfile.Callback()
	cfg.OnDrainProfile = m.drainProfile.Callback()
	return cfg
}

func (m *Main) ensureDirectProfiles() {
	if m.putRowProfile == nil {
		m.putRowProfile = &core.RouterPutRowProfile{}
	}
	if m.flushProfile == nil {
		m.flushProfile = &core.RouterFlushProfile{}
	}
	if m.drainProfile == nil {
		m.drainProfile = &core.RouterDrainProfile{}
	}
}

func (m *Main) waitDirectClusterReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		state, activeCount, targetSize := m.conn.GetClusterState()
		clientCount := len(m.conn.ClientConnections())
		if state == shared.Green && targetSize > 0 && activeCount >= targetSize && clientCount >= targetSize {
			log.Printf("TPC-H direct load cluster ready table=%s state=%s active=%d target=%d clients=%d",
				m.Index, state, activeCount, targetSize, clientCount)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("direct load cluster not ready for table=%s state=%s active=%d target=%d clients=%d",
				m.Index, state, activeCount, targetSize, clientCount)
		}
		log.Printf("TPC-H direct load waiting for cluster table=%s state=%s active=%d target=%d clients=%d",
			m.Index, state, activeCount, targetSize, clientCount)
		time.Sleep(time.Second)
	}
}

func (m *Main) commitDirectLoad() error {
	if !m.Direct || m.conn == nil {
		return nil
	}
	switch m.DirectMode {
	case directModeCluster:
		state, activeCount, targetSize := m.conn.GetClusterState()
		clientCount := len(m.conn.ClientConnections())
		log.Printf("TPC-H direct load commit cluster table=%s state=%s active=%d target=%d clients=%d",
			m.Index, state, activeCount, targetSize, clientCount)
		if state != shared.Green || targetSize <= 0 || activeCount < targetSize || clientCount < targetSize {
			return fmt.Errorf("direct load commit requires green cluster table=%s state=%s active=%d target=%d clients=%d",
				m.Index, state, activeCount, targetSize, clientCount)
		}
	case directModeStandardRemote:
		log.Printf("TPC-H direct load commit standard-remote table=%s native_grpc_addr=%s config_dir=%s",
			m.Index, m.NativeGRPCAddr, m.BasePath)
	case directModeStandardOffline:
		log.Printf("TPC-H direct load commit standard-offline table=%s data_dir=%s config_dir=%s", m.Index, m.DataDir, m.BasePath)
	default:
		return fmt.Errorf("unsupported direct load mode %q", m.DirectMode)
	}
	session, err := core.OpenSession(m.tableCache, m.BasePath, m.Index, true, m.conn)
	if err != nil {
		return err
	}
	if err := session.Commit(); err != nil {
		_ = session.CloseSession()
		return err
	}
	return session.CloseSession()
}

func (m *Main) Close() {
	if m.conn != nil {
		_ = m.conn.Disconnect()
		m.conn = nil
	}
	if m.stdBackend != nil {
		m.stdBackend.Close()
		m.stdBackend = nil
	}
}

// printStats outputs to Log current status of loader
// Includes data on processed: bytes, records, time duration in seconds, and rate of bytes per sec"
func (m *Main) printStats() *time.Ticker {
	t := time.NewTicker(time.Second * 10)
	start := time.Now()
	go func() {
		for range t.C {
			duration := time.Since(start)
			bytes := m.BytesProcessed()
			log.Printf("Bytes: %s, Records: %v, Failed: %v, Duration: %v, Rate: %v/s", core.Bytes(bytes), m.totalRecs.Get(), m.failedRecs.Get(), duration, core.Bytes(float64(bytes)/duration.Seconds()))
		}
	}()
	return t
}

// AddBytes provides thread safe processing to set the total bytes processed.
// Adds the bytes parameter to total bytes processed.
func (m *Main) AddBytes(n int) {
	m.bytesLock.Lock()
	m.totalBytes += int64(n)
	m.bytesLock.Unlock()
}

// BytesProcessed provides thread safe read of total bytes processed.
func (m *Main) BytesProcessed() (num int64) {
	m.bytesLock.Lock()
	num = m.totalBytes
	m.bytesLock.Unlock()
	return
}

// Counter - Generic counter with mutex (threading) support
type Counter struct {
	num  int64
	lock sync.Mutex
}

// Add function provides thread safe addition of counter value based on input parameter.
func (c *Counter) Add(n int) {
	c.lock.Lock()
	c.num += int64(n)
	c.lock.Unlock()
}

// Get function provides thread safe read of counter value.
func (c *Counter) Get() (ret int64) {
	c.lock.Lock()
	ret = c.num
	c.lock.Unlock()
	return
}

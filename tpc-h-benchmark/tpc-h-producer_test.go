package main

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/qsinabox"
	"github.com/QuantaStream/quantastream/shared"
)

func TestStandardDirectRouterConfigInjectsBSIPrimaryKeyResolverFactory(t *testing.T) {
	loader := NewMain()
	loader.tableCache = core.NewTableCacheStruct()
	loader.BasePath = t.TempDir()
	loader.conn = shared.NewDefaultConnection("tpch-standard-loader-test")
	loader.Workers = 1
	loader.BatchSize = 8
	loader.DirectFlushInterval = 45 * time.Second

	config := loader.standardDirectRouterConfig()

	if config.PrimaryKeyResolverFactory == nil {
		t.Fatalf("PrimaryKeyResolverFactory = nil, want standard BSI authority factory")
	}
	resolver := config.PrimaryKeyResolverFactory(&core.Session{})
	if _, ok := resolver.(qsinabox.StandardBSIPrimaryKeyResolver); !ok {
		t.Fatalf("resolver = %T, want qsinabox.StandardBSIPrimaryKeyResolver", resolver)
	}
	if config.TableCache != loader.tableCache {
		t.Fatalf("TableCache was not preserved")
	}
	if config.Conn != loader.conn {
		t.Fatalf("Conn was not preserved")
	}
	if config.ShardCount != 1 || config.ChannelSize != 8 {
		t.Fatalf("router sizing = shards %d channel %d, want 1/8", config.ShardCount, config.ChannelSize)
	}
	if config.FlushInterval != 45*time.Second {
		t.Fatalf("FlushInterval = %s, want 45s", config.FlushInterval)
	}
	if config.OnPutRowResult == nil || config.OnFlushProfile == nil || config.OnDrainProfile == nil {
		t.Fatalf("router profile callbacks were not installed")
	}
	config.OnPutRowResult("shard0", core.IngestRecord{TableName: "orders"}, core.PutRowResult{TableName: "orders", TotalElapsed: 1})
	config.OnFlushProfile("shard0", "orders", shared.BatchBufferFlushProfile{TotalElapsed: 1})
	config.OnDrainProfile(core.RouterDrainWorkerProfile{ShardID: "shard0", Elapsed: 1})
	if loader.putRowProfile.Snapshot().RecordCount != 1 {
		t.Fatalf("putrow profile callback did not update loader profile")
	}
	if loader.flushProfile.Snapshot().FlushCount != 1 {
		t.Fatalf("flush profile callback did not update loader profile")
	}
	if loader.drainProfile.Snapshot().WorkerCount != 1 {
		t.Fatalf("drain profile callback did not update loader profile")
	}
}

func TestClusterDirectRouterConfigInjectsBSIPrimaryKeyResolverFactory(t *testing.T) {
	loader := NewMain()
	loader.tableCache = core.NewTableCacheStruct()
	loader.BasePath = t.TempDir()
	loader.conn = shared.NewDefaultConnection("tpch-cluster-loader-test")
	loader.Workers = 3
	loader.BatchSize = 11

	config := loader.clusterDirectRouterConfig()

	if config.PrimaryKeyResolverFactory == nil {
		t.Fatalf("PrimaryKeyResolverFactory = nil, want BSI authority factory")
	}
	resolver := config.PrimaryKeyResolverFactory(&core.Session{})
	if _, ok := resolver.(qsinabox.StandardBSIPrimaryKeyResolver); !ok {
		t.Fatalf("resolver = %T, want qsinabox.StandardBSIPrimaryKeyResolver", resolver)
	}
	if config.TableCache != loader.tableCache {
		t.Fatalf("TableCache was not preserved")
	}
	if config.Conn != loader.conn {
		t.Fatalf("Conn was not preserved")
	}
	if config.ShardCount != 3 || config.ChannelSize != 33 {
		t.Fatalf("router sizing = shards %d channel %d, want 3/33", config.ShardCount, config.ChannelSize)
	}
	if config.FlushInterval != defaultDirectFlushInterval {
		t.Fatalf("FlushInterval = %s, want default %s", config.FlushInterval, defaultDirectFlushInterval)
	}
	if config.OnPutRowResult == nil || config.OnFlushProfile == nil || config.OnDrainProfile == nil {
		t.Fatalf("router profile callbacks were not installed")
	}
}

func TestNormalizeDirectMode(t *testing.T) {
	tests := []struct {
		name string
		mode string
		want string
	}{
		{name: "empty defaults to cluster", mode: "", want: directModeCluster},
		{name: "cluster stays cluster", mode: directModeCluster, want: directModeCluster},
		{name: "standard remote stays remote", mode: directModeStandardRemote, want: directModeStandardRemote},
		{name: "standard offline stays offline", mode: directModeStandardOffline, want: directModeStandardOffline},
		{name: "standard alias maps offline", mode: directModeStandard, want: directModeStandardOffline},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeDirectMode(tt.mode); got != tt.want {
				t.Fatalf("normalizeDirectMode(%q) = %q, want %q", tt.mode, got, tt.want)
			}
		})
	}
}

func TestLoadModeIncludesDirectTopology(t *testing.T) {
	loader := NewMain()
	loader.Direct = true
	loader.DirectMode = directModeStandardRemote

	if got := loader.loadMode(); got != "direct-standard-remote" {
		t.Fatalf("loadMode() = %q, want direct-standard-remote", got)
	}
}

func TestInitStandardRemoteDirectUsesRunningNativeEndpoint(t *testing.T) {
	config := qsinabox.StandardConfig{
		BindAddress:    "127.0.0.1",
		AuthMode:       "permissive",
		MySQLPort:      reserveTPCHEphemeralPort(t),
		NativeGRPCPort: reserveTPCHEphemeralPort(t),
		ConfigDir:      "config",
		DataDir:        t.TempDir(),
		Database:       "quanta",
	}
	process, diagnostics, err := qsinabox.MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	if process.NativeNode == nil {
		t.Fatalf("NativeNode = nil, want native gRPC endpoint")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := process.NativeNode.Start(ctx)
	defer func() {
		process.NativeNode.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("native gRPC server exited with error %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("native gRPC server did not stop")
		}
	}()

	loader := NewMain()
	loader.Index = "region"
	loader.Direct = true
	loader.DirectMode = directModeStandardRemote
	loader.ConfigDir = "config"
	loader.Database = "quanta"
	loader.NativeGRPCAddr = process.NativeNode.Address
	loader.Workers = 1
	loader.BatchSize = 8

	if err := loader.Init(); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	defer loader.Close()
	if loader.stdBackend != nil {
		t.Fatalf("stdBackend = %T, want nil for standard-remote", loader.stdBackend)
	}
	if loader.conn == nil {
		t.Fatalf("remote conn = nil, want connected standard native client")
	}
	if len(loader.conn.ClientConnections()) != 1 {
		t.Fatalf("remote conn clients = %d, want 1", len(loader.conn.ClientConnections()))
	}
	if loader.Table == nil || loader.Table.Name != "region" {
		t.Fatalf("Table = %#v, want region schema", loader.Table)
	}
}

func TestGenerateRecordKeepsPrimaryKeyShardKeyForStreamLoads(t *testing.T) {
	loader := testLineitemLoader(false)

	shardKey, env := loader.generateRecord([]string{"1001", "2", "1996-03-15"})

	if shardKey != "10012" {
		t.Fatalf("shardKey = %q, want primary key shard", shardKey)
	}
	if env["shardKey"] != "10012" {
		t.Fatalf("envelope shardKey = %q, want primary key shard", env["shardKey"])
	}
}

func TestGenerateRecordKeepsPrimaryKeyShardKeyForDirectLoads(t *testing.T) {
	loader := testLineitemLoader(true)

	shardKey, env := loader.generateRecord([]string{"1001", "2", "1996-03-15"})

	if shardKey != "10012" {
		t.Fatalf("direct shardKey = %q, want primary key shard", shardKey)
	}
	if env["shardKey"] != "10012" {
		t.Fatalf("envelope shardKey = %q, want preserved primary key shard", env["shardKey"])
	}
}

func TestDirectLoadBuildShardKeyUsesTimeQuantumShard(t *testing.T) {
	loader := testLineitemLoader(true)
	tq, _, err := shared.ToTQTimestamp("YMD", "1996-03-15")
	if err != nil {
		t.Fatalf("ToTQTimestamp() error: %v", err)
	}
	_, env := loader.generateRecord([]string{"1001", "2", "1996-03-15"})

	want := fmt.Sprintf("tq:lineitem:l_shipdate:%d", tq.UnixNano())
	if got := loader.directLoadBuildShardKey(env); got != want {
		t.Fatalf("direct build shard key = %q, want %q", got, want)
	}
}

func TestDirectLoadBuildShardKeyIsEmptyForStreamLoads(t *testing.T) {
	loader := testLineitemLoader(false)
	_, env := loader.generateRecord([]string{"1001", "2", "1996-03-15"})

	if got := loader.directLoadBuildShardKey(env); got != "" {
		t.Fatalf("stream build shard key = %q, want empty", got)
	}
}

func testLineitemLoader(direct bool) *Main {
	table := &shared.BasicTable{
		Name:             "lineitem",
		PrimaryKey:       "l_orderkey+l_linenumber",
		TimeQuantumType:  "YMD",
		TimeQuantumField: "l_shipdate",
		Attributes: []shared.BasicAttribute{
			{FieldName: "l_orderkey", SourceOrdinal: 1},
			{FieldName: "l_linenumber", SourceOrdinal: 2},
			{FieldName: "l_shipdate", SourceOrdinal: 3},
		},
	}
	return &Main{
		Index:     "lineitem",
		Table:     table,
		Direct:    direct,
		shardCols: []*shared.BasicAttribute{&table.Attributes[0], &table.Attributes[1]},
	}
}

func reserveTPCHEphemeralPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen() error = %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

package main

import (
	"fmt"
	"testing"

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
	if config.OnPutRowResult == nil || config.OnFlushProfile == nil || config.OnDrainProfile == nil {
		t.Fatalf("router profile callbacks were not installed")
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

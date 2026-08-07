package main

import (
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
}

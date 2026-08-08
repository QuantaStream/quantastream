package core

import (
	"sync"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/require"
	"github.com/stvp/rendezvous"
)

func TestSessionRouterPublishesPutRowResult(t *testing.T) {
	called := false
	router := &SessionRouter{
		cfg: SessionRouterConfig{
			OnPutRowResult: func(shardID string, record IngestRecord, result PutRowResult) {
				called = true
				require.Equal(t, "shard1", shardID)
				require.Equal(t, "orders", record.TableName)
				require.Equal(t, uint64(99), record.PayloadHash)
				require.Equal(t, "orders", result.TableName)
				require.Equal(t, 7*time.Millisecond, result.TotalElapsed)
			},
		},
	}

	router.publishPutRowResult(
		"shard1",
		IngestRecord{TableName: "orders", PayloadHash: 99},
		PutRowResult{TableName: "orders", TotalElapsed: 7 * time.Millisecond},
	)
	require.True(t, called)
}

func TestSessionRouterPublishesFlushProfile(t *testing.T) {
	called := false
	router := &SessionRouter{
		cfg: SessionRouterConfig{
			OnFlushProfile: func(shardID string, tableName string, profile shared.BatchBufferFlushProfile) {
				called = true
				require.Equal(t, "shard1", shardID)
				require.Equal(t, "orders", tableName)
				require.Equal(t, 3, profile.BSIValueEntryCount)
			},
		},
	}

	router.publishFlushProfile("shard1", "orders", shared.BatchBufferFlushProfile{
		TotalElapsed:       time.Millisecond,
		BSIValueEntryCount: 3,
	})
	require.True(t, called)
}

func TestSessionRouterPublishesDrainProfileOnClose(t *testing.T) {
	var (
		mu       sync.Mutex
		profiles []RouterDrainWorkerProfile
	)
	router, err := NewSessionRouter(SessionRouterConfig{
		TableCache:    NewTableCacheStruct(),
		Conn:          &shared.Conn{},
		ShardCount:    2,
		ChannelSize:   1,
		FlushInterval: time.Hour,
		OnDrainProfile: func(profile RouterDrainWorkerProfile) {
			mu.Lock()
			defer mu.Unlock()
			profiles = append(profiles, profile)
		},
	})
	require.NoError(t, err)

	require.NoError(t, router.Close())

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, profiles, 2)
	seen := map[string]bool{}
	for _, profile := range profiles {
		seen[profile.ShardID] = true
		require.Zero(t, profile.SessionCount)
		require.Empty(t, profile.Error)
		require.GreaterOrEqual(t, profile.Elapsed, time.Duration(0))
	}
	require.True(t, seen["shard0"])
	require.True(t, seen["shard1"])
}

func TestSessionRouterCloseDoesNotWaitForIdleFlushInterval(t *testing.T) {
	router, err := NewSessionRouter(SessionRouterConfig{
		TableCache:    NewTableCacheStruct(),
		Conn:          &shared.Conn{},
		ShardCount:    1,
		ChannelSize:   1,
		FlushInterval: 500 * time.Millisecond,
	})
	require.NoError(t, err)
	time.Sleep(25 * time.Millisecond)

	startedAt := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- router.Close()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
		require.Less(t, time.Since(startedAt), 100*time.Millisecond)
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("router Close waited for idle flush interval")
	}
}

func TestSessionRouterDoesNotPublishStaleFlushProfile(t *testing.T) {
	called := false
	router := &SessionRouter{
		cfg: SessionRouterConfig{
			OnFlushProfile: func(shardID string, tableName string, profile shared.BatchBufferFlushProfile) {
				called = true
			},
		},
	}
	finishedAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	profile := shared.BatchBufferFlushProfile{
		FinishedAt:         finishedAt,
		TotalElapsed:       time.Millisecond,
		BSIValueEntryCount: 1,
	}

	router.publishNewFlushProfile("shard1", "orders", profile, profile)
	require.False(t, called)
	router.publishNewFlushProfile("shard1", "orders", profile, shared.BatchBufferFlushProfile{
		FinishedAt:         finishedAt.Add(time.Nanosecond),
		TotalElapsed:       time.Millisecond,
		BSIValueEntryCount: 1,
	})
	require.True(t, called)
}

func TestSessionRouterSplitsShardTableKey(t *testing.T) {
	shardID, tableName := splitShardTableKey("shard2+lineitem")

	require.Equal(t, "shard2", shardID)
	require.Equal(t, "lineitem", tableName)
}

func TestSessionRouterDefaultsPrimaryKeyModeToVerifyExisting(t *testing.T) {
	router, err := NewSessionRouter(SessionRouterConfig{
		TableCache:    NewTableCacheStruct(),
		Conn:          &shared.Conn{},
		ShardCount:    1,
		ChannelSize:   1,
		FlushInterval: time.Nanosecond,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, router.Close()) }()

	require.Equal(t, PrimaryKeyModeVerifyExisting, router.cfg.PrimaryKeyMode)
}

func TestSessionRouterPreservesExplicitAssumeNewPrimaryKeyMode(t *testing.T) {
	router, err := NewSessionRouter(SessionRouterConfig{
		TableCache:     NewTableCacheStruct(),
		Conn:           &shared.Conn{},
		ShardCount:     1,
		ChannelSize:    1,
		FlushInterval:  time.Nanosecond,
		PrimaryKeyMode: PrimaryKeyModeAssumeNew,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, router.Close()) }()

	require.Equal(t, PrimaryKeyModeAssumeNew, router.cfg.PrimaryKeyMode)
}

func TestSessionRouterSnapshotReportsQueueAndSessions(t *testing.T) {
	router := &SessionRouter{
		cfg: SessionRouterConfig{
			ChannelSize:    3,
			PrimaryKeyMode: PrimaryKeyModeVerifyExisting,
		},
		shardChannels: map[string]chan IngestRecord{
			"shard0": make(chan IngestRecord, 3),
			"shard1": make(chan IngestRecord, 3),
		},
	}
	router.shardChannels["shard0"] <- IngestRecord{TableName: "orders"}
	router.shardChannels["shard0"] <- IngestRecord{TableName: "lineitem"}
	router.sessionCache.Store("shard0+orders", &Session{})

	stats := router.Snapshot()

	require.Equal(t, 2, stats.ShardCount)
	require.Equal(t, 3, stats.ChannelSize)
	require.Equal(t, PrimaryKeyModeVerifyExisting, stats.PrimaryKeyMode)
	require.Equal(t, 2, stats.TotalQueued)
	require.Equal(t, 6, stats.TotalCapacity)
	require.Equal(t, 1, stats.OpenSessionCount)
	require.Equal(t, SessionRouterShard{Queued: 2, Capacity: 3}, stats.Shards["shard0"])
	require.Equal(t, SessionRouterShard{Queued: 0, Capacity: 3}, stats.Shards["shard1"])
}

func TestIngestRecordRouteShardKeyPrefersBuildShardKey(t *testing.T) {
	record := IngestRecord{
		ShardKey:      "pk:lineitem:l_orderkey=1",
		BuildShardKey: "tq:lineitem:l_shipdate:826848000000000000",
	}

	require.Equal(t, "tq:lineitem:l_shipdate:826848000000000000", record.RouteShardKey())
}

func TestSessionRouterEnqueueRoutesByBuildShardKey(t *testing.T) {
	router := &SessionRouter{
		hashTable: rendezvous.New([]string{"shard0", "shard1", "shard2"}),
		shardChannels: map[string]chan IngestRecord{
			"shard0": make(chan IngestRecord, 1),
			"shard1": make(chan IngestRecord, 1),
			"shard2": make(chan IngestRecord, 1),
		},
	}
	record := IngestRecord{
		TableName:     "lineitem",
		ShardKey:      "pk:lineitem:l_orderkey=1001|l_linenumber=2",
		BuildShardKey: "tq:lineitem:l_shipdate:826848000000000000",
	}
	wantShard := router.hashTable.GetN(1, record.BuildShardKey)[0]

	require.NoError(t, router.Enqueue(record))

	select {
	case got := <-router.shardChannels[wantShard]:
		require.Equal(t, record.ShardKey, got.ShardKey)
		require.Equal(t, record.BuildShardKey, got.BuildShardKey)
	default:
		t.Fatalf("record was not routed to build shard %s", wantShard)
	}
}

func TestSessionRouterConfiguresSessionPrimaryKeyResolver(t *testing.T) {
	customResolver := &recordingPrimaryKeyResolver{}
	session := &Session{}
	router := &SessionRouter{
		cfg: SessionRouterConfig{
			PrimaryKeyResolverFactory: func(opened *Session) PrimaryKeyResolver {
				require.Same(t, session, opened)
				return customResolver
			},
		},
	}

	router.configureSessionResolver(session)

	require.Same(t, customResolver, session.primaryKeyResolver)
}

package core

import (
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/require"
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

package core

import (
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/require"
)

func TestRouterPutRowProfileAggregatesResultTimings(t *testing.T) {
	var profile RouterPutRowProfile
	callback := profile.Callback()

	callback("shard0", IngestRecord{TableName: "orders"}, PutRowResult{
		TableName:             "orders",
		ChildRowCount:         3,
		LogicalRowCount:       4,
		Inserted:              true,
		SourceElapsed:         time.Millisecond,
		IdentityElapsed:       2 * time.Millisecond,
		AlternateKeysElapsed:  3 * time.Millisecond,
		ChildExpansionElapsed: 4 * time.Millisecond,
		RelationElapsed:       5 * time.Millisecond,
		AttributeElapsed:      6 * time.Millisecond,
		TotalElapsed:          21 * time.Millisecond,
		PrimaryKey: PrimaryKeyResolveProfile{
			ResolveCount:                 4,
			LookupRequiredCount:          4,
			AssumeNewCount:               2,
			LocalCacheLookupCount:        4,
			LocalCacheHitCount:           1,
			SkippedLocalCacheLookupCount: 2,
			KVLookupCount:                3,
			KVHitCount:                   1,
			SkippedKVLookupCount:         2,
			RownumAllocationCount:        2,
			BatchCacheWriteCount:         2,
			TotalElapsed:                 8 * time.Millisecond,
			KVLookupElapsed:              3 * time.Millisecond,
			RownumAllocationElapsed:      time.Millisecond,
			BatchCacheWriteElapsed:       time.Millisecond,
		},
	})
	callback("shard1", IngestRecord{TableName: "orders"}, PutRowResult{
		TableName:    "orders",
		ExistingRow:  true,
		TotalElapsed: 7 * time.Millisecond,
		PrimaryKey: PrimaryKeyResolveProfile{
			ResolveCount:          1,
			LookupRequiredCount:   1,
			LocalCacheLookupCount: 1,
			KVLookupCount:         1,
			KVHitCount:            1,
			TotalElapsed:          2 * time.Millisecond,
			KVLookupElapsed:       time.Millisecond,
		},
	})

	snapshot := profile.Snapshot()

	require.Equal(t, 2, snapshot.RecordCount)
	require.Equal(t, 3, snapshot.ChildRowCount)
	require.Equal(t, 5, snapshot.LogicalRowCount)
	require.Equal(t, 1, snapshot.InsertedCount)
	require.Equal(t, 1, snapshot.ExistingCount)
	require.Equal(t, 28*time.Millisecond, snapshot.TotalElapsed)
	require.Equal(t, time.Millisecond, snapshot.SourceElapsed)
	require.Equal(t, 2*time.Millisecond, snapshot.IdentityElapsed)
	require.Equal(t, 3*time.Millisecond, snapshot.AlternateKeysElapsed)
	require.Equal(t, 4*time.Millisecond, snapshot.ChildExpansionElapsed)
	require.Equal(t, 5*time.Millisecond, snapshot.RelationElapsed)
	require.Equal(t, 6*time.Millisecond, snapshot.AttributeElapsed)
	require.Equal(t, PrimaryKeyResolveProfile{
		ResolveCount:                 5,
		LookupRequiredCount:          5,
		AssumeNewCount:               2,
		LocalCacheLookupCount:        5,
		LocalCacheHitCount:           1,
		SkippedLocalCacheLookupCount: 2,
		KVLookupCount:                4,
		KVHitCount:                   2,
		SkippedKVLookupCount:         2,
		RownumAllocationCount:        2,
		BatchCacheWriteCount:         2,
		TotalElapsed:                 10 * time.Millisecond,
		KVLookupElapsed:              4 * time.Millisecond,
		RownumAllocationElapsed:      time.Millisecond,
		BatchCacheWriteElapsed:       time.Millisecond,
	}, snapshot.PrimaryKey)
	require.Equal(t, RouterPutRowProfileCounter{
		RecordCount:     2,
		ChildRowCount:   3,
		LogicalRowCount: 5,
		TotalElapsed:    28 * time.Millisecond,
		PrimaryKey: PrimaryKeyResolveProfile{
			ResolveCount:                 5,
			LookupRequiredCount:          5,
			AssumeNewCount:               2,
			LocalCacheLookupCount:        5,
			LocalCacheHitCount:           1,
			SkippedLocalCacheLookupCount: 2,
			KVLookupCount:                4,
			KVHitCount:                   2,
			SkippedKVLookupCount:         2,
			RownumAllocationCount:        2,
			BatchCacheWriteCount:         2,
			TotalElapsed:                 10 * time.Millisecond,
			KVLookupElapsed:              4 * time.Millisecond,
			RownumAllocationElapsed:      time.Millisecond,
			BatchCacheWriteElapsed:       time.Millisecond,
		},
	}, snapshot.ByTable["orders"])
	require.Equal(t, 1, snapshot.ByShard["shard0"].RecordCount)
	require.Equal(t, 1, snapshot.ByShard["shard1"].RecordCount)
}

func TestRouterPutRowProfileSnapshotIsStableCopy(t *testing.T) {
	var profile RouterPutRowProfile
	profile.Observe("shard0", IngestRecord{TableName: "orders"}, PutRowResult{
		TableName:    "orders",
		TotalElapsed: time.Millisecond,
	})

	snapshot := profile.Snapshot()
	snapshot.ByTable["orders"] = RouterPutRowProfileCounter{}

	require.Equal(t, 1, profile.Snapshot().ByTable["orders"].RecordCount)
}

func TestRouterFlushProfileAggregatesFlushTimings(t *testing.T) {
	var profile RouterFlushProfile
	callback := profile.Callback()

	callback("shard0", "orders", shared.BatchBufferFlushProfile{
		TotalElapsed:              11 * time.Millisecond,
		PartitionStringElapsed:    time.Millisecond,
		BitmapSetElapsed:          2 * time.Millisecond,
		BSIValueElapsed:           3 * time.Millisecond,
		PartitionStringBatchCount: 1,
		PartitionStringEntryCount: 2,
		BitmapSetEntryCount:       3,
		BSIValueEntryCount:        4,
	})
	callback("shard1", "orders", shared.BatchBufferFlushProfile{
		TotalElapsed:            7 * time.Millisecond,
		BitmapClearElapsed:      4 * time.Millisecond,
		BSIClearValueElapsed:    5 * time.Millisecond,
		BitmapClearEntryCount:   6,
		BSIClearValueEntryCount: 7,
		Error:                   "flush failed",
	})

	snapshot := profile.Snapshot()

	require.Equal(t, 2, snapshot.FlushCount)
	require.Equal(t, 1, snapshot.ErrorCount)
	require.Equal(t, 18*time.Millisecond, snapshot.TotalElapsed)
	require.Equal(t, time.Millisecond, snapshot.PartitionStringElapsed)
	require.Equal(t, 2*time.Millisecond, snapshot.BitmapSetElapsed)
	require.Equal(t, 4*time.Millisecond, snapshot.BitmapClearElapsed)
	require.Equal(t, 3*time.Millisecond, snapshot.BSIValueElapsed)
	require.Equal(t, 5*time.Millisecond, snapshot.BSIClearValueElapsed)
	require.Equal(t, 1, snapshot.PartitionStringBatchCount)
	require.Equal(t, 2, snapshot.PartitionStringEntryCount)
	require.Equal(t, 3, snapshot.BitmapSetEntryCount)
	require.Equal(t, 6, snapshot.BitmapClearEntryCount)
	require.Equal(t, 4, snapshot.BSIValueEntryCount)
	require.Equal(t, 7, snapshot.BSIClearValueEntryCount)
	require.Equal(t, RouterFlushProfileCounter{
		FlushCount:   2,
		TotalElapsed: 18 * time.Millisecond,
		EntryCount:   22,
		ErrorCount:   1,
	}, snapshot.ByTable["orders"])
	require.Equal(t, 1, snapshot.ByShard["shard0"].FlushCount)
	require.Equal(t, 1, snapshot.ByShard["shard1"].FlushCount)
}

func TestRouterFlushProfileSnapshotIsStableCopy(t *testing.T) {
	var profile RouterFlushProfile
	profile.Observe("shard0", "orders", shared.BatchBufferFlushProfile{
		TotalElapsed:       time.Millisecond,
		BSIValueEntryCount: 1,
	})

	snapshot := profile.Snapshot()
	snapshot.ByTable["orders"] = RouterFlushProfileCounter{}

	require.Equal(t, 1, profile.Snapshot().ByTable["orders"].FlushCount)
}

func TestRouterDrainProfileAggregatesWorkerTimings(t *testing.T) {
	var profile RouterDrainProfile
	callback := profile.Callback()

	callback(RouterDrainWorkerProfile{
		ShardID:      "shard0",
		SessionCount: 2,
		Elapsed:      11 * time.Millisecond,
	})
	callback(RouterDrainWorkerProfile{
		ShardID:      "shard1",
		SessionCount: 1,
		Elapsed:      7 * time.Millisecond,
		Error:        "close failed",
	})

	snapshot := profile.Snapshot()

	require.Equal(t, 2, snapshot.WorkerCount)
	require.Equal(t, 3, snapshot.SessionCount)
	require.Equal(t, 1, snapshot.ErrorCount)
	require.Equal(t, 18*time.Millisecond, snapshot.TotalElapsed)
	require.Equal(t, 11*time.Millisecond, snapshot.MaxElapsed)
	require.Equal(t, RouterDrainProfileCounter{
		WorkerCount:  1,
		SessionCount: 2,
		TotalElapsed: 11 * time.Millisecond,
		MaxElapsed:   11 * time.Millisecond,
	}, snapshot.ByShard["shard0"])
	require.Equal(t, RouterDrainProfileCounter{
		WorkerCount:  1,
		SessionCount: 1,
		TotalElapsed: 7 * time.Millisecond,
		MaxElapsed:   7 * time.Millisecond,
		ErrorCount:   1,
	}, snapshot.ByShard["shard1"])
}

func TestRouterDrainProfileSnapshotIsStableCopy(t *testing.T) {
	var profile RouterDrainProfile
	profile.Observe(RouterDrainWorkerProfile{
		ShardID:      "shard0",
		SessionCount: 1,
		Elapsed:      time.Millisecond,
	})

	snapshot := profile.Snapshot()
	snapshot.ByShard["shard0"] = RouterDrainProfileCounter{}

	require.Equal(t, 1, profile.Snapshot().ByShard["shard0"].WorkerCount)
}

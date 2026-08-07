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
		ChildExpansionElapsed: 4 * time.Millisecond,
		ChildTraversalElapsed: 1500 * time.Microsecond,
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
			BSILookupCount:               3,
			BSIHitCount:                  1,
			SkippedBSILookupCount:        2,
			RownumAllocationCount:        2,
			BatchCacheWriteCount:         2,
			TotalElapsed:                 8 * time.Millisecond,
			BSILookupElapsed:             3 * time.Millisecond,
			RownumAllocationElapsed:      time.Millisecond,
			BatchCacheWriteElapsed:       time.Millisecond,
		},
		PrimaryKeyByTable: map[string]PrimaryKeyResolveProfile{
			"orders": {
				ResolveCount:        1,
				DirectColumnIDCount: 1,
			},
			"lineitem": {
				ResolveCount:        3,
				LookupRequiredCount: 3,
				BSILookupCount:      3,
			},
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
			BSILookupCount:        1,
			BSIHitCount:           1,
			TotalElapsed:          2 * time.Millisecond,
			BSILookupElapsed:      time.Millisecond,
		},
		PrimaryKeyByTable: map[string]PrimaryKeyResolveProfile{
			"orders": {
				ResolveCount:        1,
				LookupRequiredCount: 1,
				BSILookupCount:      1,
				BSIHitCount:         1,
			},
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
	require.Equal(t, 4*time.Millisecond, snapshot.ChildExpansionElapsed)
	require.Equal(t, 1500*time.Microsecond, snapshot.ChildTraversalElapsed)
	require.Equal(t, 5*time.Millisecond, snapshot.RelationElapsed)
	require.Equal(t, 6*time.Millisecond, snapshot.AttributeElapsed)
	require.Equal(t, PrimaryKeyResolveProfile{
		ResolveCount:                 5,
		LookupRequiredCount:          5,
		AssumeNewCount:               2,
		LocalCacheLookupCount:        5,
		LocalCacheHitCount:           1,
		SkippedLocalCacheLookupCount: 2,
		BSILookupCount:               4,
		BSIHitCount:                  2,
		SkippedBSILookupCount:        2,
		RownumAllocationCount:        2,
		BatchCacheWriteCount:         2,
		TotalElapsed:                 10 * time.Millisecond,
		BSILookupElapsed:             4 * time.Millisecond,
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
			BSILookupCount:               4,
			BSIHitCount:                  2,
			SkippedBSILookupCount:        2,
			RownumAllocationCount:        2,
			BatchCacheWriteCount:         2,
			TotalElapsed:                 10 * time.Millisecond,
			BSILookupElapsed:             4 * time.Millisecond,
			RownumAllocationElapsed:      time.Millisecond,
			BatchCacheWriteElapsed:       time.Millisecond,
		},
	}, snapshot.ByTable["orders"])
	require.Equal(t, PrimaryKeyResolveProfile{
		ResolveCount:        2,
		LookupRequiredCount: 1,
		DirectColumnIDCount: 1,
		BSILookupCount:      1,
		BSIHitCount:         1,
	}, snapshot.PrimaryKeyByTable["orders"])
	require.Equal(t, PrimaryKeyResolveProfile{
		ResolveCount:        3,
		LookupRequiredCount: 3,
		BSILookupCount:      3,
	}, snapshot.PrimaryKeyByTable["lineitem"])
	require.Equal(t, 1, snapshot.ByShard["shard0"].RecordCount)
	require.Equal(t, 1, snapshot.ByShard["shard1"].RecordCount)
}

func TestPrimaryKeyResolveProfileAggregatesBSIFallbackReasons(t *testing.T) {
	var first PrimaryKeyResolveProfile
	first.RecordBSIFallback("primary_key_field_not_bsi_backed")
	first.RecordBSIFallback("primary_key_field_not_bsi_backed")
	var second PrimaryKeyResolveProfile
	second.RecordBSIFallback("compound_not_encodable")

	combined := first.add(second)

	require.Equal(t, 3, combined.BSIFallbackCount)
	require.Equal(t, map[string]int{
		"primary_key_field_not_bsi_backed": 2,
		"compound_not_encodable":           1,
	}, combined.BSIFallbackReasons)
}

func TestRouterPutRowProfileSnapshotIsStableCopy(t *testing.T) {
	var profile RouterPutRowProfile
	profile.Observe("shard0", IngestRecord{TableName: "orders"}, PutRowResult{
		TableName:    "orders",
		TotalElapsed: time.Millisecond,
		PrimaryKey: PrimaryKeyResolveProfile{
			ResolveCount: 1,
		},
	})

	snapshot := profile.Snapshot()
	snapshot.ByTable["orders"] = RouterPutRowProfileCounter{}
	snapshot.PrimaryKeyByTable["orders"] = PrimaryKeyResolveProfile{}

	require.Equal(t, 1, profile.Snapshot().ByTable["orders"].RecordCount)
	require.Equal(t, 1, profile.Snapshot().PrimaryKeyByTable["orders"].ResolveCount)
}

func TestRouterFlushProfileAggregatesFlushTimings(t *testing.T) {
	var profile RouterFlushProfile
	callback := profile.Callback()

	callback("shard0", "orders", shared.BatchBufferFlushProfile{
		TotalElapsed:              11 * time.Millisecond,
		PartitionStringElapsed:    time.Millisecond,
		BitmapSetElapsed:          2 * time.Millisecond,
		BSIValueElapsed:           3 * time.Millisecond,
		BSIValueRoute:             4 * time.Millisecond,
		BSIValueBuild:             5 * time.Millisecond,
		BSIValueMarshal:           6 * time.Millisecond,
		BSIValueStream:            7 * time.Millisecond,
		BSIValueStreamOpen:        8 * time.Millisecond,
		BSIValueStreamSend:        9 * time.Millisecond,
		BSIValueStreamClose:       10 * time.Millisecond,
		BSIValueStreamMax:         12 * time.Millisecond,
		PartitionStringPutCalls:   1,
		PartitionStringBatchCount: 1,
		PartitionStringEntryCount: 2,
		BitmapSetEntryCount:       3,
		BSIValueEntryCount:        4,
		BSIValueBatchCount:        5,
		BSIValueRoutedItemCount:   6,
	})
	callback("shard1", "orders", shared.BatchBufferFlushProfile{
		TotalElapsed:            7 * time.Millisecond,
		BitmapClearElapsed:      4 * time.Millisecond,
		BSIValueStreamMax:       2 * time.Millisecond,
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
	require.Equal(t, 4*time.Millisecond, snapshot.BSIValueRoute)
	require.Equal(t, 5*time.Millisecond, snapshot.BSIValueBuild)
	require.Equal(t, 6*time.Millisecond, snapshot.BSIValueMarshal)
	require.Equal(t, 7*time.Millisecond, snapshot.BSIValueStream)
	require.Equal(t, 8*time.Millisecond, snapshot.BSIValueStreamOpen)
	require.Equal(t, 9*time.Millisecond, snapshot.BSIValueStreamSend)
	require.Equal(t, 10*time.Millisecond, snapshot.BSIValueStreamClose)
	require.Equal(t, 12*time.Millisecond, snapshot.BSIValueStreamMax)
	require.Equal(t, 5*time.Millisecond, snapshot.BSIClearValueElapsed)
	require.Equal(t, 1, snapshot.PartitionStringPutCalls)
	require.Equal(t, 1, snapshot.PartitionStringBatchCount)
	require.Equal(t, 2, snapshot.PartitionStringEntryCount)
	require.Equal(t, 3, snapshot.BitmapSetEntryCount)
	require.Equal(t, 6, snapshot.BitmapClearEntryCount)
	require.Equal(t, 4, snapshot.BSIValueEntryCount)
	require.Equal(t, 5, snapshot.BSIValueBatchCount)
	require.Equal(t, 6, snapshot.BSIValueRoutedItemCount)
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

package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRouterPutRowProfileAggregatesResultTimings(t *testing.T) {
	var profile RouterPutRowProfile
	callback := profile.Callback()

	callback("shard0", IngestRecord{TableName: "orders"}, PutRowResult{
		TableName:             "orders",
		Inserted:              true,
		SourceElapsed:         time.Millisecond,
		IdentityElapsed:       2 * time.Millisecond,
		AlternateKeysElapsed:  3 * time.Millisecond,
		ChildExpansionElapsed: 4 * time.Millisecond,
		RelationElapsed:       5 * time.Millisecond,
		AttributeElapsed:      6 * time.Millisecond,
		TotalElapsed:          21 * time.Millisecond,
	})
	callback("shard1", IngestRecord{TableName: "orders"}, PutRowResult{
		TableName:    "orders",
		ExistingRow:  true,
		TotalElapsed: 7 * time.Millisecond,
	})

	snapshot := profile.Snapshot()

	require.Equal(t, 2, snapshot.RecordCount)
	require.Equal(t, 1, snapshot.InsertedCount)
	require.Equal(t, 1, snapshot.ExistingCount)
	require.Equal(t, 28*time.Millisecond, snapshot.TotalElapsed)
	require.Equal(t, time.Millisecond, snapshot.SourceElapsed)
	require.Equal(t, 2*time.Millisecond, snapshot.IdentityElapsed)
	require.Equal(t, 3*time.Millisecond, snapshot.AlternateKeysElapsed)
	require.Equal(t, 4*time.Millisecond, snapshot.ChildExpansionElapsed)
	require.Equal(t, 5*time.Millisecond, snapshot.RelationElapsed)
	require.Equal(t, 6*time.Millisecond, snapshot.AttributeElapsed)
	require.Equal(t, RouterPutRowProfileCounter{
		RecordCount:  2,
		TotalElapsed: 28 * time.Millisecond,
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

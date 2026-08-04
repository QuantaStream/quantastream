package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recordingIngestEnqueuer struct {
	records []IngestRecord
	err     error
}

func (r *recordingIngestEnqueuer) Enqueue(record IngestRecord) error {
	if r.err != nil {
		return r.err
	}
	r.records = append(r.records, record)
	return nil
}

func TestBuildSelectedIngestRecordUsesSelectorAndShardPolicy(t *testing.T) {
	eventTime := time.Date(2026, 8, 4, 13, 14, 15, 0, time.UTC)
	orders := ingestSelectorTable("orders", `payload.kind = "order"`)
	orders.PrimaryKey = "o_orderkey"

	result, diagnostics, err := BuildSelectedIngestRecord(IngestRouteRequest{
		Tables: []*Table{
			ingestSelectorTable("customers", `payload.kind = "customer"`),
			orders,
		},
		Envelope: map[string]interface{}{
			"source":   "tpch-stream",
			"event_id": "evt-1",
		},
		Payload: map[string]interface{}{
			"kind":       "order",
			"o_orderkey": 1001,
		},
		EventTime:    eventTime,
		SourceOffset: "partition-0:22",
		DedupTTL:     48 * time.Hour,
	})

	require.False(t, diagnostics.BlocksNative(), "%#v", diagnostics)
	require.NoError(t, err)
	require.Equal(t, "orders", result.Record.TableName)
	require.Equal(t, "dedup:tpch-stream:evt-1", result.Record.ShardKey)
	require.Equal(t, IngestShardKeyDedup, result.ShardKey.Mode)
	require.Equal(t, "evt-1", result.Record.EventID)
	require.Equal(t, "tpch-stream", result.Record.Source)
	require.Equal(t, eventTime, result.Record.EventTime)
	require.Equal(t, "partition-0:22", result.Record.SourceOffset)
	require.Equal(t, 48*time.Hour, result.Record.DedupTTL)
}

func TestRouteSelectedIngestRecordEnqueuesBuiltRecord(t *testing.T) {
	enqueuer := &recordingIngestEnqueuer{}
	orders := ingestSelectorTable("orders", `payload.kind = "order"`)
	orders.PrimaryKey = "o_orderkey"

	result, diagnostics, err := RouteSelectedIngestRecord(enqueuer, IngestRouteRequest{
		Tables: []*Table{orders},
		Payload: map[string]interface{}{
			"kind":       "order",
			"o_orderkey": 1001,
		},
	})

	require.False(t, diagnostics.BlocksNative(), "%#v", diagnostics)
	require.NoError(t, err)
	require.True(t, result.Enqueued)
	require.Len(t, enqueuer.records, 1)
	require.Equal(t, "orders", enqueuer.records[0].TableName)
	require.Equal(t, result.Record.ShardKey, enqueuer.records[0].ShardKey)
	require.Equal(t, IngestShardKeyPrimaryKey, result.ShardKey.Mode)
}

func TestBuildSelectedIngestRecordReturnsErrorWhenNoSelectorMatches(t *testing.T) {
	result, diagnostics, err := BuildSelectedIngestRecord(IngestRouteRequest{
		Tables: []*Table{ingestSelectorTable("orders", `payload.kind = "order"`)},
		Payload: map[string]interface{}{
			"kind": "customer",
		},
	})

	require.False(t, diagnostics.BlocksNative(), "%#v", diagnostics)
	require.Error(t, err)
	require.False(t, result.Selector.Matched)
}

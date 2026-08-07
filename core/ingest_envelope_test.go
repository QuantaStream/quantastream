package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewStreamIngestEnvelopeNormalizesMetadata(t *testing.T) {
	eventTime := time.Date(2026, 8, 4, 15, 16, 17, 0, time.UTC)
	payload := map[string]interface{}{"kind": "order"}
	headers := map[string]interface{}{"partition": "0"}

	envelope, err := NewStreamIngestEnvelope(StreamIngestEnvelopeRequest{
		EventID:      "evt-1",
		Source:       "kafka:tpch-orders",
		EventTime:    eventTime,
		SourceOffset: "0:42",
		Payload:      payload,
		Headers:      headers,
	})
	payload["kind"] = "mutated"
	headers["partition"] = "1"

	require.NoError(t, err)
	require.Equal(t, IngestSourceStream, envelope.Mode)
	require.Equal(t, "evt-1", envelope.EventID)
	require.Equal(t, "kafka:tpch-orders", envelope.Source)
	require.Equal(t, eventTime, envelope.EventTime)
	require.Equal(t, "0:42", envelope.SourceOffset)
	require.Equal(t, "order", envelope.Payload["kind"])
	require.Equal(t, "0", envelope.Headers["partition"])

	envelopeMap := envelope.EnvelopeMap()
	require.Equal(t, "stream", envelopeMap["mode"])
	require.Equal(t, "evt-1", envelopeMap["event_id"])
	require.Equal(t, "kafka:tpch-orders", envelopeMap["source"])
	require.Equal(t, eventTime, envelopeMap["event_time"])
	require.Equal(t, "0:42", envelopeMap["source_offset"])
	require.Equal(t, "0", envelopeMap["headers"].(map[string]interface{})["partition"])
}

func TestNewBatchIngestEnvelopeNormalizesBatchModeWithoutEventID(t *testing.T) {
	envelope, err := NewBatchIngestEnvelope(BatchIngestEnvelopeRequest{
		Source:       "tpch-file:orders.tbl",
		SourceOffset: "row:1",
		Payload: map[string]interface{}{
			"kind":       "order",
			"o_orderkey": 1001,
		},
	})

	require.NoError(t, err)
	require.Equal(t, IngestSourceBatch, envelope.Mode)
	require.Empty(t, envelope.EventID)

	envelopeMap := envelope.EnvelopeMap()
	require.Equal(t, "batch", envelopeMap["mode"])
	require.Equal(t, "tpch-file:orders.tbl", envelopeMap["source"])
	require.Equal(t, "row:1", envelopeMap["source_offset"])
	require.NotContains(t, envelopeMap, "event_id")
}

func TestIngestEnvelopeRouteRequestBridgesNormalizedMetadata(t *testing.T) {
	eventTime := time.Date(2026, 8, 4, 15, 16, 17, 0, time.UTC)
	envelope, err := NewStreamIngestEnvelope(StreamIngestEnvelopeRequest{
		EventID:      "evt-1",
		Source:       "kafka:tpch-orders",
		EventTime:    eventTime,
		SourceOffset: "0:42",
		Payload:      map[string]interface{}{"kind": "order"},
	})
	require.NoError(t, err)

	routeRequest := envelope.RouteRequest(IngestEnvelopeRouteOptions{
		ExplicitShardKey: "manual-shard",
		BuildShardKey:    "tq:orders:o_orderdate:1785801600000000000",
		PayloadHash:      123,
		DedupTTL:         time.Hour,
	})

	require.Equal(t, map[string]interface{}{
		"mode":          "stream",
		"event_id":      "evt-1",
		"source":        "kafka:tpch-orders",
		"event_time":    eventTime,
		"source_offset": "0:42",
	}, routeRequest.Envelope)
	require.Equal(t, map[string]interface{}{"kind": "order"}, routeRequest.Payload)
	require.Equal(t, "manual-shard", routeRequest.ExplicitShardKey)
	require.Equal(t, "tq:orders:o_orderdate:1785801600000000000", routeRequest.BuildShardKey)
	require.Equal(t, "evt-1", routeRequest.EventID)
	require.Equal(t, "kafka:tpch-orders", routeRequest.Source)
	require.Equal(t, eventTime, routeRequest.EventTime)
	require.Equal(t, "0:42", routeRequest.SourceOffset)
	require.Equal(t, uint64(123), routeRequest.PayloadHash)
	require.Equal(t, time.Hour, routeRequest.DedupTTL)
}

func TestBuildSelectedIngestRecordFromStreamEnvelopeUsesDedupKey(t *testing.T) {
	orders := ingestSelectorTable("orders", `payload.kind = "order" && envelope.mode = "stream"`)
	orders.PrimaryKey = "o_orderkey"
	envelope, err := NewStreamIngestEnvelope(StreamIngestEnvelopeRequest{
		EventID: "evt-1",
		Source:  "kafka:tpch-orders",
		Payload: map[string]interface{}{
			"kind":       "order",
			"o_orderkey": 1001,
		},
	})
	require.NoError(t, err)

	result, diagnostics, err := BuildSelectedIngestRecordFromEnvelope(envelope, IngestEnvelopeRouteOptions{
		Tables: []*Table{orders},
	})

	require.False(t, diagnostics.BlocksNative(), "%#v", diagnostics)
	require.NoError(t, err)
	require.Equal(t, IngestShardKeyDedup, result.ShardKey.Mode)
	require.Equal(t, "dedup:kafka:tpch-orders:evt-1", result.Record.ShardKey)
	require.Equal(t, "orders", result.Record.TableName)
}

func TestRouteSelectedIngestEnvelopeForBatchFallsBackToPrimaryKey(t *testing.T) {
	enqueuer := &recordingIngestEnqueuer{}
	orders := ingestSelectorTable("orders", `payload.kind = "order" && envelope.mode = "batch"`)
	orders.PrimaryKey = "o_orderkey"
	envelope, err := NewBatchIngestEnvelope(BatchIngestEnvelopeRequest{
		Source:       "tpch-file:orders.tbl",
		SourceOffset: "row:1",
		Payload: map[string]interface{}{
			"kind":       "order",
			"o_orderkey": 1001,
		},
	})
	require.NoError(t, err)

	result, diagnostics, err := RouteSelectedIngestEnvelope(enqueuer, envelope, IngestEnvelopeRouteOptions{
		Tables: []*Table{orders},
	})

	require.False(t, diagnostics.BlocksNative(), "%#v", diagnostics)
	require.NoError(t, err)
	require.True(t, result.Enqueued)
	require.Equal(t, IngestShardKeyPrimaryKey, result.ShardKey.Mode)
	require.Len(t, enqueuer.records, 1)
	require.Equal(t, result.Record.ShardKey, enqueuer.records[0].ShardKey)
}

func TestIngestEnvelopeRequiresKnownModeAndPayload(t *testing.T) {
	_, err := (IngestEnvelope{Mode: IngestSourceMode("unknown"), Payload: map[string]interface{}{}}).validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "not supported")

	_, err = (IngestEnvelope{Mode: IngestSourceStream}).validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "payload is required")
}

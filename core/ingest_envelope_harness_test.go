package core

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIngestEnvelopeHarnessRoutesEnvelopesAndProfilesSyntheticResults(t *testing.T) {
	orders := ingestSelectorTable("orders", `payload.kind = "order"`)
	orders.PrimaryKey = "o_orderkey"
	customers := ingestSelectorTable("customers", `payload.kind = "customer"`)
	customers.PrimaryKey = "c_custkey"
	streamEnvelope, err := NewStreamIngestEnvelope(StreamIngestEnvelopeRequest{
		EventID: "evt-1",
		Source:  "kafka:tpch-orders",
		Payload: map[string]interface{}{
			"kind":       "order",
			"o_orderkey": 1001,
		},
	})
	require.NoError(t, err)
	batchEnvelope, err := NewBatchIngestEnvelope(BatchIngestEnvelopeRequest{
		Source:       "tpch-file:customer.tbl",
		SourceOffset: "row:1",
		Payload: map[string]interface{}{
			"kind":      "customer",
			"c_custkey": 7,
		},
	})
	require.NoError(t, err)

	enqueuer := &InMemoryIngestRecordEnqueuer{}
	profile := &RouterPutRowProfile{}
	harness := IngestEnvelopeHarness{
		Enqueuer: enqueuer,
		Profile:  profile,
		ObservedResult: func(index int, route IngestRouteResult) (string, PutRowResult, bool) {
			return fmt.Sprintf("shard%d", index), PutRowResult{
				TableName:    route.Record.TableName,
				Inserted:     true,
				TotalElapsed: time.Duration(index+1) * time.Millisecond,
			}, true
		},
	}

	result, diagnostics, err := harness.Run([]IngestEnvelope{streamEnvelope, batchEnvelope}, IngestEnvelopeRouteOptions{
		Tables: []*Table{orders, customers},
	})

	require.False(t, diagnostics.BlocksNative(), "%#v", diagnostics)
	require.NoError(t, err)
	require.Len(t, result.Routes, 2)
	require.True(t, result.Routes[0].Enqueued)
	require.Equal(t, "orders", result.Routes[0].Record.TableName)
	require.Equal(t, IngestShardKeyDedup, result.Routes[0].ShardKey.Mode)
	require.Equal(t, "customers", result.Routes[1].Record.TableName)
	require.Equal(t, IngestShardKeyPrimaryKey, result.Routes[1].ShardKey.Mode)

	records := enqueuer.Records()
	require.Len(t, records, 2)
	require.Equal(t, "orders", records[0].TableName)
	require.Equal(t, "customers", records[1].TableName)

	require.Equal(t, 2, result.Profile.RecordCount)
	require.Equal(t, 2, result.Profile.InsertedCount)
	require.Equal(t, 3*time.Millisecond, result.Profile.TotalElapsed)
	require.Equal(t, 1, result.Profile.ByTable["orders"].RecordCount)
	require.Equal(t, time.Millisecond, result.Profile.ByTable["orders"].TotalElapsed)
	require.Equal(t, 1, result.Profile.ByTable["customers"].RecordCount)
	require.Equal(t, 2*time.Millisecond, result.Profile.ByTable["customers"].TotalElapsed)
	require.Equal(t, 1, result.Profile.ByShard["shard0"].RecordCount)
	require.Equal(t, 1, result.Profile.ByShard["shard1"].RecordCount)
}

func TestIngestEnvelopeHarnessReturnsPartialRoutesOnError(t *testing.T) {
	orders := ingestSelectorTable("orders", `payload.kind = "order"`)
	orders.PrimaryKey = "o_orderkey"
	first, err := NewBatchIngestEnvelope(BatchIngestEnvelopeRequest{
		Payload: map[string]interface{}{
			"kind":       "order",
			"o_orderkey": 1001,
		},
	})
	require.NoError(t, err)
	second, err := NewBatchIngestEnvelope(BatchIngestEnvelopeRequest{
		Payload: map[string]interface{}{
			"kind": "order",
		},
	})
	require.NoError(t, err)

	result, diagnostics, err := (IngestEnvelopeHarness{
		Enqueuer: &InMemoryIngestRecordEnqueuer{},
	}).Run([]IngestEnvelope{first, second}, IngestEnvelopeRouteOptions{
		Tables: []*Table{orders},
	})

	require.False(t, diagnostics.BlocksNative(), "%#v", diagnostics)
	require.Error(t, err)
	require.Contains(t, err.Error(), "o_orderkey")
	require.Len(t, result.Routes, 1)
}

func TestInMemoryIngestRecordEnqueuerReturnsStableCopies(t *testing.T) {
	enqueuer := &InMemoryIngestRecordEnqueuer{}
	record := IngestRecord{
		TableName: "orders",
		Data:      map[string]interface{}{"kind": "order"},
	}
	require.NoError(t, enqueuer.Enqueue(record))
	record.Data["kind"] = "mutated"

	records := enqueuer.Records()
	require.Equal(t, "order", records[0].Data["kind"])
	records[0].Data["kind"] = "again"

	require.Equal(t, "order", enqueuer.Records()[0].Data["kind"])
}

package core

import (
	"fmt"
	"testing"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/require"
)

func TestResolveIngestShardKeyPrefersExplicitKey(t *testing.T) {
	result, err := ResolveIngestShardKey(IngestShardKeyRequest{
		ExplicitShardKey: "explicit-key",
		Source:           "tpch",
		EventID:          "evt-1",
		Table:            ingestTableWithPrimaryKey("orders", "o_orderkey"),
		Payload:          map[string]interface{}{"o_orderkey": 1001},
	})

	require.NoError(t, err)
	require.Equal(t, "explicit-key", result.ShardKey)
	require.Equal(t, IngestShardKeyExplicit, result.Mode)
}

func TestResolveIngestShardKeyUsesDedupKeyBeforePrimaryKey(t *testing.T) {
	result, err := ResolveIngestShardKey(IngestShardKeyRequest{
		Source:  "tpch",
		EventID: "evt-1",
		Table:   ingestTableWithPrimaryKey("orders", "o_orderkey"),
		Payload: map[string]interface{}{"o_orderkey": 1001},
	})

	require.NoError(t, err)
	require.Equal(t, "dedup:tpch:evt-1", result.ShardKey)
	require.Equal(t, IngestShardKeyDedup, result.Mode)
}

func TestResolveIngestShardKeyFallsBackToCompoundPrimaryKey(t *testing.T) {
	result, err := ResolveIngestShardKey(IngestShardKeyRequest{
		Table: ingestTableWithPrimaryKey("lineitem", "l_orderkey+l_linenumber"),
		Payload: map[string]interface{}{
			"l_orderkey":   1001,
			"l_linenumber": 2,
		},
	})

	require.NoError(t, err)
	require.Equal(t, IngestShardKeyPrimaryKey, result.Mode)
	require.Equal(t, []string{"l_orderkey", "l_linenumber"}, result.Fields)
	require.Contains(t, result.ShardKey, "pk:lineitem:")
	require.Contains(t, result.ShardKey, "l_orderkey=int:4:1001;")
	require.Contains(t, result.ShardKey, "l_linenumber=int:1:2;")
}

func TestResolveIngestShardKeyUsesWrappedPayloadData(t *testing.T) {
	result, err := ResolveIngestShardKey(IngestShardKeyRequest{
		Table: ingestTableWithPrimaryKey("spots_flat", "spot_id"),
		Payload: map[string]interface{}{
			"type": "rbn_spot_flat",
			"data": map[string]interface{}{
				"spot_id": int64(7684299182663830675),
			},
		},
	})

	require.NoError(t, err)
	require.Equal(t, IngestShardKeyPrimaryKey, result.Mode)
	require.Equal(t, []string{"spot_id"}, result.Fields)
	require.Contains(t, result.ShardKey, "pk:spots_flat:")
	require.Contains(t, result.ShardKey, "spot_id=int:")
}

func TestResolveIngestShardKeyReportsMissingPrimaryKeyField(t *testing.T) {
	_, err := ResolveIngestShardKey(IngestShardKeyRequest{
		Table:   ingestTableWithPrimaryKey("orders", "o_orderkey"),
		Payload: map[string]interface{}{},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "o_orderkey")
}

func TestBuildIngestDedupKeyRequiresSourceAndEvent(t *testing.T) {
	_, err := BuildIngestDedupKey("", "evt-1")
	require.Error(t, err)
	_, err = BuildIngestDedupKey("tpch", "")
	require.Error(t, err)

	key, err := BuildIngestDedupKey("tpch", "evt-1")
	require.NoError(t, err)
	require.Equal(t, "dedup:tpch:evt-1", key)
}

func TestResolveIngestBuildShardKeyUsesTimeQuantumField(t *testing.T) {
	table := &shared.BasicTable{
		Name:             "lineitem",
		TimeQuantumType:  "YMD",
		TimeQuantumField: "l_shipdate",
	}
	tq, _, err := shared.ToTQTimestamp("YMD", "1996-03-15")
	require.NoError(t, err)

	result, ok := ResolveIngestBuildShardKey(IngestBuildShardKeyRequest{
		Table:   table,
		Payload: map[string]interface{}{"l_shipdate": "1996-03-15"},
	})

	require.True(t, ok)
	require.Equal(t, fmt.Sprintf("tq:lineitem:l_shipdate:%d", tq.UnixNano()), result.BuildShardKey)
	require.Equal(t, IngestBuildShardKeyTimeQuantum, result.Mode)
	require.Equal(t, "l_shipdate", result.Field)
}

func TestResolveIngestBuildShardKeyReturnsFalseWhenTimeQuantumIsUnavailable(t *testing.T) {
	result, ok := ResolveIngestBuildShardKey(IngestBuildShardKeyRequest{
		Table:   &shared.BasicTable{Name: "orders", TimeQuantumType: "YMD", TimeQuantumField: "o_orderdate"},
		Payload: map[string]interface{}{"o_orderdate": "not-a-date"},
	})

	require.False(t, ok)
	require.Empty(t, result.BuildShardKey)
}

func TestClassifyIngestDedup(t *testing.T) {
	incoming := IngestDedupRecord{DedupKey: "dedup:tpch:evt-1", PayloadHash: 123}

	require.Equal(t, IngestDedupNew, ClassifyIngestDedup(nil, incoming))
	require.Equal(t, IngestDedupNew, ClassifyIngestDedup(&IngestDedupRecord{
		DedupKey:    "dedup:tpch:evt-2",
		PayloadHash: 123,
	}, incoming))
	require.Equal(t, IngestDedupDuplicate, ClassifyIngestDedup(&IngestDedupRecord{
		DedupKey:    "dedup:tpch:evt-1",
		PayloadHash: 123,
	}, incoming))
	require.Equal(t, IngestDedupConflict, ClassifyIngestDedup(&IngestDedupRecord{
		DedupKey:    "dedup:tpch:evt-1",
		PayloadHash: 456,
	}, incoming))
}

func ingestTableWithPrimaryKey(name, primaryKey string) *Table {
	return &Table{BasicTable: &shared.BasicTable{Name: name, PrimaryKey: primaryKey}}
}

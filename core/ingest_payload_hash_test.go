package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHashIngestPayloadStableAcrossMapOrder(t *testing.T) {
	eventTime := time.Date(2026, 8, 4, 10, 11, 12, 123000000, time.UTC)
	left := map[string]interface{}{
		"kind": "order",
		"order": map[string]interface{}{
			"id":        1001,
			"eventTime": eventTime,
			"items": []interface{}{
				map[string]interface{}{"sku": "ABC", "qty": 2},
				map[string]interface{}{"sku": "XYZ", "qty": 1},
			},
		},
	}
	right := map[string]interface{}{
		"order": map[string]interface{}{
			"items": []interface{}{
				map[string]interface{}{"qty": 2, "sku": "ABC"},
				map[string]interface{}{"qty": 1, "sku": "XYZ"},
			},
			"eventTime": eventTime,
			"id":        1001,
		},
		"kind": "order",
	}

	leftHash, err := HashIngestPayload(left)
	require.NoError(t, err)
	rightHash, err := HashIngestPayload(right)
	require.NoError(t, err)

	require.Equal(t, leftHash, rightHash)
}

func TestHashIngestPayloadPreservesArrayOrder(t *testing.T) {
	leftHash, err := HashIngestPayload(map[string]interface{}{
		"values": []interface{}{1, 2, 3},
	})
	require.NoError(t, err)
	rightHash, err := HashIngestPayload(map[string]interface{}{
		"values": []interface{}{3, 2, 1},
	})
	require.NoError(t, err)

	require.NotEqual(t, leftHash, rightHash)
}

func TestHashIngestPayloadRejectsUnsupportedNestedMapKey(t *testing.T) {
	_, err := HashIngestPayload(map[string]interface{}{
		"payload": map[int]interface{}{1: "not-supported"},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "payload map key type")
}

func TestIngestRecordBuildsStablePayloadHashWhenMissing(t *testing.T) {
	record := IngestRecord{
		EventID:        "evt-1",
		Source:         "tpch-stream",
		PrimaryKeyMode: PrimaryKeyModeAssumeNew,
		Data: map[string]interface{}{
			"kind":  "order",
			"order": map[string]interface{}{"id": 1001},
		},
	}
	wantHash, err := HashIngestPayload(record.Data)
	require.NoError(t, err)

	options, err := record.PutRowOptionsWithPayloadHash()
	require.NoError(t, err)

	require.Equal(t, "evt-1", options.EventID)
	require.Equal(t, "tpch-stream", options.Source)
	require.Equal(t, wantHash, options.PayloadHash)
	require.Equal(t, PrimaryKeyModeAssumeNew, options.PrimaryKeyMode)
}

func TestIngestRecordPreservesExplicitPayloadHash(t *testing.T) {
	options, err := (IngestRecord{
		PayloadHash: 77,
		Data: map[string]interface{}{
			"kind": map[int]interface{}{1: "would-fail-if-rehashed"},
		},
	}).PutRowOptionsWithPayloadHash()
	require.NoError(t, err)

	require.Equal(t, uint64(77), options.PayloadHash)
}

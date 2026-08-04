package core

import (
	"testing"
	"time"

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

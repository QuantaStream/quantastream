package qsfixture

import (
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/require"
)

func TestMemoryBSIPrimaryKeyBackendStagesAndLooksUpTypedValues(t *testing.T) {
	backend := NewMemoryBSIPrimaryKeyBackend()
	attr := memoryBSIPrimaryKeyTestAttribute()
	shardTime := time.Unix(10, 20).UTC()

	err := backend.StagePrimaryKey(core.BSIPrimaryKeyStageRequest{
		TableName:      "orders",
		PrimaryKey:     "o_orderkey",
		Attributes:     []*core.Attribute{attr},
		Values:         []interface{}{int64(1001)},
		RenderedValue:  "1001",
		ShardTimestamp: shardTime,
		ColumnID:       42,
	})
	require.NoError(t, err)

	result, err := backend.LookupPrimaryKey(core.BSIPrimaryKeyLookupRequest{
		TableName:      "orders",
		PrimaryKey:     "o_orderkey",
		Attributes:     []*core.Attribute{attr},
		Values:         []interface{}{int64(1001)},
		RenderedValue:  "1001",
		ShardTimestamp: shardTime,
	})

	require.NoError(t, err)
	require.True(t, result.Found)
	require.Equal(t, uint64(42), result.ColumnID)
}

func TestMemoryBSIPrimaryKeyBackendKeepsTypedIdentityDistinct(t *testing.T) {
	backend := NewMemoryBSIPrimaryKeyBackend()
	attr := memoryBSIPrimaryKeyTestAttribute()

	err := backend.StagePrimaryKey(core.BSIPrimaryKeyStageRequest{
		TableName:     "orders",
		PrimaryKey:    "o_orderkey",
		Attributes:    []*core.Attribute{attr},
		Values:        []interface{}{int64(42)},
		RenderedValue: "42",
		ColumnID:      7,
	})
	require.NoError(t, err)

	result, err := backend.LookupPrimaryKey(core.BSIPrimaryKeyLookupRequest{
		TableName:     "orders",
		PrimaryKey:    "o_orderkey",
		Attributes:    []*core.Attribute{attr},
		Values:        []interface{}{"42"},
		RenderedValue: "42",
	})

	require.NoError(t, err)
	require.False(t, result.Found)
	require.Zero(t, result.ColumnID)
}

func TestMemoryBSIPrimaryKeyBackendRejectsConflictingStage(t *testing.T) {
	backend := NewMemoryBSIPrimaryKeyBackend()
	req := core.BSIPrimaryKeyStageRequest{
		TableName:     "orders",
		PrimaryKey:    "o_orderkey",
		Attributes:    []*core.Attribute{memoryBSIPrimaryKeyTestAttribute()},
		Values:        []interface{}{int64(1001)},
		RenderedValue: "1001",
		ColumnID:      42,
	}

	require.NoError(t, backend.StagePrimaryKey(req))
	require.NoError(t, backend.StagePrimaryKey(req))

	req.ColumnID = 99
	err := backend.StagePrimaryKey(req)

	require.Error(t, err)
	require.Contains(t, err.Error(), "conflict")
}

func TestMemoryBSIPrimaryKeyBackendSnapshotIsStableCopy(t *testing.T) {
	backend := NewMemoryBSIPrimaryKeyBackend()
	err := backend.StagePrimaryKey(core.BSIPrimaryKeyStageRequest{
		TableName:     "orders",
		PrimaryKey:    "o_orderkey",
		Values:        []interface{}{int64(1001)},
		RenderedValue: "1001",
		ColumnID:      42,
	})
	require.NoError(t, err)

	snapshot := backend.Snapshot()
	require.Len(t, snapshot, 1)
	for key := range snapshot {
		snapshot[key] = 99
	}

	next := backend.Snapshot()
	for _, columnID := range next {
		require.Equal(t, uint64(42), columnID)
	}
}

func memoryBSIPrimaryKeyTestAttribute() *core.Attribute {
	return &core.Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       "o_orderkey",
			SourceName:      "o_orderkey",
			Type:            "Integer",
			MappingStrategy: "IntBSI",
		},
	}
}

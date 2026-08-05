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
	require.Equal(t, []uint64{42}, result.MatchedColumnIDs)
}

func TestMemoryBSIPrimaryKeyBackendIgnoresRenderedValueForIdentity(t *testing.T) {
	backend := NewMemoryBSIPrimaryKeyBackend()
	attr := memoryBSIPrimaryKeyTestAttribute()

	err := backend.StagePrimaryKey(core.BSIPrimaryKeyStageRequest{
		TableName:     "orders",
		PrimaryKey:    "o_orderkey",
		Attributes:    []*core.Attribute{attr},
		Values:        []interface{}{int64(1001)},
		RenderedValue: "1001",
		ColumnID:      42,
	})
	require.NoError(t, err)

	result, err := backend.LookupPrimaryKey(core.BSIPrimaryKeyLookupRequest{
		TableName:     "orders",
		PrimaryKey:    "o_orderkey",
		Attributes:    []*core.Attribute{attr},
		Values:        []interface{}{int64(1001)},
		RenderedValue: "not-the-authority",
	})

	require.NoError(t, err)
	require.True(t, result.Found)
	require.Equal(t, uint64(42), result.ColumnID)
}

func TestMemoryBSIPrimaryKeyBackendStagesAndLooksUpCompoundValues(t *testing.T) {
	backend := NewMemoryBSIPrimaryKeyBackend()
	orderKey := memoryBSIPrimaryKeyTestAttributeNamed("l_orderkey")
	lineNumber := memoryBSIPrimaryKeyTestAttributeNamed("l_linenumber")

	err := backend.StagePrimaryKey(core.BSIPrimaryKeyStageRequest{
		TableName:     "lineitem",
		PrimaryKey:    "l_orderkey,l_linenumber",
		Attributes:    []*core.Attribute{orderKey, lineNumber},
		Values:        []interface{}{int64(1001), int64(2)},
		RenderedValue: "1001+2",
		ColumnID:      77,
	})
	require.NoError(t, err)

	result, err := backend.LookupPrimaryKey(core.BSIPrimaryKeyLookupRequest{
		TableName:     "lineitem",
		PrimaryKey:    "l_orderkey,l_linenumber",
		Attributes:    []*core.Attribute{orderKey, lineNumber},
		Values:        []interface{}{int64(1001), int64(2)},
		RenderedValue: "presentation-can-change",
	})

	require.NoError(t, err)
	require.True(t, result.Found)
	require.Equal(t, uint64(77), result.ColumnID)
}

func TestMemoryBSIPrimaryKeyBackendPrefersAuthorityValue(t *testing.T) {
	backend := NewMemoryBSIPrimaryKeyBackend()
	orderKey := memoryBSIPrimaryKeyTestAttributeNamed("l_orderkey")
	lineNumber := memoryBSIPrimaryKeyTestAttributeNamed("l_linenumber")
	authorityValue, err := core.EncodeCompoundPrimaryKeyAuthorityValue(core.PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  "lineitem",
		PrimaryKey: "l_orderkey+l_linenumber",
		Attributes: []*core.Attribute{orderKey, lineNumber},
		Values:     []interface{}{int64(1001), int64(2)},
	})
	require.NoError(t, err)

	err = backend.StagePrimaryKey(core.BSIPrimaryKeyStageRequest{
		TableName:      "lineitem",
		PrimaryKey:     "l_orderkey+l_linenumber",
		Attributes:     []*core.Attribute{orderKey, lineNumber},
		Values:         []interface{}{int64(1001), int64(2)},
		AuthorityValue: authorityValue,
		Identity:       []byte("stage-identity-should-not-win"),
		RenderedValue:  "1001+2",
		ColumnID:       77,
	})
	require.NoError(t, err)

	result, err := backend.LookupPrimaryKey(core.BSIPrimaryKeyLookupRequest{
		TableName:      "lineitem",
		PrimaryKey:     "l_orderkey+l_linenumber",
		Attributes:     []*core.Attribute{orderKey, lineNumber},
		Values:         []interface{}{int64(1001), int64(2)},
		AuthorityValue: authorityValue,
		Identity:       []byte("lookup-identity-should-not-win"),
		RenderedValue:  "presentation-can-change",
	})

	require.NoError(t, err)
	require.True(t, result.Found)
	require.Equal(t, uint64(77), result.ColumnID)
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

func TestMemoryBSIPrimaryKeyBackendKeepsCompoundDelimiterValuesDistinct(t *testing.T) {
	backend := NewMemoryBSIPrimaryKeyBackend()
	left := memoryBSIPrimaryKeyTestStringAttribute("left_value")
	right := memoryBSIPrimaryKeyTestStringAttribute("right_value")

	err := backend.StagePrimaryKey(core.BSIPrimaryKeyStageRequest{
		TableName:     "compound",
		PrimaryKey:    "left_value,right_value",
		Attributes:    []*core.Attribute{left, right},
		Values:        []interface{}{"x+0", "y"},
		RenderedValue: "x+0+y",
		ColumnID:      42,
	})
	require.NoError(t, err)

	result, err := backend.LookupPrimaryKey(core.BSIPrimaryKeyLookupRequest{
		TableName:     "compound",
		PrimaryKey:    "left_value,right_value",
		Attributes:    []*core.Attribute{left, right},
		Values:        []interface{}{"x", "0+y"},
		RenderedValue: "x+0+y",
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
		Attributes:    []*core.Attribute{memoryBSIPrimaryKeyTestAttribute()},
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
	return memoryBSIPrimaryKeyTestAttributeNamed("o_orderkey")
}

func memoryBSIPrimaryKeyTestAttributeNamed(fieldName string) *core.Attribute {
	return &core.Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       fieldName,
			SourceName:      fieldName,
			Type:            "Integer",
			MappingStrategy: "IntBSI",
		},
	}
}

func memoryBSIPrimaryKeyTestStringAttribute(fieldName string) *core.Attribute {
	return &core.Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       fieldName,
			SourceName:      fieldName,
			Type:            "String",
			MappingStrategy: "StringLexBSI",
		},
	}
}

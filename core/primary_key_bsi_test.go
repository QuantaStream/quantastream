package core

import (
	"errors"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/require"
)

type recordingBSIPrimaryKeyBackend struct {
	lookupRequests []BSIPrimaryKeyLookupRequest
	stageRequests  []BSIPrimaryKeyStageRequest
	lookupResult   BSIPrimaryKeyLookupResult
	lookupErr      error
	stageErr       error
}

func (b *recordingBSIPrimaryKeyBackend) LookupPrimaryKey(req BSIPrimaryKeyLookupRequest) (BSIPrimaryKeyLookupResult, error) {
	b.lookupRequests = append(b.lookupRequests, req)
	return b.lookupResult, b.lookupErr
}

func (b *recordingBSIPrimaryKeyBackend) StagePrimaryKey(req BSIPrimaryKeyStageRequest) error {
	b.stageRequests = append(b.stageRequests, req)
	return b.stageErr
}

func TestBSIPrimaryKeyResolverUsesTypedLookupValues(t *testing.T) {
	tbuf, pk := newBSIPrimaryKeyTestBuffer()
	tbuf.CurrentTimestamp = time.Unix(123, 456).UTC()
	backend := &recordingBSIPrimaryKeyBackend{
		lookupResult: BSIPrimaryKeyLookupResult{ColumnID: 4242, Found: true},
	}
	resolver := NewBSIPrimaryKeyResolver(backend)

	result, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      tbuf,
		LookupValue:      "1001",
		PrimaryKeyValues: []interface{}{int64(1001)},
	})

	require.NoError(t, err)
	require.True(t, result.ExistingRow)
	require.Equal(t, uint64(4242), result.ColumnID)
	require.Equal(t, uint64(4242), tbuf.CurrentColumnID)
	require.Equal(t, 1, result.Profile.LookupRequiredCount)
	require.Equal(t, 1, result.Profile.BSILookupCount)
	require.Equal(t, 1, result.Profile.BSIHitCount)
	require.Zero(t, result.Profile.BSIStageWriteCount)
	require.Len(t, backend.lookupRequests, 1)
	require.Empty(t, backend.stageRequests)
	lookupReq := backend.lookupRequests[0]
	require.Equal(t, "orders", lookupReq.TableName)
	require.Equal(t, "o_orderkey", lookupReq.PrimaryKey)
	require.Equal(t, []*Attribute{pk}, lookupReq.Attributes)
	require.Equal(t, []interface{}{int64(1001)}, lookupReq.Values)
	require.Equal(t, "1001", lookupReq.RenderedValue)
	require.Equal(t, tbuf.CurrentTimestamp, lookupReq.ShardTimestamp)
	require.Equal(t, PrimaryKeyModeVerifyExisting, lookupReq.PrimaryKeyMode)
}

func TestBSIPrimaryKeyResolverAllocatesAndStagesMiss(t *testing.T) {
	tbuf, pk := newBSIPrimaryKeyTestBuffer()
	tbuf.sequencerCache = map[int64]*shared.Sequencer{
		tbuf.CurrentTimestamp.UnixNano(): shared.NewSequencer(9001, 2),
	}
	backend := &recordingBSIPrimaryKeyBackend{}
	resolver := NewBSIPrimaryKeyResolver(backend)

	result, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      tbuf,
		LookupValue:      "1002",
		PrimaryKeyValues: []interface{}{int64(1002)},
	})

	require.NoError(t, err)
	require.False(t, result.ExistingRow)
	require.Equal(t, uint64(9001), result.ColumnID)
	require.Equal(t, uint64(9001), tbuf.CurrentColumnID)
	require.Equal(t, 1, result.Profile.BSILookupCount)
	require.Equal(t, 1, result.Profile.RownumAllocationCount)
	require.Equal(t, 1, result.Profile.BSIStageWriteCount)
	require.Len(t, backend.lookupRequests, 1)
	require.Len(t, backend.stageRequests, 1)
	stageReq := backend.stageRequests[0]
	require.Equal(t, "orders", stageReq.TableName)
	require.Equal(t, "o_orderkey", stageReq.PrimaryKey)
	require.Equal(t, []*Attribute{pk}, stageReq.Attributes)
	require.Equal(t, []interface{}{int64(1002)}, stageReq.Values)
	require.Equal(t, "1002", stageReq.RenderedValue)
	require.Equal(t, tbuf.CurrentTimestamp, stageReq.ShardTimestamp)
	require.Equal(t, uint64(9001), stageReq.ColumnID)
}

func TestBSIPrimaryKeyResolverAssumeNewSkipsLookupAndStagesProvidedColumnID(t *testing.T) {
	tbuf, _ := newBSIPrimaryKeyTestBuffer()
	backend := &recordingBSIPrimaryKeyBackend{}
	resolver := NewBSIPrimaryKeyResolver(backend)

	result, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      tbuf,
		LookupValue:      "1003",
		PrimaryKeyValues: []interface{}{int64(1003)},
		ProvidedColumnID: 77,
		PrimaryKeyMode:   PrimaryKeyModeAssumeNew,
	})

	require.NoError(t, err)
	require.False(t, result.ExistingRow)
	require.Equal(t, uint64(77), result.ColumnID)
	require.Equal(t, uint64(77), tbuf.CurrentColumnID)
	require.Zero(t, result.Profile.BSILookupCount)
	require.Equal(t, 1, result.Profile.AssumeNewCount)
	require.Equal(t, 1, result.Profile.SkippedBSILookupCount)
	require.Equal(t, 1, result.Profile.ProvidedColumnIDCount)
	require.Equal(t, 1, result.Profile.BSIStageWriteCount)
	require.Empty(t, backend.lookupRequests)
	require.Len(t, backend.stageRequests, 1)
}

func TestBSIPrimaryKeyResolverReturnsLookupErrors(t *testing.T) {
	tbuf, _ := newBSIPrimaryKeyTestBuffer()
	lookupErr := errors.New("lookup failed")
	backend := &recordingBSIPrimaryKeyBackend{lookupErr: lookupErr}
	resolver := NewBSIPrimaryKeyResolver(backend)

	result, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      tbuf,
		LookupValue:      "1004",
		PrimaryKeyValues: []interface{}{int64(1004)},
	})

	require.ErrorIs(t, err, lookupErr)
	require.Zero(t, result.ColumnID)
	require.Equal(t, 1, result.Profile.BSILookupCount)
	require.Zero(t, result.Profile.BSIStageWriteCount)
}

func newBSIPrimaryKeyTestBuffer() (*TableBuffer, *Attribute) {
	table := &Table{BasicTable: &shared.BasicTable{Name: "orders", PrimaryKey: "o_orderkey"}}
	pk := &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:  "o_orderkey",
			SourceName: "o_orderkey",
			Type:       "Integer",
		},
		Parent: table,
	}
	tbuf := &TableBuffer{
		Table:            table,
		CurrentTimestamp: time.Unix(0, 0).UTC(),
		PKAttributes:     []*Attribute{pk},
		PKMap:            map[string]*Attribute{"o_orderkey": pk},
		sequencerCache:   map[int64]*shared.Sequencer{},
	}
	return tbuf, pk
}

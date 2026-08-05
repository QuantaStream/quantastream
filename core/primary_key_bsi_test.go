package core

import (
	"errors"
	"fmt"
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

type mapBSIPrimaryKeyBackend struct {
	rows map[string]uint64
}

func newMapBSIPrimaryKeyBackend() *mapBSIPrimaryKeyBackend {
	return &mapBSIPrimaryKeyBackend{rows: map[string]uint64{}}
}

func (b *mapBSIPrimaryKeyBackend) LookupPrimaryKey(req BSIPrimaryKeyLookupRequest) (BSIPrimaryKeyLookupResult, error) {
	identity := req.Identity
	if len(identity) == 0 {
		encoded, err := EncodeBSIPrimaryKeyLookupIdentity(req)
		if err != nil {
			return BSIPrimaryKeyLookupResult{}, err
		}
		identity = encoded
	}
	columnID, found := b.rows[string(identity)]
	return BSIPrimaryKeyLookupResult{ColumnID: columnID, Found: found}, nil
}

func (b *mapBSIPrimaryKeyBackend) StagePrimaryKey(req BSIPrimaryKeyStageRequest) error {
	identity := req.Identity
	if len(identity) == 0 {
		encoded, err := EncodeBSIPrimaryKeyStageIdentity(req)
		if err != nil {
			return err
		}
		identity = encoded
	}
	key := string(identity)
	if existing, found := b.rows[key]; found && existing != req.ColumnID {
		return fmt.Errorf("compound primary key conflict: existing column ID %d, staged column ID %d", existing, req.ColumnID)
	}
	b.rows[key] = req.ColumnID
	return nil
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
	require.NotEmpty(t, lookupReq.Identity)
	require.Equal(t, "1001", lookupReq.RenderedValue)
	require.Equal(t, tbuf.CurrentTimestamp, lookupReq.ShardTimestamp)
	require.Equal(t, PrimaryKeyModeVerifyExisting, lookupReq.PrimaryKeyMode)
}

func TestBSIPrimaryKeyResolverCarriesCompoundTypedValues(t *testing.T) {
	tbuf, orderKey := newBSIPrimaryKeyTestBuffer()
	lineNumber := &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:  "l_linenumber",
			SourceName: "l_linenumber",
			Type:       "Integer",
		},
		Parent: tbuf.Table,
	}
	tbuf.Table.PrimaryKey = "l_orderkey,l_linenumber"
	tbuf.PKAttributes = []*Attribute{orderKey, lineNumber}
	tbuf.PKMap = map[string]*Attribute{
		"o_orderkey":   orderKey,
		"l_linenumber": lineNumber,
	}
	backend := &recordingBSIPrimaryKeyBackend{
		lookupResult: BSIPrimaryKeyLookupResult{ColumnID: 5150, Found: true},
	}
	resolver := NewBSIPrimaryKeyResolver(backend)

	result, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      tbuf,
		LookupValue:      "1001+2",
		PrimaryKeyValues: []interface{}{int64(1001), int64(2)},
	})

	require.NoError(t, err)
	require.True(t, result.ExistingRow)
	require.Equal(t, uint64(5150), result.ColumnID)
	require.Len(t, backend.lookupRequests, 1)
	lookupReq := backend.lookupRequests[0]
	require.Equal(t, "l_orderkey,l_linenumber", lookupReq.PrimaryKey)
	require.Equal(t, []*Attribute{orderKey, lineNumber}, lookupReq.Attributes)
	require.Equal(t, []interface{}{int64(1001), int64(2)}, lookupReq.Values)
	require.NotEmpty(t, lookupReq.Identity)
	require.Equal(t, "1001+2", lookupReq.RenderedValue)
}

func TestBSIPrimaryKeyResolverReplaysCompoundKeyFromBSIBackend(t *testing.T) {
	backend := newMapBSIPrimaryKeyBackend()
	resolver := NewBSIPrimaryKeyResolver(backend)
	first, _, _ := newCompoundBSIPrimaryKeyTestBuffer()
	first.sequencerCache = map[int64]*shared.Sequencer{
		first.CurrentTimestamp.UnixNano(): shared.NewSequencer(7000, 2),
	}

	insert, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      first,
		LookupValue:      "1001+2",
		PrimaryKeyValues: []interface{}{int64(1001), int64(2)},
	})
	require.NoError(t, err)
	require.False(t, insert.ExistingRow)
	require.Equal(t, uint64(7000), insert.ColumnID)
	require.Equal(t, 1, insert.Profile.BSILookupCount)
	require.Equal(t, 1, insert.Profile.RownumAllocationCount)
	require.Equal(t, 1, insert.Profile.BSIStageWriteCount)

	replayBuffer, _, _ := newCompoundBSIPrimaryKeyTestBuffer()
	replay, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      replayBuffer,
		LookupValue:      "rendered-value-is-not-authority",
		PrimaryKeyValues: []interface{}{int64(1001), int64(2)},
	})

	require.NoError(t, err)
	require.True(t, replay.ExistingRow)
	require.Equal(t, uint64(7000), replay.ColumnID)
	require.Equal(t, uint64(7000), replayBuffer.CurrentColumnID)
	require.Equal(t, 1, replay.Profile.BSILookupCount)
	require.Equal(t, 1, replay.Profile.BSIHitCount)
	require.Zero(t, replay.Profile.BSIStageWriteCount)
	require.Zero(t, replay.Profile.RownumAllocationCount)
}

func TestBSIPrimaryKeyResolverReusesSameBatchCompoundKeyBeforeBackendLookup(t *testing.T) {
	backend := &recordingBSIPrimaryKeyBackend{}
	resolver := NewBSIPrimaryKeyResolver(backend)
	session := &Session{BatchBuffer: shared.NewBatchBuffer(nil, nil, 1000)}
	first, _, _ := newCompoundBSIPrimaryKeyTestBuffer()

	insert, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          session,
		TableBuffer:      first,
		LookupValue:      "1001+2",
		PrimaryKeyValues: []interface{}{int64(1001), int64(2)},
		ProvidedColumnID: 7000,
		PrimaryKeyMode:   PrimaryKeyModeAssumeNew,
	})
	require.NoError(t, err)
	require.False(t, insert.ExistingRow)
	require.Equal(t, uint64(7000), insert.ColumnID)
	require.Equal(t, 1, insert.Profile.LocalCacheLookupCount)
	require.Zero(t, insert.Profile.LocalCacheHitCount)
	require.Equal(t, 1, insert.Profile.AssumeNewCount)
	require.Equal(t, 1, insert.Profile.SkippedBSILookupCount)
	require.Equal(t, 1, insert.Profile.BSIStageWriteCount)
	require.Equal(t, 1, insert.Profile.BatchCacheWriteCount)

	second, _, _ := newCompoundBSIPrimaryKeyTestBuffer()
	replay, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          session,
		TableBuffer:      second,
		LookupValue:      "rendering-does-not-matter",
		PrimaryKeyValues: []interface{}{int64(1001), int64(2)},
		PrimaryKeyMode:   PrimaryKeyModeAssumeNew,
	})

	require.NoError(t, err)
	require.True(t, replay.ExistingRow)
	require.Equal(t, uint64(7000), replay.ColumnID)
	require.Equal(t, uint64(7000), second.CurrentColumnID)
	require.Equal(t, 1, replay.Profile.LocalCacheLookupCount)
	require.Equal(t, 1, replay.Profile.LocalCacheHitCount)
	require.Zero(t, replay.Profile.AssumeNewCount)
	require.Zero(t, replay.Profile.BSILookupCount)
	require.Zero(t, replay.Profile.BSIStageWriteCount)
	require.Zero(t, replay.Profile.RownumAllocationCount)
	require.Empty(t, backend.lookupRequests)
	require.Len(t, backend.stageRequests, 1)
}

func TestBSIPrimaryKeyResolverRejectsSameBatchProvidedColumnIDConflict(t *testing.T) {
	backend := &recordingBSIPrimaryKeyBackend{}
	resolver := NewBSIPrimaryKeyResolver(backend)
	session := &Session{BatchBuffer: shared.NewBatchBuffer(nil, nil, 1000)}
	first, _, _ := newCompoundBSIPrimaryKeyTestBuffer()

	_, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          session,
		TableBuffer:      first,
		LookupValue:      "1001+2",
		PrimaryKeyValues: []interface{}{int64(1001), int64(2)},
		ProvidedColumnID: 7000,
		PrimaryKeyMode:   PrimaryKeyModeAssumeNew,
	})
	require.NoError(t, err)

	second, _, _ := newCompoundBSIPrimaryKeyTestBuffer()
	conflict, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          session,
		TableBuffer:      second,
		LookupValue:      "same-key-different-rownum",
		PrimaryKeyValues: []interface{}{int64(1001), int64(2)},
		ProvidedColumnID: 8000,
		PrimaryKeyMode:   PrimaryKeyModeAssumeNew,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "local batch conflict")
	require.Zero(t, conflict.ColumnID)
	require.Equal(t, 1, conflict.Profile.LocalCacheLookupCount)
	require.Zero(t, conflict.Profile.BSILookupCount)
	require.Zero(t, conflict.Profile.BSIStageWriteCount)
	require.Len(t, backend.stageRequests, 1)
}

func TestBSIPrimaryKeyResolverRejectsCompoundAssumeNewConflict(t *testing.T) {
	backend := newMapBSIPrimaryKeyBackend()
	resolver := NewBSIPrimaryKeyResolver(backend)
	first, _, _ := newCompoundBSIPrimaryKeyTestBuffer()

	insert, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      first,
		LookupValue:      "1001+2",
		PrimaryKeyValues: []interface{}{int64(1001), int64(2)},
		ProvidedColumnID: 7000,
		PrimaryKeyMode:   PrimaryKeyModeAssumeNew,
	})
	require.NoError(t, err)
	require.False(t, insert.ExistingRow)
	require.Equal(t, uint64(7000), insert.ColumnID)
	require.Equal(t, 1, insert.Profile.AssumeNewCount)
	require.Equal(t, 1, insert.Profile.SkippedBSILookupCount)
	require.Equal(t, 1, insert.Profile.BSIStageWriteCount)

	conflictBuffer, _, _ := newCompoundBSIPrimaryKeyTestBuffer()
	conflict, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      conflictBuffer,
		LookupValue:      "same-human-rendering",
		PrimaryKeyValues: []interface{}{int64(1001), int64(2)},
		ProvidedColumnID: 8000,
		PrimaryKeyMode:   PrimaryKeyModeAssumeNew,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "BSI primary key stage error")
	require.Contains(t, err.Error(), "compound primary key conflict")
	require.Zero(t, conflict.ColumnID)
	require.False(t, conflict.ExistingRow)
	require.Equal(t, 1, conflict.Profile.AssumeNewCount)
	require.Equal(t, 1, conflict.Profile.SkippedBSILookupCount)
	require.Equal(t, 1, conflict.Profile.ProvidedColumnIDCount)
	require.Equal(t, 1, conflict.Profile.BSIStageWriteCount)
}

func TestBSIPrimaryKeyResolverRequiresTypedValues(t *testing.T) {
	tbuf, _ := newBSIPrimaryKeyTestBuffer()
	backend := &recordingBSIPrimaryKeyBackend{}
	resolver := NewBSIPrimaryKeyResolver(backend)

	result, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:     &Session{},
		TableBuffer: tbuf,
		LookupValue: "1001",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "requires 1 typed primary-key values")
	require.Zero(t, result.ColumnID)
	require.Empty(t, backend.lookupRequests)
	require.Empty(t, backend.stageRequests)
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
	require.NotEmpty(t, stageReq.Identity)
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

func newCompoundBSIPrimaryKeyTestBuffer() (*TableBuffer, *Attribute, *Attribute) {
	table := &Table{BasicTable: &shared.BasicTable{Name: "lineitem", PrimaryKey: "l_orderkey,l_linenumber"}}
	orderKey := &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       "l_orderkey",
			SourceName:      "l_orderkey",
			Type:            "Integer",
			MappingStrategy: "IntBSI",
		},
		Parent: table,
	}
	lineNumber := &Attribute{
		BasicAttribute: &shared.BasicAttribute{
			FieldName:       "l_linenumber",
			SourceName:      "l_linenumber",
			Type:            "Integer",
			MappingStrategy: "IntBSI",
		},
		Parent: table,
	}
	tbuf := &TableBuffer{
		Table:            table,
		CurrentTimestamp: time.Unix(0, 0).UTC(),
		PKAttributes:     []*Attribute{orderKey, lineNumber},
		PKMap: map[string]*Attribute{
			"l_orderkey":   orderKey,
			"l_linenumber": lineNumber,
		},
		sequencerCache: map[int64]*shared.Sequencer{},
	}
	return tbuf, orderKey, lineNumber
}

package core

import (
	"errors"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/stretchr/testify/require"
)

type recordingSingleColumnBSIPrimaryKeyReader struct {
	requests []SingleColumnBSIPrimaryKeyReadRequest
	rows     []uint64
	err      error
}

func (r *recordingSingleColumnBSIPrimaryKeyReader) LookupSingleColumnBSIPrimaryKey(req SingleColumnBSIPrimaryKeyReadRequest) (SingleColumnBSIPrimaryKeyReadResult, error) {
	r.requests = append(r.requests, req)
	return SingleColumnBSIPrimaryKeyReadResult{ColumnIDs: append([]uint64(nil), r.rows...)}, r.err
}

func TestSingleColumnBSIPrimaryKeyBackendReportsMiss(t *testing.T) {
	table, attr := singleColumnBSIPrimaryKeyBackendTestTable(false)
	reader := &recordingSingleColumnBSIPrimaryKeyReader{}
	backend := NewSingleColumnBSIPrimaryKeyBackend(table, reader)

	result, err := backend.LookupPrimaryKey(BSIPrimaryKeyLookupRequest{
		TableName:      table.Name,
		PrimaryKey:     table.PrimaryKey,
		Attributes:     []*Attribute{attr},
		Values:         []interface{}{int64(1001)},
		RenderedValue:  "1001",
		ShardTimestamp: time.Unix(100, 0).UTC(),
	})

	require.NoError(t, err)
	require.False(t, result.Found)
	require.Zero(t, result.ColumnID)
	require.Empty(t, result.MatchedColumnIDs)
	require.Len(t, reader.requests, 1)
	require.Equal(t, "orders", reader.requests[0].TableName)
	require.Equal(t, "o_orderkey", reader.requests[0].FieldName)
	require.Equal(t, "IntBSI", reader.requests[0].MappingStrategy)
	require.Equal(t, attr, reader.requests[0].Attribute)
	require.Equal(t, int64(1001), reader.requests[0].Value)
	require.Equal(t, time.Unix(100, 0).UTC(), reader.requests[0].ShardTimestamp)
	require.False(t, reader.requests[0].RequiresShardScope)
}

func TestSingleColumnBSIPrimaryKeyBackendReportsSingleMatch(t *testing.T) {
	table, attr := singleColumnBSIPrimaryKeyBackendTestTable(false)
	reader := &recordingSingleColumnBSIPrimaryKeyReader{rows: []uint64{42}}
	backend := NewSingleColumnBSIPrimaryKeyBackend(table, reader)

	result, err := backend.LookupPrimaryKey(BSIPrimaryKeyLookupRequest{
		TableName:  table.Name,
		PrimaryKey: table.PrimaryKey,
		Attributes: []*Attribute{attr},
		Values:     []interface{}{int64(1001)},
	})

	require.NoError(t, err)
	require.True(t, result.Found)
	require.Equal(t, uint64(42), result.ColumnID)
	require.Equal(t, []uint64{42}, result.MatchedColumnIDs)
}

func TestBSIPrimaryKeyResolverRejectsMultipleSingleColumnBSIMatches(t *testing.T) {
	table, attr := singleColumnBSIPrimaryKeyBackendTestTable(false)
	tbuf := singleColumnBSIPrimaryKeyBackendTestBuffer(table, attr)
	reader := &recordingSingleColumnBSIPrimaryKeyReader{rows: []uint64{42, 99}}
	resolver := NewBSIPrimaryKeyResolver(NewSingleColumnBSIPrimaryKeyBackend(table, reader))

	result, err := resolver.ResolvePrimaryKeyColumnID(PrimaryKeyResolveRequest{
		Session:          &Session{},
		TableBuffer:      tbuf,
		LookupValue:      "1001",
		PrimaryKeyValues: []interface{}{int64(1001)},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "authority conflict")
	require.Contains(t, err.Error(), "matched 2 rownums")
	require.Zero(t, result.ColumnID)
	require.False(t, result.ExistingRow)
	require.Equal(t, 1, result.Profile.BSILookupCount)
	require.Zero(t, result.Profile.BSIHitCount)
	require.Zero(t, result.Profile.BSIStageWriteCount)
	require.Len(t, reader.requests, 1)
}

func TestSingleColumnBSIPrimaryKeyBackendRejectsUnsupportedTableShapeWithoutReaderCall(t *testing.T) {
	table := testPrimaryKeyAuthorityTable("lineitem", "l_orderkey+l_linenumber", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("l_orderkey", "Integer", "IntBSI", false),
		testPrimaryKeyAuthorityAttribute("l_linenumber", "Integer", "IntBSI", false),
	})
	reader := &recordingSingleColumnBSIPrimaryKeyReader{rows: []uint64{42}}
	backend := NewSingleColumnBSIPrimaryKeyBackend(table, reader)

	_, err := backend.LookupPrimaryKey(BSIPrimaryKeyLookupRequest{
		TableName:  table.Name,
		PrimaryKey: table.PrimaryKey,
		Values:     []interface{}{int64(1001), int64(1)},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported")
	require.Contains(t, err.Error(), "compound")
	require.Empty(t, reader.requests)
}

func TestSingleColumnBSIPrimaryKeyBackendPropagatesReaderError(t *testing.T) {
	table, attr := singleColumnBSIPrimaryKeyBackendTestTable(false)
	readerErr := errors.New("read failed")
	reader := &recordingSingleColumnBSIPrimaryKeyReader{err: readerErr}
	backend := NewSingleColumnBSIPrimaryKeyBackend(table, reader)

	_, err := backend.LookupPrimaryKey(BSIPrimaryKeyLookupRequest{
		TableName:  table.Name,
		PrimaryKey: table.PrimaryKey,
		Attributes: []*Attribute{attr},
		Values:     []interface{}{int64(1001)},
	})

	require.ErrorIs(t, err, readerErr)
}

func TestSingleColumnBSIPrimaryKeyBackendRejectsDirectColumnIDMode(t *testing.T) {
	table, _ := singleColumnBSIPrimaryKeyBackendTestTable(true)
	reader := &recordingSingleColumnBSIPrimaryKeyReader{rows: []uint64{42}}
	backend := NewSingleColumnBSIPrimaryKeyBackend(table, reader)

	_, err := backend.LookupPrimaryKey(BSIPrimaryKeyLookupRequest{
		TableName:  table.Name,
		PrimaryKey: table.PrimaryKey,
		Values:     []interface{}{int64(1001)},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported")
	require.Empty(t, reader.requests)
}

func singleColumnBSIPrimaryKeyBackendTestTable(columnID bool) (*Table, *Attribute) {
	table := testPrimaryKeyAuthorityTable("orders", "o_orderkey", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("o_orderkey", "Integer", "IntBSI", columnID),
	})
	attr, _ := table.GetAttribute("o_orderkey")
	return table, attr
}

func singleColumnBSIPrimaryKeyBackendTestBuffer(table *Table, attr *Attribute) *TableBuffer {
	return &TableBuffer{
		Table:            table,
		CurrentTimestamp: time.Unix(0, 0).UTC(),
		PKAttributes:     []*Attribute{attr},
		PKMap:            map[string]*Attribute{attr.FieldName: attr},
		sequencerCache:   map[int64]*shared.Sequencer{},
	}
}

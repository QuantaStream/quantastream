package qsinabox

import (
	"fmt"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/server"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// StandardSingleColumnBSIPrimaryKeyReader resolves primary-key rownums from the
// existing catalog-designated PK BSI in the in-process standard backend.
type StandardSingleColumnBSIPrimaryKeyReader struct {
	Pool       *core.SessionPool
	TableCache *core.TableCacheStruct
	Direct     *server.BitmapIndex
}

var _ core.SingleColumnBSIPrimaryKeyReader = StandardSingleColumnBSIPrimaryKeyReader{}

// LookupSingleColumnBSIPrimaryKey maps the typed key value through the catalog
// mapper and evaluates equality against the existing PK BSI.
func (r StandardSingleColumnBSIPrimaryKeyReader) LookupSingleColumnBSIPrimaryKey(req core.SingleColumnBSIPrimaryKeyReadRequest) (core.SingleColumnBSIPrimaryKeyReadResult, error) {
	if req.TableName == "" {
		return core.SingleColumnBSIPrimaryKeyReadResult{}, fmt.Errorf("single-column BSI primary-key lookup requires table name")
	}
	if req.FieldName == "" {
		return core.SingleColumnBSIPrimaryKeyReadResult{}, fmt.Errorf("single-column BSI primary-key lookup requires field name")
	}
	attr, err := r.primaryKeyAttribute(req)
	if err != nil {
		return core.SingleColumnBSIPrimaryKeyReadResult{}, err
	}
	mapped, err := attr.MapValue(req.Value, nil, false)
	if err != nil {
		return core.SingleColumnBSIPrimaryKeyReadResult{}, err
	}
	if mapped == nil {
		return core.SingleColumnBSIPrimaryKeyReadResult{}, nil
	}
	fromTime, toTime := standardSingleColumnBSIPrimaryKeyWindowNanos(r.TableCache, req)
	bsi, err := r.projectPrimaryKeyBSI(req.TableName, req.FieldName, fromTime, toTime)
	if err != nil {
		return core.SingleColumnBSIPrimaryKeyReadResult{}, err
	}
	if bsi == nil {
		return core.SingleColumnBSIPrimaryKeyReadResult{}, nil
	}
	matches := bsi.CompareBigValue(0, roaring64.EQ, mapped, nil, nil)
	return core.SingleColumnBSIPrimaryKeyReadResult{
		ColumnIDs: standardSingleColumnBSIPrimaryKeyColumnIDs(matches),
	}, nil
}

func (r StandardSingleColumnBSIPrimaryKeyReader) primaryKeyAttribute(req core.SingleColumnBSIPrimaryKeyReadRequest) (*core.Attribute, error) {
	if req.Attribute != nil {
		return req.Attribute, nil
	}
	table := standardCachedTable(r.TableCache, req.TableName)
	if table == nil {
		return nil, fmt.Errorf("single-column BSI primary-key lookup cannot find table %s", req.TableName)
	}
	return table.GetAttribute(req.FieldName)
}

func (r StandardSingleColumnBSIPrimaryKeyReader) projectPrimaryKeyBSI(tableName, fieldName string, fromTime, toTime int64) (*roaring64.BSI, error) {
	if r.Direct != nil {
		return r.Direct.ProjectBSI(tableName, fieldName, fromTime, toTime, nil, false)
	}
	if r.Pool == nil {
		return nil, fmt.Errorf("single-column BSI primary-key reader has no bitmap backend")
	}
	session, err := r.Pool.Borrow(tableName)
	if err != nil {
		return nil, err
	}
	defer r.Pool.Return(tableName, session)
	if session == nil || session.BitIndex == nil {
		return nil, fmt.Errorf("single-column BSI primary-key reader has no bitmap index")
	}
	bsis, _, err := session.BitIndex.Projection(tableName, []string{fieldName}, fromTime, toTime, nil, false)
	if err != nil {
		return nil, err
	}
	return bsis[fieldName], nil
}

func standardSingleColumnBSIPrimaryKeyWindowNanos(cache *core.TableCacheStruct, req core.SingleColumnBSIPrimaryKeyReadRequest) (int64, int64) {
	if req.RequiresShardScope && !req.ShardTimestamp.IsZero() {
		nanos := req.ShardTimestamp.UTC().UnixNano()
		return nanos, nanos
	}
	return standardProjectionWindowNanos(cache, req.TableName, 0, 0)
}

func standardSingleColumnBSIPrimaryKeyColumnIDs(bitmap *roaring64.Bitmap) []uint64 {
	if bitmap == nil || bitmap.IsEmpty() {
		return nil
	}
	ids := make([]uint64, 0)
	it := bitmap.Iterator()
	for it.HasNext() {
		ids = append(ids, it.Next())
	}
	return ids
}

package qsinabox

import (
	"context"
	"fmt"
	"math/big"

	"github.com/QuantaStream/quantastream/core"
	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/server"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

// StandardSingleColumnBSIPrimaryKeyReader resolves primary-key rownums from the
// existing catalog-designated PK BSI in the in-process standard backend.
type StandardSingleColumnBSIPrimaryKeyReader struct {
	Pool            *core.SessionPool
	TableCache      *core.TableCacheStruct
	Direct          *server.BitmapIndex
	BitIndex        *shared.BitmapIndex
	ProjectionCache *StandardBSIProjectionCache
}

var _ core.SingleColumnBSIPrimaryKeyReader = StandardSingleColumnBSIPrimaryKeyReader{}
var _ core.SingleColumnBSIPrimaryKeyDomainReader = StandardSingleColumnBSIPrimaryKeyReader{}

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
	if columnIDs, ok := r.lookupCachedPrimaryKeyBigValue(req.TableName, req.FieldName, fromTime, toTime, mapped); ok {
		return core.SingleColumnBSIPrimaryKeyReadResult{
			ColumnIDs: columnIDs,
		}, nil
	}
	bsi, _, _, err := r.projectCachedPrimaryKeyBSI(req.TableName, req.FieldName, fromTime, toTime)
	if err != nil {
		return core.SingleColumnBSIPrimaryKeyReadResult{}, err
	}
	if bsi == nil {
		return core.SingleColumnBSIPrimaryKeyReadResult{}, nil
	}
	if lookup, ok := r.storeCachedPrimaryKeyBigValueLookup(req.TableName, req.FieldName, fromTime, toTime, bsi); ok {
		return core.SingleColumnBSIPrimaryKeyReadResult{
			ColumnIDs: standardBSIBigValueLookupColumnIDs(lookup, mapped),
		}, nil
	}
	matches := bsi.CompareBigValue(0, roaring64.EQ, mapped, nil, nil)
	return core.SingleColumnBSIPrimaryKeyReadResult{
		ColumnIDs: standardSingleColumnBSIPrimaryKeyColumnIDs(matches),
	}, nil
}

// PrimaryKeyDomainState reports whether the projected PK BSI domain currently
// contains any committed values.
func (r StandardSingleColumnBSIPrimaryKeyReader) PrimaryKeyDomainState(req core.SingleColumnBSIPrimaryKeyReadRequest) (core.PrimaryKeyDomainState, error) {
	if req.TableName == "" {
		return core.PrimaryKeyDomainUnknown, fmt.Errorf("single-column BSI primary-key domain probe requires table name")
	}
	if req.FieldName == "" {
		return core.PrimaryKeyDomainUnknown, fmt.Errorf("single-column BSI primary-key domain probe requires field name")
	}
	if _, err := r.primaryKeyAttribute(req); err != nil {
		return core.PrimaryKeyDomainUnknown, err
	}
	fromTime, toTime := standardSingleColumnBSIPrimaryKeyWindowNanos(r.TableCache, req)
	if cardinality, ok, err := r.primaryKeyBSIDomainCardinality(req.TableName, req.FieldName, fromTime, toTime); err != nil {
		return core.PrimaryKeyDomainUnknown, err
	} else if ok {
		if cardinality == 0 {
			return core.PrimaryKeyDomainEmpty, nil
		}
		return core.PrimaryKeyDomainNonEmpty, nil
	}
	bsi, _, _, err := r.projectCachedPrimaryKeyBSI(req.TableName, req.FieldName, fromTime, toTime)
	if err != nil {
		return core.PrimaryKeyDomainUnknown, err
	}
	if bsi == nil || bsi.GetCardinality() == 0 {
		return core.PrimaryKeyDomainEmpty, nil
	}
	return core.PrimaryKeyDomainNonEmpty, nil
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
	if r.BitIndex != nil {
		bsis, _, err := r.BitIndex.Projection(tableName, []string{fieldName}, fromTime, toTime, nil, false)
		if err != nil {
			return nil, err
		}
		return bsis[fieldName], nil
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

func (r StandardSingleColumnBSIPrimaryKeyReader) primaryKeyBSIDomainCardinality(tableName, fieldName string,
	fromTime, toTime int64) (uint64, bool, error) {

	if fromTime != toTime {
		return 0, false, nil
	}
	if r.Direct != nil {
		ctx, cancel := context.WithTimeout(context.Background(), shared.Deadline)
		defer cancel()
		response, err := r.Direct.SyncStatus(ctx, &pb.SyncStatusRequest{
			Index:    tableName,
			Field:    fieldName,
			Time:     fromTime,
			SendData: false,
		})
		if err != nil {
			return 0, true, err
		}
		if response == nil {
			return 0, true, nil
		}
		return response.GetCardinality(), true, nil
	}
	if r.BitIndex != nil {
		cardinality, err := r.BitIndex.BSIDomainCardinality(tableName, fieldName, fromTime, toTime)
		return cardinality, true, err
	}
	if r.Pool == nil {
		return 0, false, nil
	}
	session, err := r.Pool.Borrow(tableName)
	if err != nil {
		return 0, true, err
	}
	defer r.Pool.Return(tableName, session)
	if session == nil || session.BitIndex == nil {
		return 0, false, nil
	}
	cardinality, err := session.BitIndex.BSIDomainCardinality(tableName, fieldName, fromTime, toTime)
	return cardinality, true, err
}

func (r StandardSingleColumnBSIPrimaryKeyReader) projectCachedPrimaryKeyBSI(tableName, fieldName string,
	fromTime, toTime int64) (*roaring64.BSI, bool, bool, error) {

	if r.ProjectionCache == nil {
		bsi, err := r.projectPrimaryKeyBSI(tableName, fieldName, fromTime, toTime)
		return bsi, false, false, err
	}
	if bsi, ok := r.ProjectionCache.Lookup(tableName, fieldName, fromTime, toTime); ok {
		return bsi, true, true, nil
	}
	bsi, err := r.projectPrimaryKeyBSI(tableName, fieldName, fromTime, toTime)
	if err != nil {
		return nil, true, false, err
	}
	return r.ProjectionCache.Store(tableName, fieldName, fromTime, toTime, bsi), true, false, nil
}

func (r StandardSingleColumnBSIPrimaryKeyReader) stageCachedPrimaryKeyBSI(tableName, fieldName string,
	fromTime, toTime int64, columnID uint64, value *big.Int) {

	if r.ProjectionCache == nil {
		return
	}
	r.ProjectionCache.StageBigValue(tableName, fieldName, fromTime, toTime, columnID, value)
}

func (r StandardSingleColumnBSIPrimaryKeyReader) lookupCachedPrimaryKeyBigValue(tableName, fieldName string,
	fromTime, toTime int64, value *big.Int) ([]uint64, bool) {

	if r.ProjectionCache == nil {
		return nil, false
	}
	return r.ProjectionCache.LookupBigValue(tableName, fieldName, fromTime, toTime, value)
}

func (r StandardSingleColumnBSIPrimaryKeyReader) storeCachedPrimaryKeyBigValueLookup(tableName, fieldName string,
	fromTime, toTime int64, bsi *roaring64.BSI) (map[string][]uint64, bool) {

	if r.ProjectionCache == nil {
		return nil, false
	}
	return r.ProjectionCache.StoreBigValueLookup(tableName, fieldName, fromTime, toTime, bsi), true
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

func standardBSIBigValueLookupColumnIDs(lookup map[string][]uint64, value *big.Int) []uint64 {
	if len(lookup) == 0 || value == nil {
		return nil
	}
	return append([]uint64(nil), lookup[standardBSIBigValueLookupKey(value)]...)
}

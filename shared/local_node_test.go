package shared

import (
	"context"
	"reflect"
	"testing"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
)

func TestLocalNodeServicesReadinessRequiresBitmapAndKV(t *testing.T) {
	readiness := (LocalNodeServices{}).Readiness()
	if readiness.Ready {
		t.Fatalf("empty local services reported ready")
	}
	if len(readiness.Blockers) != 2 {
		t.Fatalf("blockers = %v, want missing bitmap and kv blockers", readiness.Blockers)
	}
	if len(readiness.StreamingRisks) == 0 {
		t.Fatalf("streaming risks were not reported")
	}
}

func TestDefaultLocalNodeStreamingRisksNamesRemainingSearchGate(t *testing.T) {
	risks := DefaultLocalNodeStreamingRisks()
	var sawBatchLookup, sawSearch bool
	for _, risk := range risks {
		switch risk.Method {
		case "BatchLookup":
			sawBatchLookup = true
		case "BatchIndex/Search":
			sawSearch = true
		}
	}
	if sawBatchLookup {
		t.Fatalf("BatchLookup risk should be promoted out of streaming risks: %+v", risks)
	}
	if !sawSearch {
		t.Fatalf("BatchIndex/Search risk missing from %+v", risks)
	}
}

func TestBitmapIndexQueryGroupUsesLocalService(t *testing.T) {
	local := &recordingLocalBitmapIndexService{}
	index := NewBitmapIndex(&Conn{LocalNodeServices: LocalNodeServices{BitmapIndex: local}})
	result, _, err := index.queryGroup("sample", &pb.BitmapQuery{Query: []*pb.QueryFragment{{Index: "sample"}}})
	if err != nil {
		t.Fatalf("queryGroup() error = %v", err)
	}
	if local.queryCalls != 1 {
		t.Fatalf("local query calls = %d, want 1", local.queryCalls)
	}
	if got := result.GetUnions()[0].GetCardinality(); got != 2 {
		t.Fatalf("union cardinality = %d, want 2", got)
	}
}

func TestBitmapIndexBSIDomainCardinalityUsesLocalService(t *testing.T) {
	local := &recordingLocalBitmapIndexService{syncStatusCardinality: 7}
	index := NewBitmapIndex(&Conn{LocalNodeServices: LocalNodeServices{BitmapIndex: local}})
	cardinality, err := index.BSIDomainCardinality("lineitem", "l_orderkey", 123, 123)
	if err != nil {
		t.Fatalf("BSIDomainCardinality() error = %v", err)
	}
	if local.syncStatusCalls != 1 {
		t.Fatalf("sync status calls = %d, want 1", local.syncStatusCalls)
	}
	if local.syncStatusRequest == nil {
		t.Fatal("sync status request was not captured")
	}
	if local.syncStatusRequest.Index != "lineitem" || local.syncStatusRequest.Field != "l_orderkey" ||
		local.syncStatusRequest.Time != 123 || local.syncStatusRequest.SendData {
		t.Fatalf("sync status request = %#v, want no-data cardinality probe", local.syncStatusRequest)
	}
	if cardinality != 7 {
		t.Fatalf("cardinality = %d, want 7", cardinality)
	}
}

func TestBitmapIndexBatchClearValueUsesLocalBatchService(t *testing.T) {
	local := &recordingLocalBitmapIndexService{}
	index := NewBitmapIndex(&Conn{LocalNodeServices: LocalNodeServices{BitmapIndex: local}})
	bitmap := roaring64.NewBitmap()
	bitmap.Add(42)
	err := index.BatchClearValue(map[string]map[string]map[int64]*roaring64.Bitmap{
		"customers_qa": {
			"age": {
				123: bitmap,
			},
		},
	})
	if err != nil {
		t.Fatalf("BatchClearValue() error = %v", err)
	}
	if local.batchMutateCalls != 1 {
		t.Fatalf("batch mutate calls = %d, want 1", local.batchMutateCalls)
	}
	if len(local.batchMutateItems) != 1 {
		t.Fatalf("batch mutate items = %d, want 1", len(local.batchMutateItems))
	}
	item := local.batchMutateItems[0]
	if item.IndexPath != "customers_qa/age" || !item.IsClear || item.Time != 123 {
		t.Fatalf("batch mutate item = %#v, want clear customers_qa/age at 123", item)
	}
	if got := UnmarshalValue(reflect.Int, item.Key); got != int(-1) {
		t.Fatalf("clear key = %#v, want -1", got)
	}
}

func TestBitmapIndexBulkClearUsesLocalService(t *testing.T) {
	local := &recordingLocalBitmapIndexService{}
	index := NewBitmapIndex(&Conn{LocalNodeServices: LocalNodeServices{BitmapIndex: local}})
	bitmap := roaring64.NewBitmap()
	bitmap.Add(42)
	if err := index.BulkClear("customers_qa", "1970-01-01T00", "1970-01-01T00", bitmap); err != nil {
		t.Fatalf("BulkClear() error = %v", err)
	}
	if local.bulkClearCalls != 1 {
		t.Fatalf("bulk clear calls = %d, want 1", local.bulkClearCalls)
	}
	if local.bulkClearRequest == nil {
		t.Fatalf("bulk clear request was not captured")
	}
	if local.bulkClearRequest.Index != "customers_qa" {
		t.Fatalf("bulk clear index = %q, want customers_qa", local.bulkClearRequest.Index)
	}
	foundSet := roaring64.NewBitmap()
	if err := foundSet.UnmarshalBinary(local.bulkClearRequest.FoundSet); err != nil {
		t.Fatalf("unmarshal bulk clear found set: %v", err)
	}
	if !foundSet.Contains(42) {
		t.Fatalf("bulk clear found set missing rownum 42")
	}
}

func TestBitmapIndexCompareBSIFieldsUsesLocalService(t *testing.T) {
	local := &recordingLocalBitmapIndexService{}
	index := NewBitmapIndex(&Conn{LocalNodeServices: LocalNodeServices{BitmapIndex: local}})
	foundSet := roaring64.BitmapOf(1, 2, 3)
	matches, err := index.CompareBSIFields("lineitem", "l_receiptdate", "l_commitdate", 10, 20, foundSet, roaring64.GT, false)
	if err != nil {
		t.Fatalf("CompareBSIFields() error = %v", err)
	}
	if local.compareBSIFieldsCalls != 1 {
		t.Fatalf("compare calls = %d, want 1", local.compareBSIFieldsCalls)
	}
	if local.compareBSIFieldsRequest == nil {
		t.Fatal("compare request was not captured")
	}
	if local.compareBSIFieldsRequest.Index != "lineitem" || local.compareBSIFieldsRequest.LeftField != "l_receiptdate" || local.compareBSIFieldsRequest.RightField != "l_commitdate" {
		t.Fatalf("compare request = %#v", local.compareBSIFieldsRequest)
	}
	if local.compareBSIFieldsRequest.Operation != int32(roaring64.GT) {
		t.Fatalf("operation = %d, want GT", local.compareBSIFieldsRequest.Operation)
	}
	if got, want := matches.ToArray(), []uint64{2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("matches = %#v, want %#v", got, want)
	}
}

func TestBitmapIndexBitmapGroupAggregatesUsesLocalService(t *testing.T) {
	local := &recordingLocalBitmapIndexService{}
	index := NewBitmapIndex(&Conn{LocalNodeServices: LocalNodeServices{BitmapIndex: local}})
	_, _, ok, err := index.BitmapGroupAggregates("lineitem", []string{"l_returnflag"}, []BitmapGroupAggregateSpec{{Function: "count"}}, 10, 20, roaring64.BitmapOf(101, 102))
	if err != nil {
		t.Fatalf("BitmapGroupAggregates() error = %v", err)
	}
	if !ok {
		t.Fatalf("BitmapGroupAggregates() ok = false, want true")
	}
	if local.bitmapGroupAggregateCalls != 1 {
		t.Fatalf("bitmap group aggregate calls = %d, want 1", local.bitmapGroupAggregateCalls)
	}
	if local.bitmapGroupAggregateRequest == nil {
		t.Fatal("bitmap group aggregate request was not captured")
	}
	if local.bitmapGroupAggregateRequest.Index != "lineitem" {
		t.Fatalf("bitmap group aggregate request = %#v", local.bitmapGroupAggregateRequest)
	}
	if got, want := local.bitmapGroupAggregateRequest.GroupFields, []string{"l_returnflag"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("group fields = %#v, want %#v", got, want)
	}
	if len(local.bitmapGroupAggregateRequest.FoundSet) == 0 {
		t.Fatal("bitmap group aggregate request found set was empty")
	}
}

func TestBitmapIndexRelationshipAlignedValueSumUsesLocalService(t *testing.T) {
	local := &recordingLocalBitmapIndexService{}
	index := NewBitmapIndex(&Conn{LocalNodeServices: LocalNodeServices{BitmapIndex: local}})
	_, _, ok, err := index.RelationshipAlignedValueSum("lineitem", "l_extendedprice", 10, 20, []uint64{101, 102}, []uint64{7, 7})
	if err != nil {
		t.Fatalf("RelationshipAlignedValueSum() error = %v", err)
	}
	if !ok {
		t.Fatalf("RelationshipAlignedValueSum() ok = false, want true")
	}
	if local.relationshipSumCalls != 1 {
		t.Fatalf("relationship sum calls = %d, want 1", local.relationshipSumCalls)
	}
	if local.relationshipSumRequest == nil {
		t.Fatal("relationship sum request was not captured")
	}
	if local.relationshipSumRequest.Index != "lineitem" || local.relationshipSumRequest.ValueField != "l_extendedprice" {
		t.Fatalf("relationship sum request = %#v", local.relationshipSumRequest)
	}
	if got, want := local.relationshipSumRequest.ChildRows, []uint64{101, 102}; !reflect.DeepEqual(got, want) {
		t.Fatalf("child rows = %#v, want %#v", got, want)
	}
	if got, want := local.relationshipSumRequest.ParentRows, []uint64{7, 7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("parent rows = %#v, want %#v", got, want)
	}
}

func TestKVStoreLookupUsesLocalService(t *testing.T) {
	local := &recordingLocalKVStoreService{}
	store := NewKVStore(&Conn{LocalNodeServices: LocalNodeServices{KVStore: local}})
	value, err := store.Lookup("sample/name", "key", reflect.String, false)
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}
	if local.lookupCalls != 1 {
		t.Fatalf("local lookup calls = %d, want 1", local.lookupCalls)
	}
	if value != "local-value" {
		t.Fatalf("value = %#v, want local-value", value)
	}
}

func TestKVStoreBatchLookupUsesLocalService(t *testing.T) {
	local := &recordingLocalKVStoreService{}
	store := NewKVStore(&Conn{LocalNodeServices: LocalNodeServices{KVStore: local}})
	lookup := map[interface{}]interface{}{
		"row-1": "",
		"row-2": "",
	}
	values, err := store.BatchLookup("sample/name", lookup, false)
	if err != nil {
		t.Fatalf("BatchLookup() error = %v", err)
	}
	if local.batchLookupCalls != 1 {
		t.Fatalf("local batch lookup calls = %d, want 1", local.batchLookupCalls)
	}
	if got := values["row-1"]; got != "value-row-1" {
		t.Fatalf("row-1 value = %#v, want value-row-1", got)
	}
	if got := values["row-2"]; got != "value-row-2" {
		t.Fatalf("row-2 value = %#v, want value-row-2", got)
	}
}

func TestKVStoreItemsUsesLocalService(t *testing.T) {
	local := &recordingLocalKVStoreService{}
	store := NewKVStore(&Conn{LocalNodeServices: LocalNodeServices{KVStore: local}})
	items, err := store.Items("sample/city.StringEnum", reflect.String, reflect.Uint64)
	if err != nil {
		t.Fatalf("Items() error = %v", err)
	}
	if local.itemsCalls != 1 {
		t.Fatalf("local items calls = %d, want 1", local.itemsCalls)
	}
	if got := items["Seattle"]; got != uint64(1) {
		t.Fatalf("Seattle item = %#v, want 1", got)
	}
	if got := items["Tacoma"]; got != uint64(2) {
		t.Fatalf("Tacoma item = %#v, want 2", got)
	}
}

type recordingLocalBitmapIndexService struct {
	queryCalls                  int
	syncStatusCalls             int
	syncStatusRequest           *pb.SyncStatusRequest
	syncStatusCardinality       uint64
	compareBSIFieldsCalls       int
	compareBSIFieldsRequest     *pb.CompareBSIFieldsRequest
	bitmapGroupAggregateCalls   int
	bitmapGroupAggregateRequest *pb.BitmapGroupAggregatesRequest
	relationshipSumCalls        int
	relationshipSumRequest      *pb.RelationshipAlignedValueSumRequest
	bulkClearCalls              int
	bulkClearRequest            *pb.BulkClearRequest
	batchMutateCalls            int
	batchMutateItems            []*pb.IndexKVPair
}

func (s *recordingLocalBitmapIndexService) Query(context.Context, *pb.BitmapQuery) (*pb.QueryResult, error) {
	s.queryCalls++
	bitmap := roaring64.NewBitmap()
	bitmap.Add(10)
	bitmap.Add(20)
	data, err := bitmap.MarshalBinary()
	if err != nil {
		return nil, err
	}
	empty, err := roaring64.NewBitmap().MarshalBinary()
	if err != nil {
		return nil, err
	}
	return &pb.QueryResult{Unions: data, Existences: empty}, nil
}

func (s *recordingLocalBitmapIndexService) SyncStatus(_ context.Context, req *pb.SyncStatusRequest) (*pb.SyncStatusResponse, error) {
	s.syncStatusCalls++
	s.syncStatusRequest = req
	return &pb.SyncStatusResponse{Cardinality: s.syncStatusCardinality}, nil
}

func (s *recordingLocalBitmapIndexService) Projection(context.Context, *pb.ProjectionRequest) (*pb.ProjectionResponse, error) {
	return &pb.ProjectionResponse{}, nil
}

func (s *recordingLocalBitmapIndexService) CompareBSIFields(_ context.Context, req *pb.CompareBSIFieldsRequest) (*pb.CompareBSIFieldsResponse, error) {
	s.compareBSIFieldsCalls++
	s.compareBSIFieldsRequest = req
	bitmap := roaring64.BitmapOf(2, 3)
	data, err := bitmap.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return &pb.CompareBSIFieldsResponse{Rownums: data}, nil
}

func (s *recordingLocalBitmapIndexService) BitmapGroupAggregates(_ context.Context, req *pb.BitmapGroupAggregatesRequest) (*pb.BitmapGroupAggregatesResponse, error) {
	s.bitmapGroupAggregateCalls++
	s.bitmapGroupAggregateRequest = req
	return &pb.BitmapGroupAggregatesResponse{Ok: true}, nil
}

func (s *recordingLocalBitmapIndexService) RelationshipAlignedValueSum(_ context.Context, req *pb.RelationshipAlignedValueSumRequest) (*pb.RelationshipAlignedValueSumResponse, error) {
	s.relationshipSumCalls++
	s.relationshipSumRequest = req
	return &pb.RelationshipAlignedValueSumResponse{Ok: true}, nil
}

func (s *recordingLocalBitmapIndexService) Join(context.Context, *pb.JoinRequest) (*pb.JoinResponse, error) {
	return &pb.JoinResponse{}, nil
}

func (s *recordingLocalBitmapIndexService) CheckoutSequence(context.Context, *pb.CheckoutSequenceRequest) (*pb.CheckoutSequenceResponse, error) {
	return &pb.CheckoutSequenceResponse{}, nil
}

func (s *recordingLocalBitmapIndexService) BulkClear(_ context.Context, req *pb.BulkClearRequest) (*empty.Empty, error) {
	s.bulkClearCalls++
	s.bulkClearRequest = req
	return &empty.Empty{}, nil
}

func (s *recordingLocalBitmapIndexService) TableOperation(context.Context, *pb.TableOperationRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (s *recordingLocalBitmapIndexService) Commit(context.Context, *empty.Empty) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (s *recordingLocalBitmapIndexService) BatchMutate(_ context.Context, items []*pb.IndexKVPair) (*empty.Empty, error) {
	s.batchMutateCalls++
	s.batchMutateItems = append([]*pb.IndexKVPair(nil), items...)
	return &empty.Empty{}, nil
}

type recordingLocalKVStoreService struct {
	lookupCalls      int
	batchLookupCalls int
	batchPutCalls    int
	batchPutItems    []*pb.IndexKVPair
	itemsCalls       int
}

func (s *recordingLocalKVStoreService) Put(context.Context, *pb.IndexKVPair) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (s *recordingLocalKVStoreService) BatchPut(_ context.Context, items []*pb.IndexKVPair) (*empty.Empty, error) {
	s.batchPutCalls++
	s.batchPutItems = append([]*pb.IndexKVPair(nil), items...)
	return &empty.Empty{}, nil
}

func (s *recordingLocalKVStoreService) Lookup(context.Context, *pb.IndexKVPair) (*pb.IndexKVPair, error) {
	s.lookupCalls++
	return &pb.IndexKVPair{Value: [][]byte{ToBytes("local-value")}}, nil
}

func (s *recordingLocalKVStoreService) BatchLookup(_ context.Context, reqs []*pb.IndexKVPair) ([]*pb.IndexKVPair, error) {
	s.batchLookupCalls++
	results := make([]*pb.IndexKVPair, 0, len(reqs))
	for _, req := range reqs {
		key := UnmarshalValue(reflect.String, req.Key).(string)
		results = append(results, &pb.IndexKVPair{
			IndexPath: req.IndexPath,
			Key:       req.Key,
			Value:     [][]byte{ToBytes("value-" + key)},
		})
	}
	return results, nil
}

func (s *recordingLocalKVStoreService) PutStringEnum(context.Context, *pb.StringEnum) (*wrappers.UInt64Value, error) {
	return &wrappers.UInt64Value{Value: 1}, nil
}

func (s *recordingLocalKVStoreService) Items(context.Context, string) ([]*pb.IndexKVPair, error) {
	s.itemsCalls++
	return []*pb.IndexKVPair{
		{Key: ToBytes("Seattle"), Value: [][]byte{ToBytes(uint64(1))}},
		{Key: ToBytes("Tacoma"), Value: [][]byte{ToBytes(uint64(2))}},
	}, nil
}

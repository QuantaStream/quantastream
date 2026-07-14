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

func TestDefaultLocalNodeStreamingRisksNamesLookupAndSearchGates(t *testing.T) {
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
	if !sawBatchLookup {
		t.Fatalf("BatchLookup risk missing from %+v", risks)
	}
	if !sawSearch {
		t.Fatalf("BatchIndex/Search risk missing from %+v", risks)
	}
}

func TestBitmapIndexQueryGroupUsesLocalService(t *testing.T) {
	local := &recordingLocalBitmapIndexService{}
	index := NewBitmapIndex(&Conn{LocalNodeServices: LocalNodeServices{BitmapIndex: local}})
	result, err := index.queryGroup("sample", &pb.BitmapQuery{Query: []*pb.QueryFragment{{Index: "sample"}}})
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
	queryCalls int
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

func (s *recordingLocalBitmapIndexService) Projection(context.Context, *pb.ProjectionRequest) (*pb.ProjectionResponse, error) {
	return &pb.ProjectionResponse{}, nil
}

func (s *recordingLocalBitmapIndexService) Join(context.Context, *pb.JoinRequest) (*pb.JoinResponse, error) {
	return &pb.JoinResponse{}, nil
}

func (s *recordingLocalBitmapIndexService) CheckoutSequence(context.Context, *pb.CheckoutSequenceRequest) (*pb.CheckoutSequenceResponse, error) {
	return &pb.CheckoutSequenceResponse{}, nil
}

func (s *recordingLocalBitmapIndexService) TableOperation(context.Context, *pb.TableOperationRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (s *recordingLocalBitmapIndexService) Commit(context.Context, *empty.Empty) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

type recordingLocalKVStoreService struct {
	lookupCalls int
	itemsCalls  int
}

func (s *recordingLocalKVStoreService) Put(context.Context, *pb.IndexKVPair) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (s *recordingLocalKVStoreService) Lookup(context.Context, *pb.IndexKVPair) (*pb.IndexKVPair, error) {
	s.lookupCalls++
	return &pb.IndexKVPair{Value: [][]byte{ToBytes("local-value")}}, nil
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

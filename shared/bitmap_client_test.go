package shared

import (
	"context"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/stvp/rendezvous"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestBatchMutateRequiresBitmapClients(t *testing.T) {
	index := NewBitmapIndex(NewDefaultConnection("empty-clients"))
	batch := map[string]map[string]map[uint64]map[int64]*Bitmap{
		"orders": {
			"o_orderkey": {
				1: {
					time.Unix(0, 0).UnixNano(): NewBitmap(roaring64.BitmapOf(1), false),
				},
			},
		},
	}

	err := index.BatchMutate(batch, false)
	if err == nil || !strings.Contains(err.Error(), "no bitmap clients available") {
		t.Fatalf("expected no-client batch mutate error, got %v", err)
	}
}

func TestBatchSetValueRequiresBitmapClients(t *testing.T) {
	index := NewBitmapIndex(NewDefaultConnection("empty-clients"))
	values := roaring64.NewDefaultBSI()
	values.SetBigValue(1, big.NewInt(10))
	batch := map[string]map[string]map[int64]*roaring64.BSI{
		"orders": {
			"o_orderkey": {
				time.Unix(0, 0).UnixNano(): values,
			},
		},
	}

	err := index.BatchSetValue(batch)
	if err == nil || !strings.Contains(err.Error(), "no bitmap clients available") {
		t.Fatalf("expected no-client batch set value error, got %v", err)
	}
}

func TestSplitBSIItemBatchPreEncodesReplicatedItems(t *testing.T) {
	conn := NewDefaultConnection("bsi-item-batch")
	conn.ServicePort = 4010
	conn.ids = []string{"node-0", "node-1", "node-2"}
	conn.clientConn = []*grpc.ClientConn{{}, {}, {}}
	conn.nodeMap = map[string]int{"node-0": 0, "node-1": 1, "node-2": 2}
	conn.HashTable = rendezvous.New(conn.ids)
	for _, id := range conn.ids {
		conn.nodeStatusMap.Store(id, &pb.StatusMessage{NodeState: "Active"})
	}

	values := roaring64.NewDefaultBSI()
	values.SetBigValue(1, big.NewInt(10))
	index := NewBitmapIndex(conn)
	_, batches, err := index.splitBSIItemBatch(map[string]map[string]map[int64]*roaring64.BSI{
		"orders": {
			"o_orderkey": {
				time.Unix(0, 0).UnixNano(): values,
			},
		},
	})
	if err != nil {
		t.Fatalf("splitBSIItemBatch returned error: %v", err)
	}

	var routed []*pb.IndexKVPair
	for _, batch := range batches {
		routed = append(routed, batch...)
	}
	if len(routed) != conn.Replicas {
		t.Fatalf("routed item count = %d, want %d replicas", len(routed), conn.Replicas)
	}
	for _, item := range routed {
		if item.IndexPath != "orders/o_orderkey" || item.Time != time.Unix(0, 0).UnixNano() {
			t.Fatalf("routed item = %+v", item)
		}
		if len(item.Value) == 0 || len(item.Value[0]) == 0 {
			t.Fatalf("routed item has empty BSI payload")
		}
	}
	if &routed[0].Value[0][0] != &routed[1].Value[0][0] {
		t.Fatalf("replicated routed BSI items should share one encoded payload")
	}
}

func TestActiveClientsSnapshotKeepsNodesWithMissingCachedStatus(t *testing.T) {
	conn := NewDefaultConnection("missing-status")
	conn.ServicePort = 4010
	conn.ids = []string{"node-0", "node-1", "node-2"}
	conn.clientConn = []*grpc.ClientConn{{}, {}, {}}
	conn.nodeStatusMap.Store("node-2", &pb.StatusMessage{NodeState: "Active"})

	index := NewBitmapIndex(conn)
	snapshot := index.activeClientsSnapshot()
	if got, want := len(snapshot), 3; got != want {
		t.Fatalf("active client snapshot len = %d, want %d", got, want)
	}
	for i, client := range snapshot {
		if client.index != i {
			t.Fatalf("snapshot[%d].index = %d, want %d", i, client.index, i)
		}
	}
}

func TestBatchMutateItemsNodeUsesUnaryChunks(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	fake := &batchMutateItemsBitmapIndexServer{}
	pb.RegisterBitmapIndexServer(server, fake)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(dialer), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})

	items := make([]*pb.IndexKVPair, bitmapBatchMutateItemsChunkSize+1)
	for i := range items {
		items[i] = &pb.IndexKVPair{
			IndexPath: "orders/o_orderkey",
			Key:       ToBytes(int64(1)),
			Value:     [][]byte{[]byte("payload")},
			Time:      int64(i),
		}
	}

	index := NewBitmapIndex(NewDefaultConnection("batch-mutate-items"))
	profile, err := index.batchMutateItemsNodeProfile(pb.NewBitmapIndexClient(conn), items)
	if err != nil {
		t.Fatalf("batchMutateItemsNodeProfile() error = %v", err)
	}
	if profile.Items != len(items) || profile.SendElapsed <= 0 || profile.TotalElapsed <= 0 {
		t.Fatalf("profile = %+v, want item count and non-zero elapsed timings", profile)
	}
	if got, want := fake.batchSizes(), []int{bitmapBatchMutateItemsChunkSize, 1}; !equalIntSlices(got, want) {
		t.Fatalf("BatchMutateItems batch sizes = %v, want %v", got, want)
	}
	if got := fake.streamCallCount(); got != 0 {
		t.Fatalf("BatchMutate stream calls = %d, want 0", got)
	}
}

func TestBitmapIndexCompareBSIFieldsFansOutToActiveClients(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	fake := &compareFanoutBitmapIndexServer{}
	pb.RegisterBitmapIndexServer(server, fake)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	conns := make([]*grpc.ClientConn, 3)
	for i := range conns {
		conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(dialer), grpc.WithInsecure())
		if err != nil {
			t.Fatalf("dial bufnet: %v", err)
		}
		conns[i] = conn
		t.Cleanup(func() {
			_ = conn.Close()
		})
	}

	conn := NewDefaultConnection("compare-fanout")
	conn.ServicePort = 4010
	conn.ids = []string{"node-0", "node-1", "node-2"}
	conn.clientConn = conns
	for _, id := range conn.ids {
		conn.nodeStatusMap.Store(id, &pb.StatusMessage{NodeState: "Active"})
	}

	index := NewBitmapIndex(conn)
	matches, err := index.CompareBSIFields("lineitem", "l_receiptdate", "l_commitdate", 10, 20, roaring64.BitmapOf(1, 2, 3), roaring64.GT, false)
	if err != nil {
		t.Fatalf("CompareBSIFields() error = %v", err)
	}
	if got, want := fake.callCount(), 3; got != want {
		t.Fatalf("compare fanout calls = %d, want %d", got, want)
	}
	if got, want := matches.GetCardinality(), uint64(3); got != want {
		t.Fatalf("matches cardinality = %d, want %d", got, want)
	}

	matches, stats, err := index.CompareBSIFieldsWithStats("lineitem", "l_receiptdate", "l_commitdate", 10, 20, roaring64.BitmapOf(1, 2, 3), roaring64.GT, false)
	if err != nil {
		t.Fatalf("CompareBSIFieldsWithStats() error = %v", err)
	}
	if got, want := fake.callCount(), 6; got != want {
		t.Fatalf("compare fanout calls after stats call = %d, want %d", got, want)
	}
	if got, want := matches.GetCardinality(), uint64(3); got != want {
		t.Fatalf("stats matches cardinality = %d, want %d", got, want)
	}
	if stats.Nodes != 3 || stats.OutputRows != 3 {
		t.Fatalf("stats nodes/output = %d/%d, want 3/3", stats.Nodes, stats.OutputRows)
	}
	if stats.Left.ShardsVisited != 3 || stats.Right.ShardsRetained != 6 {
		t.Fatalf("stats projections = left visited %d right retained %d, want 3/6", stats.Left.ShardsVisited, stats.Right.ShardsRetained)
	}
	if stats.CompareElapsed != 6*time.Millisecond {
		t.Fatalf("compare elapsed = %s, want 6ms", stats.CompareElapsed)
	}
}

func TestAggregateRelationshipAlignedValueSumResponsesMergesPartials(t *testing.T) {
	groups, stats, ok, err := aggregateRelationshipAlignedValueSumResponses([]*pb.RelationshipAlignedValueSumResponse{
		{
			Ok: true,
			Groups: []*pb.RelationshipAlignedValueSumGroup{{
				ParentValue:       7,
				RepresentativeRow: 101,
				Count:             1,
				Sum:               "10",
			}},
			Stats: &pb.RelationshipAlignedValueSumStats{
				ProjectionElapsedNanos: (2 * time.Millisecond).Nanoseconds(),
			},
		},
		{
			Ok: true,
			Groups: []*pb.RelationshipAlignedValueSumGroup{{
				ParentValue:       7,
				RepresentativeRow: 102,
				Count:             2,
				Sum:               "20",
			}, {
				ParentValue:       8,
				RepresentativeRow: 103,
				Count:             1,
				Sum:               "5",
			}},
			Stats: &pb.RelationshipAlignedValueSumStats{
				ProjectionElapsedNanos: (3 * time.Millisecond).Nanoseconds(),
			},
		},
	}, []uint64{101, 102, 103}, []uint64{7, 7, 8})
	if err != nil {
		t.Fatalf("aggregateRelationshipAlignedValueSumResponses() error = %v", err)
	}
	if !ok {
		t.Fatal("aggregateRelationshipAlignedValueSumResponses() ok = false, want true")
	}
	if stats.Rows != 3 || stats.SourceValues != 2 || stats.TargetRows != 4 || stats.Groups != 2 || stats.Values != 2 {
		t.Fatalf("stats = %+v, want rows/source/target/groups/values 3/2/4/2/2", stats)
	}
	if stats.Nodes != 2 || stats.ProjectionElapsed != 5*time.Millisecond {
		t.Fatalf("stats nodes/projection elapsed = %d/%s, want 2/5ms", stats.Nodes, stats.ProjectionElapsed)
	}
	if len(groups) != 2 {
		t.Fatalf("groups len = %d, want 2", len(groups))
	}
	if groups[0].ParentValue != 7 || groups[0].RepresentativeRow != 101 || groups[0].Count != 3 || groups[0].Sum.Cmp(big.NewInt(30)) != 0 {
		t.Fatalf("groups[0] = %+v, want parent 7 count 3 sum 30", groups[0])
	}
	if groups[1].ParentValue != 8 || groups[1].RepresentativeRow != 103 || groups[1].Count != 1 || groups[1].Sum.Cmp(big.NewInt(5)) != 0 {
		t.Fatalf("groups[1] = %+v, want parent 8 count 1 sum 5", groups[1])
	}
}

func TestAggregateRelationshipAlignedValueSumResponsesRequiresAllNodesOK(t *testing.T) {
	_, _, ok, err := aggregateRelationshipAlignedValueSumResponses([]*pb.RelationshipAlignedValueSumResponse{
		{
			Ok: true,
			Groups: []*pb.RelationshipAlignedValueSumGroup{{
				ParentValue:       7,
				RepresentativeRow: 101,
				Count:             1,
				Sum:               "10",
			}},
		},
		{Ok: false},
	}, []uint64{101}, []uint64{7})
	if err != nil {
		t.Fatalf("aggregateRelationshipAlignedValueSumResponses() error = %v", err)
	}
	if ok {
		t.Fatal("aggregateRelationshipAlignedValueSumResponses() ok = true with one non-ok node, want false")
	}
}

func TestAggregateBitmapGroupAggregateResponsesMergesPartials(t *testing.T) {
	groups, stats, ok, err := aggregateBitmapGroupAggregateResponses([]*pb.BitmapGroupAggregatesResponse{
		{
			Ok: true,
			Groups: []*pb.BitmapGroupAggregateGroup{{
				Values: []uint64{1, 10},
				Aggs: []*pb.BitmapGroupAggregateValue{{
					Count: 2,
				}, {
					Count: 2,
					Sum:   "30",
					Min:   "10",
					Max:   "20",
				}},
			}},
			Stats: &pb.BitmapGroupAggregateStats{
				ValueCount:             4,
				BsiProjectElapsedNanos: (2 * time.Millisecond).Nanoseconds(),
				SumElapsedNanos:        (1 * time.Millisecond).Nanoseconds(),
			},
		},
		{
			Ok: true,
			Groups: []*pb.BitmapGroupAggregateGroup{{
				Values: []uint64{1, 10},
				Aggs: []*pb.BitmapGroupAggregateValue{{
					Count: 1,
				}, {
					Count: 1,
					Sum:   "7",
					Min:   "7",
					Max:   "7",
				}},
			}, {
				Values: []uint64{2, 10},
				Aggs: []*pb.BitmapGroupAggregateValue{{
					Count: 1,
				}, {
					Count: 1,
					Sum:   "5",
					Min:   "5",
					Max:   "5",
				}},
			}},
			Stats: &pb.BitmapGroupAggregateStats{
				ValueCount:             3,
				BsiProjectElapsedNanos: (3 * time.Millisecond).Nanoseconds(),
				SumElapsedNanos:        (2 * time.Millisecond).Nanoseconds(),
			},
		},
	}, 4, 2, 2)
	if err != nil {
		t.Fatalf("aggregateBitmapGroupAggregateResponses() error = %v", err)
	}
	if !ok {
		t.Fatal("aggregateBitmapGroupAggregateResponses() ok = false, want true")
	}
	if stats.Nodes != 2 || stats.CandidateRows != 4 || stats.FieldCount != 2 || stats.AggregateCount != 2 || stats.ValueCount != 4 || stats.Groups != 2 {
		t.Fatalf("stats = %+v, want nodes/candidates/fields/aggs/values/groups 2/4/2/2/4/2", stats)
	}
	if stats.BSIProjectElapsed != 5*time.Millisecond || stats.SumElapsed != 3*time.Millisecond {
		t.Fatalf("stats elapsed = %s/%s, want 5ms/3ms", stats.BSIProjectElapsed, stats.SumElapsed)
	}
	if len(groups) != 2 {
		t.Fatalf("groups len = %d, want 2", len(groups))
	}
	if got, want := groups[0].Values, []uint64{1, 10}; !equalUint64Slices(got, want) {
		t.Fatalf("groups[0] values = %#v, want %#v", got, want)
	}
	if groups[0].Aggs[0].Count != 3 || groups[0].Aggs[1].Count != 3 || groups[0].Aggs[1].Sum.Cmp(big.NewInt(37)) != 0 || groups[0].Aggs[1].Min.Cmp(big.NewInt(7)) != 0 || groups[0].Aggs[1].Max.Cmp(big.NewInt(20)) != 0 {
		t.Fatalf("groups[0] aggregates = %+v, want merged count 3 sum 37 min 7 max 20", groups[0].Aggs)
	}
	if got, want := groups[1].Values, []uint64{2, 10}; !equalUint64Slices(got, want) {
		t.Fatalf("groups[1] values = %#v, want %#v", got, want)
	}
}

func TestAggregateBitmapGroupAggregateResponsesRequiresAllNodesOK(t *testing.T) {
	_, _, ok, err := aggregateBitmapGroupAggregateResponses([]*pb.BitmapGroupAggregatesResponse{
		{
			Ok: true,
			Groups: []*pb.BitmapGroupAggregateGroup{{
				Values: []uint64{1},
				Aggs:   []*pb.BitmapGroupAggregateValue{{Count: 1}},
			}},
		},
		{Ok: false},
	}, 1, 1, 1)
	if err != nil {
		t.Fatalf("aggregateBitmapGroupAggregateResponses() error = %v", err)
	}
	if ok {
		t.Fatal("aggregateBitmapGroupAggregateResponses() ok = true with one non-ok node, want false")
	}
}

type batchMutateItemsBitmapIndexServer struct {
	pb.UnimplementedBitmapIndexServer
	mu             sync.Mutex
	batchSizesSeen []int
	streamCalls    int
}

func (s *batchMutateItemsBitmapIndexServer) BatchMutateItems(_ context.Context, batch *pb.IndexKVBatch) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchSizesSeen = append(s.batchSizesSeen, len(batch.GetItems()))
	return &emptypb.Empty{}, nil
}

func (s *batchMutateItemsBitmapIndexServer) BatchMutate(pb.BitmapIndex_BatchMutateServer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streamCalls++
	return nil
}

func (s *batchMutateItemsBitmapIndexServer) batchSizes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.batchSizesSeen...)
}

func (s *batchMutateItemsBitmapIndexServer) streamCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streamCalls
}

type compareFanoutBitmapIndexServer struct {
	pb.UnimplementedBitmapIndexServer
	mu    sync.Mutex
	calls int
}

func (s *compareFanoutBitmapIndexServer) CompareBSIFields(context.Context, *pb.CompareBSIFieldsRequest) (*pb.CompareBSIFieldsResponse, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()

	bitmap := roaring64.BitmapOf(uint64(call))
	data, err := bitmap.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return &pb.CompareBSIFieldsResponse{
		Rownums: data,
		Stats: &pb.CompareBSIFieldsStats{
			Left: &pb.BSIProjectionStats{
				ShardsVisited: 1,
			},
			Right: &pb.BSIProjectionStats{
				ShardsRetained: 2,
			},
			CompareElapsedNanos: (2 * time.Millisecond).Nanoseconds(),
			OutputRows:          1,
		},
	}, nil
}

func (s *compareFanoutBitmapIndexServer) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func equalIntSlices(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func equalUint64Slices(left, right []uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

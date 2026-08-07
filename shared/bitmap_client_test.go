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

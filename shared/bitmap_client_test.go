package shared

import (
	"math/big"
	"strings"
	"testing"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"google.golang.org/grpc"
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

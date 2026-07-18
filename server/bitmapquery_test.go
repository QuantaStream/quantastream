package server

import (
	"math/big"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/hashicorp/consul/api"
	"github.com/stvp/rendezvous"
)

func TestTimeRangeBSIReadsLocalNonTimeShardRegardlessOfCurrentOwner(t *testing.T) {
	table, err := shared.LoadSchema("../tpc-h-benchmark/config", "part", nil)
	if err != nil {
		t.Fatalf("load part schema: %v", err)
	}
	bsi := roaring64.NewDefaultBSI()
	bsi.SetBigValue(42, big.NewInt(15))

	index := &BitmapIndex{
		Node: &Node{
			Conn: &shared.Conn{
				HashTable: rendezvous.New([]string{"other-node"}),
			},
			consul:  &api.Client{},
			hashKey: "this-node",
		},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"part": {
				"p_size": {
					time.Unix(0, 0).UnixNano(): {BSI: bsi},
				},
			},
		},
		tableCache: map[string]*shared.BasicTable{
			"part": table,
		},
	}

	result, err := index.timeRangeBSI("part", "p_size", time.Unix(0, 0), time.Unix(0, 0), nil, false)
	if err != nil {
		t.Fatalf("timeRangeBSI() error = %v", err)
	}
	if got := result.GetExistenceBitmap().GetCardinality(); got != 1 {
		t.Fatalf("existence cardinality = %d, want 1", got)
	}
}

package server

import (
	"math/big"
	"reflect"
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

	result, err := index.timeRangeBSI("part", "p_size", time.Unix(0, 0), time.Unix(0, 0), nil, false, true)
	if err != nil {
		t.Fatalf("timeRangeBSI() error = %v", err)
	}
	if got := result.GetExistenceBitmap().GetCardinality(); got != 1 {
		t.Fatalf("existence cardinality = %d, want 1", got)
	}
}

func TestCompareBSIFieldsWithStatsReturnsMatchingRownums(t *testing.T) {
	table, err := shared.LoadSchema("../tpc-h-benchmark/config", "part", nil)
	if err != nil {
		t.Fatalf("load part schema: %v", err)
	}
	retail := roaring64.NewDefaultBSI()
	retail.SetValue(1, 100)
	retail.SetValue(2, 200)
	retail.SetValue(3, 300)
	size := roaring64.NewDefaultBSI()
	size.SetValue(1, 90)
	size.SetValue(2, 200)
	size.SetValue(3, 400)

	index := &BitmapIndex{
		Node: &Node{Conn: shared.NewDefaultConnection("compare-bsi-test")},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"part": {
				"p_retailprice": {
					time.Unix(0, 0).UnixNano(): {BSI: retail},
				},
				"p_size": {
					time.Unix(0, 0).UnixNano(): {BSI: size},
				},
			},
		},
		tableCache: map[string]*shared.BasicTable{
			"part": table,
		},
	}

	matches, stats, err := index.CompareBSIFieldsWithStats("part", "p_retailprice", "p_size", 0, 0, roaring64.BitmapOf(1, 2, 3), roaring64.GT, false)
	if err != nil {
		t.Fatalf("CompareBSIFieldsWithStats: %v", err)
	}
	if got, want := matches.ToArray(), []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("matches = %#v, want %#v", got, want)
	}
	if stats.OutputRows != 1 {
		t.Fatalf("output rows = %d, want 1", stats.OutputRows)
	}
	if stats.Left.ShardsRetained == 0 || stats.Right.ShardsRetained == 0 {
		t.Fatalf("retained stats = left %d right %d, want both nonzero", stats.Left.ShardsRetained, stats.Right.ShardsRetained)
	}
}

func TestCompareBSIFieldsWithStatsUsesLocalReplicasForTimeShardedFields(t *testing.T) {
	table, err := shared.LoadSchema("../tpc-h-benchmark/config", "lineitem", nil)
	if err != nil {
		t.Fatalf("load lineitem schema: %v", err)
	}
	day := time.Date(1995, 3, 15, 0, 0, 0, 0, time.UTC)
	receipt := roaring64.NewDefaultBSI()
	receipt.SetBigValue(42, big.NewInt(day.Add(48*time.Hour).UnixMilli()))
	commit := roaring64.NewDefaultBSI()
	commit.SetBigValue(42, big.NewInt(day.Add(24*time.Hour).UnixMilli()))

	index := &BitmapIndex{
		Node: &Node{
			Conn: &shared.Conn{
				HashTable: rendezvous.New([]string{"different-owner"}),
			},
			consul:  &api.Client{},
			hashKey: "this-node",
		},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"lineitem": {
				"l_receiptdate": {
					day.UnixNano(): {BSI: receipt},
				},
				"l_commitdate": {
					day.UnixNano(): {BSI: commit},
				},
			},
		},
		tableCache: map[string]*shared.BasicTable{
			"lineitem": table,
		},
	}

	owned, ownedStats, err := index.ProjectBSIWithStats("lineitem", "l_receiptdate", day.UnixNano(), day.UnixNano(), roaring64.BitmapOf(42), false)
	if err != nil {
		t.Fatalf("ProjectBSIWithStats: %v", err)
	}
	if got := owned.GetCardinality(); got != 0 {
		t.Fatalf("owned projection cardinality = %d, want 0", got)
	}
	if ownedStats.ShardsLocal != 0 {
		t.Fatalf("owned projection local shards = %d, want 0", ownedStats.ShardsLocal)
	}

	matches, stats, err := index.CompareBSIFieldsWithStats("lineitem", "l_receiptdate", "l_commitdate", day.UnixNano(), day.UnixNano(), roaring64.BitmapOf(42), roaring64.GT, false)
	if err != nil {
		t.Fatalf("CompareBSIFieldsWithStats: %v", err)
	}
	if got, want := matches.ToArray(), []uint64{42}; !reflect.DeepEqual(got, want) {
		t.Fatalf("matches = %#v, want %#v", got, want)
	}
	if stats.Left.ShardsLocal != 1 || stats.Right.ShardsLocal != 1 {
		t.Fatalf("compare local shards = left %d right %d, want both 1", stats.Left.ShardsLocal, stats.Right.ShardsLocal)
	}
}

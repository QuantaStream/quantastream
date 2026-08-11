package server

import (
	"reflect"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/hashicorp/consul/api"
	"github.com/stvp/rendezvous"
)

func TestBitmapGroupCountsIntersectsBitmapFields(t *testing.T) {
	table, err := shared.LoadSchema("../tpc-h-benchmark/config", "lineitem", nil)
	if err != nil {
		t.Fatalf("load lineitem schema: %v", err)
	}
	index := &BitmapIndex{
		Node: &Node{
			Conn: &shared.Conn{
				HashTable: rendezvous.New([]string{"this-node"}),
			},
			consul:  &api.Client{},
			hashKey: "this-node",
		},
		bitmapCache: map[string]map[string]map[uint64]map[int64]*StandardBitmap{
			"lineitem": {
				"l_returnflag": {
					1: {0: {Bits: roaring64.BitmapOf(1, 2, 4)}},
					2: {0: {Bits: roaring64.BitmapOf(3, 5)}},
				},
				"l_linestatus": {
					10: {0: {Bits: roaring64.BitmapOf(1, 3, 4)}},
					20: {0: {Bits: roaring64.BitmapOf(2, 5)}},
				},
			},
		},
		tableCache: map[string]*shared.BasicTable{"lineitem": table},
	}

	groups, stats, ok, err := index.BitmapGroupCounts("lineitem", []string{"l_returnflag", "l_linestatus"}, 0, 0, roaring64.BitmapOf(1, 2, 3, 4, 5))
	if err != nil {
		t.Fatalf("BitmapGroupCounts error = %v", err)
	}
	if !ok {
		t.Fatalf("BitmapGroupCounts ok = false, want true")
	}
	want := []BitmapGroupCount{
		{Values: []uint64{1, 10}, Count: 2},
		{Values: []uint64{1, 20}, Count: 1},
		{Values: []uint64{2, 10}, Count: 1},
		{Values: []uint64{2, 20}, Count: 1},
	}
	if !reflect.DeepEqual(groups, want) {
		t.Fatalf("groups = %#v, want %#v", groups, want)
	}
	if stats.CandidateRows != 5 || stats.FieldCount != 2 || stats.ValueCount != 4 || stats.Groups != 4 {
		t.Fatalf("stats = %#v, want candidateRows=5 fieldCount=2 valueCount=4 groups=4", stats)
	}

	groups, _, ok, err = index.BitmapGroupCounts("lineitem", []string{"l_returnflag", "l_linestatus"}, 0, 0, roaring64.BitmapOf(1, 3))
	if err != nil || !ok {
		t.Fatalf("filtered BitmapGroupCounts ok=%t err=%v, want ok", ok, err)
	}
	want = []BitmapGroupCount{
		{Values: []uint64{1, 10}, Count: 1},
		{Values: []uint64{2, 10}, Count: 1},
	}
	if !reflect.DeepEqual(groups, want) {
		t.Fatalf("filtered groups = %#v, want %#v", groups, want)
	}
}

func TestBitmapGroupCountsDeclinesNonBitmapField(t *testing.T) {
	table, err := shared.LoadSchema("../tpc-h-benchmark/config", "lineitem", nil)
	if err != nil {
		t.Fatalf("load lineitem schema: %v", err)
	}
	index := &BitmapIndex{
		Node: &Node{
			Conn: &shared.Conn{
				HashTable: rendezvous.New([]string{"this-node"}),
			},
			consul:  &api.Client{},
			hashKey: "this-node",
		},
		bitmapCache: map[string]map[string]map[uint64]map[int64]*StandardBitmap{
			"lineitem": {
				"l_returnflag": {
					1: {time.Unix(0, 0).UnixNano(): {Bits: roaring64.BitmapOf(1)}},
				},
			},
		},
		tableCache: map[string]*shared.BasicTable{"lineitem": table},
	}

	_, _, ok, err := index.BitmapGroupCounts("lineitem", []string{"l_returnflag", "l_quantity"}, 0, 0, roaring64.BitmapOf(1))
	if err != nil {
		t.Fatalf("BitmapGroupCounts error = %v", err)
	}
	if ok {
		t.Fatalf("BitmapGroupCounts ok = true for non-bitmap field, want false")
	}
}

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

func TestBitmapGroupAggregatesComputesBSIValuesByBitmapGroup(t *testing.T) {
	table, err := shared.LoadSchema("../tpc-h-benchmark/config", "lineitem", nil)
	if err != nil {
		t.Fatalf("load lineitem schema: %v", err)
	}
	quantity := roaring64.NewDefaultBSI()
	quantity.SetValue(1, 10)
	quantity.SetValue(2, 20)
	quantity.SetValue(3, 30)
	quantity.SetValue(4, 40)
	quantity.SetValue(5, 50)
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
			},
		},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"lineitem": {
				"l_quantity": {
					0: {BSI: quantity},
				},
			},
		},
		tableCache: map[string]*shared.BasicTable{"lineitem": table},
	}

	groups, stats, ok, err := index.BitmapGroupAggregatesStorage("lineitem", []string{"l_returnflag"}, []BitmapGroupAggregateSpec{
		{Function: "count"},
		{Function: "sum", Field: "l_quantity"},
		{Function: "avg", Field: "l_quantity"},
		{Function: "min", Field: "l_quantity"},
		{Function: "max", Field: "l_quantity"},
	}, 0, 0, roaring64.BitmapOf(1, 2, 3, 4, 5))
	if err != nil {
		t.Fatalf("BitmapGroupAggregates error = %v", err)
	}
	if !ok {
		t.Fatalf("BitmapGroupAggregates ok = false, want true")
	}
	if len(groups) != 2 {
		t.Fatalf("groups = %#v, want 2", groups)
	}
	assertBitmapGroupAggregate(t, groups[0], []uint64{1}, []uint64{3, 70, 70, 10, 40})
	assertBitmapGroupAggregate(t, groups[1], []uint64{2}, []uint64{2, 80, 80, 30, 50})
	if stats.CandidateRows != 5 || stats.AggregateCount != 5 || stats.BSIFieldCount != 1 {
		t.Fatalf("stats = %#v, want candidateRows=5 aggregateCount=5 bsiFieldCount=1", stats)
	}
}

func TestBitmapGroupAggregatesMinMaxFallsBackForNegativeValues(t *testing.T) {
	table, err := shared.LoadSchema("../tpc-h-benchmark/config", "lineitem", nil)
	if err != nil {
		t.Fatalf("load lineitem schema: %v", err)
	}
	quantity := roaring64.NewDefaultBSI()
	quantity.SetValue(1, -10)
	quantity.SetValue(2, 20)
	quantity.SetValue(3, -30)
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
					1: {0: {Bits: roaring64.BitmapOf(1, 2, 3)}},
				},
			},
		},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"lineitem": {
				"l_quantity": {
					0: {BSI: quantity},
				},
			},
		},
		tableCache: map[string]*shared.BasicTable{"lineitem": table},
	}

	groups, _, ok, err := index.BitmapGroupAggregatesStorage("lineitem", []string{"l_returnflag"}, []BitmapGroupAggregateSpec{
		{Function: "min", Field: "l_quantity"},
		{Function: "max", Field: "l_quantity"},
	}, 0, 0, roaring64.BitmapOf(1, 2, 3))
	if err != nil {
		t.Fatalf("BitmapGroupAggregates error = %v", err)
	}
	if !ok {
		t.Fatalf("BitmapGroupAggregates ok = false, want true")
	}
	if len(groups) != 1 {
		t.Fatalf("groups = %#v, want 1", groups)
	}
	if got := groups[0].Aggs[0].Min; got == nil || got.Cmp(big.NewInt(-30)) != 0 {
		t.Fatalf("min = %v, want -30", got)
	}
	if got := groups[0].Aggs[1].Max; got == nil || got.Cmp(big.NewInt(20)) != 0 {
		t.Fatalf("max = %v, want 20", got)
	}
}

func assertBitmapGroupAggregate(t *testing.T, group BitmapGroupAggregate, values []uint64, raw []uint64) {
	t.Helper()
	if !reflect.DeepEqual(group.Values, values) {
		t.Fatalf("group values = %#v, want %#v", group.Values, values)
	}
	if len(group.Aggs) != len(raw) {
		t.Fatalf("aggs = %#v, want %d entries", group.Aggs, len(raw))
	}
	for i, want := range raw {
		got := bitmapGroupAggregateRawValue(group.Aggs[i])
		if got.Cmp(new(big.Int).SetUint64(want)) != 0 {
			t.Fatalf("agg[%d] = %s, want %d", i, got.String(), want)
		}
	}
}

func bitmapGroupAggregateRawValue(value BitmapGroupAggregateValue) *big.Int {
	switch {
	case value.Sum != nil:
		return value.Sum
	case value.Min != nil:
		return value.Min
	case value.Max != nil:
		return value.Max
	default:
		return new(big.Int).SetUint64(value.Count)
	}
}

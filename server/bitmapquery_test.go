package server

import (
	"context"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/hashicorp/consul/api"
	"github.com/stvp/rendezvous"
)

func TestBitmapIndexBSIShardYearRange(t *testing.T) {
	index := &BitmapIndex{
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"lineitem": {
				"l_shipdate": {
					time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(): nil,
					time.Date(1994, 7, 1, 0, 0, 0, 0, time.UTC).UnixNano(): nil,
					time.Date(1998, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(): nil,
				},
			},
		},
	}

	minYear, maxYear, ok := index.BSIShardYearRange("LINEITEM", "L_SHIPDATE")
	if !ok {
		t.Fatal("BSIShardYearRange ok = false, want true")
	}
	if minYear != 1992 || maxYear != 1998 {
		t.Fatalf("BSIShardYearRange = %d..%d, want 1992..1998", minYear, maxYear)
	}
}

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

func TestQueryRetainsBSIRangeWithPriorIntersectSeed(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "config", "lineitem")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("create config dir: %v", err)
	}
	schema := []byte(`
tableName: lineitem
primaryKey: l_receiptdate
timeQuantumType: YMD
timeQuantumField: l_receiptdate
attributes:
- fieldName: l_receiptdate
  sourceName: /data/l_receiptdate
  mappingStrategy: SysMillisBSI
  type: DateTime
- fieldName: l_shipmode
  sourceName: /data/l_shipmode
  mappingStrategy: StringEnum
  type: String
`)
	if err := os.WriteFile(filepath.Join(configDir, "schema.yaml"), schema, 0644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	table, err := shared.LoadSchema(filepath.Join(root, "config"), "lineitem", nil)
	if err != nil {
		t.Fatalf("load schema: %v", err)
	}
	day := time.Date(2023, 6, 1, 0, 0, 0, 0, time.UTC)
	receipt := roaring64.NewDefaultBSI()
	receipt.SetBigValue(1, big.NewInt(day.UnixMilli()))
	receipt.SetBigValue(2, big.NewInt(day.UnixMilli()))
	receipt.SetBigValue(3, big.NewInt(day.UnixMilli()))
	shipmode := roaring64.BitmapOf(1, 3)
	index := &BitmapIndex{
		Node: &Node{Conn: shared.NewDefaultConnection("query-retain-bsi-test")},
		bitmapCache: map[string]map[string]map[uint64]map[int64]*StandardBitmap{
			"lineitem": {
				"l_shipmode": {
					7: {
						day.UnixNano(): {Bits: shipmode},
					},
				},
			},
		},
		bsiCache: map[string]map[string]map[int64]*BSIBitmap{
			"lineitem": {
				"l_receiptdate": {
					day.UnixNano(): {BSI: receipt},
				},
			},
		},
		tableCache: map[string]*shared.BasicTable{
			"lineitem": table,
		},
	}
	query := &pb.BitmapQuery{
		FromTime: day.UnixNano(),
		ToTime:   day.UnixNano(),
		Query: []*pb.QueryFragment{
			{
				Id:          "shipmode",
				Index:       "lineitem",
				Field:       "l_shipmode",
				RowID:       7,
				Operation:   pb.QueryFragment_INTERSECT,
				ChildrenIds: []string{"receipt"},
			},
			{
				Id:        "receipt",
				Index:     "lineitem",
				Field:     "l_receiptdate",
				Operation: pb.QueryFragment_INTERSECT,
				BsiOp:     pb.QueryFragment_RANGE,
				Begin:     big.NewInt(day.UnixMilli()).Bytes(),
				End:       big.NewInt(day.UnixMilli()).Bytes(),
			},
		},
	}

	result, err := index.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("Query returned error: %v", err)
	}
	if got := len(result.GetIntersects()); got != 2 {
		t.Fatalf("intersect count = %d, want 2", got)
	}
	receiptBitmap := roaring64.New()
	if err := receiptBitmap.UnmarshalBinary(result.GetIntersects()[1]); err != nil {
		t.Fatalf("unmarshal receipt intersect: %v", err)
	}
	if got, want := receiptBitmap.ToArray(), []uint64{1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("retained receipt rows = %#v, want %#v", got, want)
	}
	if got, want := receipt.GetExistenceBitmap().ToArray(), []uint64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fixture receipt rows = %#v, want %#v", got, want)
	}
	shipmodeBitmap := roaring64.New()
	if err := shipmodeBitmap.UnmarshalBinary(result.GetIntersects()[0]); err != nil {
		t.Fatalf("unmarshal shipmode intersect: %v", err)
	}
	shipmodeBitmap.And(receiptBitmap)
	if got, want := shipmodeBitmap.ToArray(), []uint64{1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final rows = %#v, want %#v", got, want)
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

func TestCleanupOpTreatsMissingHashTableAsLocalOwner(t *testing.T) {
	table, err := shared.LoadSchema("../tpc-h-benchmark/config", "part", nil)
	if err != nil {
		t.Fatalf("load part schema: %v", err)
	}
	index := &BitmapIndex{
		Node: &Node{
			Conn:    shared.NewDefaultConnection("cleanup-local-test"),
			consul:  &api.Client{},
			hashKey: "this-node",
		},
		partitionQueue: make(chan *PartitionOperation, 1),
		tableCache: map[string]*shared.BasicTable{
			"part": table,
		},
	}

	err = index.cleanupOp(&Partition{
		Index:       "part",
		Field:       "p_size",
		Time:        time.Unix(0, 0),
		RowIDOrBits: -1,
	})
	if err != nil {
		t.Fatalf("cleanupOp returned error: %v", err)
	}
	select {
	case op := <-index.partitionQueue:
		t.Fatalf("cleanupOp queued unexpected partition operation: %#v", op)
	default:
	}
}

func TestCleanupOpQueuesRemoveForDistributedNonOwner(t *testing.T) {
	table, err := shared.LoadSchema("../tpc-h-benchmark/config", "part", nil)
	if err != nil {
		t.Fatalf("load part schema: %v", err)
	}
	index := &BitmapIndex{
		Node: &Node{
			Conn: &shared.Conn{
				HashTable: rendezvous.New([]string{"other-node"}),
				Replicas:  1,
			},
			consul:  &api.Client{},
			hashKey: "this-node",
		},
		partitionQueue: make(chan *PartitionOperation, 1),
		tableCache: map[string]*shared.BasicTable{
			"part": table,
		},
	}

	err = index.cleanupOp(&Partition{
		Index:       "part",
		Field:       "p_size",
		Time:        time.Unix(0, 0),
		RowIDOrBits: -1,
	})
	if err != nil {
		t.Fatalf("cleanupOp returned error: %v", err)
	}
	select {
	case op := <-index.partitionQueue:
		if op == nil || !op.RemoveOnly {
			t.Fatalf("cleanupOp queued operation = %#v, want remove-only operation", op)
		}
	default:
		t.Fatal("cleanupOp did not queue remove operation for distributed non-owner")
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

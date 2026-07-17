package shared

import (
	"math/big"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestBitmapBatchPredicateSerializesValues(t *testing.T) {
	q := NewBitmapQuery()
	frag := q.NewQueryFragment()
	frag.SetBitmapBatchPredicate("part", "p_type", []*big.Int{
		big.NewInt(11),
		big.NewInt(13),
	})
	q.AddFragment(frag)

	proto := q.ToProto()
	if len(proto.Query) != 1 {
		t.Fatalf("expected one query fragment, got %d", len(proto.Query))
	}
	values := proto.Query[0].Values
	if len(values) != 2 {
		t.Fatalf("expected two serialized values, got %d", len(values))
	}
	if got := new(big.Int).SetBytes(values[0]).Int64(); got != 11 {
		t.Fatalf("expected first value 11, got %d", got)
	}
	if got := new(big.Int).SetBytes(values[1]).Int64(); got != 13 {
		t.Fatalf("expected second value 13, got %d", got)
	}
}

func TestBitmapQueryToProtoPreservesRangeEnd(t *testing.T) {
	query := NewBitmapQuery()
	fragment := query.NewQueryFragment()
	begin := big.NewInt(8)
	end := big.NewInt(12)
	fragment.SetBSIRangePredicate("orders", "o_orderkey", begin, end)
	query.AddFragment(fragment)

	proto := query.ToProto()
	if len(proto.Query) != 1 {
		t.Fatalf("fragments = %d, want 1", len(proto.Query))
	}
	if got := new(big.Int).SetBytes(proto.Query[0].Begin).Int64(); got != 8 {
		t.Fatalf("begin = %d, want 8", got)
	}
	if got := new(big.Int).SetBytes(proto.Query[0].End).Int64(); got != 12 {
		t.Fatalf("wire end = %d, want 12", got)
	}
	if got := end.Int64(); got != 12 {
		t.Fatalf("source end mutated to %d, want 12", got)
	}
}

func TestIntermediateResultCountCanExceedCollapsedBitmapCardinality(t *testing.T) {
	result := NewIntermediateResult("lineitem")
	result.AddUnion(roaring64.BitmapOf(1, 2))
	result.AddUnion(roaring64.BitmapOf(1, 2))
	result.Collapse()
	result.SetCount(4)

	if got, want := result.GetFinalUnion().GetCardinality(), uint64(2); got != want {
		t.Fatalf("collapsed bitmap cardinality = %d, want %d", got, want)
	}
	if got, want := result.Count(), uint64(4); got != want {
		t.Fatalf("distributed count = %d, want %d", got, want)
	}
}

func TestIntermediateResultFinalBitmapAppliesLocalPredicates(t *testing.T) {
	result := NewIntermediateResult("lineitem")
	result.AddUnion(roaring64.BitmapOf(1, 2, 3, 4))
	result.AddIntersect(roaring64.BitmapOf(2, 3, 4))
	result.AddAndDifference(roaring64.BitmapOf(4))
	result.Collapse()

	final := intermediateResultFinalBitmap(result)
	if got, want := final.GetCardinality(), uint64(2); got != want {
		t.Fatalf("final bitmap cardinality = %d, want %d", got, want)
	}
	if !final.Contains(2) || !final.Contains(3) || final.Contains(1) || final.Contains(4) {
		t.Fatalf("final bitmap = %#v, want only 2 and 3", final.ToArray())
	}
}

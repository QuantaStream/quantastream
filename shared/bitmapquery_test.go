package shared

import (
	"math/big"
	"testing"

	pb "github.com/QuantaStream/quantastream/grpc"
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

func TestBitmapQueryToProtoPreservesSignedBSIOperands(t *testing.T) {
	query := NewBitmapQuery()
	fragment := query.NewQueryFragment()
	fragment.SetBSIRangePredicate("qrz_callsigns", "longitude", big.NewInt(-1250000), big.NewInt(-660000))
	query.AddFragment(fragment)

	proto := query.ToProto()
	if len(proto.Query) != 1 {
		t.Fatalf("fragments = %d, want 1", len(proto.Query))
	}
	if got := BigIntFromWireBytes(proto.Query[0].Begin).Int64(); got != -1250000 {
		t.Fatalf("begin = %d, want -1250000", got)
	}
	if got := BigIntFromWireBytes(proto.Query[0].End).Int64(); got != -660000 {
		t.Fatalf("end = %d, want -660000", got)
	}
	roundTripped := FromProto(proto, nil)
	if roundTripped.root == nil {
		t.Fatal("round-trip root is nil")
	}
	if got := roundTripped.root.Begin.Int64(); got != -1250000 {
		t.Fatalf("round-trip begin = %d, want -1250000", got)
	}
	if got := roundTripped.root.End.Int64(); got != -660000 {
		t.Fatalf("round-trip end = %d, want -660000", got)
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

func TestMergeIntermediateBitmapSlotIncludesLaterNodeSlots(t *testing.T) {
	firstNode := NewIntermediateResult("part")
	secondNode := NewIntermediateResult("part")
	secondNode.AddIntersect(roaring64.BitmapOf(7, 9))

	results := []*IntermediateResult{firstNode, secondNode}
	maxSlots := maxIntermediateBitmapSlots(results, func(result *IntermediateResult) []*roaring64.Bitmap {
		return result.GetIntersects()
	})
	if maxSlots != 1 {
		t.Fatalf("max intersect slots = %d, want 1", maxSlots)
	}

	merged := mergeIntermediateBitmapSlot(results, 0, func(result *IntermediateResult) []*roaring64.Bitmap {
		return result.GetIntersects()
	})
	if got, want := merged.GetCardinality(), uint64(2); got != want {
		t.Fatalf("merged cardinality = %d, want %d", got, want)
	}
	if !merged.Contains(7) || !merged.Contains(9) {
		t.Fatalf("merged bitmap = %#v, want values from later node", merged.ToArray())
	}
}

func TestMergeIntermediateBitmapSlotTreatsMissingDifferenceSlotsAsEmpty(t *testing.T) {
	firstNode := NewIntermediateResult("part")
	firstNode.AddAndDifference(roaring64.BitmapOf(3))
	secondNode := NewIntermediateResult("part")

	results := []*IntermediateResult{firstNode, secondNode}
	merged := mergeIntermediateBitmapSlot(results, 0, func(result *IntermediateResult) []*roaring64.Bitmap {
		return result.GetAndDifferences()
	})
	if got, want := merged.GetCardinality(), uint64(1); got != want {
		t.Fatalf("merged cardinality = %d, want %d", got, want)
	}
	if !merged.Contains(3) {
		t.Fatalf("merged bitmap = %#v, want difference from first node", merged.ToArray())
	}
}

func TestDistributedFragmentFanoutEligibilityDeclinesComplexQueries(t *testing.T) {
	simple := &pb.BitmapQuery{Query: []*pb.QueryFragment{{
		Id:        "root",
		Index:     "lineitem",
		Field:     "l_shipdate",
		Operation: pb.QueryFragment_INTERSECT,
		ChildrenIds: []string{
			"child",
		},
	}, {
		Id:        "child",
		Index:     "lineitem",
		Field:     "l_discount",
		Operation: pb.QueryFragment_INTERSECT,
	}}}
	if !distributedFragmentFanoutEligible(simple) {
		t.Fatal("simple tree-shaped intersect query should use distributed fragment fanout")
	}
	if !bitmapQueryShouldUseFragmentFanout(simple, false) {
		t.Fatal("simple query should use distributed fragment fanout when not disabled")
	}
	if bitmapQueryShouldUseFragmentFanout(simple, true) {
		t.Fatal("disabled fragment fanout should use whole-query fanout")
	}
	flattened := flattenBitmapQueryFragments(simple)
	if len(flattened) != 2 {
		t.Fatalf("flattened fragments = %d, want 2", len(flattened))
	}
	if flattened[0].GetId() != "root" || flattened[1].GetId() != "child" {
		t.Fatalf("flattened ids = %#v, want root then child", []string{flattened[0].GetId(), flattened[1].GetId()})
	}

	complex := cloneBitmapQuery(simple)
	complex.Query[0].NullCheck = true
	complex.Query[0].Negate = true
	if !distributedFragmentFanoutEligible(complex) {
		t.Fatal("existence seed query should use distributed fragment fanout")
	}

	complex = cloneBitmapQuery(simple)
	complex.Query[0].NullCheck = true
	if distributedFragmentFanoutEligible(complex) {
		t.Fatal("non-seed null-check query should stay on whole-query fanout")
	}

	join := cloneBitmapQuery(simple)
	join.Query[1].Operation = pb.QueryFragment_INNER_JOIN
	if distributedFragmentFanoutEligible(join) {
		t.Fatal("join query should stay on whole-query fanout")
	}
}

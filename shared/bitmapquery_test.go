package shared

import (
	"math/big"
	"testing"
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

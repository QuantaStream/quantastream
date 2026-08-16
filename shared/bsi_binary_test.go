package shared

import (
	"math/big"
	"testing"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestMarshalBSIPreservesSignedValues(t *testing.T) {
	bsi := roaring64.NewDefaultBSI()
	bsi.SetBigValue(7, big.NewInt(-1710))
	bsi.SetBigValue(9, big.NewInt(998771))

	data, err := MarshalBSI(bsi)
	if err != nil {
		t.Fatalf("MarshalBSI() error = %v", err)
	}
	loaded := roaring64.NewDefaultBSI()
	if err := UnmarshalBSI(loaded, data); err != nil {
		t.Fatalf("UnmarshalBSI() error = %v", err)
	}

	values := loaded.GetBigValues([]uint64{7, 9})
	if values[0] == nil || values[0].Int64() != -1710 {
		t.Fatalf("row 7 = %v, want -1710", values[0])
	}
	if values[1] == nil || values[1].Int64() != 998771 {
		t.Fatalf("row 9 = %v, want 998771", values[1])
	}
}

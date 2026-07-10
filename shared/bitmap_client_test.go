package shared

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
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

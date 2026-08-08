package shared

import (
	"math/big"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

func TestSetValueMarksBatchNonEmpty(t *testing.T) {
	batch := NewBatchBuffer(nil, nil, 1000)

	if err := batch.SetValue("part", "p_partkey", 1, big.NewInt(1), time.Unix(0, 0)); err != nil {
		t.Fatalf("SetValue returned error: %v", err)
	}

	if batch.IsEmpty() {
		t.Fatal("expected BSI SetValue to make batch non-empty")
	}
}

func TestBatchBufferModifiedAtRefreshIsThrottled(t *testing.T) {
	batch := NewBatchBuffer(nil, nil, 1_000_000)
	batch.ModifiedAt = time.Unix(1, 0)

	if err := batch.SetValue("part", "p_partkey", 1, big.NewInt(1), time.Unix(0, 0)); err != nil {
		t.Fatalf("SetValue returned error: %v", err)
	}
	firstTouch := batch.ModifiedAt
	if !firstTouch.After(time.Unix(1, 0)) {
		t.Fatalf("first mutation did not refresh ModifiedAt: %s", firstTouch)
	}
	if batch.pendingMutationCount != 1 {
		t.Fatalf("pendingMutationCount = %d, want 1", batch.pendingMutationCount)
	}

	for i := 0; i < batchModifiedAtRefreshInterval-1; i++ {
		if err := batch.SetValue("part", "p_partkey", uint64(i+2), big.NewInt(1), time.Unix(0, 0)); err != nil {
			t.Fatalf("SetValue returned error: %v", err)
		}
	}
	if !batch.ModifiedAt.Equal(firstTouch) {
		t.Fatalf("ModifiedAt refreshed before throttle interval: got %s want %s", batch.ModifiedAt, firstTouch)
	}

	batch.ModifiedAt = time.Unix(2, 0)
	if err := batch.SetValue("part", "p_partkey", 2000, big.NewInt(1), time.Unix(0, 0)); err != nil {
		t.Fatalf("SetValue returned error: %v", err)
	}
	if !batch.ModifiedAt.After(time.Unix(2, 0)) {
		t.Fatalf("ModifiedAt did not refresh after throttle interval: %s", batch.ModifiedAt)
	}
}

func TestBatchBufferPrimaryKeyIdentityCacheIsLocalToBatch(t *testing.T) {
	batch := NewBatchBuffer(nil, nil, 1000)
	identity := []byte("typed-primary-key-identity")

	batch.SetPrimaryKeyIdentity(identity, 42)
	batch.SetPrimaryKeyIdentity(identity, 99)

	columnID, ok := batch.LookupLocalCIDForPrimaryKeyIdentity(identity)
	if !ok {
		t.Fatal("expected local primary-key identity hit")
	}
	if columnID != 42 {
		t.Fatalf("columnID = %d, want first staged rownum 42", columnID)
	}
	if !batch.IsEmpty() {
		t.Fatal("primary-key identity cache should not make batch flushable")
	}
	if err := batch.Flush(); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if _, ok := batch.LookupLocalCIDForPrimaryKeyIdentity(identity); ok {
		t.Fatal("primary-key identity cache should clear after successful flush")
	}
}

func TestDefaultBSIReportsValueAfterSetBigValue(t *testing.T) {
	bsi := roaring64.NewDefaultBSI()
	bsi.SetBigValue(1, big.NewInt(42))

	t.Logf("cardinality=%d bitCount=%d existence=%d", bsi.GetCardinality(), bsi.BitCount(),
		bsi.GetExistenceBitmap().GetCardinality())

	if bsi.GetExistenceBitmap().GetCardinality() == 0 {
		t.Fatal("expected BSI existence bitmap to include the written column")
	}
}

func TestFormatShardTimeUsesUTC(t *testing.T) {
	cst := time.FixedZone("CST", -6*60*60)
	localEpoch := time.Date(1969, 12, 31, 18, 0, 0, 0, cst)

	if got := formatShardTime(localEpoch); got != "1970-01-01T00" {
		t.Fatalf("expected UTC shard time, got %q", got)
	}
}

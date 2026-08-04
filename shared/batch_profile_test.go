package shared

import (
	"math/big"
	"testing"
	"time"
)

func TestBatchBufferFlushRecordsProfile(t *testing.T) {
	bitmapService := &recordingLocalBitmapIndexService{}
	kvService := &recordingLocalKVStoreService{}
	conn := &Conn{
		LocalNodeServices: LocalNodeServices{
			BitmapIndex: bitmapService,
			KVStore:     kvService,
		},
	}
	buffer := NewBatchBuffer(NewBitmapIndex(conn), NewKVStore(conn), 1000)
	ts := time.Unix(0, 0)

	if err := buffer.SetPartitionedString("sample/city", "Buenos Aires", uint64(101)); err != nil {
		t.Fatalf("SetPartitionedString() error = %v", err)
	}
	if err := buffer.SetBit("sample", "active", 101, 1, ts, false); err != nil {
		t.Fatalf("SetBit() error = %v", err)
	}
	if err := buffer.SetValue("sample", "age", 101, big.NewInt(42), ts); err != nil {
		t.Fatalf("SetValue() error = %v", err)
	}
	if err := buffer.ClearValue("sample", "old_age", 101, ts); err != nil {
		t.Fatalf("ClearValue() error = %v", err)
	}

	if err := buffer.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	profile := buffer.LastFlushProfile()
	if profile.PartitionStringBatchCount != 1 || profile.PartitionStringEntryCount != 1 {
		t.Fatalf("partition profile = %+v, want one KV sidecar batch and entry", profile)
	}
	if profile.BitmapSetEntryCount != 1 || profile.BSIValueEntryCount != 1 || profile.BSIClearValueEntryCount != 1 {
		t.Fatalf("mutation profile = %+v, want one bitmap set, BSI value, and BSI clear", profile)
	}
	if profile.TotalElapsed <= 0 || profile.FinishedAt.Before(profile.StartedAt) {
		t.Fatalf("timing profile = %+v, want elapsed flush timing", profile)
	}
	if profile.PartitionStringElapsed <= 0 || profile.BitmapSetElapsed <= 0 ||
		profile.BSIValueElapsed <= 0 || profile.BSIClearValueElapsed <= 0 {
		t.Fatalf("phase timings = %+v, want non-zero elapsed phases", profile)
	}
	if profile.Error != "" {
		t.Fatalf("profile error = %q, want empty", profile.Error)
	}
	if !buffer.IsEmpty() {
		t.Fatalf("buffer should be empty after flush")
	}
	if kvService.batchPutCalls != 1 || len(kvService.batchPutItems) != 1 {
		t.Fatalf("KV batch put calls/items = %d/%d, want 1/1", kvService.batchPutCalls, len(kvService.batchPutItems))
	}
	if bitmapService.batchMutateCalls != 3 {
		t.Fatalf("bitmap batch mutate calls = %d, want 3", bitmapService.batchMutateCalls)
	}
}

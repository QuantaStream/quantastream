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
	if profile.PartitionStringPutCalls != 1 || profile.PartitionStringBatchCount != 1 || profile.PartitionStringEntryCount != 1 {
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

func TestBatchBufferPartitionedStringsFlushAtConfiguredBatchSize(t *testing.T) {
	kvService := &recordingLocalKVStoreService{}
	conn := &Conn{
		LocalNodeServices: LocalNodeServices{
			KVStore: kvService,
		},
	}
	buffer := NewBatchBuffer(NewBitmapIndex(conn), NewKVStore(conn), 10)

	for i := 0; i < 9; i++ {
		if err := buffer.SetPartitionedString("sample/comment", uint64(i+1), "value"); err != nil {
			t.Fatalf("SetPartitionedString(%d) error = %v", i, err)
		}
	}
	if kvService.batchPutCalls != 0 {
		t.Fatalf("KV batch put calls = %d, want no early flush before configured batch size", kvService.batchPutCalls)
	}
	if err := buffer.SetPartitionedString("sample/comment", uint64(10), "value"); err != nil {
		t.Fatalf("SetPartitionedString threshold error = %v", err)
	}
	if kvService.batchPutCalls != 1 || len(kvService.batchPutItems) != 10 {
		t.Fatalf("KV batch put calls/items = %d/%d, want one 10-entry batch", kvService.batchPutCalls, len(kvService.batchPutItems))
	}
}

func TestBatchBufferPartitionedStringFlushCollapsesPaths(t *testing.T) {
	kvService := &recordingLocalKVStoreService{}
	conn := &Conn{
		LocalNodeServices: LocalNodeServices{
			KVStore: kvService,
		},
	}
	buffer := NewBatchBuffer(NewBitmapIndex(conn), NewKVStore(conn), 100)

	paths := []string{
		"lineitem/l_comment/1994-10-16T00,lineitem/l_comment/lex_remainders/1994-10-16T00",
		"lineitem/l_comment/1994-10-17T00,lineitem/l_comment/lex_remainders/1994-10-17T00",
		"lineitem/l_comment/1994-10-18T00,lineitem/l_comment/lex_remainders/1994-10-18T00",
	}
	for i, path := range paths {
		if err := buffer.SetPartitionedString(path, uint64(i+1), "value"); err != nil {
			t.Fatalf("SetPartitionedString(%d) error = %v", i, err)
		}
	}
	if err := buffer.Flush(); err != nil {
		t.Fatalf("Flush returned error: %v", err)
	}
	if kvService.batchPutCalls != 1 || len(kvService.batchPutItems) != 3 {
		t.Fatalf("KV batch put calls/items = %d/%d, want one collapsed 3-entry batch", kvService.batchPutCalls, len(kvService.batchPutItems))
	}
	profile := buffer.LastFlushProfile()
	if profile.PartitionStringPutCalls != 1 || profile.PartitionStringBatchCount != 3 || profile.PartitionStringEntryCount != 3 {
		t.Fatalf("partition profile = %+v, want one put call for three path batches", profile)
	}
}

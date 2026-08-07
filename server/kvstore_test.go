package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/akrylysov/pogreb"
)

func TestKVStoreShutdownClosesCachedPogrebStores(t *testing.T) {
	root := t.TempDir()
	indexA := filepath.Join(root, "index-a")
	indexB := filepath.Join(root, "index-b")

	dbA := openKVStoreShutdownTestDB(t, indexA)
	dbB := openKVStoreShutdownTestDB(t, indexB)
	store := &KVStore{
		Node: &Node{hashKey: "test-node"},
		storeCache: map[string]*cacheEntry{
			"index-a": {db: dbA, accessTime: time.Now()},
			"index-b": {db: dbB, accessTime: time.Now()},
		},
		exit: make(chan bool),
	}

	store.Shutdown()
	if got := len(store.storeCache); got != 0 {
		t.Fatalf("store cache length = %d, want 0 after shutdown", got)
	}
	select {
	case <-store.exit:
	default:
		t.Fatalf("exit channel was not closed")
	}

	reopenAndCloseKVStoreShutdownTestDB(t, indexA)
	reopenAndCloseKVStoreShutdownTestDB(t, indexB)

	done := make(chan struct{})
	go func() {
		store.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("second shutdown did not return")
	}
}

func TestKVStoreBatchPutWritesMultipleIndexPaths(t *testing.T) {
	store := &KVStore{
		Node:       &Node{hashKey: "test-node", dataDir: t.TempDir()},
		storeCache: map[string]*cacheEntry{},
		exit:       make(chan bool),
	}
	t.Cleanup(store.Shutdown)

	stream := &localIndexKVBatchStream{
		ctx: context.Background(),
		items: []*pb.IndexKVPair{
			{IndexPath: "comment-1996", Key: shared.ToBytes(uint64(1)), Value: [][]byte{shared.ToBytes("first")}},
			{IndexPath: "comment-1997", Key: shared.ToBytes(uint64(2)), Value: [][]byte{shared.ToBytes("second")}},
			{IndexPath: "comment-1996", Key: shared.ToBytes(uint64(3)), Value: [][]byte{shared.ToBytes("third")}},
		},
	}

	if err := store.BatchPut(stream); err != nil {
		t.Fatalf("BatchPut() error = %v", err)
	}
	if stream.closed == nil {
		t.Fatalf("BatchPut did not close stream")
	}
	assertKVStoreValue(t, store, "comment-1996", uint64(1), "first")
	assertKVStoreValue(t, store, "comment-1996", uint64(3), "third")
	assertKVStoreValue(t, store, "comment-1997", uint64(2), "second")
	if got := len(store.storeCache); got != 2 {
		t.Fatalf("store cache length = %d, want 2 reused stores", got)
	}
}

func openKVStoreShutdownTestDB(t *testing.T, path string) *pogreb.DB {
	t.Helper()
	db, err := pogreb.Open(path, nil)
	if err != nil {
		t.Fatalf("pogreb.Open(%s) error = %v", path, err)
	}
	if err := db.Put([]byte("key"), []byte("value")); err != nil {
		db.Close()
		t.Fatalf("pogreb.Put(%s) error = %v", path, err)
	}
	return db
}

func assertKVStoreValue(t *testing.T, store *KVStore, index string, key uint64, want string) {
	t.Helper()
	result, err := store.Lookup(context.Background(), &pb.IndexKVPair{
		IndexPath: index,
		Key:       shared.ToBytes(key),
	})
	if err != nil {
		t.Fatalf("Lookup(%s, %d) error = %v", index, key, err)
	}
	if got := string(result.Value[0]); got != want {
		t.Fatalf("Lookup(%s, %d) = %q, want %q", index, key, got, want)
	}
}

func reopenAndCloseKVStoreShutdownTestDB(t *testing.T, path string) {
	t.Helper()
	db, err := pogreb.Open(path, nil)
	if err != nil {
		t.Fatalf("reopen pogreb DB %s after shutdown: %v", path, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close reopened pogreb DB %s: %v", path, err)
	}
}

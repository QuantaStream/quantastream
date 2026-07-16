package server

import (
	"path/filepath"
	"testing"
	"time"

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

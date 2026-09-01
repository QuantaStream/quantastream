package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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

func TestKVStoreBatchPutItemsWritesMultipleIndexPaths(t *testing.T) {
	store := &KVStore{
		Node:       &Node{hashKey: "test-node", dataDir: t.TempDir()},
		storeCache: map[string]*cacheEntry{},
		exit:       make(chan bool),
	}
	t.Cleanup(store.Shutdown)

	_, err := store.BatchPutItems(context.Background(), &pb.IndexKVBatch{
		Items: []*pb.IndexKVPair{
			{IndexPath: "comment-1996", Key: shared.ToBytes(uint64(1)), Value: [][]byte{shared.ToBytes("first")}},
			{IndexPath: "comment-1997", Key: shared.ToBytes(uint64(2)), Value: [][]byte{shared.ToBytes("second")}},
			{IndexPath: "comment-1996", Key: shared.ToBytes(uint64(3)), Value: [][]byte{shared.ToBytes("third")}},
		},
	})
	if err != nil {
		t.Fatalf("BatchPutItems() error = %v", err)
	}
	assertKVStoreValue(t, store, "comment-1996", uint64(1), "first")
	assertKVStoreValue(t, store, "comment-1996", uint64(3), "third")
	assertKVStoreValue(t, store, "comment-1997", uint64(2), "second")
	if got := len(store.storeCache); got != 2 {
		t.Fatalf("store cache length = %d, want 2 reused stores", got)
	}
}

func TestKVStorePutStringEnumCachesDictionary(t *testing.T) {
	store := &KVStore{
		Node:       &Node{hashKey: "test-node", dataDir: t.TempDir()},
		storeCache: map[string]*cacheEntry{},
		exit:       make(chan bool),
	}
	t.Cleanup(store.Shutdown)

	ctx := context.Background()
	index := "sample/status.StringEnum"
	open, err := store.PutStringEnum(ctx, &pb.StringEnum{IndexPath: index, Value: "OPEN"})
	if err != nil {
		t.Fatalf("PutStringEnum(OPEN) error = %v", err)
	}
	if open.Value != 1 {
		t.Fatalf("PutStringEnum(OPEN) = %d, want 1", open.Value)
	}
	closed, err := store.PutStringEnum(ctx, &pb.StringEnum{IndexPath: index, Value: "CLOSED"})
	if err != nil {
		t.Fatalf("PutStringEnum(CLOSED) error = %v", err)
	}
	if closed.Value != 2 {
		t.Fatalf("PutStringEnum(CLOSED) = %d, want 2", closed.Value)
	}
	openAgain, err := store.PutStringEnum(ctx, &pb.StringEnum{IndexPath: index, Value: "OPEN"})
	if err != nil {
		t.Fatalf("PutStringEnum(OPEN again) error = %v", err)
	}
	if openAgain.Value != open.Value {
		t.Fatalf("PutStringEnum(OPEN again) = %d, want %d", openAgain.Value, open.Value)
	}

	store.enumCacheLock.Lock()
	cache := store.enumCache[index]
	store.enumCacheLock.Unlock()
	if cache == nil || len(cache.values) != 2 || cache.greatestRowID != 2 {
		t.Fatalf("enum cache = %+v, want two cached values with max row ID 2", cache)
	}

	if _, err := store.Put(ctx, &pb.IndexKVPair{
		IndexPath: index,
		Key:       shared.ToBytes("PENDING"),
		Value:     [][]byte{shared.ToBytes(uint64(99))},
	}); err != nil {
		t.Fatalf("Put(PENDING) error = %v", err)
	}
	store.enumCacheLock.Lock()
	_, cached := store.enumCache[index]
	store.enumCacheLock.Unlock()
	if cached {
		t.Fatalf("generic Put did not invalidate enum cache for %s", index)
	}

	pending, err := store.PutStringEnum(ctx, &pb.StringEnum{IndexPath: index, Value: "PENDING"})
	if err != nil {
		t.Fatalf("PutStringEnum(PENDING) error = %v", err)
	}
	if pending.Value != 99 {
		t.Fatalf("PutStringEnum(PENDING) = %d, want persisted ID 99", pending.Value)
	}
	done, err := store.PutStringEnum(ctx, &pb.StringEnum{IndexPath: index, Value: "DONE"})
	if err != nil {
		t.Fatalf("PutStringEnum(DONE) error = %v", err)
	}
	if done.Value != 100 {
		t.Fatalf("PutStringEnum(DONE) = %d, want next persisted ID 100", done.Value)
	}
}

func TestKVStoreRejectsCleanPogrebChecksumMismatch(t *testing.T) {
	root := t.TempDir()
	index := "corrupt-index"
	dbPath := filepath.Join(root, "index", index)
	db := openKVStoreShutdownTestDB(t, dbPath)
	if err := db.Sync(); err != nil {
		t.Fatalf("pogreb Sync(%s) error = %v", dbPath, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("pogreb Close(%s) error = %v", dbPath, err)
	}
	corruptPogrebSegmentByte(t, dbPath)

	store := &KVStore{
		Node:       &Node{hashKey: "test-node", dataDir: root},
		storeCache: map[string]*cacheEntry{},
		exit:       make(chan bool),
	}
	t.Cleanup(store.Shutdown)

	_, err := store.Lookup(context.Background(), &pb.IndexKVPair{
		IndexPath: index,
		Key:       []byte("key"),
	})
	if err == nil || !strings.Contains(err.Error(), "integrity check") {
		t.Fatalf("Lookup error = %v, want integrity check failure", err)
	}
	if _, cached := store.storeCache[index]; cached {
		t.Fatalf("corrupt store was cached after failed verification")
	}
}

func TestKVStoreInitRejectsCleanPogrebChecksumMismatch(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "index", "sample-table", "status.StringEnum")
	db := openKVStoreShutdownTestDB(t, dbPath)
	if err := db.Sync(); err != nil {
		t.Fatalf("pogreb Sync(%s) error = %v", dbPath, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("pogreb Close(%s) error = %v", dbPath, err)
	}
	corruptPogrebSegmentByte(t, dbPath)

	store := &KVStore{
		Node:       &Node{hashKey: "test-node", dataDir: root},
		storeCache: map[string]*cacheEntry{},
		exit:       make(chan bool),
	}

	err := store.Init()
	if err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("Init error = %v, want checksum mismatch", err)
	}
}

func TestOpenVerifiedPogrebStoreAllowsPogrebTornTailRecovery(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recovery-index")
	db := openKVStoreShutdownTestDB(t, dbPath)
	if err := db.Sync(); err != nil {
		t.Fatalf("pogreb Sync(%s) error = %v", dbPath, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("pogreb Close(%s) error = %v", dbPath, err)
	}
	appendPogrebSegmentTail(t, dbPath, []byte{1, 0, 1})
	if err := os.WriteFile(filepath.Join(dbPath, "lock"), []byte("stale"), 0644); err != nil {
		t.Fatalf("write stale pogreb lock: %v", err)
	}

	db, err := openVerifiedPogrebStore(dbPath)
	if err != nil {
		t.Fatalf("openVerifiedPogrebStore(%s) error = %v", dbPath, err)
	}
	defer db.Close()
	value, err := db.Get([]byte("key"))
	if err != nil {
		t.Fatalf("pogreb Get after recovery error = %v", err)
	}
	if got := string(value); got != "value" {
		t.Fatalf("pogreb Get after recovery = %q, want value", got)
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

func corruptPogrebSegmentByte(t *testing.T, dbPath string) {
	t.Helper()
	segmentPath := filepath.Join(dbPath, "00000.psg")
	f, err := os.OpenFile(segmentPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open pogreb segment for corruption: %v", err)
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		t.Fatalf("stat pogreb segment for corruption: %v", err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], stat.Size()-1); err != nil {
		t.Fatalf("read pogreb checksum byte: %v", err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], stat.Size()-1); err != nil {
		t.Fatalf("write pogreb checksum byte: %v", err)
	}
}

func appendPogrebSegmentTail(t *testing.T, dbPath string, tail []byte) {
	t.Helper()
	segmentPath := filepath.Join(dbPath, "00000.psg")
	f, err := os.OpenFile(segmentPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("open pogreb segment for torn tail append: %v", err)
	}
	defer f.Close()
	if _, err := f.Write(tail); err != nil {
		t.Fatalf("append pogreb torn tail: %v", err)
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

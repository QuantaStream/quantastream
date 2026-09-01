package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/akrylysov/pogreb"
	u "github.com/araddon/gou"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
)

var (
	// Ensure KVStore implements NodeService
	_ NodeService = (*KVStore)(nil)
)

const (
	maxOpenHours = 1.0
)

// KVStore - Server side state for KVStore service.
type KVStore struct {
	*Node
	storeCache     map[string]*cacheEntry
	storeCacheLock sync.RWMutex
	enumCache      map[string]*enumCacheEntry
	enumCacheLock  sync.Mutex
	exit           chan bool
	shutdownOnce   sync.Once
	cleanupLatency int64 // current cleanup thread duration (Prometheus)
}

type cacheEntry struct {
	db         *pogreb.DB
	accessTime time.Time
}

type enumCacheEntry struct {
	values        map[string]uint64
	greatestRowID uint64
}

// NewKVStore - Construct server side state.
func NewKVStore(node *Node) *KVStore {

	e := &KVStore{Node: node}
	e.exit = make(chan bool, 1)
	e.storeCache = make(map[string]*cacheEntry)
	e.enumCache = make(map[string]*enumCacheEntry)
	pb.RegisterKVStoreServer(node.server, e)
	return e
}

// Init - Initialize.
func (m *KVStore) Init() error {

	integrityStart := time.Now()
	integrity, err := m.validatePogrebStoresAtStartup()
	if err != nil {
		return fmt.Errorf("cannot initialize kv store service: %v", err)
	}
	integrityElapsed := time.Since(integrityStart)

	if m.Node.consul == nil {
		fmt.Println(m.hashKey, "KVStore Init",
			"kv_integrity_stores", integrity.Stores,
			"kv_integrity_segments", integrity.SegmentFiles,
			"kv_integrity_records", integrity.Records,
			"kv_integrity_skipped_locks", integrity.SkippedLocks,
			"kv_integrity_elapsed", integrityElapsed)
		return nil
	}

	start := time.Now()

	tables, err := shared.GetTables(m.Node.consul)
	if err != nil {
		return err
	}

	/*
		lastDay := time.Now().AddDate(0, 0, -1)
	*/

	for _, table := range tables {
		tPath := m.Node.dataDir + sep + "index" + sep + table
		if err := os.MkdirAll(tPath, 0755); err != nil {
			return fmt.Errorf("cannot initialize kv store service: %v", err)
		}
	}

	elapsed1 := time.Since(start)

	go m.cleanupProcessLoop()

	elapsed2 := time.Since(start)

	fmt.Println(m.hashKey, "KVStore Init elapsed1", elapsed1, "elapsed2", elapsed2,
		"kv_integrity_stores", integrity.Stores,
		"kv_integrity_segments", integrity.SegmentFiles,
		"kv_integrity_records", integrity.Records,
		"kv_integrity_skipped_locks", integrity.SkippedLocks,
		"kv_integrity_elapsed", integrityElapsed)

	return nil
}

func (m *KVStore) validatePogrebStoresAtStartup() (pogrebIntegritySummary, error) {
	if m == nil || m.Node == nil || strings.TrimSpace(m.Node.dataDir) == "" {
		return pogrebIntegritySummary{}, nil
	}
	return validatePogrebStoreTree(filepath.Join(m.Node.dataDir, "index"))
}

// background thread to check and close cached DB entries that haven't been accessed in over 24 hours.
func (m *KVStore) cleanupProcessLoop() {

	for {
		select {
		case <-m.exit:
			return
		default:
		}
		select {
		case <-m.exit:
			return
		case <-time.After(time.Second * 10):
			clusterState, _, _ := m.GetClusterState()
			if m.State == Active && clusterState == shared.Green {
				m.cleanup()
			}
		}
	}
}

// Scan open cache entries and close out indices
func (m *KVStore) cleanup() {

	//u.Debug(m.hashKey, " KVStore cleanup")
	m.storeCacheLock.Lock()
	cacheCopy := maps.Clone(m.storeCache) // shallow copy
	m.storeCacheLock.Unlock()

	start := time.Now()

	for k, v := range cacheCopy {
		if time.Since(v.accessTime).Hours() >= maxOpenHours {
			u.Debugf("Closed %v due to inactivity.", k)
			m.closeStore(k)
		}
	}

	elapsed := time.Since(start)
	m.cleanupLatency = elapsed.Milliseconds()
}

// Shutdown service.
func (m *KVStore) Shutdown() {

	m.shutdownOnce.Do(func() {
		if m.exit != nil {
			close(m.exit)
		}
		hashKey := ""
		if m.Node != nil {
			hashKey = m.hashKey
		}
		u.Debug(hashKey, " KVStore Shutdown")

		m.storeCacheLock.Lock()
		defer m.storeCacheLock.Unlock()
		for k, v := range m.storeCache {
			u.Infof("%s Sync and close [%s]", hashKey, k)
			if v == nil || v.db == nil {
				delete(m.storeCache, k)
				continue
			}
			if err := v.db.Sync(); err != nil {
				u.Errorf("%s KVStore sync [%s] failed: %v", hashKey, k, err)
			}
			if err := v.db.Close(); err != nil {
				u.Errorf("%s KVStore close [%s] failed: %v", hashKey, k, err)
			}
			delete(m.storeCache, k)
		}
		m.enumCacheLock.Lock()
		m.enumCache = make(map[string]*enumCacheEntry)
		m.enumCacheLock.Unlock()
	})
}

// JoinCluster - Join the cluster
func (m *KVStore) JoinCluster() {
}

func (m *KVStore) getStore(index string) (db *pogreb.DB, err error) {

	m.storeCacheLock.Lock()
	defer m.storeCacheLock.Unlock()

	//m.storeCacheLock.RLock()
	var ok bool
	var ce *cacheEntry
	if ce, ok = m.storeCache[index]; ok {
		//m.storeCacheLock.RUnlock()
		db = ce.db
		ce.accessTime = time.Now()
		return
	}
	/* TODO: This is a potential performance optimization, but it's not clear if it's necessary.
	m.storeCacheLock.RUnlock()

	m.storeCacheLock.Lock()
	defer m.storeCacheLock.Unlock()
	*/
	path := m.Node.dataDir + sep + "index" + sep + index
	// fmt.Println(m.hashKey, "KVStore getStore", path)
	db, err = openVerifiedPogrebStore(path)
	if err == nil {
		m.storeCache[index] = &cacheEntry{db: db, accessTime: time.Now()}
	} else {
		err = fmt.Errorf("while opening [%s] - %v", index, err)
	}
	return
}

func (m *KVStore) closeStore(index string) {

	m.storeCacheLock.Lock()
	defer m.storeCacheLock.Unlock()
	var ok bool
	var ce *cacheEntry
	if ce, ok = m.storeCache[index]; ok {
		if ce != nil && ce.db != nil {
			if err := ce.db.Sync(); err != nil {
				u.Errorf("%s KVStore sync [%s] failed: %v", m.hashKey, index, err)
			}
			if err := ce.db.Close(); err != nil {
				u.Errorf("%s KVStore close [%s] failed: %v", m.hashKey, index, err)
			}
		}
		delete(m.storeCache, index)
		m.invalidateStringEnumCache(index)
	}
}

// Put - Insert a new key
func (m *KVStore) Put(ctx context.Context, kv *pb.IndexKVPair) (*empty.Empty, error) {

	if kv == nil {
		return &empty.Empty{}, fmt.Errorf("KV Pair must not be nil")
	}
	if kv.Key == nil || len(kv.Key) == 0 {
		return &empty.Empty{}, fmt.Errorf("Key must be specified")
	}
	if kv.IndexPath == "" {
		return &empty.Empty{}, fmt.Errorf("Index must be specified")
	}
	db, err := m.getStore(kv.IndexPath)
	if err != nil {
		return &empty.Empty{}, err
	}
	err = db.Put(kv.Key, kv.Value[0])
	if err != nil {
		return &empty.Empty{}, err
	}
	m.invalidateStringEnumCache(kv.IndexPath)
	return &empty.Empty{}, nil
}

// Lookup a key
func (m *KVStore) Lookup(ctx context.Context, kv *pb.IndexKVPair) (*pb.IndexKVPair, error) {
	if kv == nil {
		return &pb.IndexKVPair{}, fmt.Errorf("KV Pair must not be nil")
	}
	if kv.Key == nil || len(kv.Key) == 0 {
		return &pb.IndexKVPair{}, fmt.Errorf("Key must be specified")
	}
	if kv.IndexPath == "" {
		return &pb.IndexKVPair{}, fmt.Errorf("Index must be specified")
	}
	db, err := m.getStore(kv.IndexPath)
	if err != nil {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, 0)
		kv.Value = [][]byte{b}
		return kv, fmt.Errorf("Error opening %s - %v", kv.IndexPath, err)
	}
	if db == nil {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, 0)
		kv.Value = [][]byte{b}
		return kv, fmt.Errorf("DB is nil %s", kv.IndexPath)
	}
	val, err := db.Get(kv.Key)
	if err != nil {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, 0)
		kv.Value = [][]byte{b}
		return kv, err
	}
	kv.Value = [][]byte{val}
	return kv, nil
}

// BatchPut - Insert a batch of entries.
func (m *KVStore) BatchPut(stream pb.KVStore_BatchPutServer) error {
	state := newKVBatchPutState()
	defer state.finish(m, "stream")

	for {
		kv, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&empty.Empty{})
		}
		if err != nil {
			return err
		}
		if err := m.batchPutKV(state, kv); err != nil {
			return err
		}
	}
}

// BatchPutItems inserts a bulk batch of entries without per-item gRPC stream
// messages.
func (m *KVStore) BatchPutItems(ctx context.Context, batch *pb.IndexKVBatch) (*empty.Empty, error) {
	if batch == nil {
		return &empty.Empty{}, fmt.Errorf("KV batch must not be nil")
	}
	state := newKVBatchPutState()
	defer state.finish(m, "bulk")
	for _, kv := range batch.Items {
		if err := m.batchPutKV(state, kv); err != nil {
			return &empty.Empty{}, err
		}
	}
	return &empty.Empty{}, nil
}

type kvBatchPutState struct {
	start           time.Time
	updatedMap      map[string]*pogreb.DB
	getStoreElapsed time.Duration
	putElapsed      time.Duration
	syncElapsed     time.Duration
	putCount        int
}

func newKVBatchPutState() *kvBatchPutState {
	return &kvBatchPutState{
		start:      time.Now(),
		updatedMap: make(map[string]*pogreb.DB, 0),
	}
}

func (s *kvBatchPutState) finish(m *KVStore, mode string) {
	syncStart := time.Now()
	for index, v := range s.updatedMap {
		if err := v.Sync(); err != nil {
			u.Errorf("%s KVStore batch sync [%s] failed: %v", m.hashKey, index, err)
		}
	}
	s.syncElapsed = time.Since(syncStart)
	elapsed := time.Since(s.start)
	if s.putCount > 1000 || elapsed > 500*time.Millisecond {
		fmt.Printf("KVStore batch put node=%s mode=%s items=%d stores=%d get_store_elapsed=%s put_elapsed=%s sync_elapsed=%s total_elapsed=%s\n",
			m.hashKey, mode, s.putCount, len(s.updatedMap), s.getStoreElapsed, s.putElapsed, s.syncElapsed, elapsed)
	}
}

func (m *KVStore) batchPutKV(state *kvBatchPutState, kv *pb.IndexKVPair) error {
	if kv == nil {
		return fmt.Errorf("KV Pair must not be nil")
	}
	if kv.IndexPath == "" {
		return fmt.Errorf("Index must be specified")
	}
	db, found := state.updatedMap[kv.IndexPath]
	if !found {
		getStoreStart := time.Now()
		var err error
		db, err = m.getStore(kv.IndexPath)
		state.getStoreElapsed += time.Since(getStoreStart)
		if err != nil {
			return err
		}
		state.updatedMap[kv.IndexPath] = db
	}
	if kv.Key == nil || len(kv.Key) == 0 {
		return fmt.Errorf("Key must be specified")
	}
	if kv.Value == nil || len(kv.Value) == 0 {
		return fmt.Errorf("Value must be specified")
	}
	if db == nil {
		return fmt.Errorf("DB is nil for [%s]", kv.IndexPath)
	}
	putStart := time.Now()
	if err := db.Put(kv.Key, kv.Value[0]); err != nil {
		return err
	}
	m.invalidateStringEnumCache(kv.IndexPath)
	state.putElapsed += time.Since(putStart)
	state.putCount++
	return nil
}

// BatchLookup - Lookup a batch of keys and return values.
func (m *KVStore) BatchLookup(stream pb.KVStore_BatchLookupServer) error {

	for {
		kv, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if kv == nil {
			return fmt.Errorf("KV Pair must not be nil")
		}
		if kv.IndexPath == "" {
			return fmt.Errorf("Index must be specified")
		}
		db, err := m.getStore(kv.IndexPath)
		if err != nil {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, 0)
			kv.Value[0] = b
			return err
		}
		val, err := db.Get(kv.Key)
		if err != nil {
			b := make([]byte, 8)
			binary.LittleEndian.PutUint64(b, 0)
			kv.Value[0] = b
			return err
		}
		kv.Value[0] = val
		if err := stream.Send(kv); err != nil {
			return err
		}
	}
}

// Items - Iterate over all items.
// called by grpc stream
func (m *KVStore) Items(index *wrappers.StringValue, stream pb.KVStore_ItemsServer) error {

	if index.Value == "" {
		return fmt.Errorf("Index must be specified")
	}
	db, err := m.getStore(index.Value)
	if err != nil {
		return err
	}

	it := db.Items()
	for {
		key, val, err := it.Next()
		if err != nil {
			if err != pogreb.ErrIterationDone {
				return err
			}
			break
		}
		if err := stream.Send(&pb.IndexKVPair{IndexPath: index.Value, Key: key,
			Value: [][]byte{val}}); err != nil {
			return err
		}
	}
	return nil
}

func (m *KVStore) getStringEnumCacheLocked(indexPath string, db *pogreb.DB) (*enumCacheEntry, error) {
	if m.enumCache == nil {
		m.enumCache = make(map[string]*enumCacheEntry)
	}
	if cache, ok := m.enumCache[indexPath]; ok {
		return cache, nil
	}

	cache := &enumCacheEntry{values: make(map[string]uint64)}
	it := db.Items()
	for {
		key, v, err := it.Next()
		if err != nil {
			if err != pogreb.ErrIterationDone {
				return nil, err
			}
			break
		}
		if len(v) < 8 {
			return nil, fmt.Errorf("StringEnum value for %s/%s is not a uint64", indexPath, string(key))
		}
		rowID := binary.LittleEndian.Uint64(v)
		if rowID > cache.greatestRowID {
			cache.greatestRowID = rowID
		}
		cache.values[string(key)] = rowID
	}
	m.enumCache[indexPath] = cache
	return cache, nil
}

func (m *KVStore) invalidateStringEnumCache(indexPath string) {
	if !strings.HasSuffix(indexPath, ".StringEnum") {
		return
	}
	m.enumCacheLock.Lock()
	defer m.enumCacheLock.Unlock()
	if m.enumCache != nil {
		delete(m.enumCache, indexPath)
	}
}

func (m *KVStore) invalidateStringEnumCachesWithPrefix(prefix string) {
	m.enumCacheLock.Lock()
	defer m.enumCacheLock.Unlock()
	for indexPath := range m.enumCache {
		if strings.HasPrefix(indexPath, prefix) && strings.HasSuffix(indexPath, ".StringEnum") {
			delete(m.enumCache, indexPath)
		}
	}
}

// PutStringEnum - Insert a new enumeration value and return the new enumeration key (integer sequence).
func (m *KVStore) PutStringEnum(ctx context.Context, se *pb.StringEnum) (*wrappers.UInt64Value, error) {

	if se == nil {
		return &wrappers.UInt64Value{}, fmt.Errorf("StringEnum  must not be nil")
	}
	if se.Value == "" || len(se.Value) == 0 {
		return &wrappers.UInt64Value{}, fmt.Errorf("Value must be specified")
	}
	if se.IndexPath == "" {
		return &wrappers.UInt64Value{}, fmt.Errorf("Index must be specified")
	}
	db, err := m.getStore(se.IndexPath)
	if err != nil {
		return &wrappers.UInt64Value{}, err
	}

	m.enumCacheLock.Lock()
	defer m.enumCacheLock.Unlock()

	cache, err := m.getStringEnumCacheLocked(se.IndexPath, db)
	if err != nil {
		return &wrappers.UInt64Value{}, err
	}
	if rowID, found := cache.values[se.Value]; found {
		return &wrappers.UInt64Value{Value: rowID}, nil
	}
	cache.greatestRowID++
	rowID := cache.greatestRowID
	if err := db.Put(shared.ToBytes(se.Value), shared.ToBytes(rowID)); err != nil {
		cache.greatestRowID--
		return &wrappers.UInt64Value{}, err
	}
	cache.values[se.Value] = rowID
	if err := db.Sync(); err != nil {
		u.Errorf("%s KVStore StringEnum sync [%s] failed: %v", m.hashKey, se.IndexPath, err)
	}
	return &wrappers.UInt64Value{Value: rowID}, nil
}

// DeleteIndicesWithPrefix - Close and delete all indices with a specific prefix
func (m *KVStore) DeleteIndicesWithPrefix(ctx context.Context,
	req *pb.DeleteIndicesWithPrefixRequest) (*empty.Empty, error) {

	if req.Prefix == "" {
		return &empty.Empty{}, fmt.Errorf("Index prefix must be specified")
	}

	u.Infof("Deleting index files for prefix %v, retain enums = %v", req.Prefix, req.RetainEnums)
	if !req.RetainEnums {
		m.invalidateStringEnumCachesWithPrefix(req.Prefix + sep)
	}

	// Interate over storeCache and close everything currently open for this prefix
	m.storeCacheLock.Lock()
	for k, v := range m.storeCache {
		if strings.HasPrefix(k, req.Prefix+sep) {
			v.db.Sync()
			v.db.Close()
			delete(m.storeCache, k)
			u.Infof("Sync and close [%s]", k)
		}
	}
	m.storeCacheLock.Unlock()

	// retrieve list of indices matching the prefix from the filesystem
	baseDir := m.Node.dataDir + sep + "index" + sep + req.Prefix
	files, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			u.Infof("Index directory [%s] already absent.", baseDir)
			return &empty.Empty{}, nil
		}
		return &empty.Empty{}, fmt.Errorf("DeleteIndicesWithPrefix: %v", err)
	}
	for _, file := range files {
		if !file.IsDir() {
			continue
		}
		k := baseDir + sep + file.Name()
		if !req.RetainEnums {
			if err := os.RemoveAll(baseDir); err != nil {
				return &empty.Empty{}, fmt.Errorf("DeleteIndicesWithPrefix retain is false: error [%v]", err)
			} else {
				u.Infof("Deleted [%s]", k)
			}
		} else {
			if !strings.HasSuffix(k, "StringEnum") {
				if err := os.RemoveAll(k); err != nil {
					return &empty.Empty{}, fmt.Errorf("DeleteIndicesWithPrefix retain is true:error [%v]", err)
				} else {
					u.Infof("Deleted [%s]", k)
				}
			}
		}
	}
	return &empty.Empty{}, nil
}

// IndexInfo - Get information about an index.
func (m *KVStore) IndexInfo(ctx context.Context, req *pb.IndexInfoRequest) (*pb.IndexInfoResponse, error) {

	res := &pb.IndexInfoResponse{}
	if req == nil {
		return res, fmt.Errorf("request must not be nil")
	}
	if req.IndexPath == "" {
		return res, fmt.Errorf("IndexPath must be specified")
	}

	filePath := m.Node.dataDir + sep + "index" + sep + req.IndexPath
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return res, nil
	}

	var err error
	var ce *cacheEntry
	var db *pogreb.DB
	m.storeCacheLock.RLock()
	ce, res.WasOpen = m.storeCache[req.IndexPath]
	m.storeCacheLock.RUnlock()
	if ce == nil {
		db, err = openVerifiedPogrebStore(filePath)
		if err != nil {
			return res, fmt.Errorf("IndexInfo:Open err - %v", err)
		}
		defer db.Close()
	} else {
		db = ce.db
	}
	res.FileSize, err = db.FileSize()
	if err != nil {
		return res, fmt.Errorf("IndexInfo:FileSize err - %v", err)
	}
	res.Count = db.Count()
	metrics := db.Metrics()
	res.Puts = metrics.Puts.Value()
	res.Gets = metrics.Gets.Value()
	res.Dels = metrics.Dels.Value()
	res.HashCollisions = metrics.HashCollisions.Value()
	return res, nil
}

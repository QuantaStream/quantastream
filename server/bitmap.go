package server

//
// This file contains the main processing flows for the bitmap server.
//

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"

	//"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/tunny"
	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	u "github.com/araddon/gou"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/hashicorp/consul/api"
	"golang.org/x/sync/errgroup"
)

const (
	timeFmt = "2006-01-02T15"
)

func formatShardTime(t time.Time) string {
	return t.UTC().Format(timeFmt)
}

func skipNodeSyncEnabled() bool {
	raw := os.Getenv("QUANTASTREAM_SKIP_NODE_SYNC")
	if raw == "" {
		raw = os.Getenv("QUANTA_DEV_SKIP_SYNC")
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func bitmapFlushTimeout() time.Duration {
	const defaultTimeout = 5 * time.Minute

	raw := strings.TrimSpace(os.Getenv("QUANTASTREAM_BITMAP_FLUSH_TIMEOUT"))
	if raw == "" {
		return defaultTimeout
	}
	timeout, err := time.ParseDuration(raw)
	if err != nil || timeout <= 0 {
		return defaultTimeout
	}
	return timeout
}

var (
	// Ensure BitmapIndex implements shared.Service
	_ NodeService = (*BitmapIndex)(nil)
)

// BitmapIndex - Main state structure for bitmap indices.
//
// bitmapCache - In memory storage for "standard" bitmaps.
// bsiCache - In memory storage for BSI values.
// fragQueue - All cache mutation operations pass through a fragment queue (including server startup reads)
// workers - Count of worker threads assigned to process mutations.
// tableCache - Schema metadata cache (essentially same YAML file used by loader).
type BitmapIndex struct {
	*Node
	bitmapCache           map[string]map[string]map[uint64]map[int64]*StandardBitmap
	bitmapCacheLock       sync.RWMutex
	bsiCache              map[string]map[string]map[int64]*BSIBitmap
	bsiCacheLock          sync.RWMutex
	seedCache             map[string]*SeedBitmap
	seedCacheLock         sync.RWMutex
	reverseArtifactCache  map[string]map[string]*relationshipReverseArtifact
	siblingDiversityCache map[string]map[string]map[string]*relationshipSiblingDiversityArtifact
	siblingDiversityGen   map[string]uint64
	reverseArtifactLock   sync.RWMutex
	fragQueue             chan *BitmapFragment
	workersCount          int
	fragFileLock          sync.Mutex
	tableCache            map[string]*shared.BasicTable
	tableCacheLock        sync.RWMutex
	partitionQueue        chan *PartitionOperation
	bitmapCount           int
	bsiCount              int
	workers               []*WorkerThread
	cleanupLock           sync.RWMutex
	backgroundWG          sync.WaitGroup
	updBitmapTime         atomic.Uint64
	updBSITime            atomic.Uint64
	saveBitmapECnt        atomic.Uint64
	saveBitmapTCnt        atomic.Uint64
	saveBitmapTime        atomic.Uint64
	saveBSIECnt           atomic.Uint64
	saveBSITCnt           atomic.Uint64
	saveBSITime           atomic.Uint64
	bsiMergeCount         atomic.Uint64
	bsiMergeDisjointCount atomic.Uint64
	bsiMergeOverlapCount  atomic.Uint64
	bsiMergeCheckNanos    atomic.Uint64
	bsiMergeClearNanos    atomic.Uint64
	bsiMergeOrNanos       atomic.Uint64
}

type WorkerThread struct {
	index int
	aux   chan *BitmapFragment
}

// SeedBitmap caches a set-producing existence seed for a table/field/time window.
type SeedBitmap struct {
	Index    string
	Field    string
	FromNano int64
	ToNano   int64
	Bits     *roaring64.Bitmap
	Count    uint64
}

func NewWorkerThread(index int) *WorkerThread {
	aux := make(chan *BitmapFragment, 100)
	return &WorkerThread{index: index, aux: aux}
}

// NewBitmapIndex - Construct and initialize bitmap server state.
func NewBitmapIndex(node *Node) *BitmapIndex {

	e := &BitmapIndex{Node: node}
	e.tableCache = make(map[string]*shared.BasicTable)
	configPath := e.dataDir + sep + "config"
	schemaPath := ""        // this is normally an empty string forcing schema to come from Consul
	if e.ServicePort == 0 { // In-memory test harness
		schemaPath = configPath // read schema from local config yaml
		tables, err := shared.ActiveOrDiscoveredSchemaTables(configPath, "")
		if err != nil {
			u.Errorf("ERROR: Could not discover active schema tables - %v", err)
			os.Exit(1)
		}
		for _, index := range tables {
			if table, err := shared.LoadSchema(schemaPath, index, nil); err != nil {
				u.Errorf("ERROR: Could not load schema for %s - %v", index, err)
				os.Exit(1)
			} else {
				e.tableCache[index] = table
				u.Infof("Index %s initialized.", index)
			}
		}
	} else { // Normal (from Consul) initialization
		fmt.Println("Bitmap server Normal (from Consul) initialization", e.hashKey)
		var tables []string
		err := shared.Retry(5, 2*time.Second, func() (err error) {
			tables, err = shared.GetTables(e.consul)
			return
		})
		if err != nil {
			u.Errorf("could not load table schema, GetTables error %v", err)
			os.Exit(1)
		}
		for _, table := range tables {
			if t, err := shared.LoadSchema(schemaPath, table, e.consul); err != nil {
				u.Errorf("could not load schema for %s - %v", table, err)
				os.Exit(1)
			} else {
				e.tableCache[table] = t
				u.Infof("Table initialized. %s", table)
			}
		}
	}

	pb.RegisterBitmapIndexServer(e.server, e)
	return e
}

func (m *BitmapIndex) GetBitmapCache() map[string]map[string]map[uint64]map[int64]*StandardBitmap {
	return m.bitmapCache
}

func (m *BitmapIndex) GetBsiCache() map[string]map[string]map[int64]*BSIBitmap {
	return m.bsiCache
}

// GetTable - Get the table schema for a given key
// used for testing and debugging.
func (m *BitmapIndex) GetTable(tableName string) *shared.BasicTable {
	t := m.tableCache[tableName]
	return t
}

// Init - Initialization
func (m *BitmapIndex) Init() error {

	fmt.Println("BitmapIndex Init", m.hashKey, uintptr(unsafe.Pointer(m)))

	// TODO: Sensible configuration for queue sizes.
	m.partitionQueue = make(chan *PartitionOperation, 10000000)
	//m.fragQueue = make(chan *BitmapFragment, 20000000)
	m.fragQueue = make(chan *BitmapFragment, 10000000)
	m.bitmapCache = make(map[string]map[string]map[uint64]map[int64]*StandardBitmap)
	m.bsiCache = make(map[string]map[string]map[int64]*BSIBitmap)
	m.seedCache = make(map[string]*SeedBitmap)
	m.reverseArtifactCache = make(map[string]map[string]*relationshipReverseArtifact)
	m.siblingDiversityCache = make(map[string]map[string]map[string]*relationshipSiblingDiversityArtifact)
	m.siblingDiversityGen = make(map[string]uint64)
	m.workersCount = 20

	m.workers = make([]*WorkerThread, m.workersCount)
	for i := 0; i < m.workersCount; i++ {
		m.workers[i] = NewWorkerThread(i)
	}
	for i := 0; i < m.workersCount; i++ {
		m.backgroundWG.Add(1)
		go func(worker *WorkerThread) {
			defer m.backgroundWG.Done()
			m.batchProcessLoop(worker)
		}(m.workers[i])
	}

	// Read files from disk
	err := m.readBitmapFiles(m.fragQueue)
	if err != nil {
		return fmt.Errorf("cannot initialize bitmap server error: %v", err)
	}
	m.backgroundWG.Add(1)
	go func() {
		defer m.backgroundWG.Done()
		m.memoryUsageProcessLoop(time.Minute)
	}()

	// Partition operation worker thread
	m.backgroundWG.Add(1)
	go func() {
		defer m.backgroundWG.Done()
		m.partitionProcessLoop()
	}()

	return nil
}

// WaitForShutdown waits until BitmapIndex background loops observe Stop and exit.
func (m *BitmapIndex) WaitForShutdown() {
	m.backgroundWG.Wait()
}

// Shutdown - Shut down and clean up.
func (m *BitmapIndex) Shutdown() {
	u.Warnf("Shutting down bitmap server.")
	if err := m.flush(); err != nil {
		u.Errorf("BitmapIndex shutdown flush failed: %v", err)
	}
	// flush waits for queued worker updates to finish; shutdown should then persist
	// dirty cache entries without rewriting every clean shard.
	_, bitmapWrites, _, bsiWrites, err := m.persistCaches(false)
	if err != nil {
		u.Errorf("BitmapIndex shutdown persist failed: %v", err)
	}
	if bitmapWrites+bsiWrites > 0 {
		if err := m.saveBitmapShardManifestFromCache("shutdown"); err != nil {
			u.Warnf("BitmapIndex shutdown manifest refresh failed: %v", err)
		}
	}
}

// JoinCluster - Join the cluster
func (m *BitmapIndex) JoinCluster() {
	if m.Conn.ServicePort == 0 {
		return // Skip this for test harness mode.
	}
	u.Infof("Bitmap server is joining the cluster %s.", m.hashKey)
	m.verifyNode()
}

// BatchMutate API call (used by client SetBit call for bulk loading data)
func (m *BitmapIndex) BatchMutate(stream pb.BitmapIndex_BatchMutateServer) error {

	done := make([]chan bool, 0)

	for {
		kv, err := stream.Recv()
		if err == io.EOF {
			for _, ch := range done {
				<-ch
			}
			return stream.SendAndClose(&empty.Empty{})
		}
		if err != nil {
			return err
		}
		if kv == nil {
			return fmt.Errorf("KV Pair must not be nil")
		}
		doneCh, err := m.enqueueBatchMutation(kv)
		if err != nil {
			return err
		}
		if doneCh != nil {
			done = append(done, doneCh)
		}
	}
}

// BatchMutateItems API call applies a pre-chunked batch of bitmap or BSI
// mutations. It is semantically equivalent to BatchMutate but avoids one gRPC
// stream send per item on high-volume load paths.
func (m *BitmapIndex) BatchMutateItems(_ context.Context, batch *pb.IndexKVBatch) (*empty.Empty, error) {
	if batch == nil || len(batch.Items) == 0 {
		return &empty.Empty{}, nil
	}
	done := make([]chan bool, 0, len(batch.Items))
	for _, kv := range batch.Items {
		if kv == nil {
			return nil, fmt.Errorf("KV Pair must not be nil")
		}
		doneCh, err := m.enqueueBatchMutation(kv)
		if err != nil {
			return nil, err
		}
		if doneCh != nil {
			done = append(done, doneCh)
		}
	}
	for _, ch := range done {
		<-ch
	}
	return &empty.Empty{}, nil
}

func (m *BitmapIndex) enqueueBatchMutation(kv *pb.IndexKVPair) (chan bool, error) {
	if kv.Key == nil || len(kv.Key) == 0 {
		return nil, fmt.Errorf("key must be specified")
	}

	s := strings.Split(kv.IndexPath, "/")
	if len(s) != 2 {
		err := fmt.Errorf("IndexPath %s not valid", kv.IndexPath)
		u.Errorf("%s", err)
		return nil, err
	}
	indexName := s[0]
	fieldName := s[1]

	_, err := m.getFieldConfig(indexName, fieldName)
	if err != nil {
		return nil, err
	}

	rowIDOrBits := int64(binary.LittleEndian.Uint64(kv.Key))
	isBSI := m.isBSI(indexName, fieldName)
	ts := time.Unix(0, kv.Time)

	frag := newBitmapFragment(indexName, fieldName, rowIDOrBits, ts, kv.Value,
		isBSI, kv.IsClear, kv.IsUpdate)

	if kv.Sync {
		if frag.IsBSI {
			m.updateBSICache(frag)
		} else {
			m.updateBitmapCache(frag)
		}
		return nil, nil
	}
	select {
	case m.fragQueue <- frag:
		// fmt.Println(m.hashKey, "svr BatchMutate sent to fragQueue", frag.FieldName, frag.RowIDOrBits, frag.Time.Format(timeFmt), uintptr(unsafe.Pointer(m)))
		return frag.Done, nil
	default:
		return nil, fmt.Errorf("BatchMutate: fragment queue is full")
	}
}

// StandardBitmap is just a wrapper around the roaring libraries for simple bitmap fields.
type StandardBitmap struct {
	Bits        *roaring64.Bitmap
	ModTime     time.Time
	PersistTime time.Time
	AccessTime  time.Time
	Lock        sync.RWMutex
	TQType      string
}

func (m *BitmapIndex) newStandardBitmap(index, field string) *StandardBitmap {

	attr, err := m.getFieldConfig(index, field)
	var timeQuantumType string
	if err == nil {
		timeQuantumType = attr.TimeQuantumType
	}
	ts := time.Now()
	return &StandardBitmap{Bits: roaring64.NewBitmap(), ModTime: ts, AccessTime: ts,
		TQType: timeQuantumType}
}

// BSIBitmap represents integer values
type BSIBitmap struct {
	*roaring64.BSI
	ModTime        time.Time
	PersistTime    time.Time
	AccessTime     time.Time
	Lock           sync.RWMutex
	TQType         string
	sequencerQueue *SequencerQueue
}

func (m *BitmapIndex) newBSIBitmap(index, field string) *BSIBitmap {

	attr, err := m.getFieldConfig(index, field)
	//	var minValue, maxValue int64
	var timeQuantumType string
	if err == nil {
		timeQuantumType = attr.TimeQuantumType
		//		minValue = int64(attr.MinValue)
		//		maxValue = int64(attr.MaxValue)
	}
	var seq *SequencerQueue
	if attr.Parent.PrimaryKey != "" || attr.Parent.TimeQuantumField != "" {
		pkInfo, _ := attr.Parent.GetPrimaryKeyInfo()
		if attr.FieldName == pkInfo[0].FieldName {
			// If compound key, sequencer installed on first key attr
			seq = NewSequencerQueue()
			//			if maxValue == 0 {
			//				maxValue = math.MaxInt64
			//			}
		}
	}
	ts := time.Now()
	//	return &BSIBitmap{BSI: roaring64.NewBSI(maxValue, minValue),
	//		TQType: timeQuantumType, ModTime: ts, AccessTime: ts, sequencerQueue: seq}
	return &BSIBitmap{BSI: roaring64.NewDefaultBSI(),
		TQType: timeQuantumType, ModTime: ts, AccessTime: ts, sequencerQueue: seq}
}

// BitmapFragment is just a work unit for cache mutation operations.
type BitmapFragment struct {
	IndexName   string
	FieldName   string
	RowIDOrBits int64     // Row ID or BSI bit count number (negative values = BSI bitcount)
	Time        time.Time // Time for time quantum
	BitData     [][]byte
	ModTime     time.Time // Modification time stamp
	IsBSI       bool
	IsClear     bool // Is this a clear operation?  Otherwise set bits.
	IsUpdate    bool
	IsInit      bool      // Is this fragment part of init disk read?
	IsNop       bool      // Is this a no-op?
	Done        chan bool // when the fragment is done processing
}

func newBitmapFragment(index, field string, rowIDOrBits int64, ts time.Time, f [][]byte,
	isBSI, isClear, isUpdate bool) *BitmapFragment {
	res := &BitmapFragment{IndexName: index, FieldName: field, RowIDOrBits: rowIDOrBits, Time: ts,
		BitData: f, ModTime: time.Now(), IsBSI: isBSI, IsClear: isClear, IsUpdate: isUpdate}

	res.Done = make(chan bool, 1)
	return res
}

func fragmentApplyTime(f *BitmapFragment, startedAt time.Time) time.Time {
	if f.IsInit {
		return f.ModTime
	}
	return startedAt
}

// Lookup field metadata (time quantum, exclusivity)
func (m *BitmapIndex) getFieldConfig(index, field string) (*shared.BasicAttribute, error) {

	m.tableCacheLock.RLock()
	defer m.tableCacheLock.RUnlock()
	table := m.tableCache[index]
	attr, err := table.GetAttribute(field)
	if err != nil {
		return nil, fmt.Errorf("getFieldConfig ERROR: Non existent attribute %s for index %s was referenced",
			field, index)
	}
	if attr.TimeQuantumType == "" && table.TimeQuantumType != "" {
		attr.TimeQuantumType = table.TimeQuantumType
	}
	return attr, nil
}

// Check metadata - Is the field a BSI?
func (m *BitmapIndex) isBSI(index, field string) bool {

	m.tableCacheLock.RLock()
	defer m.tableCacheLock.RUnlock()
	table := m.tableCache[index]
	attr, err := table.GetAttribute(field)
	if err != nil {
		u.Errorf("attribute %s for index %s does not exist", field, index)
	}
	return attr.IsBSI()
}

// Worker thread.
//
// Read entries from fragment queue that were uploaded by the client SetBit/SetValue
// operations. If there is a write signal then call the persistence code.  Write signals
// are triggered when fragment queue activity tails off.  This serves to prioritize memory updates
// over disk I/O (which occurs asynchronously).
//
// The weird repetition in the select statement below is a go hack for prioritizing work.  Select
// case processing order is non-deterministic.
//
// The aux channel is for when we need to send a nop to a particular thread. As oppposed to putting
// a frag in fragQueue where any thread can pick it up. It is much smaller.
func (m *BitmapIndex) batchProcessLoop(worker *WorkerThread) {

	// fmt.Println(m.Node.hashKey, "batchProcessLoop launched", worker.index, uintptr(unsafe.Pointer(m)))
	// defer fmt.Println(m.Node.hashKey, "batchProcessLoop DONE", worker.index) // should never ever ever ever happen

	for {
		// fmt.Println(m.Node.hashKey, "batchProcessLoop top worker loop", worker.index)
		// uintptr(unsafe.Pointer(m)
		// This is a way to make sure that the fraq queue has priority over persistence.
		select {
		case _, open := <-m.Stop:
			if !open {
				return
			}
		case nop := <-worker.aux:
			select {
			case nop.Done <- true:
			default:
			}
			continue
		case frag := <-m.fragQueue:
			if frag.IsNop {
				//nothing
			} else if frag.IsBSI {
				m.updateBSICache(frag)
			} else {
				m.updateBitmapCache(frag)
			}
			select {
			case frag.Done <- true:
			default:
			}
			continue

		default:
			// Don't block
		}
		// this is where it waits for a fragment or flush marker
		select {
		case _, open := <-m.Stop:
			if !open {
				return
			}
		case nop := <-worker.aux:
			select {
			case nop.Done <- true:
			default:
			}
			continue
		case frag := <-m.fragQueue:
			if frag.IsNop {
				// nothing
			} else if frag.IsBSI {
				m.updateBSICache(frag)
			} else {
				m.updateBitmapCache(frag)
			}
			select {
			case frag.Done <- true:
			default:
			}
			continue
		}
	} // back to top, forever
}

func (m *BitmapIndex) memoryUsageProcessLoop(interval time.Duration) {
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if m.State == Stopped {
				return
			}
			m.calculateMemoryUsage()
		case _, open := <-m.Stop:
			if !open {
				return
			}
		}
	}
}

// partitionProcessLoop Partition cleanup/archive/expiration worker thread.
// Wake up on interval and run partition processing.
// note that cleanupStrandedShards is also called elesewhere
func (m *BitmapIndex) partitionProcessLoop() {

	for {
		select {
		case _, open := <-m.Stop:
			if !open {
				return
			}
		default:
		}

		select {
		case _, open := <-m.Stop:
			if !open {
				return
			}
		case p := <-m.partitionQueue:
			m.executeOperation(p)
			m.purgePartition(p.Partition)
			runtime.GC()
		case <-time.After(time.Hour):
			state, _, _ := m.GetClusterState()
			if m.State == Active && state == shared.Green {
				m.cleanupStrandedShards()
			}
		}
	}
}

func (m *BitmapIndex) verifyNode() {

	if skipNodeSyncEnabled() {
		m.State = Active
		u.Warnf("QUANTASTREAM_SKIP_NODE_SYNC enabled; skipping node synchronization and marking %s Active", m.GetNodeID())
		consul := m.Consul
		valStr := fmt.Sprintf("%s_%d", m.hashKey, time.Now().UnixMilli())
		pair := &api.KVPair{Key: "AnyNodeStatusChangeTime", Value: []byte(valStr)}
		consul.KV().Put(pair, nil)
		return
	}

	peerClient := m.Conn.GetService("BitmapIndex").(*shared.BitmapIndex)

	m.State = Syncing
	u.Warnf("Setting node state to Syncing %s", m.GetNodeID())
	tryCount := 1
	for {
		u.Debugf("verifyNode peerClient.Synchronize start %v", m.hashKey)
		diffCount, err := peerClient.Synchronize(m.GetNodeID())
		u.Debugf("verifyNode peerClient.Synchronize done %v %v %v", m.hashKey, diffCount, err)

		if err != nil {
			u.Log(u.FATAL, fmt.Errorf("Node synchronization/verification failed - %v", err))
		}
		if diffCount <= 0 {
			m.State = Active
			u.Debugf("verifyNode Setting node state to Active for %s", m.hashKey)
			// we need to 'touch' the health so everyone knows we are active atw
			consul := m.Consul
			valStr := fmt.Sprintf("%s_%d", m.hashKey, time.Now().UnixMilli())
			pair := &api.KVPair{Key: "AnyNodeStatusChangeTime", Value: []byte(valStr)}
			consul.KV().Put(pair, nil)
			break
		}
		time.Sleep(shared.SyncRetryInterval)
		tryCount++
		u.Warnf("%s %d Differences detected, retrying Synchronization (attempt %d)", m.hashKey, diffCount, tryCount)
	}
	u.Debug("verifyNode done ", m.hashKey)
}

// Updates to the standard bitmap field cache
func (m *BitmapIndex) updateBitmapCache(f *BitmapFragment) {

	// If the rowID exists then merge in the new set of bits
	start := time.Now()
	applyTime := fragmentApplyTime(f, start)
	newBm := m.newStandardBitmap(f.IndexName, f.FieldName)
	newBm.ModTime = applyTime
	newBm.AccessTime = applyTime
	if f.IsInit {
		newBm.PersistTime = applyTime
	}
	if len(f.BitData) != 1 {
		u.Errorf("updateBitmapCache - Index out of range %d, Index = %s, Field = %s",
			len(f.BitData), f.IndexName, f.FieldName)
	}
	if err := newBm.Bits.UnmarshalBinary(f.BitData[0]); err != nil {
		u.Errorf("updateBitmapCache - UnmarshalBinary error - %v", err)
		return
	}
	rowID := uint64(f.RowIDOrBits)
	m.bitmapCacheLock.Lock()
	if f.IsUpdate {
		//Handle exclusive "updates"
		m.clearAllRows(f.IndexName, f.FieldName, f.Time.UnixNano(), newBm.Bits)
	}
	if f.IsClear && f.IsUpdate { // Special case: set a non-exlusive field to null
		m.bitmapCacheLock.Unlock()
		return
	}
	if _, ok := m.bitmapCache[f.IndexName][f.FieldName][rowID][f.Time.UnixNano()]; !ok && f.IsUpdate {
		// Silently ignore attempts to update data not in local cache that is not in hashKey
		// because updates are sent to all nodes
		hashKey := fmt.Sprintf("%s/%s/%d/%s", f.IndexName, f.FieldName, rowID, formatShardTime(f.Time))
		if !m.Member(hashKey) { // not here and not a member
			m.bitmapCacheLock.Unlock()
			return
		}
	}
	if _, ok := m.bitmapCache[f.IndexName]; !ok {
		m.bitmapCache[f.IndexName] = make(map[string]map[uint64]map[int64]*StandardBitmap)
	}
	if _, ok := m.bitmapCache[f.IndexName][f.FieldName]; !ok {
		m.bitmapCache[f.IndexName][f.FieldName] = make(map[uint64]map[int64]*StandardBitmap)
	}
	if _, ok := m.bitmapCache[f.IndexName][f.FieldName][rowID]; !ok {
		m.bitmapCache[f.IndexName][f.FieldName][rowID] = make(map[int64]*StandardBitmap)
	}
	if existBm, ok := m.bitmapCache[f.IndexName][f.FieldName][rowID][f.Time.UnixNano()]; !ok {
		m.bitmapCache[f.IndexName][f.FieldName][rowID][f.Time.UnixNano()] = newBm
		m.bitmapCacheLock.Unlock()
	} else {
		// Lock de-escalation
		existBm.Lock.Lock()
		m.bitmapCacheLock.Unlock()
		if f.IsClear {
			roaring64.ClearBits(newBm.Bits, existBm.Bits)
		} else {
			existBm.Bits = roaring64.ParOr(0, existBm.Bits, newBm.Bits)
		}
		existBm.ModTime = applyTime
		existBm.AccessTime = applyTime
		if f.IsInit {
			existBm.PersistTime = applyTime
		}
		existBm.Lock.Unlock()
	}
	elapsed := time.Since(start)
	m.updBitmapTime.Store(uint64(elapsed.Milliseconds()))
	if elapsed.Nanoseconds() > (1000000 * 25) {
		u.Debugf("updateBitmapCache [%s/%s/%d/%s] done in %v.\n", f.IndexName, f.FieldName,
			rowID, f.Time.Format(timeFmt), elapsed)
	}
}

// ClearParams struct encapsulates parameters to the clear bits worker pool.
type ClearParams struct {
	FoundSet *roaring64.Bitmap
	Target   *StandardBitmap
}

func (m *BitmapIndex) clearAllRows(index, field string, ts int64, nbm *roaring64.Bitmap) {

	var wg sync.WaitGroup

	if f, ok := m.bitmapCache[index][field]; ok {
		for _, tm := range f {
			if bm, ok2 := tm[ts]; ok2 {
				wg.Add(1)
				go func(b *StandardBitmap) {
					defer wg.Done()
					b.Lock.Lock()
					defer b.Lock.Unlock()
					roaring64.ClearBits(nbm, b.Bits)
					b.ModTime = time.Now()
					b.AccessTime = b.ModTime
				}(bm)
			} else {
				continue
			}
		}
	}
	wg.Wait()
}

func (m *BitmapIndex) clearAll(index string, start, end int64, nbm *roaring64.Bitmap) {

	m.bitmapCacheLock.Lock()
	m.bsiCacheLock.Lock()
	defer m.bitmapCacheLock.Unlock()
	defer m.bsiCacheLock.Unlock()

	numCPUs := runtime.NumCPU()

	pool := tunny.NewFunc(numCPUs, func(payload interface{}) interface{} {
		params := payload.(ClearParams)
		//u.Debugf("ClearBits %s.%s ROW: %d - %s", params.Index, params.Field, params.RowID, time.Unix(0, params.Timestamp).Format(timeFmt))
		roaring64.ClearBits(params.FoundSet, params.Target.Bits)
		params.Target.ModTime = time.Now()
		params.Target.AccessTime = params.Target.ModTime
		return nil
	})
	defer pool.Close()

	if fm, ok := m.bitmapCache[index]; ok {
		for _, rm := range fm {
			for _, tm := range rm {
				for ts, bitmap := range tm {
					if ts < start || ts > end {
						continue
					}
					pool.Process(ClearParams{FoundSet: nbm, Target: bitmap})
					//, Index: index, Field: fname, RowID: rowID, Timestamp: ts})
				}
			}
		}
	}

	if fm, ok := m.bsiCache[index]; ok {
		for field, tm := range fm {
			for ts, bsi := range tm {
				if ts < start || ts > end {
					continue
				}
				bsi.Lock.Lock()
				clearSet := roaring64.FastAnd(bsi.GetExistenceBitmap(), nbm)
				if !clearSet.IsEmpty() {
					m.updateRelationshipReverseArtifactForBSIFragment(index, field, ts, bsi.BSI, clearSet, nil)
					bsi.ClearValues(clearSet)
					bsi.ModTime = time.Now()
					bsi.AccessTime = bsi.ModTime
					m.updateSeedCacheForBSIFragment(index, field, ts, nil, clearSet)
				}
				bsi.Lock.Unlock()
			}
		}
	}
}

// Updates to the standard BSI field value cache
func (m *BitmapIndex) updateBSICache(f *BitmapFragment) {

	start := time.Now()
	applyTime := fragmentApplyTime(f, start)

	// This is a special case handling of a "clear" of an existing value.  Must already exist.
	if f.IsClear {
		if len(f.BitData) != 1 {
			u.Errorf("updateBSICache clear operation EBM - Index out of range %d, Index = %s, Field = %s",
				len(f.BitData), f.IndexName, f.FieldName)
		}
		ebm := roaring64.NewBitmap()
		if err := ebm.UnmarshalBinary(f.BitData[0]); err != nil {
			u.Errorf("updateBSICache clear operation EBM - UnmarshalBinary error - %v", err)
			return
		}
		existBm, ok := m.bsiCache[f.IndexName][f.FieldName][f.Time.UnixNano()]
		if !ok {
			u.Errorf("updateBSICache clear operation - Value %s/%s/%v not found.", f.IndexName,
				f.FieldName, f.Time)
			return
		}
		existBm.Lock.Lock()
		clearSet := roaring64.FastAnd(existBm.GetExistenceBitmap(), ebm)
		m.updateRelationshipReverseArtifactForBSIFragment(f.IndexName, f.FieldName, f.Time.UnixNano(), existBm.BSI, clearSet, nil)
		existBm.ClearValues(clearSet)
		existBm.ModTime = applyTime
		existBm.AccessTime = applyTime
		existBm.Lock.Unlock()
		m.updateSeedCacheForBSIFragment(f.IndexName, f.FieldName, f.Time.UnixNano(), nil, clearSet)
		return
	}

	newBSI := m.newBSIBitmap(f.IndexName, f.FieldName)
	newBSI.ModTime = applyTime
	newBSI.AccessTime = applyTime
	if f.IsInit {
		newBSI.PersistTime = applyTime
	}

	if err := newBSI.UnmarshalBinary(f.BitData); err != nil {
		u.Errorf("updateBSICache - UnmarshalBinary error - %v", err)
		return
	}

	m.bsiCacheLock.Lock()
	if _, ok := m.bsiCache[f.IndexName][f.FieldName][f.Time.UnixNano()]; !ok && f.IsUpdate {
		// Silently ignore attempts to update data not in local cache that is not in hashKey
		// because updates are sent to all nodes
		hashKey := fmt.Sprintf("%s/%s/%s", f.IndexName, f.FieldName, formatShardTime(f.Time))
		if !m.Member(hashKey) { // not here and not a member
			m.bsiCacheLock.Unlock()
			return
		}
	}
	if _, ok := m.bsiCache[f.IndexName]; !ok {
		m.bsiCache[f.IndexName] = make(map[string]map[int64]*BSIBitmap)
	}
	if _, ok := m.bsiCache[f.IndexName][f.FieldName]; !ok {
		m.bsiCache[f.IndexName][f.FieldName] = make(map[int64]*BSIBitmap)
	}
	if existBm, ok := m.bsiCache[f.IndexName][f.FieldName][f.Time.UnixNano()]; !ok {
		m.bsiCache[f.IndexName][f.FieldName][f.Time.UnixNano()] = newBSI
		m.bsiCacheLock.Unlock()
		m.updateRelationshipReverseArtifactForBSIFragment(f.IndexName, f.FieldName, f.Time.UnixNano(), nil, nil, newBSI.BSI)
	} else {
		// Lock de-escalation
		existBm.Lock.Lock()
		m.bsiCacheLock.Unlock()
		existingRows := existBm.GetExistenceBitmap()
		newRows := newBSI.GetExistenceBitmap()
		m.bsiMergeCount.Add(1)
		checkStart := time.Now()
		intersects := existingRows.Intersects(newRows)
		m.bsiMergeCheckNanos.Add(uint64(time.Since(checkStart).Nanoseconds()))
		if intersects {
			m.bsiMergeOverlapCount.Add(1)
			clearStart := time.Now()
			clearSet := roaring64.FastAnd(existingRows, newRows)
			m.updateRelationshipReverseArtifactForBSIFragment(f.IndexName, f.FieldName, f.Time.UnixNano(), existBm.BSI, clearSet, newBSI.BSI)
			existBm.ClearValues(clearSet)
			m.bsiMergeClearNanos.Add(uint64(time.Since(clearStart).Nanoseconds()))
		} else {
			m.bsiMergeDisjointCount.Add(1)
			m.updateRelationshipReverseArtifactForBSIFragment(f.IndexName, f.FieldName, f.Time.UnixNano(), nil, nil, newBSI.BSI)
		}
		orStart := time.Now()
		existBm.ParOr(0, newBSI.BSI)
		m.bsiMergeOrNanos.Add(uint64(time.Since(orStart).Nanoseconds()))
		existBm.ModTime = applyTime
		existBm.AccessTime = applyTime
		if f.IsInit {
			existBm.PersistTime = applyTime
		}
		existBm.Lock.Unlock()
	}
	m.updateSeedCacheForBSIFragment(f.IndexName, f.FieldName, f.Time.UnixNano(), newBSI.GetExistenceBitmap(), nil)
	elapsed := time.Since(start)
	m.updBSITime.Store(uint64(elapsed.Milliseconds()))
	if elapsed.Nanoseconds() > (1000000 * 75) {
		u.Debugf("updateBSICache [%s/%s/%s] done in %v.\n", f.IndexName, f.FieldName,
			f.Time.Format(timeFmt), elapsed)
	}
}

// Truncate - Truncate the in-memory data cache for a given index
func (m *BitmapIndex) Truncate(index string) {

	m.bitmapCacheLock.Lock()
	m.bsiCacheLock.Lock()
	defer m.bitmapCacheLock.Unlock()
	defer m.bsiCacheLock.Unlock()
	m.clearSeedCacheForIndex(index)
	m.clearRelationshipReverseArtifactsForIndex(index)

	fm := m.bitmapCache[index]
	for _, rm := range fm {
		for _, tm := range rm {
			for ts := range tm {
				delete(tm, ts)
			}
		}
	}
	bm := m.bsiCache[index]
	for _, tm := range bm {
		for ts := range tm {
			delete(tm, ts)
		}
	}
}

func seedCacheKey(index, field string, fromTime, toTime time.Time) string {
	return fmt.Sprintf("%s/%s/%d/%d", index, field, fromTime.UnixNano(), toTime.UnixNano())
}

func (m *BitmapIndex) cachedSeedBitmap(index, field string, fromTime, toTime time.Time) (*roaring64.Bitmap, uint64, bool) {
	key := seedCacheKey(index, field, fromTime, toTime)
	m.seedCacheLock.RLock()
	defer m.seedCacheLock.RUnlock()
	if entry, ok := m.seedCache[key]; ok && entry != nil && entry.Bits != nil {
		return entry.Bits.Clone(), entry.Count, true
	}
	return nil, 0, false
}

func (m *BitmapIndex) storeSeedBitmap(index, field string, fromTime, toTime time.Time, bits *roaring64.Bitmap, count uint64) {
	if bits == nil {
		return
	}
	key := seedCacheKey(index, field, fromTime, toTime)
	m.seedCacheLock.Lock()
	defer m.seedCacheLock.Unlock()
	if m.seedCache == nil {
		m.seedCache = make(map[string]*SeedBitmap)
	}
	m.seedCache[key] = &SeedBitmap{
		Index:    index,
		Field:    field,
		FromNano: fromTime.UnixNano(),
		ToNano:   toTime.UnixNano(),
		Bits:     bits.Clone(),
		Count:    count,
	}
}

func (m *BitmapIndex) updateSeedCacheForBSIFragment(index, field string, shardNano int64, added *roaring64.Bitmap, removed *roaring64.Bitmap) {
	m.seedCacheLock.Lock()
	defer m.seedCacheLock.Unlock()
	for _, entry := range m.seedCache {
		if entry == nil || entry.Bits == nil || entry.Index != index || entry.Field != field {
			continue
		}
		if shardNano < entry.FromNano || shardNano > entry.ToNano {
			continue
		}
		if removed != nil {
			entry.Bits.AndNot(removed)
			removedCount := removed.GetCardinality()
			if removedCount > entry.Count {
				entry.Count = 0
			} else {
				entry.Count -= removedCount
			}
		}
		if added != nil {
			entry.Bits = roaring64.ParOr(0, entry.Bits, added)
			entry.Count += added.GetCardinality()
		}
	}
}

func (m *BitmapIndex) clearSeedCacheForIndex(index string) {
	m.seedCacheLock.Lock()
	defer m.seedCacheLock.Unlock()
	for key, entry := range m.seedCache {
		if entry != nil && entry.Index == index {
			delete(m.seedCache, key)
		}
	}
}

func (m *BitmapIndex) cleanupStrandedShards() {

	m.cleanupLock.RLock()
	defer m.cleanupLock.RUnlock()

	m.iterateBSICache(func(p *Partition) error {
		return m.cleanupOp(p)
	})
	m.iterateBitmapCache(func(p *Partition) error {
		return m.cleanupOp(p)
	})
}

// cleanupOp - Remove stranded partitions
func (m *BitmapIndex) cleanupOp(p *Partition) error {

	hashKey := partitionHashKey(p)

	owned, nodeKeys := m.cleanupPartitionOwned(hashKey)
	if owned {
		return nil
	}

	// fmt.Println("cleanupOp ", m.hashKey, " hashKey ", hashKey, " nodeKeys ", nodeKeys)

	nMap := make(map[string]int, 0)
	for _, k := range nodeKeys {
		nMap[k] = 1
	}
	_, ok := nMap[m.hashKey]
	if !ok {

		fmt.Println("cleanupOp key not in HashTable.GetN ", m.hashKey, " key ", hashKey, " nodeKeys ", nodeKeys, "nmap", nMap, "field", p.Field)
		fmt.Println("cleanupOp will delete ", hashKey, "from node", m.hashKey)
		if false && p.Field == "isActive" { // atw deleteme:  this is a test
			// seeking customers_qa/isActive/1/1970-01-01T00
			fmt.Println("cleanupOp isActive", m.hashKey, " hashKey ", hashKey, " nodeKeys ", nodeKeys, "nmap", nMap, "field", p.Field)
			nodeKeys2 := m.HashTable.GetN(m.Replicas, "customers_qa/isActive/0/1970-01-01T00")
			nodeKeys3 := m.HashTable.GetN(m.Replicas, "customers_qa/isActive/1/1970-01-01T00")
			fmt.Println("cleanupOp nodeKeys2", m.hashKey, nodeKeys2, nodeKeys3)
		}

		m.partitionQueue <- m.NewPartitionOperation(p, true)
	}
	return nil
}

func (m *BitmapIndex) cleanupPartitionOwned(hashKey string) (bool, []string) {
	if m == nil || m.Node == nil || m.Conn == nil || m.consul == nil || m.HashTable == nil || m.Replicas <= 0 {
		return true, nil
	}
	nodeKeys := m.HashTable.GetN(m.Replicas, hashKey)
	if len(nodeKeys) == 0 {
		return true, nodeKeys
	}
	for _, k := range nodeKeys {
		if k == m.hashKey {
			return true, nodeKeys
		}
	}
	return false, nodeKeys
}

func partitionHashKey(p *Partition) string {
	if p.RowIDOrBits >= 0 {
		return fmt.Sprintf("%s/%s/%d/%s", p.Index, p.Field, p.RowIDOrBits, formatShardTime(p.Time))
	}
	return fmt.Sprintf("%s/%s/%s", p.Index, p.Field, formatShardTime(p.Time))
}

func (m *BitmapIndex) iterateBitmapCache(op func(p *Partition) error) {

	m.bitmapCacheLock.RLock()
	defer m.bitmapCacheLock.RUnlock()
	for indexName, fm := range m.bitmapCache {
		for fieldName, rm := range fm {
			for rowID, tm := range rm {
				for ts, bitmap := range tm {
					bitmap.Lock.Lock()
					partition := &Partition{Index: indexName, Field: fieldName, Time: time.Unix(0, ts),
						TQType: bitmap.TQType, RowIDOrBits: int64(rowID), Shard: bitmap}
					if err := op(partition); err != nil {
						u.Error(err)
					}
					bitmap.Lock.Unlock()
				}
			}
		}
	}
}

func (m *BitmapIndex) iterateBSICache(op func(p *Partition) error) {

	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()
	for indexName, fm := range m.bsiCache {
		for fieldName, tm := range fm {
			for ts, bsi := range tm {
				bsi.Lock.Lock()
				partition := &Partition{Index: indexName, Field: fieldName, Time: time.Unix(0, ts),
					TQType: bsi.TQType, RowIDOrBits: -1, Shard: bsi}
				if err := op(partition); err != nil {
					u.Error(err)
				}
				bsi.Lock.Unlock()
			}
		}
	}
}

// calculateMemoryUsage updates the best-effort cache memory estimate.
//
// This status-only estimator must not wait behind query or mutation work. If a
// cache map is busy, the previous estimate is retained. If an individual shard
// is busy, that shard is skipped for this sample.
func (m *BitmapIndex) calculateMemoryUsage() {

	memoryUsed := 0
	shardCount := 0

	if !m.bsiCacheLock.TryRLock() {
		return
	}
	for _, fm := range m.bsiCache {
		for _, tm := range fm {
			for _, bsi := range tm {
				shardCount++
				if !bsi.Lock.TryRLock() {
					continue
				}
				if b, err := bsi.MarshalBinary(); err == nil {
					for _, x := range b {
						memoryUsed += len(x)
					}
				}
				bsi.Lock.RUnlock()
			}
		}
	}
	m.bsiCacheLock.RUnlock()

	if !m.bitmapCacheLock.TryRLock() {
		return
	}
	for _, fm := range m.bitmapCache {
		for _, rm := range fm {
			for _, tm := range rm {
				for _, bitmap := range tm {
					shardCount++
					if !bitmap.Lock.TryRLock() {
						continue
					}
					if b, err := bitmap.Bits.MarshalBinary(); err == nil {
						memoryUsed += len(b)
					}
					bitmap.Lock.RUnlock()
				}
			}
		}
	}
	m.bitmapCacheLock.RUnlock()

	m.memoryUsed = memoryUsed
	m.shardCount = shardCount
}

func (m *BitmapIndex) calculateShardCount() {
	shardCount := 0

	m.bsiCacheLock.RLock()
	for _, fm := range m.bsiCache {
		for _, tm := range fm {
			shardCount += len(tm)
		}
	}
	m.bsiCacheLock.RUnlock()

	m.bitmapCacheLock.RLock()
	for _, fm := range m.bitmapCache {
		for _, rm := range fm {
			for _, tm := range rm {
				shardCount += len(tm)
			}
		}
	}
	m.bitmapCacheLock.RUnlock()

	m.shardCount = shardCount
}

func (m *BitmapIndex) truncateCaches(index string) {

	m.bitmapCacheLock.Lock()
	m.bsiCacheLock.Lock()
	defer m.bitmapCacheLock.Unlock()
	defer m.bsiCacheLock.Unlock()

	fm, _ := m.bitmapCache[index]
	if fm != nil {
		for _, rm := range fm {
			for _, tm := range rm {
				for ts := range tm {
					delete(tm, ts)
				}
			}
		}
	}

	xm, _ := m.bsiCache[index]
	if xm != nil {
		for _, tm := range xm {
			for ts := range tm {
				delete(tm, ts)
			}
		}
	}
}

// Iterate standard bitmap cache looking for potential writes (dirty data)
func (m *BitmapIndex) checkPersistBitmapCache(forceSync bool) (int, uint64, error) {
	summary, err := m.checkPersistBitmapCacheWithTimings(forceSync)
	return summary.bitmapCount, summary.bitmapWrites, err
}

type bitmapCachePersistSummary struct {
	bitmapCount  int
	bitmapWrites uint64
	scanElapsed  time.Duration
	writeElapsed time.Duration
}

func (m *BitmapIndex) checkPersistBitmapCacheWithTimings(forceSync bool) (bitmapCachePersistSummary, error) {
	var summary bitmapCachePersistSummary

	if m.persistenceDisabled() {
		return summary, nil // test mode, persistence disabled
	}
	manifestDirty := false
	defer func() {
		if manifestDirty {
			if err := m.invalidateBitmapShardManifest("standard bitmap persistence"); err != nil {
				u.Warnf("BitmapIndex manifest invalidation failed: %v", err)
			}
		}
	}()

	m.bitmapCacheLock.RLock()
	defer m.bitmapCacheLock.RUnlock()

	start := time.Now()
	type standardPersistGroup struct {
		indexName string
		fieldName string
		shardNano int64
		tqType    string
		bitmaps   map[uint64]*StandardBitmap
		dirty     bool
	}
	groups := make(map[string]*standardPersistGroup)
	scanStart := time.Now()
	for indexName, index := range m.bitmapCache {
		for fieldName, field := range index {
			for rowID, ts := range field {
				for t, bitmap := range ts {
					summary.bitmapCount++
					if bitmap == nil {
						continue
					}
					bitmap.Lock.RLock()
					tqType := bitmap.TQType
					dirty := forceSync || bitmap.ModTime.After(bitmap.PersistTime)
					bitmap.Lock.RUnlock()
					groupShardNano := t
					if tqType == "" {
						groupShardNano = 0
					}
					key := fmt.Sprintf("%s/%s/%d", indexName, fieldName, groupShardNano)
					group := groups[key]
					if group == nil {
						group = &standardPersistGroup{
							indexName: indexName,
							fieldName: fieldName,
							shardNano: groupShardNano,
							tqType:    tqType,
							bitmaps:   make(map[uint64]*StandardBitmap),
						}
						groups[key] = group
					}
					group.bitmaps[rowID] = bitmap
					group.dirty = group.dirty || dirty
				}
			}
		}
	}
	summary.scanElapsed = time.Since(scanStart)

	writeStart := time.Now()
	for _, group := range groups {
		if !group.dirty {
			continue
		}
		shardTime := time.Unix(0, group.shardNano)
		if _, err := m.saveCompleteStandardBundle(group.bitmaps, group.indexName, group.fieldName, shardTime, group.tqType); err != nil {
			return summary, fmt.Errorf("saveCompleteStandardBundle failed index=%s field=%s time=%s: %w",
				group.indexName, group.fieldName, shardTime.UTC().Format(timeFmt), err)
		}
		summary.bitmapWrites++
		manifestDirty = true
	}
	summary.writeElapsed = time.Since(writeStart)

	elapsed := time.Since(start)
	m.saveBitmapTime.Store(uint64(elapsed.Milliseconds()))
	m.bitmapCount = summary.bitmapCount
	if summary.bitmapWrites > 0 {
		if forceSync {
			m.saveBitmapTCnt.Store(summary.bitmapWrites)
			//u.Debugf("Persist [timer expired] %d files done in %v", writeCount, elapsed)
		} else {
			m.saveBitmapECnt.Store(summary.bitmapWrites)
			//u.Debugf("Persist [edge triggered] %d files done in %v", writeCount, elapsed)
		}
	}
	return summary, nil
}

// Iterate BSI cache looking for potential writes (dirty data)
func (m *BitmapIndex) checkPersistBSICache(forceSync bool) (int, uint64, error) {
	summary, err := m.checkPersistBSICacheWithTimings(forceSync)
	return summary.bsiCount, summary.bsiWrites, err
}

type bsiCachePersistSummary struct {
	bsiCount         int
	bsiWrites        uint64
	bsiPackWrites    uint64
	scanElapsed      time.Duration
	writeElapsed     time.Duration
	marshalElapsed   time.Duration
	encodeElapsed    time.Duration
	pathElapsed      time.Duration
	fileWriteElapsed time.Duration
	cleanupElapsed   time.Duration
	chunkCount       int
	chunkBytes       uint64
	bundleBytes      uint64
}

func (m *BitmapIndex) checkPersistBSICacheWithTimings(forceSync bool) (bsiCachePersistSummary, error) {
	var summary bsiCachePersistSummary

	if m.persistenceDisabled() {
		return summary, nil // test mode persistence disabled
	}
	manifestDirty := false
	defer func() {
		if manifestDirty {
			if err := m.invalidateBitmapShardManifest("BSI persistence"); err != nil {
				u.Warnf("BitmapIndex manifest invalidation failed: %v", err)
			}
		}
	}()

	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()

	start := time.Now()
	type bsiPersistGroup struct {
		indexName string
		shardNano int64
		tqType    string
		bsis      map[string]*BSIBitmap
		dirty     bool
	}
	groups := make(map[string]*bsiPersistGroup)
	scanStart := time.Now()
	for indexName, index := range m.bsiCache {
		for fieldName, field := range index {
			for t, bsi := range field {
				summary.bsiCount++
				if bsi == nil {
					continue
				}
				bsi.Lock.RLock()
				tqType := bsi.TQType
				dirty := forceSync || bsi.ModTime.After(bsi.PersistTime)
				bsi.Lock.RUnlock()
				groupShardNano := t
				if tqType == "" {
					groupShardNano = 0
				}
				key := fmt.Sprintf("%s/%s/%d", indexName, tqType, groupShardNano)
				group := groups[key]
				if group == nil {
					group = &bsiPersistGroup{
						indexName: indexName,
						shardNano: groupShardNano,
						tqType:    tqType,
						bsis:      make(map[string]*BSIBitmap),
					}
					groups[key] = group
				}
				group.bsis[fieldName] = bsi
				group.dirty = group.dirty || dirty
			}
		}
	}
	summary.scanElapsed = time.Since(scanStart)

	writeStart := time.Now()
	for _, group := range groups {
		if !group.dirty {
			continue
		}
		shardTime := time.Unix(0, group.shardNano)
		timings, err := m.saveCompleteBSIPackWithTimings(group.bsis, group.indexName, shardTime, group.tqType)
		if err != nil {
			return summary, fmt.Errorf("saveCompleteBSIPack failed index=%s time=%s: %w",
				group.indexName, shardTime.UTC().Format(timeFmt), err)
		}
		summary.marshalElapsed += timings.marshalElapsed
		summary.encodeElapsed += timings.encodeElapsed
		summary.pathElapsed += timings.pathElapsed
		summary.fileWriteElapsed += timings.fileWriteElapsed
		summary.cleanupElapsed += timings.cleanupElapsed
		summary.chunkCount += timings.chunkCount
		summary.chunkBytes += timings.chunkBytes
		summary.bundleBytes += timings.bundleBytes
		summary.bsiWrites += uint64(len(group.bsis))
		summary.bsiPackWrites++
		manifestDirty = true
	}
	summary.writeElapsed = time.Since(writeStart)

	elapsed := time.Since(start)
	m.saveBSITime.Store(uint64(elapsed.Milliseconds()))
	m.bsiCount = summary.bsiCount
	if summary.bsiWrites > 0 {
		if forceSync {
			m.saveBSITCnt.Store(summary.bsiWrites)
			//u.Debugf("Persist BSI [timer expired] %d files done in %v", writeCount, elapsed)
		} else {
			m.saveBSIECnt.Store(summary.bsiWrites)
			//u.Debugf("Persist BSI [edge triggered] %d files done in %v", writeCount, elapsed)
		}
	}
	return summary, nil
}

func (m *BitmapIndex) persistenceDisabled() bool {
	return m.ServicePort == 0 && m.Conn == nil && !m.IsLocalCluster
}

// BulkClear - Batch "delete".
func (m *BitmapIndex) BulkClear(ctx context.Context, req *pb.BulkClearRequest) (*empty.Empty, error) {

	if req.Index == "" {
		return &empty.Empty{}, fmt.Errorf("index not specified for bulk clear criteria")
	}

	foundSet := roaring64.NewBitmap()
	if err := foundSet.UnmarshalBinary(req.FoundSet); err != nil {
		return &empty.Empty{}, err
	}
	m.clearAll(req.Index, int64(req.FromTime), int64(req.ToTime), foundSet)
	return &empty.Empty{}, nil

}

// Flush will first wait until everything currently in the queue is processed, maybe more.
// Then it will wait until every worker comes around to the top of its loop so nothing is still in progress.
// Then it will return.
func (m *BitmapIndex) flush() error {

	// fmt.Println("flush starting", m.Node.hashKey)
	timeout := bitmapFlushTimeout()

	// part 1. Put a nop on the queue, wait for it reach some worker
	frag := newBitmapFragment("", "", 0, time.Now(), nil, false, false, false)
	frag.IsNop = true
	m.fragQueue <- frag
	select {
	case <-frag.Done:
		// fmt.Println("flush part 1 done", m.Node.hashKey)
	case <-time.After(timeout):
		err := fmt.Errorf("flush part 1 timeout %v", m.Node.hashKey)
		return err
	}

	// Part 2 Put a nop in the aux of EVERY worker, wait for it to reach the top of the loop
	// when any frag that might have been in progress is done.
	group := errgroup.Group{}

	for i := range m.workers {
		index := i
		group.Go(func() error {
			w := m.workers[index]
			frag := newBitmapFragment("", "", 0, time.Now(), nil, false, false, false)
			frag.IsNop = true
			w.aux <- frag
			select {
			case <-frag.Done:
				// fmt.Println("flush part 2 done")
			case <-time.After(timeout):
				err := fmt.Errorf("flush part 2 timeout index= %v %v", w.index, m.Node.hashKey)
				return err
			}
			return nil
		})

	}

	// Wait for all goroutines to complete.
	if err := group.Wait(); err != nil {
		fmt.Printf("flush errgroup tasks ended up with an error: %v %v\n", err, m.Node.hashKey)
		return err
	} else {
		// fmt.Println("flush all works done successfully", m.Node.hashKey)
	}

	// done
	return nil
}

func (m *BitmapIndex) persistDirtyCaches() (int, uint64, int, uint64, error) {
	return m.persistCaches(false)
}

type persistCachesSummary struct {
	bitmapCount         int
	bitmapWrites        uint64
	bsiCount            int
	bsiWrites           uint64
	bsiPackWrites       uint64
	bitmapElapsed       time.Duration
	bitmapScanElapsed   time.Duration
	bitmapWriteElapsed  time.Duration
	bsiElapsed          time.Duration
	bsiScanElapsed      time.Duration
	bsiWriteElapsed     time.Duration
	bsiMarshalElapsed   time.Duration
	bsiEncodeElapsed    time.Duration
	bsiPathElapsed      time.Duration
	bsiFileWriteElapsed time.Duration
	bsiCleanupElapsed   time.Duration
	bsiChunkCount       int
	bsiChunkBytes       uint64
	bsiBundleBytes      uint64
}

func (m *BitmapIndex) persistCaches(forceSync bool) (int, uint64, int, uint64, error) {
	summary, err := m.persistCachesWithTimings(forceSync)
	return summary.bitmapCount, summary.bitmapWrites, summary.bsiCount, summary.bsiWrites, err
}

func (m *BitmapIndex) persistCachesWithTimings(forceSync bool) (persistCachesSummary, error) {
	var summary persistCachesSummary

	bitmapStart := time.Now()
	bitmapSummary, err := m.checkPersistBitmapCacheWithTimings(forceSync)
	summary.bitmapElapsed = time.Since(bitmapStart)
	summary.bitmapCount = bitmapSummary.bitmapCount
	summary.bitmapWrites = bitmapSummary.bitmapWrites
	summary.bitmapScanElapsed = bitmapSummary.scanElapsed
	summary.bitmapWriteElapsed = bitmapSummary.writeElapsed
	if err != nil {
		return summary, err
	}

	bsiStart := time.Now()
	bsiSummary, err := m.checkPersistBSICacheWithTimings(forceSync)
	summary.bsiElapsed = time.Since(bsiStart)
	summary.bsiCount = bsiSummary.bsiCount
	summary.bsiWrites = bsiSummary.bsiWrites
	summary.bsiPackWrites = bsiSummary.bsiPackWrites
	summary.bsiScanElapsed = bsiSummary.scanElapsed
	summary.bsiWriteElapsed = bsiSummary.writeElapsed
	summary.bsiMarshalElapsed = bsiSummary.marshalElapsed
	summary.bsiEncodeElapsed = bsiSummary.encodeElapsed
	summary.bsiPathElapsed = bsiSummary.pathElapsed
	summary.bsiFileWriteElapsed = bsiSummary.fileWriteElapsed
	summary.bsiCleanupElapsed = bsiSummary.cleanupElapsed
	summary.bsiChunkCount = bsiSummary.chunkCount
	summary.bsiChunkBytes = bsiSummary.chunkBytes
	summary.bsiBundleBytes = bsiSummary.bundleBytes
	if err != nil {
		return summary, err
	}
	return summary, nil
}

func (m *BitmapIndex) cacheHasDirtyEntries() bool {
	if m.standardBitmapCacheHasDirtyEntries() {
		return true
	}
	return m.bsiCacheHasDirtyEntries()
}

func (m *BitmapIndex) standardBitmapCacheHasDirtyEntries() bool {
	m.bitmapCacheLock.RLock()
	defer m.bitmapCacheLock.RUnlock()
	for _, index := range m.bitmapCache {
		for _, field := range index {
			for _, ts := range field {
				for _, bitmap := range ts {
					if bitmap == nil {
						continue
					}
					bitmap.Lock.RLock()
					dirty := bitmap.ModTime.After(bitmap.PersistTime)
					bitmap.Lock.RUnlock()
					if dirty {
						return true
					}
				}
			}
		}
	}
	return false
}

func (m *BitmapIndex) bsiCacheHasDirtyEntries() bool {
	m.bsiCacheLock.RLock()
	defer m.bsiCacheLock.RUnlock()
	for _, index := range m.bsiCache {
		for _, field := range index {
			for _, bsi := range field {
				if bsi == nil {
					continue
				}
				bsi.Lock.RLock()
				dirty := bsi.ModTime.After(bsi.PersistTime)
				bsi.Lock.RUnlock()
				if dirty {
					return true
				}
			}
		}
	}
	return false
}

// Commit flushes queued mutations and persists dirty shards to a savepoint.
func (m *BitmapIndex) Commit(ctx context.Context, e *empty.Empty) (*empty.Empty, error) {

	flushStart := time.Now()
	err := m.flush()
	if err != nil {
		return &empty.Empty{}, err
	}
	flushElapsed := time.Since(flushStart)

	manifestCheckElapsed := time.Duration(0)
	dirtyCheckStart := time.Now()
	hasDirtyEntries := m.cacheHasDirtyEntries()
	dirtyCheckElapsed := time.Since(dirtyCheckStart)
	if !hasDirtyEntries {
		manifestCheckStart := time.Now()
		manifest, observation := m.loadAndObserveBitmapShardManifest(nil)
		manifestCheckElapsed = time.Since(manifestCheckStart)
		if observation.Status == "ok" {
			fmt.Printf("BitmapIndex commit reused clean savepoint node=%s manifest_entries=%d manifest_files=%d flush_elapsed=%s dirty_check_elapsed=%s manifest_check_elapsed=%s\n",
				m.Node.hashKey, manifest.Stats.TotalEntries, manifest.Stats.TotalFiles, flushElapsed, dirtyCheckElapsed, manifestCheckElapsed)
			return e, nil
		}
		manifestStart := time.Now()
		if err := m.saveBitmapShardManifestFromCache("commit"); err != nil {
			return &empty.Empty{}, err
		}
		manifestRefreshElapsed := time.Since(manifestStart)
		manifestCheckStart = time.Now()
		manifest, observation = m.loadAndObserveBitmapShardManifest(nil)
		manifestCheckElapsed += time.Since(manifestCheckStart)
		if observation.Status == "ok" {
			fmt.Printf("BitmapIndex commit refreshed clean savepoint node=%s manifest_entries=%d manifest_files=%d flush_elapsed=%s dirty_check_elapsed=%s manifest_check_elapsed=%s manifest_refresh_elapsed=%s\n",
				m.Node.hashKey, manifest.Stats.TotalEntries, manifest.Stats.TotalFiles, flushElapsed, dirtyCheckElapsed, manifestCheckElapsed, manifestRefreshElapsed)
			return e, nil
		}
	}

	// Commit is an explicit durability savepoint. Dirty commits only need to
	// persist dirty shards; clean commits can still force a one-time savepoint
	// repair when the manifest is missing or stale.
	persistStart := time.Now()
	forceCleanSavepointRepair := !hasDirtyEntries
	persistSummary, err := m.persistCachesWithTimings(forceCleanSavepointRepair)
	if err != nil {
		return &empty.Empty{}, err
	}
	persistElapsed := time.Since(persistStart)
	m.shardCount = persistSummary.bitmapCount + persistSummary.bsiCount

	manifestElapsed := time.Duration(0)
	if persistSummary.bitmapWrites+persistSummary.bsiWrites > 0 {
		manifestStart := time.Now()
		if err := m.saveBitmapShardManifestFromCache("commit"); err != nil {
			return &empty.Empty{}, err
		}
		manifestElapsed = time.Since(manifestStart)
	} else {
		manifestCheckStart := time.Now()
		_, observation := m.loadAndObserveBitmapShardManifest(nil)
		manifestCheckElapsed += time.Since(manifestCheckStart)
		if observation.Status != "ok" {
			manifestStart := time.Now()
			if err := m.saveBitmapShardManifestFromCache("commit"); err != nil {
				return &empty.Empty{}, err
			}
			manifestElapsed = time.Since(manifestStart)
		}
	}
	fmt.Printf("BitmapIndex commit persisted node=%s bitmap_shards=%d bitmap_writes=%d bsi_shards=%d bsi_writes=%d bsi_pack_writes=%d flush_elapsed=%s dirty_check_elapsed=%s persist_elapsed=%s bitmap_persist_elapsed=%s bitmap_scan_elapsed=%s bitmap_write_elapsed=%s bsi_persist_elapsed=%s bsi_scan_elapsed=%s bsi_write_elapsed=%s bsi_marshal_elapsed=%s bsi_encode_elapsed=%s bsi_path_elapsed=%s bsi_file_write_elapsed=%s bsi_cleanup_elapsed=%s bsi_chunks=%d bsi_chunk_bytes=%d bsi_bundle_bytes=%d bsi_merge_count=%d bsi_merge_disjoint=%d bsi_merge_overlap=%d bsi_merge_check_elapsed=%s bsi_merge_clear_elapsed=%s bsi_merge_or_elapsed=%s manifest_check_elapsed=%s manifest_refresh_elapsed=%s\n",
		m.Node.hashKey,
		persistSummary.bitmapCount, persistSummary.bitmapWrites,
		persistSummary.bsiCount, persistSummary.bsiWrites, persistSummary.bsiPackWrites,
		flushElapsed, dirtyCheckElapsed, persistElapsed,
		persistSummary.bitmapElapsed, persistSummary.bitmapScanElapsed, persistSummary.bitmapWriteElapsed,
		persistSummary.bsiElapsed, persistSummary.bsiScanElapsed, persistSummary.bsiWriteElapsed,
		persistSummary.bsiMarshalElapsed, persistSummary.bsiEncodeElapsed, persistSummary.bsiPathElapsed,
		persistSummary.bsiFileWriteElapsed, persistSummary.bsiCleanupElapsed,
		persistSummary.bsiChunkCount, persistSummary.bsiChunkBytes, persistSummary.bsiBundleBytes,
		m.bsiMergeCount.Load(), m.bsiMergeDisjointCount.Load(), m.bsiMergeOverlapCount.Load(),
		time.Duration(m.bsiMergeCheckNanos.Load()), time.Duration(m.bsiMergeClearNanos.Load()),
		time.Duration(m.bsiMergeOrNanos.Load()),
		manifestCheckElapsed, manifestElapsed)
	return e, nil
}

// CheckoutSequence returns another batch of column IDs to the client.
func (m *BitmapIndex) CheckoutSequence(ctx context.Context,
	req *pb.CheckoutSequenceRequest) (*pb.CheckoutSequenceResponse, error) {

	if req.Index == "" {
		return nil, fmt.Errorf("index not specified for sequencer checkout")
	}
	if req.PkField == "" {
		return nil, fmt.Errorf("PK field not specified for sequencer checkout")
	}

	if req.ReservationSize <= 0 {
		return nil, fmt.Errorf("PK field not specified for sequencer checkout")
	}

	m.bsiCacheLock.Lock()
	if _, ok := m.bsiCache[req.Index]; !ok {
		m.bsiCache[req.Index] = make(map[string]map[int64]*BSIBitmap)
	}
	if _, ok := m.bsiCache[req.Index][req.PkField]; !ok {
		m.bsiCache[req.Index][req.PkField] = make(map[int64]*BSIBitmap)
	}
	targetBSI, ok := m.bsiCache[req.Index][req.PkField][req.Time]
	if !ok {
		targetBSI = m.newBSIBitmap(req.Index, req.PkField)
		m.bsiCache[req.Index][req.PkField][req.Time] = targetBSI
	}
	targetBSI.Lock.Lock()
	defer targetBSI.Lock.Unlock()
	m.bsiCacheLock.Unlock()

	/*
		   if !ok {
			   return nil, fmt.Errorf("cannot find BSI for %s [%s] (TS %d)", req.Index, req.PkField, req.Time)
		   }
	*/

	// Get the maximum column id from EBM
	var maxColID uint64
	if targetBSI.GetExistenceBitmap().GetCardinality() > 0 {
		maxColID = targetBSI.GetExistenceBitmap().Maximum()
	}

	// Purge any sequencers that are complete
	targetBSI.sequencerQueue.Purge(maxColID)

	// Get largest checked out maximum. if queue is empty (max = 0), the new start is the maximum column id + 1
	var nextSeqStart uint64
	maxSeq := targetBSI.sequencerQueue.Maximum()
	if maxSeq == 0 {
		if maxColID == 0 {
			// if time quantum enabled then add the timestamp to the starting sequence
			if targetBSI.TQType != "" {
				nextSeqStart = uint64(req.Time) + 1
			} else {
				nextSeqStart = 1
			}
		} else {
			nextSeqStart = maxColID + 1
		}
	} else {
		nextSeqStart = maxSeq + 1
	}
	targetBSI.sequencerQueue.Push(shared.NewSequencer(nextSeqStart, int(req.ReservationSize)))
	res := &pb.CheckoutSequenceResponse{Start: nextSeqStart, Count: req.ReservationSize}
	//u.Debugf("SERVER RESPONSE [Start %d, Count %d] Queue depth = %d", res.Start, res.Count, targetBSI.sequencerQueue.Len())
	return res, nil

}

// TableOperation - Process TableOperations.
// fill m.tableCache
func (m *BitmapIndex) TableOperation(ctx context.Context, req *pb.TableOperationRequest) (*empty.Empty, error) {

	if req.Table == "" {
		return &empty.Empty{}, fmt.Errorf("table not specified for table operation")
	}

	switch req.Operation {
	case pb.TableOperationRequest_DEPLOY:
		schemaPath := ""
		if m.consul == nil || m.ServicePort == 0 {
			schemaPath = filepath.Join(m.dataDir, "config")
		}
		if table, err := shared.LoadSchema(schemaPath, req.Table, m.consul); err != nil {
			u.Errorf("could not load schema for %s - %v", req.Table, err)
			return &empty.Empty{}, err
		} else {
			m.tableCacheLock.Lock()
			m.tableCache[req.Table] = table
			m.tableCacheLock.Unlock()
			u.Infof("%s schema for table re-loaded and initialized %s", m.hashKey, req.Table)
		}
		m.rebuildRelationshipReverseArtifactsForIndex(req.Table)
	case pb.TableOperationRequest_DROP:
		m.tableCacheLock.Lock()
		defer m.tableCacheLock.Unlock()
		delete(m.tableCache, req.Table)
		m.Truncate(req.Table)
		tableDir := m.dataDir + sep + "bitmap" + sep + req.Table
		if err := os.RemoveAll(tableDir); err != nil {
			u.Infof("error dropping table %s directory - %v", req.Table, err)
		} else {
			if err := m.invalidateBitmapShardManifest("table drop"); err != nil {
				u.Warnf("BitmapIndex manifest invalidation failed: %v", err)
			}
			if err := m.saveBitmapShardManifestFromCache("table_drop"); err != nil {
				u.Warnf("BitmapIndex manifest refresh failed after table drop: %v", err)
			}
			u.Infof("Table %s dropped.", req.Table)
		}
	case pb.TableOperationRequest_TRUNCATE:
		m.tableCacheLock.Lock()
		defer m.tableCacheLock.Unlock()
		m.Truncate(req.Table)
		tableDir := m.dataDir + sep + "bitmap" + sep + req.Table
		if err := os.RemoveAll(tableDir); err != nil {
			u.Errorf("error truncating table %s directory - %v", req.Table, err)
		} else {
			if err := m.invalidateBitmapShardManifest("table truncate"); err != nil {
				u.Warnf("BitmapIndex manifest invalidation failed: %v", err)
			}
			if err := m.saveBitmapShardManifestFromCache("table_truncate"); err != nil {
				u.Warnf("BitmapIndex manifest refresh failed after table truncate: %v", err)
			}
			u.Infof("Table %s truncated.", req.Table)
		}
	default:
		return &empty.Empty{}, fmt.Errorf("unknown operation type for table operation request")
	}

	// do a 'commit' here to make sure all nodes are in sync
	err := m.flush()

	return &empty.Empty{}, err
}

// PartitionInfo - Returns a report containing information about shards.
func (m *BitmapIndex) PartitionInfo(ctx context.Context,
	req *pb.PartitionInfoRequest) (*pb.PartitionInfoResponse, error) {

	if req.Time <= 0 {
		return nil, fmt.Errorf("Time must be specified.")
	}

	res := make([]*pb.PartitionInfoResult, 0)

	// Iterate over shard cache and generate report.
	m.iterateBSICache(func(p *Partition) error {

		if p.Time.UnixNano() > req.Time {
			return nil
		}
		if req.Index != "" && p.Index != req.Index {
			return nil
		}
		if p.TQType == "" {
			return nil
		}
		bsi := p.Shard.(*BSIBitmap)
		r := &pb.PartitionInfoResult{Time: p.Time.UnixNano(), Index: p.Index, Field: p.Field,
			RowIdOrValue: p.RowIDOrBits * -1, ModTime: bsi.ModTime.UnixNano(), TqType: p.TQType}
		if b, err := bsi.MarshalBinary(); err == nil {
			for _, x := range b {
				r.Bytes += uint32(len(x))
			}
		}
		res = append(res, r)
		return nil

	})

	m.iterateBitmapCache(func(p *Partition) error {

		if p.Time.UnixNano() > req.Time {
			return nil
		}
		if req.Index != "" && p.Index != req.Index {
			return nil
		}
		if p.TQType == "" {
			return nil
		}
		bitmap := p.Shard.(*StandardBitmap)
		r := &pb.PartitionInfoResult{Time: p.Time.UnixNano(), Index: p.Index, Field: p.Field,
			RowIdOrValue: p.RowIDOrBits, ModTime: bitmap.ModTime.UnixNano(), TqType: p.TQType}
		if b, err := bitmap.Bits.MarshalBinary(); err == nil {
			r.Bytes += uint32(len(b))
		}
		res = append(res, r)
		return nil

	})

	resp := &pb.PartitionInfoResponse{PartitionInfoResults: res}
	return resp, nil
}

// OfflinePartitions - Purge partitions from memory and move data files to archive directory.
func (m *BitmapIndex) OfflinePartitions(ctx context.Context, req *pb.PartitionInfoRequest) (*empty.Empty, error) {

	if req.Time <= 0 {
		return nil, fmt.Errorf("Time must be specified.")
	}

	ts := time.Unix(0, req.Time)

	if req.Index != "" {
		u.Infof("Offline partition request for %v,  table = %s", ts.Format(timeFmt), req.Index)
	} else {
		u.Infof("Offline partition request for %v, all partitioned tables", ts.Format(timeFmt))
	}

	// Iterate over shard cache insert into partition operation queue
	m.iterateBSICache(func(p *Partition) error {

		if p.Time.UnixNano() > req.Time {
			return nil
		}
		if req.Index != "" && p.Index != req.Index {
			return nil
		}
		m.partitionQueue <- m.NewPartitionOperation(p, false)
		return nil

	})

	m.iterateBitmapCache(func(p *Partition) error {

		if p.Time.UnixNano() > req.Time {
			return nil
		}
		if req.Index != "" && p.Index != req.Index {
			return nil
		}
		m.partitionQueue <- m.NewPartitionOperation(p, false)
		return nil

	})

	return &empty.Empty{}, nil
}

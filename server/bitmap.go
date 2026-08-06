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

func devSkipSyncEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QUANTA_DEV_SKIP_SYNC"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
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
// setBitThreads - Used to identify when incoming API SetBatch calls have fallen to zero triggering writes.
// writeSignal - Channel used by setBitThreads to initiate write operations to persist cache items.
// tableCache - Schema metadata cache (essentially same YAML file used by loader).
type BitmapIndex struct {
	*Node
	bitmapCache     map[string]map[string]map[uint64]map[int64]*StandardBitmap
	bitmapCacheLock sync.RWMutex
	bsiCache        map[string]map[string]map[int64]*BSIBitmap
	bsiCacheLock    sync.RWMutex
	seedCache       map[string]*SeedBitmap
	seedCacheLock   sync.RWMutex
	fragQueue       chan *BitmapFragment
	workersCount    int
	fragFileLock    sync.Mutex
	setBitThreads   *CountTrigger
	writeSignal     chan bool
	tableCache      map[string]*shared.BasicTable
	tableCacheLock  sync.RWMutex
	partitionQueue  chan *PartitionOperation
	bitmapCount     int
	bsiCount        int
	workers         []*WorkerThread
	cleanupLock     sync.RWMutex
	backgroundWG    sync.WaitGroup
	updBitmapTime   atomic.Uint64
	updBSITime      atomic.Uint64
	saveBitmapECnt  atomic.Uint64
	saveBitmapTCnt  atomic.Uint64
	saveBitmapTime  atomic.Uint64
	saveBSIECnt     atomic.Uint64
	saveBSITCnt     atomic.Uint64
	saveBSITime     atomic.Uint64
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
	m.workersCount = 20
	m.writeSignal = make(chan bool, 1)
	m.setBitThreads = NewCountTrigger(m.writeSignal)

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

	m.backgroundWG.Add(1)
	go func() { // wait for signal to persist cache
		defer m.backgroundWG.Done()
		for {
			select {
			case <-m.Stop:
				u.Debug("BitmapIndex Init() received stop signal", m.Node.hashKey)
				return
			case forceSync := <-m.writeSignal:
				// fmt.Println(m.Node.hashKey, "had writeSignal, checkPersist*Cache(", forceSync, ")")
				// go
				if _, _, _, _, err := m.persistCaches(forceSync); err != nil {
					u.Errorf("bitmap cache persist failed: %v", err)
				}
				// fmt.Println(m.Node.hashKey, "had writeSignal DONE")
			}
		}
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

	m.setBitThreads.Add(1)
	defer m.setBitThreads.Add(-1)
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
		if kv.Key == nil || len(kv.Key) == 0 {
			return fmt.Errorf("key must be specified")
		}

		s := strings.Split(kv.IndexPath, "/")
		if len(s) != 2 {
			err = fmt.Errorf("IndexPath %s not valid", kv.IndexPath)
			u.Errorf("%s", err)
			return err
		}
		indexName := s[0]
		fieldName := s[1]

		_, err = m.getFieldConfig(indexName, fieldName)
		if err != nil {
			return err
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
			return nil
		}
		select {
		case m.fragQueue <- frag:
			done = append(done, frag.Done)
			// fmt.Println(m.hashKey, "svr BatchMutate sent to fragQueue", frag.FieldName, frag.RowIDOrBits, frag.Time.Format(timeFmt), uintptr(unsafe.Pointer(m)))
		default:
			return fmt.Errorf("BatchMutate: fragment queue is full")
		}
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
		// fmt.Println(m.Node.hashKey, "batchProcessLoop middle worker loop", worker.index, len(m.setBitThreads.trigger), len(m.writeSignal))

		// this is where it waits for a frag or a write signal
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
		case <-time.After(time.Second * 10):

			forceSync := false
			select {
			case m.writeSignal <- forceSync:
			default:
				// it's ok to be full.  It's just a signal.
			}
			m.shardCount = m.bsiCount + m.bitmapCount
			if worker.index == 0 {
				//u.Debug("batchProcessLoop shard count ", m.hashKey, " shard ", m.shardCount, " bsi ", m.bsiCount, " bitmap ", m.bitmapCount)
			}
			// no default, block
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

	if devSkipSyncEnabled() {
		m.State = Active
		u.Warnf("QUANTA_DEV_SKIP_SYNC enabled; skipping node synchronization and marking %s Active", m.GetNodeID())
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
		for _, tm := range fm {
			for ts, bsi := range tm {
				if ts < start || ts > end {
					continue
				}
				bsi.ClearValues(nbm)
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
	} else {
		// Lock de-escalation
		existBm.Lock.Lock()
		m.bsiCacheLock.Unlock()
		clearSet := roaring64.FastAnd(existBm.GetExistenceBitmap(), newBSI.GetExistenceBitmap())
		existBm.ClearValues(clearSet)
		existBm.ParOr(0, newBSI.BSI)
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

	if !m.bsiCacheLock.TryRLock() {
		return
	}
	for _, fm := range m.bsiCache {
		for _, tm := range fm {
			for _, bsi := range tm {
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

	if m.persistenceDisabled() {
		return 0, 0, nil // test mode, persistence disabled
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

	bitmapCount := 0
	var writeCount uint64
	start := time.Now()
	for indexName, index := range m.bitmapCache {
		for fieldName, field := range index {
			for rowID, ts := range field {
				for t, bitmap := range ts {
					bitmapCount++
					bitmap.Lock.Lock()
					if forceSync || bitmap.ModTime.After(bitmap.PersistTime) {
						if err := m.saveCompleteBitmap(bitmap, indexName, fieldName, int64(rowID),
							time.Unix(0, t)); err != nil {
							bitmap.Lock.Unlock()
							return bitmapCount, writeCount, fmt.Errorf("saveCompleteBitmap failed index=%s field=%s row=%d time=%s: %w",
								indexName, fieldName, rowID, time.Unix(0, t).UTC().Format(timeFmt), err)
						}
						writeCount++
						manifestDirty = true
						bitmap.PersistTime = time.Now()
					}
					bitmap.Lock.Unlock()
				}
			}
		}
	}

	elapsed := time.Since(start)
	m.saveBitmapTime.Store(uint64(elapsed.Milliseconds()))
	m.bitmapCount = bitmapCount
	if writeCount > 0 {
		if forceSync {
			m.saveBitmapTCnt.Store(writeCount)
			//u.Debugf("Persist [timer expired] %d files done in %v", writeCount, elapsed)
		} else {
			m.saveBitmapECnt.Store(writeCount)
			//u.Debugf("Persist [edge triggered] %d files done in %v", writeCount, elapsed)
		}
	}
	return bitmapCount, writeCount, nil
}

// Iterate BSI cache looking for potential writes (dirty data)
func (m *BitmapIndex) checkPersistBSICache(forceSync bool) (int, uint64, error) {

	if m.persistenceDisabled() {
		return 0, 0, nil // test mode persistence disabled
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

	var writeCount uint64
	bsiCount := 0
	start := time.Now()
	for indexName, index := range m.bsiCache {
		for fieldName, field := range index {
			for t, bsi := range field {
				bsiCount++
				bsi.Lock.Lock()
				if forceSync || bsi.ModTime.After(bsi.PersistTime) {
					if err := m.saveCompleteBSI(bsi, indexName, fieldName, int(bsi.BitCount()),
						time.Unix(0, t)); err != nil {
						bsi.Lock.Unlock()
						return bsiCount, writeCount, fmt.Errorf("saveCompleteBSI failed index=%s field=%s time=%s: %w",
							indexName, fieldName, time.Unix(0, t).UTC().Format(timeFmt), err)
					}
					writeCount++
					manifestDirty = true
					bsi.PersistTime = time.Now()
				}
				bsi.Lock.Unlock()
			}
		}
	}

	elapsed := time.Since(start)
	m.saveBSITime.Store(uint64(elapsed.Milliseconds()))
	m.bsiCount = bsiCount
	if writeCount > 0 {
		if forceSync {
			m.saveBSITCnt.Store(writeCount)
			//u.Debugf("Persist BSI [timer expired] %d files done in %v", writeCount, elapsed)
		} else {
			m.saveBSIECnt.Store(writeCount)
			//u.Debugf("Persist BSI [edge triggered] %d files done in %v", writeCount, elapsed)
		}
	}
	return bsiCount, writeCount, nil
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

	// part 1. Put a nop on the queue, wait for it reach some worker
	frag := newBitmapFragment("", "", 0, time.Now(), nil, false, false, false)
	frag.IsNop = true
	m.fragQueue <- frag
	select {
	case <-frag.Done:
		// fmt.Println("flush part 1 done", m.Node.hashKey)
	case <-time.After(30 * time.Second):
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
			case <-time.After(30 * time.Second):
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

func (m *BitmapIndex) persistCaches(forceSync bool) (int, uint64, int, uint64, error) {
	bitmapCount, bitmapWrites, err := m.checkPersistBitmapCache(forceSync)
	if err != nil {
		return bitmapCount, bitmapWrites, 0, 0, err
	}
	bsiCount, bsiWrites, err := m.checkPersistBSICache(forceSync)
	if err != nil {
		return bitmapCount, bitmapWrites, bsiCount, bsiWrites, err
	}
	return bitmapCount, bitmapWrites, bsiCount, bsiWrites, nil
}

// Commit flushes queued mutations and persists dirty shards to a savepoint.
func (m *BitmapIndex) Commit(ctx context.Context, e *empty.Empty) (*empty.Empty, error) {

	err := m.flush()
	if err != nil {
		return &empty.Empty{}, err
	}
	bitmapCount, bitmapWrites, bsiCount, bsiWrites, err := m.persistCaches(true)
	if err != nil {
		return &empty.Empty{}, err
	}
	u.Infof("BitmapIndex commit persisted node=%s bitmap_shards=%d bitmap_writes=%d bsi_shards=%d bsi_writes=%d",
		m.Node.hashKey, bitmapCount, bitmapWrites, bsiCount, bsiWrites)
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

// CountTrigger sends a message when counter reaches zero
type CountTrigger struct {
	num     int
	lock    sync.Mutex
	trigger chan bool
}

// NewCountTrigger constructs a CountTrigger
func NewCountTrigger(t chan bool) *CountTrigger {
	return &CountTrigger{trigger: t}
}

// Add function provides thread safe addition of counter value based on input parameter.
// If counter falls to zero then a value will be sent to trigger channel.
func (c *CountTrigger) Add(n int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.num += n
	if c.num == 0 {
		const forceSync = false
		select {
		// the trigger is the BitmapIndex.writeSignal
		// We don't ask it to force
		case c.trigger <- forceSync:
			return
		default:
			// it is normal for it to get full.
			return
		}
	}
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
			defer m.tableCacheLock.Unlock()
			m.tableCache[req.Table] = table
			u.Infof("%s schema for table re-loaded and initialized %s", m.hashKey, req.Table)
		}
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

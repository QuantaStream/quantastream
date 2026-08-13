package shared

//
// Client side bitmap functions and API wrappers for bulk loading functions such as SetBit and
// SetValue for bitmap and BSI fields respectively.
//

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
	u "github.com/araddon/gou"
	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/sync/errgroup"
)

var (
	// Ensure BitmapIndex implements shared.Service
	_ Service = (*BitmapIndex)(nil)
)

const (
	timeFmt = "2006-01-02T15"
	ifDelim = "/"

	bitmapBatchMutateItemsChunkSize = 4096
)

func formatShardTime(t time.Time) string {
	return t.UTC().Format(timeFmt)
}

// BitmapIndex - Client side API for bitmap operations.
//
// Conn - "base" class wrapper for network connection to servers.
// client - Array of client API wrappers, one each for every server node.
type BitmapIndex struct {
	*Conn
	client []pb.BitmapIndexClient
	local  LocalBitmapIndexService
}

// BSIBatchSetValueProfile captures client-visible work for a routed BSI value batch.
type BSIBatchSetValueProfile struct {
	StartedAt          time.Time
	FinishedAt         time.Time
	TotalElapsed       time.Duration
	RouteElapsed       time.Duration
	BuildElapsed       time.Duration
	MarshalElapsed     time.Duration
	StreamElapsed      time.Duration
	StreamOpenElapsed  time.Duration
	StreamSendElapsed  time.Duration
	StreamCloseElapsed time.Duration
	StreamMaxElapsed   time.Duration
	InputShardCount    int
	InputEntryCount    int
	RoutedItemCount    int
	PutCalls           int
	Local              bool
}

type bitmapBatchMutateItemsNodeProfile struct {
	Items        int
	TotalElapsed time.Duration
	OpenElapsed  time.Duration
	SendElapsed  time.Duration
	CloseElapsed time.Duration
}

// CompareBSIFieldsProjectionStats summarizes projection work reported by
// readable nodes during a same-row BSI comparison.
type CompareBSIFieldsProjectionStats struct {
	ShardsVisited    uint64
	ShardsInWindow   uint64
	ShardsLocal      uint64
	ShardsRetained   uint64
	RetainedRows     uint64
	RetainBypassRows uint64
	RetainElapsed    time.Duration
	ValueElapsed     time.Duration
	MergeElapsed     time.Duration
}

// CompareBSIFieldsStats summarizes client-visible same-row BSI comparison work
// across all readable nodes that answered a comparison request.
type CompareBSIFieldsStats struct {
	Nodes          uint64
	Left           CompareBSIFieldsProjectionStats
	Right          CompareBSIFieldsProjectionStats
	CompareElapsed time.Duration
	OutputRows     uint64
}

// RelationshipReverseArtifactStats summarizes cluster-visible reverse-artifact
// candidate lookup work. Row/value counts are accumulated across node-local
// maintained artifacts.
type RelationshipReverseArtifactStats struct {
	Rows                 uint64
	Values               uint64
	SourceValues         int
	TargetRows           uint64
	LookupElapsed        time.Duration
	FanoutElapsed        time.Duration
	ResponseMergeElapsed time.Duration
	ClientRPCElapsed     time.Duration
	MaxClientRPCElapsed  time.Duration
	Nodes                uint64
}

// RelationshipAlignedValueSumGroup carries one mergeable parent-keyed aggregate
// returned by node-local relationship aggregate pushdown.
type RelationshipAlignedValueSumGroup struct {
	ParentValue       uint64
	RepresentativeRow uint64
	Count             uint64
	Sum               *big.Int
}

// RelationshipAlignedValueSumStats summarizes cluster-visible aligned aggregate
// work. Input cardinalities describe the original request; projection/timing
// fields are accumulated from node-local partials.
type RelationshipAlignedValueSumStats struct {
	Rows              uint64
	Values            uint64
	SourceValues      int
	TargetRows        uint64
	Groups            int
	LookupElapsed     time.Duration
	ProjectionElapsed time.Duration
	AggregateElapsed  time.Duration
	Projection        CompareBSIFieldsProjectionStats
	Nodes             uint64
}

// BitmapGroupAggregateSpec describes one grouped aggregate over bitmap group
// fields. Field is empty for COUNT(*).
type BitmapGroupAggregateSpec struct {
	Function string
	Field    string
}

// BitmapGroupAggregateValue carries mergeable raw aggregate state for one
// aggregate slot.
type BitmapGroupAggregateValue struct {
	Count uint64
	Sum   *big.Int
	Min   *big.Int
	Max   *big.Int
}

// BitmapGroupAggregateGroup is one raw grouped aggregate partial/result keyed
// by bitmap row IDs for each group field.
type BitmapGroupAggregateGroup struct {
	Values []uint64
	Aggs   []BitmapGroupAggregateValue
}

// BitmapGroupAggregateStats summarizes cluster-visible grouped aggregate work.
type BitmapGroupAggregateStats struct {
	Nodes             uint64
	CandidateRows     uint64
	FieldCount        int
	ValueCount        int
	Groups            int
	AggregateCount    int
	BSIFieldCount     int
	BSIProjectElapsed time.Duration
	AggregateElapsed  time.Duration
	ValueSetElapsed   time.Duration
	SumElapsed        time.Duration
	MinMaxElapsed     time.Duration
}

// NewBitmapIndex - Initializer for client side API wrappers.
func NewBitmapIndex(conn *Conn) *BitmapIndex {

	if conn.LocalNodeServices.BitmapIndex != nil {
		c := &BitmapIndex{Conn: conn, local: conn.LocalNodeServices.BitmapIndex}
		conn.RegisterService(c)
		return c
	}
	clients := make([]pb.BitmapIndexClient, len(conn.ClientConnections()))
	for i := 0; i < len(conn.ClientConnections()); i++ {
		clients[i] = pb.NewBitmapIndexClient(conn.ClientConnections()[i])
	}
	c := &BitmapIndex{Conn: conn, client: clients}
	conn.RegisterService(c)
	return c
}

type bitmapClientSnapshot struct {
	index  int
	client pb.BitmapIndexClient
}

func (c *BitmapIndex) clientsSnapshot() []pb.BitmapIndexClient {
	c.Conn.nodeMapLock.RLock()
	defer c.Conn.nodeMapLock.RUnlock()

	clients := make([]pb.BitmapIndexClient, len(c.Conn.clientConn))
	for i, conn := range c.Conn.clientConn {
		clients[i] = pb.NewBitmapIndexClient(conn)
	}
	return clients
}

func (c *BitmapIndex) activeClientsSnapshot() []bitmapClientSnapshot {
	c.Conn.nodeMapLock.RLock()
	defer c.Conn.nodeMapLock.RUnlock()

	clients := make([]bitmapClientSnapshot, 0, len(c.Conn.clientConn))
	for i, conn := range c.Conn.clientConn {
		if c.Conn.ServicePort != 0 {
			if i >= len(c.Conn.ids) {
				continue
			}
			pbStat, found := c.Conn.nodeStatusMap.Load(c.Conn.ids[i])
			if !found {
				// Missing cached status during startup/refresh should not silently
				// narrow all-node fanout. Prefer a loud query error from an
				// unreachable node over a partial, incorrect answer.
				clients = append(clients, bitmapClientSnapshot{
					index:  i,
					client: pb.NewBitmapIndexClient(conn),
				})
				continue
			}
			status := pbStat.(*pb.StatusMessage)
			if status.NodeState != "Active" {
				continue
			}
		}
		clients = append(clients, bitmapClientSnapshot{
			index:  i,
			client: pb.NewBitmapIndexClient(conn),
		})
	}
	return clients
}

// MemberJoined - A new node joined the cluster.
func (c *BitmapIndex) MemberJoined(nodeID, ipAddress string, index int) {

	c.client = append(c.client, nil)
	copy(c.client[index+1:], c.client[index:])
	c.client[index] = pb.NewBitmapIndexClient(c.Conn.clientConn[index])
}

// MemberLeft - A node left the cluster.
func (c *BitmapIndex) MemberLeft(nodeID string, index int) {

	if len(c.client) <= 1 {
		c.client = make([]pb.BitmapIndexClient, 0)
		return
	}
	c.client = append(c.client[:index], c.client[index+1:]...)
}

// Client - Get a client by index.
func (c *BitmapIndex) Client(index int) pb.BitmapIndexClient {

	return c.client[index]
}

// BatchMutate - Send a batch of standard bitmap mutations to the server cluster for processing.
// Does this by calling BatchMutateNode in parallel for optimal throughput.
func (c *BitmapIndex) BatchMutate(batch map[string]map[string]map[uint64]map[int64]*Bitmap,
	clear bool) error {

	if c.local != nil {
		local, ok := c.local.(LocalBitmapIndexBatchService)
		if !ok {
			return fmt.Errorf("local BitmapIndex adapter does not support BatchMutate")
		}
		return c.batchMutateLocal(local, clear, batch)
	}

	clients, batches, err := c.splitBitmapBatch(batch)
	if err != nil {
		return err
	}
	var eg errgroup.Group

	for i, v := range batches {
		cl := clients[i]
		batch := v
		eg.Go(func() error {
			return c.BatchMutateNode(clear, cl, batch)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

// BatchMutateNode - Send batch to its respective node.
func (c *BitmapIndex) BatchMutateNode(clear bool, client pb.BitmapIndexClient,
	batch map[string]map[string]map[uint64]map[int64]*Bitmap) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	b := make([]*pb.IndexKVPair, 0)
	i := 0
	for indexName, index := range batch {
		for fieldName, field := range index {
			for rowID, ts := range field {
				for t, bitmap := range ts {
					buf, err := bitmap.Bits.ToBytes()
					if err != nil {
						u.Errorf("bitmap.Bits.ToBytes: %v", err)
						return err
					}
					ba := make([][]byte, 1)
					ba[0] = buf
					b = append(b, &pb.IndexKVPair{IndexPath: indexName + "/" + fieldName,
						Key: ToBytes(int64(rowID)), Value: ba, Time: t, IsClear: clear,
						IsUpdate: bitmap.IsUpdate})
					i++
					//u.Debug("Sent batch %d for path %s\n", i, b[i].IndexPath)
				}
			}
		}
	}
	stream, err := client.BatchMutate(ctx)

	if err != nil {
		u.Errorf("%v.BatchMutate(_) = _, %v: ", c.client, err)
		return fmt.Errorf("%v.BatchMutate(_) = _, %v: ", c.client, err)
	}

	for i := 0; i < len(b); i++ {
		if err := stream.Send(b[i]); err != nil {
			u.Errorf("%v.Send(%v) = %v", stream, b[i], err)
			return fmt.Errorf("%v.Send(%v) = %v", stream, b[i], err)
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		u.Errorf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
		return fmt.Errorf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
	}

	return err
}

func (c *BitmapIndex) batchMutateLocal(local LocalBitmapIndexBatchService, clear bool,
	batch map[string]map[string]map[uint64]map[int64]*Bitmap) error {

	kvs := make([]*pb.IndexKVPair, 0)
	for indexName, index := range batch {
		for fieldName, field := range index {
			for rowID, ts := range field {
				for t, bitmap := range ts {
					buf, err := bitmap.Bits.ToBytes()
					if err != nil {
						u.Errorf("bitmap.Bits.ToBytes: %v", err)
						return err
					}
					kvs = append(kvs, &pb.IndexKVPair{
						IndexPath: indexName + "/" + fieldName,
						Key:       ToBytes(int64(rowID)),
						Value:     [][]byte{buf},
						Time:      t,
						IsClear:   clear,
						IsUpdate:  bitmap.IsUpdate,
					})
				}
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	_, err := local.BatchMutate(ctx, kvs)
	return err
}

// splitBitmapBatch - For a given batch of standard bitmap mutations, separate them into
// sub-batches based upon a consistently hashed shard key so that they can be send to their
// respective nodes.  For standard bitmaps, this shard key consists of [index/field/rowid/timestamp].
// Special case for updates. Send a "clear" signal to non-targeted nodes for exclusive bitmap fields.
func (c *BitmapIndex) splitBitmapBatch(batch map[string]map[string]map[uint64]map[int64]*Bitmap,
) ([]pb.BitmapIndexClient, []map[string]map[string]map[uint64]map[int64]*Bitmap, error) {

	c.Conn.nodeMapLock.RLock()
	defer c.Conn.nodeMapLock.RUnlock()

	clients := make([]pb.BitmapIndexClient, len(c.Conn.clientConn))
	for i, conn := range c.Conn.clientConn {
		clients[i] = pb.NewBitmapIndexClient(conn)
	}
	if len(clients) == 0 {
		return nil, nil, fmt.Errorf("splitBitmapBatch: no bitmap clients available")
	}
	batches := make([]map[string]map[string]map[uint64]map[int64]*Bitmap, len(clients))
	for i := range batches {
		batches[i] = make(map[string]map[string]map[uint64]map[int64]*Bitmap)
	}

	for indexName, index := range batch {
		for fieldName, field := range index {
			for rowID, ts := range field {
				for t, bitmap := range ts {
					tm := time.Unix(0, t)
					opType := WriteIntent
					if bitmap.IsUpdate {
						opType = WriteIntentAll
					}
					indices, err := c.Conn.selectNodesLocked(fmt.Sprintf("%s/%s/%d/%s", indexName, fieldName,
						rowID, formatShardTime(tm)), opType)

					if err != nil {
						return nil, nil, fmt.Errorf("splitBitmapBatch: %v", err)
					}
					for _, i := range indices {
						if i < 0 || i >= len(batches) {
							return nil, nil, fmt.Errorf("splitBitmapBatch: selected node index %d outside client count %d", i, len(batches))
						}
						if batches[i] == nil {
							batches[i] = make(map[string]map[string]map[uint64]map[int64]*Bitmap)
						}
						if _, ok := batches[i][indexName]; !ok {
							batches[i][indexName] = make(map[string]map[uint64]map[int64]*Bitmap)
						}
						if _, ok := batches[i][indexName][fieldName]; !ok {
							batches[i][indexName][fieldName] = make(map[uint64]map[int64]*Bitmap)
						}
						if _, ok := batches[i][indexName][fieldName][rowID]; !ok {
							batches[i][indexName][fieldName][rowID] = make(map[int64]*Bitmap)
						}
						batches[i][indexName][fieldName][rowID][t] = bitmap
					}
				}
			}
		}
	}
	return clients, batches, nil
}

// BatchSetValue - Send a batch of BSI mutations to the server cluster for processing.
func (c *BitmapIndex) BatchSetValue(batch map[string]map[string]map[int64]*roaring64.BSI) error {
	_, err := c.BatchSetValueProfile(batch)
	return err
}

// BatchSetValueProfile sends a batch of BSI mutations and records client-side
// routing, encoding, and stream timings.
func (c *BitmapIndex) BatchSetValueProfile(batch map[string]map[string]map[int64]*roaring64.BSI,
) (profile BSIBatchSetValueProfile, err error) {

	profile = BSIBatchSetValueProfile{StartedAt: time.Now()}
	defer func() {
		profile.FinishedAt = time.Now()
		profile.TotalElapsed = profile.FinishedAt.Sub(profile.StartedAt)
	}()
	if len(batch) == 0 {
		return profile, nil
	}
	if c.local != nil {
		local, ok := c.local.(LocalBitmapIndexBatchService)
		if !ok {
			return profile, fmt.Errorf("local BitmapIndex adapter does not support BatchSetValue")
		}
		return c.batchSetValueLocalProfile(local, batch)
	}

	clients, batches, splitProfile, err := c.splitBSIItemBatchProfile(batch)
	if err != nil {
		return profile, err
	}
	profile.RouteElapsed = splitProfile.RouteElapsed
	profile.BuildElapsed = splitProfile.BuildElapsed
	profile.MarshalElapsed = splitProfile.MarshalElapsed
	profile.InputShardCount = splitProfile.InputShardCount
	profile.InputEntryCount = splitProfile.InputEntryCount
	profile.RoutedItemCount = splitProfile.RoutedItemCount

	var eg errgroup.Group
	var profileLock sync.Mutex
	putCalls := 0
	streamStart := time.Now()
	for i, v := range batches {
		if len(v) == 0 {
			continue
		}
		cl := clients[i]
		items := v
		putCalls++
		eg.Go(func() error {
			nodeProfile, err := c.batchMutateItemsNodeProfile(cl, items)
			profileLock.Lock()
			profile.StreamOpenElapsed += nodeProfile.OpenElapsed
			profile.StreamSendElapsed += nodeProfile.SendElapsed
			profile.StreamCloseElapsed += nodeProfile.CloseElapsed
			if nodeProfile.TotalElapsed > profile.StreamMaxElapsed {
				profile.StreamMaxElapsed = nodeProfile.TotalElapsed
			}
			profileLock.Unlock()
			return err
		})
	}
	profile.PutCalls = putCalls
	if err := eg.Wait(); err != nil {
		profile.StreamElapsed = time.Since(streamStart)
		return profile, err
	}
	profile.StreamElapsed = time.Since(streamStart)
	return profile, nil
}

func (c *BitmapIndex) splitBSIItemBatch(batch map[string]map[string]map[int64]*roaring64.BSI,
) ([]pb.BitmapIndexClient, [][]*pb.IndexKVPair, error) {

	clients, batches, _, err := c.splitBSIItemBatchProfile(batch)
	return clients, batches, err
}

func (c *BitmapIndex) splitBSIItemBatchProfile(batch map[string]map[string]map[int64]*roaring64.BSI,
) ([]pb.BitmapIndexClient, [][]*pb.IndexKVPair, BSIBatchSetValueProfile, error) {

	type routedBSIItem struct {
		routeKey string
		item     *pb.IndexKVPair
	}
	profile := BSIBatchSetValueProfile{}
	items := make([]routedBSIItem, 0)
	for indexName, index := range batch {
		for fieldName, field := range index {
			for t, bsi := range field {
				if bsi.GetCardinality() == 0 {
					u.Debugf("BSI for %s - %s is empty.", indexName, fieldName)
					continue
				}
				profile.InputShardCount++
				profile.InputEntryCount += int(bsi.GetCardinality())
				bitCount := bsi.BitCount()
				if bitCount == 0 {
					bitCount = 1
				}
				marshalStart := time.Now()
				ba, err := bsi.MarshalBinary()
				profile.MarshalElapsed += time.Since(marshalStart)
				if err != nil {
					u.Errorf("BSI.MarshalBinary: %v", err)
					return nil, nil, profile, err
				}
				tm := time.Unix(0, t)
				buildStart := time.Now()
				items = append(items, routedBSIItem{
					routeKey: fmt.Sprintf("%s/%s/%s", indexName, fieldName, formatShardTime(tm)),
					item: &pb.IndexKVPair{
						IndexPath: indexName + "/" + fieldName,
						Key:       ToBytes(int64(bitCount * -1)),
						Value:     ba,
						Time:      t,
					},
				})
				profile.BuildElapsed += time.Since(buildStart)
			}
		}
	}

	c.Conn.nodeMapLock.RLock()
	defer c.Conn.nodeMapLock.RUnlock()

	clients := make([]pb.BitmapIndexClient, len(c.Conn.clientConn))
	for i, conn := range c.Conn.clientConn {
		clients[i] = pb.NewBitmapIndexClient(conn)
	}
	if len(clients) == 0 {
		return nil, nil, profile, fmt.Errorf("splitBSIItemBatch: no bitmap clients available")
	}
	batches := make([][]*pb.IndexKVPair, len(clients))

	for _, routed := range items {
		routeStart := time.Now()
		indices, err := c.Conn.selectNodesLocked(routed.routeKey, WriteIntent)
		profile.RouteElapsed += time.Since(routeStart)
		if err != nil {
			return nil, nil, profile, fmt.Errorf("splitBSIItemBatch: %v", err)
		}
		for _, i := range indices {
			if i < 0 || i >= len(batches) {
				return nil, nil, profile, fmt.Errorf("splitBSIItemBatch: selected node index %d outside client count %d", i, len(batches))
			}
			buildStart := time.Now()
			batches[i] = append(batches[i], &pb.IndexKVPair{
				IndexPath: routed.item.IndexPath,
				Key:       routed.item.Key,
				Value:     routed.item.Value,
				Time:      routed.item.Time,
			})
			profile.RoutedItemCount++
			profile.BuildElapsed += time.Since(buildStart)
		}
	}
	return clients, batches, profile, nil
}

func (c *BitmapIndex) batchSetValueLocal(local LocalBitmapIndexBatchService,
	batch map[string]map[string]map[int64]*roaring64.BSI) error {

	_, err := c.batchSetValueLocalProfile(local, batch)
	return err
}

func (c *BitmapIndex) batchSetValueLocalProfile(local LocalBitmapIndexBatchService,
	batch map[string]map[string]map[int64]*roaring64.BSI) (profile BSIBatchSetValueProfile, err error) {

	profile = BSIBatchSetValueProfile{StartedAt: time.Now(), Local: true}
	defer func() {
		profile.FinishedAt = time.Now()
		profile.TotalElapsed = profile.FinishedAt.Sub(profile.StartedAt)
	}()
	kvs := make([]*pb.IndexKVPair, 0)
	for indexName, index := range batch {
		for fieldName, field := range index {
			for t, bsi := range field {
				if bsi.GetCardinality() == 0 {
					u.Debugf("BSI for %s - %s is empty.", indexName, fieldName)
					continue
				}
				profile.InputShardCount++
				profile.InputEntryCount += int(bsi.GetCardinality())
				bitCount := bsi.BitCount()
				if bitCount == 0 {
					bitCount = 1
				}
				marshalStart := time.Now()
				ba, err := bsi.MarshalBinary()
				profile.MarshalElapsed += time.Since(marshalStart)
				if err != nil {
					u.Errorf("BSI.MarshalBinary: %v", err)
					return profile, err
				}
				buildStart := time.Now()
				kvs = append(kvs, &pb.IndexKVPair{
					IndexPath: indexName + "/" + fieldName,
					Key:       ToBytes(int64(bitCount * -1)),
					Value:     ba,
					Time:      t,
				})
				profile.RoutedItemCount++
				profile.BuildElapsed += time.Since(buildStart)
			}
		}
	}
	if len(kvs) == 0 {
		return profile, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	streamStart := time.Now()
	_, err = local.BatchMutate(ctx, kvs)
	profile.StreamElapsed = time.Since(streamStart)
	profile.StreamMaxElapsed = profile.StreamElapsed
	profile.StreamCloseElapsed = profile.StreamElapsed
	if err != nil {
		return profile, err
	}
	profile.PutCalls = 1
	return profile, nil
}

// BatchSetValueNode - Send a batch of BSI values to a specific node.
func (c *BitmapIndex) BatchSetValueNode(client pb.BitmapIndexClient,
	batch map[string]map[string]map[int64]*roaring64.BSI) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	b := make([]*pb.IndexKVPair, 0)
	i := 0
	var err error
	for indexName, index := range batch {
		for fieldName, field := range index {
			for t, bsi := range field {
				if bsi.GetCardinality() == 0 {
					u.Debugf("BSI for %s - %s is empty.", indexName, fieldName)
					continue
				}
				bitCount := bsi.BitCount()
				if bitCount == 0 {
					bitCount = 1
				}
				ba, err := bsi.MarshalBinary()
				if err != nil {
					u.Errorf("BSI.MarshalBinary: %v", err)
					return err
				}
				b = append(b, &pb.IndexKVPair{IndexPath: indexName + "/" + fieldName,
					Key: ToBytes(int64(bitCount * -1)), Value: ba, Time: t})
				i++
				//u.Debugf("Sent batch %d for path %s\n", i, b[i].IndexPath)
			}
		}
	}
	stream, err := client.BatchMutate(ctx)
	if err != nil {
		u.Errorf("%v.BatchMutate(_) = _, %v: ", c.client, err)
		return fmt.Errorf("%v.BatchMutate(_) = _, %v: ", c.client, err)
	}

	for i := 0; i < len(b); i++ {
		if err := stream.Send(b[i]); err != nil {
			u.Errorf("%v.Send(%v) = %v", stream, b[i], err)
			return fmt.Errorf("%v.Send(%v) = %v", stream, b[i], err)
		}
	}
	_, err = stream.CloseAndRecv()
	if err != nil {
		u.Errorf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
		return fmt.Errorf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
	}
	return nil
}

// BatchMutateItemsNode sends pre-encoded bitmap mutation items to a node.
func (c *BitmapIndex) BatchMutateItemsNode(client pb.BitmapIndexClient, items []*pb.IndexKVPair) error {
	_, err := c.batchMutateItemsNodeProfile(client, items)
	return err
}

func (c *BitmapIndex) batchMutateItemsNodeProfile(client pb.BitmapIndexClient,
	items []*pb.IndexKVPair) (profile bitmapBatchMutateItemsNodeProfile, err error) {

	if len(items) == 0 {
		return profile, nil
	}
	profile = bitmapBatchMutateItemsNodeProfile{Items: len(items)}
	start := time.Now()
	defer func() {
		profile.TotalElapsed = time.Since(start)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	sendStart := time.Now()
	for startIndex := 0; startIndex < len(items); startIndex += bitmapBatchMutateItemsChunkSize {
		endIndex := startIndex + bitmapBatchMutateItemsChunkSize
		if endIndex > len(items) {
			endIndex = len(items)
		}
		if _, err := client.BatchMutateItems(ctx, &pb.IndexKVBatch{Items: items[startIndex:endIndex]}); err != nil {
			profile.SendElapsed = time.Since(sendStart)
			u.Errorf("%v.BatchMutateItems(_) = _, %v: ", c.client, err)
			return profile, fmt.Errorf("%v.BatchMutateItems(_) = _, %v: ", c.client, err)
		}
	}
	profile.SendElapsed = time.Since(sendStart)
	return profile, nil
}

// For a given batch of BSI mutations, separate them into sub-batches based upon
// a consistently hashed shard key so that they can be send to their respective nodes.
// For BSI fields, this shard key consists of [index/field/timestamp].  All BSI slices
// for a given field are co-located.
func (c *BitmapIndex) splitBSIBatch(batch map[string]map[string]map[int64]*roaring64.BSI,
) ([]pb.BitmapIndexClient, []map[string]map[string]map[int64]*roaring64.BSI, error) {

	c.Conn.nodeMapLock.RLock()
	defer c.Conn.nodeMapLock.RUnlock()

	clients := make([]pb.BitmapIndexClient, len(c.Conn.clientConn))
	for i, conn := range c.Conn.clientConn {
		clients[i] = pb.NewBitmapIndexClient(conn)
	}
	if len(clients) == 0 {
		return nil, nil, fmt.Errorf("splitBSIBatch: no bitmap clients available")
	}
	batches := make([]map[string]map[string]map[int64]*roaring64.BSI, len(clients))
	for i := range batches {
		batches[i] = make(map[string]map[string]map[int64]*roaring64.BSI)
	}

	for indexName, index := range batch {
		for fieldName, field := range index {
			for t, bsi := range field {
				tm := time.Unix(0, t)
				indices, err := c.Conn.selectNodesLocked(fmt.Sprintf("%s/%s/%s", indexName, fieldName, formatShardTime(tm)), WriteIntent)
				if err != nil {
					return nil, nil, fmt.Errorf("splitBSIBatch: %v", err)
				}
				for _, i := range indices {
					if i < 0 || i >= len(batches) {
						return nil, nil, fmt.Errorf("splitBSIBatch: selected node index %d outside client count %d", i, len(batches))
					}
					if batches[i] == nil {
						batches[i] = make(map[string]map[string]map[int64]*roaring64.BSI)
					}
					if _, ok := batches[i][indexName]; !ok {
						batches[i][indexName] = make(map[string]map[int64]*roaring64.BSI)
					}
					if _, ok := batches[i][indexName][fieldName]; !ok {
						batches[i][indexName][fieldName] = make(map[int64]*roaring64.BSI)
					}
					batches[i][indexName][fieldName][t] = bsi.Clone()
				}
			}
		}
	}
	return clients, batches, nil
}

// BulkClear - Send a resultset bitmap to all nodes and perform bulk clear operation.
func (c *BitmapIndex) BulkClear(index, fromTime, toTime string,
	foundSet *roaring64.Bitmap) error {

	data, err := foundSet.MarshalBinary()
	if err != nil {
		return err
	}

	req := &pb.BulkClearRequest{Index: index, FoundSet: data}

	if from, err := time.Parse(timeFmt, fromTime); err == nil {
		req.FromTime = from.UnixNano()
	} else {
		return err
	}
	if to, err := time.Parse(timeFmt, toTime); err == nil {
		req.ToTime = to.UnixNano()
	} else {
		return err
	}

	if c.local != nil {
		local, ok := c.local.(LocalBitmapIndexService)
		if !ok {
			return fmt.Errorf("local BitmapIndex adapter does not support BulkClear")
		}
		ctx, cancel := context.WithTimeout(context.Background(), Deadline)
		defer cancel()
		_, err := local.BulkClear(ctx, req)
		return err
	}

	var eg errgroup.Group

	// Send the same clear request to each node
	indices, err := c.SelectNodes(index, WriteIntentAll)
	if err != nil {
		return fmt.Errorf("BulkClear: %v", err)
	}
	for _, n := range indices {
		client := c.client[n]
		clientIndex := n
		eg.Go(func() error {
			if err := c.clearClient(client, req, clientIndex); err != nil {
				return err
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

// Send bulk clear request to all nodes.
func (c *BitmapIndex) clearClient(client pb.BitmapIndexClient, req *pb.BulkClearRequest,
	clientIndex int) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	if _, err := client.BulkClear(ctx, req); err != nil {
		return fmt.Errorf("%v.BulkClear(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return nil
}

// CheckoutSequence - Request a sequence generator from owning server node.
func (c *BitmapIndex) CheckoutSequence(indexName, pkField string, ts time.Time,
	reservationSize int) (*Sequencer, error) {

	req := &pb.CheckoutSequenceRequest{Index: indexName, PkField: pkField, Time: ts.UnixNano(),
		ReservationSize: uint32(reservationSize)}

	if c.local != nil {
		ctx, cancel := context.WithTimeout(context.Background(), Deadline)
		defer cancel()
		res, err := c.local.CheckoutSequence(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("CheckoutSequence: %v", err)
		}
		return NewSequencer(res.Start, int(res.Count)), nil
	}

	// We are checking out a sequence with the intent to write, but we only want the Active primary hence ReadIntent
	indices, err1 := c.SelectNodes(fmt.Sprintf("%s/%s/%s", indexName, pkField, formatShardTime(ts)), ReadIntent)
	if err1 != nil {
		return nil, fmt.Errorf("CheckoutSequence: %v", err1)
	}

	/*
	 * Make sure to target the node with the true maximum column ID for the table.
	 * If time quantums are enabled, then the PK must be a timestamp field.
	 * (Note: For compound keys this must be the first (leftmost) key).
	 * In this case, the timestamp is truncated with timeFmt and it's nano value is
	 * added to the sequence start value on the server and returned to the client..
	 */
	res, err := c.sequencerClient(c.client[indices[0]], req, indices[0])
	if err != nil {
		return nil, err
	}
	return NewSequencer(res.Start, int(res.Count)), nil
}

// Send projection processing request to a specific node.
func (c *BitmapIndex) sequencerClient(client pb.BitmapIndexClient, req *pb.CheckoutSequenceRequest,
	clientIndex int) (result *pb.CheckoutSequenceResponse, err error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	if result, err = client.CheckoutSequence(ctx, req); err != nil {
		return nil, fmt.Errorf("%v.CheckoutSequence(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return result, nil
}

// Projection - Send fields and target set for a given index to cluster for projection processing.
func (c *BitmapIndex) Projection(index string, fields []string, fromTime, toTime int64,
	foundSet *roaring64.Bitmap, negate bool) (map[string]*roaring64.BSI, map[string]map[uint64]*roaring64.Bitmap, error) {

	var data []byte
	if foundSet != nil {
		var err error
		data, err = foundSet.MarshalBinary()
		if err != nil {
			return nil, nil, err
		}
	}

	req := &pb.ProjectionRequest{Index: index, Fields: fields, FromTime: fromTime,
		ToTime: toTime, FoundSet: data, Negate: negate}

	if c.local != nil {
		ctx, cancel := context.WithTimeout(context.Background(), Deadline)
		defer cancel()
		response, err := c.local.Projection(ctx, req)
		if err != nil {
			return nil, nil, err
		}
		return aggregateProjectionResponses([]*pb.ProjectionResponse{response})
	}

	resultChan := make(chan *pb.ProjectionResponse, 100)
	var eg errgroup.Group

	// Send the same projection request to each readable node.
	indices, err2 := c.SelectNodes(index, ReadIntentAll)
	if err2 != nil {
		return nil, nil, fmt.Errorf("Projection: %v", err2)
	}
	for _, n := range indices {
		client := c.client[n]
		clientIndex := n
		eg.Go(func() error {
			pr, err := c.projectionClient(client, req, clientIndex)
			if err != nil {
				return err
			}
			resultChan <- pr
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, nil, err
	}
	close(resultChan)

	responses := make([]*pb.ProjectionResponse, 0)
	for rs := range resultChan {
		responses = append(responses, rs)
	}
	return aggregateProjectionResponses(responses)
}

// BSIDomainCardinality asks the owning readable node for BSI existence
// cardinality without returning BSI payload bytes. It is intentionally limited
// to a single exact shard/domain; callers that need broad ranges should use
// Projection until a range-aware metadata path exists.
func (c *BitmapIndex) BSIDomainCardinality(index, field string, fromTime, toTime int64) (uint64, error) {
	if index == "" {
		return 0, fmt.Errorf("index not specified for BSI domain cardinality")
	}
	if field == "" {
		return 0, fmt.Errorf("field not specified for BSI domain cardinality")
	}
	if fromTime != toTime {
		return 0, fmt.Errorf("BSI domain cardinality requires an exact shard/domain window")
	}

	req := &pb.SyncStatusRequest{
		Index:    index,
		Field:    field,
		Time:     fromTime,
		SendData: false,
	}
	if c.local != nil {
		ctx, cancel := context.WithTimeout(context.Background(), Deadline)
		defer cancel()
		response, err := c.local.SyncStatus(ctx, req)
		if err != nil {
			return 0, err
		}
		if response == nil {
			return 0, nil
		}
		return response.GetCardinality(), nil
	}

	routeKey := fmt.Sprintf("%s/%s/%s", index, field, formatShardTime(time.Unix(0, fromTime)))
	indices, err := c.SelectNodes(routeKey, ReadIntent)
	if err != nil {
		return 0, fmt.Errorf("BSIDomainCardinality: %v", err)
	}

	resultChan := make(chan uint64, len(indices))
	var eg errgroup.Group
	for _, n := range indices {
		if n < 0 || n >= len(c.client) {
			return 0, fmt.Errorf("BSIDomainCardinality: selected node index %d outside client count %d", n, len(c.client))
		}
		client := c.client[n]
		clientIndex := n
		eg.Go(func() error {
			response, err := c.syncStatusClient(client, req, clientIndex)
			if err != nil {
				return err
			}
			if response != nil {
				resultChan <- response.GetCardinality()
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return 0, err
	}
	close(resultChan)

	var cardinality uint64
	for partial := range resultChan {
		cardinality += partial
	}
	return cardinality, nil
}

// CompareBSIFields asks readable nodes to compare two BSI fields against their
// local shard fragments and returns the union of matching rownums.
func (c *BitmapIndex) CompareBSIFields(index, leftField, rightField string, fromTime, toTime int64,
	foundSet *roaring64.Bitmap, op roaring64.Operation, invert bool) (*roaring64.Bitmap, error) {
	result, _, err := c.CompareBSIFieldsWithStats(index, leftField, rightField, fromTime, toTime, foundSet, op, invert)
	return result, err
}

// CompareBSIFieldsWithStats asks readable nodes to compare two BSI fields and
// returns both the union of matching rownums and aggregated node-reported work.
func (c *BitmapIndex) CompareBSIFieldsWithStats(index, leftField, rightField string, fromTime, toTime int64,
	foundSet *roaring64.Bitmap, op roaring64.Operation, invert bool) (*roaring64.Bitmap, CompareBSIFieldsStats, error) {

	var data []byte
	if foundSet != nil {
		var err error
		data, err = foundSet.MarshalBinary()
		if err != nil {
			return nil, CompareBSIFieldsStats{}, err
		}
	}
	req := &pb.CompareBSIFieldsRequest{
		Index:      index,
		LeftField:  leftField,
		RightField: rightField,
		FromTime:   fromTime,
		ToTime:     toTime,
		FoundSet:   data,
		Operation:  int32(op),
		Invert:     invert,
	}
	if c.local != nil {
		ctx, cancel := context.WithTimeout(context.Background(), Deadline)
		defer cancel()
		response, err := c.local.CompareBSIFields(ctx, req)
		if err != nil {
			return nil, CompareBSIFieldsStats{}, err
		}
		return aggregateCompareBSIFieldsResponsesWithStats([]*pb.CompareBSIFieldsResponse{response})
	}

	clients := c.activeClientsSnapshot()
	if len(clients) == 0 {
		return nil, CompareBSIFieldsStats{}, fmt.Errorf("CompareBSIFields: no active bitmap nodes")
	}
	resultChan := make(chan *pb.CompareBSIFieldsResponse, len(clients))
	var eg errgroup.Group
	for _, n := range clients {
		client := n.client
		clientIndex := n.index
		eg.Go(func() error {
			response, err := c.compareBSIFieldsClient(client, req, clientIndex)
			if err != nil {
				return err
			}
			if os.Getenv("QUANTASTREAM_QUERY_FANOUT_DEBUG") != "" {
				fmt.Fprintf(os.Stderr, "COMPARE_BSI_FANOUT_RESULT index=%s node=%d %s\n", index, clientIndex, compareBSIFieldsResponseCardinalityDebug(response))
			}
			resultChan <- response
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, CompareBSIFieldsStats{}, err
	}
	close(resultChan)

	responses := make([]*pb.CompareBSIFieldsResponse, 0)
	for response := range resultChan {
		responses = append(responses, response)
	}
	return aggregateCompareBSIFieldsResponsesWithStats(responses)
}

// BitmapGroupAggregates asks readable nodes to compute node-local grouped
// aggregate partials and merges them by raw group value IDs.
func (c *BitmapIndex) BitmapGroupAggregates(index string, groupFields []string, aggregates []BitmapGroupAggregateSpec, fromTime, toTime int64, foundSet *roaring64.Bitmap) ([]BitmapGroupAggregateGroup, BitmapGroupAggregateStats, bool, error) {
	if index == "" {
		return nil, BitmapGroupAggregateStats{}, false, fmt.Errorf("BitmapGroupAggregates: index not specified")
	}
	if len(groupFields) == 0 {
		return nil, BitmapGroupAggregateStats{}, false, fmt.Errorf("BitmapGroupAggregates: group fields not specified")
	}
	if len(aggregates) == 0 {
		return nil, BitmapGroupAggregateStats{}, false, fmt.Errorf("BitmapGroupAggregates: aggregates not specified")
	}
	var foundSetData []byte
	candidateRows := uint64(0)
	if foundSet != nil {
		candidateRows = foundSet.GetCardinality()
		var err error
		foundSetData, err = foundSet.MarshalBinary()
		if err != nil {
			return nil, BitmapGroupAggregateStats{}, false, err
		}
	}
	protoAggregates := make([]*pb.BitmapGroupAggregateSpec, 0, len(aggregates))
	for _, aggregate := range aggregates {
		protoAggregates = append(protoAggregates, &pb.BitmapGroupAggregateSpec{
			Function: aggregate.Function,
			Field:    aggregate.Field,
		})
	}
	req := &pb.BitmapGroupAggregatesRequest{
		Index:       index,
		GroupFields: append([]string(nil), groupFields...),
		Aggregates:  protoAggregates,
		FromTime:    fromTime,
		ToTime:      toTime,
		FoundSet:    foundSetData,
	}
	if c.local != nil {
		ctx, cancel := context.WithTimeout(context.Background(), Deadline)
		defer cancel()
		response, err := c.local.BitmapGroupAggregates(ctx, req)
		if err != nil {
			return nil, BitmapGroupAggregateStats{}, false, err
		}
		return aggregateBitmapGroupAggregateResponses([]*pb.BitmapGroupAggregatesResponse{response}, candidateRows, len(groupFields), len(aggregates))
	}

	clients := c.activeClientsSnapshot()
	if len(clients) == 0 {
		return nil, BitmapGroupAggregateStats{}, false, fmt.Errorf("BitmapGroupAggregates: no active bitmap nodes")
	}
	resultChan := make(chan *pb.BitmapGroupAggregatesResponse, len(clients))
	var eg errgroup.Group
	for _, n := range clients {
		client := n.client
		clientIndex := n.index
		eg.Go(func() error {
			response, err := c.bitmapGroupAggregatesClient(client, req, clientIndex)
			if err != nil {
				return err
			}
			resultChan <- response
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, BitmapGroupAggregateStats{}, false, err
	}
	close(resultChan)

	responses := make([]*pb.BitmapGroupAggregatesResponse, 0, len(clients))
	for response := range resultChan {
		responses = append(responses, response)
	}
	return aggregateBitmapGroupAggregateResponses(responses, candidateRows, len(groupFields), len(aggregates))
}

// RelationshipReverseArtifactCandidateValues asks readable nodes for child rows
// keyed by a maintained parent-to-child reverse relationship artifact.
func (c *BitmapIndex) RelationshipReverseArtifactCandidateValues(index, field string, sourceValues []int64) ([]uint64, map[uint64]int64, RelationshipReverseArtifactStats, bool, error) {
	return c.RelationshipReverseArtifactCandidateValuesForRows(index, field, sourceValues, nil)
}

// RelationshipReverseArtifactCandidateValuesForRows asks readable nodes for
// child rows keyed by a maintained parent-to-child reverse relationship
// artifact, optionally retaining only a caller-supplied child row set.
func (c *BitmapIndex) RelationshipReverseArtifactCandidateValuesForRows(index, field string, sourceValues []int64, candidateRows []uint64) ([]uint64, map[uint64]int64, RelationshipReverseArtifactStats, bool, error) {
	if index == "" {
		return nil, nil, RelationshipReverseArtifactStats{}, false, fmt.Errorf("RelationshipReverseArtifactCandidateValues: index not specified")
	}
	if field == "" {
		return nil, nil, RelationshipReverseArtifactStats{}, false, fmt.Errorf("RelationshipReverseArtifactCandidateValues: field not specified")
	}
	req := &pb.RelationshipReverseArtifactCandidatesRequest{
		Index:         index,
		Field:         field,
		SourceValues:  append([]int64(nil), sourceValues...),
		CandidateRows: append([]uint64(nil), candidateRows...),
	}
	if c.local != nil {
		ctx, cancel := context.WithTimeout(context.Background(), Deadline)
		defer cancel()
		callStart := time.Now()
		response, err := c.local.RelationshipReverseArtifactCandidates(ctx, req)
		callElapsed := time.Since(callStart)
		if err != nil {
			return nil, nil, RelationshipReverseArtifactStats{}, false, err
		}
		mergeStart := time.Now()
		rownums, parentValueByChild, stats, ok, err := aggregateRelationshipReverseArtifactCandidateResponses([]*pb.RelationshipReverseArtifactCandidatesResponse{response}, sourceValues)
		stats.FanoutElapsed = callElapsed
		stats.ClientRPCElapsed = callElapsed
		stats.MaxClientRPCElapsed = callElapsed
		stats.ResponseMergeElapsed = time.Since(mergeStart)
		return rownums, parentValueByChild, stats, ok, err
	}

	clients := c.activeClientsSnapshot()
	if len(clients) == 0 {
		return nil, nil, RelationshipReverseArtifactStats{}, false, fmt.Errorf("RelationshipReverseArtifactCandidateValues: no active bitmap nodes")
	}
	type candidateResult struct {
		response *pb.RelationshipReverseArtifactCandidatesResponse
		elapsed  time.Duration
	}
	resultChan := make(chan candidateResult, len(clients))
	var eg errgroup.Group
	fanoutStart := time.Now()
	for _, n := range clients {
		client := n.client
		clientIndex := n.index
		eg.Go(func() error {
			response, elapsed, err := c.relationshipReverseArtifactCandidatesClient(client, req, clientIndex)
			if err != nil {
				return err
			}
			resultChan <- candidateResult{response: response, elapsed: elapsed}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, nil, RelationshipReverseArtifactStats{}, false, err
	}
	fanoutElapsed := time.Since(fanoutStart)
	close(resultChan)

	responses := make([]*pb.RelationshipReverseArtifactCandidatesResponse, 0, len(clients))
	clientRPCElapsed := time.Duration(0)
	maxClientRPCElapsed := time.Duration(0)
	for result := range resultChan {
		responses = append(responses, result.response)
		clientRPCElapsed += result.elapsed
		if result.elapsed > maxClientRPCElapsed {
			maxClientRPCElapsed = result.elapsed
		}
	}
	mergeStart := time.Now()
	rownums, parentValueByChild, stats, ok, err := aggregateRelationshipReverseArtifactCandidateResponses(responses, sourceValues)
	stats.FanoutElapsed = fanoutElapsed
	stats.ClientRPCElapsed = clientRPCElapsed
	stats.MaxClientRPCElapsed = maxClientRPCElapsed
	stats.ResponseMergeElapsed = time.Since(mergeStart)
	return rownums, parentValueByChild, stats, ok, err
}

// RelationshipReverseArtifactStats asks readable nodes for reverse-artifact
// cardinality without materializing candidate rows.
func (c *BitmapIndex) RelationshipReverseArtifactStats(index, field string) (RelationshipReverseArtifactStats, bool, error) {
	if index == "" {
		return RelationshipReverseArtifactStats{}, false, fmt.Errorf("RelationshipReverseArtifactStats: index not specified")
	}
	if field == "" {
		return RelationshipReverseArtifactStats{}, false, fmt.Errorf("RelationshipReverseArtifactStats: field not specified")
	}
	req := &pb.RelationshipReverseArtifactStatsRequest{Index: index, Field: field}
	if c.local != nil {
		ctx, cancel := context.WithTimeout(context.Background(), Deadline)
		defer cancel()
		response, err := c.local.RelationshipReverseArtifactStats(ctx, req)
		if err != nil {
			return RelationshipReverseArtifactStats{}, false, err
		}
		return aggregateRelationshipReverseArtifactStatsResponses([]*pb.RelationshipReverseArtifactStatsResponse{response})
	}

	clients := c.activeClientsSnapshot()
	if len(clients) == 0 {
		return RelationshipReverseArtifactStats{}, false, fmt.Errorf("RelationshipReverseArtifactStats: no active bitmap nodes")
	}
	resultChan := make(chan *pb.RelationshipReverseArtifactStatsResponse, len(clients))
	var eg errgroup.Group
	for _, n := range clients {
		client := n.client
		clientIndex := n.index
		eg.Go(func() error {
			response, err := c.relationshipReverseArtifactStatsClient(client, req, clientIndex)
			if err != nil {
				return err
			}
			resultChan <- response
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return RelationshipReverseArtifactStats{}, false, err
	}
	close(resultChan)

	responses := make([]*pb.RelationshipReverseArtifactStatsResponse, 0, len(clients))
	for response := range resultChan {
		responses = append(responses, response)
	}
	return aggregateRelationshipReverseArtifactStatsResponses(responses)
}

// RelationshipAlignedValueSum asks readable nodes to aggregate child-domain BSI
// values by caller-supplied parent row IDs. The input row slices are already
// relationship-aligned; node partials are associative by parent value.
func (c *BitmapIndex) RelationshipAlignedValueSum(index, valueField string, fromTime, toTime int64, childRows, parentRows []uint64) ([]RelationshipAlignedValueSumGroup, RelationshipAlignedValueSumStats, bool, error) {
	if index == "" {
		return nil, RelationshipAlignedValueSumStats{}, false, fmt.Errorf("RelationshipAlignedValueSum: index not specified")
	}
	if valueField == "" {
		return nil, RelationshipAlignedValueSumStats{}, false, fmt.Errorf("RelationshipAlignedValueSum: value field not specified")
	}
	if len(childRows) != len(parentRows) {
		return nil, RelationshipAlignedValueSumStats{}, false, fmt.Errorf("RelationshipAlignedValueSum: childRows and parentRows have different lengths")
	}
	req := &pb.RelationshipAlignedValueSumRequest{
		Index:      index,
		ValueField: valueField,
		FromTime:   fromTime,
		ToTime:     toTime,
		ChildRows:  append([]uint64(nil), childRows...),
		ParentRows: append([]uint64(nil), parentRows...),
	}
	if c.local != nil {
		ctx, cancel := context.WithTimeout(context.Background(), Deadline)
		defer cancel()
		response, err := c.local.RelationshipAlignedValueSum(ctx, req)
		if err != nil {
			return nil, RelationshipAlignedValueSumStats{}, false, err
		}
		return aggregateRelationshipAlignedValueSumResponses([]*pb.RelationshipAlignedValueSumResponse{response}, childRows, parentRows)
	}

	clients := c.activeClientsSnapshot()
	if len(clients) == 0 {
		return nil, RelationshipAlignedValueSumStats{}, false, fmt.Errorf("RelationshipAlignedValueSum: no active bitmap nodes")
	}
	resultChan := make(chan *pb.RelationshipAlignedValueSumResponse, len(clients))
	var eg errgroup.Group
	for _, n := range clients {
		client := n.client
		clientIndex := n.index
		eg.Go(func() error {
			response, err := c.relationshipAlignedValueSumClient(client, req, clientIndex)
			if err != nil {
				return err
			}
			resultChan <- response
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, RelationshipAlignedValueSumStats{}, false, err
	}
	close(resultChan)

	responses := make([]*pb.RelationshipAlignedValueSumResponse, 0, len(clients))
	for response := range resultChan {
		responses = append(responses, response)
	}
	return aggregateRelationshipAlignedValueSumResponses(responses, childRows, parentRows)
}

func aggregateRelationshipReverseArtifactCandidateResponses(responses []*pb.RelationshipReverseArtifactCandidatesResponse, sourceValues []int64) ([]uint64, map[uint64]int64, RelationshipReverseArtifactStats, bool, error) {
	stats := RelationshipReverseArtifactStats{
		SourceValues: relationshipReverseArtifactUniqueInt64Count(sourceValues),
	}
	parentValueByChild := make(map[uint64]int64)
	seenRows := make(map[uint64]struct{})
	rownums := make([]uint64, 0)
	ok := len(responses) > 0
	for _, response := range responses {
		if response == nil {
			ok = false
			continue
		}
		if !response.GetOk() {
			ok = false
		}
		stats.addProto(response.GetStats())
		for _, rownum := range response.GetRownums() {
			if _, seen := seenRows[rownum]; seen {
				continue
			}
			seenRows[rownum] = struct{}{}
			rownums = append(rownums, rownum)
		}
		for _, value := range response.GetParentValues() {
			if value == nil {
				continue
			}
			parentValueByChild[value.GetRownum()] = value.GetParentValue()
		}
	}
	sort.Slice(rownums, func(i, j int) bool { return rownums[i] < rownums[j] })
	stats.TargetRows = uint64(len(rownums))
	return rownums, parentValueByChild, stats, ok, nil
}

func aggregateRelationshipReverseArtifactStatsResponses(responses []*pb.RelationshipReverseArtifactStatsResponse) (RelationshipReverseArtifactStats, bool, error) {
	var stats RelationshipReverseArtifactStats
	ok := len(responses) > 0
	for _, response := range responses {
		if response == nil {
			ok = false
			continue
		}
		if !response.GetOk() {
			ok = false
		}
		stats.addProto(response.GetStats())
	}
	return stats, ok, nil
}

func aggregateRelationshipAlignedValueSumResponses(responses []*pb.RelationshipAlignedValueSumResponse, childRows, parentRows []uint64) ([]RelationshipAlignedValueSumGroup, RelationshipAlignedValueSumStats, bool, error) {
	stats := RelationshipAlignedValueSumStats{
		Rows:         uint64(len(childRows)),
		SourceValues: relationshipAlignedValueSumUniqueCount(parentRows),
	}
	groupsByParent := make(map[uint64]*RelationshipAlignedValueSumGroup)
	ok := len(responses) > 0
	for _, response := range responses {
		if response == nil {
			ok = false
			continue
		}
		if !response.GetOk() {
			ok = false
		}
		stats.addProto(response.GetStats())
		for _, protoGroup := range response.GetGroups() {
			if protoGroup == nil || protoGroup.GetCount() == 0 {
				continue
			}
			sum := new(big.Int)
			if protoGroup.GetSum() == "" {
				sum.SetInt64(0)
			} else if _, parsed := sum.SetString(protoGroup.GetSum(), 10); !parsed {
				return nil, RelationshipAlignedValueSumStats{}, false, fmt.Errorf("RelationshipAlignedValueSum: could not parse sum %q", protoGroup.GetSum())
			}
			parent := protoGroup.GetParentValue()
			group := groupsByParent[parent]
			if group == nil {
				group = &RelationshipAlignedValueSumGroup{
					ParentValue:       parent,
					RepresentativeRow: protoGroup.GetRepresentativeRow(),
					Sum:               big.NewInt(0),
				}
				groupsByParent[parent] = group
			}
			if group.RepresentativeRow == 0 {
				group.RepresentativeRow = protoGroup.GetRepresentativeRow()
			}
			group.Count += protoGroup.GetCount()
			group.Sum.Add(group.Sum, sum)
		}
	}
	groups := relationshipAlignedValueSumSortedGroups(groupsByParent)
	stats.Groups = len(groups)
	stats.Values = uint64(len(groups))
	stats.TargetRows = 0
	for _, group := range groups {
		stats.TargetRows += group.Count
	}
	return groups, stats, ok, nil
}

func aggregateBitmapGroupAggregateResponses(responses []*pb.BitmapGroupAggregatesResponse, candidateRows uint64, fieldCount, aggregateCount int) ([]BitmapGroupAggregateGroup, BitmapGroupAggregateStats, bool, error) {
	stats := BitmapGroupAggregateStats{
		CandidateRows:  candidateRows,
		FieldCount:     fieldCount,
		AggregateCount: aggregateCount,
	}
	groupsByKey := make(map[string]*BitmapGroupAggregateGroup)
	ok := len(responses) > 0
	for _, response := range responses {
		if response == nil {
			ok = false
			continue
		}
		if !response.GetOk() {
			ok = false
		}
		stats.addProto(response.GetStats())
		for _, protoGroup := range response.GetGroups() {
			if protoGroup == nil {
				continue
			}
			if len(protoGroup.GetValues()) != fieldCount || len(protoGroup.GetAggs()) != aggregateCount {
				return nil, BitmapGroupAggregateStats{}, false, fmt.Errorf("BitmapGroupAggregates: mismatched group width values=%d aggs=%d want %d/%d", len(protoGroup.GetValues()), len(protoGroup.GetAggs()), fieldCount, aggregateCount)
			}
			key := bitmapGroupAggregateKey(protoGroup.GetValues())
			group := groupsByKey[key]
			if group == nil {
				group = &BitmapGroupAggregateGroup{
					Values: append([]uint64(nil), protoGroup.GetValues()...),
					Aggs:   make([]BitmapGroupAggregateValue, aggregateCount),
				}
				groupsByKey[key] = group
			}
			for i, protoValue := range protoGroup.GetAggs() {
				value, err := bitmapGroupAggregateValueFromProto(protoValue)
				if err != nil {
					return nil, BitmapGroupAggregateStats{}, false, err
				}
				bitmapGroupAggregateMergeValue(&group.Aggs[i], value)
			}
		}
	}
	groups := bitmapGroupAggregateSortedGroups(groupsByKey)
	stats.Groups = len(groups)
	return groups, stats, ok, nil
}

func bitmapGroupAggregateKey(values []uint64) string {
	if len(values) == 0 {
		return ""
	}
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = strconv.FormatUint(value, 10)
	}
	return strings.Join(parts, "\x00")
}

func bitmapGroupAggregateValueFromProto(value *pb.BitmapGroupAggregateValue) (BitmapGroupAggregateValue, error) {
	if value == nil {
		return BitmapGroupAggregateValue{}, nil
	}
	result := BitmapGroupAggregateValue{Count: value.GetCount()}
	var err error
	if result.Sum, err = bitmapGroupAggregateParseBigInt(value.GetSum(), "sum"); err != nil {
		return BitmapGroupAggregateValue{}, err
	}
	if result.Min, err = bitmapGroupAggregateParseBigInt(value.GetMin(), "min"); err != nil {
		return BitmapGroupAggregateValue{}, err
	}
	if result.Max, err = bitmapGroupAggregateParseBigInt(value.GetMax(), "max"); err != nil {
		return BitmapGroupAggregateValue{}, err
	}
	return result, nil
}

func bitmapGroupAggregateParseBigInt(raw, label string) (*big.Int, error) {
	if raw == "" {
		return nil, nil
	}
	value := new(big.Int)
	if _, ok := value.SetString(raw, 10); !ok {
		return nil, fmt.Errorf("BitmapGroupAggregates: could not parse %s value %q", label, raw)
	}
	return value, nil
}

func bitmapGroupAggregateMergeValue(target *BitmapGroupAggregateValue, partial BitmapGroupAggregateValue) {
	if target == nil {
		return
	}
	target.Count += partial.Count
	if partial.Sum != nil {
		if target.Sum == nil {
			target.Sum = big.NewInt(0)
		}
		target.Sum.Add(target.Sum, partial.Sum)
	}
	if partial.Min != nil && (target.Min == nil || partial.Min.Cmp(target.Min) < 0) {
		target.Min = new(big.Int).Set(partial.Min)
	}
	if partial.Max != nil && (target.Max == nil || partial.Max.Cmp(target.Max) > 0) {
		target.Max = new(big.Int).Set(partial.Max)
	}
}

func bitmapGroupAggregateSortedGroups(groupsByKey map[string]*BitmapGroupAggregateGroup) []BitmapGroupAggregateGroup {
	if len(groupsByKey) == 0 {
		return nil
	}
	keys := make([]string, 0, len(groupsByKey))
	for key := range groupsByKey {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		left := groupsByKey[keys[i]]
		right := groupsByKey[keys[j]]
		if left == nil || right == nil {
			return keys[i] < keys[j]
		}
		return bitmapGroupAggregateValuesLess(left.Values, right.Values)
	})
	groups := make([]BitmapGroupAggregateGroup, 0, len(keys))
	for _, key := range keys {
		group := groupsByKey[key]
		if group == nil {
			continue
		}
		cloned := BitmapGroupAggregateGroup{
			Values: append([]uint64(nil), group.Values...),
			Aggs:   make([]BitmapGroupAggregateValue, len(group.Aggs)),
		}
		for i, value := range group.Aggs {
			cloned.Aggs[i] = BitmapGroupAggregateValue{
				Count: value.Count,
				Sum:   cloneBigIntShared(value.Sum),
				Min:   cloneBigIntShared(value.Min),
				Max:   cloneBigIntShared(value.Max),
			}
		}
		groups = append(groups, cloned)
	}
	return groups
}

func bitmapGroupAggregateValuesLess(left, right []uint64) bool {
	for i := 0; i < len(left) && i < len(right); i++ {
		if left[i] == right[i] {
			continue
		}
		return left[i] < right[i]
	}
	return len(left) < len(right)
}

func cloneBigIntShared(value *big.Int) *big.Int {
	if value == nil {
		return nil
	}
	return new(big.Int).Set(value)
}

func relationshipAlignedValueSumSortedGroups(groupsByParent map[uint64]*RelationshipAlignedValueSumGroup) []RelationshipAlignedValueSumGroup {
	if len(groupsByParent) == 0 {
		return nil
	}
	keys := make([]uint64, 0, len(groupsByParent))
	for key := range groupsByParent {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	groups := make([]RelationshipAlignedValueSumGroup, 0, len(keys))
	for _, key := range keys {
		group := groupsByParent[key]
		if group == nil || group.Count == 0 {
			continue
		}
		groups = append(groups, RelationshipAlignedValueSumGroup{
			ParentValue:       group.ParentValue,
			RepresentativeRow: group.RepresentativeRow,
			Count:             group.Count,
			Sum:               new(big.Int).Set(group.Sum),
		})
	}
	return groups
}

func relationshipAlignedValueSumUniqueCount(values []uint64) int {
	if len(values) == 0 {
		return 0
	}
	seen := make(map[uint64]struct{}, len(values))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	return len(seen)
}

func relationshipReverseArtifactUniqueInt64Count(values []int64) int {
	if len(values) == 0 {
		return 0
	}
	seen := make(map[int64]struct{}, len(values))
	for _, value := range values {
		seen[value] = struct{}{}
	}
	return len(seen)
}

func (s *RelationshipReverseArtifactStats) addProto(stats *pb.RelationshipReverseArtifactStats) {
	if s == nil || stats == nil {
		return
	}
	s.Nodes++
	s.Rows += stats.GetRows()
	s.Values += stats.GetValues()
	s.TargetRows += stats.GetTargetRows()
	s.LookupElapsed += time.Duration(stats.GetLookupElapsedNanos())
}

func (s *RelationshipAlignedValueSumStats) addProto(stats *pb.RelationshipAlignedValueSumStats) {
	if s == nil || stats == nil {
		return
	}
	s.Nodes++
	s.LookupElapsed += time.Duration(stats.GetLookupElapsedNanos())
	s.ProjectionElapsed += time.Duration(stats.GetProjectionElapsedNanos())
	s.AggregateElapsed += time.Duration(stats.GetAggregateElapsedNanos())
	s.Projection.addProto(stats.GetProjection())
}

func (s *BitmapGroupAggregateStats) addProto(stats *pb.BitmapGroupAggregateStats) {
	if s == nil || stats == nil {
		return
	}
	s.Nodes++
	if valueCount := int(stats.GetValueCount()); valueCount > s.ValueCount {
		s.ValueCount = valueCount
	}
	s.BSIFieldCount += int(stats.GetBsiFieldCount())
	s.BSIProjectElapsed += time.Duration(stats.GetBsiProjectElapsedNanos())
	s.AggregateElapsed += time.Duration(stats.GetAggregateElapsedNanos())
	s.ValueSetElapsed += time.Duration(stats.GetValueSetElapsedNanos())
	s.SumElapsed += time.Duration(stats.GetSumElapsedNanos())
	s.MinMaxElapsed += time.Duration(stats.GetMinMaxElapsedNanos())
}

func aggregateCompareBSIFieldsResponses(responses []*pb.CompareBSIFieldsResponse) (*roaring64.Bitmap, error) {
	result, _, err := aggregateCompareBSIFieldsResponsesWithStats(responses)
	return result, err
}

func aggregateCompareBSIFieldsResponsesWithStats(responses []*pb.CompareBSIFieldsResponse) (*roaring64.Bitmap, CompareBSIFieldsStats, error) {
	bitmaps := make([]*roaring64.Bitmap, 0, len(responses))
	stats := CompareBSIFieldsStats{}
	for _, response := range responses {
		bitmap := roaring64.NewBitmap()
		if response != nil && len(response.GetRownums()) > 0 {
			if err := bitmap.UnmarshalBinary(response.GetRownums()); err != nil {
				return nil, CompareBSIFieldsStats{}, fmt.Errorf("unmarshalling BSI comparison rownums - %v", err)
			}
		}
		bitmaps = append(bitmaps, bitmap)
		stats.addProto(response.GetStats())
	}
	result := roaring64.ParOr(0, bitmaps...)
	if result != nil {
		stats.OutputRows = result.GetCardinality()
	}
	return result, stats, nil
}

func (s *CompareBSIFieldsStats) addProto(stats *pb.CompareBSIFieldsStats) {
	if s == nil || stats == nil {
		return
	}
	s.Nodes++
	s.Left.addProto(stats.GetLeft())
	s.Right.addProto(stats.GetRight())
	s.CompareElapsed += time.Duration(stats.GetCompareElapsedNanos())
}

func (s *CompareBSIFieldsProjectionStats) addProto(stats *pb.BSIProjectionStats) {
	if s == nil || stats == nil {
		return
	}
	s.ShardsVisited += stats.GetShardsVisited()
	s.ShardsInWindow += stats.GetShardsInWindow()
	s.ShardsLocal += stats.GetShardsLocal()
	s.ShardsRetained += stats.GetShardsRetained()
	s.RetainedRows += stats.GetRetainedRows()
	s.RetainBypassRows += stats.GetRetainBypassRows()
	s.RetainElapsed += time.Duration(stats.GetRetainElapsedNanos())
	s.ValueElapsed += time.Duration(stats.GetValueElapsedNanos())
	s.MergeElapsed += time.Duration(stats.GetMergeElapsedNanos())
}

func compareBSIFieldsResponseCardinalityDebug(response *pb.CompareBSIFieldsResponse) string {
	if response == nil {
		return "nil_result=true"
	}
	bitmap := roaring64.NewBitmap()
	if len(response.GetRownums()) > 0 {
		_ = bitmap.UnmarshalBinary(response.GetRownums())
	}
	return fmt.Sprintf("rownums=%d", bitmap.GetCardinality())
}

func aggregateProjectionResponses(responses []*pb.ProjectionResponse) (map[string]*roaring64.BSI, map[string]map[uint64]*roaring64.Bitmap, error) {
	bsiResults := make(map[string][]*roaring64.BSI, 0)
	bitmapResults := make(map[string]map[uint64][]*roaring64.Bitmap, 0)
	for _, rs := range responses {
		for _, v := range rs.GetBsiResults() {
			bsi, ok := bsiResults[v.Field]
			if !ok {
				bsi = make([]*roaring64.BSI, 0)
			}

			newBsi := roaring64.NewDefaultBSI()
			if err := newBsi.UnmarshalBinary(v.Bitmaps); err != nil {
				return nil, nil, fmt.Errorf("unmarshalling BSI projection results - %v", err)
			}
			bsiResults[v.Field] = append(bsi, newBsi)
		}
		for _, v := range rs.GetBitmapResults() {
			if _, ok := bitmapResults[v.Field]; !ok {
				bitmapResults[v.Field] = make(map[uint64][]*roaring64.Bitmap, 0)
			}
			field := bitmapResults[v.Field]
			bm, ok := field[v.RowId]
			if !ok {
				bm = make([]*roaring64.Bitmap, 0)
			}
			newBm := roaring64.NewBitmap()
			if err := newBm.UnmarshalBinary(v.Bitmap); err != nil {
				return nil, nil, fmt.Errorf("unmarshalling bitmap projection results - %v", err)
			}
			field[v.RowId] = append(bm, newBm)
		}
	}

	// Aggregate the per node results
	aggbsiResults := make(map[string]*roaring64.BSI)
	for k, v := range bsiResults {
		bsi := roaring64.NewDefaultBSI()
		//bsi.ParOr(0, v...)
		for _, z := range v {
			bsi.ParOr(0, z)
		}
		aggbsiResults[k] = bsi
	}
	aggbitmapResults := make(map[string]map[uint64]*roaring64.Bitmap)
	for k, v := range bitmapResults {
		aggbitmapResults[k] = make(map[uint64]*roaring64.Bitmap)
		for k2, v2 := range v {
			aggbitmapResults[k][k2] = roaring64.ParOr(0, v2...)
		}
	}
	return aggbsiResults, aggbitmapResults, nil
}

// Send projection processing request to a specific node.
func (c *BitmapIndex) projectionClient(client pb.BitmapIndexClient, req *pb.ProjectionRequest,
	clientIndex int) (*pb.ProjectionResponse, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	result, err := client.Projection(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("%v.Projection(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return result, nil
}

func (c *BitmapIndex) compareBSIFieldsClient(client pb.BitmapIndexClient, req *pb.CompareBSIFieldsRequest,
	clientIndex int) (*pb.CompareBSIFieldsResponse, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	result, err := client.CompareBSIFields(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("%v.CompareBSIFields(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return result, nil
}

func (c *BitmapIndex) relationshipReverseArtifactCandidatesClient(client pb.BitmapIndexClient, req *pb.RelationshipReverseArtifactCandidatesRequest,
	clientIndex int) (*pb.RelationshipReverseArtifactCandidatesResponse, time.Duration, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	start := time.Now()
	result, err := client.RelationshipReverseArtifactCandidates(ctx, req)
	elapsed := time.Since(start)
	if err != nil {
		return nil, elapsed, fmt.Errorf("%v.RelationshipReverseArtifactCandidates(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return result, elapsed, nil
}

func (c *BitmapIndex) relationshipReverseArtifactStatsClient(client pb.BitmapIndexClient, req *pb.RelationshipReverseArtifactStatsRequest,
	clientIndex int) (*pb.RelationshipReverseArtifactStatsResponse, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	result, err := client.RelationshipReverseArtifactStats(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("%v.RelationshipReverseArtifactStats(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return result, nil
}

func (c *BitmapIndex) relationshipAlignedValueSumClient(client pb.BitmapIndexClient, req *pb.RelationshipAlignedValueSumRequest,
	clientIndex int) (*pb.RelationshipAlignedValueSumResponse, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	result, err := client.RelationshipAlignedValueSum(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("%v.RelationshipAlignedValueSum(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return result, nil
}

func (c *BitmapIndex) bitmapGroupAggregatesClient(client pb.BitmapIndexClient, req *pb.BitmapGroupAggregatesRequest,
	clientIndex int) (*pb.BitmapGroupAggregatesResponse, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	result, err := client.BitmapGroupAggregates(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("%v.BitmapGroupAggregates(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return result, nil
}

// TableOperation - Handle TableOperations
func (c *BitmapIndex) TableOperation(table, operation string) error {

	sop := AllActive
	var op pb.TableOperationRequest_OpType
	switch operation {
	case "deploy":
		sop = Admin
		op = pb.TableOperationRequest_DEPLOY
	case "drop":
		op = pb.TableOperationRequest_DROP
	case "truncate":
		op = pb.TableOperationRequest_TRUNCATE
	default:
		return fmt.Errorf("unknown operation %v", operation)
	}
	req := &pb.TableOperationRequest{Table: table, Operation: op}

	if c.local != nil {
		ctx, cancel := context.WithTimeout(context.Background(), OpDeadline)
		defer cancel()
		_, err := c.local.TableOperation(ctx, req)
		return err
	}

	var eg errgroup.Group

	// Send the same tableOperation request to each node.  They must be all Active
	indices, err := c.SelectNodes(table, sop)
	if err != nil {
		return fmt.Errorf("table %s operation: %v", operation, err)
	}
	for _, n := range indices {
		client := c.client[n]
		clientIndex := n
		eg.Go(func() error {
			if err := c.tableOperationClient(client, req, clientIndex); err != nil {
				return err
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

// Send a tableOperation request to one node.
func (c *BitmapIndex) tableOperationClient(client pb.BitmapIndexClient, req *pb.TableOperationRequest,
	clientIndex int) error {

	ctx, cancel := context.WithTimeout(context.Background(), OpDeadline)
	defer cancel()

	_, err := client.TableOperation(ctx, req)
	if err != nil {
		return fmt.Errorf("%v.TableOperation(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return nil
}

// Commit - Send commitClient to all nodes. Wait for all to complete.
func (c *BitmapIndex) Commit() error {

	if c.local != nil {
		ctx, cancel := context.WithTimeout(context.Background(), CommitDeadline)
		defer cancel()
		_, err := c.local.Commit(ctx, &empty.Empty{})
		return err
	}

	clients := c.activeClientsSnapshot()
	if len(clients) == 0 {
		return fmt.Errorf("commit: no active bitmap nodes")
	}
	var eg errgroup.Group

	// Send the same commit request to each active node from one connection/status snapshot.
	for _, n := range clients {
		client := n.client
		clientIndex := n.index
		eg.Go(func() error {
			if err := c.commitClient(client, clientIndex); err != nil {
				return err
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

// Send a Commit request to a node.
func (c *BitmapIndex) commitClient(client pb.BitmapIndexClient, clientIndex int) error {

	ctx, cancel := context.WithTimeout(context.Background(), CommitDeadline)
	defer cancel()
	_, err := client.Commit(ctx, &empty.Empty{}) // where does this go, on the nodes?
	if err != nil {
		return fmt.Errorf("%v.Commit(_) = _, %v, node = %s", client, err,
			c.ClientConnections()[clientIndex].Target())
	}
	return nil
}

type PartitionInfoSummary struct {
	Table      string
	Quantum    time.Time
	ModTime    time.Time
	MemoryUsed uint32
	Shards     int
	TQType     string
}

// Send a shard info request
func (c *BitmapIndex) shardInfoClient(client pb.BitmapIndexClient, req *pb.PartitionInfoRequest,
	clientIndex int) (*pb.PartitionInfoResponse, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	response, err := client.PartitionInfo(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("%v.PartitionInfo(_) = _, %v, node = %s", client, err,
			c.Conn.ClientConnections()[clientIndex].Target())
	}
	return response, nil
}

// PartitionInfo - Given a "ending" timestamp (and optional table filter), show all the shards before that time.
func (c *BitmapIndex) PartitionInfo(before time.Time, index string) ([]*PartitionInfoSummary, error) {

	shardInfoAgg := make(map[string]map[int64]*PartitionInfoSummary, 0)
	indexList := make([]string, 0)
	results := make([]*PartitionInfoSummary, 0)

	req := &pb.PartitionInfoRequest{Index: index, Time: before.UnixNano()}
	resultChan := make(chan *pb.PartitionInfoResult, 10000000)
	var eg errgroup.Group

	// Send the same shard info request to each readable node.
	indices, err2 := c.SelectNodes(index, ReadIntentAll)
	if err2 != nil {
		return nil, fmt.Errorf("PartitionInfo: %v", err2)
	}
	for _, n := range indices {
		client := c.client[n]
		clientIndex := n
		eg.Go(func() error {
			pr, err := c.shardInfoClient(client, req, clientIndex)
			if err != nil {
				return err
			}
			for _, r := range pr.GetPartitionInfoResults() {
				resultChan <- r
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	close(resultChan)

	// Summarize results
	for rs := range resultChan {
		if _, ok := shardInfoAgg[rs.Index]; !ok {
			shardInfoAgg[rs.Index] = make(map[int64]*PartitionInfoSummary, 0)
			indexList = append(indexList, rs.Index)
		}
		if sim, ok := shardInfoAgg[rs.Index][rs.Time]; !ok {
			sim = &PartitionInfoSummary{Table: rs.Index, Quantum: time.Unix(0, rs.Time),
				ModTime: time.Unix(0, rs.ModTime), MemoryUsed: rs.Bytes, Shards: 1, TQType: rs.TqType}
			shardInfoAgg[rs.Index][rs.Time] = sim
		} else {
			if sim.ModTime.Before(time.Unix(0, rs.ModTime)) {
				sim.ModTime = time.Unix(0, rs.ModTime)
			}
			sim.MemoryUsed += rs.Bytes
			sim.Shards++
			shardInfoAgg[rs.Index][rs.Time] = sim
		}
	}
	sort.Strings(indexList)
	for _, x := range indexList {
		shardTimes := make([]int64, 0)
		for k, _ := range shardInfoAgg[x] {
			shardTimes = append(shardTimes, k)
		}
		sort.Slice(shardTimes, func(i, j int) bool { return shardTimes[i] > shardTimes[j] })
		for _, v := range shardTimes {
			results = append(results, shardInfoAgg[x][v])
		}
	}
	return results, nil
}

// Initiate partition purge
func (c *BitmapIndex) purgePartitionClient(client pb.BitmapIndexClient, req *pb.PartitionInfoRequest,
	clientIndex int) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	_, err := client.OfflinePartitions(ctx, req)
	if err != nil {
		return fmt.Errorf("%v.OfflinePartitions(_) = _, %v, node = %s", client, err,
			c.Conn.ClientConnections()[clientIndex].Target())
	}
	return nil
}

// OfflinePartitions - Given a "ending" timestamp (and optional table filter), offline older partitions.
func (c *BitmapIndex) OfflinePartitions(before time.Time, index string) error {

	req := &pb.PartitionInfoRequest{Index: index, Time: before.UnixNano()}
	var eg errgroup.Group

	// Send the same partition purge request to each writable  node.
	indices, err2 := c.SelectNodes(index, WriteIntentAll)
	if err2 != nil {
		return fmt.Errorf("OfflinePartitions: %v", err2)
	}
	for _, n := range indices {
		client := c.client[n]
		clientIndex := n
		eg.Go(func() error {
			err := c.purgePartitionClient(client, req, clientIndex)
			if err != nil {
				return err
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

// BatchClearValue - Send a batch of requests to clear BSI values.
func (c *BitmapIndex) BatchClearValue(batch map[string]map[string]map[int64]*roaring64.Bitmap) error {

	if c.local != nil {
		local, ok := c.local.(LocalBitmapIndexBatchService)
		if !ok {
			return fmt.Errorf("local BitmapIndex adapter does not support BatchClearValue")
		}
		return c.batchClearValueLocal(local, batch)
	}

	batches := c.splitBSIClearBatch(batch)
	var eg errgroup.Group
	for i, v := range batches {
		cl := c.client[i]
		batch := v
		eg.Go(func() error {
			return c.BatchClearValueNode(cl, batch)
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

func (c *BitmapIndex) batchClearValueLocal(local LocalBitmapIndexBatchService,
	batch map[string]map[string]map[int64]*roaring64.Bitmap) error {

	kvs := make([]*pb.IndexKVPair, 0)
	for indexName, index := range batch {
		for fieldName, field := range index {
			for t, ebm := range field {
				buf, err := ebm.ToBytes()
				if err != nil {
					u.Errorf("bitmap.ToBytes: %v", err)
					return err
				}
				kvs = append(kvs, &pb.IndexKVPair{
					IndexPath: indexName + "/" + fieldName,
					Key:       ToBytes(int64(-1)),
					Value:     [][]byte{buf},
					Time:      t,
					IsClear:   true,
				})
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	_, err := local.BatchMutate(ctx, kvs)
	return err
}

// BatchClearValueNode - Send a batch of BSI clear operations to a specific node.
func (c *BitmapIndex) BatchClearValueNode(client pb.BitmapIndexClient,
	batch map[string]map[string]map[int64]*roaring64.Bitmap) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	b := make([]*pb.IndexKVPair, 0)
	i := 0
	var err error
	for indexName, index := range batch {
		for fieldName, field := range index {
			for t, ebm := range field {
				buf, err := ebm.ToBytes()
				if err != nil {
					u.Errorf("bitmap.ToBytes: %v", err)
					return err
				}
				ba := make([][]byte, 1)
				ba[0] = buf
				b = append(b, &pb.IndexKVPair{IndexPath: indexName + "/" + fieldName,
					Key: ToBytes(int64(-1)), Value: ba, Time: t, IsClear: true})
				i++
				//u.Debugf("Sent batch %d for path %s\n", i, b[i].IndexPath)
			}
		}
	}
	stream, err := client.BatchMutate(ctx)
	if err != nil {
		u.Errorf("%v.BatchMutate(_) = _, %v: ", c.client, err)
		return fmt.Errorf("%v.BatchMutate(_) = _, %v: ", c.client, err)
	}

	for i := 0; i < len(b); i++ {
		if err := stream.Send(b[i]); err != nil {
			u.Errorf("%v.Send(%v) = %v", stream, b[i], err)
			return fmt.Errorf("%v.Send(%v) = %v", stream, b[i], err)
		}
	}
	_, err = stream.CloseAndRecv()
	if err != nil {
		u.Errorf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
		return fmt.Errorf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
	}
	return nil
}

// For a given batch of BSI clear mutations, separate them into sub-batches based upon
// a consistently hashed shard key so that they can be send to their respective nodes.
// For BSI fields, this shard key consists of [index/field/timestamp].  It contains the EBM
// Of the values to be cleared.
func (c *BitmapIndex) splitBSIClearBatch(batch map[string]map[string]map[int64]*roaring64.Bitmap,
) []map[string]map[string]map[int64]*roaring64.Bitmap {

	batches := make([]map[string]map[string]map[int64]*roaring64.Bitmap, len(c.client))
	for i := range batches {
		batches[i] = make(map[string]map[string]map[int64]*roaring64.Bitmap)
	}

	for indexName, index := range batch {
		for fieldName, field := range index {
			for t, ebm := range field {
				tm := time.Unix(0, t)
				indices, err := c.SelectNodes(fmt.Sprintf("%s/%s/%s", indexName, fieldName, formatShardTime(tm)), WriteIntent)
				if err != nil {
					u.Errorf("splitBSIClearBatch: %v", err)
					continue
				}
				for _, i := range indices {
					if batches[i] == nil {
						batches[i] = make(map[string]map[string]map[int64]*roaring64.Bitmap)
					}
					if _, ok := batches[i][indexName]; !ok {
						batches[i][indexName] = make(map[string]map[int64]*roaring64.Bitmap)
					}
					if _, ok := batches[i][indexName][fieldName]; !ok {
						batches[i][indexName][fieldName] = make(map[int64]*roaring64.Bitmap)
					}
					batches[i][indexName][fieldName][t] = ebm
				}
			}
		}
	}
	return batches
}

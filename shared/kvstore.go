package shared

// Simple KV store wrapper around the pogreb library.   Network enablement via gRPC with
// consistent hashing for scale.  The primary purpose of this is to create a backing store
// for Pym strings.   Also, indexing of high cardinality attributes where fine grained
// access is required.

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	u "github.com/araddon/gou"
	"github.com/golang/protobuf/ptypes/wrappers"
	"golang.org/x/sync/errgroup"
)

var (
	// Ensure KVStore implements shared.Service
	_ Service = (*KVStore)(nil)
)

// KVStore API wrapper
type KVStore struct {
	*Conn
	client []pb.KVStoreClient
	local  LocalKVStoreService
}

// KVBatchPutRoutedProfile captures client-visible work for a routed KV batch.
type KVBatchPutRoutedProfile struct {
	StartedAt          time.Time
	FinishedAt         time.Time
	TotalElapsed       time.Duration
	RouteElapsed       time.Duration
	BuildElapsed       time.Duration
	StreamElapsed      time.Duration
	StreamOpenElapsed  time.Duration
	StreamSendElapsed  time.Duration
	StreamCloseElapsed time.Duration
	StreamMaxElapsed   time.Duration
	PathCount          int
	InputEntryCount    int
	RoutedEntryCount   int
	PutCalls           int
	Local              bool
}

type kvBatchPutItemsNodeProfile struct {
	Items        int
	TotalElapsed time.Duration
	OpenElapsed  time.Duration
	SendElapsed  time.Duration
	CloseElapsed time.Duration
}

// NewKVStore - Construct KVStore service endpoint.
func NewKVStore(conn *Conn) *KVStore {

	if conn.LocalNodeServices.KVStore != nil {
		c := &KVStore{Conn: conn, local: conn.LocalNodeServices.KVStore}
		conn.RegisterService(c)
		return c
	}
	clients := make([]pb.KVStoreClient, len(conn.ClientConnections()))
	for i := 0; i < len(conn.ClientConnections()); i++ {
		clients[i] = pb.NewKVStoreClient(conn.ClientConnections()[i])
	}
	c := &KVStore{Conn: conn, client: clients}
	conn.RegisterService(c)
	return c
}

// MemberJoined - A new node joined the cluster.
func (c *KVStore) MemberJoined(nodeID, ipAddress string, index int) {

	c.client = append(c.client, nil)
	copy(c.client[index+1:], c.client[index:])
	c.client[index] = pb.NewKVStoreClient(c.Conn.clientConn[index])
}

// MemberLeft - A node left the cluster.
func (c *KVStore) MemberLeft(nodeID string, index int) {

	if len(c.client) <= 1 {
		c.client = make([]pb.KVStoreClient, 0)
		return
	}
	c.client = append(c.client[:index], c.client[index+1:]...)
}

// Client - Get a client by index.
func (c *KVStore) Client(index int) pb.KVStoreClient {

	return c.client[index]
}

// Put a new attribute
func (c *KVStore) Put(indexPath string, k interface{}, v interface{}, pathIsKey bool) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	key := k
	if pathIsKey {
		key, indexPath = checkAdjustKeyAndPath(indexPath)
	}
	indices, err := c.SelectNodes(key, WriteIntent)
	if err != nil {
		return fmt.Errorf("Put: %v", err)
	}
	if len(indices) == 0 {
		return fmt.Errorf("%v.Put(_) = _, %v: ", c.client, " no available nodes!")
	}
	// Iterate over replica client list and perform Put operation
	for _, i := range indices {
		_, err := c.client[i].Put(ctx, &pb.IndexKVPair{IndexPath: indexPath, Key: ToBytes(k),
			Value: [][]byte{ToBytes(v)}})
		if err != nil {
			return fmt.Errorf("%v.Put(_) = _, %v: [%s]", c.client[i], err, c.Conn.ClientConnections()[i].Target())
		}
	}
	return nil
}

func (c *KVStore) splitBatch(batch map[interface{}]interface{}, op OpType) []map[interface{}]interface{} {

	batches := make([]map[interface{}]interface{}, len(c.client))
	for i := range batches {
		batches[i] = make(map[interface{}]interface{}, 0)
	}
	for k, v := range batch {
		indices, err := c.SelectNodes(ToString(k), op)
		if err != nil {
			u.Errorf("splitBatch: %v", err)
			continue
		}
		for _, i := range indices {
			batches[i][k] = v
		}
	}
	return batches
}

// BatchPut - Insert a batch of attributes.
func (c *KVStore) BatchPut(indexPath string, batch map[interface{}]interface{}, pathIsKey bool) error {

	if c.local != nil {
		local, ok := c.local.(LocalKVStoreBatchService)
		if !ok {
			return fmt.Errorf("local KVStore adapter does not support BatchPut")
		}
		return c.batchPutLocal(local, indexPath, batch, pathIsKey)
	}

	batches := make([]map[interface{}]interface{}, len(c.client))
	for i := range batches {
		batches[i] = make(map[interface{}]interface{}, 0)
	}
	if pathIsKey {
		var key string
		key, indexPath = checkAdjustKeyAndPath(indexPath)
		indices, err := c.SelectNodes(key, WriteIntent)
		if err != nil {
			return fmt.Errorf("BatchPut: %v", err)
		}
		for _, i := range indices {
			batches[i] = batch
		}
	} else {
		batches = c.splitBatch(batch, WriteIntent)
	}

	// TODO: This should use errgroup
	done := make(chan error)
	defer close(done)
	count := len(batches)
	for i, v := range batches {
		go func(client pb.KVStoreClient, idx string, m map[interface{}]interface{}) {
			done <- c.BatchPutNode(client, idx, m)
		}(c.client[i], indexPath, v)
	}
	for {
		err := <-done
		if err != nil {
			return err
		}
		count--
		if count == 0 {
			break
		}
	}
	return nil
}

// BatchPutRouted writes batches that already carry their logical index path.
// It collapses many path-partitioned sidecar writes into one stream per target
// node while preserving per-item IndexPath on the server side.
func (c *KVStore) BatchPutRouted(batchesByPath map[string]map[interface{}]interface{},
	pathIsKey bool) (putCalls int, profile KVBatchPutRoutedProfile, err error) {

	profile = KVBatchPutRoutedProfile{StartedAt: time.Now()}
	defer func() {
		profile.FinishedAt = time.Now()
		profile.TotalElapsed = profile.FinishedAt.Sub(profile.StartedAt)
	}()

	if len(batchesByPath) == 0 {
		return 0, profile, nil
	}
	if c.local != nil {
		local, ok := c.local.(LocalKVStoreBatchService)
		if !ok {
			return 0, profile, fmt.Errorf("local KVStore adapter does not support BatchPut")
		}
		putCalls, localProfile, err := c.batchPutRoutedLocal(local, batchesByPath, pathIsKey)
		localProfile.FinishedAt = time.Now()
		localProfile.TotalElapsed = localProfile.FinishedAt.Sub(localProfile.StartedAt)
		return putCalls, localProfile, err
	}
	if len(c.client) == 0 {
		return 0, profile, fmt.Errorf("%v.BatchPutRouted(_) = _, %v: ", c.client, " no available nodes!")
	}

	routed := make([][]*pb.IndexKVPair, len(c.client))
	for indexPath, batch := range batchesByPath {
		if len(batch) == 0 {
			continue
		}
		profile.PathCount++
		profile.InputEntryCount += len(batch)
		physicalPath := indexPath
		var pathIndices []int
		if pathIsKey {
			routeStart := time.Now()
			var key string
			key, physicalPath = checkAdjustKeyAndPath(indexPath)
			indices, err := c.SelectNodes(key, WriteIntent)
			profile.RouteElapsed += time.Since(routeStart)
			if err != nil {
				return 0, profile, fmt.Errorf("BatchPutRouted: %v", err)
			}
			if len(indices) == 0 {
				return 0, profile, fmt.Errorf("BatchPutRouted: no available nodes for path %s", indexPath)
			}
			pathIndices = indices
		}
		for k, v := range batch {
			indices := pathIndices
			var err error
			if !pathIsKey {
				routeStart := time.Now()
				indices, err = c.SelectNodes(ToString(k), WriteIntent)
				profile.RouteElapsed += time.Since(routeStart)
				if err != nil {
					return 0, profile, fmt.Errorf("BatchPutRouted: %v", err)
				}
				if len(indices) == 0 {
					return 0, profile, fmt.Errorf("BatchPutRouted: no available nodes for path %s", indexPath)
				}
			}
			buildStart := time.Now()
			item := &pb.IndexKVPair{
				IndexPath: physicalPath,
				Key:       ToBytes(k),
				Value:     [][]byte{ToBytes(v)},
			}
			for _, i := range indices {
				routed[i] = append(routed[i], item)
				profile.RoutedEntryCount++
			}
			profile.BuildElapsed += time.Since(buildStart)
		}
	}

	var eg errgroup.Group
	var profileLock sync.Mutex
	putCalls = 0
	streamStart := time.Now()
	for i, items := range routed {
		if len(items) == 0 {
			continue
		}
		client := c.client[i]
		items := items
		putCalls++
		eg.Go(func() error {
			nodeProfile, err := c.batchPutItemsNodeProfile(client, items)
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
		return putCalls, profile, err
	}
	profile.StreamElapsed = time.Since(streamStart)
	return putCalls, profile, nil
}

func (c *KVStore) batchPutLocal(local LocalKVStoreBatchService, indexPath string, batch map[interface{}]interface{}, pathIsKey bool) error {
	if pathIsKey {
		_, indexPath = checkAdjustKeyAndPath(indexPath)
	}
	kvs := make([]*pb.IndexKVPair, 0, len(batch))
	for k, v := range batch {
		kvs = append(kvs, &pb.IndexKVPair{
			IndexPath: indexPath,
			Key:       ToBytes(k),
			Value:     [][]byte{ToBytes(v)},
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	_, err := local.BatchPut(ctx, kvs)
	return err
}

func (c *KVStore) batchPutRoutedLocal(local LocalKVStoreBatchService, batchesByPath map[string]map[interface{}]interface{},
	pathIsKey bool) (int, KVBatchPutRoutedProfile, error) {

	profile := KVBatchPutRoutedProfile{StartedAt: time.Now(), Local: true}
	kvs := make([]*pb.IndexKVPair, 0)
	for indexPath, batch := range batchesByPath {
		if len(batch) == 0 {
			continue
		}
		profile.PathCount++
		profile.InputEntryCount += len(batch)
		physicalPath := indexPath
		if pathIsKey {
			routeStart := time.Now()
			_, physicalPath = checkAdjustKeyAndPath(indexPath)
			profile.RouteElapsed += time.Since(routeStart)
		}
		for k, v := range batch {
			buildStart := time.Now()
			kvs = append(kvs, &pb.IndexKVPair{
				IndexPath: physicalPath,
				Key:       ToBytes(k),
				Value:     [][]byte{ToBytes(v)},
			})
			profile.RoutedEntryCount++
			profile.BuildElapsed += time.Since(buildStart)
		}
	}
	if len(kvs) == 0 {
		return 0, profile, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	streamStart := time.Now()
	_, err := local.BatchPut(ctx, kvs)
	profile.StreamElapsed = time.Since(streamStart)
	profile.StreamMaxElapsed = profile.StreamElapsed
	profile.StreamCloseElapsed = profile.StreamElapsed
	if err != nil {
		return 1, profile, err
	}
	profile.PutCalls = 1
	return 1, profile, nil
}

// BatchPutNode - Put a batch of keys on a single node.
func (c *KVStore) BatchPutNode(client pb.KVStoreClient, index string, batch map[interface{}]interface{}) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	b := make([]*pb.IndexKVPair, len(batch))
	i := 0
	for k, v := range batch {
		b[i] = &pb.IndexKVPair{IndexPath: index, Key: ToBytes(k), Value: [][]byte{ToBytes(v)}}
		i++
	}
	stream, err := client.BatchPut(ctx)
	if err != nil {
		return fmt.Errorf("%v.BatchPut(_) = _, %v: ", c.client, err)
	}

	for i := 0; i < len(b); i++ {
		if err := stream.Send(b[i]); err != nil {
			return fmt.Errorf("%v.Send(%v) = %v", stream, b[i], err)
		}
	}
	_, err2 := stream.CloseAndRecv()
	if err2 != nil {
		return fmt.Errorf("%v.CloseAndRecv() got error %v, want %v", stream, err2, nil)
	}
	return nil
}

// BatchPutItemsNode writes pre-routed KV items in a single stream.
func (c *KVStore) BatchPutItemsNode(client pb.KVStoreClient, items []*pb.IndexKVPair) error {
	_, err := c.batchPutItemsNodeProfile(client, items)
	return err
}

func (c *KVStore) batchPutItemsNodeProfile(client pb.KVStoreClient,
	items []*pb.IndexKVPair) (profile kvBatchPutItemsNodeProfile, err error) {

	if len(items) == 0 {
		return profile, nil
	}
	profile = kvBatchPutItemsNodeProfile{Items: len(items)}
	start := time.Now()
	defer func() {
		profile.TotalElapsed = time.Since(start)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	openStart := time.Now()
	stream, err := client.BatchPut(ctx)
	profile.OpenElapsed = time.Since(openStart)
	if err != nil {
		return profile, fmt.Errorf("%v.BatchPut(_) = _, %v: ", c.client, err)
	}
	sendStart := time.Now()
	for i := 0; i < len(items); i++ {
		if err := stream.Send(items[i]); err != nil {
			profile.SendElapsed = time.Since(sendStart)
			return profile, fmt.Errorf("%v.Send(%v) = %v", stream, items[i], err)
		}
	}
	profile.SendElapsed = time.Since(sendStart)
	closeStart := time.Now()
	_, err = stream.CloseAndRecv()
	profile.CloseElapsed = time.Since(closeStart)
	if err != nil {
		return profile, fmt.Errorf("%v.CloseAndRecv() got error %v, want %v", stream, err, nil)
	}
	return profile, nil
}

// Lookup a single key.
func (c *KVStore) Lookup(indexPath string, k interface{}, valueType reflect.Kind, pathIsKey bool) (interface{}, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	key := k
	if pathIsKey {
		key, indexPath = checkAdjustKeyAndPath(indexPath)
	}
	if c.local != nil {
		lookup, err := c.local.Lookup(ctx, &pb.IndexKVPair{IndexPath: indexPath, Key: ToBytes(k), Value: nil})
		if err != nil {
			return uint64(0), err
		}
		if lookup.Value != nil && len(lookup.Value) != 0 && len(lookup.Value[0]) != 0 {
			return UnmarshalValue(valueType, lookup.Value[0]), nil
		}
		return nil, nil
	}

	indices, err := c.SelectNodes(key, ReadIntent)
	if err != nil {
		return nil, fmt.Errorf("Lookup: %v", err)
	}
	if len(indices) == 0 {
		return nil, fmt.Errorf("%v.Lookup(_) = _, %v: ", c.client, " no available nodes")
	}

	// Use the highest weight client
	index := indices[0]
	client := c.client[index]
	lookup, err := client.Lookup(ctx, &pb.IndexKVPair{IndexPath: indexPath, Key: ToBytes(k), Value: nil})
	if err != nil {
		return uint64(0), fmt.Errorf("%v.Lookup(_) = _, %v: [%s]", c.client, err,
			c.Conn.ClientConnections()[indices[0]].Target())
	}
	if lookup.Value != nil && len(lookup.Value) != 0 && len(lookup.Value[0]) != 0 {
		return UnmarshalValue(valueType, lookup.Value[0]), nil
	}
	return nil, nil
}

// BatchLookup of multiple keys.
func (c *KVStore) BatchLookup(indexPath string, batch map[interface{}]interface{}, pathIsKey bool) (map[interface{}]interface{}, error) {

	if c.local != nil {
		if pathIsKey {
			_, indexPath = checkAdjustKeyAndPath(indexPath)
		}
		results := make(map[interface{}]interface{}, len(batch))
		if len(batch) == 0 {
			return results, nil
		}
		var keyType, valueType reflect.Kind
		reqs := make([]*pb.IndexKVPair, 0, len(batch))
		for k, v := range batch {
			if keyType == reflect.Invalid {
				keyType = reflect.ValueOf(k).Kind()
				valueType = reflect.ValueOf(v).Kind()
			}
			reqs = append(reqs, &pb.IndexKVPair{IndexPath: indexPath, Key: ToBytes(k), Value: [][]byte{ToBytes(v)}})
		}
		ctx, cancel := context.WithTimeout(context.Background(), Deadline)
		defer cancel()
		items, err := c.local.BatchLookup(ctx, reqs)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			if item == nil || len(item.Value) == 0 {
				continue
			}
			results[UnmarshalValue(keyType, item.Key)] = UnmarshalValue(valueType, item.Value[0])
		}
		return results, nil
	}

	if pathIsKey {
		var key string
		key, indexPath = checkAdjustKeyAndPath(indexPath)
		indices, err := c.SelectNodes(key, ReadIntent)
		if err != nil {
			return nil, fmt.Errorf("BatchLookup: %v", err)
		}
		if len(indices) == 0 {
			return nil, fmt.Errorf("no nodes available")
		}
		return c.BatchLookupNode(c.client[indices[0]], indexPath, batch)
	}

	// We dont want to iterate over replicas for lookups so count is 1, first replica is primary
	batches := c.splitBatch(batch, ReadIntent)

	// TODO: use errorgroup
	results := make(map[interface{}]interface{}, 0)
	rchan := make(chan map[interface{}]interface{})
	defer close(rchan)
	done := make(chan error)
	defer close(done)
	count := len(batches)
	for i := range batches {
		go func(client pb.KVStoreClient, idx string, b map[interface{}]interface{}) {
			r, e := c.BatchLookupNode(client, idx, b)
			rchan <- r
			done <- e
		}(c.client[i], indexPath, batches[i])

	}
	for {
		b := <-rchan
		if b != nil {
			for k, v := range b {
				results[k] = v
			}
		}
		err := <-done
		if err != nil {
			return nil, err
		}
		count--
		if count == 0 {
			break
		}
	}

	return results, nil
}

// BatchLookupNode - Batch lookup of keys on a single node.
func (c *KVStore) BatchLookupNode(client pb.KVStoreClient, index string,
	batch map[interface{}]interface{}) (map[interface{}]interface{}, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	stream, err := client.BatchLookup(ctx)
	if err != nil {
		return nil, fmt.Errorf("%v.BatchLookup(_) = _, %v", c.client, err)
	}

	var keyType, valueType reflect.Kind
	// Just grab first key/value of incoming data to determine types
	for k, v := range batch {
		keyType = reflect.ValueOf(k).Kind()
		valueType = reflect.ValueOf(v).Kind()
		break
	}

	waitc := make(chan struct{})
	results := make(map[interface{}]interface{}, len(batch))
	go func() {
		for {
			kv, err := stream.Recv()
			if err == io.EOF {
				// read done.
				close(waitc)
				return
			}
			if err != nil {
				err = fmt.Errorf("Failed to receive a KV pair : %v", err)
				return
			}
			k := UnmarshalValue(keyType, kv.Key)
			v := UnmarshalValue(valueType, kv.Value[0])
			results[k] = v
		}
	}()
	for k, v := range batch {
		newKV := &pb.IndexKVPair{IndexPath: index, Key: ToBytes(k), Value: [][]byte{ToBytes(v)}}
		if err := stream.Send(newKV); err != nil {
			return nil, fmt.Errorf("Failed to send a KV pair: %v", err)
		}
	}
	stream.CloseSend()
	<-waitc
	return results, nil
}

// Items - Iterate over entire set of items across all nodes
func (c *KVStore) Items(index string, keyType, valueType reflect.Kind) (map[interface{}]interface{}, error) {

	results := make(map[interface{}]interface{}, 0)
	if c.local != nil {
		ctx, cancel := context.WithTimeout(context.Background(), Deadline)
		defer cancel()
		items, err := c.local.Items(ctx, index)
		if err != nil {
			return results, err
		}
		for _, item := range items {
			if item == nil || len(item.Value) == 0 {
				continue
			}
			results[UnmarshalValue(keyType, item.Key)] = UnmarshalValue(valueType, item.Value[0])
		}
		return results, nil
	}

	rchan := make(chan map[interface{}]interface{}, len(c.client))

	var eg errgroup.Group

	indices, err := c.SelectNodes(index, ReadIntentAll)
	if err != nil {
		return results, fmt.Errorf("Items: %v", err)
	}
	for _, i := range indices {
		if i < 0 || i >= len(c.client) {
			return results, fmt.Errorf("Items: selected node index %d outside client count %d", i, len(c.client))
		}
		x := c.client[i]
		eg.Go(func() error {
			r, err := c.NodeItems(x, index, keyType, valueType)
			if err != nil {
				return err
			}
			rchan <- r
			return nil
		})
	}

	if err = eg.Wait(); err != nil {
		return results, err
	}
	close(rchan)

	for b := range rchan {
		if b != nil {
			for k, v := range b {
				results[k] = v
			}
		}
	}
	return results, nil
}

// NodeItems - Iterate over all  items on a single node.
func (c *KVStore) NodeItems(client pb.KVStoreClient, index string, keyType,
	valueType reflect.Kind) (map[interface{}]interface{}, error) {

	batch := make(map[interface{}]interface{}, 0)
	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()

	stream, err := client.Items(ctx, &wrappers.StringValue{Value: index})
	if err != nil {
		return nil, fmt.Errorf("%v.Items(_) = _, %v", client, err)
	}
	for {
		item, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%v.Items(_) = _, %v", client, err)
		}
		batch[UnmarshalValue(keyType, item.Key)] = UnmarshalValue(valueType, item.Value[0])
	}

	return batch, nil
}

// PutStringEnum - Put a new enum value
func (c *KVStore) PutStringEnum(index, value string) (uint64, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	if c.local != nil {
		rowID, err := c.local.PutStringEnum(ctx, &pb.StringEnum{IndexPath: index, Value: value})
		if err != nil {
			return 0, err
		}
		return rowID.Value, nil
	}
	indices, err1 := c.SelectNodes(index, WriteIntentAll)
	if err1 != nil {
		return 0, fmt.Errorf("PutStringEnum: %v", err1)
	}
	if len(indices) == 0 {
		return 0, fmt.Errorf("PutStringEnum(_) = _, %v: ", " no available nodes!")
	}

	// Perform PutStringEnum server call on first node in list (primary)
	// If the value already exists it will silently return the existing rowID
	rowID, err := c.client[indices[0]].PutStringEnum(ctx, &pb.StringEnum{IndexPath: index, Value: value})
	if err != nil {
		return 0, fmt.Errorf("PutStringEnum(_) = _, %v: [%s]", err, c.Conn.ClientConnections()[indices[0]].Target())
	}

	// Parallel iterate over remaining client list and perform Put operation (replication)
	var eg errgroup.Group
	for _, i := range indices {
		c := c.client[indices[i]]
		eg.Go(func() error {
			_, err := c.Put(ctx, &pb.IndexKVPair{IndexPath: index, Key: ToBytes(value),
				Value: [][]byte{ToBytes(rowID.Value)}})
			if err != nil {
				return fmt.Errorf("%v.Put(_) = _, %v: ", c, err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return 0, err
	}
	return rowID.Value, nil
}

// DeleteIndicesWithPrefix - Delete indices with a table prefix, optionally retain StringEnum data
func (c *KVStore) DeleteIndicesWithPrefix(prefix string, retainEnums bool) error {

	var eg errgroup.Group
	indices, err := c.SelectNodes(prefix, AllActive)
	if err != nil {
		return fmt.Errorf("DeleteIndicesWithPrefix %v", err)
	}
	if len(indices) == 0 {
		return fmt.Errorf("no available nodes")
	}
	for _, i := range indices {
		x := c.client[indices[i]]
		eg.Go(func() error {
			return c.deleteIndicesWithPrefix(x, prefix, retainEnums)
		})
	}
	if err = eg.Wait(); err != nil {
		return err
	}
	return nil
}

func (c *KVStore) deleteIndicesWithPrefix(client pb.KVStoreClient, prefix string, retainEnums bool) error {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	_, err := client.DeleteIndicesWithPrefix(ctx,
		&pb.DeleteIndicesWithPrefixRequest{Prefix: prefix, RetainEnums: retainEnums})
	if err != nil {
		return fmt.Errorf("%v.DeleteIndicesWithPrefix(_) = _, %v: ", c, err)
	}
	return nil
}

// IndexInfoNode - Get index info on a specific node
func (c *KVStore) IndexInfoNode(client pb.KVStoreClient, indexPath string) (*pb.IndexInfoResponse, error) {

	ctx, cancel := context.WithTimeout(context.Background(), Deadline)
	defer cancel()
	res, err := client.IndexInfo(ctx, &pb.IndexInfoRequest{IndexPath: indexPath})
	if err != nil {
		return nil, fmt.Errorf("%v.IndexInfo(_) = _, %v: ", c, err)
	}
	return res, nil
}

// The path may be in the form "key,filePathSuffix"
// If so, separate out key and full path becomes "key/filePathSuffix"
func checkAdjustKeyAndPath(path string) (key, indexPath string) {

	s := strings.Split(path, ",")
	if len(s) == 1 {
		key = indexPath
		indexPath = path
	} else {
		key = s[0]
		if s[1][:1] == "/" {
			indexPath = s[1][1:]
			return
		}
		indexPath = fmt.Sprintf("%s/%s", s[0], s[1])
	}
	return
}

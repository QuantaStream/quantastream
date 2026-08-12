package server

import (
	"context"
	"fmt"
	"io"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	"google.golang.org/grpc/metadata"
)

// LocalNodeAdapter exposes server node services through the in-process
// inabox-standard boundary. It deliberately avoids shared.Conn and gRPC clients.
type LocalNodeAdapter struct {
	BitmapIndex  *BitmapIndex
	KVStore      *KVStore
	StringSearch *StringSearch
	Observer     shared.LocalNodeObserver
}

// NewLocalNodeAdapter builds local service adapters around an existing node.
func NewLocalNodeAdapter(node *Node, observer shared.LocalNodeObserver) LocalNodeAdapter {
	if node == nil {
		return LocalNodeAdapter{Observer: observer}
	}
	return LocalNodeAdapter{
		BitmapIndex:  NewBitmapIndex(node),
		KVStore:      NewKVStore(node),
		StringSearch: NewStringSearch(node),
		Observer:     observer,
	}
}

// Services returns the shared local-node contracts implemented by this adapter.
func (a LocalNodeAdapter) Services() shared.LocalNodeServices {
	services := shared.LocalNodeServices{Observer: a.Observer}
	if a.BitmapIndex != nil {
		services.BitmapIndex = LocalBitmapIndexAdapter{Index: a.BitmapIndex, Observer: a.Observer}
	}
	if a.KVStore != nil {
		services.KVStore = LocalKVStoreAdapter{Store: a.KVStore, Observer: a.Observer}
	}
	// StringSearch remains intentionally unmounted until streaming search calls
	// get a local semantic helper or a stream shim.
	return services
}

// Readiness returns a structured status for the in-process node boundary.
func (a LocalNodeAdapter) Readiness() shared.LocalNodeReadiness {
	return a.Services().Readiness()
}

// LocalBitmapIndexAdapter forwards unary bitmap calls directly to BitmapIndex.
type LocalBitmapIndexAdapter struct {
	Index    *BitmapIndex
	Observer shared.LocalNodeObserver
}

// Query forwards a bitmap query without a gRPC client hop.
func (a LocalBitmapIndexAdapter) Query(ctx context.Context, req *pb.BitmapQuery) (*pb.QueryResult, error) {
	if a.Index == nil {
		return nil, fmt.Errorf("local BitmapIndex adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Index.Query(ctx, req)
	observeLocalNodeCall(a.Observer, "BitmapIndex", "Query", start, err)
	return result, err
}

// QueryWithFoundSet forwards a bitmap query with an in-process prior found set.
func (a LocalBitmapIndexAdapter) QueryWithFoundSet(ctx context.Context, req *pb.BitmapQuery, index string, rownums []uint64) (*pb.QueryResult, error) {
	if a.Index == nil {
		return nil, fmt.Errorf("local BitmapIndex adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Index.QueryWithFoundSet(ctx, req, index, rownums)
	observeLocalNodeCall(a.Observer, "BitmapIndex", "QueryWithFoundSet", start, err)
	return result, err
}

// SyncStatus forwards a sync-status request without a gRPC client hop.
func (a LocalBitmapIndexAdapter) SyncStatus(ctx context.Context, req *pb.SyncStatusRequest) (*pb.SyncStatusResponse, error) {
	if a.Index == nil {
		return nil, fmt.Errorf("local BitmapIndex adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Index.SyncStatus(ctx, req)
	observeLocalNodeCall(a.Observer, "BitmapIndex", "SyncStatus", start, err)
	return result, err
}

// Projection forwards a projection request without a gRPC client hop.
func (a LocalBitmapIndexAdapter) Projection(ctx context.Context, req *pb.ProjectionRequest) (*pb.ProjectionResponse, error) {
	if a.Index == nil {
		return nil, fmt.Errorf("local BitmapIndex adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Index.Projection(ctx, req)
	observeLocalNodeCall(a.Observer, "BitmapIndex", "Projection", start, err)
	return result, err
}

// CompareBSIFields forwards a row-local BSI comparison without a gRPC client hop.
func (a LocalBitmapIndexAdapter) CompareBSIFields(ctx context.Context, req *pb.CompareBSIFieldsRequest) (*pb.CompareBSIFieldsResponse, error) {
	if a.Index == nil {
		return nil, fmt.Errorf("local BitmapIndex adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Index.CompareBSIFields(ctx, req)
	observeLocalNodeCall(a.Observer, "BitmapIndex", "CompareBSIFields", start, err)
	return result, err
}

// RelationshipAlignedValueSum forwards a relationship aggregate request without
// a gRPC client hop.
func (a LocalBitmapIndexAdapter) RelationshipAlignedValueSum(ctx context.Context, req *pb.RelationshipAlignedValueSumRequest) (*pb.RelationshipAlignedValueSumResponse, error) {
	if a.Index == nil {
		return nil, fmt.Errorf("local BitmapIndex adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Index.RelationshipAlignedValueSum(ctx, req)
	observeLocalNodeCall(a.Observer, "BitmapIndex", "RelationshipAlignedValueSum", start, err)
	return result, err
}

// Join forwards a join request without a gRPC client hop.
func (a LocalBitmapIndexAdapter) Join(ctx context.Context, req *pb.JoinRequest) (*pb.JoinResponse, error) {
	if a.Index == nil {
		return nil, fmt.Errorf("local BitmapIndex adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Index.Join(ctx, req)
	observeLocalNodeCall(a.Observer, "BitmapIndex", "Join", start, err)
	return result, err
}

// CheckoutSequence forwards a sequence reservation request without a gRPC client hop.
func (a LocalBitmapIndexAdapter) CheckoutSequence(ctx context.Context, req *pb.CheckoutSequenceRequest) (*pb.CheckoutSequenceResponse, error) {
	if a.Index == nil {
		return nil, fmt.Errorf("local BitmapIndex adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Index.CheckoutSequence(ctx, req)
	observeLocalNodeCall(a.Observer, "BitmapIndex", "CheckoutSequence", start, err)
	return result, err
}

// BulkClear forwards a table-wide rownum clear without a gRPC client hop.
func (a LocalBitmapIndexAdapter) BulkClear(ctx context.Context, req *pb.BulkClearRequest) (*empty.Empty, error) {
	if a.Index == nil {
		return nil, fmt.Errorf("local BitmapIndex adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Index.BulkClear(ctx, req)
	observeLocalNodeCall(a.Observer, "BitmapIndex", "BulkClear", start, err)
	return result, err
}

// TableOperation forwards a table operation without a gRPC client hop.
func (a LocalBitmapIndexAdapter) TableOperation(ctx context.Context, req *pb.TableOperationRequest) (*empty.Empty, error) {
	if a.Index == nil {
		return nil, fmt.Errorf("local BitmapIndex adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Index.TableOperation(ctx, req)
	observeLocalNodeCall(a.Observer, "BitmapIndex", "TableOperation", start, err)
	return result, err
}

// Commit forwards a commit request without a gRPC client hop.
func (a LocalBitmapIndexAdapter) Commit(ctx context.Context, req *empty.Empty) (*empty.Empty, error) {
	if a.Index == nil {
		return nil, fmt.Errorf("local BitmapIndex adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Index.Commit(ctx, req)
	observeLocalNodeCall(a.Observer, "BitmapIndex", "Commit", start, err)
	return result, err
}

// BatchMutate forwards bitmap and BSI batch writes through the existing server
// stream implementation without a gRPC hop.
func (a LocalBitmapIndexAdapter) BatchMutate(ctx context.Context, reqs []*pb.IndexKVPair) (*empty.Empty, error) {
	if a.Index == nil {
		return nil, fmt.Errorf("local BitmapIndex adapter is not mounted")
	}
	start := time.Now()
	stream := &localIndexKVBatchStream{ctx: ctx, items: reqs}
	err := a.Index.BatchMutate(stream)
	observeLocalNodeCall(a.Observer, "BitmapIndex", "BatchMutate", start, err)
	if err != nil {
		return nil, err
	}
	if stream.closed != nil {
		return stream.closed, nil
	}
	return &empty.Empty{}, nil
}

// LocalKVStoreAdapter forwards unary KV calls directly to KVStore.
type LocalKVStoreAdapter struct {
	Store    *KVStore
	Observer shared.LocalNodeObserver
}

// Put forwards a KV write without a gRPC client hop.
func (a LocalKVStoreAdapter) Put(ctx context.Context, req *pb.IndexKVPair) (*empty.Empty, error) {
	if a.Store == nil {
		return nil, fmt.Errorf("local KVStore adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Store.Put(ctx, req)
	observeLocalNodeCall(a.Observer, "KVStore", "Put", start, err)
	return result, err
}

// Lookup forwards a KV lookup without a gRPC client hop.
func (a LocalKVStoreAdapter) Lookup(ctx context.Context, req *pb.IndexKVPair) (*pb.IndexKVPair, error) {
	if a.Store == nil {
		return nil, fmt.Errorf("local KVStore adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Store.Lookup(ctx, req)
	observeLocalNodeCall(a.Observer, "KVStore", "Lookup", start, err)
	return result, err
}

// BatchLookup forwards KV batch reads through the existing server stream
// implementation without a gRPC hop.
func (a LocalKVStoreAdapter) BatchLookup(ctx context.Context, reqs []*pb.IndexKVPair) ([]*pb.IndexKVPair, error) {
	if a.Store == nil {
		return nil, fmt.Errorf("local KVStore adapter is not mounted")
	}
	start := time.Now()
	stream := &localIndexKVBatchStream{ctx: ctx, items: reqs}
	err := a.Store.BatchLookup(stream)
	observeLocalNodeCall(a.Observer, "KVStore", "BatchLookup", start, err)
	if err != nil {
		return nil, err
	}
	return stream.responses, nil
}

// PutStringEnum forwards a StringEnum dictionary write without a gRPC client hop.
func (a LocalKVStoreAdapter) PutStringEnum(ctx context.Context, req *pb.StringEnum) (*wrappers.UInt64Value, error) {
	if a.Store == nil {
		return nil, fmt.Errorf("local KVStore adapter is not mounted")
	}
	start := time.Now()
	result, err := a.Store.PutStringEnum(ctx, req)
	observeLocalNodeCall(a.Observer, "KVStore", "PutStringEnum", start, err)
	return result, err
}

// BatchPut forwards KV batch writes through the existing server stream
// implementation without a gRPC hop.
func (a LocalKVStoreAdapter) BatchPut(ctx context.Context, reqs []*pb.IndexKVPair) (*empty.Empty, error) {
	if a.Store == nil {
		return nil, fmt.Errorf("local KVStore adapter is not mounted")
	}
	start := time.Now()
	stream := &localIndexKVBatchStream{ctx: ctx, items: reqs}
	err := a.Store.BatchPut(stream)
	observeLocalNodeCall(a.Observer, "KVStore", "BatchPut", start, err)
	if err != nil {
		return nil, err
	}
	if stream.closed != nil {
		return stream.closed, nil
	}
	return &empty.Empty{}, nil
}

// Items forwards full-index KV iteration through the existing server stream
// implementation without a gRPC hop.
func (a LocalKVStoreAdapter) Items(ctx context.Context, index string) ([]*pb.IndexKVPair, error) {
	if a.Store == nil {
		return nil, fmt.Errorf("local KVStore adapter is not mounted")
	}
	start := time.Now()
	stream := &localIndexKVItemsStream{ctx: ctx}
	err := a.Store.Items(&wrappers.StringValue{Value: index}, stream)
	observeLocalNodeCall(a.Observer, "KVStore", "Items", start, err)
	if err != nil {
		return nil, err
	}
	return stream.items, nil
}

type localIndexKVBatchStream struct {
	ctx       context.Context
	items     []*pb.IndexKVPair
	responses []*pb.IndexKVPair
	offset    int
	closed    *empty.Empty
}

func (s *localIndexKVBatchStream) SendAndClose(result *empty.Empty) error {
	s.closed = result
	return nil
}

func (s *localIndexKVBatchStream) Send(item *pb.IndexKVPair) error {
	s.responses = append(s.responses, item)
	return nil
}

func (s *localIndexKVBatchStream) Recv() (*pb.IndexKVPair, error) {
	if s.offset >= len(s.items) {
		return nil, io.EOF
	}
	item := s.items[s.offset]
	s.offset++
	return item, nil
}

func (s *localIndexKVBatchStream) SetHeader(metadata.MD) error {
	return nil
}

func (s *localIndexKVBatchStream) SendHeader(metadata.MD) error {
	return nil
}

func (s *localIndexKVBatchStream) SetTrailer(metadata.MD) {}

func (s *localIndexKVBatchStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (s *localIndexKVBatchStream) SendMsg(interface{}) error {
	return nil
}

func (s *localIndexKVBatchStream) RecvMsg(interface{}) error {
	return nil
}

type localIndexKVItemsStream struct {
	ctx   context.Context
	items []*pb.IndexKVPair
}

func (s *localIndexKVItemsStream) Send(item *pb.IndexKVPair) error {
	s.items = append(s.items, item)
	return nil
}

func (s *localIndexKVItemsStream) SetHeader(metadata.MD) error {
	return nil
}

func (s *localIndexKVItemsStream) SendHeader(metadata.MD) error {
	return nil
}

func (s *localIndexKVItemsStream) SetTrailer(metadata.MD) {}

func (s *localIndexKVItemsStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func (s *localIndexKVItemsStream) SendMsg(interface{}) error {
	return nil
}

func (s *localIndexKVItemsStream) RecvMsg(interface{}) error {
	return nil
}

func observeLocalNodeCall(observer shared.LocalNodeObserver, service, method string, startedAt time.Time, err error) {
	if observer == nil {
		return
	}
	observer.ObserveLocalNodeCall(shared.LocalNodeCall{
		Transport: shared.NodeTransportLocal,
		Service:   service,
		Method:    method,
		StartedAt: startedAt,
		Elapsed:   time.Since(startedAt),
		Err:       err,
	})
}

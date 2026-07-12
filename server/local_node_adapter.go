package server

import (
	"context"
	"fmt"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
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

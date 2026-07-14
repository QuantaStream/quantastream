package shared

import (
	"context"
	"time"

	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
)

// NodeTransportKind identifies the concrete boundary used to reach node services.
type NodeTransportKind string

const (
	// NodeTransportGRPC is the existing distributed query-processor-to-node path.
	NodeTransportGRPC NodeTransportKind = "grpc"
	// NodeTransportLocal is the in-process inabox-standard node path.
	NodeTransportLocal NodeTransportKind = "local"
)

// LocalNodeCall records one in-process node service call for observability.
type LocalNodeCall struct {
	Transport NodeTransportKind
	Service   string
	Method    string
	StartedAt time.Time
	Elapsed   time.Duration
	Err       error
}

// LocalNodeObserver receives local-node call observations.
type LocalNodeObserver interface {
	ObserveLocalNodeCall(LocalNodeCall)
}

// LocalNodeObserverFunc adapts a function to LocalNodeObserver.
type LocalNodeObserverFunc func(LocalNodeCall)

// ObserveLocalNodeCall calls f(call).
func (f LocalNodeObserverFunc) ObserveLocalNodeCall(call LocalNodeCall) {
	f(call)
}

// LocalBitmapIndexService is the in-process semantic equivalent of the unary
// BitmapIndex node API. Streaming/batch methods remain explicit readiness risks.
type LocalBitmapIndexService interface {
	Query(context.Context, *pb.BitmapQuery) (*pb.QueryResult, error)
	Projection(context.Context, *pb.ProjectionRequest) (*pb.ProjectionResponse, error)
	Join(context.Context, *pb.JoinRequest) (*pb.JoinResponse, error)
	CheckoutSequence(context.Context, *pb.CheckoutSequenceRequest) (*pb.CheckoutSequenceResponse, error)
	TableOperation(context.Context, *pb.TableOperationRequest) (*empty.Empty, error)
	Commit(context.Context, *empty.Empty) (*empty.Empty, error)
}

// LocalBitmapIndexBatchService is the in-process equivalent of the current
// BitmapIndex streaming write path. It remains optional so read-only local
// adapters can still satisfy LocalBitmapIndexService.
type LocalBitmapIndexBatchService interface {
	BatchMutate(context.Context, []*pb.IndexKVPair) (*empty.Empty, error)
}

// LocalKVStoreService is the in-process semantic equivalent of the unary KVStore
// node API. Batch lookup remains tracked as a streaming risk.
type LocalKVStoreService interface {
	Put(context.Context, *pb.IndexKVPair) (*empty.Empty, error)
	Lookup(context.Context, *pb.IndexKVPair) (*pb.IndexKVPair, error)
	BatchLookup(context.Context, []*pb.IndexKVPair) ([]*pb.IndexKVPair, error)
	Items(context.Context, string) ([]*pb.IndexKVPair, error)
	PutStringEnum(context.Context, *pb.StringEnum) (*wrappers.UInt64Value, error)
}

// LocalKVStoreBatchService is the in-process equivalent of the current KVStore
// streaming batch write path.
type LocalKVStoreBatchService interface {
	BatchPut(context.Context, []*pb.IndexKVPair) (*empty.Empty, error)
}

// LocalStringSearchService names the desired local string-search semantic API.
// The current server implementation exposes streaming gRPC calls, so adapters
// should satisfy this interface only after local stream shims or direct helpers exist.
type LocalStringSearchService interface {
	Search(context.Context, string) (map[uint64]struct{}, error)
	BatchIndex(context.Context, map[string]struct{}) error
}

// LocalNodeServices groups the service surfaces needed by inabox-standard.
type LocalNodeServices struct {
	BitmapIndex  LocalBitmapIndexService
	KVStore      LocalKVStoreService
	StringSearch LocalStringSearchService
	Observer     LocalNodeObserver
}

// Ready reports whether the minimum read-query local service surface is present.
func (s LocalNodeServices) Ready() bool {
	return s.BitmapIndex != nil && s.KVStore != nil
}

// Readiness returns a structured local-node boundary status.
func (s LocalNodeServices) Readiness() LocalNodeReadiness {
	readiness := LocalNodeReadiness{
		Transport:      NodeTransportLocal,
		BitmapIndex:    s.BitmapIndex != nil,
		KVStore:        s.KVStore != nil,
		StringSearch:   s.StringSearch != nil,
		StreamingRisks: DefaultLocalNodeStreamingRisks(),
	}
	readiness.Ready = readiness.BitmapIndex && readiness.KVStore
	if !readiness.BitmapIndex {
		readiness.Blockers = append(readiness.Blockers, "local BitmapIndex service is not mounted")
	}
	if !readiness.KVStore {
		readiness.Blockers = append(readiness.Blockers, "local KVStore service is not mounted")
	}
	if !readiness.StringSearch {
		readiness.Warnings = append(readiness.Warnings, "local StringSearch service is not mounted; searchable string predicates remain a follow-up")
	}
	return readiness
}

// LocalNodeReadiness summarizes whether the in-process node boundary can serve
// the first inabox-standard vertical slice.
type LocalNodeReadiness struct {
	Transport      NodeTransportKind
	BitmapIndex    bool
	KVStore        bool
	StringSearch   bool
	Ready          bool
	Blockers       []string
	Warnings       []string
	StreamingRisks []LocalNodeStreamingRisk
}

// LocalNodeStreamingRisk tracks gRPC streaming APIs that still need local
// equivalents before inabox-standard can own the full ingest/search path.
type LocalNodeStreamingRisk struct {
	Service string
	Method  string
	Risk    string
	Gate    string
}

// DefaultLocalNodeStreamingRisks returns the known stream-shaped node calls.
func DefaultLocalNodeStreamingRisks() []LocalNodeStreamingRisk {
	return []LocalNodeStreamingRisk{
		{
			Service: "StringSearch",
			Method:  "BatchIndex/Search",
			Risk:    "search indexing and search results currently use streaming semantics",
			Gate:    "provide local string-search helpers before searchable text fields are complete in inabox-standard",
		},
	}
}

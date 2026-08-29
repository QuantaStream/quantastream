package qsloader

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/golang/protobuf/ptypes/empty"
)

func TestLoadSelectorTablesUsesConfiguredAllowlist(t *testing.T) {
	cache := core.NewTableCacheStruct()
	tables, err := loadSelectorTables(cache, Config{
		ConfigDir: "../tpc-h-benchmark/config",
		Database:  "quanta",
		Tables:    []string{"orders", "lineitem"},
	})
	if err != nil {
		t.Fatalf("loadSelectorTables() error = %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("tables = %d, want 2", len(tables))
	}
	if tables[0].Name != "lineitem" || tables[1].Name != "orders" {
		t.Fatalf("tables = %s/%s, want lineitem/orders sorted", tables[0].Name, tables[1].Name)
	}
	for _, table := range tables {
		if table.Selector == "" {
			t.Fatalf("table %s selector empty", table.Name)
		}
	}
}

func TestPayloadDataMapPrefersDataWrapper(t *testing.T) {
	data := map[string]interface{}{"id": int64(7)}
	payload := map[string]interface{}{"type": "orders", "data": data}
	if got := payloadDataMap(payload); got["id"] != int64(7) {
		t.Fatalf("payloadDataMap() = %#v, want data wrapper", got)
	}
}

func TestStatsEndpointReturnsLoaderSnapshot(t *testing.T) {
	putProfile := &core.RouterPutRowProfile{}
	putProfile.Observe("shard0", core.IngestRecord{TableName: "orders"}, core.PutRowResult{
		TableName:       "orders",
		Inserted:        true,
		LogicalRowCount: 1,
		TotalElapsed:    2 * time.Millisecond,
	})
	flushProfile := &core.RouterFlushProfile{}
	flushProfile.Observe("shard0", "orders", shared.BatchBufferFlushProfile{
		TotalElapsed:        3 * time.Millisecond,
		BitmapSetEntryCount: 4,
	})
	router := &core.SessionRouter{
		// Constructed directly so this test does not open worker goroutines.
	}
	server := &Server{
		config: Config{
			ListenAddress:        "127.0.0.1:8088",
			ConnectionMode:       shared.LoaderConnectionStandardNative,
			NativeGRPCAddr:       "127.0.0.1:4100",
			Workers:              1,
			ChannelSize:          100,
			FlushInterval:        time.Second,
			CommitOnClose:        true,
			PhysicalBuildRouting: true,
		},
		tables: []*core.Table{
			{BasicTable: &shared.BasicTable{Name: "orders"}},
			{BasicTable: &shared.BasicTable{Name: "lineitem"}},
		},
		router:       router,
		putProfile:   putProfile,
		flushProfile: flushProfile,
		drainProfile: &core.RouterDrainProfile{},
		startedAt:    time.Now().Add(-time.Second).UTC(),
	}
	server.accepted.Add(1)
	server.failed.Add(2)
	server.committed.Add(3)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/stats", nil)
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("/stats status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	var stats StatsResponse
	if err := json.Unmarshal(response.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode /stats: %v", err)
	}
	if stats.Status != "ok" {
		t.Fatalf("status = %q, want ok", stats.Status)
	}
	if stats.Config.NativeGRPCAddr != "127.0.0.1:4100" || !stats.Config.PhysicalBuildRouting {
		t.Fatalf("config = %+v, want native grpc and physical routing", stats.Config)
	}
	if !stats.Config.CommitOnClose {
		t.Fatalf("commit_on_close = false, want true")
	}
	if len(stats.Tables) != 2 || stats.Tables[0] != "lineitem" || stats.Tables[1] != "orders" {
		t.Fatalf("tables = %v, want sorted lineitem/orders", stats.Tables)
	}
	if stats.PutRow.RecordCount != 1 || stats.PutRow.InsertedCount != 1 {
		t.Fatalf("putrow = %+v, want one inserted record", stats.PutRow)
	}
	if stats.Flush.FlushCount != 1 || stats.Flush.BitmapSetEntryCount != 4 {
		t.Fatalf("flush = %+v, want one bitmap flush", stats.Flush)
	}
	if stats.Pipeline.Accepted != 1 || stats.Pipeline.Failed != 2 || stats.Pipeline.Processed != 1 ||
		stats.Pipeline.Flushed != 1 || stats.Pipeline.Committed != 3 {
		t.Fatalf("pipeline = %+v, want accepted/failed/processed/flushed/committed counters", stats.Pipeline)
	}
	if stats.Runtime.Goroutines == 0 {
		t.Fatalf("runtime goroutines = 0, want positive")
	}
}

func TestFlushEndpointFlushesRouterAndReturnsStats(t *testing.T) {
	router, err := core.NewSessionRouter(core.SessionRouterConfig{
		TableCache:    core.NewTableCacheStruct(),
		Conn:          &shared.Conn{},
		ShardCount:    2,
		ChannelSize:   1,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewSessionRouter() error = %v", err)
	}
	defer func() { _ = router.Close() }()
	server := &Server{
		config:       Config{Workers: 2, ChannelSize: 1, FlushInterval: time.Hour},
		router:       router,
		putProfile:   &core.RouterPutRowProfile{},
		flushProfile: &core.RouterFlushProfile{},
		drainProfile: &core.RouterDrainProfile{},
		startedAt:    time.Now().Add(-time.Second).UTC(),
	}
	server.accepted.Add(7)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/flush", nil)
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("/flush status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	var body struct {
		Status string                 `json:"status"`
		Flush  core.RouterFlushResult `json:"flush"`
		Stats  StatsResponse          `json:"stats"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode /flush: %v", err)
	}
	if body.Status != "ok" || body.Flush.ShardCount != 2 || body.Flush.ErrorCount != 0 {
		t.Fatalf("/flush body = %+v, want ok two-shard flush", body)
	}
	if body.Stats.Pipeline.Accepted != 7 || body.Stats.Pipeline.PendingQueued != 0 {
		t.Fatalf("/flush stats pipeline = %+v, want accepted and no pending queue", body.Stats.Pipeline)
	}
}

func TestCommitEndpointFlushesThenCommits(t *testing.T) {
	local := &testLoaderBitmapIndexService{}
	router, err := core.NewSessionRouter(core.SessionRouterConfig{
		TableCache:    core.NewTableCacheStruct(),
		Conn:          &shared.Conn{LocalNodeServices: shared.LocalNodeServices{BitmapIndex: local}},
		ShardCount:    1,
		ChannelSize:   1,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewSessionRouter() error = %v", err)
	}
	defer func() { _ = router.Close() }()
	server := &Server{
		config:       Config{Workers: 1, ChannelSize: 1, FlushInterval: time.Hour},
		router:       router,
		putProfile:   &core.RouterPutRowProfile{},
		flushProfile: &core.RouterFlushProfile{},
		drainProfile: &core.RouterDrainProfile{},
		startedAt:    time.Now().Add(-time.Second).UTC(),
	}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/commit", nil)
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("/commit status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	var body struct {
		Status string                  `json:"status"`
		Flush  core.RouterFlushResult  `json:"flush"`
		Commit core.RouterCommitResult `json:"commit"`
		Stats  StatsResponse           `json:"stats"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode /commit: %v", err)
	}
	if body.Status != "ok" || body.Flush.ShardCount != 1 || body.Commit.CommitCount != 1 {
		t.Fatalf("/commit body = %+v, want flushed commit", body)
	}
	if body.Stats.Pipeline.Committed != 1 || local.commitCalls != 1 {
		t.Fatalf("committed stats/local = %d/%d, want one commit", body.Stats.Pipeline.Committed, local.commitCalls)
	}
}

type testLoaderBitmapIndexService struct {
	commitCalls int
}

func (s *testLoaderBitmapIndexService) Query(context.Context, *pb.BitmapQuery) (*pb.QueryResult, error) {
	return &pb.QueryResult{}, nil
}

func (s *testLoaderBitmapIndexService) SyncStatus(context.Context, *pb.SyncStatusRequest) (*pb.SyncStatusResponse, error) {
	return &pb.SyncStatusResponse{}, nil
}

func (s *testLoaderBitmapIndexService) Projection(context.Context, *pb.ProjectionRequest) (*pb.ProjectionResponse, error) {
	return &pb.ProjectionResponse{}, nil
}

func (s *testLoaderBitmapIndexService) CompareBSIFields(context.Context, *pb.CompareBSIFieldsRequest) (*pb.CompareBSIFieldsResponse, error) {
	return &pb.CompareBSIFieldsResponse{}, nil
}

func (s *testLoaderBitmapIndexService) BitmapGroupAggregates(context.Context, *pb.BitmapGroupAggregatesRequest) (*pb.BitmapGroupAggregatesResponse, error) {
	return &pb.BitmapGroupAggregatesResponse{}, nil
}

func (s *testLoaderBitmapIndexService) RelationshipReverseArtifactCandidates(context.Context, *pb.RelationshipReverseArtifactCandidatesRequest) (*pb.RelationshipReverseArtifactCandidatesResponse, error) {
	return &pb.RelationshipReverseArtifactCandidatesResponse{}, nil
}

func (s *testLoaderBitmapIndexService) RelationshipReverseArtifactStats(context.Context, *pb.RelationshipReverseArtifactStatsRequest) (*pb.RelationshipReverseArtifactStatsResponse, error) {
	return &pb.RelationshipReverseArtifactStatsResponse{}, nil
}

func (s *testLoaderBitmapIndexService) RelationshipAlignedValueSum(context.Context, *pb.RelationshipAlignedValueSumRequest) (*pb.RelationshipAlignedValueSumResponse, error) {
	return &pb.RelationshipAlignedValueSumResponse{}, nil
}

func (s *testLoaderBitmapIndexService) RelationshipVectorValueSum(context.Context, *pb.RelationshipVectorValueSumRequest) (*pb.RelationshipVectorValueSumResponse, error) {
	return &pb.RelationshipVectorValueSumResponse{}, nil
}

func (s *testLoaderBitmapIndexService) Join(context.Context, *pb.JoinRequest) (*pb.JoinResponse, error) {
	return &pb.JoinResponse{}, nil
}

func (s *testLoaderBitmapIndexService) CheckoutSequence(context.Context, *pb.CheckoutSequenceRequest) (*pb.CheckoutSequenceResponse, error) {
	return &pb.CheckoutSequenceResponse{}, nil
}

func (s *testLoaderBitmapIndexService) BulkClear(context.Context, *pb.BulkClearRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (s *testLoaderBitmapIndexService) TableOperation(context.Context, *pb.TableOperationRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (s *testLoaderBitmapIndexService) Commit(context.Context, *empty.Empty) (*empty.Empty, error) {
	s.commitCalls++
	return &empty.Empty{}, nil
}

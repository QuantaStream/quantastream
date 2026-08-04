package qsinabox

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsfixture"
	"github.com/QuantaStream/quantastream/shared"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestMountStandardProcessBuildsReadyNativeFrontDoor(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	config := StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	if !process.FrontDoor.Ready() {
		t.Fatalf("front door summary = %#v, want ready", process.FrontDoor.Summary())
	}
}

func TestStandardProcessNativeGRPCServesNodeAPIs(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	config := StandardConfig{
		BindAddress:    "127.0.0.1",
		MySQLPort:      reserveStandardTestPort(t),
		NativeGRPCPort: reserveStandardTestPort(t),
		ConfigDir:      configDir,
		DataDir:        filepath.Join(root, "data"),
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	if process.NativeNode == nil {
		t.Fatalf("NativeNode = nil, want native gRPC listener")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := process.NativeNode.Start(ctx)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	clientConn, err := grpc.DialContext(dialCtx, process.NativeNode.Address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial native gRPC %s: %v", process.NativeNode.Address, err)
	}
	defer clientConn.Close()
	if _, err := pb.NewBitmapIndexClient(clientConn).Commit(context.Background(), &emptypb.Empty{}); err != nil {
		t.Fatalf("BitmapIndex.Commit() over native gRPC error = %v", err)
	}

	remoteConn, err := shared.NewSingleNodeConnection(dialCtx, "standard-native-test", process.NativeNode.Address)
	if err != nil {
		t.Fatalf("NewSingleNodeConnection() error = %v", err)
	}
	defer remoteConn.Disconnect()
	kv := shared.NewKVStore(remoteConn)
	if err := kv.BatchPut("sample/native_loader", map[interface{}]interface{}{"row-1": "loaded"}, false); err != nil {
		t.Fatalf("KVStore.BatchPut() over native gRPC error = %v", err)
	}
	got, err := kv.Lookup("sample/native_loader", "row-1", reflect.String, false)
	if err != nil {
		t.Fatalf("KVStore.Lookup() over native gRPC error = %v", err)
	}
	if got != "loaded" {
		t.Fatalf("KVStore.Lookup() = %#v, want loaded", got)
	}

	cancel()
	process.NativeNode.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("native gRPC server exited with error %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("native gRPC server did not stop")
	}
}

func TestStandardProcessNativeGRPCLoaderPutRowFlushesThroughBatchBuffer(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	config := StandardConfig{
		BindAddress:    "127.0.0.1",
		MySQLPort:      reserveStandardTestPort(t),
		NativeGRPCPort: reserveStandardTestPort(t),
		ConfigDir:      configDir,
		DataDir:        filepath.Join(root, "data"),
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := process.NativeNode.Start(ctx)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	remoteConn, err := shared.NewLoaderConnection(dialCtx, shared.LoaderConnectionConfig{
		Mode:    shared.LoaderConnectionStandardNative,
		Owner:   "standard-native-loader-test",
		Address: process.NativeNode.Address,
	})
	if err != nil {
		t.Fatalf("NewLoaderConnection() error = %v", err)
	}
	defer remoteConn.Disconnect()

	loaderSession, err := core.OpenSession(
		core.NewTableCacheStruct(),
		process.Backend.ConfigBaseDir(config),
		"sample",
		false,
		remoteConn,
	)
	if err != nil {
		t.Fatalf("OpenSession() over native gRPC error = %v", err)
	}
	loaderSessionClosed := false
	defer func() {
		if !loaderSessionClosed {
			_ = loaderSession.CloseSession()
		}
	}()

	putResult, err := loaderSession.PutRowWithOptions("sample", map[string]interface{}{
		"id":   101,
		"city": "Buenos Aires",
	}, 0, false, false, core.PutRowOptions{
		EventID:      "native-loader-smoke-101",
		Source:       "native-loader-smoke",
		EventTime:    time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
		SourceOffset: "unit-test:101",
	})
	if err != nil {
		t.Fatalf("PutRowWithOptions() over native gRPC error = %v", err)
	}
	if !putResult.Inserted || putResult.ColumnID == 0 {
		t.Fatalf("PutRowWithOptions() result = %+v, want inserted row with column ID", putResult)
	}
	if err := loaderSession.Flush(); err != nil {
		t.Fatalf("loader Flush() over native gRPC error = %v", err)
	}
	flushProfile := loaderSession.LastFlushProfile()
	if flushProfile.PartitionStringEntryCount == 0 || flushProfile.BSIValueEntryCount == 0 || flushProfile.TotalElapsed <= 0 {
		t.Fatalf("loader flush profile = %+v, want KV sidecar and BSI write activity", flushProfile)
	}

	result, err := process.FrontDoor.Server.ExecuteSQL(
		context.Background(),
		"select id, city from sample where id = 101",
		qsbridge.ExecutionOptions{},
	)
	if err != nil {
		t.Fatalf("verification SELECT error = %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("verification SELECT diagnostics = %#v runtime=%#v, want none", result.Diagnostics, result.Runtime.Diagnostics)
	}
	chunk, chunkDiagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if chunkDiagnostics.BlocksNative() {
		t.Fatalf("verification SELECT chunk diagnostics = %#v", chunkDiagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 2 {
		t.Fatalf("verification rows = %#v, want one projected row", chunk.Rows)
	}
	if fmt.Sprint(chunk.Rows[0][0].Value) != "101" || fmt.Sprint(chunk.Rows[0][1].Value) != "Buenos Aires" {
		t.Fatalf("verification row = %#v, want [101 Buenos Aires]", chunk.Rows[0])
	}
	if err := loaderSession.CloseSession(); err != nil {
		t.Fatalf("loader CloseSession() over native gRPC error = %v", err)
	}
	loaderSessionClosed = true

	cancel()
	process.NativeNode.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("native gRPC server exited with error %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("native gRPC server did not stop")
	}
}

func TestStandardProcessNativeGRPCLoaderIngestsTPCHNestedOrderLineitems(t *testing.T) {
	fixture, err := qsfixture.NewTPCHOrderLineitemEnvelopeFixture(qsfixture.TPCHOrderLineitemEnvelopeOptions{
		OrderCount:        2,
		LineitemsPerOrder: 3,
		StartedAt:         time.Date(1995, 3, 15, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("TPCH fixture error = %v", err)
	}

	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTPCHNestedSchemas(t, configDir)
	config := StandardConfig{
		BindAddress:    "127.0.0.1",
		MySQLPort:      reserveStandardTestPort(t),
		NativeGRPCPort: reserveStandardTestPort(t),
		ConfigDir:      configDir,
		DataDir:        filepath.Join(root, "data"),
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	if process.NativeNode == nil {
		t.Fatalf("NativeNode = nil, want native gRPC listener")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := process.NativeNode.Start(ctx)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	remoteConn, err := shared.NewLoaderConnection(dialCtx, shared.LoaderConnectionConfig{
		Mode:    shared.LoaderConnectionStandardNative,
		Owner:   "standard-native-tpch-loader-test",
		Address: process.NativeNode.Address,
	})
	if err != nil {
		t.Fatalf("NewLoaderConnection() error = %v", err)
	}
	defer remoteConn.Disconnect()

	loaderSession, err := core.OpenSession(
		core.NewTableCacheStruct(),
		process.Backend.ConfigBaseDir(config),
		"orders",
		true,
		remoteConn,
	)
	if err != nil {
		t.Fatalf("OpenSession(orders) over native gRPC error = %v", err)
	}
	loaderSessionClosed := false
	defer func() {
		if !loaderSessionClosed {
			_ = loaderSession.CloseSession()
		}
	}()

	ingestResult, ingestDiagnostics, err := qsfixture.IngestEnvelopesWithSession(qsfixture.SessionEnvelopeIngestRequest{
		Session:   loaderSession,
		Envelopes: fixture.Envelopes,
		RouteOptions: core.IngestEnvelopeRouteOptions{
			Tables: fixture.Tables,
		},
	})
	if ingestDiagnostics.BlocksNative() {
		t.Fatalf("ingest diagnostics = %#v, want none", ingestDiagnostics)
	}
	if err != nil {
		t.Fatalf("IngestEnvelopesWithSession() over native gRPC error = %v", err)
	}
	if len(ingestResult.Routes) != 2 || len(ingestResult.PutRows) != 2 {
		t.Fatalf("ingest result = %+v, want two routed PutRow results", ingestResult)
	}
	for i, route := range ingestResult.Routes {
		if route.Record.TableName != "orders" {
			t.Fatalf("route %d table = %s, want orders", i, route.Record.TableName)
		}
		putResult := ingestResult.PutRows[i]
		if !putResult.Inserted || putResult.ColumnID == 0 {
			t.Fatalf("PutRow result %d = %+v, want inserted order with column ID", i, putResult)
		}
	}
	if ingestResult.Profile.RecordCount != 2 || ingestResult.Profile.InsertedCount != 2 {
		t.Fatalf("ingest profile = %+v, want two inserted records", ingestResult.Profile)
	}
	flushProfile := ingestResult.FlushProfile
	if flushProfile.PartitionStringEntryCount == 0 || flushProfile.BSIValueEntryCount == 0 || flushProfile.TotalElapsed <= 0 {
		t.Fatalf("loader flush profile = %+v, want PK sidecar and BSI write activity", flushProfile)
	}

	requireStandardProcessScalarString(t, process, "select count(*) from orders", "2")
	requireStandardProcessScalarString(t, process, "select count(*) from lineitem", "6")
	requireStandardProcessScalarString(t, process, `
select count(*) as joined_lineitems
from orders as o
inner join lineitem as l on l.l_orderkey = o.o_orderkey`, "6")

	if err := loaderSession.CloseSession(); err != nil {
		t.Fatalf("loader CloseSession() over native gRPC error = %v", err)
	}
	loaderSessionClosed = true

	cancel()
	process.NativeNode.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("native gRPC server exited with error %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("native gRPC server did not stop")
	}
}

func TestStandardProcessNativeGRPCRouterIngestsTPCHNestedOrderLineitems(t *testing.T) {
	result := runStandardProcessNativeGRPCRouterTPCHNestedOrderLineitems(t, standardTPCHRouterIngestScenario{
		OrderCount:        2,
		LineitemsPerOrder: 3,
		ShardCount:        1,
		SourceMode:        core.IngestSourceStream,
	})

	if result.PutProfile.RecordCount != 2 || result.PutProfile.InsertedCount != 2 {
		t.Fatalf("put profile = %+v, want two inserted records", result.PutProfile)
	}
	if result.PutProfile.ChildRowCount != 6 || result.PutProfile.LogicalRowCount != 8 {
		t.Fatalf("put profile = %+v, want six children and eight logical rows", result.PutProfile)
	}
	if result.PutProfile.ByTable["orders"].ChildRowCount != 6 || result.PutProfile.ByTable["orders"].LogicalRowCount != 8 {
		t.Fatalf("put table profile = %+v, want six children and eight logical rows", result.PutProfile.ByTable["orders"])
	}
	if result.FlushProfile.FlushCount != 1 || result.FlushProfile.PartitionStringEntryCount == 0 ||
		result.FlushProfile.BSIValueEntryCount == 0 {
		t.Fatalf("flush profile = %+v, want one write flush with PK sidecar and BSI activity", result.FlushProfile)
	}
	if result.FlushProfile.ByTable["orders"].FlushCount != 1 || result.FlushProfile.ByShard["shard0"].FlushCount != 1 {
		t.Fatalf("flush profile groups = %+v/%+v, want orders/shard0 flush",
			result.FlushProfile.ByTable, result.FlushProfile.ByShard)
	}
	for _, route := range result.Routes {
		if route.ShardKey.Mode != core.IngestShardKeyDedup {
			t.Fatalf("route shard mode = %s, want dedup", route.ShardKey.Mode)
		}
	}
}

func TestStandardProcessNativeGRPCRouterDistributesTPCHNestedOrderLineitemsAcrossShards(t *testing.T) {
	result := runStandardProcessNativeGRPCRouterTPCHNestedOrderLineitems(t, standardTPCHRouterIngestScenario{
		OrderCount:        18,
		LineitemsPerOrder: 2,
		ShardCount:        3,
		SourceMode:        core.IngestSourceStream,
	})

	if result.PutProfile.RecordCount != 18 || result.PutProfile.InsertedCount != 18 {
		t.Fatalf("put profile = %+v, want eighteen inserted records", result.PutProfile)
	}
	if result.PutProfile.ChildRowCount != 36 || result.PutProfile.LogicalRowCount != 54 {
		t.Fatalf("put profile = %+v, want thirty-six children and fifty-four logical rows", result.PutProfile)
	}
	if len(result.PutProfile.ByShard) < 2 {
		t.Fatalf("put profile by shard = %+v, want records routed across multiple shards", result.PutProfile.ByShard)
	}
	for shardID, counter := range result.PutProfile.ByShard {
		if counter.LogicalRowCount != counter.RecordCount*(1+2) {
			t.Fatalf("put profile shard %s = %+v, want logical rows per routed parent", shardID, counter)
		}
	}
	if len(result.FlushProfile.ByShard) < 2 {
		t.Fatalf("flush profile by shard = %+v, want writes flushed from multiple shards", result.FlushProfile.ByShard)
	}
}

func TestStandardProcessNativeGRPCRouterIngestsTPCHBatchEnvelopes(t *testing.T) {
	result := runStandardProcessNativeGRPCRouterTPCHNestedOrderLineitems(t, standardTPCHRouterIngestScenario{
		OrderCount:        2,
		LineitemsPerOrder: 2,
		ShardCount:        1,
		SourceMode:        core.IngestSourceBatch,
	})

	if result.PutProfile.RecordCount != 2 || result.PutProfile.InsertedCount != 2 {
		t.Fatalf("put profile = %+v, want two inserted records", result.PutProfile)
	}
	if result.PutProfile.ChildRowCount != 4 || result.PutProfile.LogicalRowCount != 6 {
		t.Fatalf("put profile = %+v, want four children and six logical rows", result.PutProfile)
	}
	for _, route := range result.Routes {
		if route.ShardKey.Mode != core.IngestShardKeyPrimaryKey {
			t.Fatalf("batch route shard mode = %s, want primary-key", route.ShardKey.Mode)
		}
		if route.Record.EventID != "" {
			t.Fatalf("batch route event ID = %s, want empty", route.Record.EventID)
		}
	}
}

type standardTPCHRouterIngestScenario struct {
	OrderCount        int
	LineitemsPerOrder int
	ShardCount        int
	SourceMode        core.IngestSourceMode
	BaseOrderKey      int64
}

type standardTPCHRouterIngestResult struct {
	Routes       []core.IngestRouteResult
	PutProfile   core.RouterPutRowProfileSummary
	FlushProfile core.RouterFlushProfileSummary
}

func runStandardProcessNativeGRPCRouterTPCHNestedOrderLineitems(tb testing.TB,
	scenario standardTPCHRouterIngestScenario) standardTPCHRouterIngestResult {

	tb.Helper()
	if scenario.OrderCount <= 0 {
		scenario.OrderCount = 1
	}
	if scenario.LineitemsPerOrder <= 0 {
		scenario.LineitemsPerOrder = 1
	}
	if scenario.ShardCount <= 0 {
		scenario.ShardCount = 1
	}
	if scenario.SourceMode == "" {
		scenario.SourceMode = core.IngestSourceStream
	}
	fixture, err := qsfixture.NewTPCHOrderLineitemEnvelopeFixture(qsfixture.TPCHOrderLineitemEnvelopeOptions{
		OrderCount:        scenario.OrderCount,
		LineitemsPerOrder: scenario.LineitemsPerOrder,
		BaseOrderKey:      scenario.BaseOrderKey,
		SourceMode:        scenario.SourceMode,
		StartedAt:         time.Date(1995, 3, 15, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		tb.Fatalf("TPCH fixture error = %v", err)
	}
	root := tb.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTPCHNestedSchemas(tb, configDir)
	config := StandardConfig{
		BindAddress:    "127.0.0.1",
		MySQLPort:      reserveStandardTestPort(tb),
		NativeGRPCPort: reserveStandardTestPort(tb),
		ConfigDir:      configDir,
		DataDir:        filepath.Join(root, "data"),
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		tb.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		tb.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	if process.NativeNode == nil {
		tb.Fatalf("NativeNode = nil, want native gRPC listener")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := process.NativeNode.Start(ctx)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	remoteConn, err := shared.NewLoaderConnection(dialCtx, shared.LoaderConnectionConfig{
		Mode:    shared.LoaderConnectionStandardNative,
		Owner:   "standard-native-tpch-router-test",
		Address: process.NativeNode.Address,
	})
	if err != nil {
		tb.Fatalf("NewLoaderConnection() error = %v", err)
	}
	defer remoteConn.Disconnect()

	putRowProfile := &core.RouterPutRowProfile{}
	flushProfile := &core.RouterFlushProfile{}
	router, err := core.NewSessionRouter(core.SessionRouterConfig{
		TableCache:     core.NewTableCacheStruct(),
		BasePath:       process.Backend.ConfigBaseDir(config),
		Conn:           remoteConn,
		ShardCount:     scenario.ShardCount,
		ChannelSize:    len(fixture.Envelopes),
		FlushInterval:  10 * time.Millisecond,
		OnPutRowResult: putRowProfile.Callback(),
		OnFlushProfile: flushProfile.Callback(),
	})
	if err != nil {
		tb.Fatalf("NewSessionRouter() error = %v", err)
	}

	routes := make([]core.IngestRouteResult, 0, len(fixture.Envelopes))
	for _, envelope := range fixture.Envelopes {
		route, routeDiagnostics, err := core.RouteSelectedIngestEnvelope(router, envelope, core.IngestEnvelopeRouteOptions{
			Tables: fixture.Tables,
		})
		if routeDiagnostics.BlocksNative() {
			tb.Fatalf("route diagnostics = %#v, want none", routeDiagnostics)
		}
		if err != nil {
			tb.Fatalf("RouteSelectedIngestEnvelope(%s) error = %v", envelope.EventID, err)
		}
		if !route.Enqueued || route.Record.TableName != "orders" {
			tb.Fatalf("route result = %+v, want enqueued orders record", route)
		}
		routes = append(routes, route)
	}
	if err := router.Close(); err != nil {
		tb.Fatalf("router Close() error = %v", err)
	}

	totalLineitems := scenario.OrderCount * scenario.LineitemsPerOrder
	requireStandardProcessScalarString(tb, process, "select count(*) from orders", fmt.Sprint(scenario.OrderCount))
	requireStandardProcessScalarString(tb, process, "select count(*) from lineitem", fmt.Sprint(totalLineitems))
	requireStandardProcessScalarString(tb, process, `
select count(*) as joined_lineitems
from orders as o
inner join lineitem as l on l.l_orderkey = o.o_orderkey`, fmt.Sprint(totalLineitems))

	cancel()
	process.NativeNode.Close()
	select {
	case err := <-done:
		if err != nil {
			tb.Fatalf("native gRPC server exited with error %v", err)
		}
	case <-time.After(5 * time.Second):
		tb.Fatalf("native gRPC server did not stop")
	}
	return standardTPCHRouterIngestResult{
		Routes:       routes,
		PutProfile:   putRowProfile.Snapshot(),
		FlushProfile: flushProfile.Snapshot(),
	}
}

func TestStandardProcessExecutesSQLThroughLocalFrontDoor(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	config := StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}

	result, err := process.FrontDoor.Server.ExecuteSQL(
		context.Background(),
		"select count(*) as row_count from sample where id >= 1",
		qsbridge.ExecutionOptions{},
	)
	if err != nil {
		t.Fatalf("ExecuteSQL() error = %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("ExecuteSQL() diagnostics = %#v runtime=%#v, want none", result.Diagnostics, result.Runtime.Diagnostics)
	}
	if result.Runtime.Count != 1 {
		t.Fatalf("ExecuteSQL() runtime count = %d, want one aggregate row", result.Runtime.Count)
	}
}

func reserveStandardTestPort(tb testing.TB) int {
	tb.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func TestStandardProcessExecutesGroupedBooleanFilterThroughLocalFrontDoor(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	config := StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}

	for _, sql := range []string{
		"insert into sample (id) values (1)",
		"insert into sample (id) values (2)",
		"commit",
	} {
		result, err := process.FrontDoor.Server.ExecuteSQL(context.Background(), sql, qsbridge.ExecutionOptions{})
		if err != nil {
			t.Fatalf("%s error = %v", sql, err)
		}
		if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
			t.Fatalf("%s diagnostics = %#v runtime=%#v, want none", sql, result.Diagnostics, result.Runtime.Diagnostics)
		}
	}

	result, err := process.FrontDoor.Server.ExecuteSQL(
		context.Background(),
		"select count(*) as row_count from sample where (id = 1 and id >= 1) or (id = 2 and id >= 2)",
		qsbridge.ExecutionOptions{},
	)
	if err != nil {
		t.Fatalf("grouped boolean SELECT error = %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("grouped boolean SELECT diagnostics = %#v runtime=%#v, want none", result.Diagnostics, result.Runtime.Diagnostics)
	}
	chunk, chunkDiagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if chunkDiagnostics.BlocksNative() {
		t.Fatalf("grouped boolean SELECT chunk diagnostics = %#v", chunkDiagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 1 || fmt.Sprint(chunk.Rows[0][0].Value) != "2" {
		t.Fatalf("grouped boolean SELECT rows = %#v, want count 2", chunk.Rows)
	}
}
func TestStandardProcessCreateAndDropTableMaintainCatalogObjects(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardDraftTestSchema(t, configDir, "sample")
	if err := shared.SaveCatalogObjectsFile(configDir, shared.CatalogObjectsFile{}); err != nil {
		t.Fatalf("write empty catalog objects: %v", err)
	}
	config := StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	if process.Backend.Adapter.BitmapIndex.GetTable("sample") != nil {
		t.Fatalf("sample should not be loaded before CREATE TABLE activates the manifest entry")
	}

	createResult, err := process.FrontDoor.Server.ExecuteSQL(context.Background(), "create table sample", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("CREATE TABLE error = %v", err)
	}
	if createResult.Diagnostics.BlocksNative() || createResult.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("CREATE TABLE diagnostics = %#v runtime=%#v", createResult.Diagnostics, createResult.Runtime.Diagnostics)
	}
	dataConfigDir := filepath.Join(config.DataDir, "config")
	active, err := shared.CatalogTableActive(dataConfigDir, "quanta", "sample")
	if err != nil {
		t.Fatalf("CatalogTableActive() error = %v", err)
	}
	if !active {
		t.Fatalf("sample should be active after CREATE TABLE")
	}
	if process.Backend.Adapter.BitmapIndex.GetTable("sample") == nil {
		t.Fatalf("sample should be deployed into the local bitmap index after CREATE TABLE")
	}

	insertResult, err := process.FrontDoor.Server.ExecuteSQL(context.Background(), "insert into sample (id, city) values (1, 'Seattle')", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("INSERT error = %v", err)
	}
	if insertResult.Diagnostics.BlocksNative() || insertResult.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("INSERT diagnostics = %#v runtime=%#v", insertResult.Diagnostics, insertResult.Runtime.Diagnostics)
	}
	commitResult, err := process.FrontDoor.Server.ExecuteSQL(context.Background(), "commit", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("COMMIT error = %v", err)
	}
	if commitResult.Diagnostics.BlocksNative() || commitResult.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("COMMIT diagnostics = %#v runtime=%#v", commitResult.Diagnostics, commitResult.Runtime.Diagnostics)
	}
	selectResult, err := process.FrontDoor.Server.ExecuteSQL(context.Background(), "select id, city from sample where id = 1", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("SELECT after INSERT error = %v", err)
	}
	if selectResult.Diagnostics.BlocksNative() || selectResult.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("SELECT after INSERT diagnostics = %#v runtime=%#v", selectResult.Diagnostics, selectResult.Runtime.Diagnostics)
	}
	chunk, chunkDiagnostics := selectResult.Runtime.RowSet.ToResultChunk(0, true)
	if chunkDiagnostics.BlocksNative() {
		t.Fatalf("SELECT chunk diagnostics = %#v", chunkDiagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 2 {
		t.Fatalf("SELECT chunk rows = %#v, want one projected row", chunk.Rows)
	}
	if fmt.Sprint(chunk.Rows[0][0].Value) != "1" || fmt.Sprint(chunk.Rows[0][1].Value) != "Seattle" {
		t.Fatalf("SELECT row = %#v, want [1 Seattle]", chunk.Rows[0])
	}

	dropResult, err := process.FrontDoor.Server.ExecuteSQL(context.Background(), "drop table sample", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("DROP TABLE error = %v", err)
	}
	if dropResult.Diagnostics.BlocksNative() || dropResult.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("DROP TABLE diagnostics = %#v runtime=%#v", dropResult.Diagnostics, dropResult.Runtime.Diagnostics)
	}
	active, err = shared.CatalogTableActive(dataConfigDir, "quanta", "sample")
	if err != nil {
		t.Fatalf("CatalogTableActive() after DROP error = %v", err)
	}
	if active {
		t.Fatalf("sample should not be active after DROP TABLE")
	}
	if process.Backend.Adapter.BitmapIndex.GetTable("sample") != nil {
		t.Fatalf("sample should be removed from the local bitmap index after DROP TABLE")
	}
}

func TestStandardProcessCatalogLifecycleEnforcesRelationships(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardDraftRelationshipSchemas(t, configDir)
	if err := shared.SaveCatalogObjectsFile(configDir, shared.CatalogObjectsFile{}); err != nil {
		t.Fatalf("write empty catalog objects: %v", err)
	}
	config := StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}

	requireStandardProcessSQLFailure(t, process, "create table orders", "missing parent FK constraint dependency", "")
	dataConfigDir := filepath.Join(config.DataDir, "config")
	active, err := shared.CatalogTableActive(dataConfigDir, "quanta", "orders")
	if err != nil {
		t.Fatalf("CatalogTableActive(orders) error = %v", err)
	}
	if active {
		t.Fatalf("orders should not be active after rejected child-before-parent CREATE TABLE")
	}

	requireStandardProcessSQLSuccess(t, process, "create table customers")
	requireStandardProcessSQLSuccess(t, process, "create table orders")
	requireStandardProcessSQLSuccess(t, process, "insert into customers (id, city) values ('C1', 'Seattle')")
	requireStandardProcessSQLSuccess(t, process, "insert into orders (order_id, cust_id) values ('O1', 'C1')")
	requireStandardProcessSQLSuccess(t, process, "commit")

	requireStandardProcessSQLFailure(t, process, "drop table customers", "cannot drop table with dependencies: orders", "")
	active, err = shared.CatalogTableActive(dataConfigDir, "quanta", "customers")
	if err != nil {
		t.Fatalf("CatalogTableActive(customers) error = %v", err)
	}
	if !active {
		t.Fatalf("customers should remain active after rejected parent DROP TABLE")
	}

	requireStandardProcessSQLFailure(
		t,
		process,
		"truncate table customers",
		"cannot truncate parent table customers while child table orders contains 1 row(s)",
		qsbridge.DiagnosticTruncateChildDataExists,
	)
}

func requireStandardProcessSQLSuccess(t *testing.T, process StandardProcess, sql string) {
	t.Helper()
	result, err := process.FrontDoor.Server.ExecuteSQL(context.Background(), sql, qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("%s error = %v", sql, err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("%s diagnostics = %#v runtime=%#v, want none", sql, result.Diagnostics, result.Runtime.Diagnostics)
	}
}

func requireStandardProcessScalarString(tb testing.TB, process StandardProcess, sql string, want string) {
	tb.Helper()
	result, err := process.FrontDoor.Server.ExecuteSQL(context.Background(), sql, qsbridge.ExecutionOptions{})
	if err != nil {
		tb.Fatalf("%s error = %v", sql, err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		tb.Fatalf("%s diagnostics = %#v runtime=%#v, want none", sql, result.Diagnostics, result.Runtime.Diagnostics)
	}
	chunk, chunkDiagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if chunkDiagnostics.BlocksNative() {
		tb.Fatalf("%s chunk diagnostics = %#v", sql, chunkDiagnostics)
	}
	if len(chunk.Rows) != 1 || len(chunk.Rows[0]) != 1 {
		tb.Fatalf("%s rows = %#v, want one scalar value", sql, chunk.Rows)
	}
	if got := fmt.Sprint(chunk.Rows[0][0].Value); got != want {
		tb.Fatalf("%s scalar = %s, want %s", sql, got, want)
	}
}

func requireStandardProcessSQLFailure(t *testing.T, process StandardProcess, sql string, wantMessage string, wantCode qsbridge.DiagnosticCode) {
	t.Helper()
	result, err := process.FrontDoor.Server.ExecuteSQL(context.Background(), sql, qsbridge.ExecutionOptions{})
	if err != nil {
		if wantMessage != "" && !strings.Contains(err.Error(), wantMessage) {
			t.Fatalf("%s error = %v, want message containing %q", sql, err, wantMessage)
		}
		return
	}
	diagnostics := append(qsbridge.DiagnosticSet(nil), result.Diagnostics...)
	diagnostics = append(diagnostics, result.Runtime.Diagnostics...)
	if !diagnostics.BlocksNative() {
		t.Fatalf("%s unexpectedly succeeded: result=%#v", sql, result)
	}
	if wantMessage != "" {
		found := false
		for _, diagnostic := range diagnostics {
			if strings.Contains(diagnostic.Message, wantMessage) {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s diagnostics = %#v, want message containing %q", sql, diagnostics, wantMessage)
		}
	}
	if wantCode != "" {
		found := false
		for _, code := range diagnostics.Codes() {
			if code == wantCode {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s diagnostic codes = %#v, want %s", sql, diagnostics.Codes(), wantCode)
		}
	}
}

func writeStandardDraftRelationshipSchemas(t *testing.T, configDir string) {
	t.Helper()
	writeStandardRelationshipSchema(t, configDir, "customers", "")
	writeStandardRelationshipSchema(t, configDir, "orders", "customers")
}

func writeStandardRelationshipSchema(t *testing.T, configDir, table, foreignKey string) {
	t.Helper()
	tableDir := filepath.Join(configDir, table)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		t.Fatalf("mkdir schema dir: %v", err)
	}
	schema := `tableName: ` + table + `
primaryKey: id
attributes:
- fieldName: id
  sourceName: /id
  mappingStrategy: StringHashBSI
  type: String
- fieldName: city
  sourceName: /city
  mappingStrategy: StringHashBSI
  type: String
`
	if foreignKey != "" {
		schema = `tableName: ` + table + `
primaryKey: order_id
attributes:
- fieldName: order_id
  sourceName: /order_id
  mappingStrategy: StringHashBSI
  type: String
- fieldName: cust_id
  sourceName: /cust_id
  mappingStrategy: ParentRelation
  type: String
  foreignKey: ` + foreignKey + `
`
	}
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), []byte(schema), 0644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
}

func writeStandardTPCHNestedSchemas(tb testing.TB, configDir string) {
	tb.Helper()
	now := time.Now().UTC()
	writeStandardTPCHNestedSchema(tb, configDir, "orders", `tableName: orders
primaryKey: o_orderkey
selector: type="orders"
attributes:
- fieldName: o_orderkey
  sourceName: /data/o_orderkey
  mappingStrategy: IntBSI
  type: Integer
  columnID: true
- fieldName: o_orderstatus
  sourceName: /data/o_orderstatus
  mappingStrategy: StringLexBSI
  configuration:
    length: "1"
  type: String
- sourceName: /data/lineitems
  childTable: lineitem
  mappingStrategy: ChildRelation
`)
	writeStandardTPCHNestedSchema(tb, configDir, "lineitem", `tableName: lineitem
primaryKey: l_orderkey+l_linenumber
selector: type="lineitem"
attributes:
- fieldName: l_orderkey
  sourceName: l_orderkey
  mappingStrategy: ParentRelation
  foreignKey: orders
  type: Integer
- fieldName: l_linenumber
  sourceName: l_linenumber
  mappingStrategy: IntBSI
  type: Integer
- fieldName: l_quantity
  sourceName: l_quantity
  mappingStrategy: IntBSI
  type: Integer
`)
	if err := shared.ActivateCatalogTable(configDir, "quanta", "orders", now); err != nil {
		tb.Fatalf("activate orders catalog object: %v", err)
	}
	if err := shared.ActivateCatalogTable(configDir, "quanta", "lineitem", now); err != nil {
		tb.Fatalf("activate lineitem catalog object: %v", err)
	}
}

func writeStandardTPCHNestedSchema(tb testing.TB, configDir, table, schema string) {
	tb.Helper()
	tableDir := filepath.Join(configDir, table)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		tb.Fatalf("mkdir schema dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), []byte(schema), 0644); err != nil {
		tb.Fatalf("write %s schema: %v", table, err)
	}
}

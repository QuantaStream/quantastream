package qsinabox

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	pb "github.com/QuantaStream/quantastream/grpc"
	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsfixture"
	"github.com/QuantaStream/quantastream/shared"
	"github.com/RoaringBitmap/roaring/v2/roaring64"
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

func TestMountStandardProcessEnablesWriteAheadLogWhenConfigured(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	walPath := filepath.Join(root, "wal", "standard.wal")
	config := StandardConfig{
		ConfigDir:         configDir,
		DataDir:           filepath.Join(root, "data"),
		WriteAheadLogPath: walPath,
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	if process.RuntimeMount.WriteAheadLog == nil {
		t.Fatalf("WriteAheadLog = nil, want configured WAL")
	}
	if process.RuntimeMount.WriteAheadLog.Path() != walPath {
		t.Fatalf("WAL path = %s, want %s", process.RuntimeMount.WriteAheadLog.Path(), walPath)
	}
	if process.RuntimeMount.WriteAheadLogRecovery.WALPath != walPath {
		t.Fatalf("WAL recovery path = %s, want %s", process.RuntimeMount.WriteAheadLogRecovery.WALPath, walPath)
	}
	if process.RuntimeMount.Pool == nil || process.RuntimeMount.Pool.WriteAheadLog() == nil {
		t.Fatalf("session pool did not receive configured WAL")
	}
}

func TestMountStandardProcessReplaysCommittedWriteAheadLogTail(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	walPath := filepath.Join(root, "wal", "standard.wal")
	wal, err := core.OpenLocalWALWithOptions(walPath, core.LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenLocalWALWithOptions() error = %v", err)
	}
	if _, err := wal.Append(context.Background(), core.LocalWALRecord{
		OperationID: "put-sample-101",
		Kind:        core.LocalWALRecordKindPutRow,
		Table:       "sample",
		Payload: json.RawMessage(`{
			"table": "sample",
			"primary_key_mode": "verify_existing",
			"options": {},
			"row": {"id": 101, "city": "Lima"}
		}`),
	}); err != nil {
		t.Fatalf("append WAL put_row: %v", err)
	}
	commitRecord, err := wal.Append(context.Background(), core.LocalWALRecord{
		OperationID: "commit-sample-101",
		Kind:        core.LocalWALRecordKindCommit,
	})
	if err != nil {
		t.Fatalf("append WAL commit: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close WAL returned error: %v", err)
	}

	config := StandardConfig{
		ConfigDir:         configDir,
		DataDir:           filepath.Join(root, "data"),
		WriteAheadLogPath: walPath,
	}
	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	if process.RuntimeMount.WriteAheadLogReplay.ReplayRecordCount != 2 ||
		process.RuntimeMount.WriteAheadLogReplay.PutRowCount != 1 ||
		process.RuntimeMount.WriteAheadLogReplay.CommitBoundaryCount != 1 {
		t.Fatalf("WAL replay summary = %+v, want one put and one commit", process.RuntimeMount.WriteAheadLogReplay)
	}
	if process.RuntimeMount.WriteAheadLogReplay.CheckpointLSN != commitRecord.LSN {
		t.Fatalf("replay checkpoint LSN = %d, want %d", process.RuntimeMount.WriteAheadLogReplay.CheckpointLSN, commitRecord.LSN)
	}
	loadedCheckpoint, found, err := core.LoadLocalWALCheckpoint(walPath + ".checkpoint.json")
	if err != nil {
		t.Fatalf("LoadLocalWALCheckpoint() error = %v", err)
	}
	if !found || loadedCheckpoint.LastCommittedLSN != commitRecord.LSN {
		t.Fatalf("checkpoint found=%t checkpoint=%+v, want LSN %d", found, loadedCheckpoint, commitRecord.LSN)
	}
	manifest, err := core.LoadBSIPrimaryKeyAuthorityManifest(config.DataDir)
	if err != nil {
		t.Fatalf("LoadBSIPrimaryKeyAuthorityManifest() error = %v", err)
	}
	if manifest.Source != "standard-wal-replay" || len(manifest.Entries) != 1 {
		t.Fatalf("manifest after WAL replay = %+v, want one standard-wal-replay entry", manifest)
	}
	requireStandardProcessScalarString(t, process, "select count(*) from sample where id = 101", "1")
}

func TestMountStandardProcessBlocksOnWriteAheadLogReplayFailure(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	walPath := filepath.Join(root, "wal", "standard.wal")
	wal, err := core.OpenLocalWALWithOptions(walPath, core.LocalWALOptions{SyncOnAppend: false})
	if err != nil {
		t.Fatalf("OpenLocalWALWithOptions() error = %v", err)
	}
	if _, err := wal.Append(context.Background(), core.LocalWALRecord{
		OperationID: "put-missing-table",
		Kind:        core.LocalWALRecordKindPutRow,
		Table:       "missing_table",
		Payload: json.RawMessage(`{
			"table": "missing_table",
			"options": {},
			"row": {"id": 101}
		}`),
	}); err != nil {
		t.Fatalf("append WAL put_row: %v", err)
	}
	if _, err := wal.Append(context.Background(), core.LocalWALRecord{
		OperationID: "commit-missing-table",
		Kind:        core.LocalWALRecordKindCommit,
	}); err != nil {
		t.Fatalf("append WAL commit: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close WAL returned error: %v", err)
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), StandardConfig{
		ConfigDir:         configDir,
		DataDir:           filepath.Join(root, "data"),
		WriteAheadLogPath: walPath,
	})
	if err == nil {
		process.Close()
		t.Fatalf("MountStandardProcess() error = nil, want replay failure")
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("diagnostics = %#v, expected mount error before runtime diagnostics", diagnostics)
	}
	if !strings.Contains(err.Error(), "enable inabox-standard WAL") || !strings.Contains(err.Error(), "missing_table") {
		t.Fatalf("MountStandardProcess() error = %v, want WAL replay missing_table failure", err)
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

	tableCache := core.NewTableCacheStruct()
	loaderSession, err := core.OpenSession(
		tableCache,
		process.Backend.ConfigBaseDir(config),
		"sample",
		false,
		remoteConn,
	)
	if err != nil {
		t.Fatalf("OpenSession() over native gRPC error = %v", err)
	}
	loaderSession.SetPrimaryKeyResolver(NewStandardSessionBSIPrimaryKeyResolverFactory(tableCache)(loaderSession))
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

func TestStandardProcessNativeGRPCLoaderRefreshesStringEnumDictionary(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardStringEnumTestSchema(t, configDir, "sample_enum")
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
		Owner:   "standard-native-enum-loader-test",
		Address: process.NativeNode.Address,
	})
	if err != nil {
		t.Fatalf("NewLoaderConnection() error = %v", err)
	}
	defer remoteConn.Disconnect()

	tableCache := core.NewTableCacheStruct()
	loaderSession, err := core.OpenSession(
		tableCache,
		process.Backend.ConfigBaseDir(config),
		"sample_enum",
		false,
		remoteConn,
	)
	if err != nil {
		t.Fatalf("OpenSession() over native gRPC error = %v", err)
	}
	loaderSession.SetPrimaryKeyResolver(NewStandardSessionBSIPrimaryKeyResolverFactory(tableCache)(loaderSession))
	loaderSessionClosed := false
	defer func() {
		if !loaderSessionClosed {
			_ = loaderSession.CloseSession()
		}
	}()

	putResult, err := loaderSession.PutRowWithOptions("sample_enum", map[string]interface{}{
		"id":     101,
		"status": "READY",
	}, 0, false, false, core.PutRowOptions{
		EventID:      "native-loader-enum-101",
		Source:       "native-loader-enum",
		EventTime:    time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC),
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

	requireStandardProcessScalarString(t, process, "select count(*) from sample_enum where status = 'READY'", "1")
	result, err := process.FrontDoor.Server.ExecuteSQL(
		context.Background(),
		"select id, status from sample_enum where id = 101",
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
	if fmt.Sprint(chunk.Rows[0][0].Value) != "101" || fmt.Sprint(chunk.Rows[0][1].Value) != "READY" {
		t.Fatalf("verification row = %#v, want [101 READY]", chunk.Rows[0])
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

	tableCache := core.NewTableCacheStruct()
	loaderSession, err := core.OpenSession(
		tableCache,
		process.Backend.ConfigBaseDir(config),
		"orders",
		true,
		remoteConn,
	)
	if err != nil {
		t.Fatalf("OpenSession(orders) over native gRPC error = %v", err)
	}
	loaderSession.SetPrimaryKeyResolver(NewStandardSessionBSIPrimaryKeyResolverFactory(tableCache)(loaderSession))
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
	if result.FlushProfile.FlushCount == 0 || result.FlushProfile.PartitionStringEntryCount == 0 ||
		result.FlushProfile.BSIValueEntryCount == 0 {
		t.Fatalf("flush profile = %+v, want write flushes with PK sidecar and BSI activity", result.FlushProfile)
	}
	if result.FlushProfile.ByTable["orders"].FlushCount == 0 || result.FlushProfile.ByShard["shard0"].FlushCount == 0 {
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

func TestStandardProcessNativeGRPCRouterDefaultsToBSIPrimaryKeyAuthority(t *testing.T) {
	result := runStandardProcessNativeGRPCRouterTPCHNestedOrderLineitems(t, standardTPCHRouterIngestScenario{
		OrderCount:        2,
		LineitemsPerOrder: 3,
		ShardCount:        1,
		SourceMode:        core.IngestSourceStream,
		ReplayCount:       2,
	})

	requirePrimaryKeyTableProfile(t, result.PutProfile, "orders", core.PrimaryKeyResolveProfile{
		ResolveCount:        4,
		DirectColumnIDCount: 4,
	})
	requirePrimaryKeyTableProfile(t, result.PutProfile, "lineitem", core.PrimaryKeyResolveProfile{
		ResolveCount:             12,
		LookupRequiredCount:      12,
		BSILookupCount:           6,
		BSIHitCount:              6,
		SkippedBSILookupCount:    6,
		EmptyDomainProbeCount:    2,
		EmptyDomainSkipCount:     6,
		EmptyDomainNonEmptyCount: 6,
		BSIStageWriteCount:       6,
	})
}

func TestStandardProcessNativeGRPCRouterReplayUsesBSIPrimaryKeyAuthority(t *testing.T) {
	orderCount := 2
	lineitemsPerOrder := 3
	replayCount := 2
	primaryKeyBackend := qsfixture.NewMemoryBSIPrimaryKeyBackend()

	result := runStandardProcessNativeGRPCRouterTPCHNestedOrderLineitems(t, standardTPCHRouterIngestScenario{
		OrderCount:        orderCount,
		LineitemsPerOrder: lineitemsPerOrder,
		ShardCount:        1,
		SourceMode:        core.IngestSourceStream,
		ReplayCount:       replayCount,
		PrimaryKeyResolverFactory: func(_ *core.Session) core.PrimaryKeyResolver {
			return core.NewBSIPrimaryKeyResolver(primaryKeyBackend)
		},
	})

	requireBSIPrimaryKeyAuthorityReplayProfile(t, result, primaryKeyBackend, orderCount, lineitemsPerOrder, replayCount)
}

func TestStandardProcessNativeGRPCRouterReplayProfilesConcretePrimaryKeyAuthorityByTable(t *testing.T) {
	orderCount := 2
	lineitemsPerOrder := 3
	replayCount := 2

	result := runStandardProcessNativeGRPCRouterTPCHNestedOrderLineitems(t, standardTPCHRouterIngestScenario{
		OrderCount:                orderCount,
		LineitemsPerOrder:         lineitemsPerOrder,
		ShardCount:                1,
		SourceMode:                core.IngestSourceStream,
		ReplayCount:               replayCount,
		PrimaryKeyResolverFactory: NewStandardSessionBSIPrimaryKeyResolverFactory(core.NewTableCacheStruct()),
	})

	expectedOrderResolves := orderCount * replayCount
	expectedLineitemResolves := orderCount * lineitemsPerOrder * replayCount
	requirePrimaryKeyTableProfile(t, result.PutProfile, "orders", core.PrimaryKeyResolveProfile{
		ResolveCount:        expectedOrderResolves,
		DirectColumnIDCount: expectedOrderResolves,
	})
	requirePrimaryKeyTableProfile(t, result.PutProfile, "lineitem", core.PrimaryKeyResolveProfile{
		ResolveCount:             expectedLineitemResolves,
		LookupRequiredCount:      expectedLineitemResolves,
		BSILookupCount:           orderCount * lineitemsPerOrder,
		BSIHitCount:              orderCount * lineitemsPerOrder,
		SkippedBSILookupCount:    orderCount * lineitemsPerOrder,
		EmptyDomainProbeCount:    2,
		EmptyDomainSkipCount:     orderCount * lineitemsPerOrder,
		EmptyDomainNonEmptyCount: orderCount * lineitemsPerOrder,
		BSIStageWriteCount:       orderCount * lineitemsPerOrder,
	})
	requirePrimaryKeyTableProjectionCacheProfile(t, result.PutProfile, "lineitem",
		1, 0)
}

func TestStandardProcessCompoundBSIPrimaryKeyAuthoritySurvivesRestart(t *testing.T) {
	orderCount := 1
	lineitemsPerOrder := 2
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
	fixture, err := qsfixture.NewTPCHOrderLineitemEnvelopeFixture(qsfixture.TPCHOrderLineitemEnvelopeOptions{
		OrderCount:        orderCount,
		LineitemsPerOrder: lineitemsPerOrder,
		BaseOrderKey:      71001,
		SourceMode:        core.IngestSourceStream,
		StartedAt:         time.Date(1995, 3, 15, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("TPCH fixture error = %v", err)
	}

	first, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("first MountStandardProcess() error = %v", err)
	}
	if diagnostics.BlocksNative() {
		t.Fatalf("first MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	firstProfile := routeStandardProcessNativeTPCHEnvelopes(t, first, config, fixture, core.NewTableCacheStruct())
	requirePrimaryKeyTableProfile(t, firstProfile, "lineitem", core.PrimaryKeyResolveProfile{
		ResolveCount:          orderCount * lineitemsPerOrder,
		LookupRequiredCount:   orderCount * lineitemsPerOrder,
		SkippedBSILookupCount: orderCount * lineitemsPerOrder,
		EmptyDomainProbeCount: 1,
		EmptyDomainSkipCount:  orderCount * lineitemsPerOrder,
		BSIStageWriteCount:    orderCount * lineitemsPerOrder,
	})
	requirePrimaryKeyTableProjectionCacheProfile(t, firstProfile, "lineitem",
		0, 0)
	requireStandardProcessScalarString(t, first, "select count(*) from lineitem", fmt.Sprint(orderCount*lineitemsPerOrder))
	requireStandardProcessSQLSuccess(t, first, "commit")
	requireStandardProcessBSIPrimaryKeyAuthorityManifestArtifact(t, config, "standard-sql-commit")
	first.Close()
	requireStandardProcessBSIPrimaryKeyAuthorityManifestArtifact(t, config, "standard-backend-close")

	second, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("second MountStandardProcess() error = %v", err)
	}
	defer second.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("second MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	secondProfile := routeStandardProcessNativeTPCHEnvelopes(t, second, config, fixture, core.NewTableCacheStruct())
	requirePrimaryKeyTableProfile(t, secondProfile, "lineitem", core.PrimaryKeyResolveProfile{
		ResolveCount:             orderCount * lineitemsPerOrder,
		LookupRequiredCount:      orderCount * lineitemsPerOrder,
		BSILookupCount:           orderCount * lineitemsPerOrder,
		BSIHitCount:              orderCount * lineitemsPerOrder,
		EmptyDomainProbeCount:    1,
		EmptyDomainNonEmptyCount: orderCount * lineitemsPerOrder,
	})
	requirePrimaryKeyTableProjectionCacheProfile(t, secondProfile, "lineitem",
		1, 0)
	requireStandardProcessScalarString(t, second, "select count(*) from lineitem", fmt.Sprint(orderCount*lineitemsPerOrder))
}

func TestStandardProcessNativeGRPCRouterFlatTimeShardedLineitemReplayUsesCompoundAuthority(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTPCHFlatSchemas(t, configDir)
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
		Owner:   "standard-native-flat-tpch-router-test",
		Address: process.NativeNode.Address,
	})
	if err != nil {
		t.Fatalf("NewLoaderConnection() error = %v", err)
	}
	defer remoteConn.Disconnect()

	tableCache := core.NewTableCacheStruct()
	tables := loadStandardTestTables(t, tableCache, configDir, "orders", "lineitem")
	envelopes := []core.IngestEnvelope{
		mustStreamEnvelope(t, "tpch.orders.100000001", map[string]interface{}{
			"type": "orders",
			"data": map[string]interface{}{
				"o_orderkey":    int64(100000001),
				"o_orderstatus": "O",
			},
		}),
		mustStreamEnvelope(t, "tpch.lineitem.100000001.1", flatLineitemPayload(100000001, 1)),
		mustStreamEnvelope(t, "tpch.lineitem.100000001.2", flatLineitemPayload(100000001, 2)),
	}
	routeStandardProcessNativeFlatTPCHEnvelopes(t, process, config, tableCache, tables, envelopes)
	requireStandardProcessScalarString(t, process, "select count(*) from orders", "1")
	requireStandardProcessScalarString(t, process, "select count(*) from lineitem", "2")
	lineitemTable := tables[1]
	pkAttrs, err := lineitemTable.GetPrimaryKeyInfo()
	if err != nil {
		t.Fatalf("lineitem GetPrimaryKeyInfo() error = %v", err)
	}
	expectedAuthority, err := core.EncodeCompoundPrimaryKeyAuthorityValue(core.PrimaryKeyAuthorityValueEncodingRequest{
		TableName:  "lineitem",
		PrimaryKey: "l_orderkey+l_linenumber",
		Attributes: pkAttrs,
		Values:     []interface{}{"1995-03-16", int64(100000001), int64(1)},
	})
	if err != nil {
		t.Fatalf("EncodeCompoundPrimaryKeyAuthorityValue() error = %v", err)
	}
	fromTime, _, err := shared.ToTQTimestamp("YMD", "1995-03-16")
	if err != nil {
		t.Fatalf("ToTQTimestamp() error = %v", err)
	}
	remoteBSIs, _, err := shared.NewBitmapIndex(remoteConn).Projection("lineitem",
		[]string{shared.CompoundPrimaryKeyAuthorityFieldName}, fromTime.UnixNano(), fromTime.UnixNano(), nil, false)
	if err != nil {
		t.Fatalf("remote Projection(__qs_pk_authority) error = %v", err)
	}
	remoteAuthorityBSI := remoteBSIs[shared.CompoundPrimaryKeyAuthorityFieldName]
	if remoteAuthorityBSI == nil {
		t.Fatalf("remote Projection(__qs_pk_authority) returned fields=%v, want %s",
			standardTestMapKeys(remoteBSIs), shared.CompoundPrimaryKeyAuthorityFieldName)
	}
	if got := remoteAuthorityBSI.CompareBigValue(0, roaring64.EQ, expectedAuthority, nil, nil).GetCardinality(); got != 1 {
		t.Fatalf("remote Projection(__qs_pk_authority) expected authority matches = %d, want 1", got)
	}

	replay := envelopes[1:]
	profile := routeStandardProcessNativeFlatTPCHEnvelopes(t, process, config, tableCache, tables, replay)
	requirePrimaryKeyTableProfile(t, profile, "lineitem", core.PrimaryKeyResolveProfile{
		ResolveCount:             2,
		LookupRequiredCount:      2,
		BSILookupCount:           2,
		BSIHitCount:              2,
		EmptyDomainProbeCount:    1,
		EmptyDomainNonEmptyCount: 2,
	})
	requireStandardProcessScalarString(t, process, "select count(*) from lineitem", "2")

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

func TestStandardProcessNativeGRPCRouterParallelReplayUsesBSIPrimaryKeyAuthority(t *testing.T) {
	orderCount := 8
	lineitemsPerOrder := 3
	replayCount := 2
	primaryKeyBackend := qsfixture.NewMemoryBSIPrimaryKeyBackend()

	result := runStandardProcessNativeGRPCRouterTPCHNestedOrderLineitems(t, standardTPCHRouterIngestScenario{
		OrderCount:        orderCount,
		LineitemsPerOrder: lineitemsPerOrder,
		ShardCount:        4,
		SourceMode:        core.IngestSourceStream,
		ReplayCount:       replayCount,
		PrimaryKeyResolverFactory: func(_ *core.Session) core.PrimaryKeyResolver {
			return core.NewBSIPrimaryKeyResolver(primaryKeyBackend)
		},
	})

	requireBSIPrimaryKeyAuthorityReplayProfile(t, result, primaryKeyBackend, orderCount, lineitemsPerOrder, replayCount)
}

func requireBSIPrimaryKeyAuthorityReplayProfile(
	t *testing.T,
	result standardTPCHRouterIngestResult,
	primaryKeyBackend *qsfixture.MemoryBSIPrimaryKeyBackend,
	orderCount int,
	lineitemsPerOrder int,
	replayCount int,
) {
	t.Helper()
	expectedTopLevelRecords := orderCount * replayCount
	expectedLogicalWrites := expectedTopLevelRecords * (1 + lineitemsPerOrder)
	expectedLineitemReplayHits := orderCount * lineitemsPerOrder
	if result.PutProfile.RecordCount != expectedTopLevelRecords {
		t.Fatalf("put profile = %+v, want %d routed order records", result.PutProfile, expectedTopLevelRecords)
	}
	if result.PutProfile.LogicalRowCount != expectedLogicalWrites {
		t.Fatalf("put profile = %+v, want %d logical writes across replays", result.PutProfile, expectedLogicalWrites)
	}
	if result.PutProfile.PrimaryKey.BSIHitCount != expectedLineitemReplayHits {
		t.Fatalf("primary key profile = %+v, want %d BSI hits on replayed lineitems",
			result.PutProfile.PrimaryKey, expectedLineitemReplayHits)
	}
	if result.PutProfile.PrimaryKey.BSIStageWriteCount != expectedLineitemReplayHits {
		t.Fatalf("primary key profile = %+v, want %d staged lineitem primary keys",
			result.PutProfile.PrimaryKey, expectedLineitemReplayHits)
	}
	if len(primaryKeyBackend.Snapshot()) != expectedLineitemReplayHits {
		t.Fatalf("BSI primary key backend entries = %d, want %d lineitem PK entries",
			len(primaryKeyBackend.Snapshot()), expectedLineitemReplayHits)
	}
}

func requirePrimaryKeyTableProfile(
	t *testing.T,
	putProfile core.RouterPutRowProfileSummary,
	tableName string,
	expected core.PrimaryKeyResolveProfile,
) {
	t.Helper()
	profile, ok := putProfile.PrimaryKeyByTable[tableName]
	if !ok {
		t.Fatalf("primary key table profile for %s missing in %+v", tableName, putProfile.PrimaryKeyByTable)
	}
	if profile.ResolveCount != expected.ResolveCount {
		t.Fatalf("%s primary key profile = %+v, want %d resolves", tableName, profile, expected.ResolveCount)
	}
	if profile.LookupRequiredCount != expected.LookupRequiredCount {
		t.Fatalf("%s primary key profile = %+v, want %d lookup-required rows", tableName, profile, expected.LookupRequiredCount)
	}
	if profile.DirectColumnIDCount != expected.DirectColumnIDCount {
		t.Fatalf("%s primary key profile = %+v, want %d direct column IDs", tableName, profile, expected.DirectColumnIDCount)
	}
	if profile.BSILookupCount != expected.BSILookupCount {
		t.Fatalf("%s primary key profile = %+v, want %d BSI lookups", tableName, profile, expected.BSILookupCount)
	}
	if profile.BSIHitCount != expected.BSIHitCount {
		t.Fatalf("%s primary key profile = %+v, want %d BSI hits", tableName, profile, expected.BSIHitCount)
	}
	if profile.SkippedBSILookupCount != expected.SkippedBSILookupCount {
		t.Fatalf("%s primary key profile = %+v, want %d skipped BSI lookups", tableName, profile, expected.SkippedBSILookupCount)
	}
	if profile.EmptyDomainProbeCount != expected.EmptyDomainProbeCount {
		t.Fatalf("%s primary key profile = %+v, want %d empty-domain probes", tableName, profile, expected.EmptyDomainProbeCount)
	}
	if profile.EmptyDomainSkipCount != expected.EmptyDomainSkipCount {
		t.Fatalf("%s primary key profile = %+v, want %d empty-domain skips", tableName, profile, expected.EmptyDomainSkipCount)
	}
	if profile.EmptyDomainNonEmptyCount != expected.EmptyDomainNonEmptyCount {
		t.Fatalf("%s primary key profile = %+v, want %d non-empty domain observations", tableName, profile, expected.EmptyDomainNonEmptyCount)
	}
	if profile.BSIStageWriteCount != expected.BSIStageWriteCount {
		t.Fatalf("%s primary key profile = %+v, want %d BSI stage writes", tableName, profile, expected.BSIStageWriteCount)
	}
}

func requirePrimaryKeyTableProjectionCacheProfile(
	t *testing.T,
	putProfile core.RouterPutRowProfileSummary,
	tableName string,
	wantLookups int,
	wantHits int,
) {
	t.Helper()
	profile, ok := putProfile.PrimaryKeyByTable[tableName]
	if !ok {
		t.Fatalf("primary key table profile for %s missing in %+v", tableName, putProfile.PrimaryKeyByTable)
	}
	if profile.BSIProjectionCacheLookupCount != wantLookups {
		t.Fatalf("%s primary key profile = %+v, want %d projection cache lookups", tableName, profile, wantLookups)
	}
	if profile.BSIProjectionCacheHitCount != wantHits {
		t.Fatalf("%s primary key profile = %+v, want %d projection cache hits", tableName, profile, wantHits)
	}
}

func requireStandardProcessBSIPrimaryKeyAuthorityManifestArtifact(t *testing.T, config StandardConfig, source string) {
	t.Helper()
	manifest, err := core.LoadBSIPrimaryKeyAuthorityManifest(config.DataDir)
	if err != nil {
		t.Fatalf("LoadBSIPrimaryKeyAuthorityManifest() error = %v", err)
	}
	if manifest.Source != source {
		t.Fatalf("manifest source = %q, want %q", manifest.Source, source)
	}
	observation := ObserveStandardBSIPrimaryKeyAuthorityManifest(config)
	if observation.Status != core.BSIPrimaryKeyAuthorityManifestStatusOK {
		t.Fatalf("manifest observation status = %s detail=%s, want ok", observation.Status, observation.Detail)
	}
	if observation.ArtifactPresence != core.BSIPrimaryKeyAuthorityArtifactPresencePresent {
		t.Fatalf("manifest artifact presence = %s detail=%s, want present", observation.ArtifactPresence, observation.ArtifactDetail)
	}
	if observation.ArtifactFileCount == 0 {
		t.Fatalf("manifest artifact file count = 0, want persisted BSI authority files")
	}
}

type standardTPCHRouterIngestScenario struct {
	OrderCount                int
	LineitemsPerOrder         int
	ShardCount                int
	SourceMode                core.IngestSourceMode
	BaseOrderKey              int64
	ReplayCount               int
	PrimaryKeyResolverFactory core.SessionPrimaryKeyResolverFactory
}

type standardTPCHRouterIngestResult struct {
	Routes       []core.IngestRouteResult
	PutProfile   core.RouterPutRowProfileSummary
	FlushProfile core.RouterFlushProfileSummary
}

func routeStandardProcessNativeTPCHEnvelopes(tb testing.TB, process StandardProcess, config StandardConfig,
	fixture qsfixture.TPCHOrderLineitemEnvelopeFixture, tableCache *core.TableCacheStruct) core.RouterPutRowProfileSummary {

	tb.Helper()
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
		Owner:   "standard-native-tpch-restart-test",
		Address: process.NativeNode.Address,
	})
	if err != nil {
		tb.Fatalf("NewLoaderConnection() error = %v", err)
	}
	defer remoteConn.Disconnect()

	putRowProfile := &core.RouterPutRowProfile{}
	router, err := core.NewSessionRouter(core.SessionRouterConfig{
		TableCache:                tableCache,
		BasePath:                  process.Backend.ConfigBaseDir(config),
		Conn:                      remoteConn,
		ShardCount:                1,
		ChannelSize:               len(fixture.Envelopes),
		FlushInterval:             10 * time.Millisecond,
		PrimaryKeyMode:            core.PrimaryKeyModeVerifyExisting,
		PrimaryKeyResolverFactory: NewStandardSessionBSIPrimaryKeyResolverFactory(tableCache),
		OnPutRowResult:            putRowProfile.Callback(),
	})
	if err != nil {
		tb.Fatalf("NewSessionRouter() error = %v", err)
	}
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
	}
	if err := router.Close(); err != nil {
		tb.Fatalf("router Close() error = %v", err)
	}
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
	return putRowProfile.Snapshot()
}

func routeStandardProcessNativeFlatTPCHEnvelopes(tb testing.TB, process StandardProcess, config StandardConfig,
	tableCache *core.TableCacheStruct, tables []*core.Table, envelopes []core.IngestEnvelope) core.RouterPutRowProfileSummary {

	tb.Helper()
	if process.NativeNode == nil {
		tb.Fatalf("NativeNode = nil, want native gRPC listener")
	}
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	remoteConn, err := shared.NewLoaderConnection(dialCtx, shared.LoaderConnectionConfig{
		Mode:    shared.LoaderConnectionStandardNative,
		Owner:   "standard-native-flat-tpch-router-test",
		Address: process.NativeNode.Address,
	})
	if err != nil {
		tb.Fatalf("NewLoaderConnection() error = %v", err)
	}
	defer remoteConn.Disconnect()

	putRowProfile := &core.RouterPutRowProfile{}
	router, err := core.NewSessionRouter(core.SessionRouterConfig{
		TableCache:                tableCache,
		BasePath:                  process.Backend.ConfigBaseDir(config),
		Conn:                      remoteConn,
		ShardCount:                1,
		ChannelSize:               len(envelopes),
		FlushInterval:             10 * time.Millisecond,
		PrimaryKeyMode:            core.PrimaryKeyModeVerifyExisting,
		PrimaryKeyResolverFactory: NewStandardSessionBSIPrimaryKeyResolverFactory(tableCache),
		OnPutRowResult:            putRowProfile.Callback(),
	})
	if err != nil {
		tb.Fatalf("NewSessionRouter() error = %v", err)
	}
	for _, envelope := range envelopes {
		route, routeDiagnostics, err := core.RouteSelectedIngestEnvelope(router, envelope, core.IngestEnvelopeRouteOptions{
			Tables: tables,
		})
		if routeDiagnostics.BlocksNative() {
			tb.Fatalf("route diagnostics = %#v, want none", routeDiagnostics)
		}
		if err != nil {
			tb.Fatalf("RouteSelectedIngestEnvelope(%s) error = %v", envelope.EventID, err)
		}
		if !route.Enqueued {
			tb.Fatalf("route result = %+v, want enqueued record", route)
		}
	}
	if err := router.Close(); err != nil {
		tb.Fatalf("router Close() error = %v", err)
	}
	return putRowProfile.Snapshot()
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
	if scenario.ReplayCount <= 0 {
		scenario.ReplayCount = 1
	}
	tableCache := core.NewTableCacheStruct()
	if scenario.PrimaryKeyResolverFactory == nil {
		scenario.PrimaryKeyResolverFactory = NewStandardSessionBSIPrimaryKeyResolverFactory(tableCache)
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
	routes := make([]core.IngestRouteResult, 0, len(fixture.Envelopes))
	for replay := 0; replay < scenario.ReplayCount; replay++ {
		router, err := core.NewSessionRouter(core.SessionRouterConfig{
			TableCache:                tableCache,
			BasePath:                  process.Backend.ConfigBaseDir(config),
			Conn:                      remoteConn,
			ShardCount:                scenario.ShardCount,
			ChannelSize:               len(fixture.Envelopes),
			FlushInterval:             10 * time.Millisecond,
			PrimaryKeyResolverFactory: scenario.PrimaryKeyResolverFactory,
			OnPutRowResult:            putRowProfile.Callback(),
			OnFlushProfile:            flushProfile.Callback(),
		})
		if err != nil {
			tb.Fatalf("NewSessionRouter() error = %v", err)
		}

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

func TestStandardProcessObservesPhysicalBSIPrimaryKeyAuthorityArtifactAfterCommittedInsert(t *testing.T) {
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
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	defer process.Close()

	requireStandardProcessSQLSuccess(t, process, "insert into sample (id, city) values (101, 'Buenos Aires')")
	requireStandardProcessSQLSuccess(t, process, "commit")

	manifest, err := core.LoadBSIPrimaryKeyAuthorityManifest(config.DataDir)
	if err != nil {
		t.Fatalf("LoadBSIPrimaryKeyAuthorityManifest() error = %v", err)
	}
	if manifest.Source != "standard-sql-commit" {
		t.Fatalf("manifest source = %q, want standard-sql-commit", manifest.Source)
	}
	if len(manifest.Entries) != 1 || len(manifest.Entries[0].Artifacts) != 1 {
		t.Fatalf("manifest entries = %+v, want one BSI authority artifact", manifest.Entries)
	}
	artifact := manifest.Entries[0].Artifacts[0]
	expectedArtifactPath := standardBSIPrimaryKeyAuthorityArtifactPath("sample", "id", "")
	if artifact.Path != expectedArtifactPath {
		t.Fatalf("artifact path = %q, want %s", artifact.Path, expectedArtifactPath)
	}
	if artifact.FileCount == 0 {
		t.Fatalf("manifest artifact file count = 0, want SQL COMMIT refresh to record persisted BSI files")
	}

	observation := ObserveStandardBSIPrimaryKeyAuthorityManifest(config)
	if observation.Status != core.BSIPrimaryKeyAuthorityManifestStatusOK {
		t.Fatalf("observation status = %s detail=%s", observation.Status, observation.Detail)
	}
	if observation.ArtifactPresence != core.BSIPrimaryKeyAuthorityArtifactPresencePresent {
		t.Fatalf("artifact presence = %s detail=%s, want present", observation.ArtifactPresence, observation.ArtifactDetail)
	}
	if observation.ArtifactFileCount == 0 {
		t.Fatalf("artifact file count = 0, want persisted BSI files")
	}

	loader := StandardBSIPrimaryKeyAuthorityArtifactLoader{Config: config}
	result, err := loader.LoadBSIPrimaryKeyAuthorityArtifact(core.BSIPrimaryKeyAuthorityArtifactLoadRequest{
		Entry:    manifest.Entries[0],
		Artifact: artifact,
	})
	if err != nil {
		t.Fatalf("LoadBSIPrimaryKeyAuthorityArtifact() error = %v", err)
	}
	if result.FileCount != observation.ArtifactFileCount {
		t.Fatalf("loader file count = %d, want observed file count %d", result.FileCount, observation.ArtifactFileCount)
	}

	process.Close()

	reopened, reopenDiagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("reopen MountStandardProcess() error = %v", err)
	}
	defer reopened.Close()
	if reopenDiagnostics.BlocksNative() {
		t.Fatalf("reopen diagnostics = %#v, want none", reopenDiagnostics)
	}
	reopenedObservation := ObserveStandardBSIPrimaryKeyAuthorityManifest(config)
	if reopenedObservation.Status != core.BSIPrimaryKeyAuthorityManifestStatusOK {
		t.Fatalf("reopened observation status = %s detail=%s", reopenedObservation.Status, reopenedObservation.Detail)
	}
	if reopenedObservation.ArtifactPresence != core.BSIPrimaryKeyAuthorityArtifactPresencePresent {
		t.Fatalf("reopened artifact presence = %s detail=%s, want present", reopenedObservation.ArtifactPresence, reopenedObservation.ArtifactDetail)
	}
	requireStandardProcessScalarString(t, reopened, "select count(*) from sample where id = 101", "1")
}

func TestStandardProcessDocumentsTransactionCompatibilityPosture(t *testing.T) {
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
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	defer process.Close()

	requireStandardProcessSQLSuccess(t, process, "begin")
	requireStandardProcessSQLSuccess(t, process, "set autocommit = 0")
	requireStandardProcessSQLSuccess(t, process, "insert into sample (id, city) values (202, 'Cartago')")
	requireStandardProcessSQLSuccess(t, process, "rollback")

	requireStandardProcessScalarString(t, process, "select count(*) from sample where id = 202", "1")
	requireStandardProcessSQLSuccess(t, process, "set autocommit = ON")
	requireStandardProcessSQLSuccess(t, process, "delete from sample where id = 202")
	requireStandardProcessSQLSuccess(t, process, "commit")
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

	dropMissingResult, err := process.FrontDoor.Server.ExecuteSQL(context.Background(), "drop table if exists missing_sample", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("DROP TABLE IF EXISTS missing error = %v", err)
	}
	if dropMissingResult.Diagnostics.BlocksNative() || dropMissingResult.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("DROP TABLE IF EXISTS missing diagnostics = %#v runtime=%#v", dropMissingResult.Diagnostics, dropMissingResult.Runtime.Diagnostics)
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

func TestStandardProcessRejectsLocalWritesWhileStorageQuiesced(t *testing.T) {
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
	lease, err := core.BeginLocalStorageQuiescence(context.Background(), core.BeginLocalStorageQuiescenceRequest{
		DataDir: config.DataDir,
		Owner:   "unit-test",
		Reason:  "snapshot",
	})
	if err != nil {
		t.Fatalf("BeginLocalStorageQuiescence returned error: %v", err)
	}
	defer func() { _ = core.EndLocalStorageQuiescence(config.DataDir, lease.ID) }()

	for _, sql := range []string{
		"insert into sample (id, city) values (1, 'Seattle')",
		"insert into sample (id, city) select id, city from sample",
		"create table sample_copy as select id, city from sample",
		"commit",
	} {
		result, err := process.FrontDoor.Server.ExecuteSQL(context.Background(), sql, qsbridge.ExecutionOptions{})
		if err != nil {
			t.Fatalf("%s error = %v", sql, err)
		}
		if !result.Diagnostics.BlocksNative() && !result.Runtime.Diagnostics.BlocksNative() {
			t.Fatalf("%s diagnostics = %#v runtime=%#v, want quiescence blocker", sql, result.Diagnostics, result.Runtime.Diagnostics)
		}
		if text := standardProcessDiagnosticText(result.Diagnostics, result.Runtime.Diagnostics); !strings.Contains(text, "quiesced") {
			t.Fatalf("%s diagnostics text = %q, want quiesced", sql, text)
		}
	}

	if err := core.EndLocalStorageQuiescence(config.DataDir, lease.ID); err != nil {
		t.Fatalf("EndLocalStorageQuiescence returned error: %v", err)
	}
	result, err := process.FrontDoor.Server.ExecuteSQL(context.Background(), "insert into sample (id, city) values (2, 'Lima')", qsbridge.ExecutionOptions{})
	if err != nil {
		t.Fatalf("INSERT after quiescence error = %v", err)
	}
	if result.Diagnostics.BlocksNative() || result.Runtime.Diagnostics.BlocksNative() {
		t.Fatalf("INSERT after quiescence diagnostics = %#v runtime=%#v, want none", result.Diagnostics, result.Runtime.Diagnostics)
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
	requireStandardProcessSQLSuccess(t, process, "insert into customers (id, city) values (1, 'Seattle')")
	requireStandardProcessSQLSuccess(t, process, "insert into orders (order_id, cust_id) values (10, 1)")
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
  mappingStrategy: IntBSI
  type: Integer
  columnID: true
- fieldName: city
  sourceName: /city
  mappingStrategy: StringLexBSI
  configuration:
    length: "8"
  type: String
`
	if foreignKey != "" {
		schema = `tableName: ` + table + `
primaryKey: order_id
attributes:
- fieldName: order_id
  sourceName: /order_id
  mappingStrategy: IntBSI
  type: Integer
  columnID: true
- fieldName: cust_id
  sourceName: /cust_id
  mappingStrategy: ParentRelation
  type: Integer
  foreignKey: ` + foreignKey + `
`
	}
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), []byte(schema), 0644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
}

func writeStandardStringEnumTestSchema(t *testing.T, configDir, table string) {
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
  mappingStrategy: IntBSI
  type: Integer
- fieldName: status
  sourceName: /status
  mappingStrategy: StringEnum
  type: String
`
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), []byte(schema), 0644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	if err := shared.ActivateCatalogTable(configDir, "quanta", table, time.Now().UTC()); err != nil {
		t.Fatalf("activate catalog object: %v", err)
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

func writeStandardTPCHFlatSchemas(tb testing.TB, configDir string) {
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
`)
	writeStandardTPCHNestedSchema(tb, configDir, "lineitem", `tableName: lineitem
primaryKey: l_orderkey+l_linenumber
selector: type="lineitem"
timeQuantumType: YMD
timeQuantumField: l_shipdate
attributes:
- fieldName: l_orderkey
  sourceName: /data/l_orderkey
  mappingStrategy: ParentRelation
  foreignKey: orders
  type: Integer
- fieldName: l_linenumber
  sourceName: /data/l_linenumber
  mappingStrategy: IntBSI
  type: Integer
- fieldName: l_quantity
  sourceName: /data/l_quantity
  mappingStrategy: IntBSI
  type: Integer
- fieldName: l_shipdate
  sourceName: /data/l_shipdate
  mappingStrategy: TimestampBSI
  configuration:
    granularity: "millisecond"
  type: DateTime
`)
	if err := shared.ActivateCatalogTable(configDir, "quanta", "orders", now); err != nil {
		tb.Fatalf("activate orders catalog object: %v", err)
	}
	if err := shared.ActivateCatalogTable(configDir, "quanta", "lineitem", now); err != nil {
		tb.Fatalf("activate lineitem catalog object: %v", err)
	}
}

func loadStandardTestTables(tb testing.TB, tableCache *core.TableCacheStruct, configDir string, names ...string) []*core.Table {
	tb.Helper()
	tables := make([]*core.Table, 0, len(names))
	for _, name := range names {
		table, err := core.LoadTable(tableCache, configDir, nil, name, nil)
		if err != nil {
			tb.Fatalf("LoadTable(%s) error = %v", name, err)
		}
		tables = append(tables, table)
	}
	return tables
}

func standardTestMapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func standardProcessDiagnosticText(sets ...qsbridge.DiagnosticSet) string {
	var messages []string
	for _, set := range sets {
		for _, diagnostic := range set {
			messages = append(messages, diagnostic.Message)
		}
	}
	return strings.Join(messages, "\n")
}

func mustStreamEnvelope(tb testing.TB, eventID string, payload map[string]interface{}) core.IngestEnvelope {
	tb.Helper()
	envelope, err := core.NewStreamIngestEnvelope(core.StreamIngestEnvelopeRequest{
		EventID:      eventID,
		Source:       "tpch-flat-test",
		EventTime:    time.Date(1995, 3, 15, 12, 0, 0, 0, time.UTC),
		SourceOffset: eventID,
		Payload:      payload,
	})
	if err != nil {
		tb.Fatalf("NewStreamIngestEnvelope(%s) error = %v", eventID, err)
	}
	return envelope
}

func flatLineitemPayload(orderKey int64, lineNumber int) map[string]interface{} {
	shipDate := time.Date(1995, 3, 15+lineNumber, 0, 0, 0, 0, time.UTC)
	return map[string]interface{}{
		"type": "lineitem",
		"data": map[string]interface{}{
			"l_orderkey":   orderKey,
			"l_linenumber": int64(lineNumber),
			"l_quantity":   int64(lineNumber + 1),
			"l_shipdate":   shipDate.Format("2006-01-02"),
		},
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

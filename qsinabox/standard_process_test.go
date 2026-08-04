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
	remoteConn, err := shared.NewSingleNodeConnection(dialCtx, "standard-native-loader-test", process.NativeNode.Address)
	if err != nil {
		t.Fatalf("NewSingleNodeConnection() error = %v", err)
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

func reserveStandardTestPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
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

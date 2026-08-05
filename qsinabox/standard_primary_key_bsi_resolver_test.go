package qsinabox

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/shared"
)

type recordingStandardPrimaryKeyResolver struct {
	requests []core.PrimaryKeyResolveRequest
	result   core.PrimaryKeyResolveResult
	err      error
}

func (r *recordingStandardPrimaryKeyResolver) ResolvePrimaryKeyColumnID(req core.PrimaryKeyResolveRequest) (core.PrimaryKeyResolveResult, error) {
	r.requests = append(r.requests, req)
	return r.result, r.err
}

type panicStandardSingleColumnBSIPrimaryKeyReader struct {
	t *testing.T
}

func (r panicStandardSingleColumnBSIPrimaryKeyReader) LookupSingleColumnBSIPrimaryKey(core.SingleColumnBSIPrimaryKeyReadRequest) (core.SingleColumnBSIPrimaryKeyReadResult, error) {
	r.t.Fatalf("LookupSingleColumnBSIPrimaryKey should not be called for unsupported primary-key shapes")
	return core.SingleColumnBSIPrimaryKeyReadResult{}, fmt.Errorf("unexpected single-column BSI lookup")
}

func TestStandardBSIPrimaryKeyResolverUsesExistingPrimaryKeyBSIOnReplay(t *testing.T) {
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

	reader := StandardSingleColumnBSIPrimaryKeyReader{
		Pool:       process.RuntimeMount.Pool,
		TableCache: process.TableCache,
		Direct:     process.Backend.Adapter.BitmapIndex,
	}
	firstSession, err := core.OpenSession(process.TableCache, process.Backend.ConfigBaseDir(config), "sample", false, process.Backend.NewLocalConnection())
	if err != nil {
		t.Fatalf("OpenSession(first) error = %v", err)
	}
	firstSession.SetPrimaryKeyResolver(NewStandardBSIPrimaryKeyResolver(reader))
	first, err := firstSession.PutRowWithOptions("sample", map[string]interface{}{
		"id":   101,
		"city": "Buenos Aires",
	}, 0, false, false, core.PutRowOptions{})
	if err != nil {
		t.Fatalf("first PutRowWithOptions() error = %v", err)
	}
	if !first.Inserted || first.ExistingRow || first.ColumnID == 0 {
		t.Fatalf("first PutRowWithOptions() = %+v, want inserted row", first)
	}
	if first.PrimaryKey.BSILookupCount != 1 || first.PrimaryKey.BSIStageWriteCount != 1 || first.PrimaryKey.KVLookupCount != 0 {
		t.Fatalf("first primary-key profile = %+v, want BSI miss/stage without KV lookup", first.PrimaryKey)
	}
	if err := firstSession.Flush(); err != nil {
		t.Fatalf("first Flush() error = %v", err)
	}
	if err := firstSession.CloseSession(); err != nil {
		t.Fatalf("first CloseSession() error = %v", err)
	}

	replaySession, err := core.OpenSession(process.TableCache, process.Backend.ConfigBaseDir(config), "sample", false, process.Backend.NewLocalConnection())
	if err != nil {
		t.Fatalf("OpenSession(replay) error = %v", err)
	}
	defer replaySession.CloseSession()
	replaySession.SetPrimaryKeyResolver(NewStandardBSIPrimaryKeyResolver(reader))
	replay, err := replaySession.PutRowWithOptions("sample", map[string]interface{}{
		"id":   101,
		"city": "Cordoba",
	}, 0, false, false, core.PutRowOptions{})
	if err != nil {
		t.Fatalf("replay PutRowWithOptions() error = %v", err)
	}
	if replay.Inserted || !replay.ExistingRow || replay.ColumnID != first.ColumnID {
		t.Fatalf("replay PutRowWithOptions() = %+v, want existing row %d", replay, first.ColumnID)
	}
	if replay.PrimaryKey.BSILookupCount != 1 || replay.PrimaryKey.BSIHitCount != 1 ||
		replay.PrimaryKey.BSIStageWriteCount != 0 || replay.PrimaryKey.KVLookupCount != 0 {
		t.Fatalf("replay primary-key profile = %+v, want BSI hit without KV lookup or stage", replay.PrimaryKey)
	}
}

func TestStandardBSIPrimaryKeyResolverDelegatesCompoundKeysToFallback(t *testing.T) {
	table := standardPrimaryKeyResolverTestTable("lineitem", "l_orderkey+l_linenumber", []shared.BasicAttribute{
		standardPrimaryKeyResolverTestAttribute("l_orderkey", "Integer", "IntBSI", false),
		standardPrimaryKeyResolverTestAttribute("l_linenumber", "Integer", "IntBSI", false),
	})
	tbuf, err := core.NewTableBuffer(table)
	if err != nil {
		t.Fatalf("NewTableBuffer() error = %v", err)
	}
	fallback := &recordingStandardPrimaryKeyResolver{
		result: core.PrimaryKeyResolveResult{ColumnID: 77, ExistingRow: true},
	}
	resolver := StandardBSIPrimaryKeyResolver{
		Reader:   panicStandardSingleColumnBSIPrimaryKeyReader{t: t},
		Fallback: fallback,
	}

	result, err := resolver.ResolvePrimaryKeyColumnID(core.PrimaryKeyResolveRequest{
		Session:          &core.Session{},
		TableBuffer:      tbuf,
		LookupValue:      "1001;1",
		PrimaryKeyValues: []interface{}{int64(1001), int64(1)},
	})

	if err != nil {
		t.Fatalf("ResolvePrimaryKeyColumnID() error = %v", err)
	}
	if result.ColumnID != 77 || !result.ExistingRow {
		t.Fatalf("ResolvePrimaryKeyColumnID() = %+v, want fallback result", result)
	}
	if len(fallback.requests) != 1 {
		t.Fatalf("fallback requests = %d, want 1", len(fallback.requests))
	}
	if fallback.requests[0].TableBuffer != tbuf || fallback.requests[0].LookupValue != "1001;1" {
		t.Fatalf("fallback request = %+v, want original request", fallback.requests[0])
	}
}

func TestStandardSessionBSIPrimaryKeyResolverUsesNativeLoaderConnection(t *testing.T) {
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
	remoteConn, err := shared.NewLoaderConnection(context.Background(), shared.LoaderConnectionConfig{
		Mode:    shared.LoaderConnectionStandardNative,
		Owner:   "standard-bsi-pk-loader-test",
		Address: process.NativeNode.Address,
	})
	if err != nil {
		t.Fatalf("NewLoaderConnection() error = %v", err)
	}
	defer remoteConn.Disconnect()

	tableCache := core.NewTableCacheStruct()
	factory := NewStandardSessionBSIPrimaryKeyResolverFactory(tableCache)
	firstSession, err := core.OpenSession(tableCache, process.Backend.ConfigBaseDir(config), "sample", false, remoteConn)
	if err != nil {
		t.Fatalf("OpenSession(first) error = %v", err)
	}
	firstSession.SetPrimaryKeyResolver(factory(firstSession))
	first, err := firstSession.PutRowWithOptions("sample", map[string]interface{}{
		"id":   202,
		"city": "Buenos Aires",
	}, 0, false, false, core.PutRowOptions{})
	if err != nil {
		t.Fatalf("first PutRowWithOptions() error = %v", err)
	}
	if !first.Inserted || first.PrimaryKey.KVLookupCount != 0 || first.PrimaryKey.BSIStageWriteCount != 1 {
		t.Fatalf("first PutRowWithOptions() = %+v, want BSI-staged insert without KV lookup", first)
	}
	if err := firstSession.Flush(); err != nil {
		t.Fatalf("first Flush() error = %v", err)
	}
	if err := firstSession.CloseSession(); err != nil {
		t.Fatalf("first CloseSession() error = %v", err)
	}

	replaySession, err := core.OpenSession(tableCache, process.Backend.ConfigBaseDir(config), "sample", false, remoteConn)
	if err != nil {
		t.Fatalf("OpenSession(replay) error = %v", err)
	}
	defer replaySession.CloseSession()
	replaySession.SetPrimaryKeyResolver(factory(replaySession))
	replay, err := replaySession.PutRowWithOptions("sample", map[string]interface{}{
		"id":   202,
		"city": "Cordoba",
	}, 0, false, false, core.PutRowOptions{})
	if err != nil {
		t.Fatalf("replay PutRowWithOptions() error = %v", err)
	}
	if replay.Inserted || !replay.ExistingRow || replay.ColumnID != first.ColumnID {
		t.Fatalf("replay PutRowWithOptions() = %+v, want existing row %d", replay, first.ColumnID)
	}
	if replay.PrimaryKey.BSILookupCount != 1 || replay.PrimaryKey.BSIHitCount != 1 ||
		replay.PrimaryKey.BSIStageWriteCount != 0 || replay.PrimaryKey.KVLookupCount != 0 {
		t.Fatalf("replay primary-key profile = %+v, want native BSI hit without KV lookup or stage", replay.PrimaryKey)
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

func TestStandardDirectPrimaryKeyResolverFactoryRequiresTrustedManifest(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "schemas")
	writeStandardTestSchema(t, configDir, "sample")
	config := StandardConfig{
		ConfigDir: configDir,
		DataDir:   filepath.Join(root, "data"),
	}
	if factory := standardDirectPrimaryKeyResolverFactory(config, nil, nil, nil); factory != nil {
		t.Fatalf("standardDirectPrimaryKeyResolverFactory without manifest = %#v, want nil", factory)
	}

	table := standardBSIPrimaryKeyAuthorityCatalogTable(t, configDir, "sample")
	entry, err := core.NewBSIPrimaryKeyAuthorityManifestEntry(table, "")
	if err != nil {
		t.Fatalf("NewBSIPrimaryKeyAuthorityManifestEntry() error = %v", err)
	}
	if err := core.SaveBSIPrimaryKeyAuthorityManifest(config.DataDir, core.BSIPrimaryKeyAuthorityManifest{
		Source: "unit-test",
		Entries: []core.BSIPrimaryKeyAuthorityManifestEntry{
			entry,
		},
	}); err != nil {
		t.Fatalf("SaveBSIPrimaryKeyAuthorityManifest() error = %v", err)
	}

	process, diagnostics, err := MountStandardProcess(context.Background(), config)
	if err != nil {
		t.Fatalf("MountStandardProcess() error = %v", err)
	}
	defer process.Close()
	if diagnostics.BlocksNative() {
		t.Fatalf("MountStandardProcess() diagnostics = %#v, want none", diagnostics)
	}
	provider, ok := process.RuntimeMount.Runtime.Sessions.(StandardDirectSessionProvider)
	if !ok {
		t.Fatalf("runtime sessions = %T, want StandardDirectSessionProvider", process.RuntimeMount.Runtime.Sessions)
	}
	if provider.PrimaryKeyResolverFactory == nil {
		t.Fatalf("PrimaryKeyResolverFactory = nil, want trusted manifest to enable standard BSI PK resolver")
	}
}

func standardPrimaryKeyResolverTestTable(name, primaryKey string, attrs []shared.BasicAttribute) *core.Table {
	table := &core.Table{
		BasicTable: &shared.BasicTable{
			Name:       name,
			PrimaryKey: primaryKey,
			Attributes: attrs,
		},
		Attributes:       make([]core.Attribute, len(attrs)),
		AttributeNameMap: make(map[string]*core.Attribute, len(attrs)),
	}
	for i := range attrs {
		attr := core.Attribute{BasicAttribute: &table.BasicTable.Attributes[i], Parent: table}
		table.Attributes[i] = attr
		table.AttributeNameMap[attr.FieldName] = &table.Attributes[i]
	}
	return table
}

func standardPrimaryKeyResolverTestAttribute(name, attrType, mapping string, columnID bool) shared.BasicAttribute {
	return shared.BasicAttribute{
		FieldName:       name,
		Type:            attrType,
		MappingStrategy: mapping,
		ColumnID:        columnID,
	}
}

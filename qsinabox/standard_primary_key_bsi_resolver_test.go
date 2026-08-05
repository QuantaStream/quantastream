package qsinabox

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/QuantaStream/quantastream/core"
)

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

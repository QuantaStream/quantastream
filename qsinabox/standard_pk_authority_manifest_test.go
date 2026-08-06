package qsinabox

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/core"
	"github.com/QuantaStream/quantastream/shared"
)

func TestObserveStandardBSIPrimaryKeyAuthorityManifestReportsMissing(t *testing.T) {
	config := StandardConfig{DataDir: t.TempDir()}

	observation := ObserveStandardBSIPrimaryKeyAuthorityManifest(config)

	if observation.Status != core.BSIPrimaryKeyAuthorityManifestStatusMissing {
		t.Fatalf("status = %s, want %s", observation.Status, core.BSIPrimaryKeyAuthorityManifestStatusMissing)
	}
}

func TestObserveStandardBSIPrimaryKeyAuthorityManifestReportsOK(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	configDir := filepath.Join(dataDir, "config")
	writeStandardTestSchema(t, configDir, "sample")
	table := standardBSIPrimaryKeyAuthorityCatalogTable(t, configDir, "sample")
	entry, err := core.NewBSIPrimaryKeyAuthorityManifestEntry(table, "")
	if err != nil {
		t.Fatalf("NewBSIPrimaryKeyAuthorityManifestEntry returned error: %v", err)
	}
	entry.KeyCount = 7
	entry.Artifacts = []core.BSIPrimaryKeyAuthorityManifestArtifact{
		{
			Kind:     core.BSIPrimaryKeyAuthorityArtifactKindPrimaryKeyBSI,
			Path:     "bitmap/sample/__qs_pk_authority",
			KeyCount: 7,
		},
	}
	if err := core.SaveBSIPrimaryKeyAuthorityManifest(dataDir, core.BSIPrimaryKeyAuthorityManifest{
		Source: "test",
		Entries: []core.BSIPrimaryKeyAuthorityManifestEntry{
			entry,
		},
	}); err != nil {
		t.Fatalf("SaveBSIPrimaryKeyAuthorityManifest returned error: %v", err)
	}

	plan := NewObservedStandardPlan(StandardConfig{DataDir: dataDir}, shared.LocalNodeServices{})
	output := strings.Join(plan.SummaryLines(), "\n")

	if plan.PKAuthority.Status != core.BSIPrimaryKeyAuthorityManifestStatusOK {
		t.Fatalf("status = %s detail=%s", plan.PKAuthority.Status, plan.PKAuthority.Detail)
	}
	if !strings.Contains(output, "bsi_pk_authority_manifest=ok") {
		t.Fatalf("summary missing manifest status:\n%s", output)
	}
	if !strings.Contains(output, "bsi_pk_authority_manifest_entries=1") {
		t.Fatalf("summary missing manifest entry count:\n%s", output)
	}
	if !strings.Contains(output, "bsi_pk_authority_manifest_validation=manifest_only") {
		t.Fatalf("summary missing manifest validation level:\n%s", output)
	}
	if !strings.Contains(output, "bsi_pk_authority_manifest_artifact_trust=metadata_only") {
		t.Fatalf("summary missing manifest artifact trust:\n%s", output)
	}
	if !strings.Contains(output, "bsi_pk_authority_manifest_artifacts=1") {
		t.Fatalf("summary missing manifest artifact count:\n%s", output)
	}
	if !strings.Contains(output, "bsi_pk_authority_manifest_entry_key_count=7") {
		t.Fatalf("summary missing manifest key count:\n%s", output)
	}
	if !strings.Contains(output, "bsi_pk_authority_manifest_clean_entries=1") {
		t.Fatalf("summary missing manifest clean entry count:\n%s", output)
	}
}

func TestBuildStandardBSIPrimaryKeyAuthorityManifestUsesActiveCatalog(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	configDir := filepath.Join(dataDir, "config")
	writeStandardTestSchema(t, configDir, "sample")
	writeStandardDraftTestSchema(t, configDir, "draft")

	manifest, err := BuildStandardBSIPrimaryKeyAuthorityManifest(StandardConfig{DataDir: dataDir}, "unit-test")
	if err != nil {
		t.Fatalf("BuildStandardBSIPrimaryKeyAuthorityManifest returned error: %v", err)
	}

	if manifest.Version != core.BSIPrimaryKeyAuthorityManifestVersion {
		t.Fatalf("version = %d, want %d", manifest.Version, core.BSIPrimaryKeyAuthorityManifestVersion)
	}
	if manifest.GeneratedAt.IsZero() {
		t.Fatalf("GeneratedAt is zero")
	}
	if manifest.Source != "unit-test" {
		t.Fatalf("source = %q, want unit-test", manifest.Source)
	}
	if len(manifest.Entries) != 1 {
		t.Fatalf("entries = %+v, want only active sample table", manifest.Entries)
	}
	if manifest.Entries[0].TableName != "sample" || manifest.Entries[0].PrimaryKey != "id" {
		t.Fatalf("entry = %+v", manifest.Entries[0])
	}
	if manifest.Entries[0].EncodingVersion != core.PrimaryKeyIdentityEncodingVersion {
		t.Fatalf("encoding version = %d, want %d", manifest.Entries[0].EncodingVersion, core.PrimaryKeyIdentityEncodingVersion)
	}
	if manifest.Entries[0].AuthorityMode != core.BSIPrimaryKeyAuthorityModeSingleColumnBSI {
		t.Fatalf("authority mode = %q, want %q", manifest.Entries[0].AuthorityMode, core.BSIPrimaryKeyAuthorityModeSingleColumnBSI)
	}
	if manifest.Entries[0].AuthorityField != "id" {
		t.Fatalf("authority field = %q, want id", manifest.Entries[0].AuthorityField)
	}
	if len(manifest.Entries[0].Artifacts) != 1 {
		t.Fatalf("artifacts = %+v, want one expected BSI authority artifact", manifest.Entries[0].Artifacts)
	}
	artifact := manifest.Entries[0].Artifacts[0]
	if artifact.Kind != core.BSIPrimaryKeyAuthorityArtifactKindPrimaryKeyBSI {
		t.Fatalf("artifact kind = %q, want %q", artifact.Kind, core.BSIPrimaryKeyAuthorityArtifactKindPrimaryKeyBSI)
	}
	if artifact.Path != "bitmap/sample/id" {
		t.Fatalf("artifact path = %q, want bitmap/sample/id", artifact.Path)
	}
}

func TestBuildStandardBSIPrimaryKeyAuthorityManifestDescribesCompoundAuthorityArtifact(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	configDir := filepath.Join(dataDir, "config")
	writeStandardCompoundPrimaryKeyTestSchema(t, configDir, "lineitem")

	manifest, err := BuildStandardBSIPrimaryKeyAuthorityManifest(StandardConfig{DataDir: dataDir}, "unit-test")
	if err != nil {
		t.Fatalf("BuildStandardBSIPrimaryKeyAuthorityManifest returned error: %v", err)
	}

	if len(manifest.Entries) != 1 {
		t.Fatalf("entries = %+v, want one lineitem entry", manifest.Entries)
	}
	entry := manifest.Entries[0]
	if entry.AuthorityMode != core.BSIPrimaryKeyAuthorityModeCompoundEncodedBSI {
		t.Fatalf("authority mode = %q, want %q", entry.AuthorityMode, core.BSIPrimaryKeyAuthorityModeCompoundEncodedBSI)
	}
	if entry.AuthorityField != shared.CompoundPrimaryKeyAuthorityFieldName {
		t.Fatalf("authority field = %q, want %q", entry.AuthorityField, shared.CompoundPrimaryKeyAuthorityFieldName)
	}
	if len(entry.Artifacts) != 1 {
		t.Fatalf("artifacts = %+v, want one expected compound authority artifact", entry.Artifacts)
	}
	artifact := entry.Artifacts[0]
	if artifact.Kind != core.BSIPrimaryKeyAuthorityArtifactKindPrimaryKeyBSI {
		t.Fatalf("artifact kind = %q, want %q", artifact.Kind, core.BSIPrimaryKeyAuthorityArtifactKindPrimaryKeyBSI)
	}
	if artifact.Path != "bitmap/lineitem/"+shared.CompoundPrimaryKeyAuthorityFieldName {
		t.Fatalf("artifact path = %q, want compound authority BSI path", artifact.Path)
	}
}

func TestBuildStandardBSIPrimaryKeyAuthorityManifestFallsBackToDiscovery(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	configDir := filepath.Join(dataDir, "config")
	writeStandardDraftTestSchema(t, configDir, "sample")

	manifest, err := BuildStandardBSIPrimaryKeyAuthorityManifest(StandardConfig{DataDir: dataDir}, "")
	if err != nil {
		t.Fatalf("BuildStandardBSIPrimaryKeyAuthorityManifest returned error: %v", err)
	}

	if len(manifest.Entries) != 1 {
		t.Fatalf("entries = %+v, want discovered sample table", manifest.Entries)
	}
	if manifest.Entries[0].TableName != "sample" {
		t.Fatalf("entry table = %q, want sample", manifest.Entries[0].TableName)
	}
}

func TestStandardBSIPrimaryKeyAuthorityManifestPublisherSkipsEmptyFlush(t *testing.T) {
	dataDir := t.TempDir()
	publisher := StandardBSIPrimaryKeyAuthorityManifestFilePublisher{
		Config: StandardConfig{DataDir: dataDir},
		Source: "unit-test",
	}

	published, err := publisher.PublishAfterFlush(shared.BatchBufferFlushProfile{})
	if err != nil {
		t.Fatalf("PublishAfterFlush returned error: %v", err)
	}
	if published {
		t.Fatalf("published = true, want empty flush to skip manifest write")
	}
	if _, err := os.Stat(core.BSIPrimaryKeyAuthorityManifestPath(dataDir)); !os.IsNotExist(err) {
		t.Fatalf("manifest stat error = %v, want missing manifest", err)
	}
}

func TestStandardBSIPrimaryKeyAuthorityManifestPublisherRejectsFailedFlush(t *testing.T) {
	publisher := StandardBSIPrimaryKeyAuthorityManifestFilePublisher{
		Config: StandardConfig{DataDir: t.TempDir()},
		Source: "unit-test",
	}

	published, err := publisher.PublishAfterFlush(shared.BatchBufferFlushProfile{
		StartedAt:          time.Now(),
		BSIValueEntryCount: 1,
		Error:              "boom",
	})
	if err == nil {
		t.Fatalf("PublishAfterFlush returned nil error, want failed flush rejection")
	}
	if published {
		t.Fatalf("published = true, want failed flush to skip manifest write")
	}
}

func TestStandardBSIPrimaryKeyAuthorityManifestPublisherWritesAfterMutatedFlush(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	configDir := filepath.Join(dataDir, "config")
	writeStandardTestSchema(t, configDir, "sample")
	publisher := StandardBSIPrimaryKeyAuthorityManifestFilePublisher{
		Config: StandardConfig{DataDir: dataDir},
		Source: "unit-test-flush",
	}

	published, err := publisher.PublishAfterFlush(shared.BatchBufferFlushProfile{
		StartedAt:          time.Now(),
		FinishedAt:         time.Now(),
		BSIValueEntryCount: 3,
	})
	if err != nil {
		t.Fatalf("PublishAfterFlush returned error: %v", err)
	}
	if !published {
		t.Fatalf("published = false, want mutated flush to write manifest")
	}
	manifest, err := core.LoadBSIPrimaryKeyAuthorityManifest(dataDir)
	if err != nil {
		t.Fatalf("LoadBSIPrimaryKeyAuthorityManifest returned error: %v", err)
	}
	if manifest.Source != "unit-test-flush" {
		t.Fatalf("manifest source = %q, want unit-test-flush", manifest.Source)
	}
	if len(manifest.Entries) != 1 || len(manifest.Entries[0].Artifacts) != 1 {
		t.Fatalf("manifest entries = %+v, want one entry with one descriptor", manifest.Entries)
	}
}

func TestObserveStandardBSIPrimaryKeyAuthorityManifestReportsInvalidVersion(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	configDir := filepath.Join(dataDir, "config")
	writeStandardTestSchema(t, configDir, "sample")
	table := standardBSIPrimaryKeyAuthorityCatalogTable(t, configDir, "sample")
	entry, err := core.NewBSIPrimaryKeyAuthorityManifestEntry(table, "")
	if err != nil {
		t.Fatalf("NewBSIPrimaryKeyAuthorityManifestEntry returned error: %v", err)
	}
	if err := core.SaveBSIPrimaryKeyAuthorityManifest(dataDir, core.BSIPrimaryKeyAuthorityManifest{
		Version: core.BSIPrimaryKeyAuthorityManifestVersion + 1,
		Entries: []core.BSIPrimaryKeyAuthorityManifestEntry{
			entry,
		},
	}); err != nil {
		t.Fatalf("SaveBSIPrimaryKeyAuthorityManifest returned error: %v", err)
	}

	observation := ObserveStandardBSIPrimaryKeyAuthorityManifest(StandardConfig{DataDir: dataDir})

	if observation.Status != core.BSIPrimaryKeyAuthorityManifestStatusInvalid {
		t.Fatalf("status = %s, want %s", observation.Status, core.BSIPrimaryKeyAuthorityManifestStatusInvalid)
	}
	if !strings.Contains(observation.Detail, "manifest version") {
		t.Fatalf("detail = %q", observation.Detail)
	}
}

func TestObserveStandardBSIPrimaryKeyAuthorityManifestReportsStaleCatalogShape(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	configDir := filepath.Join(dataDir, "config")
	writeStandardTestSchema(t, configDir, "sample")
	table := standardBSIPrimaryKeyAuthorityCatalogTable(t, configDir, "sample")
	entry, err := core.NewBSIPrimaryKeyAuthorityManifestEntry(table, "")
	if err != nil {
		t.Fatalf("NewBSIPrimaryKeyAuthorityManifestEntry returned error: %v", err)
	}
	entry.PrimaryKey = "stale_id"
	if err := core.SaveBSIPrimaryKeyAuthorityManifest(dataDir, core.BSIPrimaryKeyAuthorityManifest{
		Entries: []core.BSIPrimaryKeyAuthorityManifestEntry{
			entry,
		},
	}); err != nil {
		t.Fatalf("SaveBSIPrimaryKeyAuthorityManifest returned error: %v", err)
	}

	plan := NewObservedStandardPlan(StandardConfig{DataDir: dataDir}, shared.LocalNodeServices{})
	output := strings.Join(plan.SummaryLines(), "\n")

	if plan.PKAuthority.Status != core.BSIPrimaryKeyAuthorityManifestStatusStale {
		t.Fatalf("status = %s detail=%s", plan.PKAuthority.Status, plan.PKAuthority.Detail)
	}
	if !strings.Contains(output, "bsi_pk_authority_manifest=stale") {
		t.Fatalf("summary missing stale manifest status:\n%s", output)
	}
	if !strings.Contains(output, "bsi_pk_authority_manifest_detail=") {
		t.Fatalf("summary missing stale manifest detail:\n%s", output)
	}
	if !strings.Contains(output, "warning=BSI primary-key authority manifest is stale; mutations fail closed") {
		t.Fatalf("summary missing stale manifest warning:\n%s", output)
	}
}

func TestObserveStandardBSIPrimaryKeyAuthorityManifestReportsMalformedManifest(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.WriteFile(core.BSIPrimaryKeyAuthorityManifestPath(dataDir), []byte("entries:\n  - : bad\n"), 0644); err != nil {
		t.Fatalf("write malformed manifest returned error: %v", err)
	}

	observation := ObserveStandardBSIPrimaryKeyAuthorityManifest(StandardConfig{DataDir: dataDir})

	if observation.Status != core.BSIPrimaryKeyAuthorityManifestStatusInvalid {
		t.Fatalf("status = %s, want %s", observation.Status, core.BSIPrimaryKeyAuthorityManifestStatusInvalid)
	}
	if !strings.Contains(observation.Detail, "load manifest") {
		t.Fatalf("detail = %q", observation.Detail)
	}
}

func standardBSIPrimaryKeyAuthorityCatalogTable(t *testing.T, configDir, tableName string) *core.Table {
	t.Helper()
	basic, err := shared.LoadSchema(configDir, tableName, nil)
	if err != nil {
		t.Fatalf("LoadSchema(%s) returned error: %v", tableName, err)
	}
	return standardBSIPrimaryKeyAuthorityTable(basic)
}

func writeStandardCompoundPrimaryKeyTestSchema(t *testing.T, configDir, table string) {
	t.Helper()
	tableDir := filepath.Join(configDir, table)
	if err := os.MkdirAll(tableDir, 0755); err != nil {
		t.Fatalf("mkdir compound schema dir: %v", err)
	}
	schema := `tableName: ` + table + `
primaryKey: l_orderkey+l_linenumber
attributes:
- fieldName: l_orderkey
  sourceName: /l_orderkey
  mappingStrategy: IntBSI
  type: Integer
- fieldName: l_linenumber
  sourceName: /l_linenumber
  mappingStrategy: IntBSI
  type: Integer
- fieldName: l_quantity
  sourceName: /l_quantity
  mappingStrategy: IntBSI
  type: Integer
`
	if err := os.WriteFile(filepath.Join(tableDir, "schema.yaml"), []byte(schema), 0644); err != nil {
		t.Fatalf("write compound schema: %v", err)
	}
	if err := shared.ActivateCatalogTable(configDir, "quanta", table, time.Now().UTC()); err != nil {
		t.Fatalf("activate compound catalog object: %v", err)
	}
}

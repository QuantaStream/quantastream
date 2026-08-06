package qsinabox

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

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
			Kind:     "bsi",
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
	if !strings.Contains(output, "bsi_pk_authority_manifest_artifacts=1") {
		t.Fatalf("summary missing manifest artifact count:\n%s", output)
	}
	if !strings.Contains(output, "bsi_pk_authority_manifest_entry_key_count=7") {
		t.Fatalf("summary missing manifest key count:\n%s", output)
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

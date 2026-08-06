package core

import (
	"os"
	"strings"
	"testing"

	"github.com/QuantaStream/quantastream/shared"
)

func TestBSIPrimaryKeyAuthorityManifestObservationReportsOK(t *testing.T) {
	table := testPrimaryKeyAuthorityTable("lineitem", "l_orderkey+l_linenumber", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("l_orderkey", "Integer", "IntBSI", true),
		testPrimaryKeyAuthorityAttribute("l_linenumber", "Integer", "IntBSI", false),
	})
	entry, err := NewBSIPrimaryKeyAuthorityManifestEntry(table, "1994-01-01T00")
	if err != nil {
		t.Fatalf("NewBSIPrimaryKeyAuthorityManifestEntry returned error: %v", err)
	}

	observation := BSIPrimaryKeyAuthorityManifest{
		Version: BSIPrimaryKeyAuthorityManifestVersion,
		Entries: []BSIPrimaryKeyAuthorityManifestEntry{
			entry,
		},
	}.ObserveAgainstCatalog(map[string]*Table{
		"lineitem": table,
	})

	if observation.Status != BSIPrimaryKeyAuthorityManifestStatusOK {
		t.Fatalf("observation status = %s detail=%s", observation.Status, observation.Detail)
	}
	if observation.ArtifactDescriptors != 0 {
		t.Fatalf("artifact descriptors = %d, want 0", observation.ArtifactDescriptors)
	}
	if observation.EntryKeyCount != 0 {
		t.Fatalf("entry key count = %d, want 0", observation.EntryKeyCount)
	}
	if entry.EncodingVersion != PrimaryKeyIdentityEncodingVersion {
		t.Fatalf("entry encoding version = %d, want %d", entry.EncodingVersion, PrimaryKeyIdentityEncodingVersion)
	}
}

func TestBSIPrimaryKeyAuthorityManifestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	table := testPrimaryKeyAuthorityTable("lineitem", "l_orderkey+l_linenumber", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("l_orderkey", "Integer", "IntBSI", true),
		testPrimaryKeyAuthorityAttribute("l_linenumber", "Integer", "IntBSI", false),
	})
	entry, err := NewBSIPrimaryKeyAuthorityManifestEntry(table, "1994-01-01T00")
	if err != nil {
		t.Fatalf("NewBSIPrimaryKeyAuthorityManifestEntry returned error: %v", err)
	}
	entry.ArtifactPath = "bitmap/lineitem/primary_key_authority/1994-01-01T00"
	entry.Artifacts = []BSIPrimaryKeyAuthorityManifestArtifact{
		{
			Kind:        "bsi",
			Path:        "bitmap/lineitem/__qs_pk_authority/1994-01-01T00",
			Fingerprint: "sha256:test-artifact",
			FileCount:   12,
			KeyCount:    44,
			MinColumnID: 100,
			MaxColumnID: 143,
		},
	}
	entry.Fingerprint = "sha256:test-entry"
	entry.CatalogFingerprint = "catalog:test"
	entry.KeyCount = 44
	entry.MinColumnID = 100
	entry.MaxColumnID = 143

	if err := SaveBSIPrimaryKeyAuthorityManifest(dir, BSIPrimaryKeyAuthorityManifest{
		Source: "test",
		Entries: []BSIPrimaryKeyAuthorityManifestEntry{
			entry,
		},
	}); err != nil {
		t.Fatalf("SaveBSIPrimaryKeyAuthorityManifest returned error: %v", err)
	}

	data, err := os.ReadFile(BSIPrimaryKeyAuthorityManifestPath(dir))
	if err != nil {
		t.Fatalf("read saved manifest returned error: %v", err)
	}
	if !strings.Contains(string(data), "encoding_version") {
		t.Fatalf("saved manifest did not include encoding_version:\n%s", string(data))
	}
	if !strings.Contains(string(data), "artifacts:") || !strings.Contains(string(data), "catalog_fingerprint") {
		t.Fatalf("saved manifest did not include artifact metadata:\n%s", string(data))
	}
	if _, err := os.Stat(BSIPrimaryKeyAuthorityManifestPath(dir) + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary manifest file still exists or stat failed: %v", err)
	}

	loaded, err := LoadBSIPrimaryKeyAuthorityManifest(dir)
	if err != nil {
		t.Fatalf("LoadBSIPrimaryKeyAuthorityManifest returned error: %v", err)
	}
	if loaded.Version != BSIPrimaryKeyAuthorityManifestVersion {
		t.Fatalf("loaded version = %d, want %d", loaded.Version, BSIPrimaryKeyAuthorityManifestVersion)
	}
	if loaded.GeneratedAt.IsZero() {
		t.Fatalf("loaded GeneratedAt is zero")
	}
	if loaded.Source != "test" {
		t.Fatalf("loaded source = %q, want test", loaded.Source)
	}
	if len(loaded.Entries) != 1 || loaded.Entries[0].ArtifactPath != entry.ArtifactPath {
		t.Fatalf("loaded entries = %+v", loaded.Entries)
	}
	loadedEntry := loaded.Entries[0]
	if loadedEntry.KeyCount != entry.KeyCount || loadedEntry.MinColumnID != entry.MinColumnID || loadedEntry.MaxColumnID != entry.MaxColumnID {
		t.Fatalf("loaded entry bounds/count = key_count:%d min:%d max:%d", loadedEntry.KeyCount, loadedEntry.MinColumnID, loadedEntry.MaxColumnID)
	}
	if loadedEntry.CatalogFingerprint != entry.CatalogFingerprint || loadedEntry.Fingerprint != entry.Fingerprint {
		t.Fatalf("loaded entry fingerprints = catalog:%q artifact:%q", loadedEntry.CatalogFingerprint, loadedEntry.Fingerprint)
	}
	if len(loadedEntry.Artifacts) != 1 || loadedEntry.Artifacts[0] != entry.Artifacts[0] {
		t.Fatalf("loaded artifacts = %+v", loadedEntry.Artifacts)
	}
	if observation := loaded.ObserveAgainstCatalog(map[string]*Table{"lineitem": table}); observation.Status != BSIPrimaryKeyAuthorityManifestStatusOK {
		t.Fatalf("observation status = %s detail=%s", observation.Status, observation.Detail)
	} else if observation.ArtifactDescriptors != 1 || observation.EntryKeyCount != entry.KeyCount {
		t.Fatalf("observation artifact/key summary = artifacts:%d key_count:%d", observation.ArtifactDescriptors, observation.EntryKeyCount)
	}
}

func TestBSIPrimaryKeyAuthorityManifestLoadMissingReturnsZeroValue(t *testing.T) {
	loaded, err := LoadBSIPrimaryKeyAuthorityManifest(t.TempDir())
	if err != nil {
		t.Fatalf("LoadBSIPrimaryKeyAuthorityManifest returned error: %v", err)
	}

	observation := loaded.ObserveAgainstCatalog(map[string]*Table{
		"lineitem": testPrimaryKeyAuthorityTable("lineitem", "l_orderkey", "", []shared.BasicAttribute{
			testPrimaryKeyAuthorityAttribute("l_orderkey", "Integer", "IntBSI", true),
		}),
	})
	if observation.Status != BSIPrimaryKeyAuthorityManifestStatusMissing {
		t.Fatalf("observation status = %s, want %s", observation.Status, BSIPrimaryKeyAuthorityManifestStatusMissing)
	}
}

func TestBSIPrimaryKeyAuthorityManifestLoadRejectsMalformedYAML(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(BSIPrimaryKeyAuthorityManifestPath(dir), []byte("entries:\n  - : bad\n"), 0644); err != nil {
		t.Fatalf("write malformed manifest returned error: %v", err)
	}

	_, err := LoadBSIPrimaryKeyAuthorityManifest(dir)
	if err == nil {
		t.Fatalf("LoadBSIPrimaryKeyAuthorityManifest returned nil error for malformed manifest")
	}
	if !strings.Contains(err.Error(), "parse primary-key authority manifest") {
		t.Fatalf("error = %v", err)
	}
}

func TestBSIPrimaryKeyAuthorityManifestLoadPreservesStaleVersionForObservation(t *testing.T) {
	dir := t.TempDir()
	table := testPrimaryKeyAuthorityTable("lineitem", "l_orderkey", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("l_orderkey", "Integer", "IntBSI", true),
	})
	entry, err := NewBSIPrimaryKeyAuthorityManifestEntry(table, "")
	if err != nil {
		t.Fatalf("NewBSIPrimaryKeyAuthorityManifestEntry returned error: %v", err)
	}
	if err := SaveBSIPrimaryKeyAuthorityManifest(dir, BSIPrimaryKeyAuthorityManifest{
		Version: BSIPrimaryKeyAuthorityManifestVersion + 1,
		Entries: []BSIPrimaryKeyAuthorityManifestEntry{
			entry,
		},
	}); err != nil {
		t.Fatalf("SaveBSIPrimaryKeyAuthorityManifest returned error: %v", err)
	}

	loaded, err := LoadBSIPrimaryKeyAuthorityManifest(dir)
	if err != nil {
		t.Fatalf("LoadBSIPrimaryKeyAuthorityManifest returned error: %v", err)
	}
	observation := loaded.ObserveAgainstCatalog(map[string]*Table{"lineitem": table})
	if observation.Status != BSIPrimaryKeyAuthorityManifestStatusInvalid {
		t.Fatalf("observation status = %s, want %s", observation.Status, BSIPrimaryKeyAuthorityManifestStatusInvalid)
	}
	if !strings.Contains(observation.Detail, "manifest version") {
		t.Fatalf("observation detail = %q", observation.Detail)
	}
}

func TestBSIPrimaryKeyAuthorityManifestObservationReportsStaleMissingTable(t *testing.T) {
	table := testPrimaryKeyAuthorityTable("lineitem", "l_orderkey", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("l_orderkey", "Integer", "IntBSI", true),
	})
	entry, err := NewBSIPrimaryKeyAuthorityManifestEntry(table, "")
	if err != nil {
		t.Fatalf("NewBSIPrimaryKeyAuthorityManifestEntry returned error: %v", err)
	}

	observation := BSIPrimaryKeyAuthorityManifest{
		Version: BSIPrimaryKeyAuthorityManifestVersion,
		Entries: []BSIPrimaryKeyAuthorityManifestEntry{
			entry,
		},
	}.ObserveAgainstCatalog(map[string]*Table{
		"orders": testPrimaryKeyAuthorityTable("orders", "o_orderkey", "", []shared.BasicAttribute{
			testPrimaryKeyAuthorityAttribute("o_orderkey", "Integer", "IntBSI", true),
		}),
	})

	if observation.Status != BSIPrimaryKeyAuthorityManifestStatusStale {
		t.Fatalf("observation status = %s, want %s", observation.Status, BSIPrimaryKeyAuthorityManifestStatusStale)
	}
	if !strings.Contains(observation.Detail, "table not found: lineitem") {
		t.Fatalf("observation detail = %q", observation.Detail)
	}
}

func TestBSIPrimaryKeyAuthorityManifestObservationReportsStaleKeyShapeMismatch(t *testing.T) {
	table := testPrimaryKeyAuthorityTable("lineitem", "l_orderkey+l_linenumber", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("l_orderkey", "Integer", "IntBSI", true),
		testPrimaryKeyAuthorityAttribute("l_linenumber", "Integer", "IntBSI", false),
	})
	entry, err := NewBSIPrimaryKeyAuthorityManifestEntry(table, "")
	if err != nil {
		t.Fatalf("NewBSIPrimaryKeyAuthorityManifestEntry returned error: %v", err)
	}
	entry.PrimaryKey = "l_linenumber+l_orderkey"
	entry.Fields[0], entry.Fields[1] = entry.Fields[1], entry.Fields[0]

	observation := BSIPrimaryKeyAuthorityManifest{
		Version: BSIPrimaryKeyAuthorityManifestVersion,
		Entries: []BSIPrimaryKeyAuthorityManifestEntry{
			entry,
		},
	}.ObserveAgainstCatalog(map[string]*Table{
		"lineitem": table,
	})

	if observation.Status != BSIPrimaryKeyAuthorityManifestStatusStale {
		t.Fatalf("observation status = %s, want %s", observation.Status, BSIPrimaryKeyAuthorityManifestStatusStale)
	}
	if !strings.Contains(observation.Detail, "primary key") {
		t.Fatalf("observation detail = %q", observation.Detail)
	}
}

func TestBSIPrimaryKeyAuthorityManifestObservationReportsStaleEncodingVersionMismatch(t *testing.T) {
	table := testPrimaryKeyAuthorityTable("lineitem", "l_orderkey", "l_shipdate", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("l_shipdate", "DateTime", "TimestampBSI", false),
		testPrimaryKeyAuthorityAttribute("l_orderkey", "Integer", "IntBSI", true),
	})
	entry, err := NewBSIPrimaryKeyAuthorityManifestEntry(table, "")
	if err != nil {
		t.Fatalf("NewBSIPrimaryKeyAuthorityManifestEntry returned error: %v", err)
	}
	entry.EncodingVersion = PrimaryKeyIdentityEncodingVersion + 1

	observation := BSIPrimaryKeyAuthorityManifest{
		Version: BSIPrimaryKeyAuthorityManifestVersion,
		Entries: []BSIPrimaryKeyAuthorityManifestEntry{
			entry,
		},
	}.ObserveAgainstCatalog(map[string]*Table{
		"lineitem": table,
	})

	if observation.Status != BSIPrimaryKeyAuthorityManifestStatusStale {
		t.Fatalf("observation status = %s, want %s", observation.Status, BSIPrimaryKeyAuthorityManifestStatusStale)
	}
	if !strings.Contains(observation.Detail, "encoding version") {
		t.Fatalf("observation detail = %q", observation.Detail)
	}
	if len(entry.Fields) != 2 || entry.Fields[0].Name != "l_shipdate" {
		t.Fatalf("time quantum field was not included first: %+v", entry.Fields)
	}
}

func TestBSIPrimaryKeyAuthorityManifestObservationRejectsDirtyArtifact(t *testing.T) {
	table := testPrimaryKeyAuthorityTable("lineitem", "l_orderkey", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("l_orderkey", "Integer", "IntBSI", true),
	})
	entry, err := NewBSIPrimaryKeyAuthorityManifestEntry(table, "")
	if err != nil {
		t.Fatalf("NewBSIPrimaryKeyAuthorityManifestEntry returned error: %v", err)
	}
	entry.Clean = false

	observation := BSIPrimaryKeyAuthorityManifest{
		Version: BSIPrimaryKeyAuthorityManifestVersion,
		Entries: []BSIPrimaryKeyAuthorityManifestEntry{
			entry,
		},
	}.ObserveAgainstCatalog(map[string]*Table{
		"lineitem": table,
	})

	if observation.Status != BSIPrimaryKeyAuthorityManifestStatusInvalid {
		t.Fatalf("observation status = %s, want %s", observation.Status, BSIPrimaryKeyAuthorityManifestStatusInvalid)
	}
	if !strings.Contains(observation.Detail, "not clean") {
		t.Fatalf("observation detail = %q", observation.Detail)
	}
}

func TestBSIPrimaryKeyAuthorityManifestObservationRejectsInvalidEntryBounds(t *testing.T) {
	table := testPrimaryKeyAuthorityTable("lineitem", "l_orderkey", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("l_orderkey", "Integer", "IntBSI", true),
	})
	entry, err := NewBSIPrimaryKeyAuthorityManifestEntry(table, "")
	if err != nil {
		t.Fatalf("NewBSIPrimaryKeyAuthorityManifestEntry returned error: %v", err)
	}
	entry.MinColumnID = 200
	entry.MaxColumnID = 100

	observation := BSIPrimaryKeyAuthorityManifest{
		Version: BSIPrimaryKeyAuthorityManifestVersion,
		Entries: []BSIPrimaryKeyAuthorityManifestEntry{
			entry,
		},
	}.ObserveAgainstCatalog(map[string]*Table{
		"lineitem": table,
	})

	if observation.Status != BSIPrimaryKeyAuthorityManifestStatusInvalid {
		t.Fatalf("observation status = %s, want %s", observation.Status, BSIPrimaryKeyAuthorityManifestStatusInvalid)
	}
	if !strings.Contains(observation.Detail, "column bounds") {
		t.Fatalf("observation detail = %q", observation.Detail)
	}
}

func TestBSIPrimaryKeyAuthorityManifestObservationRejectsInvalidArtifactMetadata(t *testing.T) {
	table := testPrimaryKeyAuthorityTable("lineitem", "l_orderkey", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("l_orderkey", "Integer", "IntBSI", true),
	})
	entry, err := NewBSIPrimaryKeyAuthorityManifestEntry(table, "")
	if err != nil {
		t.Fatalf("NewBSIPrimaryKeyAuthorityManifestEntry returned error: %v", err)
	}
	entry.Artifacts = []BSIPrimaryKeyAuthorityManifestArtifact{
		{
			Kind: "bsi",
		},
	}

	observation := BSIPrimaryKeyAuthorityManifest{
		Version: BSIPrimaryKeyAuthorityManifestVersion,
		Entries: []BSIPrimaryKeyAuthorityManifestEntry{
			entry,
		},
	}.ObserveAgainstCatalog(map[string]*Table{
		"lineitem": table,
	})

	if observation.Status != BSIPrimaryKeyAuthorityManifestStatusInvalid {
		t.Fatalf("observation status = %s, want %s", observation.Status, BSIPrimaryKeyAuthorityManifestStatusInvalid)
	}
	if !strings.Contains(observation.Detail, "missing path") {
		t.Fatalf("observation detail = %q", observation.Detail)
	}
}

func testPrimaryKeyAuthorityTable(name, primaryKey, timeQuantumField string, attrs []shared.BasicAttribute) *Table {
	table := &Table{
		BasicTable: &shared.BasicTable{
			Name:             name,
			PrimaryKey:       primaryKey,
			TimeQuantumField: timeQuantumField,
			Attributes:       attrs,
		},
		Attributes:       make([]Attribute, len(attrs)),
		AttributeNameMap: make(map[string]*Attribute, len(attrs)),
	}
	for i := range attrs {
		attr := Attribute{BasicAttribute: &table.BasicTable.Attributes[i], Parent: table}
		table.Attributes[i] = attr
		table.AttributeNameMap[attr.FieldName] = &table.Attributes[i]
	}
	return table
}

func testPrimaryKeyAuthorityAttribute(name, attrType, mapping string, columnID bool) shared.BasicAttribute {
	return shared.BasicAttribute{
		FieldName:       name,
		Type:            attrType,
		MappingStrategy: mapping,
		ColumnID:        columnID,
	}
}

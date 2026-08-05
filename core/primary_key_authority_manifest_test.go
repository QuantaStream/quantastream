package core

import (
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
	if entry.EncodingVersion != PrimaryKeyIdentityEncodingVersion {
		t.Fatalf("entry encoding version = %d, want %d", entry.EncodingVersion, PrimaryKeyIdentityEncodingVersion)
	}
}

func TestBSIPrimaryKeyAuthorityManifestObservationRejectsMissingTable(t *testing.T) {
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

	if observation.Status != BSIPrimaryKeyAuthorityManifestStatusInvalid {
		t.Fatalf("observation status = %s, want %s", observation.Status, BSIPrimaryKeyAuthorityManifestStatusInvalid)
	}
	if !strings.Contains(observation.Detail, "table not found: lineitem") {
		t.Fatalf("observation detail = %q", observation.Detail)
	}
}

func TestBSIPrimaryKeyAuthorityManifestObservationRejectsKeyShapeMismatch(t *testing.T) {
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

	if observation.Status != BSIPrimaryKeyAuthorityManifestStatusInvalid {
		t.Fatalf("observation status = %s, want %s", observation.Status, BSIPrimaryKeyAuthorityManifestStatusInvalid)
	}
	if !strings.Contains(observation.Detail, "primary key") {
		t.Fatalf("observation detail = %q", observation.Detail)
	}
}

func TestBSIPrimaryKeyAuthorityManifestObservationRejectsEncodingVersionMismatch(t *testing.T) {
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

	if observation.Status != BSIPrimaryKeyAuthorityManifestStatusInvalid {
		t.Fatalf("observation status = %s, want %s", observation.Status, BSIPrimaryKeyAuthorityManifestStatusInvalid)
	}
	if !strings.Contains(observation.Detail, "encoding version") {
		t.Fatalf("observation detail = %q", observation.Detail)
	}
	if len(entry.Fields) != 2 || entry.Fields[0].Name != "l_shipdate" {
		t.Fatalf("time quantum field was not included first: %+v", entry.Fields)
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

package core

import (
	"testing"

	"github.com/QuantaStream/quantastream/shared"
)

func TestObserveBSIPrimaryKeyAuthorityEligibilityAllowsSingleColumnBSI(t *testing.T) {
	table := testPrimaryKeyAuthorityTable("orders", "o_orderkey", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("o_orderkey", "Integer", "IntBSI", false),
	})

	observation := ObserveBSIPrimaryKeyAuthorityEligibility(table)

	if !observation.Eligible {
		t.Fatalf("eligible = false reason=%s", observation.Reason)
	}
	if observation.Mode != BSIPrimaryKeyAuthorityModeSingleColumnBSI {
		t.Fatalf("mode = %s, want %s", observation.Mode, BSIPrimaryKeyAuthorityModeSingleColumnBSI)
	}
	if observation.FieldName != "o_orderkey" || observation.MappingStrategy != "IntBSI" {
		t.Fatalf("observation = %+v", observation)
	}
}

func TestObserveBSIPrimaryKeyAuthorityEligibilityClassifiesDirectColumnID(t *testing.T) {
	table := testPrimaryKeyAuthorityTable("customer", "c_custkey", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("c_custkey", "Integer", "IntBSI", true),
	})

	observation := ObserveBSIPrimaryKeyAuthorityEligibility(table)

	if !observation.Eligible {
		t.Fatalf("eligible = false reason=%s", observation.Reason)
	}
	if observation.Mode != BSIPrimaryKeyAuthorityModeDirectColumnID {
		t.Fatalf("mode = %s, want %s", observation.Mode, BSIPrimaryKeyAuthorityModeDirectColumnID)
	}
	if !observation.ColumnID {
		t.Fatalf("ColumnID = false, want true")
	}
}

func TestObserveBSIPrimaryKeyAuthorityEligibilityRejectsCompoundKeys(t *testing.T) {
	table := testPrimaryKeyAuthorityTable("lineitem", "l_orderkey+l_linenumber", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("l_orderkey", "Integer", "IntBSI", false),
		testPrimaryKeyAuthorityAttribute("l_linenumber", "Integer", "IntBSI", false),
	})

	observation := ObserveBSIPrimaryKeyAuthorityEligibility(table)

	if observation.Eligible {
		t.Fatalf("eligible = true, want unsupported for compound key")
	}
	if observation.Reason != "primary key is compound" {
		t.Fatalf("reason = %q", observation.Reason)
	}
}

func TestObserveBSIPrimaryKeyAuthorityEligibilityRejectsNonBSIKeys(t *testing.T) {
	table := testPrimaryKeyAuthorityTable("sample", "id", "", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("id", "String", "StringEnum", false),
	})

	observation := ObserveBSIPrimaryKeyAuthorityEligibility(table)

	if observation.Eligible {
		t.Fatalf("eligible = true, want unsupported for non-BSI key")
	}
	if observation.Reason != "primary key field is not BSI-backed" {
		t.Fatalf("reason = %q", observation.Reason)
	}
}

func TestObserveBSIPrimaryKeyAuthorityEligibilityMarksShardScopedSingleKeys(t *testing.T) {
	table := testPrimaryKeyAuthorityTable("events", "event_id", "event_time", []shared.BasicAttribute{
		testPrimaryKeyAuthorityAttribute("event_time", "DateTime", "TimestampBSI", false),
		testPrimaryKeyAuthorityAttribute("event_id", "Integer", "IntBSI", false),
	})

	observation := ObserveBSIPrimaryKeyAuthorityEligibility(table)

	if !observation.Eligible {
		t.Fatalf("eligible = false reason=%s", observation.Reason)
	}
	if !observation.RequiresShardScope {
		t.Fatalf("RequiresShardScope = false, want true")
	}
	if observation.Mode != BSIPrimaryKeyAuthorityModeSingleColumnBSI {
		t.Fatalf("mode = %s, want %s", observation.Mode, BSIPrimaryKeyAuthorityModeSingleColumnBSI)
	}
}

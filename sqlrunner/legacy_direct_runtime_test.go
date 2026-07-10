package main

import (
	"errors"
	"reflect"
	"testing"

	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

func TestLegacyDirectSuiteTablesExtractsSimpleQueryTables(t *testing.T) {
	suite := &roadmap.Suite{Tests: []roadmap.TestCase{
		{ID: "part-count", Kind: "query", SQL: "select count(*) from part where p_partkey >= 1"},
		{ID: "orders-count", Kind: "query", SQL: "select count(*) from orders"},
		{ID: "supplier-subquery", Kind: "query", SQL: "select count(*) from partsupp as ps where ps.ps_suppkey not in (select s_suppkey from supplier where s_comment like '%Customer%Complaints%')"},
		{ID: "unsupported", Kind: "query", SQL: "select * from part"},
		{ID: "skip", Kind: "query", Status: roadmap.CaseSkip, SQL: "select count(*) from supplier"},
		{ID: "statement", Kind: "statement", SQL: "insert into part values (1)"},
		{ID: "delete", Kind: "statement", SQL: "delete from lineitems_qa where order_id = 1001"},
	}}

	got := legacyDirectSuiteTables(suite)
	want := []string{"lineitems_qa", "orders", "part", "partsupp", "supplier"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("legacyDirectSuiteTables = %v, want %v", got, want)
	}
}

func TestLegacyDirectServicePortDefaultsAndValidates(t *testing.T) {
	if got, err := legacyDirectServicePort(""); err != nil || got != 4000 {
		t.Fatalf("default port = %d, %v; want 4000, nil", got, err)
	}
	if got, err := legacyDirectServicePort("4100"); err != nil || got != 4100 {
		t.Fatalf("explicit port = %d, %v; want 4100, nil", got, err)
	}
	if _, err := legacyDirectServicePort("bad"); err == nil {
		t.Fatalf("expected invalid port error")
	}
}

func TestLegacyDirectConfigBackedTablesInDependencyOrder(t *testing.T) {
	got, err := legacyDirectConfigBackedTablesInDependencyOrder([]string{
		"deliveries_qa",
		"lineitems_qa",
		"orders_qa",
		"customers_qa",
	})
	if err != nil {
		t.Fatalf("dependency order: %v", err)
	}
	want := []string{"customers_qa", "orders_qa", "lineitems_qa", "deliveries_qa"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dependency order = %v, want %v", got, want)
	}
}

func TestLegacyDirectMissingTablePreloadErrorRecognizesNegativeCaseTables(t *testing.T) {
	if !legacyDirectMissingTablePreloadError("puke", errors.New("Error UnmarshalConsul: table puke not found")) {
		t.Fatalf("expected missing table preload error to be recognized")
	}
	if legacyDirectMissingTablePreloadError("puke", errors.New("permission denied")) {
		t.Fatalf("unexpected non-table preload error match")
	}
	if legacyDirectMissingTablePreloadError("orders", nil) {
		t.Fatalf("nil error should not match")
	}
}

func TestLegacyDirectAdminChangesTableRecognizesLifecycleCommands(t *testing.T) {
	for _, command := range []string{"create customers_qa", "drop customers_qa", "truncate customers_qa"} {
		if !legacyDirectAdminChangesTable(command) {
			t.Fatalf("expected %q to change table lifecycle", command)
		}
	}
	if legacyDirectAdminChangesTable("commit") {
		t.Fatalf("commit should not rebuild legacy-direct runtime")
	}
}

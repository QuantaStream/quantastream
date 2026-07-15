package roadmap

import (
	"errors"
	"testing"
)

func TestAdminDropMissingTableOKOnlyMatchesDeprecatedAdminDropShorthand(t *testing.T) {
	err := errors.New("Error 1105: table orders_qa doesn't exist")

	if !AdminDropMissingTableOK(TestCase{Kind: "admin", SQL: "drop orders_qa"}, err) {
		t.Fatal("deprecated admin drop shorthand should tolerate missing tables")
	}
	if AdminDropMissingTableOK(TestCase{Kind: "admin", SQL: "drop table orders_qa"}, err) {
		t.Fatal("real DROP TABLE SQL should stay strict")
	}
	if AdminDropMissingTableOK(TestCase{Kind: "statement", SQL: "drop orders_qa"}, err) {
		t.Fatal("non-admin tests should stay strict")
	}
	if AdminDropMissingTableOK(TestCase{Kind: "admin", SQL: "drop orders_qa"}, errors.New("permission denied")) {
		t.Fatal("only missing-table errors should be tolerated")
	}
}

func TestEvaluateStatementTreatsMissingDeprecatedAdminDropAsPass(t *testing.T) {
	test := TestCase{Kind: "admin", SQL: "drop orders_qa"}
	err := errors.New("Error 1105: table orders_qa doesn't exist")

	if details := evaluateStatement(test, 0, err); details != "" {
		t.Fatalf("details = %q, want pass", details)
	}
}

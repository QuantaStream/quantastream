package roadmap

import "testing"

func TestCaptureCompatibilityQueryCaseCanonicalizesReferenceResult(t *testing.T) {
	test := TestCase{
		ID:            "mysql_compat.001",
		Order:         "rowsort",
		Feature:       "select_projection",
		Compatibility: CompatibilityMySQL,
		Requires:      []string{"select"},
	}
	actual := QueryResult{
		Columns: []string{" Total "},
		Types:   []string{"NEWDECIMAL"},
		Rows: [][]Cell{
			{{Text: "2.000"}},
			{{Text: "1.00"}},
		},
	}

	captured := CaptureCompatibilityQueryCase(test, actual, DefaultCanonicalOptions())

	if captured.Columns[0] != "total" || captured.Types[0] != "DECIMAL" {
		t.Fatalf("captured columns/types = %#v/%#v", captured.Columns, captured.Types)
	}
	if captured.Rows[0][0].Text != "1" || captured.Rows[1][0].Text != "2" {
		t.Fatalf("captured rows = %#v, want sorted canonical numbers", captured.Rows)
	}
	if captured.Metadata.Feature != "select_projection" || captured.Metadata.Compatibility != CompatibilityMySQL {
		t.Fatalf("metadata = %#v", captured.Metadata)
	}
}

func TestCompareCompatibilityQueryCaseUsesCanonicalRules(t *testing.T) {
	test := TestCase{ID: "mysql_compat.001", Order: "rowsort"}
	expected := CaptureCompatibilityQueryCase(test, QueryResult{
		Columns: []string{"n"},
		Types:   []string{"INT"},
		Rows:    [][]Cell{{{Text: "2"}}, {{Text: "1"}}},
	}, DefaultCanonicalOptions())

	options := DefaultCanonicalOptions()
	options.SortRows = true
	details := CompareCompatibilityQueryCase(expected, QueryResult{
		Columns: []string{" N "},
		Types:   []string{"BIGINT"},
		Rows:    [][]Cell{{{Text: "01.0"}}, {{Text: "2.000"}}},
	}, options)

	if details != "" {
		t.Fatalf("compare details = %q, want canonical match", details)
	}
}

func TestParseCompatibilityExpectedNormalizesAndFindsCases(t *testing.T) {
	expected, err := ParseCompatibilityExpected([]byte(`
version: 1
suite: mysql_compat_select
cases:
  - id: select.one
    kind: QUERY
    columns: [One]
    types: [INT]
    rows:
      - [{text: "1"}]
    metadata:
      feature: Select_Projection
      compatibility: MYSQL
      requires: [SELECT]
`))
	if err != nil {
		t.Fatal(err)
	}

	test, ok := expected.CaseByID("select.one")
	if !ok {
		t.Fatal("captured case not found")
	}
	if test.Kind != "query" || test.Metadata.Feature != "select_projection" || test.Metadata.Requires[0] != "select" {
		t.Fatalf("case = %#v", test)
	}
}

func TestNewCompatibilityExpectedFileCopiesCases(t *testing.T) {
	suite := &Suite{Name: "mysql_compat"}
	cases := []CompatibilityExpectedCase{{ID: "case.one"}}

	expected := NewCompatibilityExpectedFile(suite, cases)
	cases[0].ID = "mutated"

	if expected.Version != CompatibilityExpectedVersion || expected.Suite != "mysql_compat" || expected.Cases[0].ID != "case.one" {
		t.Fatalf("expected file = %#v", expected)
	}
}

func TestCompareCompatibilityStatementCase(t *testing.T) {
	expected := CaptureCompatibilityStatementCase(TestCase{ID: "insert.one"}, 3)

	if details := CompareCompatibilityStatementCase(expected, 3); details != "" {
		t.Fatalf("details = %q, want match", details)
	}
	if details := CompareCompatibilityStatementCase(expected, 2); details != "affected rows differ: expected 3, actual 2" {
		t.Fatalf("details = %q", details)
	}
}

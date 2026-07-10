package roadmap

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseNormalizesDefaults(t *testing.T) {
	suite, err := Parse([]byte(`
version: 1
name: smoke
tests:
  - id: query.one
    sql: select 1
    expect:
      rows:
        - [1]
`))
	if err != nil {
		t.Fatal(err)
	}

	test := suite.Tests[0]
	if test.Status != CaseSupported {
		t.Fatalf("expected supported status, got %s", test.Status)
	}
	if test.Kind != "query" {
		t.Fatalf("expected query kind, got %s", test.Kind)
	}
	if test.Order != "exact" {
		t.Fatalf("expected exact order, got %s", test.Order)
	}
}

func TestParseAcceptsCaseTimeout(t *testing.T) {
	suite, err := Parse([]byte(`
version: 1
name: timeout
tests:
  - id: query.slow
    timeout: 60s
    sql: select 1
    expect:
      rows:
        - [1]
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := suite.Tests[0].CaseTimeout(); got != 60*time.Second {
		t.Fatalf("expected timeout 60s, got %s", got)
	}
}

func TestParseNormalizesExpectedDiagnostics(t *testing.T) {
	suite, err := Parse([]byte(`
version: 1
name: diagnostics
tests:
  - id: query.diagnostic
    sql: select 1
    expected_diagnostics: [UNSUPPORTED_JOIN]
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := suite.Tests[0].Diagnostics; len(got) != 1 || got[0] != "unsupported_join" {
		t.Fatalf("diagnostics = %#v, want normalized unsupported_join", got)
	}
}

func TestSuiteMaxCaseTimeout(t *testing.T) {
	suite, err := Parse([]byte(`
version: 1
name: timeout
tests:
  - id: query.default
    sql: select 1
    expect:
      rows:
        - [1]
  - id: query.slow
    timeout: 90s
    sql: select 2
    expect:
      rows:
        - [2]
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := suite.MaxCaseTimeout(); got != 90*time.Second {
		t.Fatalf("expected max timeout 90s, got %s", got)
	}
}

func TestParseRejectsInvalidCaseTimeout(t *testing.T) {
	_, err := Parse([]byte(`
version: 1
name: timeout
tests:
  - id: query.bad
    timeout: nope
    sql: select 1
    expect:
      rows:
        - [1]
`))
	if err == nil {
		t.Fatal("expected invalid timeout to fail parsing")
	}
}

func TestEvaluateQueryRowsortAndNull(t *testing.T) {
	test := TestCase{
		ID:     "query.rowsort",
		Status: CaseSupported,
		Order:  "rowsort",
		Expect: Expected{
			Columns: []string{"id", "name"},
			Rows: [][]interface{}{
				{2, nil},
				{1, "Abe"},
			},
		},
	}
	actual := QueryResult{
		Columns: []string{"id", "name"},
		Rows: [][]Cell{
			{{Text: "1"}, {Text: "Abe"}},
			{{Text: "2"}, {Null: true}},
		},
	}

	if details := evaluateQuery(test, actual, nil); details != "" {
		t.Fatal(details)
	}
}

func TestXFailAndXPassClassification(t *testing.T) {
	test := TestCase{ID: "roadmap.group-by", Status: CaseXFail}

	if result := classify(test, "not implemented"); result.Status != ResultXFail {
		t.Fatalf("expected XFAIL, got %s", result.Status)
	}
	if result := classify(test, ""); result.Status != ResultXPass {
		t.Fatalf("expected XPASS, got %s", result.Status)
	}
}

func TestCompareRowsReportsFirstDifference(t *testing.T) {
	expected := [][]Cell{{{Text: "1"}, {Text: "Abe"}}}
	actual := [][]Cell{{{Text: "1"}, {Text: "Abby"}}}

	details := compareRows(expected, actual)
	if details != `row 1 column 2 differs: expected "Abe", actual "Abby"` {
		t.Fatalf("unexpected difference: %s", details)
	}
}

func TestExpectedErrorPassesOnMatchingSubstring(t *testing.T) {
	test := TestCase{
		ID:     "errors.unknown-column",
		Status: CaseSupported,
		Expect: Expected{Error: "unknown column"},
	}

	if details := evaluateQuery(test, QueryResult{}, errors.New("Error 1105: Unknown Column foo")); details != "" {
		t.Fatal(details)
	}
}

func TestEvaluateQueryRowCountOnly(t *testing.T) {
	expectedCount := 2
	test := TestCase{
		ID:     "query.row-count",
		Status: CaseSupported,
		Expect: Expected{RowCount: &expectedCount},
	}
	actual := QueryResult{Rows: [][]Cell{{{Text: "one"}}, {{Text: "two"}}}}

	if details := evaluateQuery(test, actual, nil); details != "" {
		t.Fatal(details)
	}
}

func TestParseRoadmapSuiteFiles(t *testing.T) {
	patterns := []string{
		"../sqltests/*.yaml",
		"../../tpc-h-benchmark/sqltests/*.yaml",
	}
	for _, pattern := range patterns {
		paths, err := filepath.Glob(pattern)
		if err != nil {
			t.Fatal(err)
		}
		if len(paths) == 0 {
			t.Fatalf("expected at least one roadmap suite for %s", pattern)
		}
		for _, path := range paths {
			t.Run(filepath.Base(path), func(t *testing.T) {
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := Parse(data); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

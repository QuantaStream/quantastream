package roadmap

import "testing"

func TestCanonicalizeQueryResultNormalizesColumnsTypesAndNumericDisplay(t *testing.T) {
	result := QueryResult{
		Columns: []string{" Total ", "Name"},
		Types:   []string{"NEWDECIMAL", "VARCHAR"},
		Rows: [][]Cell{
			{{Text: "044.0000"}, {Text: " Abe "}},
		},
	}

	canonical := CanonicalizeQueryResult(result, DefaultCanonicalOptions())

	if got, want := canonical.Columns, []string{"total", "name"}; !equalStrings(got, want) {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	if got, want := canonical.Types, []string{"DECIMAL", "TEXT"}; !equalStrings(got, want) {
		t.Fatalf("types = %#v, want %#v", got, want)
	}
	if got, want := canonical.Rows[0][0].Text, "44"; got != want {
		t.Fatalf("numeric cell = %q, want %q", got, want)
	}
	if got, want := canonical.Rows[0][1].Text, "Abe"; got != want {
		t.Fatalf("text cell = %q, want %q", got, want)
	}
}

func TestCanonicalizeQueryResultPreservesStringLeadingZeros(t *testing.T) {
	result := QueryResult{
		Columns: []string{"code"},
		Types:   []string{"VARCHAR"},
		Rows:    [][]Cell{{{Text: "0012"}}},
	}

	canonical := CanonicalizeQueryResult(result, DefaultCanonicalOptions())

	if got := canonical.Rows[0][0].Text; got != "0012" {
		t.Fatalf("string cell = %q, want leading zeros preserved", got)
	}
}

func TestCanonicalizeQueryResultSortsRowsWhenRequested(t *testing.T) {
	options := DefaultCanonicalOptions()
	options.SortRows = true
	result := QueryResult{
		Columns: []string{"id"},
		Types:   []string{"INT"},
		Rows: [][]Cell{
			{Cell{Text: "2"}},
			{Cell{Text: "1"}},
		},
	}

	canonical := CanonicalizeQueryResult(result, options)

	if got := canonical.Rows[0][0].Text; got != "1" {
		t.Fatalf("first row = %q, want sorted numeric text 1", got)
	}
}

func TestCompareCanonicalQueryResultsReportsTypeWarningCandidate(t *testing.T) {
	expected := CanonicalQueryResult{
		Columns: []string{"value"},
		Types:   []string{"INTEGER"},
		Rows:    [][]Cell{{{Text: "1"}}},
	}
	actual := CanonicalQueryResult{
		Columns: []string{"value"},
		Types:   []string{"DECIMAL"},
		Rows:    [][]Cell{{{Text: "1"}}},
	}

	details := CompareCanonicalQueryResults(expected, actual)

	if details != "types differ: expected INTEGER, actual DECIMAL" {
		t.Fatalf("details = %q", details)
	}
}

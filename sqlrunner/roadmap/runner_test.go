package roadmap

import (
	"context"
	"testing"
)

type fakeEngine struct {
	queries []string
	execs   []string
}

func (e *fakeEngine) Query(_ context.Context, statement string) (QueryResult, error) {
	e.queries = append(e.queries, statement)
	return QueryResult{
		Columns: []string{"id"},
		Types:   []string{"INT"},
		Rows:    [][]Cell{{{Text: "7"}}},
	}, nil
}

func (e *fakeEngine) Exec(_ context.Context, statement string) (int64, error) {
	e.execs = append(e.execs, statement)
	return 2, nil
}

func TestRunnerUsesConfiguredEngineForQueryAndStatement(t *testing.T) {
	affectedRows := int64(2)
	suite := &Suite{
		Version: 1,
		Name:    "engine",
		Tests: []TestCase{
			{
				ID:     "query.one",
				Status: CaseSupported,
				Kind:   "query",
				SQL:    "select id from orders",
				Expect: Expected{
					Columns: []string{"id"},
					Types:   []string{"INT"},
					Rows:    [][]interface{}{{7}},
				},
			},
			{
				ID:     "statement.one",
				Status: CaseSupported,
				Kind:   "statement",
				SQL:    "insert into orders values (1)",
				Expect: Expected{AffectedRows: &affectedRows},
			},
			{
				ID:     "admin.one",
				Status: CaseSupported,
				Kind:   "admin",
				SQL:    "create customers_qa",
			},
		},
	}
	engine := &fakeEngine{}

	summary := (Runner{Engine: engine}).Run(context.Background(), suite)

	if summary.HasFailures() {
		t.Fatalf("expected configured engine cases to pass, got %#v", summary.Results)
	}
	if len(engine.queries) != 1 {
		t.Fatalf("queries = %v, want one query", engine.queries)
	}
	if engine.queries[0] != "select id from orders" {
		t.Fatalf("query = %q", engine.queries[0])
	}
	if len(engine.execs) != 2 {
		t.Fatalf("execs = %v, want two execs", engine.execs)
	}
	if engine.execs[0] != "insert into orders values (1)" {
		t.Fatalf("exec = %q", engine.execs[0])
	}
	if engine.execs[1] != "create table customers_qa" {
		t.Fatalf("admin exec = %q", engine.execs[1])
	}
}

func TestRunnerPassesCaseMetadataToAwareEngine(t *testing.T) {
	suite := &Suite{
		Version: 1,
		Name:    "case-aware",
		Tests: []TestCase{{
			ID:          "query.diagnostic",
			Status:      CaseSupported,
			Kind:        "query",
			SQL:         "select id from orders",
			Diagnostics: []string{"unsupported_join"},
			Expect: Expected{
				Columns: []string{"id"},
				Types:   []string{"INT"},
				Rows:    [][]interface{}{{7}},
			},
		}},
	}
	var seen []TestCase
	engine := caseAwareRecorder{seen: &seen}

	summary := (Runner{Engine: engine}).Run(context.Background(), suite)

	if summary.HasFailures() {
		t.Fatalf("expected case-aware engine case to pass, got %#v", summary.Results)
	}
	if len(seen) != 1 || seen[0].ID != "query.diagnostic" || len(seen[0].Diagnostics) != 1 {
		t.Fatalf("seen cases = %#v", seen)
	}
}

func TestEvaluateQueryAcceptsMySQLTinyIntBooleanTextForExpectedBool(t *testing.T) {
	test := TestCase{
		ID:     "query.bool_wire",
		Status: CaseSupported,
		Kind:   "query",
		Expect: Expected{
			Rows: [][]interface{}{
				{false, int64(4)},
				{true, int64(6)},
			},
		},
	}
	actual := QueryResult{
		Types: []string{"TINYINT", "BIGINT"},
		Rows: [][]Cell{
			{{Text: "0"}, {Text: "4"}},
			{{Text: "1"}, {Text: "6"}},
		},
	}

	if details := evaluateQuery(test, actual, nil); details != "" {
		t.Fatalf("details = %q, want bool-compatible pass", details)
	}
}

func TestEvaluateQueryKeepsBoolTextForBoolTypedResults(t *testing.T) {
	test := TestCase{
		ID:     "query.bool_direct",
		Status: CaseSupported,
		Kind:   "query",
		Expect: Expected{
			Rows: [][]interface{}{{true}},
		},
	}
	actual := QueryResult{
		Types: []string{"BOOL"},
		Rows:  [][]Cell{{{Text: "true"}}},
	}

	if details := evaluateQuery(test, actual, nil); details != "" {
		t.Fatalf("details = %q, want bool text pass", details)
	}
}

func TestCompareRowsTreatsEquivalentNumericTextAsEqual(t *testing.T) {
	expected := [][]Cell{{{Text: "1.193053225300001e+06"}}}
	actual := [][]Cell{{{Text: "1193053.225300001"}}}

	if details := compareRows(expected, actual); details != "" {
		t.Fatalf("details = %q, want numeric-text match", details)
	}
}

func TestEvaluateQueryAllowsConfiguredNumericTolerance(t *testing.T) {
	tolerance := 0.01
	test := TestCase{
		ID:     "query.numeric_tolerance",
		Status: CaseSupported,
		Kind:   "query",
		Expect: Expected{
			NumericTolerance: &tolerance,
			Rows:             [][]interface{}{{"22923.03"}},
		},
	}
	actual := QueryResult{Rows: [][]Cell{{{Text: "22923.028"}}}}

	if details := evaluateQuery(test, actual, nil); details != "" {
		t.Fatalf("details = %q, want numeric tolerance match", details)
	}
}

func TestEvaluateQueryRejectsValuesOutsideConfiguredNumericTolerance(t *testing.T) {
	tolerance := 0.01
	test := TestCase{
		ID:     "query.numeric_tolerance_reject",
		Status: CaseSupported,
		Kind:   "query",
		Expect: Expected{
			NumericTolerance: &tolerance,
			Rows:             [][]interface{}{{"22923.03"}},
		},
	}
	actual := QueryResult{Rows: [][]Cell{{{Text: "22923.00"}}}}

	if details := evaluateQuery(test, actual, nil); details == "" {
		t.Fatal("expected numeric mismatch outside tolerance to be reported")
	}
}

func TestCompareRowsRejectsDifferentNumericValues(t *testing.T) {
	expected := [][]Cell{{{Text: "108715630.20"}}}
	actual := [][]Cell{{{Text: "23582877.890000008"}}}

	if details := compareRows(expected, actual); details == "" {
		t.Fatal("expected numeric mismatch to be reported")
	}
}

func TestCompareRowsRejectsDifferentTextValues(t *testing.T) {
	expected := [][]Cell{{{Text: "BUILDING"}}}
	actual := [][]Cell{{{Text: "AUTOMOBILE"}}}

	if details := compareRows(expected, actual); details == "" {
		t.Fatal("expected text mismatch to be reported")
	}
}

type caseAwareRecorder struct {
	seen *[]TestCase
	test TestCase
}

func (e caseAwareRecorder) WithTestCase(test TestCase) Engine {
	e.test = test
	return e
}

func (e caseAwareRecorder) Query(ctx context.Context, statement string) (QueryResult, error) {
	*e.seen = append(*e.seen, e.test)
	return (&fakeEngine{}).Query(ctx, statement)
}

func (e caseAwareRecorder) Exec(ctx context.Context, statement string) (int64, error) {
	*e.seen = append(*e.seen, e.test)
	return (&fakeEngine{}).Exec(ctx, statement)
}

func TestRunnerCapturesCompatibilityExpectedResults(t *testing.T) {
	suite := &Suite{
		Version: 1,
		Name:    "capture",
		Tests: []TestCase{
			{
				ID:            "capture.001.query",
				Status:        CaseSupported,
				Kind:          "query",
				Order:         "rowsort",
				Feature:       "select_projection",
				Compatibility: CompatibilityMySQL,
				SQL:           "select id from orders",
			},
			{
				ID:     "capture.002.statement",
				Status: CaseSupported,
				Kind:   "statement",
				SQL:    "insert into orders values (1)",
			},
		},
	}
	engine := &fakeEngine{}

	result := (Runner{Engine: engine}).CaptureCompatibilityExpected(context.Background(), suite, CompatibilityCaptureOptions{})

	if result.Summary.HasFailures() {
		t.Fatalf("capture summary = %#v, want no failures", result.Summary.Results)
	}
	if result.Expected.Suite != "capture" {
		t.Fatalf("suite = %q, want capture", result.Expected.Suite)
	}
	if len(result.Expected.Cases) != 2 {
		t.Fatalf("cases = %#v, want 2", result.Expected.Cases)
	}
	if len(result.Suite.Tests) != 2 {
		t.Fatalf("generated suite tests = %#v, want 2", result.Suite.Tests)
	}
	if got := result.Suite.Tests[0].Expect.Rows[0][0]; got != "7" {
		t.Fatalf("generated suite first row = %#v, want 7", got)
	}
	queryCase := result.Expected.Cases[0]
	if queryCase.ID != "capture.001.query" || queryCase.Kind != "query" {
		t.Fatalf("query case = %#v", queryCase)
	}
	if len(queryCase.Rows) != 1 || queryCase.Rows[0][0].Text != "7" {
		t.Fatalf("query rows = %#v, want 7", queryCase.Rows)
	}
	if queryCase.Types[0] != "INTEGER" {
		t.Fatalf("query types = %#v, want INTEGER", queryCase.Types)
	}
	statementCase := result.Expected.Cases[1]
	if statementCase.AffectedRows == nil || *statementCase.AffectedRows != 2 {
		t.Fatalf("affected rows = %#v, want 2", statementCase.AffectedRows)
	}
}

func TestRunnerCaptureExecutesAdminWithoutCapturingExpectedCase(t *testing.T) {
	suite := &Suite{
		Version: 1,
		Name:    "capture-admin",
		Tests: []TestCase{
			{ID: "capture.001.admin", Status: CaseSupported, Kind: "admin", SQL: "create table t"},
			{ID: "capture.002.query", Status: CaseSupported, Kind: "query", SQL: "select id from t"},
		},
	}
	engine := &fakeEngine{}

	result := (Runner{
		Engine: engine,
	}).CaptureCompatibilityExpected(context.Background(), suite, CompatibilityCaptureOptions{})

	if len(engine.execs) != 1 || engine.execs[0] != "create table t" {
		t.Fatalf("admin execs = %#v, want normalized create table", engine.execs)
	}
	if len(result.Expected.Cases) != 1 || result.Expected.Cases[0].ID != "capture.002.query" {
		t.Fatalf("captured cases = %#v, want only query", result.Expected.Cases)
	}
}

func TestBuildCompatibilityExpectedSuiteCanBeParsedAsRoadmapSuite(t *testing.T) {
	suite := &Suite{
		Version: 1,
		Name:    "capture-parse",
		Tests: []TestCase{{
			ID:            "capture.001.query",
			Status:        CaseSupported,
			Kind:          "query",
			Feature:       "select_projection",
			Compatibility: CompatibilityMySQL,
			SQL:           "select 1 as one",
		}},
	}
	capture := (Runner{Engine: &fakeEngine{}}).CaptureCompatibilityExpected(context.Background(), suite, CompatibilityCaptureOptions{})
	data, err := MarshalCompatibilityExpectedSuite(capture.Suite)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(data)
	if err != nil {
		t.Fatalf("generated compatibility suite should parse: %v\n%s", err, data)
	}
	if got := parsed.Tests[0].Expect.Rows[0][0]; got != "7" {
		t.Fatalf("parsed expected value = %#v, want 7", got)
	}
}

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
	if len(engine.execs) != 1 {
		t.Fatalf("execs = %v, want one exec", engine.execs)
	}
	if engine.execs[0] != "insert into orders values (1)" {
		t.Fatalf("exec = %q", engine.execs[0])
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

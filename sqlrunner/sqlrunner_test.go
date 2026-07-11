package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

func TestValidateFlagsAllowsRuntimeWithoutProxyConnectionFlags(t *testing.T) {
	cfg := runnerConfig{Engine: engineRuntime}

	if err := validateFlags("sqltests/basic_queries.yaml", cfg); err != nil {
		t.Fatalf("runtime validation should not require proxy host/user: %v", err)
	}
}

func TestValidateFlagsAllowsLegacyDirectWithoutProxyConnectionFlags(t *testing.T) {
	cfg := runnerConfig{Engine: engineLegacyDirect}

	if err := validateFlags("sqltests/legacy_direct_smoke.yaml", cfg); err != nil {
		t.Fatalf("legacy-direct validation should not require proxy host/user: %v", err)
	}
}

func TestValidateFlagsAllowsMySQLReferenceWithDSN(t *testing.T) {
	cfg := runnerConfig{Engine: engineMySQLReference, MySQLDSN: "user:pass@tcp(127.0.0.1:3306)/test"}

	if err := validateFlags("sqltests/mysql_compat_select.yaml", cfg); err != nil {
		t.Fatalf("mysql-reference validation should accept DSN: %v", err)
	}
}

func TestValidateFlagsRequiresMySQLReferenceDSN(t *testing.T) {
	if err := validateFlags("sqltests/mysql_compat_select.yaml", runnerConfig{Engine: engineMySQLReference}); err == nil {
		t.Fatal("mysql-reference validation should require DSN")
	}
}

func TestValidateFlagsAllowsEngineDiffWithValidEngines(t *testing.T) {
	cfg := runnerConfig{EngineDiff: "runtime,runtime"}

	if err := validateFlags("sqltests/mysql_compat_select.yaml", cfg); err != nil {
		t.Fatalf("engine_diff validation should accept runtime pair: %v", err)
	}
}

func TestValidateFlagsRequiresMySQLReferenceDSNInEngineDiff(t *testing.T) {
	cfg := runnerConfig{EngineDiff: "mysql-reference,runtime"}

	if err := validateFlags("sqltests/mysql_compat_select.yaml", cfg); err == nil {
		t.Fatal("engine_diff validation should require DSN when mysql-reference is present")
	}
}

func TestValidateFlagsRequiresProxyConnectionFlags(t *testing.T) {
	cfg := runnerConfig{Engine: engineProxy}

	if err := validateFlags("sqltests/basic_queries.yaml", cfg); err == nil {
		t.Fatal("proxy validation should require host and user")
	}
}

func TestValidateFlagsRejectsUnknownEngine(t *testing.T) {
	cfg := runnerConfig{Engine: "bogus"}

	if err := validateFlags("sqltests/basic_queries.yaml", cfg); err == nil {
		t.Fatal("unknown engine should fail validation")
	}
}

func TestFilterSuiteCaseKeepsOnlyMatchingCase(t *testing.T) {
	suite := &roadmap.Suite{
		Name: "smoke",
		Tests: []roadmap.TestCase{
			{ID: "smoke.001.first"},
			{ID: "smoke.002.second"},
		},
	}

	if err := filterSuiteCase(suite, "smoke.002.second"); err != nil {
		t.Fatal(err)
	}
	if len(suite.Tests) != 1 {
		t.Fatalf("filtered tests = %d, want 1", len(suite.Tests))
	}
	if suite.Tests[0].ID != "smoke.002.second" {
		t.Fatalf("filtered case = %q, want smoke.002.second", suite.Tests[0].ID)
	}
}

func TestFilterSuiteCaseRejectsMissingCase(t *testing.T) {
	suite := &roadmap.Suite{
		Name:  "smoke",
		Tests: []roadmap.TestCase{{ID: "smoke.001.first"}},
	}

	if err := filterSuiteCase(suite, "smoke.404.missing"); err == nil {
		t.Fatal("missing case should fail")
	}
}

func TestLegacyDirectRawSQLTablesFindsNestedScalarSubquerySources(t *testing.T) {
	sql := `select ps.ps_partkey,
       sum(ps.ps_supplycost * ps.ps_availqty) as part_value
from partsupp as ps
inner join supplier as s on s.s_suppkey = ps.ps_suppkey
inner join nation as n on n.n_nationkey = s.s_nationkey
where n.n_name = 'GERMANY'
group by ps.ps_partkey
having part_value > (
  select sum(ps2.ps_supplycost * ps2.ps_availqty) * 0.0001
  from partsupp as ps2
  inner join supplier as s2 on s2.s_suppkey = ps2.ps_suppkey
  inner join nation as n2 on n2.n_nationkey = s2.s_nationkey
  where n2.n_name = 'GERMANY'
)`

	got := legacyDirectRawSQLTables(sql)
	want := []string{"nation", "partsupp", "supplier"}
	if len(got) != len(want) {
		t.Fatalf("tables = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("tables = %#v, want %#v", got, want)
		}
	}
}

func TestSlowCaseResultsSortsCasesAtOrAboveThreshold(t *testing.T) {
	summary := roadmap.Summary{Results: []roadmap.CaseResult{
		{ID: "fast", Duration: 999 * time.Millisecond},
		{ID: "slow_b", Duration: 12 * time.Second},
		{ID: "slow_a", Duration: 12 * time.Second},
		{ID: "slower", Duration: 30 * time.Second},
	}}

	got := slowCaseResults(summary, 10*time.Second)
	want := []string{"slower", "slow_a", "slow_b"}
	if len(got) != len(want) {
		t.Fatalf("slow cases = %#v, want ids %v", got, want)
	}
	for i := range want {
		if got[i].ID != want[i] {
			t.Fatalf("slow cases = %#v, want ids %v", got, want)
		}
	}
}

func TestSlowCaseResultsDisabledWithoutThreshold(t *testing.T) {
	summary := roadmap.Summary{Results: []roadmap.CaseResult{
		{ID: "slow", Duration: 30 * time.Second},
	}}
	if got := slowCaseResults(summary, 0); len(got) != 0 {
		t.Fatalf("slow cases = %#v, want none", got)
	}
}

func TestParseEngineDiffRequiresReferenceAndTarget(t *testing.T) {
	diff, err := parseEngineDiff(" mysql-reference , runtime ")
	if err != nil {
		t.Fatal(err)
	}
	if diff.Reference != engineMySQLReference || diff.Target != engineRuntime {
		t.Fatalf("diff = %#v, want mysql-reference -> runtime", diff)
	}
	if _, err := parseEngineDiff("runtime"); err == nil {
		t.Fatal("single engine diff should fail")
	}
	if _, err := parseEngineDiff("runtime,"); err == nil {
		t.Fatal("empty target engine diff should fail")
	}
}

type captureMainFakeEngine struct{}

func (captureMainFakeEngine) Query(_ context.Context, _ string) (roadmap.QueryResult, error) {
	return roadmap.QueryResult{
		Columns: []string{"Value"},
		Types:   []string{"INT"},
		Rows:    [][]roadmap.Cell{{{Text: "1.0"}}},
	}, nil
}

func (captureMainFakeEngine) Exec(_ context.Context, _ string) (int64, error) {
	return 3, nil
}

func TestCaptureCompatibilityExpectedWritesFile(t *testing.T) {
	path := t.TempDir() + "/expected.yaml"
	suite := &roadmap.Suite{
		Version: 1,
		Name:    "capture-main",
		Tests: []roadmap.TestCase{{
			ID:            "capture.001.query",
			Status:        roadmap.CaseSupported,
			Kind:          "query",
			Feature:       "select_projection",
			Compatibility: roadmap.CompatibilityMySQL,
			SQL:           "select 1",
		}},
	}
	runner := roadmap.Runner{Engine: captureMainFakeEngine{}}

	if err := captureCompatibilityExpected(context.Background(), suite, runner, runnerConfig{CaptureExpected: path}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	captured, err := roadmap.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(captured.Tests) != 1 {
		t.Fatalf("captured tests = %#v, want 1", captured.Tests)
	}
	if got := captured.Tests[0].Expect.Rows[0][0]; got != "1" {
		t.Fatalf("captured value = %#v, want canonical 1", got)
	}
}

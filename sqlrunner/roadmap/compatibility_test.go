package roadmap

import "testing"

func TestParseCompatibilityMetadata(t *testing.T) {
	suite, err := Parse([]byte(`
version: 1
name: mysql_compat
tests:
  - id: select.literal
    feature: Select_Projection
    compatibility: MYSQL
    requires: [SELECT, Literal_Projection]
    sql: select 1
    expect:
      rows:
        - [1]
`))
	if err != nil {
		t.Fatal(err)
	}

	test := suite.Tests[0]
	if test.Feature != "select_projection" {
		t.Fatalf("feature = %q", test.Feature)
	}
	if test.Compatibility != CompatibilityMySQL {
		t.Fatalf("compatibility = %q", test.Compatibility)
	}
	if got, want := test.Requires, []string{"select", "literal_projection"}; !equalStrings(got, want) {
		t.Fatalf("requires = %#v, want %#v", got, want)
	}
}

func TestParseRejectsCompatibilityWithoutFeature(t *testing.T) {
	_, err := Parse([]byte(`
version: 1
name: mysql_compat
tests:
  - id: select.literal
    compatibility: mysql
    sql: select 1
    expect:
      rows:
        - [1]
`))
	if err == nil {
		t.Fatal("expected compatibility without feature to fail parsing")
	}
}

func TestBuildCompatibilityReportCountsByFeatureAndCategory(t *testing.T) {
	suite := &Suite{
		Name: "mysql_compat",
		Tests: []TestCase{
			{ID: "select.pass", Feature: "select_projection", Compatibility: CompatibilityMySQL},
			{ID: "select.type", Feature: "select_projection", Compatibility: CompatibilityMySQL},
			{ID: "join.unsupported", Feature: "joins_outer", Compatibility: CompatibilityMySQL},
		},
	}
	summary := Summary{
		Suite: "mysql_compat",
		Results: []CaseResult{
			{ID: "select.pass", Status: ResultPass},
			{ID: "select.type", Status: ResultFail, Details: "types differ: expected INTEGER, actual DECIMAL"},
			{ID: "join.unsupported", Status: ResultXFail, Details: "not wired"},
		},
	}

	report := BuildCompatibilityReport(suite, summary)

	if report.Counts[CompatibilityResultPass] != 1 {
		t.Fatalf("PASS count = %d", report.Counts[CompatibilityResultPass])
	}
	if report.Counts[CompatibilityResultTypeWarn] != 1 {
		t.Fatalf("TYPE_WARN count = %d", report.Counts[CompatibilityResultTypeWarn])
	}
	if report.Counts[CompatibilityResultUnsupported] != 1 {
		t.Fatalf("UNSUPPORTED count = %d", report.Counts[CompatibilityResultUnsupported])
	}
	if len(report.Features) != 2 {
		t.Fatalf("features = %#v, want two feature summaries", report.Features)
	}
	if report.Features[0].Feature != "joins_outer" || report.Features[0].Counts[CompatibilityResultUnsupported] != 1 {
		t.Fatalf("first feature summary = %#v", report.Features[0])
	}
	if report.Features[1].Feature != "select_projection" || report.Features[1].Counts[CompatibilityResultPass] != 1 || report.Features[1].Counts[CompatibilityResultTypeWarn] != 1 {
		t.Fatalf("second feature summary = %#v", report.Features[1])
	}
}

func TestBuildCompatibilityReportFallsBackToUncategorized(t *testing.T) {
	report := BuildCompatibilityReport(nil, Summary{
		Suite: "ad_hoc",
		Results: []CaseResult{
			{ID: "case.one", Status: ResultPass},
		},
	})

	if len(report.Cases) != 1 || report.Cases[0].Feature != "uncategorized" {
		t.Fatalf("cases = %#v, want uncategorized fallback", report.Cases)
	}
}

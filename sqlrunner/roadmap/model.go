package roadmap

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

const (
	CaseSupported = "supported"
	CaseXFail     = "xfail"
	CaseSkip      = "skip"

	ResultPass  = "PASS"
	ResultFail  = "FAIL"
	ResultXFail = "XFAIL"
	ResultXPass = "XPASS"
	ResultSkip  = "SKIP"
)

type Suite struct {
	Version int        `yaml:"version"`
	Name    string     `yaml:"name"`
	Tests   []TestCase `yaml:"tests"`
}

type TestCase struct {
	ID              string        `yaml:"id"`
	Status          string        `yaml:"status"`
	Kind            string        `yaml:"kind"`
	Order           string        `yaml:"order"`
	Capabilities    []string      `yaml:"capabilities"`
	Feature         string        `yaml:"feature"`
	Compatibility   string        `yaml:"compatibility"`
	Requires        []string      `yaml:"requires"`
	Diagnostics     []string      `yaml:"expected_diagnostics"`
	Issue           string        `yaml:"issue"`
	Timeout         string        `yaml:"timeout"`
	TimeoutDuration time.Duration `yaml:"-"`
	SQL             string        `yaml:"sql"`
	Expect          Expected      `yaml:"expect"`
}

type Expected struct {
	Columns                  []string        `yaml:"columns"`
	Types                    []string        `yaml:"types"`
	Rows                     [][]interface{} `yaml:"rows"`
	RowCount                 *int            `yaml:"row_count"`
	AffectedRows             *int64          `yaml:"affected_rows"`
	Error                    string          `yaml:"error_contains"`
	NumericTolerance         *float64        `yaml:"numeric_tolerance"`
	NumericRelativeTolerance *float64        `yaml:"numeric_relative_tolerance"`
}

type Cell struct {
	Null bool
	Text string
}

// ProfileRow is one structured row from an engine-owned query profile.
type ProfileRow struct {
	Kind    string `json:"kind"`
	Section string `json:"section"`
	Name    string `json:"name"`
	Value   string `json:"value"`
	Detail  string `json:"detail,omitempty"`
}

type QueryResult struct {
	Columns []string
	Types   []string
	Rows    [][]Cell
}

type CaseResult struct {
	ID           string
	Status       string
	Details      string
	Duration     time.Duration
	Profile      []ProfileRow
	ProfileError string
}

type Summary struct {
	Suite   string
	Results []CaseResult
}

func Parse(data []byte) (*Suite, error) {
	var suite Suite
	if err := yaml.UnmarshalStrict(data, &suite); err != nil {
		return nil, err
	}
	if suite.Version != 1 {
		return nil, fmt.Errorf("unsupported suite version %d", suite.Version)
	}
	if strings.TrimSpace(suite.Name) == "" {
		return nil, fmt.Errorf("suite name is required")
	}
	for i := range suite.Tests {
		if err := normalizeTestCase(&suite.Tests[i]); err != nil {
			return nil, fmt.Errorf("test %d: %w", i+1, err)
		}
	}
	return &suite, nil
}

func normalizeTestCase(test *TestCase) error {
	test.ID = strings.TrimSpace(test.ID)
	test.Status = strings.ToLower(strings.TrimSpace(test.Status))
	test.Kind = strings.ToLower(strings.TrimSpace(test.Kind))
	test.Order = strings.ToLower(strings.TrimSpace(test.Order))
	test.Feature = normalizeTag(test.Feature)
	test.Compatibility = normalizeTag(test.Compatibility)
	test.SQL = strings.TrimSpace(test.SQL)
	test.Timeout = strings.TrimSpace(test.Timeout)
	for i := range test.Diagnostics {
		test.Diagnostics[i] = strings.ToLower(strings.TrimSpace(test.Diagnostics[i]))
	}
	test.Requires = normalizeTags(test.Requires)

	if test.Timeout != "" {
		duration, err := time.ParseDuration(test.Timeout)
		if err != nil || duration <= 0 {
			return fmt.Errorf("%s has invalid timeout %q", test.ID, test.Timeout)
		}
		test.TimeoutDuration = duration
	}

	if test.ID == "" {
		return fmt.Errorf("id is required")
	}
	if test.Status == "" {
		test.Status = CaseSupported
	}
	switch test.Status {
	case CaseSupported, CaseXFail, CaseSkip:
	default:
		return fmt.Errorf("%s has invalid status %q", test.ID, test.Status)
	}
	if test.Kind == "" {
		test.Kind = inferKind(test.SQL)
	}
	switch test.Kind {
	case "query", "statement", "admin":
	default:
		return fmt.Errorf("%s has invalid kind %q", test.ID, test.Kind)
	}
	if test.Order == "" {
		test.Order = "exact"
	}
	switch test.Order {
	case "exact", "rowsort":
	default:
		return fmt.Errorf("%s has invalid order %q", test.ID, test.Order)
	}
	if test.Status != CaseSkip && test.SQL == "" {
		return fmt.Errorf("%s requires sql", test.ID)
	}
	if test.Kind == "query" && test.Expect.AffectedRows != nil {
		return fmt.Errorf("%s query cannot expect affected_rows", test.ID)
	}
	if test.Kind != "query" && (len(test.Expect.Rows) > 0 || test.Expect.RowCount != nil) {
		return fmt.Errorf("%s statement cannot expect rows", test.ID)
	}
	if test.Kind != "query" && (test.Expect.NumericTolerance != nil || test.Expect.NumericRelativeTolerance != nil) {
		return fmt.Errorf("%s statement cannot expect numeric tolerance", test.ID)
	}
	if test.Expect.NumericTolerance != nil && *test.Expect.NumericTolerance < 0 {
		return fmt.Errorf("%s has invalid numeric_tolerance %v", test.ID, *test.Expect.NumericTolerance)
	}
	if test.Expect.NumericRelativeTolerance != nil && *test.Expect.NumericRelativeTolerance < 0 {
		return fmt.Errorf("%s has invalid numeric_relative_tolerance %v", test.ID, *test.Expect.NumericRelativeTolerance)
	}
	if test.Compatibility != "" {
		switch test.Compatibility {
		case CompatibilityMySQL, CompatibilityQuanta, CompatibilityQuantaExtension:
		default:
			return fmt.Errorf("%s has invalid compatibility %q", test.ID, test.Compatibility)
		}
		if test.Feature == "" {
			return fmt.Errorf("%s compatibility metadata requires feature", test.ID)
		}
	}
	return nil
}

func normalizeTag(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func normalizeTags(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	for _, value := range values {
		normalized := normalizeTag(value)
		if normalized != "" {
			result = append(result, normalized)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func (t TestCase) CaseTimeout() time.Duration {
	if t.TimeoutDuration > 0 {
		return t.TimeoutDuration
	}
	return defaultCaseTimeout
}

func (s Suite) MaxCaseTimeout() time.Duration {
	maxTimeout := defaultCaseTimeout
	for _, test := range s.Tests {
		if timeout := test.CaseTimeout(); timeout > maxTimeout {
			maxTimeout = timeout
		}
	}
	return maxTimeout
}

func inferKind(sql string) string {
	fields := strings.Fields(strings.ToLower(sql))
	if len(fields) > 0 && (fields[0] == "select" || fields[0] == "show") {
		return "query"
	}
	return "statement"
}

func (s Summary) HasFailures() bool {
	for _, result := range s.Results {
		if result.Status == ResultFail || result.Status == ResultXPass {
			return true
		}
	}
	return false
}

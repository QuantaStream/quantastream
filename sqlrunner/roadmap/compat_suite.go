package roadmap

import "fmt"

// BuildCompatibilityExpectedSuite returns a runnable SQLRunner suite with expectations
// populated from captured reference results.
func BuildCompatibilityExpectedSuite(source *Suite, expected CompatibilityExpectedFile) Suite {
	if source == nil {
		return Suite{Version: CompatibilityExpectedVersion, Name: expected.Suite}
	}
	generated := Suite{
		Version: source.Version,
		Name:    source.Name,
		Tests:   make([]TestCase, len(source.Tests)),
	}
	capturedByID := make(map[string]CompatibilityExpectedCase, len(expected.Cases))
	for _, captured := range expected.Cases {
		captured.normalize()
		capturedByID[captured.ID] = captured
	}
	for i, test := range source.Tests {
		generated.Tests[i] = test
		if compatibilityExpectedShouldPreserveSource(test.Expect) {
			continue
		}
		captured, ok := capturedByID[test.ID]
		if !ok {
			continue
		}
		generated.Tests[i].Expect = captured.ExpectedBlock()
	}
	return generated
}

func compatibilityExpectedShouldPreserveSource(expected Expected) bool {
	if expected.Rows != nil || expected.AffectedRows != nil || expected.Error != "" {
		return false
	}
	return len(expected.Columns) > 0 || len(expected.Types) > 0 || expected.RowCount != nil
}

// ExpectedBlock converts a captured reference result into a SQLRunner expectation.
func (c CompatibilityExpectedCase) ExpectedBlock() Expected {
	if c.Error != "" {
		return Expected{Error: c.Error}
	}
	if c.Kind == "statement" {
		return Expected{AffectedRows: c.AffectedRows}
	}
	return Expected{
		Columns: append([]string(nil), c.Columns...),
		Rows:    compatibilityExpectedRows(c.Rows),
	}
}

// MarshalCompatibilityExpectedSuite serializes a runnable SQLRunner suite with compact YAML.
func MarshalCompatibilityExpectedSuite(suite Suite) ([]byte, error) {
	data, err := yamlMarshal(compatibilitySuiteYAMLFromSuite(suite))
	if err != nil {
		return nil, fmt.Errorf("marshal compatibility expected suite: %w", err)
	}
	return data, nil
}

type compatibilitySuiteYAML struct {
	Version int                     `yaml:"version"`
	Name    string                  `yaml:"name"`
	Tests   []compatibilityTestYAML `yaml:"tests"`
}

type compatibilityTestYAML struct {
	ID            string                     `yaml:"id"`
	Status        string                     `yaml:"status,omitempty"`
	Kind          string                     `yaml:"kind,omitempty"`
	Order         string                     `yaml:"order,omitempty"`
	Capabilities  []string                   `yaml:"capabilities,omitempty"`
	Feature       string                     `yaml:"feature,omitempty"`
	Compatibility string                     `yaml:"compatibility,omitempty"`
	Requires      []string                   `yaml:"requires,omitempty"`
	Diagnostics   []string                   `yaml:"expected_diagnostics,omitempty"`
	Issue         string                     `yaml:"issue,omitempty"`
	Timeout       string                     `yaml:"timeout,omitempty"`
	SQL           string                     `yaml:"sql"`
	Expect        *compatibilityExpectedYAML `yaml:"expect,omitempty"`
}

type compatibilityExpectedYAML struct {
	Columns      []string        `yaml:"columns,omitempty"`
	Rows         [][]interface{} `yaml:"rows,omitempty"`
	RowCount     *int            `yaml:"row_count,omitempty"`
	AffectedRows *int64          `yaml:"affected_rows,omitempty"`
	Error        string          `yaml:"error_contains,omitempty"`
}

func compatibilitySuiteYAMLFromSuite(suite Suite) compatibilitySuiteYAML {
	out := compatibilitySuiteYAML{
		Version: suite.Version,
		Name:    suite.Name,
		Tests:   make([]compatibilityTestYAML, len(suite.Tests)),
	}
	for i, test := range suite.Tests {
		out.Tests[i] = compatibilityTestYAML{
			ID:            test.ID,
			Status:        test.Status,
			Kind:          test.Kind,
			Order:         test.Order,
			Capabilities:  append([]string(nil), test.Capabilities...),
			Feature:       test.Feature,
			Compatibility: test.Compatibility,
			Requires:      append([]string(nil), test.Requires...),
			Diagnostics:   append([]string(nil), test.Diagnostics...),
			Issue:         test.Issue,
			Timeout:       test.Timeout,
			SQL:           test.SQL,
			Expect:        compatibilityExpectedYAMLFromExpected(test.Expect),
		}
	}
	return out
}

func compatibilityExpectedYAMLFromExpected(expect Expected) *compatibilityExpectedYAML {
	if len(expect.Columns) == 0 && len(expect.Rows) == 0 && expect.RowCount == nil && expect.AffectedRows == nil && expect.Error == "" {
		return nil
	}
	return &compatibilityExpectedYAML{
		Columns:      append([]string(nil), expect.Columns...),
		Rows:         cloneExpectedRows(expect.Rows),
		RowCount:     expect.RowCount,
		AffectedRows: expect.AffectedRows,
		Error:        expect.Error,
	}
}

func compatibilityExpectedRows(rows [][]CompatibilityCell) [][]interface{} {
	out := make([][]interface{}, len(rows))
	for i, row := range rows {
		out[i] = make([]interface{}, len(row))
		for j, cell := range row {
			if cell.Null {
				out[i][j] = nil
			} else {
				out[i][j] = cell.Text
			}
		}
	}
	return out
}

func cloneExpectedRows(rows [][]interface{}) [][]interface{} {
	out := make([][]interface{}, len(rows))
	for i, row := range rows {
		out[i] = append([]interface{}(nil), row...)
	}
	return out
}

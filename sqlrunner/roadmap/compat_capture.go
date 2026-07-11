package roadmap

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v2"
)

const CompatibilityExpectedVersion = 1

// CompatibilityExpectedFile stores canonical reference results captured from a compatibility engine.
type CompatibilityExpectedFile struct {
	Version int                         `yaml:"version"`
	Suite   string                      `yaml:"suite"`
	Cases   []CompatibilityExpectedCase `yaml:"cases"`
}

// CompatibilityExpectedCase stores one captured query or statement result.
type CompatibilityExpectedCase struct {
	ID           string                    `yaml:"id"`
	Kind         string                    `yaml:"kind"`
	Columns      []string                  `yaml:"columns,omitempty"`
	Types        []string                  `yaml:"types,omitempty"`
	Rows         [][]CompatibilityCell     `yaml:"rows,omitempty"`
	AffectedRows *int64                    `yaml:"affected_rows,omitempty"`
	Error        string                    `yaml:"error,omitempty"`
	Metadata     CompatibilityCaseMetadata `yaml:"metadata,omitempty"`
}

// CompatibilityCaseMetadata preserves feature metadata beside captured expected results.
type CompatibilityCaseMetadata struct {
	Feature       string   `yaml:"feature,omitempty"`
	Compatibility string   `yaml:"compatibility,omitempty"`
	Requires      []string `yaml:"requires,omitempty"`
}

// CompatibilityCell stores a canonical result cell in a YAML-friendly shape.
type CompatibilityCell struct {
	Null bool   `yaml:"null,omitempty"`
	Text string `yaml:"text,omitempty"`
}

// NewCompatibilityExpectedFile builds an expected-results file for a suite.
func NewCompatibilityExpectedFile(suite *Suite, cases []CompatibilityExpectedCase) CompatibilityExpectedFile {
	name := ""
	if suite != nil {
		name = suite.Name
	}
	return CompatibilityExpectedFile{
		Version: CompatibilityExpectedVersion,
		Suite:   name,
		Cases:   append([]CompatibilityExpectedCase(nil), cases...),
	}
}

// ParseCompatibilityExpected parses a captured expected-results file.
func ParseCompatibilityExpected(data []byte) (*CompatibilityExpectedFile, error) {
	var expected CompatibilityExpectedFile
	if err := yaml.UnmarshalStrict(data, &expected); err != nil {
		return nil, err
	}
	if expected.Version != CompatibilityExpectedVersion {
		return nil, fmt.Errorf("unsupported compatibility expected version %d", expected.Version)
	}
	if strings.TrimSpace(expected.Suite) == "" {
		return nil, fmt.Errorf("expected suite is required")
	}
	for i := range expected.Cases {
		expected.Cases[i].normalize()
		if expected.Cases[i].ID == "" {
			return nil, fmt.Errorf("case %d: id is required", i+1)
		}
	}
	return &expected, nil
}

// CaptureCompatibilityQueryCase captures a canonical query result for one test case.
func CaptureCompatibilityQueryCase(test TestCase, result QueryResult, options CanonicalOptions) CompatibilityExpectedCase {
	if test.Order == "rowsort" {
		options.SortRows = true
	}
	canonical := CanonicalizeQueryResult(result, options)
	return CompatibilityExpectedCase{
		ID:      test.ID,
		Kind:    "query",
		Columns: append([]string(nil), canonical.Columns...),
		Types:   append([]string(nil), canonical.Types...),
		Rows:    compatibilityRows(canonical.Rows),
		Metadata: CompatibilityCaseMetadata{
			Feature:       test.Feature,
			Compatibility: test.Compatibility,
			Requires:      append([]string(nil), test.Requires...),
		},
	}
}

// CaptureCompatibilityStatementCase captures a canonical statement result for one test case.
func CaptureCompatibilityStatementCase(test TestCase, affectedRows int64) CompatibilityExpectedCase {
	return CompatibilityExpectedCase{
		ID:           test.ID,
		Kind:         "statement",
		AffectedRows: &affectedRows,
		Metadata: CompatibilityCaseMetadata{
			Feature:       test.Feature,
			Compatibility: test.Compatibility,
			Requires:      append([]string(nil), test.Requires...),
		},
	}
}

// CompareCompatibilityQueryCase compares a captured query case with an actual query result.
func CompareCompatibilityQueryCase(expected CompatibilityExpectedCase, actual QueryResult, options CanonicalOptions) string {
	if expected.Kind != "" && expected.Kind != "query" {
		return fmt.Sprintf("expected case %s is %s, not query", expected.ID, expected.Kind)
	}
	canonicalActual := CanonicalizeQueryResult(actual, options)
	return CompareCanonicalQueryResults(expected.CanonicalQueryResult(), canonicalActual)
}

// CompareCompatibilityStatementCase compares a captured statement case with an affected row count.
func CompareCompatibilityStatementCase(expected CompatibilityExpectedCase, affectedRows int64) string {
	if expected.Kind != "" && expected.Kind != "statement" {
		return fmt.Sprintf("expected case %s is %s, not statement", expected.ID, expected.Kind)
	}
	if expected.AffectedRows == nil {
		return ""
	}
	if *expected.AffectedRows != affectedRows {
		return fmt.Sprintf("affected rows differ: expected %d, actual %d", *expected.AffectedRows, affectedRows)
	}
	return ""
}

// CanonicalQueryResult returns the captured query result as a comparable canonical result.
func (c CompatibilityExpectedCase) CanonicalQueryResult() CanonicalQueryResult {
	return CanonicalQueryResult{
		Columns: append([]string(nil), c.Columns...),
		Types:   append([]string(nil), c.Types...),
		Rows:    roadmapRows(c.Rows),
	}
}

// CaseByID returns the captured case with the requested id.
func (f CompatibilityExpectedFile) CaseByID(id string) (CompatibilityExpectedCase, bool) {
	for _, test := range f.Cases {
		if test.ID == id {
			return test, true
		}
	}
	return CompatibilityExpectedCase{}, false
}

func (c *CompatibilityExpectedCase) normalize() {
	c.ID = strings.TrimSpace(c.ID)
	c.Kind = strings.ToLower(strings.TrimSpace(c.Kind))
	c.Metadata.Feature = normalizeTag(c.Metadata.Feature)
	c.Metadata.Compatibility = normalizeTag(c.Metadata.Compatibility)
	c.Metadata.Requires = normalizeTags(c.Metadata.Requires)
	if c.Kind == "" {
		if len(c.Rows) > 0 || len(c.Columns) > 0 {
			c.Kind = "query"
		} else {
			c.Kind = "statement"
		}
	}
}

func compatibilityRows(rows [][]Cell) [][]CompatibilityCell {
	result := make([][]CompatibilityCell, len(rows))
	for i, row := range rows {
		result[i] = make([]CompatibilityCell, len(row))
		for j, cell := range row {
			result[i][j] = CompatibilityCell{Null: cell.Null, Text: cell.Text}
		}
	}
	return result
}

func roadmapRows(rows [][]CompatibilityCell) [][]Cell {
	result := make([][]Cell, len(rows))
	for i, row := range rows {
		result[i] = make([]Cell, len(row))
		for j, cell := range row {
			result[i][j] = Cell{Null: cell.Null, Text: cell.Text}
		}
	}
	return result
}

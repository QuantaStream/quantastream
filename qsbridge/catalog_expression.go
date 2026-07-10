package qsbridge

import "strings"

// CatalogExpressionPurpose identifies where catalog expression text is used.
type CatalogExpressionPurpose string

const (
	// CatalogExpressionPurposeUnknown means the catalog expression has not been classified.
	CatalogExpressionPurposeUnknown CatalogExpressionPurpose = ""
	// CatalogExpressionPurposeColumnDefault evaluates a missing INSERT column from row values.
	CatalogExpressionPurposeColumnDefault CatalogExpressionPurpose = "column_default"
	// CatalogExpressionPurposeTableSelector evaluates an ingest payload to choose a target table.
	CatalogExpressionPurposeTableSelector CatalogExpressionPurpose = "table_selector"
)

// CatalogExpressionPath identifies one row or payload value referenced by a catalog expression.
type CatalogExpressionPath struct {
	Parts []string
}

// NewCatalogExpressionPath creates a catalog expression path and drops empty parts.
func NewCatalogExpressionPath(parts ...string) CatalogExpressionPath {
	path := CatalogExpressionPath{Parts: make([]string, 0, len(parts))}
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		path.Parts = append(path.Parts, trimmed)
	}
	return path
}

// ParseCatalogExpressionPath parses a dotted catalog expression path.
func ParseCatalogExpressionPath(raw string) (CatalogExpressionPath, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return CatalogExpressionPath{}, false
	}
	parts := strings.Split(raw, ".")
	path := NewCatalogExpressionPath(parts...)
	if len(path.Parts) != len(parts) {
		return CatalogExpressionPath{}, false
	}
	return path, true
}

// Empty reports whether the path has no parts.
func (p CatalogExpressionPath) Empty() bool {
	return len(p.Parts) == 0
}

// String returns the dotted path form.
func (p CatalogExpressionPath) String() string {
	return strings.Join(p.Parts, ".")
}

// CatalogExpression stores schema-owned expression text plus planner-visible metadata.
type CatalogExpression struct {
	Raw          string
	Purpose      CatalogExpressionPurpose
	Dependencies []CatalogExpressionPath
}

// NewCatalogExpression creates catalog expression metadata and copies dependencies.
func NewCatalogExpression(raw string, purpose CatalogExpressionPurpose, dependencies ...CatalogExpressionPath) CatalogExpression {
	return CatalogExpression{
		Raw:          strings.TrimSpace(raw),
		Purpose:      purpose,
		Dependencies: cloneCatalogExpressionPaths(dependencies),
	}
}

// ColumnDefaultExpression creates metadata for a blind-column INSERT default expression.
func ColumnDefaultExpression(raw string, dependencies ...CatalogExpressionPath) CatalogExpression {
	return NewCatalogExpression(raw, CatalogExpressionPurposeColumnDefault, dependencies...)
}

// TableSelectorExpression creates metadata for a streaming ingest table selector expression.
func TableSelectorExpression(raw string, dependencies ...CatalogExpressionPath) CatalogExpression {
	return NewCatalogExpression(raw, CatalogExpressionPurposeTableSelector, dependencies...)
}

// WithDependencies returns a copy of the expression with dependency metadata replaced.
func (e CatalogExpression) WithDependencies(dependencies ...CatalogExpressionPath) CatalogExpression {
	e.Dependencies = cloneCatalogExpressionPaths(dependencies)
	return e
}

func cloneCatalogExpressionPaths(paths []CatalogExpressionPath) []CatalogExpressionPath {
	if len(paths) == 0 {
		return nil
	}
	cloned := make([]CatalogExpressionPath, 0, len(paths))
	for _, path := range paths {
		if path.Empty() {
			continue
		}
		cloned = append(cloned, CatalogExpressionPath{Parts: append([]string(nil), path.Parts...)})
	}
	return cloned
}

package roadmap

import (
	"math/big"
	"strings"
)

// CanonicalOptions controls result normalization for compatibility comparisons.
type CanonicalOptions struct {
	NormalizeColumns bool
	NormalizeTypes   bool
	TrimText         bool
	SortRows         bool
}

// DefaultCanonicalOptions returns conservative defaults for MySQL compatibility comparisons.
func DefaultCanonicalOptions() CanonicalOptions {
	return CanonicalOptions{
		NormalizeColumns: true,
		NormalizeTypes:   true,
		TrimText:         true,
	}
}

// CanonicalQueryResult is a normalized result-set representation for compatibility comparisons.
type CanonicalQueryResult struct {
	Columns []string
	Types   []string
	Rows    [][]Cell
}

// CanonicalizeQueryResult normalizes a query result without mutating the source result.
func CanonicalizeQueryResult(result QueryResult, options CanonicalOptions) CanonicalQueryResult {
	columns := append([]string(nil), result.Columns...)
	if options.NormalizeColumns {
		for i := range columns {
			columns[i] = canonicalColumn(columns[i])
		}
	}

	types := append([]string(nil), result.Types...)
	if options.NormalizeTypes {
		for i := range types {
			types[i] = CanonicalType(types[i])
		}
	}

	rows := cloneRows(result.Rows)
	for row := range rows {
		for column := range rows[row] {
			typeName := ""
			if column < len(types) {
				typeName = types[column]
			}
			rows[row][column] = canonicalCell(rows[row][column], typeName, options)
		}
	}
	if options.SortRows {
		sortRows(rows)
	}

	return CanonicalQueryResult{
		Columns: columns,
		Types:   types,
		Rows:    rows,
	}
}

// CompareCanonicalQueryResults compares two normalized result sets.
func CompareCanonicalQueryResults(expected, actual CanonicalQueryResult) string {
	if len(expected.Columns) > 0 && !equalStrings(expected.Columns, actual.Columns) {
		return "columns differ: expected " + strings.Join(expected.Columns, ",") + ", actual " + strings.Join(actual.Columns, ",")
	}
	if len(expected.Types) > 0 && !equalStrings(expected.Types, actual.Types) {
		return "types differ: expected " + strings.Join(expected.Types, ",") + ", actual " + strings.Join(actual.Types, ",")
	}
	return compareRows(expected.Rows, actual.Rows)
}

// CanonicalType maps common MySQL and QuantaStream type names into compatibility families.
func CanonicalType(typeName string) string {
	normalized := strings.ToUpper(strings.TrimSpace(typeName))
	switch normalized {
	case "INT", "INTEGER", "BIGINT", "SMALLINT", "MEDIUMINT", "TINYINT", "UINT", "UINT64":
		return "INTEGER"
	case "DECIMAL", "DEC", "NUMERIC", "NEWDECIMAL", "NUMBER":
		return "DECIMAL"
	case "DOUBLE", "FLOAT", "REAL", "FLOAT64":
		return "FLOAT"
	case "CHAR", "VARCHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT", "STRING", "VAR_STRING":
		return "TEXT"
	case "DATE", "DATETIME", "TIMESTAMP", "TIME":
		return "TIME"
	case "BOOL", "BOOLEAN":
		return "BOOLEAN"
	default:
		return normalized
	}
}

func canonicalColumn(column string) string {
	return strings.ToLower(strings.TrimSpace(column))
}

func canonicalCell(cell Cell, typeName string, options CanonicalOptions) Cell {
	if cell.Null {
		return cell
	}
	text := cell.Text
	if options.TrimText {
		text = strings.TrimSpace(text)
	}
	if canonicalNumericType(typeName) {
		text = canonicalNumber(text)
	}
	return Cell{Text: text}
}

func canonicalNumericType(typeName string) bool {
	switch CanonicalType(typeName) {
	case "INTEGER", "DECIMAL", "FLOAT":
		return true
	default:
		return false
	}
}

func canonicalNumber(text string) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return text
	}
	value, _, err := big.ParseFloat(trimmed, 10, 256, big.ToNearestEven)
	if err != nil {
		return text
	}
	if value.Sign() == 0 {
		return "0"
	}
	return value.Text('g', -1)
}

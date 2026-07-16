package roadmap

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Runner struct {
	DB         *sql.DB
	Engine     Engine
	Verbose    bool
	DumpActual bool
	Logf       func(string, ...interface{})
}

type Engine interface {
	Query(context.Context, string) (QueryResult, error)
	Exec(context.Context, string) (int64, error)
}

type caseAwareEngine interface {
	WithTestCase(TestCase) Engine
}

type SQLEngine struct {
	DB *sql.DB
}

const defaultCaseTimeout = 30 * time.Second

func (r Runner) Run(ctx context.Context, suite *Suite) Summary {
	summary := Summary{Suite: suite.Name, Results: make([]CaseResult, 0, len(suite.Tests))}
	engine := r.engine()
	for _, test := range suite.Tests {
		if test.Status == CaseSkip {
			summary.Results = append(summary.Results, CaseResult{ID: test.ID, Status: ResultSkip})
			continue
		}

		caseCtx, cancel := context.WithTimeout(ctx, test.CaseTimeout())
		if r.Verbose {
			r.logf("RUN    %s (%s)", test.ID, test.Kind)
			if strings.TrimSpace(test.SQL) != "" {
				r.logf("SQL    %s:\n%s", test.ID, test.SQL)
			}
		}
		started := time.Now()
		var details string
		caseEngine := engine
		if aware, ok := engine.(caseAwareEngine); ok {
			caseEngine = aware.WithTestCase(test)
		}
		sql := executableSQL(test)
		if test.Kind == "query" {
			actual, err := caseEngine.Query(caseCtx, sql)
			details = evaluateQuery(test, actual, err)
			if details != "" && r.DumpActual {
				r.logf("ACTUAL %s columns=%v rows=%s", test.ID, actual.Columns, formatRows(actual.Rows))
			}
		} else {
			affected, err := caseEngine.Exec(caseCtx, sql)
			details = evaluateStatement(test, affected, err)
		}
		result := classify(test, details)
		result.Duration = time.Since(started)
		if r.Verbose {
			if result.Details == "" {
				r.logf("DONE   %s %s in %s", result.Status, result.ID, result.Duration.Round(time.Millisecond))
			} else {
				r.logf("DONE   %s %s in %s: %s", result.Status, result.ID, result.Duration.Round(time.Millisecond), result.Details)
			}
		}
		summary.Results = append(summary.Results, result)
		caseErr := caseCtx.Err()
		cancel()
		if caseErr != nil {
			break
		}
	}
	return summary
}

func (r Runner) logf(format string, args ...interface{}) {
	if r.Logf != nil {
		r.Logf(format, args...)
	}
}

func (r Runner) engine() Engine {
	if r.Engine != nil {
		return r.Engine
	}
	return SQLEngine{DB: r.DB}
}

func (e SQLEngine) Query(ctx context.Context, statement string) (QueryResult, error) {
	if e.DB == nil {
		return QueryResult{}, fmt.Errorf("sql engine has no database")
	}
	rows, err := e.DB.QueryContext(ctx, statement)
	if err != nil {
		return QueryResult{}, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return QueryResult{}, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return QueryResult{}, err
	}
	types := make([]string, len(columnTypes))
	for i, columnType := range columnTypes {
		types[i] = strings.ToUpper(columnType.DatabaseTypeName())
	}

	result := QueryResult{Columns: columns, Types: types}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		dest := make([]interface{}, len(columns))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return QueryResult{}, err
		}
		row := make([]Cell, len(values))
		for i, value := range values {
			row[i] = actualCell(value)
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return QueryResult{}, err
	}
	return result, nil
}

func (e SQLEngine) Exec(ctx context.Context, statement string) (int64, error) {
	if e.DB == nil {
		return 0, fmt.Errorf("sql engine has no database")
	}
	result, err := e.DB.ExecContext(ctx, statement)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func evaluateQuery(test TestCase, actual QueryResult, queryErr error) string {
	if test.Expect.Error != "" {
		return evaluateExpectedError(test.Expect.Error, queryErr)
	}
	if queryErr != nil {
		return "unexpected error: " + queryErr.Error()
	}
	if len(test.Expect.Columns) > 0 && !equalStrings(test.Expect.Columns, actual.Columns) {
		return fmt.Sprintf("columns differ: expected %v, actual %v", test.Expect.Columns, actual.Columns)
	}
	if len(test.Expect.Types) > 0 {
		expectedTypes := upperStrings(test.Expect.Types)
		if !equalStrings(expectedTypes, actual.Types) {
			return fmt.Sprintf("types differ: expected %v, actual %v", expectedTypes, actual.Types)
		}
	}
	if test.Expect.RowCount != nil && len(actual.Rows) != *test.Expect.RowCount {
		return fmt.Sprintf("row count differs: expected %d, actual %d", *test.Expect.RowCount, len(actual.Rows))
	}

	if test.Expect.Rows == nil {
		return ""
	}
	expectedRows := expectedCellsForTypes(test.Expect.Rows, actual.Types)
	actualRows := cloneRows(actual.Rows)
	if test.Order == "rowsort" {
		sortRows(expectedRows)
		sortRows(actualRows)
	}
	if details := compareRows(expectedRows, actualRows); details != "" {
		return details
	}
	return ""
}

func evaluateStatement(test TestCase, affected int64, execErr error) string {
	if test.Expect.Error != "" {
		return evaluateExpectedError(test.Expect.Error, execErr)
	}
	if AdminDropMissingTableOK(test, execErr) {
		execErr = nil
		affected = 0
	}
	if execErr != nil {
		return "unexpected error: " + execErr.Error()
	}
	if test.Expect.AffectedRows != nil && affected != *test.Expect.AffectedRows {
		return fmt.Sprintf("affected rows differ: expected %d, actual %d", *test.Expect.AffectedRows, affected)
	}
	return ""
}

func evaluateExpectedError(expected string, actual error) string {
	if actual == nil {
		return "expected an error containing " + strconv.Quote(expected)
	}
	if !strings.Contains(strings.ToLower(actual.Error()), strings.ToLower(expected)) {
		return fmt.Sprintf("error differs: expected substring %q, actual %q", expected, actual.Error())
	}
	return ""
}

func classify(test TestCase, details string) CaseResult {
	passed := details == ""
	if test.Status == CaseXFail {
		if passed {
			return CaseResult{ID: test.ID, Status: ResultXPass, Details: "roadmap goal now passes; promote it to supported"}
		}
		return CaseResult{ID: test.ID, Status: ResultXFail, Details: details}
	}
	if passed {
		return CaseResult{ID: test.ID, Status: ResultPass}
	}
	return CaseResult{ID: test.ID, Status: ResultFail, Details: details}
}

func actualCell(value interface{}) Cell {
	if value == nil {
		return Cell{Null: true}
	}
	switch typed := value.(type) {
	case []byte:
		return Cell{Text: string(typed)}
	case time.Time:
		return Cell{Text: typed.UTC().Format(time.RFC3339Nano)}
	default:
		return Cell{Text: fmt.Sprint(typed)}
	}
}

func expectedCells(rows [][]interface{}) [][]Cell {
	return expectedCellsForTypes(rows, nil)
}

func expectedCellsForTypes(rows [][]interface{}, types []string) [][]Cell {
	result := make([][]Cell, len(rows))
	for i, row := range rows {
		result[i] = make([]Cell, len(row))
		for j, value := range row {
			if value == nil {
				result[i][j] = Cell{Null: true}
			} else {
				typeName := ""
				if j < len(types) {
					typeName = types[j]
				}
				result[i][j] = expectedCell(value, typeName)
			}
		}
	}
	return result
}

func expectedCell(value interface{}, typeName string) Cell {
	switch typed := value.(type) {
	case bool:
		if expectedBoolAsMySQLInteger(typeName) {
			if typed {
				return Cell{Text: "1"}
			}
			return Cell{Text: "0"}
		}
	}
	return Cell{Text: fmt.Sprint(value)}
}

func expectedBoolAsMySQLInteger(typeName string) bool {
	switch strings.ToUpper(strings.TrimSpace(typeName)) {
	case "BIT", "BOOL", "BOOLEAN":
		return false
	case "TINYINT", "INT", "INTEGER", "BIGINT", "SMALLINT", "MEDIUMINT":
		return true
	default:
		return false
	}
}

func compareRows(expected, actual [][]Cell) string {
	if len(expected) != len(actual) {
		return fmt.Sprintf("row count differs: expected %d, actual %d", len(expected), len(actual))
	}
	for row := range expected {
		if len(expected[row]) != len(actual[row]) {
			return fmt.Sprintf("row %d column count differs: expected %d, actual %d", row+1, len(expected[row]), len(actual[row]))
		}
		for column := range expected[row] {
			if !cellsEqual(expected[row][column], actual[row][column]) {
				return fmt.Sprintf("row %d column %d differs: expected %s, actual %s",
					row+1, column+1, formatCell(expected[row][column]), formatCell(actual[row][column]))
			}
		}
	}
	return ""
}

func cellsEqual(expected, actual Cell) bool {
	if expected == actual {
		return true
	}
	if expected.Null || actual.Null {
		return false
	}
	expectedNumber, expectedErr := strconv.ParseFloat(strings.TrimSpace(expected.Text), 64)
	actualNumber, actualErr := strconv.ParseFloat(strings.TrimSpace(actual.Text), 64)
	if expectedErr != nil || actualErr != nil {
		return false
	}
	diff := math.Abs(expectedNumber - actualNumber)
	scale := math.Max(1, math.Max(math.Abs(expectedNumber), math.Abs(actualNumber)))
	return diff <= scale*1e-9
}

func formatRows(rows [][]Cell) string {
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		cells := make([]string, 0, len(row))
		for _, cell := range row {
			cells = append(cells, formatCell(cell))
		}
		parts = append(parts, "["+strings.Join(cells, ", ")+"]")
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func formatCell(cell Cell) string {
	if cell.Null {
		return "NULL"
	}
	return strconv.Quote(cell.Text)
}

func sortRows(rows [][]Cell) {
	sort.Slice(rows, func(i, j int) bool {
		return rowKey(rows[i]) < rowKey(rows[j])
	})
}

func rowKey(row []Cell) string {
	var builder strings.Builder
	for _, cell := range row {
		if cell.Null {
			builder.WriteString("-1:")
		} else {
			builder.WriteString(strconv.Itoa(len(cell.Text)))
			builder.WriteByte(':')
			builder.WriteString(cell.Text)
		}
		builder.WriteByte('|')
	}
	return builder.String()
}

func cloneRows(rows [][]Cell) [][]Cell {
	result := make([][]Cell, len(rows))
	for i := range rows {
		result[i] = append([]Cell(nil), rows[i]...)
	}
	return result
}

func upperStrings(values []string) []string {
	result := make([]string, len(values))
	for i, value := range values {
		result[i] = strings.ToUpper(value)
	}
	return result
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

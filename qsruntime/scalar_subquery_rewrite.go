package qsruntime

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/QuantaStream/quantastream/qsbridge"
)

type scalarHavingSubqueryDescriptor struct {
	SubquerySQL      string
	ComparisonSQL    string
	ReplacementStart int
	ReplacementEnd   int
}

func (d scalarHavingSubqueryDescriptor) rewriteWithLiteral(sql string, literal string) string {
	return sql[:d.ReplacementStart] + literal + sql[d.ReplacementEnd:]
}

func scalarSubqueryRewriteTrace() qsbridge.OptimizationTrace {
	trace := qsbridge.NewOptimizationTrace()
	trace.Add(qsbridge.RewriteAppliedRecord(
		qsbridge.RewriteScalarSubqueryPreflight,
		"scalar subquery intent is not planner-native yet; evaluated uncorrelated HAVING subquery once and replaced it with a literal before native planning",
		"having(comparison(scalar_subquery))",
		"having(comparison(literal))",
	))
	return trace
}

func (r SQLRuntime) rewriteUncorrelatedHavingScalarSubquery(ctx context.Context, sql string, options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) (string, qsbridge.DiagnosticSet, qsbridge.OptimizationTrace, []PreflightHelperExecutionRequestReport, error, bool) {
	descriptor, ok := findUncorrelatedHavingScalarSubquery(sql)
	if !ok {
		return "", nil, qsbridge.OptimizationTrace{}, nil, nil, false
	}
	request := descriptor.scalarHelperRequest(options, values...)
	helper, err := r.executeScalarNativeSubqueryStep(ctx, request)
	helperReports := []PreflightHelperExecutionRequestReport{helper.Report()}
	diagnostics := append(qsbridge.DiagnosticSet(nil), helper.Diagnostics...)
	if err != nil || diagnostics.BlocksNative() {
		return "", diagnostics, qsbridge.OptimizationTrace{}, helperReports, err, true
	}
	cell, cellDiagnostics := scalarMaterializedCell(helper)
	diagnostics = append(diagnostics, cellDiagnostics...)
	if diagnostics.BlocksNative() {
		return "", diagnostics, qsbridge.OptimizationTrace{}, helperReports, nil, true
	}
	literal, literalDiagnostics := scalarSubqueryLiteral(cell)
	diagnostics = append(diagnostics, literalDiagnostics...)
	if diagnostics.BlocksNative() {
		return "", diagnostics, qsbridge.OptimizationTrace{}, helperReports, nil, true
	}
	rewritten := descriptor.rewriteWithLiteral(sql, literal)
	return rewritten, diagnostics, scalarSubqueryRewriteTrace(), helperReports, nil, true
}

func (d scalarHavingSubqueryDescriptor) scalarHelperRequest(options qsbridge.ExecutionOptions, values ...qsbridge.ParameterValue) PreflightHelperExecutionRequest {
	return PreflightHelperExecutionRequest{
		Plan: d.scalarHelperPlan(),
		SQL:  aliasScalarSubqueryProjection(d.SubquerySQL),
		Payload: PreflightHelperPayload{Scalar: &PreflightScalarHelperPayload{
			SubquerySQL: d.SubquerySQL,
			OutputName:  "scalar_subquery_value",
		}},
		Options: options,
		Values:  append([]qsbridge.ParameterValue(nil), values...),
	}
}

func aliasScalarSubqueryProjection(sql string) string {
	selectBody, ok := consumeScalarRewriteKeyword(sql, "select")
	if !ok {
		return sql
	}
	projection, source, ok := splitScalarRewriteTopLevelKeyword(selectBody, "from")
	if !ok || strings.Contains(strings.ToLower(projection), " as ") {
		return sql
	}
	return "select " + strings.TrimSpace(projection) + " as scalar_subquery_value from " + strings.TrimSpace(source)
}

func consumeScalarRewriteKeyword(text string, keyword string) (string, bool) {
	trimmed := strings.TrimSpace(text)
	lowered := strings.ToLower(trimmed)
	keyword = strings.ToLower(keyword)
	if !strings.HasPrefix(lowered, keyword) || !keywordBoundary(trimmed, len(keyword)) {
		return "", false
	}
	return strings.TrimSpace(trimmed[len(keyword):]), true
}

func splitScalarRewriteTopLevelKeyword(text string, keyword string) (string, string, bool) {
	index := topLevelKeywordIndex(text, keyword, 0)
	if index < 0 {
		return "", "", false
	}
	left := strings.TrimSpace(text[:index])
	right := strings.TrimSpace(text[index+len(keyword):])
	return left, right, left != "" && right != ""
}

func findUncorrelatedHavingScalarSubquery(sql string) (scalarHavingSubqueryDescriptor, bool) {
	having := topLevelKeywordIndex(sql, "having", 0)
	if having < 0 {
		return scalarHavingSubqueryDescriptor{}, false
	}
	regionEnd := len(sql)
	for _, keyword := range []string{"order by", "limit", "offset"} {
		if index := topLevelKeywordIndex(sql, keyword, having+len("having")); index >= 0 && index < regionEnd {
			regionEnd = index
		}
	}
	lower := strings.ToLower(sql)
	for i := having + len("having"); i < regionEnd; i++ {
		if sql[i] != '(' {
			continue
		}
		innerStart := nextNonSpace(sql, i+1)
		if innerStart >= regionEnd || !strings.HasPrefix(lower[innerStart:], "select") || !keywordBoundary(sql, innerStart+len("select")) {
			continue
		}
		closeParen := matchingParenIndex(sql, i)
		if closeParen < 0 || closeParen > regionEnd {
			continue
		}
		return scalarHavingSubqueryDescriptor{
			SubquerySQL:      strings.TrimSpace(sql[innerStart:closeParen]),
			ComparisonSQL:    strings.TrimSpace(sql[having+len("having") : regionEnd]),
			ReplacementStart: i,
			ReplacementEnd:   closeParen + 1,
		}, true
	}
	return scalarHavingSubqueryDescriptor{}, false
}

func scalarMaterializedCell(helper PreflightHelperExecutionResult) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if helper.Payload.Scalar != nil && (helper.Payload.Scalar.Materialized.Kind != "" || helper.Payload.Scalar.Materialized.Value != nil) {
		return helper.Payload.Scalar.Materialized, nil
	}
	return scalarSubqueryResultCell(helper.Result.Runtime.RowSet)
}

func scalarSubqueryResultCell(rowSet qsbridge.QuantaProjectedRowSet) (qsbridge.ResultCell, qsbridge.DiagnosticSet) {
	if diagnostics := rowSet.ValidateShape(); diagnostics.BlocksNative() {
		return qsbridge.ResultCell{}, diagnostics
	}
	if rowSet.CandidateCount() != 1 || len(rowSet.ProjectionVectors) != 1 || len(rowSet.ProjectionVectors[0].Values) != 1 {
		return qsbridge.ResultCell{}, helperExecutionDiagnostic(PreflightHelperPlanScalarSubquery, "scalar subquery must return exactly one row and one column")
	}
	return rowSet.ProjectionVectors[0].Values[0], nil
}

func scalarSubqueryLiteral(cell qsbridge.ResultCell) (string, qsbridge.DiagnosticSet) {
	switch v := cell.Value.(type) {
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case string:
		if cell.Kind == qsbridge.ValueInt || cell.Kind == qsbridge.ValueFloat {
			return v, nil
		}
		return "'" + strings.ReplaceAll(v, "'", "''") + "'", nil
	case nil:
		return "null", nil
	default:
		switch cell.Kind {
		case qsbridge.ValueInt, qsbridge.ValueFloat:
			return fmt.Sprint(v), nil
		}
		return "", helperExecutionDiagnostic(PreflightHelperPlanScalarSubquery, fmt.Sprintf("scalar subquery value %T cannot be injected as a literal", cell.Value))
	}
}

func topLevelKeywordIndex(sql string, keyword string, start int) int {
	lower := strings.ToLower(sql)
	keyword = strings.ToLower(keyword)
	depth := 0
	quote := rune(0)
	for i := start; i <= len(sql)-len(keyword); i++ {
		ch := rune(sql[i])
		if quote != 0 {
			if ch == quote {
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"':
			quote = ch
			continue
		case '(':
			depth++
			continue
		case ')':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth == 0 && strings.HasPrefix(lower[i:], keyword) && keywordBoundary(sql, i-1) && keywordBoundary(sql, i+len(keyword)) {
			return i
		}
	}
	return -1
}

func matchingParenIndex(sql string, open int) int {
	depth := 0
	quote := rune(0)
	for i := open; i < len(sql); i++ {
		ch := rune(sql[i])
		if quote != 0 {
			if ch == quote {
				quote = 0
			}
			continue
		}
		switch ch {
		case '\'', '"':
			quote = ch
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

func nextNonSpace(text string, start int) int {
	for i := start; i < len(text); i++ {
		if !unicode.IsSpace(rune(text[i])) {
			return i
		}
	}
	return len(text)
}

func keywordBoundary(text string, index int) bool {
	if index < 0 || index >= len(text) {
		return true
	}
	ch := rune(text[index])
	return !(unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_')
}

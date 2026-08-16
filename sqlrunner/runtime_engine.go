package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/QuantaStream/quantastream/qsbridge"
	"github.com/QuantaStream/quantastream/qsruntime"
	"github.com/QuantaStream/quantastream/sqlrunner/roadmap"
)

type runtimeRoadmapEngine struct {
	Runtime     qsruntime.SQLRuntime
	Inspect     bool
	Logf        func(string, ...interface{})
	Test        roadmap.TestCase
	lastProfile []roadmap.ProfileRow
}

func (e *runtimeRoadmapEngine) WithTestCase(test roadmap.TestCase) roadmap.Engine {
	if e == nil {
		return e
	}
	e.Test = test
	e.lastProfile = nil
	return e
}

func (e *runtimeRoadmapEngine) Query(ctx context.Context, statement string) (roadmap.QueryResult, error) {
	e.clearProfile()
	if e.Inspect {
		return e.inspect(ctx, statement)
	}
	result, err := e.Runtime.ExecuteSQL(ctx, statement, qsbridge.ExecutionOptions{})
	e.rememberProfile(result)
	e.logRuntimePlan(result)
	if err != nil {
		return roadmap.QueryResult{}, err
	}
	if result.Diagnostics.BlocksNative() {
		return roadmap.QueryResult{}, diagnosticsError(result.Diagnostics)
	}
	if result.Runtime.Diagnostics.BlocksNative() {
		return roadmap.QueryResult{}, diagnosticsError(result.Runtime.Diagnostics)
	}
	if err := e.applySessionActions(result); err != nil {
		return roadmap.QueryResult{}, err
	}
	return runtimeQueryResult(result)
}

func (e *runtimeRoadmapEngine) QueryProfile(ctx context.Context) ([]roadmap.ProfileRow, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(e.lastProfile) == 0 {
		return nil, nil
	}
	return append([]roadmap.ProfileRow(nil), e.lastProfile...), nil
}

func (e *runtimeRoadmapEngine) clearProfile() {
	if e == nil {
		return
	}
	e.lastProfile = nil
}

func (e *runtimeRoadmapEngine) rememberProfile(result qsruntime.SQLExecutionResult) {
	if e == nil {
		return
	}
	e.lastProfile = runtimeInstrumentationProfileRows(result.Instrumentation)
}

func runtimeInstrumentationProfileRows(snapshot qsruntime.ExecutionInstrumentationSnapshot) []roadmap.ProfileRow {
	if snapshot.Empty() {
		return nil
	}
	rows := make([]roadmap.ProfileRow, 0, len(snapshot.Timings)+len(snapshot.Counters)+len(snapshot.Events))
	for _, timing := range snapshot.Timings {
		rows = append(rows, roadmap.ProfileRow{
			Kind:    "timing",
			Section: timing.Section,
			Name:    timing.Name,
			Value:   timing.Duration.String(),
			Detail:  timing.Detail,
		})
	}
	for _, counter := range snapshot.Counters {
		rows = append(rows, roadmap.ProfileRow{
			Kind:    "counter",
			Section: counter.Section,
			Name:    counter.Name,
			Value:   strconv.FormatUint(counter.Value, 10),
			Detail:  counter.Detail,
		})
	}
	for _, event := range snapshot.Events {
		rows = append(rows, roadmap.ProfileRow{
			Kind:    "event",
			Section: event.Section,
			Name:    event.Name,
			Value:   event.Value,
			Detail:  event.Detail,
		})
	}
	return rows
}

func (e runtimeRoadmapEngine) logRuntimePlan(result qsruntime.SQLExecutionResult) {
	e.logPredicates(result.Request.Bound.Prepared.Query.Predicates)
	e.logIntermediate(result.Intermediate)
	e.logExecutionProbes(result.Runtime.Probes)
	e.logExecutionInstrumentation(result.Instrumentation)
}

func (e runtimeRoadmapEngine) logPredicates(predicates []qsbridge.Predicate) {
	if e.Logf == nil {
		return
	}
	for i, predicate := range predicates {
		e.Logf("RUNTIME predicate[%d] placement=%s scope=%s combinator=%s expr=%v",
			i, predicate.Placement, predicate.Scope, predicate.Combinator, predicate.Expr)
	}
}

func (e runtimeRoadmapEngine) logIntermediate(query qsbridge.QuantaIntermediateQuery) {
	if e.Logf == nil {
		return
	}
	for i, fragment := range query.Fragments {
		e.Logf("RUNTIME fragment[%d] index=%s field=%s op=%s bsi=%s value=%v values=%d null=%v negate=%v",
			i, fragment.Index, fragment.Field, fragment.Operation, fragment.BSIOp, fragment.Value, len(fragment.Values), fragment.NullCheck, fragment.Negate)
	}
	e.logFilterTree("root", query.Filter)
}

func (e runtimeRoadmapEngine) logFilterTree(path string, filter qsbridge.QuantaFilterExpression) {
	if e.Logf == nil || filter.Empty() {
		return
	}
	if filter.Leaf() {
		fragment := filter.Fragment
		e.Logf("RUNTIME filter[%s] op=%s index=%s field=%s fragment_op=%s bsi=%s value=%v values=%d literals=%d null=%v negate=%v",
			path, filter.Operation, fragment.Index, fragment.Field, fragment.Operation, fragment.BSIOp, fragment.Value, len(fragment.Values), len(fragment.Literals), fragment.NullCheck, fragment.Negate)
		return
	}
	if filter.CandidateSetLeaf() {
		e.Logf("RUNTIME filter[%s] op=%s candidate_index=%s candidate_rows=%d",
			path, filter.Operation, filter.CandidateSet.Index, len(filter.CandidateSet.Rownums))
		return
	}
	e.Logf("RUNTIME filter[%s] op=%s children=%d", path, filter.Operation, len(filter.Children))
	for i, child := range filter.Children {
		e.logFilterTree(fmt.Sprintf("%s.%d", path, i), child)
	}
}

func (e runtimeRoadmapEngine) logExecutionProbes(probes []qsruntime.ExecutionProbe) {
	if e.Logf == nil {
		return
	}
	for _, probe := range probes {
		e.Logf("RUNTIME probe section=%s name=%s value=%s detail=%s",
			probe.Section, probe.Name, probe.Value, probe.Detail)
	}
}

func (e runtimeRoadmapEngine) logExecutionInstrumentation(snapshot qsruntime.ExecutionInstrumentationSnapshot) {
	if e.Logf == nil || snapshot.Empty() {
		return
	}
	for _, timing := range snapshot.Timings {
		e.Logf("RUNTIME timing section=%s name=%s value=%s detail=%s",
			timing.Section, timing.Name, timing.Duration, timing.Detail)
	}
	for _, counter := range snapshot.Counters {
		e.Logf("RUNTIME counter section=%s name=%s value=%d detail=%s",
			counter.Section, counter.Name, counter.Value, counter.Detail)
	}
	for _, event := range snapshot.Events {
		e.Logf("RUNTIME event section=%s name=%s value=%s detail=%s",
			event.Section, event.Name, event.Value, event.Detail)
	}
}

func (e runtimeRoadmapEngine) inspect(ctx context.Context, statement string) (roadmap.QueryResult, error) {
	if err := ctx.Err(); err != nil {
		return roadmap.QueryResult{}, err
	}
	result := e.Runtime.InspectSQL(statement, qsbridge.ExecutionOptions{})
	if result.Diagnostics.BlocksNative() {
		if e.expectedDiagnosticsMatch(result.Diagnostics) {
			return runtimeInspectionQueryResult(result), nil
		}
		return roadmap.QueryResult{}, diagnosticsError(result.Diagnostics)
	}
	if result.Runtime.Diagnostics.BlocksNative() {
		if e.expectedDiagnosticsMatch(result.Runtime.Diagnostics) {
			return runtimeInspectionQueryResult(result), nil
		}
		return roadmap.QueryResult{}, diagnosticsError(result.Runtime.Diagnostics)
	}
	return runtimeInspectionQueryResult(result), nil
}

func (e runtimeRoadmapEngine) expectedDiagnosticsMatch(diagnostics qsbridge.DiagnosticSet) bool {
	if len(e.Test.Diagnostics) == 0 {
		return false
	}
	actual := diagnostics.Codes()
	if len(actual) != len(e.Test.Diagnostics) {
		return false
	}
	expected := make(map[string]int, len(e.Test.Diagnostics))
	for _, code := range e.Test.Diagnostics {
		expected[strings.ToLower(strings.TrimSpace(code))]++
	}
	for _, code := range actual {
		key := strings.ToLower(strings.TrimSpace(string(code)))
		if expected[key] == 0 {
			return false
		}
		expected[key]--
	}
	return true
}

func (e *runtimeRoadmapEngine) Exec(ctx context.Context, statement string) (int64, error) {
	e.clearProfile()
	if e.Inspect {
		return 0, fmt.Errorf("runtime inspection does not execute statements")
	}
	result, err := e.Runtime.ExecuteSQL(ctx, statement, qsbridge.ExecutionOptions{})
	e.rememberProfile(result)
	e.logRuntimePlan(result)
	if err != nil {
		return 0, err
	}
	if result.Diagnostics.BlocksNative() {
		return 0, diagnosticsError(result.Diagnostics)
	}
	if result.Runtime.Diagnostics.BlocksNative() {
		return 0, diagnosticsError(result.Runtime.Diagnostics)
	}
	if err := e.applySessionActions(result); err != nil {
		return 0, err
	}
	return int64(result.Runtime.Statement.AffectedRows), nil
}

func (e *runtimeRoadmapEngine) applySessionActions(result qsruntime.SQLExecutionResult) error {
	if e == nil {
		return nil
	}
	actions := runtimeRoadmapSessionActions(result)
	if len(actions) == 0 {
		return nil
	}
	transition := e.Runtime.Session.PreviewSessionTransition(actions)
	if transition.Diagnostics.BlocksNative() {
		return diagnosticsError(transition.Diagnostics)
	}
	e.Runtime.Session = transition.After
	return nil
}

func runtimeRoadmapSessionActions(result qsruntime.SQLExecutionResult) []qsbridge.SessionAction {
	if len(result.Runtime.Statement.SessionActions) > 0 {
		return append([]qsbridge.SessionAction(nil), result.Runtime.Statement.SessionActions...)
	}
	if len(result.Request.Statement.SessionActions) > 0 {
		return append([]qsbridge.SessionAction(nil), result.Request.Statement.SessionActions...)
	}
	return nil
}

func runtimeQueryResult(result qsruntime.SQLExecutionResult) (roadmap.QueryResult, error) {
	queryResult := roadmap.QueryResult{
		Columns: make([]string, 0, len(result.Request.ResultColumns)),
		Types:   make([]string, 0, len(result.Request.ResultColumns)),
	}
	for _, column := range result.Request.ResultColumns {
		queryResult.Columns = append(queryResult.Columns, column.Name)
		queryResult.Types = append(queryResult.Types, roadmapTypeName(column.Type))
	}

	chunk, diagnostics := result.Runtime.RowSet.ToResultChunk(0, true)
	if diagnostics.BlocksNative() {
		return queryResult, diagnosticsError(diagnostics)
	}
	for _, row := range chunk.Rows {
		cells := make([]roadmap.Cell, len(row))
		for i, cell := range row {
			cells[i] = roadmapCell(cell)
		}
		queryResult.Rows = append(queryResult.Rows, cells)
	}
	return queryResult, nil
}

func runtimeInspectionQueryResult(result qsruntime.SQLInspectionResult) roadmap.QueryResult {
	columns := qsruntime.ExecutionInspectionResultColumns()
	queryResult := roadmap.QueryResult{
		Columns: make([]string, 0, len(columns)),
		Types:   make([]string, 0, len(columns)),
	}
	for _, column := range columns {
		queryResult.Columns = append(queryResult.Columns, column.Name)
		queryResult.Types = append(queryResult.Types, roadmapTypeName(column.Type))
	}

	chunk := result.ResultChunk(0, true)
	for _, row := range chunk.Rows {
		cells := make([]roadmap.Cell, len(row))
		for i, cell := range row {
			cells[i] = roadmapCell(cell)
		}
		queryResult.Rows = append(queryResult.Rows, cells)
	}
	return queryResult
}

func diagnosticsError(diagnostics qsbridge.DiagnosticSet) error {
	if len(diagnostics) == 0 {
		return fmt.Errorf("runtime diagnostics blocked execution")
	}
	messages := make([]string, 0, len(diagnostics))
	for _, diagnostic := range diagnostics {
		if diagnostic.BlocksNative() {
			messages = append(messages, diagnostic.Error())
		}
	}
	if len(messages) == 0 {
		return fmt.Errorf("runtime diagnostics blocked execution")
	}
	return errors.New(strings.Join(messages, "; "))
}

func roadmapTypeName(dataType qsbridge.DataType) string {
	switch dataType {
	case qsbridge.DataTypeBool:
		return "BOOL"
	case qsbridge.DataTypeInt:
		return "BIGINT"
	case qsbridge.DataTypeFloat:
		return "DOUBLE"
	case qsbridge.DataTypeString:
		return "VARCHAR"
	case qsbridge.DataTypeTime:
		return "DATETIME"
	default:
		return strings.ToUpper(string(dataType))
	}
}

func roadmapCell(cell qsbridge.ResultCell) roadmap.Cell {
	if cell.Kind == qsbridge.ValueNull || cell.Value == nil {
		return roadmap.Cell{Null: true}
	}
	switch value := cell.Value.(type) {
	case time.Time:
		return roadmap.Cell{Text: value.UTC().Format(time.RFC3339Nano)}
	case fmt.Stringer:
		return roadmap.Cell{Text: value.String()}
	case string:
		return roadmap.Cell{Text: value}
	case []byte:
		return roadmap.Cell{Text: string(value)}
	case int:
		return roadmap.Cell{Text: strconv.Itoa(value)}
	case int64:
		return roadmap.Cell{Text: strconv.FormatInt(value, 10)}
	case uint64:
		return roadmap.Cell{Text: strconv.FormatUint(value, 10)}
	case float64:
		return roadmap.Cell{Text: strconv.FormatFloat(value, 'g', -1, 64)}
	default:
		return roadmap.Cell{Text: fmt.Sprint(value)}
	}
}
